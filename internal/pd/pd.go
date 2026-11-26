// Package pd 实现 PD（Prefill/Decode）分离的网关层编排（M7）。
//
// 两种引擎协议模式：
//
//   - sglang（HTTP PD，双池 + bootstrap 注入）：
//     分离路径把请求发往 P 池副本，并在 JSON body 注入 bootstrap_* 三元组
//     （bootstrap_host / bootstrap_port / bootstrap_room，指向 D 池副本的
//     --disaggregation-bootstrap-port 监听端口）；SGLang prefill 实例完成 prefill 后
//     经 KV 传输引擎把 KV 交给 decode 实例，decode 产出完整响应经 prefill 回流客户端。
//     网关只需选对 (P, D) 副本对并注入 bootstrap，其余为引擎间协作。
//
//   - vllm（proxy 编排式两段请求，vLLM 0.9 disaggregated_prefill_proxy 协议）：
//     阶段1 发 P 池：max_tokens=1、stream=false、kv_transfer_params 指向 D 池
//     （do_remote_decode=true），取回响应中的 kv_transfer_params（含 remote_engine_id 等）；
//     阶段2 发 D 池：原始 body + 该 kv_transfer_params，D 池产出最终响应（含 SSE）流回客户端。
//
// 路径决策：估算 prompt token 量，短请求（< path_threshold_prompt_tokens）inline 直连
// D 池端到端服务（省一次 KV 传输），长请求走分离路径。
package pd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	"infer-router/internal/metrics"
	"infer-router/internal/proxy"
	"infer-router/internal/registry"
)

// 支持的 pd.mode 取值。
const (
	ModeSglang = "sglang"
	ModeVllm   = "vllm"
)

// SGLang decode 实例 --disaggregation-bootstrap-port 的引擎默认值
// （backend 未配置 bootstrap_port 时的兜底）。
const DefaultSglangBootstrapPort = 8998

// Cfg pd: 配置段（mode 为空 = PD 关闭，全部后端按普通池路由）。
type Cfg struct {
	Mode         string            `yaml:"mode" json:"mode"`                                 // "" | sglang | vllm
	ThresholdTok int               `yaml:"path_threshold_prompt_tokens" json:"path_threshold_prompt_tokens"` // inline/分离路径的 token 阈值
	BootstrapPort int              `yaml:"default_bootstrap_port" json:"default_bootstrap_port"`             // decode 未配置 bootstrap_port 时的兜底端口
	RemoteTPSize int               `yaml:"remote_tp_size" json:"remote_tp_size"`             // vllm 模式 remote_tp_size 兜底（1）
	InjectHeaders map[string]string `yaml:"inject_headers" json:"inject_headers"`             // 分离请求额外注入的静态头
}

// Bootstrap sglang 模式注入 prefill 请求体的三元组。
type Bootstrap struct {
	Host string
	Port int
	Room uint64
}

// ---------------------------------------------------------------- room 生成

// roomGen 进程级实例盐（随机 32bit）：跨进程部署时降低 room 碰撞概率；
// 单进程内由原子计数器保证全局唯一（跨热更代际也唯一）。
var roomSalt = func() uint64 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.LittleEndian.PutUint32(b[:], 1)
	}
	return uint64(binary.LittleEndian.Uint32(b[:]))
}()

var roomCounter atomic.Uint64

// NextRoom 生成下一个 bootstrap_room（u64 唯一）。
func NextRoom() uint64 {
	n := roomCounter.Add(1)
	return roomSalt<<32 | (n & 0xFFFFFFFF)
}

// ---------------------------------------------------------------- 地址推导

// BootstrapHost 返回 decode 副本的 bootstrap 主机：优先 backend 显式配置，
// 否则从 base_url 的 host 推导（同机部署时 prefill 可直达）。
func BootstrapHost(d *registry.State) string {
	if d.BootstrapHost != "" {
		return d.BootstrapHost
	}
	if u, err := url.Parse(d.BaseURL); err == nil && u.Hostname() != "" {
		return u.Hostname()
	}
	return "127.0.0.1"
}

// BootstrapPort 返回 decode 副本的 bootstrap 端口：
// backend 配置 > pd.default_bootstrap_port > base_url 端口（vllm 语义）> SGLang 默认 8998。
func BootstrapPort(d *registry.State, cfgPort int) int {
	if d.BootstrapPort > 0 {
		return d.BootstrapPort
	}
	if cfgPort > 0 {
		return cfgPort
	}
	if u, err := url.Parse(d.BaseURL); err == nil {
		if p := u.Port(); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				return v
			}
		}
	}
	return DefaultSglangBootstrapPort
}

// ---------------------------------------------------------------- body 注入

// InjectSglang 把 bootstrap 三元组写入请求 JSON body（路由器权威覆盖既有字段）。
// body 必须是 JSON object（调用方已预校验）。
func InjectSglang(body []byte, b Bootstrap) ([]byte, error) {
	return injectFields(body, map[string]any{
		"bootstrap_host": b.Host,
		"bootstrap_port": b.Port,
		"bootstrap_room": b.Room,
	})
}

// BuildVllmPrefillBody 构造阶段1（prefill）请求体：
// max_tokens=1、stream=false、kv_transfer_params 指向 decode 目标。
func BuildVllmPrefillBody(body []byte, remoteHost string, remotePort, remoteTP int) ([]byte, error) {
	return injectFields(body, map[string]any{
		"max_tokens": 1,
		"stream":     false,
		"kv_transfer_params": map[string]any{
			"do_remote_decode": true,
			"do_remote_prefill": false,
			"remote_engine_id": nil,
			"remote_host":      remoteHost,
			"remote_port":      remotePort,
			"remote_tp_size":   remoteTP,
		},
	})
}

// InjectKvParams 把阶段1返回的 kv_transfer_params 写入阶段2（decode）请求体。
func InjectKvParams(body []byte, kvParams map[string]any) ([]byte, error) {
	return injectFields(body, map[string]any{"kv_transfer_params": kvParams})
}

// injectFields 以 map 覆写方式合并字段后重新序列化（保留其余字段）。
func injectFields(body []byte, fields map[string]any) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("body is not a json object: %w", err)
	}
	for k, v := range fields {
		m[k] = v
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExtractKVParams 从阶段1响应 JSON 中提取 kv_transfer_params。
func ExtractKVParams(respBody []byte) (map[string]any, bool) {
	var m struct {
		KVTransferParams map[string]any `json:"kv_transfer_params"`
	}
	if json.Unmarshal(respBody, &m) != nil || m.KVTransferParams == nil {
		return nil, false
	}
	return m.KVTransferParams, true
}

// ---------------------------------------------------------------- vLLM 两段编排

// ServeVllm 执行 vLLM proxy 编排式 PD 两段请求并流式回写客户端。
//
// 阶段1按 prefillRanked 做首字节前 failover（语义同 proxy.RetryConfig）；
// 阶段2固定发往 KV 传输目标副本 decode（阶段1的 remote_* 指向它）——
// KV 只落在该副本，向其他 decode 重试会缺 KV，因此阶段2不做跨副本 failover。
func ServeVllm(w http.ResponseWriter, r *http.Request,
	prefillRanked []*registry.State, decode *registry.State,
	body []byte, model string, retry proxy.RetryConfig, cfg Cfg) {

	client := &http.Client{} // 不设总超时：SSE 长响应由客户端断开控制

	maxP := 1
	if retry.Enabled && len(prefillRanked) > 1 {
		maxP = retry.Max + 1
		if maxP > len(prefillRanked) {
			maxP = len(prefillRanked)
		}
	}

	remoteHost := BootstrapHost(decode)
	remotePort := BootstrapPort(decode, cfg.BootstrapPort)
	tp := cfg.RemoteTPSize
	if tp < 1 {
		tp = 1
	}

	var kvParams map[string]any
	for idx := 0; idx < maxP; idx++ {
		st := prefillRanked[idx]
		pb, err := BuildVllmPrefillBody(body, remoteHost, remotePort, tp)
		if err != nil {
			http.Error(w, `{"error":{"message":"pd prefill body build failed: `+err.Error()+`"}}`, http.StatusBadGateway)
			return
		}
		resp, err := pdPost(r.Context(), client, st, pb)
		if err != nil || resp.StatusCode >= http.StatusInternalServerError {
			if resp != nil {
				drainClose(resp)
			}
			st.RecordFailure()
			if idx+1 < maxP {
				next := prefillRanked[idx+1].Name
				metrics.Retries.WithLabelValues(st.Name, next).Inc()
				continue
			}
			metrics.PdBootstrap.WithLabelValues("fail").Inc()
			classifyReq(st, http.StatusBadGateway, model)
			http.Error(w, `{"error":{"message":"pd prefill phase failed"}}`, http.StatusBadGateway)
			return
		}
		if resp.StatusCode >= http.StatusBadRequest { // 4xx 透传（400/404 等客户端语义错误）
			passthroughStatus(w, resp, st, model)
			return
		}
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		st.RecordSuccess()
		kv, ok := ExtractKVParams(rb)
		if !ok {
			metrics.PdBootstrap.WithLabelValues("fail").Inc()
			classifyReq(st, http.StatusBadGateway, model)
			http.Error(w, `{"error":{"message":"pd prefill returned no kv_transfer_params"}}`, http.StatusBadGateway)
			return
		}
		kvParams = kv
		metrics.PdBootstrap.WithLabelValues("ok").Inc()
		metrics.PdPair.WithLabelValues(st.Name, decode.Name).Inc()
		break
	}

	// 阶段2：原始 body + kv_transfer_params → decode；流式回写客户端
	db, err := InjectKvParams(body, kvParams)
	if err != nil {
		http.Error(w, `{"error":{"message":"pd decode body build failed: `+err.Error()+`"}}`, http.StatusBadGateway)
		return
	}
	resp, err := pdPost(r.Context(), client, decode, db)
	if err != nil {
		decode.RecordFailure()
		classifyReq(decode, http.StatusBadGateway, model)
		http.Error(w, `{"error":{"message":"pd decode phase failed"}}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusInternalServerError {
		decode.RecordFailure()
		classifyReq(decode, resp.StatusCode, model)
		http.Error(w, `{"error":{"message":"pd decode phase failed"}}`, resp.StatusCode)
		return
	}
	decode.RecordSuccess()
	streamPassthrough(w, resp)
	classifyReq(decode, resp.StatusCode, model)
}

// pdPost 向副本发 POST /v1/chat/completions（保留客户端的鉴权与内容类型头）。
func pdPost(ctx context.Context, client *http.Client, st *registry.State, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, st.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	return client.Do(req)
}

// streamPassthrough 把上游响应流式复制给客户端（逐块 flush，SSE 安全）。
func streamPassthrough(w http.ResponseWriter, resp *http.Response) {
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			_ = rc.Flush()
		}
		if err != nil {
			return
		}
	}
}

// passthroughStatus 把 4xx 上游响应原样回给客户端并归档指标。
func passthroughStatus(w http.ResponseWriter, resp *http.Response, st *registry.State, model string) {
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
	classifyReq(st, resp.StatusCode, model)
}

func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
}

// classifyReq 复刻 proxy.classify 的指标归档（pd 包内独立实现避免暴露内部类型）。
func classifyReq(st *registry.State, status int, model string) {
	switch {
	case status < http.StatusBadRequest, status >= http.StatusInternalServerError:
		metrics.ReqTotal.WithLabelValues(st.Name, http.StatusText(status)).Inc()
	default:
		if status == http.StatusNotFound && model != "" {
			metrics.ModelMismatch.WithLabelValues(st.Name, model).Inc()
		}
		metrics.ReqTotal.WithLabelValues(st.Name, http.StatusText(status)).Inc()
	}
}
