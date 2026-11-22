// Package app 组装 infer-router 的运行态与 HTTP 处理链：
// 配置加载与默认值、registry/picker/balancer 构建与 mux 装配、
// 热加载（Swap 原子换装 + SIGHUP/管理 API 触发）与探针端点。
// cmd/router 与集成测试共用同一装配路径，保证被测对象与生产行为一致。
package app

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"infer-router/internal/affinity"
	"infer-router/internal/balancer"
	"infer-router/internal/metrics"
	"infer-router/internal/proxy"
	"infer-router/internal/registry"
)

type BreakerCfg struct {
	FailThreshold int     `yaml:"fail_threshold"`
	CooldownSec   float64 `yaml:"cooldown_s"`
}

type AffinityCfg struct {
	Alpha        float64 `yaml:"alpha"`         // bounded load 系数，cap = ceil(alpha × 平均在途)
	StickyWindow int     `yaml:"sticky_window"` // 粘性统计窗口容量
	Tree         TreeCfg `yaml:"tree"`          // 混合前缀树（M3）
}

// TreeCfg 混合前缀树配置（仅 prefix_affinity 生效，缺省开启）。
type TreeCfg struct {
	Enabled           *bool   `yaml:"enabled"`             // 缺省 true
	MaxSize           int     `yaml:"max_tree_size"`       // 全局节点上限，缺省 65536
	EvictionIntervalS float64 `yaml:"eviction_interval_s"` // LRU 驱逐周期，缺省 60
	CacheThreshold    float64 `yaml:"cache_threshold"`     // 命中判定比例，缺省 0.5
	ChunkChars        int     `yaml:"chunk_chars"`         // 归一化分块粒度（rune），缺省 256
}

// KVCfg KV 容量感知路由配置（M5，差异化能力）。
type KVCfg struct {
	Enabled       *bool   `yaml:"enabled"`        // 高水位分流，缺省 true
	HighWatermark float64 `yaml:"high_watermark"` // KV 高水位阈值，缺省 0.9
	TopK          int     `yaml:"top_k"`          // 分流决策窗口（rendezvous 前 k 候选），缺省 2
	MetricName    string  `yaml:"metric_name"`    // 副本 /metrics 的 KV 指标名，缺省 gpu_cache_usage_perc（vLLM）
	CapScale      *bool   `yaml:"cap_scale"`      // 负载上限按 KV 余量缩放，缺省 false
}

// RetryCfg 请求级 failover 配置。指针字段区分"未配置"与显式 false，保证缺省值语义。
type RetryCfg struct {
	Enabled             *bool `yaml:"enabled"`                // 缺省 true
	Max                 int   `yaml:"max"`                    // 额外重试次数，缺省 1
	RetryOn5xx          *bool `yaml:"retry_on_5xx"`           // 缺省 true
	RetryOnConnectError *bool `yaml:"retry_on_connect_error"` // 缺省 true
}

func boolVal(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

type Config struct {
	Addr              string             `yaml:"addr"`
	Strategy          string             `yaml:"strategy"`
	HealthIntervalSec int                `yaml:"health_interval_s"`
	AdminToken        string             `yaml:"admin_token"` // 管理接口令牌；留空 = 仅允许 localhost 调用
	Breaker           BreakerCfg         `yaml:"breaker"`
	Affinity          AffinityCfg        `yaml:"affinity"`
	Retry             RetryCfg           `yaml:"retry"`
	KV                KVCfg              `yaml:"kv"`
	Backends          []registry.Backend `yaml:"backends"`
}

// LoadConfig 读取配置文件并补齐默认值。
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&cfg)
	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.HealthIntervalSec <= 0 {
		cfg.HealthIntervalSec = 5
	}
	if cfg.Breaker.FailThreshold <= 0 {
		cfg.Breaker.FailThreshold = 3
	}
	if cfg.Breaker.CooldownSec <= 0 {
		cfg.Breaker.CooldownSec = 15
	}
	if cfg.Retry.Max <= 0 {
		cfg.Retry.Max = 1
	}
	if cfg.Affinity.Tree.MaxSize <= 0 {
		cfg.Affinity.Tree.MaxSize = 65536
	}
	if cfg.Affinity.Tree.EvictionIntervalS <= 0 {
		cfg.Affinity.Tree.EvictionIntervalS = 60
	}
	if cfg.Affinity.Tree.CacheThreshold <= 0 {
		cfg.Affinity.Tree.CacheThreshold = 0.5
	}
	if cfg.Affinity.Tree.ChunkChars <= 0 {
		cfg.Affinity.Tree.ChunkChars = 256
	}
	if cfg.KV.HighWatermark <= 0 {
		cfg.KV.HighWatermark = 0.9
	}
	if cfg.KV.TopK <= 0 {
		cfg.KV.TopK = 2
	}
	if cfg.KV.MetricName == "" {
		cfg.KV.MetricName = "gpu_cache_usage_perc"
	}
}

// resolvedRetry 把指针形态的 RetryCfg 解析为 proxy 层的具体配置（缺省全开、max=1）。
func (c *Config) resolvedRetry() proxy.RetryConfig {
	return proxy.RetryConfig{
		Enabled:             boolVal(c.Retry.Enabled, true),
		Max:                 c.Retry.Max,
		RetryOn5xx:          boolVal(c.Retry.RetryOn5xx, true),
		RetryOnConnectError: boolVal(c.Retry.RetryOnConnectError, true),
	}
}

// Runtime 持有路由运行态依赖（registry、策略选择器）。
// 组件以代（generation）为单位整体换装：热更时 Swap 原子替换整组字段，
// 已持有旧组件引用的在途请求自然排空；mu 保护字段集的一致性读取。
type Runtime struct {
	mu     sync.RWMutex
	Cfg    *Config
	Reg    *registry.Registry
	Bal    balancer.Balancer
	Picker *affinity.Picker // 仅 prefix_affinity 策略非 nil
	Retry  proxy.RetryConfig

	probeCancel func() // 当前 registry 探活循环的取消器（Swap 时一并轮转）

	probeInterval time.Duration // 探活周期（Swap 后自动重启探活循环用）
}

// snapshot 一致性读取当前代组件。请求处理以 snapshot 中的引用为准——
// 即使热更在请求处理中途发生，本次请求仍完整走旧组件直到结束（排空语义）。
func (rt *Runtime) snapshot() (*Config, *registry.Registry, balancer.Balancer, *affinity.Picker, proxy.RetryConfig) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.Cfg, rt.Reg, rt.Bal, rt.Picker, rt.Retry
}

// buildComponents 按配置构建一整代组件（registry + 策略选择器）。
func buildComponents(cfg *Config) (*registry.Registry, balancer.Balancer, *affinity.Picker) {
	reg := registry.New(
		cfg.Backends,
		cfg.Breaker.FailThreshold,
		time.Duration(cfg.Breaker.CooldownSec*float64(time.Second)),
	)
	reg.SetKVMetric(cfg.KV.MetricName) // 探活循环顺带抓 KV 使用率（M5）

	var bal balancer.Balancer
	var picker *affinity.Picker
	switch cfg.Strategy {
	case "least_queue":
		bal = &balancer.LeastQueue{}
	case "prefix_affinity":
		picker = affinity.NewPicker(cfg.Affinity.Alpha, cfg.Affinity.StickyWindow)
		metrics.StickyRatioFn = picker.StickyRatio
		if boolVal(cfg.Affinity.Tree.Enabled, true) { // 混合前缀树（缺省开启）
			tr := affinity.NewTree(cfg.Affinity.Tree.MaxSize)
			tr.StartEviction(time.Duration(cfg.Affinity.Tree.EvictionIntervalS * float64(time.Second)))
			picker.AttachTree(tr, cfg.Affinity.Tree.CacheThreshold)
			metrics.PrefixMatchRatioFn = tr.MatchRatio
		}
		if boolVal(cfg.KV.Enabled, true) { // KV 容量感知分流（缺省开启，仅高水位触发）
			picker.AttachKV(cfg.KV.HighWatermark, cfg.KV.TopK, boolVal(cfg.KV.CapScale, false))
		}
		bal = &balancer.RoundRobin{} // key 不可提取时（如非 chat 请求）的兜底
	default:
		bal = &balancer.RoundRobin{}
	}
	return reg, bal, picker
}

// Swap 以 cfg 原子重建一整代运行态组件并热切换。
// 组件构建失败时返回错误且不动现有运行态（回滚语义）；切换成功后旧探活循环被取消，
// 持有旧组件引用的在途请求继续处理直到完成（自然排空）。
// 若探活循环此前在运行，则对新 registry 自动重启，新副本即刻纳入探测。
func (rt *Runtime) Swap(cfg *Config) error {
	applyDefaults(cfg)
	reg, bal, picker := buildComponents(cfg)

	rt.mu.Lock()
	wasProbing := rt.probeCancel != nil
	interval := rt.probeInterval
	if rt.probeCancel != nil {
		rt.probeCancel()
		rt.probeCancel = nil
	}
	rt.Cfg = cfg
	rt.Reg = reg
	rt.Bal = bal
	rt.Picker = picker
	rt.Retry = cfg.resolvedRetry()
	rt.mu.Unlock()

	if wasProbing {
		rt.StartProbes(interval)
	}
	return nil
}

// ReloadFrom 从配置文件重建运行态；文件读取/解析失败返回错误，运行态保持不变（回滚）。
func (rt *Runtime) ReloadFrom(path string) error {
	cfg, err := LoadConfig(path)
	if err != nil {
		return err
	}
	return rt.Swap(cfg)
}

// StartProbes 启动当前 registry 的周期探活（先探测一轮再进入周期循环）。
// 热更 Swap 时旧探活循环自动取消，并对新 registry 以同一周期重启。
func (rt *Runtime) StartProbes(interval time.Duration) {
	rt.mu.Lock()
	if rt.probeCancel != nil {
		rt.probeCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt.probeCancel = cancel
	rt.probeInterval = interval
	reg := rt.Reg
	rt.mu.Unlock()

	reg.ProbeAll(ctx)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				reg.ProbeAll(ctx)
			}
		}
	}()
}

// Build 按策略装配 Runtime 与 HTTP handler。
// 无论配置来自 LoadConfig 还是直接构造（集成测试），这里统一补齐默认值（幂等）。
func Build(cfg *Config) (*Runtime, http.Handler) {
	rt := &Runtime{}
	_ = rt.Swap(cfg) // Swap 无失败路径（组件构建不产生错误），默认值在此幂等补齐
	return rt, Handler(rt)
}

// Handler 装配 /metrics /statusz /readyz /admin/backends /v1/* 路由。
func Handler(rt *Runtime) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
		cfg, reg, _, picker, _ := rt.snapshot()
		w.Header().Set("Content-Type", "application/json")
		out := map[string]any{
			"strategy": cfg.Strategy,
			"backends": len(reg.All()),
			"healthy":  len(reg.Healthy()),
		}
		if picker != nil && picker.TreeActive() {
			out["prefix_tree_nodes"] = picker.Tree.Nodes()
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		_, reg, _, _, _ := rt.snapshot()
		w.Header().Set("Content-Type", "application/json")
		// 进程存活由 K8s liveness（/statusz 或进程本身）判断；/readyz 回答"是否可接流量"
		if n := len(reg.Healthy()); n == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `{"ready":false,"healthy_backends":0}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"ready":true}`)
	})
	mux.HandleFunc("/admin/backends", rt.handleAdminBackends)
	mux.HandleFunc("/v1/", rt.handleV1)
	return mux
}

// handleV1 统一请求入口：快照当前代组件后处理，热更不影响处理中的请求。
func (rt *Runtime) handleV1(w http.ResponseWriter, r *http.Request) {
	cfg, reg, bal, picker, retry := rt.snapshot()
	states := reg.Healthy()

	// 亲和路径：读 body 提取会话 key 与 model，转发时重放 body
	if picker != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body)) // 兜底：非 chat 路径仍可转发
		key, model, chain, kerr := affinity.ParseRequestChain(r, body, cfg.Affinity.Tree.ChunkChars)

		candidates, ok := filterByModel(w, states, model)
		if !ok {
			return // 已写出 502
		}

		var st *registry.State
		var ranked []*registry.State
		if kerr == nil {
			ck := compositeKey(model, key)
			if picker.TreeActive() && len(chain) > 0 {
				// 混合前缀树：sid 快路径外的请求走 树命中 → 前缀派单 → 全链派单
				st = picker.PickHybrid(model, ck, chain, candidates)
			} else {
				st = picker.Pick(ck, candidates)
			}
			if st != nil {
				// failover 次序：bounded load 主选 + 加权 rendezvous 次序的其余副本
				ranked = []*registry.State{st}
				for _, s := range picker.Rank(ck, candidates) {
					if s.Name != st.Name {
						ranked = append(ranked, s)
					}
				}
			}
		} else {
			st = bal.Pick(candidates) // key 不可提取：退化为 RR
			ranked = singleOrEmpty(st)
		}
		if st == nil {
			http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
			return
		}
		proxy.ServeWithFailover(w, r, ranked, body, model, retry)
		return
	}

	// RR / least_queue 路径：同样按 model 筛池（重试仅限亲和路径，RR 次序无"次优"语义）
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	r.Body = io.NopCloser(bytes.NewReader(body))
	_, model, _ := affinity.ParseRequest(r, body)
	candidates, ok := filterByModel(w, states, model)
	if !ok {
		return
	}
	st := bal.Pick(candidates)
	if st == nil {
		http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
		return
	}
	proxy.Serve(w, r, st)
}

func singleOrEmpty(st *registry.State) []*registry.State {
	if st == nil {
		return nil
	}
	return []*registry.State{st}
}

// handleAdminBackends 管理 API：运行中增删副本并原子热更。
// POST {"op":"add"|"remove","backend":{"name":...,"base_url":...,"model":...,"weight":...}}
// 鉴权：配置 admin_token 时要求 Authorization: Bearer <token>（或 X-Admin-Token）；
// 未配置令牌时仅允许 localhost 调用。
func (rt *Runtime) handleAdminBackends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !rt.authAdmin(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}
	var req struct {
		Op      string           `json:"op"`
		Backend registry.Backend `json:"backend"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	rt.mu.RLock()
	cur := rt.Cfg
	rt.mu.RUnlock()
	next := *cur // 浅拷贝值字段；backends 重建
	next.Backends = append([]registry.Backend(nil), cur.Backends...)

	switch req.Op {
	case "add":
		b := req.Backend
		if strings.TrimSpace(b.Name) == "" || strings.TrimSpace(b.BaseURL) == "" {
			http.Error(w, `{"error":"backend.name and backend.base_url are required"}`, http.StatusBadRequest)
			return
		}
		for _, e := range next.Backends {
			if e.Name == b.Name {
				http.Error(w, fmt.Sprintf(`{"error":"backend %q already exists"}`, b.Name), http.StatusConflict)
				return
			}
		}
		next.Backends = append(next.Backends, b)
	case "remove":
		kept := next.Backends[:0]
		found := false
		for _, e := range cur.Backends {
			if e.Name == req.Backend.Name {
				found = true
				continue
			}
			kept = append(kept, e)
		}
		if !found {
			http.Error(w, fmt.Sprintf(`{"error":"backend %q not found"}`, req.Backend.Name), http.StatusNotFound)
			return
		}
		next.Backends = kept
		if len(next.Backends) == 0 {
			http.Error(w, `{"error":"cannot remove the last backend"}`, http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, `{"error":"op must be add or remove"}`, http.StatusBadRequest)
		return
	}

	if err := rt.Swap(&next); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"swap failed: %v"}`, err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"ok":true,"backends":%d}`, len(next.Backends))
}

// authAdmin 管理接口鉴权（见 handleAdminBackends 注释）。
func (rt *Runtime) authAdmin(r *http.Request) bool {
	rt.mu.RLock()
	tok := rt.Cfg.AdminToken
	rt.mu.RUnlock()
	if tok == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		return host == "127.0.0.1" || host == "::1"
	}
	const prefix = "Bearer "
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, prefix) &&
		subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(tok)) == 1 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Token")), []byte(tok)) == 1
}

// filterByModel 按请求 model 筛选健康副本；model 为空放行全池。
// 无副本服务该模型时写出 502 + 计数器并返回 ok=false。
func filterByModel(w http.ResponseWriter, states []*registry.State, model string) ([]*registry.State, bool) {
	if model == "" {
		return states, true
	}
	out := make([]*registry.State, 0, len(states))
	for _, s := range states {
		if s.Serves(model) {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		metrics.ModelNoMatch.WithLabelValues(model).Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = fmt.Fprintf(w, `{"error":{"message":"no healthy backend serving model %q","type":"no_backend_for_model"}}`, model)
		return nil, false
	}
	return out, true
}

// compositeKey 会话 key 加上模型维度：同一会话切换模型视为不同亲和目标，
// 避免切换模型的请求把会话钉在与该模型无关的副本上。
func compositeKey(model, key string) string {
	if model == "" {
		return key
	}
	return "m:" + model + "|" + key
}
