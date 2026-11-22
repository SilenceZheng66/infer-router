// Package mockengine 提供可编程的 OpenAI 兼容 mock 推理引擎，用于 infer-router 集成测试。
// 能力：模型清单（/v1/models 探活）、状态码序列（模拟失败/恢复）、响应延迟、
// KV 使用率指标（/metrics 暴露 gpu_cache_usage_perc）、请求计数。
package mockengine

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options mock 引擎的可编程参数。
type Options struct {
	Name      string   // 副本名，注入响应正文用于断言"请求落在哪个副本"
	Models    []string // 该副本服务的模型清单（/v1/models 返回）
	StatusSeq []int    // 对 chat/completions 依次返回的状态码；耗尽后恒 200（如 [500,500,200]）
	LatencyMs int      // 每次响应固定延迟
}

// Engine 一个 mock 引擎实例（对应 httptest.Server）。
type Engine struct {
	Server *httptest.Server
	Name   string

	mu        sync.Mutex
	statusSeq []int
	seqIdx    int

	models  []string
	latency time.Duration
	kvUsage atomic.Value // float64

	abortAfter atomic.Int64 // >0 时 chat 响应写出后强制断开连接（模拟流式中途失败）

	requests atomic.Int64
	lastBody atomic.Value // string，最后一次 chat 请求体（调试/断言用）
}

// New 启动一个 mock 引擎。
func New(opts Options) *Engine {
	e := &Engine{
		Name:      opts.Name,
		statusSeq: append([]int(nil), opts.StatusSeq...),
		models:    append([]string(nil), opts.Models...),
		latency:   time.Duration(opts.LatencyMs) * time.Millisecond,
	}
	if len(e.models) == 0 {
		e.models = []string{"mock-model"}
	}
	e.kvUsage.Store(0.5)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", e.handleModels)
	mux.HandleFunc("/v1/chat/completions", e.handleChat)
	mux.HandleFunc("/v1/", e.handlePassthrough) // 其余 OpenAI 兼容路径（embeddings 等）原样 200
	mux.HandleFunc("/metrics", e.handleMetrics)
	e.Server = httptest.NewServer(mux)
	return e
}

// Close 停掉 mock 引擎（模拟副本下线）。
func (e *Engine) Close() { e.Server.Close() }

// URL 副本地址（base_url）。
func (e *Engine) URL() string { return e.Server.URL }

// Requests 累计收到的 chat 请求数。
func (e *Engine) Requests() int64 { return e.requests.Load() }

// LastBody 最近一次 chat 请求体。
func (e *Engine) LastBody() string {
	s, _ := e.lastBody.Load().(string)
	return s
}

// SetStatusSeq 运行中重置状态码序列（重试/熔断场景用）。
func (e *Engine) SetStatusSeq(seq []int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.statusSeq = append([]int(nil), seq...)
	e.seqIdx = 0
}

// SetKVUsage 设置 /metrics 暴露的 KV 使用率（KV 感知路由场景用）。
func (e *Engine) SetKVUsage(v float64) { e.kvUsage.Store(v) }

// SetAbortAfter 设置 chat 响应写出后强制断开（模拟 SSE 流式中途失败）；0 关闭。
func (e *Engine) SetAbortAfter(n int64) { e.abortAfter.Store(n) }

func (e *Engine) handleModels(w http.ResponseWriter, _ *http.Request) {
	var sb strings.Builder
	for _, m := range e.models {
		sb.WriteString(fmt.Sprintf(`{"id":%q,"object":"model"},`, m))
	}
	data := strings.TrimSuffix(sb.String(), ",")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"object":"list","data":[%s]}`, data)
}

func (e *Engine) nextStatus() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.seqIdx >= len(e.statusSeq) {
		return http.StatusOK
	}
	code := e.statusSeq[e.seqIdx]
	e.seqIdx++
	return code
}

func (e *Engine) handleChat(w http.ResponseWriter, r *http.Request) {
	e.requests.Add(1)
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	e.lastBody.Store(string(body))

	if e.latency > 0 {
		time.Sleep(e.latency)
	}
	code := e.nextStatus()
	if code != http.StatusOK {
		http.Error(w, fmt.Sprintf(`{"error":{"message":"mock upstream failure at %s"}}`, e.Name), code)
		return
	}
	if n := e.abortAfter.Load(); n > 0 {
		// 先写出一部分响应并 flush（响应头已发出），随后强制断开——
		// 用于验证"流式开始后不再 failover"
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"partial\":true}\n\n")
		flusher.Flush()
		time.Sleep(10 * time.Millisecond)
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"id":"mock-1","object":"chat.completion","model":"%s","choices":[{"index":0,"message":{"role":"assistant","content":"%s-ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}`, e.models[0], e.Name)
}

func (e *Engine) handlePassthrough(w http.ResponseWriter, _ *http.Request) {
	e.requests.Add(1)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"object":"%s-passthrough"}`, e.Name)
}

func (e *Engine) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP gpu_cache_usage_perc KV cache usage fraction\n# TYPE gpu_cache_usage_perc gauge\ngpu_cache_usage_perc %v\n", e.kvUsage.Load())
}
