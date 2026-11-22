package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

	"infer-router/internal/metrics"
	"infer-router/internal/registry"
)

// RetryConfig 请求级 failover 参数（已解析默认值后的形态）。
// 语义：仅当上游返回 5xx / 连接错误、且响应尚未向客户端写出任何字节时，
// 按候选次序换下一个副本重试；SSE 一旦开始流出即不再重试。
type RetryConfig struct {
	Enabled             bool
	Max                 int // 额外重试次数
	RetryOn5xx          bool
	RetryOnConnectError bool
}

// recorder 捕获上游响应状态码用于指标。
type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Serve 将请求透传到指定副本（支持 SSE：FlushInterval=-1 逐块 flush）。
func Serve(w http.ResponseWriter, req *http.Request, state *registry.State) {
	ServeWithFailover(w, req, []*registry.State{state}, nil, "", RetryConfig{Enabled: false})
}

// ServeWithBody 在亲和路由下使用：请求体已被上游读取（用于提取 session key），
// 用重放的 body 构造转发请求。body 为 nil 时退化为原始 req。
func ServeWithBody(w http.ResponseWriter, req *http.Request, state *registry.State, body []byte) {
	ServeWithFailover(w, req, []*registry.State{state}, body, "", RetryConfig{Enabled: false})
}

// ServeWithFailover 带请求级 failover 的透传。
// ranked 为按优先级排序的候选副本（ranked[0] 为主选，其余为重试次序）；model 用于 404 mismatch 归因（可为空）。
// body 为请求体缓冲（重试需要重放）；nil 时仅首选拿原始流、不做重试。
func ServeWithFailover(w http.ResponseWriter, req *http.Request, ranked []*registry.State, body []byte, model string, retry RetryConfig) {
	if len(ranked) == 0 {
		http.Error(w, "no healthy backend", http.StatusBadGateway)
		return
	}
	primary := ranked[0]

	target, err := url.Parse(primary.BaseURL)
	if err != nil {
		http.Error(w, "bad backend url", http.StatusBadGateway)
		return
	}

	maxAttempts := 1
	if retry.Enabled && retry.Max >= 0 && len(ranked) > 1 {
		maxAttempts = retry.Max + 1
		if maxAttempts > len(ranked) {
			maxAttempts = len(ranked)
		}
	}

	rec := &recorder{ResponseWriter: w, status: http.StatusOK}
	rtt := &retryRoundTripper{
		base:   http.DefaultTransport,
		ranked: ranked,
		body:   body,
		retry:  retry,
		max:    maxAttempts,
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: target.Scheme, Host: target.Host})
			pr.Out.Host = target.Host
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, perr error) {
			// 传输层失败 / 流式复制中断：记入实际服务副本的熔断失败，并向客户端回错
			if rtt.served != nil {
				rtt.served.RecordFailure()
			}
			rec.status = http.StatusBadGateway
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
		Transport: rtt,
	}
	primary.Inc()
	defer primary.Dec()

	proxy.ServeHTTP(rec, req)
	classify(rtt.served, rec.status, model)
}

// classify 按最终响应状态码归档指标与模型归因（state = 实际产出最终响应的副本）。
// 熔断失败计数只发生在 retryRoundTripper / ErrorHandler——此处不重复计数：
// <400 清零失败计数；4xx 不改变熔断状态（404+model 记 mismatch）；5xx 仅记指标。
func classify(state *registry.State, status int, model string) {
	if state == nil {
		return
	}
	switch {
	case status < http.StatusBadRequest:
		state.RecordSuccess()
		metrics.ReqTotal.WithLabelValues(state.Name, http.StatusText(status)).Inc()
	case status < http.StatusInternalServerError:
		if status == http.StatusNotFound && model != "" {
			metrics.ModelMismatch.WithLabelValues(state.Name, model).Inc()
		}
		metrics.ReqTotal.WithLabelValues(state.Name, http.StatusText(status)).Inc()
	default:
		metrics.ReqTotal.WithLabelValues(state.Name, http.StatusText(status)).Inc()
	}
}

// retryRoundTripper 在传输层实现请求级 failover：
// 5xx / 连接错误时按 ranked 次序换下一个副本重发——此刻响应尚未写给客户端，重试安全。
// 成功响应（<500）一旦返回给 ReverseProxy，body 复制开始后的一切失败都不再重试（SSE 语义）。
type retryRoundTripper struct {
	base   http.RoundTripper
	ranked []*registry.State
	body   []byte
	retry  RetryConfig
	max    int

	served *registry.State // 实际产出最终响应的副本（成功响应或最后一次失败）
}

func (t *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for idx := 0; idx < t.max; idx++ {
		st := t.ranked[idx]

		req2 := req.Clone(req.Context())
		target, perr := url.Parse(st.BaseURL)
		if perr != nil {
			continue
		}
		u := *req.URL
		u.Scheme = target.Scheme
		u.Host = target.Host
		req2.URL = &u
		req2.Host = u.Host
		// 重放 body：有缓冲时每次尝试都用新 reader；无缓冲且非首次尝试则不可重试
		if len(t.body) > 0 {
			req2.Body = io.NopCloser(bytes.NewReader(t.body))
			req2.ContentLength = int64(len(t.body))
		} else if idx > 0 && req.Body != nil {
			break
		}

		resp, err := t.base.RoundTrip(req2)
		if err == nil && resp.StatusCode < http.StatusInternalServerError {
			t.served = st
			return resp, nil
		}

		// 失败归档：连接错误 / 5xx 都计入该副本的熔断失败
		st.RecordFailure()

		if !t.retryable(err) || idx+1 >= t.max {
			t.served = st
			return resp, err
		}
		if resp != nil { // 丢弃失败响应体，释放连接
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
		}
		metrics.Retries.WithLabelValues(st.Name, t.ranked[idx+1].Name).Inc()
	}
	return nil, errors.New("retry loop exhausted")
}

func (t *retryRoundTripper) retryable(err error) bool {
	if err != nil {
		return t.retry.RetryOnConnectError
	}
	return t.retry.RetryOn5xx
}
