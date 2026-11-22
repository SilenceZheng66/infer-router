package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

	"infer-router/internal/metrics"
	"infer-router/internal/registry"
)

// recorder 捕获上游响应状态码用于指标与熔断判定。
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
	serve(w, req, state, nil)
}

// ServeWithBody 在亲和路由下使用：请求体已被上游读取（用于提取 session key），
// 用重放的 body 构造转发请求。body 为 nil 时退化为原始 req。
func ServeWithBody(w http.ResponseWriter, req *http.Request, state *registry.State, body []byte) {
	serve(w, req, state, body)
}

func serve(w http.ResponseWriter, req *http.Request, state *registry.State, body []byte) {
	target, err := url.Parse(state.BaseURL)
	if err != nil {
		http.Error(w, "bad backend url", http.StatusBadGateway)
		return
	}
	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: target.Scheme, Host: target.Host})
			pr.Out.Host = target.Host
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, perr error) {
			state.RecordFailure()
			metrics.ReqTotal.WithLabelValues(state.Name, "error").Inc()
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
	state.Inc()
	defer state.Dec()

	rec := &recorder{ResponseWriter: w, status: http.StatusOK}
	proxy.ServeHTTP(rec, req)

	if rec.status < http.StatusInternalServerError {
		state.RecordSuccess()
		metrics.ReqTotal.WithLabelValues(state.Name, http.StatusText(rec.status)).Inc()
	} else {
		state.RecordFailure()
		metrics.ReqTotal.WithLabelValues(state.Name, http.StatusText(rec.status)).Inc()
	}
}
