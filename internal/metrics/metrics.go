package metrics

import "github.com/prometheus/client_golang/prometheus"

var ReqTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "router_requests_total", Help: "Proxied requests by backend and status"},
	[]string{"backend", "status"},
)

var InFlight = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "router_in_flight_requests", Help: "In-flight requests per backend"},
	[]string{"backend"},
)

// 亲和路由观测：粘性率自证 + 路由分布 + bounded-load 降级计数。
var AffinitySticky = prometheus.NewGaugeFunc(
	prometheus.GaugeOpts{Name: "router_affinity_sticky_ratio",
		Help: "Ratio of requests routed to the same backend as the previous request with the same session key"},
	func() float64 { return StickyRatioFn() },
)

var AffinityAssign = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "router_affinity_assign_total",
		Help: "Session-key assignments per backend (distribution evenness)"},
	[]string{"backend"},
)

var AffinityOverloadFallback = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "router_affinity_overload_fallback_total",
		Help: "Requests demoted from the affinity-first backend due to bounded load"},
)

// 模型路由观测：无健康副本服务该模型的请求计数（配置错误或全部下线的信号）。
var ModelNoMatch = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "router_model_nomatch_total",
		Help: "Requests rejected because no healthy backend serves the requested model"},
	[]string{"model"},
)

// 请求级 failover 观测：按 (原副本, 重试副本) 维度的重试计数。
var Retries = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "router_retries_total",
		Help: "Request-level failover attempts from one backend to another"},
	[]string{"from", "to"},
)

// 模型声明不符观测：副本声明服务该模型但上游返回 404（声明与实际 serving 不符）。
var ModelMismatch = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "router_model_mismatch_total",
		Help: "Upstream 404s on requests whose model the backend claims to serve"},
	[]string{"backend", "model"},
)

// StickyRatioFn 由 affinity 包注入（避免 metrics -> affinity 反向依赖）。
var StickyRatioFn = func() float64 { return 1.0 }

func init() {
	prometheus.MustRegister(ReqTotal, InFlight, AffinitySticky, AffinityAssign, AffinityOverloadFallback, ModelNoMatch, Retries, ModelMismatch)
}
