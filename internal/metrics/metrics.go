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

func init() {
	prometheus.MustRegister(ReqTotal, InFlight)
}
