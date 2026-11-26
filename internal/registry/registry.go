package registry

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"infer-router/internal/metrics"
)

// Backend 为配置中的副本描述。
type Backend struct {
	Name    string `yaml:"name" json:"name"`
	BaseURL string `yaml:"base_url" json:"base_url"` // 不含 /v1
	Model   string `yaml:"model" json:"model"`       // 该副本服务的模型；支持逗号分隔多模型与 "*" 通配；留空 = 不过滤
	Weight  int    `yaml:"weight" json:"weight"`     // 亲和排序与负载上限分摊权重（≥1）
	Role    string `yaml:"role" json:"role"`         // PD 角色（M7）："" | prefill | decode；仅 pd.mode 启用时生效
	// BootstrapPort 副本角色端口的显式配置：sglang=--disaggregation-bootstrap-port，
	// vllm=KV connector 握手端口（remote_port）；0 = 按 pd.default_bootstrap_port / base_url 兜底。
	BootstrapPort int `yaml:"bootstrap_port" json:"bootstrap_port"`
	// BootstrapHost bootstrap 主机显式覆盖（跨机部署时 prefill 需直达 decode 的可达地址）；
	// 空 = 从 base_url host 推导。
	BootstrapHost string `yaml:"bootstrap_host" json:"bootstrap_host"`
}

// Serves 报告该副本是否服务指定模型。
// 请求 model 为空（非 chat 请求）或副本未声明 model 时放行（不过滤）。
func (b *Backend) Serves(model string) bool {
	if model == "" || b.Model == "" || b.Model == "*" {
		return true
	}
	for _, m := range strings.Split(b.Model, ",") {
		if strings.TrimSpace(m) == model {
			return true
		}
	}
	return false
}

// State 为副本运行态：健康、在途计数与简易熔断。
type State struct {
	Backend

	mu        sync.Mutex
	healthy   bool
	inFlight  int64
	failures  int
	openedAt  time.Time
	threshold int
	cooldown  time.Duration
	kvUtil    float64 // 最近一次探测抓到的 KV cache 使用率（0 = 未知）
}

func NewState(b Backend, threshold int, cooldown time.Duration) *State {
	return &State{Backend: b, healthy: true, threshold: threshold, cooldown: cooldown}
}

// Healthy 返回是否可接收流量：健康、未触发熔断，或熔断冷却期满（half-open 探测）。
func (s *State) Healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.healthy {
		return false
	}
	if s.failures < s.threshold {
		return true
	}
	return time.Since(s.openedAt) >= s.cooldown
}

func (s *State) SetHealthy(h bool) { s.mu.Lock(); s.healthy = h; s.mu.Unlock() }

func (s *State) InFlight() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight
}
func (s *State) Inc() { s.mu.Lock(); s.inFlight++; s.mu.Unlock() }
func (s *State) Dec() { s.mu.Lock(); s.inFlight--; s.mu.Unlock() }

func (s *State) RecordSuccess() { s.mu.Lock(); s.failures = 0; s.mu.Unlock() }

// KVUtil 返回最近一次探测抓到的 KV cache 使用率（0 = 尚未抓到/引擎未暴露）。
func (s *State) KVUtil() float64 { s.mu.Lock(); defer s.mu.Unlock(); return s.kvUtil }

// SetKVUtil 更新 KV 使用率（探活循环调用）。
func (s *State) SetKVUtil(v float64) { s.mu.Lock(); s.kvUtil = v; s.mu.Unlock() }

func (s *State) RecordFailure() {
	s.mu.Lock()
	s.failures++
	s.openedAt = time.Now()
	s.mu.Unlock()
}

// Registry 维护副本集合并周期探测健康，顺带抓取副本 KV 使用率（KV 容量感知路由，M5）。
type Registry struct {
	mu       sync.RWMutex
	states   []*State
	client   *http.Client
	kvClient *http.Client // KV 抓取独立长超时：vLLM 0.9 池满时 /metrics 响应可 >3s，共用短超时会冻结旧水位
	kvMetric string       // 副本 /metrics 中的 KV 使用率指标名（vLLM: gpu_cache_usage_perc）
}

func New(backends []Backend, threshold int, cooldown time.Duration) *Registry {
	r := &Registry{
		client:   &http.Client{Timeout: 3 * time.Second},
		kvClient: &http.Client{Timeout: 10 * time.Second},
		kvMetric: "gpu_cache_usage_perc",
	}
	for _, b := range backends {
		r.states = append(r.states, NewState(b, threshold, cooldown))
	}
	return r
}

// SetKVMetric 配置 KV 使用率指标名（SGLang 等非 vLLM 引擎的指标名不同）。
func (r *Registry) SetKVMetric(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name != "" {
		r.kvMetric = name
	}
}

// Healthy 返回健康且熔断放行的副本。
func (r *Registry) Healthy() []*State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*State
	for _, s := range r.states {
		if s.Healthy() {
			out = append(out, s)
		}
	}
	return out
}

// All 返回全部副本（/statusz 用）。
func (r *Registry) All() []*State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*State(nil), r.states...)
}

// ProbeAll 逐个探测 GET {base}/v1/models，并顺带抓取 /metrics 的 KV 使用率。
// KV 抓取失败保留上一次取值（0 = 从未抓到），不影响健康判定。
func (r *Registry) ProbeAll(ctx context.Context) {
	r.mu.RLock()
	states := append([]*State(nil), r.states...)
	kvMetric := r.kvMetric
	r.mu.RUnlock()
	for _, s := range states {
		url := s.BaseURL + "/v1/models"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		ok := false
		if err == nil {
			resp, err := r.client.Do(req)
			if err == nil {
				ok = resp.StatusCode == http.StatusOK
				resp.Body.Close()
			}
		}
		s.SetHealthy(ok)

		fetchKV(ctx, r.kvClient, s, kvMetric)
	}
}

// fetchKV 抓取 {base}/metrics 并解析 KV 使用率指标（Prometheus 文本格式的最小解析）。
func fetchKV(ctx context.Context, client *http.Client, s *State, metric string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.BaseURL+"/metrics", nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return
	}
	if v, ok := parseMetricValue(string(body), metric); ok {
		s.SetKVUtil(v)
		metrics.BackendKVUtil.WithLabelValues(s.Name).Set(v)
		metrics.PoolKVUtil.WithLabelValues(s.Name, s.Role).Set(v) // PD 池维度观测（M7）
	}
}

// parseMetricValue 从 Prometheus 文本格式中解析无标签或带标签指标的值。
// 指标名兼容引擎命名空间前缀：真实 vLLM 暴露 vllm:gpu_cache_usage_perc，
// 配置使用短名 gpu_cache_usage_perc 时按 ":name" 后缀匹配（先剥离 {label} 部分）。
func parseMetricValue(text, name string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		ident := fields[0]
		if i := strings.Index(ident, "{"); i >= 0 {
			ident = ident[:i]
		}
		if ident == name || strings.HasSuffix(ident, ":"+name) {
			if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
				return v, true
			}
		}
	}
	return 0, false
}

// Start 启动后台健康探测循环，ctx 取消即退出。
func (r *Registry) Start(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.ProbeAll(ctx)
			}
		}
	}()
}
