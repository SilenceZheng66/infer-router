package registry

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Backend 为配置中的副本描述。
type Backend struct {
	Name    string `yaml:"name"`
	BaseURL string `yaml:"base_url"` // 不含 /v1
	Model   string `yaml:"model"`    // 该副本服务的模型；支持逗号分隔多模型与 "*" 通配；留空 = 不过滤
	Weight  int    `yaml:"weight"`   // 亲和排序与负载上限分摊权重（≥1）
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

func (s *State) RecordFailure() {
	s.mu.Lock()
	s.failures++
	s.openedAt = time.Now()
	s.mu.Unlock()
}

// Registry 维护副本集合并周期探测健康。
type Registry struct {
	mu     sync.RWMutex
	states []*State
	client *http.Client
}

func New(backends []Backend, threshold int, cooldown time.Duration) *Registry {
	r := &Registry{client: &http.Client{Timeout: 3 * time.Second}}
	for _, b := range backends {
		r.states = append(r.states, NewState(b, threshold, cooldown))
	}
	return r
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

// ProbeAll 逐个探测 GET {base}/models。
func (r *Registry) ProbeAll(ctx context.Context) {
	r.mu.RLock()
	states := append([]*State(nil), r.states...)
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
	}
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
