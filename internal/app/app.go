// Package app 组装 infer-router 的运行态与 HTTP 处理链：
// 配置加载与默认值、registry/picker/balancer 构建与 mux 装配。
// cmd/router 与集成测试共用同一装配路径，保证被测对象与生产行为一致。
package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
}

type Config struct {
	Addr              string             `yaml:"addr"`
	Strategy          string             `yaml:"strategy"`
	HealthIntervalSec int                `yaml:"health_interval_s"`
	Breaker           BreakerCfg         `yaml:"breaker"`
	Affinity          AffinityCfg        `yaml:"affinity"`
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
}

// Runtime 持有路由运行态依赖（registry、策略选择器）。
type Runtime struct {
	Cfg    *Config
	Reg    *registry.Registry
	Bal    balancer.Balancer
	Picker *affinity.Picker // 仅 prefix_affinity 策略非 nil
}

// Build 按策略装配 Runtime 与 HTTP handler。
func Build(cfg *Config) (*Runtime, http.Handler) {
	reg := registry.New(
		cfg.Backends,
		cfg.Breaker.FailThreshold,
		time.Duration(cfg.Breaker.CooldownSec*float64(time.Second)),
	)

	var bal balancer.Balancer
	var picker *affinity.Picker
	switch cfg.Strategy {
	case "least_queue":
		bal = &balancer.LeastQueue{}
	case "prefix_affinity":
		picker = affinity.NewPicker(cfg.Affinity.Alpha, cfg.Affinity.StickyWindow)
		metrics.StickyRatioFn = picker.StickyRatio
		bal = &balancer.RoundRobin{} // key 不可提取时（如非 chat 请求）的兜底
	default:
		bal = &balancer.RoundRobin{}
	}

	rt := &Runtime{Cfg: cfg, Reg: reg, Bal: bal, Picker: picker}
	return rt, Handler(rt)
}

// Handler 装配 /metrics /statusz /v1/* 路由。
func Handler(rt *Runtime) http.Handler {
	cfg := rt.Cfg
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"strategy": cfg.Strategy,
			"backends": len(rt.Reg.All()),
			"healthy":  len(rt.Reg.Healthy()),
		})
	})
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		states := rt.Reg.Healthy()

		// 亲和路径：读 body 提取会话 key 与 model，转发时重放 body
		if rt.Picker != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body)) // 兜底：非 chat 路径仍可转发
			key, model, kerr := affinity.ParseRequest(r, body)

			candidates, ok := filterByModel(w, states, model)
			if !ok {
				return // 已写出 502
			}

			var st *registry.State
			if kerr == nil {
				st = rt.Picker.Pick(compositeKey(model, key), candidates)
			} else {
				st = rt.Bal.Pick(candidates) // key 不可提取：退化为 RR
			}
			if st == nil {
				http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
				return
			}
			proxy.ServeWithBody(w, r, st, body)
			return
		}

		// RR / least_queue 路径：同样按 model 筛池
		body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		r.Body = io.NopCloser(bytes.NewReader(body))
		_, model, _ := affinity.ParseRequest(r, body)
		candidates, ok := filterByModel(w, states, model)
		if !ok {
			return
		}
		st := rt.Bal.Pick(candidates)
		if st == nil {
			http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
			return
		}
		proxy.Serve(w, r, st)
	})
	return mux
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
