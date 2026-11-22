// infer-router 入口：加载配置 → 健康探测 → 策略路由 → 透传。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"infer-router/internal/affinity"
	"infer-router/internal/balancer"
	"infer-router/internal/metrics"
	"infer-router/internal/proxy"
	"infer-router/internal/registry"
)

type breakerCfg struct {
	FailThreshold int     `yaml:"fail_threshold"`
	CooldownSec   float64 `yaml:"cooldown_s"`
}

type affinityCfg struct {
	Alpha        float64 `yaml:"alpha"`         // bounded load 系数，cap = ceil(alpha × 平均在途)
	StickyWindow int     `yaml:"sticky_window"` // 粘性统计窗口容量
}

type config struct {
	Addr              string             `yaml:"addr"`
	Strategy          string             `yaml:"strategy"`
	HealthIntervalSec int                `yaml:"health_interval_s"`
	Breaker           breakerCfg         `yaml:"breaker"`
	Affinity          affinityCfg        `yaml:"affinity"`
	Backends          []registry.Backend `yaml:"backends"`
}

func main() {
	cfgPath := flag.String("config", "config/router.yaml", "配置文件路径")
	flag.Parse()

	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	var cfg config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.HealthIntervalSec <= 0 {
		cfg.HealthIntervalSec = 5
	}
	if cfg.Breaker.FailThreshold <= 0 {
		cfg.Breaker.FailThreshold = 3
	}
	if cfg.Breaker.CooldownSec <= 0 {
		cfg.Breaker.CooldownSec = 15
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := registry.New(
		cfg.Backends,
		cfg.Breaker.FailThreshold,
		time.Duration(cfg.Breaker.CooldownSec*float64(time.Second)),
	)
	reg.ProbeAll(ctx)
	reg.Start(ctx, time.Duration(cfg.HealthIntervalSec)*time.Second)

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

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"strategy": cfg.Strategy,
			"backends": len(reg.All()),
			"healthy":  len(reg.Healthy()),
		})
	})
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		states := reg.Healthy()

		// 亲和路径：读 body 提取会话 key，转发时重放 body
		if picker != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body)) // 兜底：非 chat 路径仍可转发
			key, kerr := affinity.SessionKey(r, body)
			var st *registry.State
			if kerr == nil {
				st = picker.Pick(key, states)
			} else {
				st = bal.Pick(states) // key 不可提取：退化为 RR
			}
			if st == nil {
				http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
				return
			}
			proxy.ServeWithBody(w, r, st, body)
			return
		}

		st := bal.Pick(states)
		if st == nil {
			http.Error(w, `{"error":{"message":"no healthy backend"}}`, http.StatusBadGateway)
			return
		}
		proxy.Serve(w, r, st)
	})

	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("infer-router listening on %s (strategy=%s backends=%d)", cfg.Addr, cfg.Strategy, len(cfg.Backends))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
