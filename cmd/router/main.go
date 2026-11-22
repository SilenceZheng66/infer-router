// infer-router 入口：加载配置 → 健康探测 → 策略路由 → 透传。
// 装配逻辑在 internal/app（cmd 与集成测试共用同一代码路径）。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"infer-router/internal/app"
)

func main() {
	cfgPath := flag.String("config", "config/router.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := app.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, handler := app.Build(cfg)
	rt.Reg.ProbeAll(ctx)
	rt.Reg.Start(ctx, time.Duration(cfg.HealthIntervalSec)*time.Second)

	srv := &http.Server{Addr: cfg.Addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
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
