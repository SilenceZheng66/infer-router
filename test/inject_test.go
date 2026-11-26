package test

// M7.5 场景：全局 inject_headers——服务端静态头注入所有经路由的上游请求，
// 服务端配置覆盖客户端同名头（公网网关场景：服务端持有对上游的鉴权 token）。

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"infer-router/internal/app"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// buildInjectRouter 装配单副本 affinity 路由，配置全局注入头。
func buildInjectRouter(t *testing.T, inject map[string]string) (string, *mockengine.Engine) {
	t.Helper()
	eng := mockengine.New(mockengine.Options{Name: "inj-a", Models: []string{"m-1"}})
	t.Cleanup(eng.Close)

	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		InjectHeaders:     inject,
		Backends: []registry.Backend{
			{Name: "inj-a", BaseURL: eng.URL(), Model: "m-1", Weight: 1},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, eng
}

// TestInjectHeadersAffinityPath 亲和路径注入：服务端头到达上游。
func TestInjectHeadersAffinityPath(t *testing.T) {
	url, eng := buildInjectRouter(t, map[string]string{
		"Authorization":  "Bearer gw-token-1",
		"X-Router-Plane": "gateway",
	})
	body := `{"model":"m-1","messages":[{"role":"user","content":"hi"}]}`
	code, _ := pdPostChat(t, url, body)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := eng.LastHeader("Authorization"); got != "Bearer gw-token-1" {
		t.Fatalf("Authorization = %q, want server token", got)
	}
	if got := eng.LastHeader("X-Router-Plane"); got != "gateway" {
		t.Fatalf("X-Router-Plane = %q, want gateway", got)
	}
}

// TestInjectHeadersOverridesClient 服务端配置覆盖客户端同名头。
func TestInjectHeadersOverridesClient(t *testing.T) {
	url, eng := buildInjectRouter(t, map[string]string{"Authorization": "Bearer gw-token-1"})
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions",
		strings.NewReader(fmt.Sprintf(`{"model":"m-1","messages":[{"role":"user","content":"hi"}]}`)))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer client-token") // 客户端自带同名头
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := eng.LastHeader("Authorization"); got != "Bearer gw-token-1" {
		t.Fatalf("Authorization = %q, want server token overriding client header", got)
	}
}

// TestInjectHeadersUnset 无注入配置时不添加任何多余头（客户端头原样透传）。
func TestInjectHeadersUnset(t *testing.T) {
	url, eng := buildInjectRouter(t, nil)
	body := `{"model":"m-1","messages":[{"role":"user","content":"hi"}]}`
	code, _ := pdPostChat(t, url, body)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := eng.LastHeader("Authorization"); got != "" {
		t.Fatalf("Authorization = %q, want empty (no injection configured)", got)
	}
}
