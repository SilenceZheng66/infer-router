package test

// M4 场景：热加载（Swap 原子换装）+ 管理 API + /readyz 探针 + 失败回滚。

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"infer-router/internal/app"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// buildAdminRouter 装配带探活循环的路由（副本集与策略可指定）。
func buildAdminRouter(t *testing.T, adminToken string, engines []*mockengine.Engine, strategy string) (string, *app.Runtime, []string) {
	t.Helper()
	backs := make([]registry.Backend, 0, len(engines))
	names := make([]string, 0, len(engines))
	for _, e := range engines {
		backs = append(backs, registry.Backend{Name: e.Name, BaseURL: e.URL(), Model: "m-1", Weight: 1})
		names = append(names, e.Name)
	}
	t.Cleanup(func() {
		for _, e := range engines {
			e.Close()
		}
	})
	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          strategy,
		HealthIntervalSec: 1,
		AdminToken:        adminToken,
		Breaker:           app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		Backends:          backs,
	}
	rt, handler := app.Build(cfg)
	rt.StartProbes(150 * time.Millisecond)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, rt, names
}

func adminPost(t *testing.T, url, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/admin/backends", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new admin req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("admin post: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// waitStatusz 轮询 /statusz 直到条件满足或超时（热更与探活均为异步生效）。
func waitStatusz(t *testing.T, url string, cond func(map[string]any) bool, what string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		z := statusz(t, url)
		if cond(z) {
			return z
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting %s: %v", what, z)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func addBody(name, baseURL string) string {
	return fmt.Sprintf(`{"op":"add","backend":{"name":%q,"base_url":%q,"model":"m-1","weight":1}}`, name, baseURL)
}

// TestAdminAddRemoveBackend：运行中加/摘副本，/statusz 即时体现，全程零重启（同一 server URL）。
func TestAdminAddRemoveBackend(t *testing.T) {
	url, _, _ := buildAdminRouter(t, "", []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
	}, "prefix_affinity")

	waitStatusz(t, url, func(z map[string]any) bool { return z["healthy"] == float64(1) }, "initial healthy=1")

	// 运行中加副本：statusz 即时 backends=2，且新副本可承接流量
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}})
	t.Cleanup(engB.Close)
	if code := adminPost(t, url, "", addBody("eng-b", engB.URL())); code != 200 {
		t.Fatalf("add eng-b: code=%d", code)
	}
	waitStatusz(t, url, func(z map[string]any) bool { return z["backends"] == float64(2) && z["healthy"] == float64(2) }, "after add")

	sid := sessionKeyPrefers("m-1", "eng-b", []string{"eng-a", "eng-b"})
	if code, body := chatReq(t, url, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-b-ok") {
		t.Fatalf("request should reach newly added replica: code=%d body=%s", code, body)
	}

	// 摘副本：statusz 即时 backends=1，流量全部落回 eng-a
	if code := adminPost(t, url, "", `{"op":"remove","backend":{"name":"eng-b"}}`); code != 200 {
		t.Fatalf("remove eng-b: code=%d", code)
	}
	waitStatusz(t, url, func(z map[string]any) bool { return z["backends"] == float64(1) && z["healthy"] == float64(1) }, "after remove")
	if code, body := chatReq(t, url, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-a-ok") {
		t.Fatalf("traffic must stay on remaining replica: code=%d body=%s", code, body)
	}
}

// TestAdminInflightDrain：摘除副本时，正在处理中的请求在旧组件上完成（自然排空）。
func TestAdminInflightDrain(t *testing.T) {
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}, LatencyMs: 400})
	url, _, _ := buildAdminRouter(t, "", []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
		engB,
	}, "prefix_affinity")

	sid := sessionKeyPrefers("m-1", "eng-b", []string{"eng-a", "eng-b"})
	type result struct {
		code int
		body string
	}
	done := make(chan result, 1)
	go func() {
		code, body := chatReq(t, url, sid, "m-1")
		done <- result{code, body}
	}()

	time.Sleep(100 * time.Millisecond) // 请求已在 eng-b 上处理中（延迟 400ms）
	if code := adminPost(t, url, "", `{"op":"remove","backend":{"name":"eng-b"}}`); code != 200 {
		t.Fatalf("remove during inflight: code=%d", code)
	}
	waitStatusz(t, url, func(z map[string]any) bool { return z["backends"] == float64(1) }, "after remove")

	r := <-done
	if r.code != 200 || !strings.Contains(r.body, "eng-b-ok") {
		t.Fatalf("in-flight request must drain on removed replica: code=%d body=%s", r.code, r.body)
	}
	if got := engB.Requests(); got != 1 {
		t.Fatalf("eng-b requests = %d, want 1", got)
	}
}

// TestAdminValidationAndRollback：管理 API 的非法输入不触碰运行态（回滚语义）。
func TestAdminValidationAndRollback(t *testing.T) {
	url, _, _ := buildAdminRouter(t, "", []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
	}, "prefix_affinity")

	cases := []struct {
		name string
		body string
		code int
	}{
		{"bad json", `{oops`, 400},
		{"bad op", `{"op":"restart"}`, 400},
		{"missing fields", `{"op":"add","backend":{"name":"x"}}`, 400},
		{"duplicate name", `{"op":"add","backend":{"name":"eng-a","base_url":"http://127.0.0.1:1"}}`, 409},
		{"unknown name", `{"op":"remove","backend":{"name":"ghost"}}`, 404},
	}
	for _, c := range cases {
		if code := adminPost(t, url, "", c.body); code != c.code {
			t.Fatalf("%s: code=%d want %d", c.name, code, c.code)
		}
	}
	z := statusz(t, url)
	if z["backends"] != float64(1) || z["healthy"] != float64(1) {
		t.Fatalf("runtime must be untouched after rejected ops: %v", z)
	}
	// 最后一个副本不允许摘除
	if code := adminPost(t, url, "", `{"op":"remove","backend":{"name":"eng-a"}}`); code != 400 {
		t.Fatalf("removing last backend must be rejected: code=%d", code)
	}
}

// TestAdminAuth：admin_token 配置后强制令牌鉴权。
func TestAdminAuth(t *testing.T) {
	url, _, _ := buildAdminRouter(t, "s3cret", []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
	}, "prefix_affinity")

	if code := adminPost(t, url, "", addBody("eng-b", "http://127.0.0.1:1")); code != 403 {
		t.Fatalf("missing token must be forbidden: code=%d", code)
	}
	if code := adminPost(t, url, "wrong", addBody("eng-b", "http://127.0.0.1:1")); code != 403 {
		t.Fatalf("wrong token must be forbidden: code=%d", code)
	}
	if code := adminPost(t, url, "s3cret", addBody("eng-b", "http://127.0.0.1:1")); code != 200 {
		t.Fatalf("valid token must pass: code=%d", code)
	}
	waitStatusz(t, url, func(z map[string]any) bool { return z["backends"] == float64(2) }, "after authed add")
}

// TestReloadFromRollback：配置文件热更成功生效；解析失败报错且运行态不变（回滚）。
// 策略与副本集合均可经文件热更（含 prefix_affinity → round_robin）。
func TestReloadFromRollback(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}})
	url, rt, _ := buildAdminRouter(t, "", []*mockengine.Engine{engA}, "prefix_affinity")

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "router.yaml")

	write := func(content string) {
		if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write cfg: %v", err)
		}
	}

	// 热更到 round_robin + 同一副本
	write(fmt.Sprintf("addr: \":0\"\nstrategy: round_robin\nbackends:\n  - name: eng-a\n    base_url: %s\n    model: m-1\n", engA.URL()))
	if err := rt.ReloadFrom(cfgPath); err != nil {
		t.Fatalf("reload: %v", err)
	}
	waitStatusz(t, url, func(z map[string]any) bool { return z["strategy"] == "round_robin" && z["backends"] == float64(1) }, "strategy swap")
	if code, body := chatReq(t, url, "", "m-1"); code != 200 || !strings.Contains(body, "eng-a-ok") {
		t.Fatalf("serve after strategy swap: code=%d body=%s", code, body)
	}

	// 坏配置：回滚，运行态保持
	write("strategy: [broken\n  - : : :")
	if err := rt.ReloadFrom(cfgPath); err == nil {
		t.Fatal("malformed config must fail")
	}
	z := statusz(t, url)
	if z["strategy"] != "round_robin" || z["backends"] != float64(1) {
		t.Fatalf("runtime must survive a failed reload: %v", z)
	}
	if code, _ := chatReq(t, url, "", "m-1"); code != 200 {
		t.Fatalf("serve must continue after failed reload")
	}
}

// TestReadyz：readyz 区分"可接流量"与"进程存活"——全副本摘除时 503，statusz 仍 200。
func TestReadyz(t *testing.T) {
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}}),
	}
	url, rt, _ := buildAdminRouter(t, "", engines, "round_robin")

	get := func(path string) int {
		resp, err := http.Get(url + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	if code := get("/readyz"); code != 200 {
		t.Fatalf("readyz with healthy backends: code=%d", code)
	}

	for _, e := range engines {
		e.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Reg.ProbeAll(ctx) // 立即摘除（不等探活周期）

	if code := get("/readyz"); code != 503 {
		t.Fatalf("readyz with all backends evicted: code=%d", code)
	}
	if code := get("/statusz"); code != 200 {
		t.Fatalf("statusz (liveness) must stay 200: code=%d", code)
	}
}
