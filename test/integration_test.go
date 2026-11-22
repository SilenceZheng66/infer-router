package test

// 集成测试：经 app.Build 装配的真实处理链（registry → picker → proxy），
// 后端为可编程 mock 引擎。覆盖 M0 基线场景；M1~M5 里程碑在此基础上扩展。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"infer-router/internal/app"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// buildRouter 起两台 mock 引擎并装配 router handler，返回 (router URL, 引擎列表)。
func buildRouter(t *testing.T, strategy string, opts ...func(*app.Config)) (string, []*mockengine.Engine) {
	t.Helper()
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}}),
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
		Breaker:           app.BreakerCfg{FailThreshold: 3, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		Backends: []registry.Backend{
			{Name: "eng-a", BaseURL: engines[0].URL(), Model: "m-1", Weight: 1},
			{Name: "eng-b", BaseURL: engines[1].URL(), Model: "m-1", Weight: 1},
		},
	}
	for _, f := range opts {
		f(cfg)
	}

	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	rt.Reg.Start(ctx, 200*time.Millisecond) // 集成测试用短探活周期，模拟生产摘除行为
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, engines
}

func chatReq(t *testing.T, url, sessionID, model string) (int, string) {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, model)
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if sessionID != "" {
		req.Header.Set("X-Session-Id", sessionID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func statusz(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url + "/statusz")
	if err != nil {
		t.Fatalf("statusz: %v", err)
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode statusz: %v", err)
	}
	return m
}

// --- M1 场景 ---

// buildThreeModelRouter：eng-a/b 服务 m-1（权重 2:1），eng-c 服务 m-2。
func buildThreeModelRouter(t *testing.T, strategy string) (string, []*mockengine.Engine) {
	t.Helper()
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}}),
		mockengine.New(mockengine.Options{Name: "eng-c", Models: []string{"m-2"}}),
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
		Breaker:           app.BreakerCfg{FailThreshold: 3, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		Backends: []registry.Backend{
			{Name: "eng-a", BaseURL: engines[0].URL(), Model: "m-1", Weight: 2},
			{Name: "eng-b", BaseURL: engines[1].URL(), Model: "m-1", Weight: 1},
			{Name: "eng-c", BaseURL: engines[2].URL(), Model: "m-2", Weight: 1},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	rt.Reg.Start(ctx, 200*time.Millisecond)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, engines
}

// TestModelRoutingNoCrossPool：请求按 model 精确落池，零串池。
func TestModelRoutingNoCrossPool(t *testing.T) {
	router, engines := buildThreeModelRouter(t, "prefix_affinity")

	// 阶段一：20× m-1 → 只落 eng-a/eng-b
	for i := 0; i < 20; i++ {
		if code, body := chatReq(t, router, fmt.Sprintf("mm1-%d", i), "m-1"); code != 200 {
			t.Fatalf("m-1 req %d: code=%d body=%s", i, code, body)
		}
	}
	if got := engines[2].Requests(); got != 0 {
		t.Fatalf("m-1 leaked to m-2 engine: %d", got)
	}
	if got := engines[0].Requests() + engines[1].Requests(); got != 20 {
		t.Fatalf("m-1 pool received %d, want 20", got)
	}

	// 阶段二：20× m-2 → 只落 eng-c，且不回流 m-1 池
	for i := 0; i < 20; i++ {
		if code, body := chatReq(t, router, fmt.Sprintf("mm2-%d", i), "m-2"); code != 200 {
			t.Fatalf("m-2 req %d: code=%d body=%s", i, code, body)
		}
	}
	if got := engines[2].Requests(); got != 20 {
		t.Fatalf("m-2 pool received %d, want 20", got)
	}
	if got := engines[0].Requests() + engines[1].Requests(); got != 20 {
		t.Fatalf("m-2 leaked to m-1 pool: %d", got)
	}
}

// TestModelNoMatch：无副本服务请求的模型 → 502 + 结构化错误。
func TestModelNoMatch(t *testing.T) {
	router, engines := buildThreeModelRouter(t, "prefix_affinity")

	code, body := chatReq(t, router, "nm-1", "m-x")
	if code != http.StatusBadGateway || !strings.Contains(body, "no_backend_for_model") {
		t.Fatalf("code=%d body=%s, want 502 no_backend_for_model", code, body)
	}
	if got := engines[0].Requests() + engines[1].Requests() + engines[2].Requests(); got != 0 {
		t.Fatalf("nomatch request reached a backend: %d", got)
	}
}

// TestCompositeKeyModelSwitch：同一会话切换模型 → 亲和目标随模型切换。
func TestCompositeKeyModelSwitch(t *testing.T) {
	router, engines := buildThreeModelRouter(t, "prefix_affinity")

	if code, _ := chatReq(t, router, "same-sid", "m-1"); code != 200 {
		t.Fatalf("m-1 req failed")
	}
	if code, body := chatReq(t, router, "same-sid", "m-2"); code != 200 {
		t.Fatalf("m-2 req failed: %s", body)
	}
	// m-2 必须落在 m-2 池（复合 key 不把会话钉死在 m-1 的原家）
	if got := engines[2].Requests(); got != 1 {
		t.Fatalf("model-switch request did not reach m-2 engine: %d", got)
	}
}

// TestWeightedDistributionIntegration：权重 2:1 在网关全链路上的会话分布。
func TestWeightedDistributionIntegration(t *testing.T) {
	router, engines := buildThreeModelRouter(t, "prefix_affinity")

	const N = 300
	for i := 0; i < N; i++ {
		if code, body := chatReq(t, router, fmt.Sprintf("wd-%d", i), "m-1"); code != 200 {
			t.Fatalf("req %d: code=%d body=%s", i, code, body)
		}
	}
	a := float64(engines[0].Requests())
	want := 2.0 / 3.0 * N
	if math.Abs(a-want) > 0.10*want {
		t.Fatalf("weighted distribution off: eng-a=%d eng-b=%d (want a≈%.0f ±10%%)",
			engines[0].Requests(), engines[1].Requests(), want)
	}
}

// TestModelMultiServing：副本声明逗号分隔多模型 → 按模型正确落池。
func TestModelMultiServing(t *testing.T) {
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1", "m-3"}}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-2"}}),
	}
	t.Cleanup(func() { engines[0].Close(); engines[1].Close() })
	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: 3, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		Backends: []registry.Backend{
			{Name: "eng-a", BaseURL: engines[0].URL(), Model: "m-1,m-3", Weight: 1},
			{Name: "eng-b", BaseURL: engines[1].URL(), Model: "m-2", Weight: 1},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	rt.Reg.Start(ctx, 200*time.Millisecond)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	if code, _ := chatReq(t, srv.URL, "multi-1", "m-3"); code != 200 {
		t.Fatalf("m-3 req failed")
	}
	if got := engines[0].Requests(); got != 1 {
		t.Fatalf("m-3 did not reach multi-model engine: %d", got)
	}
	if code, _ := chatReq(t, srv.URL, "multi-2", "m-2"); code != 200 {
		t.Fatalf("m-2 req failed")
	}
	if got := engines[1].Requests(); got != 1 {
		t.Fatalf("m-2 did not reach its engine: %d", got)
	}
}

// TestEndToEndSticky：同一 X-Session-Id 稳定命中同一副本；不同会话分布到两台。
func TestEndToEndSticky(t *testing.T) {
	router, engines := buildRouter(t, "prefix_affinity")

	if got := statusz(t, router)["healthy"]; got != float64(2) {
		t.Fatalf("healthy = %v, want 2", got)
	}

	// 同一会话 10 次 → 单台副本独收
	for i := 0; i < 10; i++ {
		code, body := chatReq(t, router, "sess-fixed", "m-1")
		if code != 200 || !strings.Contains(body, "-ok") {
			t.Fatalf("req %d: code=%d body=%s", i, code, body)
		}
	}
	a, b := engines[0].Requests(), engines[1].Requests()
	if (a == 10 && b == 0) || (a == 0 && b == 10) {
		// 粘性成立
	} else {
		t.Fatalf("sticky broken: eng-a=%d eng-b=%d", a, b)
	}

	// 16 个不同会话 → 两台都有流量（rendezvous 均匀性）
	for i := 0; i < 16; i++ {
		if code, _ := chatReq(t, router, fmt.Sprintf("s-%d", i), "m-1"); code != 200 {
			t.Fatalf("session %d: code=%d", i, code)
		}
	}
	a, b = engines[0].Requests(), engines[1].Requests()
	if a < 1 || b < 1 {
		t.Fatalf("distribution degenerate: eng-a=%d eng-b=%d", a, b)
	}
}

// TestProbeEviction：副本下线 → 探活摘除 → 流量全部落存活副本。
func TestProbeEviction(t *testing.T) {
	router, engines := buildRouter(t, "prefix_affinity")

	engines[0].Close() // 模拟副本宕机
	// 探活循环 1s 周期；这里直接触发一次全量探测并稍等循环刷新
	deadline := time.Now().Add(5 * time.Second)
	for {
		if statusz(t, router)["healthy"] == float64(1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica not evicted within 5s: %v", statusz(t, router))
		}
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < 20; i++ {
		if code, body := chatReq(t, router, fmt.Sprintf("after-kill-%d", i), "m-1"); code != 200 {
			t.Fatalf("req %d: code=%d body=%s", i, code, body)
		}
	}
	if got := engines[1].Requests(); got < 20 {
		t.Fatalf("survivor requests = %d, want >= 20 (dead replica got %d)", got, engines[0].Requests())
	}
}

// TestNoHealthyBackend：全部副本下线 → 502 快速失败。
func TestNoHealthyBackend(t *testing.T) {
	router, engines := buildRouter(t, "prefix_affinity")
	engines[0].Close()
	engines[1].Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if statusz(t, router)["healthy"] == float64(0) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not evicted: %v", statusz(t, router))
		}
		time.Sleep(100 * time.Millisecond)
	}
	code, body := chatReq(t, router, "any", "m-1")
	if code != http.StatusBadGateway || !strings.Contains(body, "no healthy backend") {
		t.Fatalf("code=%d body=%s, want 502 no healthy backend", code, body)
	}
}

// TestRoundRobinFallback：非 chat 请求（key 不可提取）退化为 RR，仍正常透传。
func TestRoundRobinFallback(t *testing.T) {
	router, engines := buildRouter(t, "prefix_affinity")

	var lastCode int
	for i := 0; i < 6; i++ {
		req, _ := http.NewRequest(http.MethodPost, router+"/v1/embeddings", strings.NewReader(`{}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("embeddings req: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		lastCode = resp.StatusCode
		if lastCode != 200 {
			t.Fatalf("embeddings: code=%d", lastCode)
		}
	}
	if engines[0].Requests()+engines[1].Requests() != 6 {
		t.Fatalf("fallback path did not proxy: %d+%d", engines[0].Requests(), engines[1].Requests())
	}
}
