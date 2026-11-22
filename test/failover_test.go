package test

// M2 场景：请求级 failover（一次顺延重试）与熔断错误分类。

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"infer-router/internal/affinity"
	"infer-router/internal/app"
	"infer-router/internal/metrics"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

func bp(b bool) *bool { return &b }

// chatReqErr 与 chatReq 相同，但客户端侧错误（如流中断开）不视为致命，交由调用方断言。
func chatReqErr(t *testing.T, url, sessionID, model string) (int, string, error) {
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
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

// buildTwoEngineRouter：eng-a（可编程 StatusSeq）/ eng-b 均服务 m-1，权重相同。
func buildTwoEngineRouter(t *testing.T, seqA []int, retry app.RetryCfg, breakerFailThreshold int) (string, []*mockengine.Engine) {
	t.Helper()
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}, StatusSeq: seqA}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}}),
	}
	t.Cleanup(func() { engines[0].Close(); engines[1].Close() })

	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: breakerFailThreshold, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		Retry:             retry,
		Backends: []registry.Backend{
			{Name: "eng-a", BaseURL: engines[0].URL(), Model: "m-1", Weight: 1},
			{Name: "eng-b", BaseURL: engines[1].URL(), Model: "m-1", Weight: 1},
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

// sessionKeyPrefers 返回一个 X-Session-Id，使复合 key 的亲和主选为 wantName。
func sessionKeyPrefers(model, wantName string, names []string) string {
	weights := map[string]int{}
	for _, n := range names {
		weights[n] = 1
	}
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("%s-%d", wantName, i)
		ck := "m:" + model + "|sid:" + k
		if affinity.WeightedSort(ck, names, weights)[0] == wantName {
			return k
		}
	}
	return ""
}

// TestRetryOn5xx：主选副本连续 5xx → 每次自动顺延重试，客户端全部拿到 200。
// 熔断阈值调高以保持整个序列中 eng-a 不被摘除。
func TestRetryOn5xx(t *testing.T) {
	router, engines := buildTwoEngineRouter(t, []int{500, 500, 500, 500, 500}, app.RetryCfg{}, 10)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	if sid == "" {
		t.Fatal("no session key prefers eng-a")
	}

	for i := 0; i < 5; i++ {
		code, body := chatReq(t, router, sid, "m-1")
		if code != 200 || !strings.Contains(body, "eng-b-ok") {
			t.Fatalf("req %d: code=%d body=%s (want 200 from eng-b via failover)", i, code, body)
		}
	}
	if got := engines[0].Requests(); got != 5 {
		t.Fatalf("eng-a attempts = %d, want 5 (each first-tried)", got)
	}
	if got := engines[1].Requests(); got != 5 {
		t.Fatalf("eng-b retries = %d, want 5", got)
	}
	if got := promtestutil.ToFloat64(metrics.Retries.WithLabelValues("eng-a", "eng-b")); got != 5 {
		t.Fatalf("retries counter = %v, want 5", got)
	}
}

// TestRetryDisabled：retry.enabled=false → 5xx 直接透传给客户端，不重试。
func TestRetryDisabled(t *testing.T) {
	router, engines := buildTwoEngineRouter(t, []int{500}, app.RetryCfg{Enabled: bp(false)}, 10)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	code, _ := chatReq(t, router, sid, "m-1")
	if code != 500 {
		t.Fatalf("code=%d, want 500 (retry disabled)", code)
	}
	if got := engines[1].Requests(); got != 0 {
		t.Fatalf("eng-b should not be attempted: %d", got)
	}
}

// TestNoRetryAfterStreamStart：流式响应已开始（响应头已发出）→ 中途断开不触发 failover。
func TestNoRetryAfterStreamStart(t *testing.T) {
	router, engines := buildTwoEngineRouter(t, nil, app.RetryCfg{}, 10)
	engines[0].SetAbortAfter(1)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	_, _, _ = chatReqErr(t, router, sid, "m-1") // 客户端拿到 200-截断或连接错误均可

	if got := engines[1].Requests(); got != 0 {
		t.Fatalf("failover after stream start is forbidden: eng-b got %d requests", got)
	}
}

// TestBreaker4xxNotCounted：4xx 不计入熔断失败（404+model 记 mismatch）。
func TestBreaker4xxNotCounted(t *testing.T) {
	router, _ := buildTwoEngineRouter(t, []int{404, 404, 404}, app.RetryCfg{}, 2)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	for i := 0; i < 3; i++ {
		code, body := chatReq(t, router, sid, "m-1")
		if code != 404 {
			t.Fatalf("req %d: code=%d body=%s (4xx must pass through, no failover)", i, code, body)
		}
	}
	// 连续 3×404 ≥ 阈值 2，但 4xx 不计熔断 → 两副本仍健康
	if got := statusz(t, router)["healthy"]; got != float64(2) {
		t.Fatalf("healthy = %v, want 2 (4xx must not open breaker)", got)
	}
	if got := promtestutil.ToFloat64(metrics.ModelMismatch.WithLabelValues("eng-a", "m-1")); got != 3 {
		t.Fatalf("mismatch counter = %v, want 3", got)
	}
}

// TestBreaker5xxOpensHalfOpen：连续 5xx 打开熔断 → 副本被摘除 → 冷却后半开探测恢复。
func TestBreaker5xxOpensHalfOpen(t *testing.T) {
	router, engines := buildTwoEngineRouter(t, []int{500, 500, 500}, app.RetryCfg{}, 3)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	// 3 次请求：eng-a 各 500（计入熔断），每次重试 eng-b 拿 200
	for i := 0; i < 3; i++ {
		if code, body := chatReq(t, router, sid, "m-1"); code != 200 {
			t.Fatalf("req %d: code=%d body=%s", i, code, body)
		}
	}
	if got := engines[0].Requests(); got != 3 {
		t.Fatalf("eng-a attempts = %d, want 3", got)
	}

	// 熔断打开：eng-a 被摘除，后续请求直接落 eng-b（不再尝试 eng-a）
	deadline := time.Now().Add(5 * time.Second)
	for {
		if statusz(t, router)["healthy"] == float64(1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("breaker did not evict eng-a within 5s: %v", statusz(t, router))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code, _ := chatReq(t, router, sid, "m-1"); code != 200 {
		t.Fatalf("post-breaker request failed")
	}
	if got := engines[0].Requests(); got != 3 {
		t.Fatalf("eng-a should receive no traffic while breaker open: %d", got)
	}

	// 冷却 1s 后半开放行探测请求 → eng-a 状态码序列耗尽恢复 200 → 清零复原
	time.Sleep(1200 * time.Millisecond)
	if code, body := chatReq(t, router, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-a-ok") {
		t.Fatalf("half-open probe failed: code=%d body=%s", code, body)
	}
}
