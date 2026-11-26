package test

// M7 场景：PD（Prefill/Decode）分离网关层——inline/分离路径决策、sglang bootstrap 注入、
// vLLM 两段编排、双池 failover、/readyz 双池语义与配置校验。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"infer-router/internal/app"
	"infer-router/internal/metrics"
	"infer-router/internal/pd"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// pdPostChat 向 router 发自定义 chat 请求（body 原样），返回状态码与响应体。
func pdPostChat(t *testing.T, routerURL, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, routerURL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// pdLongBody 构造超阈值长 prompt（约 1200 字符 → 估算 ~300 token）。
func pdLongBody(model string) string {
	content := strings.Repeat("背景资料要点重述与上下文锚定文本，", 68) // ~1156 汉字 ≈ 1156 token（汉字按字符数估算）
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"system","content":"你是路由测试助手"},{"role":"user","content":%q}],"max_tokens":32}`, model, content)
}

// buildPDRouter 装配 1P + 2D 的 PD 路由（mode=sglang/vllm，模型 m-1，阈值 256）。
// 返回 router URL 与引擎引用（顺序：prefill, decode0, decode1）。
func buildPDRouter(t *testing.T, mode string, prefillSeq []int) (string, []*mockengine.Engine) {
	t.Helper()
	engP := mockengine.New(mockengine.Options{Name: "pd-p0", Models: []string{"m-1"}, StatusSeq: prefillSeq})
	engD0 := mockengine.New(mockengine.Options{Name: "pd-d0", Models: []string{"m-1"}})
	engD1 := mockengine.New(mockengine.Options{Name: "pd-d1", Models: []string{"m-1"}})
	engines := []*mockengine.Engine{engP, engD0, engD1}
	t.Cleanup(func() { engP.Close(); engD0.Close(); engD1.Close() })

	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		PD:                pd.Cfg{Mode: mode},
		Backends: []registry.Backend{
			{Name: "pd-p0", BaseURL: engP.URL(), Model: "m-1", Weight: 1, Role: "prefill"},
			{Name: "pd-d0", BaseURL: engD0.URL(), Model: "m-1", Weight: 1, Role: "decode", BootstrapPort: 8998},
			{Name: "pd-d1", BaseURL: engD1.URL(), Model: "m-1", Weight: 1, Role: "decode", BootstrapPort: 8999},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	rt.Reg.Start(ctx, 200*time.Millisecond) // 周期探活：readyz 双池语义依赖探测刷新
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, engines
}

// TestPDSglangInline：短请求（估算 token < 阈值）inline 直连 D 池，prefill 零流量。
func TestPDSglangInline(t *testing.T) {
	router, engines := buildPDRouter(t, pd.ModeSglang, nil)

	body := `{"model":"m-1","messages":[{"role":"user","content":"hi"}]}`
	code, resp := pdPostChat(t, router, body)
	if code != 200 || !strings.Contains(resp, "pd-d0-ok") && !strings.Contains(resp, "pd-d1-ok") {
		t.Fatalf("inline code=%d body=%s (want decode pool response)", code, resp)
	}
	if got := engines[0].Requests(); got != 0 {
		t.Fatalf("prefill got %d requests, want 0 (inline path skips prefill)", got)
	}
	if got := engines[1].Requests() + engines[2].Requests(); got != 1 {
		t.Fatalf("decode pool requests = %d, want 1", got)
	}
	if got := promtestutil.ToFloat64(metrics.PdPath.WithLabelValues("inline")); got != 1 {
		t.Fatalf("pd path inline counter = %v, want 1", got)
	}
}

// TestPDSglangDisaggBootstrap：长请求走分离路径，prefill 收到注入 bootstrap 三元组的 body，
// decode 不经 router 收到任何请求；两次请求 room 唯一。
func TestPDSglangDisaggBootstrap(t *testing.T) {
	router, engines := buildPDRouter(t, pd.ModeSglang, nil)

	rooms := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		code, resp := pdPostChat(t, router, pdLongBody("m-1"))
		if code != 200 || !strings.Contains(resp, "pd-p0-ok") {
			t.Fatalf("disagg req %d: code=%d body=%s (want prefill response)", i, code, resp)
		}
		var payload struct {
			BootstrapHost string `json:"bootstrap_host"`
			BootstrapPort int    `json:"bootstrap_port"`
			BootstrapRoom uint64 `json:"bootstrap_room"`
			Model         string `json:"model"`
		}
		if err := json.Unmarshal([]byte(engines[0].LastBody()), &payload); err != nil {
			t.Fatalf("prefill body not json: %v (%s)", err, engines[0].LastBody())
		}
		if payload.BootstrapHost != "127.0.0.1" {
			t.Fatalf("bootstrap_host = %q, want 127.0.0.1 (derived from decode base_url)", payload.BootstrapHost)
		}
		if payload.BootstrapPort != 8998 && payload.BootstrapPort != 8999 {
			t.Fatalf("bootstrap_port = %d, want decode bootstrap port", payload.BootstrapPort)
		}
		if payload.BootstrapRoom == 0 {
			t.Fatal("bootstrap_room must be non-zero")
		}
		if rooms[payload.BootstrapRoom] {
			t.Fatalf("bootstrap_room %d reused across requests", payload.BootstrapRoom)
		}
		rooms[payload.BootstrapRoom] = true
		if got := engines[1].Requests() + engines[2].Requests(); got != 0 {
			t.Fatalf("decode pool got %d router requests, want 0 (engine-to-engine transfer is invisible)", got)
		}
	}
	if got := promtestutil.ToFloat64(metrics.PdPath.WithLabelValues("disagg")); got != 2 {
		t.Fatalf("pd path disagg counter = %v, want 2", got)
	}
}

// TestPDSglangPrefillFailover：P 池主选 5xx → 换次优 prefill 重试（重新生成 room），客户端拿到 200。
func TestPDSglangPrefillFailover(t *testing.T) {
	// 2P + 1D 专用拓扑：buildPDRouter 只有 1 个 prefill，无法验证 P 池内 failover
	engP0 := mockengine.New(mockengine.Options{Name: "pd-p0", Models: []string{"m-1"}, StatusSeq: []int{500}})
	engP1 := mockengine.New(mockengine.Options{Name: "pd-p1", Models: []string{"m-1"}})
	engD0 := mockengine.New(mockengine.Options{Name: "pd-d0", Models: []string{"m-1"}})
	t.Cleanup(func() { engP0.Close(); engP1.Close(); engD0.Close() })

	cfg := &app.Config{
		Addr: ":0", Strategy: "round_robin", HealthIntervalSec: 1,
		Breaker: app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		PD:      pd.Cfg{Mode: pd.ModeSglang},
		Backends: []registry.Backend{
			{Name: "pd-p0", BaseURL: engP0.URL(), Model: "m-1", Role: "prefill"},
			{Name: "pd-p1", BaseURL: engP1.URL(), Model: "m-1", Role: "prefill"},
			{Name: "pd-d0", BaseURL: engD0.URL(), Model: "m-1", Role: "decode", BootstrapPort: 8998},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	code, resp := pdPostChat(t, srv.URL, pdLongBody("m-1"))
	if code != 200 || !strings.Contains(resp, "pd-p1-ok") {
		t.Fatalf("code=%d body=%s (want pd-p1 response after failover)", code, resp)
	}
	if got := engP0.Requests(); got != 1 {
		t.Fatalf("pd-p0 attempts = %d, want 1 (failed first try)", got)
	}
	var payload struct {
		BootstrapRoom uint64 `json:"bootstrap_room"`
	}
	if err := json.Unmarshal([]byte(engP1.LastBody()), &payload); err != nil || payload.BootstrapRoom == 0 {
		t.Fatalf("retry attempt body missing bootstrap_room: %v", err)
	}
}

// TestPDVllmTwoPhase：vllm 模式两段编排——阶段1 改写 max_tokens=1/stream=false 并携带
// 指向 decode 的 kv_transfer_params；阶段2 用阶段1返回的 kv_transfer_params 发 decode，
// 客户端拿到 decode 的最终响应。
func TestPDVllmTwoPhase(t *testing.T) {
	router, engines := buildPDRouter(t, pd.ModeVllm, nil)
	engines[0].SetExtraResponseFields(map[string]any{
		"kv_transfer_params": map[string]any{
			"remote_engine_id": "engine-p0",
			"remote_host":      "127.0.0.1",
			"remote_port":      8998,
		},
	})

	code, resp := pdPostChat(t, router, pdLongBody("m-1"))
	if code != 200 || !strings.Contains(resp, "pd-d0-ok") {
		t.Fatalf("code=%d body=%s (want decode final response)", code, resp)
	}

	// 阶段1断言：prefill 收到 max_tokens=1 + do_remote_decode
	var phase1 struct {
		MaxTokens        int           `json:"max_tokens"`
		Stream           bool          `json:"stream"`
		KVTransferParams map[string]any `json:"kv_transfer_params"`
	}
	if err := json.Unmarshal([]byte(engines[0].LastBody()), &phase1); err != nil {
		t.Fatalf("prefill body not json: %v", err)
	}
	if phase1.MaxTokens != 1 || phase1.Stream {
		t.Fatalf("phase1 max_tokens=%d stream=%v, want 1/false", phase1.MaxTokens, phase1.Stream)
	}
	if phase1.KVTransferParams["do_remote_decode"] != true {
		t.Fatalf("phase1 kv_transfer_params.do_remote_decode = %v, want true", phase1.KVTransferParams["do_remote_decode"])
	}

	// 阶段2断言：decode 收到原始 max_tokens + 阶段1返回的 kv_transfer_params
	var phase2 struct {
		MaxTokens        int            `json:"max_tokens"`
		KVTransferParams map[string]any `json:"kv_transfer_params"`
	}
	if err := json.Unmarshal([]byte(engines[1].LastBody()), &phase2); err != nil {
		t.Fatalf("decode body not json: %v (%s)", err, engines[1].LastBody())
	}
	if phase2.MaxTokens != 32 {
		t.Fatalf("phase2 max_tokens = %d, want original 32", phase2.MaxTokens)
	}
	if phase2.KVTransferParams == nil || phase2.KVTransferParams["remote_engine_id"] != "engine-p0" {
		t.Fatalf("phase2 kv_transfer_params.remote_engine_id = %v, want engine-p0", phase2.KVTransferParams)
	}
	if got := promtestutil.ToFloat64(metrics.PdBootstrap.WithLabelValues("ok")); got != 1 {
		t.Fatalf("bootstrap ok counter = %v, want 1", got)
	}
}

// TestPDVllmPrefillFailover：阶段1 主选 5xx → 次优 prefill 重试 → decode 收到次优返回的参数。
func TestPDVllmPrefillFailover(t *testing.T) {
	// 1P 场景无法做 prefill failover（P 池 1 副本），这里装配 2P + 1D 专用拓扑
	engP0 := mockengine.New(mockengine.Options{Name: "vp0", Models: []string{"m-1"}, StatusSeq: []int{500}})
	engP1 := mockengine.New(mockengine.Options{Name: "vp1", Models: []string{"m-1"}})
	engD0 := mockengine.New(mockengine.Options{Name: "vd0", Models: []string{"m-1"}})
	t.Cleanup(func() { engP0.Close(); engP1.Close(); engD0.Close() })
	engP1.SetExtraResponseFields(map[string]any{
		"kv_transfer_params": map[string]any{"remote_engine_id": "engine-vp1"},
	})

	cfg := &app.Config{
		Addr: ":0", Strategy: "round_robin", HealthIntervalSec: 1,
		Breaker: app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		PD:      pd.Cfg{Mode: pd.ModeVllm},
		Backends: []registry.Backend{
			{Name: "vp0", BaseURL: engP0.URL(), Model: "m-1", Role: "prefill"},
			{Name: "vp1", BaseURL: engP1.URL(), Model: "m-1", Role: "prefill"},
			{Name: "vd0", BaseURL: engD0.URL(), Model: "m-1", Role: "decode", BootstrapPort: 8998},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	code, resp := pdPostChat(t, srv.URL, pdLongBody("m-1"))
	if code != 200 || !strings.Contains(resp, "vd0-ok") {
		t.Fatalf("code=%d body=%s (want decode response after prefill failover)", code, resp)
	}
	if got := engP0.Requests(); got != 1 {
		t.Fatalf("vp0 attempts = %d, want 1", got)
	}
	var phase2 struct {
		KVTransferParams map[string]any `json:"kv_transfer_params"`
	}
	if err := json.Unmarshal([]byte(engD0.LastBody()), &phase2); err != nil {
		t.Fatalf("decode body not json: %v", err)
	}
	if phase2.KVTransferParams["remote_engine_id"] != "engine-vp1" {
		t.Fatalf("decode got params from %v, want engine-vp1 (failover prefill)", phase2.KVTransferParams)
	}
}

// TestPDReadyzDualPool：pd.mode 启用后 /readyz 要求 P、D 池各 ≥1 健康；decode 全挂 → 503。
func TestPDReadyzDualPool(t *testing.T) {
	router, engines := buildPDRouter(t, pd.ModeSglang, nil)

	resp, err := http.Get(router + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz = %d (%s), want 200 with both pools healthy", resp.StatusCode, b)
	}

	engines[1].Close() // d0 下线
	engines[2].Close() // d1 下线
	deadline := time.Now().Add(4 * time.Second)
	for {
		resp, err = http.Get(router + "/readyz")
		if err == nil {
			b, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 503 && strings.Contains(string(b), `"decode_healthy":0`) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("readyz did not report 503/decode_healthy=0 after decode pool down: %d %s", resp.StatusCode, b)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestPDConfigValidation：PD 配置一致性校验（Swap 失败回滚语义）。
func TestPDConfigValidation(t *testing.T) {
	base := []registry.Backend{
		{Name: "p", BaseURL: "http://127.0.0.1:9001", Model: "m-1", Role: "prefill"},
		{Name: "d", BaseURL: "http://127.0.0.1:9002", Model: "m-1", Role: "decode"},
	}

	cases := []struct {
		name    string
		mutate  func(*app.Config)
		wantErr bool
	}{
		{"valid dual pool", func(c *app.Config) {}, false},
		{"bad mode", func(c *app.Config) { c.PD.Mode = "chained" }, true},
		{"missing role", func(c *app.Config) { c.Backends = base[:1] }, true},
		{"role without pd", func(c *app.Config) {
			c.PD.Mode = ""
		}, true},
	}
	for _, tc := range cases {
		cfg := &app.Config{
			Addr: ":0", Strategy: "round_robin",
			Backends: append([]registry.Backend(nil), base...),
		}
		cfg.PD.Mode = pd.ModeSglang
		tc.mutate(cfg)
		rt := &app.Runtime{}
		err := rt.Swap(cfg)
		if tc.wantErr != (err != nil) {
			t.Fatalf("%s: err=%v, wantErr=%v", tc.name, err, tc.wantErr)
		}
	}

	// role 存在但 pd 未启用 → 拒绝
	cfg := &app.Config{Addr: ":0", Strategy: "round_robin", Backends: base}
	rt := &app.Runtime{}
	if err := rt.Swap(cfg); err == nil {
		t.Fatal("role without pd.mode must be rejected")
	}
}
