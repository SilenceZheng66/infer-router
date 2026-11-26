package test

// M5 场景：KV 容量感知路由（差异化能力）。
// 探活顺带抓副本 KV 使用率 → 暴露 router_backend_kv_util；
// rendezvous top-k 内首选副本超 KV 高水位时改派余量大者（含树命中路径）；
// cap 按 KV 余量缩放为可选开关；低水位零回归。

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"infer-router/internal/affinity"
	"infer-router/internal/app"
	"infer-router/internal/metrics"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// buildKVRouter 装配带 KV 配置的路由（探活周期短，含 KV 抓取）。
func buildKVRouter(t *testing.T, kvCfg app.KVCfg, engines ...*mockengine.Engine) (string, *app.Runtime, []string) {
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
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096},
		KV:                kvCfg,
		Backends:          backs,
	}
	rt, handler := app.Build(cfg)
	rt.StartProbes(150 * time.Millisecond)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, rt, names
}

func probeAll(t *testing.T, rt *app.Runtime) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Reg.ProbeAll(ctx)
}

func stateByName(t *testing.T, rt *app.Runtime, name string) *registry.State {
	t.Helper()
	for _, s := range rt.Reg.All() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("backend %q not found", name)
	return nil
}

// TestKVProbeCollectsUtil：探活循环顺带抓 KV 使用率并暴露指标。
func TestKVProbeCollectsUtil(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}})
	engA.SetKVUsage(0.72)
	url, rt, _ := buildKVRouter(t, app.KVCfg{}, engA)

	probeAll(t, rt)

	if got := stateByName(t, rt, "eng-a").KVUtil(); got < 0.7199 || got > 0.7201 {
		t.Fatalf("kv util = %v, want 0.72", got)
	}
	if got := promtestutil.ToFloat64(metrics.BackendKVUtil.WithLabelValues("eng-a")); got < 0.7199 || got > 0.7201 {
		t.Fatalf("gauge router_backend_kv_util = %v, want 0.72", got)
	}
	_ = url
}

// TestKVSteeringHighWatermark：首选副本 KV 超高水位 → top-k 内改派余量大者；
// 水位回落后行为与基线完全一致（零回归）。
func TestKVSteeringHighWatermark(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}})
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}})
	url, rt, _ := buildKVRouter(t, app.KVCfg{}, engA, engB)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})

	// 高水位：eng-a 0.95 → 分流到余量大的 eng-b
	engA.SetKVUsage(0.95)
	engB.SetKVUsage(0.30)
	probeAll(t, rt)
	if code, body := chatReq(t, url, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-b-ok") {
		t.Fatalf("high watermark must steer to eng-b: code=%d body=%s", code, body)
	}
	if got := promtestutil.ToFloat64(metrics.KVSteered); got < 1 {
		t.Fatalf("kv_steered counter = %v, want ≥ 1", got)
	}

	// 水位回落：eng-a 0.50（< 0.9）→ 行为与基线一致，仍落主选 eng-a
	engA.SetKVUsage(0.50)
	engB.SetKVUsage(0.20)
	probeAll(t, rt)
	for i := 0; i < 3; i++ {
		if code, body := chatReq(t, url, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-a-ok") {
			t.Fatalf("below watermark must keep affinity: code=%d body=%s", code, body)
		}
	}
}

// TestKVDisabledBaseline：kv.enabled=false 时完全关闭分流（显式开关）。
func TestKVDisabledBaseline(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}})
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}})
	url, rt, _ := buildKVRouter(t, app.KVCfg{Enabled: boolPtr(false)}, engA, engB)

	engA.SetKVUsage(0.98)
	engB.SetKVUsage(0.10)
	probeAll(t, rt)

	sid := sessionKeyPrefers("m-1", "eng-a", []string{"eng-a", "eng-b"})
	if code, body := chatReq(t, url, sid, "m-1"); code != 200 || !strings.Contains(body, "eng-a-ok") {
		t.Fatalf("kv disabled must keep affinity: code=%d body=%s", code, body)
	}
}

// TestKVTreeHitSteering：树命中的长前缀会话，在命中副本高水位时分流到低水位副本；
// 高水位解除后沿新副本保持粘性（树已按最新归属延伸）。
func TestKVTreeHitSteering(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}})
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}})
	url, rt, names := buildKVRouter(t, app.KVCfg{}, engA, engB)

	sys := "长会话共享的系统提示，用于前缀树匹配与 KV 复用。"
	turns := []string{"第一轮问题。", "第二轮追问。", "第三轮追问。"}

	// 轮 1：落点记为 first（无会话 ID，走前缀树）
	code, body := postRaw(t, url, convTurn("m-1", sys, turns, 1))
	if code != 200 {
		t.Fatalf("turn1: code=%d", code)
	}
	first := replicaOf(names, body)
	if first == "" {
		t.Fatalf("turn1: unknown replica: %s", body)
	}
	other := "eng-a"
	if first == "eng-a" {
		other = "eng-b"
	}
	engineByName := map[string]*mockengine.Engine{"eng-a": engA, "eng-b": engB}

	// 抬高 first 的 KV 水位 → 轮 2 分流到 other（树命中路径的高水位检查）
	engineByName[first].SetKVUsage(0.96)
	engineByName[other].SetKVUsage(0.20)
	probeAll(t, rt)
	code, body = postRaw(t, url, convTurn("m-1", sys, turns, 2))
	if code != 200 {
		t.Fatalf("turn2: code=%d", code)
	}
	if got := replicaOf(names, body); got != other {
		t.Fatalf("turn2 must steer from %s (kv high) to %s: got %s", first, other, got)
	}

	// first 水位回落 → 轮 3 沿 other 保持粘性（树已按 other 归属延伸）
	engineByName[first].SetKVUsage(0.30)
	probeAll(t, rt)
	code, body = postRaw(t, url, convTurn("m-1", sys, turns, 3))
	if code != 200 {
		t.Fatalf("turn3: code=%d", code)
	}
	if got := replicaOf(names, body); got != other {
		t.Fatalf("turn3 must stay on steered replica %s: got %s", other, got)
	}
}

// TestKVCapScale：cap 按 KV 余量缩放（可选开关）——高水位副本的并发承载被压缩，
// 并发突发下大部分流量被 bounded load 顺延到低水位副本。
func TestKVCapScale(t *testing.T) {
	engA := mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}, LatencyMs: 800})
	engB := mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}})
	url, rt, _ := buildKVRouter(t, app.KVCfg{CapScale: boolPtr(true)}, engA, engB)

	engA.SetKVUsage(0.85) // < 0.9 水位（分流不触发）；cap 缩放因子 0.15
	engB.SetKVUsage(0.30)
	probeAll(t, rt)

	// 6 个都以 eng-a 为 rendezvous 主选的并发会话
	sids := make([]string, 0, 6)
	for i := 0; len(sids) < 6 && i < 100000; i++ {
		sid := fmt.Sprintf("cap-%d", i)
		ck := "m:m-1|sid:" + sid
		if affinity.WeightedSort(ck, []string{"eng-a", "eng-b"}, map[string]int{"eng-a": 1, "eng-b": 1})[0] == "eng-a" {
			sids = append(sids, sid)
		}
	}
	if len(sids) < 6 {
		t.Fatal("cannot find enough eng-a-preferred session keys")
	}

	var wg sync.WaitGroup
	for _, sid := range sids {
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			_, _, _ = chatReqErr(t, url, sid, "m-1")
		}(sid)
	}
	wg.Wait()

	aGot, bGot := engA.Requests(), engB.Requests()
	if aGot > 2 || bGot < 4 {
		t.Fatalf("cap scale must shed load to eng-b: eng-a=%d eng-b=%d", aGot, bGot)
	}
	_ = rt
}
