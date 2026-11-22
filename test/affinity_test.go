package test

import (
	"fmt"
	"math"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"infer-router/internal/affinity"
	"infer-router/internal/registry"
)

func names(ss []*registry.State) []string {
	var out []string
	for _, s := range ss {
		out = append(out, s.Name)
	}
	return out
}

func chatBody(system, firstUser string) []byte {
	return []byte(fmt.Sprintf(`{"model":"m","messages":[
		{"role":"system","content":%[1]q},
		{"role":"user","content":%[2]q},
		{"role":"assistant","content":"ok"},
		{"role":"user","content":"turn-2 question"}]}`,
		system, firstUser))
}

// ---- SessionKey ----

func TestSessionKeyPrefersHeader(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chatBody("s", "u1")))
	r.Header.Set("X-Session-Id", "abc-123")
	key, err := affinity.SessionKey(r, chatBody("s", "u1"))
	if err != nil {
		t.Fatal(err)
	}
	if key != "sid:abc-123" {
		t.Fatalf("want sid key, got %s", key)
	}
}

func TestSessionKeyStableAcrossTurns(t *testing.T) {
	// 关键性质：多轮对话中，前缀（system + 首条 user）不变 → key 不变 → 亲和成立
	b1 := []byte(`{"model":"m","messages":[{"role":"system","content":"sys"},
		{"role":"user","content":"hello"}]}`)
	b2 := []byte(`{"model":"m","messages":[{"role":"system","content":"sys"},
		{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},
		{"role":"user","content":"second question"}]}`)
	r1 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(b1)))
	r2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(b2)))
	k1, err1 := affinity.SessionKey(r1, b1)
	k2, err2 := affinity.SessionKey(r2, b2)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if k1 != k2 {
		t.Fatalf("multi-turn key drifted: %s vs %s", k1, k2)
	}
}

func TestSessionKeyDifferentiatesConversations(t *testing.T) {
	b1 := chatBody("s", "conversation-one")
	b2 := chatBody("s", "conversation-two")
	r1 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(b1)))
	r2 := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(b2)))
	k1, _ := affinity.SessionKey(r1, b1)
	k2, _ := affinity.SessionKey(r2, b2)
	if k1 == k2 {
		t.Fatal("different conversations must not share a key")
	}
}

func TestSessionKeyInvalidBody(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("not json"))
	if _, err := affinity.SessionKey(r, []byte("not json")); err == nil {
		t.Fatal("invalid body should error (fallback path)")
	}
}

// ---- Rendezvous 基本性质 ----

func TestSortNodesDeterministic(t *testing.T) {
	nodes := []string{"vllm-a", "vllm-b", "vllm-c"}
	first := affinity.SortNodes("key-X", nodes)
	for i := 0; i < 50; i++ {
		again := affinity.SortNodes("key-X", nodes)
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("non-deterministic ordering at i=%d", i)
			}
		}
	}
}

func TestRendezvousMinimalDisruption(t *testing.T) {
	// rendezvous 核心承诺：节点从 n 增至 n+1，已有 key 仅 ~1/(n+1) 迁移
	nodes3 := []string{"a", "b", "c"}
	nodes4 := []string{"a", "b", "c", "d"}
	var moved int
	const N = 10000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("conv-%d", i)
		o3 := affinity.SortNodes(key, nodes3)[0]
		o4 := affinity.SortNodes(key, nodes4)[0]
		if o3 != o4 {
			moved++
		}
	}
	ratio := float64(moved) / N
	// 理论 1/4 = 0.25，允许统计涨落
	if ratio < 0.20 || ratio > 0.32 {
		t.Fatalf("disruption ratio %.3f outside [0.20,0.32] (theory 0.25)", ratio)
	}
}

func TestRendezvousEvenSpread(t *testing.T) {
	// 均匀性：3 节点各得 ~1/3（±8%）
	nodes := []string{"a", "b", "c"}
	count := map[string]int{}
	const N = 30000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("conv-%d", i)
		count[affinity.SortNodes(key, nodes)[0]]++
	}
	for _, n := range nodes {
		want := float64(N) / 3
		dev := math.Abs(float64(count[n])-want) / want
		if dev > 0.08 {
			t.Fatalf("node %s share deviation %.3f > 0.08 (%d/%d)", n, dev, count[n], N)
		}
	}
}

// ---- Picker：亲和 + bounded load ----

func TestPickerStickyWhenIdle(t *testing.T) {
	states := makeStates([]string{"a", "b"})
	p := affinity.NewPicker(1.25, 1024)
	first := p.Pick("conv-A", states)
	for i := 0; i < 20; i++ {
		if got := p.Pick("conv-A", states); got.Name != first.Name {
			t.Fatalf("sticky broken at %d: %s -> %s", i, first.Name, got.Name)
		}
	}
}

func TestPickerBoundedLoadFallback(t *testing.T) {
	states := makeStates([]string{"a", "b"})
	p := affinity.NewPicker(1.25, 1024)
	home := p.Pick("conv-A", states)
	other := states[0]
	if other.Name == home.Name {
		other = states[1]
	}
	// 把亲和目标灌满：两个节点平均在途 = 50+500/2 = 275，cap = ceil(1.25×275) = 344
	// home 在途 500 > 344 → 顺延到 other（在途 50 < 344）
	home.Inc()
	for i := 0; i < 500; i++ {
		home.Inc()
	}
	for i := 0; i < 50; i++ {
		other.Inc()
	}
	got := p.Pick("conv-A", states)
	if got.Name == home.Name {
		t.Fatalf("bounded load failed: overloaded home %s still chosen (cap≈344, in_flight=%d)",
			home.Name, home.InFlight())
	}
	if got.Name != other.Name {
		t.Fatalf("want fallback to %s, got %s", other.Name, got.Name)
	}
}

func TestPickerStickyRatioObservable(t *testing.T) {
	states := makeStates([]string{"a", "b"})
	p := affinity.NewPicker(1.25, 1024)
	for i := 0; i < 100; i++ {
		p.Pick(fmt.Sprintf("conv-%d", i%10), states)
	}
	if ratio := p.StickyRatio(); ratio < 0.9 {
		t.Fatalf("sticky ratio %.2f too low for idle fleet", ratio)
	}
}

// ---- 端到端小模拟：多轮会话流量在双副本下的路由分布 ----

func TestMultiTurnTrafficDistribution(t *testing.T) {
	states := makeStates([]string{"vllm-a", "vllm-b"})
	p := affinity.NewPicker(1.25, 4096)
	count := map[string]int{}
	for c := 0; c < 200; c++ { // 200 个会话
		body := chatBody("system-prompt", fmt.Sprintf("conversation starter %d", c))
		for turn := 0; turn < 5; turn++ { // 每会话 5 轮
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(body)))
			key, _ := affinity.SessionKey(r, body)
			st := p.Pick(key, states)
			if turn == 0 {
				count[st.Name]++
			}
		}
	}
	var shares []float64
	for _, s := range states {
		shares = append(shares, float64(count[s.Name])/200)
	}
	sort.Float64s(shares)
	if shares[1]-shares[0] > 0.35 { // 亲和分布允许会话级倾斜，但不应一家独大
		t.Fatalf("session distribution too skewed: %v", shares)
	}
	// 全部轮次结束后粘性率应接近 1（每轮 key 相同）
	if ratio := p.StickyRatio(); ratio < 0.99 {
		t.Fatalf("multi-turn sticky ratio %.3f < 0.99", ratio)
	}
}
