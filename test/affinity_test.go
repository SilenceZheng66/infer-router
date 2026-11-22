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
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(chatBody("s", "u1"))))
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

// ---- M1b：加权 rendezvous ----

func TestWeightedSortDistribution(t *testing.T) {
	// 权重 2:1 → 大样本份额 2/3 : 1/3（±10%）
	nodes := []string{"heavy", "light"}
	weights := map[string]int{"heavy": 2, "light": 1}
	count := map[string]int{}
	const N = 10000
	for i := 0; i < N; i++ {
		count[affinity.WeightedSort(fmt.Sprintf("conv-%d", i), nodes, weights)[0]]++
	}
	heavyShare := float64(count["heavy"]) / N
	want := 2.0 / 3.0
	dev := math.Abs(heavyShare-want) / want
	if dev > 0.10 {
		t.Fatalf("heavy share %.3f deviation %.3f > 0.10 (want ~0.667)", heavyShare, dev)
	}
}

func TestWeightedSortDeterministic(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := map[string]int{"a": 3, "b": 2, "c": 1}
	first := affinity.WeightedSort("key-W", nodes, weights)
	for i := 0; i < 20; i++ {
		again := affinity.WeightedSort("key-W", nodes, weights)
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("non-deterministic weighted ordering")
			}
		}
	}
}

func TestWeightedSortEqualsUnweightedWhenEqual(t *testing.T) {
	nodes := []string{"a", "b", "c", "d"}
	w1 := map[string]int{"a": 1, "b": 1, "c": 1, "d": 1}
	const N = 2000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("eq-%d", i)
		u := affinity.SortNodes(key, nodes)
		w := affinity.WeightedSort(key, nodes, w1)
		for j := range u {
			if u[j] != w[j] {
				t.Fatalf("equal-weight mismatch at key %s pos %d:\n unweighted %v\n weighted   %v", key, j, u, w)
			}
		}
	}
}

func TestWeightedSortMinimalDisruption(t *testing.T) {
	// 加权最小扰动：加入新副本后，原有副本之间的指派完全不变，
	// 只有部分 key 迁往新副本（迁出比例 ≈ 新副本的加权份额）。
	nodes3 := []string{"a", "b", "c"}
	nodes4 := []string{"a", "b", "c", "d"}
	w3 := map[string]int{"a": 2, "b": 1, "c": 1}
	w4 := map[string]int{"a": 2, "b": 1, "c": 1, "d": 3}
	movedToD, movedElsewhere := 0, 0
	const N = 10000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("wconv-%d", i)
		o3 := affinity.WeightedSort(key, nodes3, w3)[0]
		o4 := affinity.WeightedSort(key, nodes4, w4)[0]
		if o3 != o4 {
			if o4 == "d" {
				movedToD++
			} else {
				movedElsewhere++
			}
		}
	}
	if movedElsewhere != 0 {
		t.Fatalf("existing assignments reshuffled: %d keys moved to non-new nodes", movedElsewhere)
	}
	share := float64(movedToD) / N
	want := 3.0 / 7.0
	if math.Abs(share-want) > 0.08*want {
		t.Fatalf("new node share %.3f deviates from weighted share %.3f", share, want)
	}
}

func TestPickerWeightedCap(t *testing.T) {
	// 加权负载上限：cap_i = ceil(α × Σinflight × w_i / Σw)
	states := makeStates([]string{"a", "b"})
	states[0].Weight = 2
	states[1].Weight = 1
	for i := 0; i < 6; i++ {
		states[0].Inc()
	}
	for i := 0; i < 3; i++ {
		states[1].Inc()
	}
	p := affinity.NewPicker(1.25, 1024)

	key := ""
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("cap-%d", i)
		if affinity.WeightedSort(k, []string{"a", "b"}, map[string]int{"a": 2, "b": 1})[0] == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key prefers weighted-heavy node")
	}
	// 总在途 9：cap_a = ceil(1.25×9×2/3)=8，a=6 < 8 → 不顺延
	if got := p.Pick(key, states); got.Name != "a" {
		t.Fatalf("weighted cap too tight: chose %s (a in_flight=6, cap=8)", got.Name)
	}

	// a 灌到 50：总在途 53，cap_a = ceil(1.25×53×2/3)=45 → 50≥45 顺延；
	// cap_b = ceil(1.25×53×1/3)=23 → b=3 < 23 接住
	for states[0].InFlight() < 50 {
		states[0].Inc()
	}
	if got := p.Pick(key, states); got.Name != "b" {
		t.Fatalf("overloaded weighted home not demoted: chose %s (a=%d, cap≈45)", got.Name, states[0].InFlight())
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
