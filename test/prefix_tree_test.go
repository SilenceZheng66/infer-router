package test

// M3 场景：混合前缀树路由（sid 快路径 → 前缀树命中 → 前缀派单 → 全链派单）。
// 单测覆盖链构建/插入/查找/LRU 驱逐/并发；集成覆盖多轮稳定同副本、
// 新会话摊开、树塞满后命中率不劣化、共享前缀集中度（tree vs affinity-only 对比）。

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"infer-router/internal/affinity"
	"infer-router/internal/app"
	"infer-router/internal/registry"
	"infer-router/test/mockengine"
)

// ---------------------------------------------------------------- helpers

func chatMsg(role, content string) string {
	return fmt.Sprintf(`{"role":%q,"content":%q}`, role, content)
}

// convTurn 构造多轮对话第 upto 轮的请求体：system + (user, assistant)* + user。
func convTurn(model, system string, userTurns []string, upto int) string {
	var parts []string
	if system != "" {
		parts = append(parts, chatMsg("system", system))
	}
	for i := 0; i < upto && i < len(userTurns); i++ {
		parts = append(parts, chatMsg("user", userTurns[i]))
		if i < upto-1 {
			parts = append(parts, chatMsg("assistant", "ack"))
		}
	}
	return fmt.Sprintf(`{"model":%q,"messages":[%s]}`, model, strings.Join(parts, ","))
}

// postRaw 发送原始 body（不带 X-Session-Id），返回状态码与响应体。
func postRaw(t *testing.T, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
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

// replicaOf 从 mock 响应体识别落点副本（"eng-x-ok"）。
func replicaOf(names []string, body string) string {
	for _, n := range names {
		if strings.Contains(body, n+"-ok") {
			return n
		}
	}
	return ""
}

// buildTreeRouter 起两台 mock 引擎并装配 prefix_affinity 路由（树配置可指定），
// 返回 (router URL, Runtime, 引擎列表)。
func buildTreeRouter(t *testing.T, treeCfg app.TreeCfg) (string, *app.Runtime, []*mockengine.Engine, []string) {
	t.Helper()
	engines := []*mockengine.Engine{
		mockengine.New(mockengine.Options{Name: "eng-a", Models: []string{"m-1"}}),
		mockengine.New(mockengine.Options{Name: "eng-b", Models: []string{"m-1"}}),
	}
	t.Cleanup(func() { engines[0].Close(); engines[1].Close() })

	cfg := &app.Config{
		Addr:              ":0",
		Strategy:          "prefix_affinity",
		HealthIntervalSec: 1,
		Breaker:           app.BreakerCfg{FailThreshold: 10, CooldownSec: 1},
		Affinity:          app.AffinityCfg{Alpha: 1.25, StickyWindow: 4096, Tree: treeCfg},
		Backends: []registry.Backend{
			{Name: "eng-a", BaseURL: engines[0].URL(), Model: "m-1", Weight: 1},
			{Name: "eng-b", BaseURL: engines[1].URL(), Model: "m-1", Weight: 1},
		},
	}
	rt, handler := app.Build(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt.Reg.ProbeAll(ctx)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, rt, engines, []string{"eng-a", "eng-b"}
}

// ---------------------------------------------------------------- 单测：哈希链

// TestMessageChainPrefixStability：多轮追加是严格前缀延伸；局部改写只作废所在块。
func TestMessageChainPrefixStability(t *testing.T) {
	sys := strings.Repeat("共享系统提示词。", 45)           // 315 rune → 2 块
	u1 := strings.Repeat("第一轮用户输入的内容。", 20)      // 220 rune → 1 块
	u2 := strings.Repeat("第二轮用户输入的内容更长一些。", 12) // 1 块

	turn1 := affinity.MessageChain([]byte(convTurn("m", sys, []string{u1}, 1)), 0)
	turn2 := affinity.MessageChain([]byte(convTurn("m", sys, []string{u1, u2}, 2)), 0)

	if len(turn1) == 0 || len(turn2) == 0 {
		t.Fatalf("empty chain: turn1=%d turn2=%d", len(turn1), len(turn2))
	}
	if len(turn2) <= len(turn1) {
		t.Fatalf("turn2 chain must extend turn1: %d vs %d", len(turn2), len(turn1))
	}
	for i := range turn1 {
		if turn1[i] != turn2[i] {
			t.Fatalf("chain diverged at seg %d: multi-turn append must keep prior segs", i)
		}
	}

	// 扰动 u1 末尾字符：system 块（前 2 段）不变，u1 所在块起变化
	u1b := u1[:len(u1)-3] + "改"
	turn1b := affinity.MessageChain([]byte(convTurn("m", sys, []string{u1b}, 1)), 0)
	if len(turn1b) != len(turn1) {
		t.Fatalf("perturbation must not change chain length: %d vs %d", len(turn1b), len(turn1))
	}
	if turn1b[0] != turn1[0] || turn1b[1] != turn1[1] {
		t.Fatal("chunks before the perturbed chunk must stay identical")
	}
	if turn1b[2] == turn1[2] {
		t.Fatal("chunk containing the perturbation must differ")
	}
}

// TestParseRequestChain：sid 快路径不建链；prefix 路径建链；非 chat 报错。
func TestParseRequestChain(t *testing.T) {
	req := func(sid, body string) *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "http://x/v1/chat/completions", strings.NewReader(body))
		if sid != "" {
			r.Header.Set("X-Session-Id", sid)
		}
		return r
	}
	key, model, chain, err := affinity.ParseRequestChain(req("s1", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`), []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`), 0)
	if err != nil || key != "sid:s1" || model != "m" || chain != nil {
		t.Fatalf("sid fast path: key=%q model=%q chain=%v err=%v", key, model, chain, err)
	}

	body := `{"model":"m","messages":[{"role":"system","content":"s"},{"role":"user","content":"hi"}]}`
	key, model, chain, err = affinity.ParseRequestChain(req("", body), []byte(body), 0)
	if err != nil || !strings.HasPrefix(key, "prefix:") || model != "m" || len(chain) == 0 {
		t.Fatalf("prefix path: key=%q model=%q chain=%v err=%v", key, model, chain, err)
	}

	_, _, chain, err = affinity.ParseRequestChain(req("", `{"input":"embed"}`), []byte(`{"input":"embed"}`), 0)
	if err == nil || chain != nil {
		t.Fatalf("non-chat body must error with nil chain: err=%v chain=%v", err, chain)
	}
}

// ---------------------------------------------------------------- 单测：树

func TestTreeUpdateLookup(t *testing.T) {
	tr := affinity.NewTree(100)
	tr.Update("m1", []string{"a", "b", "c"}, "eng-a")
	if d, hit := tr.Lookup("m1", []string{"a", "b", "c"}); d != 3 || hit != "eng-a" {
		t.Fatalf("full match: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m1", []string{"a", "b", "x"}); d != 2 || hit != "eng-a" {
		t.Fatalf("partial match: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m1", []string{"x"}); d != 0 || hit != "" {
		t.Fatalf("no match: d=%d hit=%q", d, hit)
	}

	// first-writer-wins：共享前缀段（a/b）的归属不被后来者覆盖；
	// 叶子段 z 归当前对话（z 由本次 Update 创建）——更长的链沿 z 继续匹配
	tr.Update("m1", []string{"a", "b", "z"}, "eng-b")
	if d, hit := tr.Lookup("m1", []string{"a", "b"}); d != 2 || hit != "eng-a" {
		t.Fatalf("shared prefix must keep first writer: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m1", []string{"a", "b", "z"}); d != 3 || hit != "eng-b" {
		t.Fatalf("leaf segment belongs to its conversation: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m1", []string{"a", "b", "x"}); d != 2 || hit != "eng-a" {
		t.Fatalf("partial match falls back to shared prefix: d=%d hit=%q", d, hit)
	}

	// 按模型分棵：互不串
	tr.Update("m2", []string{"a"}, "eng-b")
	if d, hit := tr.Lookup("m2", []string{"a"}); d != 1 || hit != "eng-b" {
		t.Fatalf("per-model tree: d=%d hit=%q", d, hit)
	}
	if _, hit := tr.Lookup("m3", []string{"a"}); hit != "" {
		t.Fatalf("unknown model tree must be empty: %q", hit)
	}
}

func TestTreeLRUEviction(t *testing.T) {
	tr := affinity.NewTree(5)
	tr.Update("m", []string{"a", "b"}, "e1") // 2 节点
	tr.Update("m", []string{"c", "d"}, "e2") // 2 节点（合计 4）
	if _, hit := tr.Lookup("m", []string{"a", "b"}); hit != "e1" {
		t.Fatal("precondition: touch a/b")
	}
	tr.Update("m", []string{"e", "f"}, "e3") // +2 → 6 > 5 → LRU 驱逐最旧的叶子 d

	if n := tr.Nodes(); n > 5 {
		t.Fatalf("tree size %d exceeds cap 5", n)
	}
	// d 被逐出，父节点 c 保留为端点：匹配优雅降级到深度 1
	if d, hit := tr.Lookup("m", []string{"c", "d"}); d != 1 || hit != "e2" {
		t.Fatalf("evicted leaf must degrade to shallower prefix: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m", []string{"a", "b"}); d != 2 || hit != "e1" {
		t.Fatalf("touched chain must survive: d=%d hit=%q", d, hit)
	}
	if d, hit := tr.Lookup("m", []string{"e", "f"}); d != 2 || hit != "e3" {
		t.Fatalf("newest chain must survive: d=%d hit=%q", d, hit)
	}
}

func TestTreeConcurrent(t *testing.T) {
	tr := affinity.NewTree(512)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				chain := []string{"k" + strconv.Itoa(g), strconv.Itoa(i % 50), "leaf"}
				tr.Update("m", chain, "eng-a")
				tr.Lookup("m", chain)
				if i%37 == 0 {
					tr.Evict()
				}
			}
		}(g)
	}
	wg.Wait()
	if n := tr.Nodes(); n > 512 {
		t.Fatalf("concurrent updates broke the cap: %d", n)
	}
}

// ---------------------------------------------------------------- 集成

// TestTreeMultiTurnStableBackend：无会话 ID 的多轮对话稳定同副本（树命中链路）。
func TestTreeMultiTurnStableBackend(t *testing.T) {
	url, rt, _, names := buildTreeRouter(t, app.TreeCfg{})

	sys := strings.Repeat("共享系统提示，", 40) // 280 rune → 2 块
	turns := []string{"第一轮：请记住数字 42", "第二轮：追问一", "第三轮：追问二", "第四轮：追问三"}

	first := ""
	for k := 1; k <= 4; k++ {
		code, body := postRaw(t, url, convTurn("m-1", sys, turns, k))
		if code != 200 {
			t.Fatalf("turn %d: code=%d body=%s", k, code, body)
		}
		name := replicaOf(names, body)
		if name == "" {
			t.Fatalf("turn %d: cannot identify replica: %s", k, body)
		}
		if first == "" {
			first = name
		} else if name != first {
			t.Fatalf("multi-turn drifted: turn1→%s, turn%d→%s", first, k, name)
		}
	}
	if got := rt.Picker.Tree.MatchRatio(); got < 0.5 {
		t.Fatalf("prefix match ratio = %.2f, want ≥ 0.5 (turn2+ must hit the tree)", got)
	}
	if n := rt.Picker.Tree.Nodes(); n == 0 {
		t.Fatal("tree must contain the conversation path")
	}
}

// TestTreeFreshSpread：全新会话（无共享前缀、无会话 ID）按派单键均匀摊开。
func TestTreeFreshSpread(t *testing.T) {
	url, _, _, names := buildTreeRouter(t, app.TreeCfg{})

	counts := map[string]int{}
	for i := 0; i < 30; i++ {
		sys := fmt.Sprintf("会话 %d 专属系统提示。", i)
		u := fmt.Sprintf("第 %d 个独立问题的正文。", i)
		code, body := postRaw(t, url, convTurn("m-1", sys, []string{u}, 1))
		if code != 200 {
			t.Fatalf("conv %d: code=%d", i, code)
		}
		name := replicaOf(names, body)
		if name == "" {
			t.Fatalf("conv %d: unknown replica: %s", i, body)
		}
		counts[name]++
	}
	for _, n := range names {
		if counts[n] < 6 { // 30 会话两副本，单侧 ≥20% 即视为摊开
			t.Fatalf("fresh conversations not spread: %v", counts)
		}
	}
}

// TestTreeSharedPrefixConcentration：共享长前缀负载下，
// tree 模式把同族会话集中到持有该前缀的副本（KV 复用），
// affinity-only 模式按会话键散开（前缀在各副本各存一份）。
// 本测试同时产出对比数据（写入 docs/design.md 的依据）。
func TestTreeSharedPrefixConcentration(t *testing.T) {
	sys := strings.Repeat("长系统提示词，所有会话共享这一段。", 20) // 320 rune → 2 块
	users := make([]string, 30)
	for i := range users {
		users[i] = fmt.Sprintf("独立用户问题 %d 的具体内容。", i)
	}

	run := func(treeCfg app.TreeCfg) map[string]int {
		url, _, _, names := buildTreeRouter(t, treeCfg)
		counts := map[string]int{}
		for i, u := range users {
			code, body := postRaw(t, url, convTurn("m-1", sys, []string{u}, 1))
			if code != 200 {
				t.Fatalf("conv %d: code=%d", i, code)
			}
			name := replicaOf(names, body)
			if name == "" {
				t.Fatalf("conv %d: unknown replica: %s", i, body)
			}
			counts[name]++
		}
		return counts
	}

	treeCounts := run(app.TreeCfg{})
	affinityOnlyCounts := run(app.TreeCfg{Enabled: boolPtr(false)})

	treeMax := 0
	for _, c := range treeCounts {
		if c > treeMax {
			treeMax = c
		}
	}
	affMin := 30
	for _, c := range affinityOnlyCounts {
		if c < affMin {
			affMin = c
		}
	}
	t.Logf("shared-prefix workload (30 convs, 2 replicas): tree=%v affinity-only=%v", treeCounts, affinityOnlyCounts)

	if treeMax < 27 { // 共享前缀 ≥90% 集中到单一副本
		t.Fatalf("tree mode failed to concentrate shared prefixes: %v", treeCounts)
	}
	if affMin < 5 { // affinity-only 明显散开（每副本 ≥1/6）
		t.Fatalf("affinity-only baseline unexpectedly concentrated: %v", affinityOnlyCounts)
	}
}

func boolPtr(b bool) *bool { return &b }

// TestTreeFullNoDegrade：树塞满后（持续 LRU 驱逐），热会话命中率与同副本稳定性不劣化。
func TestTreeFullNoDegrade(t *testing.T) {
	url, rt, _, names := buildTreeRouter(t, app.TreeCfg{MaxSize: 48})

	const hotN, fillN, rounds = 3, 5, 6
	hotTurns := make([][]string, hotN)
	hotBackend := make([]string, hotN)

	for r := 0; r < rounds; r++ {
		for h := 0; h < hotN; h++ {
			hotTurns[h] = append(hotTurns[h], fmt.Sprintf("热会话 %d 第 %d 轮的问题。", h, r))
			code, body := postRaw(t, url, convTurn("m-1", fmt.Sprintf("热会话 %d 的系统提示。", h), hotTurns[h], r+1))
			if code != 200 {
				t.Fatalf("hot %d round %d: code=%d", h, r, code)
			}
			name := replicaOf(names, body)
			if name == "" {
				t.Fatalf("hot %d round %d: unknown replica: %s", h, r, body)
			}
			if hotBackend[h] == "" {
				hotBackend[h] = name
			} else if name != hotBackend[h] {
				t.Fatalf("hot conv %d drifted under eviction pressure: %s → %s (round %d)", h, hotBackend[h], name, r)
			}
		}
		for f := 0; f < fillN; f++ {
			i := r*fillN + f
			body := convTurn("m-1", fmt.Sprintf("填充会话 %d 的系统提示。", i), []string{fmt.Sprintf("填充问题 %d。", i)}, 1)
			if code, _ := postRaw(t, url, body); code != 200 {
				t.Fatalf("filler %d: code=%d", i, code)
			}
		}
		if n := rt.Picker.Tree.Nodes(); n > 48 {
			t.Fatalf("tree cap breached: %d", n)
		}
	}

	if got := rt.Picker.Tree.MatchRatio(); got < 0.2 {
		t.Fatalf("match ratio under churn = %.2f, want ≥ 0.2 (hot convs must keep hitting)", got)
	}
}

// TestTreeDisabledFallback：树关闭（affinity-only）时行为与 M1 基线一致：
// 无会话 ID 的多轮对话仍按 prefix key 亲和（逐轮同副本）。
func TestTreeDisabledFallback(t *testing.T) {
	url, rt, _, names := buildTreeRouter(t, app.TreeCfg{Enabled: boolPtr(false)})
	if rt.Picker.TreeActive() {
		t.Fatal("tree must be inactive")
	}

	sys := strings.Repeat("关闭树的系统提示。", 30)
	turns := []string{"第一轮", "第二轮", "第三轮"}
	first := ""
	for k := 1; k <= 3; k++ {
		code, body := postRaw(t, url, convTurn("m-1", sys, turns, k))
		if code != 200 {
			t.Fatalf("turn %d: code=%d", k, code)
		}
		name := replicaOf(names, body)
		if first == "" {
			first = name
		} else if name != first {
			t.Fatalf("affinity-only multi-turn drifted: %s → %s", first, name)
		}
	}
}

// TestTreeSidFastPathUnaffected：带 X-Session-Id 的请求不经过树（快路径语义不变）。
func TestTreeSidFastPathUnaffected(t *testing.T) {
	url, rt, _, names := buildTreeRouter(t, app.TreeCfg{})

	sid := sessionKeyPrefers("m-1", "eng-b", []string{"eng-a", "eng-b"})
	for i := 0; i < 5; i++ {
		code, body := chatReq(t, url, sid, "m-1")
		if code != 200 || replicaOf(names, body) != "eng-b" {
			t.Fatalf("sid fast path: code=%d body=%s", code, body)
		}
	}
	if rt.Picker.Tree.Nodes() != 0 {
		t.Fatalf("sid requests must not touch the tree: %d nodes", rt.Picker.Tree.Nodes())
	}
}
