// 混合前缀树（对齐 SGLang cache_aware 的树语义）：
// 键为 messages 归一化分块哈希链（链段前缀稳定，局部改写只作废该块之后的段），
// 树按模型分棵，路径逐节点记录归属副本（first-writer-wins），
// 全局节点上限 + LRU 驱叶 + 向上剪枝；命中判定与 bounded load 叠加在 Picker 侧完成。

package affinity

import (
	"encoding/json"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"infer-router/internal/metrics"
)

// DefaultChunkChars 归一化分块的默认粒度（rune/块）。块粒度防单字符扰动：
// 局部改写只作废所在块及之后的链段，之前的前缀仍命中。
const DefaultChunkChars = 256

func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// normalizeText 折叠连续空白并去首尾：同义排版差异不影响前缀匹配。
func normalizeText(s string) string { return strings.Join(strings.Fields(s), " ") }

// MessageChain 把 chat 请求 messages 归一化后按消息分块生成哈希链段。
// 分块以消息为界（不跨消息拼接）：多轮追加只延伸链段，既有段保持不变，
// 因此"上一轮的完整链"是"本轮链"的严格前缀，树可沿既有路径深入匹配。
// 返回 nil 表示无法构建（无 messages / 非 chat JSON）。
func MessageChain(body []byte, chunkChars int) []string {
	var payload struct {
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &payload) != nil || len(payload.Messages) == 0 {
		return nil
	}
	texts := make([]string, 0, len(payload.Messages))
	for _, m := range payload.Messages {
		texts = append(texts, normalizeText(m.Role)+"\x00"+normalizeText(contentString(m.Content)))
	}
	return hashChain(texts, chunkChars)
}

// hashChain 逐消息按 chunkChars rune 分块，链式混合哈希：
// seg[i] = fmix64(fnv(chunk[i]) ^ seg[i-1])，链段末位即整条前缀的指纹。
func hashChain(messages []string, chunkChars int) []string {
	if chunkChars <= 0 {
		chunkChars = DefaultChunkChars
	}
	var total int
	for _, m := range messages {
		total += (len([]rune(m)) + chunkChars - 1) / chunkChars
	}
	if total == 0 {
		return nil
	}
	segs := make([]string, 0, total)
	var h uint64
	for _, m := range messages {
		runes := []rune(m)
		for i := 0; i < len(runes); i += chunkChars {
			end := i + chunkChars
			if end > len(runes) {
				end = len(runes)
			}
			h = fmix64(fnv64a(string(runes[i:end])) ^ h)
			segs = append(segs, strconv.FormatUint(h, 36))
		}
	}
	return segs
}

// DispatchKey 前缀派单键（确定性派单用）：链段末位即整条前缀的链式指纹。
// d>0 时按已匹配前缀派单——共享前缀的会话聚合到同一副本；
// d==0 时按全链派单——全新会话仍由 rendezvous 均匀摊开。
func DispatchKey(chain []string, d int) string {
	if len(chain) == 0 {
		return ""
	}
	if d > len(chain) {
		d = len(chain)
	}
	if d <= 0 {
		d = len(chain)
	}
	return "t:" + chain[d-1]
}

// ---------------------------------------------------------------- 前缀树

type treeNode struct {
	children map[string]*treeNode
	backend  string // first-writer-wins：最先服务该前缀的副本
	access   int64  // 最近访问时间（unix nano），LRU 依据
}

func (n *treeNode) leaf() bool { return len(n.children) == 0 }

// Tree 按模型分棵的混合前缀树。节点上限全局生效（跨模型求和），
// 超限即 LRU 驱逐最久未访问的叶子并向上剪枝空父节点。
type Tree struct {
	mu      sync.Mutex
	roots   map[string]*treeNode
	sizes   map[string]int
	total   int
	maxSize int
	clock   int64 // 逻辑时钟：LRU 排序不依赖墙钟（规避 OS 计时精度抖动）

	hits    int64
	lookups int64
}

func NewTree(maxSize int) *Tree {
	if maxSize <= 0 {
		maxSize = 65536
	}
	return &Tree{roots: map[string]*treeNode{}, sizes: map[string]int{}, maxSize: maxSize}
}

// now 递增并返回逻辑时钟值（调用方须持锁）。
func (t *Tree) now() int64 { t.clock++; return t.clock }

// Lookup 返回（最深已记录深度, 该深度的副本名），并顺路刷新途经节点访问时间。
// 深度按"最近一个记录了 backend 的节点"计——逐节点记录保证共享前缀的会话
// 即使自身从未落树，也能沿他人写入的路径命中。
func (t *Tree) Lookup(model string, chain []string) (int, string) {
	if len(chain) == 0 {
		return 0, ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lookups++
	if t.lookups > 40960 { // 统计窗口按指数衰减，避免历史占比长期主导
		t.lookups /= 2
		t.hits /= 2
	}
	root := t.roots[model]
	if root == nil {
		return 0, ""
	}
	now := t.now()
	cur := root
	d, hit := 0, ""
	for i, seg := range chain {
		c := cur.children[seg]
		if c == nil {
			break
		}
		c.access = now
		cur = c
		if cur.backend != "" {
			d, hit = i+1, cur.backend
		}
	}
	if d > 0 {
		t.hits++
	}
	return d, hit
}

// Update 写入整条链：每个节点记录 backend（已有记录保持不变），刷新访问时间；
// 写入后超过全局节点上限时立即执行 LRU 驱逐。
func (t *Tree) Update(model string, chain []string, backend string) {
	if len(chain) == 0 || backend == "" {
		return
	}
	t.mu.Lock()
	root := t.roots[model]
	if root == nil {
		root = &treeNode{children: map[string]*treeNode{}}
		t.roots[model] = root
	}
	now := t.now()
	added := 0
	cur := root
	for _, seg := range chain {
		c := cur.children[seg]
		if c == nil {
			c = &treeNode{children: map[string]*treeNode{}}
			cur.children[seg] = c
			added++
		}
		if c.backend == "" {
			c.backend = backend
		}
		c.access = now
		cur = c
	}
	if added > 0 {
		t.sizes[model] += added
		t.total += added
	}
	if t.total > t.maxSize {
		t.evictLocked()
	}
	metrics.PrefixTreeNodes.WithLabelValues(model).Set(float64(t.sizes[model]))
	t.mu.Unlock()
}

// Evict 强制执行一次 LRU 驱逐（周期任务入口），并把各模型节点数刷进指标。
func (t *Tree) Evict() {
	t.mu.Lock()
	t.evictLocked()
	for m, n := range t.sizes {
		metrics.PrefixTreeNodes.WithLabelValues(m).Set(float64(n))
	}
	t.mu.Unlock()
}

// StartEviction 启动周期驱逐循环（进程生命周期后台任务，interval ≤0 不启动）。
func (t *Tree) StartEviction(interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for range tick.C {
			t.Evict()
		}
	}()
}

// Nodes 返回树当前总节点数（跨模型）。
func (t *Tree) Nodes() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.total
}

// MatchRatio 近期窗口内的前缀命中率。仅统计走树的请求；
// X-Session-Id 快路径不经过树，不计入分母。
func (t *Tree) MatchRatio() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lookups == 0 {
		return 0
	}
	return float64(t.hits) / float64(t.lookups)
}

// evictLocked 全局 LRU：收集全部叶子按访问时间升序删除（含向上剪枝），
// 直到 total ≤ maxSize。调用方须持锁。
func (t *Tree) evictLocked() {
	if t.total <= t.maxSize {
		return
	}
	type item struct {
		model string
		path  []string
		acc   int64
	}
	leaves := make([]item, 0, 64)
	var dfs func(model string, n *treeNode, path []string)
	dfs = func(model string, n *treeNode, path []string) {
		for seg, c := range n.children {
			np := append(path[:len(path):len(path)], seg) // 三索引切片强制拷贝
			if c.leaf() {
				leaves = append(leaves, item{model, np, c.access})
			} else {
				dfs(model, c, np)
			}
		}
	}
	for m, r := range t.roots {
		dfs(m, r, nil)
	}
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].acc < leaves[j].acc })

	for _, it := range leaves {
		if t.total <= t.maxSize {
			break
		}
		t.removeLocked(it.model, it.path)
	}
}

// removeLocked 删除指定路径的叶子节点本身（不做向上级联剪枝）。
// 父节点若因此变为叶子，保留为合法端点（带 backend 记录）——
// 后续匹配自然退化到更浅前缀，且它参与后续 LRU 竞争。调用方须持锁。
func (t *Tree) removeLocked(model string, path []string) {
	root := t.roots[model]
	if root == nil {
		return
	}
	parent := root
	for _, seg := range path[:len(path)-1] {
		c := parent.children[seg]
		if c == nil {
			return // 已被并发删除
		}
		parent = c
	}
	leafNode := parent.children[path[len(path)-1]]
	if leafNode == nil || !leafNode.leaf() {
		return // 已被并发删除 / 已长出子节点
	}
	delete(parent.children, path[len(path)-1])
	t.total--
	t.sizes[model]--
	if t.sizes[model] <= 0 {
		delete(t.sizes, model)
	}
}
