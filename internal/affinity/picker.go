package affinity

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math"
	"net/http"
	"strings"
	"sync"

	"infer-router/internal/metrics"
	"infer-router/internal/registry"
)

// ParseRequest 从请求中提取 (会话 key, 请求 model)。
//
// key 优先级：
//  1. X-Session-Id header（调用方显式声明，最可靠）；
//  2. messages 的"对话前缀"：system 全文 + 首条 user 消息。多轮对话中该前缀逐轮不变，
//     因此同一对话的所有请求稳定命中同一副本（KV cache 前缀复用的前提）。
//     注意：这与"全 messages hash"不同——全量 hash 每轮都变，没有任何亲和性。
//
// model 为请求体中的 model 字段（可能为空），用于按模型筛候选池。
// body 必须由调用方预先读出（proxy 层需要重放 body），本函数不再消费 req.Body。
func ParseRequest(req *http.Request, body []byte) (key, model string, err error) {
	key, model, _, err = ParseRequestChain(req, body, 0)
	return key, model, err
}

// ParseRequestChain 在 ParseRequest 基础上额外返回 messages 归一化分块哈希链
// （混合前缀树的路由键，M3）。X-Session-Id 快路径不构建链——树不参与快路径；
// 无 user 消息或非 chat JSON 时链为 nil，走兜底派单。
func ParseRequestChain(req *http.Request, body []byte, chunkChars int) (key, model string, chain []string, err error) {
	if sid := strings.TrimSpace(req.Header.Get("X-Session-Id")); sid != "" {
		return "sid:" + sid, parseModel(body), nil, nil
	}
	if len(body) == 0 {
		return "", "", nil, errors.New("no session id and empty body")
	}
	var payload struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"` // string 或分片数组，先取字符串路径
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", nil, errors.New("body is not openai chat json")
	}
	var system, firstUser string
	for _, m := range payload.Messages {
		c := contentString(m.Content)
		switch m.Role {
		case "system":
			if system == "" {
				system = c
			}
		case "user":
			if firstUser == "" {
				firstUser = c
			}
		}
		if system != "" && firstUser != "" {
			break
		}
	}
	if firstUser == "" {
		return "", payload.Model, nil, errors.New("no user message to key on")
	}
	texts := make([]string, 0, len(payload.Messages))
	for _, m := range payload.Messages {
		texts = append(texts, normalizeText(m.Role)+"\x00"+normalizeText(contentString(m.Content)))
	}
	chain = hashChain(texts, chunkChars)
	return "prefix:" + shortHash(system+"\x00"+firstUser), payload.Model, chain, nil
}

// SessionKey 仅返回会话 key（兼容既有调用）。
func SessionKey(req *http.Request, body []byte) (string, error) {
	key, _, err := ParseRequest(req, body)
	return key, err
}

// parseModel 从请求体提取 model 字段，失败返回空串。
func parseModel(body []byte) string {
	var payload struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return payload.Model
}

// contentString 处理 content 的两种形态：字符串或 OpenAI 分片数组。
func contentString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		var sb strings.Builder
		for _, part := range t {
			if m, ok := part.(map[string]any); ok {
				if s, ok := m["text"].(string); ok {
					sb.WriteString(s)
				}
			}
		}
		return sb.String()
	default:
		return ""
	}
}

// EstimatePromptTokens 以"字符数/4"粗估 prompt token 量（PD inline/分离路径决策用）。
// 与真实 tokenizer 有 ±30% 级误差，足以判定长/短请求；非 chat 结构退化为 body 规模粗估。
func EstimatePromptTokens(body []byte) int {
	var payload struct {
		Messages []struct {
			Content any `json:"content"`
		} `json:"messages"`
		Prompt string `json:"prompt"`
	}
	chars := 0
	if json.Unmarshal(body, &payload) == nil {
		for _, m := range payload.Messages {
			chars += len(contentString(m.Content))
		}
		chars += len(payload.Prompt)
	}
	if chars == 0 {
		chars = len(body) / 3 // 兜底：非标准结构按 body 规模粗估
	}
	return (chars + 3) / 4
}

func shortHash(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// Picker = 加权 rendezvous 排序 + bounded load 兜底 + 混合前缀树（可选）+ KV 容量感知（可选）。
type Picker struct {
	Alpha float64 // 负载上限系数：cap = ceil(alpha × 平均在途)。1.25 为论文推荐区间

	Tree          *Tree   // 混合前缀树；nil = affinity-only（不启用树）
	TreeThreshold float64 // 树命中判定比例：匹配深度/链长 ≥ 阈值才沿用树记录副本

	KVSteering  bool    // KV 高水位分流开关（AttachKV 启用）
	KVWatermark float64 // KV 高水位阈值：超过则 top-k 内优先派给余量大者
	KVTopK      int     // 分流决策窗口：rendezvous 前 k 个候选内比较
	KVCapScale  bool    // 负载上限按 KV 余量缩放（可选，默认关）

	track *tracker
}

func NewPicker(alpha float64, stickyWindow int) *Picker {
	if alpha <= 1.0 {
		alpha = 1.25
	}
	return &Picker{Alpha: alpha, track: newTracker(stickyWindow)}
}

// AttachTree 启用混合前缀树（prefix_affinity 策略缺省开启）。
func (p *Picker) AttachTree(tree *Tree, cacheThreshold float64) {
	if cacheThreshold <= 0 || cacheThreshold >= 1 {
		cacheThreshold = 0.5
	}
	p.Tree = tree
	p.TreeThreshold = cacheThreshold
}

// AttachKV 启用 KV 容量感知分流（M5）：rendezvous top-k 内，首选副本 KV 超过高水位时
// 改派余量最大的未过载候选；cap_scale 开启时负载上限按 KV 余量缩放。
func (p *Picker) AttachKV(highWatermark float64, topK int, capScale bool) {
	if highWatermark <= 0 || highWatermark >= 1 {
		highWatermark = 0.9
	}
	if topK < 2 {
		topK = 2
	}
	p.KVSteering = true
	p.KVWatermark = highWatermark
	p.KVTopK = topK
	p.KVCapScale = capScale
}

// TreeActive 报告混合前缀树是否启用。
func (p *Picker) TreeActive() bool { return p.Tree != nil }

// Rank 返回 key 对候选副本的加权 rendezvous 完整排序（不做 bounded load）。
// 供请求级 failover 取"主选之后的次优序列"。
func (p *Picker) Rank(key string, candidates []*registry.State) []*registry.State {
	if len(candidates) == 0 {
		return nil
	}
	nodes := make([]string, len(candidates))
	byName := make(map[string]*registry.State, len(candidates))
	weights := make(map[string]int, len(candidates))
	for i, s := range candidates {
		nodes[i] = s.Name
		byName[s.Name] = s
		w := s.Weight
		if w < 1 {
			w = 1
		}
		weights[s.Name] = w
	}
	out := make([]*registry.State, 0, len(candidates))
	for _, name := range WeightedSort(key, nodes, weights) {
		out = append(out, byName[name])
	}
	return out
}

// capFor 计算 name 在当前候选集下的负载上限：
// cap_i = ceil(α × Σinflight × w_i / Σw)，等权时退化为 α × 平均在途；
// cap_scale 开启时再按 KV 余量缩放：cap_i × (1 − kv_util_i)。
func (p *Picker) capFor(name string, candidates []*registry.State) int64 {
	var total, sumW, w int64
	w = 1
	var st *registry.State
	for _, s := range candidates {
		total += s.InFlight()
		weight := s.Weight
		if weight < 1 {
			weight = 1
		}
		sumW += int64(weight)
		if s.Name == name {
			w = int64(weight)
			st = s
		}
	}
	if sumW <= 0 {
		sumW = 1
	}
	cap := int64(math.Ceil(p.Alpha * float64(total) * float64(w) / float64(sumW)))
	if p.KVCapScale && st != nil {
		if u := st.KVUtil(); u > 0 {
			cap = int64(math.Ceil(float64(cap) * (1 - u)))
		}
	}
	if cap < 1 { // 空闲集群边界：avg=0 时 cap=0 会让所有副本"过载"，退化为固定首节点
		cap = 1
	}
	return cap
}

func (p *Picker) underCap(s *registry.State, candidates []*registry.State) bool {
	return s.InFlight() < p.capFor(s.Name, candidates)
}

// pickRanked 按 key 的加权 rendezvous 次序取首个未过载副本；
// 全员过载（极少见）时退化为全局最小在途，保证不饿死。
func (p *Picker) pickRanked(key string, candidates []*registry.State) *registry.State {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	nodes := make([]string, len(candidates))
	byName := make(map[string]*registry.State, len(candidates))
	weights := make(map[string]int, len(candidates))
	for i, s := range candidates {
		nodes[i] = s.Name
		byName[s.Name] = s
		w := s.Weight
		if w < 1 {
			w = 1
		}
		weights[s.Name] = w
	}

	var chosen *registry.State
	ordered := WeightedSort(key, nodes, weights)
	for _, name := range ordered {
		s := byName[name]
		if s.InFlight() < p.capFor(name, candidates) {
			chosen = s
			break
		}
	}
	if chosen == nil { // 全员达到上限：选全局最小在途
		chosen = candidates[0]
		for _, s := range candidates[1:] {
			if s.InFlight() < chosen.InFlight() {
				chosen = s
			}
		}
	} else if chosen.Name != ordered[0] {
		metrics.AffinityOverloadFallback.Inc()
	} else if p.KVSteering && byName[ordered[0]].KVUtil() > p.KVWatermark {
		// KV 容量感知（M5）：首选副本超 KV 高水位时，在 top-k 窗口内
		// 改派 KV 余量最大的未过载候选（高水位副本粘性收益已被 LRU 驱逐吃掉）
		k := p.KVTopK
		if k > len(ordered) {
			k = len(ordered)
		}
		best, bestUtil := ordered[0], byName[ordered[0]].KVUtil()
		for _, name := range ordered[1:k] {
			s := byName[name]
			u := s.KVUtil()
			if u < bestUtil && s.InFlight() < p.capFor(name, candidates) {
				best, bestUtil = name, u
			}
		}
		if best != ordered[0] {
			chosen = byName[best]
			metrics.KVSteered.Inc()
		}
	}
	return chosen
}

// Pick 选出 key 的亲和副本：加权 rendezvous 主选，过载时顺延。
// 负载上限按权重分摊（见 capFor）。
func (p *Picker) Pick(key string, candidates []*registry.State) *registry.State {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		p.track.observe(key, candidates[0].Name)
		metrics.AffinityAssign.WithLabelValues(candidates[0].Name).Inc()
		return candidates[0]
	}
	st := p.pickRanked(key, candidates)
	if st == nil {
		return nil
	}
	p.track.observe(key, st.Name)
	metrics.AffinityAssign.WithLabelValues(st.Name).Inc()
	return st
}

// PickHybrid 混合前缀树主路径：sid/prefix key 仍按普通亲和派单；
// 树启用且链可构建时——前缀命中（比例 ≥ 阈值）沿用树记录副本（叠加 bounded load，
// 命中目标过载或不在池则转入派单）；未命中/低于阈值按"已匹配前缀"确定性派单
// （共享前缀的会话聚合，全新会话按全链摊开），并把本次路径写回树。
// key 为调用方合成的复合会话键（粘性统计用）；chain 为 messages 分块哈希链。
func (p *Picker) PickHybrid(model, key string, chain []string, candidates []*registry.State) *registry.State {
	if p.Tree == nil || len(chain) == 0 {
		return p.Pick(key, candidates)
	}
	d, hit := p.Tree.Lookup(model, chain)
	ratio := float64(d) / float64(len(chain))
	if hit != "" && ratio >= p.TreeThreshold {
		for _, s := range candidates {
			if s.Name == hit {
				if p.KVSteering && s.KVUtil() > p.KVWatermark {
					// 命中副本 KV 高水位：继续沿用只会加剧 LRU 驱逐，转入派单分流
					break
				}
				if p.underCap(s, candidates) { // bounded load 叠加：命中副本不超限才沿用
					p.track.observe(key, s.Name)
					metrics.AffinityAssign.WithLabelValues(s.Name).Inc()
					return s
				}
				metrics.AffinityOverloadFallback.Inc()
				break
			}
		}
		// 命中副本已不在健康池 / 过载 / KV 高水位 → 走派单
	}
	st := p.pickRanked(DispatchKey(chain, d), candidates)
	if st == nil {
		return nil
	}
	p.track.observe(key, st.Name)
	metrics.AffinityAssign.WithLabelValues(st.Name).Inc()
	p.Tree.Update(model, chain, st.Name)
	return st
}

// StickyRatio 返回最近窗口内"同 key 同副本"的比例（亲和有效性的自证指标）。
func (p *Picker) StickyRatio() float64 { return p.track.stickyRatio() }

// ---------------------------------------------------------------- tracker

// tracker 用固定容量环形窗口记录最近 W 个 (key, backend) 决策，
// 以"该 key 上一次被路由到的副本"是否与本次一致来度量粘性。
type tracker struct {
	mu     sync.Mutex
	window map[string]string // key -> last backend
	keys   []string          // 环形淘汰队列
	cap    int
	hits   int64
	total  int64
}

func newTracker(window int) *tracker {
	if window <= 0 {
		window = 4096
	}
	return &tracker{window: make(map[string]string, window), cap: window}
}

func (t *tracker) observe(key, backend string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 首见 key 不计入分母：粘性率衡量"已有会话的保持率"，首见无粘/漂可言，
	// 否则指标被 (轮数-1)/轮数 系统性稀释，失去可观测意义。
	if last, ok := t.window[key]; ok {
		t.total++
		if last == backend {
			t.hits++
		} else {
			t.window[key] = backend
		}
	} else {
		t.window[key] = backend
		t.keys = append(t.keys, key)
		if len(t.keys) > t.cap {
			evict := t.keys[0]
			t.keys = t.keys[1:]
			delete(t.window, evict)
		}
	}
	if t.total > 10*int64(t.cap) { // 窗口滑动：统计量按指数衰减重置
		t.hits /= 2
		t.total /= 2
	}
}

func (t *tracker) stickyRatio() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.total == 0 {
		return 1.0
	}
	return float64(t.hits) / float64(t.total)
}
