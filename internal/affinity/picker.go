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
	if sid := strings.TrimSpace(req.Header.Get("X-Session-Id")); sid != "" {
		return "sid:" + sid, parseModel(body), nil
	}
	if len(body) == 0 {
		return "", "", errors.New("no session id and empty body")
	}
	var payload struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"` // string 或分片数组，先取字符串路径
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", errors.New("body is not openai chat json")
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
		return "", payload.Model, errors.New("no user message to key on")
	}
	return "prefix:" + shortHash(system+"\x00"+firstUser), payload.Model, nil
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

func shortHash(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// Picker = rendezvous 排序 + bounded load 兜底。
type Picker struct {
	Alpha float64 // 负载上限系数：cap = ceil(alpha × 平均在途)。1.25 为论文推荐区间
	track *tracker
}

func NewPicker(alpha float64, stickyWindow int) *Picker {
	if alpha <= 1.0 {
		alpha = 1.25
	}
	return &Picker{Alpha: alpha, track: newTracker(stickyWindow)}
}

// Pick 选出 key 的亲和副本；亲和目标过载时按加权 rendezvous 次序顺延到首个未过载副本。
// 全员过载（极少见）时退化为全局最小在途，保证不饿死。
// 负载上限按权重分摊：cap_i = ceil(α × Σinflight × w_i / Σw)，等权时退化为 α × 平均在途。
func (p *Picker) Pick(key string, candidates []*registry.State) *registry.State {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		p.track.observe(key, candidates[0].Name)
		metrics.AffinityAssign.WithLabelValues(candidates[0].Name).Inc()
		return candidates[0]
	}

	nodes := make([]string, len(candidates))
	byName := make(map[string]*registry.State, len(candidates))
	weights := make(map[string]int, len(candidates))
	var total int64
	var sumW int64
	for i, s := range candidates {
		nodes[i] = s.Name
		byName[s.Name] = s
		w := s.Weight
		if w < 1 {
			w = 1
		}
		weights[s.Name] = w
		sumW += int64(w)
		total += s.InFlight()
	}

	ordered := WeightedSort(key, nodes, weights)

	var chosen *registry.State
	for _, name := range ordered {
		s := byName[name]
		cap := int64(math.Ceil(p.Alpha * float64(total) * float64(weights[name]) / float64(sumW)))
		if cap < 1 { // 空闲集群边界：avg=0 时 cap=0 会让所有副本"过载"，退化为固定首节点
			cap = 1
		}
		if s.InFlight() < cap {
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
	}

	p.track.observe(key, chosen.Name)
	metrics.AffinityAssign.WithLabelValues(chosen.Name).Inc()
	return chosen
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
