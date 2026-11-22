// Package affinity 实现 prefix/session 亲和路由的核心原语。
//
// 设计立场（与 SGLang/vLLM Router 的差异见 README）：
//   - 路由器完全无状态：key→副本由确定性哈希决定，不维护 radix tree / 命中表，
//     因此路由器自身可水平扩展，不存在"多网关副本命中表不同步导致命中率下降"的问题；
//   - 亲和与均衡的冲突由 bounded load 解决（Google "Consistent Hashing with
//     Bounded Loads" 的 rendezvous 变体）：亲和目标超过负载上限时顺延到次优副本，
//     超限事件计数暴露，行为可观测可调参。
package affinity

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sort"
)

// hashPair 返回 key 与 node 组合的 64 位得分（rendezvous 核心函数）。
// 得分最高的 node 胜出；node 集合不变时结果确定，node 增减只迁移 1/n 左右的 key。
func hashPair(key, node string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})          // 分隔符，避免 "ab"+"c" 与 "a"+"bc" 撞车
	_, _ = h.Write([]byte(node))
	// FNV-1a 对"仅末尾单字节不同"的输入雪崩性弱（会系统性偏斜节点胜出率），
	// 追加 murmur3 fmix64 终态混合，把输出打回均匀分布。
	return fmix64(h.Sum64())
}

// fmix64 是 murmur3 的 64 位终态混合器（finalizer），实现强雪崩。
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// SortNodes 按 key 的 rendezvous 得分降序返回 node 名单。
// 得分并列时按 node 名字典序稳定排序，保证无状态多实例路由器给出一致结果。
func SortNodes(key string, nodes []string) []string {
	type pair struct {
		name  string
		score uint64
	}
	ps := make([]pair, 0, len(nodes))
	for _, n := range nodes {
		ps = append(ps, pair{n, hashPair(key, n)})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].score != ps[j].score {
			return ps[i].score > ps[j].score
		}
		return ps[i].name < ps[j].name
	})
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.name)
	}
	return out
}

// WeightedSort 加权 rendezvous（Thaler 形式）：u_i = hash/2^64 ∈ (0,1)，
// score_i = u_i^(1/w_i)，比较 ln(score_i) = ln(u_i)/w_i——权重越大 rank 越接近 0，越容易胜出。
// 等权（w=1）时 rank 与 ln(u) 单调等价，结果与 SortNodes 完全一致。
func WeightedSort(key string, nodes []string, weights map[string]int) []string {
	type pair struct {
		name string
		rank float64
	}
	const twoPow64 = 18446744073709551616.0 // 2^64
	ps := make([]pair, 0, len(nodes))
	for _, n := range nodes {
		w := weights[n]
		if w < 1 {
			w = 1
		}
		h := hashPair(key, n)
		if h == 0 {
			h = 1 // 2^-64 概率边界，防御性处理（ln(0) 未定义）
		}
		u := float64(h) / twoPow64
		ps = append(ps, pair{n, math.Log(u) / float64(w)})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].rank != ps[j].rank {
			return ps[i].rank > ps[j].rank
		}
		return ps[i].name < ps[j].name
	})
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.name)
	}
	return out
}

// SessionID 返回请求的会话级稳定 key 的 64 位指纹。
// candidates 的 key 由外部从（X-Session-Id 或 messages 前缀）派生，本包只负责哈希编排。
func SessionID(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}
