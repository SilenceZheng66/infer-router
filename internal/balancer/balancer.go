package balancer

import (
	"sync/atomic"

	"infer-router/internal/registry"
)

// Balancer 从健康副本中选择一个目标。
type Balancer interface {
	Pick(candidates []*registry.State) *registry.State
}

// RoundRobin 均匀轮转。
type RoundRobin struct{ i atomic.Uint64 }

func (rr *RoundRobin) Pick(c []*registry.State) *registry.State {
	if len(c) == 0 {
		return nil
	}
	idx := (rr.i.Add(1) - 1) % uint64(len(c))
	return c[idx]
}

// LeastQueue 在途最少优先，并列时 RR 决胜。
type LeastQueue struct{ rr RoundRobin }

func (lq *LeastQueue) Pick(c []*registry.State) *registry.State {
	if len(c) == 0 {
		return nil
	}
	min := c[0].InFlight()
	for _, s := range c[1:] {
		if v := s.InFlight(); v < min {
			min = v
		}
	}
	var tied []*registry.State
	for _, s := range c {
		if s.InFlight() == min {
			tied = append(tied, s)
		}
	}
	return lq.rr.Pick(tied)
}
