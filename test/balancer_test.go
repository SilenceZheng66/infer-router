package test

import (
	"testing"
	"time"

	"infer-router/internal/balancer"
	"infer-router/internal/registry"
)

func makeStates(names []string) []*registry.State {
	var out []*registry.State
	for _, n := range names {
		out = append(out, registry.NewState(
			registry.Backend{Name: n, BaseURL: "http://" + n, Model: "m"},
			3, 15*time.Second,
		))
	}
	return out
}

func TestRoundRobinCycles(t *testing.T) {
	rr := &balancer.RoundRobin{}
	states := makeStates([]string{"a", "b", "c"})
	var got []string
	for i := 0; i < 6; i++ {
		got = append(got, rr.Pick(states).Name)
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick[%d]=%s want %s", i, got[i], want[i])
		}
	}
}

func TestRoundRobinEmpty(t *testing.T) {
	if (&balancer.RoundRobin{}).Pick(nil) != nil {
		t.Fatal("empty candidates should return nil")
	}
}

func TestLeastQueuePicksMinInFlight(t *testing.T) {
	lq := &balancer.LeastQueue{}
	states := makeStates([]string{"a", "b", "c"})
	states[0].Inc()
	states[0].Inc()
	states[2].Inc()
	if got := lq.Pick(states).Name; got != "b" {
		t.Fatalf("want b got %s", got)
	}
}

func TestBreakerOpensAndRecovers(t *testing.T) {
	s := registry.NewState(registry.Backend{Name: "x"}, 2, 50*time.Millisecond)
	s.RecordFailure()
	if !s.Healthy() {
		t.Fatal("1 failure below threshold should stay healthy")
	}
	s.RecordFailure()
	if s.Healthy() {
		t.Fatal("threshold reached should open breaker")
	}
	time.Sleep(60 * time.Millisecond)
	if !s.Healthy() {
		t.Fatal("cooldown elapsed should allow half-open probe")
	}
	s.RecordSuccess()
	if !s.Healthy() {
		t.Fatal("success should close breaker")
	}
}
