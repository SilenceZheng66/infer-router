# infer-router

A **engine-neutral, state-aware routing layer** (Go, single static binary, zero external dependencies) that sits in front of N vLLM / SGLang replicas.

The sibling project [`llm-inference-lab`](https://github.com/SilenceZheng66/llm-inference-lab) is a full-stack reference implementation of an inference service; this repo extracts its "replica routing layer" into a standalone component that can directly replace a bare round-robin LB or a heavyweight gateway.

## Why it exists

vLLM Prefix Cache is **local to each replica**: when consecutive turns of the same session are round-robined across replicas, every replica re-prefills the same KV prefix — wasted compute and higher TTFT. Heavyweight gateways do everything but are heavy to deploy; a bare LB is completely unaware of this structure.

infer-router does one thing: **route OpenAI-compatible requests (including SSE streaming) to the right replica with session affinity**, so the engine's Prefix Cache actually gets reused.

## Features

- **prefix_affinity strategy** (core):
  - Stateless **rendezvous hashing** (murmur3 fmix64 finalizer + FNV pre-mix) — adding/removing a replica migrates only `1/n` of sessions
  - **Bounded load** (α=1.25): when the preferred replica's in-flight count exceeds `ceil(α × avg in-flight)`, overflow goes to the next-best replica — stickiness and balance at the same time
  - **Sticky tracker**: sliding-window ratio of repeated session requests landing on the same replica, exported as `router_affinity_sticky_ratio`
  - Session key: `X-Session-Id` header first, else the messages prefix (system + first user turn) — stable across turns
- **Strategies**: `round_robin | least_queue | prefix_affinity`
- **Health probing + breaker**: periodic probes evict dead replicas; request-level fail_threshold breaker with cooldown half-open probing
- **Observability**: `/metrics` (Prometheus: per-backend requests, sticky ratio, overflow fallbacks), `/statusz` (health snapshot)
- **SSE pass-through**: `FlushInterval=-1` streams chunks through without buffering

## Benchmark (2× vLLM Qwen2.5-1.5B replicas, A10 24GB)

Incremental-conversation load (40 sessions × 6 turns; each turn appends a ~250-token "document chunk" as user message and folds the assistant reply back into messages — the history prefix grows every turn; 0-0.3s inter-turn jitter; 8 concurrent):

| Strategy | Prefix Cache hit rate | TTFT avg | TTFT p90 |
|---|---|---|---|
| **prefix_affinity** | **67.3%** | 0.461s | **0.531s** |
| round_robin | 62.5% | 0.470s | 0.568s |

- Sticky ratio 0.965 (synced load), load split 122/118 (49%/47%)
- Bounded-load overflow fallbacks: 22-53 (expected behavior of α=1.25)
- **Failover drill**: kill one vLLM → health probe evicts it → 30/30 requests succeed on the surviving replica, zero failures

> Caveat: hit rates are delta ratios of replica `vllm:prefix_cache_{queries,hits}_total` counters; the ~67% absolute value is capped by per-replica KV-capacity LRU eviction (~100k tokens/replica) — the routing benefit stacks on top of that constraint. The full experiment log (including three rounds of invalid controls and how they were fixed) is in `docs/design.md`.

## Quick start

```bash
# Build (static binary, no CGO)
CGO_ENABLED=0 go build -o infer-router ./cmd/router

# Configure and run (replica list in config/router.yaml)
./infer-router -config config/router.yaml

# Status & metrics
curl localhost:8080/statusz
curl localhost:8080/metrics

# Request through the router (identical to hitting vLLM directly)
curl localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-1.5b","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Minimal config:

```yaml
addr: ":8080"
strategy: prefix_affinity   # round_robin | least_queue | prefix_affinity
health_interval_s: 5
breaker: { fail_threshold: 3, cooldown_s: 15 }
affinity: { alpha: 1.25, sticky_window: 4096 }
backends:
  - { name: vllm-a, base_url: "http://127.0.0.1:8001", model: qwen2.5-1.5b, weight: 1 }
  - { name: vllm-b, base_url: "http://127.0.0.1:8002", model: qwen2.5-1.5b, weight: 1 }
```

## Load-test tools

```bash
# Incremental-conversation load test (streaming TTFT + sticky session traffic)
python tools/load_test.py http://127.0.0.1:8080 affinity 40 6 8 s1 0.3

# Delta hit-rate from replica prefix-cache counters
# (append 4 baseline lines + 4 final lines into one file, then)
python tools/compare_cache.py cb_file.txt affinity
```

## Architecture

```
client ──► :8080 mux
             ├── /statusz  health/strategy snapshot
             ├── /metrics  Prometheus (requests, sticky_ratio, overload_fallback)
             └── /v1/*
                  ├─ extract session key (X-Session-Id or messages prefix)
                  ├─ picker.Pick(key, healthy): rendezvous ranking + bounded-load overflow
                  └─ ReverseProxy (FlushInterval=-1, body replay) ──► vLLM/SGLang replica
```

## Design docs

- [`docs/design.md`](docs/design.md): rendezvous + bounded-load math, sticky tracker, breaker/failover, and the experiment methodology behind the "three invalid controls"
- [`docs/usage.md`](docs/usage.md): config reference, deployment (bare metal / Docker), benchmark & failover drill reproduction

## Tests

```bash
go test ./...        # rendezvous consistency, bounded-load cap, sticky stats, RR/least_queue, SSE pass-through
```

> Honest scope: a learning/portfolio project; performance numbers come only from free-tier GPU experiments, no production SLA claims. MIT License.
