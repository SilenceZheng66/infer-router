# infer-router

A **engine-neutral, state-aware routing layer** (Go, single static binary, zero external dependencies) that sits in front of N vLLM / SGLang replicas. Works alongside [`llm-inference-lab`](https://github.com/SilenceZheng66/llm-inference-lab) (a full-stack reference implementation) or standalone, replacing a bare round-robin LB or the routing tier of a heavyweight gateway.

## Problem

vLLM Prefix Cache is **local to each replica**: when consecutive turns of the same session are round-robined across replicas, every replica re-prefills the same KV prefix — wasted compute and higher TTFT. Heavyweight gateways do everything but are heavy to deploy; a bare LB is completely unaware of this structure.

infer-router does one thing: **route OpenAI-compatible requests (including SSE streaming) to the right replica with session affinity**, so the engine's Prefix Cache actually gets reused.

## Features

- **prefix_affinity strategy** (core):
  - Stateless **rendezvous hashing** (murmur3 fmix64 finalizer + FNV pre-mix) — adding/removing a replica migrates only `1/n` of sessions
  - **Weighted rendezvous**: `score_i = u_i^(1/w_i)` (u ∈ (0,1) uniform hash) — replicas take traffic proportional to `weight`; the load cap is sharded by weight, `cap_i = ceil(α × Σinflight × w_i / Σw)`
  - **Model-aware routing**: candidate pool filtered by the request's `model` (backend.model accepts comma-separated lists and `*` wildcards); the affinity key is model-scoped, so switching models mid-session never crosses pools
  - **Bounded load** (α=1.25): when the preferred replica's in-flight count exceeds its cap, overflow goes to the next-best replica — stickiness and balance at the same time
  - **Sticky tracker**: sliding-window ratio of repeated session requests landing on the same replica, exported as `router_affinity_sticky_ratio`
  - **Hybrid prefix tree**: session-less requests are approximately matched on per-model radix trees keyed by the normalized, chunked hash chain of `messages`; on a hit (match ratio ≥ cache_threshold) the tree-recorded replica is reused with bounded load still applied, on a miss dispatch is deterministic over the matched prefix. A global node cap (`max_tree_size`) plus periodic LRU eviction bound memory. Conversations sharing a long prefix (e.g. the same system prompt) concentrate on the replica holding that prefix for better KV reuse — mock run with 30 shared-system conversations: tree mode 30/30 on one replica vs 16/14 spread in affinity-only mode
  - **KV-capacity-aware steering**: probes also scrape each replica's `/metrics` for KV usage (`router_backend_kv_util{backend}`, metric name configurable); when the top rendezvous candidate exceeds `kv.high_watermark` (default 0.9) within the top-k window, the request goes to the candidate with the most KV headroom (tree hits included), avoiding requests piling onto a replica that is actively evicting. Optional `kv.cap_scale` shrinks load caps by KV headroom (off by default). Below the watermark behavior is identical to the baseline
  - Three-stage session resolution: `X-Session-Id` fast path → prefix-tree approximate match → deterministic prefix dispatch (session key still prefers the `X-Session-Id` header, else the messages prefix — stable across turns)
- **Strategies**: `round_robin | least_queue | prefix_affinity`
- **Health probing + breaker**: periodic probes evict dead replicas; request-level fail_threshold breaker with cooldown half-open probing; **error classification** — 5xx/connection errors count as breaker failures, 4xx do not (404+model lands in `router_model_mismatch_total`, surfacing serving-state drift between replica declaration and reality)
- **Request-level failover**: on upstream 5xx/connection error with zero bytes written to the client, the router automatically retries once on the next-best replica by affinity order (never after SSE has started streaming); observable via `router_retries_total{from,to}`, disable with `retry.enabled=false`
- **Discovery + hot reload**: `POST /admin/backends` (bearer token, or localhost-only) adds/removes replicas on a running router with zero restarts; `SIGHUP` reloads the whole config file, keeping the previous one on parse failure; `deploy/k8s/` ships a ConfigMap + reloader script that renders Endpoints changes into the config and triggers a reload (capability parity with service-discovery gateways)
- **Probes & HA**: `/readyz` returns 503 when every replica is evicted (readiness), `/statusz` stays 200 (liveness); the router is stateless — run multiple instances behind an LB to scale out
- **Observability**: `/metrics` (Prometheus: per-backend requests, sticky ratio, overflow fallbacks, retries, model-routing rejects/mismatches), `/statusz` (health snapshot)
- **SSE pass-through**: `FlushInterval=-1` streams chunks through without buffering
- **PD disaggregated routing (experimental)**: a Prefill/Decode split gateway layer — short requests go inline to the D pool, long requests (estimated prompt tokens ≥ threshold) take a (P,D) disaggregated path; sglang mode injects the `bootstrap_host/port/room` triplet into the body (engine-native KV transfer, room IDs are salted and atomically unique); vllm mode is a two-phase orchestration (prefill sends `max_tokens=1` + `kv_transfer_params{do_remote_decode}`, then the returned KV params re-target the decode pool with streaming write-back); P-pool failover, per-pool probing/breaking, dual-pool `readyz` semantics, and 4 PD-specific metrics

## Benchmark (2× vLLM Qwen2.5-1.5B replicas, A10 24GB)

Incremental-conversation load (40 sessions × 6 turns; each turn appends a ~250-token "document chunk" as user message and folds the assistant reply back into messages — the history prefix grows every turn; 0-0.3s inter-turn jitter; 8 concurrent):

| Strategy | Prefix Cache hit rate | TTFT avg | TTFT p90 |
|---|---|---|---|
| **prefix_affinity** | **67.3%** | 0.461s | **0.531s** |
| round_robin | 62.5% | 0.470s | 0.568s |

- Sticky ratio 0.965 (synced load), load split 122/118 (49%/47%)
- Bounded-load overflow fallbacks: 22-53 (expected behavior of α=1.25)
- **Failover drill**: kill one vLLM → health probe evicts it → 30/30 requests succeed on the surviving replica, zero failures

> Note: hit rates are delta ratios of replica `vllm:prefix_cache_{queries,hits}_total` counters; the ~67% absolute value is capped by per-replica KV-capacity LRU eviction (~100k tokens/replica) — the routing benefit stacks on top of that constraint. The larger the replica KV capacity and the longer the session prefixes, the greater the marginal value of sticky routing. Reproduction steps in `docs/usage.md`.

### v0.2.0 five-scenario end-to-end regression (`scripts/gpu_regression.sh`, A10 × 2× vLLM 0.10.x, Qwen2.5-1.5B + LoRA adapter)

| Scenario | Gate | Result |
|---|---|---|
| S1a sync-load stickiness | sticky ≥ 0.95 | **PASS** sticky=0.985, 240/240, TTFT p90=0.525s |
| S1b jitter hit rate | ≥ 67.3% (v0.1 baseline) | **PASS** hit=74.4%–76.7% (two runs), 240/240 |
| S2 dual-model zero cross-pool | 0 cross-pool requests, no mismatch | **PASS** LoRA dual-model (base+adapter): all 20 requests land on the target replica (delta=20, a=0), `router_model_mismatch_total`=0, undeclared model → 502 |
| S3 kill a replica mid-load | zero client failures | **PASS** replica TERMed at t+20s, 80/80 zero failures (request-level failover) |
| S4 KV-capacity-aware steering | high-watermark re-dispatch | **FAIL (engine metric gap)**: this vLLM build's `kv_cache_usage_perc` is constant 0.0 (external KV-connector scope only) and no GPU-block water-level gauge is exposed, so steering cannot trigger; M5 fully validated in mock integration tests (`test/kvaware_test.go`), and the namespace-prefix tolerance of `kv.metric_name` was verified against real /metrics. Legacy vLLM (0.8/0.9, exposing `gpu_cache_usage_perc`) works |
| S5 live replica add/remove | backends/healthy change instantly, PID unchanged | **PASS** admin API add=200 + SIGHUP hot-reload, backends 2→3→2 with health following, router PID unchanged |
| S6 wrap-up | /readyz 200 | **PASS** |

Regression runbook (phase markers, result upload, replica cleanup) and bootstrap ship with the repo: `scripts/gpu_regression.sh`, `scripts/gpu_bootstrap.sh`, `tools/load_test.py`, `tools/compare_cache.py`.

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

### Minimal local setup (single-replica smoke test)

```bash
# One vLLM replica is enough to verify routing and pass-through
vllm serve /path/qwen2.5-1.5b --served-model-name qwen2.5-1.5b --port 8001 --gpu-memory-utilization 0.85 &
./infer-router -config config/router.yaml   # keep only the vllm-a backend entry
curl localhost:8080/v1/chat/completions -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-1.5b","messages":[{"role":"user","content":"hi"}]}'
```

### Production-recommended configuration

```yaml
addr: ":8080"
strategy: prefix_affinity
health_interval_s: 3            # faster failure eviction
breaker: { fail_threshold: 3, cooldown_s: 10 }
affinity:
  alpha: 1.25                   # bounded-load spillover factor
  sticky_window: 16384          # raise for high concurrent-session counts
  tree:                         # hybrid prefix tree (enabled by default)
    max_tree_size: 65536        # global node cap, LRU eviction beyond it
    eviction_interval_s: 60     # periodic eviction
    cache_threshold: 0.5        # tree hit ratio (matched depth / chain length)
    chunk_chars: 256            # normalization chunk granularity (runes)
kv:                             # KV-capacity-aware steering (on by default, triggers only at high watermark)
  high_watermark: 0.9           # steer away when the top candidate exceeds this
  top_k: 2                      # steering decision window (top-k rendezvous candidates)
  metric_name: gpu_cache_usage_perc  # engine KV metric name (vLLM default; vllm: prefix auto-handled, adjust for SGLang)
  cap_scale: false              # scale load caps by KV headroom (off by default)
backends:
  - { name: vllm-a, base_url: "http://vllm-a.svc:8001", model: qwen2.5-1.5b, weight: 2 }
  - { name: vllm-b, base_url: "http://vllm-b.svc:8001", model: qwen2.5-1.5b, weight: 1 }
  - { name: vllm-c, base_url: "http://vllm-c.svc:8001", model: qwen2.5-7b,   weight: 1 }
# model accepts comma-separated lists and "*" wildcards; weight drives both affinity share and load-cap sharding (2:1 = double traffic)
```

Deployment notes: run the router in the same host/zone as the replicas (probes stay on the internal network); host the static binary or image via systemd/Kubernetes (~1 vCPU / 64MB); let Prometheus scrape `:8080/metrics` and watch `router_affinity_sticky_ratio` (<0.9 → check the session-key contract) and sustained growth of `router_affinity_overload_fallback_total` (raise alpha or add replicas); have clients consistently send `X-Session-Id`.

## PD disaggregated routing (M7, experimental)

Prefill (compute-bound) and decode (bandwidth/capacity-bound) have different resource profiles, and production stacks (SGLang SMG, vLLM Production Stack, llm-d) commonly split the two phases onto separate replica pools. infer-router plugs in as a **gateway layer**: clients still see a single OpenAI-compatible endpoint, and the router decides per request between inline delivery and disaggregated orchestration.

**How it works** (with `pd.mode` unset the whole layer is off and every backend routes as a normal pool — v0.2 configs need zero changes):

- When PD is enabled, backends must declare `role: prefill | decode` with ≥1 replica per pool; prefix trees are namespaced by `model#role` so the P and D pools never pollute each other.
- **Path decision**: prompt tokens are estimated as chars/4 (falls back to body/3 if chat parsing fails). Below `pd.path_threshold_prompt_tokens` (default 256) → inline to the D pool (prefill is pure extra latency for short requests); at/above → pick a (P,D) pair.
- **sglang mode**: injects `bootstrap_host / bootstrap_port / bootstrap_room` into the prefill request body (native fields on SGLang's `ChatCompletionRequest`, verified against main); room = random salt + atomic counter, never reused; the router performs request-level failover on the P pool (re-injecting a fresh room per retry). Decode replicas run `--disaggregation-bootstrap-port` (default 8998).
- **vllm mode**: two-phase orchestration — phase 1 sends the P pool `max_tokens=1`, `stream=false`, `kv_transfer_params{do_remote_decode:true, remote_host/port/tp_size→decode}` and reads the returned KV params; phase 2 re-sends the original body plus those params to the D pool and streams the answer back to the client (KV lands on exactly one replica, so phase 2 does not fail over across decode replicas).
- **Health & observability**: independent probing/breaking per pool; `/readyz` requires ≥1 healthy replica in each pool; new metrics `router_pd_path_total{path}`, `router_pd_pair_total{prefill,decode}`, `router_pd_bootstrap_total{result}`, `router_pool_kv_util{backend,role}`.

See the PD section of [`config/router.yaml`](config/router.yaml) for a full example (SGLang 1P + 2D).

> **Status**: the full PD path (inline delivery / sglang bootstrap injection & failover / vllm two-phase orchestration / dual-pool readyz / config validation rollback) is covered by the `test/pd_test.go` mock integration suite; A10 GPU results (SGLang 0.5.x 1P+1D bootstrap-422 alignment, vLLM 0.9 end-to-end KV transfer) ship with v0.3.0.

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

MIT License.
