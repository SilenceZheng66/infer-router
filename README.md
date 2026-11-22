# infer-router

部署在 N 个 vLLM / SGLang 副本前的**引擎中立轻量状态感知路由层**（Go，单二进制，无外部依赖）。

姊妹项目 [`llm-inference-lab`](https://github.com/SilenceZheng66/llm-inference-lab) 是推理服务全链路参考实现，本项目把其中"副本路由层"抽出来，做成可直接替换裸轮询 LB / 重型网关的独立组件。

## 为什么需要它

多副本 vLLM 的 Prefix Cache 是**副本本地**的：同一会话的请求被轮询到不同副本时，副本各自重建 KV 前缀，既浪费算力（重复 prefill）又抬高 TTFT。重型网关功能全但部署重；裸轮询 LB 完全不感知这个结构。

infer-router 只做一件事：**把 OpenAI 兼容请求（含 SSE 流式）以会话感知的策略路由到最合适的副本**，让引擎 Prefix Cache 真正被复用。

## 特性

- **prefix_affinity 策略**（核心）：
  - 无状态 **rendezvous hashing**（murmur3 fmix64 终结器 + FNV 预混）——副本增减时只迁移 `1/n` 会话，其余会话原副本不动
  - **bounded load**（α=1.25）：亲和副本在途数超过 `ceil(α × 平均在途)` 时顺延次优副本，粘性与均衡兼得
  - **sticky tracker**：滑动窗口统计重复会话请求的同副本比例，暴露为 `router_affinity_sticky_ratio`
  - 会话 key 优先取 `X-Session-Id` header，否则取 messages 前缀（system + 首条 user）——多轮对话该前缀不变
- **策略集**：`round_robin | least_queue | prefix_affinity`
- **健康探测 + 熔断**：周期探活自动摘除死副本；请求级 fail_threshold 熔断 + 冷却半开探测
- **可观测**：`/metrics`（Prometheus：per-backend 请求计数、粘性率、fallback 计数）、`/statusz`（健康快照）
- **SSE 原样透传**：`FlushInterval=-1` 逐块转发，不缓冲流式响应

## 实测数据（双副本 vLLM 2.5-1.5B，A10 24GB）

递增对话负载（40 会话 × 6 轮，每轮 user 注入 ~250 token 新片段、assistant 响应并入 messages——历史前缀逐轮增长；轮间 0-0.3s 抖动；8 并发）：

| 路由策略 | Prefix Cache 命中率 | TTFT avg | TTFT p90 |
|---|---|---|---|
| **prefix_affinity** | **67.3%** | 0.461s | **0.531s** |
| round_robin | 62.5% | 0.470s | 0.568s |

- 亲和组粘性率 0.965（同步负载），负载分布 122/118（49%/47%）
- bounded load 在途超限 fallback 22-53 次（α=1.25 的设计行为）
- **降级演练**：kill 一个 vLLM → 健康探针自动摘除 → 30/30 请求全部落到存活副本，零失败

> 口径说明：命中率为副本 `vllm:prefix_cache_{queries,hits}_total` 计数器差值比；~67% 的绝对值受副本 KV 容量 LRU 驱逐限制（≈10 万 token/副本），路由收益叠加在容量约束之上。完整实验记录（含三轮无效对照的返工过程）见仓库 `docs/design.md` 与姊妹项目 lab-notes。

## 快速开始

```bash
# 构建（静态二进制，无 CGO）
CGO_ENABLED=0 go build -o infer-router ./cmd/router

# 配置并启动（副本清单见 config/router.yaml）
./infer-router -config config/router.yaml

# 状态与指标
curl localhost:8080/statusz
curl localhost:8080/metrics

# 经路由层请求（与直连 vLLM 完全同构）
curl localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-1.5b","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

最小配置：

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

## 压测工具

```bash
# 递增对话负载压测（流式 TTFT + 会话粘性流量）
python tools/load_test.py http://127.0.0.1:8080 affinity 40 6 8 s1 0.3

# 采集副本 prefix cache 指标差值命中率
# （基线/终值各 4 行追加进同一文件后）
python tools/compare_cache.py cb_file.txt affinity
```

## 架构

```
client ──► :8080 mux
             ├── /statusz  副本健康/策略快照
             ├── /metrics  Prometheus（requests、sticky_ratio、overload_fallback）
             └── /v1/*
                  ├─ 提取会话 key（X-Session-Id 或 messages 前缀）
                  ├─ picker.Pick(key, healthy)：rendezvous 排序 + bounded load 顺延
                  └─ ReverseProxy（FlushInterval=-1，body 重放）──► vLLM/SGLang 副本
```

## 设计文档

- [`docs/design.md`](docs/design.md)：rendezvous + bounded load 的选型与公式、sticky tracker、熔断与降级、以及"三轮无效对照"实验方法论
- [`docs/usage.md`](docs/usage.md)：配置项、部署（裸机/Docker）、压测与降级演练复现步骤

## 测试

```bash
go test ./...        # 单测：rendezvous 一致性、bounded load 上限、sticky 统计、RR/least_queue、SSE 透传
```

> 诚实口径：学习/作品集项目，性能结论仅来自免费算力环境的实测对比，不声明生产 SLA。MIT License。
