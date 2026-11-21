# infer-router

部署在 N 个 vLLM/SGLang 副本前的**引擎中立轻量状态感知路由层**（Go）。姊妹项目 [`llm-inference-lab`](../llm-inference-lab) 是全链路参考实现，本项目把其中"副本路由层"抽出来做成可直接替换裸轮询 LB / 重型网关的独立组件。

## 定位

- 只做一件事：把 OpenAI 兼容请求（含 SSE 流式）以正确的策略路由到最合适的引擎副本。
- 状态感知：探测副本健康、采样在途/队列深度，配合引擎 Prefix Cache 做前缀亲和。
- 明确不做：云 provider 聚合、计费治理、MCP、多租户 RBAC（这些是重型网关的职责）。

## 策略集

| 策略 | 说明 | 状态 |
|---|---|---|
| round_robin | 均匀轮转 | M1 |
| least_queue | 在途最少优先，并列 RR 决胜 | M1 |
| prefix_hash | 首条用户消息前缀哈希亲和，提升引擎 Prefix Cache 命中 | M2 |
| weighted | 权重灰度 | M2 |
| breaker fallback | 副本熔断跳过 + 冷却探测 | M2 |

## 里程碑

- **M1**：OpenAI 兼容透传 + RR/least_queue + 健康探测 + 基础指标（当前）
- **M2**：前缀哈希亲和、权重灰度、熔断 fallback、引擎 /metrics queue depth 采样
- **M3**：双副本压测对比报告（vs 裸轮询 / NGINX / LiteLLM 附加延迟）

## 快速开始

```bash
go run ./cmd/router -config config/router.yaml
# 状态：curl localhost:8080/statusz
# 指标：curl localhost:8080/metrics
# 请求：curl localhost:8080/v1/chat/completions -d '{"model":"...","messages":[...]}'
```

## 架构

```
client ──► :8080 mux
             ├── /statusz（副本健康/在途）
             ├── /metrics（Prometheus）
             └── /v1/* ── balancer.Pick(healthy) ──► ReverseProxy ──► vLLM/SGLang 副本
                          （FlushInterval=-1 保证 SSE 逐块透传）
```

> 诚实口径：学习/作品集项目，性能结论仅来自免费算力环境的实测对比，不声明生产 SLA。
