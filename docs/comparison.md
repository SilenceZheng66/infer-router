# 竞品对照表：infer-router vs SGLang SMG vs llm-d 生产栈

> 本文为能力对照，措辞保持中性。各项能力均给出机制层面的说明与出处（文档/代码），
> 未实测项如实标注。更新于 v0.2.0。

## 1. 定位差异

| | infer-router | SGLang SMG（sglang-router） | llm-d / vLLM production-stack |
|---|---|---|---|
| 形态 | 引擎无知的 L7 路由器，单二进制（约 9MB 静态编译），依赖仅 Prometheus client | SGLang 生态内置路由器，与 vLLM/SGLang 引擎同进程生态 | vLLM 生态的生产化参考栈（Gateway + 调度器 + KV 连接器） |
| 引擎假设 | 任意 OpenAI 兼容引擎（vLLM / SGLang / LMDeploy 等 /v1/* 服务） | 主要面向 SGLang（vLLM 兼容层支持） | vLLM 为主（KV Connector 机制绑定 vLLM） |
| 无状态性 | 完全无状态：路由决策由确定性哈希 + 前缀树重放得出，多实例部署无需互同步 | 路由器内维护 per-worker radix tree；官方文档承认多路由器副本间不同步会导致缓存命中率下降（10–20%） | 调度器依赖 KV 事件索引（有状态组件） |
| 部署开销 | 1 核 / 64MB 即可运行 | Go 进程 + 引擎生态 | 完整参考栈（含 operator / KV 事件管道） |

## 2. 能力矩阵

| 能力 | infer-router v0.2.0 | SGLang SMG | llm-d 生产栈 | 说明 |
|---|---|---|---|---|
| 缓存感知路由 | ✅ 会话 ID 快路径 + 混合前缀树（消息分块哈希链，per-model 分棵，LRU 驱逐，`cache_threshold` 判定） | ✅ 真前缀树（逐 token 级） | ✅ KV 事件驱动的精确前缀索引 | 精度换无状态：树键为分块哈希链，O(1) 指纹比对；SMG 树粒度更细但路由器实例间不同步 |
| 多模型路由 | ✅ 按 `model` 筛健康副本池 + 无匹配 502（`router_model_nomatch_total`）+ 404 单列（`router_model_mismatch_total`） | ✅ 按模型策略 | ✅ 按模型 + 别名 | M1；GPU 实测见 README |
| 异构权重 | ✅ 加权 rendezvous（`score=hash^(1/w)`）+ 按权重分摊 bounded-load 上限 | ✅ 有 | ✅ 有 | M1a/M1b；2:1 分布偏差单测 ≤±10% |
| 请求级重试 | ✅ 首字节前按亲和次序顺延一次（SSE 首字节后绝不重试），`router_retries_total{from,to}` | ✅ 有重试 | ✅ 发现容错 | M2a |
| 熔断与错误分类 | ✅ 5xx/连接错误计失败、4xx 不计（可配例外），连续失败熔断 + 冷却半开 | ✅ 有 | ✅ 有 | M2b |
| 副本探活与摘除 | ✅ 周期探活（间隔可配），全摘除时 `/readyz` 503 | ✅ 有 | ✅ 有 | M4c |
| 动态发现 | ✅ 热加载（SIGHUP/文件触发，原子换装 + 在途排空 + 失败回滚）+ `POST /admin/backends` + K8s ConfigMap-reloader 等价方案 | ✅ K8s service discovery（selector 轮询） | ✅ K8s API 原生 | M4；机制对齐、形态不同（configmap 渲染 vs API watch） |
| KV 容量感知路由 | ✅ 探活顺带抓 `kv.metric_name`（缺省 `gpu_cache_usage_perc`，自动兼容 `vllm:` 命名空间前缀与标签行），top-k 高水位分流 + 可选按 KV 余量缩放负载上限（`router_backend_kv_util` / `router_kv_steered_total`） | ➖ 以容量/负载为中心的均衡，未见 KV 水位驱动的会话改派 | ➖ 侧重 KV 精确索引与卸载，路由侧以事件同步为主 | M5；mock 全链路验证；GPU 上受引擎指标缺口限制见 §4 |
| KV 缓存共享/卸载 | ⛔ 不做缓存管理 —— 通过标准 API 串联 LMCache 等外置层获得同等收益 | ➖ 依赖引擎侧 | ✅ LMCache / NIXL | 架构取舍：路由层不接管 KV 生命周期 |
| PD 分离（prefill/decode） | ⛔ 路由层不拆分池 —— 由引擎侧网关/调度承担，或上层按池分别部署 infer-router 串联 | ✅ 原生 | ✅ NIXL | M7（Stretch）：如实施则为透传双池路由；未实施前如实标注 |
| 服务发现语义 | ✅ admin API 即时增删 + 热更 + K8s 等价脚本 | ✅ | ✅ | M4 |
| 可观测性 | ✅ Prometheus 全量指标（粘性率、分配分布、重试、熔断、树规模、KV 水位） | ✅ | ✅ | 各家口径不同，见 usage.md §指标 |

## 3. 明确不做（边界即设计）

- **Provider 聚合 / 多云路由**：不做多家模型供应商的聚合代理（LiteLLM / OpenRouter 定位）。
- **计费 / 配额 / RBAC**：不做租户与计费层，由 API 网关承担。
- **限流**：不做租户级限流（副本级过载保护由 bounded load 承担）。
- **MCP / Agent 编排**：不做请求改写与工具编排。
- **KV 缓存管理**：不接管 KV 生命周期（见上表串联说明）。

## 4. 如实标注的局限

- **gRPC 不支持**：仅 HTTP（OpenAI 兼容 `/v1/*`）。
- **亲和仅覆盖 chat 语义**：`/v1/*` 全部透传，但会话/前缀亲和仅对 chat completions 生效（completions/embeddings 等按 rendezvous 均衡，不做前缀匹配）。
- **前缀树为近似匹配**：分块哈希链精度低于逐 token 树；以多路由器水平扩展时的命中率一致性换回状态复杂度。
- **KV 水位依赖引擎指标**：M5 分流需要引擎 /metrics 暴露真实 GPU KV 块水位 gauge。vLLM 0.8/0.9 的 `gpu_cache_usage_perc` 可用；0.10.x 及以上的 `kv_cache_usage_perc` 仅统计外部 KV connector（恒 0.0），本仓 GPU 实测无法在该构建上触发分流（mock 集成测试全绿，`kv.metric_name` 的命名空间前缀兼容在真实 /metrics 验证通过）。
- **SGLang KV 指标名需手工对齐**：`kv.metric_name` 可配，SGLang 侧指标名未在本仓 GPU 环境实测。

## 5. 实测数据

- v0.1 基线与 v0.2.0 GPU 回归（A10 × vLLM，递增对话 40×6）：见 README「生产验证」与 docs/usage.md「GPU 回归」；复现步骤见 `scripts/gpu_regression.sh`（随仓库）。
