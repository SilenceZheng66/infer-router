# infer-router 设计文档

## 1. 问题定义

多副本推理引擎（vLLM / SGLang）的 Prefix Cache 是副本本地的 KV 缓存。请求被无状态负载均衡打散时：

1. 同一会话的连续轮次落到不同副本 → 各副本重复 prefill 同一前缀（算力浪费 + TTFT 抬高）；
2. 副本 KV 池被"路过"请求的前缀污染，有效容量下降。

路由层要做的事：**让同一会话的请求尽量命中同一副本，同时不让任何副本过载**。约束：无外部存储（不引入 Redis/一致性哈希环服务）、单二进制、副本可随时增减。

## 2. 策略：rendezvous hashing + bounded load

### 2.1 为什么是 rendezvous（HRW）而不是环哈希

- **无状态**：`score_i = hash(key, replica_i)`，每个路由进程独立计算、结果一致，不需要共享哈希环元数据。
- **最小扰动**：副本增减只迁移 `1/n` 会话；环哈希在虚拟节点配比不当时扰动不可控。
- **加权 rendezvous（v0.2.0）**：`u_i = hash/2^64 ∈ (0,1)`，`score_i = u_i^(1/w_i)`，比较 `ln(score_i) = ln(u_i)/w_i`——权重越大 rank 越接近 0，越容易胜出（指数竞速性质：副本份额 = w_i/Σw）。各副本 rank 相互独立，副本增减时已有副本间的指派不变，扰动仍只发生在"迁往新副本"的 key 上。负载上限同步按权重分摊：`cap_i = ceil(α × Σinflight × w_i / Σw)`。

### 2.2 哈希函数选型

原始实现用 FNV-1a 后缀拼接（`hash(replica) ^ hash(key + replica)`）做 key×replica 混合，性质测试暴露**单字节后缀弱雪崩**：相邻 key 只有一位差异时，若干副本对的 score 相对次序不变，亲和分布出现偏斜。

修复：FNV-1a 之后接 **murmur3 fmix64 finalizer**：

```go
h ^= h >> 33; h *= 0xff51afd7ed558ccd; h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53; h ^= h >> 33
```

fmix64 保证输入低位翻转扩散到输出全部 64 位，score 分布恢复均匀。9 个单测中的性质测试（均匀性、最小扰动、确定性）全绿。

### 2.3 bounded load（粘性与均衡的折中）

纯 HRW 在副本异构/短时慢响应时会堆积：某副本在途爆仓，而次优副本闲置。采用 bounded load（Google《Consistent Hashing with Bounded Loads》）：

```
cap_i = ceil(α × Σinflight / n)，α = 1.25
```

选副本时按 score 从高到低遍历，跳过在途 ≥ cap_i 的副本；全部超限时选 score 最高的（避免无解）。代价是少量会话轮次落到次优副本——实测 sticky_ratio 0.965（同步负载）、fallback 计数暴露为 `router_affinity_overload_fallback_total`，可观测可调参。

### 2.4 会话 key 提取

优先级：`X-Session-Id` header → messages 前缀（system 内容 + 首条 user 内容，SHA-256 截断）。多轮对话中该前缀不变，因此整段对话稳定命中同一副本。key 不可提取（如空 messages）时退化为 RR。

### 2.5 模型感知路由（v0.2.0）

- 亲和前先按请求 `model` 筛选健康副本池：backend.model 支持逗号分隔多模型与 `*` 通配；请求 model 为空（非 chat 请求）不过滤。
- 无副本服务该模型 → 502 结构化错误（`no_backend_for_model`）+ `router_model_nomatch_total{model}` 计数，不落入任何副本。
- 会话亲和 key 为"模型 + 会话"复合 key：同一会话切换模型视为不同亲和目标，避免把请求钉在与该模型无关的副本上。
- RR / least_queue 策略路径应用同一过滤语义。

## 3. 粘性观测：sticky tracker

环形窗口（容量 sticky_window=4096）记录 `key → last replica`。每次 Pick 后统计：**重复出现的 key 中，本次副本 == 上次副本的比例**。首见 key 不计入分母（否则新会话流入会稀释指标）。暴露为 gauge `router_affinity_sticky_ratio`。

## 4. 健康与降级

- 周期探活（health_interval_s=5）：`GET {base_url}/v1/models`（OpenAI 兼容端点，vLLM/SGLang 均原生提供），连续失败摘除，恢复自动加回。
- 请求级熔断：连续 fail_threshold 次失败打开熔断，cooldown_s 后半开放一个探测请求。失败计数按错误分类：**5xx / 连接错误计失败；4xx 不计**（客户端/配置问题不推动熔断），其中"副本声明服务该模型但上游 404"单独记 `router_model_mismatch_total{backend,model}`。
- 请求级 failover（v0.2.0）：传输层（retryRoundTripper）在 5xx/连接错误、且响应尚未写给客户端时，按加权 rendezvous 次序换下一个副本重发一次（`retry.enabled=false` 关闭）。SSE 一旦开始流出即不再重试——响应头到达客户端后，body 复制期间的一切失败走 ErrorHandler 原样回错并计入实际服务副本。重试计数 `router_retries_total{from,to}`。
- 实测降级：kill 一个 vLLM → probe 5s 内摘除 → 后续请求 100% 落到存活副本（30/30），`/statusz` 显示 healthy 1/2。副本恢复后 probe 自动加回。

## 5. 转发层

- `httputil.ReverseProxy` + `FlushInterval=-1`：SSE 逐块透传，不缓冲。
- 亲和路径需要读 body 提取会话 key：`io.LimitReader(8MB)` 读入 → 重放（`NopCloser(bytes.NewReader)`）→ 转发，保证非流式/流式与文件上传路径都不破坏。

## 6. 指标

| 指标 | 类型 | 含义 |
|---|---|---|
| `router_requests_total{backend,status}` | counter | per-backend 请求计数 |
| `router_affinity_assign_total{backend}` | counter | 亲和指派分布（负载均衡度） |
| `router_affinity_overload_fallback_total` | counter | bounded load 顺延次数 |
| `router_model_nomatch_total{model}` | counter | 无副本服务请求模型的拒绝计数 |
| `router_model_mismatch_total{backend,model}` | counter | 副本声明服务该模型但上游 404 |
| `router_retries_total{from,to}` | counter | 请求级 failover 重试计数 |
| `router_affinity_sticky_ratio` | gauge | 重复会话同副本比例 |
| `/statusz` | JSON | strategy / backends / healthy 快照 |

## 7. 实验方法论：三轮无效对照与修正

对照实验（affinity vs RR）的三次返工，每次都被数据当场抓住：

1. **公共前缀污染**：会话独占标识放在 system 末尾 → 40 会话共享 ~1800 token 前缀 → prefix cache 跨会话命中 → 两组都 ~94%。修正：**独占标识放最前**（首块哈希不同即整链不共享）。
2. **RR 隐性粘性**：同步波次负载下，8 线程到达顺序稳定 → RR 全局交替退化为 per-thread 固定副本。修正：**轮间随机 jitter**。
3. **负载形态错误**："system + 每轮独立问题"测不出粘性价值——vLLM 块哈希链缓存"历史出现过的前缀"，稳定前缀两副本都会持有。判别实验（单线程×20 轮 RR）命中率仍 ~93% 证实。修正：**递增对话负载**（每轮 user 新片段 + assistant 回复并入 messages，历史前缀逐轮增长）。

最终有效对照（40 会话×6 轮、8 并发、jitter 0.3s、双副本 0.38 KV util）：

| 组 | 命中率 | TTFT p90 |
|---|---|---|
| prefix_affinity | 67.3% | 0.531s |
| round_robin | 62.5% | 0.568s |

**为何绝对值只有 67%**：递增负载 KV 需求 ≈ 30 万 token，副本 KV 容量 ≈ 10 万 token（A10×0.38，1.5B GQA ≈ 57KB/token），LRU 驱逐是主导 miss 源。结论：**prefix affinity 的收益上限受副本 KV 容量约束**——副本内存越大、会话前缀越长，路由粘性的边际价值越大。这也解释了为什么生产网关（如 vLLM 生产栈的 prefix-aware 调度）在长上下文 agent 场景收益最显著。

## 8. 不做什么

云 provider 聚合、计费治理、MCP、多租户 RBAC、请求级配额——这些是重型网关（LiteLLM/Higress 等）的职责。infer-router 定位是**引擎池前最后一跳**，与它们可串联部署。
