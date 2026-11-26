# infer-router

部署在 N 个 vLLM / SGLang 副本前的**引擎中立轻量状态感知路由层**（Go，单静态二进制，无外部依赖）。与 [`llm-inference-lab`](https://github.com/SilenceZheng66/llm-inference-lab)（推理服务全链路参考实现）配合使用，也可独立替换现有链路中的裸轮询 LB / 重型网关路由环节。

## 解决的问题

多副本 vLLM 的 Prefix Cache 是**副本本地**的：同一会话的请求被轮询到不同副本时，副本各自重建 KV 前缀，既浪费算力（重复 prefill）又抬高 TTFT。重型网关功能全但部署重；裸轮询 LB 完全不感知这个结构。

infer-router 只做一件事：**把 OpenAI 兼容请求（含 SSE 流式）以会话感知的策略路由到最合适的副本**，让引擎 Prefix Cache 真正被复用。

## 特性

- **prefix_affinity 策略**（核心）：
  - 无状态 **rendezvous hashing**（murmur3 fmix64 终结器 + FNV 预混）——副本增减时只迁移 `1/n` 会话，其余会话原副本不动
  - **加权 rendezvous**：`score_i = u_i^(1/w_i)`（u ∈ (0,1) 均匀哈希），`weight` 副本按权重承担对应份额流量；负载上限按权重分摊 `cap_i = ceil(α × Σinflight × w_i / Σw)`
  - **模型感知路由**：按请求 model 筛选候选池（backend.model 支持逗号分隔多模型与 `*` 通配）；会话亲和 key 含模型维度，同会话切模型不串池
  - **bounded load**（α=1.25）：亲和副本在途数超过上限时顺延次优副本，粘性与均衡兼得
  - **sticky tracker**：滑动窗口统计重复会话请求的同副本比例，暴露为 `router_affinity_sticky_ratio`
  - **混合前缀树**：无会话 ID 的请求按 messages 归一化分块哈希链在按模型分棵的前缀树上近似匹配，命中（匹配比例 ≥ cache_threshold）沿用树记录副本并叠加 bounded load，未命中按"已匹配前缀"确定性派单；全局节点上限 `max_tree_size` + LRU 周期驱逐。共享长前缀（如同一 system）的会话集中到持有该前缀的副本以提升 KV 复用——mock 实测 30 个共享 system 会话：tree 模式 30/30 单副本集中，affinity-only 模式 16/14 摊开
  - **KV 容量感知分流**：探活顺带抓副本 `/metrics` 的 KV 使用率（`router_backend_kv_util{backend}`，指标名可配）；rendezvous top-k 内首选副本超 `kv.high_watermark`（默认 0.9）时改派 KV 余量最大的未过载候选（树命中路径同样生效），避免把请求压进正在 LRU 驱逐的副本；`kv.cap_scale` 可选把负载上限按 KV 余量缩放（默认关）。低水位时行为与基线完全一致
  - 会话解析三级：`X-Session-Id` 快路径 → 前缀树近似匹配 → 前缀确定性派单（会话 key 仍优先取 `X-Session-Id` header，否则取 messages 前缀——多轮对话该前缀不变）
- **策略集**：`round_robin | least_queue | prefix_affinity`
- **健康探测 + 熔断**：周期探活自动摘除死副本；请求级 fail_threshold 熔断 + 冷却半开探测；**错误分类**——5xx/连接错误计熔断失败，4xx 不计（404+model 记 `router_model_mismatch_total`，暴露副本声明与实际 serving 不符）
- **请求级 failover**：上游 5xx/连接错误且响应尚未写给客户端时，按亲和次序自动换下一个副本重试一次（SSE 一旦开始流出即不重试）；`router_retries_total{from,to}` 可观测，`retry.enabled=false` 可关闭
- **动态发现 + 热加载**：`POST /admin/backends`（Bearer token 或仅 localhost）运行中增删副本、零重启；`SIGHUP` 重载整份配置，解析失败自动保留旧配置；`deploy/k8s/` 提供 ConfigMap + reloader 脚本，把 Endpoints 变化渲染进配置触发热更（与 service-discovery 网关能力对齐）
- **探针与 HA**：`/readyz` 全副本摘除时 503（readiness），`/statusz` 恒 200（liveness）；路由器无状态，多实例 + 上层 LB 即水平扩展
- **可观测**：`/metrics`（Prometheus：per-backend 请求计数、粘性率、fallback 计数、重试计数、模型路由拒绝/不符）、`/statusz`（健康快照）
- **SSE 原样透传**：`FlushInterval=-1` 逐块转发，不缓冲流式响应
- **PD 分离路由（实验性）**：Prefill/Decode 分离网关层——短请求 inline 直连 D 池，长请求（估算 prompt token ≥ 阈值）走 (P,D) 分离路径；sglang 模式 body 注入 `bootstrap_host/port/room` 三元组（引擎原生 KV 传输，房间号原子递增唯一）；vllm 模式两段编排（prefill 发 `max_tokens=1` + `kv_transfer_params{do_remote_decode}`，取回 KV 参数后改发 decode 池流式回写）；P 池 failover、双池独立探活熔断、`readyz` 双池就绪语义与 4 个 PD 专属指标

## 实测数据（双副本 vLLM 2.5-1.5B，A10 24GB）

递增对话负载（40 会话 × 6 轮，每轮 user 注入 ~250 token 新片段、assistant 响应并入 messages——历史前缀逐轮增长；轮间 0-0.3s 抖动；8 并发）：

| 路由策略 | Prefix Cache 命中率 | TTFT avg | TTFT p90 |
|---|---|---|---|
| **prefix_affinity** | **67.3%** | 0.461s | **0.531s** |
| round_robin | 62.5% | 0.470s | 0.568s |

- 亲和组粘性率 0.965（同步负载），负载分布 122/118（49%/47%）
- bounded load 在途超限 fallback 22-53 次（α=1.25 的设计行为）
- **降级演练**：kill 一个 vLLM → 健康探针自动摘除 → 30/30 请求全部落到存活副本，零失败

> 注：命中率为副本 `vllm:prefix_cache_{queries,hits}_total` 计数器差值比；~67% 的绝对值受副本 KV 容量 LRU 驱逐限制（≈10 万 token/副本），路由收益叠加在容量约束之上——副本 KV 容量越大、会话前缀越长，粘性路由的边际收益越大。实验复现步骤见 `docs/usage.md`。

### v0.2.0 五场景端到端回归（`scripts/gpu_regression.sh`，A10 × 双副本 vLLM 0.10.x，qwen2.5-1.5B + LoRA adapter）

| 场景 | 门禁 | 实测 |
|---|---|---|
| S1a 同步负载粘性 | sticky ≥ 0.95 | **PASS** sticky=0.985，240/240，TTFT p90=0.525s |
| S1b jitter 命中率 | ≥ 67.3%（v0.1 基线） | **PASS** hit=74.4%~76.7%（两轮复测），240/240 |
| S2 双模型零串池 | 跨池请求 0、mismatch 不增 | **PASS** LoRA 双模型（base+adapter）：20 发全部落目标副本（delta=20，a=0），`router_model_mismatch_total`=0，未声明模型 → 502 |
| S3 压测中途 kill 副本 | 客户端零失败 | **PASS** TERM 副本于 t+20s，80/80 请求零失败（请求级 failover） |
| S4 KV 容量感知分流 | 高水位改派 | **FAIL（引擎指标缺口）**：该 vLLM 构建的 `kv_cache_usage_perc` 恒 0.0（仅统计外部 KV connector），不暴露 GPU 块水位，分流无法触发；M5 全链路经 mock 集成测试验证（`test/kvaware_test.go`），`kv.metric_name` 的命名空间前缀兼容已在真实 /metrics 上验证。旧版 vLLM（0.8/0.9，暴露 `gpu_cache_usage_perc`）可用 |
| S5 运行中扩缩副本 | backends/healthy 即时变化、PID 不变 | **PASS** 管理 API add=200 + SIGHUP 热更，backends 2→3→2、healthy 相应变化，路由进程 PID 不变 |
| S6 收尾 | /readyz 200 | **PASS** |

回归 runbook（含阶段 marker、结果回传、副本清理）与引导脚本随仓库发布：`scripts/gpu_regression.sh`、`scripts/gpu_bootstrap.sh`、`tools/load_test.py`、`tools/compare_cache.py`。

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

### 本地最小启动（单副本冒烟）

```bash
# 一个 vLLM 副本即可验证路由与透传
vllm serve /path/qwen2.5-1.5b --served-model-name qwen2.5-1.5b --port 8001 --gpu-memory-utilization 0.85 &
./infer-router -config config/router.yaml   # backends 只留 vllm-a 一条
curl localhost:8080/v1/chat/completions -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-1.5b","messages":[{"role":"user","content":"hi"}]}'
```

### 生产推荐配置

```yaml
addr: ":8080"
strategy: prefix_affinity
health_interval_s: 3            # 更快的故障摘除
breaker: { fail_threshold: 3, cooldown_s: 10 }
affinity:
  alpha: 1.25                   # 超载顺延系数
  sticky_window: 16384          # 高并发会话数大时调大统计窗口
  tree:                         # 混合前缀树（缺省开启）
    max_tree_size: 65536        # 全局节点上限，超限 LRU 驱逐
    eviction_interval_s: 60     # 周期驱逐
    cache_threshold: 0.5        # 树命中判定比例（匹配深度/链长）
    chunk_chars: 256            # messages 归一化分块粒度（rune）
kv:                             # KV 容量感知分流（缺省开启，仅高水位触发）
  high_watermark: 0.9           # 首选副本 KV 超过此值时分流
  top_k: 2                      # 分流决策窗口（rendezvous 前 k 候选）
  metric_name: gpu_cache_usage_perc  # 副本 KV 指标名（vLLM 缺省；自动兼容 vllm: 前缀，SGLang 按需替换）
  cap_scale: false              # 负载上限按 KV 余量缩放（默认关）
backends:
  - { name: vllm-a, base_url: "http://vllm-a.svc:8001", model: qwen2.5-1.5b, weight: 2 }
  - { name: vllm-b, base_url: "http://vllm-b.svc:8001", model: qwen2.5-1.5b, weight: 1 }
  - { name: vllm-c, base_url: "http://vllm-c.svc:8001", model: qwen2.5-7b,   weight: 1 }
# model 支持逗号分隔多模型与 "*" 通配；weight 决定亲和份额与负载上限分摊（2:1 即双倍流量）
```

部署要点：路由层与副本同机/同可用区部署（探活走内网）；systemd 或 Kubernetes 以静态二进制/镜像托管，资源占用约 1 vCPU / 64MB；Prometheus 抓取 `:8080/metrics` 监控 `router_affinity_sticky_ratio`（<0.9 检查会话 key 约定）与 `router_affinity_overload_fallback_total` 持续增长（调大 alpha 或扩容）；客户端统一携带 `X-Session-Id`。

## PD 分离路由（M7，实验性）

大模型推理的 prefill（算力密集）与 decode（带宽/容量密集）资源画像不同，生产栈（SGLang SMG、vLLM Production Stack、llm-d）普遍把两阶段拆到不同副本池。infer-router 以**网关层**身份接入这一形态：客户端仍只见一个 OpenAI 兼容端点，路由层按请求长度决定 inline 直连或分离编排。

**工作方式**（`pd.mode` 为空时整层关闭，全部后端按普通池路由，v0.2 配置零改动）：

- 启用 PD 后 backends 须声明 `role: prefill | decode`，两池各 ≥1 副本；按 `model#role` 命名空间隔离，P/D 池的前缀树互不污染。
- **路径决策**：按字符数/4 粗估 prompt token（chat 结构解析失败退化为 body/3）。低于 `pd.path_threshold_prompt_tokens`（默认 256）→ inline 直连 D 池（prefill 对短请求是额外一跳延迟）；≥ 阈值 → 选 (P,D) 对走分离路径。
- **sglang 模式**：向 prefill 请求 body 注入 `bootstrap_host / bootstrap_port / bootstrap_room`（房间号 = 随机盐 + 原子计数，永不复用），SGLang 引擎经 bootstrap 端口完成 KV 传输（decode 副本需 `--disaggregation-bootstrap-port`，默认 8998）；路由层对 P 池做请求级 failover（重试前重新注入新 room）。
- **vllm 模式**：两段编排——阶段 1 向 P 池发 `max_tokens=1`、`stream=false`、`kv_transfer_params{do_remote_decode:true, remote_host/port/tp_size→decode}`，从响应取回 KV 参数；阶段 2 以原始 body + 引擎返回的参数改发 D 池，流式回写客户端（KV 只落一地，阶段 2 不做跨副本 failover）。
- **健康与观测**：P/D 池独立探活与熔断；`/readyz` 要求两池各 ≥1 健康；新增指标 `router_pd_path_total{path}`、`router_pd_pair_total{prefill,decode}`、`router_pd_bootstrap_total{result}`、`router_pool_kv_util{backend,role}`。

配置示例（SGLang 1P + 2D）见 [`config/router.yaml`](config/router.yaml) 的 PD 分离段；字段语义与注入通道的引擎侧查证记录见设计文档。

> **状态**：PD 全链路（inline 直连 / sglang bootstrap 注入与 failover / vllm 两段编排 / readyz 双池语义 / 配置校验回滚）已由 `test/pd_test.go` 全量 mock 集成测试覆盖；A10 GPU 实测（SGLang 0.5.x 1P+1D bootstrap 422 对齐、vLLM 0.9 KV 传输端到端）与门禁结果随 v0.3.0 发布补录。

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

MIT License.
