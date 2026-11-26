# Changelog

## [Unreleased] — v0.3.0（PD 分离网关层）

### 新增
- **M7 PD 分离路由层**（`internal/pd`，+1035 行，9 文件）：`pd.mode: sglang | vllm` 两套分离编排——
  - **sglang 模式**：body 注入 `bootstrap_host/port/room` 三元组（SGLang `ChatCompletionRequest` 原生字段，main 分支查证确认），房间号随机盐 + 原子计数保证唯一；prefill 请求级 failover（重试重新注入新 room）。
  - **vllm 模式**：两段编排（prefill `max_tokens=1` + `kv_transfer_params{do_remote_decode}` → 取回 KV 参数改发 decode 流式回写）；阶段 2 不做跨副本 failover（KV 只落一地）。
  - 路径决策：字符数/4 粗估 token（chat 解析失败退化 body/3），低于 `path_threshold_prompt_tokens`（默认 256）inline 直连 D 池。
  - 池语义：backend 增加 `role / bootstrap_host / bootstrap_port` 字段；按 `model#role` 命名空间隔离前缀树；P/D 独立探活熔断；`readyz` 双池就绪、`statusz` 加 `pd_mode/prefill_healthy/decode_healthy`；配置校验失败回滚旧配置。
  - 新指标：`router_pd_path_total`、`router_pd_pair_total`、`router_pd_bootstrap_total`、`router_pool_kv_util{role}`。
- **测试**：`test/pd_test.go` 8 场景（inline 路径 / bootstrap 注入与 room 唯一性 / sglang prefill failover 2P+1D / vllm 两段编排参数断言 / vllm prefill failover / readyz 双池 / 配置校验）；mockengine 支持响应额外字段重编码。
- `config/router.yaml` PD 配置段文档与 1P+2D 示例；README PD 章节。

### 变更
- 全仓库 gofmt 清零（6 文件格式对齐 + import 排序）。
- `TestKVCapScale` 并发窗口收紧（eng-a 延迟 200→800ms），消除 pip 编译等 CPU 抢占下的非确定性失败。

### 兼容性
- v0.2 配置零改动可运行：`pd` 段缺省 = PD 关闭，单池形态完全不变。

## [v0.2.0] — 2026-11-22

对标 SGLang SMG / llm-d 生产栈的完整迭代：模型感知路由、请求级重试、混合前缀树、动态发现与热加载、KV 容量感知路由，全部经 mock 单测与 A10×vLLM GPU 端到端回归。

### 新增
- **M1 模型感知路由 + 加权 rendezvous**：按 `model` 筛健康副本池（支持逗号分隔多模型与 `*` 通配），无匹配返回 502（`router_model_nomatch_total`）；`score_i = hash(key,node)^(1/w_i)` 加权亲和与按权重分摊的负载上限。
- **M2 请求级重试 + 熔断错误分类**：客户端未收到首字节前按亲和次序顺延重试一次（SSE 首字节后不重试）；5xx/连接错误计入熔断、4xx 不计（408/429 可配）；`router_retries_total{from,to}`、`router_model_mismatch_total{backend,model}`。
- **M3 混合前缀树路由**：messages 归一化分块哈希链构建 per-model radix 树，`cache_threshold` 以下走派单、命中沿用树记录副本；全局节点上限 + 逻辑时钟 LRU 驱逐；`router_prefix_match_ratio`、`router_prefix_tree_nodes{model}`。
- **M4 动态发现 + 热加载 + HA**：运行时代码"代换装"（原子快照切换，在途请求自然排空，失败回滚）；SIGHUP/文件触发热加载；`POST /admin/backends` 管理 API（token 或 localhost）；`/readyz` 健康语义拆分；`deploy/k8s/` ConfigMap-reloader 等价方案。
- **M5 KV 容量感知路由**：探活顺带抓副本 KV 水位（vLLM `gpu_cache_usage_perc`，指标名可配且自动兼容 `vllm:` 命名空间前缀与标签行）；rendezvous top-k 内高水位分流 + 可选按 KV 余量缩放负载上限；`router_backend_kv_util{backend}`、`router_kv_steered_total`。
- **GPU 端到端回归**：`scripts/gpu_regression.sh` 五场景回归（基线门禁 / 双模型零串池 / 压测中 kill 副本 / KV 分流 / 运行中扩缩副本）+ `scripts/gpu_bootstrap.sh` 引导脚本，随仓库可复现。A10×双副本实测 7/8 门禁通过：S1a sticky=0.985、S1b 命中率 74.4%/76.7%（基线 67.3%）、S2 LoRA 双模型零串池、S3 kill 零失败、S5 热扩缩 PID 不变、S6 readyz 200；S4 因该 vLLM 构建（0.10.x）`kv_cache_usage_perc` 恒 0（仅外部 connector 口径）无法触发，如实标注（见 docs/comparison.md §4）。
- **docs/comparison.md**：与 SGLang SMG / llm-d 生产栈的逐项能力对照（含明确不做与局限清单）。

### 变更
- `config/router.yaml` 新增 `kv:`、`retry:` 配置段；README/README_EN/design/usage 全量更新（含生产配置示例与检查清单）。

### 兼容性
- v0.1 配置文件无需修改即可运行；新增配置段均有缺省值（KV 分流默认开启、水位 0.9、top-k 2）。
