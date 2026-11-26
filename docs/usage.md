# infer-router 使用文档

## 1. 构建

```bash
# 本机构建（Go 1.23+）
CGO_ENABLED=0 go build -o infer-router ./cmd/router

# Docker
docker build -t infer-router -f deploy/Dockerfile .
docker run -p 8080:8080 -v $(pwd)/config:/app/config infer-router -config /app/config/router.yaml
```

产物为静态二进制（约 8.3MB），可任意拷贝到同架构机器直接运行。

## 2. 配置项

```yaml
addr: ":8080"                    # 监听地址
strategy: prefix_affinity        # round_robin | least_queue | prefix_affinity
admin_token: ""                  # 管理 API 令牌；留空 = 仅接受 localhost 调用
health_interval_s: 5             # 副本探活周期
breaker:
  fail_threshold: 3              # 连续失败次数 → 打开熔断
  cooldown_s: 15                 # 熔断冷却后半开探测
affinity:                        # 仅 prefix_affinity 生效
  alpha: 1.25                    # bounded load 系数：cap = ceil(alpha × 平均在途)
  sticky_window: 4096            # 粘性统计环形窗口容量
  tree:                          # 混合前缀树（缺省开启，仅 prefix_affinity 生效）
    enabled: true                # false = 退回 affinity-only（无树）
    max_tree_size: 65536         # 全局节点上限（跨模型求和），超限 LRU 驱逐
    eviction_interval_s: 60      # 周期驱逐
    cache_threshold: 0.5         # 树命中判定比例（匹配深度/链长）
    chunk_chars: 256             # messages 归一化分块粒度（rune）
retry:                           # 请求级 failover（缺省开启，重试一次）
  enabled: true                  # false = 5xx 直接透传客户端
  max: 1                         # 额外重试次数
  retry_on_5xx: true
  retry_on_connect_error: true
kv:                              # KV 容量感知分流（缺省开启，仅 prefix_affinity 生效）
  enabled: true                  # false = 完全关闭分流
  high_watermark: 0.9            # 首选副本 KV 超过此值时分流到余量大者
  top_k: 2                       # 分流决策窗口（rendezvous 前 k 候选）
  metric_name: gpu_cache_usage_perc  # 副本 KV 指标名（vLLM 缺省；自动兼容 vllm: 前缀与 {label} 标签行，SGLang 按需替换）
  cap_scale: false               # 负载上限按 KV 余量缩放（默认关）
backends:
  - name: vllm-a
    base_url: http://127.0.0.1:8001   # 不含 /v1，路由层原样透传 /v1/* 路径
    model: qwen2.5-1.5b               # 该副本服务的模型；逗号分隔多模型（如 "qwen2.5-1.5b,qwen2.5-7b"）或 "*" 通配
    weight: 1                         # 亲和排序与负载上限分摊权重（≥1），2 表示承担 2 倍流量
```

多模型混管示例（一个路由实例管两个模型池）：

```yaml
strategy: prefix_affinity
backends:
  - { name: slm-a, base_url: "http://slm-a.svc:8001", model: qwen2.5-1.5b, weight: 2 }
  - { name: slm-b, base_url: "http://slm-b.svc:8001", model: qwen2.5-1.5b, weight: 1 }
  - { name: llm-a, base_url: "http://llm-a.svc:8001", model: qwen2.5-7b,   weight: 1 }
# 请求 model=qwen2.5-1.5b 只会路由到 slm-*；model=qwen2.5-7b 只到 llm-*
# 无副本服务请求的模型 → 502 + router_model_nomatch_total{model} 计数
```

## 3. 会话 key 约定

- 客户端带 `X-Session-Id` header → 直接作为会话 key（推荐，最稳定）。
- 未携带 → 取 `messages` 前缀（system 全文 + 首条 user 全文）哈希。多轮对话中该前缀不变，效果等价。
- 两者都不可用（如空 messages）→ 退化为 RR。
- 会话亲和 key 含模型维度：同一会话切换 model 视为不同亲和目标，不会把请求钉在与该模型无关的副本上。

## 4. 与 vLLM 组合部署

```bash
# 副本 A / B（同一 GPU 错峰启动，避免显存初始化争抢）
vllm serve /path/qwen2.5-1.5b --served-model-name qwen2.5-1.5b \
  --port 8001 --gpu-memory-utilization 0.38 --max-model-len 8192 &
sleep 90
vllm serve /path/qwen2.5-1.5b --served-model-name qwen2.5-1.5b \
  --port 8002 --gpu-memory-utilization 0.38 --max-model-len 8192 &

# 路由层
./infer-router -config config/router.yaml
```

客户端把 base_url 从 `http://8001` 换成 `http://8080` 即完成接入，协议无任何改动。

## 5. 压测与对照实验复现

### 5.1 递增对话压测（tools/load_test.py）

```bash
# affinity 组（router 端 strategy=prefix_affinity）
python tools/load_test.py http://127.0.0.1:8080 affinity 40 6 8 s1 0.3 | tee result_aff.json

# RR 对照组（改 config strategy=round_robin 并重启 router）
python tools/load_test.py http://127.0.0.1:8080 round_robin 40 6 8 s2 0.3 | tee result_rr.json
```

参数：`<base_url> <mode_tag> [sessions] [turns] [conc] [sess_prefix] [jitter_s]`。
要点：**两组必须用不同 sess_prefix**（避免前一批实验的 cache 残留污染），jitter 建议开启（否则 RR 在同步波次下出现隐性粘性）。

### 5.2 命中率采集（tools/compare_cache.py）

```bash
# 基线（压测前）：每个副本 2 行，共 4 行
curl -s http://127.0.0.1:8001/metrics | grep ":prefix_cache_\(queries\|hits\)_total" | grep -v "#" >> cb.txt
curl -s http://127.0.0.1:8002/metrics | grep ":prefix_cache_\(queries\|hits\)_total" | grep -v "#" >> cb.txt

# ……跑压测……

# 终值（压测后）：再追加 4 行
curl -s http://127.0.0.1:8001/metrics | grep ":prefix_cache_\(queries\|hits\)_total" | grep -v "#" >> cb.txt
curl -s http://127.0.0.1:8002/metrics | grep ":prefix_cache_\(queries\|hits\)_total" | grep -v "#" >> cb.txt

# 解析差值命中率（含 Δhits > Δqueries 单调性异常检查）
python tools/compare_cache.py cb.txt affinity
```

### 5.3 降级演练

```bash
# 杀掉副本 B
pkill -f "port 8002"
# 5s 内 probe 摘除：curl :8080/statusz → {"healthy":1,...}
# 跑流量：全部落副本 A，0 失败
python tools/load_test.py http://127.0.0.1:8080 degraded 10 3 5 e1 0.2
# 恢复：重新启动副本 B，probe 自动加回
```

### 5.4 GPU 五场景回归（scripts/gpu_regression.sh）

`scripts/gpu_regression.sh` 是面向真实 vLLM 副本的端到端回归 runbook，一条命令跑完五个场景并自动落盘门禁结果：

| 场景 | 做法 | 门禁 |
| --- | --- | --- |
| S1 标准负载门禁 | 40×6 递增对话，先同步后 jitter | sticky≥0.95；vLLM `prefix_cache` 计数差值命中率≥67.3% |
| S2 双模型混池零串池 | 副本 B 声明 base+LoRA 双模型（自动探测 adapter），20 发跨池请求 | 请求全部落 B、A 增量=0、`router_model_mismatch_total` 不增；未声明模型→502 |
| S3 压测中途 kill 副本 | 压测进行到 t+20s 时 TERM 副本 B | 客户端 0 失败（请求级 failover），重试计数如实记录 |
| S4 异构 KV 分流 | 直灌副本 A 至 KV 水位 ≥0.9（`kv.metric_name` 指定的 gauge），再发长前缀会话 | `router_kv_steered_total≥1`，改派目标增量 > 被灌副本；**要求引擎暴露真实 GPU 块水位 gauge**（vLLM 0.9.x 实测为 `vllm:gpu_cache_usage_perc` / `vllm:kv_cache_usage_perc`，带命名空间前缀，metric_name 冒号后缀匹配自动兼容；0.10.x 的 `kv_cache_usage_perc` 仅统计外部 KV connector、恒 0，不可用） |
| S5 运行中扩缩副本 | 管理 API add/remove + SIGHUP 热更 | backends/healthy 数值即时变化且路由进程 PID 不变 |

```bash
# 前置：GPU 机器上已装 vLLM，模型权重缓存在 $MODEL_PATH，路由二进制与工具在 $WS/bundle
bash scripts/gpu_regression.sh   # 结果与 run.log 落在 $WS/results，门禁汇总在 summary.txt
```

环境变量可覆盖：`WS`（工作区）、`MODEL_PATH`（权重路径）、`MODEL_DECL`（对路由层声明的模型名）、`BIN`（路由二进制）。脚本自带阶段 marker（`PH_S0`…`PH_UPLOAD`）与失败 marker（`BOOT_FAIL`/`BUILD_FAIL`），便于无人值守运行时观测进度；结果文件可逐个经 HTTP POST 回传（见脚本 `upload_one`）。

## 6. 指标接入

- Prometheus scrape `:8080/metrics`；Grafana 常用查询：
  - 负载分布：`sum by (backend) (rate(router_requests_total[1m]))`
  - 粘性率：`router_affinity_sticky_ratio`
  - 超限顺延：`rate(router_affinity_overload_fallback_total[1m])`
  - 模型路由错误：`rate(router_model_nomatch_total[1m]) > 0` → 检查请求 model 拼写与 backends 的 model 声明
  - 模型声明不符：`rate(router_model_mismatch_total[1m]) > 0` → 副本实际 serving 与配置声明漂移
  - 重试率：`rate(router_retries_total[1m])` 持续偏高 → 有副本半病态（探活未到阈值但请求失败），检查该副本 GPU/进程状态
  - 前缀树命中率：`router_prefix_match_ratio` → 混合前缀树的有效性自证；走 `X-Session-Id` 快路径的流量不计数
  - 树规模：`router_prefix_tree_nodes` 接近 `max_tree_size` → 驱逐压力偏高，调大上限或检查会话基数
  - KV 水位：`router_backend_kv_util` 任一副本持续 > 0.9 → KV 容量瓶颈（粘性收益上限），扩显存/降并发
  - 分流量：`rate(router_kv_steered_total[1m])` 持续偏高 → 有副本长期高水位，检查其 KV 容量与会话分布
- `/statusz` 适合接入存活探针与发布检查。

## 7. 生产部署检查清单

- [ ] 路由层与副本同可用区，探活走内网（`health_interval_s: 3`）
- [ ] 客户端统一携带 `X-Session-Id`（比 messages 前缀提取更稳定，且省一次 body 解析）
- [ ] Prometheus 抓取 `:8080/metrics`：`router_affinity_sticky_ratio < 0.9` 告警（检查会话 key 约定）；`router_affinity_overload_fallback_total` 持续增长 → 调大 `alpha` 或扩副本
- [ ] 混合前缀树监控：`router_prefix_tree_nodes` 设上限告警；`router_prefix_match_ratio` 随会话基数下滑 → 调大 `max_tree_size` 或 `chunk_chars`
- [ ] KV 分流：确认引擎暴露真实 GPU KV 块水位 gauge（vLLM 0.8/0.9 `gpu_cache_usage_perc`；0.10.x 的 `kv_cache_usage_perc` 恒 0、不可用）→ `kv.metric_name`（自动兼容 `vllm:` 命名空间前缀与标签行）；`router_backend_kv_util` 应为非零正值
- [ ] 副本 KV 容量核算：`gpu-memory-utilization × 显存 - 模型权重` 折算 token 容量，会话前缀总需求超容量时 LRU 驱逐会稀释粘性收益（见 README 实测数据）
- [ ] systemd/Kubernetes 托管静态二进制（约 1 vCPU / 64MB），配 liveness probe 打 `/statusz`、readiness probe 打 `/readyz`
- [ ] 滚动更新副本时 rendezvous 只迁移 1/n 会话，无需路由层重启

## 8. 常见问题

- **副本启动后 statusz 一直 healthy 0**：检查 `base_url` 是否含 `/v1`（不应含）；探活用的是 `{base_url}/v1/models`（OpenAI 兼容端点，vLLM/SGLang 均原生提供）。
- **SSE 客户端收不到流**：确认客户端未开响应缓冲；路由层 `FlushInterval=-1` 已逐块转发。
- **命中率上不去**：先看 KV 容量（`nvidia-smi` 显存 × gpu-memory-utilization 换算 token 容量），LRU 驱逐主导时调大副本显存或减少会话并发；再看 `router_affinity_overload_fallback_total` 是否过高（调大 alpha）。

## 9. 动态发现与热更（v0.2.0）

### 9.1 运行中增删副本（管理 API）

```bash
# 未配置 admin_token：仅接受 localhost 调用
curl -s -X POST http://127.0.0.1:8080/admin/backends \
  -d '{"op":"add","backend":{"name":"vllm-c","base_url":"http://10.0.0.13:8000","model":"qwen2.5-1.5b","weight":1}}'
# 配置了 admin_token：Authorization: Bearer <token>（或 X-Admin-Token 头）
curl -s -X POST http://127.0.0.1:8080/admin/backends -H "Authorization: Bearer $TOKEN" \
  -d '{"op":"remove","backend":{"name":"vllm-c"}}'
```

生效为原子换装：`/statusz` 即时体现、零重启；被摘副本上处理中的请求自然排空后释放。非法输入（重名/未知副本/缺字段）返回 4xx 且不触碰运行态。

### 9.2 整份配置热更（SIGHUP）

修改配置文件后 `kill -HUP <pid>`：解析成功原子生效（副本集/策略/affinity/retry 全部可热更），失败保留旧配置继续服务并输出日志。

### 9.3 Kubernetes 动态发现（deploy/k8s/）

`configmap.yaml` + `deployment.yaml` + `reloader.sh`：reloader 脚本从引擎 Service 的 Endpoints 渲染 backends 进 ConfigMap，再对 Pod 发 SIGHUP（或改用管理 API）。与带 `--service-discovery` 的推理网关能力对齐（轮询脚本机制，可后续升级 client-go watch）。副本扩缩容全程不重启路由进程。

### 9.4 探针

- readiness：`/readyz`——有可用副本 200，全副本摘除 503（K8s readinessProbe 同步摘除本 Pod 流量）
- liveness：`/statusz`——进程存活即 200
