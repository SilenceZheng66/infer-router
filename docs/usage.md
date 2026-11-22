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
health_interval_s: 5             # 副本探活周期
breaker:
  fail_threshold: 3              # 连续失败次数 → 打开熔断
  cooldown_s: 15                 # 熔断冷却后半开探测
affinity:                        # 仅 prefix_affinity 生效
  alpha: 1.25                    # bounded load 系数：cap = ceil(alpha × 平均在途)
  sticky_window: 4096            # 粘性统计环形窗口容量
backends:
  - name: vllm-a
    base_url: http://127.0.0.1:8001   # 不含 /v1，路由层原样透传 /v1/* 路径
    model: qwen2.5-1.5b
    weight: 1
```

## 3. 会话 key 约定

- 客户端带 `X-Session-Id` header → 直接作为会话 key（推荐，最稳定）。
- 未携带 → 取 `messages` 前缀（system 全文 + 首条 user 全文）哈希。多轮对话中该前缀不变，效果等价。
- 两者都不可用（如空 messages）→ 退化为 RR。

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

## 6. 指标接入

- Prometheus scrape `:8080/metrics`；Grafana 常用查询：
  - 负载分布：`sum by (backend) (rate(router_requests_total[1m]))`
  - 粘性率：`router_affinity_sticky_ratio`
  - 超限顺延：`rate(router_affinity_overload_fallback_total[1m])`
- `/statusz` 适合接入存活探针与发布检查。

## 7. 常见问题

- **副本启动后 statusz 一直 healthy 0**：检查 `base_url` 是否含 `/v1`（不应含）；探活用的是 `{base_url}/health`。
- **SSE 客户端收不到流**：确认客户端未开响应缓冲；路由层 `FlushInterval=-1` 已逐块转发。
- **命中率上不去**：先看 KV 容量（`nvidia-smi` 显存 × gpu-memory-utilization 换算 token 容量），LRU 驱逐主导时调大副本显存或减少会话并发；再看 `router_affinity_overload_fallback_total` 是否过高（调大 alpha）。
