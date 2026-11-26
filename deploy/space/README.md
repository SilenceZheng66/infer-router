---
title: Infer Router Demo
emoji: "\U0001F6A6"
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# infer-router 创空间部署（Docker）

一个单副本 infer-router 网关，上游指向 ModelScope API-Inference（OpenAI 兼容接口），
通过 `inject_headers` 由网关侧注入 `Authorization: Bearer` 凭证。

## 使用

网关监听 `:7860`，按 OpenAI 兼容形态透传 `/v1/*`：

```bash
BASE=https://<your-space>.modelscope.cn
curl $BASE/healthz
curl $BASE/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "Qwen/Qwen2.5-7B-Instruct",
  "messages": [{"role":"user","content":"你好"}]
}'
```

## 配置

- `config.yaml`：上游 `api-inference.modelscope.cn`，模型 `Qwen/Qwen2.5-7B-Instruct`（可在 Space 仓库内直接修改）。
- Token：Space 的 **Secrets** 中配置 `MODELSCOPE_API_TOKEN`，`entrypoint.sh` 启动时注入配置，
  凭证不落仓库、不出镜像构建缓存。

## 说明

- 管理接口（`/admin/*`）未配置令牌，默认仅允许容器本机 `localhost` 调用，公网请求一律拒绝。
- 指标端点 `GET /metrics`（Prometheus 文本）；健康检查 `GET /healthz`、`GET /readyz`。
