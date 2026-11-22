#!/usr/bin/env bash
# K8s Endpoints → infer-router 配置热更（reloader）。
# 与推理网关自带 --service-discovery 的能力对齐：后端增减由本脚本渲染进 ConfigMap，
# 再经 SIGHUP（或管理 API）触发热更，路由进程零重启。建议 CronJob/循环部署。
#
# 依赖：kubectl、curl；RBAC 需 get endpoints、patch configmap、exec 进 Pod（走 SIGHUP 时）。
set -euo pipefail

NS="${NS:-default}"
SVC="${SVC:-llm-backends}"          # 后端引擎 Service（Endpoints 来源）
PORT="${PORT:-8000}"                # 引擎端口
DEPLOY="${DEPLOY:-infer-router}"    # 路由 Deployment 名
CM="${CM:-infer-router-config}"
ADMIN_SVC="${ADMIN_SVC:-infer-router}"   # 管理接口地址（走 admin API 时使用）
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
MODEL="${MODEL:-\*}"

# 1) 读取就绪 Endpoints 地址
mapfile -t IPS < <(kubectl -n "$NS" get endpoints "$SVC" -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}')
if [ "${#IPS[@]}" -eq 0 ]; then
  echo "no ready endpoints for $SVC, keep current config" >&2
  exit 0
fi

# 2) 渲染 backends 段
BACKENDS=""
for ip in "${IPS[@]}"; do
  BACKENDS+="      - name: ep-${ip//./-}
        base_url: http://${ip}:${PORT}
        model: \"${MODEL}\"
        weight: 1
"
done

# 3) 渲染完整配置并 patch ConfigMap（ConfigMap volume 经 kubelet 原子同步进 Pod）
CFG="addr: \":8080\"
strategy: prefix_affinity
health_interval_s: 3
affinity:
  alpha: 1.25
  sticky_window: 16384
backends:
${BACKENDS}"

kubectl -n "$NS" patch configmap "$CM" --type merge -p "{\"data\":{\"config.yaml\":$(printf '%s' "$CFG" | jq -Rs .)}}"
echo "configmap $CM updated with ${#IPS[@]} endpoints"

# 4) 触发热更：优先 SIGHUP（ConfigMap 挂载文件已更新）；或改用管理 API（需 admin_token）
#    a) SIGHUP：kubectl -n "$NS" exec deploy/"$DEPLOY" -- kill -HUP 1
#    b) admin API（示例，按 name 逐个对账增删）：
#       curl -s -X POST "http://${ADMIN_SVC}/admin/backends" \
#            -H "Authorization: Bearer ${ADMIN_TOKEN}" \
#            -d '{"op":"add","backend":{"name":"ep-1","base_url":"http://10.0.0.11:8000","weight":1}}'
kubectl -n "$NS" exec deploy/"$DEPLOY" -- kill -HUP 1
echo "SIGHUP sent to $DEPLOY"
