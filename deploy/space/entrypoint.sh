#!/bin/sh
# 创空间启动脚本：把 Secret 注入配置占位符后拉起网关
set -e
CFG=/etc/infer-router/router.yaml
if [ -n "${MODELSCOPE_API_TOKEN:-}" ]; then
  sed -i "s|\${MODELSCOPE_API_TOKEN}|${MODELSCOPE_API_TOKEN}|g" "$CFG"
else
  echo "[entrypoint] WARN: MODELSCOPE_API_TOKEN not set; upstream auth header will be empty"
fi
exec infer-router -config "$CFG"
