#!/usr/bin/env bash
# bootstrap.sh — DSW 侧引导：装 Go → 拉源码分块 → 构建 → 启动 GPU 回归
# 用法: src_urls.txt 逐行放 paste.rs 分块 URL；nohup bash bootstrap.sh &
set -uo pipefail
M=/mnt/workspace/m6
B=/mnt/workspace/infer-router-gpu
mkdir -p "$B/bundle/tools" "$B/bundle/scripts" "$B/results" "$M/src"
mark(){ touch "$M/$1"; }
log(){ echo "[$(date +%T)] $*" >> "$M/bootstrap.log"; }

log "bootstrap start"
if [ ! -x /mnt/workspace/go/bin/go ]; then
  curl -sL --max-time 900 -o /tmp/go.tgz https://golang.google.cn/dl/go1.23.4.linux-amd64.tar.gz && mark GO_TGZ
  log "go tarball downloaded: $(du -h /tmp/go.tgz | awk '{print $1}')"
  tar -C /mnt/workspace -xzf /tmp/go.tgz && mark GO_INSTALLED && log "go installed"
fi
export PATH=/mnt/workspace/go/bin:$PATH
go version >> "$M/bootstrap.log" 2>&1 || { log "GO_INSTALL_FAILED"; mark BOOT_FAIL; exit 1; }

: > "$M/src/joined.b64"
n=0
for u in $(cat "$M/src_urls.txt"); do
  curl -s --max-time 60 "$u" >> "$M/src/joined.b64" || { log "FETCH_FAIL $u"; mark BOOT_FAIL; exit 1; }
  n=$((n+1))
done
log "fetched $n chunks"
mark JOINED
base64 -d "$M/src/joined.b64" > "$M/src.tar.gz" && mark B64D
tar -C "$M/src" -xzf "$M/src.tar.gz" && mark UNTAR
ls "$M/src" >> "$M/bootstrap.log"

cd "$M/src"
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags "-s -w" -o "$B/bundle/infer-router-linux" ./cmd/router \
  >> "$M/bootstrap.log" 2>&1 && mark BUILD_OK || { log "BUILD_FAILED"; mark BUILD_FAIL; mark BOOT_FAIL; }
cp tools/load_test.py tools/compare_cache.py "$B/bundle/tools/"
cp scripts/gpu_regression.sh "$B/bundle/scripts/"
chmod +x "$B/bundle/infer-router-linux"
log "binary: $(du -h "$B/bundle/infer-router-linux" 2>/dev/null | awk '{print $1}')"

nohup bash "$B/bundle/scripts/gpu_regression.sh" > "$B/results/stdout.log" 2>&1 &
mark REGRESSION_LAUNCHED
log "regression launched"
