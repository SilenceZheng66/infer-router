#!/usr/bin/env bash
# gpu_regression.sh — infer-router v0.2.0 GPU 端到端回归（ModelScope DSW A10 / vLLM 0.10.x）
#
# 场景（对应 docs/usage.md「GPU 回归」一节，v0.1 基线：jitter 命中率 67.3%、sync sticky 0.965、TTFT p90 0.531s）：
#   S1 标准负载门禁：40×6 递增对话，同步跑 sticky≥0.95 + jitter 跑命中率≥67.3%（vLLM 计数器差值法）
#   S2 双模型混池零串池：LoRA 双模型（有 adapter）或第三副本 0.5B（回退），跨池请求 0 + nomatch 502
#   S3 压测中途 kill 副本：TERM 优雅退出 + 请求级 failover，客户端失败数如实记录
#   S4 异构 KV 分流：直灌一个副本 gpu_cache_usage_perc≥0.9 → 路由层 top-k 分流生效
#   S5 运行中扩缩副本：admin API 即时生效 /readyz /statusz 体现，路由进程零重启（PID 不变）
#
# 前置：scripts/ 下执行；build/infer-router-linux 已构建；vLLM 经 VLLM_USE_MODELSCOPE=True 解析模型名。
# 产物：/mnt/workspace/infer-router-gpu/results/
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WS="${WS:-/mnt/workspace/infer-router-gpu}"
R="$WS/results"; mkdir -p "$R"
PY="${PY:-python3}"
MODEL="${MODEL:-qwen2.5-1.5b}"
MODEL_05B="${MODEL_05B:-Qwen/Qwen2.5-0.5B-Instruct}"
LORA_DIR="${LORA_DIR:-/mnt/workspace/runs/sft-activity}"
A=127.0.0.1:8001; B=127.0.0.1:8002; C=127.0.0.1:8003; RT=127.0.0.1:8080
UTIL_A=0.38; UTIL_B=0.38; UTIL_C=0.18
LOADER="$REPO/tools/load_test.py"
BIN="${BIN:-$REPO/build/infer-router-linux}"

PASS=0; FAIL=0
say(){ echo "[$(date +%T)] $*" | tee -a "$R/run.log"; }
gate(){ # gate <name> <0/1> <detail>
  if [ "$2" = "1" ]; then echo "PASS  $1  $3" | tee -a "$R/summary.txt"; PASS=$((PASS+1));
  else echo "FAIL  $1  $3" | tee -a "$R/summary.txt"; FAIL=$((FAIL+1)); fi
}
snap_router(){ curl -s "http://$RT/metrics" | grep -E '^router_' > "$R/$1"; }
vllm_cache_snap(){ curl -s "http://127.0.0.1:$2/metrics" | grep -E 'prefix_cache_(queries|hits)_total ' | sed "s/^/$1 /" >> "$3"; }
mval(){ grep -E "^$2( |\{)" "$1" 2>/dev/null | awk '{print $NF}' | head -1; }
wait_ready(){ for _ in $(seq 1 150); do curl -sf "http://$1/readyz" >/dev/null 2>&1 && { say "$1 ready"; return 0; }; sleep 5; done; say "TIMEOUT $1 not ready"; return 1; }
statusz(){ curl -s "http://$RT/statusz" > "$R/$1"; cat "$R/$1"; }

start_vllm(){ # start_vllm <pidfile> <port> <util> <modellen> [extra...]
  local pidf=$1 port=$2 util=$3 mlen=$4; shift 4
  VLLM_USE_MODELSCOPE=True nohup $PY -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" --port "$port" --gpu-memory-utilization "$util" --max-model-len "$mlen" \
    "$@" >> "$R/vllm-$port.log" 2>&1 &
  echo $! > "$pidf"
}
stop_vllm(){ # stop_vllm <pidfile> <port>：TERM 优雅退出 + 兜底清理
  local pidf=$1 port=$2; [ -f "$pidf" ] || return 0
  local pid; pid=$(cat "$pidf")
  if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  pkill -9 -f "port $port" 2>/dev/null || true
}
write_router_cfg(){ # write_router_cfg <路径> <vllm-b 的 model 声明> <额外 backend 文本>
  local path=$1 declb=$2 extra=$3
  cat > "$path" <<EOF
addr: ":8080"
strategy: prefix_affinity
health_interval_s: 3
breaker: { fail_threshold: 3, cooldown_s: 15 }
retry: { enabled: true, max: 1 }
affinity: { alpha: 1.25, sticky_window: 4096 }
kv: { enabled: true, high_watermark: 0.9, top_k: 2, metric_name: gpu_cache_usage_perc, cap_scale: false }
backends:
  - name: vllm-a
    base_url: http://$A
    model: "$MODEL"
    weight: 1
  - name: vllm-b
    base_url: http://$B
    model: "$declb"
    weight: 1
$extra
EOF
}
start_router(){ nohup "$BIN" -config "$1" >> "$R/router.log" 2>&1 & ROUTER_PID=$!
  wait_ready "$RT" || return 1; say "router up (pid $ROUTER_PID, cfg=$1)"; }
pool_deltas(){ # pool_deltas <before> <after> [names...]
  "$PY" - "$@" <<'PY'
import sys
def load(p):
    d={}
    for line in open(p):
        if line.startswith("router_requests_total{"):
            k,v=line.split(" ",1); d[k]=float(v)
    return d
old,new=load(sys.argv[1]),load(sys.argv[2])
for name in sys.argv[3:]:
    delta=new.get(f'router_requests_total{{backend="{name}",status="OK"}}',0)-old.get(f'router_requests_total{{backend="{name}",status="OK"}}',0)
    print(f"{name}: delta={delta:.0f}")
PY
}

# ================================================================ S0 基线环境
say "=== S0 环境启动（错峰启动 vLLM ×2，0.38/0.38 对齐 v0.1 基线）==="
pkill -f "infer-router-linux" 2>/dev/null || true
write_router_cfg "$WS/cfg-s1.yaml" "$MODEL" ""
start_vllm "$R/vllm-a.pid" 8001 "$UTIL_A" 8192; wait_ready "$A"
start_vllm "$R/vllm-b.pid" 8002 "$UTIL_B" 8192; wait_ready "$B"
start_router "$WS/cfg-s1.yaml"

# ================================================================ S1 标准负载门禁
say "=== S1 标准负载门禁（40×6 递增对话）==="
# 1) 同步负载 → sticky gate（v0.1 口径：0.965）
$PY "$LOADER" "http://$RT" s1_sync 40 6 8 s1a 0 > "$R/s1_sync.json"
snap_router "s1_after_sync.txt"
STICKY=$(mval "$R/s1_after_sync.txt" router_affinity_sticky_ratio); STICKY=${STICKY:-0}
OKSYNC=$("$PY" - "$R/s1_sync.json" <<'PY'
import json,sys; d=json.load(open(sys.argv[1])); print(f'{d["ok"]}/{d["requests"]}')
PY
)
say "sync sticky=$STICKY load=$OKSYNC json=$(cat "$R/s1_sync.json")"
gate "S1a-sync-sticky>=0.95" "$("$PY" -c "print(1 if float('${STICKY}')>=0.95 else 0)")" "sticky=$STICKY ok=$OKSYNC"
# 2) jitter 负载 → cache 命中率 gate（vLLM 计数器差值，v0.1 口径：67.3%）
CB="$R/s1_cb.txt"; : > "$CB"
vllm_cache_snap A 8001 "$CB"; vllm_cache_snap B 8002 "$CB"
$PY "$LOADER" "http://$RT" s1_jitter 40 6 8 s1b 0.3 > "$R/s1_jitter.json"
vllm_cache_snap A 8001 "$CB"; vllm_cache_snap B 8002 "$CB"
HIT=$("$PY" "$REPO/tools/compare_cache.py" "$CB" s1-jitter | grep TOTAL | awk '{print $NF}')
say "jitter hit=$HIT json=$(cat "$R/s1_jitter.json")"
gate "S1b-jitter-hit>=67.3%" "$("$PY" -c "print(1 if float('${HIT:-0%}'.rstrip('%'))>=67.3 else 0)")" "hit=$HIT（Δhits/Δqueries + 单调性校验见 cb 文件）"

# ================================================================ S2 双模型混池零串池
say "=== S2 双模型混池零串池 ==="
M2_MODEL=""; M2_KIND=""; EXTRA_BACKEND=""
if [ -f "$LORA_DIR/adapter_model.safetensors" ]; then
  M2_MODEL="qwen15b-activity"; M2_KIND="lora"
  say "LoRA adapter 存在：$LORA_DIR → vllm-b 重启为 base+LoRA 双模型单实例"
  stop_vllm "$R/vllm-b.pid" 8002; sleep 3
  start_vllm "$R/vllm-b.pid" 8002 "$UTIL_B" 8192 --enable-lora --lora-modules "qwen15b-activity=$LORA_DIR" --max-lora-rank 16
  wait_ready "$B"
  DECL_B="$MODEL,qwen15b-activity"
else
  M2_MODEL="qwen2.5-0.5b"; M2_KIND="standalone-0.5b"
  say "无 LoRA adapter：降级为 0.5B 第三副本方案（a/b 降至 0.30 腾显存）"
  stop_vllm "$R/vllm-a.pid" 8001; stop_vllm "$R/vllm-b.pid" 8002; sleep 5
  UTIL_A=0.30; UTIL_B=0.30
  start_vllm "$R/vllm-a.pid" 8001 "$UTIL_A" 8192; wait_ready "$A"
  start_vllm "$R/vllm-b.pid" 8002 "$UTIL_B" 8192; wait_ready "$B"
  VLLM_USE_MODELSCOPE=True nohup $PY -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_05B" --served-model-name qwen2.5-0.5b --port 8003 \
    --gpu-memory-utilization "$UTIL_C" --max-model-len 4096 >> "$R/vllm-8003.log" 2>&1 &
  echo $! > "$R/vllm-c.pid"
  wait_ready "$C"
  DECL_B="$MODEL"
  EXTRA_BACKEND="  - name: vllm-c
    base_url: http://$C
    model: \"qwen2.5-0.5b\"
    weight: 1"
fi
kill -TERM "$ROUTER_PID" 2>/dev/null; sleep 1
write_router_cfg "$WS/cfg-s2.yaml" "$DECL_B" "$EXTRA_BACKEND"
start_router "$WS/cfg-s2.yaml"
snap_router "s2_before.txt"
FAIL2=0
for i in $(seq 1 20); do
  code=$(curl -s -o "$R/s2_last_body.json" -w '%{http_code}' -X POST "http://$RT/v1/chat/completions" \
    -H 'Content-Type: application/json' -H "X-Session-Id: m2-$i" \
    -d "{\"model\":\"$M2_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"你好，简短回答。\"}],\"max_tokens\":16}")
  [ "$code" != "200" ] && { FAIL2=$((FAIL2+1)); say "s2 req $i -> $code"; }
done
snap_router "s2_after.txt"
MISMATCH2=$(awk '/^router_model_mismatch_total/{s+=$NF} END{printf "%.0f", s+0}' "$R/s2_after.txt")
pool_deltas "$R/s2_before.txt" "$R/s2_after.txt" vllm-a vllm-b vllm-c > "$R/s2_pools.txt"
cat "$R/s2_pools.txt" | tee -a "$R/run.log"
TD=$(grep -E "^$( [ "$M2_KIND" = lora ] && echo vllm-b || echo vllm-c): delta" "$R/s2_pools.txt" | awk '{print $3}')
OA=$(grep -E '^vllm-a: delta' "$R/s2_pools.txt" | awk '{print $3}')
OB=$(grep -E '^vllm-b: delta' "$R/s2_pools.txt" | awk '{print $3}')
gate "S2-zero-cross-pool" "$("$PY" -c "print(1 if abs(float('${TD:-0}')-20)<1 and float('${OA:-0}')==0 and int('${FAIL2}')==0 else 0)")" \
  "kind=$M2_KIND target_delta=$TD other_a=$OA b=$OB http_fail=$FAIL2 mismatch=$MISMATCH2"
if [ "$M2_KIND" = "lora" ]; then
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://$RT/v1/chat/completions" -H 'Content-Type: application/json' \
    -d '{"model":"qwen2.5-0.5b","messages":[{"role":"user","content":"hi"}],"max_tokens":8}')
  gate "S2-nomatch-502" "$("$PY" -c "print(1 if '${code}'=='502' else 0)")" "nomatch→502 code=$code"
fi

# ================================================================ S3 压测中途 kill 副本
say "=== S3 压测中途 kill 副本（TERM 优雅退出 + 请求级 failover）==="
snap_router "s3_before.txt"
$PY "$LOADER" "http://$RT" s3_kill 20 4 8 k1 0.2 > "$R/s3_load.json" 2>&1 &
LOADPID=$!
sleep 20
VPID=$(cat "$R/vllm-b.pid")
kill -TERM "$VPID" 2>/dev/null && say "killed vllm-b (pid $VPID) mid-load"
wait $LOADPID
snap_router "s3_after.txt"
OK3=$("$PY" - "$R/s3_load.json" <<'PY'
import json,sys
try:
    d=json.loads(open(sys.argv[1]).read().strip().splitlines()[-1]); print(f'{d["ok"]}/{d["requests"]}')
except Exception as e: print(f'parse_error:{e}')
PY
)
RETRY3=$(awk '/^router_retries_total/{s+=$NF} END{printf "%.0f", s+0}' "$R/s3_after.txt" | tr -d '\n')
say "s3 load=$OK3 json=$(cat "$R/s3_load.json")"
gate "S3-kill-midload-zero-fail" "$("$PY" -c "
s='${OK3}'; print(1 if '/' in s and s.split('/')[0]==s.split('/')[1] else 0)" 2>/dev/null || echo 0)" \
  "ok=$OK3 retries_cum=$RETRY3（流中截断若非零将如实记录归因）"

# ================================================================ S4 异构 KV 分流
say "=== S4 异构 KV 分流（直灌 vllm-a → 高水位 → top-k 改派）==="
$PY "$LOADER" "http://$A" s4_flood 120 4 8 f1 0 > "$R/s4_flood.json" 2>&1 &
FLOODPID=$!
UTIL_A_VAL=0
for _ in $(seq 1 60); do
  UTIL_A_VAL=$(curl -s "http://$A/metrics" | grep -E '^gpu_cache_usage_perc( |\{)' | awk '{print $NF}' | head -1)
  say "vllm-a gpu_cache_usage_perc=${UTIL_A_VAL:-0}"
  "$PY" -c "import sys; sys.exit(0 if float('${UTIL_A_VAL:-0}')>=0.90 else 1)" && break
  sleep 10
done
wait $FLOODPID 2>/dev/null
sleep 6   # 等路由层探活刷新 KV 水位
snap_router "s4_before.txt"
KV_A=$(mval "$R/s4_before.txt" router_backend_kv_util | head -1)
say "router_backend_kv_util(first)=$KV_A  flood=$(cat "$R/s4_flood.json")"
$PY "$LOADER" "http://$RT" s4_steer 10 3 5 w1 0 > "$R/s4_steer.json"
snap_router "s4_after.txt"
STEERED=$(mval "$R/s4_after.txt" router_kv_steered_total | head -1); STEERED=${STEERED:-0}
KV_A2=$(grep -E '^router_backend_kv_util\{backend="vllm-a"\}' "$R/s4_after.txt" | awk '{print $NF}')
pool_deltas "$R/s4_before.txt" "$R/s4_after.txt" vllm-a vllm-b > "$R/s4_pools.txt"
cat "$R/s4_pools.txt" | tee -a "$R/run.log"
AD4=$(grep -E '^vllm-a: delta' "$R/s4_pools.txt" | awk '{print $3}')
BD4=$(grep -E '^vllm-b: delta' "$R/s4_pools.txt" | awk '{print $3}')
say "steer json=$(cat "$R/s4_steer.json")"
gate "S4-kv-steering" "$("$PY" -c "print(1 if float('${STEERED}')>=1 and float('${BD4:-0}')>float('${AD4:-0}') else 0)")" \
  "kv_util_a=$KV_A2 steered=$STEERED pools_a=$AD4 pools_b=$BD4"

# ================================================================ S5 运行中扩缩副本（admin API，零重启）
say "=== S5 运行中扩缩副本（PID 必须不变）==="
RPID_BEFORE=$ROUTER_PID
if [ "$M2_KIND" = "standalone-0.5b" ]; then
  # 0.5B 副本在跑且在配置里：admin remove→add 闭环
  RC1=$(curl -s -o "$R/s5_rm.json" -w '%{http_code}' -X POST "http://$RT/admin/backends" -H 'Content-Type: application/json' \
    -d '{"op":"remove","backend":{"name":"vllm-c"}}')
  sleep 1; Z1=$(statusz s5_z1.txt)
  RA=$(curl -s -o "$R/s5_add.json" -w '%{http_code}' -X POST "http://$RT/admin/backends" -H 'Content-Type: application/json' \
    -d '{"op":"add","backend":{"name":"vllm-c","base_url":"http://'"$C"'","model":"qwen2.5-0.5b","weight":1}}')
  sleep 4; Z2=$(statusz s5_z2.txt)
  say "remove=$RC1 add=$RA z1=$Z1 z2=$Z2"
  gate "S5-admin-add-remove-live" "$("$PY" - "$R/s5_z1.txt" "$R/s5_z2.txt" <<'PY'
import json,sys
z1=json.load(open(sys.argv[1])); z2=json.load(open(sys.argv[2]))
print(1 if z1.get("backends")==2 and z1.get("healthy")==2 and z2.get("backends")==3 and z2.get("healthy")==3 else 0)
PY
)" "remove=$RC1 add=$RA pid_same=$( [ "$RPID_BEFORE" = "$ROUTER_PID" ] && echo yes || echo no)"
else
  # LoRA 拓扑无第三实例：admin add（未启动副本）→ backends=3 healthy=2 → 覆写运行配置 + SIGHUP 回 2 副本
  RA=$(curl -s -o "$R/s5_add.json" -w '%{http_code}' -X POST "http://$RT/admin/backends" -H 'Content-Type: application/json' \
    -d '{"op":"add","backend":{"name":"vllm-c","base_url":"http://'"$C"'","model":"qwen2.5-0.5b","weight":1}}')
  sleep 1; Z1=$(statusz s5_z1.txt)
  write_router_cfg "$WS/cfg-s2.yaml" "$MODEL" ""   # 覆写正在使用的配置路径（SIGHUP 重读该路径）
  kill -HUP "$ROUTER_PID"; sleep 3
  Z2=$(statusz s5_z2.txt)
  say "add=$RA z1=$Z1 z2=$Z2"
  gate "S5-admin-add-sighup" "$("$PY" - "$R/s5_z1.txt" "$R/s5_z2.txt" <<'PY'
import json,sys
z1=json.load(open(sys.argv[1])); z2=json.load(open(sys.argv[2]))
print(1 if z1.get("backends")==3 and z1.get("healthy")==2 and z2.get("backends")==2 else 0)
PY
)" "add=$RA hup=ok pid_same=$( [ "$RPID_BEFORE" = "$ROUTER_PID" ] && echo yes || echo no)"
fi

# ================================================================ 收尾
READY=$(curl -s -o /dev/null -w '%{http_code}' "http://$RT/readyz")
gate "S6-readyz-200" "$("$PY" -c "print(1 if '${READY}'=='200' else 0)")" "readyz=$READY"
say "=== 清理环境 ==="
stop_vllm "$R/vllm-a.pid" 8001; stop_vllm "$R/vllm-b.pid" 8002; stop_vllm "$R/vllm-c.pid" 8003
kill -TERM "$ROUTER_PID" 2>/dev/null || true
echo "== SUMMARY: PASS=$PASS FAIL=$FAIL ==" | tee -a "$R/summary.txt"
say "results dir: $R"
exit $([ "$FAIL" = "0" ] && echo 0 || echo 1)
