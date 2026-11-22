# load_test.py — 粘性多会话流量压测：流式采集 TTFT + 总延迟
#
# 模拟真实多轮对话负载：messages 历史随轮次递增（客户端每轮回传完整历史），
# 每轮 user 注入一段新"文档片段"，assistant 响应并入历史——
# 这正是 prefix affinity 的收益主场：换副本会丢失最近两轮的增量历史前缀。
#
# 用法: python load_test.py <base_url> <mode_tag> [sessions] [turns] [conc] [sess_prefix] [jitter_s]
import json, time, threading, urllib.request, sys, random

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"
MODE = sys.argv[2] if len(sys.argv) > 2 else "affinity"
N_SESSION = int(sys.argv[3]) if len(sys.argv) > 3 else 40
TURNS = int(sys.argv[4]) if len(sys.argv) > 4 else 6
CONC = int(sys.argv[5]) if len(sys.argv) > 5 else 8
SESS_PREFIX = sys.argv[6] if len(sys.argv) > 6 else "s"
JITTER = float(sys.argv[7]) if len(sys.argv) > 7 else 0.0

# system 保持很短：让"递增历史"成为 prompt 体积的主体，
# 若 system 很长且轮间不变，两副本都会缓存该公共前缀，RR 也能拿到高命中率（实验教训）
SYS = "你是文档分析助手，逐段总结要点。"

# 每轮注入 ~250 token 的新片段（模拟长文档分段分析 / agent 工具结果回灌）
def chunk_text(sid, t):
    body = (f"文档{sidsuffix(sid)}第{t}段：数据管道第{t}阶段覆盖采集、清洗、转换、加载四个步骤，"
            "每一步有明确的输入输出契约与失败重试策略；调度层采用有界并发防止下游过载，"
            "断点续跑依赖检查点与幂等写入，指标侧按阶段统计吞吐与失败率并输出日报。") * 4
    return body

def sidsuffix(sid):
    return str(sid)

def chat_once(session_id, messages):
    body = json.dumps({
        "model": "qwen2.5-1.5b",
        "messages": messages,
        "max_tokens": 48,
        "temperature": 0,
        "stream": True,
    }).encode()
    req = urllib.request.Request(BASE + "/v1/chat/completions", data=body)
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Session-Id", session_id)
    t0 = time.time()
    ttft = None
    reply_parts = []
    with urllib.request.urlopen(req, timeout=180) as r:
        for raw in r:
            line = raw.strip()
            if not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if payload == b"[DONE]":
                break
            if ttft is None and (b"delta" in payload or b"content" in payload):
                ttft = time.time() - t0
            try:
                obj = json.loads(payload)
                delta = obj["choices"][0].get("delta", {})
                piece = delta.get("content") or ""
                if piece:
                    reply_parts.append(piece)
            except Exception:
                pass
    total = time.time() - t0
    if ttft is None:
        ttft = total
    return ttft, total, "".join(reply_parts)

results = []
lock = threading.Lock()

def worker(sid):
    # 会话独占标识放在最前：prefix caching 按块哈希链匹配，首块不同则跨会话零共享
    messages = [{"role": "system", "content": f"专属工号{sid}。" + SYS}]
    for t in range(TURNS):
        if JITTER > 0 and t > 0:
            time.sleep(random.uniform(0, JITTER))  # 打破同步波次，避免 RR 出现隐性 per-thread 亲和
        messages.append({"role": "user", "content": chunk_text(sid, t)})
        try:
            ttft, total, reply = chat_once(f"{SESS_PREFIX}-{sid}", messages)
            messages.append({"role": "assistant", "content": reply or "已收到该段。"})
            with lock:
                results.append((sid, t, ttft, total))
        except Exception:
            with lock:
                results.append((sid, t, None, None))

th = [threading.Thread(target=worker, args=(i,)) for i in range(N_SESSION)]
wall0 = time.time()
for i in range(0, N_SESSION, CONC):
    batch = th[i:i + CONC]
    for t in batch:
        t.start()
    for t in batch:
        t.join()
wall = time.time() - wall0

ok = [r for r in results if r[2] is not None]
ttfts = sorted(r[2] for r in ok)
totals = sorted(r[3] for r in ok)
def pct(a, p):
    return a[min(len(a) - 1, int(len(a) * p))]
out = {
    "mode": MODE,
    "base": BASE,
    "sessions": N_SESSION,
    "turns": TURNS,
    "requests": len(results),
    "ok": len(ok),
    "wall_s": round(wall, 2),
    "ttft_p50_s": round(pct(ttfts, 0.5), 3),
    "ttft_p90_s": round(pct(ttfts, 0.9), 3),
    "ttft_avg_s": round(sum(ttfts) / len(ttfts), 3),
    "total_p50_s": round(pct(totals, 0.5), 3),
    "total_p90_s": round(pct(totals, 0.9), 3),
}
print(json.dumps(out, ensure_ascii=False))
