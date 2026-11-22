# compare_cache.py — 解析 cb_*.txt（8 行：A/B × baseline/final 的 prefix_cache queries+hits），算差值命中率
# 用法: python compare_cache.py <cb_file> [label]
import re, sys

path = sys.argv[1]
label = sys.argv[2] if len(sys.argv) > 2 else ""
vals = []
for line in open(path):
    # 只取数值行：跳过 HELP/TYPE 注释、_created 派生指标与 external_* 跨实例指标
    if line.lstrip().startswith("#") or "_created" in line or "external" in line:
        continue
    m = re.search(r'prefix_cache_(queries|hits)_total.* ([0-9.]+)', line)
    if m:
        vals.append(float(m.group(2)))
if len(vals) != 8:
    print(f"expect 8 lines got {len(vals)}: {vals}")
    sys.exit(1)
Aq0, Ah0, Bq0, Bh0, Aq1, Ah1, Bq1, Bh1 = vals
for name, q0, h0, q1, h1 in [("A", Aq0, Ah0, Aq1, Ah1), ("B", Bq0, Bh0, Bq1, Bh1)]:
    dq, dh = q1 - q0, h1 - h0
    rate = dh / dq * 100 if dq > 0 else float("nan")
    flag = "" if 0 <= dh <= dq else "  <-- ANOMALY"
    print(f"{name}: dq={dq:.0f} dh={dh:.0f} hit={rate:.1f}%{flag}")
dq, dh = (Aq1 - Aq0) + (Bq1 - Bq0), (Ah1 - Ah0) + (Bh1 - Bh0)
print(f"TOTAL{' ' + label if label else ''}: dq={dq:.0f} dh={dh:.0f} hit={dh / dq * 100:.1f}%")
