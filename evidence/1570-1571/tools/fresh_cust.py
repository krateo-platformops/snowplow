"""fresh_cust.py FRESH_DIR: customer latency ratio v3/main (median of runs) per rate."""
import json, os, statistics, sys
d = sys.argv[1]
runs = [json.loads(l) for l in open(os.path.join(d, "runs.jsonl")) if l.strip()]
agg = {}
for r in runs:
    if r["status"] != "RUN":
        continue
    j = json.load(open(r["out"]))
    for ph in j["phases"]:
        a = agg.setdefault((r["arm"], ph["rate"]), {"fast_p50": [], "fast_p95": [], "cold_p50": [], "cold_p95": [], "rps": []})
        a["fast_p50"].append(ph["cust_fast"].get("p50_ms"))
        a["fast_p95"].append(ph["cust_fast"].get("p95_ms"))
        a["cold_p50"].append(ph["cust_cold"].get("p50_ms"))
        a["cold_p95"].append(ph["cust_cold"].get("p95_ms"))
        a["rps"].append(ph["cust_rps"])
for rate in sorted({k[1] for k in agg}):
    m, v = agg[("main", rate)], agg[("v3", rate)]
    print(rate, {k: round(statistics.median(v[k]) / statistics.median(m[k]), 2) for k in m},
          "main medians", {k: round(statistics.median(m[k]), 2) for k in m})
