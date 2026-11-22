"""analyze_fresh.py OUT_DIR: pool the alternated freshness runs per arm and rate.

push: every sample of every run pooled (nearest-rank p50/p95) and the per-run p95s.
viewed wait share <= 100 ms: weighted by each run's viewed refresh count.
Customer and refresher numbers: per run, then median across runs, and the ratio of the
arm's median to main's median (load-sensitive numbers are compared only as ratios).
Bars (#1570/#1571): viewed waits <= 100 ms >= 90%; probe push p95 <= ~850 ms."""
import json, math, os, statistics, sys
out = sys.argv[1]
runs = [json.loads(l) for l in open(os.path.join(out, "runs.jsonl"))]
runs = [r for r in runs if r["test"] == "TestPairedFresh1570"]

def q(xs, p):
    xs = sorted(xs)
    return xs[max(1, math.ceil(p * len(xs))) - 1] if xs else None

# the probe's raw samples are not in the JSON (only quantiles): read them from the runs' quantiles
# is not poolable, so the harness JSON carries the quantiles per run; pool via per-run p95s and
# the pooled viewed-wait histogram, plus the median-of-runs.
table = {}
status = {}
for r in runs:
    status.setdefault(r["arm"], []).append(r["status"])
    if r["status"] != "RUN":
        continue
    d = json.load(open(r["out"]))
    for p in d["phases"]:
        k = (r["arm"], p["rate"])
        t = table.setdefault(k, {"p50": [], "p95": [], "max": [], "vw_n": 0, "vw_le100": 0.0, "vw_mean": [],
                                 "yielded": 0, "capped": 0, "parked_s": 0.0, "floored": 0, "compl_s": [],
                                 "rps": [], "fast_p50": [], "fast_p95": [], "cold_p50": [], "cold_p95": [],
                                 "unconverged": 0, "stale": 0, "samples": 0, "rounds": 0, "all": []})
        t["rounds"] += 1
        t["samples"] += p["samples"]
        t["unconverged"] += p["unconverged"]
        t["stale"] += p["refetch_without_marker"]
        t["all"].extend(p.get("push_samples_ms") or [])
        t["p50"].append(p["push"].get("p50_ms"))
        t["p95"].append(p["push"].get("p95_ms"))
        t["max"].append(p["push"].get("max_ms"))
        vw = p["viewed_wait"]
        if vw.get("n"):
            t["vw_n"] += vw["n"]
            t["vw_le100"] += vw["share_le_100"] * vw["n"]
            t["vw_mean"].append(vw["mean_ms"])
        for a, b in (("yielded", "refresh_yielded"), ("capped", "refresh_capped"), ("floored", "refresh_floored")):
            t[a] += p[b]
        t["parked_s"] += p["refresh_parked_s"]
        t["compl_s"].append(p["completions_s"])
        t["rps"].append(p["cust_rps"])
        t["fast_p50"].append(p["cust_fast"].get("p50_ms"))
        t["fast_p95"].append(p["cust_fast"].get("p95_ms"))
        t["cold_p50"].append(p["cust_cold"].get("p50_ms"))
        t["cold_p95"].append(p["cust_cold"].get("p95_ms"))

med = lambda xs: statistics.median([x for x in xs if x is not None]) if xs else None
summary = {"status_by_arm": status, "rows": []}
for (arm, rate), t in sorted(table.items(), key=lambda kv: (kv[0][1], kv[0][0])):
    m = table.get(("main", rate))
    row = {"arm": arm, "rate": rate, "rounds": t["rounds"], "samples": t["samples"], "unconverged": t["unconverged"],
           "refetch_without_marker": t["stale"],
           "push_pooled_n": len(t["all"]), "push_pooled_p50": q(t["all"], 0.5), "push_pooled_p95": q(t["all"], 0.95),
           "push_p50_median_of_runs": med(t["p50"]), "push_p95_median_of_runs": med(t["p95"]),
           "push_p95_worst_run": max(x for x in t["p95"] if x is not None), "push_max": max(x for x in t["max"] if x is not None),
           "viewed_wait_share_le_100": round(t["vw_le100"] / t["vw_n"], 3) if t["vw_n"] else None, "viewed_refreshes": t["vw_n"],
           "yielded": t["yielded"], "capped": t["capped"], "parked_s": round(t["parked_s"], 1), "floored": t["floored"],
           "completions_s_median": med(t["compl_s"]), "cust_rps_median": med(t["rps"]),
           "cust_fast_p50_median": med(t["fast_p50"]), "cust_fast_p95_median": med(t["fast_p95"]),
           "cust_cold_p50_median": med(t["cold_p50"]), "cust_cold_p95_median": med(t["cold_p95"])}
    if m and arm != "main":
        for k in ("compl_s", "rps", "fast_p50", "fast_p95", "cold_p50", "cold_p95"):
            a, b = med(t[k]), med(m[k])
            row[f"ratio_{k}_vs_main"] = round(a / b, 3) if a and b else None
    row["bar_viewed_wait"] = "PASS" if row["viewed_wait_share_le_100"] is not None and row["viewed_wait_share_le_100"] >= 0.90 else "FAIL"
    row["bar_push_p95"] = "PASS" if row["push_pooled_p95"] is not None and row["push_pooled_p95"] <= 850 else "FAIL"
    summary["rows"].append(row)
json.dump(summary, open(os.path.join(out, "summary.json"), "w"), indent=1)
hdr = ["arm", "rate", "rounds", "push_pooled_n", "push_pooled_p50", "push_pooled_p95", "push_p95_median_of_runs",
       "viewed_wait_share_le_100", "capped", "yielded", "parked_s", "floored", "completions_s_median", "cust_rps_median",
       "bar_viewed_wait", "bar_push_p95"]
print("\t".join(hdr))
for r in summary["rows"]:
    print("\t".join(str(round(r[h], 1)) if isinstance(r[h], float) else str(r[h]) for h in hdr))
print("status:", {a: f"{s.count('RUN')}/{len(s)} RUN" for a, s in status.items()})
