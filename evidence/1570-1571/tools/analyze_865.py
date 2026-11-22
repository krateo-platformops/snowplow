"""analyze_865.py OUT_DIR: the binding paired-throughput gate (#854/#865 customer arms).

Per rate and round, each arm's metric is taken as a RATIO to the same round's `main` run
(same window, alternated order). The main/main spread is the range of mainB/main over the
rounds. A branch passes a metric when its ratios stay inside the spread on the bad side:
latency (lower is better) median ratio <= max(mainB/main); throughput (higher is better)
median ratio >= min(mainB/main). Absolute numbers are reported, never gated."""
import json, os, statistics, sys
out = sys.argv[1]
runs = [json.loads(l) for l in open(os.path.join(out, "runs.jsonl"))]
runs = [r for r in runs if r["test"] == "TestPaired865Customer"]
data = {}
status = {}
for r in runs:
    status.setdefault(r["arm"], []).append(r["status"])
    if r["status"] != "RUN" or r.get("alone") is False:
        continue
    d = json.load(open(r["out"]))
    rate = d["rate"]
    m = {
        "dep_p50": d["cust_dep"].get("p50_ms"), "dep_p95": d["cust_dep"].get("p95_ms"),
        "unrel_p50": d["cust_unrel"].get("p50_ms"), "unrel_p95": d["cust_unrel"].get("p95_ms"),
        "dep_rps": d["cust_dep_rps"], "unrel_rps": d["cust_unrel_rps"],
        "completions_s": d["completions_s"], "probe_p50": d["probe"].get("p50_ms"), "probe_p95": d["probe"].get("p95_ms"),
        "hit_p50": d["hit_bench"].get("p50_ms"), "staleness_p95": d["cust_dep_staleness"].get("p95_ms"),
        "yielded": d["refresh_yielded"], "capped": d["refresh_capped"], "parked_s": d["refresh_parked_s"],
        "floored": d["refresh_floored"], "cpu_cores": d["cpu_cores"], "errors": d["cust_errors"],
    }
    data.setdefault(rate, {}).setdefault(r["round"], {})[r["arm"]] = m
LOWER = ["dep_p50", "dep_p95", "unrel_p50", "unrel_p95", "probe_p50", "probe_p95", "hit_p50"]
HIGHER = ["dep_rps", "unrel_rps", "completions_s"]
REPORT = ["staleness_p95", "yielded", "capped", "parked_s", "floored", "cpu_cores", "errors"]
summary = {"status_by_arm": {a: f"{s.count('RUN')}/{len(s)} RUN" for a, s in status.items()}, "rates": {}}
for rate, rounds in sorted(data.items()):
    def ratios(arm, k):
        xs = []
        for rd in rounds.values():
            if arm in rd and "main" in rd and rd["main"].get(k) and rd[arm].get(k) is not None:
                xs.append(rd[arm][k] / rd["main"][k])
        return xs
    rs = {}
    for arm in sorted({a for rd in rounds.values() for a in rd} - {"main"}):
        row = {}
        for k in LOWER + HIGHER:
            xs = ratios(arm, k)
            sp = ratios("mainB", k)
            if not xs or not sp:
                continue
            med = statistics.median(xs)
            if arm == "mainB":
                verdict = "spread"
            elif k in LOWER:
                verdict = "PASS" if med <= max(sp) else "FAIL"
            else:
                verdict = "PASS" if med >= min(sp) else "FAIL"
            row[k] = {"median_ratio": round(med, 3), "ratios": [round(x, 3) for x in xs],
                      "main_main_spread": [round(min(sp), 3), round(max(sp), 3)], "verdict": verdict}
        for k in REPORT:
            row[k] = [rd[arm][k] for rd in rounds.values() if arm in rd]
        rs[arm] = row
    rs["main"] = {k: [rd["main"][k] for rd in rounds.values() if "main" in rd] for k in LOWER + HIGHER + REPORT}
    summary["rates"][rate] = rs
json.dump(summary, open(os.path.join(out, "summary.json"), "w"), indent=1)
for rate, rs in summary["rates"].items():
    print(f"== rate {rate}")
    for arm, row in rs.items():
        if arm == "main":
            continue
        cells = [f"{k}={v['median_ratio']}({v['verdict']})" for k, v in row.items() if isinstance(v, dict)]
        print(f"  {arm}: " + " ".join(cells))
        print(f"     capped={row['capped']} yielded={row['yielded']} parked_s={row['parked_s']} errors={row['errors']}")
    print(f"  main: capped={rs['main']['capped']} yielded={rs['main']['yielded']} parked_s={rs['main']['parked_s']}")
print(summary["status_by_arm"])
