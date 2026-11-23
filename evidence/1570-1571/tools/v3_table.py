"""v3_table.py R10_DIR R50_DIR FRESH_DIR: per-arm ranges for the #1581 waiver table.

#865 split runs: normal-tier wait (mean, share <= 1 s), completions per core, refresher CPU,
dependent staleness p50/p95/max, dirty_to_fresh p95/max. Only RUN + alone runs count.
Freshness runs: dirty_to_fresh p95/max per rate (fresh-d2f overlay), per arm."""
import json, os, sys


def load(d, test):
    out = {}
    for l in open(os.path.join(d, "runs.jsonl")):
        if not l.strip():
            continue
        r = json.loads(l)
        if r["test"] != test:
            continue
        ok = r["status"] == "RUN" and r.get("alone") is not False
        out.setdefault(r["arm"], []).append((r["round"], ok, json.load(open(r["out"])) if ok else None, r["status"]))
    return out


def rng(xs, scale=1.0, nd=2):
    xs = [x * scale for x in xs if x is not None]
    if not xs:
        return "n/a"
    a, b = min(xs), max(xs)
    return f"{a:.{nd}f}" if round(a, nd) == round(b, nd) else f"{a:.{nd}f}-{b:.{nd}f}"


for d in sys.argv[1:3]:
    print("==", os.path.basename(d.rstrip("/")))
    runs = load(d, "TestPaired865Customer")
    for arm, rs in sorted(runs.items()):
        st = [s for _, _, _, s in rs]
        good = [j for _, ok, j, _ in rs if ok]
        def f(fn):
            return [fn(j) for j in good]
        nw = f(lambda j: j["refresh_wait_by_tier"]["normal"].get("mean_ms"))
        nw1 = f(lambda j: j["refresh_wait_by_tier"]["normal"].get("share_le_1000"))
        cpc = f(lambda j: j["completions_s"] / j["cpu_cores"] if j["cpu_cores"] else None)
        cpu = f(lambda j: j["cpu_cores"])
        cs = f(lambda j: j["completions_s"])
        s50 = f(lambda j: j["cust_dep_staleness"].get("p50_ms"))
        s95 = f(lambda j: j["cust_dep_staleness"].get("p95_ms"))
        smx = f(lambda j: j["cust_dep_staleness"].get("max_ms"))
        d95 = f(lambda j: j["dirty_to_fresh"].get("p95_ms"))
        dmx = f(lambda j: j["dirty_to_fresh"].get("max_ms"))
        dep = f(lambda j: j["cust_dep"].get("p50_ms"))
        unr = f(lambda j: j["cust_unrel"].get("p50_ms"))
        rps = f(lambda j: j["cust_dep_rps"] + j["cust_unrel_rps"])
        print(f"  {arm:6s} status={st}")
        print(f"     normal wait mean {rng(nw, nd=0)} ms, share<=1s {rng(nw1, 100, 0)}%; completions/s {rng(cs, nd=1)}; cpu {rng(cpu)} cores; completions/core {rng(cpc, nd=1)}")
        print(f"     dep staleness p50 {rng(s50, .001)} p95 {rng(s95, .001)} max {rng(smx, .001)} s; d2f p95 {rng(d95, .001)} max {rng(dmx, .001)} s")
        print(f"     cust dep p50 {rng(dep, nd=1)} ms, unrel p50 {rng(unr, nd=1)} ms, rps {rng(rps, nd=0)}")

if len(sys.argv) > 3:
    d = sys.argv[3]
    print("==", os.path.basename(d.rstrip("/")))
    runs = load(d, "TestPairedFresh1570")
    for arm, rs in sorted(runs.items()):
        st = [s for _, _, _, s in rs]
        byrate = {}
        for _, ok, j, _ in rs:
            if not ok:
                continue
            for ph in j["phases"]:
                byrate.setdefault(ph["rate"], []).append(ph)
        print(f"  {arm:6s} status={st}")
        for rate, phs in sorted(byrate.items()):
            d95 = [p.get("dirty_to_fresh", {}).get("p95_ms") for p in phs]
            dmx = [p.get("dirty_to_fresh", {}).get("max_ms") for p in phs]
            pp95 = [p["push"].get("p95_ms") for p in phs]
            print(f"     @{rate}/s: push p95 {rng(pp95, nd=1)} ms; d2f p95 {rng(d95, .001)} max {rng(dmx, .001)} s")
