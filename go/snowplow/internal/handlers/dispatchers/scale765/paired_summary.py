"""paired_summary.py: compare a #765 paired A/B run by RATIOS.

usage: paired_summary.py <out-dir> <base-name> <arm-name> <pairs>

Reads <name>-<n>.json (the TestScale765_Point reports). Per run it prints the
granted CPU and the CPU-normalised figures; per pair the arm/base ratio of
each, and whether the pair is COMPARABLE: the two runs' granted CPU (rusage
cores) within 15% of each other (coordinator ruling). A pair outside that is
reported, never compared. Every number here is computed from the reports; no
bound is read from the code under test.
"""
import json
import os
import sys

out, base, arm, pairs = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
TOL = 0.15


def figures(r):
    secs_cpu = r.get("cpu_cores_rusage") or 0.0
    share = r.get("refresher_worker_state_share") or {}
    blocked = sum(v for k, v in share.items() if k.startswith("blocked_other") or k == "waiting_fill")
    cust = (r.get("customer_widget_l1_miss") or 0) + (r.get("customer_widget_l1_hit") or 0)
    conc = r.get("bench_list_forced_fills_in_flight") or {}
    return {
        "granted_cores": secs_cpu,
        "granted_share_of_busy": (secs_cpu / r["cpu_cores_runtime_metrics"]) if r.get("cpu_cores_runtime_metrics") else 0.0,
        "bench_fills_per_event": r.get("bench_list_fills_per_event", 0.0),
        "refreshes_per_core_s": (r.get("refresher_completed_per_sec", 0.0) / secs_cpu) if secs_cpu else 0.0,
        "fills_in_flight_p50": conc.get("p50", 0.0),
        "fills_in_flight_max": conc.get("max", 0.0),
        "worker_blocked_share": blocked,
        "fill_waiters_mean": r.get("stage_goroutines_waiting_on_fill_mean", 0.0),
        "customer_miss_share": (r.get("customer_widget_l1_miss", 0) / cust) if cust else 0.0,
        "customer_misses": r.get("customer_widget_l1_miss", 0),
        "dirty_ttl_evictions_lb": r.get("dirty_ttl_expiries_lower_bound", 0),
        "dirty_to_fresh_p95_ms": (r.get("dirty_to_fresh_ms") or {}).get("p95", 0.0),
        "probe_p95_ms": (r.get("probe_converged_ms") or {}).get("p95", 0.0),
        "probe_unconverged": r.get("probe_unconverged", 0),
    }


def load(name, n):
    p = os.path.join(out, "%s-%d.json" % (name, n))
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return None


keys = None
rows = []
for n in range(1, pairs + 1):
    b, a = load(base, n), load(arm, n)
    if b is None or a is None:
        print("pair %d: MISSING (%s=%s, %s=%s)" % (n, base, b is not None, arm, a is not None))
        continue
    fb, fa = figures(b), figures(a)
    keys = keys or list(fb)
    cb, ca = fb["granted_cores"], fa["granted_cores"]
    comparable = cb > 0 and ca > 0 and abs(ca - cb) / max(ca, cb) <= TOL
    print("pair %d: granted cores %s=%.2f %s=%.2f -> %s" % (n, base, cb, arm, ca,
          "COMPARABLE" if comparable else "NOT COMPARABLE (granted CPU differs > 15%)"))
    for k in keys:
        ratio = (fa[k] / fb[k]) if fb[k] else float("nan")
        print("    %-24s %s=%-12.4g %s=%-12.4g ratio=%.3g" % (k, base, fb[k], arm, fa[k], ratio))
    rows.append((n, comparable, fb, fa))

ok = [r for r in rows if r[1]]
print("\ncomparable pairs: %d of %d" % (len(ok), pairs))
if ok:
    for k in keys:
        ratios = sorted((fa[k] / fb[k]) for _, _, fb, fa in ok if fb[k])
        if ratios:
            print("  %-24s %s/%s ratio: median %.3g (min %.3g, max %.3g)" % (
                k, arm, base, ratios[len(ratios) // 2], ratios[0], ratios[-1]))
