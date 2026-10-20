"""paired_summary.py: compare a #765 paired A/B run by RATIOS.

usage: paired_summary.py <out-dir> <base-name> <arm-name> <pairs>

Reads <name>-<n>.json (the TestScale765_Point reports). Per run it prints the
granted CPU and the CPU-normalised figures; per pair the arm/base ratio of
each, and whether the pair is COMPARABLE: the two runs' HOST CONTENTION
(granted rusage cores / Go-busy cores) within 15% of each other (coordinator
ruling; it replaced a raw-granted-cores rule after #765 pair 1). A pair
outside that is reported, never compared. Every number here is computed from the reports; no
bound is read from the code under test.
"""
import json
import os
import sys

out, base, arm, pairs = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
TOL = 0.15


def parse_go_duration(s):
    """Seconds in a Go duration string such as '10m0s' or '2m30.5s'."""
    import re
    total = 0.0
    for num, unit in re.findall(r"([0-9.]+)(h|ms|m|s)", s or ""):
        total += float(num) * {"h": 3600, "m": 60, "s": 1, "ms": 0.001}[unit]
    return total


def figures(r):
    secs_cpu = r.get("cpu_cores_rusage") or 0.0
    share = r.get("refresher_worker_state_share") or {}
    blocked = sum(v for k, v in share.items() if k.startswith("blocked_other") or k == "waiting_fill")
    cust = (r.get("customer_widget_l1_miss") or 0) + (r.get("customer_widget_l1_hit") or 0)
    conc = r.get("bench_list_forced_fills_in_flight") or {}
    vw = r.get("viewed") or {}
    sub, uns = vw.get("subscribed") or {}, vw.get("unsubscribed") or {}
    viewed = {
        "viewed_sub_age_p50_s": (sub.get("served_body_age_s") or {}).get("p50", 0.0),
        "viewed_sub_age_p95_s": (sub.get("served_body_age_s") or {}).get("p95", 0.0),
        "viewed_unsub_age_p50_s": (uns.get("served_body_age_s") or {}).get("p50", 0.0),
        "viewed_unsub_age_p95_s": (uns.get("served_body_age_s") or {}).get("p95", 0.0),
        "viewed_sub_pushes_per_cell": sub.get("sse_pushes_per_cell", 0.0),
        "viewed_sub_dirty_share": sub.get("dirty_share", 0.0),
    } if vw else {}
    rd = r.get("refresher_delta") or {}
    for k in ("viewed_enqueue", "viewed_promoted", "viewed_dequeued", "viewed_skipped_consumed",
              "viewed_customer_triggered", "viewed_unsubscribed"):
        if k in rd:
            viewed["ctr_" + k] = rd[k]
    # Per-tier progress and the TTL-eviction causes (#799 diagnosis): normal-tier
    # completions = completed - viewed_dequeued; an expired cell is reaped when
    # the refresher dequeues it (skipped_no_entry), so a starved tier keeps
    # expired cells resident.
    viewed["refresher_completed"] = rd.get("completed", 0)
    viewed["normal_tier_completed"] = rd.get("completed", 0) - rd.get("viewed_dequeued", 0)
    # Normal-tier completions per granted core-second (coordinator, #799): the
    # tier's throughput normalised by the CPU the host actually gave the run.
    secs = parse_go_duration(r.get("measure", "")) or 0.0
    cores = r.get("cpu_cores_rusage") or 0.0
    viewed["normal_tier_per_core_s"] = (viewed["normal_tier_completed"] / (cores * secs)) if cores and secs else 0.0
    viewed["viewed_customer_share_of_dequeues"] = (
        rd.get("viewed_customer_triggered", 0) / rd["viewed_dequeued"]) if rd.get("viewed_dequeued") else 0.0
    viewed["deferred_in_flight"] = rd.get("deferred_in_flight", 0)
    viewed["skipped_no_entry"] = rd.get("skipped_no_entry", 0)
    viewed["evict_ttl"] = r.get("evict_ttl_total_delta", 0)
    viewed["dirty_evicted"] = r.get("dirty_ended_unfresh_evicted_delta", 0)
    per_min = r.get("normal_tier_completions_per_minute") or []
    if per_min:
        # architect-623's bar: > 0 in EVERY 60 s bucket of the fix arm.
        viewed["normal_tier_min_per_minute"] = min(per_min)
        viewed["normal_tier_zero_minutes"] = sum(1 for x in per_min if x <= 0)
    if (r.get("viewed_queue_depth") or {}).get("n"):
        viewed["viewed_queue_depth_p95"] = r["viewed_queue_depth"]["p95"]
    return dict(viewed, **{
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
    })


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
    keys = list(dict.fromkeys((keys or []) + list(fb) + list(fa)))
    for f in (fb, fa):
        for k in keys:
            f.setdefault(k, 0)
    # The pairing rule equalises HOST CONDITIONS: two runs are comparable when
    # their host contention (granted cores / Go-busy cores) is within 15%.
    # Changed after #765 pair 1 (coordinator ruling): the first rule compared
    # raw granted cores, which penalises an arm for using LESS CPU, the very
    # effect a fix like A is meant to have. Both are always reported.
    cb, ca = fb["granted_share_of_busy"], fa["granted_share_of_busy"]
    comparable = cb > 0 and ca > 0 and abs(ca - cb) / max(ca, cb) <= TOL
    print("pair %d: host contention (granted/busy) %s=%.2f %s=%.2f; granted cores %s=%.2f %s=%.2f -> %s" % (
        n, base, cb, arm, ca, base, fb["granted_cores"], arm, fa["granted_cores"],
        "COMPARABLE" if comparable else "NOT COMPARABLE (host contention differs > 15%)"))
    for k in keys:
        ratio = (fa[k] / fb[k]) if fb[k] else float("nan")
        print("    %-24s %s=%-12.4g %s=%-12.4g ratio=%.3g" % (k, base, fb[k], arm, fa[k], ratio))
    rows.append((n, comparable, fb, fa))

ok = [r for r in rows if r[1]]
print("\ncomparable pairs: %d of %d" % (len(ok), pairs))
if ok:
    for k in keys:
        ratios = sorted((fa.get(k, 0) / fb[k]) for _, _, fb, fa in ok if fb.get(k))
        if ratios:
            print("  %-24s %s/%s ratio: median %.3g (min %.3g, max %.3g)" % (
                k, arm, base, ratios[len(ratios) // 2], ratios[0], ratios[-1]))
