"""The figures #922's acceptance is read from, per run and as B/A ratios of a
pair — report only (#919, from #922's review F3).

#922 removes client-go's 5/s client-side limiter. That changes when every
request snowplow sends at boot goes out: about 280 concurrent watch starts
and unpaced streaming-LIST pages. One run's figures (evaluate:
report["acceptance_figures_report_only"]), with boot as its own phase:

  boot          created_to_ready_s: the new pod's creation to its Ready
                condition, both CLUSTER timestamps (its before-record);
                started_to_ready_s: its container's start to Ready (cluster);
                restart_to_created_s: run606's `rollout restart` (the
                operator's clock) to the pod's creation (the cluster's):
                two clocks, kept apart; sampled_max_memory_bytes: the max of
                kubectl top's samples of the new pod in the boot window
                (metrics-server, about 15 s apart: not the true peak).
                Ready is the condition's LAST transition: a Ready flap before
                the measure would lengthen the boot (the pod record's
                restarts and readiness say so).
  by_phase[p][identity]
                identity = snowplow_sa (snowplow's ServiceAccount),
                snowplow_users (what snowplow sends as the bench's users),
                bench_drivers (the bench's own churn and probes): each in a
                FlowSchema of its own (bench/flowcontrol.py, the API server's
                own classification), so its figures are its alone:
                rejected, wait_requests / wait_mean_s / p95 / p99 (APF queue
                time), sampled_max_inflight_requests (executing + in-queue,
                the max of the 15 s scrapes: not the true peak; APF exposes
                no per-schema watermark) and the level's
                demand_seats_high_watermark (the level's own peak demand per
                adjustment period — shared by every schema at that level).
                A phase with no two scrapes of one replica inside it has its
                counter figures None, "NOT MEASURED" — never 0 — and the
                differences that cross its edges are under `spanning`.
                p = boot, before_measure, storm, burst, each churn step,
                steady.
  steady_refreshes_per_core  snowplow's refresher completions per second
                over its CPU cores (median kubectl top), in the steady phase
                while customers ran.

custlat pair adds `acceptance_ratios_report_only`: each figure as
{a, b, b_over_a}.
"""

from __future__ import annotations

import re

from bench import apf, flowcontrol, load606

COUNTERS = ("rejected", "wait_requests", "wait_mean_s", "wait_p95_s", "wait_p99_s")


def _fs_figures(rep: dict, fs: str | None, pl: str | None) -> dict:
    """One flow schema's figures in one phase (or spanning stretch) of the
    APF report; its counter figures None when the phase measured none."""
    lv = ((rep or {}).get("priority_levels") or {}).get(pl) or {}
    node = (lv.get("flow_schemas") or {}).get(fs) or {}
    w = (node.get("wait") or {}).get("executed") or {}
    out = {"rejected": sum((node.get("rejected") or {}).values()),
           "wait_requests": w.get("requests", 0),
           "wait_mean_s": w.get("mean_s"), "wait_p95_s": w.get("p95_s"), "wait_p99_s": w.get("p99_s"),
           "sampled_max_inflight_requests": (node.get(apf.INFLIGHT) or {}).get("max"),
           "level_demand_seats_high_watermark": lv.get("demand_seats_high_watermark")}
    if not (rep or {}).get("same_replica_pairs"):
        out.update(dict.fromkeys(COUNTERS), counters="NOT MEASURED: no two scrapes of one replica inside it")
    return out


def boot_window(recs: list[dict]) -> tuple[tuple[float, float] | None, dict]:
    """(boot window, its facts). The durations: cluster timestamps only
    (created_at / started_at / ready_at). The window the APF and top samples
    (operator clock) are placed in: run606's restart time to the new pod's
    Ready."""
    boot = next((r for r in recs if r.get("kind") == "boot" and r.get("t_restart") is not None), None)
    pod = next((r for r in recs if r.get("kind") == "snowplow_pod" and r.get("phase") == "before"), None)
    p = pod or {}
    facts = {"t_restart": (boot or {}).get("t_restart"), "pod": p.get("pod"), "created_at": p.get("created_at"),
             "started_at": p.get("started_at"), "ready_at": p.get("ready_at")}
    for name, a in (("created_to_ready_s", "created_at"), ("started_to_ready_s", "started_at")):
        if p.get(a) is not None and p.get("ready_at") is not None:
            facts[name] = round(float(p["ready_at"]) - float(p[a]), 1)
    if boot is not None and p.get("created_at") is not None:
        facts["restart_to_created_s"] = round(float(p["created_at"]) - float(boot["t_restart"]), 1)
        facts["restart_to_created_clocks"] = "operator (restart) vs cluster (created)"
    if boot is None or p.get("ready_at") is None:
        facts["note"] = ("no boot record (run606 before #919)" if boot is None else
                         "no Ready time for the new pod (its before-record has no ready_at)")
        return None, facts
    return (float(boot["t_restart"]), float(p["ready_at"])), facts


def boot_memory(recs: list[dict], window, pod: str | None) -> dict:
    """The new pod's memory, the max of kubectl top's samples (kind
    top_boot) in the window."""
    rows = [r for r in recs if r.get("kind") == "top_boot" and r.get("t") is not None
            and window is not None and window[0] <= r["t"] <= window[1]]
    peak = None
    for r in rows:
        parts = (r.get("raw") or "").split()
        for i in range(0, len(parts) - 2, 3):
            if parts[i] == pod:
                v = load606._parse_quantity(parts[i + 2])
                if v is not None and (peak is None or v > peak):
                    peak = v
    return {"sampled_max_memory_bytes": peak, "memory_samples": sum(1 for r in rows if not r.get("failed")),
            "memory_failed_samples": sum(1 for r in rows if r.get("failed"))}


def steady_refreshes_per_core(cl: dict | None) -> dict:
    """Refresher completions per second over snowplow's CPU cores, in the
    steady phase while customers ran (customer_latency's own per-phase
    reports)."""
    rc = (((cl or {}).get("refresher_completions") or {}).get("by_phase") or {}).get("steady") or {}
    cpu = (((cl or {}).get("snowplow_cpu") or {}).get("by_phase") or {}).get("steady") or {}
    per_s = (rc.get("with_customers") or {}).get("per_s")
    med = (cpu.get("with_customers") or {}).get("median_m")
    if per_s is None or not med:
        return {"value": None, "note": "no steady phase with customers in both the refresher and the CPU "
                                       "report (no k6, or no steady time)"}
    return {"value": round(per_s / (med / 1000), 3), "refreshes_per_s": per_s, "cpu_median_m": med}


# ─── natural relists (#933 review) ──────────────────────────────────────────
# What snowplow exposes (mem606 samples it every 15 s, MEM_VARS_PREFIXES):
#   reflector_watchlist_established_total (snowplow_reflector_path): a WATCH
#     with sendInitialEvents — a reflector (re-)establishing its whole state
#     from the watch cache; only reflectors send one. After boot, each is a
#     relist (watch-list regime).
#   watch_errors_total (snowplow_informer_watch): every ListAndWatch error;
#     the reflector relists after each.
#   the two classic-LIST counters (reflector_list_cache_eligible_total,
#     reflector_list_etcd_delegated_total) count snowplow's own fallthrough
#     LISTs too: reported, never read as relists.
# All process-wide: snowplow exposes no per-GVR relist count or time (its
# per-GVR path map holds only the current path; the cache.watch.broken WARN
# names a GVR on its FIRST failure only). So a relist's GVR is not known, and
# k6's requests carry no GVR either: the 50K-composition-GVR comparison is
# not derivable from what the bench can collect — the windows below are
# "some GVR relisted" against "none did", for every k6 call.
RELIST = {"watchlist_established": ("snowplow_reflector_path", "reflector_watchlist_established_total"),
          "watch_errors": ("snowplow_informer_watch", "watch_errors_total")}
CLASSIC_LIST = ("reflector_list_cache_eligible_total", "reflector_list_etcd_delegated_total")
_TOP_M = re.compile(r"(\S+)\s+(\d+)m\s+(\S+)")


def _pct(v: list[float], p: float) -> float | None:
    if not v:
        return None
    s = sorted(v)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 1)


def _relist_counts(m: dict) -> dict | None:
    ex = m.get("extra") or {}
    out = {}
    for name, (fam, stat) in RELIST.items():
        v = (ex.get(fam) or {}).get(stat) if isinstance(ex.get(fam), dict) else None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out[name] = v
    return out or None


def relists(mem: list[dict], after: float | None, k6_calls: list[tuple[float, float]], tops: list[dict]) -> dict:
    """Natural relists after the boot (snowplow Ready at `after`): their
    count per hour, each one's interval (two consecutive mem606 samples, 15 s)
    with snowplow's heap before / after and CPU (kubectl top) in it, and k6's
    calls completing in relist intervals against the quiet ones."""
    rows = sorted((m for m in mem if m.get("kind") == "mem" and m.get("t") is not None
                   and _relist_counts(m) is not None and (after is None or m["t"] >= after)),
                  key=lambda m: m["t"])
    if len(rows) < 2:
        n = sum(1 for m in mem if m.get("kind") == "mem")
        return {"status": "absent",
                "note": (f"{n} mem samples, {len(rows)} after Ready carry "
                         f"{' / '.join(f'{f}.{s}' for f, s in RELIST.values())}: the relist count is MISSING "
                         f"(an image or MEM_VARS_PREFIXES without them), not zero")}
    events, quiet, seconds, totals, resets = [], [], 0.0, dict.fromkeys(RELIST, 0), 0
    for a, b in zip(rows, rows[1:]):
        ca, cb = _relist_counts(a), _relist_counts(b)
        d = {k: cb[k] - ca[k] for k in cb if k in ca}
        if any(v < 0 for v in d.values()):
            resets += 1                       # a restart reset the counters: not this interval
            continue
        seconds += b["t"] - a["t"]
        ms = [v for t, v in k6_calls if a["t"] < t <= b["t"]]
        if any(d.values()):
            for k, v in d.items():
                totals[k] += v
            cpu = [sum(int(x.group(2)) for x in _TOP_M.finditer(r.get("raw") or ""))
                   for r in tops if r.get("kind") == "top" and not r.get("failed")
                   and r.get("t") is not None and a["t"] < r["t"] <= b["t"]]
            events.append({"t0": a["t"], "t1": b["t"], **{k: v for k, v in d.items() if v},
                           "heap_inuse_mb_before": a.get("heap_inuse_mb"),
                           "heap_inuse_mb_after": b.get("heap_inuse_mb"),
                           "cpu_m_samples": cpu, "k6_calls": len(ms)})
            events[-1]["_ms"] = ms
        else:
            quiet.append(ms)

    def lat(groups):
        v = [x for g in groups for x in g]
        return {"windows": len(groups), "calls": len(v), "p95_ms": _pct(v, 95), "max_ms": max(v) if v else None}
    in_relist = lat([e.pop("_ms") for e in events])
    hours = seconds / 3600
    return {"status": "present", "measured_s": round(seconds, 1), "counter_resets_skipped": resets,
            "totals": totals,
            "per_hour": {k: round(v / hours, 3) if hours else None for k, v in totals.items()},
            "none_occurred": not events,
            "per_gvr": "not exposed by snowplow (process-wide counters only)",
            "events": events,
            "k6_calls_in_relist_windows": in_relist,
            "k6_calls_in_quiet_windows": lat(quiet),
            "composition_gvr_comparison": "not derivable: neither a relist's GVR nor a k6 call's GVR is "
                                          "collected (snowplow exposes no per-GVR relist signal)",
            "classic_list_note": f"{' / '.join(CLASSIC_LIST)} also count snowplow's own fallthrough LISTs: "
                                 f"not relists, not reported as such"}


def run_figures(report: dict, recs: list[dict], window, facts: dict,
                k6_calls: list[tuple[float, float]] | None = None) -> dict:
    """One run's figures (report: its verdict report, with the APF report)."""
    cfg = next((r for r in recs if r.get("kind") == "flowcontrol_config"), None)
    ap = report.get("apiserver_flowcontrol_report_only") or {}
    out = {"boot": dict(facts, **boot_memory(recs, window, facts.get("pod"))),
           "flow": cfg or {"note": "no flowcontrol_config record: the identities' flow schemas are not known"},
           "steady_refreshes_per_core": steady_refreshes_per_core(report.get("customer_latency")),
           "relists": relists([r for r in recs if r.get("kind") == "mem"],
                              facts.get("ready_at"), k6_calls or [],
                              [r for r in recs if r.get("kind") == "top"])}
    ids = (cfg or {}).get("identities") or {}
    if cfg and cfg.get("verified") and ap.get("status") == "present":
        def per(rep):
            return {i: _fs_figures(rep, (ids.get(i) or {}).get("measured", {}).get("flow_schema"),
                                   (ids.get(i) or {}).get("measured", {}).get("priority_level"))
                    for i in flowcontrol.IDENTITIES}
        out["by_phase"] = {p: per(r) for p, r in (ap.get("by_phase") or {}).items()}
        out["spanning"] = {k: per(r) for k, r in (ap.get("spanning") or {}).items()}
    else:
        out["by_phase"] = None
        out["by_phase_note"] = ("the APF report is absent" if ap.get("status") != "present"
                                else "the identities' own flow schemas were not verified (flowcontrol_config)")
    return out


def _leaves(d, prefix=()):
    if isinstance(d, dict):
        for k, v in d.items():
            yield from _leaves(v, prefix + (str(k),))
    elif isinstance(d, (int, float)) and not isinstance(d, bool):
        yield prefix, d


SKIP = ("t_restart", "created_at", "started_at", "ready_at")


def ratios(va: dict | None, vb: dict | None) -> dict:
    """B/A of every number both runs' figures carry (timestamps aside), by
    path: {a, b, b_over_a} (None when A's is 0). A figure one run did not
    measure is listed apart, never a ratio. Report only."""
    fa = (((va or {}).get("report") or {}).get("acceptance_figures_report_only")) or {}
    fb = (((vb or {}).get("report") or {}).get("acceptance_figures_report_only")) or {}
    a, b = dict(_leaves(fa)), dict(_leaves(fb))
    out = {}
    for path in sorted(set(a) & set(b)):
        if path[-1] in SKIP:
            continue
        out["/".join(path)] = {"a": a[path], "b": b[path],
                               "b_over_a": round(b[path] / a[path], 4) if a[path] else None}
    notes = []
    if not fa or not fb:
        notes.append("a run has no acceptance figures (a verdict before #919): no ratio for it")
    ia, ib = ((f.get("flow") or {}).get("identities") or {} for f in (fa, fb))
    for i in flowcontrol.IDENTITIES:
        ma, mb = (ia.get(i) or {}).get("measured"), (ib.get(i) or {}).get("measured")
        if ma != mb:
            notes.append(f"{i}'s flow schema / priority level differ between the runs ({ma} vs {mb})")
    return {"ratios": out, "notes": notes,
            "missing_in_one_run": sorted("/".join(p) for p in set(a) ^ set(b) if p[-1] not in SKIP)}
