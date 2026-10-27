"""The figures #922's acceptance is read from, per run and as B/A ratios of a
pair — report only (#919, from #922's review F3).

#922 removes client-go's 5/s client-side limiter. That changes when every
request snowplow sends at boot goes out: about 280 concurrent watch starts
and unpaced streaming-LIST pages. One run's figures (evaluate:
report["acceptance_figures_report_only"]), with boot as its own phase:

  boot                  restart_to_ready_s (`rollout restart` to the new
                        pod's Ready condition), start_to_ready_s (its
                        container's start to Ready), peak_memory_bytes (kubectl
                        top of the new pod during the boot window; metrics-
                        server's resolution, about 15 s);
  by_phase[p]           for snowplow's OWN flow schema and priority level
                        (flowcontrol_config: the schema APF classifies its
                        ServiceAccount's LIST / WATCH into): rejected
                        requests, request wait in APF's queues (count, mean,
                        p95, p99: the queue time), peak in-flight requests
                        (executing + in-queue of that flow schema, the max
                        over the scrapes: a sampled peak, shared with every
                        other client the schema serves) and the level's
                        demand_seats_high_watermark (its peak demand per
                        adjustment period); p = boot, before_measure, storm,
                        burst, each churn step, steady;
  steady_refreshes_per_core  snowplow's refresher completions per second
                        over its CPU cores (median kubectl top), in the
                        steady phase while customers ran.

A request the refresher sends as an impersonated user (#615) is classified
by that user, not by snowplow's ServiceAccount: the record names that flow
schema too (impersonated_user), and by_phase carries it apart.

custlat pair adds `acceptance_ratios_report_only`: each figure as
{a, b, b_over_a}.
"""

from __future__ import annotations

from bench import apf, load606


def _fs_figures(rep: dict, fs: str | None, pl: str | None) -> dict:
    """One flow schema's figures in one phase of the APF report."""
    lv = ((rep or {}).get("priority_levels") or {}).get(pl) or {}
    node = (lv.get("flow_schemas") or {}).get(fs) or {}
    w = (node.get("wait") or {}).get("executed") or {}
    return {"rejected": sum((node.get("rejected") or {}).values()),
            "wait_requests": w.get("requests", 0),
            "wait_mean_s": w.get("mean_s"), "wait_p95_s": w.get("p95_s"), "wait_p99_s": w.get("p99_s"),
            "peak_inflight_requests": (node.get(apf.INFLIGHT) or {}).get("max"),
            "level_demand_seats_high_watermark": lv.get("demand_seats_high_watermark")}


def boot_window(recs: list[dict]) -> tuple[tuple[float, float] | None, dict]:
    """(boot window, its facts): from run606's boot record (the restart's
    time) to the new pod's Ready (its before-record's ready_at)."""
    boot = next((r for r in recs if r.get("kind") == "boot" and r.get("t_restart") is not None), None)
    pod = next((r for r in recs if r.get("kind") == "snowplow_pod" and r.get("phase") == "before"), None)
    facts = {"t_restart": (boot or {}).get("t_restart"), "pod": (pod or {}).get("pod"),
             "started_at": (pod or {}).get("started_at"), "ready_at": (pod or {}).get("ready_at")}
    if boot is None or pod is None or pod.get("ready_at") is None:
        facts["note"] = ("no boot record (run606 before #919)" if boot is None else
                         "no Ready time for the new pod (its before-record has no ready_at)")
        return None, facts
    t0, t1 = float(boot["t_restart"]), float(pod["ready_at"])
    facts["restart_to_ready_s"] = round(t1 - t0, 1)
    if pod.get("started_at") is not None:
        facts["start_to_ready_s"] = round(t1 - float(pod["started_at"]), 1)
    return (t0, t1), facts


def boot_memory(recs: list[dict], window, pod: str | None) -> dict:
    """The new pod's peak memory (kubectl top, kind top_boot) in the window."""
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
    return {"peak_memory_bytes": peak, "memory_samples": sum(1 for r in rows if not r.get("failed")),
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


def run_figures(report: dict, recs: list[dict], window, facts: dict) -> dict:
    """One run's figures (report: its verdict report, with the APF report)."""
    cfg = next((r for r in recs if r.get("kind") == "flowcontrol_config"), None)
    ap = report.get("apiserver_flowcontrol_report_only") or {}
    out = {"boot": dict(facts, **boot_memory(recs, window, facts.get("pod"))),
           "flow": ({k: cfg.get(k) for k in ("snowplow_sa", "flow_schema", "priority_level", "partial_rules",
                                             "impersonated_user", "error")} if cfg else
                    {"note": "no flowcontrol_config record: snowplow's flow schema is not known"}),
           "steady_refreshes_per_core": steady_refreshes_per_core(report.get("customer_latency"))}
    if cfg and cfg.get("flow_schema") and ap.get("status") == "present":
        iu = cfg.get("impersonated_user") or {}
        out["by_phase"] = {p: {"snowplow_sa": _fs_figures(r, cfg["flow_schema"], cfg["priority_level"]),
                               "impersonated_user": _fs_figures(r, iu.get("flow_schema"),
                                                                iu.get("priority_level"))}
                           for p, r in (ap.get("by_phase") or {}).items()}
    else:
        out["by_phase"] = None
        out["by_phase_note"] = ("the APF report is absent" if ap.get("status") != "present"
                                else "snowplow's flow schema is not known")
    return out


def _leaves(d, prefix=()):
    if isinstance(d, dict):
        for k, v in d.items():
            yield from _leaves(v, prefix + (str(k),))
    elif isinstance(d, (int, float)) and not isinstance(d, bool):
        yield prefix, d


def ratios(va: dict | None, vb: dict | None) -> dict:
    """B/A of every number both runs' figures carry (timestamps aside), by
    path: {a, b, b_over_a} (None when A's is 0). Report only."""
    fa = (((va or {}).get("report") or {}).get("acceptance_figures_report_only")) or {}
    fb = (((vb or {}).get("report") or {}).get("acceptance_figures_report_only")) or {}
    skip = ("t_restart", "started_at", "ready_at")
    a, b = dict(_leaves(fa)), dict(_leaves(fb))
    out = {}
    for path in sorted(set(a) & set(b)):
        if path[-1] in skip:
            continue
        out["/".join(path)] = {"a": a[path], "b": b[path], "b_over_a": round(b[path] / a[path], 4) if a[path] else None}
    notes = []
    if not fa or not fb:
        notes.append("a run has no acceptance figures (a verdict before #919): no ratio for it")
    for k in ("flow_schema", "priority_level"):
        if (fa.get("flow") or {}).get(k) != (fb.get("flow") or {}).get(k):
            notes.append(f"the runs' {k} differ ({(fa.get('flow') or {}).get(k)} vs {(fb.get('flow') or {}).get(k)})")
    return {"ratios": out, "notes": notes,
            "missing_in_one_run": sorted("/".join(p) for p in set(a) ^ set(b) if p[-1] not in skip)}
