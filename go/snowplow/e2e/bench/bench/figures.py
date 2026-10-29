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

from bench import apf, flowcontrol, load606, store_rebuild

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


# ─── reflector establishments after Ready (#933 review; #924 review R2) ─────
# What snowplow exposes (mem606 samples it every 15 s, MEM_VARS_PREFIXES):
#   reflector_watchlist_established_total (snowplow_reflector_path): a WATCH
#     with sendInitialEvents — a reflector establishing its whole state from
#     the watch cache. After Ready that is a relist, OR an informer snowplow
#     starts lazily for a GVR it first meets then (DepTracker.ensureInformer
#     -> EnsureResourceTypeFor), OR a CRD schema-change relist: one counter
#     for all three, so this report counts REFLECTOR ESTABLISHMENTS, never
#     "relists";
#   watch_errors_total (snowplow_informer_watch): every ListAndWatch error;
#     the reflector re-establishes after each (beside it, same window);
#   the two classic-LIST counters (reflector_list_cache_eligible_total,
#     reflector_list_etcd_delegated_total) count snowplow's own fallthrough
#     LISTs too: not read at all.
# All process-wide. A relist cannot be told from a first start, nor its GVR
# known, without a per-GVR / per-cause signal in snowplow itself — #932 step
# 0's; #933 reads relists from that. k6's requests carry no GVR either: the
# 50K-composition-GVR comparison is not derivable here, and the windows below
# are "some reflector established" against "none did", for every k6 call.
ESTABLISHMENT = {"watchlist_established": ("snowplow_reflector_path", "reflector_watchlist_established_total"),
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


def _establishment_counts(m: dict) -> dict | None:
    ex = m.get("extra") or {}
    out = {}
    for name, (fam, stat) in ESTABLISHMENT.items():
        v = (ex.get(fam) or {}).get(stat) if isinstance(ex.get(fam), dict) else None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out[name] = v
    return out or None


def reflector_establishments(mem: list[dict], after: float | None, k6_calls: list[tuple[float, float]],
                             tops: list[dict]) -> dict:
    """Reflector establishments after the boot (snowplow Ready at `after`):
    relists, lazy first informer starts and schema relists alike (the block
    above). Their count per hour, each one's interval (two consecutive mem606
    samples, 15 s) with snowplow's heap before / after and CPU (kubectl top)
    in it, and k6's calls completing in those intervals against the quiet
    ones."""
    rows = sorted((m for m in mem if m.get("kind") == "mem" and m.get("t") is not None
                   and _establishment_counts(m) is not None and (after is None or m["t"] >= after)),
                  key=lambda m: m["t"])
    if len(rows) < 2:
        n = sum(1 for m in mem if m.get("kind") == "mem")
        return {"status": "absent",
                "note": (f"{n} mem samples, {len(rows)} after Ready carry "
                         f"{' / '.join(f'{f}.{s}' for f, s in ESTABLISHMENT.values())}: the count is MISSING "
                         f"(an image or MEM_VARS_PREFIXES without them), not zero")}
    events, quiet, seconds, totals, resets = [], [], 0.0, dict.fromkeys(ESTABLISHMENT, 0), 0
    for a, b in zip(rows, rows[1:]):
        ca, cb = _establishment_counts(a), _establishment_counts(b)
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
    in_events = lat([e.pop("_ms") for e in events])
    hours = seconds / 3600
    return {"status": "present", "counts": "reflector establishments (relists, lazy first informer starts and "
                                           "schema relists alike) and ListAndWatch errors; NOT relists",
            "relists": "not derivable: a relist cannot be told from a first start without snowplow's per-GVR / "
                       "per-cause signal (#932 step 0)",
            "measured_s": round(seconds, 1), "counter_resets_skipped": resets,
            "totals": totals,
            "per_hour": {k: round(v / hours, 3) if hours else None for k, v in totals.items()},
            "none_occurred": not events,
            "per_gvr": "not exposed by snowplow (process-wide counters only)",
            "events": events,
            "k6_calls_in_establishment_windows": in_events,
            "k6_calls_in_quiet_windows": lat(quiet),
            "composition_gvr_comparison": "not derivable: neither an establishment's GVR nor a k6 call's GVR is "
                                          "collected (snowplow exposes no per-GVR signal; #932 step 0)",
            "classic_list_note": f"{' / '.join(CLASSIC_LIST)} also count snowplow's own fallthrough LISTs: "
                                 f"not read"}


# ─── first visits by fresh identity classes (#935) ──────────────────────────

def _int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def fresh_classes(o: dict | None) -> list[dict]:
    """browser.js's browser_fresh_class points: each fresh class's window
    (its sign-in to its first page) and snowplow's learned-class counters at
    both ends."""
    if not o:
        return []
    out = []
    for (_, t), at in zip(o["points"].get("browser_fresh_class", []),
                          (o.get("times") or {}).get("browser_fresh_class") or []):
        out.append({"session": t.get("session"), "class": t.get("class"), "t0": _int(t.get("t_before")),
                    "t1": at, **{k: _int(t.get(k)) for k in ("registered_before", "registered_after",
                                                              "seeded_before", "seeded_after")}})
    return sorted(out, key=lambda c: (c["t0"] is None, c["t0"] or 0))


def prove_cold(classes: list[dict]) -> dict:
    """Mark each class `cold_proven`: snowplow's learned-class registrations
    across its window cover it. Windows that overlap (any session; the
    counter is process-wide) form ONE group, every class in it — a class
    whose own counter read failed included: it may have registered, so it
    counts toward what the group must cover, and is itself unproven (#939
    review R1). A group's classes are proven when the registrations from
    its first read to its last (`max(after) - min(before)`, over the reads
    that came back) are at least the classes in the group; one registration
    for two sign-ins proves neither.

    The assumption that nothing else registers a class meanwhile is
    falsified, not trusted: the registrations seen BETWEEN groups (from one
    group's last read to the next group's first) are returned; non-zero
    means something else registers classes, and a group's proof may have
    counted one of them. The counter is a gauge (an eviction lowers it), so
    only the gaps that ROSE are summed: a gap that fell says nothing about
    registrations, and summing it signed would let an eviction in one gap
    cancel a foreign registration in another (#939 delta L7)."""
    known = [c for c in classes if c["t1"] is not None]
    for c in classes:
        if c["t1"] is None:
            c["cold_proven"], c["why"] = False, "no time for its window"
    groups: list[list[dict]] = []
    for c in sorted(known, key=lambda c: c["t0"] if c["t0"] is not None else c["t1"]):
        start = c["t0"] if c["t0"] is not None else c["t1"]
        if groups and start <= max(x["t1"] for x in groups[-1]):
            groups[-1].append(c)
        else:
            groups.append([c])
    edges = []                                    # per group: (first read, last read), None when none came back
    for g in groups:
        before = [x["registered_before"] for x in g if x["registered_before"] is not None]
        after = [x["registered_after"] for x in g if x["registered_after"] is not None]
        moved = max(after) - min(before) if before and after else None
        ok = moved is not None and moved >= len(g)
        for x in g:
            read_ok = x["t0"] is not None and x["registered_before"] is not None and x["registered_after"] is not None
            x["cold_proven"] = ok and read_ok
            if not read_ok:
                x["why"] = "its learned-class read failed (it still counts toward its window's registrations)"
            elif not ok:
                x["why"] = f"{moved} class(es) registered for {len(g)} sign-in(s) in the window"
            x["seed_ran_during_first_visit"] = (x["seeded_before"] is not None and x["seeded_after"] is not None
                                                and x["seeded_after"] > x["seeded_before"])
        edges.append((min(before) if before else None, max(after) if after else None))
    between = [nb - pa for (_, pa), (nb, _) in zip(edges, edges[1:]) if pa is not None and nb is not None]
    return {"registered_between_windows": sum(d for d in between if d > 0), "gaps_read": len(between)}


def _stats(v: list[float]) -> dict:
    return {"n": len(v), "p50_ms": _pct(v, 50), "p95_ms": _pct(v, 95), "max_ms": max(v) if v else None}


def fresh_first_visits(o: dict | None, phases, phase_of) -> dict:
    """Per route: the FIRST visits of fresh identity classes proven cold, per
    phase and over the run — n, p50, p95, max — and the classes not proven
    (left out, with why)."""
    classes = fresh_classes(o)
    if not classes:
        return {"status": "absent", "note": "no browser_fresh_class point: no fresh identity class ran "
                                            "(browser.js or run606 before #935, or no session switched)"}
    between = prove_cold(classes)
    ok = {(c["session"], c["class"]) for c in classes if c["cold_proven"]}
    by_route: dict[str, dict[str, list[float]]] = {}
    for (v, t), at in zip(o["points"].get("browser_page_load_ms", []),
                          (o.get("times") or {}).get("browser_page_load_ms") or []):
        if t.get("identity") != "fresh" or t.get("visit") != "first" or v is None or at is None:
            continue
        if (t.get("session"), t.get("class")) not in ok:
            continue
        r = by_route.setdefault(t.get("route"), {})
        ph = phase_of(at, phases)
        # each phase, the churn steps pooled (as every per-phase report), the run
        for p in (ph,) + (("churn",) if ph.startswith("churn:") else ()) + ("all",):
            r.setdefault(p, []).append(v)
    return {"status": "present", "classes": len(classes),
            "cold_proven": sum(1 for c in classes if c["cold_proven"]),
            "not_proven": [{k: c.get(k) for k in ("session", "class", "why")} for c in classes if not c["cold_proven"]],
            "seed_ran_during_first_visit": sum(1 for c in classes if c.get("seed_ran_during_first_visit")),
            # the falsifier of "nothing else registers a class": the rises of
            # the learned-class gauge between the fresh windows; non-zero
            # means a proof may have counted one of them
            "registered_between_windows": between["registered_between_windows"],
            "between_windows_read": between["gaps_read"],
            **({"warning": f"{between['registered_between_windows']} class(es) registered between the fresh "
                           f"windows: something else registers classes, so a window's proof may have counted "
                           f"one of them"} if between["registered_between_windows"] > 0 else {}),
            "by_route": {r: {p: _stats(v) for p, v in ps.items()} for r, ps in sorted(by_route.items())},
            "how": "first visits (visit=first) of fresh identity classes (identity=fresh) whose cold start "
                   "snowplow's learned-class counter proves; a page that timed out counts at its timeout"}


def run_figures(report: dict, recs: list[dict], window, facts: dict,
                k6_calls: list[tuple[float, float]] | None = None, browser: dict | None = None) -> dict:
    """One run's figures (report: its verdict report, with the APF report)."""
    cfg = next((r for r in recs if r.get("kind") == "flowcontrol_config"), None)
    ap = report.get("apiserver_flowcontrol_report_only") or {}
    out = {"boot": dict(facts, **boot_memory(recs, window, facts.get("pod"))),
           "flow": cfg or {"note": "no flowcontrol_config record: the identities' flow schemas are not known"},
           "steady_refreshes_per_core": steady_refreshes_per_core(report.get("customer_latency")),
           "reflector_establishments_after_ready": reflector_establishments(
               [r for r in recs if r.get("kind") == "mem"], facts.get("ready_at"), k6_calls or [],
               [r for r in recs if r.get("kind") == "top"]),
           # #969: store rebuilds, the quantities both mechanisms report (same paths on both arms)
           "store_rebuilds": store_rebuild.figures(report.get("store_rebuild_report_only")),
           # #935: phases from the run's own schedule (load606.run_phases), as every per-phase report
           "fresh_first_visits": fresh_first_visits(
               browser, load606.run_phases([r for r in recs if r.get("kind") in ("mem_start", "mem", "mem_end")],
                                           [r for r in recs if r.get("kind") in ("fresh", "churn")]),
               load606._phase_of)}
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
