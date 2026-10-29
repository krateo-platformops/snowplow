"""How a snowplow build rebuilds an informer's store, and what a rebuild costs,
read from the counters THAT build publishes (#969).

A rebuild is a confirmed divergence's repair, or a CRD schema change. Two
mechanisms exist:

  relist          (to 1.12.59) the informer is torn down and relisted; the
                  1.12.5 post-sync re-fire and the 1.12.6 relist bridge cover
                  the teardown window; repairs wait in a serialised queue
                  (snowplow_crd_discovery.relist_bridge_*,
                  snowplow_store_verification.store_repairs_pending).
  build_and_swap  (#870, from 1.12.60) the replacement is built beside the
                  live informer and swapped in; the GVR stays servable; the
                  changed keys are known-stale until the swap
                  (snowplow_store_swap, snowplow_store_swap_known_stale).

A snapshot's mechanism is the one whose identifying counter it PUBLISHES. It
is never inferred from a version string, so the two arms of a paired run
(1.12.59 against 1.12.60) are each read by their own counters, and a build
that publishes neither or both gives no answer, never a zero: an absent key is
not a zero reading.

What both mechanisms share is reported under the same names, so a pair reads
the same quantities on both arms:
  repairs_requested     store_repairs_fired_total (kept, same name, by #870)
  stale_served          snowplow_resolved_cache stale_served_total and
                        known_stale_served_total
  resolve_fallthroughs  snowplow_apiserver_fallthrough_cells for the two
                        reasons a rebuild window causes
                        (informer-fallthrough-not-synced / -not-servable), by
                        GVR. The meter counts a resolve's call to the live
                        apiserver; whether it was a GET or a LIST is not in it.
                        No /debug/vars counter counts resolve-ORIGINATED LISTs
                        (checked at #928): snowplow_reflector_path counts every
                        collection GET through the shared rest.Config,
                        reflectors' and fall-throughs' alike, and #870's K1
                        RoundTripper LIST count is the kind arm's own
                        (store_swap_kind_integration_test.go), not published.
  watch_submits         snowplow_deps_dirty_mark_attribution.submit_source.watch
  rebuild intervals     the mem606 sample pairs (15 s) a rebuild was requested,
                        installed or open in: the window both mechanisms can be
                        bounded by, at the sampler's resolution.
What only one mechanism publishes (build_and_swap: the installed builds, the
known-stale keys and window, the push ledger) is reported for it and listed as
`not_published` for the other, never as a zero.

STDLIB ONLY.
"""

from __future__ import annotations

import statistics

SWAP = "snowplow_store_swap"
SWAP_KS = "snowplow_store_swap_known_stale"
VERIFY = "snowplow_store_verification"
CRD = "snowplow_crd_discovery"
LIST_INCREMENTAL = "snowplow_list_incremental"
FALLTHROUGH = "snowplow_apiserver_fallthrough_cells"
RESOLVED = "snowplow_resolved_cache"
DIRTY = "snowplow_deps_dirty_mark_attribution"

# #870's closed label sets (internal/cache/store_swap*.go at #928)
REASONS = ("divergence", "schema")
OUTCOMES = ("swap", "abort_clean", "abort_refused", "abort_teardown")
CLOSES = ("pushed", "pushed_on_arm", "pushed_resync", "known_stale", "evicted", "live_event")
# the fall-through reasons a rebuild window causes (#870 K1: 0 of each for the GVR during a build)
WINDOW_REASONS = ("informer-fallthrough-not-synced", "informer-fallthrough-not-servable")

# The /debug/vars prefixes mem606 must capture for this report (load606.RUN8_VARS_PREFIXES).
VARS_PREFIXES = (SWAP, FALLTHROUGH, VERIFY, RESOLVED, "snowplow_deps_dirty_mark_")

MECHANISMS = {
    "relist": {
        "identified_by": ((VERIFY, "store_repairs_pending"), (CRD, "relist_bridge_runs_total")),
        "how": "the informer is torn down and relisted (to 1.12.59)",
    },
    "build_and_swap": {
        "identified_by": ((SWAP, "store_swaps_total"),),
        "how": "the replacement is built beside the live informer and swapped in (#870, from 1.12.60)",
    },
}

# Cumulative counters per mechanism: (name, family, stat, label). label None =
# a scalar; a label reads one member of a counter map.
_SWAP_COUNTERS = (
    [("rebuilds_installed", SWAP, "store_swaps_total", None),
     ("superseded", SWAP, "store_swap_superseded_total", None),
     ("m_keys", SWAP, "store_swap_m_keys_total", None),
     ("deliveries", SWAP, "store_swap_deliveries_total", None),
     ("reused_objects", SWAP, "store_swap_reused_objects_total", None),
     ("regressed_keys", SWAP, "store_swap_regressed_keys", None),
     # #928 F5 (5a6bb60c): a schema change on a factory-backed GVR, which no copy can be built for
     ("schema_unowned", SWAP, "store_swap_schema_unowned_total", None)]
    + [(f"{q}/{r}|{o}", SWAP_KS, f"store_swap_known_stale_{q}_total", f"{r}|{o}")
       for q in ("keys", "builds", "window_seconds") for r in REASONS for o in OUTCOMES]
    + [(f"closes/{r}|{c}", SWAP_KS, "store_swap_ledger_closes_total", f"{r}|{c}")
       for r in REASONS for c in CLOSES]
    + [(f"{h}/{r}|{c}/{s}", SWAP_KS, f"store_swap_{h}_to_push_ms", (f"{r}|{c}", s))
       for h in ("detect", "install") for r in REASONS for c in CLOSES for s in ("count", "sum_ms")])
COUNTERS = {
    "relist": (),
    "build_and_swap": tuple(_SWAP_COUNTERS),
}
# Gauges per mechanism: open = any of them above 0.
GAUGES = {
    "relist": (("store_repairs_pending", VERIFY, "store_repairs_pending", None),
               ("store_repair_queue_age_seconds", VERIFY, "store_repair_queue_age_seconds", None)),
    "build_and_swap": tuple(
        (f"store_swap_{g}.{r}", SWAP_KS, f"store_swap_{g}", r)
        for g in ("known_stale_keys_open", "known_stale_window_open_seconds_max") for r in REASONS),
}
# the histograms (detect / install -> push) and the labelled groups' report names
HISTOGRAMS = {"relist": (), "build_and_swap": ("detect", "install")}
RENAME = {"keys": "known_stale_keys", "builds": "known_stale_builds", "window_seconds": "known_stale_window_s",
          "closes": "ledger_closes"}
# lifetime maxima read at the last sample: (name, family, stat, labels)
LIFETIME_MAX = {"relist": (), "build_and_swap": tuple(
    (f"{h}_to_push_max_ms", SWAP_KS, f"store_swap_{h}_to_push_ms", tuple(f"{r}|{c}" for r in REASONS for c in CLOSES))
    for h in ("detect", "install"))}
# report-only gauges (not an "open" signal: ledger_owed is an expected outcome, not a detector)
EXTRA_GAUGES = {"relist": (), "build_and_swap": tuple(
    (f"store_swap_ledger_owed.{r}", SWAP_KS, "store_swap_ledger_owed", r) for r in REASONS)}
# what each mechanism does not publish (listed, never zero)
NOT_PUBLISHED = {
    "relist": ("rebuilds_installed", "known_stale_keys", "known_stale_window_s", "ledger_closes",
               "detect_to_push_ms", "install_to_push_ms", "m_keys", "deliveries", "regressed_keys"),
    "build_and_swap": ("store_repairs_pending", "store_repair_queue_age_seconds"),
}
# what a mechanism CLAIMS during a rebuild (the #870 design's K1): checked, report only
CLAIMS = {
    "relist": {},
    "build_and_swap": {"resolve_fallthroughs_during_rebuilds": 0},
}


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def read(x: dict, family: str, stat: str, label=None):
    """One published number, or None when the build does not publish it."""
    f = x.get(family)
    if not isinstance(f, dict) or stat not in f:
        return None
    v = f[stat]
    if label is None:
        return _num(v)
    if isinstance(label, tuple):                 # a histogram cell's member
        cell = v.get(label[0]) if isinstance(v, dict) else None
        return _num(cell.get(label[1])) if isinstance(cell, dict) else None
    return _num(v.get(label)) if isinstance(v, dict) else None


def mechanism(x: dict) -> tuple[str | None, str]:
    """(the mechanism this /debug/vars snapshot publishes, why); None when it
    publishes neither or both."""
    hits = {m: [f"{f}.{s}" for f, s in d["identified_by"] if read(x, f, s) is not None]
            for m, d in MECHANISMS.items()}
    found = [m for m, keys in hits.items() if keys]
    if len(found) == 1:
        return found[0], f"publishes {', '.join(hits[found[0]])}"
    if not found:
        return None, ("publishes neither " + " nor ".join(
            "/".join(f"{f}.{s}" for f, s in d["identified_by"]) for d in MECHANISMS.values()))
    return None, "publishes both: " + "; ".join(f"{m}: {', '.join(k)}" for m, k in hits.items() if k)


def counters(x: dict, mech: str) -> dict | None:
    """The cumulative counters of one sample under `mech` (None = a counter the
    build does not publish; a fall-through cell is 0 only when the meter is
    published and the cell has not counted yet)."""
    out = {"repairs_requested": read(x, VERIFY, "store_repairs_fired_total"),
           "stale_served": read(x, RESOLVED, "stale_served_total"),
           "known_stale_served": read(x, RESOLVED, "known_stale_served_total"),
           "watch_submits": read(x, DIRTY, "submit_source", "watch")}
    for name, fam, stat, label in COUNTERS[mech]:
        out[name] = read(x, fam, stat, label)
    cells = x.get(FALLTHROUGH)
    if isinstance(cells, dict):
        for reason in WINDOW_REASONS:
            out[f"fallthrough/{reason}"] = 0
        for cell, n in cells.items():
            parts = str(cell).split("|")
            if len(parts) == 3 and parts[2] in WINDOW_REASONS and _num(n) is not None:
                out[f"fallthrough/{parts[2]}"] += n
                out[f"fallthrough_gvr/{parts[1]}|{parts[2]}"] = out.get(f"fallthrough_gvr/{parts[1]}|{parts[2]}", 0) + n
    return out


def gauges(x: dict, mech: str) -> dict:
    return {name: read(x, fam, stat, label) for name, fam, stat, label in GAUGES[mech] + EXTRA_GAUGES[mech]}


def is_open(x: dict, mech: str) -> bool:
    return any((read(x, fam, stat, label) or 0) > 0 for _, fam, stat, label in GAUGES[mech])


# ─── the report ─────────────────────────────────────────────────────────────

def _extra(m: dict) -> dict:
    return m.get("extra") or {}


def _start_at(pods: list[dict], t: float) -> float | None:
    """snowplow's container start for a sample at `t` (the latest start before it)."""
    starts = [p["started_at"] for p in pods if _num(p.get("started_at")) is not None and p["started_at"] <= t]
    return max(starts) if starts else None


def gc_share(a: dict, b: dict, pods: list[dict]) -> float | None:
    """The GC's share of the available CPU between two samples' last GCs.

    Go's GCCPUFraction is GC CPU / available CPU (wall x GOMAXPROCS) since the
    process started, as of the last GC (memstats LastGC). Over a pair:
    (f_b * U_b - f_a * U_a) / (U_b - U_a), U = last GC - process start; GOMAXPROCS
    cancels. The start is the container's (pod startedAt, 1 s resolution).
    None when a sample lacks the fraction or LastGC, no start precedes it, or
    no GC ran in between."""
    fa, fb = _num(a.get("gc_cpu_fraction")), _num(b.get("gc_cpu_fraction"))
    la, lb = _num(a.get("last_gc")), _num(b.get("last_gc"))
    s = _start_at(pods, a.get("t") or 0)
    if None in (fa, fb, la, lb, s) or s != _start_at(pods, b.get("t") or 0):
        return None
    ua, ub = la / 1e9 - s, lb / 1e9 - s
    if ub <= ua or ua <= 0:
        return None
    return round((fb * ub - fa * ua) / (ub - ua), 4)


def _sum_d(da: dict, prefix: str) -> float | None:
    vs = [v for k, v in da.items() if k.startswith(prefix) and v is not None]
    return round(sum(vs), 3) if vs else None


def report(mem: list[dict], pods: list[dict], phases: list, phase_of, deltas) -> dict:
    """The rebuild report for one run.

    mem: mem606 records; pods: snowplow_pod records (container starts);
    phases / phase_of: load606.run_phases' schedule and its lookup; deltas:
    load606._deltas (a pair's counter deltas, restart-aware)."""
    rows = sorted((m for m in mem if m.get("kind") == "mem" and m.get("extra") is not None
                   and m.get("t") is not None), key=lambda m: m["t"])
    if not rows:
        return {"status": "absent",
                "note": "no mem sample carries /debug/vars extras: the report is MISSING, not zero"}
    seen: dict[str, int] = {}
    for m in rows:
        mech, why = mechanism(_extra(m))
        label = mech or f"none ({why})"
        seen[label] = seen.get(label, 0) + 1
    mechs = {mechanism(_extra(m))[0] for m in rows}
    if len(mechs) != 1 or None in mechs:
        return {"status": "absent" if mechs == {None} else "invalid",
                "samples_by_mechanism": seen,
                "note": ("no sample publishes a rebuild mechanism's counters: the report is MISSING, not a zero"
                         if mechs == {None} else
                         "the samples do not all publish one rebuild mechanism: no rebuild quantity is read "
                         "(an absent counter is not a zero)")}
    mech = mechs.pop()
    intervals, quiet, phase_tot, restarts = [], [], {}, 0
    for a, b in zip(rows, rows[1:]):
        ca, cb = counters(_extra(a), mech), counters(_extra(b), mech)
        # a fall-through cell the earlier sample's meter had not counted yet read 0 there
        meter_a = isinstance(_extra(a).get(FALLTHROUGH), dict)
        d, r = deltas(a, b, {k: ca.get(k, 0 if meter_a and k.startswith("fallthrough_gvr/") else None)
                             for k in cb}, cb)
        if r:
            restarts += 1
            continue
        secs = b["t"] - a["t"]
        ph = phase_of(b["t"], phases)
        p = phase_tot.setdefault(ph, {"seconds": 0.0, "pairs": 0, "rebuild_pairs": 0, "counters": {}})
        p["seconds"] += secs
        p["pairs"] += 1
        for k, v in d.items():
            if v is not None:
                p["counters"][k] = p["counters"].get(k, 0) + v
        active = (is_open(_extra(a), mech) or is_open(_extra(b), mech)
                  or (d.get("repairs_requested") or 0) > 0 or (d.get("rebuilds_installed") or 0) > 0)
        row = {"t0": a["t"], "t1": b["t"], "phase": ph, "d": d,
               "heap_inuse_mb": [x for x in (a.get("heap_inuse_mb"), b.get("heap_inuse_mb")) if x is not None],
               "gc_share": gc_share(a, b, pods)}
        if active:
            p["rebuild_pairs"] += 1
            intervals.append(row)
        else:
            quiet.append(row)

    def per_s(rws, key):
        return [r["d"][key] / (r["t1"] - r["t0"]) for r in rws
                if r["d"].get(key) is not None and r["t1"] > r["t0"]]

    # the pair right after each run of rebuild pairs ends (for build_and_swap,
    # the install: the swap unmutes there)
    after = []
    for i, r in enumerate(intervals):
        nxt = intervals[i + 1] if i + 1 < len(intervals) else None
        if nxt is None or nxt["t0"] != r["t1"]:
            follow = next((q for q in quiet if q["t0"] == r["t1"]), None)
            if follow is not None:
                after.append((r, follow))
    q_rate = per_s(quiet, "watch_submits")
    q_med = statistics.median(q_rate) if q_rate else None
    def rate_of(pair):
        rate = per_s([pair], "watch_submits")
        return {"t0": pair["t0"], "t1": pair["t1"], "phase": pair["phase"],
                "watch_submits_per_s": round(rate[0], 3) if rate else None,
                "over_quiet_median": round(rate[0] / q_med, 3) if rate and q_med else None}
    # the pair a rebuild ends in (build_and_swap: it holds the install and the
    # unmute) and the pair after it
    watch_after = [{"end_pair": rate_of(r), "next_pair": rate_of(f)} for r, f in after]

    fall = {k: sum(r["d"].get(f"fallthrough/{k}") or 0 for r in intervals) for k in WINDOW_REASONS}
    fall_known = all(r["d"].get(f"fallthrough/{k}") is not None for r in intervals for k in WINDOW_REASONS)
    by_gvr: dict[str, float] = {}
    for r in intervals:
        for k, v in r["d"].items():
            if k.startswith("fallthrough_gvr/") and v:
                by_gvr[k.split("/", 1)[1]] = by_gvr.get(k.split("/", 1)[1], 0) + v
    heap_in = [h for r in intervals for h in r["heap_inuse_mb"]]
    heap_q = [h for r in quiet for h in r["heap_inuse_mb"]]
    gc_in = [r["gc_share"] for r in intervals if r["gc_share"] is not None]
    gc_q = [r["gc_share"] for r in quiet if r["gc_share"] is not None]
    first = intervals[0] if intervals else None
    before = next((q for q in reversed(quiet) if first and q["t1"] <= first["t0"]), None)
    total_fall = sum(fall.values())
    watch = {
        "resolve_fallthroughs_during_rebuilds": {
            "value": total_fall if fall_known else None,
            "by_reason": fall if fall_known else None,
            "by_gvr_reason": by_gvr,
            "claim": CLAIMS[mech].get("resolve_fallthroughs_during_rebuilds"),
            "how": (f"Δ {FALLTHROUGH} for {' / '.join(WINDOW_REASONS)} over the rebuild pairs: a resolve's "
                    f"call to the live apiserver (GET or LIST: the meter does not say which; no /debug/vars "
                    f"counter counts resolve-originated LISTs, #870's RoundTripper count is its kind arm's own)"),
            **({} if fall_known else {"missing": f"{FALLTHROUGH} absent in a rebuild pair's samples"})},
        "watch_submits_after_rebuild": {
            "quiet_median_per_s": round(q_med, 3) if q_med is not None else None,
            "after_each_rebuild": watch_after,
            "max_over_quiet_median": max((w[x]["over_quiet_median"] for w in watch_after for x in w
                                          if w[x]["over_quiet_median"] is not None), default=None),
            "how": (f"{DIRTY}.submit_source.watch per second in the sample pair each rebuild ends in "
                    f"(build_and_swap: it holds the install, where the swap unmutes) and in the pair after it, "
                    f"against the quiet pairs' median; 15 s pairs, so a spike in the seconds after the unmute "
                    f"is diluted over its pair")},
        "heap_and_gc_during_rebuilds": {
            "heap_inuse_mb_peak": max(heap_in) if heap_in else None,
            "heap_inuse_mb_before_first": (before["heap_inuse_mb"][-1] if before and before["heap_inuse_mb"]
                                           else None),
            "heap_inuse_mb_quiet_p50": statistics.median(heap_q) if heap_q else None,
            "gc_cpu_share_max": max(gc_in) if gc_in else None,
            "gc_cpu_share_quiet_p50": statistics.median(gc_q) if gc_q else None,
            "how": ("heap_inuse over the rebuild pairs' samples; the GC's share of the available CPU per pair "
                    "from GCCPUFraction and memstats LastGC (gc_share); quiet = the pairs with no rebuild")},
    }
    claim = watch["resolve_fallthroughs_during_rebuilds"]["claim"]
    violations = ([f"store rebuilds ({mech}): {total_fall} resolve fall-through(s) to the apiserver during "
                   f"rebuilds ({', '.join(f'{k} {v}' for k, v in fall.items() if v)}); the mechanism claims "
                   f"{claim}"] if claim is not None and fall_known and total_fall > claim else [])

    # every phase summed, as the other per-phase reports
    tot = {"seconds": 0.0, "pairs": 0, "rebuild_pairs": 0, "counters": {}}
    for p in phase_tot.values():
        tot["seconds"] += p["seconds"]
        tot["pairs"] += p["pairs"]
        tot["rebuild_pairs"] += p["rebuild_pairs"]
        for k, v in p["counters"].items():
            tot["counters"][k] = tot["counters"].get(k, 0) + v
    phase_tot["all"] = tot
    by_phase = {}
    for ph, p in phase_tot.items():
        c = p["counters"]
        q = {"seconds": round(p["seconds"], 1), "pairs": p["pairs"], "rebuild_pairs": p["rebuild_pairs"],
             "repairs_requested": c.get("repairs_requested"),
             "stale_served": c.get("stale_served"), "known_stale_served": c.get("known_stale_served"),
             "resolve_fallthroughs": {k: c.get(f"fallthrough/{k}") for k in WINDOW_REASONS},
             "watch_submits": c.get("watch_submits")}
        q |= _published(c, mech)
        by_phase[ph] = q
    last = _extra(rows[-1])
    lifetime_max = {name: {lbl: v for lbl, v in ((lbl, read(last, fam, stat, (lbl, "max_ms"))) for lbl in labels)
                           if v}
                    for name, fam, stat, labels in LIFETIME_MAX[mech]}
    return {"status": "present", "mechanism": mech, "how": MECHANISMS[mech]["how"],
            "identified_by": mechanism(last)[1], "samples": len(rows), "restarts_skipped": restarts,
            "rebuild_pairs": len(intervals),
            "rebuild_s_bounded": round(sum(r["t1"] - r["t0"] for r in intervals), 1),
            "by_phase": by_phase,
            "rebuilds": [{"t0": r["t0"], "t1": r["t1"], "phase": r["phase"],
                          **{k: r["d"].get(k) for k in ("repairs_requested", "rebuilds_installed", "m_keys",
                                                        "deliveries", "stale_served", "watch_submits")
                             if r["d"].get(k) is not None},
                          "resolve_fallthroughs": {k: r["d"].get(f"fallthrough/{k}") for k in WINDOW_REASONS},
                          "heap_inuse_mb_max": max(r["heap_inuse_mb"]) if r["heap_inuse_mb"] else None,
                          "gc_cpu_share": r["gc_share"]} for r in intervals],
            "gauges_last": gauges(last, mech),
            **lifetime_max,
            "watch": watch, "claim_violations": violations,
            "not_published": list(NOT_PUBLISHED[mech]),
            "note": ("report only. A rebuild pair is a mem606 sample pair in which a rebuild was requested, "
                     "installed or open: the window at the sampler's resolution, the same for both mechanisms. "
                     "*_to_push_max_ms are lifetime maxima (the last sample's)")}


def _published(c: dict, mech: str) -> dict:
    """A phase's sums of the counters only `mech` publishes: a scalar by name, a
    labelled counter as {label: n} (non-zero labels), a histogram as the mean
    of its entries {label: mean_ms}."""
    out: dict = {}
    for name, *_ in COUNTERS[mech]:
        group, _, label = name.partition("/")
        if not label:
            out[name] = c.get(name)
        elif group in HISTOGRAMS[mech]:
            lbl, _, part = label.rpartition("/")
            n, s = c.get(f"{group}/{lbl}/count"), c.get(f"{group}/{lbl}/sum_ms")
            if part == "count" and n:
                out.setdefault(f"{group}_to_push_mean_ms", {})[lbl] = round(s / n, 1) if s is not None else None
        elif c.get(name):
            out.setdefault(RENAME.get(group, group), {})[label] = c[name]
    return out


def figures(rep: dict | None) -> dict:
    """The quantities both mechanisms report, for the pair's B/A ratios
    (figures.run_figures): same paths on both arms."""
    if not rep or rep.get("status") != "present":
        return {"status": (rep or {}).get("status", "absent")}
    w = rep["watch"]
    allp = rep["by_phase"]["all"]
    tot = {k: allp.get(k) for k in ("repairs_requested", "stale_served", "known_stale_served", "watch_submits")}
    return {"rebuild_pairs": rep["rebuild_pairs"], "rebuild_s_bounded": rep["rebuild_s_bounded"],
            **tot,
            "resolve_fallthroughs_during_rebuilds": w["resolve_fallthroughs_during_rebuilds"]["value"],
            "watch_submits_after_rebuild_max_over_quiet": w["watch_submits_after_rebuild"]["max_over_quiet_median"],
            "heap_inuse_mb_peak_during_rebuilds": w["heap_and_gc_during_rebuilds"]["heap_inuse_mb_peak"],
            "gc_cpu_share_max_during_rebuilds": w["heap_and_gc_during_rebuilds"]["gc_cpu_share_max"]}
