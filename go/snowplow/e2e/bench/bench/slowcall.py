"""#1054 (snowplow PR #1059): the slow-call record, read from /debug/vars.

A request whose server time passes slowcall.Bound (the page p95 goal, 3 s)
leaves, on snowplow builds that carry #1059:
  snowplow_slow_requests_total            {route: n}
  snowplow_slow_request_phase_ms          {phase: {buckets {le_<ms>, le_inf}, count, sum_ms, max_ms}}
  snowplow_slow_requests_recent           the last 32 records (route, wall_ms, outcome,
                                          phase, wait_phase, kind, wait_key digest, gvr, ...)
  snowplow_slow_request_snapshots_total / _suppressed_total
  snowplow_slow_request_snapshots_recent  the last 2 goroutine snapshots (stacks, requests)
  snowplow_slow_request_bound_ms
mem606 records every key under load606.SLOW_REQUEST_PREFIX every 15 s,
unscrubbed (metadata: routes are patterns, keys are digests, stacks are code
locations). That is the source: the log path keeps a record's numbers but
logscrub hashes a snapshot's stacks whole.

report() is REPORT ONLY, per run (each arm of a pair has its own): a build
without the keys is UNMEASURED, never 0; a malformed value is counted and
named, never fatal. STDLIB ONLY.
"""

from __future__ import annotations

BY_ROUTE = "snowplow_slow_requests_total"
PHASE_MS = "snowplow_slow_request_phase_ms"
RECENT = "snowplow_slow_requests_recent"
SNAPSHOTS = "snowplow_slow_request_snapshots_total"
SUPPRESSED = "snowplow_slow_request_snapshots_suppressed_total"
SNAPSHOTS_RECENT = "snowplow_slow_request_snapshots_recent"
BOUND = "snowplow_slow_request_bound_ms"
# a record's fields the report keeps (#1059 monitor.go's attrs; the per-phase
# <phase>_ms / _n / _max_ms numbers are in the phase histogram)
RECORD_FIELDS = ("time", "route", "method", "wall_ms", "bound_ms", "outcome", "class", "phase", "phase_sum_ms",
                 "wait_phase", "kind", "wait_ms", "wait_outcome", "wait_key", "gvr")
SNAPSHOT_FIELDS = ("time", "route", "inflight", "slow_inflight", "oldest_age_ms", "yield_parked", "goroutines",
                   "request_goroutines", "background_goroutines", "other_goroutines", "stacks_kept",
                   "stacks_omitted", "truncated", "wait_phase", "kind", "wait_ms", "wait_outcome", "wait_key", "gvr")
GOAL_MARKS_MS = (3000, 10000)       # the page goal's p95 / max marks, buckets of the histogram
NOTE = ("report only; from /debug/vars as mem606 samples it (every 15 s): the record ring holds the last 32 "
        "and the snapshot ring the last 2, so a burst between two samples can be under-counted (records_seen "
        "against slow_requests_during_run says how much)")


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _samples(mem: list[dict]) -> list[dict]:
    return [m.get("extra") or {} for m in mem or [] if m.get("kind") == "mem"]


def report(mem: list[dict], prefix: str) -> dict:
    """The slow-call section of one run's verdict (report only)."""
    ex = _samples(mem)
    if not ex:
        return {"status": "UNMEASURED", "why": "no mem606 sample: /debug/vars was not recorded", "note": NOTE}
    carrying = [e for e in ex if any(str(k).startswith(prefix) for k in e)]
    if not carrying:
        return {"status": "UNMEASURED",
                "why": f"no /debug/vars sample carries a {prefix}* key: the build predates #1054's record "
                       f"(snowplow PR #1059), or mem606 was not given the prefix",
                "note": NOTE}
    malformed: list[str] = []
    first, last = carrying[0], carrying[-1]

    def routes(e):
        v = e.get(BY_ROUTE)
        if v is None:
            return None
        if not isinstance(v, dict) or any(_num(n) is None for n in v.values()):
            malformed.append(f"{BY_ROUTE} is not a {{route: count}} map")
            return None
        return v
    r0, r1 = routes(first), routes(last)
    during = None
    if r1 is not None:
        during = {k: n - ((r0 or {}).get(k) or 0) for k, n in r1.items() if n - ((r0 or {}).get(k) or 0)}
    phases = {}
    hist = last.get(PHASE_MS)
    if hist is not None and not isinstance(hist, dict):
        malformed.append(f"{PHASE_MS} is not a {{phase: histogram}} map")
    for ph, h in (hist or {}).items() if isinstance(hist, dict) else ():
        if not isinstance(h, dict) or _num(h.get("count")) is None:
            malformed.append(f"{PHASE_MS}[{ph}] is not a histogram")
            continue
        if not h["count"]:
            continue
        b = h.get("buckets") if isinstance(h.get("buckets"), dict) else {}
        phases[ph] = {"count": h["count"], "sum_ms": _num(h.get("sum_ms")), "max_ms": _num(h.get("max_ms")),
                      **{f"le_{m}": _num(b.get(f"le_{m}")) for m in GOAL_MARKS_MS},
                      "over_3000": (h["count"] - b["le_3000"]) if _num(b.get("le_3000")) is not None else None}
    records, seen = [], set()
    for e in carrying:
        ring = e.get(RECENT)
        if ring is None:
            continue
        if not isinstance(ring, list):
            malformed.append(f"{RECENT} is not a list")
            continue
        for r in ring:
            if not isinstance(r, dict) or _num(r.get("wall_ms")) is None or not isinstance(r.get("route"), str):
                malformed.append(f"a {RECENT} entry has no route / wall_ms ({type(r).__name__})")
                continue
            k = (str(r.get("time")), r["route"], r["wall_ms"], str(r.get("wait_key")))
            if k in seen:
                continue
            seen.add(k)
            records.append({f: r[f] for f in RECORD_FIELDS if f in r})
    snaps, sseen, last_text = [], set(), None
    for e in carrying:
        ring = e.get(SNAPSHOTS_RECENT)
        if ring is None:
            continue
        if not isinstance(ring, list):
            malformed.append(f"{SNAPSHOTS_RECENT} is not a list")
            continue
        for s in ring:
            if not isinstance(s, dict) or not isinstance(s.get("stacks"), str):
                malformed.append(f"a {SNAPSHOTS_RECENT} entry carries no stacks text ({type(s).__name__})")
                continue
            k = (str(s.get("time")), str(s.get("route")))
            if k in sseen:
                continue
            sseen.add(k)
            snaps.append({**{f: s[f] for f in SNAPSHOT_FIELDS if f in s}, "stacks_bytes": len(s["stacks"])})
            last_text = {"time": s.get("time"), "stacks": s["stacks"],
                         "requests": s.get("requests") if isinstance(s.get("requests"), str) else None}
    total = sum(during.values()) if during is not None else None
    out = {"status": "present", "bound_ms": _num(last.get(BOUND)),
           "samples": len(carrying),
           "slow_requests_by_route": r1, "slow_requests_by_route_during_run": during,
           "slow_requests_during_run": total,
           "phase_ms": phases,
           "records_seen": len(records), "records_last": records[-32:],
           "snapshots": {"taken": _num(last.get(SNAPSHOTS)), "suppressed": _num(last.get(SUPPRESSED)),
                         "captured": bool(snaps), "seen": snaps, "last": last_text},
           "malformed": sorted(set(malformed)), "note": NOTE}
    return out
