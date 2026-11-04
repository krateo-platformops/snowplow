"""Two report-only sections for run 21, read from /debug/vars (mem606):

refresh_cycle (#1099, snowplow PR #1099, merged; internal/cache/refresh_cycle.go)
  snowplow_refresh_cycle_put_total          {"<class>|<tier>|<content>": n}, content identical | changed
  snowplow_refresh_cycle_wait_ms            {tier: hist}  queued tiers: viewed, normal, cluster_list
  snowplow_refresh_cycle_duration_ms        {tier: hist}  every tier (direct included)
  snowplow_refresh_broadcaster_put_to_frame_ms {tier: hist} (captured by snowplow_refresh_broadcaster)
  -> per tier the wait / duration / Put -> frame during the run; per class the
     identical vs changed Puts and the changed share (null for a class that
     made no Put: "no refresh ran" is never "every refresh was identical").

stale_push_by_layer (#1082, snowplow PR #1082; internal/cache/stale_push.go)
  snowplow_stale_to_push_ms                 {layer: hist}  apistage | l1
  snowplow_stale_serves_total / _unreplaced_total / _serves_open / _serve_closes_total {layer: ...}
  -> per layer stale serve -> the replacing frame WRITTEN, the serves left
     unreplaced past 10 s, the open serves. NOT comparable with run 20's
     apistage stale_to_push: a sample exists only once a frame was written.
     A layer that timed no stale serve during the run reads its unreplaced
     as null, NOT EXERCISED (a 0 there is the goal's value: #1105's class).

A build is read as carrying a family by its NAMED keys, never by a prefix:
1.12.56 already publishes snowplow_stale_retry_*, and is UNMEASURED here.
Histograms are cumulative le_<ms> buckets + le_inf, count, sum_ms, max_ms;
each is read as its change over the run (first -> last sample: count, sum,
buckets; max is lifetime). A build without the keys is UNMEASURED, never 0;
a value of the wrong shape, or a counter that went down (a restart), is
named in `malformed`. STDLIB ONLY.
"""

from __future__ import annotations

PUTS = "snowplow_refresh_cycle_put_total"
WAIT = "snowplow_refresh_cycle_wait_ms"
DURATION = "snowplow_refresh_cycle_duration_ms"
PUT_TO_FRAME = "snowplow_refresh_broadcaster_put_to_frame_ms"
STALE_TO_PUSH = "snowplow_stale_to_push_ms"
STALE_SERVES = "snowplow_stale_serves_total"
STALE_UNREPLACED = "snowplow_stale_unreplaced_total"
STALE_OPEN = "snowplow_stale_serves_open"
STALE_CLOSES = "snowplow_stale_serve_closes_total"
REFRESH_CYCLE_KEYS = (PUTS, WAIT, DURATION)
STALE_KEYS = (STALE_TO_PUSH, STALE_SERVES, STALE_UNREPLACED, STALE_OPEN, STALE_CLOSES)
GOAL_MARKS_MS = (1000, 3000, 10000)     # the north-star, the p95 goal, the max goal
NOT_EXERCISED = "NOT EXERCISED: no stale serve timed during the run"
STALE_NOTE = ("#1082 times a stale serve to the replacing frame WRITTEN, for every serving layer: a sample exists "
              "only once a frame was written, so this is not comparable with run 20's apistage stale_to_push "
              "(its end was the replacement's publish); serves no stream could take are open, not unreplaced")


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _carrying(mem: list[dict], keys) -> list[dict]:
    """The samples that publish at least one of the family's NAMED keys."""
    ex = [m.get("extra") or {} for m in mem or [] if m.get("kind") == "mem"]
    return [e for e in ex if any(k in e for k in keys)]


def _down(name: str, d, malformed: list[str]):
    if d is not None and d < 0:
        malformed.append(f"{name} went down during the run ({d}): a restart reset it")
    return d


def _hist_delta(h0, h1, malformed: list[str], name: str) -> dict | None:
    """One histogram's change over the run, with the goal marks and the p95's bucket."""
    if not isinstance(h1, dict) or _num(h1.get("count")) is None or not isinstance(h1.get("buckets"), dict):
        malformed.append(f"{name} is not a histogram")
        return None
    h0 = h0 if isinstance(h0, dict) and isinstance(h0.get("buckets"), dict) else {"count": 0, "sum_ms": 0, "buckets": {}}
    n = _down(name, h1["count"] - (h0.get("count") or 0), malformed)
    b = {k: v - (h0["buckets"].get(k) or 0) for k, v in h1["buckets"].items() if _num(v) is not None}
    out = {"count": n, "max_ms_lifetime": _num(h1.get("max_ms"))}
    if n > 0:
        s = (_num(h1.get("sum_ms")) or 0) - (_num(h0.get("sum_ms")) or 0)
        out["mean_ms"] = round(s / n, 1)
        bounds = sorted(int(k[3:]) for k in b if k[3:].isdigit())
        # past the top bound: "> top", never read as within it
        out["p95_bucket"] = next((f"<= {x}" for x in bounds if b[f"le_{x}"] >= 0.95 * n),
                                 f"> {bounds[-1]}" if bounds else None)
        for m in GOAL_MARKS_MS:
            if f"le_{m}" in b:
                out[f"share_le_{m}"] = round(b[f"le_{m}"] / n, 4)
    return out


def _by_label(first: dict, last: dict, key: str, malformed: list[str]) -> dict | None:
    v1 = last.get(key)
    if v1 is None:
        return None
    if not isinstance(v1, dict):
        malformed.append(f"{key} is not a map")
        return None
    v0 = first.get(key) if isinstance(first.get(key), dict) else {}
    return {lb: _hist_delta(v0.get(lb), h, malformed, f"{key}[{lb}]") for lb, h in sorted(v1.items())}


def refresh_cycle(mem: list[dict]) -> dict:
    """report["refresh_cycle"] (report only)."""
    c = _carrying(mem, REFRESH_CYCLE_KEYS)
    if not c:
        return {"status": "UNMEASURED", "why": f"no /debug/vars sample carries {PUTS} / {WAIT} / {DURATION}: the "
                                               f"build predates #1099, or mem606 was not given its prefix"}
    first, last, malformed = c[0], c[-1], []
    classes = None
    p1 = last.get(PUTS)
    if p1 is not None and not isinstance(p1, dict):
        malformed.append(f"{PUTS} is not a map")
    elif isinstance(p1, dict):
        classes = {}
        p0 = first.get(PUTS) if isinstance(first.get(PUTS), dict) else {}
        for cell, n in p1.items():
            parts = str(cell).split("|")
            if len(parts) != 3 or _num(n) is None or parts[2] not in ("identical", "changed"):
                malformed.append(f"{PUTS}[{cell}] is not a class|tier|content count")
                continue
            d = _down(f"{PUTS}[{cell}]", n - (_num(p0.get(cell)) or 0), malformed)
            e = classes.setdefault(parts[0], {"identical": 0, "changed": 0})
            e[parts[2]] += d
        for e in classes.values():
            t = e["identical"] + e["changed"]
            # a class that made no Put during the run: no share (never 0, never "all identical")
            e["changed_share"] = round(e["changed"] / t, 4) if t > 0 else None
    return {"status": "present", "samples": len(c),
            "puts_by_class": classes,
            "wait_by_tier": _by_label(first, last, WAIT, malformed),
            "duration_by_tier": _by_label(first, last, DURATION, malformed),
            "put_to_frame_by_tier": _by_label(first, last, PUT_TO_FRAME, malformed),
            "malformed": sorted(set(malformed)),
            "note": "report only; histograms as their change over the run (mem606's first to last sample)"}


def stale_push_by_layer(mem: list[dict]) -> dict:
    """report["stale_push_by_layer"] (report only)."""
    c = _carrying(mem, STALE_KEYS)
    if not c:
        return {"status": "UNMEASURED", "why": f"no /debug/vars sample carries {STALE_TO_PUSH} / {STALE_SERVES} / "
                                               f"{STALE_UNREPLACED}: the build predates #1082, or mem606 was not "
                                               f"given its prefixes", "note": STALE_NOTE}
    first, last, malformed = c[0], c[-1], []

    def counts(key):
        """{layer: change over the run}; None when the key is not published."""
        v1 = last.get(key)
        if v1 is None:
            return None
        if not isinstance(v1, dict) or any(_num(n) is None for n in v1.values()):
            malformed.append(f"{key} is not a {{layer: count}} map")
            return None
        v0 = first.get(key) if isinstance(first.get(key), dict) else {}
        return {lb: _down(f"{key}[{lb}]", n - (_num(v0.get(lb)) or 0), malformed) for lb, n in v1.items()}

    def layer_map(key, shape_ok):
        v = last.get(key)
        if v is None:
            return None
        if not isinstance(v, dict) or not all(shape_ok(x) for x in v.values()):
            malformed.append(f"{key} is not a {{layer: ...}} map of the published shape")
            return None
        return v
    serves, unrep = counts(STALE_SERVES), counts(STALE_UNREPLACED)
    hists = _by_label(first, last, STALE_TO_PUSH, malformed) or {}
    opn = layer_map(STALE_OPEN, lambda x: _num(x) is not None)
    cl1 = layer_map(STALE_CLOSES, lambda x: isinstance(x, dict) and all(_num(n) is not None for n in x.values()))
    cl0 = first.get(STALE_CLOSES) if isinstance(first.get(STALE_CLOSES), dict) else {}
    layers = sorted(set(serves or {}) | set(unrep or {}) | set(hists))
    out = {}
    for lb in layers:
        sv = (serves or {}).get(lb)                       # None: not published, never 0
        e = {"serves": sv, "open_last": _num((opn or {}).get(lb)),
             "closes": ({k: n - ((cl0.get(lb) or {}).get(k) or 0) for k, n in cl1[lb].items()}
                        if cl1 is not None and lb in cl1 else None),
             "to_push": hists.get(lb)}
        u = (unrep or {}).get(lb)
        if sv is None:
            e["unreplaced"], e["unreplaced_why"] = None, f"{STALE_SERVES}[{lb}] not published: unknown"
        elif sv <= 0 and not u:
            e["unreplaced"], e["unreplaced_why"] = None, NOT_EXERCISED
        else:
            e["unreplaced"] = u
        out[lb] = e
    exercised = [e["unreplaced"] for e in out.values() if e["unreplaced"] is not None]
    return {"status": "present", "samples": len(c), "layers": out,
            "unreplaced_total": sum(exercised) if exercised else None,
            **({} if exercised else {"unreplaced_total_why": NOT_EXERCISED}),
            "malformed": sorted(set(malformed)), "note": STALE_NOTE}
