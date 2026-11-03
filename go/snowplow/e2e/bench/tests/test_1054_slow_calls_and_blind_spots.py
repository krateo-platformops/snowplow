"""#1054 slow-call collection, and run 20's three blind spots (k6 driver).

Causes (read from run 20b's own files, before any change):
  B  run8.*_burst_ran FAIL null: on the k6 driver evaluate's navs are Chrome's
     (nav = the load's method); the bursts are k6's page loads, tagged
     kind=first_load (795) / composite_first_load (200). Wrong place.
  D  every stale-to-push check INCONCLUSIVE, streams_opened 0: the open tabs'
     viewer_stats / push_refetch are in the viewer sidecar's k6/viewer.jsonl
     (264,239 streams, 268,542 refetches), never read for run 8. Wrong place;
     and apistage_stale_serve_total stayed 0: honestly NOT EXERCISED.
  P  page loads only all/steady: that split is a profiling-overhead
     comparison; no per-phase page-load report existed, though Chrome loaded
     pages in every phase (storm 195, burst 95, churn 457, steady 3,827).
  S  #1054's record (snowplow PR #1059) reaches the bench only through
     /debug/vars: snowplow_slow_request* is a named mem606 prefix; the report
     is per run, UNMEASURED on an older image, a malformed value is named.
Every arm runs the evaluate CLI (`python -m bench.load606 evaluate`) on
run-20b-shaped outputs, or the function the CLI calls.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import custlat, k6eval, load606, slowcall
from tests.test_k6_830 import (T_POINTS, _browser_points, _browser_summary, _k6_summary, _point,
                               _write_out, _write_viewer)

BENCH_DIR = Path(__file__).resolve().parents[1]


def _k6_points_run20():
    """k6's page loads as run 20b's: the login storm's first loads, the burst's
    composite deep links, then route clicks; each page_load_ms tagged by kind."""
    pts = []
    for i, kind in enumerate(["first_load"] * 6 + ["composite_first_load"] * 3 + ["route"] * 11):
        tags = {"kind": kind, "route": f"/r{i % 3}", "visit": "first" if kind != "route" else "warm"}
        pts += [_point("page_load_ms", 150.0 + i, **tags), _point("page_loads", 1, **tags),
                _point("http_req_duration", 10 + i, level=str(i % 2), status="200")]
    return pts


def _slow_vars(n_by_route=(1, 3), malformed=False, snapshot=True):
    rec = {"time": "2026-11-01T10:00:01Z", "route": "GET /call", "method": "GET", "wall_ms": 4200,
           "bound_ms": 3000, "outcome": "completed", "class": "customer", "phase": "flight_wait",
           "phase_sum_ms": 4100, "wait_phase": "flight_wait", "kind": "content_flight", "wait_ms": 4000,
           "wait_outcome": "completed", "wait_key": "k:ab12cd34", "gvr": "composition.krateo.io/v1-2-2, Resource=x",
           "flight_wait_ms": 4000, "flight_wait_n": 1}
    ring = [rec, "not a record", {"time": "x", "route": "GET /x"}] if malformed else [rec]
    snaps = [{"time": "2026-11-01T10:00:02Z", "route": "GET /call", "goroutines": 900, "stacks_kept": 7,
              "truncated": False, "kind": "content_flight", "wait_key": "k:ab12cd34",
              "stacks": "1 @ apistage.(*flight).wait flight.go:88", "requests": "GET /call 4200ms"}]
    hist = {"flight_wait": {"buckets": {"le_3000": 0, "le_10000": n_by_route[1], "le_inf": n_by_route[1]},
                            "count": n_by_route[1], "sum_ms": 12000, "max_ms": 4500},
            "auth": {"buckets": {"le_3000": 0, "le_10000": 0, "le_inf": 0}, "count": 0, "sum_ms": 0, "max_ms": 0}}
    return [{slowcall.BY_ROUTE: {"GET /call": n_by_route[0]}, slowcall.PHASE_MS: hist, slowcall.RECENT: [],
             slowcall.SNAPSHOTS: 0, slowcall.SUPPRESSED: 0, slowcall.SNAPSHOTS_RECENT: [], slowcall.BOUND: 3000},
            {slowcall.BY_ROUTE: {"GET /call": n_by_route[1]}, slowcall.PHASE_MS: hist, slowcall.RECENT: ring,
             slowcall.SNAPSHOTS: 1 if snapshot else 0, slowcall.SUPPRESSED: 2,
             slowcall.SNAPSHOTS_RECENT: snaps if snapshot else [], slowcall.BOUND: 3000}]


def _mem(extras):
    out = [{"kind": "mem_start", "t": T_POINTS - 30}]
    out += [{"kind": "mem", "t": T_POINTS - 20 + 10 * i, "readyz": 200, "vars_status": 200, "heap_inuse_mb": 100.0,
             "extra": e} for i, e in enumerate(extras)]
    return out + [{"kind": "mem_end", "t": T_POINTS + 10}]


def _evaluate(tmp_path, *, extras=None, viewer_kw=None, two_stats=False):
    """The evaluate CLI on run-20b-shaped k6 / browser / viewer outputs."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points_run20(), _k6_summary(20, burst=3, burst_loads=3), 0)
    _write_viewer(k6dir, **(viewer_kw or {}))
    if two_stats:   # a cumulative viewer_stats written twice: the last one counts, never their sum
        with open(k6dir / "viewer.jsonl", "a") as fh:
            fh.write(json.dumps({"kind": "viewer_stats", "t": 7, "streams_opened": 5, "frames": 1}) + "\n")
    _write_out(bdir, "browser", _browser_points(), _browser_summary(63), 0)
    fresh = tmp_path / "fresh.jsonl"
    fresh.write_text(json.dumps({"kind": "fresh", "label": "list_dep@1", "converged": True, "latency_ms": 100,
                                 "polls": 1, "poll_ms": 100}) + "\n")
    mem = tmp_path / "mem-x.jsonl"
    mem.write_text("".join(json.dumps(r) + "\n" for r in _mem(extras if extras is not None else [{}, {}])))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(fresh), str(mem), "--run8",
                        "--k6-out", str(k6dir), "--browser-out", str(bdir), "--browser-sessions", "3"],
                       capture_output=True, text=True, cwd=BENCH_DIR, timeout=180)
    assert p.stdout, p.stderr[-3000:]
    return json.loads(p.stdout)


def _check(v, name):
    return next((c for c in v["checks"] + v["validity"] if c["check"] == name), None)


# ─── B: the bursts are k6's page loads by kind ───────────────────────────────

def test_the_k6_bursts_are_found_by_kind(tmp_path):
    v = _evaluate(tmp_path)
    assert _check(v, "run8.first_load_burst_ran") is None, _check(v, "run8.first_load_burst_ran")
    assert _check(v, "run8.composite_page_burst_ran") is None
    w = v["report"]["run8"]["memory_risk"]["windows"]
    assert w["first_load_burst"]["navs"] == 6 and w["composite_page_burst"]["navs"] == 3
    assert v["report"]["run8"]["burst_and_delivery_source"].startswith("k6's page loads by kind")


def test_k6_burst_navs_keep_only_the_burst_kinds(tmp_path):
    _write_out(tmp_path, "k6", _k6_points_run20(), None, 0)
    navs = k6eval.k6_burst_navs(k6eval.read_k6_out(str(tmp_path), "k6"))
    assert sorted({n["nav"] for n in navs}) == ["composite_first_load", "first_load"] and len(navs) == 9
    assert all(n["start"] < T_POINTS for n in navs)                 # its start: the point's time minus its ms
    assert k6eval.k6_burst_navs(None) == []


def test_a_k6_run_with_no_burst_still_says_so(tmp_path):
    """The check is not silenced: k6 page loads with no first / composite
    load read as before (FAIL, not recorded)."""
    k6dir = tmp_path / "k6"
    pts = [p for p in _k6_points_run20() if (p["data"]["tags"].get("kind") or "route") == "route"
           or p["metric"] == "http_req_duration"]
    _write_out(k6dir, "k6", pts, None, 0)
    r8, checks = load606.run8_report(k6eval.k6_burst_navs(k6eval.read_k6_out(str(k6dir), "k6")), [], [], None)
    assert {c["check"]: c["result"] for c in checks}["run8.first_load_burst_ran"] == "FAIL"


# ─── D: the open tabs are the viewer sidecar's ───────────────────────────────

def test_the_sidecars_tabs_are_the_delivery_and_a_missing_stale_serve_is_named(tmp_path):
    v = _evaluate(tmp_path)
    ex = v["report"]["run8"]["exercised_710_740"]
    assert ex["streams_opened"] == 5 and ex["frames"] == 1 and ex["push_refetches"] == 1, ex
    assert not ex["exercised"]
    assert "no client /refreshes stream" not in ex["missing"]
    assert ex["missing"][0] == "no stale serve (apistage_stale_serve_total 0)"
    c = _check(v, "run8.710_740_exercised")
    assert c["result"] == "INCONCLUSIVE" and c["why"].startswith("no stale serve (apistage_stale_serve_total 0)")
    assert "no client /refreshes stream" not in c["why"]


def test_a_cumulative_viewer_stats_counts_once(tmp_path):
    ex = _evaluate(tmp_path, two_stats=True)["report"]["run8"]["exercised_710_740"]
    assert ex["streams_opened"] == 5


# ─── P: page loads by phase, against the goal ────────────────────────────────

def _o(points: dict, times: dict | None = None) -> dict:
    return {"points": points, "times": times or {}}


def test_page_loads_by_phase_report_every_phase_and_an_empty_one_as_unmeasured():
    wins = [("storm", float("-inf"), 100.0), ("burst", 200.0, 300.0), ("churn", 400.0, 500.0)]
    # (99.5 s, 1.5 s): it starts in the storm and ends after it: a storm load
    loads = [(50.0, 900.0), (60.0, 2500.0), (70.0, 12000.0), (99.5, 1500.0), (450.0, 400.0), (900.0, 300.0)]
    browser = _o({"browser_page_load_ms": [(ms, {"t0": str(int(t * 1000)), "method": "click"}) for t, ms in loads]})
    k6o = _o({"page_load_ms": [(1000.0, {"kind": "first_load"}), (500.0, {"kind": "route"})]},
             {"page_load_ms": [51.0, 901.0]})
    r = custlat.page_loads_by_phase(browser, k6o, wins)
    b = r["browser"]
    assert b["storm"]["n"] == 4 and b["storm"]["max_ms"] == 12000.0
    assert b["storm"]["max_within_goal"] is False and b["churn"]["p95_within_goal"] is True
    assert b["burst"] == {"n": 0, "result": "UNMEASURED"}
    assert b["steady"]["n"] == 1
    assert r["k6"]["first_load"]["storm"]["n"] == 1 and r["k6"]["route"]["steady"]["n"] == 1
    assert r["goal"] == {"p95_ms": 3000.0, "max_ms": 10000.0}


def test_page_loads_by_phase_reach_the_verdict(tmp_path):
    pl = _evaluate(tmp_path)["report"]["customer_latency"]["page_loads_by_phase"]
    assert sum(x.get("n", 0) for x in pl["browser"].values()) == 63
    assert set(pl["k6"]) == {"first_load", "composite_first_load", "route"}


# ─── S: the slow-call record ─────────────────────────────────────────────────

def test_the_slow_request_prefix_is_a_named_collection_entry():
    assert load606.SLOW_REQUEST_PREFIX in load606.RUN8_VARS_PREFIXES
    p = subprocess.run([sys.executable, "-m", "bench.load606", "prefixes"], capture_output=True, text=True,
                       cwd=BENCH_DIR, check=True)
    assert load606.SLOW_REQUEST_PREFIX in p.stdout.split()            # run606's MEM_VARS_PREFIXES default
    for k in (slowcall.BY_ROUTE, slowcall.PHASE_MS, slowcall.RECENT, slowcall.SNAPSHOTS, slowcall.SUPPRESSED,
              slowcall.SNAPSHOTS_RECENT, slowcall.BOUND):
        assert k.startswith(load606.SLOW_REQUEST_PREFIX), k


def test_a_present_record_reports_routes_phases_records_and_the_snapshot(tmp_path):
    r = _evaluate(tmp_path, extras=_slow_vars())["report"]["slow_requests"]
    assert r["status"] == "present" and r["bound_ms"] == 3000
    assert r["slow_requests_by_route"] == {"GET /call": 3} and r["slow_requests_by_route_during_run"] == {"GET /call": 2}
    assert r["slow_requests_during_run"] == 2
    assert r["phase_ms"] == {"flight_wait": {"count": 3, "sum_ms": 12000, "max_ms": 4500, "le_3000": 0,
                                             "le_10000": 3, "over_3000": 3}}
    rec = r["records_last"][0]
    assert (rec["phase"], rec["kind"], rec["outcome"], rec["wait_key"]) == ("flight_wait", "content_flight",
                                                                          "completed", "k:ab12cd34")
    assert r["snapshots"]["captured"] and r["snapshots"]["taken"] == 1 and r["snapshots"]["suppressed"] == 2
    assert r["snapshots"]["last"]["stacks"].startswith("1 @ apistage")
    assert r["malformed"] == []


def test_an_image_without_the_record_is_unmeasured_never_zero(tmp_path):
    r = _evaluate(tmp_path, extras=[{"snowplow_resolved_cache": {}}] * 2)["report"]["slow_requests"]
    assert r["status"] == "UNMEASURED" and "predates #1054" in r["why"]
    assert "slow_requests_during_run" not in r
    assert slowcall.report([], load606.SLOW_REQUEST_PREFIX)["status"] == "UNMEASURED"


def test_a_malformed_record_is_named_and_the_rest_reported(tmp_path):
    r = _evaluate(tmp_path, extras=_slow_vars(malformed=True))["report"]["slow_requests"]
    assert r["status"] == "present" and r["records_seen"] == 1
    assert r["malformed"] == ["a snowplow_slow_requests_recent entry has no route / wall_ms (dict)",
                              "a snowplow_slow_requests_recent entry has no route / wall_ms (str)"]
    bad = _slow_vars()
    bad[1][slowcall.BY_ROUTE] = ["GET /call"]
    r = slowcall.report(_mem(bad), load606.SLOW_REQUEST_PREFIX)
    assert r["slow_requests_by_route"] is None and "is not a {route: count} map" in r["malformed"][0]


def test_no_snapshot_reads_not_captured(tmp_path):
    r = slowcall.report(_mem(_slow_vars(snapshot=False)), load606.SLOW_REQUEST_PREFIX)
    assert r["snapshots"]["captured"] is False and r["snapshots"]["last"] is None
