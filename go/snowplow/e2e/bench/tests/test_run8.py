"""Bench run 8 (#710 issuecomment-6187374111): the cold composite-page burst,
the memory-risk report against the 8Gi limit, both gates' over_ceiling_bytes,
stale_to_push against 3 s / 10 s (and the gap to 1 s), stale_overdue_total,
background admission wait and refresher dirty_to_fresh. A key no sample
carries is "not exposed" and FAILS; a p95 read from buckets is a bucket bound
and is INCONCLUSIVE when the bucket straddles the bar."""

from __future__ import annotations

import concurrent.futures as cf
import json
import subprocess

import pytest

from bench import load606
from tests.test_navmodel import PORTAL, Fake
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

K = load606.RUN8_KEYS
GIB = 1024 ** 3


# ─── composite pages, from the live menu ────────────────────────────────────


def test_composite_routes_rank_nested_trees():
    rows = [{"path": "/flat", "tree_depth": 1, "tree_calls": 1},
            {"path": "/tabs", "tree_depth": 2, "tree_calls": 3},
            {"path": "/deep", "tree_depth": 3, "tree_calls": 4},
            {"path": "/deeper", "tree_depth": 4, "tree_calls": 40},
            {"path": "/broken", "tree_depth": 5, "tree_calls": 90, "tree_errors": 2}]
    assert load606.composite_routes(rows) == ["/deeper", "/deep"]


def test_preflight_derives_composite_pages_from_the_menu():
    res = load606.preflight(Fake(), "t", PORTAL)
    rows = {r["path"]: r for r in res["routes"]}
    assert rows["/dashboard"]["tree_depth"] == 3          # Flex → Table → Button
    assert rows["/compositions"]["tree_depth"] == 2       # Tabs → first tab only
    assert res["composite_routes"] == ["/dashboard"]


# ─── the burst itself ───────────────────────────────────────────────────────


def test_composite_burst_cohort_arrives_together_after_idling():
    c = Fake()
    recs = []
    users = {f"u{i}": "t" for i in range(1, 7)}
    d = load606.Driver(c, users, PORTAL, think_s=0.05, duration_s=0.9,
                       nav_workers=4, call_workers=16, sink=recs.append,
                       composite_routes=["/nope", "/dashboard"],
                       burst_users=["u4", "u5", "u6"], burst_at_s=0.4)
    import time as _t
    t_start = _t.time()
    d.run()
    burst = [r for r in recs if r["nav"] == "composite_first_load"]
    assert sorted(r["user"] for r in burst) == ["u4", "u5", "u6"]
    assert all(r["page"] == "/dashboard" and r["visit"] == 1 for r in burst)
    starts = [r["start"] for r in burst]
    assert max(starts) - min(starts) < 0.15                       # together
    assert min(starts) - t_start >= 0.35                          # not before burst_at_s
    for u in ("u4", "u5", "u6"):                                  # idle until the burst
        assert all(r["start"] >= min(starts) for r in recs if r["user"] == u)
    assert not any(r["nav"] == "first_load" and r["user"] in ("u4", "u5", "u6") for r in recs)


def test_composite_burst_without_a_composite_route_records_failures():
    recs = []
    d = load606.Driver(Fake(), {"u1": "t", "u2": "t"}, PORTAL, think_s=0.05,
                       duration_s=0.4, nav_workers=4, call_workers=8, sink=recs.append,
                       composite_routes=[], burst_users=["u2"], burst_at_s=0.1)
    d.run()
    b = [r for r in recs if r["nav"] == "composite_first_load"]
    assert b and b[0]["page"] == "(no-composite-route)" and b[0]["first_load_failed"]


# ─── histogram readings ─────────────────────────────────────────────────────


def H(counts: dict, max_ms: int):
    """Cumulative le_* buckets from per-bucket counts."""
    bounds = [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
    cum, b = 0, {}
    for x in bounds:
        cum += counts.get(x, 0)
        b[f"le_{x}"] = cum
    b["le_inf"] = cum + counts.get("inf", 0)
    return {"buckets": b, "count": b["le_inf"], "sum_ms": 0, "max_ms": max_ms}


def test_hist_helpers():
    h = H({500: 80, 2500: 16, 5000: 4}, 4200)
    assert load606.hist_fraction_le(h, 1000) == 0.8
    assert load606.hist_fraction_le(h, 2500) == 0.96
    assert load606.hist_p95_bound(h) == "<= 2500"
    assert load606.hist_p95_bound(H({"inf": 5}, 99999)) == "> 10000"
    assert load606.hist_fraction_le({"count": 0}, 1000) is None


# ─── the run-8 report ───────────────────────────────────────────────────────


def _mem(t, heap_mb=1000, sys_mb=1500, **extra):
    return {"kind": "mem", "t": t, "readyz": 200, "heap_inuse_mb": heap_mb,
            "sys_mb": sys_mb, "extra": {K["build_info"]: {"commit": "abc123"}, **extra}}


def _navs():
    return ([{"kind": "nav", "nav": "first_load", "start": 100.0 + i, "visit": 1,
              "wall_ms": 100, "errors": 0, "calls": 5} for i in range(5)]
            + [{"kind": "nav", "nav": "composite_first_load", "start": 1000.0, "visit": 1,
                "wall_ms": 200, "errors": 0, "calls": 9} for _ in range(3)])


PODS = [{"kind": "snowplow_pod", "phase": "before", "pod": "s", "restarts": 0,
         "limits": {"memory": "8Gi"}},
        {"kind": "snowplow_pod", "phase": "after", "pod": "s", "restarts": 0,
         "limits": {"memory": "8Gi"}}]


def test_every_missing_key_is_not_exposed_and_fails():
    rep, checks = load606.run8_report(_navs(), [_mem(150), _mem(1050)], [], PODS)
    failed = {c["check"]: c for c in checks if c["result"] == "FAIL"}
    for name in ("run8.content_fill.over_ceiling_bytes", "run8.nested_resolve.over_ceiling_bytes",
                 "run8.stale_to_push.p95", "run8.stale_to_push.max", "run8.stale_overdue_total",
                 "run8.admission_wait_ms.background", "run8.refresher.dirty_to_fresh_p95",
                 "run8.refresher.dirty_to_fresh_max"):
        assert failed[name]["why"] == "not exposed", name
        assert failed[name]["key"]                                 # names the key
    assert all(c["build"] == {"commit": "abc123"} for c in checks)  # and the build read from


def test_full_report_memory_risk_and_gates():
    stp = H({500: 90, 2500: 8, 5000: 2}, 4100)                     # p95 <= 2500, max 4.1 s
    wait = {"background": H({250: 10, 1000: 2}, 900), "customer": H({10: 50}, 3)}
    mem = [_mem(90, 800, 1000, **{K["dirty_to_fresh_p95"]: 900, K["dirty_to_fresh_max"]: 1200,
                                 K["stale_overdue"]: 4}),
           _mem(120, 5200, 6700, **{K["fill_over_ceiling"]: 0, K["fill_over_ceiling_peak"]: 1_500_000_000,
                                   K["nested_over_ceiling"]: 0, K["nested_over_ceiling_peak"]: 0,
                                   K["dirty_to_fresh_p95"]: 2600, K["dirty_to_fresh_max"]: 7000,
                                   K["stale_overdue"]: 6}),
           _mem(1010, 6100, 7100, **{K["nested_over_ceiling_peak"]: 2_000_000_000,
                                    K["stale_to_push"]: stp, K["admission_wait"]: wait,
                                    K["stale_overdue"]: 9})]
    tops = [{"kind": "top", "t": 125, "rss_bytes": int(5.5 * GIB)},
            {"kind": "top", "t": 1020, "rss_bytes": int(7.2 * GIB)}]
    rep, checks = load606.run8_report(_navs(), mem, tops, PODS)
    risk = rep["memory_risk"]
    assert risk["limit_bytes"] == 8 * GIB
    assert risk["peak_rss_share_of_limit"] == 0.9                   # 7.2 / 8
    w1, w2 = risk["windows"]["first_load_burst"], risk["windows"]["composite_page_burst"]
    assert w1["heap_inuse_mb_peak"] == 5200 and w1["fill_over_ceiling_bytes_peak"] == 1_500_000_000
    assert w2["heap_inuse_mb_peak"] == 6100 and w2["nested_over_ceiling_bytes_peak"] == 2_000_000_000
    assert w2["rss_bytes_peak"] == int(7.2 * GIB) and w2["navs"] == 3
    res = {c["check"]: c["result"] for c in checks}
    assert res["run8.stale_to_push.p95"] == "PASS" and res["run8.stale_to_push.max"] == "PASS"
    assert rep["stale_to_push"]["fraction_le_1000"] == 0.9
    assert "10.0% of pushes slower than 1 s" in rep["stale_to_push"]["gap_to_1s_goal"]
    assert rep["stale_overdue_delta"] == 5
    assert rep["admission_wait_ms"]["background"]["p95_bucket"] == "<= 1000"
    assert rep["refresher_dirty_to_fresh"]["dirty_to_fresh_max_max_over_samples"] == 7000
    assert not [c for c in checks if c["result"] == "FAIL"]


@pytest.mark.parametrize("hist,p95,mx", [
    (H({2500: 90, 5000: 10}, 4800), "INCONCLUSIVE", "PASS"),        # p95 in le_5000
    (H({2500: 80, 10000: 20}, 9000), "FAIL", "PASS"),
    (H({500: 99, "inf": 1}, 12000), "PASS", "FAIL"),                 # max over 10 s
    (H({}, 0), "INCONCLUSIVE", "PASS"),                              # no pushes observed
])
def test_stale_to_push_bucket_rules(hist, p95, mx):
    mem = [_mem(150, **{K["stale_to_push"]: hist})]
    rep, checks = load606.run8_report(_navs(), mem, [], PODS)
    res = {c["check"]: c for c in checks}
    assert res["run8.stale_to_push.p95"]["result"] == p95
    if p95 == "INCONCLUSIVE" and hist["count"]:
        assert "INCONCLUSIVE vs 3 s" in res["run8.stale_to_push.p95"]["why"]
    assert res["run8.stale_to_push.max"]["result"] == mx


def test_inconclusive_stale_to_push_makes_the_verdict_inconclusive():
    full = {K["fill_over_ceiling"]: 0, K["nested_over_ceiling"]: 0, K["stale_overdue"]: 0,
            K["admission_wait"]: {"background": H({10: 1}, 5)},
            K["dirty_to_fresh_p95"]: 1, K["dirty_to_fresh_max"]: 2,
            K["stale_to_push"]: H({2500: 90, 5000: 10}, 4800)}
    navs = [{"kind": "nav", "nav": "first_load", "user": "u", "page": "p", "start": 100.0,
             "visit": 1, "wall_ms": 100, "calls": 3, "errors": 0, "sched_lag_ms": 1}]
    navs += [{"kind": "nav", "nav": "composite_first_load", "user": "v", "page": "q",
              "start": 500.0, "visit": 1, "wall_ms": 100, "calls": 3, "errors": 0, "sched_lag_ms": 1}]
    navs += [{"kind": "nav", "nav": "route", "user": "u", "page": "p", "start": 200.0 + i,
              "visit": 2, "wall_ms": 100, "calls": 3, "errors": 0, "sched_lag_ms": 1} for i in range(300)]
    fresh = [{"kind": "fresh", "label": "list_dep@10", "converged": True, "latency_ms": 100}
             for _ in range(40)]
    r = load606.evaluate(navs, fresh, [], snowplow_pods=PODS, mem=[_mem(150, **full)],
                         tops=[], run8=True)
    assert r["verdict"] == "INCONCLUSIVE", [c for c in r["checks"] if c["result"] != "PASS"]


# ─── run606 end to end ──────────────────────────────────────────────────────


def test_run606_runs_the_composite_burst_and_the_run8_report(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1",
               COMPOSITE_BURST_USERS="2", TOP_POLL_S="0.2")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    assert "composite pages for the cold burst: /observability /compositions" in out
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert '--burst-users "$BURST"' in job and "BURST=2" in job
    assert '--composite-route "/observability"' in job and '--composite-route "/compositions"' in job
    for prefix in load606.RUN8_VARS_PREFIXES:
        assert f'--vars-prefix "{prefix}"' in job
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    r8 = v["report"]["run8"]
    assert r8["memory_risk"]["limit_bytes"] == 8 * GIB
    assert r8["keys"] == load606.RUN8_KEYS
    # the fake snowplow exposes none of the run-8 keys yet → they FAIL as "not exposed"
    nexp = {c["check"] for c in v["checks"]
            if c.get("why") == "not exposed" and c["result"] == "FAIL"}
    assert "run8.content_fill.over_ceiling_bytes" in nexp and "run8.stale_to_push.p95" in nexp
    # never a pass while keys are missing (this toy run is also INVALID: too
    # few warm navs / fresh samples, which outranks FAIL)
    assert v["verdict"] in ("FAIL", "INVALID")
