"""architect-ch's two run-8 fixes:

1. stale_to_push with a le_3000 bucket (2b's bounds are 1000/3000/10000) is
   DECIDABLE: PASS iff fraction <= 3000 >= 0.95, else FAIL. The 2500/5000
   INCONCLUSIVE fallback applies only when le_3000 is absent.
2. A pod restart resets every cumulative-since-boot key; a last-sample read
   after it reports only the post-restart tail and could PASS. Detect it (a
   counter drop between samples, build_info change, or pod restartCount) and
   mark every run-8 check RESTARTED → FAIL, reporting the restart and the
   pre-restart peak.
"""

from __future__ import annotations

import pytest

from bench import load606
from tests.test_run8 import PODS, _mem, _navs

K = load606.RUN8_KEYS
GIB = 1024 ** 3


def H3(counts: dict, max_ms: int):
    """2b-shaped histogram: bounds 1000/3000/10000."""
    cum, b = 0, {}
    for x in (1000, 3000, 10000):
        cum += counts.get(x, 0)
        b[f"le_{x}"] = cum
    b["le_inf"] = cum + counts.get("inf", 0)
    return {"buckets": b, "count": b["le_inf"], "sum_ms": 0, "max_ms": max_ms}


def _p95(hist):
    rep, checks = load606.run8_report(_navs(), [_mem(150, **{K["stale_to_push"]: hist})], [], PODS)
    return {c["check"]: c for c in checks}["run8.stale_to_push.p95"], rep


def test_le_3000_present_and_met_is_a_decidable_pass():
    # p95 between 2.5 and 3.0 s: the old 2500/5000 rule called this INCONCLUSIVE
    c, rep = _p95(H3({1000: 50, 3000: 46, 10000: 4}, 4000))
    assert c["result"] == "PASS" and rep["stale_to_push"]["fraction_le_3000"] == 0.96


def test_le_3000_present_and_missed_is_a_decidable_fail():
    # p95 between 3.0 and 5.0 s: the old rule called this INCONCLUSIVE too
    c, _ = _p95(H3({1000: 50, 3000: 40, 10000: 10}, 4800))
    assert c["result"] == "FAIL" and "fraction <= 3000 ms = 0.9" in c["why"]


def test_fallback_without_le_3000_stays_inconclusive_in_the_5000_bucket():
    from tests.test_run8 import H
    c, _ = _p95(H({2500: 90, 5000: 10}, 4800))
    assert c["result"] == "INCONCLUSIVE" and "INCONCLUSIVE vs 3 s" in c["why"]


# ─── restarts reset the counters ────────────────────────────────────────────


def _series_with_reset():
    """stale_to_push count 400 -> 20 mid-run (a restart), heap 7.6 GB before it.
    The post-restart tail alone would PASS every bar."""
    good_before = H3({1000: 300, 3000: 50, 10000: 50}, 9000)    # 87.5% <= 3 s: a FAIL
    tail = H3({1000: 20}, 400)                                   # would PASS alone
    full = {K["fill_over_ceiling"]: 0, K["nested_over_ceiling"]: 0,
            K["admission_wait"]: {"background": H3({1000: 5}, 50)},
            K["dirty_to_fresh_p95"]: 1, K["dirty_to_fresh_max"]: 2}
    return [
        _mem(110, 3000, 4000, **{**full, K["stale_to_push"]: H3({1000: 100}, 300),
                                K["stale_overdue"]: 10}),
        _mem(130, 7600, 7900, **{**full, K["stale_to_push"]: good_before,
                                K["stale_overdue"]: 50}),
        _mem(200, 700, 1500, **{**full, K["stale_to_push"]: tail,      # restarted
                               K["stale_overdue"]: 0}),
    ]


def test_counter_drop_mid_run_marks_every_run8_check_restarted_fail():
    tops = [{"kind": "top", "t": 131, "rss_bytes": int(7.9 * GIB)},
            {"kind": "top", "t": 201, "rss_bytes": int(0.8 * GIB)}]
    rep, checks = load606.run8_report(_navs(), _series_with_reset(), tops, PODS)
    assert checks[0]["check"] == "run8.no_restart" and checks[0]["result"] == "FAIL"
    assert all(c["result"] == "FAIL" for c in checks)
    assert all(c["why"].startswith("RESTARTED") for c in checks)
    r = rep["restart"]
    assert r["t"] == 200
    assert any(s["signal"] == "counter reset" and K["stale_to_push"] + ".count" in s["keys"]
               for s in r["signals"])
    assert r["peak_heap_inuse_mb_before"] == 7600
    assert r["peak_rss_bytes_before"] == int(7.9 * GIB)
    assert r["peak_rss_share_of_limit_before"] == round(int(7.9 * GIB) / (8 * GIB), 3)


def test_the_tail_alone_would_have_passed():
    """Guard for the test above: without the reset, the last sample PASSes —
    which is exactly the false pass the restart check prevents."""
    tail_only = [_mem(200, 700, 1500, **{K["stale_to_push"]: H3({1000: 20}, 400)})]
    rep, checks = load606.run8_report(_navs(), tail_only, [], PODS)
    c = {x["check"]: x for x in checks}
    assert c["run8.stale_to_push.p95"]["result"] == "PASS"
    assert rep["restart"] is None


@pytest.mark.parametrize("make", [
    # memstats NumGC drops (the Go runtime restarted)
    lambda: [{**_mem(100), "num_gc": 900}, {**_mem(160), "num_gc": 12}],
    # build_info changes between samples
    lambda: [_mem(100), {**_mem(160), "extra": {K["build_info"]: {"commit": "other"}}}],
])
def test_other_restart_signals(make):
    rep, checks = load606.run8_report(_navs(), make(), [], PODS)
    assert rep["restart"] is not None and checks[0]["check"] == "run8.no_restart"


def test_pod_restart_count_alone_is_a_restart():
    pods = [{**PODS[0]}, {**PODS[1], "restarts": 1,
                         "last_state": {"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                                       "finishedAt": "2026-10-15T10:00:00Z"}}}]
    rep, checks = load606.run8_report(_navs(), [_mem(100)], [], pods)
    sig = rep["restart"]["signals"][-1]
    assert sig["signal"] == "pod restartCount" and sig["reason"] == "OOMKilled"
    assert all(c["result"] == "FAIL" for c in checks)


def test_restart_count_only_uses_finished_at_for_the_peak_before():
    import datetime as dt
    t_oom = dt.datetime(2026, 10, 15, 10, 0, 0, tzinfo=dt.timezone.utc).timestamp()
    pods = [{**PODS[0]}, {**PODS[1], "restarts": 1,
                         "last_state": {"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                                       "finishedAt": "2026-10-15T10:00:00Z"}}}]
    mem = [_mem(t_oom - 30, 6900, 7700), _mem(t_oom - 10, 7800, 8000)]   # no sample after it
    tops = [{"kind": "top", "t": t_oom - 5, "rss_bytes": int(7.95 * GIB)}]
    rep, _ = load606.run8_report(_navs(), mem, tops, pods)
    r = rep["restart"]
    assert r["t"] == t_oom
    assert r["peak_heap_inuse_mb_before"] == 7800 and r["peak_sys_mb_before"] == 8000
    assert r["peak_rss_bytes_before"] == int(7.95 * GIB)


def test_monotonic_series_is_not_a_restart():
    mem = [{**_mem(100, **{K["stale_overdue"]: 1}), "num_gc": 10},
           {**_mem(160, **{K["stale_overdue"]: 4}), "num_gc": 40}]
    rep, _ = load606.run8_report(_navs(), mem, [], PODS)
    assert rep["restart"] is None
