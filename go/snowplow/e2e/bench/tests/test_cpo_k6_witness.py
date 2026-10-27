"""A control-plane outage of B is judged by B's k6 requests (#606 run 16).

Run 16 came back INVALID on control_plane_outage[b] alone: ONE failed laptop
poll inside a 63 s poll window (last answered 1793061139, first unanswered
1793061200, answered again 1793061202), while B's k6 served normally. The
rule now:

- the WHOLE window [t0, t1] is judged: the drop began somewhere between t0
  and the first unanswered poll, when is not known, so a narrower window
  could miss up to a poll interval of real failure (Diego, 2026-10-27: the
  clearest approach, never the narrower one). The poll gap is reported for
  information only;
- the part an A outage covers is the vantage's (cut from both runs);
- the rest is judged by k6 as a readiness gap is (#891): not served over the
  1 % bar -> FAIL; served -> jointly-Ready time with a NOTE; no request in
  flight -> void.

Fixture: run 16 B's real outage record and every k6 request of B in flight in
the record's 63 s window (tests/fixtures/run16b_cpo_witness.json.gz,
extracted from snowplow-bench-run16b-6e261cdf). Pair-level cases are in
tests/test_driver_stuck_api_blip.py (run 14's recorded arms).
"""

from __future__ import annotations

import gzip
import json

import pytest

from bench import custlat, k6eval
from tests.test_run606_real_sequence import BENCH_DIR

FIX = BENCH_DIR / "tests" / "fixtures" / "run16b_cpo_witness.json.gz"
BAR = k6eval.K6_THRESHOLDS["max_k6_error_rate"]
T0, FU, T1 = 1793061139, 1793061200, 1793061202


@pytest.fixture(scope="module")
def run16():
    f = json.load(gzip.open(FIX, "rt"))
    f["spans"] = [(s, t, bool(x), ms) for s, t, x, ms in f["spans"]]
    return f


def judge(run16, spans):
    rec = run16["record"]
    return custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), spans, run16["probe_timeout_s"], BAR)


def test_the_fixture_is_run16s_window_whole(run16):
    """provenance: every B request in flight in the window is there"""
    rec, s = run16["record"], run16["summary"]
    assert (rec["t0"], rec["first_unanswered"], rec["t1"]) == (T0, FU, T1)
    w = custlat.k6_witness(run16["spans"], T0, T1, run16["probe_timeout_s"])
    assert w["requests"] == s["poll_window_requests"] == len(run16["spans"]) == 10035
    assert sum(sp[2] for sp in run16["spans"]) == s["poll_window_failed"] == 48


def test_the_whole_window_is_judged_the_poll_gap_only_reported(run16):
    w = custlat.outage_window(run16["record"])
    assert (w["t0"], w["t1"], w["seconds"]) == (float(T0), float(T1), 63.0)
    assert (w["first_unanswered"], w["poll_gap_s"]) == (FU, 61.0)
    assert custlat.b_only_parts(run16["record"], []) == [(float(T0), float(T1))]


def test_run16_window_is_witnessed_served_not_void(run16):
    """A had no outage then: B's own part is the whole 63 s window, and B's
    k6 served it (10,035 requests in flight, 48 failed: 0.48 %) -> served,
    kept as jointly-Ready time, not void."""
    kind, out = judge(run16, run16["spans"])
    assert kind == "served", out["why"]
    assert out["k6"] == {"requests": 10035, "not_served": 48, "rate": 0.004783, "timeout_s": 30.0}
    assert "within the 0.01 bar" in out["why"] and "first unanswered poll 61.0 s in" in out["why"]


def test_customers_failing_only_in_the_poll_gap_fail_b(run16):
    """B's requests fail ONLY in the poll gap (before the first unanswered
    poll): that is inside the window, so B was not serving — FAIL. Judging
    from the first unanswered poll on would have missed every one of them."""
    gap_failed = [(s, t, True if t <= FU else f, ms) for s, t, f, ms in run16["spans"]]
    # every request that completed after the first unanswered poll is as recorded (served)
    after = [sp for sp in gap_failed if sp[1] > FU]
    assert after == [sp for sp in run16["spans"] if sp[1] > FU] and not any(sp[2] for sp in after)
    kind, out = judge(run16, gap_failed)
    assert kind == "fail", out["why"]
    assert out["k6"]["not_served"] > 0.9 * out["k6"]["requests"] and "B was not serving" in out["why"]


def test_customers_failing_or_too_slow_fail_b(run16):
    bad = [(s, t, True, ms) for s, t, _, ms in run16["spans"]]
    assert judge(run16, bad)[0] == "fail"
    slow = [(s, t, f, run16["probe_timeout_s"] * 1000 + 1) for s, t, f, _ in run16["spans"]]
    assert judge(run16, slow)[0] == "fail"           # not served = failed, or past the probe's timeout


def test_no_request_in_flight_voids(run16):
    kind, out = judge(run16, [])
    assert kind == "void" and "no witness" in out["why"] and out["k6"]["requests"] == 0
    kind, out = judge(run16, None)
    assert kind == "void" and "do not pair up" in out["why"]


def test_a_record_without_first_unanswered_has_no_poll_gap():
    w = custlat.outage_window({"t0": 100, "t1": 160, "source": "x"})
    assert (w["t0"], w["t1"], w["poll_gap_s"]) == (100.0, 160.0, None)
    assert custlat.vantage_cover([{"t0": 100, "t1": 160}]) == [(100.0, 160.0)]
