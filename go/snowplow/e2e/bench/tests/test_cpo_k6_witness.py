"""A control-plane outage of B is judged by B's k6 requests (#606 run 16).

Run 16 came back INVALID on control_plane_outage[b] alone: ONE failed laptop
poll (first_unanswered 1793061200, answered again at 1793061202) inside a
63 s poll window, while B's k6 served normally. The rule now:

- the window is narrowed to what was actually unanswered (first_unanswered
  -> t1); the poll gap before it is reported apart (edge_uncertainty_s);
- the part an A outage covers is the vantage's (cut from both runs);
- the rest is judged by k6 as a readiness gap is (#891): not served over the
  1 % bar -> FAIL; served -> jointly-Ready time with a NOTE; no request in
  flight -> void.

Fixture: run 16 B's real outage record and every k6 request of B in flight in
the record's 63 s poll window (tests/fixtures/run16b_cpo_witness.json.gz,
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


@pytest.fixture(scope="module")
def run16():
    f = json.load(gzip.open(FIX, "rt"))
    f["spans"] = [(s, t, bool(x), ms) for s, t, x, ms in f["spans"]]
    return f


def test_the_fixture_is_run16s_poll_window_whole(run16):
    """provenance: every B request in flight in the poll window is there"""
    rec, s = run16["record"], run16["summary"]
    assert (rec["t0"], rec["first_unanswered"], rec["t1"]) == (1793061139, 1793061200, 1793061202)
    w = custlat.k6_witness(run16["spans"], rec["t0"], rec["t1"], run16["probe_timeout_s"])
    assert w["requests"] == s["poll_window_requests"] == len(run16["spans"])
    assert sum(sp[2] for sp in run16["spans"]) == s["poll_window_failed"]


def test_run16_window_is_narrowed_to_what_was_unanswered(run16):
    n = custlat.narrowed_outage(run16["record"])
    assert (n["t0"], n["t1"], n["seconds"]) == (1793061200.0, 1793061202.0, 2.0)
    assert (n["poll_t0"], n["poll_window_s"], n["edge_uncertainty_s"]) == (1793061139.0, 63.0, 61.0)
    assert custlat.narrowed_outage(n) == n                                   # idempotent


def test_run16_single_failed_poll_is_witnessed_served_not_void(run16):
    """A had no outage then: B's own part is the whole narrowed window, and
    B's k6 served it -> served (kept as jointly-Ready time), not void."""
    rec = run16["record"]
    own = custlat.b_only_parts(rec, [])
    assert own == [(1793061200.0, 1793061202.0)]
    kind, out = custlat.judge_b_outage(rec, own, run16["spans"], run16["probe_timeout_s"], BAR)
    assert kind == "served", out["why"]
    assert out["k6"]["requests"] > 0 and out["k6"]["rate"] <= BAR
    assert "within the 0.01 bar" in out["why"] and "narrowed from a 63.0 s poll window" in out["why"]


def test_run16_window_with_its_customers_failing_would_fail(run16):
    rec = run16["record"]
    bad = [(s, t, True, ms) for s, t, _, ms in run16["spans"]]
    kind, out = custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), bad, run16["probe_timeout_s"], BAR)
    assert kind == "fail" and "B was not serving" in out["why"]


def test_run16_window_with_its_customers_too_slow_would_fail(run16):
    """not served = failed, or no reply within the probe's own timeout"""
    rec = run16["record"]
    slow = [(s, t, f, run16["probe_timeout_s"] * 1000 + 1) for s, t, f, _ in run16["spans"]]
    kind, _ = custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), slow, run16["probe_timeout_s"], BAR)
    assert kind == "fail"


def test_no_request_in_flight_voids(run16):
    rec = run16["record"]
    kind, out = custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), [], run16["probe_timeout_s"], BAR)
    assert kind == "void" and "no witness" in out["why"] and out["k6"]["requests"] == 0
    kind, out = custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), None, run16["probe_timeout_s"], BAR)
    assert kind == "void" and "do not pair up" in out["why"]


def test_only_requests_in_flight_in_the_narrowed_window_count(run16):
    """requests of the poll gap (before first_unanswered) are not B's
    outage's witnesses: failing every one of them changes nothing"""
    rec = run16["record"]
    fu = float(rec["first_unanswered"])
    gap_failed = [(s, t, True if t <= fu else f, ms) for s, t, f, ms in run16["spans"]]
    kind, out = custlat.judge_b_outage(rec, custlat.b_only_parts(rec, []), gap_failed,
                                       run16["probe_timeout_s"], BAR)
    assert kind == "served", out["why"]


def test_a_record_without_first_unanswered_keeps_its_poll_window():
    n = custlat.narrowed_outage({"t0": 100, "t1": 160, "source": "x"})
    assert (n["t0"], n["t1"], n["edge_uncertainty_s"]) == (100.0, 160.0, 0.0)
    # bad data (first unanswered at or after the first answered): no narrowing
    n = custlat.narrowed_outage({"t0": 100, "t1": 160, "first_unanswered": 160, "source": "x"})
    assert (n["t0"], n["edge_uncertainty_s"]) == (100.0, 0.0)
