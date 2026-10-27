"""A control-plane outage of B is judged by B's customers (#606 run 16).

Run 16 came back INVALID on its control-plane-outage check alone: ONE failed
laptop poll in a 63 s window (last answered 1793061139, first unanswered
1793061200, answered again 1793061202), while B's customers were served. Now:

- the WHOLE window [t0, t1] is judged: the drop began somewhere between t0
  and the first unanswered poll, when is not known (Diego, 2026-10-27: the
  clearest approach, never a narrower one); the poll gap is in the run's
  report for information only;
- exactly the part an A outage's window also covers is the vantage's (cut
  from both runs);
- the rest is one more stretch for the one customer witness (judge_stretch,
  the same as a readiness gap): not served over the 1 % bar -> FAIL; served
  -> jointly-Ready time with a NOTE; no request in flight -> void.

Fixture: run 16 B's real outage record and every customer request of B in
flight in its 63 s window, per source — k6load (10,035, 48 failed: 0.48 %)
and the browser's completed requests (357, none failed) —
tests/fixtures/run16b_cpo_witness.json.gz, extracted with
custlat.raw_from_dir from snowplow-bench-run16b-6e261cdf.
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
    f["customers"] = {k: [(s, t, bool(x), ms) for s, t, x, ms in v] for k, v in f["customers"].items()}
    return f


def judge(run16, customers):
    (rec,) = custlat.control_plane_outages([run16["record"]], run16["summary"]["k6_span"])
    return custlat.judge_stretch(custlat.b_only_parts(rec, []), customers, run16["probe_timeout_s"], BAR)


def test_the_fixture_is_run16s_window_whole(run16):
    """provenance: every customer request in flight in the window is there"""
    s = run16["summary"]
    assert (run16["record"]["t0"], run16["record"]["first_unanswered"], run16["record"]["t1"]) == (T0, FU, T1)
    assert {k: len(v) for k, v in run16["customers"].items()} == {"k6load": 10035, "browser": 357}
    assert s["k6load"]["window_failed"] == 48 and s["browser"]["window_failed"] == 0
    assert s["run_requests"] == {"k6load": 2701229, "browser": 94497}


def test_the_whole_window_is_judged_and_the_poll_gap_only_reported(run16):
    (rec,) = custlat.control_plane_outages([run16["record"]], run16["summary"]["k6_span"])
    assert (rec["t0"], rec["t1"], rec["seconds"]) == (float(T0), float(T1), 63.0)
    assert (rec["first_unanswered"], rec["poll_gap_s"]) == (FU, 61.0)
    assert custlat.b_only_parts(rec, []) == [(float(T0), float(T1))]


def test_run16_window_is_witnessed_served_not_void(run16):
    """A had no outage then: B's own part is the whole 63 s window; B's
    customers had 10,392 requests in flight in it (k6load 10,035 — 48
    failed — and the browser's 357), 48 not served: 0.46 % -> served, kept
    as jointly-Ready time, not void."""
    j = judge(run16, run16["customers"])
    assert j["verdict"] == "served"
    assert j["k6"]["by_source"] == {"browser": {"requests": 357, "not_served": 0},
                                    "k6load": {"requests": 10035, "not_served": 48}}
    assert (j["k6"]["requests"], j["k6"]["not_served"], j["k6"]["rate"]) == (10392, 48, 0.004619)


def test_customers_failing_only_in_the_poll_gap_fail_b(run16):
    """B's requests fail ONLY in the poll gap (completing before the first
    unanswered poll): inside the window, so B was not serving — FAIL.
    Judging from the first unanswered poll on would have missed them."""
    gap = {k: [(s, t, True if t <= FU else f, ms) for s, t, f, ms in v] for k, v in run16["customers"].items()}
    # every request that completed after the first unanswered poll is as recorded
    assert all([sp for sp in gap[k] if sp[1] > FU] == [sp for sp in v if sp[1] > FU]
               for k, v in run16["customers"].items())
    j = judge(run16, gap)
    assert j["verdict"] == "fail" and j["k6"]["not_served"] > 0.9 * j["k6"]["requests"]


def test_customers_failing_or_too_slow_fail_b(run16):
    bad = {k: [(s, t, True, ms) for s, t, _, ms in v] for k, v in run16["customers"].items()}
    assert judge(run16, bad)["verdict"] == "fail"
    slow = {k: [(s, t, f, run16["probe_timeout_s"] * 1000 + 1) for s, t, f, _ in v]
            for k, v in run16["customers"].items()}
    assert judge(run16, slow)["verdict"] == "fail"   # not served = failed, or past the probe's timeout


def test_the_browser_alone_can_fail_b(run16):
    """the browser's customers are B's customers too: its requests in the
    window failing (k6load served) is over the bar -> FAIL"""
    c = dict(run16["customers"], browser=[(s, t, True, ms) for s, t, _, ms in run16["customers"]["browser"]])
    j = judge(run16, c)
    assert j["verdict"] == "fail" and j["k6"]["by_source"]["browser"]["not_served"] == 357


def test_no_request_in_flight_voids(run16):
    assert judge(run16, {"k6load": [], "browser": []})["verdict"] == "none"
    assert judge(run16, {"k6load": None, "browser": []})["verdict"] == "unpaired"


def test_a_record_without_first_unanswered_has_no_poll_gap():
    (rec,) = custlat.control_plane_outages([{"t0": 100, "t1": 160, "source": "x"}], None)
    assert (rec["t0"], rec["t1"], rec["poll_gap_s"]) == (100.0, 160.0, None)
