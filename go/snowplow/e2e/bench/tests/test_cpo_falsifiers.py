"""#917 reviewer falsifiers, adopted (scratchpad wt917 test_review917_falsifier.py
and mutbench test_review917_gap.py).

The shape is run 16's as run606 used to stamp it: last answered 900, the
unanswered request sent 930 hung until kubectl gave up at 959 (recorded as
first_unanswered), answered again 961. B's customers failing anywhere inside
the window — the poll gap included — must FAIL the pair through the customer
witness: not INVALID, not PASS, and not only because the whole-run k6 error
rate happens to be over its bar.
"""

from __future__ import annotations

import copy

import pytest

from bench import custlat
from tests.test_pair_ready_time import K0, P14, SHIFT, _arms, _down, _failing, _shift

DRIVE_POLL_S = 30
BAR = 0.01


def _rec(doc, t0_s, fu_s, t1_s):
    d = copy.deepcopy(doc)
    d["api_outages"] = [{"kind": "api_unreachable", "t0": K0 + t0_s, "t1": K0 + t1_s, "seconds": t1_s - t0_s,
                         "first_unanswered": K0 + fu_s, "seen_by": "get job/k6load", "open": False,
                         "source": "run606 polls"}]
    return d


def _failing_every(doc, lo, hi, nth):
    """every nth k6 request from lo to hi s after the run's first call failed"""
    d = copy.deepcopy(doc)
    c0, i = d["calls"][0][0], 0
    out = []
    for o, v in d["failed"]:
        if lo <= o - c0 < hi:
            i += 1
            v = 1 if i % nth == 0 else v
        out.append([o, v])
    d["failed"] = out
    return d


def _fails_by_the_witness(p):
    assert p["verdict"] == "FAIL", (p["invalid_reasons"], p["fail_reasons"])
    assert p["candidate_not_ready"]["result"] == "FAIL"
    assert any(st["kind"] == "api_outage" for st in p["candidate_not_ready"]["value"]["stretches"])


def test_customers_failing_while_the_unanswered_request_hung_fail_the_pair(tmp_path):
    t0, fu, t1 = 900, 959, 961
    b = _failing(_shift(_rec(P14, t0, fu, t1), SHIFT), t0 + DRIVE_POLL_S - SHIFT, fu - SHIFT)
    _fails_by_the_witness(_arms(tmp_path, P14, b))


def test_a_quarter_of_customers_failing_in_the_hung_flight_fails_the_pair(tmp_path):
    """~25 % not served, 25x the bar, while the whole-run k6 error rate stays
    under its bar: only the outage's customer witness can see it"""
    t0, fu, t1 = 900, 959, 961
    b = _failing_every(_shift(_rec(P14, t0, fu, t1), SHIFT), t0 + DRIVE_POLL_S - SHIFT, fu - SHIFT, 4)
    p = _arms(tmp_path, P14, b)
    _fails_by_the_witness(p)
    assert not p["invalid_reasons"]                       # not the whole-run rate: the witness


@pytest.fixture
def no_whole_run_bar(monkeypatch):
    """the pair with its whole-run k6 error-rate check switched off: what
    FAILs must be the outage's own witness"""
    orig = custlat._validity

    def v(name, ok, *a):
        return orig(name, True if name == "k6_error_rate[b]" else ok, *a)
    monkeypatch.setattr(custlat, "_validity", v)


@pytest.mark.parametrize("case", ["all_fail_50s_before", "quarter_fail_50s_before",
                                  "readyz_503_and_all_fail_50s_before"])
@pytest.mark.parametrize("bar", ["whole_run_bar_on", "whole_run_bar_off"])
def test_b_breaking_50s_before_the_first_unanswered_poll_fails(tmp_path, request, case, bar):
    """the reviewer's gap probe: B breaks over [909, 959), 50 s before the
    recorded first_unanswered — inside the window, so FAIL, with or without
    the whole-run k6 bar"""
    if bar == "whole_run_bar_off":
        request.getfixturevalue("no_whole_run_bar")
    base = _shift(_rec(P14, 900, 959, 961), SHIFT)
    b = {
        "all_fail_50s_before": lambda: _failing(base, 909 - SHIFT, 959 - SHIFT),
        "quarter_fail_50s_before": lambda: _failing_every(base, 909 - SHIFT, 959 - SHIFT, 4),
        "readyz_503_and_all_fail_50s_before": lambda: _failing(
            _shift(_down(_rec(P14, 900, 959, 961), 909 - SHIFT, 959 - SHIFT, restart=False, code=503), SHIFT),
            909 - SHIFT, 959 - SHIFT),
    }[case]()
    p = _arms(tmp_path, P14, b)
    assert p["verdict"] == "FAIL", (p["invalid_reasons"], p["fail_reasons"])
    assert p["candidate_not_ready"]["result"] == "FAIL"


# ── judge_stretch, unit (#917 F2, F4) ──

def test_a_request_hung_through_the_stretch_is_not_served():
    """started before the stretch, finished after it, past the probe's
    timeout: in flight in it, and not served — failed or not"""
    for failed in (True, False):
        j = custlat.judge_stretch([(40.0, 60.0)], {"k6load": [(0.0, 100.0, failed, 100000.0)]}, 30.0, BAR)
        assert j["k6"]["requests"] == 1 and j["k6"]["not_served"] == 1 and j["verdict"] == "fail"


def test_hung_requests_through_a_b_outage_fail_it():
    rec = {"t0": 100.0, "t1": 160.0}
    own = custlat.b_only_parts(rec, [])
    spans = [(90.0, 200.0, False, 110000.0)] * 5 + [(131.0, 131.1, False, 100.0)] * 400
    j = custlat.judge_stretch(own, {"k6load": spans, "browser": []}, 30.0, BAR)
    assert j["verdict"] == "fail" and j["k6"]["not_served"] == 5          # 5 of 405: 1.2 %, over the bar


@pytest.mark.parametrize("failed, want", [(False, "served"), (True, "fail")])
def test_the_bar_decides_alone_even_at_one_request(failed, want):
    """#917 F4: no minimum count — the 1 % bar decides: 0 of 1 is within it,
    1 of 1 is over it (the same as every other stretch the witness judges)"""
    j = custlat.judge_stretch([(10.0, 20.0)], {"k6load": [(12.0, 13.0, failed, 1000.0)], "browser": []}, 30.0, BAR)
    assert j["verdict"] == want and j["k6"]["requests"] == 1


def test_every_customer_source_counts_and_none_is_no_witness():
    spans_ok = [(12.0, 13.0, False, 1000.0)] * 99
    j = custlat.judge_stretch([(10.0, 20.0)], {"k6load": spans_ok, "browser": [(14.0, 15.0, True, 1000.0)] * 2},
                              30.0, BAR)
    assert j["verdict"] == "fail" and j["k6"]["by_source"]["browser"] == {"requests": 2, "not_served": 2}
    assert custlat.judge_stretch([(10.0, 20.0)], {"k6load": [], "browser": []}, 30.0, BAR)["verdict"] == "none"
    j = custlat.judge_stretch([(10.0, 20.0)], {"k6load": None, "browser": []}, 30.0, BAR)
    assert j["verdict"] == "unpaired" and j["k6"]["unpaired"] == ["k6load"]


def test_browser_spans_pair_by_time_and_a_response_without_timing_is_not_served():
    o = {"points": {"browser_http_req_duration": [(70.0, {}), (35.0, {}), (50.0, {})],
                    "browser_http_req_failed": [(0, {}), (1, {})]},
         "times": {"browser_http_req_duration": [100.07, 100.035, 200.05],
                   "browser_http_req_failed": [100.07, 100.035]}}
    got = sorted((round(s, 6), t, f, ms) for s, t, f, ms in custlat.browser_request_spans(o))
    # the third duration has no failed point at its time: no timed response, not served
    assert got == [(100.0, 100.035, True, 35.0), (100.0, 100.07, False, 70.0), (200.0, 200.05, True, 50.0)]
