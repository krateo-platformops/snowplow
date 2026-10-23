"""Run 15 (1.12.55), report only: the incremental list cell (#865) and the
store verification re-check (#875), from mem606's samples, per phase.

The expvar documents are RECORDED from snowplow's own code at the PR heads:
  * tests/fixtures/list_incremental_expvar_aec5f1fb.json — snowplow_list_incremental
    before / after #865's F5s flow (a divergence, an unanchor, two comparator passes);
  * tests/fixtures/store_verification_expvar_01d32632.json — snowplow_store_verification
    and its skipped_by_reason map before / after one forced pass over a true lost update.
They go through mem606's real sampler (load606.mem_sample, default prefixes)
on run 13's real phase timeline (tests/fixtures/fresh_run13.json). Later
samples reuse the recorded documents' shape with the counters set to the
cumulative values written below.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

from bench import load606
from tests.test_run14_list_narrow import F13, _sample

FIX = Path(__file__).resolve().parent / "fixtures"
LI = json.loads((FIX / "list_incremental_expvar_aec5f1fb.json").read_text())
SV = json.loads((FIX / "store_verification_expvar_01d32632.json").read_text())
LI_KEY, SV_KEY = "snowplow_list_incremental", "snowplow_store_verification"
SKIP_KEY = "snowplow_store_verification_skipped_by_reason"
BIG, SMALL = "composition.krateo.io/v1-2-2, Resource=compositions", "/v1, Resource=events"


def _li(passes, unanchors, divergences, refresh_skipped, anchors=1, live=1):
    d = copy.deepcopy(LI["after"][LI_KEY])
    d.update(passes=passes, unanchors=unanchors, divergences=divergences,
             divergence_total=sum(divergences.values()), refresh_skipped=refresh_skipped,
             anchors=anchors, live=live)
    return d


def _sv(gets, cleared, lost_update_forced, candidates, fired, skipped):
    d = copy.deepcopy(SV["after"])
    d[SV_KEY].update(store_divergence_recheck_gets_total=gets,
                     store_divergence_recheck_cleared_total=cleared,
                     store_divergent_lost_update_forced_total=lost_update_forced,
                     store_divergence_candidates_total=candidates, store_repairs_fired_total=fired)
    d[SKIP_KEY] = skipped
    return d


def _fills(big, small, reanchor):
    return {"snowplow_apistage_content_fill_by_gvr": {
                BIG: {"fills": big, "bytes": big * 9_000_000, "max_bytes": 9_000_000},
                SMALL: {"fills": small, "bytes": small * 1000, "max_bytes": 1000}},
            "snowplow_apistage_content_fill_total": {"absent": big + small, "reanchor": reanchor}}


# seconds after run 13's mem_start: storm < 624, burst 864-1164,
# churn list_dep@1 76-2677, @10 2682-3079, ..., steady after 4675
POINTS = [
    (100, LI["before"][LI_KEY], SV["before"], _fills(0, 0, 0)),              # storm (recorded)
    (500, LI["after"][LI_KEY], SV["after"], _fills(10, 2, 0)),               # storm (recorded)
    (1000, _li(12, {"divergence": 1}, {"key_mismatch": 1}, 5),
     _sv(12, 4, 1, 6, 0, {"undecorated_informer": 5, "recheck_error": 1}), _fills(15, 2, 1)),  # burst
    (2000, _li(40, {"divergence": 1, "lost_event": 3}, {"key_mismatch": 1}, 50),
     _sv(30, 14, 2, 20, 1, {"undecorated_informer": 5, "recheck_error": 1}), _fills(15, 40, 4)),  # churn@1
    (2900, _li(41, {"divergence": 1, "lost_event": 3}, {"key_mismatch": 1, "obj_src": 2}, 950),
     _sv(80, 60, 2, 70, 1, {"undecorated_informer": 5, "recheck_error": 1}), _fills(19, 400, 6)),  # churn@10
    (4700, _li(42, {"divergence": 1, "lost_event": 3}, {"key_mismatch": 1, "obj_src": 2}, 951),
     _sv(80, 60, 2, 70, 1, {"undecorated_informer": 5, "recheck_error": 1}), _fills(19, 400, 6)),  # steady
]


def _mem(points=POINTS):
    s0 = F13["mem_start"]["t"]
    mem = [dict(F13["mem_start"]), {"kind": "mem", "t": F13["load_detected_t"], "load_detected": True}]
    for dt, li, sv, fills in points:
        mem.append(_sample(s0 + dt, {LI_KEY: li, **sv, **fills}))
    return mem


def test_mem606_captures_both_with_the_default_prefixes():
    pre = load606.RUN8_VARS_PREFIXES
    assert "snowplow_list_incremental" in pre and "snowplow_store_verification" in pre
    rec = _sample(1, {LI_KEY: LI["after"][LI_KEY], **SV["after"]})
    assert set(rec["extra"]) >= {LI_KEY, SV_KEY, SKIP_KEY}
    assert rec["extra"][SV_KEY]["store_divergence_recheck_gets_total"] == 2      # the recorded value
    assert rec["extra"][LI_KEY]["divergences"] == {"key_mismatch": 1}


def test_list_incremental_report_per_phase():
    r = load606.list_incremental_report(_mem(), F13["fresh"])
    assert r["status"] == "present" and r["largest_cell_gvr"] == BIG
    p = r["by_phase"]
    st = p["storm"]
    # the recorded F5s flow: 1 key_mismatch divergence, 1 unanchor (divergence), 2 passes
    assert st["divergences_by_reason"] == {"key_mismatch": 1} and st["divergence_total"] == 1
    assert st["unanchors_by_reason"] == {"divergence": 1} and st["comparator_passes"] == 2
    assert st["largest_cell_fills"] == 10
    b = p["burst"]
    assert (b["comparator_passes"], b["fills_avoided_refresh_skipped"], b["reanchor_fills"],
            b["largest_cell_fills"], b["divergence_total"]) == (10, 5, 1, 5, 0)
    assert b["largest_cell_fills_per_min"] == round(5 / (500 / 60), 3)           # 1000 - 500 s
    c1 = p["churn:list_dep@1"]
    assert c1["unanchors_by_reason"] == {"lost_event": 3} and c1["largest_cell_fills"] == 0
    c10 = p["churn:list_dep@10"]
    assert c10["divergences_by_reason"] == {"obj_src": 2} and c10["fills_avoided_refresh_skipped"] == 900
    assert p["steady"]["comparator_passes"] == 1
    assert p["all"]["divergence_total"] == 3 and p["all"]["largest_cell_fills"] == 19
    assert r["anchored_cells_last"] == 1 and r["restarts"] == 0


def test_store_verification_report_per_phase():
    r = load606.store_verification_report(_mem(), F13["fresh"])
    assert r["status"] == "present"
    p = r["by_phase"]
    st = p["storm"]
    # the recorded forced pass: 2 re-check GETs, 1 forced lost update, 1 candidate
    assert (st["recheck_gets"], st["recheck_cleared"], st["divergence_candidates"]) == (2, 0, 1)
    assert st["findings"]["lost_update_forced"] == 1 and st["repairs"]["repairs_fired"] == 0
    b = p["burst"]
    assert (b["recheck_gets"], b["recheck_cleared"]) == (10, 4)
    assert b["skipped_by_reason"] == {"recheck_error": 1}
    c1 = p["churn:list_dep@1"]
    assert c1["repairs"]["repairs_fired"] == 1 and c1["findings"]["lost_update_forced"] == 1
    assert p["churn:list_dep@10"]["recheck_cleared"] == 46
    assert p["all"]["recheck_gets"] == 80 and p["all"]["recheck_cleared"] == 60
    assert r["gauges_last"]["store_repairs_pending"] == 1                       # recorded


def test_a_restart_takes_the_new_values():
    # 1050: still in the burst (the windows end at the last sample, here 1100)
    pts = POINTS[:3] + [(1050, _li(2, {}, {}, 1), _sv(3, 1, 0, 1, 0, {"undecorated_informer": 5}),
                         _fills(1, 0, 0)), POINTS[-1]]
    mem = _mem(pts)
    mem[-2]["num_gc"], mem[-3]["num_gc"] = 3, 900                       # NumGC went down
    r = load606.list_incremental_report(mem, F13["fresh"])
    assert r["restarts"] == 1 and r["by_phase"]["burst"]["comparator_passes"] == 10 + 2
    s = load606.store_verification_report(mem, F13["fresh"])
    assert s["restarts"] == 1 and s["by_phase"]["burst"]["recheck_gets"] == 10 + 3


def test_absent_reports_say_missing_with_a_warning_and_no_verdict_change():
    present = load606.evaluate([], F13["fresh"], [], mem=_mem())
    assert present["report"]["list_incremental_report_only"]["status"] == "present"
    assert present["report"]["store_verification_report_only"]["status"] == "present"
    assert not any("#865" in w or "#875" in w for w in present["report"]["report_warnings"])
    mem = [m for m in _mem() if m.get("kind") != "mem" or not m.get("extra")]
    absent = load606.evaluate([], F13["fresh"], [], mem=mem)
    for name, key in (("list_incremental_report_only", LI_KEY),
                      ("store_verification_report_only", SV_KEY)):
        rep = absent["report"][name]
        assert rep["status"] == "absent_in_all_samples" and "missing" in rep["note"].lower()
        assert any(w.startswith(f"{key} absent in all") and "missing, not zero" in w
                   for w in absent["report"]["report_warnings"])

    def strip(x):
        return [(c["check"], c["result"]) for c in x]
    assert strip(absent["checks"]) == strip(present["checks"])
    assert strip(absent["validity"]) == strip(present["validity"])
    assert absent["verdict"] == present["verdict"]


def test_run13_real_mem_samples_report_both_absent():
    """Run 13's real mem606 samples (1.12.52, neither key) -> absent."""
    with gzip.open(FIX / "mem_run13_samples.jsonl.gz", "rt") as fh:
        mem = [json.loads(ln) for ln in fh]
    v = load606.evaluate([], [], [], mem=mem)
    assert v["report"]["list_incremental_report_only"]["status"] == "absent_in_all_samples"
    assert v["report"]["store_verification_report_only"]["status"] == "absent_in_all_samples"
    assert len(v["report"]["report_warnings"]) == 3                     # + #855's list_narrow
