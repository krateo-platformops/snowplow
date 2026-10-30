"""#995: load606's store-verification report reads how the build judges a
forced pass's divergence candidates by the counters it PUBLISHES
(bench/store_verify.py), never by its version: the recheck (#875, to 1.12.58)
or the lost-update rule (#918/#923, from 1.12.59). On 1.12.59 the report used
to read None for the four recheck keys #923 removed, and none of #923's own
keys.

The 1.12.59 documents are RENDERED by 1.12.59's own expvar functions
(102ed658, internal/cache -tags unit, through its C7 stats seam, with every
stat a DISTINCT value: tests/fixtures/store_verification_expvar_102ed658.json).
The recheck documents are the recorded #875 capture
(store_verification_expvar_0934e202.json). The documents go through mem606's
real sampler and the report through the evaluate CLI.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import load606, store_verify
from tests.test_run14_list_narrow import _Vars

FIX = Path(__file__).resolve().parent / "fixtures"
RULE = json.loads((FIX / "store_verification_expvar_102ed658.json").read_text())
RECHECK = json.loads((FIX / "store_verification_expvar_0934e202.json").read_text())
SV = "snowplow_store_verification"
PBG = "snowplow_store_verification_pending_by_gvr"
G = "composition.krateo.io/v1-2-2, Resource=githubscaffoldingwithcompositionpages"
T0 = 1_800_000_000.0
RV = RULE["after"][SV]                                       # every stat a distinct value


def _rule(k=1):
    """1.12.59's family with every counter scaled by k (a cumulative series)."""
    d = copy.deepcopy(RULE["after"])
    for key, v in d[SV].items():
        if key.endswith("_total"):
            d[SV][key] = v * k
    d[PBG] = {G: 2}
    return d


def _mem(docs):
    out = [{"kind": "mem_start", "t": T0 - 1}]
    for i, doc in enumerate(docs):
        rec = load606.mem_sample(_Vars(doc), "tok", tuple(load606.RUN8_VARS_PREFIXES))
        rec["t"] = T0 + 15 * i
        out.append(rec)
    return out


def _evaluate(tmp_path, docs, whole=False):
    f = tmp_path / "mem-mem606-0.jsonl"
    f.write_text("".join(json.dumps(m) + "\n" for m in _mem(docs)))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)], capture_output=True, text=True,
                       cwd=FIX.parent.parent, check=False, timeout=300)
    v = json.loads(p.stdout)
    return v if whole else v["report"]["store_verification_report_only"]


def test_the_judging_mechanism_is_what_the_build_publishes():
    assert store_verify.mechanism(RULE["after"])[0] == "lost_update_rule"
    assert store_verify.mechanism(RECHECK["after"])[0] == "recheck"
    both = copy.deepcopy(RULE["after"])
    both[SV]["store_divergence_recheck_gets_total"] = 0
    assert store_verify.mechanism(both)[0] is None and "both" in store_verify.mechanism(both)[1]
    neither = copy.deepcopy(RECHECK["after"])
    for k in store_verify.identifying("recheck"):
        neither[SV].pop(k, None)
    assert store_verify.mechanism(neither)[0] is None and "neither" in store_verify.mechanism(neither)[1]


@pytest.mark.parametrize("keep", ["store_divergence_recheck_cleared_total", "store_divergence_recheck_undelivered_total"])
def test_any_one_of_a_mechanisms_own_keys_identifies_it(keep):
    """#996 F7: a partial recheck family beside the rule's keys is 'both',
    never the rule; a rule family that carries only its gauges is still the rule."""
    partial = copy.deepcopy(RULE["after"])
    partial[SV][keep] = 1
    assert store_verify.mechanism(partial)[0] is None
    only_gauge = {SV: {"store_divergence_pending_keys": 0, "store_verifications_total": 1}}
    assert store_verify.mechanism(only_gauge)[0] == "lost_update_rule"
    assert store_verify.counters(only_gauge[SV], "lost_update_rule")["rule_cleared"] is None


def test_the_1_12_59_rendering_carries_the_rule_and_not_the_recheck():
    for name, stat in store_verify.COMMON["lost_update_rule"] + store_verify.OWN["lost_update_rule"] + \
            store_verify.GAUGES["lost_update_rule"]:
        assert stat in RV, name
    assert not any(k.startswith("store_divergence_recheck") for k in RV)
    assert PBG in RULE["after"]
    assert len(set(RV.values())) == len(RV)                  # distinct: a stat read under another name shows


def test_1_12_59_reports_its_own_mechanism_through_the_cli(tmp_path):
    r = _evaluate(tmp_path, [_rule(0), _rule(1), _rule(3)])
    assert r["judging"] == "lost_update_rule" and r["judging_identified_by"].startswith("publishes ")
    a = r["by_phase"]["all"]
    # each quantity is its own stat's rendered (distinct) value, x3 over the run; the expected
    # stat per name is written HERE, read in 102ed658's store_verify_stats.go, never taken from
    # store_verify's own table (a test derived from it would pass a swapped mapping)
    expect = {"rule_cleared": "store_divergence_cleared_total",
              "no_delivery_record": "store_divergence_unrecorded_total",
              "store_ahead": "store_divergence_store_ahead_total",
              "received": "store_divergence_received_total",
              "undecided": "store_divergence_undecided_total",
              "pending_resolved_lost": "store_divergence_pending_resolved_lost_total",
              "pending_resolved_received": "store_divergence_pending_resolved_received_total",
              "pending_superseded": "store_divergence_pending_superseded_total"}
    for name, stat in expect.items():
        assert a[name] == 3 * RV[stat], (name, stat)
    assert "recheck_gets" not in a and "recheck_cleared" not in a
    assert {"recheck_gets", "recheck_cleared", "recheck_undelivered"} <= set(r["not_published"])
    assert r["judging_gauges_last"] == {"pending_keys": RV["store_divergence_pending_keys"],
                                        "pending_oldest_age_seconds": RV["store_divergence_pending_oldest_age_seconds"],
                                        "position_unavailable": RV["store_position_unavailable"],
                                        "pending_by_gvr": {G: 2}}


def test_the_cleared_counts_are_not_one_quantity_and_keep_their_names(tmp_path):
    """#996 F1: the recheck counts forced-pass candidates only (delivered
    during the pass, still divergent after the confirm loop), the rule every
    raw candidate at both sites: never one name for the two."""
    rc = _evaluate(tmp_path, [RECHECK["before"], RECHECK["after"]])
    ru = _evaluate(tmp_path, [_rule(0), _rule(1)])
    assert "recheck_cleared" in rc["by_phase"]["all"] and "rule_cleared" not in rc["by_phase"]["all"]
    assert "rule_cleared" in ru["by_phase"]["all"] and "recheck_cleared" not in ru["by_phase"]["all"]
    assert "rule_cleared" in rc["not_published"] and "recheck_cleared" in ru["not_published"]
    assert not {"candidates_cleared"} & (set(rc["by_phase"]["all"]) | set(ru["by_phase"]["all"]))
    shared = {"no_delivery_record", "divergence_candidates", "verifications"}
    assert shared <= set(rc["by_phase"]["all"]) & set(ru["by_phase"]["all"])
    assert "re-check GETs that" not in store_verify.__doc__   # it counts candidates, not GETs


def test_the_recheck_build_reports_its_own_quantities(tmp_path):
    r = _evaluate(tmp_path, [RECHECK["before"], RECHECK["after"]])
    assert r["judging"] == "recheck"
    a = r["by_phase"]["all"]
    assert {"no_delivery_record", "recheck_gets", "recheck_cleared", "recheck_undelivered"} <= set(a)
    assert "store_ahead" not in a and {"store_ahead", "pending_keys", "pending_by_gvr"} <= set(r["not_published"])
    assert r["judging_gauges_last"] == {}


@pytest.mark.parametrize("odd", ["neither", "both"])
def test_one_sample_with_neither_or_both_makes_the_run_read_none(tmp_path, odd):
    """#996 F3/F6: one ambiguous sample among rule samples: no mechanism is
    read, the reason names it, and the report warns."""
    x = copy.deepcopy(_rule(1))
    if odd == "neither":
        for k in store_verify.identifying("lost_update_rule"):
            x[SV].pop(k, None)
    else:
        x[SV]["store_divergence_recheck_gets_total"] = 5
    v = _evaluate(tmp_path, [_rule(0), x, _rule(2)], whole=True)
    r = v["report"]["store_verification_report_only"]
    assert r["judging"] is None and r["not_published"] is None and r["judging_gauges_last"] == {}
    assert "rule_cleared" not in r["by_phase"]["all"]
    assert odd in r["judging_identified_by"] and "lost_update_rule (publishes" in r["judging_identified_by"]
    assert any(w.startswith("store verification: how the build judges") and odd in w
               for w in v["report"]["report_warnings"])


def test_a_stream_of_both_mechanisms_reads_neither(tmp_path):
    v = _evaluate(tmp_path, [RECHECK["before"], RECHECK["after"], _rule(1), _rule(2)], whole=True)
    r = v["report"]["store_verification_report_only"]
    assert r["judging"] is None and "rule_cleared" not in r["by_phase"]["all"]
    assert r["not_published"] is None
    assert r["judging_gauges_last"] == {}                     # F5: never the last sample's mechanism's gauges
    assert r["by_phase"]["all"]["findings"] is not None       # the shared families still read
    assert "recheck (publishes" in r["judging_identified_by"]
    assert any("store verification" in w for w in v["report"]["report_warnings"])


def test_one_mechanism_on_every_sample_adds_no_warning(tmp_path):
    v = _evaluate(tmp_path, [_rule(0), _rule(1)], whole=True)
    assert not any("store verification" in w for w in v["report"]["report_warnings"])


def test_a_counter_the_build_stops_publishing_is_missing_not_zero():
    v = copy.deepcopy(RV)
    del v["store_divergence_unrecorded_total"]
    assert store_verify.counters(v, "lost_update_rule")["no_delivery_record"] is None
    assert store_verify.counters(v, None) == {}


def test_a_missing_gauge_or_map_is_none_never_zero():
    """#996 F4/F8."""
    x = copy.deepcopy(RULE["after"])
    del x[SV]["store_divergence_pending_keys"]
    x.pop(PBG)
    g = store_verify.gauges(x, "lost_update_rule")
    assert g["pending_keys"] is None and g["pending_by_gvr"] is None
    x[PBG] = {G: "2", "other": True}                          # not a number: None, never passed through
    assert store_verify.gauges(x, "lost_update_rule")["pending_by_gvr"] == {G: None, "other": None}


def test_the_mechanism_never_changes_the_verdict(tmp_path):
    """#996 F9: the report is report-only: the same run reads the same
    verdict, checks and validity whatever store-verification family it carries."""
    def core(docs):
        v = _evaluate(tmp_path, docs, whole=True)
        return v["verdict"], v.get("checks"), v.get("validity")
    base = core([_rule(0), _rule(1)])
    for docs in ([RECHECK["before"], RECHECK["after"]], [RECHECK["after"], _rule(1)],
                 [{k: v for k, v in RULE["after"].items() if k != SV}] * 2):
        assert core(docs) == base
