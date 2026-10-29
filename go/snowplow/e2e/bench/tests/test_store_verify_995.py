"""#995: load606's store-verification report reads how the build judges a
forced pass's divergence candidates by the counters it PUBLISHES
(bench/store_verify.py), never by its version: the recheck (#875, to 1.12.58)
or the lost-update rule (#918/#923, from 1.12.59). 1.12.59 used to read None
for the four recheck keys #923 removed, and none of its own keys.

The 1.12.59 documents are RENDERED by its own expvar functions (102ed658,
internal/cache -tags unit, through its C7 stats seam:
tests/fixtures/store_verification_expvar_102ed658.json); the recheck ones are
the recorded #875 capture (store_verification_expvar_0934e202.json). They go
through mem606's real sampler and the report through the evaluate CLI.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

from bench import load606, store_verify
from tests.test_run14_list_narrow import _Vars

FIX = Path(__file__).resolve().parent / "fixtures"
RULE = json.loads((FIX / "store_verification_expvar_102ed658.json").read_text())
RECHECK = json.loads((FIX / "store_verification_expvar_0934e202.json").read_text())
SV = "snowplow_store_verification"
PBG = "snowplow_store_verification_pending_by_gvr"
G = "composition.krateo.io/v1-2-2, Resource=githubscaffoldingwithcompositionpages"
T0 = 1_800_000_000.0


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


def _evaluate(tmp_path, docs):
    f = tmp_path / "mem-mem606-0.jsonl"
    f.write_text("".join(json.dumps(m) + "\n" for m in _mem(docs)))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)], capture_output=True, text=True,
                       cwd=FIX.parent.parent, check=False, timeout=300)
    return json.loads(p.stdout)["report"]["store_verification_report_only"]


def test_the_judging_mechanism_is_what_the_build_publishes():
    assert store_verify.mechanism(RULE["after"])[0] == "lost_update_rule"
    assert store_verify.mechanism(RECHECK["after"])[0] == "recheck"
    both = copy.deepcopy(RULE["after"])
    both[SV]["store_divergence_recheck_gets_total"] = 0
    assert store_verify.mechanism(both)[0] is None and "both" in store_verify.mechanism(both)[1]
    neither = copy.deepcopy(RECHECK["after"])
    del neither[SV]["store_divergence_recheck_gets_total"]
    assert store_verify.mechanism(neither)[0] is None and "neither" in store_verify.mechanism(neither)[1]


def test_the_1_12_59_rendering_carries_the_rule_and_not_the_recheck():
    v = RULE["after"][SV]
    for name, stat in store_verify.COMMON["lost_update_rule"] + store_verify.OWN["lost_update_rule"] + \
            store_verify.GAUGES["lost_update_rule"]:
        assert stat in v, name
    assert not any(k.startswith("store_divergence_recheck") for k in v)
    assert PBG in RULE["after"]


def test_1_12_59_reports_its_own_mechanism_through_the_cli(tmp_path):
    r = _evaluate(tmp_path, [_rule(0), _rule(1), _rule(3)])
    assert r["judging"] == "lost_update_rule" and r["judging_identified_by"].startswith("publishes ")
    a = r["by_phase"]["all"]
    # the rendered values (cleared 6, store_ahead 4, received 3, undecided 1, pending lost 1 / received 2 /
    # superseded 1, unrecorded 1), x3 over the run
    assert (a["candidates_cleared"], a["no_delivery_record"], a["store_ahead"], a["received"], a["undecided"]) == \
        (18, 3, 12, 9, 3)
    assert (a["pending_resolved_lost"], a["pending_resolved_received"], a["pending_superseded"]) == (3, 6, 3)
    assert "recheck_gets" not in a and {"recheck_gets", "recheck_undelivered"} <= set(r["not_published"])
    assert r["judging_gauges_last"] == {"pending_keys": 2, "pending_oldest_age_seconds": 7.5,
                                        "position_unavailable": 0, "pending_by_gvr": {G: 2}}


def test_the_recheck_build_reports_the_same_shared_quantities(tmp_path):
    r = _evaluate(tmp_path, [RECHECK["before"], RECHECK["after"]])
    assert r["judging"] == "recheck"
    a = r["by_phase"]["all"]
    assert {"candidates_cleared", "no_delivery_record", "recheck_gets", "recheck_undelivered"} <= set(a)
    assert "store_ahead" not in a and {"store_ahead", "pending_keys", "pending_by_gvr"} <= set(r["not_published"])
    assert r["judging_gauges_last"] == {}
    rule = _evaluate(tmp_path, [_rule(0), _rule(1)])["by_phase"]["all"]
    shared = {"candidates_cleared", "no_delivery_record", "divergence_candidates", "verifications"}
    assert shared <= set(a) & set(rule)                                   # the same names on both


def test_a_stream_of_both_mechanisms_reads_neither(tmp_path):
    r = _evaluate(tmp_path, [RECHECK["before"], RECHECK["after"], _rule(1), _rule(2)])
    assert r["judging"] is None and "candidates_cleared" not in r["by_phase"]["all"]
    assert "publish" in r["judging_identified_by"] and r["not_published"] is None
    assert r["by_phase"]["all"]["findings"] is not None                   # the shared families still read


def test_a_counter_the_build_stops_publishing_is_missing_not_zero():
    v = copy.deepcopy(RULE["after"][SV])
    del v["store_divergence_unrecorded_total"]
    assert store_verify.counters(v, "lost_update_rule")["no_delivery_record"] is None
    assert store_verify.counters(v, None) == {}
