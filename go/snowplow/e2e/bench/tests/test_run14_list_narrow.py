"""Run 14 (report only): #765 option B's shadow counters and the per-GVR
content fills, from mem606's samples.

The expvar values are RECORDED, not invented:
  * tests/fixtures/list_narrow_expvar_525b4ddf.json — snowplow's own
    ListNarrowExpvar() / DirtyMarkAttributionExpvar() at main 525b4ddf, around
    the G1 flow of internal/cache/list_narrow_shadow_765_test.go;
  * tests/fixtures/content_fill_by_gvr_run13.json — five samples of
    snowplow_apistage_content_fill_by_gvr from run 13's mem606 stream, the
    last pair across its snowplow restart.
They go through mem606's real sampler (load606.mem_sample with the default
prefixes) and then the report functions. Later samples reuse the recorded
documents' shape with the counters set to the cumulative values below.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

from bench import load606

FIX = Path(__file__).resolve().parent / "fixtures"
LN = json.loads((FIX / "list_narrow_expvar_525b4ddf.json").read_text())
CF = json.loads((FIX / "content_fill_by_gvr_run13.json").read_text())
G = "composition.krateo.io/v1-2-2, Resource=githubscaffoldingwithcompositionpages"


class _Vars:
    """A snowplow whose /debug/vars is `doc` (what mem_sample reads)."""

    def __init__(self, doc):
        self.doc = doc

    def get(self, path, token):
        if path == "/debug/vars":
            return load606.Response(200, json.dumps(self.doc).encode(), 1.0)
        return load606.Response(200 if path == "/readyz" else 404, b"", 1.0)


def _sample(t, doc):
    rec = load606.mem_sample(_Vars(doc), "tok", tuple(load606.RUN8_VARS_PREFIXES))
    rec["t"] = t
    return rec


def _shaped(avoid, change, ident, narrow_marks, list_dep):
    """The recorded `after` documents with these cumulative counters."""
    doc = copy.deepcopy(LN["after"])
    n = doc["snowplow_deps_list_narrow"]
    n["would_avoid_total"], n["would_change_total"] = avoid, change
    n["audit"]["identical"], n["list_dep_marks_total"] = ident, narrow_marks
    for b in doc["snowplow_deps_dirty_mark_attribution"]["by_bucket"]:
        if (b["path"], b["cause"], b["class"]) == ("object_event", "add_update", "list_dep"):
            b["count"] = list_dep
        elif (b["path"], b["cause"], b["class"]) == ("object_event", "add_update", "exact_dep"):
            b["count"] = 7 * list_dep        # other classes' marks are NOT list_dep marks
    return doc


T0 = 1000.0
PLAN = {"profile_s": 30, "storm_s": 600.0, "every_s": 300.0, "burst_at_s": 900.0,
        "burst_pre_s": 60.0, "burst_post_s": 240.0}   # run 13's plan: storm < 1600, burst 1840-2140


def _mem():
    docs = [
        (1010, LN["before"]),                     # storm: everything 0 (recorded)
        (1100, LN["after"]),                      # storm: 4 avoid, 1 identical, 4 list_dep (recorded)
        (2000, _shaped(10, 0, 3, 10, 12)),        # burst
        (2600, _shaped(30, 2, 9, 30, 60)),        # churn
        (3000, _shaped(34, 2, 10, 40, 80)),       # steady
        (3100, _shaped(3, 0, 1, 3, 5)),           # steady, after a snowplow restart
    ]
    mem = [{"kind": "mem_start", "t": T0, "cpu_plan": PLAN}]
    mem += [_sample(t, d) for t, d in docs]
    # the per-GVR fills ride on the same samples' cadence (their own times)
    for s in CF["samples"]:
        mem.append(_sample(T0 + 4000 + s["t_rel_s"],
                           {"snowplow_apistage_content_fill_by_gvr": s["by_gvr"]}))
    return mem


FRESH = [{"kind": "fresh", "label": "list_dep@10", "t": 2800.0, "latency_ms": 300000.0},
         {"kind": "fresh", "label": "list_dep@10", "t": 2850.0, "latency_ms": 1000.0}]   # churn 2500-2850


def test_mem606_captures_the_shadow_with_the_dirty_mark_attribution():
    assert "snowplow_deps_list_narrow" in load606.RUN8_VARS_PREFIXES
    rec = _sample(1, LN["after"])
    assert set(rec["extra"]) >= {"snowplow_deps_list_narrow", "snowplow_deps_dirty_mark_attribution"}
    rec = _sample(1, {"snowplow_apistage_content_fill_by_gvr": CF["samples"][0]["by_gvr"]})
    assert "snowplow_apistage_content_fill_by_gvr" in rec["extra"]


def test_list_narrow_report_per_phase():
    r = load606.list_narrow_report(_mem(), FRESH)
    p = r["by_phase"]
    keys = ("would_avoid", "would_change", "identical_audits", "list_narrow_list_dep_marks",
            "list_dep_marks")
    assert {k: p["storm"][k] for k in keys} == dict(zip(keys, (4, 0, 1, 4, 4)))
    assert {k: p["burst"][k] for k in keys} == dict(zip(keys, (6, 0, 2, 6, 8)))
    assert {k: p["churn"][k] for k in keys} == dict(zip(keys, (20, 2, 6, 20, 48)))
    # 3000: +4/+0/+1/+10/+20; 3100 restarted: the new values 3/0/1/3/5
    assert {k: p["steady"][k] for k in keys} == dict(zip(keys, (7, 0, 2, 13, 25)))
    assert {k: p["all"][k] for k in keys} == dict(zip(keys, (37, 2, 11, 43, 85)))
    assert p["storm"]["would_avoid_share_of_list_dep_marks"] == 1.0
    assert p["burst"]["would_avoid_share_of_list_dep_marks"] == 0.75
    assert p["churn"]["would_avoid_share_of_list_dep_marks"] == round(20 / 48, 4)
    assert p["steady"]["would_avoid_share_of_list_dep_marks"] == 0.28
    assert p["steady"]["would_avoid_share_of_list_narrow_marks"] == round(7 / 13, 4)
    assert r["counter_resets"] == 5
    assert r["phase_windows"]["churn"] == [2500.0, 2850.0]
    assert r["gate_last"]["identical_audits"] == 1           # the recorded gate readout


def test_content_fill_report_per_minute_and_gvr():
    r = load606.content_fill_report(_mem())
    s = CF["samples"]
    big = [x["by_gvr"][G] for x in s]
    want_fills = (big[1]["fills"] - big[0]["fills"]) + (big[3]["fills"] - big[2]["fills"]) + big[4]["fills"]
    want_bytes = (big[1]["bytes"] - big[0]["bytes"]) + (big[3]["bytes"] - big[2]["bytes"]) + big[4]["bytes"]
    assert r["total_by_gvr"][G] == {"fills": want_fills, "bytes": want_bytes}
    assert next(iter(r["total_by_gvr"])) == G                 # sorted by bytes
    assert r["counter_resets"] == 3                           # the restart, once per GVR
    minutes = {m["minute"]: m["by_gvr"] for m in r["per_minute"]}
    t0 = s[0]["t_rel_s"]
    m1 = int((s[1]["t_rel_s"] - t0) // 60)
    assert minutes[m1][G] == {"fills": big[1]["fills"] - big[0]["fills"],
                              "bytes": big[1]["bytes"] - big[0]["bytes"]}
    # a minute lists only GVRs that filled: in minute m1 only the big one did,
    # and sample 1 -> 2 filled nothing
    assert set(minutes[m1]) == {G}
    assert set(minutes) == {m1, int((s[3]["t_rel_s"] - t0) // 60), int((s[4]["t_rel_s"] - t0) // 60)}


def test_reports_land_in_the_verdict_and_change_no_check():
    mem = _mem()
    v = load606.evaluate([], FRESH, [], mem=mem)
    assert v["report"]["list_narrow_shadow_report_only"]["by_phase"]["all"]["would_avoid"] == 37
    assert v["report"]["content_fill_by_gvr_report_only"]["counter_resets"] == 3
    base = load606.evaluate([], FRESH, [], mem=[m for m in mem if not m.get("extra")])
    assert base["report"]["list_narrow_shadow_report_only"] is None
    assert base["report"]["content_fill_by_gvr_report_only"] is None
    # report only: the same checks, validity and verdict with or without them

    def strip(x):
        return [(c["check"], c["result"]) for c in x]
    assert strip(v["checks"]) == strip(base["checks"])
    assert strip(v["validity"]) == strip(base["validity"])
    assert v["verdict"] == base["verdict"]
