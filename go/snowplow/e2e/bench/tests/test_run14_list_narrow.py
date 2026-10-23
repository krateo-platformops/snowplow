"""Run 14 (report only): #765 option B's shadow counters and the per-GVR
content fills, from mem606's samples.

The expvar values are RECORDED, not invented:
  * tests/fixtures/list_narrow_expvar_525b4ddf.json — snowplow's own
    ListNarrowExpvar() / DirtyMarkAttributionExpvar() at main 525b4ddf, around
    the G1 flow of internal/cache/list_narrow_shadow_765_test.go;
  * tests/fixtures/content_fill_by_gvr_run13.json — five samples of
    snowplow_apistage_content_fill_by_gvr from run 13's mem606 stream, the
    last pair across its snowplow restart;
  * tests/fixtures/fresh_run13.json — run 13's fresh606 stream (samples and
    churn records) and mem606's anchors: the real phase timeline.
They go through mem606's real sampler (load606.mem_sample with the default
prefixes) and then the report functions. Later samples reuse the recorded
documents' shape with the counters set to the cumulative values below.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

from bench import load606

FIX = Path(__file__).resolve().parent / "fixtures"
LN = json.loads((FIX / "list_narrow_expvar_525b4ddf.json").read_text())
CF = json.loads((FIX / "content_fill_by_gvr_run13.json").read_text())
LN_KEY = "snowplow_deps_list_narrow"
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


# fresh606's churn phase list_dep@10 ran 2550-2850 (its record's t0/t1, from
# run 14), from before its first probe sample started (2700)
FRESH = [{"kind": "fresh", "label": "list_dep@10", "t": 2800.0, "latency_ms": 100000.0},
         {"kind": "fresh", "label": "list_dep@10", "t": 2850.0, "latency_ms": 1000.0},
         {"kind": "churn", "label": "list_dep@10", "issued": 3000, "failed": 0, "t0": 2550.0,
          "t1": 2850.0, "target_rate": 10.0, "achieved_rate": 10.0}]
KEYS = ("would_avoid", "would_change", "identical_audits", "list_narrow_list_dep_marks",
        "list_dep_marks")


def _vals(phase):
    return tuple(phase[k] for k in KEYS)


def test_mem606_captures_the_shadow_with_the_dirty_mark_attribution():
    assert "snowplow_deps_list_narrow" in load606.RUN8_VARS_PREFIXES
    rec = _sample(1, LN["after"])
    assert set(rec["extra"]) >= {"snowplow_deps_list_narrow", "snowplow_deps_dirty_mark_attribution"}
    rec = _sample(1, {"snowplow_apistage_content_fill_by_gvr": CF["samples"][0]["by_gvr"]})
    assert "snowplow_apistage_content_fill_by_gvr" in rec["extra"]


def test_list_narrow_report_per_phase():
    r = load606.list_narrow_report(_mem(), FRESH)
    p = r["by_phase"]
    assert _vals(p["storm"]) == (4, 0, 1, 4, 4)
    assert _vals(p["burst"]) == (6, 0, 2, 6, 8)
    assert _vals(p["churn:list_dep@10"]) == _vals(p["churn"]) == (20, 2, 6, 20, 48)
    # 3000: +4/+0/+1/+10/+20; 3100 restarted: the new values 3/0/1/3/5
    assert _vals(p["steady"]) == (7, 0, 2, 13, 25)
    assert _vals(p["all"]) == (37, 2, 11, 43, 85)
    assert p["storm"]["would_avoid_share_of_list_dep_marks"] == 1.0
    assert p["burst"]["would_avoid_share_of_list_dep_marks"] == 0.75
    assert p["churn"]["would_avoid_share_of_list_dep_marks"] == round(20 / 48, 4)
    assert p["steady"]["would_avoid_share_of_list_dep_marks"] == 0.28
    assert p["steady"]["would_avoid_share_of_list_narrow_marks"] == round(7 / 13, 4)
    assert r["restarts"] == 1                                 # one restart, not 5 series
    assert {"phase": "churn:list_dep@10", "t0": 2550.0, "t1": 2850.0} in r["phase_windows"]
    assert r["churn_rates"] == {"churn:list_dep@10": 10.0}
    assert r["gate_last"]["identical_audits"] == 1           # the recorded gate readout


# ─── run 13's REAL timeline: fresh606 churns for the whole run ─────────────

F13 = json.loads((FIX / "fresh_run13.json").read_text())


def _mem13(points):
    """Run 13's real mem_start and load anchor, with list-narrow samples at
    the given seconds after mem_start (cumulative values)."""
    s0 = F13["mem_start"]["t"]
    mem = [dict(F13["mem_start"]),
           {"kind": "mem", "t": F13["load_detected_t"], "load_detected": True}]
    mem += [_sample(s0 + dt, _shaped(*vals)) for dt, vals in points]
    return mem


def test_run13_phases_come_from_the_churn_schedule_and_storm_wins():
    """#855 gate: on run 13, fresh606's probe samples cover 73-4,671 s, so a
    window from them made "churn" the whole run. The churn PHASES (rate and
    mode) come from the schedule; storm and burst win where they overlap
    list_dep@1, which churned from 76 s; steady is only after the last."""
    s0 = F13["mem_start"]["t"]
    mem = _mem13([
        (100, (0, 0, 0, 0, 0)),          # storm (and churn@1)
        (500, (40, 0, 10, 40, 50)),      # storm (and churn@1)
        (1000, (50, 0, 12, 50, 70)),     # burst (and churn@1)
        (2000, (60, 0, 14, 60, 90)),     # churn list_dep@1 only
        (2900, (160, 5, 30, 200, 400)),  # churn list_dep@10
        (3300, (360, 9, 60, 600, 1200)),  # churn list_dep@50
        (4700, (361, 9, 60, 601, 1201)),  # steady: fresh606 has finished
    ])
    phases = load606.run_phases(mem, F13["fresh"])
    names = [n for n, _, _ in phases]
    assert names == ["storm", "burst", "churn:list_dep@1", "churn:list_dep@10", "churn:list_dep@50",
                     "churn:list_dep_status@1", "churn:list_dep_status@10",
                     "churn:list_dep_status@50"]
    win = {n: (a, b) for n, a, b in phases}
    assert win["churn:list_dep@1"][0] - s0 < 100 < win["storm"][1] - s0     # they overlap
    r = load606.list_narrow_report(mem, F13["fresh"])
    p = r["by_phase"]
    assert _vals(p["storm"]) == (40, 0, 10, 40, 50)
    assert _vals(p["burst"]) == (10, 0, 2, 10, 20)
    assert _vals(p["churn:list_dep@1"]) == (10, 0, 2, 10, 20)
    assert _vals(p["churn:list_dep@10"]) == (100, 5, 16, 140, 310)
    assert _vals(p["churn:list_dep@50"]) == (200, 4, 30, 400, 800)
    assert _vals(p["churn"]) == (310, 9, 48, 550, 1130)
    assert _vals(p["steady"]) == (1, 0, 0, 1, 1)
    assert r["churn_rates"]["churn:list_dep@50"] == 50.0


def test_churn_driver_records_its_schedule():
    d = load606.ChurnDriver([("t", "x")], 50.0, lambda t, m: True).start()
    import time
    time.sleep(0.1)
    rec = d.stop()
    assert rec["t0"] <= rec["t1"] and rec["t1"] - rec["t0"] < 5 and rec["issued"] >= 1


# ─── restarts: once per sample pair ────────────────────────────────────────


def _fill(t, num_gc, by_gvr):
    return {"kind": "mem", "t": t, "num_gc": num_gc,
            "extra": {"snowplow_apistage_content_fill_by_gvr": by_gvr}}


def test_a_restart_takes_every_series_as_is_even_one_that_grew_past_its_old_value():
    """#855 gate: run 13's restart left 11 of 55 GVR series above their old
    value; a per-series test missed 13 fills. Per pair: NumGC went down (or
    any series did), so every value is what it counted since the restart."""
    mem = [_fill(1000, 900, {"a": {"fills": 10, "bytes": 1000}, "b": {"fills": 5, "bytes": 50}}),
           # restarted: a dropped; b already counted 8 since the restart (> its old 5)
           _fill(1700, 40, {"a": {"fills": 2, "bytes": 200}, "b": {"fills": 8, "bytes": 80}}),
           _fill(1760, 41, {"a": {"fills": 3, "bytes": 300}, "b": {"fills": 8, "bytes": 80}})]
    r = load606.content_fill_report(mem)
    assert r["restarts"] == 1
    assert r["total_by_gvr"]["b"] == {"fills": 8, "bytes": 80}        # not 3 / 30
    assert r["total_by_gvr"]["a"] == {"fills": 3, "bytes": 300}
    # NumGC alone (no series went down) still marks the restart
    mem = [_fill(1000, 900, {"b": {"fills": 5, "bytes": 50}}),
           _fill(1700, 40, {"b": {"fills": 8, "bytes": 80}})]
    r = load606.content_fill_report(mem)
    assert r["restarts"] == 1 and r["total_by_gvr"]["b"] == {"fills": 8, "bytes": 80}


def test_run13_restart_is_one_restart():
    s = CF["samples"]
    mem = [_fill(5000 + x["t_rel_s"], None, x["by_gvr"]) for x in s]
    assert load606.content_fill_report(mem)["restarts"] == 1


def test_content_fill_report_per_minute_and_gvr():
    r = load606.content_fill_report(_mem())
    s = CF["samples"]
    big = [x["by_gvr"][G] for x in s]
    want_fills = (big[1]["fills"] - big[0]["fills"]) + (big[3]["fills"] - big[2]["fills"]) + big[4]["fills"]
    want_bytes = (big[1]["bytes"] - big[0]["bytes"]) + (big[3]["bytes"] - big[2]["bytes"]) + big[4]["bytes"]
    assert r["total_by_gvr"][G] == {"fills": want_fills, "bytes": want_bytes}
    assert next(iter(r["total_by_gvr"])) == G                 # sorted by bytes
    assert r["restarts"] == 1
    minutes = {m["minute"]: m["by_gvr"] for m in r["per_minute"]}
    t0 = s[0]["t_rel_s"]
    m1 = int((s[1]["t_rel_s"] - t0) // 60)
    assert minutes[m1][G] == {"fills": big[1]["fills"] - big[0]["fills"],
                              "bytes": big[1]["bytes"] - big[0]["bytes"]}
    # a minute lists only GVRs that filled: in minute m1 only the big one did,
    # and sample 1 -> 2 filled nothing
    assert set(minutes[m1]) == {G}
    assert set(minutes) == {m1, int((s[3]["t_rel_s"] - t0) // 60), int((s[4]["t_rel_s"] - t0) // 60)}


def test_content_fill_report_caps_the_gvrs():
    """verdict.json keeps the top N GVRs by bytes; the rest is "(other)"."""
    r = load606.content_fill_report(_mem(), top=1)
    assert list(r["total_by_gvr"]) == [G, "(other)"]
    full = load606.content_fill_report(_mem(), top=100)["total_by_gvr"]
    other = r["total_by_gvr"]["(other)"]
    assert other["gvrs"] == 2
    assert other["fills"] == sum(v["fills"] for g, v in full.items() if g != G)
    assert other["bytes"] == sum(v["bytes"] for g, v in full.items() if g != G)
    assert all(set(m["by_gvr"]) <= {G, "(other)"} for m in r["per_minute"])
    assert r["gvrs_seen"] == 3


def test_reports_land_in_the_verdict_and_change_no_check():
    mem = _mem()
    v = load606.evaluate([], FRESH, [], mem=mem)
    assert v["report"]["list_narrow_shadow_report_only"]["by_phase"]["all"]["would_avoid"] == 37
    assert v["report"]["content_fill_by_gvr_report_only"]["restarts"] == 1
    assert v["report"]["list_narrow_shadow_report_only"]["status"] == "present"
    assert v["report"]["report_warnings"] == []
    base = load606.evaluate([], FRESH, [], mem=[m for m in mem if not m.get("extra")])
    assert base["report"]["list_narrow_shadow_report_only"]["status"] == "absent_in_all_samples"
    assert base["report"]["content_fill_by_gvr_report_only"] is None
    # report only: the same checks, validity and verdict with or without them

    def strip(x):
        return [(c["check"], c["result"]) for c in x]
    assert strip(v["checks"]) == strip(base["checks"])
    assert strip(v["validity"]) == strip(base["validity"])
    assert v["verdict"] == base["verdict"]


def test_an_absent_shadow_reads_as_missing_with_a_warning_and_no_verdict_change(tmp_path):
    """No sample carried snowplow_deps_list_narrow (run 13's image had none):
    the report says ABSENT, never a silent null that reads as zero; a
    report-only warning goes to verdict.json and stderr (run606 logs it);
    checks, validity and verdict are unchanged."""
    mem = [m for m in _mem() if LN_KEY not in (m.get("extra") or {})]
    r = load606.list_narrow_report(mem, FRESH)
    assert r["status"] == "absent_in_all_samples" and r["mem_samples"] == 5
    assert "by_phase" not in r and "missing" in r["note"].lower()
    v = load606.evaluate([], FRESH, [], mem=mem)
    w = v["report"]["report_warnings"]
    assert len(w) == 1 and LN_KEY in w[0] and "missing, not zero" in w[0]
    present = load606.evaluate([], FRESH, [], mem=_mem())

    def strip(x):
        return [(c["check"], c["result"]) for c in x]
    assert strip(v["checks"]) == strip(present["checks"])
    assert strip(v["validity"]) == strip(present["validity"])
    assert v["verdict"] == present["verdict"]
    # the CLI prints it as a WARNING on stderr
    f = tmp_path / "mem-mem606-x.jsonl"
    f.write_text("".join(json.dumps(m) + "\n" for m in mem))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)],
                       capture_output=True, text=True, cwd=FIX.parent.parent, check=False)
    assert f"WARNING: {LN_KEY} absent in all" in p.stderr
    assert json.loads(p.stdout)["report"]["list_narrow_shadow_report_only"]["status"] == \
        "absent_in_all_samples"


def test_run13_real_mem_stream_reports_the_shadow_absent():
    """Run 13's REAL mem606 samples (snowplow 1.12.52, no
    snowplow_deps_list_narrow; tests/fixtures/mem_run13_samples.jsonl.gz =
    its mem_start and three full mem samples): the report says absent, with
    a warning, and the content-fill report still works from the same samples."""
    import gzip
    with gzip.open(FIX / "mem_run13_samples.jsonl.gz", "rt") as fh:
        mem = [json.loads(ln) for ln in fh]
    assert [m["kind"] for m in mem] == ["mem_start", "mem", "mem", "mem"]
    assert all(LN_KEY not in m["extra"] for m in mem[1:])
    r = load606.list_narrow_report(mem)
    assert r == {"status": "absent_in_all_samples", "mem_samples": 3, "note": r["note"]}
    v = load606.evaluate([], [], [], mem=mem)
    assert v["report"]["list_narrow_shadow_report_only"]["status"] == "absent_in_all_samples"
    assert v["report"]["report_warnings"] == [
        f"{LN_KEY} absent in all 3 mem samples: the #765 B shadow report is missing, not zero"]
    assert v["report"]["content_fill_by_gvr_report_only"]["total_by_gvr"]
