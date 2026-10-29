"""#969: the bench reads each arm's store-rebuild counters by the mechanism
that arm's build PUBLISHES (bench/store_rebuild.py), never by its version, and
reports the same quantities on both: rebuild windows, repairs, served-stale,
resolve fall-throughs to the apiserver; plus the paired run's three watches
(fall-throughs during a rebuild, watch submits right after it, heap and GC
CPU during it).

The 1.12.60 documents are RENDERED by #928's own expvar functions (5a6bb60c,
internal/cache -tags unit: tests/fixtures/store_swap_expvar_5a6bb60c.json);
the 1.12.5x ones are the recorded store-verification capture (#875,
tests/fixtures/store_verification_expvar_0934e202.json). They go through
mem606's real sampler (load606.mem_sample, default prefixes) and the report
through the evaluate CLI (`python -m bench.load606 evaluate`).
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench import figures, load606, store_rebuild
from tests.test_run14_list_narrow import _Vars

FIX = Path(__file__).resolve().parent / "fixtures"
SWAP_DOC = json.loads((FIX / "store_swap_expvar_5a6bb60c.json").read_text())
SV = json.loads((FIX / "store_verification_expvar_0934e202.json").read_text())
G = "composition.krateo.io/v1-2-2, Resource=githubscaffoldingwithcompositionpages"
T0, START = 1_800_000_000.0, 1_799_999_000.0        # the first sample; snowplow's container start


def _memstats(heap_mb, num_gc, frac, t):
    return {"HeapInuse": int(heap_mb * 1048576), "HeapAlloc": int(heap_mb * 1048576), "NumGC": num_gc,
            "GCCPUFraction": frac, "LastGC": int((t - 1) * 1e9), "PauseNs": [100_000] * 256,
            "PauseTotalNs": num_gc * 100_000}


def _common(repairs, stale, watch, fall, heap, num_gc, frac, t, pending=None):
    sv = copy.deepcopy(SV["after"])
    sv["snowplow_store_verification"]["store_repairs_fired_total"] = repairs
    if pending is None:                              # 1.12.60: the queue's gauges are deleted
        for k in ("store_repairs_pending", "store_repair_queue_age_seconds"):
            sv["snowplow_store_verification"].pop(k)
    else:
        sv["snowplow_store_verification"]["store_repairs_pending"] = pending
        sv["snowplow_store_verification"]["store_repair_queue_age_seconds"] = 4.0 if pending else 0
    return {**sv,
            "memstats": _memstats(heap, num_gc, frac, t),
            "snowplow_resolved_cache": {"entries": 1, "bytes": 1, "stale_served_total": stale,
                                        "known_stale_served_total": stale // 2},
            "snowplow_deps_dirty_mark_attribution": {"submit_source": {"watch": watch, "reconcile": 0}},
            "snowplow_apiserver_fallthrough_cells": {
                f"call-restactions|{G}|informer-fallthrough-not-synced": fall,
                f"call-widgets|{G}|informer-fallthrough-not-servable": fall // 4,
                "call-generic||secret-get": 7}}


def _swap(swaps, keys_open, window_s, m_keys, deliveries):
    d = copy.deepcopy(SWAP_DOC["after"])
    d["snowplow_store_swap"].update(store_swaps_total=swaps, store_swap_m_keys_total=m_keys,
                                    store_swap_deliveries_total=deliveries)
    ks = d["snowplow_store_swap_known_stale"]
    ks["store_swap_known_stale_window_seconds_total"]["divergence|swap"] = window_s
    ks["store_swap_known_stale_keys_total"]["divergence|swap"] = 3 * swaps
    ks["store_swap_known_stale_builds_total"]["divergence|swap"] = swaps
    ks["store_swap_known_stale_keys_open"]["divergence"] = keys_open
    ks["store_swap_known_stale_window_open_seconds_max"]["divergence"] = 9.0 if keys_open else 0
    return {k: v for k, v in d.items() if k.startswith("snowplow_store_swap")}


# One rebuild at the 4th sample of 8 (15 s apart). Per sample:
# (repairs, stale, watch, fall, heap_mb, num_gc, gc_fraction, rebuild open)
RELIST = [(0, 10, 100, 0, 2000, 10, 0.010, 0), (0, 12, 200, 0, 2010, 12, 0.010, 0),
          (0, 14, 300, 0, 2005, 14, 0.010, 0), (1, 30, 400, 40, 2600, 20, 0.012, 1),
          (1, 60, 500, 80, 3100, 30, 0.014, 1), (1, 62, 1500, 80, 2700, 33, 0.014, 0),
          (1, 64, 1600, 80, 2100, 35, 0.014, 0), (1, 66, 1700, 80, 2050, 37, 0.014, 0)]
# build_and_swap: requested at s3, open through s4, installed by s5
SWAP = [(0, 10, 100, 0, 2000, 10, 0.010, (0, 0, 0)), (0, 12, 200, 0, 2010, 12, 0.010, (0, 0, 0)),
        (0, 14, 300, 0, 2005, 14, 0.010, (0, 0, 0)), (1, 15, 400, 0, 2030, 15, 0.010, (0, 3, 0)),
        (1, 16, 500, 0, 2040, 16, 0.010, (0, 3, 0)), (1, 17, 610, 0, 2035, 17, 0.010, (1, 0, 20.5)),
        (1, 18, 700, 0, 2020, 18, 0.010, (1, 0, 20.5)), (1, 19, 800, 0, 2010, 19, 0.010, (1, 0, 20.5))]


def _doc(row, mech, t):
    repairs, stale, watch, fall, heap, num_gc, frac, extra = row
    if mech == "relist":
        return _common(repairs, stale, watch, fall, heap, num_gc, frac, t, pending=extra) | {
            "snowplow_crd_discovery": {"schema_relists_fired": 0, "relist_bridge_runs_total": 0}}
    swaps, keys_open, window_s = extra
    return _common(repairs, stale, watch, fall, heap, num_gc, frac, t) | _swap(swaps, keys_open, window_s,
                                                                                3 * swaps, 2 * swaps)


def _mem(rows, mech, extra=None):
    """mem606 samples of `rows`; extra(i) -> a dict merged into sample i's /debug/vars."""
    out = [{"kind": "mem_start", "t": T0 - 1}]
    for i, row in enumerate(rows):
        t = T0 + 15 * i
        doc = _doc(row, mech, t)
        for k, v in ((extra(i) if extra else None) or {}).items():
            doc[k] = {**doc.get(k, {}), **v} if isinstance(v, dict) and isinstance(doc.get(k), dict) else v
        rec = load606.mem_sample(_Vars(doc), "tok", tuple(load606.RUN8_VARS_PREFIXES))
        rec["t"] = t
        out.append(rec)
    return out


POD = {"kind": "snowplow_pod", "phase": "before", "pod": "snowplow-0", "restarts": 0, "ready": True,
       "started_at": START}


def _evaluate(tmp_path, rows, mech, *, churn=True, extra=None):
    """The verdict of the evaluate CLI over one run's mem606 stream."""
    f = tmp_path / f"mem-mem606-{mech}.jsonl"
    recs = _mem(rows, mech, extra)
    f.write_text("".join(json.dumps(m) + "\n" for m in recs))
    files = [str(f)]
    p2 = tmp_path / f"pod-{mech}.jsonl"
    p2.write_text(json.dumps(POD) + "\n")
    files.append(str(p2))
    if churn:
        c = tmp_path / f"fresh-{mech}.jsonl"
        c.write_text(json.dumps({"kind": "churn", "label": "list_dep@10", "t0": T0 + 40, "t1": T0 + 80,
                                 "target_rate": 10}) + "\n")
        files.append(str(c))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *files],
                       capture_output=True, text=True, cwd=FIX.parent.parent, check=False, timeout=300)
    v = json.loads(p.stdout)
    return v, p.stderr


# ─── which mechanism a snapshot publishes ──────────────────────────────────

def test_the_mechanism_is_what_the_build_publishes_never_its_version():
    relist = _doc(RELIST[0], "relist", T0)
    swap = _doc(SWAP[0], "build_and_swap", T0)
    assert store_rebuild.mechanism(relist)[0] == "relist"
    assert store_rebuild.mechanism(swap)[0] == "build_and_swap"
    # neither, and both: no answer (never a default)
    neither = {k: v for k, v in swap.items() if not k.startswith("snowplow_store_swap")}
    assert store_rebuild.mechanism(neither) == (None, store_rebuild.mechanism(neither)[1])
    assert "neither" in store_rebuild.mechanism(neither)[1]
    both = relist | _swap(0, 0, 0, 0, 0)
    assert store_rebuild.mechanism(both)[0] is None and "both" in store_rebuild.mechanism(both)[1]
    # no version string is read
    assert "snowplow_build_info" not in json.dumps(store_rebuild.MECHANISMS)


def test_the_1_12_60_rendering_carries_what_this_module_reads():
    """The rendered fixture IS #928's shape: every counter this module reads
    exists, the deleted ones do not (#969's table)."""
    a = SWAP_DOC["after"]
    for name, fam, stat, label in store_rebuild.COUNTERS["build_and_swap"]:
        assert store_rebuild.read(a, fam, stat, label) is not None, name
    for name, fam, stat, label in store_rebuild.GAUGES["build_and_swap"] + store_rebuild.EXTRA_GAUGES["build_and_swap"]:
        assert store_rebuild.read(a, fam, stat, label) is not None, name
    sv = a["snowplow_store_verification"]
    assert "store_repairs_pending" not in sv and "store_repair_queue_age_seconds" not in sv
    assert "store_repairs_fired_total" in sv
    crd = a.get("snowplow_crd_discovery") or {}
    assert not any(k.startswith("relist_bridge") or k.startswith("relist_") for k in crd), sorted(crd)


def test_mem606_captures_every_family_the_report_reads():
    assert all(any(q.startswith(p) for p in load606.RUN8_VARS_PREFIXES) for q in store_rebuild.VARS_PREFIXES)
    rec = _mem(SWAP[:1], "build_and_swap")[1]
    assert {"snowplow_store_swap", "snowplow_store_swap_known_stale", "snowplow_apiserver_fallthrough_cells",
            "snowplow_store_verification", "snowplow_resolved_cache",
            "snowplow_deps_dirty_mark_attribution"} <= set(rec["extra"])
    assert rec["last_gc"] == int((T0 - 1) * 1e9)


# ─── the report, through the evaluate CLI ───────────────────────────────────

def test_both_arms_report_the_same_quantities(tmp_path):
    va, _ = _evaluate(tmp_path, RELIST, "relist")
    vb, _ = _evaluate(tmp_path, SWAP, "build_and_swap")
    ra, rb = (v["report"]["store_rebuild_report_only"] for v in (va, vb))
    assert (ra["mechanism"], rb["mechanism"]) == ("relist", "build_and_swap")
    fa, fb = (v["report"]["acceptance_figures_report_only"]["store_rebuilds"] for v in (va, vb))
    assert set(fa) == set(fb) and len(fa) >= 9                         # the same paths on both arms
    # the pair's B/A ratios cover them
    rat = figures.ratios(va, vb)["ratios"]
    assert "store_rebuilds/rebuild_pairs" in rat and "store_rebuilds/heap_inuse_mb_peak_during_rebuilds" in rat
    # a quantity one mechanism does not publish is listed, never a zero
    assert "rebuilds_installed" in ra["not_published"]
    assert all("rebuilds_installed" not in p for p in ra["by_phase"].values())
    assert rb["by_phase"]["all"]["rebuilds_installed"] == 1


def test_the_relist_arm(tmp_path):
    v, _ = _evaluate(tmp_path, RELIST, "relist")
    r = v["report"]["store_rebuild_report_only"]
    assert r["status"] == "present" and r["identified_by"].startswith("publishes ")
    # rebuild pairs: requested in s2->s3, open s3, s4 (pending 1): pairs 2-3, 3-4, 4-5
    assert r["rebuild_pairs"] == 3 and r["rebuild_s_bounded"] == 45.0
    w = r["watch"]
    f = w["resolve_fallthroughs_during_rebuilds"]
    assert f["value"] == 80 + 20 and f["by_reason"] == {"informer-fallthrough-not-synced": 80,
                                                       "informer-fallthrough-not-servable": 20}
    assert f["claim"] is None and r["claim_violations"] == []
    assert f["by_gvr_reason"] == {f"{G}|informer-fallthrough-not-synced": 80,
                                  f"{G}|informer-fallthrough-not-servable": 20}
    # the watch submits in the pair the rebuild ended in (s4->s5: +1000) and the next one (+100),
    # each against the quiet median of ITS OWN phase: the end pair is in the churn phase, which has
    # no quiet pair (no ratio); the next one is steady (+100 per 15 s)
    a = w["watch_submits_after_rebuild"]
    assert a["quiet_median_per_s_by_phase"] == {"steady": round(100 / 15, 3)}
    [x] = a["after_each_rebuild"]
    assert (x["end_pair"]["phase"], x["end_pair"]["watch_submits_per_s"], x["end_pair"]["over_quiet_median"]) == \
        ("churn:list_dep@10", round(1000 / 15, 3), None)
    assert (x["next_pair"]["phase"], x["next_pair"]["watch_submits_per_s"], x["next_pair"]["over_quiet_median"]) == \
        ("steady", round(100 / 15, 3), 1.0)
    assert a["max_over_quiet_median"] == 1.0
    h = w["heap_and_gc_during_rebuilds"]
    assert h["heap_inuse_mb_peak"] == 3100.0 and h["heap_inuse_mb_before_first"] == 2005.0
    assert h["gc_cpu_share_max"] is not None and h["gc_cpu_share_max"] > h["gc_cpu_share_quiet_p50"]
    assert r["gauges_last"] == {"store_repairs_pending": 0, "store_repair_queue_age_seconds": 0}
    # the churn phase holds the rebuild (T0+40 .. T0+80)
    assert r["by_phase"]["churn:list_dep@10"]["rebuild_pairs"] >= 1


def test_the_build_and_swap_arm(tmp_path):
    v, err = _evaluate(tmp_path, SWAP, "build_and_swap")
    r = v["report"]["store_rebuild_report_only"]
    assert r["status"] == "present" and r["mechanism"] == "build_and_swap"
    assert r["rebuild_pairs"] == 3                                     # s2->s3, s3->s4, s4->s5
    f = r["watch"]["resolve_fallthroughs_during_rebuilds"]
    assert f["value"] == 0 and f["claim"] == 0 and r["claim_violations"] == []
    a = r["watch"]["watch_submits_after_rebuild"]
    # the install pair (s4->s5, +110, churn: no quiet pair there) and the next one (+90, steady)
    assert [(x["end_pair"]["watch_submits_per_s"], x["next_pair"]["watch_submits_per_s"])
            for x in a["after_each_rebuild"]] == [(round(110 / 15, 3), round(90 / 15, 3))]
    assert a["max_over_quiet_median"] == 0.9
    p = r["by_phase"]["all"]
    assert (p["rebuilds_installed"], p["m_keys"], p["deliveries"]) == (1, 3, 2)
    assert p["known_stale_window_s"] == {"divergence|swap": 20.5}
    assert p["known_stale_keys"] == {"divergence|swap": 3}
    assert r["gauges_last"]["store_swap_known_stale_keys_open.divergence"] == 0
    assert "store_repairs_pending" in r["not_published"]
    # the build the rendering accounted through the production path (abort_clean, 2 keys, 3 s)
    assert SWAP_DOC["after"]["snowplow_store_swap_known_stale"]["store_swap_known_stale_builds_total"][
        "divergence|abort_clean"] == 1
    assert "store rebuilds" not in err


def test_one_fall_through_during_a_swap_already_breaks_its_claim(tmp_path):
    """The claim's boundary, exactly: a single fall-through is a breach."""
    rows = [list(x) for x in SWAP]
    for i in (3, 4, 5, 6, 7):
        rows[i][3] = 1                                                 # 1 not-synced, 0 not-servable at s3
    v, err = _evaluate(tmp_path, [tuple(x) for x in rows], "build_and_swap")
    r = v["report"]["store_rebuild_report_only"]
    assert r["watch"]["resolve_fallthroughs_during_rebuilds"]["value"] == 1
    assert len(r["claim_violations"]) == 1 and "1 resolve fall-through(s)" in r["claim_violations"][0]


def test_with_no_rebuild_the_claim_is_untested_never_a_zero(tmp_path):
    """#987 review F1: a run with no rebuild watched nothing: no value, no
    figure, and a warning that the claim is untested."""
    quiet = [SWAP[0]] * 8
    v, err = _evaluate(tmp_path, quiet, "build_and_swap")
    r = v["report"]["store_rebuild_report_only"]
    f = r["watch"]["resolve_fallthroughs_during_rebuilds"]
    assert r["rebuild_pairs"] == 0
    assert f["value"] is None and f["by_reason"] is None and "untested" in f["missing"]
    assert r["claim_violations"] == [] and "untested" in r["warnings"][0]
    assert r["warnings"][0] in v["report"]["report_warnings"]
    assert v["report"]["acceptance_figures_report_only"]["store_rebuilds"][
        "resolve_fallthroughs_during_rebuilds"] is None


def test_a_schema_relist_is_a_rebuild_on_the_relist_arm(tmp_path):
    """#987 review F5: snowplow_crd_discovery is captured; a schema relist
    with no repair is a rebuild pair (as a schema swap is on build_and_swap)."""
    mem = _mem([r[:7] + (0,) for r in RELIST[:3]] * 1, "relist")
    for i, m in enumerate(mem[1:]):
        m["extra"]["snowplow_crd_discovery"]["schema_relists_fired"] = 1 if i >= 2 else 0
        m["extra"]["snowplow_store_verification"]["store_repairs_fired_total"] = 0
    r = store_rebuild.report(mem, [POD], [], load606._phase_of, load606._deltas)
    assert r["rebuild_pairs"] == 1 and r["by_phase"]["all"]["schema_changes"] == 1


def test_what_one_mechanism_reports_the_other_lists_as_not_published(tmp_path):
    """#987 review F4: every quantity the swap arm reports is either reported
    or listed as not published by the relist arm, and the reverse."""
    reps = {m: store_rebuild.report(_mem(rows, m), [POD], [], load606._phase_of, load606._deltas)
            for m, rows in (("relist", RELIST), ("build_and_swap", SWAP))}

    def names(r):
        return set(r["by_phase"]["all"]) | set(r["gauges_last"]) | {k for k in r if k.endswith("_to_push_max_ms")}
    for m, o in (("relist", "build_and_swap"), ("build_and_swap", "relist")):
        missing = names(reps[o]) - names(reps[m]) - set(reps[m]["not_published"])
        assert not missing, (m, missing)
    assert {"superseded", "reused_objects", "schema_unowned", "known_stale_builds", "detect_to_push_max_ms",
            "install_to_push_max_ms", "store_swap_ledger_owed.divergence"} <= set(reps["relist"]["not_published"])


def test_a_fall_through_during_a_swap_breaks_its_claim(tmp_path):
    rows = [list(x) for x in SWAP]
    for i in (3, 4, 5, 6, 7):
        rows[i][3] = 12                                                # 12 not-synced, 3 not-servable at s3
    v, err = _evaluate(tmp_path, [tuple(x) for x in rows], "build_and_swap")
    r = v["report"]["store_rebuild_report_only"]
    assert r["watch"]["resolve_fallthroughs_during_rebuilds"]["value"] == 15
    assert len(r["claim_violations"]) == 1 and "claims 0" in r["claim_violations"][0]
    assert r["claim_violations"][0] in v["report"]["report_warnings"]
    assert "WARNING: store rebuilds (build_and_swap): 15 resolve fall-through(s)" in err


def test_no_mechanism_is_absent_and_a_mixed_stream_is_invalid(tmp_path):
    mem = _mem(SWAP[:3], "build_and_swap")
    for m in mem[1:]:
        m["extra"] = {k: v for k, v in m["extra"].items()
                      if not k.startswith("snowplow_store_swap") and k != "snowplow_store_verification"}
    r = store_rebuild.report(mem, [POD], [], load606._phase_of, load606._deltas)
    assert r["status"] == "absent" and "not a zero" in r["note"] and "by_phase" not in r
    mixed = _mem(RELIST[:3], "relist") + _mem(SWAP[3:5], "build_and_swap")[1:]
    r = store_rebuild.report(mixed, [POD], [], load606._phase_of, load606._deltas)
    assert r["status"] == "invalid" and set(r["samples_by_mechanism"]) == {"relist", "build_and_swap"}


def test_a_counter_the_build_stops_publishing_is_missing_not_zero():
    a = _doc(SWAP[3], "build_and_swap", T0)
    del a["snowplow_store_swap_known_stale"]["store_swap_known_stale_window_seconds_total"]
    c = store_rebuild.counters(a, "build_and_swap")
    assert c["window_seconds/divergence|swap"] is None
    del a["snowplow_apiserver_fallthrough_cells"]
    c = store_rebuild.counters(a, "build_and_swap")
    assert not any(k.startswith("fallthrough") for k in c)               # no meter: no zero


def test_gc_share_is_the_windowed_fraction():
    # 10 s of uptime at 1 %, then 20 s at 2 % overall: the 10 s between used 3 %
    a = {"t": START + 11, "gc_cpu_fraction": 0.01, "last_gc": int((START + 10) * 1e9)}
    b = {"t": START + 21, "gc_cpu_fraction": 0.02, "last_gc": int((START + 20) * 1e9)}
    assert store_rebuild.gc_share(a, b, [POD]) == 0.03
    assert store_rebuild.gc_share(a, b, []) is None                      # no container start: not derivable
    assert store_rebuild.gc_share(a, dict(b, last_gc=a["last_gc"]), [POD]) is None   # no GC in between


def test_store_verification_gauges_follow_the_mechanism():
    v = load606.store_verification_report(_mem(SWAP, "build_and_swap"))
    assert v["mechanism"] == "build_and_swap"
    assert set(v["gauges_last"]) == {"store_gvrs_unverified", "store_gvrs_unverifiable",
                                     "store_swap_known_stale_keys_open.divergence",
                                     "store_swap_known_stale_window_open_seconds_max.divergence",
                                     "store_swap_ledger_owed.divergence"}
    assert "builds REQUESTED" in v["note"] and "INSTALLED" in v["note"]
    v = load606.store_verification_report(_mem(RELIST, "relist"))
    assert v["mechanism"] == "relist" and v["gauges_last"]["store_repairs_pending"] == 0
    assert "relists" in v["note"]


@pytest.mark.parametrize("mech,rows", [("relist", RELIST), ("build_and_swap", SWAP)])
def test_a_restart_pair_is_skipped_not_counted(mech, rows):
    """The restart pair (s3->s4) is left out: its absolute values are never
    summed in as deltas (#987 review M13), so the totals are those of the
    other pairs, exactly."""
    mem = _mem(rows, mech)
    mem[5]["num_gc"] = 1                                               # snowplow restarted before s4
    r = store_rebuild.report(mem, [POD], [], load606._phase_of, load606._deltas)
    assert r["restarts_skipped"] == 1
    a = r["by_phase"]["all"]
    stale = (rows[3][1] - rows[0][1]) + (rows[7][1] - rows[4][1])        # s0..s3 and s4..s7
    watch = (rows[3][2] - rows[0][2]) + (rows[7][2] - rows[4][2])
    assert (a["repairs_requested"], a["stale_served"], a["watch_submits"], a["pairs"]) == (1, stale, watch, 6)
    assert r["rebuild_pairs"] == 2                                     # s2->s3 and s4->s5


# ─── #988 (PR #991): the resolves' collection LISTs on the wire ─────────────

LISTS = "snowplow_resolve_collection_lists"
CM = "/v1, Resource=configmaps"


def _lists(composition_at, other_at=None, refresher_from=None):
    """A build with #991: the lists counter (G's LISTs from sample `composition_at`
    on, a configmap LIST from `other_at` on) and the refresher's fall-through path."""
    def f(i):
        cells = {f"{G}|informer-fallthrough-not-synced": 1 if composition_at is not None and i >= composition_at else 0}
        if other_at is not None and i >= other_at:
            cells[f"{CM}|unattributed"] = 1
        out = {LISTS: cells}
        if refresher_from is not None:
            out["snowplow_apiserver_fallthrough_cells"] = {
                f"refresher|{G}|informer-fallthrough-not-synced": 5 if i >= refresher_from else 0}
        return out
    return f


def test_the_lists_counter_is_captured_and_reads_zero_through_a_clean_swap(tmp_path):
    v, err = _evaluate(tmp_path, SWAP, "build_and_swap", extra=_lists(None))
    r = v["report"]["store_rebuild_report_only"]
    w = r["watch"]["resolve_collection_lists_during_rebuilds"]
    assert (w["value"], w["claim"], w["by_gvr"]) == (0, 0, {}) and r["claim_violations"] == []
    assert "resolve_collection_lists" not in r["not_published"]
    assert v["report"]["acceptance_figures_report_only"]["store_rebuilds"][
        "resolve_collection_lists_during_rebuilds"] == 0


def test_a_list_during_or_right_after_a_swap_breaks_870s_claim(tmp_path):
    """During (s3) and right after (s5->s6, the pair after the install): each
    is a breach, named by GVR; a list long after (s7) is not counted."""
    for at, want in ((3, {G: 1}), (6, {G: 1})):
        v, err = _evaluate(tmp_path, SWAP, "build_and_swap", extra=_lists(at))
        w = v["report"]["store_rebuild_report_only"]["watch"]["resolve_collection_lists_during_rebuilds"]
        assert w["value"] == 1 and w["by_gvr"] == want, at
        viol = v["report"]["store_rebuild_report_only"]["claim_violations"]
        assert len(viol) == 1 and "1 resolve collection LIST(s)" in viol[0] and G in viol[0]
        assert viol[0] in v["report"]["report_warnings"]
    v, _ = _evaluate(tmp_path, SWAP, "build_and_swap", extra=_lists(7))
    assert v["report"]["store_rebuild_report_only"]["watch"]["resolve_collection_lists_during_rebuilds"]["value"] == 0
    # every GVR is listed: the bench cannot single out the swapped one
    v, _ = _evaluate(tmp_path, SWAP, "build_and_swap", extra=_lists(None, other_at=4))
    w = v["report"]["store_rebuild_report_only"]["watch"]["resolve_collection_lists_during_rebuilds"]
    assert w["by_gvr"] == {CM: 1} and v["report"]["store_rebuild_report_only"]["claim_violations"]


def test_a_build_without_the_counter_reads_not_published(tmp_path):
    v, _ = _evaluate(tmp_path, RELIST, "relist")
    r = v["report"]["store_rebuild_report_only"]
    w = r["watch"]["resolve_collection_lists_during_rebuilds"]
    assert w["value"] is None and w["missing"] == "not published by this build" and w["claim"] is None
    assert {"resolve_collection_lists", "refresher_fallthroughs"} <= set(r["not_published"])
    assert v["report"]["acceptance_figures_report_only"]["store_rebuilds"][
        "resolve_collection_lists_during_rebuilds"] is None


def test_the_refresher_s_fall_throughs_never_join_the_resolve_count(tmp_path):
    """#991 counts the refresher's fall-throughs under path "refresher": kept
    apart, so resolve_fallthroughs means the same on 1.12.59 and 1.12.60."""
    v, _ = _evaluate(tmp_path, SWAP, "build_and_swap", extra=_lists(None, refresher_from=4))
    r = v["report"]["store_rebuild_report_only"]
    assert r["watch"]["resolve_fallthroughs_during_rebuilds"]["value"] == 0
    assert r["watch"]["resolve_collection_lists_during_rebuilds"]["refresher_fallthroughs_during_rebuilds"] == {
        "informer-fallthrough-not-synced": 5, "informer-fallthrough-not-servable": 0}
