"""#969: accept1126's gated CRD stage reads the rebuild mechanism the build
PUBLISHES. On 1.12.60 (#870) the relist bridge, its re-fire and their counters
are deleted: B2 asked for relist_bridge_runs_total >= 1 and could not pass.
Now the rows follow the mechanism: relist (to 1.12.59, unchanged) or
build-and-swap (#969's table). The 1.12.60 snapshot is RENDERED by #928's own
expvar functions (tests/fixtures/store_swap_expvar_5a6bb60c.json)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from bench import accept1126 as a

FIX = Path(__file__).resolve().parent / "fixtures"
R = json.loads((FIX / "store_swap_expvar_5a6bb60c.json").read_text())
KS = a.K_SWAP_KS


def _swap_snapshot(swaps=0, m_keys=0, deliveries=0, relists=0, type_event=None, keys_open=0, open_max=0.0,
                   abort_teardown=0, abort_refused=0, divergence_builds=0, heap_mb=None):
    s = copy.deepcopy(R["after"])
    s["memstats"] = {"HeapInuse": int((heap_mb or 2000) * 1048576), "NumGC": 10, "PauseTotalNs": 1_000_000,
                     "GCCPUFraction": 0.01}
    s[a.K_SWAP].update(store_swaps_total=swaps, store_swap_m_keys_total=m_keys,
                       store_swap_deliveries_total=deliveries)
    s[a.K_CRD]["schema_relists_fired"] = relists
    if type_event is not None:
        s[a.K_LIST_INCREMENTAL]["unanchors"] = {"type_event": type_event}
    k = s[KS]
    k["store_swap_known_stale_keys_open"]["schema"] = keys_open
    k["store_swap_known_stale_window_open_seconds_max"]["schema"] = open_max
    k["store_swap_known_stale_builds_total"]["schema|abort_teardown"] = abort_teardown
    k["store_swap_known_stale_builds_total"]["schema|abort_refused"] = abort_refused
    k["store_swap_known_stale_builds_total"]["divergence|swap"] = divergence_builds
    k["store_swap_known_stale_window_seconds_total"]["schema|swap"] = 2.5 * swaps
    k["store_swap_known_stale_keys_total"]["schema|swap"] = m_keys
    return s


def _relist_snapshot(**v):
    s = copy.deepcopy(R["after"])
    for key in [k for k in s if k.startswith("snowplow_store_swap")]:
        del s[key]
    s[a.K_CRD] |= {"relist_bridge_runs_total": 0, "relist_bridge_enqueued_total": 0,
                   "relist_bridge_timeout_total": 0, "relist_bridge_aborted_total": 0,
                   "relist_dirtymark_postsync_total": 0, "relist_postsync_timeout_total": 0} | v
    return s


def test_the_stage_reads_the_mechanism_the_build_publishes():
    assert a.crd_mechanism(_swap_snapshot()) == "build_and_swap"
    assert a.crd_mechanism(_relist_snapshot()) == "relist"
    both = _relist_snapshot() | {a.K_SWAP: R["after"][a.K_SWAP]}
    with pytest.raises(a.AssertionsFailed, match="both"):
        a.crd_mechanism(both)
    neither = {k: v for k, v in _swap_snapshot().items() if not k.startswith("snowplow_store_swap")}
    with pytest.raises(a.AssertionsFailed, match="neither"):
        a.crd_mechanism(neither)


def test_the_1_12_60_snapshot_has_no_relist_counter_and_the_rows_need_none():
    s = _swap_snapshot()
    with pytest.raises(a.AssertionsFailed):
        a.snapshot_crd_counters(s, "relist")                 # B2's old key is deleted on 1.12.60
    c = a.snapshot_crd_counters(s, "build_and_swap")
    assert not any("relist_bridge" in k for k in c)
    keys = {r.key for r in a.crd_rows("build_and_swap")}
    assert not any("relist" in (k or "") and "schema_relists" not in k for k in keys)


def test_a_closed_label_must_be_published_an_open_one_reads_zero():
    s = _swap_snapshot()
    assert a.snapshot_crd_counters(s, "build_and_swap")[f"{a.K_LIST_INCREMENTAL}.unanchors[type_event]"] == 0
    del s[KS]["store_swap_known_stale_builds_total"]["schema|abort_refused"]
    with pytest.raises(a.AssertionsFailed, match="absent"):
        a.snapshot_crd_counters(s, "build_and_swap")
    s = _swap_snapshot()
    del s[a.K_LIST_INCREMENTAL]
    with pytest.raises(a.AssertionsFailed, match="not a zero"):
        a.snapshot_crd_counters(s, "build_and_swap")


def _run(before, after, changed=0):
    mech = a.crd_mechanism(before)
    d = a.delta(a.snapshot_crd_counters(before, mech), a.snapshot_crd_counters(after, mech))
    end = a.crd_end_readings(after, mech)
    ok, res = a.evaluate(a.crd_rows(mech, changed), d | end)
    return ok, {x["id"]: x for x in res}, d, end


def test_b1s_widen_changes_no_object_and_its_swap_delivers_none():
    """B1's widen (no default) changes no object's served bytes: an EMPTY M is
    the correct behaviour, and B3 expects exactly that."""
    assert a.widen_changed_objects([dict(a.CR_SPEC)], None) == 0
    ok, rows, d, end = _run(_swap_snapshot(), _swap_snapshot(swaps=1, relists=1))
    assert ok, [r for r in rows.values() if not r["passed"]]
    rep = a.crd_swap_report(d, end)
    assert rep["known_stale_window_s"] == {"schema|swap": 2.5} and rep["known_stale_keys"] == {}


def test_the_full_widen_reports_b3_exactly_but_does_not_gate_on_it():
    """#987 review F3: a CRD update ends the live watches; the live informer
    may re-list and deliver the changed objects first, which #928 leaves out
    of M and of deliveries and does not count. The full widen's B3 is
    therefore REPORTED (exact, with the reason) but gates nothing, until a
    cluster run shows the live copy does not re-list. B1's (0 changed) gates."""
    n = a.widen_changed_objects([dict(a.CR_SPEC) for _ in range(7)], "widened")
    assert n == 7
    ok, rows, _, _ = _run(_swap_snapshot(), _swap_snapshot(swaps=1, m_keys=7, deliveries=7, relists=1), changed=n)
    assert ok and rows["B3"]["passed"] and rows["B3"]["report_only"] is True
    assert "re-list" in rows["B3"]["why"]
    for m_keys, deliveries in ((7, 6), (6, 6), (50, 50)):           # one missed; M short; M = some other store
        ok, rows, _, _ = _run(_swap_snapshot(),
                              _swap_snapshot(swaps=1, m_keys=m_keys, deliveries=deliveries, relists=1), changed=n)
        assert ok and not rows["B3"]["passed"] and rows["B3"]["report_only"] is True
    # B1 (nothing changed): B3 gates, exactly
    ok, rows, _, _ = _run(_swap_snapshot(), _swap_snapshot(swaps=1, m_keys=1, deliveries=1, relists=1), changed=0)
    assert not ok and not rows["B3"]["passed"] and "report_only" not in rows["B3"]


@pytest.mark.parametrize("after,row", [
    (dict(swaps=0, relists=1), "B2"),                                         # nothing installed
    (dict(swaps=1, m_keys=1, deliveries=1, relists=1), "B3"),                 # delivered what the widen did not change
    (dict(swaps=1, m_keys=1, relists=1), "B3"),                               # M not empty
    (dict(swaps=1, relists=1, divergence_builds=1), "B3"),                   # not attributable: a divergence build
    (dict(swaps=1, relists=1, type_event=1), "B4"),                          # a whole-type mark at the swap
    (dict(swaps=1, relists=1, keys_open=1), "B4"),                           # the window did not end
    (dict(swaps=1, relists=1, abort_teardown=1), "B5"),
    (dict(swaps=1, relists=1, abort_refused=1), "B7"),
    (dict(swaps=1, relists=1, open_max=4.0), "B7"),
    (dict(swaps=1, relists=0), "B1"),
])
def test_each_swap_row_fails_on_its_own_defect(after, row):
    ok, rows, _, _ = _run(_swap_snapshot(), _swap_snapshot(**after))
    assert not ok and [r for r, x in rows.items() if not x["passed"]] == [row]


def test_the_relist_rows_are_unchanged_for_a_1_12_59_build():
    rows = {r.rid: r for r in a.crd_rows("relist")}
    assert rows["B2"].key == f"{a.K_CRD}.relist_bridge_runs_total"
    assert rows["B4"].key == f"{a.K_CRD}.relist_bridge_timeout_total"
    ok, res, _, _ = _run(_relist_snapshot(), _relist_snapshot(relist_bridge_runs_total=1, schema_relists_fired=1))
    assert ok
    ok, res, _, _ = _run(_relist_snapshot(), _relist_snapshot(relist_bridge_runs_total=1, schema_relists_fired=1,
                                                              relist_bridge_timeout_total=1))
    assert not ok and not res["B4"]["passed"]


def test_b1s_widen_is_unchanged_and_only_the_full_widen_carries_a_default():
    """B1 keeps measuring what it measured (a widen with no default: no
    object's bytes change); the full widen is its own stage."""
    narrow = a._crd_manifest("harness.krateo.io", "s6runs", "S6run", widened=False)
    wide = a._crd_manifest("harness.krateo.io", "s6runs", "S6run", widened=True)
    full = a._crd_manifest("harness.krateo.io", "s6runs", "S6run", widened=True, default="widened")
    assert "default" not in narrow and "default" not in wide
    assert "widened:\n                    type: string\n" in wide
    assert full == wide + "                    default: widened\n"
    assert a.widen_changed_objects([{"note": "x", "widened": "set"}], "widened") == 0   # it sets the field


def _stage(monkeypatch, tmp_path, snaps, fn, applied=None, **ctx):
    snaps = list(snaps)
    seq = iter(snaps[:1] + snaps[1:-1] + [snaps[-1]] * (3 - len(snaps) + 2))
    monkeypatch.setattr(a, "SETTLE_SECONDS", 3)
    monkeypatch.setattr(a, "read_debug_vars", lambda *x, **k: next(seq))
    monkeypatch.setattr(a, "_kubectl_apply", lambda m, what: (applied if applied is not None else []).append(m))
    monkeypatch.setattr(a, "wait_hook", lambda *x, **k: None)
    monkeypatch.setattr(a, "announce", lambda *x, **k: None)
    monkeypatch.setattr(a, "_teardown_crd", lambda objs: None)
    monkeypatch.setattr(a.time, "sleep", lambda s: None)
    return fn({"run_dir": tmp_path, "portal_base": "https://p", "reqlog": a._RequestLog(), "run_id": "r",
               "token": "t", "crd_name": "s6runs.harness.krateo.io",
               "full_widen_crd_name": "s6wides.harness.krateo.io", **ctx})


def test_the_stage_end_to_end_on_build_and_swap(monkeypatch, tmp_path):
    monkeypatch.setenv("S6_CRD_RELIST_APPROVED", "1")
    applied = []
    out = _stage(monkeypatch, tmp_path, [_swap_snapshot(), _swap_snapshot(swaps=1, relists=1, heap_mb=2300),
                                         _swap_snapshot(swaps=1, relists=1, heap_mb=2100)],
                 a.stage_crd_relist, applied, enable_crd_relist=True)
    assert out["__passed__"] and out["mechanism"] == "build_and_swap" and out["failed_rows"] == []
    assert (out["crs"], out["widen_default"], out["objects_changed_by_the_widen"]) == (1, None, 0)
    assert out["report_only"]["known_stale_window_s"] == {"schema|swap": 2.5}
    assert out["memory_report_only"]["heap_inuse_mb_peak"] == 2300.0
    assert not any("default:" in m for m in applied)                  # B1's widen is unchanged


def test_the_full_widen_stage_end_to_end(monkeypatch, tmp_path):
    monkeypatch.setenv("S6_CRD_FULL_WIDEN_APPROVED", "1")
    applied = []
    out = _stage(monkeypatch, tmp_path, [_swap_snapshot(), _swap_snapshot(swaps=1, m_keys=5, deliveries=5,
                                                                          relists=1, heap_mb=2600)],
                 a.stage_crd_full_widen, applied, enable_crd_full_widen=True, full_widen_crs=5)
    assert out["__passed__"], out["assertions"]
    assert (out["crs"], out["widen_default"], out["objects_changed_by_the_widen"]) == (5, "widened", 5)
    assert sum(1 for m in applied if m.startswith("apiVersion: harness.krateo.io/v1alpha1")) == 5   # the CRs
    assert any("default: widened" in m for m in applied)
    assert out["memory_report_only"]["heap_inuse_mb_peak"] == 2600.0


def test_the_full_widen_stage_is_gated_and_needs_its_count(monkeypatch, tmp_path):
    with pytest.raises(a.PreflightFailed, match="approved-gated"):
        a.stage_crd_full_widen({"enable_crd_full_widen": True})
    monkeypatch.setenv("S6_CRD_FULL_WIDEN_APPROVED", "1")
    with pytest.raises(a.PreflightFailed, match="--full-widen-crs"):
        a.stage_crd_full_widen({"enable_crd_full_widen": True})


def test_each_layer_of_the_full_widen_gate_holds_on_its_own(monkeypatch):
    """#987 review M23 / M24: the stage's own flag check, with the approval
    and the count both given, refuses by itself (a plain RuntimeError, not the
    PreflightFailed the other layers raise); and the stage list leaves B2 out
    without the flag, whatever the approval says."""
    monkeypatch.setenv("S6_CRD_FULL_WIDEN_APPROVED", "1")
    with pytest.raises(RuntimeError) as e:
        a.stage_crd_full_widen({"full_widen_crs": 5, "full_widen_crd_name": "s6wides.harness.krateo.io"})
    assert type(e.value) is RuntimeError and "--enable-crd-full-widen" in str(e.value)
    ids = [x[0] for x in a.stage_list(enable_crd_relist=True)]
    assert "B2_crd_full_widen" not in ids and "B1_crd_relist" in ids
    assert "B2_crd_full_widen" in [x[0] for x in a.stage_list(enable_crd_full_widen=True)]
    assert "B1_crd_relist" not in [x[0] for x in a.stage_list(enable_crd_full_widen=True)]
    import inspect
    assert "stage_list(enable_crd_relist=enable_crd_relist, enable_crd_full_widen=enable_crd_full_widen)" in \
        inspect.getsource(a.run)                                   # run() takes its stages from it


def test_the_fixture_count_is_derived_from_the_manifest_the_stage_applies():
    """#987 review M29: the CR the stage applies carries exactly CR_SPEC, so
    widen_changed_objects counts what is applied, never a restated literal."""
    import yaml
    applied = yaml.safe_load(a._cr_manifest("harness.krateo.io", "S6run", "s6-probe-s6runs"))
    assert applied["spec"] == a.CR_SPEC
    assert a.widen_changed_objects([applied["spec"]], "widened") == 1


def test_a_pod_replaced_by_another_mechanism_mid_stage_is_refused(monkeypatch, tmp_path):
    monkeypatch.setenv("S6_CRD_RELIST_APPROVED", "1")
    with pytest.raises(a.AssertionsFailed, match="mechanism changed"):
        _stage(monkeypatch, tmp_path, [_relist_snapshot(), _swap_snapshot(swaps=1, relists=1)],
               a.stage_crd_relist, enable_crd_relist=True)
