"""#1004 — #870's full-widen worst case as its own stage (bench/full_widen.py,
run606.sh s_full_widen, run606_pair.sh BENCH_1004_GO_A / _B).

Rules, each with an arm (and a logic-only mutation shown RED, see the PR):
  R1  build_and_swap ends ONLY when the stage GVR's own outcome moved SINCE
      ARMED and both [schema] gauges are 0 (ruling Q1): an outcome before
      armed, or gauges at 0 before the CRD event, never end it; an open
      window never ends it.
  R2  relist ends ONLY when the post-sync re-fire caught up AND the stage's
      read serves again (ruling Q2).
  R3  the widen waits for a read that serves: registered, 200, no fall-through
      (ruling Q4); a GVR that never serves ends the Job unarmed.
  R4  every phase is bounded, recorded with its source; the settle's clock
      starts at the widen; "bound reached" is an outcome, never a pass (Q3).
  R5  the bounds: relist = max(relistPostSyncWait, the live
      DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS as the snowplow container sees it
      (envFrom included), else 120) + one sample; build_and_swap and register
      = ROLLOUT_TIMEOUT_S.
  R6  inputs from what the run installs: N = the data step's proof (= SCALE),
      bodies = the compositions', batching = the data step's.
  R7  the stage runs after the evaluate step and "run complete", before
      teardown, only with BENCH_1004_GO for this run; every bounded wait is a
      term of its budget; every apply goes through kapply (the inventory);
      its cleanup deletes the CRD; the bodies, the password file and the
      password in its manifests never outlive the run (signal, failure).
  R8  the pair: BENCH_1004_GO_A and _B, both or neither, each a go for its run.
  R9  attribution (Q2, #1050 F7): from the cluster, never snowplow's log (it
      runs at LOG_LEVEL warn): the stage's CRD is the only one whose
      generation moved in the window, and it moved once; else refused.
  R10 the record's baseline is the armed sample: register-phase counters and
      heap never reach the rows or the memory report; detection is the first
      sample whose relists moved.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from bench import data_manifests, full_widen as fw
from bench.load606 import SCALEUSER_PASSWORD
from tests.test_run606_real_sequence import BENCH_DIR, RUN_NAME, fake_registry, harness  # noqa: F401
from tests.test_run15_customer_latency import NA, NB, _launch, _calls, _verdicts, pairbed, va  # noqa: F401

FAKE_DIGEST = "sha256:" + "ab" * 32
MB = 1048576
T0 = 1_800_000_000.0


def _iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int((t % 1) * 1e6):06d}Z"


# ─── a fake snowplow: the families a build publishes, by mechanism ───────────

class FakeSnowplow:
    """Answers /debug/vars and the stage's read.
    register: the read registers the GVR and falls through (not-synced) for
      `sync_reads` reads (`register_noise`: schema_unchanged moves on each of
      them; `register_heap_spike`: the heap is 5 GB higher while they fall
      through); `pre_outcome`: the stage GVR already has a build outcome
      (abort_clean) before armed.
    armed: widened() answers True after `widen_delay` checks (the operator's
      poll, apply and exec); `never_widen`: never.
    settle: k counts settle samples; the CRD event is seen at k ==
      `detect_after`; the build installs (swap) / the re-fire runs (relist)
      `build_samples` samples later counting the event's; `window_after`
      more samples of open window; `never_end`; `switch_mech_at`: the families
      switch to the other mechanism (a replaced pod); `other_crd_relist_at`:
      another CRD's schema change relists at that k."""

    def __init__(self, mech="build_and_swap", *, sync_reads=2, build_samples=3, window_after=0, never_end=False,
                 never_serve=False, relist_unservable=2, n=4, pre_outcome=0, detect_after=1, register_noise=False,
                 register_heap_spike=False, widen_delay=1, never_widen=False, switch_mech_at=None,
                 other_crd_relist_at=None):
        self.mech, self.sync_reads, self.build_samples = mech, sync_reads, build_samples
        self.window_after, self.never_end, self.never_serve = window_after, never_end, never_serve
        self.relist_unservable, self.n, self.pre_outcome = relist_unservable, n, pre_outcome
        self.detect_after, self.register_noise, self.register_heap_spike = detect_after, register_noise, \
            register_heap_spike
        self.widen_delay, self.never_widen, self.switch_mech_at = widen_delay, never_widen, switch_mech_at
        self.other_crd_relist_at = other_crd_relist_at
        self.t = T0
        self.reads = 0
        self.registered = self.armed = self.is_widened = False
        self.widen_checks = 0
        self.k = 0
        self.cells = {}
        self.relists = self.postsync = self.swaps = self.bridge_runs = self.unchanged = 0
        self.base_heap = self.heap = 2000 * MB
        self.num_gc, self.pause_ns = 100, 5_000_000
        self.events = []                # (t, crd) of each schema relist, for snowplow's log

    def clock(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def login(self):
        return "tok"

    def arm(self):
        self.armed = True

    def widened(self):
        if self.never_widen:
            return False
        self.widen_checks += 1
        if self.widen_checks > self.widen_delay:
            self.is_widened = True
        return self.is_widened

    def _cell(self, reason):
        c = f"restactions|{fw.GVR}|{reason}"
        self.cells[c] = self.cells.get(c, 0) + 1

    def _kd(self):
        return self.k - self.detect_after + 1          # 1 at the CRD event

    def _servable(self):
        if self.never_serve or self.reads <= self.sync_reads:
            return False
        if self.mech == "relist" and self.is_widened and self._kd() >= 1:
            return self.postsync >= 1 and self._kd() >= self.build_samples + self.relist_unservable
        return True

    def _read(self):
        self.reads += 1
        self.registered = True
        if not self.armed and self.register_noise:
            self.unchanged += 1
        if self.register_heap_spike and not self.armed:
            self.heap = self.base_heap + (5000 * MB if self.reads <= self.sync_reads else 0)
        if not self._servable():
            self._cell("informer-fallthrough-not-synced")

    def _advance(self):
        if not self.is_widened:
            return
        self.k += 1
        if self.other_crd_relist_at is not None and self.k == self.other_crd_relist_at:
            self.relists += 1
            self.events.append((self.t, "otherkinds.example.io"))
        kd = self._kd()
        if kd == 1:
            self.relists += 1
            self.events.append((self.t, fw.CRD_NAME))
            if self.mech == "relist":
                self.bridge_runs += 1
        if kd < 1:
            return
        if self.never_end:
            self.heap += 50 * MB
            return
        if kd < self.build_samples:
            self.heap += 400 * MB          # two copies coexist
            self.num_gc += 2
            self.pause_ns += 2_000_000
        elif kd == self.build_samples:
            if self.mech == "build_and_swap":
                self.swaps += 1
            else:
                self.postsync += 1
            self.heap -= 300 * MB

    def _ks(self):
        kd = self._kd()
        open_ = self.is_widened and kd >= 1 and (self.swaps == 0 or kd < self.build_samples + self.window_after)
        R, O, C = ("divergence", "schema"), ("swap", "abort_clean", "abort_refused", "abort_teardown"), \
            ("pushed", "pushed_on_arm", "pushed_resync", "known_stale", "evicted", "live_event")
        lab = lambda f: {f"{r}|{o}": f(r, o) for r in R for o in O}          # noqa: E731
        hist = {f"{r}|{c}": {"count": 0, "sum_ms": 0, "max_ms": 0} for r in R for c in C}
        return {"store_swap_known_stale_keys_total": lab(lambda r, o: self.n if (r, o) == ("schema", "swap")
                                                         and self.swaps else 0),
                "store_swap_known_stale_builds_total": lab(lambda r, o: self.swaps if (r, o) == ("schema", "swap")
                                                           else 0),
                "store_swap_known_stale_window_seconds_total": lab(lambda r, o: 0.0),
                "store_swap_ledger_closes_total": {f"{r}|{c}": 0 for r in R for c in C},
                "store_swap_detect_to_push_ms": hist, "store_swap_install_to_push_ms": hist,
                "store_swap_known_stale_keys_open": {"divergence": 0, "schema": self.n if open_ else 0},
                "store_swap_known_stale_window_open_seconds_max": {"divergence": 0,
                                                                   "schema": float(kd) if open_ else 0.0},
                "store_swap_ledger_owed": {"divergence": 0, "schema": 0}}

    def vars(self):
        mech = self.mech
        if self.switch_mech_at is not None and self.k >= self.switch_mech_at:
            mech = "relist" if mech == "build_and_swap" else "build_and_swap"
        v = {"memstats": {"HeapInuse": self.heap, "HeapAlloc": self.heap - 100 * MB, "HeapSys": self.heap,
                          "Sys": self.heap + 500 * MB, "NumGC": self.num_gc, "PauseTotalNs": self.pause_ns,
                          "GCCPUFraction": 0.01, "PauseNs": [1_000_000] * 256},
             fw.REGISTERED_F: {"count": 1, "gvrs": [fw.GVR_REGISTERED] if self.registered else ["a/v1/b"]},
             fw.FALLTHROUGH_F: dict(self.cells) | {"restactions|other/v1, Resource=x|informer-fallthrough-not-synced": 9}}
        crd = {"schema_relists_fired": self.relists, "schema_unchanged": self.unchanged}
        if mech == "build_and_swap":
            v[fw.SWAP_F] = {"store_swaps_total": self.swaps, "store_swap_superseded_total": 0,
                            "store_swap_m_keys_total": self.n * self.swaps,
                            "store_swap_deliveries_total": self.n * self.swaps, "store_swap_regressed_keys": 0,
                            "store_swap_schema_unowned_total": 0, "store_swap_reused_objects_total": 0}
            v[fw.SWAP_KS_F] = self._ks()
            ours = {"installed": self.swaps, "superseded": 0, "abort_clean": self.pre_outcome, "abort_refused": 0,
                    "abort_teardown": 0}
            v[fw.SWAPS_BY_GVR_F] = {fw.GVR: ours} if (self.swaps or self.pre_outcome) else {}
            v[fw.LIST_INCREMENTAL_F] = {"unanchors": {}}
        elif mech == "relist":
            crd |= {"relist_bridge_runs_total": self.bridge_runs, "relist_bridge_enqueued_total": 0,
                    "relist_bridge_timeout_total": 0, "relist_bridge_aborted_total": 0,
                    "relist_dirtymark_postsync_total": self.postsync, "relist_postsync_timeout_total": 0}
            v[fw.VERIFY_F] = {"store_repairs_pending": 0, "store_repairs_fired_total": 0}
        v[fw.CRD_F] = crd
        return v

    def get(self, path, token):
        self.t += 0.01
        if path == "/debug/vars":
            return 200, json.dumps(self.vars()).encode()
        assert path.startswith("/call?"), path
        self._advance()
        self._read()
        return 200, b"{}"

    def raw_log(self, kubelet=False) -> list[str]:
        """snowplow's log over the stage, as the chart runs it (LOG_LEVEL warn,
        #1050 F7): WARN lines only, no schema_relist (an INFO line) (kubelet:
        with `kubectl logs --timestamps`' prefix)."""
        out = []
        for t, _ in self.events:
            ln = json.dumps({"time": _iso(t), "level": "WARN", "msg": "cache.refresher.decline", "subsystem": "cache"})
            out.append((_iso(t) + " " + ln) if kubelet else ln)
        return out

    def crds(self, end: bool) -> dict:
        """Every CRD's uid and generation at the window's start / end: the widen
        bumps the stage's; `other_crd_relist_at` bumps another's."""
        other = 1 + (1 if end and self.other_crd_relist_at is not None else 0)
        return {"otherkinds.example.io": {"uid": "u-o", "generation": other},
                "users.basic.authn.krateo.io": {"uid": "u-u", "generation": 4},
                fw.CRD_NAME: {"uid": "u-fw", "generation": 1 + (1 if end else 0)}}


def B(swap=60.0, relist=121.0, register=60.0):
    return {"register": {"s": register, "source": "test register"}, "relist": {"s": relist, "source": "test relist"},
            "build_and_swap": {"s": swap, "source": "test swap"}}


def watch(sp: FakeSnowplow, bounds=None, sample_s=1.0, widen_wait_s=30.0):
    recs = []
    armed_at = []

    def arm():
        armed_at.append(sp.reads)
        sp.arm()
    rc = fw.Watch(get=sp.get, login=sp.login, emit=recs.append, arm=arm, widened=sp.widened,
                  bounds=bounds or B(), widen_wait_s=widen_wait_s, sample_s=sample_s, clock=sp.clock,
                  sleep=sp.sleep).run()
    return rc, recs, armed_at


def end_of(recs):
    return next(r for r in recs if r["kind"] == "end")


def first(recs, kind):
    return next(r for r in recs if r["kind"] == kind)


# ─── R1: build_and_swap's end ────────────────────────────────────────────────

def test_a_swap_ends_when_its_gvr_outcome_moved_and_both_schema_gauges_are_zero():
    sp = FakeSnowplow("build_and_swap", build_samples=3)
    rc, recs, armed = watch(sp)
    e = end_of(recs)
    assert rc == 0 and e["outcome"] == "ended", e
    assert "snowplow_store_swaps_by_gvr" in e["signal"] and e["detected"] is not None
    assert sp.swaps == 1 and armed
    settle = [r for r in recs if r["kind"] == "sample" and r["phase"] == "settle"]
    assert len(settle) == 3                   # the end is the FIRST sample at which every part holds


def test_an_outcome_before_armed_never_ends_a_swap():
    """The baseline is the armed sample: the stage GVR already had a build
    outcome (a divergence build's abort_clean) before armed; the gauges read 0
    until the CRD event (seen at the 3rd settle sample). Ends only after the
    swap."""
    sp = FakeSnowplow("build_and_swap", pre_outcome=1, detect_after=3, build_samples=3)
    rc, recs, _ = watch(sp)
    e = end_of(recs)
    assert e["outcome"] == "ended" and sp.swaps == 1, e
    assert sp.k == 3 + 3 - 1                  # the swap: 3 samples counting the event's


def test_gauges_at_zero_before_the_crd_event_never_end_a_swap():
    sp = FakeSnowplow("build_and_swap", never_end=True)
    rc, recs, _ = watch(sp, bounds=B(swap=10.0))
    e = end_of(recs)
    assert e["outcome"] == "bound_reached" and e["bound_s"] == 10.0, e
    t_w = first(recs, "widened")["t"]
    assert t_w + 10.0 <= e["t"] <= t_w + 10.0 + 1.5                    # bounded: the bound plus one sample


def test_an_open_schema_window_after_the_swap_does_not_end_it():
    sp = FakeSnowplow("build_and_swap", build_samples=2, window_after=3)
    rc, recs, _ = watch(sp)
    assert end_of(recs)["outcome"] == "ended"
    assert sp.k == 2 + 3                       # the swap at k=2, the window closed at k=5


def test_build_ended_swap_needs_all_three_parts():
    sp = FakeSnowplow("build_and_swap")
    armed = sp.vars()
    sp.is_widened, sp.k, sp.swaps = True, 3, 1
    now = sp.vars()
    assert fw.build_ended("build_and_swap", armed, now, {})[0]
    open_ = json.loads(json.dumps(now))
    open_[fw.SWAP_KS_F]["store_swap_known_stale_keys_open"]["schema"] = 1
    assert not fw.build_ended("build_and_swap", armed, open_, {})[0]
    win = json.loads(json.dumps(now))
    win[fw.SWAP_KS_F]["store_swap_known_stale_window_open_seconds_max"]["schema"] = 0.5
    assert not fw.build_ended("build_and_swap", armed, win, {})[0]
    assert not fw.build_ended("build_and_swap", armed, armed, {})[0]
    other = json.loads(json.dumps(now))
    other[fw.SWAPS_BY_GVR_F] = {"other/v1, Resource=x": {"installed": 1}}
    assert not fw.build_ended("build_and_swap", armed, other, {})[0]


# ─── R2: relist's end ────────────────────────────────────────────────────────

def test_a_relist_ends_when_the_refire_caught_up_and_the_read_serves_again():
    sp = FakeSnowplow("relist", build_samples=3, relist_unservable=2)
    rc, recs, _ = watch(sp)
    assert end_of(recs)["outcome"] == "ended", end_of(recs)
    assert sp.k == 3 + 2


def test_build_ended_relist_needs_the_refire_and_a_serving_read():
    sp = FakeSnowplow("relist")
    armed = sp.vars()
    sp.relists, sp.postsync, sp.bridge_runs = 1, 1, 1
    now = sp.vars()
    ok = {"status": 200, "moved_window": {}}
    assert fw.build_ended("relist", armed, now, ok)[0]
    assert not fw.build_ended("relist", armed, now, {"status": 200, "moved_window": {"restactions|x": 1}})[0]
    assert not fw.build_ended("relist", armed, now, {"status": 500, "moved_window": {}})[0]
    assert not fw.build_ended("relist", armed, now, {"status": 200, "moved_window": None})[0]
    sp.postsync = 0
    assert not fw.build_ended("relist", armed, sp.vars(), ok)[0]
    sp.relists = 0
    assert not fw.build_ended("relist", armed, sp.vars(), ok)[0]


# ─── R3: the widen waits for a read that serves ──────────────────────────────

def test_the_widen_waits_for_a_read_with_no_fall_through():
    sp = FakeSnowplow("build_and_swap", sync_reads=4)
    rc, recs, armed = watch(sp)
    assert armed == [5]
    reg = [r for r in recs if r["kind"] == "sample" and r["phase"] == "register"]
    assert [bool(r["probe"]["moved"]) for r in reg] == [True] * 4 + [False]


def test_a_gvr_that_never_serves_ends_the_job_unarmed_within_its_register_bound():
    sp = FakeSnowplow("build_and_swap", never_serve=True)
    rc, recs, armed = watch(sp, bounds=B(register=8.0))
    e = end_of(recs)
    assert rc == 3 and armed == [] and e["outcome"] == "not_serving_within_bound", e
    assert e["bound_s"] == 8.0 and e["t"] - recs[0]["t"] <= 8.0 + 1.5


def test_serves_refuses_an_unregistered_gvr_and_an_unpublished_meter():
    v = FakeSnowplow().vars()
    assert fw.serves({"status": 200, "moved": {}}, v)[1] == "the GVR is not registered yet"
    v[fw.REGISTERED_F]["gvrs"] = [fw.GVR_REGISTERED]
    assert fw.serves({"status": 200, "moved": {}}, v)[0]
    assert not fw.serves({"status": 200, "moved": None}, v)[0]
    assert not fw.serves({"status": 403, "moved": {}}, v)[0]


def test_a_build_publishing_no_mechanism_ends_the_job():
    rc, recs, armed = watch(FakeSnowplow("none"))
    assert rc == 3 and end_of(recs)["outcome"] == "mechanism_unknown" and not armed


def test_a_mechanism_that_changes_mid_settle_ends_the_job_by_name():
    rc, recs, _ = watch(FakeSnowplow("build_and_swap", switch_mech_at=2, build_samples=5))
    assert rc == 3 and end_of(recs)["outcome"] == "mechanism_changed"


def test_every_read_is_a_new_ra_key():
    a, b = fw.read_path(1), fw.read_path(2)
    assert a != b and "extras=%7B%22bench1004_read%22%3A1%7D" in a
    assert "resource=restactions" in a and f"name={fw.RA_NAME}" in a and "apiVersion=templates.krateo.io%2Fv1" in a


def test_the_sample_keeps_only_the_stage_gvrs_cells_and_registration():
    sp = FakeSnowplow()
    sp._read()
    t = fw.trim(sp.vars(), None)
    assert set(t[fw.FALLTHROUGH_F]) == {f"restactions|{fw.GVR}|informer-fallthrough-not-synced"}
    assert t[fw.REGISTERED_F] == {"gvrs": [fw.GVR_REGISTERED]}
    assert "PauseNs" not in t["memstats"] and t["memstats"]["HeapInuse"] == sp.heap


# ─── R4: the widen and the bound's clock ─────────────────────────────────────

def test_the_settle_bound_counts_from_the_widen_not_from_armed():
    """The operator's widen lands 8 samples after armed; the build ends 5
    samples later: 13 s after armed, inside a 10 s bound that starts at the
    widen."""
    sp = FakeSnowplow("build_and_swap", widen_delay=8, build_samples=5)
    rc, recs, _ = watch(sp, bounds=B(swap=10.0))
    e = end_of(recs)
    assert e["outcome"] == "ended", e
    assert e["t"] - first(recs, "armed")["t"] > 10.0
    assert e["t"] - first(recs, "widened")["t"] <= 10.0


def test_a_widen_that_never_comes_ends_the_job_within_its_wait():
    sp = FakeSnowplow("build_and_swap", never_widen=True)
    rc, recs, _ = watch(sp, widen_wait_s=7.0)
    e = end_of(recs)
    assert rc == 3 and e["outcome"] == "never_widened" and 7.0 <= e["waited_s"] <= 8.5


def test_detection_is_the_first_sample_whose_relists_moved():
    sp = FakeSnowplow("build_and_swap", detect_after=3, build_samples=2)
    rc, recs, _ = watch(sp)
    det = first(recs, "detected")["t"]
    settle = [r for r in recs if r["kind"] == "sample" and r["phase"] == "settle"]
    assert det == settle[2]["t"]
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after=PODS, crds_before=sp.crds(False),
                      crds_after=sp.crds(True))
    assert rec["timeline_s"]["widened_to_detected"] >= 2.0


# ─── R4 + R9 + R10: the record ───────────────────────────────────────────────

PLAN = {"n": 4, "namespaces": 2, "per_namespace": 2, "batches": 2, "batch": 200, "workers": 32,
        "object_bytes": 1400, "composition_bytes": 1420, "crd": fw.CRD_NAME, "gvr": fw.GVR, "derived_from": {}}
PODS = {"snowplow-abc": 0}


def _eval(sp, recs, **kw):
    return fw.evaluate(recs, plan_=PLAN, pods_before=kw.get("before", PODS), pods_after=kw.get("after", PODS),
                       crds_before=kw["crds_before"] if "crds_before" in kw else sp.crds(False),
                       crds_after=kw["crds_after"] if "crds_after" in kw else sp.crds(True))


@pytest.mark.parametrize("mech", ["build_and_swap", "relist"])
def test_a_build_that_ended_passes_with_its_rows_and_the_heap_peak(mech):
    sp = FakeSnowplow(mech)
    rc, recs, _ = watch(sp)
    rec = _eval(sp, recs)
    assert rec["passed"], rec["fail_reasons"]
    assert rec["mechanism"] == mech and rec["outcome"] == "ended"
    assert {r["id"] for r in rec["assertions"]} == {f"B{i}" for i in range(1, 8)}
    m = rec["memory"]
    assert m["heap_inuse_mb_armed"] == 2000.0 and m["heap_inuse_mb_peak"] == 2800.0
    assert m["heap_inuse_mb_peak_over_armed"] == 800.0 and m["gc_cycles"] == 4
    assert rec["attribution"]["stage_crd_generation_delta"] == 1 and rec["attribution"]["other_crds_changed"] == {}
    if mech == "relist":
        assert rec["relist_bridge_timeout_total"] == 0
    else:
        assert "report_only" in rec and "B3" in [r["id"] for r in rec["assertions"] if r.get("report_only")]
    assert rec["bound"]["source"].startswith("test ") and rec["bound"]["clock"].startswith("from the widen")
    assert any(ln.startswith("full widen: PASS") for ln in fw.report_lines(rec))


def test_the_rows_baseline_is_the_armed_sample_not_the_register_phase():
    sp = FakeSnowplow("build_and_swap", register_noise=True, sync_reads=3)
    rc, recs, _ = watch(sp)
    rec = _eval(sp, recs)
    assert rec["passed"], rec["fail_reasons"]                      # B6 reads Δschema_unchanged 0 since armed
    assert rec["delta"][f"{fw.CRD_F}.schema_unchanged"] == 0


def test_the_memory_report_starts_at_armed():
    sp = FakeSnowplow("build_and_swap", register_heap_spike=True)
    rc, recs, _ = watch(sp)
    m = _eval(sp, recs)["memory"]
    assert m["heap_inuse_mb_armed"] == 2000.0 and m["heap_inuse_mb_peak"] == 2800.0   # not the register's 7000


def test_bound_reached_is_never_a_pass():
    sp = FakeSnowplow("build_and_swap", never_end=True)
    rc, recs, _ = watch(sp, bounds=B(swap=5.0))
    rec = _eval(sp, recs)
    assert not rec["passed"] and rec["outcome"] == "bound_reached"
    assert any(r.startswith("outcome bound_reached") for r in rec["fail_reasons"])


def test_a_restarted_snowplow_fails_the_stage():
    sp = FakeSnowplow()
    rc, recs, _ = watch(sp)
    rec = _eval(sp, recs, after={"snowplow-abc": 1})
    assert not rec["passed"] and "restarted or was replaced" in rec["fail_reasons"][0]
    assert not _eval(sp, recs, after={"snowplow-def": 0})["passed"]


def test_a_stream_without_its_end_fails():
    sp = FakeSnowplow()
    rc, recs, _ = watch(sp)
    rec = _eval(sp, [r for r in recs if r["kind"] != "end"])
    assert not rec["passed"] and "no end record" in rec["fail_reasons"][0]


def test_a_failing_row_fails_the_stage():
    sp = FakeSnowplow("build_and_swap")
    rc, recs, _ = watch(sp)
    for r in recs:                           # a whole-type mark at the swap (B4's falsifier)
        if r["kind"] == "sample" and r["phase"] == "settle":
            r["vars"][fw.LIST_INCREMENTAL_F] = {"unanchors": {"type_event": 1}}
    rec = _eval(sp, recs)
    assert not rec["passed"] and "B4" in rec["failed_rows"]


@pytest.mark.parametrize("mech", ["build_and_swap", "relist"])
def test_another_crds_schema_change_in_the_window_refuses_the_result(mech):
    sp = FakeSnowplow(mech, other_crd_relist_at=2, build_samples=4)
    rc, recs, _ = watch(sp)
    rec = _eval(sp, recs)
    assert not rec["passed"]
    assert any(r.startswith("another CRD's spec changed in the window") for r in rec["fail_reasons"])
    assert rec["attribution"]["other_crds_changed"] == {"otherkinds.example.io": 1}


def test_crds_unread_at_either_end_refuse_the_result():
    sp = FakeSnowplow("relist")
    rc, recs, _ = watch(sp)
    for kw in ({"crds_before": None}, {"crds_after": None}):
        rec = _eval(sp, recs, **kw)
        assert not rec["passed"] and any("the CRDs were not read at both ends" in r for r in rec["fail_reasons"])


@pytest.mark.parametrize("gen_after,ok", [(1, False), (2, True), (3, False)])
def test_the_stage_crd_changes_exactly_once_the_widen(gen_after, ok):
    sp = FakeSnowplow("build_and_swap")
    rc, recs, _ = watch(sp)
    after = sp.crds(True) | {fw.CRD_NAME: {"uid": "u-fw", "generation": gen_after}}
    rec = _eval(sp, recs, crds_after=after)
    assert rec["passed"] is ok, rec["fail_reasons"]


def test_a_crd_created_deleted_or_recreated_in_the_window_is_reported_not_refused():
    """A CRD's first observation and its deletion relist nothing: reported. A
    recreated CRD (a new uid, its generation back to 1) is not a spec change."""
    sp = FakeSnowplow("build_and_swap")
    rc, recs, _ = watch(sp)
    before = sp.crds(False) | {"gone.example.io": {"uid": "u-g", "generation": 2},
                               "again.example.io": {"uid": "u-a1", "generation": 7}}
    after = sp.crds(True) | {"new.example.io": {"uid": "u-n", "generation": 1},
                             "again.example.io": {"uid": "u-a2", "generation": 1}}
    rec = _eval(sp, recs, crds_before=before, crds_after=after)
    assert rec["passed"], rec["fail_reasons"]
    att = rec["attribution"]
    assert att["created"] == ["again.example.io", "new.example.io"] and att["deleted"] == ["again.example.io",
                                                                                         "gone.example.io"]
    assert att["other_crds_changed"] == {}


def test_a_warn_level_log_does_not_decide_attribution():
    """(the #1050 re-check's b5, unit) snowplow runs at LOG_LEVEL warn: no
    schema_relist line exists; the stage still PASSes, attributed from the CRDs."""
    sp = FakeSnowplow("relist")
    rc, recs, _ = watch(sp)
    assert all('"level": "WARN"' in ln for ln in sp.raw_log())
    assert _eval(sp, recs)["passed"]


# ─── R5: the bounds ──────────────────────────────────────────────────────────

def _deploy(env=(), env_from=(("snowplow", ""),), name="snowplow", containers=None):
    c = {"name": name, "env": list(env),
         "envFrom": [{"configMapRef": {"name": cm}, **({"prefix": p} if p else {})} for cm, p in env_from]}
    return {"metadata": {"name": "snowplow"}, "spec": {"template": {"spec": {"containers": containers or [c]}}}}


def _cms(**maps):
    return {"items": [{"metadata": {"name": k}, "data": v} for k, v in maps.items()]}


@pytest.mark.parametrize("cm,env,want", [
    ({}, [], 121.0),
    ({fw.RELIST_BRIDGE_ENV: "300"}, [], 301.0),                                     # the chart's way: envFrom
    ({fw.RELIST_BRIDGE_ENV: "60"}, [], 121.0),
    ({fw.RELIST_BRIDGE_ENV: "0"}, [], 121.0),
    ({fw.RELIST_BRIDGE_ENV: "x"}, [], 121.0),
    ({fw.RELIST_BRIDGE_ENV: "300"}, [{"name": fw.RELIST_BRIDGE_ENV, "value": "200"}], 201.0),   # env shadows envFrom
])
def test_the_relist_bound_is_snowplows_own_give_up_plus_one_sample(cm, env, want):
    b = fw.bounds(_deploy(env=env), _cms(snowplow=cm), 2700)
    assert b["relist"]["s"] == want
    assert "relistPostSyncWait 120 s" in b["relist"]["source"] and fw.RELIST_BRIDGE_ENV in b["relist"]["source"]
    if cm.get(fw.RELIST_BRIDGE_ENV) == "300" and not env:
        assert "envFrom ConfigMap snowplow" in b["relist"]["source"]
    assert b["build_and_swap"]["s"] == 2700.0 and b["build_and_swap"]["source"].startswith("ROLLOUT_TIMEOUT_S 2700 s")
    assert b["register"]["s"] == 2700.0


def test_a_bound_it_cannot_read_is_refused_never_defaulted():
    with pytest.raises(ValueError, match="was not read"):
        fw.bounds(_deploy(), _cms(other={}), 2700)                       # the ConfigMap it names is not there
    dep = _deploy()
    dep["spec"]["template"]["spec"]["containers"][0]["envFrom"].append({"secretRef": {"name": "s"}})
    with pytest.raises(ValueError, match="does not read Secrets"):
        fw.bounds(dep, _cms(snowplow={}), 2700)
    with pytest.raises(ValueError, match="whose env is snowplow's is not known"):
        fw.bounds(_deploy(name="sidecar"), _cms(snowplow={}), 2700)


def test_the_snowplow_container_is_picked_by_name():
    side = {"name": "sidecar", "env": [{"name": fw.RELIST_BRIDGE_ENV, "value": "900"}]}
    sp = {"name": "snowplow", "envFrom": [{"configMapRef": {"name": "snowplow"}}]}
    b = fw.bounds(_deploy(containers=[side, sp]), _cms(snowplow={fw.RELIST_BRIDGE_ENV: "300"}), 2700)
    assert b["relist"]["s"] == 301.0


# ─── R6: what the stage creates ──────────────────────────────────────────────

def _plan(tmp_path, n, scale, namespaces=2):
    return fw.plan(n=n, scale=scale, namespaces=namespaces, out_dir=str(tmp_path), user_prefix="scaleuser-",
                   password="pw", snowplow_url="http://s", authn_url="http://a",
                   bounds=fw.bounds(_deploy(), _cms(snowplow={}), 2700), linger_s=5, widen_wait_s=660)


def test_the_plan_takes_n_from_the_proof_and_the_bodies_and_batching_from_the_data_step(tmp_path):
    p = _plan(tmp_path, 600, 600)
    assert p["batch"] == data_manifests.COMPOSITION_APPLY_BATCH and p["workers"] == data_manifests.COMPOSITION_APPLY_WORKERS
    seen = []
    for f in p["files"]:
        docs = list(yaml.safe_load_all(open(f)))
        assert len(docs) <= data_manifests.COMPOSITION_APPLY_BATCH
        assert len({d["metadata"]["namespace"] for d in docs}) == 1
        seen += docs
    assert len(seen) == 600 and len({(d["metadata"]["namespace"], d["metadata"]["name"]) for d in seen}) == 600
    d = seen[-1]
    comp = yaml.safe_load(data_manifests.composition_yaml(d["metadata"]["namespace"], d["metadata"]["name"]))
    assert d["spec"] == comp["spec"] and d["metadata"] == comp["metadata"]
    assert (d["apiVersion"], d["kind"]) == (f"{fw.GROUP}/{fw.VERSION}", fw.KIND)
    with pytest.raises(ValueError, match="budgeted SCALE"):
        _plan(tmp_path, 599, 600)
    with pytest.raises(ValueError, match="split evenly"):
        fw.batches(5, 2, 200)


def test_the_widen_adds_one_defaulted_property_and_nothing_else():
    a, b = fw.crd(False), fw.crd(True)
    sa = a["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    sb = b["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    assert sb.pop("properties") == {fw.WIDEN_FIELD: {"type": "string", "default": fw.WIDEN_DEFAULT}}
    assert sa == sb and {k: v for k, v in a.items() if k != "spec"} == {k: v for k, v in b.items() if k != "spec"}


def test_the_stage_identity_reads_only_its_objects_and_its_probe():
    docs = fw.identity_docs("scaleuser-fullwiden", "pw")
    role = next(d for d in docs if d["kind"] == "ClusterRole")
    assert role["rules"] == [{"apiGroups": [fw.GROUP], "resources": [fw.PLURAL], "verbs": ["get", "list", "watch"]},
                             {"apiGroups": ["templates.krateo.io"], "resources": ["restactions"], "verbs": ["get"],
                              "resourceNames": [fw.RA_NAME]}]
    assert next(d for d in docs if d["kind"] == "User")["spec"]["groups"] == [fw.IDENTITY_GROUP]


def test_the_stage_documents_are_valid_against_their_crds():
    from bench import crd_schema
    from tests.test_crd_schema import _sources
    res = crd_schema.check(fw.documents(2, "scaleuser-"), _sources() + [fw.crd_source()])
    st = {e["kind"]: e["status"] for e in res["kinds"]}
    assert res["ok"], res
    assert st[f"{fw.KIND}.{fw.GROUP}/{fw.VERSION}"] == "valid"
    assert st["User.basic.authn.krateo.io/v1alpha1"] == "valid"
    assert st["RESTAction.templates.krateo.io/v1"] in ("valid", "not faithfully validated")


def test_the_stage_objects_and_job_fit_the_api(tmp_path):
    from tests.test_apply_size import _objects, check
    p = _plan(tmp_path, 400, 400)
    objs = [o for f in ["job.yaml", "probe.yaml", "crd.yaml", "crd-widened.yaml"] for o in
            _objects((tmp_path / f).read_text())] + _objects(Path(p["files"][0]).read_text())
    assert objs and all(check(o, client_side=False) == [] for o in objs)


def test_the_job_ships_what_it_imports_and_runs_the_watch():
    text = fw.job_yaml(snowplow_url="http://s", authn_url="http://a", user="u", password="pw",
                       bounds={"relist": {"s": 1, "source": "x"}}, linger_s=5, widen_wait_s=660)
    cm, job = [d for d in yaml.safe_load_all(text) if d]
    assert set(cm["data"]) == {"full_widen.py", "load606.py", "store_rebuild.py"}
    assert cm["data"]["full_widen.py"] == (BENCH_DIR / "bench" / "full_widen.py").read_text()
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    assert "python3 -u -m bench.full_widen watch" in script and f"echo \"$rc\" > {fw.EXITED}" in script
    assert "--widen-wait-s 660" in script
    assert job["spec"]["template"]["spec"]["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}


# ─── R7: run606 — the dry pass ───────────────────────────────────────────────

def _dry(tmp_path, **env):
    tmp_path.mkdir(parents=True, exist_ok=True)
    e = dict(os.environ, **fake_registry(tmp_path))
    e.pop("BENCH_1004_GO", None)
    e.update(HOME=str(tmp_path), OUT=str(tmp_path / "out"), RUN_NAME=RUN_NAME, INSTALLER_VERSION="0.3.422",
             SNOWPLOW_TAG="1.12.46", THINK_S="0", BENCH_606_DRY_RUN="1", EXPECTED_IMAGE_DIGEST=FAKE_DIGEST,
             PYTHONPATH=str(BENCH_DIR), **env)
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e, capture_output=True, text=True,
                          timeout=300)


def _budget(stderr):
    ln = next(x for x in stderr.splitlines() if "run budget:" in x)
    return int(ln.split(" = ", 1)[1].split(" s;", 1)[0]), ln


def test_the_dry_pass_places_the_stage_after_evaluate_and_run_complete(tmp_path):
    p = _dry(tmp_path / "on", BENCH_1004_GO="anything-in-a-dry-pass")
    assert p.returncode == 0, p.stderr[-3000:]
    log = (tmp_path / "on" / "out" / "run.log").read_text()
    i_measure, i_collect = log.index("=== step measure ==="), log.index("=== step collect ===")
    i_eval, i_done = log.index("=== step evaluate ==="), log.index("run complete: ")
    i_fw = log.index("=== step full-widen ===")
    assert i_measure < i_collect < i_eval < i_done < i_fw
    assert "DRY: full widen (#1004): after the evaluate step" in log[i_fw:]
    assert "=== step" not in log[i_fw + 10:]


def test_every_bounded_wait_of_the_stage_is_a_budget_term(tmp_path):
    off = _dry(tmp_path / "off")
    on = _dry(tmp_path / "on", BENCH_1004_GO="x")
    assert off.returncode == 0 and on.returncode == 0, (off.stderr + on.stderr)[-3000:]
    assert "=== step full-widen ===" not in (tmp_path / "off" / "out" / "run.log").read_text()
    b_off, line_off = _budget(off.stderr)
    b_on, line_on = _budget(on.stderr)
    deletes = len(fw.cleanup_targets("x"))
    widen_wait = 300 + 300 + 60                      # the operator's armed poll, its widened apply, one exec
    job = 300 + 2700 + widen_wait + 2700             # start + register + widen wait + settle
    want = 3 * 300 + 300 + 2400 + job + 9 * 300 + deletes * 300 + 2400
    assert b_on - b_off == want == 16860
    assert (f"full widen {want} (applies 3x300 + ready 300 + objects 2400 + job {job} (preflight 300 + register "
            f"2700 + widen wait {widen_wait} + settle 2700) + reads 9x300 + deletes {deletes}x300 + CRD delete 2400)"
            ) in line_on
    assert "full widen" not in line_off


def test_the_stage_reads_and_deletes_are_counted_where_they_run():
    """The budget's counts are the stage's own: FW_READS fw_read / bounded reads,
    FW_APPLIES bounded single applies outside the Job's window, one delete term
    per cleanup line."""
    src = (BENCH_DIR / "run606.sh").read_text()
    body = src[src.index("s_full_widen() {"):src.index('log "run budget: ${budget_line}')]
    reads = body.count("fw_read ") + body.count(" cp \"${FW_POD}") + body.count(" logs deploy/snowplow") \
        + body.count('get "$FW_CRD" -A')
    assert reads == 9 and "FW_READS=9" in src
    singles = [ln for ln in body.splitlines() if ln.strip().startswith('bounded "$COLLECT_TIMEOUT_S" kapply')]
    assert len(singles) == 4 and "FW_APPLIES=3" in src          # the widened one is inside the job's widen wait
    assert 'FW_DELETES="$(python3 -m bench.full_widen cleanup-count)"' in src


def test_the_dry_pass_validates_the_stage_documents(tmp_path):
    p = _dry(tmp_path, BENCH_1004_GO="x")
    assert p.returncode == 0, p.stderr[-3000:]
    res = json.loads((tmp_path / "out" / "crd-check.json").read_text())
    kinds = {e["kind"]: e for e in res["kinds"]}
    assert kinds[f"{fw.KIND}.{fw.GROUP}/{fw.VERSION}"]["status"] == "valid"
    assert "the full-widen stage's identity" in kinds["User.basic.authn.krateo.io/v1alpha1"]["applied_by"]


# ─── R7: run606 — the real sequence under fake tools ─────────────────────────

def _stream(tmp: Path, sp: FakeSnowplow, name="fw-stream", **kw) -> tuple[str, str]:
    rc, recs, _ = watch(sp, **kw)
    f, g = tmp / f"{name}.jsonl", tmp / f"{name}.log"
    f.write_text("".join(json.dumps(r) + "\n" for r in recs + [{"kind": "exit", "rc": rc, "t": 0}]))
    g.write_text("".join(x + "\n" for x in sp.raw_log(kubelet=True)))
    return str(f), str(g)


def _env(harness_, sp=None, **env):
    e, state, ambient, tmp = harness_
    today = datetime.now(UTC).strftime("%Y%m%d")
    stream, log = _stream(tmp, sp or FakeSnowplow(n=4), name=env.pop("_name", "fw-stream"))
    e = dict(e, BENCH_1004_GO=f"{today}-{RUN_NAME}", FAKE_FW_STREAM=stream, FAKE_FW_LOG=log)
    e.update(env)
    return e


def _events(state):
    return [json.loads(x)[1] for x in (state / "fw-events").read_text().splitlines()] \
        if (state / "fw-events").exists() else []


def _real(harness_, sp=None, **env):
    e = _env(harness_, sp, **env)
    _, state, _, tmp = harness_
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e, capture_output=True, text=True, timeout=400)
    return p, _events(state), state, tmp


def _no_password_left(out: Path):
    assert not (out / ".bench-password").exists()
    for f in (out / "full-widen").rglob("*"):
        if f.is_file():
            assert SCALEUSER_PASSWORD not in f.read_text(errors="replace"), f


def test_the_stage_runs_after_run_complete_widens_only_once_served_and_cleans_up(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness)
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    log = (tmp / "out" / "run.log").read_text()
    assert log.index("run complete: ") < log.index("=== step full-widen ===")
    applies = [x for x in ev if x.startswith("apply ")]
    assert applies[0] == "apply crd.yaml" and applies[-3:] == ["apply probe.yaml", "apply job.yaml",
                                                               "apply crd-widened.yaml"]
    assert sorted(applies[1:-3]) == ["apply objects-0001.yaml", "apply objects-0002.yaml"]
    assert ev.index("exec /out/armed yes") < ev.index("apply crd-widened.yaml") < ev.index("exec touch /out/widened")
    assert ev.index("exec touch /out/widened") < ev.index("exec /out/exited yes") < ev.index("cp")
    assert ev[-1] == "delete crd" and any(x.startswith("delete ") and "job,configmap" in x for x in ev)
    # F5: every apply of the stage bounds each of its requests
    fw_applies = [c["argv"] for c in map(json.loads, (state / "calls.jsonl").read_text().splitlines())
                  if c["tool"] == "kubectl" and "apply" in c["argv"] and any("/full-widen/" in x for x in c["argv"])]
    assert len(fw_applies) == 6 and all("--request-timeout=300s" in a for a in fw_applies), fw_applies
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert rec["passed"] and rec["plan"]["n"] == 4 and rec["outcome"] == "ended", rec.get("fail_reasons")
    att = rec["attribution"]                                   # from the cluster's CRDs (F7)
    assert att["stage_crd_generation_delta"] == 1 and att["other_crds_changed"] == {}
    assert "full widen: PASS" in log and "full widen: CRD fullwidens.fullwiden.bench.krateo.io and its objects deleted" in log
    assert "envFrom ConfigMap snowplow" in log                 # the bound's source: the chart's way
    assert not list((tmp / "out" / "full-widen").glob("objects-*.yaml"))
    _no_password_left(tmp / "out")
    assert SCALEUSER_PASSWORD not in log
    # #1051: the stage never re-ran collect or evaluate
    assert sum(1 for ln in log.splitlines() if "] verdict exit=" in ln) == 1


def test_the_live_configmap_value_sets_the_relist_bound_and_its_refusal(harness):  # noqa: F811
    """The chart sets DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS in snowplow's ConfigMap
    (envFrom). 300 there makes the relist bound 301 s: past a 200 s budget
    term, refused before anything is created."""
    p, ev, state, tmp = _real(harness, ROLLOUT_TIMEOUT_S="200",
                              FAKE_FW_CM=json.dumps({"DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS": "300"}))
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "a bound exceeds the run budget's 200s term" in log and '"s": 301.0' in log
    assert "DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS=300 (envFrom ConfigMap snowplow)" in log
    assert not [x for x in ev if x.startswith("apply ")]


def test_a_warn_level_snowplow_log_still_passes(harness):  # noqa: F811
    """(the #1050 re-check's b5) the real sequence, a build that ended, and the
    log a LOG_LEVEL=warn snowplow writes: no INFO line. The stage PASSes: its
    attribution is the CRDs' generations."""
    p, ev, state, tmp = _real(harness)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    log_lines = (tmp / "out" / "full-widen" / "snowplow.log").read_text().splitlines()
    assert log_lines and not any('"INFO"' in ln for ln in log_lines)
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert rec["passed"], rec["fail_reasons"]


def test_another_crds_change_in_the_window_fails_the_real_stage(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, FAKE_FW_OTHER_CRD_BUMP="1")
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert not rec["passed"] and rec["attribution"]["other_crds_changed"] == {"installers.composition.krateo.io": 1}
    assert "full widen FAILED: another CRD's spec changed in the window" in (tmp / "out" / "run.log").read_text()


def test_missing_objects_fail_the_stage_before_any_widen_and_leave_no_password(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, FAKE_FW_SHORT="1")
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "run complete: " in log
    assert "full widen FAILED: 3 of 4 objects exist after the batches" in log
    assert "apply job.yaml" not in ev and "apply crd-widened.yaml" not in ev and ev[-1] == "delete crd"
    # job.yaml / probe.yaml were never applied (kapply never redacted them): no password left, nor its file
    assert (tmp / "out" / "full-widen" / "job.yaml").exists()
    _no_password_left(tmp / "out")
    # #1051: the stage's failure never rewrote the run's verdict
    assert sum(1 for ln in log.splitlines() if "] verdict exit=" in ln) == 1
    assert "collecting partial results" not in log


def test_a_pair_stop_mid_stage_leaves_no_bodies_and_no_password(harness):  # noqa: F811
    """USR1 (run606_pair.sh's stop) while the stage waits for its CRD, after
    the plan wrote the bodies and the manifests. stderr goes to a file, not a
    pipe: bounded's watchdog outlives the run and would hold a pipe open."""
    e = _env(harness, FAKE_FW_WAIT_HANG="60")
    _, state, _, tmp = harness
    errf = tmp / "stage-signal.err"
    with open(errf, "w") as fh:
        p = subprocess.Popen(["bash", str(BENCH_DIR / "run606.sh")], env=e, stdout=subprocess.DEVNULL, stderr=fh)
        deadline = time.monotonic() + 300
        while "full widen plan:" not in errf.read_text() and time.monotonic() < deadline and p.poll() is None:
            time.sleep(0.2)
        time.sleep(1.5)
        p.send_signal(signal.SIGUSR1)
        p.wait(timeout=200)
    assert p.returncode == 143
    err = (tmp / "out" / "run.log").read_text()
    i_stage = err.index("=== step full-widen ===")
    assert "pair stop" in err[i_stage:] and "=== teardown " in err[i_stage:]
    fwd = tmp / "out" / "full-widen"
    assert (fwd / "plan.json").exists() and not list(fwd.glob("objects-*.yaml"))
    _no_password_left(tmp / "out")


def test_a_job_that_ends_unarmed_is_recorded_and_nothing_is_widened(harness):  # noqa: F811
    sp = FakeSnowplow(never_serve=True)
    p, ev, state, tmp = _real(harness, sp, FAKE_FW_JOB="ends_unarmed", _name="fw-unarmed")
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert "apply crd-widened.yaml" not in ev and "cp" in ev and ev[-1] == "delete crd"
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert not rec["passed"] and rec["outcome"] == "not_serving_within_bound"


def test_a_job_that_never_arms_is_waited_for_only_to_its_deadline(harness):  # noqa: F811
    t0 = time.monotonic()
    p, ev, state, tmp = _real(harness, FAKE_FW_JOB="never", PREFLIGHT_TIMEOUT_S="2", ROLLOUT_TIMEOUT_S="121")
    took = time.monotonic() - t0
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "full widen FAILED: the fullwiden606 Job did not finish" in log
    assert "apply crd-widened.yaml" not in ev and ev[-1] == "delete crd"
    assert took < 2 + 121 + 120, took                    # the armed deadline, plus the run around it


def test_the_crds_established_wait_is_bounded(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, FAKE_FW_WAIT_HANG="60", PREFLIGHT_TIMEOUT_S="3")
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "full widen FAILED: the CRD fullwidens.fullwiden.bench.krateo.io not Established within 3s" in log
    assert "apply objects-0001.yaml" not in ev and ev[-1] == "delete crd"
    _no_password_left(tmp / "out")


def test_a_relist_bound_past_the_budget_term_is_refused_before_anything_is_created(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, ROLLOUT_TIMEOUT_S="100")
    assert p.returncode != 0
    assert "a bound exceeds the run budget's 100s term" in (tmp / "out" / "run.log").read_text()
    assert not [x for x in ev if x.startswith("apply ")]


def test_a_go_for_another_run_is_refused_before_any_cloud_call(harness):  # noqa: F811
    e, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(e, BENCH_1004_GO="20200101-" + RUN_NAME),
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 2 and "refusing: BENCH_1004_GO must be" in p.stderr
    calls = (state / "calls.jsonl").read_text().splitlines() if (state / "calls.jsonl").exists() else []
    assert not [c for c in calls if json.loads(c)["tool"] == "gcloud"]


def test_every_kind_the_stage_applies_is_in_the_schema_check(harness):  # noqa: F811
    from bench import crd_schema
    p, ev, state, tmp = _real(harness)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    applied = {tuple(json.loads(x)) for x in (state / "applied-kinds.jsonl").read_text().splitlines()}
    custom = {f"{k}.{api.rpartition('/')[0]}/{api.rpartition('/')[2]}" for api, k in applied
              if not crd_schema._BUILTIN_GROUP.search(api.rpartition("/")[0])}
    checked = {e["kind"] for e in json.loads((tmp / "out" / "crd-check.json").read_text())["kinds"]}
    assert f"{fw.KIND}.{fw.GROUP}/{fw.VERSION}" in custom and custom <= checked, custom - checked


# ─── R8: the pair ────────────────────────────────────────────────────────────

def test_the_pair_refuses_the_stage_on_one_arm(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    today = datetime.now(UTC).strftime("%Y%m%d")
    p = _launch(repo, env, BENCH_1004_GO_A=f"{today}-{NA}")
    assert p.returncode == 2 and "set both (the full-widen stage on both arms) or neither" in p.stderr
    assert not _calls(fake)


def test_the_pair_refuses_an_unsuffixed_go_and_a_go_for_another_run(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    today = datetime.now(UTC).strftime("%Y%m%d")
    p = _launch(repo, env, BENCH_1004_GO=f"{today}-{NA}")
    assert p.returncode == 2 and "BENCH_1004_GO is per run" in p.stderr
    p = _launch(repo, env, BENCH_1004_GO_A=f"{today}-{NA}", BENCH_1004_GO_B=f"{today}-{NA}")
    assert p.returncode == 2 and "BENCH_1004_GO_B must be" in p.stderr
    assert not _calls(fake)


def test_the_pair_hands_each_run_its_own_go(pairbed, va):  # noqa: F811
    repo, fake, env, tmp = pairbed
    _verdicts(fake, va)
    today = datetime.now(UTC).strftime("%Y%m%d")
    p = _launch(repo, env, BENCH_1004_GO_A=f"{today}-{NA}", BENCH_1004_GO_B=f"{today}-{NB}")
    assert p.returncode == 0, p.stderr[-3000:]
    calls = [c for c in _calls(fake) if " dry=" in c]
    assert len(calls) == 4
    for c in calls:
        assert f"fw={today}-{c.split()[0]} " in c, c
