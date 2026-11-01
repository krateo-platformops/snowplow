"""#1004 — #870's full-widen worst case as its own stage (bench/full_widen.py,
run606.sh s_full_widen, run606_pair.sh BENCH_1004_GO_A / _B).

Rules, each with an arm (and a logic-only mutation shown RED, see the PR):
  R1  build_and_swap ends ONLY when the stage GVR's own outcome moved AND both
      [schema] gauges are 0 (ruling Q1): gauges at 0 before the CRD event
      never end it; an open window never ends it.
  R2  relist ends ONLY when the post-sync re-fire caught up AND the stage's
      read serves again (ruling Q2).
  R3  the widen waits for a read that serves: registered, 200, no fall-through
      (ruling Q4); a GVR that never serves ends the Job unarmed.
  R4  every phase is bounded by its mechanism's bound, recorded with its source;
      "bound reached" is an outcome, never a pass (ruling Q3).
  R5  the bounds: relist = max(relistPostSyncWait, the live
      DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS else 120) + one sample;
      build_and_swap = ROLLOUT_TIMEOUT_S.
  R6  inputs from what the run installs: N = the data step's proof (= SCALE),
      bodies = the compositions', batching = the data step's.
  R7  the stage runs after the evaluate step and "run complete", before
      teardown, only with BENCH_1004_GO for this run; its terms are in the
      run budget; every apply goes through kapply (tests/test_apply_size.py's
      inventory); every wait is bounded; its cleanup deletes the CRD.
  R8  the pair: BENCH_1004_GO_A and _B, both or neither, each a go for its run.
"""

from __future__ import annotations

import json
import os
import subprocess
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


# ─── a fake snowplow: the families a build publishes, by mechanism ───────────

class FakeSnowplow:
    """Answers /debug/vars and the stage's read. The read registers the GVR
    and falls through (not-synced) for `sync_reads` reads; the widen (arm())
    starts the rebuild: detected on the next sample, its end after
    `build_samples`; `window_after` samples of open window after the swap;
    `never_end`: the build never installs / the relist never syncs."""

    def __init__(self, mech="build_and_swap", *, sync_reads=2, build_samples=3, window_after=0,
                 never_end=False, never_serve=False, relist_unservable=2, publish=True, n=4):
        self.mech, self.sync_reads, self.build_samples = mech, sync_reads, build_samples
        self.window_after, self.never_end, self.never_serve = window_after, never_end, never_serve
        self.relist_unservable, self.publish, self.n = relist_unservable, publish, n
        self.t = 1000.0
        self.reads = 0
        self.registered = False
        self.widened = False
        self.k = 0                     # samples since the widen
        self.cells = {}
        self.relists = self.postsync = self.swaps = self.bridge_runs = 0
        self.installed_at = None
        self.heap = 2000 * MB
        self.num_gc, self.pause_ns = 100, 5_000_000

    # Watch's injected clock
    def clock(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def arm(self):
        self.widened = True

    def login(self):
        return "tok"

    def _cell(self, reason):
        c = f"restactions|{fw.GVR}|{reason}"
        self.cells[c] = self.cells.get(c, 0) + 1

    def _servable(self):
        if self.never_serve:
            return False
        if self.reads <= self.sync_reads:
            return False
        if self.mech == "relist" and self.widened and self.k >= 1:
            return self.postsync >= 1 and self.k >= self.build_samples + self.relist_unservable
        return True

    def _read(self):
        self.reads += 1
        self.registered = True
        if not self._servable():
            self._cell("informer-fallthrough-not-synced")

    def _advance(self):
        """One sample's worth of rebuild progress (run on the first vars read of a tick)."""
        if not self.widened:
            return
        self.k += 1
        if self.k == 1:
            self.relists += 1
            if self.mech == "relist":
                self.bridge_runs += 1
        if self.never_end:
            self.heap += 50 * MB
            return
        if self.k < self.build_samples:
            self.heap += 400 * MB          # two copies coexist
            self.num_gc += 2
            self.pause_ns += 2_000_000
        elif self.k == self.build_samples:
            if self.mech == "build_and_swap":
                self.swaps += 1
            else:
                self.postsync += 1
            self.installed_at = self.k
            self.heap -= 300 * MB

    def _ks(self):
        open_ = self.widened and self.k >= 1 and (self.installed_at is None
                                                  or self.k < self.installed_at + self.window_after)
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
                                                                   "schema": float(self.k) if open_ else 0.0},
                "store_swap_ledger_owed": {"divergence": 0, "schema": 0}}

    def vars(self):
        v = {"memstats": {"HeapInuse": self.heap, "HeapAlloc": self.heap - 100 * MB, "HeapSys": self.heap,
                          "Sys": self.heap + 500 * MB, "NumGC": self.num_gc, "PauseTotalNs": self.pause_ns,
                          "GCCPUFraction": 0.01, "PauseNs": [1_000_000] * 256},
             fw.REGISTERED_F: {"count": 1, "gvrs": [fw.GVR_REGISTERED] if self.registered else ["a/v1/b"]},
             fw.FALLTHROUGH_F: dict(self.cells) | {"restactions|other/v1, Resource=x|informer-fallthrough-not-synced": 9}}
        crd = {"schema_relists_fired": self.relists, "schema_unchanged": 0}
        if self.mech == "build_and_swap":
            v[fw.SWAP_F] = {"store_swaps_total": self.swaps, "store_swap_superseded_total": 0,
                            "store_swap_m_keys_total": self.n * self.swaps,
                            "store_swap_deliveries_total": self.n * self.swaps, "store_swap_regressed_keys": 0,
                            "store_swap_schema_unowned_total": 0, "store_swap_reused_objects_total": 0}
            v[fw.SWAP_KS_F] = self._ks()
            v[fw.SWAPS_BY_GVR_F] = ({fw.GVR: {"installed": self.swaps, "superseded": 0, "abort_clean": 0,
                                              "abort_refused": 0, "abort_teardown": 0}} if self.swaps else {})
            v[fw.LIST_INCREMENTAL_F] = {"unanchors": {}}
        elif self.mech == "relist":
            crd |= {"relist_bridge_runs_total": self.bridge_runs, "relist_bridge_enqueued_total": 0,
                    "relist_bridge_timeout_total": 0, "relist_bridge_aborted_total": 0,
                    "relist_dirtymark_postsync_total": self.postsync, "relist_postsync_timeout_total": 0}
            v[fw.VERIFY_F] = {"store_repairs_pending": 0, "store_repairs_fired_total": 0}
        v[fw.CRD_F] = crd
        if not self.publish:
            v.pop(fw.FALLTHROUGH_F)
        return v

    def get(self, path, token):
        self.t += 0.01
        if path == "/debug/vars":
            return 200, json.dumps(self.vars()).encode()
        assert path.startswith("/call?"), path
        self._advance()
        self._read()
        return 200, b"{}"


def watch(sp: FakeSnowplow, bounds=None, sample_s=1.0):
    recs = []
    b = bounds or {"relist": {"s": 121.0, "source": "test relist"},
                   "build_and_swap": {"s": 60.0, "source": "test swap"}}
    armed_at = []

    def arm():
        armed_at.append(sp.reads)
        sp.arm()
    rc = fw.Watch(get=sp.get, login=sp.login, emit=recs.append, arm=arm, bounds=b, sample_s=sample_s,
                  clock=sp.clock, sleep=sp.sleep).run()
    return rc, recs, armed_at


def end_of(recs):
    return next(r for r in recs if r["kind"] == "end")


# ─── R1: build_and_swap's end ────────────────────────────────────────────────

def test_a_swap_ends_when_its_gvr_outcome_moved_and_both_schema_gauges_are_zero():
    sp = FakeSnowplow("build_and_swap", build_samples=3)
    rc, recs, armed = watch(sp)
    e = end_of(recs)
    assert rc == 0 and e["outcome"] == "ended", e
    assert "snowplow_store_swaps_by_gvr" in e["signal"] and e["detected"] is not None
    assert sp.swaps == 1 and armed                                      # armed, widened, swapped
    # the end is the FIRST sample at which every part holds
    settle = [r for r in recs if r["kind"] == "sample" and r["phase"] == "settle"]
    assert len(settle) == 3


def test_gauges_at_zero_before_the_crd_event_never_end_a_swap():
    """Ruling Q1's trap: keys_open / window_open are 0 BEFORE the build is seen.
    A build that never installs (no outcome of the stage GVR) runs to its bound."""
    sp = FakeSnowplow("build_and_swap", never_end=True)
    rc, recs, _ = watch(sp, bounds={"relist": {"s": 121.0, "source": "x"}, "build_and_swap": {"s": 10.0, "source": "y"}})
    e = end_of(recs)
    assert e["outcome"] == "bound_reached" and e["bound_s"] == 10.0, e
    armed_t = next(r["t"] for r in recs if r["kind"] == "armed")
    assert armed_t + 10.0 <= e["t"] <= armed_t + 10.0 + 1.5                # bounded: the bound plus one sample


def test_an_open_schema_window_after_the_swap_does_not_end_it():
    sp = FakeSnowplow("build_and_swap", build_samples=2, window_after=3)
    rc, recs, _ = watch(sp)
    e = end_of(recs)
    assert e["outcome"] == "ended"
    assert sp.k == 2 + 3                       # the swap at k=2, the window closed at k=5


def test_build_ended_swap_needs_all_three_parts():
    sp = FakeSnowplow("build_and_swap")
    armed = sp.vars()
    sp.widened, sp.k, sp.swaps, sp.installed_at = True, 3, 1, 3
    now = sp.vars()
    assert fw.build_ended("build_and_swap", armed, now, {})[0]
    open_ = json.loads(json.dumps(now))
    open_[fw.SWAP_KS_F]["store_swap_known_stale_keys_open"]["schema"] = 1
    assert not fw.build_ended("build_and_swap", armed, open_, {})[0]
    win = json.loads(json.dumps(now))
    win[fw.SWAP_KS_F]["store_swap_known_stale_window_open_seconds_max"]["schema"] = 0.5
    assert not fw.build_ended("build_and_swap", armed, win, {})[0]
    assert not fw.build_ended("build_and_swap", armed, armed, {})[0]     # nothing moved: gauges 0 is not an end
    other = json.loads(json.dumps(now))
    other[fw.SWAPS_BY_GVR_F] = {"other/v1, Resource=x": {"installed": 1}}
    assert not fw.build_ended("build_and_swap", armed, other, {})[0]     # another GVR's build: not ours


# ─── R2: relist's end ────────────────────────────────────────────────────────

def test_a_relist_ends_when_the_refire_caught_up_and_the_read_serves_again():
    sp = FakeSnowplow("relist", build_samples=3, relist_unservable=2)
    rc, recs, _ = watch(sp)
    e = end_of(recs)
    assert e["outcome"] == "ended", e
    assert sp.k == 3 + 2                       # caught up at k=3; servable from k=5


def test_build_ended_relist_needs_the_refire_and_a_serving_read():
    sp = FakeSnowplow("relist")
    armed = sp.vars()
    sp.relists, sp.postsync, sp.bridge_runs = 1, 1, 1
    now = sp.vars()
    ok = {"status": 200, "moved_window": {}}
    assert fw.build_ended("relist", armed, now, ok)[0]
    assert not fw.build_ended("relist", armed, now, {"status": 200, "moved_window": {"restactions|x": 1}})[0]
    assert not fw.build_ended("relist", armed, now, {"status": 500, "moved_window": {}})[0]
    assert not fw.build_ended("relist", armed, now, {"status": 200, "moved_window": None})[0]   # unpublished
    sp.postsync = 0
    assert not fw.build_ended("relist", armed, sp.vars(), ok)[0]          # re-fire not caught up
    sp.relists = 0
    assert not fw.build_ended("relist", armed, sp.vars(), ok)[0]          # nothing fired


# ─── R3: the widen waits for a read that serves ──────────────────────────────

def test_the_widen_waits_for_a_read_with_no_fall_through():
    sp = FakeSnowplow("build_and_swap", sync_reads=4)
    rc, recs, armed = watch(sp)
    assert armed == [5]                        # 4 reads fell through (the copy was listing); the 5th served
    reg = [r for r in recs if r["kind"] == "sample" and r["phase"] == "register"]
    assert [bool(r["probe"]["moved"]) for r in reg] == [True] * 4 + [False]


def test_a_gvr_that_never_serves_ends_the_job_unarmed_within_its_bound():
    sp = FakeSnowplow("build_and_swap", never_serve=True)
    rc, recs, armed = watch(sp, bounds={"relist": {"s": 121.0, "source": "x"}, "build_and_swap": {"s": 8.0, "source": "y"}})
    e = end_of(recs)
    assert rc == 3 and armed == [] and e["outcome"] == "not_serving_within_bound", e
    assert e["t"] - recs[0]["t"] <= 8.0 + 1.5


def test_serves_refuses_an_unregistered_gvr_and_an_unpublished_meter():
    v = FakeSnowplow().vars()
    assert fw.serves({"status": 200, "moved": {}}, v)[1] == "the GVR is not registered yet"
    v[fw.REGISTERED_F]["gvrs"] = [fw.GVR_REGISTERED]
    assert fw.serves({"status": 200, "moved": {}}, v)[0]
    assert not fw.serves({"status": 200, "moved": None}, v)[0]
    assert not fw.serves({"status": 403, "moved": {}}, v)[0]


def test_a_build_publishing_no_mechanism_ends_the_job():
    sp = FakeSnowplow("none")
    rc, recs, armed = watch(sp)
    assert rc == 3 and end_of(recs)["outcome"] == "mechanism_unknown" and not armed


def test_every_read_is_a_new_l1_key():
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


# ─── R4 + evaluate: the record ───────────────────────────────────────────────

PLAN = {"n": 4, "namespaces": 2, "per_namespace": 2, "batches": 2, "batch": 200, "workers": 32,
        "object_bytes": 1400, "composition_bytes": 1420, "crd": fw.CRD_NAME, "gvr": fw.GVR, "derived_from": {}}
PODS = {"snowplow-abc": 0}


@pytest.mark.parametrize("mech", ["build_and_swap", "relist"])
def test_a_build_that_ended_passes_with_its_rows_and_the_heap_peak(mech):
    rc, recs, _ = watch(FakeSnowplow(mech))
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after=PODS)
    assert rec["passed"], rec["fail_reasons"]
    assert rec["mechanism"] == mech and rec["outcome"] == "ended"
    assert {r["id"] for r in rec["assertions"]} == {f"B{i}" for i in range(1, 8)}
    m = rec["memory"]
    assert m["heap_inuse_mb_armed"] == 2000.0 and m["heap_inuse_mb_peak"] == 2800.0
    assert m["heap_inuse_mb_peak_over_armed"] == 800.0 and m["gc_cycles"] == 4
    if mech == "relist":
        assert rec["relist_bridge_timeout_total"] == 0
    else:
        assert "report_only" in rec and "B3" in [r["id"] for r in rec["assertions"] if r.get("report_only")]
    assert rec["bound"]["source"].startswith("test ")
    assert any(ln.startswith("full widen: PASS") for ln in fw.report_lines(rec))


def test_bound_reached_is_never_a_pass():
    rc, recs, _ = watch(FakeSnowplow("build_and_swap", never_end=True),
                        bounds={"relist": {"s": 121.0, "source": "x"}, "build_and_swap": {"s": 5.0, "source": "y"}})
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after=PODS)
    assert not rec["passed"] and rec["outcome"] == "bound_reached"
    assert any(r.startswith("outcome bound_reached") for r in rec["fail_reasons"])


def test_a_restarted_snowplow_fails_the_stage():
    rc, recs, _ = watch(FakeSnowplow())
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after={"snowplow-abc": 1})
    assert not rec["passed"] and "restarted or was replaced" in rec["fail_reasons"][0]
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after={"snowplow-def": 0})
    assert not rec["passed"]


def test_a_stream_without_its_end_fails():
    rc, recs, _ = watch(FakeSnowplow())
    rec = fw.evaluate([r for r in recs if r["kind"] != "end"], plan_=PLAN, pods_before=PODS, pods_after=PODS)
    assert not rec["passed"] and "no end record" in rec["fail_reasons"][0]


def test_a_failing_row_fails_the_stage():
    sp = FakeSnowplow("build_and_swap")
    rc, recs, _ = watch(sp)
    for r in recs:                           # a whole-type mark at the swap (B4's falsifier)
        if r["kind"] == "sample" and r["phase"] == "settle":
            r["vars"][fw.LIST_INCREMENTAL_F] = {"unanchors": {"type_event": 1}}
    rec = fw.evaluate(recs, plan_=PLAN, pods_before=PODS, pods_after=PODS)
    assert not rec["passed"] and "B4" in rec["failed_rows"]


# ─── R5: the bounds ──────────────────────────────────────────────────────────

def _deploy(env):
    return {"spec": {"template": {"spec": {"containers": [{"name": "snowplow", "env": env}]}}}}


@pytest.mark.parametrize("env,want", [([], 121.0), ([{"name": fw.RELIST_BRIDGE_ENV, "value": "300"}], 301.0),
                                      ([{"name": fw.RELIST_BRIDGE_ENV, "value": "60"}], 121.0),
                                      ([{"name": fw.RELIST_BRIDGE_ENV, "value": "0"}], 121.0),
                                      ([{"name": fw.RELIST_BRIDGE_ENV, "value": "x"}], 121.0)])
def test_the_relist_bound_is_snowplows_own_give_up_plus_one_sample(env, want):
    b = fw.bounds(_deploy(env), 2700)
    assert b["relist"]["s"] == want
    assert "relistPostSyncWait 120 s" in b["relist"]["source"] and fw.RELIST_BRIDGE_ENV in b["relist"]["source"]
    assert b["build_and_swap"] == {"s": 2700.0, "source": b["build_and_swap"]["source"]}
    assert b["build_and_swap"]["source"].startswith("ROLLOUT_TIMEOUT_S 2700 s")


# ─── R6: what the stage creates ──────────────────────────────────────────────

def test_the_plan_takes_n_from_the_proof_and_the_bodies_and_batching_from_the_data_step(tmp_path):
    p = fw.plan(n=600, scale=600, namespaces=2, out_dir=str(tmp_path), user_prefix="scaleuser-", password="pw",
                snowplow_url="http://s", authn_url="http://a", bounds=fw.bounds({}, 2700), linger_s=5)
    assert p["batch"] == data_manifests.COMPOSITION_APPLY_BATCH and p["workers"] == data_manifests.COMPOSITION_APPLY_WORKERS
    seen = []
    for f in p["files"]:
        docs = list(yaml.safe_load_all(open(f)))
        assert len(docs) <= data_manifests.COMPOSITION_APPLY_BATCH
        assert len({d["metadata"]["namespace"] for d in docs}) == 1              # one namespace per batch
        seen += docs
    assert len(seen) == 600 and len({(d["metadata"]["namespace"], d["metadata"]["name"]) for d in seen}) == 600
    d = seen[-1]
    comp = yaml.safe_load(data_manifests.composition_yaml(d["metadata"]["namespace"], d["metadata"]["name"]))
    assert d["spec"] == comp["spec"] and d["metadata"] == comp["metadata"]        # the composition's body, verbatim
    assert (d["apiVersion"], d["kind"]) == (f"{fw.GROUP}/{fw.VERSION}", fw.KIND)
    with pytest.raises(ValueError, match="budgeted SCALE"):
        fw.plan(n=599, scale=600, namespaces=2, out_dir=str(tmp_path), user_prefix="u", password="pw",
                snowplow_url="s", authn_url="a", bounds={}, linger_s=1)
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
    user = next(d for d in docs if d["kind"] == "User")
    assert user["spec"]["groups"] == [fw.IDENTITY_GROUP]


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
    p = fw.plan(n=400, scale=400, namespaces=2, out_dir=str(tmp_path), user_prefix="scaleuser-", password="pw",
                snowplow_url="http://s", authn_url="http://a", bounds=fw.bounds({}, 2700), linger_s=5)
    objs = [o for f in ["job.yaml", "probe.yaml", "crd.yaml", "crd-widened.yaml"] for o in
            _objects((tmp_path / f).read_text())] + _objects(Path(p["files"][0]).read_text())
    assert objs and all(check(o, client_side=False) == [] for o in objs), [check(o, client_side=False) for o in objs]


def test_the_job_ships_what_it_imports_and_runs_the_watch():
    text = fw.job_yaml(snowplow_url="http://s", authn_url="http://a", user="u", password="pw",
                       bounds={"relist": {"s": 1, "source": "x"}}, linger_s=5)
    cm, job = [d for d in yaml.safe_load_all(text) if d]
    assert set(cm["data"]) == {"full_widen.py", "load606.py", "store_rebuild.py"}
    assert cm["data"]["full_widen.py"] == (BENCH_DIR / "bench" / "full_widen.py").read_text()
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    assert "python3 -u -m bench.full_widen watch" in script and f"echo \"$rc\" > {fw.EXITED}" in script
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
    assert "=== step" not in log[i_fw + 10:]                          # nothing measured after it


def test_without_the_go_there_is_no_stage_and_no_budget_term(tmp_path):
    off = _dry(tmp_path / "off")
    on = _dry(tmp_path / "on", BENCH_1004_GO="x")
    assert off.returncode == 0 and on.returncode == 0, (off.stderr + on.stderr)[-3000:]
    assert "=== step full-widen ===" not in (tmp_path / "off" / "out" / "run.log").read_text()
    b_off, line_off = _budget(off.stderr)
    b_on, line_on = _budget(on.stderr)
    # objects (data 2400) + job (preflight 300 + 2 x reboot 2700) + collect read 300 + delete (data 2400)
    assert b_on - b_off == 2400 + (300 + 2 * 2700) + 300 + 2400
    assert "full widen 10800 (objects 2400 + job 5700 (preflight 300 + 2x2700) + collect 300 + delete 2400)" in line_on
    assert "full widen" not in line_off


def test_the_dry_pass_validates_the_stage_documents(tmp_path):
    p = _dry(tmp_path, BENCH_1004_GO="x")
    assert p.returncode == 0, p.stderr[-3000:]
    res = json.loads((tmp_path / "out" / "crd-check.json").read_text())
    kinds = {e["kind"]: e for e in res["kinds"]}
    assert kinds[f"{fw.KIND}.{fw.GROUP}/{fw.VERSION}"]["status"] == "valid"
    assert "the full-widen stage's identity" in kinds["User.basic.authn.krateo.io/v1alpha1"]["applied_by"]


# ─── R7: run606 — the real sequence under fake tools ─────────────────────────

def _stream(tmp: Path, sp: FakeSnowplow, name="fw-stream.jsonl", **kw) -> str:
    rc, recs, _ = watch(sp, **kw)
    f = tmp / name
    f.write_text("".join(json.dumps(r) + "\n" for r in recs + [{"kind": "exit", "rc": rc, "t": 0}]))
    return str(f)


def _real(harness_, **env):
    e, state, ambient, tmp = harness_
    today = datetime.now(UTC).strftime("%Y%m%d")
    e = dict(e, BENCH_1004_GO=f"{today}-{RUN_NAME}", FAKE_FW_STREAM=_stream(tmp, FakeSnowplow(n=4)))
    e.update(env)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e, capture_output=True, text=True, timeout=300)
    ev = [json.loads(x)[1] for x in (state / "fw-events").read_text().splitlines()] \
        if (state / "fw-events").exists() else []
    return p, ev, state, tmp


def test_the_stage_runs_after_run_complete_widens_only_once_served_and_cleans_up(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness)
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    log = (tmp / "out" / "run.log").read_text()
    assert log.index("run complete: ") < log.index("=== step full-widen ===")
    # in order: the CRD, the objects (2 batches at SCALE 4 / NAMESPACES 2), the read, the Job;
    # the widen only after /out/armed answered yes; the copy after /out/exited; the CRD deleted last
    applies = [x for x in ev if x.startswith("apply ")]
    assert applies[0] == "apply crd.yaml" and applies[-3:] == ["apply probe.yaml", "apply job.yaml",
                                                               "apply crd-widened.yaml"]
    assert sorted(applies[1:-3]) == ["apply objects-0001.yaml", "apply objects-0002.yaml"]
    assert ev.index("exec /out/armed yes") < ev.index("apply crd-widened.yaml")
    assert ev.index("apply crd-widened.yaml") < ev.index("exec /out/exited yes") < ev.index("cp")
    assert ev[-1] == "delete crd" and any(x.startswith("delete ") and "job,configmap" in x for x in ev)
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert rec["passed"] and rec["plan"]["n"] == 4 and rec["outcome"] == "ended", rec.get("fail_reasons")
    assert "full widen: PASS" in log and "full widen: CRD fullwidens.fullwiden.bench.krateo.io and its objects deleted" in log
    assert not list((tmp / "out" / "full-widen").glob("objects-*.yaml"))          # N bodies not left on disk
    assert SCALEUSER_PASSWORD not in log and SCALEUSER_PASSWORD not in (tmp / "out" / "full-widen" / "probe.yaml").read_text()
    assert SCALEUSER_PASSWORD not in (tmp / "out" / "full-widen" / "job.yaml").read_text()


def test_missing_objects_fail_the_stage_before_any_widen_and_clean_up(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, FAKE_FW_SHORT="1")
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "run complete: " in log                       # the run completed: the pair reads it as a measurement
    assert "full widen FAILED: 3 of 4 objects exist after the batches" in log
    assert "apply job.yaml" not in ev and "apply crd-widened.yaml" not in ev and ev[-1] == "delete crd"


def test_a_job_that_ends_unarmed_is_recorded_and_nothing_is_widened(harness):  # noqa: F811
    e, state, ambient, tmp = harness
    stream = _stream(tmp, FakeSnowplow(never_serve=True), "fw-unarmed.jsonl",
                     bounds={"relist": {"s": 121.0, "source": "x"}, "build_and_swap": {"s": 3.0, "source": "y"}})
    p, ev, state, tmp = _real(harness, FAKE_FW_JOB="ends_unarmed", FAKE_FW_STREAM=stream)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    assert "apply crd-widened.yaml" not in ev and "cp" in ev and ev[-1] == "delete crd"
    rec = json.loads((tmp / "out" / "full-widen.json").read_text())
    assert not rec["passed"] and rec["outcome"] == "not_serving_within_bound"


def test_the_crds_established_wait_is_bounded(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, FAKE_FW_WAIT_HANG="60", PREFLIGHT_TIMEOUT_S="3")
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "full widen FAILED: the CRD fullwidens.fullwiden.bench.krateo.io not Established within 3s" in log
    assert "apply objects-0001.yaml" not in ev and ev[-1] == "delete crd"


def test_a_relist_bound_past_the_budget_term_is_refused_before_anything_is_created(harness):  # noqa: F811
    p, ev, state, tmp = _real(harness, ROLLOUT_TIMEOUT_S="100")
    assert p.returncode != 0
    log = (tmp / "out" / "run.log").read_text()
    assert "a mechanism's bound exceeds the run budget's 100s term" in log
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
        name = c.split()[0]
        assert f"fw={today}-{name} " in c, c
