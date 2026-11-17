"""#1422, the owner's ruling of 2026-11-16 ("warm up until settled, then measure"), and #1508 (page timeouts
and lost sessions), as launch A run 27c N2 showed them both: GKE resized N2's control plane during its measure
(15:45-15:50Z), snowplow's apiserver dials stalled ~10 s, one fresh sign-in timed out and the bench navigated its
unsigned context 10 more times, and two sessions ended on an uncaught login-form error; the verdict said
INVALID through browser.cpu_starvation.

A. the settle (bench/clusterops.py): its terms derived from the recorded operations; settled only on a readable
   listing with nothing running and nothing in GKE's last two checks under the warm-up load; not settled by the
   deadline = INVALID by environment, not measured; run606's settle step between the data and the reboot.
B. evaluate: an operation overlapping the measure, or an overlap not decidable, is INVALID by environment; so is a
   settle record that is missing or not SETTLED.
C. #1508: a censored timeout is never flipped by the stall; a failed sign-in is reported once and its unsigned
   context's navigations are not page timeouts; a session that ended early is INVALID by environment, named.

Every INVALID path here has a case that turns RED when its logic alone is mutated (each test names the mutation
it guards against).
"""

from __future__ import annotations

import copy
import gzip
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from bench import clusterops, gke, k6eval, load606
from tests.fake_gke_ops import operation
from tests.test_k6_830 import (BENCH_DIR, _browser_points, _browser_summary, _check, _cpu_file, _k6_points,
                               _k6_summary, _point, _write_out, _write_viewer)
from tests.test_run606_real_sequence import RUN_NAME, harness  # noqa: F401  (fixture)

NAME = "snowplow-bench-settle"
SPEC = gke.default_spec(NAME)
T0 = 1794843000.0                      # 2026-11-16T14:50:00Z
CREATE = operation("CREATE_CLUSTER", NAME, start=T0 + 4, end=T0 + 250)


# ─── A. the terms, derived from the evidence ────────────────────────────────

def test_the_settle_terms_are_derived_from_the_recorded_operations():
    cad = clusterops.cadence(clusterops.load_evidence()[0])
    # the cadence: run 27c N2's checks 15:40:24 (31 s) -> 15:45:25, the longest interval after a check that ended
    # within the shortest one (300.0 s, run 26j N1B); the burst: run 26j N1B (8ef7e3ec), two resizes, 1074.5 s
    assert (cad["period_s"], cad["burst_s"]) == (300.334, 1074.483), cad
    assert cad["window_s"] == pytest.approx(2 * 300.334, abs=1e-3)
    assert cad["deadline_s"] == pytest.approx(2 * cad["window_s"] + cad["burst_s"], abs=1e-3)
    assert cad["poll_s"] == pytest.approx(300.334 / 5, abs=1e-3)
    assert "snowplow-bench-run27n2-40d9fc36" in cad["from"]["period"]
    assert "snowplow-bench-run26jn1b-8ef7e3ec" in cad["from"]["burst"]


def _cluster(name, *spans):
    return {"cluster": name, "operations": [
        {"type": "RESIZE_CLUSTER", "start": clusterops.stamp(T0 + a), "end": clusterops.stamp(T0 + b)} for a, b in spans]}


def test_the_terms_follow_the_evidence_and_refuse_without_a_cadence():
    """Mutations: a fixed period, max instead of the cadence filter, a burst that ignores chains: each RED."""
    base = {"clusters": [_cluster("a", (0, 30), (300, 301), (900, 1400), (1430, 1431))]}
    cad = clusterops.cadence(base)
    # (0,30)->(300,301): cadence (the first ended within the shortest gap); (300,301)->(900,..): two slots on, a
    # silent check between them: not cadence; (900,1400)->(1430,1431): not (a 500 s resize held the next check
    # back); the burst: 900..1431 (the next check started 30 s after the resize ended)
    assert cad["period_s"] == 300.0 and cad["burst_s"] == 531.0
    longer = {"clusters": base["clusters"] + [_cluster("b", (0, 1), (400, 900), (920, 1600), (1650, 1651))]}
    cad2 = clusterops.cadence(longer)
    # b: (0,1)->(400,..) is a 400 s cadence pair: the period follows; its chain 0..1651 is the longest burst
    assert cad2["period_s"] == 400.0 and cad2["burst_s"] == 1651.0
    assert cad2["deadline_s"] == 2 * 800.0 + 1651.0
    with pytest.raises(ValueError, match="no two consecutive checks"):
        clusterops.cadence({"clusters": [_cluster("c", (0, 300))]})


# ─── A. the decision ────────────────────────────────────────────────────────

def _rec(*ops, readable=True):
    if not readable:
        return {"readable": False, "unreadable": "could not be read: rc=1", "operations": None}
    return {"readable": True, "operations": [
        {"id": f"op-{i}", "type": t, "status": "DONE" if b is not None else "RUNNING", "target": "cluster",
         "start": clusterops.stamp(a), "end": clusterops.stamp(b) if b is not None else None, "start_t": a, "end_t": b}
        for i, (t, a, b) in enumerate(ops)]}


W = 600.0
NOW = T0 + 5000


@pytest.mark.parametrize("rec,load_since,state,said", [
    # an unreadable listing is never settled (fail closed)
    (_rec(readable=False), NOW - 3600, clusterops.NOT_READ, "could not be read"),
    # a resize running now
    (_rec(("RESIZE_CLUSTER", NOW - 100, None)), NOW - 3600, clusterops.WAITING, "running: RESIZE_CLUSTER"),
    # a 1 s check inside the last two checks' window: not taken for a no-op (strict)
    (_rec(("RESIZE_CLUSTER", NOW - 200, NOW - 199)), NOW - 3600, clusterops.WAITING, "within the last 600 s"),
    # a resize that ended inside the window
    (_rec(("RESIZE_CLUSTER", NOW - 900, NOW - 500)), NOW - 3600, clusterops.WAITING, "within the last 600 s"),
    # the warm-up load has not yet run for the whole window
    (_rec(), NOW - 599, clusterops.WAITING, "has run 599 s of the 600 s"),
    # nothing in the window, nothing running, the load ran it whole
    (_rec(("RESIZE_CLUSTER", NOW - 1200, NOW - W)), NOW - W, clusterops.SETTLED, "nothing ran in the last 600 s"),
])
def test_settled_means_nothing_runs_nothing_in_the_last_two_checks_and_the_load_ran_them(rec, load_since, state, said):
    got, why = clusterops.settle_decision(rec, load_since=load_since, now=NOW, window=W)
    assert got == state and said in why, (got, why)


# ─── A. the loop, on a simulated clock and a simulated GKE ──────────────────

class World:
    """GKE's listing at the simulated time (operations not yet started are not listed; one still running has
    no end), the warm-up pod, and the clock the settle reads and sleeps on."""

    def __init__(self, ops, *, load_at=T0 + 3600, pod="run", listing="ok"):
        self.t, self.ops, self.load_at, self.pod, self.listing = T0 + 3590, ops, load_at, pod, listing
        self.calls: list[tuple[list, dict]] = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def run(self, argv, **kw):
        self.calls.append((argv, kw.get("env") or {}))
        if self.listing == "fails":
            kw["stderr_out"].append("ERROR: (gcloud.container.operations.list) ResponseError: code=503\n")
            return 1, ""
        if self.listing == "empty":                # gcloud failing open: rc 0 and []
            kw["stderr_out"].append("")
            return 0, "[]"
        ops = [CREATE]
        for kind, a, b in self.ops:
            if a <= self.t:
                ops.append(operation(kind, NAME, start=a, end=b if b is not None and b <= self.t else None))
        kw["stderr_out"].append("")
        return 0, json.dumps(ops)

    def pods(self):
        if self.pod == "never" or self.t < self.load_at:
            return {"items": []}, ""
        # "unread": every read of the pod from the load's start + 900 s on fails (the API did not answer)
        if self.pod == "unread" and self.t > self.load_at + 900:
            return None, "listing the warm-up pod did not answer within 60 s"
        st = ({"terminated": {"exitCode": 1, "reason": "Error"}} if self.pod == "ended" and self.t > self.load_at + 900
              else {"running": {"startedAt": clusterops.stamp(self.load_at)}})
        # "restarted": from the load's start + 900 s on, its container is running again after a restart
        restarts = 1 if self.pod == "restarted" and self.t > self.load_at + 900 else 0
        return {"items": [{"metadata": {"name": "warmup606-0"}, "status": {
            "phase": "Running", "containerStatuses": [{"name": "k6", "state": st, "restartCount": restarts}]}}]}, ""


def _settle(tmp_path, w, **kw):
    out, verdict = tmp_path / "settle.json", tmp_path / "verdict.json"
    said: list[str] = []
    rc = clusterops.settle(SPEC, created=T0, kubectl=["kubectl"], job="warmup606", container="k6",
                           start_within=kw.pop("start_within", 300.0), out=str(out), verdict_out=str(verdict),
                           run=w.run, read_pods=w.pods, now=w.now, sleep=w.sleep, say=said.append, **kw)
    rec = json.loads(out.read_text())
    v = json.loads(verdict.read_text()) if verdict.exists() else None
    return rc, rec, v, said


CAD = clusterops.cadence(clusterops.load_evidence()[0])


def test_the_arm_waits_out_the_burst_and_two_checks_after_it(tmp_path):
    """Run 27c N2's own burst, shifted under the warm-up: a 31 s check, a 270 s resize, a 1 s check after it."""
    load = T0 + 3600
    burst = [("RESIZE_CLUSTER", load + 60, load + 91), ("RESIZE_CLUSTER", load + 361, load + 631),
             ("RESIZE_CLUSTER", load + 661, load + 662)]
    w = World(burst, load_at=load)
    rc, rec, v, said = _settle(tmp_path, w)
    assert rc == clusterops.EXIT_SETTLED and rec["state"] == clusterops.SETTLED and v is None, rec
    settled_at = gke.rfc3339_epoch(rec["settled_at"])
    # not before the last check's end + the two-check window, and within one poll of it
    assert load + 662 + CAD["window_s"] <= settled_at <= load + 662 + CAD["window_s"] + CAD["poll_s"] + 1
    states = [p["state"] for p in rec["polls"]]
    assert states[-1] == clusterops.SETTLED and clusterops.WAITING in states
    assert any("running: RESIZE_CLUSTER" in p["why"] for p in rec["polls"])
    # every operation it read is in the evidence, each with its type, start and end
    assert [(o["type"], o["end"] is not None) for o in rec["operations"]][-3:] == [("RESIZE_CLUSTER", True)] * 3
    assert rec["evidence"]["sha256"] == clusterops.load_evidence()[1] and rec["evidence"]["deadline_s"] == CAD["deadline_s"]
    # the listing is read with the compute partial-error property off, never left to its default
    assert all(env.get("CLOUDSDK_COMPUTE_ALLOW_PARTIAL_ERROR") == "false" for _, env in w.calls) and w.calls


def test_a_burst_past_the_deadline_is_invalid_by_environment_and_not_measured(tmp_path):
    """Mutation guarded: settling on a running operation (dropping the `running` test) turns this RED."""
    load = T0 + 3600
    w = World([("RESIZE_CLUSTER", load + 100, None)], load_at=load)
    rc, rec, v, said = _settle(tmp_path, w)
    assert rc == clusterops.EXIT_UNSETTLED and rec["state"] == clusterops.UNSETTLED, rec
    assert gke.rfc3339_epoch(rec["polls"][-1]["t"]) >= load + CAD["deadline_s"]
    assert v["verdict"] == "INVALID" and v["report"]["measured"] is False
    (chk,) = v["validity"]
    assert chk["check"] == "environment.control_plane_settled" and chk["result"] == "FAIL"
    assert v["invalid_reasons"] == [f"environment.control_plane_settled: {chk['why']}"]
    assert chk["why"].startswith("INVALID by environment: GKE's control plane did not settle within "
                                 f"{CAD['deadline_s']:g} s") and "running: RESIZE_CLUSTER" in chk["why"]


@pytest.mark.parametrize("listing", ["fails", "empty"])
def test_an_unreadable_listing_is_never_settled(tmp_path, listing):
    """A listing that fails, or answers rc 0 with [] (gcloud failing open: no CREATE_CLUSTER control in it), is
    never 'nothing ran'. Mutation guarded: treating UNREADABLE as settled turns this RED."""
    w = World([], listing=listing)
    rc, rec, v, _ = _settle(tmp_path, w)
    assert rc == clusterops.EXIT_UNSETTLED and v["verdict"] == "INVALID", rec
    assert all(p["state"] == clusterops.NOT_READ for p in rec["polls"])
    assert "could not be read" in rec["why"] and rec["operations"] is None
    # every poll unreadable is no finding about the control plane: undecided, never "by environment: did not settle"
    # (mutation: settle_verdict's or finish's NOT_READ naming dropped turns this RED)
    assert rec["state"] == clusterops.NOT_READ
    assert v["invalid_reasons"][0].startswith("environment.control_plane_settled: INVALID by environment (undecided): "
                                              "GKE's operations could not be read on any of "), v["invalid_reasons"]
    assert "did not settle" not in v["invalid_reasons"][0]


def test_a_cluster_with_no_operation_settles_after_two_checks_of_load(tmp_path):
    load = T0 + 3600
    w = World([], load_at=load)
    rc, rec, _, _ = _settle(tmp_path, w)
    assert rc == 0 and load + CAD["window_s"] <= gke.rfc3339_epoch(rec["settled_at"]) <= load + CAD["window_s"] + CAD["poll_s"]


@pytest.mark.parametrize("pod,state", [("never", clusterops.NOT_STARTED), ("ended", clusterops.LOAD_ENDED)])
def test_a_warm_up_that_does_not_run_is_a_tooling_failure_never_a_settle(tmp_path, pod, state):
    w = World([("RESIZE_CLUSTER", T0 + 3700, None)], pod=pod)
    rc, rec, v, _ = _settle(tmp_path, w, start_within=120.0)
    assert rc == clusterops.EXIT_TOOLING and rec["state"] == state and v is None, rec


def test_no_poll_settles_while_the_warm_up_pod_cannot_be_read(tmp_path):
    """Z5: the listing reads settled, but the warm-up pod's own read failed at that poll: that the load ran through
    the window is not known, so it is not settled (mutation: dropping `and pods is not None` turns this RED)."""
    load = T0 + 3600
    w = World([("RESIZE_CLUSTER", load + 100, load + 400)], load_at=load, pod="unread")
    rc, rec, v, _ = _settle(tmp_path, w)
    assert rc == clusterops.EXIT_UNSETTLED and rec["state"] == clusterops.UNSETTLED, rec
    late = [p for p in rec["polls"] if gke.rfc3339_epoch(p["t"]) > load + 900]
    assert late and all(p.get("load", "").startswith("not read: ") for p in late), late


def test_a_restarted_warm_up_container_is_a_cut_load_never_a_window_it_ran_through(tmp_path):
    """Z6: a restartCount >= 1 is a load that was cut: the warm-up did not run through the window (mutation:
    dropping the restartCount check turns this RED)."""
    load = T0 + 3600
    w = World([("RESIZE_CLUSTER", load + 100, load + 400)], load_at=load, pod="restarted")
    rc, rec, v, _ = _settle(tmp_path, w)
    assert rc == clusterops.EXIT_TOOLING and rec["state"] == clusterops.LOAD_ENDED, rec
    assert "restarted (1): the load was cut" in rec["why"]


# ─── B. evaluate: the overlap and the settle record ─────────────────────────

BOOT = T0 + 6000


def _run_dir(d: Path, ops, *, readable=True, settle_state=clusterops.SETTLED, boot=True):
    d.mkdir(parents=True, exist_ok=True)
    files = []
    if boot:
        (d / "boot.jsonl").write_text(json.dumps({"kind": "boot", "t_restart": BOOT}) + "\n")
        files.append(d / "boot.jsonl")
    (d / "fresh-fresh606-0.jsonl").write_text("".join(json.dumps(r) + "\n" for r in (
        {"kind": "churn", "label": "list_dep@1", "t0": BOOT + 600, "t1": BOOT + 1200, "target_rate": 1},
        {"kind": "churn", "label": "list_dep@10", "t0": BOOT + 1200, "t1": BOOT + 1800, "target_rate": 10},
        {"kind": "fresh", "label": "list_dep@10", "converged": True, "latency_ms": 100, "polls": 1, "poll_ms": 100,
         "t": BOOT + 1300})))
    files.append(d / "fresh-fresh606-0.jsonl")
    rec = {"kind": "cluster_operations", "cluster": NAME, "region": "europe-west4", "read_at": BOOT + 4000,
           "readable": readable, "unreadable": None if readable else "could not be read: rc=1",
           "operations": _rec(*ops)["operations"] if readable else None}
    (d / "cluster-operations.json").write_text(json.dumps(rec))
    if settle_state is not None:
        (d / "settle.json").write_text(json.dumps({"kind": clusterops.SETTLE_KIND, "state": settle_state,
                                                   "why": "the deadline passed", "settled_at": "x"}))
    return files


def _evaluate(d: Path, files, *extra):
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *map(str, files),
                        "--expect-digest", "sha256:" + "ab" * 32, "--cluster-operations",
                        str(d / "cluster-operations.json"), "--settle", str(d / "settle.json"), *extra],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=300)
    return json.loads(p.stdout), p.returncode


@pytest.mark.parametrize("ops,hit", [
    # N2's shape: a resize during a churn step
    ([("RESIZE_CLUSTER", BOOT + 1300, BOOT + 1570)], ["measure", "churn:list_dep@10"]),
    # between the reboot and the first phase: only the measure as a whole saw it
    ([("RESIZE_CLUSTER", BOOT + 100, BOOT + 400)], ["measure"]),
    # a 1 s check inside the measure is an operation overlapping it all the same
    ([("RESIZE_CLUSTER", BOOT + 700, BOOT + 701)], ["measure", "churn:list_dep@1"]),
])
def test_an_operation_overlapping_the_measure_makes_the_arm_invalid_by_environment(tmp_path, ops, hit):
    """Mutation guarded: overlap_check ignoring OVERLAPPED phases, or the measure span left out, turns RED."""
    v, rc = _evaluate(tmp_path, _run_dir(tmp_path, ops))
    chk = _check(v["validity"], "environment.cluster_operations")
    assert chk["result"] == "FAIL" and chk["value"] == hit and v["verdict"] == "INVALID" and rc == 3, chk
    assert chk["why"].startswith("INVALID by environment: GKE's RESIZE_CLUSTER")
    assert f"environment.cluster_operations: {chk['why']}" in v["invalid_reasons"]


def test_operations_before_the_reboot_leave_the_arm_valid(tmp_path):
    v, _ = _evaluate(tmp_path, _run_dir(tmp_path, [("RESIZE_CLUSTER", BOOT - 900, BOOT - 1)]))
    assert _check(v["validity"], "environment.cluster_operations")["result"] == "PASS"
    assert _check(v["validity"], "environment.control_plane_settled")["result"] == "PASS"
    assert not any(r.startswith("environment.") for r in v["invalid_reasons"]), v["invalid_reasons"]


@pytest.mark.parametrize("kw,said", [
    ({"readable": False}, "the cluster's operations could not be read"),
    ({"boot": False}, "phase measure: the phase's own start or end is not recorded"),
])
def test_an_overlap_that_cannot_be_decided_is_invalid_never_valid(tmp_path, kw, said):
    """Mutation guarded: overlap_check returning PASS for UNREADABLE / UNKNOWN phases turns RED."""
    v, _ = _evaluate(tmp_path, _run_dir(tmp_path, [], **kw))
    chk = _check(v["validity"], "environment.cluster_operations")
    assert chk["result"] == "FAIL" and chk["why"].startswith("INVALID by environment (undecided): ") and said in chk["why"]
    assert v["verdict"] == "INVALID"


@pytest.mark.parametrize("state,said", [(clusterops.UNSETTLED, "INVALID by environment: the control plane was "
                                                               "UNSETTLED before the measure"),
                                        (None, "INVALID by environment (undecided): the run has no settle record")])
def test_a_measure_without_a_settled_control_plane_is_invalid(tmp_path, state, said):
    """Mutation guarded: settle_check passing any state but SETTLED, or a missing record, turns RED."""
    v, _ = _evaluate(tmp_path, _run_dir(tmp_path, [], settle_state=state))
    chk = _check(v["validity"], "environment.control_plane_settled")
    assert chk["result"] == "FAIL" and chk["why"].startswith(said) and v["verdict"] == "INVALID", chk


def test_invalid_by_environment_wins_over_fail(tmp_path):
    """#1495's precedence: a run that also FAILs a bar (fresh p95 over its bar) reads INVALID."""
    files = _run_dir(tmp_path, [("RESIZE_CLUSTER", BOOT + 1300, BOOT + 1570)])
    f = tmp_path / "fresh-fresh606-0.jsonl"
    f.write_text(f.read_text().replace('"latency_ms": 100', '"latency_ms": 99999'))
    v, rc = _evaluate(tmp_path, files)
    assert any(c["result"] == "FAIL" for c in v["checks"]) and v["verdict"] == "INVALID" and rc == 3


# ─── C. #1508: censored timeouts, failed sign-ins, lost sessions ────────────

T_POINTS = 1792576800.0


def _nav(ms, *, timeout=False, completed=None, bound=None, method="click", session="s1", cls="0", start=None):
    return {"kind": "nav", "nav": method, "page": "/r", "session": session, "class": cls, "wall_ms": ms,
            "visit": 2, "timeout": timeout, "completed": completed, "bound_ms": bound,
            "start": T_POINTS - 30 if start is None else start, "t0": str(int((T_POINTS - 30) * 1000))}


def _stalled_state():
    """Every 5 s window stalled 0.5 s: a 60 s load overlaps 12 windows (d = 6 s), a 99 s one 20 (d = 10 s)."""
    wins = [{"t0": T_POINTS - 60 + 5 * i, "t1": T_POINTS - 55 + 5 * i, "stall_s": 0.5, "saturated": False}
            for i in range(40)]
    return {"pressure": True, "node": True, "anchored": True, "end_anchored": True, "clock_step_s": 0.0,
            "slack_s": 0.0, "node_cpus": 16, "windows": wins, "stall_share": 0.4, "saturated_windows": 0}


def _starvation(navs):
    th = dict(load606.THRESHOLDS)
    return load606.starvation_check(navs, _stalled_state(), th)


def test_a_censored_timeout_with_a_stall_is_not_flipped():
    """#1508's unit case: run 27c N2 read 'FAIL as measured, PASS without the stall' for timeouts censored at
    their 60 s bound. Mutation guarded: the old `timeout and not d` rule turns this RED."""
    navs = [_nav(300.0) for _ in range(5)] + [_nav(60000.0, timeout=True)]           # completed None: censored
    assert k6eval.possible_delay_ms(navs[-1]["start"], 60000.0, _stalled_state()) > 0
    c = _starvation(navs)
    assert c["result"] == "PASS", c


@pytest.mark.parametrize("ms,completed,flipped", [
    (61000.0, True, True),       # completed 1 s past its bound, ~6 s of stall: within it without the stall
    (99000.0, True, False),      # 39 s past it, ~10 s of stall: the stall cannot explain it
    (60500.0, False, False),     # never completed: no time to subtract from
])
def test_a_load_past_its_bound_is_judged_by_the_bars_subtraction(ms, completed, flipped):
    navs = [_nav(300.0) for _ in range(5)] + [_nav(ms, timeout=True, completed=completed, bound=60000)]
    c = _starvation(navs)
    assert (c["result"] == "FAIL") is flipped, c
    if flipped:
        assert "browser.page_timeouts (FAIL as measured, PASS without the stall)" in c["why"]


def _n2_browser(d: Path, *, new_format: bool, lost=("s4", "s5"), lost_loads=True, start_at=None):
    """Run 27c N2's browser shape: 5 sessions; s2's class 2 sign-in timed out (censored at 60 s) and 10
    navigations of its unsigned context 'timed out' after it; s4 and s5 ended on an uncaught login-form error
    (a browser.js from #1508 records their end; one before it only logged it). `lost_loads` False: the lost
    sessions recorded no page load (they died at their first sign-in); `start_at`: the drive's start frame's time."""
    pts = [p for p in _browser_points(sessions=("s1", "s2", "s3", "s4", "s5"))
           if (p["metric"] != "browser_session_end" or new_format)
           and (lost_loads or p["metric"] not in ("browser_page_load_ms", "browser_page_loads")
                or p["data"]["tags"].get("session") not in lost)]
    if start_at is not None:
        start = _point("bench_frame", 1, frame="start")
        start["data"]["time"] = clusterops.stamp(start_at)
        pts.insert(0, start)
    t0 = int((T_POINTS - 2) * 1000)
    fail = {"session": "s2", "class": "2", "route": "(none)", "visit": "first", "method": "login",
            "skeleton": "timeout", "timeout": "yes", "t0": str(t0)}
    pts += [_point("browser_page_timeouts", 1, session="s2", route="(none)", t0=fail["t0"]),
            _point("browser_page_load_ms", 60000.0, **fail), _point("browser_page_loads", 1, **fail)]
    for i in range(10):
        t = dict(fail, route=f"/r{i % 5}", method="history", t0=str(t0 + 75000 * (i + 1)))
        pts += [_point("browser_page_timeouts", 1, session="s2", route=t["route"], t0=t["t0"]),
                _point("browser_page_load_ms", 60000.0, **t), _point("browser_page_loads", 1, **t)]
    if new_format:
        for p in pts:
            if p["metric"] == "browser_session_end" and p["data"]["tags"]["session"] in lost:
                p["data"]["tags"].update(early="yes", cause="session s4: the login form form#login_basic could not "
                                                            "be filled: timed out", **{"class": "2"})
    summary = dict(_browser_summary(5 * 21 + 11), sessions=["s1", "s2", "s3", "s4", "s5"])
    _write_out(d, "browser", pts, summary, 0, frames=("end",) if start_at is not None else ("start", "end"))
    _cpu_file(d, "browser", 3000, [1.0] * 20)
    log = d.parent / "driver-k6browser-x1.log"
    log.write_text("".join(
        f'time="{clusterops.stamp(T_POINTS + 2)}" level=error msg="Uncaught (in promise) Error: session {s}: the login form '
        f'form#login_basic (#login_basic_username, #login_basic_password) could not be filled: filling '
        f'\\"#login_basic_username\\" with \\"<user>\\": timed out\\n\\tat runClass (file:///opt/k6/browser.js:315:13(175))'
        f'\\n" executor=per-vu-iterations scenario=ui\n' for s in lost)
        + 'time="2026-11-16T15:49:32Z" level=warning msg="page did not settle (sign-in): waiting for function, '
          'polling: timed out after 120000ms" source=console\n')
    return log


def _n2_evaluate(tmp_path, *, new_format, with_log=True, lost=("s4", "s5"), resize=(-60.0, 100.0), **browser):
    """`resize`: a RESIZE_CLUSTER of the run's cluster, (start, end) seconds from the points' time (N2: its s4 / s5
    were lost at 15:48:02Z inside the 15:45:25-15:49:55Z resize), or None: the record holds no operation then."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_viewer(k6dir)
    log = _n2_browser(bdir, new_format=new_format, lost=lost, **browser)
    fresh = tmp_path / "fresh.jsonl"
    fresh.write_text(json.dumps({"kind": "fresh", "label": "list_dep@1", "converged": True, "latency_ms": 100,
                                 "polls": 1, "poll_ms": 100}) + "\n")
    ops =_rec(*([("RESIZE_CLUSTER", T_POINTS + resize[0], T_POINTS + resize[1])] if resize else []))["operations"]
    (tmp_path / "cluster-operations.json").write_text(json.dumps({
        "kind": "cluster_operations", "cluster": NAME, "region": "europe-west4", "read_at": T_POINTS + 3600,
        "readable": True, "unreadable": None, "operations": ops}))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(fresh), "--k6-out", str(k6dir),
                        "--browser-out", str(bdir), "--browser-sessions", "5",
                        "--cluster-operations", str(tmp_path / "cluster-operations.json"),
                        *(["--browser-log", str(log)] if with_log else [])],
                       capture_output=True, text=True, cwd=BENCH_DIR, timeout=300)
    return json.loads(p.stdout)


@pytest.mark.parametrize("new_format", [False, True])
def test_n2s_timeouts_read_as_one_sign_in_failure_and_two_lost_sessions(tmp_path, new_format):
    """#1508's acceptance, on N2's shape: 1 sign-in failure, 0 page timeouts (the unsigned context's 10
    navigations dropped), 2 of 5 sessions lost: INVALID by environment, not by cpu_starvation.
    Mutations guarded: counting the dropped navigations as page timeouts; sessions_check passing lost sessions."""
    v = _n2_evaluate(tmp_path, new_format=new_format)
    pt = _check(v["checks"], "browser.page_timeouts")
    si = _check(v["checks"], "browser.signin_failures")
    assert pt["result"] == "PASS" and pt["value"] == 0, pt
    assert si["result"] == "FAIL" and si["value"] == 1 and si["dropped_navigations"] == 10, si
    assert "session s2 class 2: stopped at its bound (60000 ms)" in si["why"]
    ses = _check(v["validity"], "environment.browser_sessions")
    assert ses["result"] == "FAIL" and ses["value"] == 2, ses
    # judged by cause (2026-11-17): both were lost while the recorded resize overlapped their last stretch
    assert ses["why"].startswith("INVALID by environment: 2 of 5 declared real-browser session(s) ended before the "
                                 "drive's end while a recorded environment event overlapped them: s4: "), ses["why"]
    assert "(during RESIZE_CLUSTER " in ses["why"] and "the arm drove 3 of 5 sessions to the end" in ses["why"]
    assert "the login form" in ses["why"]
    assert v["verdict"] == "INVALID"
    assert _check(v["validity"], "browser.cpu_starvation")["result"] == "PASS"
    assert not any(r.startswith("browser.cpu_starvation") for r in v["invalid_reasons"]), v["invalid_reasons"]


@pytest.mark.parametrize("new_format", [False, True])
def test_a_session_lost_with_no_recorded_environment_event_is_a_product_fail(tmp_path, new_format):
    """The owner's ruling (2026-11-17): the same two losses, and the cluster's only operation ended an hour before
    them: no recorded environment event in their window, so a product FAIL named with its cause, never INVALID
    (mutation: judging every lost session INVALID by environment again turns this RED)."""
    v = _n2_evaluate(tmp_path, new_format=new_format, resize=(-4000.0, -3700.0))
    ses = _check(v["validity"], "environment.browser_sessions")
    assert ses["result"] == "PASS" and "judged a product failure" in ses["note"], ses
    lost = _check(v["checks"], "browser.sessions_lost")
    assert lost["result"] == "FAIL" and lost["value"] == 2, lost
    for s in ("s4", "s5"):
        part = lost["why"].split(f"{s}: ", 1)[1].split("; ", 1)[0]
        assert "the login form form#login_basic" in part and "could not be filled" in part and part.endswith(
            "timed out, no environment event in its window") or part.endswith("no environment event in its window: "
                                                                              "the arm drove 3 of 5 sessions to the end "
                                                                              f"({116 if new_format else 116} page loads)"), part
    assert not any(r.startswith("environment.browser_sessions") for r in v["invalid_reasons"]), v["invalid_reasons"]


def test_a_lost_session_without_readable_environment_events_is_undecided():
    """No readable cluster-operations record: whether an event overlapped is unknown, never "none"."""
    ends ={"state": "READ", "declared": 5, "lost": [{"session": "s4", "cause": "x", "at": 10.0}]}
    sess, lost = k6eval.sessions_check(ends, 3, None, "the record could not be read", {"s4": 5.0})
    assert sess["result"] == "FAIL" and sess["why"].startswith("INVALID by environment (undecided): s4: x (the "
                                                               "environment's events could not be read") and lost is None


def _judge(lost: dict, events, last_activity=None, drive_start=None):
    ends = {"state": "READ", "declared": 5, "lost": [dict({"session": "s4", "cause": "c"}, **lost)]}
    return k6eval.sessions_check(ends, 3, events, "", {"s4": 100.0} if last_activity is None else last_activity,
                                 drive_start)


def _ev(t0, t1):
    return {"t0": t0, "t1": t1, "what": f"RESIZE_CLUSTER {t0}..{t1}"}


def test_an_event_that_ended_between_the_last_activity_and_the_loss_is_the_environment():
    """#1520 review W2 / W6: the window is [the session's last activity, its end], not the end instant, and an
    event need not still run at the loss (mutations: w0 = the end; the event must cover the end: RED)."""
    sess, lost = _judge({"at": 200.0}, [_ev(120.0, 150.0)])
    assert sess["result"] == "FAIL" and sess["why"].startswith("INVALID by environment: 1 of 5 ") and lost is None
    assert sess["lost"][0]["window"] == [100.0, 200.0]


def test_a_lost_session_whose_end_is_not_recorded_is_undecided():
    """#1520 review W4: no end time: undecided, never a product FAIL (mutation: judging it with no window: RED)."""
    sess, lost = _judge({"at": None}, [])
    assert sess["result"] == "FAIL" and lost is None
    assert sess["why"].startswith("INVALID by environment (undecided): s4: c (its end is not recorded)"), sess["why"]


@pytest.mark.parametrize("event", [_ev(50.0, 100.0), _ev(201.0, 300.0), _ev(200.5, 300.0)],
                         ids=["ends-at-the-last-activity", "starts-at-the-latest-end", "starts-within-the-end's-second"])
def test_an_event_touching_the_window_s_edges_overlaps_it(event):
    """#1520 review W1: both edges inclusive (mutation: strict edges: RED for the first two); k6's log time is cut
    to the second, so an end logged at 200 can be as late as 201 (at_until)."""
    sess, lost = _judge({"at": 200.0, "at_until": 201.0}, [event])
    assert sess["result"] == "FAIL" and sess["why"].startswith("INVALID by environment: 1 of 5 "), sess
    assert lost is None


def test_an_event_outside_the_window_is_not_the_environment():
    sess, lost = _judge({"at": 200.0, "at_until": 201.0}, [_ev(10.0, 99.0), _ev(201.5, 300.0)])
    assert sess["result"] == "PASS" and lost["result"] == "FAIL" and lost["lost"][0]["window"] == [100.0, 201.0]


def test_a_session_lost_before_any_load_opens_its_window_at_the_drive_s_start():
    """#1520 review B2: no recorded activity of the session: the window opens at the drive's start, never at the
    loss (mutation: w0 = the loss: an event during its first sign-in is missed, RED); neither known: undecided."""
    sess, lost = _judge({"at": 200.0}, [_ev(60.0, 70.0)], last_activity={}, drive_start=50.0)
    assert sess["why"].startswith("INVALID by environment: 1 of 5 ") and sess["lost"][0]["window"] == [50.0, 200.0]
    sess, lost = _judge({"at": 200.0}, [_ev(60.0, 70.0)], last_activity={}, drive_start=None)
    assert sess["why"].startswith("INVALID by environment (undecided): s4: c (no activity of it and no start of the "
                                  "drive is recorded"), sess["why"]


@pytest.mark.parametrize("new_format", [False, True])
def test_n2s_losses_at_the_first_sign_in_are_judged_from_the_drive_s_start(tmp_path, new_format):
    """B2 end to end: s4 / s5 recorded no page load; the resize ended 200 s before their loss, after the drive's
    start frame: INVALID by environment, read from the browser's own start frame."""
    v = _n2_evaluate(tmp_path, new_format=new_format, resize=(-300.0, -200.0), lost_loads=False,
                     start_at=T_POINTS - 600)
    ses = _check(v["validity"], "environment.browser_sessions")
    assert ses["result"] == "FAIL" and ses["why"].startswith("INVALID by environment: 2 of 5 "), ses
    assert all(x["window"][0] == T_POINTS - 600 for x in ses["lost"]), ses["lost"]
    assert not any(c["check"] == "browser.sessions_lost" for c in v["checks"])


def test_sessions_that_all_ran_to_the_end_pass(tmp_path):
    v = _n2_evaluate(tmp_path, new_format=True, lost=())
    assert _check(v["validity"], "environment.browser_sessions")["result"] == "PASS"


def test_a_browser_without_session_ends_and_without_its_log_is_undecided(tmp_path):
    """Mutation guarded: session_ends reading 'no end recorded and no log' as none lost turns this RED."""
    v = _n2_evaluate(tmp_path, new_format=False, with_log=False)
    ses = _check(v["validity"], "environment.browser_sessions")
    assert ses["result"] == "FAIL" and ses["why"].startswith("INVALID by environment (undecided): ")
    assert v["verdict"] == "INVALID"


def test_a_session_with_no_end_point_is_lost():
    o = {"summary": {"sessions": ["s1", "s2"]}, "points": {"browser_session_end": [
        (1, {"session": "s1", "early": "no", "cause": "the drive is over", "class": "3"})]}}
    ends = k6eval.session_ends(o, 2)
    assert [x["session"] for x in ends["lost"]] == ["s2"] and "no end recorded" in ends["lost"][0]["cause"]


def test_a_late_sign_in_that_completed_keeps_its_class():
    navs = [_nav(130000.0, timeout=True, completed=True, bound=120000, method="login", start=T_POINTS - 300),
            _nav(400.0, start=T_POINTS - 100)]
    kept, failed, dropped = k6eval.split_after_failed_signin(navs)
    assert len(kept) == 2 and len(failed) == 1 and not dropped
    assert "completed after 130000 ms, 10000 ms past its 120000 ms bound" in k6eval.load_outcome(failed[0])


# ─── C. pageload.js: a load past its bound, under node ──────────────────────

NODE = __import__("shutil").which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")

_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const c = JSON.parse(readFileSync(process.argv[2], 'utf8'))
process.stdout.write(JSON.stringify({
  ops: c.ops.map((x) => pl.recordOps(x.r, x.tags, x.bound, x.at, x.watch)),
  causes: c.causes.map((x) => pl.watchCause(x.e, x.ms, x.secrets)),
  ends: c.ends.map((x) => pl.sessionEndTags(x.s, x.k, x.early, x.cause, x.secrets)),
}))
"""


@needs_node
def test_pageload_records_a_late_load_with_its_real_time_and_a_never_completed_one_with_its_cause(tmp_path):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_DRIVER)
    tags = {"session": "s1", "route": "/r", "visit": "warm", "method": "click"}
    r = {"ms": 70500, "saw": True, "path": "/r", "states": {"error": 1}, "t0": 1792576801456.7}
    cases = {"ops": [
        {"r": r, "tags": tags, "bound": 60000, "at": 1792576800000, "watch": {"ms": 71500, "cause": None}},
        {"r": None, "tags": tags, "bound": 60000, "at": 1792576800000,
         "watch": {"ms": 3600000, "cause": "not settled by the drive's end (watched 3600000 ms)"}},
        {"r": dict(r, ms=400), "tags": tags, "bound": 60000, "at": 1792576800000, "watch": {"ms": 1400, "cause": None}},
    ], "causes": [
        {"e": "Error: waiting for function, polling: timed out after 3600000ms", "ms": 3600000, "secrets": []},
        {"e": "Error: Cannot find context with specified id (user scaleuser-0042)\n at x", "ms": 5000,
         "secrets": ["scaleuser-0042", "pw"]},
    ], "ends": [{"s": "s4", "k": 2, "early": True,
                 "cause": "Error: session s4: filling with scaleuser-0042 / pw: timed out\n\tat runClass",
                 "secrets": ["scaleuser-0042", "pw"]}]}
    (tmp_path / "c.json").write_text(json.dumps(cases))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "c.json")], capture_output=True, text=True,
                       timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout)
    late, never, fine = out["ops"]
    assert late[0][0] == "browser_page_timeouts" and late[1][:2] == ["browser_page_load_ms", 71500]
    assert late[1][2]["completed"] == "yes" and late[1][2]["bound"] == "60000" and late[1][2]["timeout"] == "yes"
    assert ["browser_widget_states", 1] == late[3][:2]                          # it loaded: its states count
    assert never[1][1] == 3600000 and never[1][2]["completed"] == "no" and "drive's end" in never[1][2]["cause"]
    assert fine[0][:2] == ["browser_page_load_ms", 400] and fine[0][2]["timeout"] == "no"
    assert out["causes"][0] == "not settled by the drive's end (watched 3600000 ms)"
    assert out["causes"][1].startswith("the watch ended: Error: Cannot find context") and "scaleuser" not in out["causes"][1]
    end = out["ends"][0]
    assert end == {"session": "s4", "class": "2", "early": "yes", "cause": end["cause"]}
    assert "scaleuser" not in end["cause"] and "pw" not in end["cause"].replace("<redacted>", "") and "\n" not in end["cause"]


def test_browser_js_records_every_session_end_and_runs_nothing_after_a_failed_sign_in():
    """The driver's own source: one browser_session_end in the catch (rethrown: k6 still logs it) and one at the
    end; a sign-in that never completed returns before any navigation."""
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    catch = src[src.index("} catch (e) {\n    // #1508"):]
    assert catch.index("SESSION_END.add(") < catch.index("throw e")
    assert src.count("SESSION_END.add(") == 2
    run = src[src.index("async function runClass"):]
    assert run.index("if (first === null) {") < run.index("return { token, signinFailed }") < run.index("while (Date.now() < deadline")
    assert "browser_session_end" in src[src.index("export function handleSummary"):]


# ─── A. run606: the settle step between the data and the reboot ─────────────

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra),
                          capture_output=True, text=True, timeout=600)


def _steps(log):
    return [ln.split("=== step ", 1)[1].split(" ===", 1)[0] for ln in log.splitlines() if "=== step " in ln]


def test_run606_settles_before_the_reboot_and_stops_the_warm_up_first(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env)
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    steps = _steps(log)
    assert steps.index("data") < steps.index("settle") < steps.index("reboot")
    rec = json.loads((out / "settle.json").read_text())
    assert rec["state"] == clusterops.SETTLED and rec["load"]["job"] == "warmup606" and rec["polls"]
    assert "settle: control plane SETTLED: nothing runs on the cluster" in log
    # the warm-up Job: applied, then deleted (its pod gone) before snowplow's restart
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    kub = [c["argv"] for c in calls if c["tool"] == "kubectl"]
    i_del = next(i for i, a in enumerate(kub) if "delete" in a and "job/warmup606" in a)
    i_restart = next(i for i, a in enumerate(kub) if "rollout" in a and "restart" in a)
    assert i_del < i_restart and "warm-up stopped: no warmup606 pod left" in log
    job = json.loads((state / "job-warmup606").read_text())
    assert job["spec"]["template"]["spec"]["containers"][0]["name"] == "driver"     # LOAD_DRIVER=load606
    v = json.loads((out / "verdict.json").read_text())
    assert _check(v["validity"], "environment.control_plane_settled")["result"] == "PASS"
    assert _check(v["validity"], "environment.cluster_operations")["result"] == "PASS"
    assert "--settle" in json.loads((out / "evaluate-argv.json").read_text())


def test_run606_does_not_measure_an_arm_whose_control_plane_did_not_settle(harness):  # noqa: F811
    """GKE resizes the control plane after the create and has not finished: the arm is INVALID by environment,
    its verdict says so, nothing is measured, the cluster is torn down."""
    env, state, _, tmp = harness
    p = _run606(env, FAKE_GKE_RESIZES="1")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode != 0 and "=== step reboot ===" not in log, log[-3000:]
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID" and v["report"]["measured"] is False
    assert v["invalid_reasons"][0].startswith("environment.control_plane_settled: INVALID by environment: GKE's "
                                              "control plane did not settle")
    assert "INVALID: environment.control_plane_settled: INVALID by environment" in log
    assert "warm-up stopped: no warmup606 pod left" in log
    gcl = [json.loads(x)["argv"] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    assert any(a[:4] == ["container", "clusters", "delete", RUN_NAME] for a in gcl)


@pytest.mark.parametrize("mode,said", [("never", "settle FAILED (exit 2): LOAD_NOT_STARTED"),
                                       ("stays", "the warm-up's pod is still listed")])
def test_run606_fails_the_step_when_the_warm_up_does_not_run_or_does_not_go(harness, mode, said):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_WARMUP=mode, PREFLIGHT_TIMEOUT_S="5")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and said in log and "=== step reboot ===" not in log, log[-3000:]


# ─── #1511's follow-ups: the warm-up's users at the boot, and reevaluate's settle ─────────────────────────────

def test_the_report_says_the_measured_users_were_known_at_boot(tmp_path):
    """The owner's ruling (2026-11-17): accepted, and recorded."""
    rec = tmp_path / "settle.json"
    rec.write_text(json.dumps({"kind": clusterops.SETTLE_KIND, "state": clusterops.SETTLED,
                               "load": {"started_at": "2026-11-17T09:00:00Z"}}))
    got = load606.measured_users_known_at_boot(str(rec))
    assert got["value"] is True and got["line"].startswith("measured users known at boot (warm-up signed them in")
    assert "the fresh never-signed-in identity classes are the cold path" in got["line"]
    assert load606.measured_users_known_at_boot(None)["value"] is False
    assert load606.measured_users_known_at_boot(str(tmp_path / "missing.json"))["value"] is None


@pytest.mark.parametrize("log,want", [
    ("[10:00:00] === step settle ===\n[10:40:00] === step reboot ===\n", True),
    ("[10:00:00] === step data ===\n[10:40:00] === step reboot ===\n", False),
    (None, None),
])
def test_reevaluate_decides_the_settle_check_from_the_runs_own_log_never_from_the_files_presence(tmp_path, log, want):
    """A run whose own run.log names the settle step is judged with --settle even when its settle.json is lost (it
    then reads INVALID, undecided); one with no such step has none to judge; no run.log: not decided (mutation:
    deciding from settle.json's presence turns the first case RED)."""
    from bench import reevaluate
    if log is not None:
        (tmp_path / "run.log").write_text(log)
    got, _ = reevaluate.derive(str(tmp_path))["settle"]
    assert got is want
    if want:
        s = {"run8": (False, ""), "expect_digest": ("sha256:" + "ab" * 32, ""), "driver": ("load606", ""),
             "frontend": ("none", ""), "settle": (True, "")}
        argv = reevaluate.argv_for(str(tmp_path), s)
        assert argv[argv.index("--settle") + 1] == str(tmp_path / "settle.json")
        assert not (tmp_path / "settle.json").exists()
