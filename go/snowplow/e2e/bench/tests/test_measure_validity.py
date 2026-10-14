"""Run-7 harness defects (OOM at 50K, #710):

(a) a failed first load must NOT count as first-loaded: it retries with
    backoff, so the arrival burst is replayed after snowplow recovers;
(b) the freshness probe starts only once the driver's load reaches snowplow,
    pauses while /readyz fails, and marks samples taken during an outage
    invalid (excluded from the gate);
(c) a snowplow restart during measure makes the verdict INVALID with the
    reason (restart count + lastState in the record);
plus the standard memory capture (memstats + heap profiles) of every run.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from bench import load606, load606_job
from tests.test_navmodel import INIT, PORTAL, TREE, Fake, canon
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401


# ─── (a) failed first loads retry ───────────────────────────────────────────


def _wr(statuses, errors=0):
    return load606.WalkResult(1.0, sum(statuses.values()), errors, 0, 1.0, [], statuses, {})


def test_first_load_failed_rules():
    ok = _wr({"200": 5})
    assert load606.first_load_failed(ok, [{"path": "/d"}], ok) is False
    assert load606.first_load_failed(ok, [], ok) is True                     # no menu
    assert load606.first_load_failed(_wr({"200": 3, "0": 1}), [{"path": "/d"}], ok) is True
    assert load606.first_load_failed(ok, [{"path": "/d"}], _wr({"0": 2})) is True
    # a product 5xx inside the page is a finding, not a failed arrival
    assert load606.first_load_failed(ok, [{"path": "/d"}], _wr({"200": 3, "500": 1}, 1)) is False


class Flaky(Fake):
    """snowplow down (transport error) for the first `down` INIT requests."""

    def __init__(self, down):
        super().__init__()
        self.down = down

    def get(self, path, token):
        if canon(path) == canon(INIT) and self.down > 0:
            with self.lock:
                self.down -= 1
                self.calls.append(path)
            return load606.Response(0, b"", 1.0, "ConnectionRefusedError")
        return super().get(path, token)


def test_failed_first_load_is_retried_and_replayed_after_recovery():
    c = Flaky(down=2)
    recs = []
    d = load606.Driver(c, {"u1": "t"}, PORTAL, think_s=0.02, duration_s=1.2,
                       nav_workers=2, call_workers=8, sink=recs.append,
                       retry_base_s=0.05, retry_cap_s=0.2)
    d.run()
    first = [r for r in recs if r["nav"] == "first_load"]
    assert [r["first_load_failed"] if "first_load_failed" in r else False for r in first[:3]] == [True, True, False]
    assert [r["attempt"] for r in first[:3]] == [1, 2, 3]
    assert first[0]["visit"] == 0 and first[1]["visit"] == 0     # failures are not visits
    assert first[2]["visit"] == 1                                # the replayed arrival is the cold one
    assert any(r["nav"] == "route" for r in recs)                # normal navigation resumes
    assert c.calls.count(INIT) >= 3


def test_backoff_spaces_retries():
    c = Flaky(down=10 ** 6)                                       # never recovers
    recs = []
    d = load606.Driver(c, {"u1": "t"}, PORTAL, think_s=0.01, duration_s=0.6,
                       nav_workers=2, call_workers=4, sink=recs.append,
                       retry_base_s=0.1, retry_cap_s=0.2)
    d.run()
    starts = [r["start"] for r in recs]
    assert len(starts) <= 6                                       # not a hot loop
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert all(g >= 0.08 for g in gaps)


# ─── (b) freshness gating ───────────────────────────────────────────────────


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_freshness_pauses_while_not_ready_and_invalidates_outage_samples():
    clk = Clock()
    state = {"marker": None}
    # healthy() calls, in order: s0 pre-check F; pause loop F, T; `recovered` T;
    # s0 after-check T; s1 pre-check T; s1 after-check F (outage → invalid).
    seq = [False, False, True, True, True, True, False]
    calls = iter(seq + [True] * 10)
    health = calls

    class C:
        def get(self, path, token):
            return load606.Response(200, (state["marker"] or "").encode(), 1)

    def mutate(m):
        state["marker"] = m
        return True

    recs = []
    load606.freshness_probe(C(), "t", "/p", mutate, samples=2, gap_s=0,
                            sink=recs.append, healthy=lambda: next(health),
                            pause_poll_s=1, clock=clk.now, sleep=clk.sleep)
    kinds = [r["kind"] for r in recs]
    assert kinds == ["fresh_pause", "fresh", "fresh"]
    assert recs[0]["recovered"] is True and recs[0]["paused_s"] >= 1
    assert recs[1].get("valid", True) is True
    assert recs[2]["valid"] is False and "not Ready" in recs[2]["invalid_reason"]


def test_invalid_fresh_samples_do_not_gate_and_are_counted():
    good = [{"kind": "fresh", "label": "list_dep@10", "converged": True, "latency_ms": 100}
            for _ in range(40)]
    outage = [{"kind": "fresh", "label": "list_dep@10", "converged": False,
               "latency_ms": None, "valid": False} for _ in range(5)]
    navs = [{"kind": "nav", "user": "u", "page": "p", "visit": v, "wall_ms": 100,
             "calls": 3, "errors": 0, "sched_lag_ms": 1} for v in [1] + [2] * 300]
    r = load606.evaluate(navs, good + outage, [])
    fresh_checks = [c for c in r["checks"] if c["check"].startswith("fresh_p95")]
    assert fresh_checks and all(c["result"] == "PASS" for c in fresh_checks)
    assert r["report"]["fresh_invalid_samples"] == 5


def test_wait_for_customer_load():
    clk = Clock()
    totals = iter([10, 10, 50, 140, 500])

    class C:
        def get(self, path, token):
            t = next(totals)
            body = {"snowplow_dispatch_l1_lookups": {"w|x": {"hit_total": t, "miss_total": 0}}}
            return load606.Response(200, json.dumps(body).encode(), 1)
    res = load606.wait_for_customer_load(C(), "t", min_delta=100, poll_s=5,
                                         clock=clk.now, sleep=clk.sleep)
    assert res["started"] is True and res["delta"] == 130 and res["waited_s"] == 15

    class Dead:
        def get(self, path, token):
            return load606.Response(0, b"", 1)
    res = load606.wait_for_customer_load(Dead(), "t", max_wait_s=20, poll_s=5,
                                         clock=clk.now, sleep=clk.sleep)
    assert res["started"] is False


def test_fresh_job_waits_for_load():
    m = load606_job.render_fresh(snowplow_url="http://s", authn_url="http://a",
                                 user="scaleuser-0001", password="pw", rates=["10"],
                                 samples=2, gap_s=1, probe_name="p", namespaces=1,
                                 comp_group="g", comp_version="v", comp_plural="x")
    assert "--wait-for-load" in m


# ─── (c) restarts invalidate the run ────────────────────────────────────────


def _pod(phase, restarts, pod="snowplow-abc", reason=None):
    ls = {"terminated": {"reason": reason, "exitCode": 137,
                         "finishedAt": "2026-10-14T15:04:53Z"}} if reason else {}
    return {"kind": "snowplow_pod", "phase": phase, "pod": pod,
            "restarts": restarts, "last_state": ls}


def test_restart_check():
    assert load606.restart_check(None) is None
    assert load606.restart_check([_pod("before", 0)])["result"] == "FAIL"           # unknown
    assert load606.restart_check([_pod("before", 0), _pod("after", 0)])["result"] == "PASS"
    r = load606.restart_check([_pod("before", 0), _pod("after", 1, reason="OOMKilled")])
    assert r["result"] == "FAIL" and "OOMKilled" in r["why"] and "15:04:53" in r["why"]
    r = load606.restart_check([_pod("before", 0), _pod("after", 0, pod="snowplow-new")])
    assert r["result"] == "FAIL" and "REPLACED" in r["why"]


def test_restart_makes_an_otherwise_passing_run_invalid():
    navs = [{"kind": "nav", "user": "u", "page": "p", "visit": v, "wall_ms": 100,
             "calls": 3, "errors": 0, "sched_lag_ms": 1} for v in [1] + [2] * 300]
    fresh = [{"kind": "fresh", "label": "list_dep@10", "converged": True, "latency_ms": 100}
             for _ in range(40)]
    ok = load606.evaluate(navs, fresh, [], snowplow_pods=[_pod("before", 0), _pod("after", 0)])
    assert ok["verdict"] == "PASS"
    bad = load606.evaluate(navs, fresh, [], snowplow_pods=[_pod("before", 0),
                                                           _pod("after", 1, reason="OOMKilled")])
    assert bad["verdict"] == "INVALID"
    assert any("OOMKilled" in (c.get("why") or "") for c in bad["validity"])


def test_pod_record_parses_kubectl_json():
    pl = {"items": [{"metadata": {"name": "snowplow-x"},
                     "spec": {"containers": [{"resources": {"limits": {"memory": "8Gi"}}}]},
                     "status": {"containerStatuses": [{"restartCount": 2, "ready": False,
                                                       "lastState": {"terminated": {"reason": "OOMKilled"}}}]}}]}
    r = load606.pod_record(pl, "after")
    assert r["restarts"] == 2 and r["last_state"]["terminated"]["reason"] == "OOMKilled"
    assert r["limits"] == {"memory": "8Gi"}
    assert load606.pod_record({"items": []}, "before")["restarts"] is None


# ─── standard memory capture ────────────────────────────────────────────────


class MemSnowplow:
    """readyz sequence per sample; /debug/vars memstats; /debug/pprof/heap."""

    def __init__(self, ready_seq):
        self.ready = iter(ready_seq)
        self.cur = 503
        self.heaps = 0

    def get(self, path, token):
        if path == "/readyz":
            self.cur = next(self.ready, 200)
            return load606.Response(self.cur, b"", 1)
        if path == "/debug/vars":
            if self.cur != 200:
                return load606.Response(0, b"", 1)
            v = {"memstats": {"HeapInuse": 1048576 * 1400, "Sys": 1048576 * 1550, "NumGC": 3},
                 "snowplow_resolved_cache": {"entries": 1, "bytes": 2},
                 "snowplow_fill_inflight_bytes": 123, "other": 9}
            return load606.Response(200, json.dumps(v).encode(), 1)
        if path == "/debug/pprof/heap":
            self.heaps += 1
            return load606.Response(200, b"\x1f\x8bprofile", 1)
        return load606.Response(404, b"", 1)


def test_memwatch_profiles_at_ready_under_load_and_after_restart(tmp_path):
    clk = Clock()
    sp = MemSnowplow([503, 200, 200, 200, 0, 200, 200])
    recs = []
    n = load606.memwatch(sp, lambda: "tok", duration_s=105, interval_s=15,
                         profile_every_s=30, out_dir=str(tmp_path), sink=recs.append,
                         extra_prefixes=("snowplow_fill_",), clock=clk.now, sleep=clk.sleep)
    profs = [r for r in recs if r["kind"] == "heap_profile"]
    ats = [p["at"] for p in profs]
    assert ats[0] == "ready"                       # first Ready
    assert "load" in ats                           # periodic under load
    assert ats.count("ready") == 2                 # again after the restart
    assert n == len(profs) and len(os.listdir(tmp_path)) == n + 1   # + done marker
    mems = [r for r in recs if r["kind"] == "mem"]
    assert mems[1]["heap_inuse_mb"] == 1400.0 and mems[1]["sys_mb"] == 1550.0
    assert mems[1]["extra"] == {"snowplow_fill_inflight_bytes": 123}  # prefix capture only
    rep = load606.memory_report(recs)
    assert rep["heap_inuse_mb_max"] == 1400.0 and rep["not_ready_samples"] == 2
    assert rep["heap_profiles"] == [p["profile"] for p in profs]


def test_mem_job_renders_with_prefixes():
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a",
                               user="scaleuser-0000", password="pw", duration_s=60,
                               vars_prefixes=("snowplow_fill_",))
    assert "memwatch" in m and '--vars-prefix "snowplow_fill_"' in m and "emptyDir" in m


# ─── run606 end to end ──────────────────────────────────────────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_run_collects_memory_and_restart_state(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", TOP_POLL_S="0.2")
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    out = tmp / "out"
    assert (out / "top.log").exists()
    assert (out / "heap" / "heap-01-150000-ready.pb.gz").exists()
    assert (out / "pod-before.jsonl").exists() and (out / "pod-after.jsonl").exists()
    v = json.loads((out / "verdict.json").read_text())
    assert any(c["check"] == "snowplow.restarts_during_measure" and c["result"] == "PASS"
               for c in v["validity"])
    assert v["report"]["memory"]["heap_inuse_mb_max"] == 3789.4
    assert v["report"]["memory"]["heap_profiles"] == ["heap-01-150000-ready.pb.gz"]
    assert "mem606" in (out / "loadgen-job.yaml").read_text()


def test_run_with_a_restart_during_measure_is_invalid(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1",
               FAKE_RESTART_DURING_MEASURE="1")
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]   # a verdict, not a tooling failure
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    why = [c.get("why", "") for c in v["validity"] if c["result"] == "FAIL"]
    assert any("restarted 1x during measure" in w and "OOMKilled" in w for w in why)
    after = json.loads((tmp / "out" / "pod-after.jsonl").read_text())
    assert after["last_state"]["terminated"]["reason"] == "OOMKilled"
