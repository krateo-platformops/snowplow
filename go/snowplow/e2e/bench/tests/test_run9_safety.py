"""reviewer-small's FAIL on be6f5178 (#606 issuecomment-6267177150):

(a) teardown is signal-safe (a TERM during the partial collect still deletes
    the cluster), the whole partial collect has ONE wall-clock cap, and every
    collect read carries --request-timeout AND an outer bound;
(d) the whole run's worst case is checked against TTL_HOURS before any
    cloud call, and fresh606 is itself bounded per phase;
R6: the fresh CLI wires "status" to the status writer.
"""

from __future__ import annotations

import signal
import subprocess
import time

import pytest

from bench import load606
from tests.test_606 import _run606
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401


def _measure_fails(env, **extra):
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", FAKE_WAIT_FAIL="1",
               **extra)
    return env


# ─── (a) teardown ──────────────────────────────────────────────────────────


def test_term_during_partial_collect_still_deletes_the_cluster(harness):
    env, state, ambient, tmp = harness
    env = _measure_fails(env, FAKE_SLOW_LOGS="40")
    p = subprocess.Popen(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    lines, t0 = [], time.time()
    for line in p.stderr:
        lines.append(line)
        if "collecting partial results before teardown" in line:
            time.sleep(1.5)                       # inside the (slow) collect
            p.send_signal(signal.SIGTERM)
            break
    _, rest = p.communicate(timeout=60)
    err = "".join(lines) + rest
    assert time.time() - t0 < 40                  # did not sit out the hung read
    assert "signal during teardown" in err
    assert "=== teardown " in err
    gcl = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert gcl[-1] == ["container", "clusters", "delete"]


def test_partial_collect_has_one_wall_clock_cap(harness):
    env, state, ambient, tmp = harness
    env = _measure_fails(env, FAKE_SLOW_LOGS="40", COLLECT_CAP_S="2")
    t0 = time.time()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    assert time.time() - t0 < 35
    assert "partial collect: stopped after 2s (cap 2s reached)" in p.stderr
    gcl = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert gcl[-1] == ["container", "clusters", "delete"]


def test_every_collect_read_is_bounded_twice(harness):
    env, state, ambient, tmp = harness
    # one hung read, bounded per read at 1 s (the total cap is not what stops it)
    env = _measure_fails(env, FAKE_SLOW_LOGS="40", COLLECT_TIMEOUT_S="1")
    t0 = time.time()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    assert time.time() - t0 < 35
    assert "could not read the driver log" in p.stderr
    assert "stopped after" not in p.stderr          # the per-read bound, not the total cap
    reads = [c["argv"] for c in _calls(state) if c["tool"] == "kubectl"
             and ("cp" in c["argv"] or any(x.startswith("pod/") for x in c["argv"]))
             and any("bench-loadgen" == x for x in c["argv"])]
    assert reads and all(any(x.startswith("--request-timeout=") for x in r) for r in reads)
    assert (tmp / "out" / "fresh-fresh606-0.jsonl").exists()       # the others still came


# ─── (d) TTL budget ────────────────────────────────────────────────────────


def test_default_plan_fits_the_default_ttl(tmp_path):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", THINK_S="60")
    assert p.returncode == 0, p.stderr[-2000:]
    assert "run budget:" in p.stderr and "fits TTL_HOURS=11" in p.stderr


@pytest.mark.parametrize("env,needs", [({"FRESH_SAMPLES": "100"}, None),
                                       ({"TTL_HOURS": "8"}, None)])
def test_a_plan_beyond_the_ttl_is_refused_before_any_cloud_call(tmp_path, env, needs):
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", THINK_S="60", **env)
    assert p.returncode == 2
    assert "refusing: worst-case run" in p.stderr and "needs TTL_HOURS >=" in p.stderr
    assert "measure " in p.stderr and "fresh " in p.stderr          # the breakdown is shown
    assert "=== step" not in p.stderr                                # nothing ran


def test_fresh606_phases_are_bounded_and_truncation_is_recorded():
    t = [0.0]
    recs = []

    class C:
        def get(self, path, token):
            return load606.Response(200, b"old", 1.0)    # never converges

    def sleep(s):
        t[0] += s

    out = load606.freshness_probe(C(), "t", "/p", lambda m: True, samples=60, poll_ms=100,
                                  gap_s=5, label="list_dep_status@10", sink=recs.append,
                                  clock=lambda: t[0], sleep=sleep, deadline=100.0)
    assert len(out) == 3                                   # 3 x 35 s > 100 s: stopped
    tr = [r for r in recs if r["kind"] == "fresh_truncated"]
    assert tr == [{"kind": "fresh_truncated", "label": "list_dep_status@10", "done": 3,
                   "of": 60, "t": tr[0]["t"]}]
    assert load606.fresh_phase_budget_s(60, 5) == 60 * 35 * 1.25


def test_fresh_phases_hand_each_phase_its_own_deadline():
    seen = []

    def probe(client, token, path, mutate, **kw):
        seen.append(kw["deadline"])

    clock = iter([10.0, 5000.0])
    load606.fresh_phases(None, "t", lambda r: None, modes=["annotation"], rates=["0", "0"],
                         mutators={"annotation": lambda *a: True}, paths={"annotation": "/a"},
                         targets=[], probe=("n", "p"), samples=2, poll_ms=100, gap_s=0,
                         viewer=False, probe_fn=probe, healthy=lambda: True,
                         clock=lambda: next(clock))
    assert seen == [10.0 + 75.0, 5000.0 + 75.0]


# ─── R6: the CLI wires each mode to its own writer ─────────────────────────


@pytest.mark.parametrize("mutator", ["incluster", "kubectl"])
def test_fresh_cli_maps_status_to_the_status_writer(monkeypatch, tmp_path, mutator):
    calls = []
    monkeypatch.setattr(load606, "login", lambda *a, **k: "tok")
    for fn in ("incluster_annotate", "kubectl_annotate"):
        monkeypatch.setattr(load606, fn, lambda *a, fn=fn, **k: calls.append(("annotation", fn)) or True)
    for fn in ("incluster_status_condition", "kubectl_status_condition"):
        monkeypatch.setattr(load606, fn, lambda *a, fn=fn, **k: calls.append(("status", fn)) or True)
    got = {}
    monkeypatch.setattr(load606, "fresh_phases", lambda *a, **kw: got.update(kw))
    args = ["fresh", "--snowplow-url", "http://s", "--authn-url", "http://a", "--user", "u",
            "--password", "p", "--version", "v1-2-2", "--plural", "comps",
            "--probe-name", "bench-app-01-01", "--mutator", mutator, "--no-viewer",
            "--modes", "annotation", "status", "--out", str(tmp_path / "o.jsonl")]
    if mutator == "kubectl":
        args += ["--kubeconfig", "/k", "--context", "gke_x_y_snowplow-bench-1"]
    assert load606.main(args) == 0
    muts = got["mutators"]
    muts["status"]("ns", "n", "v")
    muts["annotation"]("ns", "n", "v")
    side = "incluster" if mutator == "incluster" else "kubectl"
    assert calls == [("status", f"{side}_status_condition"), ("annotation", f"{side}_annotate")]
    assert got["modes"] == ["annotation", "status"]
    assert got["paths"]["status"].endswith("name=bench-fresh-listdep-status&namespace=bench-ns-01")
