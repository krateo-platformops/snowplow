"""#834 review nits, folded into run 12's PR:
1. the browser wrapper's forward wait is bounded per attempt: a port that
   streams at / (headers, then keepalives forever) answered, and must not
   hold the wait open past ORIGIN_WAIT_S (it hung > 90 s on a 20 s budget);
2. run606's driver wait fails fast, with the reason, when a load or browser
   pod stays Pending / in Init (a forwarder whose startupProbe never passes)
   instead of only at MEASURE_WAIT_S."""

from __future__ import annotations

import json
import os
import subprocess
import time

import yaml

from bench import k6_job, load606
from tests.test_k6_830 import _run606_k6, needs_docker
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

FIX = BENCH_DIR / "tests" / "fixtures"


def _wait_fn() -> str:
    """The rendered browser container's wait for the portal's origin."""
    y = k6_job.render_browser(config={"sessions": [], "frontendUrl": "http://127.0.0.1:8080"}, linger_s=0)
    job = next(d for d in yaml.safe_load_all(y) if d and d["kind"] == "Job")
    args = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    return args[args.index("wait_origin() {"):args.index("wait_origin && ")]


def _run_wait(tmp_path, ports: str, budget_s: int):
    name = f"k6wait-{os.getpid()}-{tmp_path.name[-10:]}"
    out = tmp_path / "out"
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    urls = " ".join(f"http://127.0.0.1:{p}/" for p in ports.split())
    script = (_wait_fn().replace("for u in http://127.0.0.1:8080/; do", f"for u in {urls}; do")
              + "wait_origin; echo rc=$?\n")
    subprocess.run(["docker", "run", "-d", "--name", name, "-v", f"{FIX}:/f:ro",
                    "python:3.12-slim", "python3", "-u", "/f/k6_stream_fixture.py"],
                   capture_output=True, check=True)
    try:
        subprocess.run(["docker", "exec", name, "python3", "-c",
                        "import socket, time\n"
                        "for _ in range(200):\n"
                        "    try:\n"
                        "        socket.create_connection(('127.0.0.1', 8081), 1).close(); break\n"
                        "    except OSError:\n"
                        "        time.sleep(0.05)\n"], capture_output=True, check=True)
        t0 = time.time()
        p = subprocess.run(["docker", "run", "--rm", "--network", f"container:{name}",
                            "-v", f"{out}:/out", "-e", f"ORIGIN_WAIT_S={budget_s}",
                            "--entrypoint", "/bin/sh", k6_job.K6_BROWSER_IMAGE, "-c", script],
                           capture_output=True, text=True, timeout=120)
        took = time.time() - t0
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    return p, took, json.loads((out / "browser-forward.json").read_text())


@needs_docker
def test_a_streaming_port_answers_and_does_not_hold_the_wait(tmp_path):
    p, took, fw = _run_wait(tmp_path, "8080", budget_s=20)
    assert "rc=0" in p.stdout, p.stdout + p.stderr
    assert fw["ok"] is True
    # one attempt at most: ORIGIN_ATTEMPT_S + 2 s kill grace (+ docker start)
    assert took < k6_job.ORIGIN_ATTEMPT_S + 2 + 8, took


@needs_docker
def test_a_silent_port_fails_the_wait_within_its_budget(tmp_path):
    p, took, fw = _run_wait(tmp_path, "8081", budget_s=6)
    assert "rc=3" in p.stdout and "did not answer within 6s" in p.stderr, p.stdout + p.stderr
    assert fw["ok"] is False and fw["url"] == "http://127.0.0.1:8081/"
    assert took < 6 + k6_job.ORIGIN_ATTEMPT_S + 2 + 8, took


def test_every_attempt_has_an_overall_time_limit():
    assert f"timeout -k 2 {k6_job.ORIGIN_ATTEMPT_S} wget " in _wait_fn()
    assert "for u in http://127.0.0.1:8080/; do" in _wait_fn()        # the docker arms rewrite this line


# ─── 2. a driver pod that never starts fails the driver wait at once ───────


def _pod(phase="Pending", created="2026-10-22T09:45:18Z", init_restarts=0, sched=None):
    st = {"phase": phase,
          "initContainerStatuses": [{"name": "forward", "restartCount": init_restarts,
                                     "state": {"running": {}},
                                     "lastState": ({"terminated": {"reason": "Error",
                                                                   "exitCode": 137}}
                                                   if init_restarts else {})}],
          "containerStatuses": [{"name": "browser", "restartCount": 0,
                                 "state": {"waiting": {"reason": "PodInitializing"}}}]}
    if sched:
        st["conditions"] = [{"type": "PodScheduled", "status": "False", "reason": sched,
                             "message": "0/3 nodes are available"}]
    return {"items": [{"metadata": {"name": "k6browser-x", "creationTimestamp": created},
                       "status": st}]}


T0 = 1792662318.0                      # 2026-10-22T09:45:18Z


def test_a_restarting_sidecar_is_stuck_at_once():
    why = load606.driver_pod_stuck(_pod(init_restarts=2), T0 + 30)
    assert why == ("pod k6browser-x is still Pending and its init container forward restarted 2 "
                   "times (last: Error, exit 137): a sidecar whose startupProbe never passes, "
                   "or a crashing init step")
    assert load606.driver_pod_stuck(_pod(init_restarts=1), T0 + 30) is None


def test_a_pod_pending_past_its_bound_is_stuck_with_the_reason():
    assert load606.driver_pod_stuck(_pod(), T0 + load606.DRIVER_PENDING_S - 1) is None
    why = load606.driver_pod_stuck(_pod(sched="Unschedulable"), T0 + load606.DRIVER_PENDING_S)
    assert why == (f"pod k6browser-x is still Pending {load606.DRIVER_PENDING_S} s after it was "
                   f"created (unscheduled: Unschedulable) (waiting: browser PodInitializing)")
    assert "0/3 nodes" not in why                  # reasons only, never messages


def test_a_running_pod_is_never_stuck():
    assert load606.driver_pod_stuck(_pod(phase="Running", init_restarts=5), T0 + 10**6) is None
    assert load606.driver_pod_stuck({}, T0) is None


def test_run606_fails_fast_on_a_browser_pod_stuck_in_init(harness):  # noqa: F811
    t0 = time.time()
    # k6load is still running: the browser pod is checked while the wait is on k6load
    p, _, out = _run606_k6(harness, FAKE_DRIVER_STUCK="k6browser", FAKE_DRIVER_RUNNING="k6load",
                           MEASURE_WAIT_S="3600", DRIVE_POLL_S="1")
    log = (out / "run.log").read_text()
    assert ("measure FAILED: the k6browser driver never started: pod k6browser-0 is still "
            "Pending and its init container forward restarted 3 times (last: Error, exit 137)"
            in log), log[-3000:]
    assert time.time() - t0 < 240                   # not MEASURE_WAIT_S
    assert "driver done" not in log
