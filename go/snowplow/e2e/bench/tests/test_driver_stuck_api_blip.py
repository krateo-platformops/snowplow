"""Run 16 attempt 2 (2026-10-25): 65 healthy minutes of measure lost to a
bench bug. B's API server stopped answering (TLS handshake timeouts) for a few
minutes; run606's driver wait piped the failed `kubectl get pods` into
`load606 driver-stuck`, and under `set -o pipefail` the pipeline's non-zero
exit read as "the k6load driver never started: " with an empty reason.

Now a read the API server did not answer proves nothing: driver-stuck fails
ONLY on positive evidence (a driver pod seen Pending past DRIVER_PENDING_S by
its own creationTimestamp, or a restarting init step); an empty, unparseable
or item-less listing is "unknown" (exit 3), and run606 waits it out to the
measure deadline. The same for the job-status read, the fresh606 wait and the
start of collect. Each outage is logged at its start and end and recorded in
api-outages.jsonl, which evaluate shows beside the verdict (never judged).
"""

from __future__ import annotations

import json
import subprocess
import time

import pytest

from bench import load606
from tests.test_k6_830 import _run606_k6
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

T0 = 1792662318.0                      # 2026-10-22T09:45:18Z


def _stuck(stdin: str, pending_s=load606.DRIVER_PENDING_S):
    p = subprocess.run(["python3", "-m", "bench.load606", "driver-stuck", "--pending-s", str(pending_s)],
                       input=stdin, cwd=BENCH_DIR, capture_output=True, text=True, check=False)
    return p.returncode, p.stdout.strip()


def _pod(created, phase="Pending"):
    return {"metadata": {"name": "k6load-0", "creationTimestamp": created},
            "status": {"phase": phase, "initContainerStatuses": [], "containerStatuses": []}}


@pytest.mark.parametrize("stdin", ["", "   ", "not json", "{}", '{"kind": "Status"}', '{"items": null}'])
def test_a_listing_that_proves_nothing_is_unknown_never_stuck(stdin):
    rc, out = _stuck(stdin)
    assert rc == load606.DRIVER_STUCK_UNKNOWN == 3 and "unknown, not stuck" in out


def test_only_a_pod_seen_pending_past_its_bound_is_stuck():
    now = time.time()
    old = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - load606.DRIVER_PENDING_S - 60))
    new = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 30))
    assert _stuck(json.dumps({"items": []})) == (0, "")
    assert _stuck(json.dumps({"items": [_pod(new)]}))[0] == 0
    assert _stuck(json.dumps({"items": [_pod(old, phase="Running")]}))[0] == 0
    rc, out = _stuck(json.dumps({"items": [_pod(old)]}))
    assert rc == 1 and out.startswith("pod k6load-0 is still Pending")


def _log(out):
    return (out / "run.log").read_text()


def test_run16_shape_an_api_outage_mid_measure_is_waited_out(harness):  # noqa: F811
    """k6load running; the API server stops answering (the job status and the
    driver pods: TLS handshake timeout) for 4 polls, then answers again and
    the drivers are Ready: the run completes, with the outage logged and
    recorded — never "the k6load driver never started"."""
    p, _, out = _run606_k6(harness, FAKE_API_DOWN_POLLS="4", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert p.returncode == 0, log[-3000:]
    assert "never started" not in log and "measure FAILED" not in log
    assert "API server unreachable since" in log and "(get job/k6load): waiting it out, not a verdict" in log
    assert "API server reachable again at" in log
    assert log.count("API server unreachable since") == 1          # logged on the change of state only
    assert "driver done" in log and "run complete" in log
    (rec,) = [json.loads(x) for x in (out / "api-outages.jsonl").read_text().splitlines()]
    assert rec["kind"] == "api_unreachable" and rec["t1"] >= rec["t0"] and rec["open"] is False
    v = json.loads((out / "verdict.json").read_text())
    assert v["report"]["api_unreachable_report_only"] == [rec]


def test_an_empty_pod_listing_answered_rc_0_is_unknown_too(harness):  # noqa: F811
    """kubectl exits 0 but the body is empty: driver-stuck says unknown (3),
    and run606 waits on — only its "stuck" (1) fails the step."""
    p, _, out = _run606_k6(harness, FAKE_API_EMPTY_PODS="3", FAKE_DRIVER_RUNNING="k6load",
                           DRIVE_POLL_S="1", MEASURE_WAIT_S="4")
    log = _log(out)
    assert "never started" not in log, log[-3000:]
    assert "measure FAILED: k6load not done within 4s" in log      # the deadline, not the empty read


def test_positive_evidence_still_fails_after_an_outage(harness):  # noqa: F811
    """The browser pod really stuck in Init (restarting sidecar): after the
    outage it is seen, and the step fails with the reason, fast."""
    t0 = time.time()
    p, _, out = _run606_k6(harness, FAKE_API_DOWN_POLLS="3", FAKE_DRIVER_STUCK="k6browser",
                           FAKE_DRIVER_RUNNING="k6load", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert ("measure FAILED: the k6browser driver never started: pod k6browser-0 is still Pending and its "
            "init container forward restarted 3 times" in log), log[-3000:]
    assert "API server unreachable since" in log
    assert time.time() - t0 < 240


def test_an_outage_during_the_fresh606_wait_is_waited_out(harness):  # noqa: F811
    env, state, _, tmp = harness
    env = dict(env, FAKE_API_DOWN_WAITS="2", DRIVE_POLL_S="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "(wait job/fresh606): waiting it out" in log and "API server reachable again" in log


def test_a_reachable_api_still_fails_a_fresh606_not_complete(harness):  # noqa: F811
    env, state, _, tmp = harness
    env = dict(env, FAKE_WAIT_FAIL="1", DRIVE_POLL_S="1")
    subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert "measure FAILED: fresh606 not Complete within" in log
    assert "API server unreachable" not in log


def test_collect_waits_out_an_outage_before_declaring_results_missing(harness):  # noqa: F811
    env, state, _, tmp = harness
    env = dict(env, FAKE_API_DOWN_COLLECT="2", DRIVE_POLL_S="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert "(collect): waiting it out" in log and "API server reachable again" in log
    assert "could not copy the driver's results" not in log
    assert json.loads((tmp / "out" / "api-outages.jsonl").read_text().splitlines()[0])["seen_by"] == "collect"
