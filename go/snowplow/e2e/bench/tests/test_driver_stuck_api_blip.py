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

import copy
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


# ─── #898 review: answered errors, collect, exit codes, the outage rule, the budget ─

@pytest.mark.parametrize("what, line", [
    ("job", 'Error from server (NotFound): jobs.batch "k6load" not found'),
    ("pods", "Error from server (InternalError): an error on the server has prevented the request"),
])
def test_an_answered_error_fails_the_step_with_kubectls_line(harness, what, line):  # noqa: F811
    """1: a read the API server ANSWERED with an error (NotFound, Forbidden, a
    5xx) is a real answer — /readyz answers — so the step fails at once with
    kubectl's own first error line; it is never waited out as an outage."""
    t0 = time.time()
    p, _, out = _run606_k6(harness, FAKE_ANSWERED_ERROR=what, FAKE_DRIVER_RUNNING="k6load",
                           DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert "measure FAILED:" in log and line in log, log[-3000:]
    assert "while the API server answers" in log and "API server unreachable" not in log
    assert time.time() - t0 < 240


def test_a_fresh606_wait_answered_with_an_error_fails_by_readyz(harness):  # noqa: F811
    """1: the fresh606 wait's probe is /readyz: an answered Forbidden fails
    the step, it is not waited to the deadline (a `get job/fresh606` probe
    would have failed too and read as an outage)."""
    env, state, _, tmp = harness
    env = dict(env, FAKE_WAIT_ANSWERED="1", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    t0 = time.time()
    subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert "measure FAILED: fresh606 not Complete within 3600s" in log and "API server unreachable" not in log
    assert time.time() - t0 < 240


def test_job_pod_record_never_reads_an_unreadable_listing_as_missing():
    """2: an empty / unparseable listing, or run606's --unreadable, is
    recorded unreadable — never `missing` ("never started")."""
    def rec(stdin, *extra):
        p = subprocess.run(["python3", "-m", "bench.load606", "job-pod-record", "--job", "k6load", *extra],
                           input=stdin, cwd=BENCH_DIR, capture_output=True, text=True, check=True)
        return json.loads(p.stdout)
    for stdin in ("", "garbage", "{}"):
        r = rec(stdin)
        assert r["unreadable"] is True and "missing" not in r, stdin
    r = rec("", "--unreadable", "API did not answer")
    assert r["why"] == "API did not answer"
    assert rec('{"items": []}')["missing"] is True              # an answered empty list: the pod IS missing
    (chk,) = load606.job_pod_checks([r])
    assert chk["result"] == "FAIL" and "unreadable at collect (API did not answer)" in chk["why"]
    assert "never started" not in chk["why"] or "not that it never started" in chk["why"]


def test_collect_records_unanswered_pod_reads_as_unreadable(harness):  # noqa: F811
    """2: at collect the API stops answering the pod listings: retried within
    collect's cap, then recorded "unreadable (API did not answer)"; the
    verdict says so — not "no k6load pod at collect: it never started"."""
    p, _, out = _run606_k6(harness, FAKE_COLLECT_PODS_UNANSWERED="1", DRIVE_POLL_S="1",
                           COLLECT_CAP_S="30", COLLECT_TIMEOUT_S="29")
    recs = [json.loads(x) for x in (out / "job-pods.jsonl").read_text().splitlines()]
    assert recs and all(r.get("unreadable") and r["why"] == "API did not answer" for r in recs), recs
    v = json.loads((out / "verdict.json").read_text())
    whys = [c.get("why") or "" for c in v["validity"] if c["check"].endswith(".pod") and c["result"] == "FAIL"]
    assert whys and all("unreadable at collect (API did not answer)" in w for w in whys)
    assert not any("never started" in w and "not that it never started" not in w for w in whys)


def test_unanswered_exit_codes_are_unknown_never_exited_0(harness):  # noqa: F811
    """3: the driver's exit codes cannot be listed: "unknown", never "every
    run exited 0"."""
    p, _, out = _run606_k6(harness, FAKE_EXIT_CODES_UNANSWERED="1", DRIVE_POLL_S="1")
    log = _log(out)
    assert "driver k6load exit codes unknown (API did not answer" in log, log[-3000:]
    assert "k6load pod(s) Ready, every run exited 0" not in log      # the unanswered listing claims nothing
    assert "k6browser pod(s) Ready, every run exited 0" in log       # the next one answered


# 4: the outage rule in the paired verdict (run 14's recorded run, test_pair_ready_time's arms)

def _outage(doc, t0_s, t1_s, k0):
    d = copy.deepcopy(doc)
    d["api_outages"] = [{"kind": "api_unreachable", "t0": k0 + t0_s, "t1": k0 + t1_s,
                         "seconds": t1_s - t0_s, "first_unanswered": k0 + t0_s + 30, "seen_by": "get job/k6load",
                         "open": False,
                         "source": "run606 polls: the last answered poll before to the first answered poll after"}]
    return d


def test_an_a_outage_is_excluded_from_both_runs_like_an_a_restart(tmp_path):
    p = _arms(tmp_path, _outage(P14, 900, 1100, K0), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
    (it,) = p["exclusion"]["items"]
    assert it["what"] == "A control-plane outage #1" and "run606 polls" in it["why"]
    (a0, a1), = it["a"]
    (b0, b1), = it["b"]
    assert round(b0 - a0, 6) == SHIFT and round(b1 - a1, 6) == SHIFT
    assert any("A's control plane did not answer for 200" in n and "excluded from both runs" in n for n in p["notes"])
    churn = [c for c in p["checks"] if c["check"].startswith("customer_latency.pair[churn]")]
    assert all(c["excluded"]["a_s"] == c["excluded"]["b_s"] > 0 for c in churn)


def test_a_b_outage_makes_the_pair_invalid_with_its_window(tmp_path):
    p = _arms(tmp_path, P14, _shift(_outage(P14, 900, 1100, K0), SHIFT))
    assert p["verdict"] == "INVALID"
    assert [r.split(":")[0] for r in p["invalid_reasons"]] == ["customer_latency.pair.control_plane_outage[b]"]
    why = p["invalid_reasons"][0]
    assert "200.0 s from" in why and "run606 polls" in why and "never excluded silently" in why
    v = next(c for c in p["validity"] if c["check"].endswith("control_plane_outage[b]"))
    assert v["value"][0]["seconds"] == 200.0


def test_an_outage_outside_the_measure_is_not_the_pairs(tmp_path):
    """only an outage that touches k6's span counts (collect-time ones do not)"""
    p = _arms(tmp_path, P14, _shift(_outage(P14, 5000, 5100, K0), SHIFT))
    assert p["verdict"] == "PASS"


def test_evaluate_is_in_the_run_budget():
    """5: evaluate runs while the cluster is up (run 15: 809 s A, 828 s B)."""
    src = (BENCH_DIR / "run606.sh").read_text()
    assert 'EVALUATE_EST_S="${EVALUATE_EST_S:-828}"' in src and "809 s (A) and 828 s (B)" in src
    assert "+ COLLECT_CAP_S + EVALUATE_EST_S + DIAG_CAP_S + DOWN_BUDGET_S ))" in src
    assert "+ evaluate ${EVALUATE_EST_S} +" in src


from tests.test_pair_ready_time import K0, P14, SHIFT, _arms, _shift  # noqa: E402
