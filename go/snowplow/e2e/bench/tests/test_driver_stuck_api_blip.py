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
import os
import subprocess
import sys
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
    ("pods", 'Error from server (Forbidden): pods is forbidden: User "<redacted>" cannot list resource "pods"'),
])
def test_an_answered_error_fails_the_step_with_kubectls_line(harness, what, line):  # noqa: F811
    """1: a read the API server ANSWERED with an error (NotFound, Forbidden,
    Unauthorized, BadRequest, Invalid), /readyz answering and the one re-read
    answered so too, is a real answer: the step fails with kubectl's own
    first error line; it is never waited out as an outage."""
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
    assert ('measure FAILED: waiting for fresh606 failed while the API server answers: Error from server '
            '(Forbidden): jobs.batch "fresh606" is forbidden' in log) and "API server unreachable" not in log
    assert time.time() - t0 < 240


# ─── #898 re-review: the error itself decides; one answer is re-read ───────

def test_the_read_that_spans_an_outages_end_is_waited_out(harness):  # noqa: F811
    """The reviewer's edge (#898 re-review): the last read of an outage times
    out (TLS handshake timeout) while the API server comes back, so /readyz
    right after it ANSWERS. That read proves nothing: waited out, the run
    completes — not "reading job/k6load failed while the API server answers"
    (run 16 attempt 2 again)."""
    p, _, out = _run606_k6(harness, FAKE_EDGE_READS="3", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "API server unreachable since" in log and "API server reachable again" in log
    rec = [json.loads(x) for x in (out / "api-outages.jsonl").read_text().splitlines()]
    assert rec and rec[0]["seen_by"] == "get job/k6load"


def test_the_outage_end_edge_on_the_driver_pod_listing_is_waited_out(harness):  # noqa: F811
    """k6load keeps running; its pod listings time out while /readyz answers:
    waited out — the step ends at its deadline, never on the timed-out read."""
    p, _, out = _run606_k6(harness, FAKE_EDGE_PODS="3", FAKE_DRIVER_RUNNING="k6load", DRIVE_POLL_S="1",
                           MEASURE_WAIT_S="8")
    log = _log(out)
    assert "listing the k6load driver pods failed" not in log and "never started" not in log, log[-3000:]
    assert "measure FAILED: k6load not done within 8s" in log
    assert "(get pods job-name=" in log and "waiting it out" in log


@pytest.mark.parametrize("line, want", [
    ('Error from server (Forbidden): pods is forbidden: User "a.b@ex.io" cannot list resource "pods"',
     'Error from server (Forbidden): pods is forbidden: User "<redacted>" cannot list resource "pods"'),
    ('Error from server (Forbidden): jobs.batch "k6load" is forbidden: User "system:anonymous" cannot get',
     'Error from server (Forbidden): jobs.batch "k6load" is forbidden: User "<redacted>" cannot get'),
    ("error: authenticated as someone@example.invalid (and 'x@y.z')",
     "error: authenticated as <redacted> (and '<redacted>')"),
    ("Unable to connect to the server: net/http: TLS handshake timeout",
     "Unable to connect to the server: net/http: TLS handshake timeout"),
    # the #898 re-reviewer's three forms after "authenticated as", and users "<name>"
    ("error: request denied (authenticated as someone@example.com)",
     "error: request denied (authenticated as <redacted>)"),
    ("error: request denied (authenticated as: 112233445566778899)",
     "error: request denied (authenticated as <redacted>)"),
    ("error: request denied, authenticated as system:serviceaccount:ns:sa",
     "error: request denied, authenticated as <redacted>"),
    ('Error from server (Forbidden): users "alice" is forbidden: User "bob" cannot impersonate resource "users"',
     'Error from server (Forbidden): users "<redacted>" is forbidden: User "<redacted>" cannot impersonate '
     'resource "users"'),
    ('Error from server (Forbidden): pods is forbidden: User "system:serviceaccount:ns:sa" cannot list',
     'Error from server (Forbidden): pods is forbidden: User "<redacted>" cannot list'),
    ("error: see (x@y.z), then", "error: see (<redacted>), then"),
])
def test_kerr_line_redacts_users(tmp_path, line, want):
    assert kerr_line(tmp_path, line) == want


def kerr_line(tmp_path, line, here=None):
    """run606.sh's kerr_line, run on one kubectl error line (HERE: the bench dir)."""
    src = (BENCH_DIR / "run606.sh").read_text()
    fn = src[src.index("kerr_line() {"):]
    fn = fn[:fn.index("\n}\n") + 3]
    kerr = tmp_path / "kerr"
    kerr.write_text(line + "\n")
    env = {"KERR": str(kerr), "HERE": str(here or BENCH_DIR),
           "PATH": f"{os.path.dirname(sys.executable)}:/usr/bin:/bin"}
    p = subprocess.run(["bash", "-c", fn + "\nkerr_line"], env=env, capture_output=True, text=True)
    return p.stdout.rstrip("\n")


# The #900 re-reviewer's sample lines (scratchpad/classify2.sh): ONE identity
# redaction (bench/identity.py) behind run606's kerr_line and bench.gke's
# error_summary — both paths give the same, identity-free answer.
CLASSIFY2 = [
    'Error from server (InternalError): an error on the server ("") has prevented the request from succeeding',
    "Error from server (ServiceUnavailable): the server is currently unable to handle the request",
    "Error from server (TooManyRequests): the server has received too many requests and has asked us to try "
    "again later",
    "Error from server (Timeout): the server was unable to return a response in the time allotted",
    "Error from server: etcdserver: request timed out",
    "Unable to connect to the server: net/http: TLS handshake timeout",
    "Unable to connect to the server: EOF",
    "error: unknown flag: --request-timeoutt",
    'error: error loading config file "/tmp/x/kubeconfig": open /tmp/x/kubeconfig: no such file or directory',
    'error: context "gke_p_z_c" does not exist',
    "The connection to the server localhost:8080 was refused - did you specify the right host or port?",
    "error: You must be logged in to the server (Unauthorized)",
    'Error from server (NotFound): jobs.batch "k6load" not found',
    'Error from server (Forbidden): jobs.batch "k6load" is forbidden: User "someone@example.com" cannot get '
    'resource "jobs" in API group "batch" in the namespace "bench-loadgen"',
    'Error from server (Forbidden): pods is forbidden: User "system:serviceaccount:ns:sa" cannot list resource '
    '"pods"',
    "error: request denied (authenticated as someone@example.com)",
    "error: request denied (authenticated as: 112233445566778899)",
    "error: request denied, authenticated as system:serviceaccount:ns:sa",
    'Error from server (Forbidden): users "alice" is forbidden: User "bob" cannot impersonate resource "users"',
    # gcloud's real 404 (the account a placeholder)
    "ERROR: (gcloud.container.clusters.describe) ResponseError: code=404, message=Not found: projects/p/"
    "locations/europe-west4/clusters/c. This command is authenticated as PLACEHOLDER-OPERATOR@example.invalid "
    "which is the active account specified by the [core/account] property.",
]
_IDENTITIES = ("someone@example.com", "system:serviceaccount", "112233445566778899", '"alice"', '"bob"',
               "PLACEHOLDER-OPERATOR", "@example")


@pytest.mark.parametrize("line", CLASSIFY2)
def test_one_identity_redaction_behind_both_paths(tmp_path, line):
    from bench import gke, identity
    assert gke.error_summary is identity.error_summary          # bench.gke has no redaction of its own
    want = identity.redact_identity(line)[:300]
    assert kerr_line(tmp_path, line) == want                    # run606's kerr_line: the same function
    for text in (want, identity.error_summary(line)):
        assert not any(x in text for x in _IDENTITIES), text
    src = (BENCH_DIR / "run606.sh").read_text()
    fn = src[src.index("kerr_line() {"):]
    assert "sed" not in fn[:fn.index("\n}\n")]                 # no second rule set in the shell


def test_a_redactor_that_does_not_run_withholds_the_line(tmp_path):
    out = kerr_line(tmp_path, 'Error from server (Forbidden): User "someone@example.com" cannot get',
                    here=tmp_path / "nowhere")
    assert out == "<kubectl's error withheld: the identity redactor did not run>"


def test_answered_error_carries_local_errors_patterns_verbatim():
    """answered_error is self-contained (a reviewer's classifier copies it
    alone) and so repeats local_error's patterns: the two must not drift."""
    import re as _re
    fns = _sh_fns("local_error", "answered_error")
    local, answered = _re.findall(r"grep -qE '([^']*)'", fns)
    assert answered.endswith("|" + local), (local, answered)


def test_a_local_kubectl_misconfiguration_is_a_verdict_at_once(harness):  # noqa: F811
    """kubectl's own client-side error (here: the context does not exist)
    never reached an API server: a verdict at once, with the reason — not
    "API server unreachable", waited to the deadline."""
    t0 = time.time()
    p, _, out = _run606_k6(harness, FAKE_LOCAL_ERROR="1", FAKE_DRIVER_RUNNING="k6load", DRIVE_POLL_S="1",
                           MEASURE_WAIT_S="3600")
    log = _log(out)
    assert ("measure FAILED: reading job/k6load failed before reaching any API server (kubectl's local "
            'configuration): error: context "gke_bench" does not exist') in log, log[-3000:]
    assert "API server unreachable" not in log
    assert time.time() - t0 < 240


def test_logged_out_is_waited_out_the_operator_can_log_in_again(harness):  # noqa: F811
    p, _, out = _run606_k6(harness, FAKE_LOGGED_OUT_READS="3", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "API server unreachable since" in log


def test_kubectl_error_lines_never_carry_a_user(harness):  # noqa: F811
    """kubectl's own error line is logged (and recorded at collect): any user
    name or e-mail address in it is redacted first."""
    p, _, out = _run606_k6(harness, FAKE_ANSWERED_ERROR="pods", FAKE_DRIVER_RUNNING="k6load",
                           DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert 'User "<redacted>" cannot list resource "pods"' in log, log[-3000:]
    assert "someone@example.invalid" not in log and "@example" not in log


def test_the_outage_end_edge_on_the_fresh606_wait_is_waited_out(harness):  # noqa: F811
    env, state, _, tmp = harness
    env = dict(env, FAKE_EDGE_WAITS="2", DRIVE_POLL_S="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "(wait job/fresh606): waiting it out" in log


def test_the_outage_end_edge_at_collect_is_waited_out_not_unreadable(harness):  # noqa: F811
    """collect's pod listing times out while /readyz answers: retried, then
    read — never recorded "unreadable (the API answered: Unable to connect…)"."""
    p, _, out = _run606_k6(harness, FAKE_EDGE_COLLECT="2", DRIVE_POLL_S="1")
    recs = [json.loads(x) for x in (out / "job-pods.jsonl").read_text().splitlines()]
    assert recs and not any(r.get("unreadable") for r in recs), recs


def test_a_5xx_is_waited_out_never_a_verdict(harness):  # noqa: F811
    """A 5xx (InternalError) is the server failing, not answering the read:
    waited out like an outage, while /readyz answers."""
    p, _, out = _run606_k6(harness, FAKE_5XX_READS="3", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "API server unreachable since" in log


def test_one_answered_error_with_a_good_reread_is_no_verdict(harness):  # noqa: F811
    """An answered NotFound whose re-read right after succeeds: the API
    answers and the Job is there — no verdict, no outage, the run completes."""
    p, _, out = _run606_k6(harness, FAKE_ANSWERED_ONCE="1", DRIVE_POLL_S="1", MEASURE_WAIT_S="3600")
    log = _log(out)
    assert p.returncode == 0 and "measure FAILED" not in log, log[-3000:]
    assert "API server unreachable" not in log


def _sh_fns(*names):
    src = (BENCH_DIR / "run606.sh").read_text()
    out = []
    for n in names:
        fn = src[src.index(f"{n}() {{"):]
        out.append(fn[:fn.index("\n}\n") + 3])
    return "\n".join(out)


# kind: "answer" (the server refused: re-read, then a verdict), "local"
# (kubectl's own configuration: a verdict at once), "wait" (waited out).
# The #898 re-reviewer's samples (scratchpad/classify.sh) and more.
@pytest.mark.parametrize("line, kind", [
    ('Error from server (NotFound): jobs.batch "k6load" not found', "answer"),
    ('Error from server (Forbidden): jobs.batch "k6load" is forbidden', "answer"),
    ("Error from server (Unauthorized): the server has asked for the client to provide credentials", "answer"),
    ("Error from server (BadRequest): a container name must be specified", "answer"),
    ('Error from server (Invalid): Job.batch "x" is invalid', "answer"),
    ('Error from server (Forbidden): users "alice" is forbidden: User "bob" cannot impersonate resource "users"',
     "answer"),
    ("Unable to connect to the server: net/http: TLS handshake timeout", "wait"),
    ("Unable to connect to the server: dial tcp 10.0.0.2:443: i/o timeout", "wait"),
    ("Unable to connect to the server: EOF", "wait"),
    ('Error from server (InternalError): an error on the server ("") has prevented the request from succeeding',
     "wait"),
    ("Error from server (ServiceUnavailable): the server is currently unable to handle the request", "wait"),
    ("Error from server (TooManyRequests): the server has received too many requests", "wait"),
    ("Error from server (Timeout): the server was unable to return a response in the time allotted", "wait"),
    ("Error from server: etcdserver: request timed out", "wait"),
    ("error: unexpected EOF", "wait"),
    ("The connection to the server 10.0.0.2:443 was refused - did you specify the right host or port?", "wait"),
    ("error: You must be logged in to the server (Unauthorized)", "wait"),      # the operator can log in again
    ("error: unknown flag: --request-timeoutt", "local"),
    ("error: unknown shorthand flag: 'x' in -x", "local"),
    ('error: error loading config file "/tmp/x/kubeconfig": open /tmp/x/kubeconfig: no such file or directory',
     "local"),
    ('error: context "gke_p_z_c" does not exist', "local"),
    ("The connection to the server localhost:8080 was refused - did you specify the right host or port?", "local"),
    ("", "wait"),
])
def test_only_the_servers_own_refusals_or_a_local_misconfiguration_end_a_wait(tmp_path, line, kind):
    """run606's answered_error / local_error, run on kubectl's own error lines."""
    kerr = tmp_path / "kerr"
    kerr.write_text(line + "\n")
    fns = _sh_fns("local_error", "answered_error")

    def rc(f):
        return subprocess.run(["bash", "-c", fns + "\n" + f],
                              env={"KERR": str(kerr), "PATH": "/usr/bin:/bin"}).returncode
    assert (rc("answered_error") == 0) == (kind != "wait"), line
    assert (rc("local_error") == 0) == (kind == "local"), line


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


# 4: the outage rule in the paired verdict (run 14's recorded run, test_pair_ready_time's arms).
# The WHOLE window [t0, t1] of a record is judged; its first unanswered poll
# (here t0 + 30) is reported, and widens an A outage's vantage cover by its
# poll gap.

def _outage(doc, t0_s, t1_s, k0):
    d = copy.deepcopy(doc)
    d["api_outages"] = [{"kind": "api_unreachable", "t0": k0 + t0_s, "t1": k0 + t1_s,
                         "seconds": t1_s - t0_s, "first_unanswered": k0 + t0_s + 30, "seen_by": "get job/k6load",
                         "open": False,
                         "source": "run606 polls: the last answered poll before to the first answered poll after"}]
    return d


def _b_failing(doc, lo, hi):
    """B's k6 requests failing over [K0 + lo, K0 + hi) wall-clock (B runs SHIFT later)"""
    return _failing(doc, lo - SHIFT, hi - SHIFT)


def _b_silent(doc, lo, hi):
    """not one B k6 request completing over [K0 + lo, K0 + hi) wall-clock"""
    d = copy.deepcopy(doc)
    c0 = d["calls"][0][0]
    keep = lambda o: not (lo - SHIFT <= o - c0 < hi - SHIFT)     # noqa: E731
    d["calls"] = [x for x in d["calls"] if keep(x[0])]
    d["failed"] = [x for x in d["failed"] if keep(x[0])]
    return d


def _cpo_b(p):
    return next(c for c in p["validity"] if c["check"].endswith("control_plane_outage[b]"))


def test_an_a_outage_is_excluded_from_both_runs_like_an_a_restart(tmp_path):
    p = _arms(tmp_path, _outage(P14, 900, 1100, K0), _shift(P14, SHIFT))
    assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
    (it,) = p["exclusion"]["items"]
    assert it["what"] == "A control-plane outage #1" and "run606 polls" in it["why"]
    (a0, a1), = it["a"]
    (b0, b1), = it["b"]
    assert round(b0 - a0, 6) == SHIFT and round(b1 - a1, 6) == SHIFT
    assert round(a0 - K0, 6) == 900.0                       # its whole window
    assert any("A's control plane did not answer for 200" in n and "excluded from both runs" in n for n in p["notes"])
    churn = [c for c in p["checks"] if c["check"].startswith("customer_latency.pair[churn]")]
    assert all(c["excluded"]["a_s"] == c["excluded"]["b_s"] > 0 for c in churn)


def test_a_b_outage_k6_saw_served_is_jointly_ready_time_with_a_note(tmp_path):
    """B's control plane did not answer, B's k6 requests in flight over its
    whole window were served within the 1 % bar: not void, not cut —
    jointly-Ready time, and a NOTE (#606 run 16)."""
    p = _arms(tmp_path, P14, _shift(_outage(P14, 900, 1100, K0), SHIFT))
    assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
    assert _cpo_b(p)["result"] == "PASS" and _cpo_b(p)["value"] == "none"
    (w,) = p["b_outages_witnessed_served"]
    assert (w["t0"] - K0, w["t1"] - K0, w["seconds"], w["poll_gap_s"]) == (900.0, 1100.0, 200.0, 30.0)
    assert w["b_only"] == [[round(K0 + 900.0, 1), round(K0 + 1100.0, 1)]]
    assert w["k6"]["requests"] > 0 and w["k6"]["rate"] <= 0.01
    assert not p["exclusion"]["items"]                       # kept: nothing cut from either run
    assert any("kept as jointly-Ready time" in n and "within the 0.01 bar" in n for n in p["notes"])


def test_a_b_outage_with_customer_errors_fails_the_pair(tmp_path):
    """B's requests in flight in it not served over the bar: B was not
    serving — FAIL (candidate_not_ready), never cut, never void."""
    b = _b_failing(_shift(_outage(P14, 900, 1100, K0), SHIFT), 900, 1100)
    p = _arms(tmp_path, P14, b)
    assert p["verdict"] == "FAIL", (p["invalid_reasons"], p["fail_reasons"])
    assert p["candidate_not_ready"]["result"] == "FAIL"
    (st,) = p["candidate_not_ready"]["value"]["stretches"]
    assert st["seconds"] == 200.0 and st["k6"]["rate"] > 0.01 and "B was not serving" in st["why"]


def test_customer_errors_only_in_the_poll_gap_fail_the_pair(tmp_path):
    """B's customers fail ONLY in the poll gap — between the last answered
    poll (900) and the first unanswered one (930). That is inside the
    window: B was not serving. Judging from the first unanswered poll on
    would have missed every one of them (#917, Diego's rule)."""
    b = _b_failing(_shift(_outage(P14, 900, 1100, K0), SHIFT), 900, 930)
    p = _arms(tmp_path, P14, b)
    assert p["verdict"] == "FAIL", (p["invalid_reasons"], p["fail_reasons"])
    (st,) = p["candidate_not_ready"]["value"]["stretches"]
    assert (st["t0"] - K0, st["t1"] - K0) == (900.0, 1100.0) and st["k6"]["rate"] > 0.01


def test_a_b_outage_no_k6_request_witnessed_voids(tmp_path):
    """not one B request in flight: whether B served cannot be told — void"""
    b = _b_silent(_shift(_outage(P14, 900, 1100, K0), SHIFT), 880, 1150)
    p = _arms(tmp_path, P14, b)
    assert p["verdict"] == "INVALID"
    assert "customer_latency.pair.control_plane_outage[b]" in [r.split(":")[0] for r in p["invalid_reasons"]]
    v = _cpo_b(p)
    assert v["value"][0]["k6"]["requests"] == 0 and "no witness" in v["why"]


def test_the_operators_own_network_blip_is_the_vantages_not_bs(tmp_path):
    """#898 re-review: both runs are polled from the same machine, so its own
    network dropping records an outage in BOTH at the same wall-clock time.
    That B outage is the vantage's: excluded from both runs (like A's), noted
    — k6 is not asked. (The reviewer's same-time probe.)"""
    t0 = 1000                                        # seconds after A's first call: wall-clock, both runs
    a = _outage(P14, t0, t0 + 40, K0)                          # the same wall-clock moment
    b = _b_silent(_shift(_outage(P14, t0, t0 + 40, K0), SHIFT), t0, t0 + 40)   # even with no k6 witness
    p = _arms(tmp_path, a, b)
    assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
    assert _cpo_b(p)["result"] == "PASS" and not p["b_outages_witnessed_served"]
    whats = {it["what"] for it in p["exclusion"]["items"]}
    assert {"A control-plane outage #1", "B control-plane outage #1"} <= whats
    assert any("the operator's vantage, not the candidate" in n for n in p["notes"])


# A's outage window [1000, 1040], first unanswered poll at 1030: its own poll
# gap, 30 s, is how far its true edges can sit from the window's, so A covers
# [970, 1070] of wall-clock time — no other margin. Whatever of B's whole
# window A does not cover, k6 judges.
@pytest.mark.parametrize("b_t0, b_t1, b_only", [
    (1000, 1040, None),          # the same window: the vantage's
    (980, 1060, None),           # inside A's cover: the vantage's
    (1030, 1080, (1070, 1080)),  # 10 s past A's cover: B's alone
    (1040, 1080, (1070, 1080)),  # starts as A's ends: 10 s B-only
    (1200, 1240, (1200, 1240)),  # B only, later
])
def test_only_the_part_of_bs_outage_that_as_covers_is_the_vantages(tmp_path, b_t0, b_t1, b_only):
    a = _outage(P14, 1000, 1040, K0)
    b = _shift(_outage(P14, b_t0, b_t1, K0), SHIFT)
    p = _arms(tmp_path / "recorded", a, b)
    if b_only is None:
        assert p["verdict"] == "PASS", (p["invalid_reasons"], p["fail_reasons"])
        assert not p["b_outages_witnessed_served"] and _cpo_b(p)["value"] == "none"
    else:
        # B's own part judged by its recorded k6 (run 14's stream), whatever it says
        judged = p["b_outages_witnessed_served"] or p["candidate_not_ready"]["value"]["stretches"]
        (w,) = judged
        assert w["b_only"] == [[round(K0 + b_only[0], 1), round(K0 + b_only[1], 1)]]
        # B's own part with its customers failing: FAIL
        p = _arms(tmp_path / "failing", a, _b_failing(b, *b_only))
        assert p["verdict"] == "FAIL" and p["candidate_not_ready"]["result"] == "FAIL", p["fail_reasons"]


@pytest.mark.parametrize("b_t1", [1100, 1300, 1600])
def test_a_long_b_outage_touching_a_short_a_blip_is_judged_by_bs_k6(tmp_path, b_t1):
    """The #898 re-reviewer's probe: A blips 30 s (1000-1030; first unanswered
    1030: A covers [970, 1060]), B is down from 1010 to b_t1. Only what A's
    blip covers is the vantage's; the rest (1060 on) is B's alone, and B's
    k6 judges it: its customers failing -> FAIL. Never excused by A's blip."""
    a = _outage(P14, 1000, 1030, K0)
    b = _shift(_outage(P14, 1010, b_t1, K0), SHIFT)
    p = _arms(tmp_path / "failing", a, _b_failing(b, 1060, b_t1))
    assert p["verdict"] == "FAIL" and p["candidate_not_ready"]["result"] == "FAIL", p["fail_reasons"]
    (st,) = p["candidate_not_ready"]["value"]["stretches"]
    assert st["b_only"] == [[round(K0 + 1060.0, 1), round(K0 + b_t1, 1)]]
    p = _arms(tmp_path / "recorded", a, b)
    judged = p["b_outages_witnessed_served"] or p["candidate_not_ready"]["value"]["stretches"]
    (w,) = judged
    assert w["b_only"] == [[round(K0 + 1060.0, 1), round(K0 + b_t1, 1)]]


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


from tests.test_pair_ready_time import K0, P14, SHIFT, _arms, _failing, _shift  # noqa: E402
