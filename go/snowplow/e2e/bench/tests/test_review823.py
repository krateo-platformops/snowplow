"""#823 gate (review823.md at 302a4848): one arm per finding.

1. mem_stop waits against a wall-clock deadline, execs included.
2. logscrub: every input the reviewer got through is now hashed; key names
   are allowlisted; cause / reason are never kept.
3. The log stream drops the line(s) an inclusive --since-time resends, and
   reads a replaced pod's tail.
4. Pending / Unknown / evicted / no pod / no mem606 stream are INVALID.
5. Teardown never kills a stale stream PID.
6. The stream follower never outlives the run (a follower that never exits on
   its own: success, failure at measure, failure at collect, SIGINT); restarts
   count; a count mismatch fails in both directions.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time

import pytest

from bench import load606, logscrub
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

# ─── 2. the scrubber ────────────────────────────────────────────────────────

LEAKS = [
    ({"msg": "user cyberjoker cannot list"}, "cyberjoker"),
    ({"msg": 'secrets "tenant-a-kubeconf" not found'}, "tenant-a-kubeconf"),
    ({"hint": "token=abcd1234efgh"}, "abcd1234"),
    ({"cause": '{"kind":"Status","message":"secret s3 is forbidden","body":"PAYLOAD"}'},
     "PAYLOAD"),
    ({"event": "subject=system:serviceaccount:team-a:robot"}, "robot"),
    ({"scaleuser-0001": "x"}, "scaleuser-0001"),
    ({"cyberjoker": 1}, "cyberjoker"),
    ({"reason": "user cyberjoker is not allowed"}, "cyberjoker"),
]


@pytest.mark.parametrize("rec,secret", LEAKS, ids=[s for _, s in LEAKS])
def test_the_reviewers_inputs_are_hashed(rec, secret):
    sc = logscrub.Scrubber(deny_literals=("scaleuser-",))
    out = sc.line(json.dumps(rec))
    assert secret not in out, out


def test_cause_and_reason_are_never_kept_verbatim():
    out = json.loads(logscrub.Scrubber().line(json.dumps(
        {"cause": "timeout", "reason": "Evicted", "msg": "list failed"})))
    assert out["cause"].startswith("h:") and out["reason"].startswith("h:")
    assert out["msg"] == "list failed"


def test_an_unknown_key_name_is_hashed_and_a_known_one_kept():
    out = json.loads(logscrub.Scrubber().line(json.dumps({"snap_seq": 16, "whoever": 2})))
    assert out["snap_seq"] == 16 and "whoever" not in out
    assert [v for k, v in out.items() if k.startswith("k:")] == [2]


def test_a_user_prefix_given_as_deny_hashes_a_key_and_a_kept_value():
    sc = logscrub.Scrubber(deny_literals=("acme-",))
    out = sc.line(json.dumps({"msg": "acme-42 logged in", "acme-42": 1}))
    assert "acme-42" not in out


def test_run9_log_messages_stay_readable():
    """The hardening keeps the operational messages run 9 actually logged."""
    sc = logscrub.Scrubber(deny_literals=("scaleuser-",))
    for msg in ("otel export/sdk error", "api call response failure",
                "rbac.evaluate.snapshot.miss", "refreshes: subscribed"):
        assert json.loads(sc.line(json.dumps({"msg": msg})))["msg"] == msg


# ─── 3. the stream: no duplicate on resume ─────────────────────────────────


def _scrub_cli(lines, state):
    p = subprocess.run(["python3", "-m", "bench.logscrub", "--kubelet-timestamps",
                        "--state-file", str(state)], input="\n".join(lines) + "\n",
                       capture_output=True, text=True, cwd=BENCH_DIR, check=True)
    return [json.loads(x) for x in p.stdout.splitlines()]


def test_an_inclusive_resume_drops_the_resent_lines_only(tmp_path):
    st = tmp_path / "state"
    first = _scrub_cli(["2026-10-21T06:00:01.5Z {\"msg\": \"a\"}",
                        "2026-10-21T06:00:02.25Z {\"msg\": \"b\"}",
                        "2026-10-21T06:00:02.25Z {\"msg\": \"c\"}"], st)
    # the resume: --since-time=06:00:02.25 resends b and c, then new lines,
    # one of them at the SAME instant (not a duplicate: a different line)
    again = _scrub_cli(["2026-10-21T06:00:02.250Z {\"msg\": \"b\"}",
                        "2026-10-21T06:00:02.25Z {\"msg\": \"c\"}",
                        "2026-10-21T06:00:02.25Z {\"msg\": \"d\"}",
                        "2026-10-21T06:00:03Z {\"msg\": \"e\"}"], st)
    assert [r["msg"] for r in first] == ["a", "b", "c"]
    assert [r["msg"] for r in again] == ["d", "e"]
    assert again[-1]["kts"] == "2026-10-21T06:00:03.000000000Z"


def test_the_stream_reads_a_replaced_pods_tail(harness):
    env, state, ambient, tmp = harness
    env.update(FAKE_SNOWPLOW_PODS="snowplow-old,snowplow-new", LOG_STREAM_RETRY_S="0.2")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    recs = [json.loads(x) for x in (tmp / "out" / "snowplow-stream.log").read_text().splitlines()]
    i_tail = next(i for i, r in enumerate(recs) if r.get("msg") == "old pod tail")
    assert recs[i_tail + 1] == dict(recs[i_tail + 1], bench_stream="pod_replaced", tail="read")
    calls = [json.loads(x)["argv"] for x in (state / "calls.jsonl").read_text().splitlines()]
    follows = [c for c in calls if "logs" in c and "-f" in c]
    assert any("pod/snowplow-new" in c for c in follows)


# ─── 4. pod records ─────────────────────────────────────────────────────────


def _pod(phase, **st):
    return {"items": [{"metadata": {"name": "mem606-x"},
                       "status": {"phase": phase, "containerStatuses": [], **st}}]}


@pytest.mark.parametrize("phase,extra,why", [
    ("Pending", {"conditions": [{"type": "PodScheduled", "status": "False",
                                 "reason": "Unschedulable"}]},
     "the pod is Pending at collect (Unschedulable)"),
    ("Unknown", {}, "the pod is Unknown at collect"),
    ("Failed", {"reason": "Evicted"}, "the pod Failed (Evicted)"),
])
def test_a_pod_that_did_not_run_or_was_evicted_is_invalid(phase, extra, why):
    (c,) = load606.job_pod_checks(load606.job_pod_records(_pod(phase, **extra), "mem606"))
    assert c["result"] == "FAIL" and why in c["why"]


def test_a_job_with_no_pod_is_invalid():
    recs = load606.job_pod_records({"items": []}, "fresh606")
    v = load606.evaluate([], [], [], job_pods=recs)
    assert "fresh606.pod: no fresh606 pod at collect: it never started, or it was deleted" \
        in v["invalid_reasons"]


def test_a_restart_alone_is_invalid():
    """Running again after a restart, no termination on record: still a death."""
    recs = load606.job_pod_records({"items": [{"metadata": {"name": "load606-0"},
        "status": {"phase": "Running", "containerStatuses": [
            {"name": "driver", "ready": True, "restartCount": 2, "state": {"running": {}}}]}}]},
        "load606")
    (c,) = load606.job_pod_checks(recs)
    assert c["result"] == "FAIL" and "restarted 2x" in c["why"]


def test_a_deployed_mem606_without_a_stream_is_invalid():
    recs = load606.job_pod_records(_pod("Running"), "mem606")
    v = load606.evaluate([], [], [], job_pods=recs, mem_streams=[])
    assert ("mem606.stream: no mem606 output was collected: no file holds a mem606 record"
            in v["invalid_reasons"])


@pytest.mark.parametrize("framing,end,items", [
    (load606.MEM_FRAMING, {"kind": "mem_end", "samples": 1}, 2),     # fewer counted
    (load606.MEM_FRAMING, {"kind": "mem_end", "samples": 3}, 2),     # more counted
    (load606.DRIVER_FRAMING, {"kind": "drive_end", "navs": 1, "ok": True}, 2),
])
def test_a_count_mismatch_fails_in_both_directions(framing, end, items):
    recs = ([{"kind": framing.start, "t": 0}] + [{"kind": framing.item}] * items
            + [end, {"kind": framing.exit, "rc": 0}])
    s = load606.framed_stream(framing, "f.jsonl", recs)
    (c,) = load606.framed_stream_checks(framing, [s], required=True)
    assert c["result"] == "FAIL" and "records were lost" in c["why"]


# ─── 1. mem_stop's wall-clock deadline ─────────────────────────────────────


def test_mem_stop_is_bounded_by_wall_clock_with_slow_execs(harness):
    """Each `test -f /out/exited` exec takes 3 s and fails. With a 6 s
    deadline, mem_stop must give up at ~6 s; counting only its 5 s sleeps it
    ran 3 + 5 + 3 + 5 + 3 = 19 s."""
    env, state, ambient, tmp = harness
    env.update(FAKE_MEM_NEVER_EXITS="1", FAKE_EXEC_SLOW="3", MEM_STOP_WAIT_S="6")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    log = (tmp / "out" / "run.log").read_text()
    assert "mem606-0 did not end its stream within 6s" in log
    calls = [json.loads(x) for x in (state / "calls.jsonl").read_text().splitlines()]
    waits = [c for c in calls if c["argv"][-3:] == ["test", "-f", "/out/exited"]]
    assert 1 <= len(waits) <= 2, len(waits)          # 19 s of execs would be 3
    assert p.returncode == 0


# ─── 5. no stale PID ────────────────────────────────────────────────────────


def _functions(*names):
    src = (BENCH_DIR / "run606.sh").read_text()
    out = []
    for n in names:
        body = src.split(f"\n{n}() {{\n", 1)[1].split("\n}\n", 1)[0]
        out.append(f"{n}() {{\n{body}\n}}")
    return "\n".join(out)


def test_stop_without_a_pidfile_kills_nothing(tmp_path):
    """The parent shell's variables survive collect's subshell: once the
    pidfile is gone, teardown's stop must not kill whatever owns that PID now."""
    victim = subprocess.Popen(["sleep", "30"])
    try:
        script = (_functions("stop_log_stream")
                  + f'\nOUT="{tmp_path}"; LOG_STREAM_PIDFILE="{tmp_path}/.log-stream.pid"\n'
                  + f"LOG_STREAM_PID={victim.pid}\nstop_log_stream\n")
        subprocess.run(["bash", "-c", script], check=True, timeout=10)
        time.sleep(0.2)
        assert victim.poll() is None, "a PID with no pidfile was killed"
    finally:
        victim.kill()


# ─── 6. the follower never outlives the run ────────────────────────────────


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _followers_gone(state):
    pids = [int(x) for x in (state / "followers").read_text().split()]
    deadline = time.time() + 5
    while time.time() < deadline and any(_alive(p) for p in pids):
        time.sleep(0.1)
    return pids, [p for p in pids if _alive(p)]


@pytest.mark.parametrize("case", ["success", "fail_at_measure", "fail_at_collect", "sigint"])
def test_a_never_ending_follower_is_stopped_on_every_exit_path(harness, case):
    env, state, ambient, tmp = harness
    env.update(FAKE_FOLLOW_FOREVER="1", LOG_STREAM_RETRY_S="0.2")
    if case == "fail_at_measure":
        env["BENCH_606_FAIL_AT"] = "measure"
    if case == "fail_at_collect":
        env["BENCH_606_FAIL_AT"] = "collect"
    if case == "sigint":
        env["FAKE_SLOW_WAIT"] = "30"
        p = subprocess.Popen(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                             stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        for line in p.stderr:
            if "bench-loadgen wait job/fresh606" in line:
                time.sleep(1.0)
                p.send_signal(signal.SIGINT)       # the script only, not its group
                break
        p.communicate(timeout=120)
        rc = p.returncode
    else:
        rc = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                            capture_output=True, text=True, timeout=300).returncode
    assert rc == {"success": 0, "sigint": 130}.get(case, 1), rc
    pids, left = _followers_gone(state)
    for p in left:                     # never leave a leak behind, even on failure
        os.kill(p, signal.SIGKILL)
    assert pids, "the follower never started"
    assert not left, f"follower(s) {left} outlived the run ({case})"
    assert '"msg": "tick"' in (tmp / "out" / "snowplow-stream.log").read_text()
