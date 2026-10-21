"""Run-10 observability (#822, #817 review):

- snowplow's log is streamed for the whole run and scrubbed to metadata;
- a driver pod's death reaches verdict.json with its reason;
- "driver done" is logged only for a zero exit;
- mem606's stream is framed (a cut file is detected) and survives a crash;
- every verdict says the driver's load/lag are not comparable with runs <= 9.
"""

from __future__ import annotations

import json
import subprocess

import yaml

from bench import load606, load606_job, logscrub
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

PW = load606.SCALEUSER_PASSWORD


# ─── the scrubber ───────────────────────────────────────────────────────────


def test_scrub_keeps_metadata_and_hashes_everything_else():
    line = json.dumps({"time": "2026-10-21T06:00:00Z", "level": "INFO",
                       "msg": "refreshes: subscribed", "subsystem": "cache",
                       "user": "scaleuser-0001", "name": "scaleuser-0001-clientconfig",
                       "error": 'GET /x: {"spec":{"secret":"s3cr3t"}}',
                       "restaction_name": "incident-detail", "armed_keys": 3, "ok": True,
                       "nested": {"a": 1}})
    out, t = logscrub.scrub_line(line)
    rec = json.loads(out)
    assert t == "2026-10-21T06:00:00Z"
    assert (rec["msg"], rec["subsystem"], rec["restaction_name"]) == (
        "refreshes: subscribed", "cache", "incident-detail")
    assert rec["armed_keys"] == 3 and rec["ok"] is True
    for k in ("user", "name", "error", "nested"):
        assert rec[k].startswith("h:") and len(rec[k]) == 14, k
    assert "scaleuser" not in out and "s3cr3t" not in out and "clientconfig" not in out


def test_scrub_hashes_an_allowlisted_field_carrying_identity_or_a_denied_literal():
    out, _ = logscrub.scrub_line(json.dumps({"msg": "login scaleuser-0042 ok",
                                             "hint": f"pw {PW}"}), (PW,))
    assert "scaleuser" not in out and PW not in out


def test_scrub_turns_a_non_json_line_into_length_and_hash():
    out, t = logscrub.scrub_line("panic: body={secret}\n")
    assert t is None and json.loads(out)["unparsed"] is True and "secret" not in out


def test_scrub_cli_records_the_newest_time(tmp_path):
    last = tmp_path / "last"
    src = "\n".join(json.dumps({"time": f"2026-10-21T06:00:0{i}Z", "msg": "m"})
                    for i in range(3)) + "\n"
    p = subprocess.run(["python3", "-m", "bench.logscrub", "--last-time-file", str(last)],
                       input=src, capture_output=True, text=True, cwd=BENCH_DIR, check=True)
    assert len(p.stdout.splitlines()) == 3 and last.read_text() == "2026-10-21T06:00:02Z"


# ─── job pods: how a pod ended reaches the verdict ─────────────────────────


def _pods(state, phase, restarts=0):
    return {"items": [{"metadata": {"name": "load606-0-abc"},
                       "status": {"phase": phase, "containerStatuses": [
                           {"name": "driver", "ready": False, "restartCount": restarts,
                            "state": state}]}}]}


def test_a_dead_driver_pod_is_invalid_with_its_reason():
    recs = load606.job_pod_records(_pods({"terminated": {
        "reason": "OOMKilled", "exitCode": 137, "finishedAt": "2026-10-21T06:40:00Z"}},
        "Failed"), "load606")
    v = load606.evaluate([], [], [], job_pods=recs)
    assert v["verdict"] == "INVALID"
    assert ("load606.pod[load606-0-abc]: the pod Failed; container driver terminated: "
            "OOMKilled (exit 137) at 2026-10-21T06:40:00Z") in v["invalid_reasons"]


def test_a_lingering_or_cleanly_exited_pod_passes():
    for state, phase in (({"running": {}}, "Running"),
                         ({"terminated": {"reason": "Completed", "exitCode": 0}}, "Succeeded")):
        (c,) = load606.job_pod_checks(load606.job_pod_records(_pods(state, phase), "mem606"))
        assert c["result"] == "PASS", c


# ─── mem606: framed, stoppable, crash-proof ────────────────────────────────


class _Snow:
    def get(self, path, token):
        if path == "/readyz":
            return load606.Response(200, b"ok", 1.0)
        if path == "/debug/vars":
            return load606.Response(200, json.dumps({"memstats": {}}).encode(), 1.0)
        return load606.Response(404, b"", 1.0)


def test_memwatch_frames_its_stream_and_stops_on_request(tmp_path):
    recs, clock = [], [0.0]
    stop = tmp_path / "stop"

    def sleep(s):
        clock[0] += s
        if clock[0] >= 30:
            stop.write_text("")
    load606.memwatch(_Snow(), lambda: "tok", duration_s=3600, interval_s=15,
                     out_dir=str(tmp_path), sink=recs.append, stop_file=str(stop),
                     clock=lambda: clock[0], sleep=sleep)
    kinds = [r["kind"] for r in recs]
    assert kinds[0] == "mem_start" and kinds[-1] == "mem_end"
    assert recs[-1]["samples"] == kinds.count("mem") and recs[-1]["stopped"] is True
    assert clock[0] < 3600                                   # ended early, on request
    s = load606.framed_stream(load606.MEM_FRAMING, "mem-x.jsonl",
                              recs + [{"kind": "mem_exit", "rc": 0}])
    assert [c["result"] for c in
            load606.framed_stream_checks(load606.MEM_FRAMING, [s], required=False)] == ["PASS"]


def test_a_cut_mem_stream_is_invalid():
    recs = [{"kind": "mem_start", "t": 0}, {"kind": "mem", "t": 1}]
    s = load606.framed_stream(load606.MEM_FRAMING, "mem-mem606-0.jsonl", recs)
    (c,) = load606.framed_stream_checks(load606.MEM_FRAMING, [s], required=False)
    assert c["result"] == "FAIL" and "no mem_end record" in c["why"]


def test_mem_wrapper_survives_a_crashed_sampler(tmp_path):
    """The wrapper, not python, records the exit and lingers: run it with the
    sampler replaced by a crash and /out pointed at tmp."""
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a", user="u",
                               password="pw", duration_s=60, linger_s=0)
    job = next(d for d in yaml.safe_load_all(m) if d and d.get("kind") == "Job")
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    assert "--linger-s 0 --stop-file /out/stop" in script and "exec python3" not in script
    first, rest = script.split("python3 -u /opt/load606/load606.py memwatch", 1)
    rest = rest.split("|| rc=$?", 1)[1]
    sh = (first + "sh -c 'exit 3' || rc=$?" + rest).replace("/out/", f"{tmp_path}/")
    p = subprocess.run(["sh", "-c", sh], capture_output=True, text=True)
    assert p.returncode == 3
    (rec,) = load606._read_jsonl(str(tmp_path / "mem.jsonl"))
    assert rec["kind"] == "mem_exit" and rec["rc"] == 3
    assert (tmp_path / "exited").read_text().strip() == "3"


# ─── the verdict carries the comparability note ────────────────────────────


def test_every_verdict_says_driver_load_is_not_comparable_with_runs_up_to_9():
    notes = load606.evaluate([], [], [])["report"]["harness_notes"]
    assert any("NOT comparable with runs <= 9" in n for n in notes)


# ─── run606 end to end (fake tools) ────────────────────────────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_run606_streams_a_scrubbed_snowplow_log_and_records_job_pods(harness):
    env, state, ambient, tmp = harness
    env["LOG_STREAM_RETRY_S"] = "0.2"
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    out = tmp / "out"
    stream = (out / "snowplow-stream.log").read_text().splitlines()
    msgs = [json.loads(x).get("msg") for x in stream]
    assert "refreshes: subscribed" in msgs
    assert (out / ".log-stream-stop").exists()                # stopped at collect
    for f in out.glob("snowplow*.log"):
        text = f.read_text()
        assert "scaleuser" not in text and "clientconfig" not in text and PW not in text, f
    pods = [json.loads(x) for x in (out / "job-pods.jsonl").read_text().splitlines() if x]
    assert {r["job"] for r in pods} == {"load606", "fresh606", "mem606"}
    v = json.loads((out / "verdict.json").read_text())
    assert all(c["result"] == "PASS" for c in v["validity"]
               if c["check"].startswith(("load606.pod", "mem606.")))


def test_run606_logs_driver_failed_not_done_on_a_nonzero_exit(harness):
    env, state, ambient, tmp = harness
    env["FAKE_DRIVE_RC"] = "3"
    p = _run(env)
    assert "driver FAILED: load606-0=rc3" in p.stderr
    assert "driver done" not in p.stderr


def test_run606_driver_pod_death_reaches_the_verdict(harness):
    env, state, ambient, tmp = harness
    env.update(FAKE_DRIVER_FAILED="1", FAKE_DRIVER_POD_DEATH="1")
    _run(env)
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert any(r.startswith("load606.pod[load606-0]: the pod Failed; container c terminated: "
                            "OOMKilled (exit 137)") for r in v["invalid_reasons"])


def test_run606_a_cut_mem_stream_is_invalid_in_the_verdict(harness):
    env, state, ambient, tmp = harness
    env["FAKE_MEM_STREAM"] = "cut"
    _run(env)
    v = json.loads((tmp / "out" / "verdict.json").read_text())
    assert any(r.startswith("mem606.stream_complete[mem-mem606-0.jsonl]:") and
               "no mem_end record" in r for r in v["invalid_reasons"])
