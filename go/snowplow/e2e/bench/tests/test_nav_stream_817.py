"""#817: runs 8 and 9 recorded ZERO navigations, and the verdict only said
`warm_navs 0`.

Root cause (run 9 artifacts): the driver's JSONL went to the container's
stdout and was read back with `kubectl logs`, which after kubelet log rotation
returns only the current file. The collected file started mid-run (a
`push_refetch` at 07:05:47Z, no `login_curve` / `viewer_config` head) and
covered the last ~10 minutes, all after the navigation phase ended at
06:44:37Z. Those 10 minutes existed because closing the 1000 viewer streams
one by one took 31 min: HTTPResponse.close() waits for the blocked reader.

These arms prove, without a cluster:
  - the driver's stream is framed (drive_start .. drive_end) and the
    evaluator turns any stream that cannot prove it is whole into a named
    INVALID reason (run 9's own shape included);
  - `drive` against a real HTTP portal + /refreshes server records its
    navigations, frames them, and finishes promptly although every viewer
    stream sits idle in a read (the close no longer waits for it);
  - zero navigations is a FATAL exit, not a quiet success;
  - the Job writes to /out, turns Ready when the drive is over, and lingers.
"""

from __future__ import annotations

import http.server
import json
import subprocess
import threading
import time
import urllib.parse

import pytest
import yaml

from bench import load606, load606_job
from tests.test_navmodel import BUILDERS, INIT, TREE, canon
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401  (fixture)

# ─── the evaluator: a stream must prove it is whole ────────────────────────

NAV = {"kind": "nav", "nav": "route", "user": "scaleuser-0001", "page": "/dashboard",
       "wall_ms": 80.0, "calls": 4, "errors": 0, "sched_lag_ms": 2.0}


def _framed(navs, *, end_navs=None, ok=True, rc=0, exit_rc=0, exit_rec=True):
    recs = [{"kind": "drive_start", "t": 1.0}, {"kind": "login_curve", "users": 1,
                                                "failed": 0, "curve": []}]
    recs += navs
    recs.append({"kind": "drive_end", "t": 9.0, "ok": ok, "rc": rc,
                 "reason": None if ok else "the driver recorded zero navigations",
                 "navs": len(navs) if end_navs is None else end_navs})
    if exit_rec:
        recs.append({"kind": "drive_exit", "rc": exit_rc, "t": 9})
    return recs


def _evaluate(tmp_path, capsys, files: dict[str, list[dict]]):
    paths = []
    for name, recs in files.items():
        p = tmp_path / name
        p.write_text("".join(json.dumps(r) + "\n" for r in recs))
        paths.append(str(p))
    rc = load606.main(["evaluate", *paths])
    return rc, json.loads(capsys.readouterr().out)


def _stream_check(v):
    return [c for c in v["validity"] if c["check"].startswith("driver.stream")]


def test_run9_shape_is_invalid_and_says_why(tmp_path, capsys):
    """Run 9's collected file, in miniature: push_refetch + vars +
    viewer_stats, no head, no navigation."""
    run9 = ([{"kind": "push_refetch", "user": "scaleuser-0101", "key": "k",
              "path": "/call?x", "t_event": 1792566347.97, "start_lag_ms": 0.1,
              "wall_ms": 2.6, "status": 200}]
            + [{"kind": "vars", "t": 1792566356.4 + 15 * i} for i in range(3)]
            + [{"kind": "viewer_stats", "t": 1792565077.66, "streams_opened": 27202}])
    rc, v = _evaluate(tmp_path, capsys, {"nav-load606-0-89mwd.jsonl": run9})
    assert rc == 3 and v["verdict"] == "INVALID"
    reasons = "\n".join(v["invalid_reasons"])
    assert "driver.navs_recorded: the driver recorded zero navigations" in reasons
    assert ("driver.stream_complete[nav-load606-0-89mwd.jsonl]: the stream starts "
            "mid-run (first record: push_refetch at t=1792566347.97): its head is "
            "missing") in reasons
    assert "no drive_end record" in reasons and "no drive_exit record" in reasons


def test_whole_stream_passes_framing_and_counts_its_navigations(tmp_path, capsys):
    navs = [dict(NAV, visit=1, nav="first_load")] + [dict(NAV, visit=v) for v in range(2, 6)]
    rc, v = _evaluate(tmp_path, capsys, {"nav-load606-0.jsonl": _framed(navs)})
    assert [(c["check"], c["result"]) for c in _stream_check(v)] == [
        ("driver.stream_complete[nav-load606-0.jsonl]", "PASS")]
    assert v["report"]["navs"] == 5
    assert not any(r.startswith(("driver.stream", "driver.navs_recorded"))
                   for r in v["invalid_reasons"])


@pytest.mark.parametrize("kw,why", [
    (dict(end_navs=7), "drive_end counts 7 navigations, the stream holds 2: navigation "
                       "records were lost"),
    (dict(exit_rc=137), "the driver exited rc=137"),
    (dict(exit_rec=False), "no drive_exit record: the driver's pod died"),
    (dict(ok=False, rc=3), "the driver failed: the driver recorded zero navigations"),
])
def test_a_stream_that_cannot_prove_it_is_whole_is_invalid(tmp_path, capsys, kw, why):
    navs = [dict(NAV, visit=1), dict(NAV, visit=2)]
    rc, v = _evaluate(tmp_path, capsys, {"nav-load606-0.jsonl": _framed(navs, **kw)})
    (c,) = _stream_check(v)
    assert c["result"] == "FAIL" and why in c["why"]
    assert rc == 3 and any(why in r for r in v["invalid_reasons"])


def test_a_stream_cut_at_its_tail_is_invalid(tmp_path, capsys):
    recs = _framed([dict(NAV, visit=1), dict(NAV, visit=2)])[:-2]   # no drive_end/exit
    rc, v = _evaluate(tmp_path, capsys, {"nav-load606-0.jsonl": recs})
    (c,) = _stream_check(v)
    assert "no drive_end record" in c["why"] and rc == 3


def test_no_driver_stream_collected_is_invalid(tmp_path, capsys):
    rc, v = _evaluate(tmp_path, capsys, {"fresh-fresh606-0.jsonl": [
        {"kind": "fresh", "label": "list_dep@10", "converged": True, "latency_ms": 100}]})
    assert rc == 3
    assert ("driver.stream: no driver output was collected: no file holds a driver "
            "record") in v["invalid_reasons"]


# ─── `drive` end to end against a real HTTP portal ─────────────────────────

IDLE_S = 20.0          # each /refreshes stream: one frame, then silent this long


class _Portal(http.server.BaseHTTPRequestHandler):
    tree = {canon(k): v for k, v in TREE.items()}
    login_status = 200
    streams = 0

    def _json(self, obj, status=200, headers=()):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/basic/login":
            if _Portal.login_status != 200:
                return self._json({}, _Portal.login_status)
            return self._json({"accessToken": "tok"})
        if u.path == "/debug/vars":
            return self._json({})
        if self.path == BUILDERS:
            return self._json({"items": []})
        k = canon(self.path)
        if k not in _Portal.tree:
            return self._json({}, 404)
        name = urllib.parse.parse_qs(u.query).get("name", ["?"])[0]
        return self._json(_Portal.tree[k], headers=[
            (load606.REFRESH_HEADER_KEY, "key-" + name),
            (load606.REFRESH_HEADER_CLASS, "widgets")])

    def do_POST(self):
        if self.path != "/refreshes":
            return self._json({}, 404)
        n = int(self.headers.get("Content-Length") or 0)
        coords = json.loads(self.rfile.read(n) or b"[]")
        _Portal.streams += 1
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(f"event: refresh\ndata: key-{coords[0]['name']}\n\n".encode())
        self.wfile.flush()
        time.sleep(IDLE_S)            # an idle viewer: the reader blocks in recv

    def log_message(self, *a):
        pass


@pytest.fixture
def portal():
    _Portal.login_status, _Portal.streams = 200, 0
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Portal)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _drive(url, out, *, duration_s, think_s, users=3):
    return load606.main([
        "drive", "--snowplow-url", url, "--authn-url", url, "--password", "pw",
        "--user-start", "1", "--user-end", str(users), "--init", INIT,
        "--think-s", str(think_s), "--duration-s", str(duration_s),
        "--vars-interval", "0.5", "--nav-workers", "8", "--call-workers", "32",
        "--out", str(out)])


def _recs(path):
    return [json.loads(l) for l in path.read_text().splitlines()]


def test_drive_records_framed_navigations_and_closes_idle_viewers_promptly(portal, tmp_path):
    out = tmp_path / "nav.jsonl"
    t0 = time.monotonic()
    rc = _drive(portal, out, duration_s=2.0, think_s=0.3)
    elapsed = time.monotonic() - t0
    recs = _recs(out)
    navs = [r for r in recs if r["kind"] == "nav"]
    assert rc == 0
    assert recs[0]["kind"] == "drive_start"
    assert recs[-1]["kind"] == "drive_end" and recs[-1]["ok"] is True
    assert recs[-1]["navs"] == len(navs) > 3               # first loads AND route clicks
    assert {n["nav"] for n in navs} == {"first_load", "route"}
    assert not any(n.get("first_load_failed") for n in navs)
    assert any(r["kind"] == "push_refetch" for r in recs)  # the viewers were live
    assert _Portal.streams > 3
    # Every re-arm and the final close_all closed a stream whose reader sat in
    # an idle read (IDLE_S). HTTPResponse.close() alone waited for the reader:
    # run 9's 31-minute close_all. The drive must not wait for an idle stream.
    assert elapsed < IDLE_S / 2, f"drive took {elapsed:.1f}s: a close waited for an idle viewer"
    s = load606.driver_stream(str(out), recs + [{"kind": "drive_exit", "rc": 0}])
    assert [c["result"] for c in load606.driver_stream_checks([s])] == ["PASS"]


def test_drive_with_zero_navigations_is_fatal(portal, tmp_path, capsys):
    out = tmp_path / "nav.jsonl"
    # The first loads are spread over one think period (60 s); the drive is
    # over long before any of them is due.
    rc = _drive(portal, out, duration_s=0.05, think_s=60.0, users=2)
    recs = _recs(out)
    assert rc == 3
    assert "FATAL: the driver recorded zero navigations" in capsys.readouterr().err
    assert recs[0]["kind"] == "drive_start"
    assert recs[-1] == dict(recs[-1], kind="drive_end", ok=False, rc=3, navs=0,
                            reason="the driver recorded zero navigations")


def test_drive_refusing_at_login_still_frames_its_stream(portal, tmp_path, capsys):
    _Portal.login_status = 401
    out = tmp_path / "nav.jsonl"
    rc = _drive(portal, out, duration_s=1.0, think_s=0.3)
    recs = _recs(out)
    assert rc == 2 and "FATAL: first login failed" in capsys.readouterr().err
    assert [r["kind"] for r in recs] == ["drive_start", "drive_end"]
    assert recs[-1]["ok"] is False and recs[-1]["reason"].startswith("first login failed")


# ─── the Job: results on /out, Ready when final, lingers for collect ───────


def test_driver_job_writes_to_out_turns_ready_when_done_and_lingers():
    m = load606_job.render(users=4, shards=1, think_s=1, duration_s=1,
                           snowplow_url="http://s", authn_url="http://a", password="pw",
                           init=INIT, src="#", linger_s=7200)
    job = list(yaml.safe_load_all(m))[2]
    c = job["spec"]["template"]["spec"]["containers"][0]
    script = c["args"][0]
    assert "--out /out/nav.jsonl || rc=$?" in script and "/dev/stdout" not in script
    assert "exec python3" not in script            # the wrapper outlives the drive
    i_exit = script.index('"kind": "drive_exit"')
    assert i_exit < script.index('echo "$rc" > /out/done') < script.index("sleep 7200")
    assert script.rstrip().endswith('exit "$rc"')
    assert c["readinessProbe"]["exec"]["command"] == ["test", "-f", "/out/done"]
    assert {"name": "out", "mountPath": "/out"} in c["volumeMounts"]
    assert {"name": "out", "emptyDir": {}} in job["spec"]["template"]["spec"]["volumes"]


def test_mem_job_writes_its_trajectory_to_out():
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a",
                               user="u", password="pw", duration_s=60)
    assert "--out /out/mem.jsonl" in m and "/dev/stdout" not in m


def test_drive_exit_line_is_json_the_evaluator_reads(tmp_path):
    """The wrapper's printf must produce a line _read_jsonl accepts."""
    m =load606_job.render(users=1, shards=1, think_s=1, duration_s=1,
                           snowplow_url="http://s", authn_url="http://a", password="pw",
                           init=INIT, src="#")
    script = list(yaml.safe_load_all(m))[2]["spec"]["template"]["spec"]["containers"][0]["args"][0]
    line = next(l for l in script.splitlines() if "drive_exit" in l)
    out = tmp_path / "nav.jsonl"
    line = line.replace("/out/nav.jsonl", str(out))
    subprocess.run(["sh", "-c", "rc=137\n" + line], check=True)
    (rec,) = load606._read_jsonl(str(out))
    assert rec["kind"] == "drive_exit" and rec["rc"] == 137 and isinstance(rec["t"], int)


# ─── run606: collect from /out, say INVALID reasons in run.log ─────────────


def _run606(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_run606_collects_the_driver_file_and_mem_trajectory_from_out(harness):
    env, state, ambient, tmp = harness
    p = _run606(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    out = tmp / "out"
    assert "driver done: 1/1 load606 pod(s) Ready" in p.stderr
    assert _recs(out / "nav-load606-0.jsonl")[0]["kind"] == "drive_start"
    assert (out / "driver-load606-0.log").read_text().startswith("[login]")
    assert [r["kind"] for r in _recs(out / "mem-mem606-0.jsonl")] == [
        "mem_start", "mem", "heap_profile", "mem", "mem_end", "mem_exit"]
    assert not (out / "heap" / "mem.jsonl").exists()           # moved, not duplicated
    kube = [c["argv"] for c in _calls(state) if c["tool"] == "kubectl"]
    assert any("load606-0:/out/nav.jsonl" in a for a in kube if "cp" in a)
    assert not any("pod/mem606-0" in a for a in kube if "logs" in a)   # no log read for results


def test_run606_says_why_a_head_cut_stream_is_invalid(harness):
    env, state, ambient, tmp = harness
    env["FAKE_DRIVE_STREAM"] = "headcut"
    p = _run606(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]       # a verdict is a result
    log = (tmp / "out" / "run.log").read_text()
    assert "verdict exit=3" in log
    assert "INVALID: driver.navs_recorded: the driver recorded zero navigations" in log
    assert ("INVALID: driver.stream_complete[nav-load606-0.jsonl]: the stream starts "
            "mid-run (first record: push_refetch") in log


def test_run606_missing_driver_file_is_loud(harness):
    env, state, ambient, tmp = harness
    env["FAKE_DRIVE_STREAM"] = "nofile"
    _run606(env)
    log = (tmp / "out" / "run.log").read_text()
    assert "could not copy the driver's results (/out/nav.jsonl) from load606-0" in log
    assert "INVALID: driver.stream: no driver output was collected" in log


def test_run606_a_failed_driver_pod_fails_measure_at_once(harness):
    env, state, ambient, tmp = harness
    env["FAKE_DRIVER_FAILED"] = "1"
    t0 = time.monotonic()
    p = _run606(env)
    assert p.returncode != 0 and time.monotonic() - t0 < 120
    assert "measure FAILED: a load606 driver pod failed before its results were final" in p.stderr
    assert "collecting partial results before teardown" in p.stderr
