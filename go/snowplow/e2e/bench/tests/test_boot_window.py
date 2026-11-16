"""Runs 26j and 27 (2026-11-16): the measure's first snowplow read, /debug/vars as the debug reader (the
runtime606 Job), landed 6-11 s after the rebooted pod's Ready, got no byte for 30 s, and failed the arm
on "/debug/vars answered 0": one attempt, and a record that kept neither the error nor where the time went.

The fix (load606 read_answered, go_runtime_read): the start gate's rule for the same endpoint. A real HTTP
answer that is not 200 ends the read at once; no answer is tried again within the step's EXISTING budget
(the step's deadline, run606 GO_RUNTIME_S), each attempt keeping its 30 s bound; every attempt is recorded
(status, elapsed, connect, first byte, the error's class), and a stall is a finding of its own in the
record, the run log and the report, however the read ended. And its boot window (the owner's ruling: no
separate rehearsal, every measuring run's own read carries the probe; load606 BootWindow, report-only):
/readyz and the boot root every 5 s while the read runs and one round more, one goroutine dump when a
request passes 5 s without a byte, said by bench/bootwindow.py.
"""

from __future__ import annotations

import gzip
import http.server
import json
import os
import socket
import subprocess
import sys
import threading
import time

import pytest

from bench import bootwindow, load606, snowplow_env
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

GR = {"snowplow_go_runtime": {"gogc": 50, "memory_limit_bytes": 7516192768, "memory_limit_source": "cgroup_v2",
                              "cgroup_memory_limit_bytes": 8589934592},
      "memstats": {"NumGC": 7, "PauseTotalNs": 3_000_000, "GCCPUFraction": 0.01, "HeapAlloc": 2 ** 30}}
NO_BYTE = load606.Response(0, b"", 30001.0, "TimeoutError: timed out", connect_ms=0.9, first_byte_ms=None,
                           error_class="TimeoutError")
REFUSED = load606.Response(0, b"", 1.2, "URLError: <urlopen error [Errno 111] Connection refused>",
                           error_class="ConnectionRefusedError")


class Clock:
    """A clock the fake client and the sleeps advance (no real time passes)."""

    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


class Seq:
    """A client answering a fixed sequence of Responses; each takes its own ms of the clock."""

    def __init__(self, clock, answers, timeout=30.0):
        self.clock, self.answers, self.timeout, self.calls = clock, list(answers), timeout, []

    def get(self, path, token, timing=None):
        self.calls.append((path, token))
        r = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        self.clock.t += r.ms / 1000.0
        return r


def _read(answers, budget=300.0):
    clk = Clock()
    c = Seq(clk, answers)
    lines = []
    rec = load606.go_runtime_read(c, "tok", deadline=clk.t + budget, clock=clk, sleep=clk.sleep, sink=lines.append)
    return rec, c, lines


OK = load606.Response(200, json.dumps(GR).encode(), 14.0, connect_ms=0.7, first_byte_ms=13.0)


# ─── the rule: no answer retried within the budget, a refusal at once, the stall kept ───

def test_no_answer_then_200_passes_and_records_the_stall():
    rec, c, lines = _read([NO_BYTE, OK])
    assert rec["ok"] and rec["effective"]["gogc"] == 50
    read = rec["read"]
    assert (read["attempts"], read["no_answer"], read["answered"]) == (2, 1, 200)
    st = read["stall"]
    assert st["no_answer_attempts"] == 1 and st["answered"] == 200
    # the first attempt began at 1000, ran 30.0 s, the poll 5 s, the answer 0.014 s
    assert st["seconds_to_answer"] == 35.0
    assert st["first"]["error_class"] == "TimeoutError" and st["first"]["stalled_at"] == "first_byte"
    assert st["first"]["connect_ms"] == 0.9 and st["first"]["first_byte_ms"] is None
    # each attempt streamed as it ended, the record never carries a body
    assert [x["kind"] for x in lines] == ["go_runtime_attempt", "go_runtime_attempt"]
    assert "body" not in json.dumps(lines) and "memstats" not in json.dumps(rec["read"])


def test_a_refusal_fails_at_once_and_is_not_waited_out():
    rec, c, _ = _read([load606.Response(403, b"", 3.0, "http 403", connect_ms=0.5, first_byte_ms=2.9), OK])
    assert len(c.calls) == 1
    assert not rec["ok"] and rec["why"] == "/debug/vars answered 403"
    assert rec["read"]["stall"] is None and rec["read"]["answered"] == 403


def test_a_refusal_after_no_answer_still_fails_and_keeps_the_stall():
    rec, c, _ = _read([REFUSED, load606.Response(401, b"", 3.0, "http 401")])
    assert len(c.calls) == 2 and rec["why"] == "/debug/vars answered 401"
    assert rec["read"]["stall"]["no_answer_attempts"] == 1
    assert rec["read"]["stall"]["first"]["error_class"] == "ConnectionRefusedError"
    assert rec["read"]["stall"]["first"]["stalled_at"] == "connect"


def test_no_answer_until_the_budget_fails_naming_attempts_and_seconds():
    rec, c, _ = _read([NO_BYTE], budget=300.0)
    # attempts start at 0, 35, ..., 245 (each 30 s, 5 s apart): one more would end at 310 > 300
    assert len(c.calls) == 8
    assert not rec["ok"] and rec["effective"] is None
    assert rec["why"] == ("/debug/vars: no answer to 8 attempt(s) over 275.0 s (each bounded at 30 s, 5 s apart, "
                          "within the step's budget of 300.0 s); the last: TimeoutError after 30001.0 ms: "
                          "connected, no byte of an answer (connected in 0.9 ms)")
    assert rec["read"]["answered"] is None and rec["read"]["stall"]["no_answer_attempts"] == 8


def test_no_attempt_starts_that_could_not_end_before_the_deadline():
    rec, c, _ = _read([OK], budget=29.0)
    assert c.calls == [] and not rec["ok"]
    assert "not one attempt (30 s) fits" in rec["why"]


def test_the_first_attempt_answered_is_no_stall():
    rec, c, _ = _read([OK])
    assert rec["ok"] and rec["read"]["stall"] is None and rec["read"]["attempts"] == 1


# ─── the HTTP client says where the time went, and the failure's own class ───

def _server(handler_sleep_s=0.0, status=200):
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(handler_sleep_s)
            body = b'{"x": 1}'
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def test_the_client_times_connect_and_first_byte_of_an_answer():
    srv = _server(handler_sleep_s=0.3)
    try:
        r = load606.HttpClient(f"http://127.0.0.1:{srv.server_address[1]}").get("/x", None)
    finally:
        srv.shutdown()
    assert r.status == 200 and r.body == b'{"x": 1}'
    assert r.connect_ms is not None and r.connect_ms < 100
    assert 250 <= r.first_byte_ms <= r.ms


def test_connected_and_no_byte_is_a_timeout_at_first_byte():
    ls = socket.socket()
    ls.bind(("127.0.0.1", 0))
    ls.listen(8)                           # accepts (the kernel's backlog), never answers
    try:
        tm: dict = {}
        r = load606.HttpClient(f"http://127.0.0.1:{ls.getsockname()[1]}", timeout=0.5).get("/debug/vars", "t", timing=tm)
    finally:
        ls.close()
    a = r.attempt()
    assert r.status == 0 and a["error_class"] == "TimeoutError" and a["stalled_at"] == "first_byte"
    assert a["connect_ms"] is not None and a["first_byte_ms"] is None and a["elapsed_ms"] >= 500
    assert "connect_ms" in tm and "first_byte_ms" not in tm          # the live timing a watcher reads


def test_no_connection_is_a_refusal_at_connect():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()                              # nothing listens there
    a = load606.HttpClient(f"http://127.0.0.1:{port}", timeout=2).get("/debug/vars", "t").attempt()
    assert a["status"] == 0 and a["error_class"] == "ConnectionRefusedError" and a["stalled_at"] == "connect"
    assert a["connect_ms"] is None


def test_an_http_error_answer_keeps_its_status_and_timing():
    srv = _server(status=403)
    try:
        r = load606.HttpClient(f"http://127.0.0.1:{srv.server_address[1]}").get("/debug/vars", "t")
    finally:
        srv.shutdown()
    assert r.status == 403 and r.first_byte_ms is not None and r.error_class == ""


# ─── the finding in the record, the run log and the report ──────────────────

def _jsonl(tmp_path, rows):
    p = tmp_path / "go-runtime.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return str(p)


def test_the_stall_is_its_own_finding_in_the_check_and_the_report(tmp_path):
    rec, _, lines = _read([NO_BYTE, NO_BYTE, OK])
    path = _jsonl(tmp_path, lines + [rec])
    said = snowplow_env.read_text(snowplow_env.read_of(path))
    assert said.startswith("STALLED: 2 attempt(s) with no answer (the first: TimeoutError after 30001.0 ms: "
                           "connected, no byte of an answer (connected in 0.9 ms)), then HTTP 200 70.0 s after")
    tr = tmp_path / "tracing.json"
    tr.write_text("{}")
    chk = snowplow_env.check({"GOGC": "50"}, str(tr), effective_path=path)
    assert chk["runtime_read"]["stall"]["no_answer_attempts"] == 2
    out = tmp_path / "snowplow-env.json"
    out.write_text(json.dumps(chk))
    rep = snowplow_env.report(str(out))
    assert rep["runtime_read_stall"]["no_answer_attempts"] == 2
    assert any(ln.startswith(f"INFO {snowplow_env.READ_FINDING}: STALLED: 2 attempt(s)") for ln in rep["lines"])


def test_an_unstalled_read_says_so(tmp_path):
    rec, _, lines = _read([OK])
    assert snowplow_env.read_text(snowplow_env.read_of(_jsonl(tmp_path, lines + [rec]))) == \
        "answered 200 on its first attempt"


def test_a_job_cut_short_is_read_from_its_attempt_lines(tmp_path):
    _, _, lines = _read([NO_BYTE] * 3, budget=300.0)
    path = _jsonl(tmp_path, lines[:3])                      # no record: the step's wait ended first
    eff, why = snowplow_env.effective_of(path)
    assert eff is None and why.startswith("the Job that reads the process's settings printed no reading: its 3 "
                                          "attempt(s) so far had no answer, over 100.0 s")
    assert snowplow_env.read_text(snowplow_env.read_of(path)).startswith("NO ANSWER: 3 attempt(s), none answered")


def test_the_cli_requires_the_steps_deadline():
    p = subprocess.run([sys.executable, str(BENCH_DIR / "bench" / "load606.py"), "go-runtime", "--snowplow-url", "x",
                        "--authn-url", "x", "--user", "u", "--password", "p"], capture_output=True, text=True)
    assert p.returncode == 2 and "--deadline" in p.stderr


# ─── the real run: the stall and the boot window said in the run log; the read's budget is the step's ───

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_the_rehearsal_logs_the_stall_and_its_boot_window_and_passes_once_answered(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_GO_RUNTIME_FAIL="stall")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    assert f"{snowplow_env.READ_FINDING}: STALLED: 1 attempt(s) with no answer" in log
    job = json.load(open(state / "job-runtime606"))
    args = " ".join(job["spec"]["template"]["spec"]["containers"][0]["args"])
    assert "--deadline " in args and '--preflight-user "scaleuser-0000"' in args and '--init "/call?' in args
    # the boot window: its block in the run log, its record, its dump written to its own file
    assert "INFO boot window: READING: only /debug/vars not served: the expvar render" in log, log[-4000:]
    bw = json.loads((out / "boot-window.json").read_text())
    assert bw["dump"]["path"] == str(out / "boot-goroutines.txt.gz")
    assert gzip.decompress((out / "boot-goroutines.txt.gz").read_bytes()).startswith(b"goroutine 1 [select")
    assert f"goroutine dump (taken when /debug/vars (debug reader, the Go runtime read) was 5.3 s without a " \
           f"byte): saved {out / 'boot-goroutines.txt.gz'}" in log
    rec = json.loads((out / "snowplow-env.json").read_text())
    assert rec["verified"] is True and rec["boot_window"]["reading"] == bw["reading"]
    # the dump's body is in no log line, the bench password nowhere
    assert "goroutine 1 [select" not in log
    from bench.load606 import SCALEUSER_PASSWORD
    assert SCALEUSER_PASSWORD not in log and SCALEUSER_PASSWORD not in (out / "go-runtime-job.yaml").read_text()


def test_no_answer_to_the_end_fails_the_arm_naming_attempts_and_seconds(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_GO_RUNTIME_FAIL="noanswer")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log
    assert "/debug/vars: no answer to 8 attempt(s) over 275.0 s" in log, log[-3000:]
    assert f"{snowplow_env.READ_FINDING}: NO ANSWER: 8 attempt(s)" in log
    assert "INFO boot window: READING: only /debug/vars not served: the expvar render" in log


def test_the_boot_window_never_changes_the_gate(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    assert "INFO boot window: READING: no stall in the window" in log
    # a boot window that cannot be read (its file gone) says so, and the verdict on the settings stands
    rec = snowplow_env.check({"GOGC": "100"}, str(tmp / "out" / "tracing.json"),
                             effective_path=str(tmp / "out" / "go-runtime.jsonl"),
                             boot_window_path=str(tmp / "nowhere.json"))
    assert rec["verified"] is True and rec["boot_window"] is None
    (tmp / "env.json").write_text(json.dumps(rec))
    assert "INFO boot window: NO RECORD" in " ".join(snowplow_env.report(str(tmp / "env.json"))["lines"])


# ─── the boot window (load606 BootWindow) ──────────────────────────────────

class WindowClient:
    """Answers per path at once, but /debug/vars (and, with `stall_call`, the boot root) holds `stall_s`
    with no byte, then times out (status 0); `dump_stalls`: the dump does too. Real sleeps: the window's
    watch sees the requests in flight."""

    def __init__(self, stall_s=0.5, stall_vars=True, stall_call=False, dump_stalls=False, timeout=2.0,
                 slow_body=False):
        self.stall_s, self.stall_vars, self.stall_call, self.dump_stalls = stall_s, stall_vars, stall_call, dump_stalls
        self.timeout, self.slow_body = timeout, slow_body
        self.calls, self.lock = [], threading.Lock()

    def get(self, path, token, timing=None):
        with self.lock:
            self.calls.append((path, token))
        tm = timing if timing is not None else {}
        tm["connect_ms"] = 0.5
        hold = ((path == "/debug/vars" and self.stall_vars) or (path.startswith("/call") and self.stall_call)
                or (path.endswith("debug=2") and self.dump_stalls))
        if hold:
            time.sleep(self.stall_s)
            return load606.Response(0, b"", self.stall_s * 1000, "TimeoutError: timed out", connect_ms=0.5,
                                    error_class="TimeoutError")
        tm["first_byte_ms"] = 1.0
        if self.slow_body and path == "/debug/vars":
            time.sleep(self.stall_s)               # answered at once, its body slow: no stall
        body = (json.dumps(GR).encode() if path == "/debug/vars" else
                b"goroutine 1 [chan receive, 3 minutes]:\nmain.main()\n" if path.endswith("debug=2") else b"{}")
        return load606.Response(200, body, 1.5, connect_ms=0.5, first_byte_ms=1.0)


def _window(client, *, read_s=1.2, stall_s=0.2, every_s=0.2, deadline_s=30.0):
    """A window around a stand-in for the Go runtime read: one tracked /debug/vars request (the client
    decides how it goes) every 0.3 s for read_s, then the read ends."""
    recs = []
    t0 = time.time()
    w = load606.BootWindow(client, "/call?name=app-shell", {"debug_reader": "DBG", "preflight_user": "PFU"},
                           deadline=t0 + deadline_s, sink=recs.append, every_s=every_s, stall_s=stall_s,
                           tick_s=0.02).start()
    while time.time() < t0 + read_s:
        timing, done = w.track()
        try:
            client.get("/debug/vars", "DBG", timing=timing)
        finally:
            done()
        time.sleep(0.05)
    t_read_end = time.time()            # the read ended here: one more round must start after it
    end = dict(w.finish(), t_read_end=t_read_end)
    return recs, end


def boot_window_identities():
    """The proof tests/test_debug_reader.py names for BootWindow's /debug read (its guard's table)."""
    c = WindowClient()
    recs, _ = _window(c)
    who = {}
    for path, tok in c.calls:
        who.setdefault(path, set()).add(tok)
    assert who["/readyz"] == {None}
    assert who["/call?name=app-shell"] == {"PFU"}
    assert who["/debug/pprof/goroutine?debug=2"] == {"DBG"}           # the dump
    text = json.dumps([r for r in recs if r["kind"] != "boot_dump_chunk"])
    assert "DBG" not in text and "PFU" not in text and "chan receive" not in text


def test_the_window_probes_until_the_read_ends_and_one_round_more():
    recs, end = _window(WindowClient(stall_vars=False), read_s=0.7, every_s=0.2)
    rounds = sorted({r["round"] for r in recs if r["kind"] == "boot_probe"})
    assert rounds == list(range(end["rounds"])) and end["rounds"] >= 3
    for k in rounds:
        assert {r["route"] for r in recs if r["kind"] == "boot_probe" and r["round"] == k} == {"readyz", "call"}
    after = sorted({r["round"] for r in recs if r["kind"] == "boot_probe" and r["t"] >= end["t_read_end"]})
    assert after == [rounds[-1]]                          # exactly one round after the read ended
    assert recs[0]["kind"] == "boot_window_start" and recs[-1]["kind"] == "boot_window_end"


def test_one_dump_when_a_request_passes_the_stall_bound_and_only_one():
    recs, end = _window(WindowClient(stall_s=0.5), read_s=1.5)
    dumps = [r for r in recs if r["kind"] == "boot_dump"]
    assert len(dumps) == 1 and end["dump"] is True
    d = dumps[0]
    assert d["status"] == 200 and d["trigger"]["route"] == "vars" and d["trigger"]["waited_s"] >= 0.2
    chunks = [r for r in recs if r["kind"] == "boot_dump_chunk"]
    assert len(chunks) == d["chunks"] >= 1
    import base64
    assert gzip.decompress(base64.b64decode("".join(c["data"] for c in chunks))).startswith(b"goroutine 1 [chan")


def test_a_dump_that_stalls_is_recorded_so():
    recs, _ = _window(WindowClient(stall_s=0.5, dump_stalls=True), read_s=1.0)
    [d] = [r for r in recs if r["kind"] == "boot_dump"]
    assert d["status"] == 0 and d["error_class"] == "TimeoutError" and d["chunks"] == 0
    assert not [r for r in recs if r["kind"] == "boot_dump_chunk"]


def test_no_stall_no_dump():
    recs, end = _window(WindowClient(stall_vars=False), read_s=0.6)
    assert end["dump"] is False and not [r for r in recs if r["kind"] == "boot_dump"]


def test_a_slow_body_after_its_first_byte_is_no_stall_and_takes_no_dump():
    recs, end = _window(WindowClient(stall_vars=False, slow_body=True, stall_s=0.5), read_s=1.0)
    assert end["dump"] is False


def test_a_round_that_cannot_end_before_the_deadline_is_not_started():
    recs, end = _window(WindowClient(stall_vars=False, timeout=30.0), read_s=0.3, deadline_s=29.0)
    assert end["rounds"] == 0 and end["skipped"] and not [r for r in recs if r["kind"] == "boot_probe"]


def test_the_job_prints_the_window_and_the_record_last(tmp_path):
    """The real Job program (`load606 go-runtime`) against a local server whose /debug/vars holds its first
    byte past the stall bound once: the attempts, the window's probes and its dump, then the record."""
    hits = {"vars": 0}

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/debug/vars":
                hits["vars"] += 1
                if hits["vars"] == 1:
                    time.sleep(6.5)                  # past BOOT_STALL_S, within the 30 s bound
            body = (b'{"accessToken": "t"}' if self.path == "/basic/login" else
                    json.dumps(GR).encode() if self.path == "/debug/vars" else
                    b"goroutine 1 [select]:\n" if "debug=2" in self.path else b"{}")
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        p = subprocess.run([sys.executable, str(BENCH_DIR / "bench" / "load606.py"), "go-runtime", "--snowplow-url",
                            url, "--authn-url", url, "--user", "u", "--password", "p", "--preflight-user", "v",
                            "--init", "/call?name=app-shell", "--deadline", str(int(time.time()) + 120)],
                           capture_output=True, text=True, timeout=90)
    finally:
        srv.shutdown()
    rows = [json.loads(ln) for ln in p.stdout.splitlines()]
    assert p.returncode == 0, p.stdout + p.stderr
    assert rows[-1]["kind"] == "go_runtime" and rows[-1]["ok"] and rows[-1]["read"]["gc"]["num_gc"] == 7
    kinds = [r["kind"] for r in rows]
    assert kinds[0] == "boot_window_start" and "boot_window_end" in kinds
    assert kinds.count("boot_dump") == 1 and "boot_dump_chunk" in kinds
    assert {r["route"] for r in rows if r["kind"] == "boot_probe"} == {"readyz", "call"}
    (tmp_path / "go-runtime.jsonl").write_text(p.stdout)
    rec = bootwindow.summarize(str(tmp_path / "go-runtime.jsonl"), None, str(tmp_path / "dump.gz"))
    assert rec["reading"] == "only /debug/vars not served: the expvar render"
    assert gzip.decompress((tmp_path / "dump.gz").read_bytes()) == b"goroutine 1 [select]:\n"


# ─── the report's reading key ──────────────────────────────────────────────

def _p(route, t, fb_ms, status=200):
    return {"route": route, "t": t, "status": status if fb_ms is not None else 0,
            "elapsed_ms": fb_ms if fb_ms is not None else 30000.0, "first_byte_ms": fb_ms,
            **({} if fb_ms is not None else {"error_class": "TimeoutError", "stalled_at": "first_byte"})}


@pytest.mark.parametrize("stalls,want", [
    (set(), "no stall in the window"),
    ({"readyz", "call", "vars"}, "every route not served: network or endpoint churn"),
    ({"call", "vars"}, "/readyz fast, the gated routes not served: the auth/refresh gate"),
    ({"vars"}, "only /debug/vars not served: the expvar render"),
    ({"call"}, "a pattern the key does not name: not served the boot root /call (preflight user)"),
])
def test_the_reading_key(stalls, want):
    routes = {r: bootwindow.route_summary([_p(r, 106.0, None if r in stalls else 3.0)], 100.0, 5.0)
              for r in bootwindow.ROUTES}
    assert bootwindow.reading(routes) == want


def test_a_late_first_byte_is_a_stall_too():
    s = bootwindow.route_summary([_p("vars", 106.0, 12000.0)], 100.0, 5.0)
    assert s["stalled"] == 1 and s["max_no_byte_s"] == 12.0 and s["from_s"] == 6.0 and s["stall_s"] == 12.0


def test_a_fast_failure_is_not_served_but_no_stall():
    refused = {"route": "vars", "t": 106.0, "status": 0, "elapsed_ms": 2.0, "connect_ms": None,
               "first_byte_ms": None, "error_class": "ConnectionRefusedError", "stalled_at": "connect"}
    s = bootwindow.route_summary([refused], 100.0, 5.0)
    assert (s["stalled"], s["failed_fast"], s["not_served"]) == (0, 1, 1)
    routes = {r: bootwindow.route_summary([dict(refused, route=r)], 100.0, 5.0) for r in bootwindow.ROUTES}
    assert bootwindow.reading(routes) == "every route not served: network or endpoint churn"


def _log(tmp_path, ready):
    rows = [{"kind": "boot_window_start", "t": ready + 6.0, "every_s": 5.0, "stall_s": 5.0, "timeout_s": 30.0}]
    for k in range(4):
        t = ready + 6.0 + 5 * k
        rows += [dict(_p("readyz", t, 2.0), kind="boot_probe", round=k),
                 dict(_p("call", t, None if k < 2 else 4.0), kind="boot_probe", round=k)]
    rows += [dict(_p("vars", ready + 6.0, None), kind="go_runtime_attempt", n=1, t_end=ready + 36.0),
             dict(_p("vars", ready + 41.0, 15.0), kind="go_runtime_attempt", n=2, t_end=ready + 41.1),
             {"kind": "boot_dump", "t": ready + 11.3, "trigger": {"route": "vars", "waited_s": 5.3}, "status": 0,
              "elapsed_ms": 30000.0, "first_byte_ms": None, "error_class": "TimeoutError", "stalled_at": "first_byte",
              "bytes": 0, "chunks": 0},
             {"kind": "boot_window_end", "t": ready + 46.0, "rounds": 4, "dump": True, "skipped": None},
             {"kind": "go_runtime", "ok": True, "read": {"gc": {"gogc": 50, "num_gc": 7}}}]
    (tmp_path / "go-runtime.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    (tmp_path / "pod-before.jsonl").write_text(json.dumps({"kind": "snowplow_pod", "ready_at": ready}) + "\n")


def test_the_block_from_a_runs_files(tmp_path):
    ready = 1794812705.0
    _log(tmp_path, ready)
    out = tmp_path / "boot-window.json"
    p = subprocess.run([sys.executable, "-m", "bench.bootwindow", "said", "--effective", str(tmp_path / "go-runtime.jsonl"),
                        "--pod", str(tmp_path / "pod-before.jsonl"), "--dump", str(tmp_path / "d.gz"), "--out", str(out)],
                       cwd=BENCH_DIR, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    said = p.stdout
    assert "READING: /readyz fast, the gated routes not served: the auth/refresh gate" in said
    assert ("the boot root /call (preflight user): NOT SERVED: 2 of 4 stalled, 0 failed fast; for 35.0 s "
            "(Ready+6.0 s to Ready+41.0 s)") in said
    assert "/debug/vars (debug reader, the Go runtime read): NOT SERVED: 1 of 2 stalled" in said
    assert ("goroutine dump (taken when /debug/vars (debug reader, the Go runtime read) was 5.3 s without a byte): "
            "NOT SAVED: it answered 0, TimeoutError after 30000.0 ms (first_byte): the dump stalled too") in said
    assert "GOGC 50, NumGC 7" in said
    rec = json.loads(out.read_text())
    assert rec["routes"]["readyz"]["not_served"] == 0 and rec["dump"]["path"] is None


def test_no_window_in_the_log_says_so(tmp_path):
    (tmp_path / "go-runtime.jsonl").write_text(json.dumps({"kind": "go_runtime", "ok": True}) + "\n")
    rec = bootwindow.summarize(str(tmp_path / "go-runtime.jsonl"), None, str(tmp_path / "d.gz"))
    assert rec["lines"] == ["INFO boot window: NO RECORD (the runtime606 Job's log holds no boot window (no Job, "
                            "or one that predates it))"]


def test_a_dump_missing_a_chunk_is_not_written(tmp_path):
    rows = [{"kind": "boot_dump", "status": 200, "chunks": 2, "bytes": 9, "trigger": {"route": "vars"}},
            {"kind": "boot_dump_chunk", "i": 0, "n": 2, "data": "AAAA"}]
    d = bootwindow.write_dump(rows, str(tmp_path / "d.gz"))
    assert d["path"] is None and d["why"] == "1 of its 2 chunks are in the log" and not (tmp_path / "d.gz").exists()


def test_the_dry_pass_says_the_boot_window(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", SNOWPLOW_ENV="GOGC=50")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    m = log[log.index("=== step measure ==="):]
    assert "its boot window probes /readyz anonymous and the boot root as the preflight user every 5s" in m
    assert "python3 -m bench.bootwindow said" in m and "--boot-window" in m
