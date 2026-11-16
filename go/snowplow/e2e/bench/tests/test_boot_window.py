"""Runs 26j and 27 (2026-11-16): the measure's first snowplow read, /debug/vars as the debug reader (the
runtime606 Job), landed 6-11 s after the rebooted pod's Ready, got no byte for 30 s, and failed the arm
on "/debug/vars answered 0": one attempt, and a record that kept neither the error nor where the time went.

The fix (load606 read_answered, go_runtime_read): the start gate's rule for the same endpoint. A real HTTP
answer that is not 200 ends the read at once; no answer is tried again within the step's EXISTING budget
(the step's deadline, run606 GO_RUNTIME_S), each attempt keeping its 30 s bound; every attempt is recorded
(status, elapsed, connect, first byte, the error's class), and a stall is a finding of its own in the
record, the run log and the report, however the read ended. Then the boot-window probe rehearsal
(BENCH_606_REHEARSAL=bootwindow): the probe (load606 BootProbe) and its report (bench/bootwindow.py).
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


# ─── the real run: the stall said in the run log, and the read's budget is the step's ───

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_the_rehearsal_logs_the_stall_and_passes_once_answered(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_GO_RUNTIME_FAIL="stall")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    assert f"{snowplow_env.READ_FINDING}: STALLED: 1 attempt(s) with no answer" in log
    job = json.load(open(state / "job-runtime606"))
    args = " ".join(job["spec"]["template"]["spec"]["containers"][0]["args"])
    assert "--deadline " in args


def test_no_answer_to_the_end_fails_the_arm_naming_attempts_and_seconds(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_GO_RUNTIME_FAIL="noanswer")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log
    assert "/debug/vars: no answer to 8 attempt(s) over 275.0 s" in log, log[-3000:]
    assert f"{snowplow_env.READ_FINDING}: NO ANSWER: 8 attempt(s)" in log


# ─── the boot-window probe ─────────────────────────────────────────────────

class ProbeClient:
    """Answers per path: `stall` paths take `stall_s` with no byte (status 0) while ticking a real
    sleep (the probe's watch thread sees them in flight); everything else answers at once."""

    def __init__(self, stall_paths=(), stall_s=0.0, timeout=30.0, slow_body_paths=()):
        self.stall_paths, self.stall_s, self.timeout = stall_paths, stall_s, timeout
        self.slow_body_paths = slow_body_paths
        self.calls, self.lock = [], threading.Lock()

    def get(self, path, token, timing=None):
        with self.lock:
            self.calls.append((path, token))
        tm = timing if timing is not None else {}
        tm["connect_ms"] = 0.5
        if any(path.startswith(p) for p in self.stall_paths) and not path.endswith("debug=2"):
            time.sleep(self.stall_s)
            return load606.Response(0, b"", self.stall_s * 1000, "TimeoutError: timed out", connect_ms=0.5,
                                    error_class="TimeoutError")
        tm["first_byte_ms"] = 1.0
        if any(path.startswith(p) for p in self.slow_body_paths):
            time.sleep(self.stall_s)              # answered at once, its body slow: not a stall
        body = (json.dumps(GR).encode() if path == "/debug/vars" else
                b"goroutine 1 [chan receive, 3 minutes]:\nmain.main()\n" if path.endswith("debug=2") else b"{}")
        return load606.Response(200, body, 1.5, connect_ms=0.5, first_byte_ms=1.0)


def _probe(tmp_path, client, *, ready_after=0.0, window_s=0.6, stall_s=0.2, every_s=0.2):
    recs = []
    pr = load606.BootProbe(client, "/call?name=app-shell", {"debug_reader": "DBG", "preflight_user": "PFU"},
                           {}, out_dir=str(tmp_path), sink=recs.append, every_s=every_s, window_s=window_s,
                           stall_s=stall_s, bound_s=30.0, tick_s=0.02)
    (tmp_path / "ready").write_text(str(time.time() + ready_after))
    end = pr.run()
    return recs, end


def boot_probe_identities(tmp_path):
    """The proof tests/test_debug_reader.py names for BootProbe's /debug reads (its guard's table)."""
    c = ProbeClient(stall_paths=("/debug/vars",), stall_s=0.5)
    recs, end = _probe(tmp_path, c)
    who = {}
    for path, tok in c.calls:
        who.setdefault(path, set()).add(tok)
    assert who["/readyz"] == {None}
    assert who["/debug/pprof/goroutine?debug=1"] == {"DBG"} and who["/debug/vars"] == {"DBG"}
    assert who["/debug/pprof/goroutine?debug=2"] == {"DBG"}          # the dump
    assert who["/call?name=app-shell"] == {"PFU"}
    assert end["reason"] == "window"
    # no identity, no token and no body in any record
    text = json.dumps(recs)
    assert "DBG" not in text and "PFU" not in text and "chan receive" not in text


def test_a_probe_past_the_stall_bound_saves_a_dump_that_names_it(tmp_path):
    c = ProbeClient(stall_paths=("/debug/vars",), stall_s=0.5)
    recs, end = _probe(tmp_path, c)
    dumps = [r for r in recs if r["kind"] == "dump"]
    assert dumps and end["dumps"] == len(dumps)
    d = dumps[0]
    assert d["status"] == 200 and d["file"] and {t["route"] for t in d["triggers"]} == {"vars"}
    assert gzip.decompress((tmp_path / d["file"]).read_bytes()).startswith(b"goroutine 1 [chan receive")
    stalled = [r for r in recs if r["kind"] == "probe" and r["route"] == "vars"]
    assert all(r["dump"] for r in stalled) and all(r["stalled_at"] == "first_byte" for r in stalled)
    # the routes that answered never triggered one
    assert all(r["dump"] is None for r in recs if r["kind"] == "probe" and r["route"] != "vars")


def test_a_slow_body_after_its_first_byte_is_no_stall_and_takes_no_dump(tmp_path):
    recs, end = _probe(tmp_path, ProbeClient(slow_body_paths=("/debug/vars",), stall_s=0.5))
    assert end["dumps"] == 0 and not [r for r in recs if r["kind"] == "dump"]


def test_no_stall_no_dump_and_the_gc_fields_are_kept(tmp_path):
    recs, end = _probe(tmp_path, ProbeClient())
    assert end["dumps"] == 0 and not [r for r in recs if r["kind"] == "dump"]
    v = next(r for r in recs if r["kind"] == "probe" and r["route"] == "vars")
    assert v["gc"] == {"num_gc": 7, "pause_total_ns": 3_000_000, "gc_cpu_fraction": 0.01, "heap_alloc": 2 ** 30,
                       "last_gc": None, "gogc": 50, "memory_limit_bytes": 7516192768, "prewarm_complete": None}


def test_the_probe_ends_at_its_bound_without_a_ready_time(tmp_path):
    recs = []
    pr = load606.BootProbe(ProbeClient(), "/call?x", {"debug_reader": "D", "preflight_user": "P"}, {},
                           out_dir=str(tmp_path), sink=recs.append, every_s=0.1, window_s=10, stall_s=1,
                           bound_s=0.3, tick_s=0.02)
    end = pr.run()
    assert end["reason"] == "bound" and end["ready"] is None


# ─── the report's reading key ──────────────────────────────────────────────

def _p(route, t, fb_ms, status=200):
    return {"kind": "probe", "route": route, "t": t, "status": status if fb_ms is not None else 0,
            "elapsed_ms": fb_ms if fb_ms is not None else 30000.0, "first_byte_ms": fb_ms,
            **({} if fb_ms is not None else {"error_class": "TimeoutError", "stalled_at": "first_byte"})}


@pytest.mark.parametrize("stalls,want", [
    (set(), "no stall in the window"),
    ({"readyz", "goroutine", "vars", "call"}, "every route stalls: network or endpoint churn"),
    ({"goroutine", "vars", "call"}, "/readyz fast, the gated routes stall: the auth/refresh gate"),
    ({"vars"}, "only /debug/vars stalls: the expvar render"),
    ({"vars", "call"}, "a pattern the key does not name: stalled /debug/vars (debug reader), the boot root /call "
                       "(preflight user)"),
])
def test_the_reading_key(stalls, want):
    ready = 100.0
    routes = {r: bootwindow.route_summary([_p(r, ready + 6, None if r in stalls else 3.0)], ready, 5.0)
              for r in bootwindow.ROUTES}
    assert bootwindow.reading(routes) == want


def test_a_late_first_byte_is_a_stall_too():
    s = bootwindow.route_summary([_p("vars", 106.0, 12000.0)], 100.0, 5.0)
    assert s["stalled"] == 1 and s["max_no_byte_s"] == 12.0 and s["stall_from_ready_s"] == 6.0


def test_the_report_from_a_run_dir(tmp_path):
    ready = 1794812705.0
    bw = tmp_path / "bootwindow"
    bw.mkdir()
    rows = [{"kind": "boot_probe_start", "t": ready - 200, "every_s": 5, "window_s": 180, "stall_s": 5,
             "timeout_s": 30.0}]
    for k in range(37):
        t = ready + 5 * k
        stall = k in (1, 2)
        for r in bootwindow.ROUTES:
            p = _p(r, t, None if stall and r != "readyz" else 2.0)
            if r == "vars" and not stall:
                p["gc"] = {"num_gc": 10 + k, "pause_total_ns": 1_000_000 * k, "gc_cpu_fraction": 0.02,
                           "heap_alloc": 2 ** 30, "gogc": 50, "prewarm_complete": k > 20}
            rows.append(p)
    rows.append({"kind": "dump", "n": 1, "t": ready + 11, "status": 0, "error_class": "TimeoutError", "file": None,
                 "triggers": [{"route": "vars", "tick": 41, "waited_s": 5.2}]})
    rows.append({"kind": "boot_probe_end", "reason": "window", "ticks": 77, "dumps": 1, "ready": ready})
    (bw / "probes.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    (tmp_path / "pod-boot.jsonl").write_text(json.dumps({"kind": "snowplow_pod", "pod": "snowplow-new",
                                                         "ready_at": ready}) + "\n")
    (tmp_path / "boot.jsonl").write_text(json.dumps({"kind": "boot", "t_restart": ready - 190}) + "\n")
    (tmp_path / "top-boot.jsonl").write_text(json.dumps(
        {"kind": "top_boot", "t": ready + 15, "raw": "snowplow-old 54m 1414Mi snowplow-new 1358m 1331Mi"}) + "\n")
    out = tmp_path / "bootwindow.json"
    p = subprocess.run([sys.executable, "-m", "bench.bootwindow", "report", "--out-dir", str(tmp_path),
                        "--declared", "GOGC=50", "--out", str(out)], cwd=BENCH_DIR, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    said = p.stdout
    assert "READING: /readyz fast, the gated routes stall: the auth/refresh gate" in said
    assert "/debug/vars (debug reader): STALLED: 2 of 37 probes, from Ready+5.0 s to Ready+40.0 s" in said
    assert "effective GOGC [50]" in said and "NumGC +36 (from 10)" in said and "GC pauses +36.0 ms" in said
    assert "max 1358m" in said and "#1 at Ready+11.0 s: NOT saved (0: TimeoutError)" in said
    rec = json.loads(out.read_text())
    assert rec["ok"] and rec["routes"]["readyz"]["stalled"] == 0


def test_a_report_without_the_window_is_incomplete(tmp_path):
    (tmp_path / "bootwindow").mkdir()
    rec, lines = bootwindow.report(str(tmp_path))
    assert not rec["ok"] and lines[1:] == ["snowplow env declared: (none)",
                                           "NO REPORT: the probe left no records (bootwindow/probes.jsonl)",
                                           "=== end of bootwindow report ==="]


def _steps(log):
    return [ln.split("=== step ", 1)[1].split(" ===")[0] for ln in log.splitlines() if "=== step " in ln]


def test_the_bootwindow_dry_pass_says_each_step(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", BENCH_606_REHEARSAL="bootwindow", SNOWPLOW_ENV="GOGC=50")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    steps = _steps(log)
    assert steps[steps.index("preflight"):] == ["preflight", "data", "bootwindow-start", "reboot", "bootwindow",
                                                "digest"]
    assert "load606_job --boot-probe-only" in log and "python3 -m bench.bootwindow report" in log
    assert "rehearsal complete" in log


def test_the_bootwindow_rehearsal_probes_the_reboot_and_reports_it(harness):  # noqa: F811
    env, state, _, tmp = harness
    (state / "vars-stall").write_text("0.8")          # /debug/vars: 0.8 s to its first byte, every read
    p = _run606(env, BENCH_606_REHEARSAL="bootwindow", SNOWPLOW_ENV="GOGC=50", BOOT_WINDOW_S="3",
                BOOT_EVERY_S="0.5", BOOT_STALL_S="0.3")
    out = tmp / "out"
    log = (out / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-4000:]
    steps = _steps(log)
    assert steps[steps.index("preflight"):] == ["preflight", "data", "bootwindow-start", "reboot", "bootwindow",
                                                "digest"]
    # the probe ran before the reboot and through the window, its files came in, its Job is gone
    rows = [json.loads(x) for x in (out / "bootwindow" / "probes.jsonl").read_text().splitlines()]
    boot = json.loads((out / "boot.jsonl").read_text())["t_restart"]
    assert any(r["kind"] == "probe" and r["t"] < boot for r in rows)
    assert next(r for r in rows if r["kind"] == "boot_probe_end")["reason"] == "window"
    rec = json.loads((out / "bootwindow.json").read_text())
    assert rec["ok"] and rec["reading"] == "only /debug/vars stalls: the expvar render"
    assert rec["dumps"] and all(d["file"] for d in rec["dumps"])
    assert all((out / "bootwindow" / d["file"]).exists() for d in rec["dumps"])
    assert "=== bootwindow report ===" in log and "READING: only /debug/vars stalls: the expvar render" in log
    deleted = (state / "deleted.jsonl").read_text()
    assert "job/boot606" in deleted
    # the bench user's password is in no file of the run (the Job's manifest is redacted once applied)
    from bench.load606 import SCALEUSER_PASSWORD
    for f in out.rglob("*"):
        if f.is_file() and not f.name.endswith(".gz"):
            assert SCALEUSER_PASSWORD not in f.read_text(errors="replace"), f
