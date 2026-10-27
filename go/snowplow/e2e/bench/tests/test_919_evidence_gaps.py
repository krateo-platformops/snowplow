"""#919: five evidence gaps run 16's slow-minute investigation (#918) hit.

1. run606's `kubectl top` loop had no request timeout: one hung call ended
   CPU sampling for the last 3.5 h of run B. Each sample is now bounded by its
   period (bench/sampler.py); a failed sample is RECORDED as failed and the
   loop carries on.
2. logscrub hashed whole non-JSON lines: a burst of 21 klog lines was
   unreadable. A klog line keeps severity, klog time, thread and file:line;
   its message is redacted part by part with the JSON path's redaction and
   bench/identity.py (no user, CN or Secret name survives).
3. The browsers' per-request metrics had no /call resource, name or
   namespace: browser_call_ms carries them (k6/pageload.js callOf).
4. Churn write failures had no time or status code: each one is recorded
   (churn_write_failed: t, ms, status, error).
5. The API server's flow-control (APF) metrics were not collected: run606
   scrapes apiserver_flowcontrol_* for the whole run; evaluate reports them
   per phase (bench/apf.py), report only.

Every report is checked through the evaluate CLI (`load606 evaluate`, what
run606 runs), not only through its function.
"""

from __future__ import annotations

import gzip
import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from bench import identity, load606, logscrub
from tests.test_k6_830 import _point, _write_out
from tests.test_review823 import LEAKS
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401  (fixture)

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _mod(name: str):
    """bench.<name>, imported where it is used: before #919 bench.sampler and
    bench.apf do not exist, and each gap's test then fails on its own line,
    not the whole file at collection."""
    import importlib
    return importlib.import_module(f"bench.{name}")


def _evaluate(tmp_path, recs_by_file: dict[str, list[dict]], *extra) -> dict:
    """`load606 evaluate` over files written as run606 collects them, with
    its arguments (apf.jsonl.gz goes in as --apf, as run606 passes it)."""
    paths, opts = [], []
    for name, recs in recs_by_file.items():
        p = tmp_path / name
        if name == "apf.jsonl.gz":
            for r in recs:                                    # one gzip member per record, as bench.sampler
                with gzip.open(p, "ab") as fh:
                    fh.write((json.dumps(r) + "\n").encode())
            opts += ["--apf", str(p)]
        else:
            p.write_text("".join(json.dumps(r) + "\n" for r in recs))
            paths.append(str(p))
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", *paths, *opts, *extra],
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=300)
    assert p.stdout.strip().startswith("{"), p.stderr[-3000:]
    out = json.loads(p.stdout)
    out["_stderr"] = p.stderr
    return out


# ─── 1. kubectl top: each sample bounded, a failed one recorded ─────────────

FAKE_TOP = """#!/usr/bin/env python3
import os, sys, time
state = os.environ["FAKE_STATE"]
n_path = os.path.join(state, "n")
n = int(open(n_path).read()) if os.path.exists(n_path) else 0
open(n_path, "w").write(str(n + 1))
open(os.path.join(state, "argv-%d" % n), "w").write(" ".join(sys.argv[1:]))
if n == 0:                                   # the first call hangs (run 16 B)
    open(os.path.join(state, "hung.pid"), "w").write(str(os.getpid()))
    time.sleep(3600)
print("snowplow-abc 120m 650Mi")
"""


def _all_fns() -> str:
    """Every function run606.sh defines (none of its steps run): whatever
    start_top_loop needs, on this branch or before #919."""
    out, cur = [], None
    for ln in (BENCH_DIR / "run606.sh").read_text().splitlines():
        if cur is None and re.match(r"^[A-Za-z_][A-Za-z0-9_]*\(\) \{", ln):
            if ln.rstrip().endswith("}"):
                out.append(ln)
            else:
                cur = [ln]
        elif cur is not None:
            cur.append(ln)
            if ln == "}":
                out.append("\n".join(cur))
                cur = None
    return "\n".join(out)


def _reap(state: Path) -> bool:
    """Kill the hung fake if it is still there (the loop before #919 never
    did): True when it was left behind."""
    try:
        os.kill(int((state / "hung.pid").read_text()), 9)
        return True
    except (OSError, ValueError):
        return False


def _top_loop(tmp_path, run_s: float, poll_s: str = "1"):
    """run606.sh's own start_top_loop / stop_top_loop, with a kubectl whose
    first call hangs."""
    state = tmp_path / "state"
    state.mkdir()
    out = tmp_path / "out"
    out.mkdir()
    fake = tmp_path / "kubectl"
    fake.write_text(FAKE_TOP)
    fake.chmod(0o755)
    script = _all_fns() + f"\nK=({fake})\nstart_top_loop\nsleep {run_s}\nstop_top_loop\nsleep 1\n"
    env = dict(os.environ, OUT=str(out), DRY="0", TOP_POLL_S=poll_s, PW="pw",
               SNOWPLOW_POD_SELECTOR="app.kubernetes.io/name=snowplow", FAKE_STATE=str(state),
               PYTHONPATH=str(BENCH_DIR))
    try:
        p = subprocess.run(["bash", "-c", script], env=env, cwd=BENCH_DIR, capture_output=True,
                           text=True, timeout=120)
    finally:
        left_behind = _reap(state)
    assert p.returncode == 0, p.stderr
    recs = [json.loads(ln) for ln in (out / "top.jsonl").read_text().splitlines()] \
        if (out / "top.jsonl").exists() else []
    return recs, state, out, left_behind


def test_a_hung_top_call_fails_alone_and_the_samples_after_it_still_arrive(tmp_path):
    recs, state, out, left_behind = _top_loop(tmp_path, run_s=6)
    assert recs, "no sample at all: the hung call stopped the loop (run 16 B)"
    first = recs[0]
    assert first["kind"] == "top" and first["failed"] is True and first["timed_out"] is True
    assert first["bound_s"] == 1 and first["rc"] is None
    assert first["t"] - first["t_start"] < 3           # ended at its bound, not by the hang
    later = [r for r in recs[1:] if not r.get("failed")]
    assert len(later) >= 2, recs
    assert all(r["raw"] == "snowplow-abc 120m 650Mi" for r in later)
    # every call carried kubectl's own request timeout, the same bound
    assert "--request-timeout=1s" in (state / "argv-0").read_text()
    # the hung call's process was killed by the sampler, not left behind
    assert (state / "hung.pid").exists() and not left_behind
    assert not (out / ".sampler-top.pid").exists()      # stopped through its pidfile


def test_a_failed_sample_is_counted_as_failed_through_evaluate(tmp_path):
    tops = [{"kind": "top", "t": 100.0, "raw": "snowplow-a 300m 1Gi"},
            {"kind": "top", "t": 116.0, "t_start": 101.0, "failed": True, "rc": None, "timed_out": True,
             "bound_s": 15, "error": "no answer within the sample's bound (15 s): killed"},
            {"kind": "top", "t": 131.0, "t_start": 116.0, "failed": True, "rc": 1, "timed_out": False,
             "bound_s": 15, "error": "Unable to connect to the server: EOF"},
            {"kind": "top", "t": 146.0, "raw": "snowplow-a 500m 1Gi"}]
    v = _evaluate(tmp_path, {"top.jsonl": tops})
    t = v["report"]["top_samples_report_only"]
    assert (t["answered"], t["failed"]) == (2, 2)
    assert t["longest_unanswered_s"] == 46.0             # last answered (100) to the next one (146)
    assert t["failed_by_error"] == {"no answer within the sample's bound (15 s): killed": 1,
                                    "Unable to connect to the server: EOF": 1}
    assert any("kubectl top: 2 of 4 samples failed" in w for w in v["report"]["report_warnings"])
    # the customer-latency CPU report counts them as failed, not as missing
    from bench import custlat
    c = custlat.cpu_per_phase(tops, [], [0, 200])["by_phase"]["steady"]
    assert c["whole_run"]["samples"] == 2 and c["whole_run"]["failed_samples"] == 2


def test_a_failed_samples_error_line_is_identity_redacted(tmp_path):
    fake = tmp_path / "k"
    fake.write_text("#!/bin/sh\necho 'Error from server (Forbidden): pods is forbidden: User "
                    "\"someone@example.com\" cannot list' >&2\nexit 1\n")
    fake.chmod(0o755)
    rec = _mod("sampler").sample("top", [str(fake)], 5, _mod("sampler").Runner())
    assert rec["failed"] and rec["rc"] == 1 and rec["timed_out"] is False
    assert rec["error"] == 'Error from server (Forbidden): pods is forbidden: User "<redacted>" cannot list'


# ─── 2. logscrub: klog lines readable, identities redacted ──────────────────

PW = "S3cret-Test-Pw"
KTS = "2026-10-27T00:02:54.{:03d}Z "
KLOG_BURST = [
    'I1027 00:02:54.101234       1 request.go:697] Waited for 1.093s due to client-side throttling, not '
    'priority and fairness, request: GET:https://10.0.0.1:443/api/v1/namespaces/krateo-system/secrets/'
    'scaleuser-0042-clientconfig',
    'W1027 00:02:54.102345       1 reflector.go:561] k8s.io/client-go/tools/cache/reflector.go:243: failed '
    'to list *v1.Secret: secrets "alice-clientconfig" is forbidden: User "system:serviceaccount:krateo-system:'
    'snowplow" cannot list resource "secrets" in API group "" in the namespace "krateo-system"',
    'E1027 00:02:54.103456       1 authn.go:88] x509: certificate for CN=alice,O=devs rejected; '
    'authenticated as bob)',
    'I1027 00:02:54.104567       1 request.go:697] Waited for 2.5s due to client-side throttling, not '
    'priority and fairness, request: GET:https://10.0.0.1:443/apis/composition.krateo.io/v1-2-2/namespaces/'
    'bench-ns-01/githubscaffoldingwithcompositionpages?limit=500',
    'W1027 00:02:54.105678       1 warnings.go:70] token for user carol expired; GET /api/v1/namespaces/x/'
    'secrets/dave-kubeconfig (mail erin@example.com, password ' + PW + ')',
]
NEVER = ("scaleuser-0042", "alice", "bob", "carol", "dave", "erin", "example.com",
         "system:serviceaccount", PW, "CN=alice")


def _scrub(lines: list[str]) -> list[dict]:
    p = subprocess.run([sys.executable, "-m", "bench.logscrub", "--kubelet-timestamps",
                        "--deny", PW, "--deny", "scaleuser-"],
                       input="".join(KTS.format(i) + ln + "\n" for i, ln in enumerate(lines)),
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    for x in NEVER:
        assert x not in p.stdout, x
    return [json.loads(ln) for ln in p.stdout.splitlines()]


def test_a_klog_burst_keeps_severity_time_and_file_line_and_no_identity():
    burst = (KLOG_BURST * 5)[:21]                         # run 16 B: 21 lines at 00:02:54Z
    out = _scrub(burst)
    assert len(out) == 21 and all(r.get("format") == "klog" for r in out)
    assert [r["severity"] for r in out[:5]] == ["I", "W", "E", "I", "W"]
    assert [r["source"] for r in out[:5]] == ["request.go:697", "reflector.go:561", "authn.go:88",
                                              "request.go:697", "warnings.go:70"]
    assert out[0]["klog_time"] == "1027 00:02:54.101234" and out[0]["thread"] == 1
    assert out[0]["kts"].startswith("2026-10-27T00:02:54.000")
    # the message is readable where it names no one
    assert out[0]["msg"].startswith("Waited for 1.093s due to client-side throttling, not priority and fairness")
    assert "/apis/composition.krateo.io/v1-2-2/namespaces/bench-ns-01/githubscaffoldingwithcompositionpages" \
        in out[3]["msg"]
    assert "authenticated as <redacted>)" in out[2]["msg"] and '"<redacted>" cannot list' in out[1]["msg"]
    assert "<redacted>" in out[4]["msg"]


# review823's leak list, each as a klog message. Not the one whose name is
# only a bare JSON KEY ({"cyberjoker": 1}): the JSON path hashes it because
# key names are allowlisted, and a bare word in prose names nothing to tell it
# from any other word.
@pytest.mark.parametrize("rec, secret", [(r, s) for r, s in LEAKS
                                         if not (s in next(iter(r)) and not isinstance(r[s], str))])
def test_the_json_paths_leak_cases_never_leak_from_a_klog_message(rec, secret):
    msg = next(k if secret in k else str(v) for k, v in rec.items())
    assert secret in msg
    out = _scrub([f"W1027 00:02:54.000001       7 x.go:1] {msg}"])
    assert out[0]["format"] == "klog" and secret not in json.dumps(out)


def test_one_redaction_behind_the_json_and_klog_paths():
    """The JSON path's kept values pass bench/identity.py too (a msg its DENY
    does not catch), and the klog path redacts with the same DENY and hash."""
    sc = logscrub.Scrubber(deny_literals=("scaleuser-",))
    j = json.loads(sc.line(json.dumps({"msg": "request denied, authenticated as dave"})))
    assert j["msg"] == "request denied, authenticated as <redacted>"
    k = json.loads(sc.line("I1027 00:02:54.000001       1 a.go:1] limit=500 scaleuser-0001 ok"))
    assert k["msg"] == f"{logscrub._h('limit=500')} {logscrub._h('scaleuser-0001')} ok"
    assert k["msg"].split()[0] == json.loads(sc.line(json.dumps({"msg": "limit=500"})))["msg"]


def test_a_non_klog_non_json_line_is_still_hashed_whole():
    out = _scrub(["panic: body={secret} for alice"])
    assert out[0]["unparsed"] is True and "alice" not in json.dumps(out)


@pytest.mark.parametrize("line", [
    "GET /api/v1/namespaces/x/secrets/alice-kubeconfig failed",
    'Error from server (NotFound): secrets "alice-kubeconfig" not found',
    "x509: certificate signed for CN=alice, not CN=bob",
    "token for user carol expired",
])
def test_run606_kerr_line_uses_the_same_redaction(tmp_path, line):
    from tests.test_driver_stuck_api_blip import kerr_line
    got = kerr_line(tmp_path, line)
    assert got == identity.redact_identity(line)
    assert not any(x in got for x in ("alice", "bob", "carol"))


# ─── 3. browser /call requests tagged with resource, name, namespace ────────

_CALLS_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'))
// the observer, in a fake page: Resource Timing entries delivered to it
let deliver = null
globalThis.window = { location: { href: 'http://localhost:8080/dashboard', pathname: '/dashboard' },
                      fetch: () => Promise.resolve({ ok: true }) }
globalThis.history = { pushState () {}, replaceState () {} }
globalThis.document = { addEventListener () {}, querySelectorAll: () => [] }
globalThis.MutationObserver = class { observe () {} }
window.PerformanceObserver = class {
  constructor (cb) { deliver = (es) => cb({ getEntries: () => es }) }
  observe (o) { if (o.type !== 'resource') throw new Error('type') }
}
window.addEventListener = () => {}
;(0, eval)(pl.OBSERVER)
deliver(cases.entries)
process.stdout.write(JSON.stringify({
  calls: window.__benchCalls,
  callOf: cases.urls.map((u) => pl.callOf(u, 'http://localhost:8080/x', 12.5, 200)),
  ops: pl.callOps(window.__benchCalls, { session: 's1', route: '/dashboard', visit: 'warm', during: 'load' }),
  source: pl.callOf.toString(),
}))
"""
EXTRAS = "extras=%7B%22username%22%3A%22alice%22%2C%22groups%22%3A%5B%22devs%22%5D%7D"
CALL_URL = ("http://localhost:8081/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1&resource=tables"
            f"&name=compositions-list&namespace=krateo-system&{EXTRAS}")


def _calls_node(tmp_path, cases):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_CALLS_DRIVER)
    (tmp_path / "cases.json").write_text(json.dumps(cases))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "cases.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


@needs_node
def test_each_call_carries_its_resource_name_and_namespace_never_the_extras(tmp_path):
    out = _calls_node(tmp_path, {
        "entries": [{"name": CALL_URL, "duration": 812.4, "responseStatus": 200},
                    {"name": "http://localhost:8080/static/app.js", "duration": 3, "responseStatus": 200},
                    {"name": "http://localhost:8081/refreshes?x=1", "duration": 9, "responseStatus": 200},
                    {"name": "http://localhost:8081/call?resource=restactions&name=ra&namespace=ns",
                     "duration": 40}],
        "urls": [CALL_URL, "/call?resource=flexes&name=f", "http://h/callx?resource=r", "::bad"]})
    assert out["calls"] == [
        {"resource": "tables", "name": "compositions-list", "namespace": "krateo-system", "ms": 812.4,
         "status": 200},
        {"resource": "restactions", "name": "ra", "namespace": "ns", "ms": 40, "status": 0}]
    assert out["callOf"][1] == {"resource": "flexes", "name": "f", "namespace": "", "ms": 12.5, "status": 200}
    assert out["callOf"][2] is None and out["callOf"][3] is None
    op = out["ops"][0]
    assert op[0] == "browser_call_ms" and op[1] == 812.4
    assert op[2] == {"session": "s1", "route": "/dashboard", "visit": "warm", "during": "load",
                     "call_resource": "tables", "call_name": "compositions-list",
                     "call_namespace": "krateo-system", "call_status": "200"}
    assert "alice" not in json.dumps(out["calls"]) + json.dumps(out["ops"])
    assert not out["source"].lstrip().startswith("export")      # shipped into the page as text


def test_browser_js_drains_the_calls_after_each_load_and_before_each_click():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    loop = src[src.index("while (Date.now() < deadline)"):]
    assert loop.index("during: 'between'") < loop.index("await markStart(page)")
    assert loop.index("const r = await settled(page, route)") < loop.index("during: 'load'")
    assert "new Trend('browser_call_ms', true)" in src and "browser_call_ms|" in src
    # no url / name system tag: the extras (the user) never reach a point
    assert re.search(r"systemTags: \[([^\]]*)\]", src).group(1).count("'url'") == 0


def test_the_calls_report_through_evaluate(tmp_path):
    tags = dict(session="s1", route="/dashboard", visit="warm", during="load", call_resource="tables",
                call_name="compositions-list", call_namespace="krateo-system", call_status="200")
    pts = [_point("browser_page_load_ms", 900.0, session="s1", route="/dashboard", visit="warm",
                  method="click", skeleton="yes", timeout="no"),
           _point("browser_page_loads", 1, session="s1", route="/dashboard", visit="warm", method="click"),
           _point("browser_call_ms", 812.0, **tags), _point("browser_call_ms", 20.0, **tags),
           _point("browser_call_ms", 5000.0, **dict(tags, during="between", call_resource="restactions",
                                                     call_name="ra", call_namespace="ns", call_status="0"))]
    _write_out(tmp_path / "browser", "browser", pts, None, 0)
    v = _evaluate(tmp_path, {"preflight.jsonl": [{"kind": "preflight"}]},
                  "--browser-out", str(tmp_path / "browser"), "--browser-sessions", "1")
    c = v["report"]["browser"]["calls_report_only"]
    assert c["status"] == "present" and c["calls"] == 3
    assert c["by_call"]["tables/krateo-system/compositions-list"]["n"] == 2
    assert c["by_call"]["tables/krateo-system/compositions-list"]["max_ms"] == 812.0
    assert c["by_call"]["restactions/ns/ra"]["max_ms"] == 5000.0
    assert list(c["by_route_call"]) == ["/dashboard/tables/krateo-system/compositions-list"]
    assert c["by_call_status"] == {"restactions/ns/ra 0": 1, "tables/krateo-system/compositions-list 200": 2}


# ─── 4. churn write failures: time and status code ──────────────────────────


class _API(http.server.BaseHTTPRequestHandler):
    codes: list = []

    def _answer(self):
        code = _API.codes.pop(0) if _API.codes else 200
        self.send_response(code)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_PATCH(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self._answer()

    def do_GET(self):
        body = json.dumps({"metadata": {"name": "p", "resourceVersion": "1"}}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self._answer()

    def log_message(self, *a):
        pass


@pytest.fixture
def api(tmp_path, monkeypatch):
    import ssl
    monkeypatch.setattr(ssl, "create_default_context", lambda **k: None)
    (tmp_path / "token").write_text("sa-token")
    (tmp_path / "ca.crt").write_text("x")
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _API)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", str(tmp_path)
    srv.shutdown()


GVR = ("composition.krateo.io", "v1-2-2", "comps")


def test_incluster_writes_return_the_api_servers_status(api):
    url, sa = api
    _API.codes = [429]
    w = load606.incluster_annotate(*GVR, "ns", "n", "v", sa_dir=sa, api=url)
    assert (w.ok, w.status) == (False, 429) and w.error.startswith("429")
    _API.codes = []
    assert load606.incluster_annotate(*GVR, "ns", "n", "v", sa_dir=sa, api=url) == load606.WriteResult(True, 200)
    _API.codes = [409, 409, 409]
    w = load606.incluster_status_condition(*GVR, "ns", "n", "v", sa_dir=sa, api=url)
    assert (w.ok, w.status) == (False, 409) and "on all 3 attempts" in w.error
    w = load606.incluster_annotate(*GVR, "ns", "n", "v", sa_dir=sa, api="http://127.0.0.1:1", timeout=2)
    assert w.ok is False and w.status is None and w.error          # no answer: what failed instead


def test_each_failed_churn_write_is_recorded_with_its_time_and_status():
    """fresh_phases with the REAL ChurnDriver: a failed write reaches the
    sink at once, with the label of its phase."""
    sink, lock = [], threading.Lock()
    n = {"i": 0}

    def mut(ns, name, value):
        with lock:
            n["i"] += 1
            i = n["i"]
        return load606.WriteResult(True, 200) if i % 2 else load606.WriteResult(False, 429, "429 Too Many Requests")

    def probe(client, token, path, mutate, **kw):
        time.sleep(0.4)                                   # the phase lasts while its probe runs
    t0 = time.time()
    load606.fresh_phases(None, "t", lambda r: (lock.acquire(), sink.append(r), lock.release()),
                         modes=["annotation"], rates=["20"], mutators={"annotation": mut},
                         paths={"annotation": "/a"}, targets=[("ns", "x"), ("ns", "y")], probe=("ns", "p"),
                         samples=1, poll_ms=100, gap_s=0, viewer=False, probe_fn=probe, healthy=lambda: True)
    fails = [r for r in sink if r["kind"] == "churn_write_failed"]
    churn = next(r for r in sink if r["kind"] == "churn")
    assert fails and len(fails) == churn["failed"]
    for f in fails:
        assert f["label"] == "list_dep@20" and f["status"] == 429 and f["error"] == "429 Too Many Requests"
        assert t0 <= f["t"] <= time.time() and f["ms"] >= 0


def test_a_failed_probe_write_carries_its_status_too():
    recs = load606.freshness_probe(None, "t", "/p", lambda m: load606.WriteResult(False, 503, "503 x"),
                                   samples=1, gap_s=0, clock=lambda: 50.0, sleep=lambda s: None)
    assert recs[0]["error"] == "mutate failed" and recs[0]["write_status"] == 503 and recs[0]["t"] == 50.0


def test_churn_write_failures_report_through_evaluate(tmp_path):
    fresh = [{"kind": "churn", "label": "list_dep@10", "issued": 100, "failed": 3, "t0": 10, "t1": 20,
              "target_rate": 10, "achieved_rate": 9.7},
             {"kind": "churn_write_failed", "label": "list_dep@10", "t": 12.5, "ms": 30000.0, "status": None,
              "error": "TimeoutError: timed out"},
             {"kind": "churn_write_failed", "label": "list_dep@10", "t": 14.0, "ms": 5.0, "status": 429,
              "error": "429 Too Many Requests"},
             {"kind": "churn_write_failed", "label": "list_dep@10", "t": 15.0, "ms": 6.0, "status": 429,
              "error": "429 Too Many Requests"},
             {"kind": "churn", "label": "list_dep@50", "issued": 50, "failed": 2, "t0": 30, "t1": 40,
              "target_rate": 50, "achieved_rate": 4.8}]
    v = _evaluate(tmp_path, {"fresh-fresh606-0.jsonl": fresh})
    r = v["report"]["churn_write_failures_report_only"]
    assert r["list_dep@10"]["by_status"] == {"429": 2, "no status: TimeoutError: timed out": 1}
    assert (r["list_dep@10"]["first_t"], r["list_dep@10"]["last_t"], r["list_dep@10"]["recorded"]) == (12.5, 15.0, 3)
    assert "before #919" in r["list_dep@50"]["note"]      # counted, never recorded one by one


# ─── 5. the API server's flow control (APF), per phase ──────────────────────

LB = 'flow_schema="snowplow",priority_level="workload-low"'


def _metrics(dispatched, waited, slow, wait_sum, inqueue, rejected=None, start=1.7929e9, extra=""):
    """An API server's /metrics as kube-apiserver writes it (abridged)."""
    lines = ["# HELP process_start_time_seconds Start time", "# TYPE process_start_time_seconds gauge",
             f"process_start_time_seconds {start}",
             f"apiserver_flowcontrol_dispatched_requests_total{{{LB}}} {dispatched}",
             f"apiserver_flowcontrol_current_inqueue_requests{{{LB}}} {inqueue}",
             f"apiserver_flowcontrol_current_executing_requests{{{LB}}} 3",
             f"apiserver_flowcontrol_current_executing_seats{{{LB}}} 4",
             'apiserver_flowcontrol_nominal_limit_seats{priority_level="workload-low"} 245',
             f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{LB},le="0.005"}} '
             f'{waited - slow}',
             f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{LB},le="1"}} '
             f'{waited - slow}',
             f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{LB},le="2"}} {waited}',
             f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{LB},le="+Inf"}} '
             f'{waited}',
             f'apiserver_flowcontrol_request_wait_duration_seconds_sum{{execute="true",{LB}}} {wait_sum}',
             f'apiserver_flowcontrol_request_wait_duration_seconds_count{{execute="true",{LB}}} {waited}',
             'apiserver_request_total{code="200",resource="pods",verb="LIST"} 99']
    if rejected is not None:
        lines.append(f'apiserver_flowcontrol_rejected_requests_total{{{LB},reason="queue-full"}} {rejected}')
    return "\n".join(lines) + "\n" + extra


def _apf(t, text):
    return {"kind": "apf", "t": t, "t_start": t - 1, **_mod("sampler").parse_apf(text)}


def test_parse_keeps_every_flowcontrol_series_and_the_replica():
    rec = _mod("sampler").parse_apf(_metrics(10, 9, 1, 0.5, 2, extra='apiserver_flowcontrol_x{a="b c"} 7 1700000000\n'))
    assert rec["replica"] == 1.7929e9
    assert rec["series"][f"apiserver_flowcontrol_dispatched_requests_total{{{LB}}}"] == 10
    assert rec["series"]['apiserver_flowcontrol_x{a="b c"}'] == 7          # a timestamp after the value
    assert not any(k.startswith("apiserver_request_total") for k in rec["series"])
    assert _mod("apf").parse_key(f'x{{{LB},le="+Inf"}}') == ("x", {"flow_schema": "snowplow",
                                                         "priority_level": "workload-low", "le": "+Inf"})


def test_counters_are_differenced_within_one_replica_never_across():
    """Two replicas answer in turn (a regional control plane): replica A's
    counters are in the thousands, B's in the tens. Differencing across them
    would read thousands of requests that never happened."""
    A, B = 1.0e9, 2.0e9
    recs = [_apf(100, _metrics(5000, 4000, 0, 10.0, 0, start=A)),
            _apf(115, _metrics(10, 10, 0, 0.0, 0, start=B)),
            _apf(130, _metrics(5100, 4090, 10, 25.0, 7, start=A, rejected=4)),
            _apf(145, _metrics(30, 30, 0, 0.0, 1, start=B))]
    r = _mod("apf").report(recs, [], load606._phase_of, None)
    pl = r["by_phase"]["steady"]["priority_levels"]["workload-low"]
    fs = pl["flow_schemas"]["snowplow"]
    assert fs["dispatched"] == 100 + 20                    # A: 5000 -> 5100, B: 10 -> 30
    assert fs["wait"]["executed"]["requests"] == 90 + 20
    assert fs["wait"]["executed"]["mean_s"] == round(15.0 / 110, 6)
    # 10 of A's 90 waited between 1 s and 2 s: p95 interpolated inside that bucket
    assert 1.0 < fs["wait"]["executed"]["p95_s"] <= 2.0 and fs["wait"]["executed"]["p50_s"] <= 0.005
    assert fs["rejected"] == {"queue-full": 4}             # A first exported it at 130: since its start
    assert fs["inqueue_requests"]["max"] == 7 and pl["nominal_limit_seats"] == 245
    assert r["by_phase"]["steady"]["replicas"] == 2 and r["replicas"] == [A, B]
    assert r["by_phase"]["steady"]["same_replica_pairs"] == 2


def test_rejected_absent_is_said_never_read_as_zero_and_a_restart_is_not_differenced():
    recs = [_apf(100, _metrics(500, 400, 0, 1.0, 0)),
            _apf(115, _metrics(10, 5, 0, 0.0, 0)),                  # same start time, counters back: skipped
            _apf(130, _metrics(30, 25, 0, 0.0, 0))]
    r = _mod("apf").report(recs, [], load606._phase_of, None)
    s = r["by_phase"]["steady"]
    assert r["counter_resets_skipped"] == 1 and s["same_replica_pairs"] == 1
    assert s["rejected_series_seen"] is False and "rejected" not in \
        s["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"]
    assert "absent = none rejected" in r["how"]


def test_apf_per_phase_through_evaluate(tmp_path):
    """The evaluate CLI over a gzip apf stream (as bench.sampler writes it),
    with the phases run606's other files give: before the measure (the data
    load), a churn step, after it."""
    recs = [_apf(50, _metrics(100, 100, 0, 0.0, 0)),                       # before the measure
            _apf(90, _metrics(200, 150, 0, 0.0, 0)),
            _apf(110, _metrics(300, 200, 0, 0.0, 1)),                      # churn list_dep@10
            _apf(125, _metrics(700, 550, 50, 80.0, 9, rejected=2)),
            _apf(160, _metrics(800, 600, 50, 81.0, 0, rejected=2)),        # after: steady
            {"kind": "apf", "t": 175, "t_start": 160, "failed": True, "rc": None, "timed_out": True,
             "bound_s": 15, "error": "no answer within the sample's bound (15 s): killed"}]
    v = _evaluate(tmp_path, {
        "apf.jsonl.gz": recs,
        "pod-before.jsonl": [{"kind": "snowplow_pod", "phase": "before", "pod": "s", "restarts": 0, "t": 100}],
        "fresh-fresh606-0.jsonl": [{"kind": "churn", "label": "list_dep@10", "issued": 10, "failed": 0,
                                    "t0": 105, "t1": 140, "target_rate": 10, "achieved_rate": 10}]})
    r = v["report"]["apiserver_flowcontrol_report_only"]
    assert r["status"] == "present" and r["measure_t0"] == 100
    assert list(r["by_phase"]) == ["before_measure", "churn:list_dep@10", "steady", "churn", "all"]
    churn = r["by_phase"]["churn:list_dep@10"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"]
    assert churn["dispatched"] == 100 + 400 and churn["rejected"] == {"queue-full": 2}
    assert churn["wait"]["executed"]["requests"] == 50 + 350 and churn["inqueue_requests"]["max"] == 9
    assert r["by_phase"]["before_measure"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"][
        "dispatched"] == 100
    assert r["by_phase"]["steady"]["failed_scrapes"] == 1 and r["samples"]["failed"] == 1
    assert r["by_phase"]["all"]["priority_levels"]["workload-low"]["dispatched"] == 700


def test_no_apf_sample_is_a_warning_missing_not_zero(tmp_path):
    v = _evaluate(tmp_path, {"top.jsonl": [{"kind": "top", "t": 1.0, "raw": "s 1m 1Mi"}]})
    assert v["report"]["apiserver_flowcontrol_report_only"]["status"] == _mod("apf").REPORT_ABSENT
    assert any("flow-control metrics are MISSING" in w for w in v["report"]["report_warnings"])
    # run606 always passes --apf: a run that never reached env has no file, said so
    v = _evaluate(tmp_path, {"top.jsonl": [{"kind": "top", "t": 1.0, "raw": "s 1m 1Mi"}]},
                  "--apf", str(tmp_path / "apf.jsonl.gz"))
    r = v["report"]["apiserver_flowcontrol_report_only"]
    assert r["status"] == _mod("apf").REPORT_ABSENT and r["note"].endswith("apf.jsonl.gz does not exist")


def test_a_cut_last_gzip_member_keeps_the_samples_before_it(tmp_path):
    p = tmp_path / "apf.jsonl.gz"
    _mod("sampler").write(str(p), _apf(1, _metrics(1, 1, 0, 0, 0)))
    _mod("sampler").write(str(p), _apf(2, _metrics(2, 2, 0, 0, 0)))
    p.write_bytes(p.read_bytes()[:-10])                       # the writer killed mid-member
    recs = load606._read_jsonl(str(p))
    assert [r["t"] for r in recs if r.get("kind") == "apf"] == [1] and recs[-1].get("truncated")


# ─── run606: the samplers in the dry run and in the real sequence ───────────


def test_dry_run_shows_both_bounded_samplers(tmp_path):
    from tests.test_606 import _run606
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", TOP_POLL_S="15")
    assert p.returncode == 0, p.stderr[-2000:]
    sam = [ln for ln in p.stderr.splitlines() if "DRY: python3 -m bench.sampler" in ln]
    assert len(sam) == 2, sam
    assert all("--every-s 15" in ln and "--request-timeout=15s" in ln for ln in sam)
    assert "--kind apf" in sam[0] and "get --raw=/metrics" in sam[0]          # from env: the whole run
    assert "--kind top" in sam[1] and "top pod -l app.kubernetes.io/name=snowplow" in sam[1]
    assert "get --raw=/metrics (APF preflight)" in p.stderr
    ev = next(ln for ln in p.stderr.splitlines() if "DRY: python3 -m bench.load606 evaluate" in ln)
    assert ev.endswith(f"--apf {tmp_path / 'out' / 'apf.jsonl.gz'}")


def test_the_real_sequence_samples_top_and_apf_into_the_verdict(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env.update(TOP_POLL_S="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=600)
    assert p.returncode == 0, p.stderr[-4000:]
    out = tmp / "out"
    assert "APF preflight: the API server's /metrics has apiserver_flowcontrol_* series" in p.stderr
    apfs = [r for r in load606._read_jsonl(str(out / "apf.jsonl.gz")) if r.get("kind") == "apf"]
    assert apfs and all(not r.get("failed") for r in apfs) and apfs[0]["replica"] == 1.7929e9
    tops = [json.loads(ln) for ln in (out / "top.jsonl").read_text().splitlines()]
    assert tops and tops[0]["raw"] == "snowplow-abc 120m 650Mi"
    v = json.loads((out / "verdict.json").read_text())
    r = v["report"]["apiserver_flowcontrol_report_only"]
    assert r["status"] == "present" and r["samples"]["answered"] == len(apfs)
    assert v["report"]["top_samples_report_only"]["answered"] == len(tops)
    # every scrape went to the bench cluster, bounded by kubectl's own timeout too
    scrapes = [c for c in _calls(state) if c["tool"] == "kubectl" and "--raw=/metrics" in c["argv"]]
    # + the preflight; + at most the one in flight when collect stopped the sampler (not recorded)
    assert len(apfs) + 1 <= len(scrapes) <= len(apfs) + 2
    assert not (out / ".sampler-apf.pid").exists() and not (out / ".sampler-top.pid").exists()


def test_a_refused_metrics_endpoint_fails_the_run_before_the_data_load(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env.update(FAKE_METRICS_FORBIDDEN="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=600)
    assert p.returncode != 0
    assert "APF preflight FAILED: the API server refused /metrics" in p.stderr
    assert "someone@example.invalid" not in p.stderr                      # kerr_line redacts
    assert "=== step data ===" not in p.stderr and "deleted (gke-down OK)" in p.stderr
