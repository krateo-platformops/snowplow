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
# client-go's structured throttling line (klog InfoS), and printf-style lines
# that name a user, a Secret, a CN, an e-mail and the test password
THROTTLE = ('I1027 00:02:54.101234       1 request.go:697] "Waited before sending request" delay="1.093s" '
            'reason="client-side throttling, not priority and fairness" verb="GET" '
            'URL="https://10.0.0.1:443/api/v1/namespaces/krateo-system/secrets/scaleuser-0042-clientconfig"')
KLOG_BURST = [
    THROTTLE,
    'W1027 00:02:54.102345       1 reflector.go:561] k8s.io/client-go/tools/cache/reflector.go:243: failed '
    'to list *v1.Secret: secrets "alice-clientconfig" is forbidden: User "system:serviceaccount:krateo-system:'
    'snowplow" cannot list resource "secrets" in API group "" in the namespace "krateo-system"',
    'E1027 00:02:54.103456       1 authn.go:88] x509: certificate for CN=alice,O=devs rejected; '
    'authenticated as bob)',
    'I1027 00:02:54.104567       1 reflector.go:359] "Starting reflector" type="*v1.ConfigMap" resyncPeriod=0 '
    'reflector="k8s.io/client-go/informers/factory.go:160"',
    'W1027 00:02:54.105678       1 warnings.go:70] token for user carol expired; GET /api/v1/namespaces/x/'
    'secrets/dave-kubeconfig (mail erin@example.com, password ' + PW + ')',
    'E1027 00:02:54.106789       1 reflector.go:158] "Unhandled Error" err="secrets \\"alice\\" is forbidden" '
    'owner="carol"',
]
NEVER = ("scaleuser-0042", "alice", "bob", "carol", "dave", "erin", "example.com",
         "system:serviceaccount", PW, "CN=alice")
# #924 review H1: the reviewer's two messages, which the per-part redaction
# let through (a name next to a denied word)
REVIEW_H1 = ["refresh token for carol expired",
             "GET https://10.0.0.1/api/v1/namespaces/team-carol/configmaps?labelSelector=owner%3Dcarol 403"]


def _scrub(lines: list[str]) -> list[dict]:
    p = subprocess.run([sys.executable, "-m", "bench.logscrub", "--kubelet-timestamps",
                        "--deny", PW, "--deny", "scaleuser-"],
                       input="".join(KTS.format(i) + ln + "\n" for i, ln in enumerate(lines)),
                       cwd=BENCH_DIR, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    for x in NEVER:
        assert x not in p.stdout, x
    return [json.loads(ln) for ln in p.stdout.splitlines()]


def _json_msg(text: str, deny=("scaleuser-",)) -> str:
    """What the JSON path makes of `text` as a line's msg."""
    return json.loads(logscrub.Scrubber(deny_literals=deny).line(json.dumps({"msg": text})))["msg"]


def test_a_klog_burst_keeps_its_header_and_judges_each_message_whole():
    burst = (KLOG_BURST * 4)[:21]                         # run 16 B: 21 lines at 00:02:54Z
    out = _scrub(burst)
    assert len(out) == 21 and all(r.get("format") == "klog" for r in out)
    assert [r["severity"] for r in out[:6]] == ["I", "W", "E", "I", "W", "E"]
    assert [r["source"] for r in out[:6]] == ["request.go:697", "reflector.go:561", "authn.go:88",
                                              "reflector.go:359", "warnings.go:70", "reflector.go:158"]
    assert out[0]["klog_time"] == "1027 00:02:54.101234" and out[0]["thread"] == 1
    assert out[0]["kts"].startswith("2026-10-27T00:02:54.000")
    # the throttling line, readable through its structured fields; its URL hashed
    t = out[0]
    assert (t["msg"], t["delay"], t["verb"]) == ("Waited before sending request", "1.093s", "GET")
    assert t["URL"].startswith("h:") and t["reason"].startswith("h:")
    assert out[3]["msg"] == "Starting reflector" and out[3]["k:" + logscrub._h("resyncPeriod")[2:]] == 0
    # a printf-style message that names anyone is hashed WHOLE, as the JSON path hashes a msg
    for i in (1, 2, 4):
        body = burst[i].split("] ", 1)[1]
        assert out[i]["msg"] == logscrub._h(body) == _json_msg(body, deny=(PW, "scaleuser-"))
    # a structured value that names anyone is hashed; an unknown key name too
    assert out[5]["msg"] == "Unhandled Error" and out[5]["err"].startswith("h:")
    assert ("owner" not in out[5]) and any(k.startswith("k:") for k in out[5])


@pytest.mark.parametrize("text", REVIEW_H1)
def test_the_reviewers_messages_are_hashed_whole_as_the_json_path_does(text):
    out = _scrub([f"W1027 00:02:54.000001       7 x.go:1] {text}"])
    assert out[0]["msg"] == _json_msg(text, deny=(PW, "scaleuser-")) == logscrub._h(text)
    assert "carol" not in json.dumps(out)


@pytest.mark.parametrize("rec, secret", LEAKS)
def test_a_klog_message_is_redacted_exactly_as_a_json_msg(rec, secret):
    """review823's leak list, each as a klog message: the klog path's msg is
    the JSON path's msg for the same text — one function. (A name that is a
    bare JSON KEY, {"cyberjoker": 1}, is hashed by the key allowlist; as the
    whole text of a message it is kept by both paths alike.)"""
    msg = next(k if secret in k else str(v) for k, v in rec.items())
    sc = logscrub.Scrubber(deny_literals=("scaleuser-",))
    k = json.loads(sc.line(f"W1027 00:02:54.000001       7 x.go:1] {msg}"))
    assert k["format"] == "klog" and k["msg"] == _json_msg(msg)
    if secret != msg:                                     # the secret sits inside a longer message
        assert secret not in json.dumps(k)


def test_one_function_behind_the_json_and_klog_paths():
    """The JSON path's kept values pass bench/identity.py too (a msg its DENY
    does not catch); a klog body is scrubbed by the same Scrubber.scrub."""
    sc = logscrub.Scrubber(deny_literals=("scaleuser-",))
    j = json.loads(sc.line(json.dumps({"msg": "request denied, authenticated as dave"})))
    assert j["msg"] == "request denied, authenticated as <redacted>"
    k = json.loads(sc.line('I1027 00:02:54.000001       1 a.go:1] "request denied, authenticated as dave" '
                           'verb="LIST" count=3'))
    assert {x: k[x] for x in ("msg", "verb", "count")} == {
        "msg": "request denied, authenticated as <redacted>", "verb": "LIST", "count": 3}
    assert logscrub.klog_record('"m" a="x" b=2') == {"msg": "m", "a": "x", "b": 2}
    assert logscrub.klog_record('"m" a="x" trailing words') == {"msg": '"m" a="x" trailing words'}


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
    loop = src[src.index("while (Date.now() < deadline && !switchDue("):]   # each class's route loop (#935)
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


# ─── 3b (N7). browser requests that failed or never completed ──────────────
# k6's browser module emits nothing for a request that failed at network
# level (run 16 B: 99,504 browser_data_sent vs 94,497 durations).

_LEDGER_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const ev = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const l = pl.requestLedger()
const out = { failed: [], tags: ev.tags.map(([u, t]) => pl.requestTags(u, t)) }
for (const [kind, method, url, t, x, type] of ev.events) {
  if (kind === 'request') l.started(method, url, t, type)
  else if (kind === 'response') l.responded(method, url)
  else if (kind === 'finished') l.finished(method, url)
  else if (kind === 'failed') out.failed.push(l.failed(method, url, t, x, type))
  else if (kind === 'end') { out.open = l.size(); out.unfinished = l.unfinished(t) }
}
process.stdout.write(JSON.stringify(out))
"""
SECRET_CALL = f"http://h:8081/call?apiVersion=v1&resource=tables&name=t1&namespace=ns1&{EXTRAS}"


def _ledger(tmp_path, events, tags=()):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_LEDGER_DRIVER)
    (tmp_path / "ev.json").write_text(json.dumps({"events": events, "tags": list(tags)}))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "ev.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


@needs_node
def test_a_failed_and_a_never_completed_request_each_emit_one_point(tmp_path):
    out = _ledger(tmp_path, [
        ["request", "GET", SECRET_CALL, 1000, None, "Fetch"],
        ["request", "GET", "http://h:8080/static/app.js", 1001, None, "Script"],
        ["request", "POST", "http://h:8081/refreshes", 1002, None, "Fetch"],     # a live stream
        ["response", "POST", "http://h:8081/refreshes"],                         # ... answered, open
        ["request", "GET", SECRET_CALL, 1500, None, "Fetch"],                    # the same URL again
        ["finished", "GET", "http://h:8080/static/app.js"],
        ["failed", "GET", SECRET_CALL, 4000, "net::ERR_CONNECTION_REFUSED", "Fetch"],
        ["failed", "GET", "http://h:8081/call?resource=gone&name=g&namespace=n", 4100, "net::ERR_FAILED", "Fetch"],
        ["end", None, None, 9000],
        ["failed", "POST", "http://h:8081/refreshes", 9001, "net::ERR_ABORTED", "Fetch"],  # page closed
    ])
    f = out["failed"]
    # the oldest of the two identical requests failed: 3,000 ms after it started
    assert f[0] == ["browser_request_incomplete_ms", 3000,
                    {"request": "call", "call_resource": "tables", "call_name": "t1", "call_namespace": "ns1",
                     "outcome": "failed", "error": "net::ERR_CONNECTION_REFUSED", "answered": "no"}]
    # #924 L1: a failure whose start was never seen has no duration: a count, never 0 ms
    assert f[1] == ["browser_request_failed_start_unseen", 1,
                    {"request": "call", "call_resource": "gone", "call_name": "g", "call_namespace": "n",
                     "outcome": "failed", "error": "net::ERR_FAILED", "answered": "no", "start_seen": "false"}]
    assert f[2] is None                         # after the session's end: already reported, not twice
    assert out["open"] == 2
    got = sorted((v, t["request"], t["outcome"], t["answered"], t["call_resource"]) for _, v, t in out["unfinished"])
    assert got == [(7500, "call", "never_completed", "no", "tables"),       # never answered: hung
                   (7998, "fetch", "never_completed", "yes", "")]           # the live stream, answered
    assert "alice" not in json.dumps(out) and "extras" not in json.dumps(out)


@needs_node
def test_request_tags_tell_a_call_from_anything_else_and_carry_no_path(tmp_path):
    out = _ledger(tmp_path, [], tags=[
        [SECRET_CALL, "Fetch"], ["/call?resource=a%20b&name=n", "XHR"],
        ["http://h/users/alice/avatar.png", "Image"], ["http://h/callx?resource=r", "Fetch"], ["", None]])
    assert out["tags"] == [
        {"request": "call", "call_resource": "tables", "call_name": "t1", "call_namespace": "ns1"},
        {"request": "call", "call_resource": "a b", "call_name": "n", "call_namespace": ""},
        {"request": "image", "call_resource": "", "call_name": "", "call_namespace": ""},
        {"request": "fetch", "call_resource": "", "call_name": "", "call_namespace": ""},
        {"request": "other", "call_resource": "", "call_name": "", "call_namespace": ""}]
    assert "alice" not in json.dumps(out)


def test_browser_js_tracks_every_request_from_before_the_first_navigation():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    body = src[src.index("export default async function"):]
    for ev in ("'request'", "'response'", "'requestfinished'", "'requestfailed'"):
        assert body.index(f"page.on({ev}") < body.index("await gotoLogin(page)"), ev
    fin = body[body.index("} finally {"):]
    assert fin.index("ledger.unfinished(Date.now())") < fin.index("await page.close()")
    assert "new Trend('browser_request_incomplete_ms', true)" in src
    assert "new Counter('browser_request_failed_start_unseen')" in src
    assert "browser_request_incomplete_ms|browser_request_failed_start_unseen|" in src


def test_no_trend_carries_a_count_threshold():
    """k6 refuses `count` on a Trend ("unsupported aggregation method count
    on metric of type trend") and the run never starts: caught by the
    bench-tests workflow's Chrome tests on #924's first run."""
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    trends = set(re.findall(r"new Trend\('([a-z_]+)'", src))
    thresholds = src[src.index("thresholds: {"):]
    thresholds = thresholds[:thresholds.index("}")]
    counted = set(re.findall(r"([a-z_]+): \['count", thresholds))
    assert trends >= {"browser_call_ms", "browser_request_incomplete_ms"} and not trends & counted


def _incomplete_points(base):
    return [_point("browser_request_incomplete_ms", 3000.0, **base, request="call", call_resource="tables",
                   call_name="t1", call_namespace="ns1", outcome="failed", error="net::ERR_CONNECTION_REFUSED",
                   answered="no"),
            _point("browser_request_incomplete_ms", 60000.0, **base, request="fetch", call_resource="",
                   call_name="", call_namespace="", outcome="never_completed", error="", answered="yes"),
            _point("browser_request_incomplete_ms", 60000.0, **base, request="eventsource", call_resource="",
                   call_name="", call_namespace="", outcome="never_completed", error="", answered="yes"),
            _point("browser_request_failed_start_unseen", 1, **base, request="call", call_resource="gone",
                   call_name="g", call_namespace="n", outcome="failed", error="net::ERR_FAILED", answered="no",
                   start_seen="false")]


def test_the_incomplete_requests_report_through_evaluate(tmp_path):
    base = dict(session="s1", route="/dashboard")
    pts = [_point("browser_page_load_ms", 900.0, session="s1", route="/dashboard", visit="warm",
                  method="click", skeleton="yes", timeout="no")] + _incomplete_points(base)
    _write_out(tmp_path / "browser", "browser", pts, None, 0)
    v = _evaluate(tmp_path, {"preflight.jsonl": [{"kind": "preflight"}]},
                  "--browser-out", str(tmp_path / "browser"), "--browser-sessions", "1")
    r = v["report"]["browser"]["incomplete_requests_report_only"]
    assert r["status"] == "present" and r["requests"] == 3
    assert r["by_outcome"] == {"failed": 1, "never_completed": 2}
    assert r["by_outcome_request_answered_error"] == {
        "failed call answered=no net::ERR_CONNECTION_REFUSED": 1,
        "never_completed eventsource answered=yes -": 1, "never_completed fetch answered=yes -": 1}
    assert r["calls"]["failed/tables/ns1/t1"]["max_ms"] == 3000.0
    assert r["failed_start_unseen"] == {"call net::ERR_FAILED": 1}


def _timed_point(metric, value, t, **tags):
    pt = _point(metric, value, **tags)
    pt["data"]["time"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + "Z"
    return pt


def test_a_network_failed_request_inside_a_stretch_is_not_served(tmp_path):
    """#917's judge_stretch with the browser's incomplete requests as one
    more source (raw_from_dir, the pair's own read of a run's dir): a request
    that failed at network level inside the stretch, or was never answered
    across it, is not served; an answered live stream is not a witness."""
    from bench import custlat
    T = 1_793_000_000
    base = dict(session="s1", route="/dashboard", call_name="", call_namespace="")
    pts = [  # failed 10 s into [T, T+60): its flight [T+5, T+10]
        _timed_point("browser_request_incomplete_ms", 5000.0, T + 10, **base, request="call",
                     call_resource="tables", outcome="failed", error="net::ERR_CONNECTION_REFUSED", answered="no"),
        # the live stream, answered, open to the session's end: not a witness
        _timed_point("browser_request_incomplete_ms", 600000.0, T + 300, **base, request="fetch",
                     call_resource="", outcome="never_completed", error="", answered="yes"),
        # a failure whose start was never seen, AFTER the stretch: outside it
        _timed_point("browser_request_failed_start_unseen", 1, T + 120, **base, request="call",
                     call_resource="gone", outcome="failed", error="net::ERR_FAILED", answered="no",
                     start_seen="false")]
    _write_out(tmp_path / "browser", "browser", pts, None, 0)
    raw = custlat.raw_from_dir(str(tmp_path))
    assert raw["customers"]["browser_incomplete"] == [
        (T + 5.0, T + 10.0, True, 5000.0, "net::ERR_CONNECTION_REFUSED"),
        (T + 120, T + 120, True, None, "net::ERR_FAILED, start unseen")]
    j = custlat.judge_stretch([(T, T + 60)], raw["customers"], timeout_s=10, bar=0.01)
    assert j["verdict"] == "fail" and j["k6"]["by_source"]["browser_incomplete"] == {
        "requests": 1, "not_served": 1, "not_served_by": {"net::ERR_CONNECTION_REFUSED": 1}}
    # the unseen-start failure counts where it happened
    j = custlat.judge_stretch([(T + 100, T + 130)], raw["customers"], timeout_s=10, bar=0.01)
    assert j["k6"]["by_source"]["browser_incomplete"] == {
        "requests": 1, "not_served": 1, "not_served_by": {"net::ERR_FAILED, start unseen": 1}}


def _incomplete(T, *pts):
    base = dict(session="s1", route="/dashboard", call_name="", call_namespace="", call_resource="")
    return [_timed_point(m, v, t, **base, request="fetch", **tags) for m, v, t, tags in pts]


def test_a_page_cancel_or_a_session_end_cut_is_no_witness_until_it_outlasts_the_probe(tmp_path):
    """#924 delta review R1, the reviewer's case: a 2 s stretch in which B
    served all 150 k6load requests, plus two fetches the PAGE cancelled
    (net::ERR_ABORTED, 120 ms) and one request cut 40 ms before the session
    ended. By the witness's own definition (failed, or no reply within the
    probe's timeout) none of the three is "not served": the stretch is served."""
    from bench import custlat
    T = 1_793_000_000
    _write_out(tmp_path / "browser", "browser", _incomplete(
        T,
        ("browser_request_incomplete_ms", 120.0, T + 101, dict(outcome="failed", error="net::ERR_ABORTED", answered="no")),
        ("browser_request_incomplete_ms", 120.0, T + 101, dict(outcome="failed", error="net::ERR_ABORTED", answered="no")),
        ("browser_request_incomplete_ms", 40.0, T + 101, dict(outcome="never_completed", error="", answered="no")),
        # a page cancel whose start was never seen: its flight unknown, no witness at all
        ("browser_request_failed_start_unseen", 1, T + 101, dict(outcome="failed", error="net::ERR_ABORTED",
                                                                  answered="no", start_seen="false"))),
        None, 0)
    raw = custlat.raw_from_dir(str(tmp_path))
    customers = dict(raw["customers"], k6load=[(T + 100 + i / 100, T + 100.5 + i / 100, False, 50.0) for i in range(150)])
    j = custlat.judge_stretch([(T + 100, T + 102)], customers, timeout_s=10, bar=0.01)
    assert j["verdict"] == "served", j
    assert j["k6"]["by_source"]["browser_incomplete"] == {"requests": 0, "not_served": 0, "no_witness": 3}
    assert j["k6"]["requests"] == 150 and j["k6"]["not_served"] == 0


def test_a_page_cancel_or_a_cut_that_outlasted_the_probe_is_not_served(tmp_path):
    from bench import custlat
    T = 1_793_000_000
    _write_out(tmp_path / "browser", "browser", _incomplete(
        T,
        ("browser_request_incomplete_ms", 15000.0, T + 105, dict(outcome="failed", error="net::ERR_ABORTED", answered="no")),
        ("browser_request_incomplete_ms", 20000.0, T + 110, dict(outcome="never_completed", error="", answered="no")),
        ("browser_request_incomplete_ms", 50.0, T + 101, dict(outcome="failed", error="net::ERR_CONNECTION_RESET",
                                                              answered="no"))),
        None, 0)
    raw = custlat.raw_from_dir(str(tmp_path))
    j = custlat.judge_stretch([(T + 100, T + 102)], raw["customers"], timeout_s=10, bar=0.01)
    # hung 15 s / 20 s through the stretch: past the 10 s probe, not served; a reset: a network failure
    # #924 review LOW: a fail-closed FAIL names its cause, each by Chrome's errorText
    assert j["k6"]["by_source"]["browser_incomplete"] == {"requests": 3, "not_served": 3, "not_served_by": {
        "net::ERR_ABORTED, outlasting the probe's 10 s": 1, "never answered, outlasting the probe's 10 s": 1,
        "net::ERR_CONNECTION_RESET": 1}}
    # ... and the pair's verdict says it, in the stretch's why
    text = custlat.witness_text(j["k6"], 10)
    assert text.endswith("— browser_incomplete: 1 net::ERR_ABORTED, outlasting the probe's 10 s, "
                         "1 net::ERR_CONNECTION_RESET, 1 never answered, outlasting the probe's 10 s"), text
    assert custlat.CLIENT_CANCEL == "net::ERR_ABORTED"


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


@pytest.mark.parametrize("mutate, error", [
    # no HTTP status: the API server did not answer (run 16's case)
    (lambda t, m: load606.WriteResult(False, None, "TimeoutError: timed out"), "TimeoutError: timed out"),
    (lambda t, m: load606.WriteResult(False, None, "ConnectionRefusedError: [Errno 111] Connection refused"),
     "ConnectionRefusedError: [Errno 111] Connection refused"),
    # a mutate that raised is a failed write too, recorded with what it raised
    (lambda t, m: (_ for _ in ()).throw(OSError("network is unreachable")), "OSError: network is unreachable"),
])
def test_a_churn_failure_without_an_http_status_is_recorded(mutate, error):
    """#924 review M5: the failures with no status are exactly an API server
    that does not answer; each is handed on, with its time, never dropped."""
    seen, lock = [], threading.Lock()
    t0 = time.time()
    d = load606.ChurnDriver([("ns", "x")], 50.0, mutate,
                            on_failure=lambda f: (lock.acquire(), seen.append(f), lock.release())).start()
    time.sleep(0.2)
    rec = d.stop()
    t1 = time.time()
    assert rec["failed"] == rec["issued"] == len(seen) >= 1
    # t is rounded to the millisecond (the record's own resolution, up or down): within one of
    # the clock reads before the start and after the stop
    assert all(f["status"] is None and f["error"] == error and t0 - 0.001 <= f["t"] <= t1 + 0.001 for f in seen), (t0, t1, seen)


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


def test_a_pair_that_straddles_a_whole_phase_spans_it():
    """#924 delta review L4, the reviewer's case: burst [200, 230) inside
    churn:a [100, 1000); a replica scraped at 190 and 240 — both scrapes in
    churn:a, the burst between them. Its 30 rejections are spanning
    (churn:a..burst..churn:a), never booked to churn:a."""
    r = _mod("apf").report([_apf(150, _metrics(1, 1, 0, 0.0, 0, rejected=0)),
                            _apf(190, _metrics(2, 2, 0, 0.0, 0, rejected=5)),
                            _apf(240, _metrics(3, 3, 0, 0.0, 0, rejected=35))],
                           [("burst", 200, 230), ("churn:a", 100, 1000)], load606._phase_of, None)
    assert list(r["spanning"]) == ["churn:a..burst..churn:a"]
    sp = r["spanning"]["churn:a..burst..churn:a"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"]
    assert sp["rejected"] == {"queue-full": 30}
    ch = r["by_phase"]["churn:a"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"]
    assert ch["rejected"] == {"queue-full": 5}                       # only 150 -> 190, inside churn:a


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
    # only the pair with both scrapes in the step (110 -> 125); 90 -> 110 and
    # 125 -> 160 cross its edges: spanning, booked to neither side (#924 M1)
    churn = r["by_phase"]["churn:list_dep@10"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"]
    assert churn["dispatched"] == 400 and churn["rejected"] == {"queue-full": 2}
    assert churn["wait"]["executed"]["requests"] == 350 and churn["inqueue_requests"]["max"] == 9
    assert r["by_phase"]["before_measure"]["priority_levels"]["workload-low"]["flow_schemas"]["snowplow"][
        "dispatched"] == 100
    sp = {k: v["priority_levels"]["workload-low"]["dispatched"] for k, v in r["spanning"].items()}
    # 90 -> 110 crosses the measure's start (100) and the churn step's (105): every phase it crossed
    assert sp == {"before_measure..steady..churn:list_dep@10": 100, "churn:list_dep@10..steady": 100}
    assert r["by_phase"]["steady"]["failed_scrapes"] == 1 and r["samples"]["failed"] == 1
    assert r["by_phase"]["steady"]["counters"].startswith("NOT MEASURED")          # its only pair spans
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


# ─── 5b (#922 F3). the acceptance figures, boot as its own phase, B/A ───────

FC_RECORD = {"kind": "flowcontrol_config", "verified": True, "snowplow_sa": "krateo-system/snowplow-sa",
             "identities": {
                 "snowplow_sa": {"original": {"flow_schema": "service-accounts", "priority_level": "workload-low"},
                                 "measured": {"flow_schema": "bench-snowplow-sa", "priority_level": "workload-low"}},
                 "snowplow_users": {"original": {"flow_schema": "global-default", "priority_level": "global-default"},
                                    "measured": {"flow_schema": "bench-snowplow-users",
                                                 "priority_level": "global-default"}},
                 "bench_drivers": {"original": {"flow_schema": "service-accounts", "priority_level": "workload-low"},
                                   "measured": {"flow_schema": "bench-drivers", "priority_level": "workload-low"}}}}


def _apf_text(sa, users, drivers, start=1.7929e9):
    """Each identity's schema: (dispatched, waited, slow, wait_sum, inqueue, executing, rejected)."""
    out = [f"process_start_time_seconds {start}",
           'apiserver_flowcontrol_demand_seats_high_watermark{priority_level="workload-low"} ' + str(sa[4] * 2)]
    for (fs, pl), (disp, waited, slow, wsum, inq, exe, rej) in (
            (("bench-snowplow-sa", "workload-low"), sa), (("bench-snowplow-users", "global-default"), users),
            (("bench-drivers", "workload-low"), drivers)):
        lb = f'flow_schema="{fs}",priority_level="{pl}"'
        out += [f"apiserver_flowcontrol_dispatched_requests_total{{{lb}}} {disp}",
                f"apiserver_flowcontrol_current_inqueue_requests{{{lb}}} {inq}",
                f"apiserver_flowcontrol_current_executing_requests{{{lb}}} {exe}",
                f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{lb},le="0.005"}} '
                f"{waited - slow}",
                f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{lb},le="5"}} {waited}',
                f'apiserver_flowcontrol_request_wait_duration_seconds_bucket{{execute="true",{lb},le="+Inf"}} {waited}',
                f'apiserver_flowcontrol_request_wait_duration_seconds_sum{{execute="true",{lb}}} {wsum}',
                f'apiserver_flowcontrol_request_wait_duration_seconds_count{{execute="true",{lb}}} {waited}']
        if rej:
            out.append(f'apiserver_flowcontrol_rejected_requests_total{{{lb},reason="time-out"}} {rej}')
    return "\n".join(out) + "\n"


Z = (0, 0, 0, 0.0, 0, 0, 0)


def _a(t, sa, users=Z, drivers=Z, start=1.7929e9):
    return {"kind": "apf", "t": t, "t_start": t - 1, **_mod("sampler").parse_apf(_apf_text(sa, users, drivers, start))}


def _run_files(boot_mem_mi: int, boot_rejected: int) -> dict:
    """One run's collected files: restart at 1000 (operator clock); the new
    pod created at 1005, started at 1010, Ready at 1100 (cluster clock); the
    measure from 1200. APF scrapes of one replica at 990, 1050, 1095, 1250:
    one pair inside the boot (1050 -> 1095), two across its edges."""
    return {
        "boot.jsonl": [{"kind": "boot", "t_restart": 1000.0}],
        "pod-before.jsonl": [{"kind": "snowplow_pod", "phase": "before", "pod": "snowplow-new", "restarts": 0,
                              "created_at": 1005.0, "started_at": 1010.0, "ready_at": 1100.0, "t": 1200.0}],
        "top-boot.jsonl": [
            {"kind": "top_boot", "t": 1020.0, "raw": f"snowplow-old 900m 7000Mi snowplow-new 500m {boot_mem_mi // 2}Mi"},
            {"kind": "top_boot", "t": 1060.0, "raw": f"snowplow-new 800m {boot_mem_mi}Mi"},
            {"kind": "top_boot", "t": 1080.0, "t_start": 1065.0, "failed": True, "rc": None, "timed_out": True,
             "bound_s": 15, "error": "no answer within the sample's bound (15 s): killed"},
            {"kind": "top_boot", "t": 1150.0, "raw": "snowplow-new 100m 9999Mi"}],      # after Ready: not boot
        "flowcontrol-config.jsonl": [FC_RECORD],
        "apf.jsonl.gz": [_a(990, (100, 100, 0, 0.0, 0, 1, 0), (10, 10, 0, 0.0, 0, 0, 0), (5, 5, 0, 0.0, 0, 0, 0)),
                         _a(1050, (500, 400, 50, 120.0, 40, 60, 0), (20, 20, 0, 0.0, 0, 1, 0), (9, 9, 0, 0.0, 0, 0, 0)),
                         _a(1095, (900, 700, 100, 300.0, 5, 20, boot_rejected), (30, 30, 0, 0.0, 0, 1, 0),
                            (20, 20, 0, 0.0, 0, 0, 3)),
                         _a(1250, (950, 750, 100, 301.0, 0, 2, boot_rejected), (40, 40, 0, 0.0, 0, 0, 0),
                            (30, 30, 0, 0.0, 0, 0, 3))]}


def test_the_acceptance_figures_through_evaluate(tmp_path):
    v = _evaluate(tmp_path, _run_files(boot_mem_mi=2600, boot_rejected=7))
    f = v["report"]["acceptance_figures_report_only"]
    b = f["boot"]
    # #924 L2: the boot's durations from cluster timestamps alone; the restart's gap apart, two clocks
    assert (b["created_to_ready_s"], b["started_to_ready_s"]) == (95.0, 90.0)
    assert b["restart_to_created_s"] == 5.0 and "operator" in b["restart_to_created_clocks"]
    # #924 L3: the max of kubectl top's samples of the NEW pod in the window, named as such
    assert b["sampled_max_memory_bytes"] == 2600 * 1024 ** 2 and b["pod"] == "snowplow-new"
    assert (b["memory_samples"], b["memory_failed_samples"]) == (2, 1)
    boot = f["by_phase"]["boot"]
    # only the pair inside the boot: 1050 -> 1095
    assert boot["snowplow_sa"]["rejected"] == 7 and boot["snowplow_sa"]["wait_requests"] == 300
    assert boot["snowplow_sa"]["sampled_max_inflight_requests"] == 100
    assert boot["snowplow_sa"]["level_demand_seats_high_watermark"] == 80
    assert boot["snowplow_sa"]["wait_p99_s"] > 0.005                 # 50 of 300 waited up to 5 s
    assert boot["snowplow_users"]["wait_requests"] == 10
    # #924 M3: the bench's own churn in its own schema, never in snowplow's
    assert boot["bench_drivers"]["rejected"] == 3 and boot["bench_drivers"]["wait_requests"] == 11
    # #924 M1: the differences across the boot's edges are spanning, booked to neither side
    # 1095 -> 1250 crosses Ready (1100, before_measure) and the measure's start (1200)
    assert set(f["spanning"]) == {"before_measure..boot", "boot..before_measure..steady"}
    assert f["spanning"]["before_measure..boot"]["snowplow_sa"]["wait_requests"] == 300
    assert f["by_phase"]["before_measure"]["snowplow_sa"]["wait_requests"] is None   # not measured, never 0
    assert f["by_phase"]["steady"]["snowplow_sa"]["counters"].startswith("NOT MEASURED")
    assert f["steady_refreshes_per_core"]["value"] is None and "no steady phase" in \
        f["steady_refreshes_per_core"]["note"]                       # no k6 in this run


def test_a_phase_without_a_pair_is_not_measured_and_a_crossing_pair_spans(tmp_path):
    """#924 review M1, the reviewer's case: a boot of [100, 130), replica 1
    scraped at 101 and 200, replica 2 at 116; replica 1's rejections 5 -> 50.
    The boot has no pair: its counters are NOT MEASURED, never 0. The 45
    rejections are spanning (boot..before_measure..steady), never booked to steady."""
    r1, r2 = 1.0e9, 2.0e9
    files = {"boot.jsonl": [{"kind": "boot", "t_restart": 100.0}],
             "pod-before.jsonl": [{"kind": "snowplow_pod", "phase": "before", "pod": "s", "restarts": 0,
                                   "created_at": 101.0, "started_at": 102.0, "ready_at": 130.0, "t": 150.0}],
             "flowcontrol-config.jsonl": [FC_RECORD],
             "apf.jsonl.gz": [_a(101, (10, 10, 0, 0.0, 1, 1, 5), start=r1), _a(116, (3, 3, 0, 0.0, 2, 2, 0), start=r2),
                              _a(200, (90, 90, 0, 0.0, 0, 1, 50), start=r1)]}
    v = _evaluate(tmp_path, files)
    f = v["report"]["acceptance_figures_report_only"]
    boot = f["by_phase"]["boot"]["snowplow_sa"]
    assert boot["rejected"] is None and boot["wait_requests"] is None
    assert boot["counters"].startswith("NOT MEASURED")
    assert boot["sampled_max_inflight_requests"] == 4               # a gauge: both scrapes in the boot
    assert f["by_phase"]["steady"]["snowplow_sa"]["rejected"] is None
    assert f["spanning"]["boot..before_measure..steady"]["snowplow_sa"]["rejected"] == 45
    ap = v["report"]["apiserver_flowcontrol_report_only"]
    assert ap["by_phase"]["boot"]["counters"].startswith("NOT MEASURED")
    assert ap["by_phase"]["all"]["same_replica_pairs"] == 1           # the whole run keeps it


def test_steady_refreshes_per_core():
    from bench import figures
    cl = {"refresher_completions": {"by_phase": {"steady": {"with_customers": {"per_s": 30.0}}}},
          "snowplow_cpu": {"by_phase": {"steady": {"with_customers": {"median_m": 1500}}}}}
    assert figures.steady_refreshes_per_core(cl) == {"value": 20.0, "refreshes_per_s": 30.0, "cpu_median_m": 1500}


def test_the_pair_reports_each_figure_as_b_over_a_through_the_cli(tmp_path):
    for x, mem, rej in (("a", 2000, 4), ("b", 3000, 10)):
        d = tmp_path / x
        d.mkdir()
        v = _evaluate(d, _run_files(boot_mem_mi=mem, boot_rejected=rej))
        v.pop("_stderr")
        (d / "verdict.json").write_text(json.dumps(v))
    p = subprocess.run([sys.executable, "-m", "bench.custlat", "pair", "--a", str(tmp_path / "a" / "verdict.json"),
                        "--b", str(tmp_path / "b" / "verdict.json")], cwd=BENCH_DIR, capture_output=True,
                       text=True, timeout=300)
    out = json.loads(p.stdout)
    r = out["acceptance_ratios_report_only"]
    assert r["ratios"]["boot/sampled_max_memory_bytes"]["b_over_a"] == 1.5
    assert r["ratios"]["by_phase/boot/snowplow_sa/rejected"] == {"a": 4, "b": 10, "b_over_a": 2.5}
    assert r["ratios"]["boot/created_to_ready_s"]["b_over_a"] == 1.0
    assert not any(k.endswith(("t_restart", "ready_at", "started_at", "created_at")) for k in r["ratios"])
    # a figure not measured (None) is no ratio
    assert "by_phase/steady/snowplow_sa/rejected" not in r["ratios"]
    assert r["notes"] == []


# ─── 5d (#933 review). natural relists: count, k6 around them, heap / CPU ───

def _mem(t, established, errors, heap):
    return {"kind": "mem", "t": t, "heap_inuse_mb": heap, "vars_status": 200,
            "extra": {"snowplow_reflector_path": {"reflector_watchlist_established_total": established,
                                                  "reflector_list_cache_eligible_total": 99},
                      "snowplow_informer_watch": {"watch_errors_total": errors}}}


def _relist_run(tmp_path, counts):
    """Ready at 1000; mem606 every 15 s; k6 calls at 1005 (5 s), 1010, 1020, 1040."""
    calls = [(1005, 5000.0), (1010, 100.0), (1020, 50.0), (1040, 60.0)]
    _write_out(tmp_path / "k6", "k6", [_timed_point("http_req_duration", ms, t, level="0") for t, ms in calls]
               + [_timed_point("http_req_failed", 0, t, level="0") for t, _ in calls], None, 0)
    return _evaluate(tmp_path, {
        "boot.jsonl": [{"kind": "boot", "t_restart": 900.0}],
        "pod-before.jsonl": [{"kind": "snowplow_pod", "phase": "before", "pod": "s", "restarts": 0,
                              "created_at": 905.0, "started_at": 906.0, "ready_at": 1000.0, "t": 1000.0}],
        "mem-mem606-0.jsonl": [{"kind": "mem_start", "t": 980.0}] + [
            _mem(t, est, err, heap) for t, est, err, heap in counts],
        "top.jsonl": [{"kind": "top", "t": 1010.0, "raw": "s 900m 1Gi"}, {"kind": "top", "t": 1025.0, "raw": "s 300m 1Gi"}]},
        "--k6-out", str(tmp_path / "k6"))


def test_a_reflector_establishment_its_k6_windows_and_its_heap_and_cpu_through_evaluate(tmp_path):
    v = _relist_run(tmp_path, [(990, 0, 0, 100.0),            # before Ready: the boot's establishments
                               (1000, 280, 0, 500.0), (1015, 281, 1, 700.0),   # one relist in (1000, 1015]
                               (1030, 281, 1, 650.0), (1045, 281, 1, 640.0)])
    r = v["report"]["acceptance_figures_report_only"]["reflector_establishments_after_ready"]
    assert r["status"] == "present" and r["none_occurred"] is False and r["measured_s"] == 45.0
    assert r["totals"] == {"watchlist_established": 1, "watch_errors": 1}
    assert r["per_hour"]["watchlist_established"] == 80.0
    e = r["events"]
    assert len(e) == 1 and (e[0]["t0"], e[0]["t1"]) == (1000, 1015)
    assert (e[0]["heap_inuse_mb_before"], e[0]["heap_inuse_mb_after"], e[0]["cpu_m_samples"]) == (500.0, 700.0, [900])
    assert r["k6_calls_in_establishment_windows"] == {"windows": 1, "calls": 2, "p95_ms": 4755.0, "max_ms": 5000.0}
    assert r["k6_calls_in_quiet_windows"] == {"windows": 2, "calls": 2, "p95_ms": 59.5, "max_ms": 60.0}
    assert "not exposed" in r["per_gvr"] and "not derivable" in r["composition_gvr_comparison"]


def test_no_relist_is_said_as_none_and_no_counter_as_missing(tmp_path):
    v = _relist_run(tmp_path, [(1000, 280, 0, 500.0), (1015, 280, 0, 500.0), (1030, 280, 0, 500.0)])
    r = v["report"]["acceptance_figures_report_only"]["reflector_establishments_after_ready"]
    assert r["none_occurred"] is True and r["totals"] == {"watchlist_established": 0, "watch_errors": 0}
    assert r["per_hour"]["watchlist_established"] == 0.0 and r["events"] == []
    d = tmp_path / "absent"
    d.mkdir()
    v = _evaluate(d, {"mem-mem606-0.jsonl": [{"kind": "mem", "t": 1.0, "extra": {}}, {"kind": "mem", "t": 2.0}]})
    r = v["report"]["acceptance_figures_report_only"]["reflector_establishments_after_ready"]
    assert r["status"] == "absent" and "MISSING" in r["note"]


def test_mem606_captures_the_relist_counters_by_default():
    assert {"snowplow_reflector_path", "snowplow_informer_watch"} <= set(load606.RUN8_VARS_PREFIXES)


# ─── 5c (#924 review M2/M3). the API server says who lands where ────────────

V8_OLD = """I1027 10:00:00.000000  1 round_trippers.go:553] GET https://x/api/v1/namespaces/krateo-system/configmaps?limit=1 403 Forbidden in 4 milliseconds
I1027 10:00:00.000000  1 round_trippers.go:570] Response Status: 403 Forbidden in 4 milliseconds
I1027 10:00:00.000000  1 round_trippers.go:573] Response Headers:
I1027 10:00:00.000000  1 round_trippers.go:576]     X-Kubernetes-Pf-Flowschema-Uid: 0b0e5f6a-1c2d-4e5f-8a9b-0c1d2e3f4a5b
I1027 10:00:00.000000  1 round_trippers.go:576]     X-Kubernetes-Pf-Prioritylevel-Uid: 9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d
"""
V8_STRUCTURED = """I1027 10:00:00.000000  1 round_trippers.go:632] "Response" verb="GET" url="https://x/api/v1/namespaces/krateo-system/configmaps?limit=1" status="403 Forbidden" headers=<
	Audit-Id: 1
	X-Kubernetes-Pf-Flowschema-Uid: 0b0e5f6a-1c2d-4e5f-8a9b-0c1d2e3f4a5b
	X-Kubernetes-Pf-Prioritylevel-Uid: 9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d
 > milliseconds=4
"""


@pytest.mark.parametrize("text", [V8_OLD, V8_STRUCTURED])
def test_the_apf_headers_are_read_from_kubectls_own_trace(text):
    from bench import flowcontrol
    assert flowcontrol.parse_pf(text) == {"fs_uid": "0b0e5f6a-1c2d-4e5f-8a9b-0c1d2e3f4a5b",
                                          "pl_uid": "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d", "status": 403}
    assert flowcontrol.parse_pf("Unable to connect to the server: EOF")["fs_uid"] is None


def test_each_identity_gets_its_own_schema_at_the_level_it_landed_in():
    from bench import flowcontrol, rbac_model
    import yaml
    orig = {"snowplow_sa": {"flow_schema": "service-accounts", "precedence": 9000, "priority_level": "workload-low"},
            "snowplow_users": {"flow_schema": "global-default", "precedence": 9900, "priority_level": "global-default"},
            "bench_drivers": {"flow_schema": "service-accounts", "precedence": 9000, "priority_level": "workload-low"}}
    docs = {d["metadata"]["name"]: d for d in yaml.safe_load_all(flowcontrol.render(orig, "snowplow-sa"))}
    assert set(docs) == {"bench-snowplow-sa", "bench-snowplow-users", "bench-drivers"}
    sa, users, drv = docs["bench-snowplow-sa"]["spec"], docs["bench-snowplow-users"]["spec"], docs["bench-drivers"]["spec"]
    assert (sa["matchingPrecedence"], sa["priorityLevelConfiguration"]["name"]) == (8999, "workload-low")
    assert (users["matchingPrecedence"], users["priorityLevelConfiguration"]["name"]) == (9899, "global-default")
    assert sa["rules"][0]["subjects"] == [{"kind": "ServiceAccount",
                                           "serviceAccount": {"namespace": "krateo-system", "name": "snowplow-sa"}}]
    assert users["rules"][0]["subjects"] == [{"kind": "Group", "group": {"name": rbac_model.BASE_GROUP}}]
    assert drv["rules"][0]["subjects"] == [{"kind": "Group", "group": {"name": "system:serviceaccounts:bench-loadgen"}}]
    assert all(d["spec"]["distinguisherMethod"] == {"type": "ByUser"} for d in docs.values())
    with pytest.raises(ValueError):
        flowcontrol.render(dict(orig, snowplow_sa={"error": "no APF headers in the response"}), "snowplow-sa")


# ─── run606: the samplers in the dry run and in the real sequence ───────────


def test_dry_run_shows_both_bounded_samplers(tmp_path):
    from tests.test_606 import _run606
    p = _run606(tmp_path, BENCH_606_DRY_RUN="1", TOP_POLL_S="15")
    assert p.returncode == 0, p.stderr[-2000:]
    sam = [ln for ln in p.stderr.splitlines() if "DRY: python3 -m bench.sampler" in ln]
    assert len(sam) == 3, sam
    assert all("--every-s 15" in ln and "--request-timeout=15s" in ln for ln in sam)
    assert "--kind apf" in sam[0] and "get --raw=/metrics" in sam[0]          # from env: the whole run
    assert "--kind top_boot" in sam[1] and "top-boot.jsonl" in sam[1]         # #922 F3: from the restart
    assert "--kind top " in sam[2] and "top pod -l app.kubernetes.io/name=snowplow" in sam[2]
    assert "bench.flowcontrol" in p.stderr and "probe -> bench-flowschemas.yaml (kapply) -> verify" in p.stderr
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
    # #922 F3: the boot's facts, its own top samples, snowplow's flow schema
    f = v["report"]["acceptance_figures_report_only"]
    assert f["boot"]["t_restart"] and f["boot"]["pod"] == "snowplow-abc" and f["boot"]["ready_at"]
    assert (out / "top-boot.jsonl").exists() and not (out / ".sampler-top_boot.pid").exists()
    # #924 M2/M3: the API server's own classification, then a schema of each identity's own
    fc = f["flow"]
    assert fc["verified"] is True and fc["snowplow_sa"] == "krateo-system/snowplow"
    assert {i: x["measured"]["flow_schema"] for i, x in fc["identities"].items()} == {
        "snowplow_sa": "bench-snowplow-sa", "snowplow_users": "bench-snowplow-users", "bench_drivers": "bench-drivers"}
    assert fc["identities"]["snowplow_users"]["original"] == {"flow_schema": "global-default",
                                                              "priority_level": "global-default"}
    assert f["by_phase"] is not None
    applied = (state / "bench_flowschemas").read_text()                   # through kapply, server-side
    assert "bench-snowplow-sa" in applied and "8999" in applied
    probes = [c["argv"] for c in _calls(state) if c["tool"] == "kubectl"
              and any(x.startswith("--as=") for x in c["argv"])]
    assert probes and all("-v=8" in a and any(x.startswith("--request-timeout=") for x in a) for a in probes)
    assert "scaleuser-0001" not in (out / "flowcontrol-config.jsonl").read_text()   # names only
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


@pytest.mark.parametrize("ignored, missed", [
    ({"FAKE_BENCH_FS_IGNORED": "1"}, {"snowplow_sa", "snowplow_users", "bench_drivers"}),
    # #924 review R3: EVERY identity is checked, not snowplow's ServiceAccount alone
    ({"FAKE_BENCH_FS_IGNORED_FOR": "snowplow_users"}, {"snowplow_users"}),
    ({"FAKE_BENCH_FS_IGNORED_FOR": "bench_drivers"}, {"bench_drivers"}),
])
def test_an_identity_that_does_not_land_in_its_own_schema_fails_env(harness, ignored, missed):  # noqa: F811
    """#924 M3: the figures are only each identity's own when its bench
    schema took. A cluster where any one does not (FAKE_BENCH_FS_IGNORED*)
    fails env, before the data load is paid for."""
    env, state, ambient, tmp = harness
    env.update(ignored, COLLECT_TIMEOUT_S="2")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=600)
    assert p.returncode != 0
    assert "flow control FAILED: an identity does not land in its bench FlowSchema" in p.stderr
    rec = json.loads((tmp / "out" / "flowcontrol-config.jsonl").read_text())
    assert rec["verified"] is False and "PROBE identities" in rec["probes"]
    from bench import flowcontrol
    landed = {i for i, x in rec["identities"].items() if x["measured"]["flow_schema"] == flowcontrol.BENCH_SCHEMA[i]}
    assert set(flowcontrol.IDENTITIES) - landed == missed
    assert "scaleuser-0001" not in p.stderr
    assert "=== step data ===" not in p.stderr and "deleted (gke-down OK)" in p.stderr
