"""Run 9: status-condition churn next to annotation churn (coordinator,
2026-10-19; mirrors architect-623's #765 in-process spec,
issuecomment-6266045747).

- fresh606 runs churn MODES as separate labelled phases at every rate:
  annotation ("list_dep@<rate>", run 8's) then status
  ("list_dep_status@<rate>"): a condition in .status.conditions written
  through the /status subresource, polled through a sibling RESTAction
  that projects that condition.
- The annotation behaviour is byte-identical to 71f25c6f (goldens captured
  from that SHA's own code).
- evaluate gates the 10/s point in EVERY mode present.
- run606 derives the measure wait from the probe's worst case (run 8's fixed
  5400 s expired while fresh606 was still at list_dep@50 30/60).
"""

from __future__ import annotations

import http.server
import json
import re
import subprocess
import threading
from pathlib import Path

import pytest

from bench import load606, load606_job
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401

FIX = Path(__file__).parent / "fixtures"
GVR = ("composition.krateo.io", "v1-2-2", "githubscaffoldingwithcompositionpages")
RENDER_KW = dict(snowplow_url="http://snowplow.krateo-system.svc:8081",
                 authn_url="http://authn.krateo-system.svc:8082", user="scaleuser-0001",
                 password="PW", rates=["1", "10", "50"], samples=60, gap_s=5.0,
                 probe_name="bench-app-01-01", namespaces=50, comp_group=GVR[0],
                 comp_version=GVR[1], comp_plural=GVR[2], delay_s=60)


# ─── annotation: byte-identical to 71f25c6f ────────────────────────────────


def test_annotation_only_fresh606_manifest_is_byte_identical_to_71f25c6f():
    golden = (FIX / "fresh606_annotation_71f25c6f.yaml").read_text()
    assert load606_job.render_fresh(**RENDER_KW) == golden
    assert load606_job.render_fresh(modes=("annotation",), **RENDER_KW) == golden


def test_annotation_probe_restaction_is_byte_identical_to_71f25c6f():
    golden = (FIX / "fresh_ra_annotation_71f25c6f.yaml").read_text()
    assert load606.list_dep_fixture_yaml("bench-ns-01", *GVR) == golden


class _Rec:
    def __init__(self):
        self.calls = []

    def probe(self, client, token, path, mutate, **kw):
        mutate("M")
        self.calls.append(("probe", kw["label"], path, kw["samples"], kw["poll_ms"],
                           kw["gap_s"], kw["push_since"]))

    def churn(self, targets, rate, mutate):
        rec = self

        class C:
            def start(self):
                mutate(targets[0], "c")
                rec.calls.append(("churn", rate, len(targets)))
                return self

            def stop(self):
                return {"issued": 1, "failed": 0, "target_rate": rate, "achieved_rate": rate}
        return C()


def _old_annotation_loop(rec, sink, path, targets, probe, patch, push_since):
    """71f25c6f's _cmd_fresh loop, verbatim but for the injected fakes."""
    for rate in ["1", "10", "50"]:
        rate = float(rate)
        label = f"list_dep@{rate:g}"
        churn = None
        if rate > 0:
            churn = rec.churn(targets, rate, lambda t, m: patch(t[0], t[1], m)).start()
        rec.probe(None, "t", path, lambda m: patch(probe[0], probe[1], m),
                  samples=60, poll_ms=100, gap_s=5.0, label=label, sink=sink,
                  healthy=None, push_since=push_since)
        if churn:
            sink({"kind": "churn", "label": label, **churn.stop()})


def test_annotation_only_phases_match_the_71f25c6f_loop():
    targets = [("bench-ns-01", "bench-app-01-02"), ("bench-ns-02", "bench-app-02-01")]
    probe = ("bench-ns-01", "bench-app-01-01")
    path = load606.call_path("templates.krateo.io", "v1", "restactions",
                             load606.FRESH_RA_NAME, "bench-ns-01")
    ps = object()

    old, old_sink, old_mut = _Rec(), [], []
    _old_annotation_loop(old, old_sink.append, path, targets, probe,
                         lambda ns, n, v: old_mut.append((ns, n, v)) or True, ps)
    new, new_sink, new_mut = _Rec(), [], []
    load606.fresh_phases(
        None, "t", new_sink.append, modes=["annotation"], rates=["1", "10", "50"],
        mutators={"annotation": lambda ns, n, v: new_mut.append((ns, n, v)) or True,
                  "status": lambda *x: pytest.fail("status mutator used in annotation mode")},
        paths={"annotation": path, "status": "/never"}, targets=targets, probe=probe,
        samples=60, poll_ms=100, gap_s=5.0, viewer=True, push_since=ps,
        probe_fn=new.probe, churn_factory=new.churn,
        arm=lambda *a: pytest.fail("no re-arm with a single mode"), healthy=lambda: True)
    assert new.calls == old.calls
    assert new_sink == old_sink and new_mut == old_mut


# ─── status mode ────────────────────────────────────────────────────────────


def test_status_phases_follow_annotation_with_their_own_labels_path_and_mutator():
    rec, sink, used, armed = _Rec(), [], [], []
    paths = {"annotation": "/ann", "status": "/st"}
    load606.fresh_phases(
        None, "t", sink.append, modes=["annotation", "status"], rates=["1", "10", "50"],
        mutators={"annotation": lambda ns, n, v: used.append("annotation") or True,
                  "status": lambda ns, n, v: used.append("status") or True},
        paths=paths, targets=[("ns", "x")], probe=("ns", "p"), samples=2, poll_ms=100,
        gap_s=0, viewer=True, push_since=None,
        probe_fn=rec.probe, churn_factory=rec.churn,
        arm=lambda c, t, path, s: armed.append(path) or ("PS2", None), healthy=lambda: True)
    labels = [c[1] for c in rec.calls if c[0] == "probe"]
    assert labels == ["list_dep@1", "list_dep@10", "list_dep@50",
                      "list_dep_status@1", "list_dep_status@10", "list_dep_status@50"]
    assert [c[2] for c in rec.calls if c[0] == "probe"] == ["/ann"] * 3 + ["/st"] * 3
    assert armed == ["/st"]                                  # re-armed on the status cell
    assert [c[6] for c in rec.calls if c[0] == "probe"][3:] == ["PS2"] * 3
    assert used == ["annotation", "annotation"] * 3 + ["status", "status"] * 3
    assert [s["label"] for s in sink if s["kind"] == "churn"][3:] == [
        "list_dep_status@1", "list_dep_status@10", "list_dep_status@50"]


def test_with_fresh_condition_flips_and_keeps_other_conditions():
    obj = {"metadata": {"name": "x", "resourceVersion": "7"},
           "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
    a = load606.with_fresh_condition(obj, "fresh-1", "T1")
    c = {x["type"]: x for x in a["status"]["conditions"]}
    assert c["Ready"] == {"type": "Ready", "status": "True"}
    assert c["SnowplowBenchFresh"]["status"] == "True" and c["SnowplowBenchFresh"]["message"] == "fresh-1"
    b = load606.with_fresh_condition(a, "fresh-2", "T2")
    fresh = [x for x in b["status"]["conditions"] if x["type"] == "SnowplowBenchFresh"]
    assert len(fresh) == 1 and fresh[0]["status"] == "False" and fresh[0]["message"] == "fresh-2"
    assert obj["status"]["conditions"] == [{"type": "Ready", "status": "True"}]   # not mutated
    assert b["metadata"]["resourceVersion"] == "7"          # optimistic concurrency kept
    assert load606.with_fresh_condition({"metadata": {}}, "m", "T")["status"]["conditions"][0][
        "message"] == "m"


class _API(http.server.BaseHTTPRequestHandler):
    puts: list = []
    conflicts = 0

    def do_GET(self):
        body = json.dumps({"metadata": {"name": "p", "resourceVersion": str(len(_API.puts))},
                           "status": {"conditions": [{"type": "Ready", "status": "True"}]}}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        n = int(self.headers.get("Content-Length") or 0)
        _API.puts.append((self.path, json.loads(self.rfile.read(n))))
        code = 409 if _API.conflicts > 0 else 200
        _API.conflicts -= 1
        self.send_response(code)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


@pytest.mark.parametrize("conflicts,ok,puts", [(0, True, 1), (1, True, 2), (3, False, 3)])
def test_incluster_status_write_goes_to_the_status_subresource(tmp_path, monkeypatch,
                                                              conflicts, ok, puts):
    import ssl
    monkeypatch.setattr(ssl, "create_default_context", lambda **k: None)
    (tmp_path / "token").write_text("sa-token")
    (tmp_path / "ca.crt").write_text("x")
    _API.puts, _API.conflicts = [], conflicts
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _API)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    got = load606.incluster_status_condition(
        *GVR, "bench-ns-01", "bench-app-01-01", "fresh-x", sa_dir=str(tmp_path),
        api=f"http://127.0.0.1:{srv.server_address[1]}")
    srv.shutdown()
    assert got is ok and len(_API.puts) == puts
    path, body = _API.puts[0]
    assert path.endswith("/namespaces/bench-ns-01/githubscaffoldingwithcompositionpages"
                         "/bench-app-01-01/status")
    cond = {c["type"]: c for c in body["status"]["conditions"]}
    assert cond["SnowplowBenchFresh"]["message"] == "fresh-x" and "Ready" in cond


def test_status_probe_restaction_lists_the_same_namespace_and_projects_the_condition():
    y = load606.list_dep_status_fixture_yaml("bench-ns-01", *GVR)
    ann = load606.list_dep_fixture_yaml("bench-ns-01", *GVR)
    path = re.search(r'path: "([^"]+)"', y).group(1)
    assert path == re.search(r'path: "([^"]+)"', ann).group(1)      # same list_dep edge
    assert "name: bench-fresh-listdep-status" in y
    assert 'select(.type == "SnowplowBenchFresh") | .message' in y
    assert "annotations" not in y


def test_status_mode_job_adds_phases_and_only_the_status_write():
    y = load606_job.render_fresh(modes=("annotation", "status"), **RENDER_KW)
    assert "--modes annotation status" in y
    assert ('resources: ["githubscaffoldingwithcompositionpages/status"]\n'
            '  verbs: ["get", "update"]') in y
    assert "secrets" not in y.lower()


# ─── evaluate: 10/s gates in every mode ────────────────────────────────────


def _f(label, n=40, ms=100, conv=True):
    return [{"kind": "fresh", "label": label, "converged": conv,
             "latency_ms": ms if conv else None} for _ in range(n)]


def test_each_mode_gates_at_its_own_10_per_second_point():
    fresh = (_f("list_dep@1") + _f("list_dep@10") + _f("list_dep@50")
             + _f("list_dep_status@1") + _f("list_dep_status@10", ms=4000) + _f("list_dep_status@50"))
    r = load606.evaluate([], fresh, [])
    c = {x["check"]: x for x in r["checks"]}
    assert c["fresh_p95_ms[list_dep@10]"]["result"] == "PASS"
    assert c["fresh_p95_ms[list_dep_status@10]"]["result"] == "FAIL"     # status misses 1 s
    assert set(r["report"]["fresh_curve_report_only"]) == {
        "list_dep@1", "list_dep@50", "list_dep_status@1", "list_dep_status@50"}
    assert r["report"]["fresh_modes"] == ["list_dep", "list_dep_status"]


def test_a_mode_without_its_10_per_second_point_fails():
    r = load606.evaluate([], _f("list_dep@10") + _f("list_dep_status@1"), [])
    c = {x["check"]: x for x in r["checks"]}
    assert c["fresh_p95_ms[list_dep_status@10]"]["result"] == "FAIL"
    assert c["fresh_p95_ms[list_dep_status@10]"]["why"] == "no samples at the gate churn rate"


def test_annotation_only_gating_is_unchanged():
    r = load606.evaluate([], _f("list_dep@1") + _f("list_dep@10") + _f("list_dep@50"), [])
    gated = [x["check"] for x in r["checks"] if x["check"].startswith("fresh_p95")]
    assert gated == ["fresh_p95_ms[list_dep@10]"]
    r = load606.evaluate([], _f("list_dep@1"), [])
    assert [x["check"] for x in r["checks"] if x["check"].startswith("fresh_p95")] == [
        "fresh_p95_ms[list_dep@10]"]


# ─── run606: modes reach data + Jobs; the wait is derived ──────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_run606_default_runs_both_modes_and_derives_the_wait(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1")
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert "--modes annotation status" in job
    assert "githubscaffoldingwithcompositionpages/status" in job
    # 0 delay + 1800 load-gate max + 2 modes x 1 rate x (2 x 30 s x 1.25 + 60 s overrun)
    #   = 2070 s; the driver needs 1 + 900; wait = 2070 + 600
    waits = [c["argv"] for c in _calls(state) if c["tool"] == "kubectl" and "wait" in c["argv"]
             and "job/fresh606" in c["argv"]]
    # the wait is the deadline minus `now`, read after waiting for the driver
    # Jobs: crossing a second (or two, with two k6 Jobs) takes 1-2 s off it
    # (#831 gate: 4/15 runs failed on an exact 2670)
    assert waits
    timeout = next(int(a[len("--timeout="):-1]) for a in waits[0] if a.startswith("--timeout="))
    assert 2667 <= timeout <= 2670, waits[0]


def test_run606_annotation_only_renders_the_run8_probe(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", FRESH_MODES="annotation")
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert "--wait-for-load --out /dev/stdout" in job          # no --modes appended
    assert 'githubscaffoldingwithcompositionpages/status"]' not in job
    assert "fresh_modes" not in json.loads((tmp / "out" / "data-proof.json").read_text())


def test_run606_refuses_an_unknown_mode_before_any_cloud_call(harness):
    env, state, ambient, tmp = harness
    env["FRESH_MODES"] = "annotation spec"
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 2 and "unknown mode 'spec'" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]
