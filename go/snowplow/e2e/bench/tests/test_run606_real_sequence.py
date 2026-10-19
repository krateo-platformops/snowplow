"""run606.sh — the REAL step sequence (not the dry run) under a WRONG ambient
kube context.

The first shakedown attempt (2026-10-14) died at step 1: `python -m bench
gke-reap` imported bench.cluster, whose import-time guard compared the
operator's ambient current-context (`kind-krateo-4388`) to the dead neon pin
and exited 3. The dry-run arms could not see it — they never execute a step
and conftest sets BENCH_ALLOW_NON_GKE=1 for the whole suite.

This arm executes every step for real, with:
  - fake `gcloud` / `kubectl` / `helm` first on PATH (no cloud, no cluster),
    each recording argv + the kubeconfig it resolved;
  - an ambient KUBECONFIG whose current-context is a deliberately wrong kind
    context, and BENCH_ALLOW_NON_GKE removed;
  - a local HTTP server standing in for authn + snowplow.

It asserts the run completes, tears down, never resolves the ambient
kubeconfig or any context but the bench cluster's, leaves the ambient file
byte-identical, and never prints the synthetic users' password.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import os
import subprocess
import sys
import textwrap
import threading
import urllib.parse
from pathlib import Path

import pytest

from bench import gke
from bench.load606 import SCALEUSER_PASSWORD
from tests import _planted

BENCH_DIR = Path(__file__).resolve().parent.parent
RUN_NAME = "snowplow-bench-1014-1300"
PROJECT = "operations-dev-krateo-io"
ZONE = "europe-west4-a"
KCTX = f"gke_{PROJECT}_{ZONE}_{RUN_NAME}"
AMBIENT_CTX = "kind-krateo-4388"
# The digest the fake snowplow pod runs and the run expects (a test value).
FAKE_DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32

_COMMON = textwrap.dedent("""\
    import json, os, sys, time
    STATE = os.environ["FAKE_STATE"]
    def record(tool, kubeconfig=None, context=None):
        with open(os.path.join(STATE, "calls.jsonl"), "a") as fh:
            fh.write(json.dumps({"tool": tool, "argv": sys.argv[1:],
                                 "kubeconfig": kubeconfig,
                                 "context": context}) + "\\n")
    """)

FAKE_GCLOUD = _COMMON + textwrap.dedent("""\
    a = sys.argv[1:]
    record("gcloud", os.environ.get("KUBECONFIG"))
    def opt(name):
        return a[a.index(name) + 1] if name in a else None
    if a[:3] == ["container", "clusters", "list"]:
        print("[]"); sys.exit(0)
    if a[:3] == ["container", "clusters", "get-credentials"]:
        name, p, z = a[3], opt("--project"), opt("--zone")
        ctx = f"gke_{p}_{z}_{name}"
        with open(os.environ["KUBECONFIG"], "w") as fh:
            json.dump({"apiVersion": "v1", "kind": "Config",
                       "current-context": ctx,
                       "contexts": [{"name": ctx, "context": {"cluster": ctx}}],
                       "clusters": [{"name": ctx, "cluster": {"server": "https://127.0.0.1:1"}}],
                       "users": []}, fh)
        sys.exit(0)
    if a[:3] == ["container", "clusters", "describe"]:
        labels = {"purpose": "snowplow-bench", "issue": "606",
                  "created-by": "bench-gke", "owner": "t",
                  "ttl-expires": str(int(time.time()) + 3600)}
        print(json.dumps({"name": a[3], "zone": opt("--zone"),
                          "resourceLabels": labels}))
        sys.exit(0)
    sys.exit(0)   # create / node-pools create / delete
    """)

FAKE_KUBECTL = _COMMON + textwrap.dedent("""\
    raw = sys.argv[1:]
    kc = os.environ.get("KUBECONFIG"); ctx = None; a = []; ns = None
    it = iter(raw)
    for x in it:
        if x.startswith("--kubeconfig="): kc = x.split("=", 1)[1]
        elif x.startswith("--context="): ctx = x.split("=", 1)[1]
        elif x in ("-n", "--namespace"): ns = next(it, None)   # anywhere, like kubectl
        elif x.startswith("--request-timeout="): pass          # global flag, anywhere
        else: a.append(x)
    resolved = ctx
    if resolved is None:
        try:
            resolved = json.load(open(kc))["current-context"]
        except Exception:
            resolved = None
    record("kubectl", kc, resolved)
    port = os.environ["FAKE_PORT"]
    if a[:2] == ["config", "current-context"]:
        if resolved is None: sys.exit(1)
        print(resolved); sys.exit(0)
    if a[:1] == ["annotate"]:
        kv = a[-1]
        if "bench-app-01-01" in a and kv.startswith("snowplow-bench/fresh-marker="):
            open(os.path.join(STATE, "marker"), "w").write(kv.split("=", 1)[1])
        sys.exit(0)
    ready = os.environ.get("FAKE_INSTALLER_READY", "1") == "1"
    kog = os.environ.get("FAKE_KOG", "0") == "1"           # run-3 private-registry 401
    kog_reason = os.environ.get("FAKE_KOG_REASON", "response status code 401: unauthorized")
    extra = os.environ.get("FAKE_EXTRA_NOT_READY", "0") == "1"
    def cr(kind, name, ok, reason="Available", msg="", synced=True, smsg=""):
        return {"kind": kind, "metadata": {"name": name, "namespace": "krateo-system"},
                "status": {"conditions": [{"type": "Ready", "status": "True" if ok else "False",
                                           "reason": reason, "message": msg},
                                          {"type": "Synced", "status": "True" if synced else "False",
                                           "reason": "ReconcileSuccess" if synced else "ReconcileError",
                                           "message": smsg}]}}
    def items(xs):
        print(json.dumps({"items": xs})); sys.exit(0)
    f483 = os.environ.get("FAKE_483", "")          # "", pass, wide
    if a[:3] == ["get", "crd", "alerts.observability.krateo.io"]:
        sys.exit(0 if f483 in ("pass", "wide") else 1)
    if a[:2] == ["get", "pods"] and "app.kubernetes.io/name=snowplow" in a and "json" in a:
        # snowplow's own pod: optionally OOM-restarted between the before/after
        # records (FAKE_RESTART_DURING_MEASURE=1).
        cnt = os.path.join(STATE, "snowplow_pod_reads")
        n = int(open(cnt).read()) if os.path.exists(cnt) else 0
        open(cnt, "w").write(str(n + 1))
        # read 0 = the digest gate, 1 = the before record, 2+ = after
        restarted = os.environ.get("FAKE_RESTART_DURING_MEASURE") == "1" and n >= 2
        cs = {"name": "snowplow", "ready": True, "restartCount": 1 if restarted else 0,
              "image": "ghcr.io/krateo-platformops/snowplow:1.12.50",
              "imageID": "ghcr.io/krateo-platformops/snowplow@" + os.environ["FAKE_IMAGE_DIGEST"],
              "lastState": ({"terminated": {"reason": "OOMKilled", "exitCode": 137,
                                            "finishedAt": "2026-10-14T15:04:53Z"}}
                            if restarted else {})}
        pods = [{"metadata": {"name": "snowplow-abc"},
                 "spec": {"containers": [{"resources": {"limits": {"memory": "8Gi"}}}]},
                 "status": {"containerStatuses": [cs]}}]
        # FAKE_TERMINATING_READS=N: the first N reads also list the old
        # ReplicaSet's Terminating pod (old digest) FIRST; -1 = it never goes.
        tr = int(os.environ.get("FAKE_TERMINATING_READS", "0"))
        if tr < 0 or n < tr:
            old = {"name": "snowplow", "ready": False, "restartCount": 0,
                   "imageID": "ghcr.io/krateo-platformops/snowplow@sha256:" + "ee" * 32}
            pods.insert(0, {"metadata": {"name": "snowplow-old",
                                         "deletionTimestamp": "2026-10-19T08:00:00Z"},
                            "spec": {"containers": [{}]},
                            "status": {"containerStatuses": [old]}})
        print(json.dumps({"items": pods}))
        sys.exit(0)
    if a[:2] == ["top", "pod"]:
        print("snowplow-abc 120m 650Mi"); sys.exit(0)
    if a[:1] == ["cp"] and any("mem606" in x for x in a):
        dest = a[-1]
        os.makedirs(dest, exist_ok=True)
        open(os.path.join(dest, "heap-01-150000-ready.pb.gz"), "wb").write(b"\\x1f\\x8bfake")
        sys.exit(0)
    if a[:1] == ["logs"] and any("mem606" in x for x in a):
        print(json.dumps({"kind": "mem", "t": 1.0, "readyz": 200, "vars_status": 200,
                          "heap_inuse_mb": 1392.8, "sys_mb": 1551.9}))
        print(json.dumps({"kind": "heap_profile", "t": 1.0, "profile": "heap-01-150000-ready.pb.gz",
                          "bytes": 152190, "at": "ready"}))
        print(json.dumps({"kind": "mem", "t": 16.0, "readyz": 200, "vars_status": 200,
                          "heap_inuse_mb": 3789.4, "sys_mb": 6741.5}))
        sys.exit(0)
    if a[:1] == ["get"] and "json" in a:
        what = a[1]
        if what == "crd":
            kinds = [("installers", "Installer"), ("authns", "Authn"), ("snowplows", "Snowplow"),
                     ("frontends", "Frontend"), ("portals", "Portal")]
            if f483:
                kinds += [("alertprovidercrds", "AlertProviderCrds"),
                          ("krateoobservabilities", "KrateoObservability")]
            if f483 == "wide":
                kinds.append(("alertproviders", "AlertProvider"))
            items([{"spec": {"group": "composition.krateo.io",
                             "names": {"plural": p, "kind": k}}} for p, k in kinds])
        if what.startswith("alertprovidercrds."):
            items([cr("AlertProviderCrds", "alert-provider-crd", True)])
        if what.startswith("krateoobservabilities."):
            items([cr("KrateoObservability", "krateo-observability", True)])
        if what.startswith("alertproviders."):
            items([cr("AlertProvider", "alert-provider", True)])
        if what.startswith("alerts.observability"):
            items([{"metadata": {"name": "sre-pod-crashloop", "namespace": "krateo-system"}}])
        if what.startswith("installers."):
            kids = []
            if not ready:
                kids.append("authns/krateo-system/authn")
            if kog:
                kids.append("compositiondefinitions/krateo-system/github-provider-kog")
            if extra:
                kids.append("compositiondefinitions/krateo-system/oasgen-provider")
            msg = (f"{len(kids)} of 41 managed children are not ready: " + ", ".join(kids)) if kids else ""
            items([cr("Installer", "installer", not kids,
                      "Creating" if kids else "Available", msg)])
        if what.startswith("authns."):
            items([cr("Authn", "authn", ready, "ReconcileError",
                      "helm install failed: token=" + os.environ.get("FAKE_TOKVAL", ""))])
        if what.startswith("snowplows."):
            items([cr("Snowplow", "snowplow", ready)])
        if what.startswith("frontends."):
            items([cr("Frontend", "frontend", True)])
        if what.startswith("portals."):
            items([cr("Portal", "portal", True)])
        if what.startswith("compositiondefinitions."):
            cds = [cr("CompositionDefinition", "authn", True)]
            if f483:
                cds.append(cr("CompositionDefinition", "alert-provider-crd", True))
            if f483 == "wide":
                cds.append(cr("CompositionDefinition", "alert-provider", True))
            if kog:
                cds.append(cr("CompositionDefinition", "github-provider-kog", False, "Creating", "",
                              synced=False, smsg=(
                                  "observe failed: error getting chart info: failed to get chart "
                                  "after 5 attempts: failed to get chart: failed to get "
                                  "oci://ghcr.io/krateo-blueprints/charts/github-provider-kog: failed "
                                  "to resolve reference ghcr.io/krateo-blueprints/charts/"
                                  "github-provider-kog:0.3.2: HEAD \\"https://ghcr.io/v2/krateo-"
                                  "blueprints/charts/github-provider-kog/manifests/0.3.2\\": GET "
                                  "\\"https://ghcr.io/token?scope=repository%3Akrateo-blueprints%2F"
                                  "charts%2Fgithub-provider-kog%3Apull&service=ghcr.io\\": "
                                  + kog_reason)))
            if extra:
                cds.append(cr("CompositionDefinition", "oasgen-provider", False, "Creating",
                              "chart fetch timeout"))
            items(cds)
        if what == "pods":
            items([{"metadata": {"name": "ok-0", "namespace": "krateo-system"},
                    "spec": {"nodeName": "n1"}, "status": {"phase": "Running",
                    "containerStatuses": [{"name": "c", "ready": True, "restartCount": 0, "state": {"running": {}}}]}},
                   {"metadata": {"name": "crash-0", "namespace": "krateo-system"},
                    "spec": {"nodeName": "n1"}, "status": {"phase": "Running",
                    "containerStatuses": [{"name": "app", "ready": False, "restartCount": 3,
                        "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                        "lastState": {"terminated": {"reason": "Error", "exitCode": 1}}}]}}])
        if what == "events":
            ev = [{"lastTimestamp": "2026-10-14T08:10:00Z", "type": "Warning",
                   "reason": "BackOff", "involvedObject": {"kind": "Pod", "namespace": "krateo-system", "name": "crash-0"},
                   "message": "Back-off restarting failed container"}]
            if os.environ.get("FAKE_SVC") == "pending":
                ev.append({"lastTimestamp": "2026-10-14T11:17:53Z", "type": "Warning",
                           "reason": "SyncLoadBalancerFailed",
                           "involvedObject": {"kind": "Service", "namespace": "krateo-system", "name": "snowplow"},
                           "message": "Error syncing load balancer: googleapi: Error 403: QUOTA_EXCEEDED - "
                                      "Quota 'IN_USE_ADDRESSES' exceeded.  Limit: 14.0 globally."})
            items(ev)
        if what in ("pvc", "nodes"):
            items([])
    if a[:1] == ["logs"] and "crash-0" in a:
        print("starting app")
        print("Authorization: Bearer " + os.environ.get("FAKE_BEARER", ""))
        print("jwt=" + os.environ.get("FAKE_JWT", ""))
        print("login with " + os.environ.get("FAKE_PW", ""))
        print("panic: cannot reach ghcr.io")
        sys.exit(0)
    if a[:1] == ["get"]:
        what = a[1] if len(a) > 1 else ""
        if what == "ns":
            print("bench-ns-01   Active   1m\\nbench-ns-02   Active   1m"); sys.exit(0)
        if what == "crd":
            sys.exit(1)
        if what.startswith("compositiondefinitions"):
            print("True"); sys.exit(0)
        if what.startswith("githubscaffoldingwithcompositionpages"):
            print("\\n".join(f"bench-ns-01 bench-app-01-0{i} 1m" for i in range(1, 5)))
            sys.exit(0)
        if what == "svc":
            mode = os.environ.get("FAKE_SVC", "lb")     # lb | clusterip | pending
            ports = {"snowplow": 8081, "authn": 8082, "frontend": 8080}
            def svc(n):
                typ = "ClusterIP" if mode == "clusterip" else "LoadBalancer"
                ing = [{"ip": "127.0.0.1"}] if mode == "lb" else []
                return {"metadata": {"name": n, "namespace": "krateo-system"},
                        "spec": {"type": typ, "ports": [{"port": ports[n]}]},
                        "status": {"loadBalancer": {"ingress": ing}}}
            print(json.dumps({"items": [svc("snowplow"), svc("authn"), svc("frontend")]}))
            sys.exit(0)
        if what == "pods" and "job-name=load606" in a:
            print("pod/load606-0"); sys.exit(0)
        if what == "pods" and "job-name=fresh606" in a:
            print("pod/fresh606-0"); sys.exit(0)
        if what == "pods" and "job-name=mem606" in a:
            print("pod/mem606-0"); sys.exit(0)
        if what == "configmap" and "frontend-config-vars" in a:
            init = os.environ.get("FAKE_INIT", "/call?resource=layouts&apiVersion="
                                  "widgets.templates.krateo.io/v1beta1&name=app-shell"
                                  "&namespace=krateo-system")
            print(json.dumps({"api": {"INIT": init, "BUILDERS_NAMESPACE": "krateo-system"},
                              "params": {"FRONTEND_NAMESPACE": "krateo-system"}}))
            sys.exit(0)
        if what == "job" and "preflight606" in a:
            refused = os.environ.get("FAKE_PREFLIGHT", "ok") == "refused"
            if any(x.endswith("{.status.succeeded}") for x in a):
                print("" if refused else "1"); sys.exit(0)
            if any(x.endswith("{.status.failed}") for x in a):
                print("1" if refused else ""); sys.exit(0)
        sys.exit(0)
    if a[:1] == ["logs"] and any("preflight606" in x for x in a):
        if os.environ.get("FAKE_PREFLIGHT", "ok") == "refused":
            print(json.dumps({"kind": "preflight", "ok": False, "init_status": 200,
                              "routes": [], "refused": [{"path": "/dashboard", "status": 500,
                              "body": "no matches for widgets.templates.krateo.io/v1beta1, Resource=pages"}],
                              "product_errors": []}))
            print("preflight REFUSED: /dashboard -> 500", file=sys.stderr)
        else:
            print(json.dumps({"kind": "preflight", "ok": True, "init_status": 200,
                              "routes": [{"path": "/dashboard", "status": 200}],
                              "refused": [], "product_errors": [
                                  {"path": "/agents/new", "status": 500,
                                   "body": "unexpected end of JSON input"}],
                              "composite_routes": ["/observability", "/compositions"],
                              "full_walk_report_only": {"calls": 590, "bytes": 8429613,
                                                        "wall_ms": 17644}}))
        sys.exit(0)
    if a[:1] == ["logs"] and any("fresh606" in x for x in a):
        print("fresh: starting")                       # stderr-ish noise, skipped
        for rate in os.environ.get("FAKE_FRESH_RATES", "1 10 50").split():
            for i in range(2):
                print(json.dumps({"kind": "fresh", "label": f"list_dep@{rate}",
                                  "converged": True, "latency_ms": 120 + i,
                                  "polls": 2, "poll_ms": 100}))
        sys.exit(0)
    if a[:1] == ["logs"] and any("load606" in x for x in a):
        time.sleep(float(os.environ.get("FAKE_SLOW_LOGS", "0")))   # a slow / hung read
        for v in (1, 2):
            print(json.dumps({"kind": "nav", "user": "scaleuser-0001", "page": "p",
                              "visit": v, "wall_ms": 50, "calls": 3, "errors": 0,
                              "sched_lag_ms": 1}))
        sys.exit(0)
    if a[:1] == ["wait"] and "job/fresh606" in a and os.environ.get("FAKE_WAIT_FAIL") == "1":
        print("error: timed out waiting for the condition on jobs/fresh606", file=sys.stderr)
        sys.exit(1)
    if a[:1] == ["logs"] and "deploy/snowplow" in a and "--previous" not in a:
        print("snowplow log line"); sys.exit(0)
    sys.exit(0)   # apply / wait / rollout / version / scale / top / logs
    """)

FAKE_HELM = _COMMON + textwrap.dedent("""\
    a = sys.argv[1:]
    kc = a[a.index("--kubeconfig") + 1] if "--kubeconfig" in a else os.environ.get("KUBECONFIG")
    ctx = a[a.index("--kube-context") + 1] if "--kube-context" in a else None
    record("helm", kc, ctx)
    ver = a[a.index("--version") + 1] if "--version" in a else "0"
    if a[:2] == ["show", "chart"]:
        if os.environ.get("FAKE_CHART_MISSING") == "1":
            print("Error: not found", file=sys.stderr); sys.exit(1)
        print(f"apiVersion: v2\\nname: installer\\nversion: {ver}"); sys.exit(0)
    if a[:1] == ["pull"]:
        d = os.path.join(a[a.index("-d") + 1], "installer")
        os.makedirs(os.path.join(d, "files"), exist_ok=True)
        os.makedirs(os.path.join(d, "charts"), exist_ok=True)
        open(os.path.join(d, "charts", "core-provider-2.13.24.tgz"), "w").write("x")
        open(os.path.join(d, "Chart.yaml"), "w").write(json.dumps({
            "apiVersion": "v2", "name": "installer", "version": ver,
            "dependencies": [{"name": "core-provider", "version": "2.13.24"}]}))
        good = os.environ.get("FAKE_PINS", "good") == "good"
        open(os.path.join(d, "files", "component-pins.yaml"), "w").write(json.dumps({
            "components": [
                {"name": "alert-provider-crd", "feature": "portal" if good else "specialistAgents"},
                {"name": "alert-provider", "feature": "specialistAgents"},
                {"name": "krateo-observability", "feature": "portal",
                 "deps": ["clickhouse-operator"] + (["alert-provider-crd"] if good else [])}]}))
        sys.exit(0)
    if "list" in a:
        print(json.dumps([{"name": "installer", "namespace": "krateo-system",
                           "status": "deployed", "chart": "installer-0.3.422"}]))
    sys.exit(0)
    """)


def _fake_server(state: Path):
    class H(http.server.BaseHTTPRequestHandler):
        def _send(self, obj):
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            if u.path == "/basic/login":
                return self._send({"accessToken": "fake-jwt"})
            if u.path == "/call" and q.get("name") == ["bench-fresh-listdep"]:
                m = state / "marker"
                marker = m.read_text() if m.exists() else ""
                return self._send({"status": [{"name": "bench-app-01-01",
                                               "marker": marker}]})
            return self._send({})

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _write_exe(path: Path, body: str):
    path.write_text(f"#!{sys.executable} -I\n" + body)
    path.chmod(0o755)


@pytest.fixture
def harness(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    _write_exe(fakebin / "gcloud", FAKE_GCLOUD)
    _write_exe(fakebin / "kubectl", FAKE_KUBECTL)
    _write_exe(fakebin / "helm", FAKE_HELM)
    ambient = tmp_path / "ambient.kubeconfig"
    ambient.write_text(json.dumps({
        "apiVersion": "v1", "kind": "Config", "current-context": AMBIENT_CTX,
        "contexts": [{"name": AMBIENT_CTX, "context": {"cluster": AMBIENT_CTX}}],
        "clusters": [], "users": []}))
    srv = _fake_server(state)
    env = dict(os.environ)
    for k in ("BENCH_ALLOW_NON_GKE", "BENCH_GKE_CONTEXT", "BENCH_606_DRY_RUN",
              "BENCH_606_FAIL_AT", "SNOWPLOW_URL", "AUTHN_URL", "FRONTEND_URL"):
        env.pop(k, None)
    env.update({
        "PATH": f"{fakebin}{os.pathsep}{env['PATH']}",
        "HOME": str(tmp_path / "home"),
        "KUBECONFIG": str(ambient),          # the WRONG ambient context
        "FAKE_STATE": str(state),
        "FAKE_PORT": str(srv.server_address[1]),
        "PYTHONPATH": str(BENCH_DIR),
        "RUN_NAME": RUN_NAME, "INSTALLER_VERSION": "0.3.422",
        "SNOWPLOW_TAG": "1.12.46", "OUT": str(tmp_path / "out"),
        "EXPECTED_IMAGE_DIGEST": FAKE_DIGEST, "FAKE_IMAGE_DIGEST": FAKE_DIGEST,
        "SCALE": "4", "NAMESPACES": "2", "USERS": "2",
        "THINK_S": "0", "DRIVE_S": "1", "CHURN_RATES": "1",
        "FRESH_SAMPLES": "2", "FRESH_GAP_S": "0", "HOLD_PROBE_WAIT_S": "0",
        "PLATFORM_TIMEOUT_S": "1", "PLATFORM_POLL_S": "0.2",
        "BENCH_NO_COLOR": "1", "FAKE_PW": SCALEUSER_PASSWORD,
    })
    from datetime import datetime, timezone
    env["BENCH_606_GO"] = datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + RUN_NAME
    (tmp_path / "home").mkdir()
    yield env, state, ambient, tmp_path
    srv.shutdown()


def _calls(state: Path):
    p = state / "calls.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


def test_real_sequence_under_wrong_ambient_context(harness):
    env, state, ambient, tmp = harness
    before = hashlib.sha256(ambient.read_bytes()).hexdigest()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-4000:]
    assert "context guard FAIL" not in out
    assert "=== step evaluate ===" in p.stderr and "run complete" in p.stderr

    calls = _calls(state)
    kcfg = str(tmp / "home" / ".kube" / "snowplow-bench" / f"{RUN_NAME}.kubeconfig")
    kube_calls = [c for c in calls if c["tool"] in ("kubectl", "helm")]
    assert kube_calls, "no kubectl/helm call recorded"
    for c in kube_calls:
        assert c["kubeconfig"] == kcfg, c
        assert c["context"] in (KCTX, None), c
        assert c["context"] != AMBIENT_CTX
    # every kubectl call resolved to the bench context (explicit or via the
    # dedicated kubeconfig's current-context)
    assert all(c["context"] == KCTX for c in calls if c["tool"] == "kubectl")
    # no gcloud call ever ran against the ambient kubeconfig
    assert all(c["kubeconfig"] != str(ambient) for c in calls)
    # the ambient kubeconfig is untouched
    assert hashlib.sha256(ambient.read_bytes()).hexdigest() == before

    # the harness steps really ran (data606, both in-cluster Jobs, verdict)
    proof = json.loads((tmp / "out" / "data-proof.json").read_text())
    assert proof["compositions"] == 4
    assert proof["logins"] == "deferred-to-in-cluster-driver"
    fresh = [json.loads(l) for l in
             (tmp / "out" / "fresh-fresh606-0.jsonl").read_text().splitlines()
             if l.startswith("{")]
    assert {r["label"] for r in fresh} == {"list_dep@1", "list_dep@10", "list_dep@50"}
    navs = [json.loads(l) for l in
            (tmp / "out" / "nav-load606-0.jsonl").read_text().splitlines()]
    assert [n["visit"] for n in navs] == [1, 2]                 # collected from the Job
    verdict = json.loads((tmp / "out" / "verdict.json").read_text())
    assert verdict["report"]["navs"] == 2
    gated = [c["check"] for c in verdict["checks"] if c["check"].startswith("fresh_p95")]
    assert gated == ["fresh_p95_ms[list_dep@10]"]          # ruling: gate at 10/s
    assert set(verdict["report"]["fresh_curve_report_only"]) == {"list_dep@1", "list_dep@50"}

    # teardown went through the label-checked path and deleted THIS cluster
    gcl = [c["argv"] for c in calls if c["tool"] == "gcloud"]
    assert ["container", "clusters", "describe", RUN_NAME] == gcl[-2][:4]
    assert gcl[-1][:4] == ["container", "clusters", "delete", RUN_NAME]
    assert "krateo-057" not in json.dumps(calls)

    # the synthetic users' password never reaches a log
    assert SCALEUSER_PASSWORD not in out
    assert SCALEUSER_PASSWORD not in (tmp / "out" / "run.log").read_text()


def test_gke_entrypoint_never_imports_the_context_guard():
    """`python -m bench.gke` must not import bench.cluster (its import-time
    guard reads the ambient context)."""
    code = ("import sys, bench.gke; "
            "print('bench.cluster' in sys.modules, 'bench.cli' in sys.modules)")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, env=dict(os.environ, PYTHONPATH=str(BENCH_DIR)))
    assert p.stdout.split() == ["False", "False"], p.stderr


def test_dry_run_log_redacts_password(tmp_path):
    env = dict(os.environ, HOME=str(tmp_path), OUT=str(tmp_path / "out"),
               RUN_NAME=RUN_NAME, INSTALLER_VERSION="0.3.422",
               SNOWPLOW_TAG="1.12.46", THINK_S="0", BENCH_606_DRY_RUN="1",
               EXPECTED_IMAGE_DIGEST=FAKE_DIGEST, PYTHONPATH=str(BENCH_DIR))
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr
    # The password is only ever written into the in-cluster Job manifest (a
    # file), never into a logged command; any log line that did carry it would
    # be redacted by log(). The property that matters: it is in NO log.
    assert SCALEUSER_PASSWORD not in p.stderr + p.stdout
    assert SCALEUSER_PASSWORD not in (tmp_path / "out" / "run.log").read_text()


def test_failure_path_writes_redacted_diagnostics_before_teardown(harness):
    """Installer never goes Ready (shakedown #2's failure): the platform step
    must print WHERE it is stuck, time out, snapshot the cluster BEFORE the
    delete, read no Secret, and redact every token-looking value."""
    env, state, ambient, tmp = harness
    env["FAKE_INSTALLER_READY"] = "0"
    # Planted credentials, built at runtime (no credential literal in source).
    jwt, bear, tokval = _planted.jwt(), _planted.bearer(), _planted.opaque()
    env.update(FAKE_JWT=jwt, FAKE_BEARER=bear, FAKE_TOKVAL=tokval)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    # progress shows the stuck component and its reason
    assert "[platform] installer Ready=False" in out
    assert "NOT READY: authns/krateo-system/authn(ReconcileError" in out
    assert "TIMEOUT" in out

    diag = tmp / "out" / "diagnostics"
    for f in ("installer-cr.json", "composition-crs.json", "compositiondefinitions.json",
              "pods.json", "pods-failing.json", "events.json", "pvcs.json",
              "nodes.json", "helm-list.json", "progress.txt"):
        assert (diag / f).exists(), f
    failing = json.loads((diag / "pods-failing.json").read_text())
    assert [x["name"] for x in failing] == ["crash-0"]
    assert failing[0]["containers"][0]["waiting"] == "CrashLoopBackOff"
    assert failing[0]["containers"][0]["last_reason"] == "Error"
    cur = diag / "logs" / "krateo-system__crash-0__app__current.log"
    prev = diag / "logs" / "krateo-system__crash-0__app__previous.log"
    assert cur.exists() and prev.exists()
    assert "panic: cannot reach ghcr.io" in cur.read_text()
    comps = json.loads((diag / "composition-crs.json").read_text())
    assert any(c["name"] == "authn" and c["Ready"]["reason"] == "ReconcileError"
               for c in comps["components"])

    bundle = "\n".join(f.read_text() for f in diag.rglob("*") if f.is_file())
    for leaked in (SCALEUSER_PASSWORD, bear, jwt, jwt.split(".")[2], tokval):
        assert leaked not in bundle, leaked
    assert "<redacted" in bundle

    calls = _calls(state)

    def verb(c):
        return [a for a in c["argv"]
                if not a.startswith(("--kubeconfig=", "--context="))]
    assert not any(a in ("secret", "secrets") for c in calls
                   if c["tool"] == "kubectl" for a in verb(c)[:3])
    # diagnostics were taken BEFORE the delete, and the delete still happened
    idx_logs = max(i for i, c in enumerate(calls)
                   if c["tool"] == "kubectl" and verb(c)[:1] == ["logs"])
    idx_delete = max(i for i, c in enumerate(calls)
                     if c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "delete"])
    assert idx_logs < idx_delete
    assert all(c["context"] in (KCTX, None) for c in calls if c["tool"] in ("kubectl", "helm"))
