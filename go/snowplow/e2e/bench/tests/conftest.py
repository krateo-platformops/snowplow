"""Shared pytest fixtures for the bench harness (Block 1 scope).

Adds `e2e/bench/` to sys.path so `import bench.cluster` resolves without
requiring an editable install. Sets BENCH_ALLOW_NON_GKE=1 before any
test imports bench.cluster so the module-level gke_context_guard does
NOT exit during test collection.

Risk Register R1.1 mitigation: `reset_k8s_state` fixture (opt-in) nulls
the cluster.py module-level globals so tests don't share state.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# ─── Test isolation: env vars that MUST be set before bench.* imports ────────

# Bypass the GKE context guard — tests should never contact a real cluster.
os.environ.setdefault("BENCH_ALLOW_NON_GKE", "1")

# BENCH_GKE_CONTEXT retargets the pinned context (cluster.canonical_gke_context).
# An operator who exports it in their shell to drive a real bench run would
# otherwise change what the context-pinning tests assert. Clear it so the suite
# is hermetic against the operator's environment; the tests that exercise the
# override set it explicitly via monkeypatch.
os.environ.pop("BENCH_GKE_CONTEXT", None)

# Add e2e/bench/ to sys.path so `import bench.cluster` works without install.
_E2E_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_E2E_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_E2E_BENCH_DIR))


# ─── The ghcr pull token (bench/ghcr_pull.py) ────────────────────────────────
# run606 refuses without the operator's read-only pull-token file and resolves
# the repo-override charts with it before any cluster exists. The suite must
# never read the operator's real file nor reach ghcr.io: every run606 it
# starts (any HOME) inherits a FAKE file (mode 0600) and a fake registry, over
# TLS (the module sends the token over https only), whose self-signed
# certificate is trusted through SSL_CERT_FILE.
#
# The fake registry knows these tokens (`fake_bearer(token)` is the bearer
# its exchange hands out, derived from the token so a leaked bearer is
# visible to a scan):
#   FAKE_GHCR_TOKEN   reads every chart below;
#   "scope401/403"    under-scoped: the exchange succeeds, the private
#                     (krateo-agentiko) manifests answer 401 / 403;
#   "redirect"        the exchange answers a redirect to another host;
#   "imgscope403"     reads the charts, but the private (krateo-agentiko)
#                     IMAGES answer 403;
#   anything else     the exchange answers 401.
# Every chart and image is published at every version except
# FAKE_GHCR_UNPUBLISHED (a 404): the suite's fake pins and the real installer
# charts some tests pull (their real pins) both resolve, and an arm names the
# missing version. A chart's content (GET manifest, then its helm layer) is
# built from FAKE_CHART_VALUES, appVersion = the version asked for; its blob
# is served, as ghcr does, through a redirect to a blob store that REFUSES
# any Authorization header (a client must not forward the registry's bearer).
# Chart versions that misbehave on purpose: FAKE_CHART_BAD_DIGEST (its blob
# does not match the manifest's digest), FAKE_CHART_HTTP_BLOB (its blob
# redirect points at plain http), FAKE_CHART_BIG_MANIFEST (a manifest past
# the distribution spec's 4 MiB), FAKE_CHART_BLOB_OVER_SIZE (a blob larger
# than the size its manifest declares), FAKE_CHART_DRIP (a manifest dripped
# a byte at a time, for ever).
FAKE_GHCR_USER, FAKE_GHCR_TOKEN = "fake-ghcr-user", "fake-ghcr-token-" + "f00d" * 4
FAKE_GHCR_UNPUBLISHED = "9.9.9"
FAKE_CHART_BAD_DIGEST, FAKE_CHART_HTTP_BLOB = "6.6.6", "5.5.5"
FAKE_CHART_BIG_MANIFEST, FAKE_CHART_BLOB_OVER_SIZE, FAKE_CHART_DRIP = "4.4.4", "3.3.3", "7.7.7"
FAKE_CHART_VALUES = {
    # the two private hops, as run 19's dry pass found them: certReplayHop's image is in the values;
    # uiTokenHop is OFF in the chart's own values and names no image there: the installer turns it on
    # through componentValues, and its image is in a template only (#1023 A)
    "agentgateway-policies": {
        "uiTokenHop": {"enabled": False},
        "certReplayHop": {"enabled": True,
                          "image": {"repository": "ghcr.io/krateo-agentiko/cert-replay-hop", "tag": ""}},
        "proxy": {"image": {"registry": "cr.agentgateway.dev", "repository": "agentgateway", "tag": "v0.7.0"}}},
    "agentgateway-controller": {"image": {"registry": "cr.agentgateway.dev", "repository": "controller",
                                          "tag": "v0.7.0"}},
}
#: each chart's templates, real Helm templates (rendered by the real helm)
FAKE_CHART_TEMPLATES = {
    "agentgateway-policies": {
        "templates/cert-replay-hop.yaml": """{{- if .Values.certReplayHop.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cert-replay-hop
spec:
  template:
    spec:
      containers:
      - name: hop
        image: "{{ .Values.certReplayHop.image.repository }}:{{ .Values.certReplayHop.image.tag | default .Chart.AppVersion }}"
{{- end }}
""",
        # the dashboard's hop: OFF in the chart's values, its image named only here
        "templates/ui-token-hop.yaml": """{{- if .Values.uiTokenHop.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ui-token-hop
spec:
  template:
    spec:
      containers:
      - name: hop
        image: "ghcr.io/krateo-agentiko/ui-token-hop:{{ .Chart.AppVersion }}"
{{- end }}
"""},
    # an init image hard-coded in a template, named by no values.yaml
    "agentgateway-controller": {"templates/controller.yaml": """apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller
spec:
  template:
    spec:
      initContainers:
      - name: init
        image: "ghcr.io/krateo-agentiko/agw-init:{{ .Chart.AppVersion }}"
      containers:
      - name: c
        image: "{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}"
"""},
}


#: the `helm template` the suite renders with: the REAL helm (found before any fake is on PATH),
#: except FAKE_HELM_TEMPLATE_FAIL / _EMPTY = a chart name (the release name the preflight passes):
#: that chart's render fails / renders nothing
FAKE_HELM_TEMPLATE = """import os
import sys
REAL = %r
a = sys.argv[1:]
name = a[1] if len(a) > 1 else ""
if os.environ.get("FAKE_HELM_TEMPLATE_FAIL") == name:
    print(f"Error: execution error at ({name}/templates/hop.yaml:3:4): a required value is missing", file=sys.stderr)
    sys.exit(1)
if os.environ.get("FAKE_HELM_TEMPLATE_EMPTY") == name:
    sys.stdin.read()
    sys.exit(0)
if not REAL:
    print("Error: helm is not installed: the suite renders charts with the real helm", file=sys.stderr)
    sys.exit(1)
os.execv(REAL, [REAL, *a])
"""


def fake_bearer(token: str) -> str:
    import hashlib
    return "fake-bearer-" + hashlib.sha256(token.encode()).hexdigest()[:24]


def _fake_ghcr():
    import base64
    import shutil
    import http.server
    import json
    import ssl
    import subprocess
    import tempfile
    import threading

    d = tempfile.mkdtemp(prefix="bench-ghcr-")
    cert, key = os.path.join(d, "cert.pem"), os.path.join(d, "key.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert,
                    "-days", "2", "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1"],
                   check=True, capture_output=True)
    known = {FAKE_GHCR_TOKEN, "scope401", "scope403", "redirect", "imgscope403"}
    bearers = {fake_bearer(t): t for t in known}
    blobs, http_blobs = {}, set()

    def chart_tgz(name, version):
        import io
        import tarfile
        import yaml
        chart = name.rsplit("/", 1)[1]
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            files = [("Chart.yaml", {"apiVersion": "v2", "name": chart, "version": version, "appVersion": version}),
                     ("values.yaml", FAKE_CHART_VALUES.get(chart, {}))]
            files += sorted((FAKE_CHART_TEMPLATES.get(chart) or {}).items())
            if chart == "agentgateway-policies":     # a subchart: its own values, its own appVersion
                files += [("charts/hop-sidecar/Chart.yaml", {"apiVersion": "v2", "name": "hop-sidecar",
                                                             "version": "9.0.1", "appVersion": "9.0.1"}),
                          ("charts/hop-sidecar/values.yaml",
                           {"image": {"repository": "ghcr.io/krateo-agentiko/hop-sidecar", "tag": ""}})]
            for fn, doc in files:
                data = (doc if isinstance(doc, str) else yaml.safe_dump(doc)).encode()
                ti = tarfile.TarInfo(f"{chart}/{fn}")
                ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
        return buf.getvalue()

    class H(http.server.BaseHTTPRequestHandler):
        def _answer(self, code, body=b"", headers=()):
            self.send_response(code)
            for k, v in headers:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/token"):
                auth = self.headers.get("Authorization") or ""
                try:
                    user, _, token = base64.b64decode(auth[len("Basic "):]).decode().partition(":")
                except ValueError:
                    return self._answer(401)
                if user != FAKE_GHCR_USER or token not in known:
                    return self._answer(401)
                if token == "redirect":
                    return self._answer(307, headers=[("Location", "https://elsewhere.invalid/token")])
                return self._answer(200, json.dumps({"token": fake_bearer(token)}).encode())
            if self.path.startswith("/blobstore/"):
                if self.headers.get("Authorization"):
                    return self._answer(400, b"an Authorization header reached the blob store")
                data = blobs.get(self.path[len("/blobstore/"):])
                return self._answer(200, data) if data is not None else self._answer(404)
            return self.do_HEAD()

        def do_HEAD(self):
            import hashlib
            token = bearers.get((self.headers.get("Authorization") or "")[len("Bearer "):])
            if token is None:
                return self._answer(401)
            if "/blobs/" in self.path:
                digest = self.path.rsplit("/blobs/", 1)[1]
                scheme = "http" if digest in http_blobs else "https"
                return self._answer(307, headers=[("Location", f"{scheme}://127.0.0.1:{self.server.server_address[1]}"
                                                               f"/blobstore/{digest}")])
            name, _, tag = self.path[len("/v2/"):].partition("/manifests/")
            if token in ("scope401", "scope403") and name.startswith("krateo-agentiko/"):
                return self._answer(int(token[-3:]))
            if token == "imgscope403" and name.startswith("krateo-agentiko/") and "/charts/" not in f"/{name}":
                return self._answer(403)
            if tag == FAKE_GHCR_UNPUBLISHED:
                return self._answer(404)
            if self.command == "GET" and "/charts/" in f"/{name}" and tag == FAKE_CHART_DRIP:
                import time
                self.send_response(200)
                self.send_header("Content-Length", "1000000")
                self.end_headers()
                for _ in range(600):
                    try:
                        self.wfile.write(b" ")
                        self.wfile.flush()
                    except OSError:
                        return
                    time.sleep(0.2)
                return
            if self.command == "GET" and "/charts/" in f"/{name}" and tag == FAKE_CHART_BIG_MANIFEST:
                return self._answer(200, b"{" + b" " * (4 * 1024 * 1024 + 10) + b"}")
            if self.command == "GET" and "/charts/" in f"/{name}":
                data = chart_tgz(name, tag)
                digest = "sha256:" + hashlib.sha256(data).hexdigest()
                # tampered IN PLACE (same size): the digest check, not the size bound, must catch it
                blobs[digest] = data[:-1] + bytes([data[-1] ^ 1]) if tag == FAKE_CHART_BAD_DIGEST else data
                if tag == FAKE_CHART_HTTP_BLOB:
                    http_blobs.add(digest)
                size = len(data) - (1 if tag == FAKE_CHART_BLOB_OVER_SIZE else 0)
                return self._answer(200, json.dumps({"schemaVersion": 2, "layers": [
                    {"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip", "digest": digest,
                     "size": size}]}).encode())
            return self._answer(200)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    f = os.path.join(d, "ghcr-pull.env")
    with open(os.open(f, os.O_WRONLY | os.O_CREAT, 0o600), "w") as fh:
        fh.write(f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\nGHCR_PULL_TOKEN={FAKE_GHCR_TOKEN}\n")
    tpl = os.path.join(d, "fake_helm_template.py")
    with open(tpl, "w") as fh:
        fh.write("#!/usr/bin/env python3" + chr(10) + FAKE_HELM_TEMPLATE % shutil.which("helm"))
    os.chmod(tpl, 0o755)
    os.environ["FAKE_HELM_TEMPLATE_BIN"] = tpl
    return f"https://127.0.0.1:{srv.server_address[1]}", f, cert


os.environ["GHCR_API_URL"], os.environ["GHCR_PULL_ENV_FILE"], os.environ["SSL_CERT_FILE"] = _fake_ghcr()


@pytest.fixture
def _require_docker():
    """tests/test_k6_830.py needs_docker: skip unless a Docker daemon answers."""
    import shutil
    import subprocess
    if shutil.which("docker") is None or subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        pytest.skip("docker is not available")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "docker: runs containers (k6 / Chrome); deselect on the shared host with "
                   "-m 'not docker', CI runs them (tests/test_k6_830.py needs_docker)")


@pytest.fixture(autouse=True)
def _stub_k8s_client_preflight(monkeypatch):
    """#320: cmd_check gate 8 / the cmd_phase6 pre-check call
    cluster._k8s_init(), which loads a real kubeconfig — unit tests must
    never do that (and a successful init leaks K8S_CLIENT_AVAILABLE=True
    across tests). Stub the GATE; tests of the gate itself bind the real
    function at module import time, and wiring tests re-patch explicitly.
    """
    import bench.cli as cli_mod
    monkeypatch.setattr(cli_mod, "_gate_k8s_client",
                        lambda: (True, "k8s_client: PASS (test stub)"))


@pytest.fixture(autouse=True)
def _stub_deploy_fingerprint(monkeypatch):
    """#320: _run_stage fingerprints the live snowplow Deployment around
    every stage (`kubectl get deploy -o json`). Unit tests must never shell
    real kubectl — stub the fingerprint to None (annotation fields become
    None / pod_rerolled_mid_stage=False). Tests that exercise the guard
    re-set `phases._snowplow_deploy_fingerprint` themselves; the real
    function is capturable at test-module import time (collection runs
    before fixtures).
    """
    import bench.phases as phases_mod
    monkeypatch.setattr(phases_mod, "_snowplow_deploy_fingerprint",
                        lambda: None)


@pytest.fixture(autouse=True)
def _stub_conv_discrimination_probes(monkeypatch):
    """Task #334a: browser_measure_stage's VERIFY block now snapshots
    `snowplow_refresher_completed_total` (HTTP /debug/vars) and fetches a
    bounded pod-log window (`kubectl logs --since-time`) to assemble the
    convergence-cell discrimination tuple. Unit tests must never reach the
    live cluster — neutralise BOTH seams so the tuple fails soft to all-None
    (the same path a cache-off pod takes).

    - `_snowplow_pod_log_window` is brand-new and only called from the VERIFY
      path → stub straight to None.
    - `read_snowplow_expvar_int` is a SHARED helper with its own dedicated
      unit tests (test_browser.py) that pass an explicit `base_url=` to drive
      a mocked transport. We delegate to the REAL function ONLY when a
      `base_url` is supplied (those unit tests); the VERIFY-path callers pass
      no base_url and get None. test_phases_s8bc re-binds the attribute
      explicitly and so overrides this wrapper entirely (fixtures apply
      before the test body).
    - `read_snowplow_expvar_map` (Task #217) is the map-valued sibling, called
      with no base_url from _run_stage's per-stage L1 snapshot. Same wrapper:
      delegate to the REAL function only when a base_url is supplied (its
      transport unit tests); base_url-less _run_stage callers get None, so the
      proof's l1_lookup_delta is None — the same path a cache-off pod takes.
    """
    import bench.browser as browser_mod
    _real_expvar = browser_mod.read_snowplow_expvar_int
    _real_expvar_map = browser_mod.read_snowplow_expvar_map

    def _expvar_or_none(key, *, base_url=None, timeout=10):
        if base_url is not None:
            return _real_expvar(key, base_url=base_url, timeout=timeout)
        return None

    def _expvar_map_or_none(key, *, base_url=None, timeout=10):
        if base_url is not None:
            return _real_expvar_map(key, base_url=base_url, timeout=timeout)
        return None

    monkeypatch.setattr(browser_mod, "read_snowplow_expvar_int",
                        _expvar_or_none)
    monkeypatch.setattr(browser_mod, "read_snowplow_expvar_map",
                        _expvar_map_or_none)
    monkeypatch.setattr(browser_mod, "_snowplow_pod_log_window",
                        lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _stub_cache_list_transport(monkeypatch):
    """#178: neutralise the post-VERIFY CONTENT cache-list call by default.

    browser_measure_stage's CONTENT check calls
    `list_composition_names_from_cache(token)` when `cache_mode=="ON"` and a
    token is set — which does a LIVE `http_get` → `urllib.request.urlopen` to
    the default snowplow URL. In a bench UNIT test there is no snowplow, so it
    blocks on `sock.connect` (http_get timeout=120 × retries=3 ≈ 6 min) and
    HANGS the whole suite (#178: test_browser_measure_stage_cyber_uses_intra_
    user_consistency was the one that reached it, via cache_mode="ON").

    Default to None — the CONTENT check treats None as "cache-list transport
    unavailable, skip", so no measure-stage unit test can make a live call.
    This mirrors the expvar / k8s / deploy-fingerprint autouse stubs above (no
    live network from a unit test). It is stubbed at
    `list_composition_names_from_cache` (NOT at `http_get`/`urlopen`) so the
    http_get transport unit tests — which fake urlopen and call http_get with
    base_url=None — are NOT blinded.

    A test that DOES exercise the CONTENT-match path re-monkeypatches
    `list_composition_names_from_cache` explicitly; fixtures apply before the
    test body, so the per-test setattr wins over this default (see the Task
    #181 / #298 CONTENT tests, which are unaffected).
    """
    import bench.browser as browser_mod
    monkeypatch.setattr(browser_mod, "list_composition_names_from_cache",
                        lambda token: None)


@pytest.fixture
def reset_k8s_state():
    """Reset bench.cluster k8s-client module globals between tests.

    Use this fixture in any test that monkey-patches `_k8s_init`,
    flips `K8S_CLIENT_AVAILABLE`, or sets the API client globals. Without
    it, mutations leak between cases.

    Risk Register §I R1.1 mitigation.
    """
    import bench.cluster as cluster_mod
    snapshot = {
        "K8S_CLIENT_AVAILABLE": cluster_mod.K8S_CLIENT_AVAILABLE,
        "_k8s_core": cluster_mod._k8s_core,
        "_k8s_rbac": cluster_mod._k8s_rbac,
        "_k8s_custom": cluster_mod._k8s_custom,
        "_k8s_apiext": cluster_mod._k8s_apiext,
        "_k8s_init_attempted": cluster_mod._k8s_init_attempted,
    }
    yield cluster_mod
    cluster_mod.K8S_CLIENT_AVAILABLE = snapshot["K8S_CLIENT_AVAILABLE"]
    cluster_mod._k8s_core = snapshot["_k8s_core"]
    cluster_mod._k8s_rbac = snapshot["_k8s_rbac"]
    cluster_mod._k8s_custom = snapshot["_k8s_custom"]
    cluster_mod._k8s_apiext = snapshot["_k8s_apiext"]
    cluster_mod._k8s_init_attempted = snapshot["_k8s_init_attempted"]


@pytest.fixture
def mock_kubectl(monkeypatch):
    """Install a recording subprocess.run; yield the call log.

    Each entry in the log is a dict {argv, input, rc, stdout, stderr}.
    By default every call returns rc=0 with empty stdout/stderr; the test
    can pre-load `log.replies` (a list of (rc, stdout, stderr) tuples) to
    return scripted outputs.
    """
    import subprocess as subprocess_mod

    class _Log:
        def __init__(self):
            self.calls: list[dict] = []
            self.replies: list[tuple[int, str, str]] = []

    log = _Log()

    def _fake_run(argv, **kwargs):
        rc, stdout, stderr = (0, "", "")
        if log.replies:
            rc, stdout, stderr = log.replies.pop(0)
        log.calls.append({
            "argv": list(argv),
            "input": kwargs.get("input"),
            "rc": rc,
            "stdout": stdout,
            "stderr": stderr,
        })

        class _FakeCompletedProcess:
            def __init__(self, rc, stdout, stderr):
                self.returncode = rc
                # subprocess.run with capture_output=True returns bytes
                # unless text=True; bench.cluster.kubectl uses bytes.
                if kwargs.get("text"):
                    self.stdout = stdout
                    self.stderr = stderr
                else:
                    self.stdout = stdout.encode() if isinstance(stdout, str) else stdout
                    self.stderr = stderr.encode() if isinstance(stderr, str) else stderr

        return _FakeCompletedProcess(rc, stdout, stderr)

    monkeypatch.setattr(subprocess_mod, "run", _fake_run)
    yield log


@pytest.fixture
def tmp_run_dir(tmp_path):
    """Yield a clean /tmp/snowplow-runs/{tag}/run-{ts}/-shaped path."""
    run_dir = tmp_path / "snowplow-runs" / "test-tag" / "run-20260602-test"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


# ─── Block 3: Playwright fake-page fixture ──────────────────────────────────


class FakePage:
    """Minimal duck-typed Playwright Page stand-in for unit tests.

    Records every call so tests can assert on invocation order. The DOM
    state is a dict the test pre-populates; `evaluate()` dispatches on
    JS-string prefix to a method that consults the dict.

    Field semantics:
      _skeleton_scoped / _skeleton_raw / _skeleton_widget — return values
        for _WIDGET_SKELETON_COUNT_JS evaluate().
      _call_count — return value for the resource-timing /call probe.
      _errored_count — return value for `.ant-result-error`.count().
      _ui_count — return value for the verify_composition_count_ui JS.
      _has_token — return value for the K_user localStorage probe.
      _nav_timing — dict returned by the navigation-timing JS.
      _waterfall — dict returned by the waterfall-levels JS.
    """

    def __init__(self, *, skeleton_scoped: int = 0, skeleton_raw: int = 0,
                 skeleton_widget: int = 0, call_count: int = 0,
                 errored_count: int = 0, ui_count: int = -1,
                 has_token: bool = True,
                 rendered_cards: int = 0,
                 nav_timing: dict | None = None,
                 waterfall: dict | None = None,
                 url: str = "http://fake/login"):
        self._skeleton_scoped = skeleton_scoped
        self._skeleton_raw = skeleton_raw
        self._skeleton_widget = skeleton_widget
        # Task #284: return value for _RENDERED_COMP_CARDS_JS — the count of
        # composition cards the datagrid rendered on page 1 at nav-time.
        self._rendered_cards = rendered_cards
        self._call_count = call_count
        self._errored_count = errored_count
        self._ui_count = ui_count
        self._has_token = has_token
        self._nav_timing = nav_timing or {"domContentLoaded": 100,
                                          "loadComplete": 200}
        self._waterfall = waterfall or {"callCount": call_count,
                                        "waterfallMs": 0,
                                        "calls": [], "levels": []}
        self.url = url
        self.evaluate_log: list[str] = []
        self.goto_log: list[tuple] = []
        self.screenshot_log: list[str] = []
        self.response_listeners: list = []

    def evaluate(self, js, *args):
        self.evaluate_log.append(js)
        # Match by canonical prefix snippets — keep the heuristic robust
        # to whitespace tweaks in the JS literals.
        # Task #284 rendered-card count: the JS takes a `namePrefix` arg and
        # returns matched.size. Match on the distinctive `matched.size` /
        # `createTreeWalker` shape BEFORE the skeleton branch (the skeleton JS
        # also uses createTreeWalker-free querySelectorAll, so no overlap, but
        # be explicit to keep the dispatch order-robust).
        if "matched.size" in js and "namePrefix" in js:
            return self._rendered_cards
        if "EXCLUDED_ANCESTORS" in js:
            return {"scoped": self._skeleton_scoped,
                    "raw": self._skeleton_raw,
                    "widget_scoped": self._skeleton_widget}
        # Order-sensitive: the verify_composition_count_ui JS contains both
        # `K_user` AND `dashboard-compositions-panel-row-piechart`. Match the
        # more-specific UI-count probe FIRST so the login-token branch does
        # not eat the verify call.
        if "dashboard-compositions-panel-row-piechart" in js:
            return self._ui_count
        if "K_user" in js:
            return self._has_token
        if "performance.clearResourceTimings" in js:
            return None
        if "filter(e => e.name.includes('/call')).length" in js:
            return self._call_count
        if "GAP_MS" in js and "calls.length" in js:
            return dict(self._waterfall, callCount=self._call_count)
        if "navigation" in js and "loadEventEnd" in js:
            return self._nav_timing
        if "scrollIntoView" in js:
            return True
        return None

    def goto(self, url, **kwargs):
        self.goto_log.append((url, kwargs))
        self.url = url

    def click(self, selector, **kwargs):
        # Behavioural no-op for login flow.
        return None

    def wait_for_timeout(self, ms):
        return None

    def wait_for_load_state(self, state, **kwargs):
        return None

    def screenshot(self, path: str, **kwargs):
        self.screenshot_log.append(path)
        # Touch the file so callers that stat() the path see a file exists.
        try:
            from pathlib import Path
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_bytes(b"\x89PNG\r\n\x1a\n")
        except Exception:
            pass

    def on(self, event, handler):
        self.response_listeners.append((event, handler))

    def remove_listener(self, event, handler):
        self.response_listeners = [
            (e, h) for (e, h) in self.response_listeners
            if not (e == event and h is handler)
        ]

    # keyboard.type and locator are used by login + validation paths.
    @property
    def keyboard(self):
        class _KB:
            def type(self, text, delay=0):
                return None
        return _KB()

    def locator(self, selector):
        page = self

        class _Locator:
            def count(self_inner):
                if selector == ".ant-result-error":
                    return page._errored_count
                return 0
        return _Locator()


@pytest.fixture
def fake_page():
    """Yield a FakePage with sensible defaults (no skeletons, 16 /calls,
    valid token, dashboard-shaped UI count).
    """
    return FakePage(
        skeleton_scoped=0,
        skeleton_raw=0,
        skeleton_widget=0,
        call_count=16,
        errored_count=0,
        ui_count=0,
        has_token=True,
    )
