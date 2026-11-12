"""Run 26d (frontend 1.8.40 on every arm): 0 of 5 browser sessions signed in. Every VU logged
`filling "#basic_username" with "scaleuser-0001": timed out after 30s`: frontend#659 renamed the
antd login form ('basic' -> `login_<kind>[_<name>]`, ui/src/pages/Login/LoginForm/LoginForm.tsx),
and the run measured on for hours with no browser session.

  - k6/browser.js signs in through the contract (pageload.LOGIN = login_contract.SELECTORS):
    #login_basic_username / #login_basic_password in form#login_basic, submitted by that form's
    "Sign in" button. No fallback to the old ids.
  - run606.sh's login-contract step refuses, before any cluster, a frontend older than the first
    release carrying #659 (1.8.36), read from the installer's pin or FRONTEND_TAG, by name.
  - browser.js's sign-in gate: no session signed in within its bound -> k6 exits 99 (and aborts a
    test that would run on); run606.sh's measure step fails at once when a driver's run ends with a
    non-zero exit while another still drives (26d's browser ended in its first minute, rc 0, and
    nothing looked at it until k6load's whole drive was over).
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

import pytest
import yaml

from bench import k6_job, k6eval, login_contract
from tests.test_606 import _run606
from tests.test_k6_830 import _check, _run606_k6, needs_docker
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _launch, pairbed  # noqa: F401  (fixture)
from tests.test_run15_customer_latency import _calls as _pair_calls

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")
FD = "sha256:" + "1a" * 32
FIXTURE = BENCH_DIR / "tests" / "fixtures" / "k6_browser_fixture.py"


# ─── the contract and the version refusal (pure) ────────────────────────────


def test_the_first_contract_release_is_1_8_36():
    """Read from the frontend repository (module doc of bench/login_contract.py): #659's merge
    commit is in 1.8.36 .. 1.8.40 and not in 1.8.35."""
    assert login_contract.FIRST_RELEASE == "1.8.36" and login_contract.CONTRACT_PR == "#659"
    assert login_contract.SELECTORS == {"form": "form#login_basic", "username": "#login_basic_username",
                                        "password": "#login_basic_password", "submit": "Sign in"}


@pytest.mark.parametrize("tag", ["1.8.36", "1.8.40", "1.9.0", "1.10.1", "2.0.0"])
def test_a_contract_release_is_accepted(tag):
    assert login_contract.check(tag, "FRONTEND_TAG_A") is None


@pytest.mark.parametrize("tag", ["1.8.35", "1.8.34", "1.8.4", "1.7.99", "0.9.100"])
def test_an_older_release_is_refused_by_name(tag):
    """Numbers, not strings: 1.8.4 predates 1.8.36 (as a string it sorts after it)."""
    assert login_contract.check(tag, "FRONTEND_TAG_A") == (
        f"frontend {tag} predates the login form contract #659 (first in 1.8.36): set FRONTEND_TAG_A ≥ 1.8.36")


@pytest.mark.parametrize("tag", ["1.8.36-rc1", "latest", "sha-e46198b", "v1.8.40", ""])
def test_a_tag_that_is_not_a_release_is_refused(tag):
    why = login_contract.check(tag, "FRONTEND_TAG")
    assert why.startswith(f"frontend {tag} is not a release version (X.Y.Z)") and "set FRONTEND_TAG ≥ 1.8.36" in why


def _installer(tmp_path, *, pin="1.8.34", inst_tag=None, run_tag=None, frontend=True):
    """installer-pulled/installer as 0.3.545 has it: frontend pinned with no repo of its own."""
    root = tmp_path / "pulled" / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 0.3.545\n")
    (root / "templates" / "_helpers.tpl").write_text('{{- define "inst.coreFeatures" -}}{{- end -}}\n')
    comps = [{"name": "snowplow", "version": "1.12.69", "feature": "portal"}]
    if frontend:
        comps.append({"name": "frontend", "kind": "Frontend", "chart": "frontend", "version": pin,
                      "feature": "portal"})
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": comps}))
    iv = {"ociRepo": "oci://ghcr.io/krateo-platformops/charts", "features": {"portal": True},
          "componentValues": {"frontend": {"config": {"X": "y"}}}}
    if inst_tag:
        iv["componentValues"]["frontend"]["image"] = {"tag": inst_tag}
    (root / "values.yaml").write_text(yaml.safe_dump(iv))
    v = tmp_path / "values.yaml"
    v.write_text(yaml.safe_dump({"componentValues": {"frontend": {"image": {"tag": run_tag}}}} if run_tag else {}))
    return tmp_path / "pulled", v


def test_the_default_frontend_is_the_installers_pin(tmp_path):
    d, v = _installer(tmp_path)
    assert login_contract.frontend_tag(str(d), str(v)) == {
        "tag": "1.8.34", "source": "the installer's frontend pin (its chart's appVersion)", "pin": "1.8.34"}


def test_an_override_is_the_image_the_install_renders(tmp_path):
    d, v = _installer(tmp_path, run_tag="1.8.40")
    assert login_contract.frontend_tag(str(d), str(v))["tag"] == "1.8.40"
    # the installer's own componentValues override the pin as well, and the run's override wins over them
    d, v = _installer(tmp_path / "i", inst_tag="1.8.33")
    assert login_contract.frontend_tag(str(d), str(v))["tag"] == "1.8.33"
    d, v = _installer(tmp_path / "r", inst_tag="1.8.33", run_tag="1.8.38")
    assert login_contract.frontend_tag(str(d), str(v))["tag"] == "1.8.38"


def _cli(d, v, out, var="FRONTEND_TAG"):
    return subprocess.run([sys.executable, "-m", "bench.login_contract", "check", "--installer-pulled", str(d),
                           "--values", str(v), "--var", var, "--out", str(out)],
                          cwd=BENCH_DIR, capture_output=True, text=True, timeout=60)


def test_the_cli_refuses_and_records(tmp_path):
    d, v = _installer(tmp_path)
    p = _cli(d, v, tmp_path / "lc.json", var="FRONTEND_TAG_N1")
    assert p.returncode == 2
    assert ("refusing: frontend 1.8.34 predates the login form contract #659 (first in 1.8.36): "
            "set FRONTEND_TAG_N1 ≥ 1.8.36") in p.stderr
    rec = json.loads((tmp_path / "lc.json").read_text())
    assert rec["ok"] is False and rec["tag"] == "1.8.34" and rec["first_release"] == "1.8.36"
    d, v = _installer(tmp_path / "ok", run_tag="1.8.40")
    p = _cli(d, v, tmp_path / "ok.json")
    assert p.returncode == 0, p.stderr
    assert json.loads((tmp_path / "ok.json").read_text())["ok"] is True
    # an install with no frontend has no login form: refused, not passed
    d, v = _installer(tmp_path / "none", frontend=False)
    p = _cli(d, v, tmp_path / "none.json")
    assert p.returncode == 2 and "this install runs no frontend" in p.stderr
    # the variable named in the refusal is a frontend variable only
    p = _cli(d, v, tmp_path / "x.json", var="PATH")
    assert p.returncode == 2 and "FRONTEND_TAG or FRONTEND_TAG_<run>" in p.stderr


# ─── browser.js: the contract's selectors, no fallback ──────────────────────


_PAGELOAD_DRIVER = """
import * as pl from './pageload.mjs'
const cfg = { pageTimeoutMs: 3000, signinTimeoutMs: 2000 }
console.log(JSON.stringify({ login: pl.LOGIN, bound: pl.signinBoundMs(cfg, 6, 5000, 10000),
                             bound1: pl.signinBoundMs(cfg, 1, 5000, 10000),
                             dflt: pl.signinBoundMs({ pageTimeoutMs: 3000 }, 1, 0, 0) }))
"""


@needs_node
def test_pageload_login_is_the_contract_and_the_bound_is_the_scripts_own(tmp_path):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_PAGELOAD_DRIVER)
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs")], capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout)
    assert out["login"] == login_contract.SELECTORS
    # 6 navigations of 3 s, 5 s apart; 2 fills + the click; the arrival (3 + 2 s); its record (10 s)
    assert out["bound"] == 6 * 3000 + 5 * 5000 + 3 * 3000 + 3000 + 2000 + 10000
    assert out["bound1"] == 3000 + 3 * 3000 + 3000 + 2000 + 10000
    assert out["dflt"] == 3000 + 3 * 3000 + 3000 + 60000          # the arrival's default signinTimeoutMs


def test_browser_js_signs_in_through_the_contract_only():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    body = src[src.index("async function runClass"):]
    assert "#basic_" not in src and "press('Enter')" not in src
    assert "page.locator(LOGIN.username).fill(user, opts)" in body
    assert "page.locator(LOGIN.password).fill(CFG.password, opts)" in body
    # the submit is the "Sign in" button OF form#login_basic, never a page-wide button
    assert "const form = page.locator(LOGIN.form)" in body
    assert "const submit = form.getByRole('button', { name: LOGIN.submit, exact: true })" in body
    assert body.index("submit.click(opts)") < body.index("await settled(page, null)")
    for s in ("getByLabel", "autocomplete", "button[type"):
        assert s not in body, s


def test_browser_js_gate_is_the_only_threshold_that_can_fail():
    src = (BENCH_DIR / "k6" / "browser.js").read_text()
    th = src[src.index("thresholds: {"):src.index("summaryTrendStats")]
    assert ("browser_first_signins: [{ threshold: 'count>0', abortOnFail: true, "
            "delayAbortEval: `${Math.ceil(SIGNIN_BOUND_MS / 1000)}s` }]") in th
    assert th.count("'count>=0'") == 8 and th.count("abortOnFail") == 1
    body = src[src.index("export default async function"):]
    # every session's 0 comes first (an empty Counter is not judged while the test runs), its 1 on
    # its FIRST class's landed sign-in only
    assert body.index("SIGNED_IN.add(0, { session: s.name })") < body.index("runClass(")
    assert "if (k === 0) SIGNED_IN.add(1, { session: s.name })" in src
    assert "const SIGNIN_BOUND_MS = signinBoundMs(CFG, GOTO_TRIES, GOTO_WAIT_MS, LOAD_READ_MS)" in src


# ─── run606.sh: the refusal before any cluster ──────────────────────────────


def _dry(tmp_path, **env):
    return _run606(tmp_path, BENCH_606_DRY_RUN="1", **env)


def test_run606_refuses_the_installers_default_frontend_that_predates_the_contract(tmp_path):
    """Installer 0.3.545's own pin, 1.8.34, unset FRONTEND_TAG: refused by name, before the cluster."""
    p = _dry(tmp_path, FAKE_FRONTEND_PIN="1.8.34")
    assert p.returncode != 0
    want = ("login-contract FAILED: frontend 1.8.34 predates the login form contract #659 (first in 1.8.36): "
            "set FRONTEND_TAG ≥ 1.8.36")
    assert want in p.stderr, p.stderr[-3000:]
    log = p.stderr
    assert log.index("=== step source ===") < log.index("=== step login-contract ===")
    for later in ("cert-manager-source", "schema", "registry", "reap", "up"):
        assert f"=== step {later} ===" not in log, later
    assert "gke-up" not in p.stdout + p.stderr and "teardown" not in log
    fails = [json.loads(x) for x in (tmp_path / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert [f["step"] for f in fails] == ["login-contract"] and fails[0]["what"] == want


def test_run606_refuses_an_override_older_than_the_contract(tmp_path):
    p = _dry(tmp_path, FRONTEND_TAG="1.8.35", EXPECTED_FRONTEND_DIGEST=FD)
    assert p.returncode != 0 and "=== step reap ===" not in p.stderr
    assert ("frontend 1.8.35 predates the login form contract #659 (first in 1.8.36): set FRONTEND_TAG ≥ 1.8.36"
            in p.stderr)
    # launched by the pair as run N1: its own variable is named
    (tmp_path / "pair").mkdir()
    p = _dry(tmp_path / "pair", FRONTEND_TAG="1.8.35", EXPECTED_FRONTEND_DIGEST=FD, FRONTEND_TAG_VAR="FRONTEND_TAG_N1")
    assert "set FRONTEND_TAG_N1 ≥ 1.8.36" in p.stderr, p.stderr[-2000:]


def test_run606_an_override_carrying_the_contract_runs_on_an_older_pin(tmp_path):
    """Run 26d's own inputs: installer pin 1.8.34, FRONTEND_TAG=1.8.40: accepted, recorded."""
    p = _dry(tmp_path, FAKE_FRONTEND_PIN="1.8.34", FRONTEND_TAG="1.8.40", EXPECTED_FRONTEND_DIGEST=FD)
    assert p.returncode == 0, p.stderr[-3000:]
    assert ("login form contract #659: frontend 1.8.40 (componentValues.frontend.image.tag) ≥ 1.8.36: "
            "the browser signs in through form#login_basic") in p.stderr
    rec = json.loads((tmp_path / "out" / "login-contract.json").read_text())
    assert rec["ok"] is True and rec["tag"] == "1.8.40" and rec["pin"] == "1.8.34"


def test_run606_without_a_browser_does_not_read_the_contract(tmp_path):
    p = _dry(tmp_path, FAKE_FRONTEND_PIN="1.8.34", LOAD_DRIVER="load606")
    assert p.returncode == 0, p.stderr[-3000:]
    assert "login form contract: not checked (LOAD_DRIVER=load606 signs in through no UI)" in p.stderr
    assert not (tmp_path / "out" / "login-contract.json").exists()


def test_run606_refuses_a_bad_frontend_variable_name(tmp_path):
    p = _dry(tmp_path, FRONTEND_TAG_VAR="FRONTEND_TAG_A; rm")
    assert p.returncode == 2 and "FRONTEND_TAG_VAR must be FRONTEND_TAG or FRONTEND_TAG_<run>" in p.stderr


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _seen(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/__seen", timeout=5) as r:
        return json.loads(r.read())


def test_the_old_dom_is_refused_by_version_before_it_is_reached(tmp_path):
    """A portal serving the page BEFORE #659 (the old ids) runs; the run names the frontend as
    1.8.35. It is refused at the login-contract step, before any cluster and before any browser
    exists: the page is never requested. The fixture's own record is checked to read (a request
    made after the run is seen), so its empty record is a reading."""
    port = _free_port()
    srv = subprocess.Popen([sys.executable, "-u", str(FIXTURE), str(port), "300", "50", "100", "old"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                _seen(port)
                break
            except OSError:
                time.sleep(0.1)
        p = _dry(tmp_path, FRONTEND_TAG="1.8.35", EXPECTED_FRONTEND_DIGEST=FD)
        assert p.returncode != 0 and "predates the login form contract #659" in p.stderr
        assert "=== step up ===" not in p.stderr and "=== step measure ===" not in p.stderr
        assert _seen(port) == []
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/login", timeout=5) as r:
            page = r.read().decode()
        assert "DOM = 'old'" in page and 'id="basic_username"' in page          # it IS the old page
        assert _seen(port) == ["/login"]
    finally:
        srv.kill()
        srv.wait()


# ─── run606_pair.sh: each run's refusal names its own variable ──────────────


def test_the_pair_hands_each_run_its_frontend_variable(pairbed):  # noqa: F811
    repo, fake, env, tmp = pairbed
    f = repo / "run606.sh"
    f.write_text(f.read_text().replace(' pid=$$" >> "$FAKE/calls"', ' fev=${FRONTEND_TAG_VAR:-none} pid=$$" >> "$FAKE/calls"', 1))
    for c in (["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "fev"]):
        subprocess.run(["git", *c], cwd=repo, check=True, capture_output=True)
    p = _launch(repo, env, BENCH_606_DRY_RUN="1")
    assert p.returncode == 0, p.stderr[-2000:]
    calls = [c for c in _pair_calls(fake) if " dry=1 " in c]
    from tests.test_run15_customer_latency import NA, NB
    assert any(c.startswith(NA) and " fev=FRONTEND_TAG_A " in c for c in calls), calls
    assert any(c.startswith(NB) and " fev=FRONTEND_TAG_B " in c for c in calls), calls


# ─── run606.sh: the early gate in the measure step ──────────────────────────


def test_run606_stops_at_once_when_the_browser_ends_with_its_gate_while_k6_drives(harness):  # noqa: F811
    """26d's shape: the browser's run is over in its first minute while k6load drives on. Its
    exit 99 (browser.js's sign-in gate) fails the measure step at its next poll, by name, not at
    MEASURE_WAIT_S and not after a 4-hour drive."""
    p, calls, out = _run606_k6(harness, FAKE_DRIVER_RUNNING="k6load", FAKE_DRIVE_RC="99",
                               MEASURE_WAIT_S="40", DRIVE_POLL_S="1")
    log = (out / "run.log").read_text()
    assert p.returncode != 0
    assert ("measure FAILED: the k6browser driver's run ended while k6load still drives: k6browser-0=rc99: "
            "no browser session signed in within k6/browser.js's sign-in bound") in log, log[-3000:]
    assert "k6load not done within" not in log and "driver done" not in log
    fails = [json.loads(x) for x in (out / "command-failures.jsonl").read_text().splitlines()]
    assert any(f["step"] == "measure" and "k6browser-0=rc99" in f["what"] for f in fails)


def test_run606_a_browser_over_with_exit_0_is_not_an_early_failure(harness):  # noqa: F811
    """The control: the same early end with exit 0 is not judged here; the wait goes on to its
    deadline (the verdict judges the sessions)."""
    p, calls, out = _run606_k6(harness, FAKE_DRIVER_RUNNING="k6load", MEASURE_WAIT_S="8", DRIVE_POLL_S="1")
    log = (out / "run.log").read_text()
    assert "measure FAILED: k6load not done within 8s" in log, log[-3000:]
    assert "driver's run ended while" not in log


def test_run606_after_the_drive_a_non_zero_browser_exit_stays_the_verdicts(harness):  # noqa: F811
    """Once k6load's drive is over its exit is taken; a non-zero exit is logged and judged by the
    verdict, as before, never read as an early end."""
    p, calls, out = _run606_k6(harness, FAKE_DRIVE_RC="3")
    log = (out / "run.log").read_text()
    assert "driver FAILED: k6load-0=rc3" in log and "driver FAILED: k6browser-0=rc3" in log, log[-3000:]
    assert "driver's run ended while" not in log


# ─── browser.js in real Chrome (docker) ─────────────────────────────────────


def _browser(tmp_path, *, dom, signin_ms=300, sessions=2, duration_s=12, k6dir=None, timeout=240):
    """k6/browser.js in the pinned k6 browser image against the fixture SPA serving `dom`."""
    net = f"k6lc-{os.getpid()}-{dom}-{signin_ms}"
    spa = f"{net}-spa"
    out = tmp_path / "out"
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    cfg = k6_job.browser_config(
        sessions=[{"name": f"s{i}", "user": f"fixture-{i}", "offset": i, "cold": True} for i in range(1, sessions + 1)],
        routes=["/dashboard", "/compositions"], password="x", think_s=0.3, duration_s=duration_s,
        page_timeout_ms=3000, signin_timeout_ms=2000, frontend_url=f"http://{spa}:8080",
        snowplow_url=f"http://{spa}:8080")
    cfg.update(loginTries=1)
    (tmp_path / "cfg.json").write_text(json.dumps(cfg))
    subprocess.run(["docker", "network", "create", net], capture_output=True, check=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", spa, "--network", net,
                        "-v", f"{BENCH_DIR / 'tests' / 'fixtures'}:/f:ro", "python:3.12-slim",
                        "python3", "-u", "/f/k6_browser_fixture.py", "8080", str(signin_ms), "50", "100", dom],
                       capture_output=True, check=True)
        time.sleep(1)
        p = subprocess.run(
            ["docker", "run", "--rm", "--name", f"{net}-k6", "--network", net,
             "-v", f"{k6dir or BENCH_DIR / 'k6'}:/opt/k6:ro", "-v", f"{tmp_path / 'cfg.json'}:/cfg.json:ro",
             "-v", f"{out}:/out", "-e", "BROWSER_CONFIG=/cfg.json",
             "-e", "SUMMARY_FILE=/out/browser-summary.json", "-e", "K6_BROWSER_HEADLESS=true",
             "-e", "K6_BROWSER_ARGS=no-sandbox", k6_job.K6_BROWSER_IMAGE,
             "run", "--quiet", "--out", "json=/out/browser.json.gz", "/opt/k6/browser.js"],
            capture_output=True, text=True, timeout=timeout)
    finally:
        subprocess.run(["docker", "rm", "-f", spa, f"{net}-k6"], capture_output=True)   # a timed-out k6 too
        subprocess.run(["docker", "network", "rm", net], capture_output=True)
    (out / "browser-exit.json").write_text(json.dumps({"kind": "browser_exit", "rc": p.returncode}))
    return p, k6eval.read_k6_out(str(out), "browser")


def _first_signins(tmp_path):
    """The gate's own points, from k6's stream (the evaluator does not keep them)."""
    out = []
    with gzip.open(tmp_path / "out" / "browser.json.gz", "rt") as fh:
        for ln in fh:
            if '"metric":"browser_first_signins"' in ln:
                r = json.loads(ln)
                if r.get("type") == "Point":
                    out.append((r["data"]["tags"]["session"], r["data"]["value"]))
    return sorted(out)


@needs_docker
def test_in_chrome_the_1_8_40_page_signs_in_through_form_login_basic(tmp_path):
    """frontend 1.8.40's login page, an LDAPConfig's form first with the same labels: every
    session signs in through form#login_basic's "Sign in" (the LDAP button signs nobody in)."""
    p, o = _browser(tmp_path, dom="contract", sessions=3)          # the evaluator's bar is 3 sessions
    assert p.returncode == 0, (p.stdout + p.stderr)[-3000:]
    c = _check(k6eval.browser_checks(o, 3), "browser.sessions")
    assert c["result"] == "PASS" and c["value"] == 3
    assert _first_signins(tmp_path) == [("s1", 0), ("s1", 1), ("s2", 0), ("s2", 1), ("s3", 0), ("s3", 1)]
    assert "could not be filled" not in p.stdout + p.stderr


@needs_docker
def test_in_chrome_the_old_page_fails_every_sign_in_and_the_gate_says_so(tmp_path):
    """The page before #659, reached anyway (the version refusal bypassed): the contract ids are
    not there, every session's fill fails, named, without the identity; no session signs in, so
    the sign-in gate fails the test (k6 exit 99), where run 26d's exited 0."""
    p, o = _browser(tmp_path, dom="old")
    err = p.stdout + p.stderr
    assert p.returncode == 99, err[-3000:]
    assert "the login form form#login_basic (#login_basic_username, #login_basic_password) could not be filled" in err
    assert "thresholds on metrics 'browser_first_signins' have been crossed" in err      # judged at the test's end
    assert _first_signins(tmp_path) == [("s1", 0), ("s2", 0)]
    assert _check(k6eval.browser_checks(o, 2), "browser.sessions")["value"] == 0
    assert "fixture-1" not in err and "fixture-2" not in err and "<user>" in err      # no identity in the log


@needs_docker
def test_in_chrome_a_sign_in_that_never_lands_is_aborted_at_the_bound(tmp_path):
    """The form fills and submits but authn never answers: each session's arrival times out and,
    without the gate, it would walk routes for its whole duration (600 s here) signed out. The
    gate aborts the whole test at its bound (27 s with this config); the 240 s subprocess bound
    is what a missing abort would hit."""
    p, o = _browser(tmp_path, dom="contract", signin_ms=10**7, duration_s=600)
    err = p.stdout + p.stderr
    assert p.returncode == 99, err[-3000:]
    assert ("thresholds on metrics 'browser_first_signins' were crossed; at least one has abortOnFail enabled, "
            "stopping test prematurely") in err
    assert _first_signins(tmp_path) == [("s1", 0), ("s2", 0)]
    assert o["summary"] is not None                   # the summary is still written
