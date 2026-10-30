"""Run 19 died at the platform step: the agentgateway charts the portal's
gateway needs are PRIVATE (oci://ghcr.io/krateo-agentiko/charts), and their
CompositionDefinitions stayed Synced=False on `401 unauthorized`. The bench
now installs with a read-only pull token, exactly as 057 does (installer
registryAuth), from an operator-written file (mode 0600). Without the file
the run is refused before any cloud call. A token that does not resolve the
private charts at their pinned versions is refused before any cluster exists.
The token never reaches argv, a log, the run dir or a diagnostic.

The suite never reads the operator's file nor reaches ghcr.io: conftest gives
every run606 a fake file and a fake registry (FAKE_GHCR_*)."""

from __future__ import annotations

import gzip
import json
import os
import subprocess

import pytest
import yaml

from bench import ghcr_pull
from tests.conftest import FAKE_GHCR_TOKEN, FAKE_GHCR_USER, fake_bearer
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401


def _file(tmp_path, text, mode=0o600, name="ghcr-pull.env"):
    f = tmp_path / name
    f.write_text(text)
    f.chmod(mode)
    return f


GOOD = f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\nGHCR_PULL_TOKEN={FAKE_GHCR_TOKEN}\n"


# ─── the file: refused before any cloud call ────────────────────────────────

@pytest.mark.parametrize("case,why", [
    ("missing", "does not exist"),
    ("mode", "has mode 0644, not 0600"),
    ("no_token", "lacks GHCR_PULL_TOKEN"),
    ("no_user", "lacks GHCR_PULL_USERNAME"),
    ("empty", "lacks GHCR_PULL_USERNAME and GHCR_PULL_TOKEN"),
])
def test_without_a_usable_token_file_the_run_is_refused_before_any_cloud_call(harness, case, why):  # noqa: F811
    env, state, ambient, tmp = harness
    f = {"missing": lambda: tmp / "nope.env",
         "mode": lambda: _file(tmp, GOOD, 0o644),
         "no_token": lambda: _file(tmp, f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\n"),
         "no_user": lambda: _file(tmp, f"GHCR_PULL_TOKEN={FAKE_GHCR_TOKEN}\n"),
         "empty": lambda: _file(tmp, "GHCR_PULL_USERNAME=\nGHCR_PULL_TOKEN=\n")}[case]()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, GHCR_PULL_ENV_FILE=str(f)),
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 2
    assert f"refusing: the ghcr pull token: {f} {why}" in p.stderr
    assert "read:packages" in p.stderr and "chmod 600" in p.stderr      # what to create
    assert f"mkdir -p {f.parent} " in p.stderr and "~/.config" not in p.stderr   # where: the override's own dir
    assert "=== step" not in p.stderr and not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl")]
    assert FAKE_GHCR_TOKEN not in p.stdout + p.stderr


# ─── the preflight: before any cluster exists ───────────────────────────────

def _leaks():
    """Every form of the credentials a leak could take: the token, the
    username, base64(user:token) (the Basic header), base64(token), and the
    bearer the registry exchanged the token for."""
    import base64
    return {"token": FAKE_GHCR_TOKEN, "username": FAKE_GHCR_USER,
            "basic": base64.b64encode(f"{FAKE_GHCR_USER}:{FAKE_GHCR_TOKEN}".encode()).decode(),
            "b64token": base64.b64encode(FAKE_GHCR_TOKEN.encode()).decode(),
            "bearer": fake_bearer(FAKE_GHCR_TOKEN)}


@pytest.mark.parametrize("case,why", [
    ("bad_token", "HTTP 401"),
    ("missing_chart", "agentgateway-controller:9.9.9: HTTP 404"),
    ("scope401", "krateo-agentiko/charts/agentgateway-controller:0.2.0: HTTP 401"),   # #1007 F3: under-scoped
    ("scope403", "krateo-agentiko/charts/agentgateway-controller:0.2.0: HTTP 403"),
    ("network", "Connection refused"),
    ("http", "is not https: the token is never sent in clear text"),                 # F6
    ("redirect", "redirect to https://elsewhere.invalid/token refused"),
    ("core_feature", "krateo-agentiko/charts/incident-agent:9.9.9: HTTP 404"),        # F5: an always-on feature
])
def test_a_token_that_does_not_resolve_the_charts_is_refused_before_up(harness, case, why):  # noqa: F811
    env, state, ambient, tmp = harness

    def token_file(tok):
        return str(_file(tmp, f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\nGHCR_PULL_TOKEN={tok}\n"))
    env = dict(env, **{
        "bad_token": lambda: {"GHCR_PULL_ENV_FILE": token_file("revoked")},
        "missing_chart": lambda: {"FAKE_AGW_VERSION": "9.9.9"},
        "scope401": lambda: {"GHCR_PULL_ENV_FILE": token_file("scope401")},
        "scope403": lambda: {"GHCR_PULL_ENV_FILE": token_file("scope403")},
        "network": lambda: {"GHCR_API_URL": "https://127.0.0.1:1"},
        "http": lambda: {"GHCR_API_URL": env["GHCR_API_URL"].replace("https://", "http://")},
        "redirect": lambda: {"GHCR_PULL_ENV_FILE": token_file("redirect")},
        "core_feature": lambda: {"FAKE_CORE_FEATURES": "specialistAgents"},
    }[case]())
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0 and "registry FAILED" in out and why in out, out[-2000:]
    assert "=== step registry ===" in out and "=== step up ===" not in out
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"]
                   for c in _calls(state))
    for what, v in _leaks().items():
        assert v not in out or what == "username", what
    assert "revoked" not in out and "scope40" not in out


# ─── the install: 057's registryAuth, the Secret from stdin, nothing kept ───

def test_the_install_carries_057s_registry_auth_and_the_token_reaches_only_the_secret(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    # the preflight resolved every repo-override chart of an enabled component (#1007 F5): the
    # private agentgateway pair, the public blueprints (one featureless); never the off feature's
    pf = json.loads((tmp / "out" / "ghcr-pull-preflight.json").read_text())
    assert sorted((c["chart"].rsplit("/", 1)[1], c["version"], c["error"]) for c in pf["charts"]) == [
        ("agentgateway-controller", "0.2.0", None), ("agentgateway-policies", "0.2.18", None),
        ("github-provider-kog", "0.6.0", None), ("krateo-marketplace-catalog", "0.1.2", None)]
    # registryAuth: 057's fields (installer-platform values, read-only), the username not in the record
    vals = yaml.safe_load((tmp / "out" / "installer-values.yaml").read_text())
    assert vals["registryAuth"] == {"enabled": True, "imagePullSecretName": "", "insecureSkipVerifyTLS": False,
                                    "registries": [], "passwordRef": {"name": "krateo-agentiko-pull",
                                                                       "namespace": "krateo-system", "key": "token"}}
    # the Secret: created from stdin, before the install, with exactly the token
    sec = json.loads((state / "created-secret").read_text())
    assert sec["stdin"] == FAKE_GHCR_TOKEN and sec["ns"] == "krateo-system"
    assert sec["argv"][:4] == ["create", "secret", "generic", "krateo-agentiko-pull"]
    assert "--from-file=token=/dev/stdin" in sec["argv"]
    calls = _calls(state)
    i_secret = next(i for i, c in enumerate(calls) if c["tool"] == "kubectl" and "secret" in c["argv"])
    i_install = next(i for i, c in enumerate(calls) if c["tool"] == "helm" and "install" in c["argv"])
    assert i_secret < i_install
    # the username: piped into helm's `-f -` (#1007 F2), no file: helm received it
    helm = calls[i_install]["argv"]
    assert [helm[i + 1] for i, x in enumerate(helm) if x == "-f"][1] == "-"
    assert json.loads((state / "helm-registry-auth.json").read_text())["username"] == FAKE_GHCR_USER
    # no credential form in argv or the run's output
    for what, v in _leaks().items():
        assert not any(v in x for c in calls for x in c["argv"]), what
        assert v not in out, what


def test_nothing_under_the_run_dir_carries_any_form_of_the_credentials(harness):  # noqa: F811
    """Diagnostics, collect, the logs, the evaluate report: every file the run
    left, gzip members included, is free of the token, the pull account's
    name, base64(user:token), base64(token) and the registry's bearer. The
    failure path takes diagnostics, and the fake Installer CR carries the
    registryAuth helm installed with, username included, as installer
    0.3.468's self-bootstrap.yaml copies it (#1007 F1)."""
    env, state, ambient, tmp = harness
    env = dict(env, FAKE_INSTALLER_READY="0")
    subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    assert json.loads((state / "helm-registry-auth.json").read_text())["username"] == FAKE_GHCR_USER
    files = [f for f in (tmp / "out").rglob("*") if f.is_file()]
    inst = tmp / "out" / "diagnostics" / "installer-cr.json"
    assert inst.exists()
    ra = json.loads(inst.read_text())[0]["spec"]["registryAuth"]
    assert ra["username"] == "<redacted>" and ra["passwordRef"]["name"] == "krateo-agentiko-pull"
    for f in files:
        raw = f.read_bytes()
        if f.suffix == ".gz":
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass
        for what, v in _leaks().items():
            assert v.encode() not in raw, (what, f)


def test_diag606_redacts_a_registry_username_by_key_at_any_depth():
    from bench import diag606
    obj = {"spec": {"registryAuth": {"username": "u1", "registries": [{"username": "u2", "repo": "r"}],
                                     "passwordRef": {"name": "n"}},
                    "chart": {"credentials": {"username": "u3", "passwordRef": {"name": "n"}}},
                    "owner": {"username": "kept"}}}
    r = diag606.redact_credentials(obj)
    assert r["spec"]["registryAuth"]["username"] == "<redacted>"
    assert r["spec"]["registryAuth"]["registries"][0] == {"username": "<redacted>", "repo": "r"}
    assert r["spec"]["chart"]["credentials"]["username"] == "<redacted>"
    assert r["spec"]["owner"]["username"] == "kept"               # by KEY under a credential, not everywhere


def test_diag606_collect_redacts_each_layer_on_its_own(tmp_path):
    """The bundle, through the real Kube and collect(), with each layer the
    only one that can catch its leak (#1007 F1):
    - by key: the Installer CR's registryAuth and a CompositionDefinition's
      chart.credentials name an account that is NOT the pull file's, so no
      literal can redact it;
    - as a literal: a condition message names the pull file's account and
      token, under no credential key."""
    import subprocess as sp
    from bench import diag606
    inst = {"apiVersion": "composition.krateo.io/v1", "kind": "Installer",
            "metadata": {"name": "installer", "namespace": "krateo-system"},
            "spec": {"registryAuth": {"enabled": True, "username": "by-key-only-account",
                                      "passwordRef": {"name": "krateo-agentiko-pull"}}},
            "status": {"conditions": [{"type": "Ready", "status": "False", "reason": "Creating",
                                       "message": f"pull as {FAKE_GHCR_USER} with {FAKE_GHCR_TOKEN} failed"}]}}
    cd = {"apiVersion": "core.krateo.io/v1alpha1", "kind": "CompositionDefinition",
          "metadata": {"name": "agentgateway-controller", "namespace": "krateo-system"},
          "spec": {"chart": {"url": "oci://ghcr.io/krateo-agentiko/charts/agentgateway-controller",
                             "credentials": {"username": "by-key-only-cd-account",
                                             "passwordRef": {"name": "krateo-agentiko-pull"}}}}}

    def run(argv, **kw):
        a = " ".join(argv)
        if " get installers." in a:
            out = {"items": [inst]}
        elif " get compositiondefinitions" in a:
            out = {"items": [cd]}
        else:
            return sp.CompletedProcess(argv, 1, "", "not served")
        return sp.CompletedProcess(argv, 0, json.dumps(out), "")

    k = diag606.Kube("kc", "gke_p_europe-west4_snowplow-bench-x", run=run)
    diag606.collect(k, str(tmp_path / "diag"), log=lambda *_: None)
    files = [f for f in (tmp_path / "diag").rglob("*") if f.is_file()]
    assert any(f.name == "installer-cr.json" for f in files)
    for f in files:
        raw = f.read_text()
        for leak in ("by-key-only-account", "by-key-only-cd-account", FAKE_GHCR_USER, FAKE_GHCR_TOKEN):
            assert leak not in raw, (leak, f.name)


# ─── the module ─────────────────────────────────────────────────────────────

def test_the_pins_are_every_repo_override_the_installer_turns_on(tmp_path):
    """By inst.featureEnabled: a pin with no feature is on, a core feature is
    always on, else the install's features (#1007 F5)."""
    root = tmp_path / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 0.3.468\n")
    (root / "templates" / "_helpers.tpl").write_text(
        '{{- define "inst.coreFeatures" -}}codegenAgents{{- end -}}\n')
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "snowplow", "version": "1.12.56"},                                  # no repo of its own
        {"name": "agentgateway-controller", "version": "0.2.0", "repo": "oci://ghcr.io/krateo-agentiko/charts",
         "feature": "agentGateway"},
        {"name": "github-provider-kog", "version": "0.6.0", "repo": "oci://ghcr.io/krateo-blueprints/charts"},
        {"name": "codegen", "version": "1.0.0", "repo": "oci://ghcr.io/krateo-agentiko/charts",
         "feature": "codegenAgents"},
        {"name": "autopilot", "version": "1.0.0", "repo": "oci://ghcr.io/krateo-agentiko/charts",
         "feature": "coreAgents"}]}))
    (root / "values.yaml").write_text(yaml.safe_dump({"features": {"agentGateway": False, "coreAgents": False,
                                                                   "codegenAgents": False}}))
    v = tmp_path / "v.yaml"
    v.write_text(yaml.safe_dump({"features": {"agentGateway": True}}))
    assert [p["name"] for p in ghcr_pull.private_pins(str(tmp_path), str(v))] == [
        "agentgateway-controller", "github-provider-kog", "codegen"]


def test_check_never_names_the_token(tmp_path):
    f = _file(tmp_path, GOOD)
    assert ghcr_pull.check(str(f)) is None
    assert ghcr_pull.creds(str(f)) == (FAKE_GHCR_USER, FAKE_GHCR_TOKEN)
    f.chmod(0o640)
    assert FAKE_GHCR_TOKEN not in ghcr_pull.check(str(f))
