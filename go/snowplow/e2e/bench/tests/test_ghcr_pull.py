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
from tests.conftest import FAKE_GHCR_TOKEN, FAKE_GHCR_USER
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
    assert "=== step" not in p.stderr and not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl")]
    assert FAKE_GHCR_TOKEN not in p.stdout + p.stderr


# ─── the preflight: before any cluster exists ───────────────────────────────

@pytest.mark.parametrize("case,why", [
    ("bad_token", "HTTP 401"),
    ("missing_chart", "agentgateway-controller:9.9.9: HTTP 404"),
])
def test_a_token_that_does_not_resolve_the_private_charts_is_refused_before_up(harness, case, why):  # noqa: F811
    env, state, ambient, tmp = harness
    if case == "bad_token":
        env = dict(env, GHCR_PULL_ENV_FILE=str(_file(tmp, f"GHCR_PULL_USERNAME={FAKE_GHCR_USER}\n"
                                                         "GHCR_PULL_TOKEN=revoked\n")))
    else:
        env = dict(env, FAKE_AGW_VERSION="9.9.9")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0 and "registry FAILED" in out and why in out
    assert "=== step registry ===" in out and "=== step up ===" not in out
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"]
                   for c in _calls(state))
    assert "revoked" not in out and FAKE_GHCR_TOKEN not in out


# ─── the install: 057's registryAuth, the Secret from stdin, nothing kept ───

def test_the_install_carries_057s_registry_auth_and_the_token_reaches_only_the_secret(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    # the preflight resolved both private charts at their pins
    pf = json.loads((tmp / "out" / "ghcr-pull-preflight.json").read_text())
    assert sorted((c["chart"].rsplit("/", 1)[1], c["version"], c["error"]) for c in pf["charts"]) == [
        ("agentgateway-controller", "0.2.0", None), ("agentgateway-policies", "0.2.18", None)]
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
    helm = calls[i_install]["argv"]
    fs = [helm[i + 1] for i, x in enumerate(helm) if x == "-f"]
    assert fs[0].endswith("/out/installer-values.yaml") and not os.path.exists(fs[1])   # private, then deleted
    # the token never in argv or the run's output
    assert not any(FAKE_GHCR_TOKEN in x for c in calls for x in c["argv"])
    assert FAKE_GHCR_TOKEN not in out


def test_nothing_under_the_run_dir_carries_the_token_or_the_username(harness):  # noqa: F811
    """Diagnostics, collect, the logs, the evaluate report: every file the run
    left, gzip members included, is free of the token (and of the pull
    account's name)."""
    env, state, ambient, tmp = harness
    env = dict(env, FAKE_INSTALLER_READY="0")      # the failure path: diagnostics are taken too
    subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True, timeout=300)
    files = [f for f in (tmp / "out").rglob("*") if f.is_file()]
    assert any("diagnostics" in str(f) for f in files)
    for f in files:
        raw = f.read_bytes()
        if f.suffix == ".gz":
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass
        assert FAKE_GHCR_TOKEN.encode() not in raw, f
        assert FAKE_GHCR_USER.encode() not in raw, f


# ─── the module ─────────────────────────────────────────────────────────────

def test_the_private_pins_are_the_repo_overrides_of_the_enabled_features(tmp_path):
    root = tmp_path / "installer"
    (root / "files").mkdir(parents=True)
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "snowplow", "version": "1.12.56"},                                  # public: no repo
        {"name": "agentgateway-controller", "version": "0.2.0", "repo": "oci://ghcr.io/krateo-agentiko/charts",
         "feature": "agentGateway"},
        {"name": "autopilot", "version": "1.0.0", "repo": "oci://ghcr.io/krateo-agentiko/charts",
         "feature": "coreAgents"}]}))
    (root / "values.yaml").write_text(yaml.safe_dump({"features": {"agentGateway": False, "coreAgents": False}}))
    v = tmp_path / "v.yaml"
    v.write_text(yaml.safe_dump({"features": {"agentGateway": True}}))
    assert [p["name"] for p in ghcr_pull.private_pins(str(tmp_path), str(v))] == ["agentgateway-controller"]


def test_check_never_names_the_token(tmp_path):
    f = _file(tmp_path, GOOD)
    assert ghcr_pull.check(str(f)) is None
    assert ghcr_pull.creds(str(f)) == (FAKE_GHCR_USER, FAKE_GHCR_TOKEN)
    f.chmod(0o640)
    assert FAKE_GHCR_TOKEN not in ghcr_pull.check(str(f))
