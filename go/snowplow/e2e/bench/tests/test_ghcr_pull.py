"""Run 19 died at the platform step: the agentgateway charts the portal's
gateway needs are PRIVATE (oci://ghcr.io/krateo-agentiko/charts), and their
CompositionDefinitions stayed Synced=False on `401 unauthorized`. The bench
now installs with a read-only pull token, exactly as 057 does (installer
registryAuth), from an operator-written file (mode 0600). Without the file
the run is refused before any cloud call. A token that does not resolve the
private charts at their pinned versions is refused before any cluster exists.
The token never reaches argv, a log, the run dir or a diagnostic.

Run 19 attempt 2 died at the platform step too: installer 0.3.468 renders its
derived image-pull Secret only with coreAgents or specialistAgents on, yet
injects its name into agentgateway-policies' private hop pods (ImagePullBackOff
for 43 min). The bench now creates its own dockerconfigjson Secret
(bench-ghcr-image-pull) from the same file and names it in
registryAuth.imagePullSecretName (the installer's documented opt-out); the
preflight also resolves every ghcr.io image the pinned charts' values name;
and the platform step names an image that cannot be pulled.

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
from tests.conftest import (FAKE_CHART_BAD_DIGEST, FAKE_CHART_BIG_MANIFEST, FAKE_CHART_BLOB_OVER_SIZE,
                            FAKE_CHART_DRIP, FAKE_CHART_HEADER_DRIP, FAKE_CHART_HOP_UNRENDERED, FAKE_CHART_HTTP_BLOB,
                            FAKE_GHCR_TOKEN, FAKE_GHCR_USER, fake_bearer)

HELM = os.environ["FAKE_HELM_TEMPLATE_BIN"]
AGW = "oci://ghcr.io/krateo-agentiko/charts"
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
    dcj = ghcr_pull.dockerconfigjson(FAKE_GHCR_USER, FAKE_GHCR_TOKEN)
    return {"token": FAKE_GHCR_TOKEN, "username": FAKE_GHCR_USER,
            "basic": base64.b64encode(f"{FAKE_GHCR_USER}:{FAKE_GHCR_TOKEN}".encode()).decode(),
            "b64token": base64.b64encode(FAKE_GHCR_TOKEN.encode()).decode(),
            "bearer": fake_bearer(FAKE_GHCR_TOKEN),
            "dockerconfigjson": dcj, "b64dockerconfigjson": base64.b64encode(dcj.encode()).decode()}


@pytest.mark.parametrize("case,why", [
    ("bad_token", "HTTP 401"),
    ("missing_chart", "agentgateway-controller:9.9.9: HTTP 404"),
    ("scope401", "krateo-agentiko/charts/agentgateway-controller:0.2.0: HTTP 401"),   # #1007 F3: under-scoped
    ("scope403", "krateo-agentiko/charts/agentgateway-controller:0.2.0: HTTP 403"),
    ("network", "Connection refused"),
    ("http", "is not https: the token is never sent in clear text"),                 # F6
    ("redirect", "redirect to https://elsewhere.invalid/token refused"),
    ("core_feature", "krateo-agentiko/charts/incident-agent:9.9.9: HTTP 404"),        # F5: an always-on feature
    ("image_scope", "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18: HTTP 403"),          # run 19b: the images
    # #1023 B: a chart whose images cannot be read is refused, never passed with none checked
    ("bad_digest", f"agentgateway-controller:{FAKE_CHART_BAD_DIGEST}: its images unreadable: "),
    ("http_blob", f"agentgateway-controller:{FAKE_CHART_HTTP_BLOB}: its images unreadable: "),
    # #1023 A: a private-image component (inst.privateImageComponents) must render, and render a ghcr image
    ("private_unrendered", "agentgateway-policies:0.2.18: the installer lists it as a private-image component "
                           "(inst.privateImageComponents) and it cannot be rendered: helm template failed: "),
    ("private_no_image", "agentgateway-policies:0.2.18: the installer lists it as a private-image component "
                         "(inst.privateImageComponents), yet no rendered pod running a ghcr.io image carries the "
                         "pull secret it injects at certReplayHop.image.pullSecrets, uiTokenHop.image.pullSecrets"),
    # #1041 review F4: cert-replay-hop alone cannot stand for a ui-token-hop that never rendered
    ("private_path_unrendered", "agentgateway-policies:1.1.1: the installer lists it as a private-image component "
                                "(inst.privateImageComponents), yet no rendered pod running a ghcr.io image carries "
                                "the pull secret it injects at uiTokenHop.image.pullSecrets"),
    # #1041 review F1: a private-image component with no repo of its own is checked too
    ("private_repoless", "ghcr.io/krateo-platformops/charts/alert-provider:0.2.53: the installer lists it as a "
                         "private-image component (inst.privateImageComponents) and it cannot be rendered"),
    ("private_rule_dynamic", "inst.privateImageComponents is not one static list"),
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
        "image_scope": lambda: {"GHCR_PULL_ENV_FILE": token_file("imgscope403")},
        "bad_digest": lambda: {"FAKE_AGW_VERSION": FAKE_CHART_BAD_DIGEST},
        "http_blob": lambda: {"FAKE_AGW_VERSION": FAKE_CHART_HTTP_BLOB},
        "private_unrendered": lambda: {"FAKE_HELM_TEMPLATE_FAIL": "agentgateway-policies"},
        "private_no_image": lambda: {"FAKE_HELM_TEMPLATE_EMPTY": "agentgateway-policies"},
        "private_path_unrendered": lambda: {"FAKE_POLICIES_VERSION": FAKE_CHART_HOP_UNRENDERED},
        "private_repoless": lambda: {"FAKE_HELM_TEMPLATE_FAIL": "alert-provider"},
        "private_rule_dynamic": lambda: {"FAKE_PRIVATE_IMAGES": "dynamic"},
    }[case]())
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0 and ("registry FAILED" in out or "refusing: " in out) and why in out, out[-2000:]
    assert "=== step registry ===" in out and "=== step up ===" not in out
    assert not any(c["tool"] == "gcloud" and c["argv"][:3] == ["container", "clusters", "create"]
                   for c in _calls(state))
    for what, v in _leaks().items():
        assert v not in out or what == "username", what
    assert "revoked" not in out and "scope40" not in out and "imgscope" not in out


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
        ("alert-provider", "0.2.53", None),
        ("github-provider-kog", "0.6.0", None), ("krateo-marketplace-catalog", "0.1.2", None)]
    # #1041 review F1: the repo-less private-image component, from the installer's ociRepo
    disp = next(c for c in pf["charts"] if c["chart"].endswith("/alert-provider"))
    assert disp["chart"] == "oci://ghcr.io/krateo-platformops/charts/alert-provider"
    # F4: each pull-secret path tied to the pod that uses it
    pol = next(c for c in pf["charts"] if c["chart"].endswith("/agentgateway-policies"))
    assert [(x["path"], x["result"], x["images"]) for x in pol["paths"]] == [
        ("certReplayHop.image.pullSecrets", "ok", ["ghcr.io/krateo-agentiko/cert-replay-hop:0.2.18"]),
        ("uiTokenHop.image.pullSecrets", "ok", ["ghcr.io/krateo-agentiko/ui-token-hop:0.2.18"])]
    # N1: its secret is on its ServiceAccount, which its Deployment and Job name: ok, as on a cluster
    assert [(x["path"], x["result"], x["images"]) for x in disp["paths"]] == [
        ("imagePullSecrets", "ok", ["ghcr.io/krateo-platformops/alert-provider:0.2.53"])]
    assert pf["caps"]["chart_inflated_max"] == pf["caps"]["blob_max"] == 100 * 1024 * 1024
    assert pf["caps"]["chart_file_max"] == 5 * 1024 * 1024
    assert all(set(c["measured"]) == {"blob", "largest_file", "inflated", "parsed", "render"} for c in pf["charts"])
    assert pf["caps"]["chart_parsed_max"] == 16 * ghcr_pull.REAL_LARGEST["parsed"]
    assert pf["stale_inputs"] == []                    # the fakes are far inside every real measurement
    # the images: read from the pinned chart's own values (an empty tag = its appVersion), each
    # ghcr.io one resolved with the token; another registry's named, not this token's to resolve
    # #1023 A: RENDERED with the installer's componentValues: ui-token-hop (on only through them, named
    # by no values.yaml) and agw-init (hard-coded in a template) are preflighted
    assert pf["images"] == [
        {"image": "ghcr.io/krateo-agentiko/agw-init:0.2.0", "charts": ["agentgateway-controller"], "error": None},
        {"image": "ghcr.io/krateo-agentiko/cert-replay-hop:0.2.18", "charts": ["agentgateway-policies"], "error": None},
        {"image": "ghcr.io/krateo-agentiko/hop-sidecar:9.0.1", "charts": ["agentgateway-policies"], "error": None},
        {"image": "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18", "charts": ["agentgateway-policies"], "error": None},
        {"image": "ghcr.io/krateo-platformops/alert-provider:0.2.53", "charts": ["alert-provider"], "error": None}]
    assert {c["chart"].rsplit("/", 1)[1]: (c["render"].split(" ")[0], c["private_image_component"])
            for c in pf["charts"]} == {"agentgateway-controller": ("ok", False), "agentgateway-policies": ("ok", True),
                                       "alert-provider": ("ok", True),
                                       "github-provider-kog": ("ok", False), "krateo-marketplace-catalog": ("ok", False)}
    assert pf["private_image_components"] == ["agentgateway-policies", "alert-provider", "core-provider-agent"]
    # F2: every call at its own WHOLE bound
    assert pf["bound_s"] == 5 * (4 * 30 + 60 + 120 + 120) + 5 * 2 * 30           # round 3 M1: + the parse
    assert "5 charts x (4 x 30 + 60 + 120 + 120 s) + 5 images x (2 x 30 s)" == pf["bound"]
    assert f"[ghcr] the step's bound: {pf['bound_s']:g} s" in out
    assert pf["images_elsewhere"] == [
        {"image": "cr.agentgateway.dev/agentgateway:v0.7.0", "charts": ["agentgateway-policies"]},
        {"image": "cr.agentgateway.dev/controller:v0.7.0", "charts": ["agentgateway-controller"]}]
    # registryAuth: 057's fields (installer-platform values, read-only), the username not in the
    # record; the image-pull Secret is the bench's own (the installer's documented opt-out)
    vals = yaml.safe_load((tmp / "out" / "installer-values.yaml").read_text())
    assert vals["registryAuth"] == {"enabled": True, "imagePullSecretName": "bench-ghcr-image-pull",
                                    "insecureSkipVerifyTLS": False, "registries": [],
                                    "passwordRef": {"name": "krateo-agentiko-pull",
                                                    "namespace": "krateo-system", "key": "token"}}
    # the two Secrets: created from stdin, before the install, in krateo-system
    secs = [json.loads(ln) for ln in (state / "created-secrets.jsonl").read_text().splitlines()]
    assert [x["argv"][3] for x in secs] == ["krateo-agentiko-pull", "bench-ghcr-image-pull"]
    tok, dcj = secs
    assert tok["stdin"] == FAKE_GHCR_TOKEN and tok["ns"] == "krateo-system"
    assert "--from-file=token=/dev/stdin" in tok["argv"]
    # the dockerconfigjson: the shape the installer's imagepull-secret.yaml renders, for ghcr.io
    import base64
    assert json.loads(dcj["stdin"]) == {"auths": {"ghcr.io": {
        "username": FAKE_GHCR_USER, "password": FAKE_GHCR_TOKEN,
        "auth": base64.b64encode(f"{FAKE_GHCR_USER}:{FAKE_GHCR_TOKEN}".encode()).decode()}}}
    assert dcj["ns"] == "krateo-system"
    assert "--type=kubernetes.io/dockerconfigjson" in dcj["argv"]
    assert "--from-file=.dockerconfigjson=/dev/stdin" in dcj["argv"]
    calls = _calls(state)
    i_secrets = [i for i, c in enumerate(calls) if c["tool"] == "kubectl" and "secret" in c["argv"]]
    i_install = next(i for i, c in enumerate(calls) if c["tool"] == "helm" and "install" in c["argv"])
    assert len(i_secrets) == 2 and max(i_secrets) < i_install
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


def test_the_images_are_what_the_chart_values_name():
    """As the charts template them: a dict under `image` with a repository (a
    registry before it), its tag, else its digest, else the chart's
    appVersion; a string under `image`."""
    v = {"uiTokenHop": {"image": {"repository": "ghcr.io/krateo-agentiko/ui-token-hop", "tag": ""}},
         "proxy": {"image": {"registry": "cr.agentgateway.dev", "repository": "agentgateway", "tag": "v1"}},
         "sidecars": [{"image": {"repository": "ghcr.io/x/pinned", "digest": "sha256:" + "a" * 64}}],
         "job": {"image": "ghcr.io/x/job"}, "notAnImage": {"repository": "ghcr.io/x/ignored"},
         "image": {"pullPolicy": "IfNotPresent"}}
    assert ghcr_pull.images_in_values(v, "0.2.18") == [
        "cr.agentgateway.dev/agentgateway:v1", "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18",
        "ghcr.io/x/job:latest", "ghcr.io/x/pinned@sha256:" + "a" * 64]
    assert ghcr_pull.split_image("ghcr.io/krateo-agentiko/ui-token-hop:0.2.18") == (
        "ghcr.io", "krateo-agentiko/ui-token-hop", "0.2.18")


def test_a_chart_is_read_through_the_blob_redirect_without_the_registry_bearer():
    """ghcr serves a blob through a redirect to its storage; the fake store
    refuses any Authorization header, so reading the chart proves the bearer
    is not forwarded; the content is digest-verified."""
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN,
                               {"uiTokenHop": {"enabled": True}}, HELM)
    assert r["error"] is None and r["render_error"] is None
    imgs = r["images"]
    assert "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18" in imgs
    assert "ghcr.io/krateo-agentiko/cert-replay-hop:0.2.18" in imgs
    assert "ghcr.io/krateo-agentiko/hop-sidecar:9.0.1" in imgs          # a subchart: its own appVersion


@pytest.mark.parametrize("version,why", [(FAKE_CHART_BAD_DIGEST, "does not match its digest"),
                                         (FAKE_CHART_HTTP_BLOB, "redirected off https"),
                                         # #1023: every read bounded in size
                                         (FAKE_CHART_BIG_MANIFEST, f"an answer larger than its {4 * 1024 * 1024} byte bound"),
                                         (FAKE_CHART_BLOB_OVER_SIZE, "an answer larger than its ")])
def test_a_chart_blob_that_is_not_what_the_manifest_names_is_refused(version, why):
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", version, FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert r["images"] == [] and r["error"].startswith(f"{AGW}/agentgateway-policies:{version}: its images unreadable: ")
    assert why in r["error"]


def test_the_render_uses_the_installers_values_for_the_component():
    """#1023 A, run 19's dry pass: values-only found cert-replay-hop, not
    ui-token-hop, which the installer turns on through componentValues and
    which only a template names. The chart's own defaults render it off."""
    on = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN,
                                {"uiTokenHop": {"enabled": True}}, HELM)
    off = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18" in on["rendered"]
    assert "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18" not in off["rendered"]
    # as the real 0.2.18, its values.yaml names the image: the values walk lists it either way
    assert "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18" in off["images"]


def test_the_pins_carry_the_installers_component_values_overridden_by_the_benchs(tmp_path):
    root = tmp_path / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 0.3.468\n")
    (root / "templates" / "_helpers.tpl").write_text('{{- define "inst.coreFeatures" -}}{{- end -}}\n')
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "agentgateway-policies", "version": "0.2.18", "repo": AGW}]}))
    (root / "values.yaml").write_text(yaml.safe_dump({"componentValues": {"agentgateway-policies": {
        "uiTokenHop": {"enabled": True}, "networkPolicy": {"ui": {"enabled": False}, "egress": {"enabled": True}}}}}))
    v = tmp_path / "v.yaml"
    v.write_text(yaml.safe_dump({"componentValues": {"agentgateway-policies": {"networkPolicy": {"ui": {"enabled": True}},
                                                                               "certReplayHop": {"replicas": 2}}}}))
    assert ghcr_pull.private_pins(str(tmp_path), str(v))[0]["values"] == {      # merged map by map, as helm does
        "uiTokenHop": {"enabled": True}, "networkPolicy": {"ui": {"enabled": True}, "egress": {"enabled": True}},
        "certReplayHop": {"replicas": 2}}


def test_a_render_that_does_not_finish_is_bounded(monkeypatch, tmp_path):
    """Past the bound, the answer AND helm itself stop (it is killed)."""
    import time
    pid = tmp_path / "pid"
    slow = tmp_path / "helm"
    slow.write_text(f"#!/bin/sh\necho $$ > {pid}\nexec sleep 30\n")
    slow.chmod(0o755)
    monkeypatch.setattr(ghcr_pull, "RENDER_BOUND_S", 1.0)
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, str(slow))
    assert r["render_error"] == "helm template did not finish within its 1 s bound"
    time.sleep(0.5)
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid.read_text()), 0)


def test_a_registry_that_drips_its_answer_is_bounded(monkeypatch):
    """Each call's WHOLE answer within its bound, not each socket read."""
    import time
    monkeypatch.setattr(ghcr_pull, "HTTP_BOUND_S", 2.0)
    t0 = time.monotonic()
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", FAKE_CHART_DRIP, FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert "no whole answer within its 2 s bound" in r["error"] and time.monotonic() - t0 < 10


def test_a_registry_that_drips_its_headers_is_bounded_too(monkeypatch):
    """#1041 review F2: the WHOLE call within its bound, connect, status line
    and headers included (a header line every 0.3 s held a 1 s bound 18 s)."""
    import time
    monkeypatch.setattr(ghcr_pull, "HTTP_BOUND_S", 1.0)
    t0 = time.monotonic()
    err = ghcr_pull.resolve(AGW, "agentgateway-policies", FAKE_CHART_HEADER_DRIP, FAKE_GHCR_USER, FAKE_GHCR_TOKEN)
    assert "no whole answer within its 1 s bound" in err and time.monotonic() - t0 < 3


def test_a_render_is_bounded_through_its_parse(monkeypatch, tmp_path):
    """#1041 review F2: helm's run AND the parse of what it printed, within
    RENDER_BOUND_S (300 000 small documents parse for seconds)."""
    import time
    big = tmp_path / "helm"
    big.write_text("#!/usr/bin/env python3\nimport sys\nsys.stdin.read()\n"
                   "sys.stdout.write('a: 1\\n---\\n' * 300000)\n")
    big.chmod(0o755)
    monkeypatch.setattr(ghcr_pull, "RENDER_BOUND_S", 1.0)
    t0 = time.monotonic()
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, str(big))
    assert r["render_error"] == "helm template did not finish within its 1 s bound", r["render_error"]
    assert time.monotonic() - t0 < 5


@pytest.mark.parametrize("cap,value,why", [
    ("BLOB_MAX", 100, "over the 100 byte ceiling of a chart"),
    ("CHART_INFLATED_MAX", 100, "its chart inflates past 100 bytes"),
    ("CHART_FILE_MAX", 20, "inflates to"),
])
def test_a_hostile_chart_is_refused_within_its_size_caps(monkeypatch, cap, value, why):
    """#1041 review F3: the blob's ceiling, the chart inflated, one file
    inflated: each checked before it is read."""
    monkeypatch.setattr(ghcr_pull, cap, value)
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert r["error"] and why in r["error"], r


def test_a_render_over_its_output_cap_is_refused(monkeypatch):
    monkeypatch.setattr(ghcr_pull, "RENDER_OUT_MAX", 10)
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN,
                               {"uiTokenHop": {"enabled": True}}, HELM)
    assert r["render_error"] == "helm template rendered more than its 10 byte cap"


def test_the_caps_are_helms_own_limits_and_the_render_a_measured_margin():
    """The chart's files: helm's own loader limits (100 MiB a chart, 5 MiB a
    file); the blob: a gzip stream inflates to about its own size at least,
    so the same 100 MiB; the render (helm has no limit): 16 x the largest
    measured. Every real chart run 20 pulls sits far inside each."""
    c = ghcr_pull.CAPS
    assert (c["chart_inflated_max"], c["chart_file_max"]) == (100 * 1024 * 1024, 5 * 1024 * 1024)
    assert c["blob_max"] == c["chart_inflated_max"]
    assert c["render_out_max"] == c["render_margin"] * c["real_largest"]["render"]
    assert all(c["real_largest"][k] < cap for k, cap in (("blob", c["blob_max"]), ("file", c["chart_file_max"]),
                                                         ("inflated", c["chart_inflated_max"]),
                                                         ("render", c["render_out_max"])))


def _many_member_chart(n):
    import io
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, body in (("c/Chart.yaml", b"name: c\nversion: 1.0.0\n"), ("c/values.yaml", b"a: 1\n")):
            ti = tarfile.TarInfo(name)
            ti.size = len(body)
            tf.addfile(ti, io.BytesIO(body))
        for i in range(n):
            tf.addfile(tarfile.TarInfo(f"c/templates/e{i}.yaml"), io.BytesIO(b""))
    return buf.getvalue()


def _walk_peak(data):
    import tracemalloc
    tracemalloc.start()
    out = ghcr_pull.read_chart_files(data)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return out, peak


def test_a_chart_of_many_members_is_walked_in_constant_memory():
    """#1041 review (round 2, info): the walk's memory does not grow with the
    member count. 90 000 more members, kept as headers, would add tens of MB
    (about 420 B each); walked one at a time they add nearly nothing. A
    difference, not an absolute bound, so other threads' allocations do not
    count."""
    (charts, vals, _, _, _), small = _walk_peak(_many_member_chart(10_000))
    _, large = _walk_peak(_many_member_chart(100_000))
    assert charts["c"]["name"] == "c" and vals["c"] == {"a": 1}
    assert large - small < 90_000 * 420 // 4, (small, large)


def test_only_the_tail_of_helms_stderr_is_read(tmp_path):
    """#1041 review N3: the reason is helm's last line; the rest of a huge
    stderr is never read."""
    f = tmp_path / "stderr"
    f.write_bytes(b"x" * 1_000_000 + b"\nError: the reason\n")
    with open(f, "rb") as fh:
        t = ghcr_pull._tail(fh)
    assert len(t) <= ghcr_pull.STDERR_TAIL and t.strip().splitlines()[-1] == "Error: the reason"


def _pod(image, secrets=(), sa=None, kind="Deployment", ns=None):
    spec = {"containers": [{"name": "c", "image": image}]}
    if secrets:
        spec["imagePullSecrets"] = [{"name": s} for s in secrets]
    if sa:
        spec["serviceAccountName"] = sa
    return {"kind": kind, **({"metadata": {"namespace": ns}} if ns else {}), "spec": {"template": {"spec": spec}}}


def _sa(name, ns=None):
    return {"kind": "ServiceAccount", "metadata": {"name": name, **({"namespace": ns} if ns else {})},
            "imagePullSecrets": [{"name": ghcr_pull.marker(0)}]}


@pytest.mark.parametrize("case,docs,effective,want", [
    # A: the hop is enabled: false, but its template ignores it and runs the image with no secret
    ("A", [_pod("ghcr.io/o/hop:1.0.0")], {"hop": {"enabled": False}}, "missing"),
    # B: an UNRELATED ancestor is off; the chartGate pod renders, reading its secret from another key
    ("B", [_pod("ghcr.io/o/chart-gate:1.0.0", ["other-secret"])], {"mcpServers": {"enabled": False}}, "missing"),
    # C: the control: B with the ancestor on
    ("C", [_pod("ghcr.io/o/chart-gate:1.0.0", ["other-secret"])], {"mcpServers": {"enabled": True}}, "missing"),
    # D: the secret on the pod's ServiceAccount, which the pod names (alert-provider 0.2.53's shape)
    ("D", [{"kind": "ServiceAccount", "metadata": {"name": "ctl-sa"},
            "imagePullSecrets": [{"name": ghcr_pull.marker(0)}]}, _pod("ghcr.io/o/ctl:1.0.0", sa="ctl-sa")],
     {}, "ok"),
    # a pod naming no ServiceAccount runs as `default`
    ("D-default", [{"kind": "ServiceAccount", "metadata": {"name": "default"},
                    "imagePullSecrets": [{"name": ghcr_pull.marker(0)}]}, _pod("ghcr.io/o/ctl:1.0.0")], {}, "ok"),
    # legitimately off: the hop disabled and nothing of it rendered
    ("off", [], {"hop": {"enabled": False}}, "off"),
    # F (round 3): the marker's ServiceAccount is in ANOTHER namespace than the pod: not the pod's
    ("F", [_sa("sa", ns="ns-b"), _pod("ghcr.io/o/ctl:1.0.0", sa="sa", ns="ns-a")], {}, "missing"),
    # its control: the same namespace, named on both
    ("F-same", [_sa("sa", ns="ns-a"), _pod("ghcr.io/o/ctl:1.0.0", sa="sa", ns="ns-a")], {}, "ok"),
    # a manifest with no namespace is in the release's (helm template --namespace): the same as one naming it
    ("F-release", [_sa("sa"), _pod("ghcr.io/o/ctl:1.0.0", sa="sa", ns=ghcr_pull.SECRET_NS)], {}, "ok"),
])
def test_off_means_nothing_runs_behind_it(case, docs, effective, want):
    """#1041 review N2 (A, B, D): a path is off only when its sub-component is
    disabled AND no rendered ghcr pod runs without the component's markers;
    a pod's secrets include its ServiceAccount's (N1)."""
    path = {"A": ["hop", "image", "pullSecrets"], "off": ["hop", "image", "pullSecrets"]}.get(
        case, ["mcpServers", "chartGate", "imagePullSecrets"] if case in ("B", "C") else ["imagePullSecrets"])
    got = ghcr_pull.path_results(docs, [path], effective)
    assert got[0]["result"] == want, (case, got)


def test_each_path_is_tied_to_the_pod_that_uses_it():
    """#1041 review F4: ok = a pod running a ghcr image carries the path's
    marker; off = the path's own sub-component is enabled: false; else
    missing."""
    docs = [{"kind": "Deployment", "spec": {"template": {"spec": {
        "imagePullSecrets": [{"name": ghcr_pull.marker(0)}],
        "containers": [{"name": "a", "image": "ghcr.io/x/a:1"}]}}}},
        {"kind": "Deployment", "spec": {"template": {"spec": {
            "imagePullSecrets": [{"name": ghcr_pull.marker(2)}],
            "containers": [{"name": "b", "image": "docker.io/x/b:1"}]}}}}]
    paths = [["a", "image", "pullSecrets"], ["b", "image", "pullSecrets"], ["c", "pullSecrets"]]
    got = ghcr_pull.path_results(docs, paths, {"b": {"enabled": False}})
    assert [(r["path"], r["result"]) for r in got] == [
        ("a.image.pullSecrets", "ok"), ("b.image.pullSecrets", "off"), ("c.pullSecrets", "missing")]
    assert ghcr_pull.with_markers({"a": {"image": {"pullSecrets": []}}}, paths)["a"]["image"]["pullSecrets"] == [
        {"name": ghcr_pull.marker(0)}]


def test_helms_merge_deletes_a_null_and_the_release_is_the_components_name(tmp_path):
    """#1041 review F5: what can change an image is mirrored: a null deletes
    the key, and the release is named after the component, as the
    installer's $c.name (a recording helm writes the argv it was given)."""
    assert ghcr_pull._merge({"a": {"b": 1, "c": 2}}, {"a": {"c": None}}) == {"a": {"b": 1}}
    argv = tmp_path / "argv.json"
    rec = tmp_path / "helm"
    rec.write_text("#!/usr/bin/env python3\nimport json, sys\nsys.stdin.read()\n"
                   f"open({str(argv)!r}, 'w').write(json.dumps(sys.argv[1:]))\n")
    rec.chmod(0o755)
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, str(rec),
                               name="the-component")
    assert r["error"] is None and r["render_error"] is None, r
    got = json.loads(argv.read_text())
    assert got[:2] == ["template", "the-component"], got


def test_every_enabled_private_component_is_a_pin_whatever_its_repo(tmp_path):
    """#1041 review F1: a private-image component with no repo of its own
    comes from the installer's ociRepo (the bench's, if it sets one)."""
    root = tmp_path / "installer"
    (root / "files").mkdir(parents=True)
    (root / "templates").mkdir()
    (root / "Chart.yaml").write_text("apiVersion: v2\nname: installer\nversion: 0.3.468\n")
    (root / "templates" / "_helpers.tpl").write_text('{{- define "inst.coreFeatures" -}}{{- end -}}\n')
    (root / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "alert-provider", "version": "0.2.55", "feature": "portal"},
        {"name": "snowplow", "version": "1.12.56"}]}))
    (root / "values.yaml").write_text(yaml.safe_dump({"ociRepo": "oci://ghcr.io/krateo-platformops/charts",
                                                      "features": {"portal": True}}))
    v = tmp_path / "v.yaml"
    v.write_text(yaml.safe_dump({}))
    assert [p["name"] for p in ghcr_pull.private_pins(str(tmp_path), str(v))] == []
    pins = ghcr_pull.private_pins(str(tmp_path), str(v), include={"alert-provider"})
    assert [(p["name"], p["repo"]) for p in pins] == [("alert-provider", "oci://ghcr.io/krateo-platformops/charts")]
    v.write_text(yaml.safe_dump({"ociRepo": "oci://ghcr.io/mirror/charts"}))
    assert ghcr_pull.private_pins(str(tmp_path), str(v), include={"alert-provider"})[0]["repo"] == \
        "oci://ghcr.io/mirror/charts"


def test_yaml_aliases_are_walked_once():
    """A chain of aliases, each doubling the one before: 40 levels would be
    2^40 visits without a visited set."""
    import signal
    doc = "l0: &l0 {image: {repository: ghcr.io/x/y, tag: '1'}}\n" + "".join(
        f"l{i}: &l{i} [*l{i - 1}, *l{i - 1}]\n" for i in range(1, 41))
    values = yaml.safe_load(doc)

    def boom(*_):
        raise TimeoutError("the walk did not finish: aliases are walked more than once")
    old = signal.signal(signal.SIGALRM, boom)
    signal.alarm(20)
    try:
        assert ghcr_pull.images_in_values(values, "0") == ["ghcr.io/x/y:1"]
        assert ghcr_pull.images_in_manifests([{"spec": values}]) == []
        assert ghcr_pull.images_in_manifests(yaml.safe_load(doc.replace("{image: {repository: ghcr.io/x/y, tag: '1'}}",
                                                                         "{image: ghcr.io/x/y:1}"))) == ["ghcr.io/x/y:1"]
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def test_the_image_pull_failure_redacts_the_pull_account():
    import subprocess as sp
    from bench import diag606
    pod = {"metadata": {"name": "p", "namespace": "ns"}, "spec": {"containers": [{"name": "c", "image": "ghcr.io/x/c:1"}]},
           "status": {"containerStatuses": [{"name": "c", "state": {"waiting": {
               "reason": "ErrImagePull", "message": f"pulling as {FAKE_GHCR_USER}: 401"}}}]}}

    def run(argv, **kw):
        out = {"items": [pod]} if " get pods" in " ".join(argv) else {"items": []}
        return sp.CompletedProcess(argv, 0, json.dumps(out), "")
    f = diag606.image_pull_failures(diag606.Kube("kc", "gke_p_z_snowplow-bench-x", run=run))
    assert FAKE_GHCR_USER not in f[0]["message"] and "<redacted>" in f[0]["message"]


@pytest.mark.parametrize("env,render", [({"FAKE_HELM_TEMPLATE_FAIL": "agentgateway-controller"}, "failed: helm template failed: "),
                                        ({"FAKE_PRIVATE_IMAGES": "none"}, "ok")])
def test_what_is_not_a_private_image_component_is_recorded_not_refused(harness, env, render):  # noqa: F811
    """A chart the installer injects no pull Secret into may fail to render:
    recorded (its values' images still preflighted), the run goes on. An
    installer with no inst.privateImageComponents says so."""
    env0, state, ambient, tmp = harness
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env0, **env), capture_output=True,
                       text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    pf = json.loads((tmp / "out" / "ghcr-pull-preflight.json").read_text())
    ctl = next(c for c in pf["charts"] if c["chart"].endswith("/agentgateway-controller"))
    assert ctl["render"].startswith(render) and ctl["error"] is None
    if "FAKE_PRIVATE_IMAGES" in env:
        assert pf["private_image_components"] is None and "declares no inst.privateImageComponents" in out


def test_an_init_container_that_cannot_pull_is_named_too():
    import subprocess as sp
    from bench import diag606
    pod = {"metadata": {"name": "p", "namespace": "ns"},
           "spec": {"initContainers": [{"name": "init", "image": "ghcr.io/x/init:1"}],
                    "containers": [{"name": "app", "image": "ghcr.io/x/app:1"}]},
           "status": {"initContainerStatuses": [{"name": "init", "state": {"waiting": {"reason": "ErrImagePull",
                                                                                       "message": "401"}}}],
                      "containerStatuses": [{"name": "app", "state": {"waiting": {"reason": "PodInitializing"}}}]}}

    def run(argv, **kw):
        if " get pods" in " ".join(argv):
            return sp.CompletedProcess(argv, 0, json.dumps({"items": [pod]}), "")
        return sp.CompletedProcess(argv, 0, json.dumps({"items": []}), "")
    f = diag606.image_pull_failures(diag606.Kube("kc", "gke_p_z_snowplow-bench-x", run=run))
    assert [(x["pod"], x["container"], x["image"], x["reason"], x["pull_secrets"]) for x in f] == [
        ("ns/p", "init", "ghcr.io/x/init:1", "ErrImagePull", [])]


@pytest.mark.parametrize("mode,kubelet", [("backoff", None), ("missing_secret", "Unable to retrieve some image pull "
                                                                              "secrets (krateo-registry-image-pull)")])
def test_an_image_that_cannot_be_pulled_is_the_platform_failures_name(harness, mode, kubelet):  # noqa: F811
    """Run 19 attempt 2: the hop pods sat in ImagePullBackOff for 43 min and
    the step said only "timeout". The pod, its image and the pull Secret it
    references are named at the poll it appears and ARE the failure's
    reason; a Secret the kubelet cannot retrieve is said so (read from the
    events: Secrets are never read)."""
    env, state, ambient, tmp = harness
    env = dict(env, FAKE_IMAGE_PULL=mode, FAKE_INSTALLER_READY="0", PLATFORM_TIMEOUT_S="1",
               PLATFORM_POLL_S="0.2")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    line = ("krateo-system/agentgateway-policies-ui-token-hop-7d9f-x2 [hop] "
            "ghcr.io/krateo-agentiko/ui-token-hop:0.2.18: ImagePullBackOff "
            "(image-pull Secrets referenced: krateo-registry-image-pull")
    assert f"[platform] IMAGE PULL FAILING: {line}" in out
    assert f"platform FAILED: image pull: {line}" in out, out[-3000:]
    assert (f"; kubelet: {kubelet}" in out) if kubelet else ("kubelet:" not in out)
    rec = [json.loads(ln) for ln in (tmp / "out" / "command-failures.jsonl").read_text().splitlines()]
    assert any(r["step"] == "platform" and r["what"].startswith("platform FAILED: image pull: ") for r in rec)


def test_without_an_image_pull_failure_the_platform_failure_names_what_is_not_ready(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env = dict(env, FAKE_INSTALLER_READY="0", PLATFORM_TIMEOUT_S="1", PLATFORM_POLL_S="0.2")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=300)
    out = p.stdout + p.stderr
    assert "platform FAILED: not Ready after 1s: " in out and "IMAGE PULL" not in out


def test_check_never_names_the_token(tmp_path):
    f = _file(tmp_path, GOOD)
    assert ghcr_pull.check(str(f)) is None
    assert ghcr_pull.creds(str(f)) == (FAKE_GHCR_USER, FAKE_GHCR_TOKEN)
    f.chmod(0o640)
    assert FAKE_GHCR_TOKEN not in ghcr_pull.check(str(f))


# ── #1041 review round 3 ──────────────────────────────────────────────────

def _yaml_heavy_chart(sub_bytes, nsub=2):
    """The review's M1 probe shape: a small chart whose subcharts each carry
    a values.yaml of ordinary structured YAML (`kN: {a: 1, b: [x, y]}`)."""
    import io
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        def add(name, body):
            ti = tarfile.TarInfo(name)
            ti.size = len(body)
            tf.addfile(ti, io.BytesIO(body))
        add("c/Chart.yaml", b"apiVersion: v2\nname: c\nversion: 1.0.0\n")
        add("c/values.yaml", b"a: 1\n")
        line = "k{}: {{a: 1, b: [x, y]}}\n"
        for j in range(nsub):
            body, k = [], 0
            size = 0
            while size + len(line.format(k)) <= sub_bytes:
                body.append(line.format(k))
                size += len(body[-1])
                k += 1
            add(f"c/charts/s{j}/Chart.yaml", f"apiVersion: v2\nname: s{j}\nversion: 1.0.0\n".encode())
            add(f"c/charts/s{j}/values.yaml", "".join(body).encode())
    return buf.getvalue()


def test_the_reviews_yaml_heavy_chart_is_refused_before_any_parse(monkeypatch):
    """Round 3, M1: two 5 MiB values.yaml (within helm's own limits) took
    941 s and +496 MiB to parse. All of a chart's Chart.yaml + values.yaml
    together are capped at 16 x the largest real total, counted before any
    is parsed: refused at once, the parse never run."""
    import time
    data = _yaml_heavy_chart(5 * 1024 * 1024 - 64)
    parsed = []
    real = ghcr_pull._yaml_loader
    monkeypatch.setattr(ghcr_pull, "_yaml_loader", lambda: parsed.append(1) or real())
    t0 = time.monotonic()
    with pytest.raises(ghcr_pull._Refused, match=r"more than its 2260688 byte parse cap \(16 x the largest real total\)"):
        ghcr_pull.read_chart_files(data)
    assert time.monotonic() - t0 < 5 and not parsed
    assert ghcr_pull.CHART_PARSED_MAX == 16 * 141_293
    # just under the cap, the same shape is read whole
    under = ghcr_pull.read_chart_files(_yaml_heavy_chart((ghcr_pull.CHART_PARSED_MAX - 400) // 2))
    assert under[4] <= ghcr_pull.CHART_PARSED_MAX and len(under[1]["c/charts/s0"]) == 43_894


def test_a_chart_over_its_parse_cap_is_refused_through_chart_images(monkeypatch):
    monkeypatch.setattr(ghcr_pull, "CHART_PARSED_MAX", 10)
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert r["error"] and "byte parse cap" in r["error"], r


def test_the_chart_files_are_read_within_their_own_bound(monkeypatch):
    """Round 3, M1: the tar walk and the parse run WHOLE under PARSE_BOUND_S
    (_call), and that bound is counted in the step's."""
    import time
    real = ghcr_pull.read_chart_files

    def slow(data):
        time.sleep(3)
        return real(data)
    monkeypatch.setattr(ghcr_pull, "read_chart_files", slow)
    monkeypatch.setattr(ghcr_pull, "PARSE_BOUND_S", 0.5)
    t0 = time.monotonic()
    r = ghcr_pull.chart_images(AGW, "agentgateway-policies", "0.2.18", FAKE_GHCR_USER, FAKE_GHCR_TOKEN, {}, HELM)
    assert r["error"] and "its chart files were not read within their 0.5 s bound" in r["error"], r
    assert time.monotonic() - t0 < 2.5


def test_the_render_reads_only_the_tail_of_helms_stderr(tmp_path):
    """Round 3, R28: render() itself (not only _tail) reads helm's reason
    from the end of its stderr: a 32 MiB stderr costs nearly nothing."""
    import tracemalloc
    helm = tmp_path / "helm"
    helm.write_text("#!/bin/sh\ncat > /dev/null\nhead -c 33554432 /dev/zero | tr '\\0' x >&2\n"
                    "printf '\\nError: the real reason\\n' >&2\nexit 1\n")
    helm.chmod(0o755)
    tracemalloc.start()
    with pytest.raises(ghcr_pull._Refused, match="^helm template failed: Error: the real reason$"):
        ghcr_pull.render(b"chart", "rel", {}, str(helm))
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert peak < 32 * 1024 * 1024 // 8, peak


def test_a_helm_that_exits_before_reading_its_values_is_a_render_failure(tmp_path):
    """Round 3 (info): a helm that never reads -f - breaks the values pipe;
    that is helm failing (its own reason), never an unreadable chart."""
    helm = tmp_path / "helm"
    helm.write_text("#!/bin/sh\necho 'Error: no such chart' >&2\nexit 1\n")
    helm.chmod(0o755)
    big = {"k": "v" * (1 << 20)}                         # past any pipe buffer: the write must break
    with pytest.raises(ghcr_pull._Refused, match="^helm template failed: Error: no such chart$"):
        ghcr_pull.render(b"chart", "rel", big, str(helm))
    ok = tmp_path / "helm0"                              # exits 0 without reading: no render from it
    ok.write_text("#!/bin/sh\nexit 0\n")
    ok.chmod(0o755)
    with pytest.raises(ghcr_pull._Refused, match="exited without reading its values"):
        ghcr_pull.render(b"chart", "rel", big, str(ok))


def test_the_caps_input_is_the_largest_real_measurement():
    """Round 3, R31: REAL_LARGEST is not a hand-copied number on its own: it
    is the largest of each measurement in the committed record of the real
    charts (tests/fixtures/ghcr_measured_0_3_468.json, the preflight's own
    `measured` over installer 0.3.468)."""
    rec = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "ghcr_measured_0_3_468.json")))
    largest = {}
    for c in rec["charts"]:
        for k, v in c["measured"].items():
            key = ghcr_pull.MEASURED_INPUT[k]
            largest[key] = max(largest.get(key, 0), v)
    assert largest == ghcr_pull.REAL_LARGEST


def test_a_measurement_over_its_input_is_reported_not_refused():
    """Round 3, R31: a run whose chart measures over REAL_LARGEST says so (the
    16 x margin is over a stale input), in the record and as a report line."""
    big = ghcr_pull.REAL_LARGEST["render"] + 1
    charts = [{"chart": "oci://ghcr.io/o/c", "version": "1.0.0",
               "measured": {"blob": 1, "largest_file": 1, "inflated": 1, "parsed": 1, "render": big}},
              {"chart": "oci://ghcr.io/o/d", "version": "2.0.0", "measured": None}]
    st = ghcr_pull.stale_inputs(charts)
    assert st == [{"chart": "oci://ghcr.io/o/c", "version": "1.0.0", "measured": "render", "value": big,
                   "input": "render", "real_largest": ghcr_pull.REAL_LARGEST["render"]}]
    (line,) = ghcr_pull.stale_notes(st)
    assert line.startswith("[ghcr] NOTE: oci://ghcr.io/o/c 1.0.0 measured render") and "reported, not refused" in line
    at = [dict(charts[0], measured={k: ghcr_pull.REAL_LARGEST[ghcr_pull.MEASURED_INPUT[k]]
                                    for k in ghcr_pull.MEASURED_INPUT})]
    assert ghcr_pull.stale_inputs(at) == []                # AT the input is not over it
