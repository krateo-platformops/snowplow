"""#1588 follow-up: the installer's composition render of the run's effective spec, before any cluster
(bench/installer_render.py), the run's Installer CR patch and its read-back (run606.sh s_install), and where
cert-manager reads cluster CA Secrets from (bench/cert_manager.py cluster_resource_namespace).

The renders are of the REAL published installer the records were read on (tests/installer_archives.py RECORDED),
the REAL pinned cert-manager chart and the REAL trust-manager blueprint chart installer 0.3.703 pins (0.17.0,
helm pull oci://ghcr.io/krateo-blueprints/charts/trust-manager --version 0.17.0, 2026-11-23)."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from bench import cert_manager, installer_features, installer_render, installer_values
from tests import installer_archives as ia
from tests.test_run606_real_sequence import CERT_MANAGER_TGZ, harness  # noqa: F401  (fixture)

BENCH_DIR = Path(__file__).resolve().parent.parent
TRUST_MANAGER_TGZ = Path(__file__).resolve().parent / "fixtures" / "trust-manager-0.17.0.tgz"
TRUST_MANAGER_SHA256 = "da4a8f0f389f3efbcc32d4b0722daa39a40c1f9ab2426e5e27542da555ef171a"
GUARDS = {"certManagerNamespace": "features.internalTls is on but internalTls.certManagerNamespace is empty",
          "target": "features.internalTls is on but componentValues.internal-ca-bundle.target.namespaces is empty"}


def _cm_render() -> str:
    """The bench's cert-manager chart rendered as its step renders it (bench/cert_manager.py preflight)."""
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        v = os.path.join(d, "values.yaml")
        with open(v, "w") as fh:
            yaml.safe_dump(cert_manager.VALUES, fh)
        return subprocess.run(["helm", "template", cert_manager.RELEASE, str(CERT_MANAGER_TGZ), "--namespace",
                               cert_manager.NAMESPACE, "-f", v], capture_output=True, text=True, check=True).stdout


@pytest.fixture(scope="module")
def pulled(tmp_path_factory):
    return str(ia.pull(tmp_path_factory.mktemp("installer"), ia.RECORDED))


def _values() -> dict:
    return yaml.safe_load(installer_values.document("1.12.74"))


def _patch() -> dict:
    return installer_render.cr_patch(installer_features.bench_features(), cert_manager.cluster_resource_namespace(_cm_render()))


# ─── where cert-manager reads cluster CA Secrets from ─────────────────────

def test_cert_managers_cluster_resource_namespace_is_its_pods_namespace():
    """v1.21.1 renders --cluster-resource-namespace=$(POD_NAMESPACE), POD_NAMESPACE a fieldRef to metadata.namespace:
    the namespace the bench's step installs into. As read on krateo-057's controller (2026-11-23)."""
    out = _cm_render()
    assert "--cluster-resource-namespace=$(POD_NAMESPACE)" in out
    assert cert_manager.cluster_resource_namespace(out) == cert_manager.NAMESPACE == "cert-manager"


def _deploy(args, env=None, ns="cert-manager") -> str:
    return yaml.safe_dump({"kind": "Deployment", "metadata": {"name": "cert-manager", "namespace": ns},
                           "spec": {"template": {"spec": {"containers": [{"args": args, "env": env or []}]}}}})


def test_falsifier_the_flags_value_is_read_never_assumed():
    assert cert_manager.cluster_resource_namespace(_deploy(["--cluster-resource-namespace=elsewhere"])) == "elsewhere"
    assert cert_manager.cluster_resource_namespace(_deploy(["--cluster-resource-namespace=$(X)"],
                                                           [{"name": "X", "value": "lit"}])) == "lit"
    with pytest.raises(cert_manager.Refused, match="in 0 container"):
        cert_manager.cluster_resource_namespace(_deploy(["--v=2"]))
    with pytest.raises(cert_manager.Refused, match="neither a value nor the pod's namespace"):
        cert_manager.cluster_resource_namespace(_deploy(["--cluster-resource-namespace=$(X)"],
                                                        [{"name": "X", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}]))


# ─── the run's CR patch ─────────────────────────────────────────────────────

def test_the_patch_turns_internal_tls_on_with_where_cert_manager_runs():
    assert _patch() == {"spec": {"features": {"internalTls": True}, "internalTls": {"certManagerNamespace": "cert-manager"}}}
    # the values write it off: the installer reads its namespace only from the CR, set in the same patch
    assert _values()["features"]["internalTls"] is False and "internalTls" not in _values()
    assert installer_features.bench_features()["internalTls"] is True


def test_no_patch_when_no_feature_is_cr_only_and_none_without_where_cert_manager_runs():
    assert installer_render.cr_patch({"internalTls": False}, "cert-manager") == {}
    with pytest.raises(installer_render.Refused, match="--cluster-resource-namespace is not known"):
        installer_render.cr_patch({"internalTls": True}, "")


def test_readback_names_every_field_the_cr_does_not_hold():
    p = _patch()
    assert installer_render.readback({"spec": {**p["spec"], "other": 1}}, p) == []
    lost = copy.deepcopy(p["spec"])
    lost.pop("internalTls")
    assert installer_render.readback({"spec": lost}, p) == [
        "spec.internalTls.certManagerNamespace is None, the patch wrote 'cert-manager'"]


# ─── the composition render ─────────────────────────────────────────────────

def test_the_runs_effective_spec_renders_every_enabled_component(pulled):
    rec = installer_render.check(pulled, _values(), _patch())
    assert rec["ok"] and rec["rendered"] == rec["enabled"] and len(rec["rendered"]) > 40
    assert {"trust-manager", "internal-ca-bundle", "cert-manager-issuers", "cert-manager"} <= set(rec["rendered"])
    assert rec["cr_api_version"] == "composition.krateo.io/v" + ia.RECORDED.replace(".", "-")
    ln = installer_render.lines(rec)
    assert ln[0].startswith(f"installer render: rendered {len(rec['rendered'])} of {len(rec['rendered'])} enabled")
    # the bypass is in the check's in-memory copy only: the pulled chart s_install installs is untouched
    assert not any(n.startswith("00-bench") for n in os.listdir(os.path.join(pulled, "installer", "templates")))


@pytest.mark.parametrize("case", ["no_cert_manager_namespace", "no_target_namespaces", "top_level_internal_tls"])
def test_falsifier_what_the_installer_refuses_is_refused_before_any_cluster(pulled, case):
    values, patch = _values(), _patch()
    if case == "no_cert_manager_namespace":
        patch["spec"].pop("internalTls")
    elif case == "no_target_namespaces":
        values["componentValues"].pop("internal-ca-bundle")
    else:
        values["internalTls"] = {"certManagerNamespace": "cert-manager"}
    with pytest.raises(installer_render.Refused) as e:
        installer_render.check(pulled, values, patch)
    why = str(e.value)
    if case == "top_level_internal_tls":
        assert why.startswith("the run's values set a top-level internalTls, which the installer never writes"), why
    else:
        assert why.startswith("the installer refuses to render its components with the run's effective spec"), why
        assert GUARDS["certManagerNamespace" if case == "no_cert_manager_namespace" else "target"] in why, why
        assert "installer/templates/compositions.yaml:" in why, why


def test_falsifier_a_bypass_that_does_not_take_cannot_pass(pulled, monkeypatch):
    """Under a name that sorts after _helpers.tpl the chart's own gate wins and emits nothing offline: refused,
    never read as a pass."""
    monkeypatch.setattr(installer_render, "BYPASS_TEMPLATE", "templates/zz-bench-render-check.tpl")
    with pytest.raises(installer_render.Refused, match=r"^rendered 0 components"):
        installer_render.check(pulled, _values(), _patch())


def test_falsifier_a_component_enabled_but_not_rendered_is_named(pulled, monkeypatch):
    monkeypatch.setattr(installer_render, "BYPASS", installer_render.BYPASS.replace(
        '{{- if eq (include "inst.compEnabled" (list $top $c)) "true" -}}',
        '{{- if and (ne $c.name "trust-manager") (eq (include "inst.compEnabled" (list $top $c)) "true") -}}'))
    with pytest.raises(installer_render.Refused, match=r"^rendered (\d+) of (\d+) enabled components; not rendered: "
                                                       r"trust-manager$"):
        installer_render.check(pulled, _values(), _patch())


# ─── trust-manager's RBAC with the bench's values (installer guard L1005's reason) ──

def _trust_manager_rules(spec: dict) -> list[tuple]:
    import tempfile
    assert hashlib.sha256(TRUST_MANAGER_TGZ.read_bytes()).hexdigest() == TRUST_MANAGER_SHA256
    with tempfile.TemporaryDirectory() as d:
        v = os.path.join(d, "v.yaml")
        with open(v, "w") as fh:
            yaml.safe_dump(spec, fh)
        out = subprocess.run(["helm", "template", "trust-manager", str(TRUST_MANAGER_TGZ), "--namespace",
                              installer_values.KRATEO_NAMESPACE, "-f", v], capture_output=True, text=True, check=True).stdout
    return [(d["kind"], (d.get("metadata") or {}).get("namespace"), tuple(r.get("verbs") or []))
            for d in yaml.safe_load_all(out) if d and d.get("kind") in ("Role", "ClusterRole")
            for r in d.get("rules") or [] if "configmaps" in (r.get("resources") or [])]


def test_trust_managers_configmap_rbac_is_namespaced_with_the_benchs_values(pulled):
    """installer guard L1005: with target.namespaces empty, trust-manager puts ConfigMap delete in a ClusterRole. With
    the spec the installer gives trust-manager from the bench's values, every ConfigMap rule is a Role, in the
    namespace the bench installs krateo into and the trust namespace (where cert-manager runs)."""
    rec = installer_render.check(pulled, _values(), _patch())
    spec = next(c["spec"] for c in rec["components"] if c["name"] == "trust-manager")
    assert spec["trust-manager"]["app"]["targetNamespaces"] == [installer_values.KRATEO_NAMESPACE]
    assert spec["trust-manager"]["app"]["trust"]["namespace"] == "cert-manager"
    rules = _trust_manager_rules(spec)
    assert rules and all(k == "Role" for k, _, _ in rules), rules
    assert {ns for _, ns, _ in rules} == {installer_values.KRATEO_NAMESPACE, "cert-manager"}
    # the falsifier: the same chart with targetNamespaces empty grants ConfigMap delete cluster-wide
    empty = copy.deepcopy(spec)
    empty["trust-manager"]["app"]["targetNamespaces"] = []
    assert any(k == "ClusterRole" and "delete" in verbs for k, _, verbs in _trust_manager_rules(empty))


# ─── run606.sh: the render before any cluster, the patch and its read-back before the platform wait ──

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_run606_renders_before_any_cluster_and_patches_then_reads_back_before_the_platform(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    assert json.loads((tmp / "out" / "installer-cr-patch.json").read_text()) == {
        "spec": {"features": {"internalTls": True}, "internalTls": {"certManagerNamespace": "cert-manager"}}}
    r = log.index("installer render: rendered ")
    assert r < log.index("=== step up ===")
    patched = log.index("installer CR patch: kubectl -n krateo-system patch installers.v")
    back = log.index("installer CR read back: it holds the run's patch")
    assert log.index("=== step install ===") < patched < back < log.index("=== step platform ===")
    # the calls themselves: one patch, of the CR version the installed chart writes, between the install and its read-back
    import yaml
    from tests.test_run606_real_sequence import _calls
    version = yaml.safe_load((tmp / "out" / "installer-pulled" / "installer" / "Chart.yaml").read_text())["version"]
    res = f"installers.v{version.replace('.', '-')}.composition.krateo.io"
    calls = [(c["tool"], c["argv"]) for c in _calls(state)]

    def at(tool, *words):
        return [i for i, (t, a) in enumerate(calls) if t == tool and all(w in a for w in words)]
    install = [i for i in at("helm", "install", "installer") if "template" not in calls[i][1]]
    patches = [i for i in at("kubectl", "patch") if any(x.startswith("installers.") for x in calls[i][1])]
    reads = [i for i in at("kubectl", "get", "installer") if any(x.startswith("installers.v") for x in calls[i][1])]
    assert len(install) == 1 and len(patches) == 1 and reads, (install, patches, reads)
    argv = calls[patches[0]][1]
    assert res in argv and argv[argv.index("-n") + 1] == "krateo-system"
    assert argv[argv.index("--type") + 1] == "merge" and argv[argv.index("patch") + 2] == "installer"
    assert res in calls[reads[0]][1] and install[0] < patches[0] < reads[0]
    assert (state / "helm-registry-auth.json").exists()
    _no_registry_username_kept(tmp, state)


@pytest.mark.parametrize("mode,why", [
    ("drops", "install FAILED: the Installer CR does not hold the run's patch: spec.internalTls.certManagerNamespace "
              "is None, the patch wrote 'cert-manager'"),
    ("fails", "install FAILED: the Installer CR patch: "),
    ("unreadable", "install FAILED: the Installer CR could not be read back: "),
])
def test_falsifier_a_cr_that_does_not_hold_the_patch_fails_before_the_platform_wait(harness, mode, why):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_CR_PATCH=mode)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and why in log, log[-3000:]
    assert "=== step platform ===" not in log
    _no_registry_username_kept(tmp, state)


@pytest.mark.parametrize("mode,asked", [("blip", "installer CR patch: the API server did not answer; asked again"),
                                        ("blip-read", "installer CR read back: the API server did not answer; asked again")])
def test_an_api_that_does_not_answer_once_is_asked_again_within_the_bound(harness, mode, asked):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_CR_PATCH=mode)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    assert log.count(asked) == 1 and "installer CR read back: it holds the run's patch" in log
    _no_registry_username_kept(tmp, state)


def test_falsifier_an_api_that_never_answers_the_patch_fails_at_its_bound(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, FAKE_CR_PATCH="down", CR_PATCH_TIMEOUT_S="3")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0, log[-3000:]
    assert "install FAILED: the Installer CR patch: the API server did not answer within 3 s" in log
    assert "=== step platform ===" not in log


def test_the_cr_patchs_bound_is_in_the_runs_budget(harness):  # noqa: F811
    """One more second of the bound is one more second of the plan (the TTL is derived from it)."""
    import re
    env, state, _, tmp = harness

    def plan(s):
        _run606(dict(env, OUT=str(tmp / f"out-{s}")), BENCH_606_DRY_RUN="1", CR_PATCH_TIMEOUT_S=str(s))
        log = (tmp / f"out-{s}" / "run.log").read_text()
        assert f"+ CR patch {s} +" in log, log[:2000]
        return int(re.search(r"= (\d+) s; fits TTL_HOURS", log).group(1))
    assert plan(301) - plan(300) == 1


def _no_registry_username_kept(tmp, state):
    """#1590 review B2: the live CR carries the registry username; the read-back keeps no file of it."""
    from tests.conftest import FAKE_GHCR_USER
    if (state / "helm-registry-auth.json").exists():          # the CR the fake serves carries it (else: proves nothing)
        assert json.loads((state / "helm-registry-auth.json").read_text())["username"] == FAKE_GHCR_USER
    kept = [f for f in (tmp / "out").rglob("*") if f.is_file() and FAKE_GHCR_USER.encode() in f.read_bytes()]
    assert kept == []
    assert not [f for f in (tmp / "out").rglob("*readback*")]


def test_falsifier_run606_refuses_a_guard_nobody_listed_before_up(harness):  # noqa: F811
    """The dry pass (and so the rehearsal) refuses what the installer refuses, before any cluster, whatever the
    guard: here an installer whose guard requires a value the bench does not write (FAKE_INSTALLER_GUARD=new)."""
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", FAKE_INSTALLER_GUARD="new")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0, log[-3000:]
    assert ("registry FAILED: installer render: the installer refuses to render its components with the run's "
            "effective spec") in log, log[-3000:]
    assert "features.internalTls is on but componentValues.internal-ca-bundle.required is empty" in log
    assert "=== step up ===" not in log


def test_run606s_dry_pass_renders_and_logs_the_patch_it_would_apply(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert "installer render: rendered " in log and "DRY: kubectl -n krateo-system get installers.v" in log
    assert "--patch-file" in log
