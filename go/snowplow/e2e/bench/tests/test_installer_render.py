"""#1588 follow-up: the installer's composition render of the run's effective spec, before any cluster
(bench/installer_render.py), internalTls from the run's install values (bench/installer_values.py), and where
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


CRN = "cert-manager"      # as cert_manager.cluster_resource_namespace reads it from the pinned chart (held below)


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


# ─── internalTls, from the install values ───────────────────────────────────

def test_the_values_turn_internal_tls_on_with_where_cert_manager_runs():
    v = _values()
    assert installer_features.bench_features()["internalTls"] is True
    assert v["features"]["internalTls"] is True and v["internalTls"] == {"certManagerNamespace": cert_manager.NAMESPACE}
    assert cert_manager.cluster_resource_namespace(_cm_render()) == cert_manager.NAMESPACE == CRN


def test_no_internal_tls_value_while_the_feature_is_off():
    assert installer_values.internal_tls_block({"internalTls": False}) == installer_values.internal_tls_block({}) == ""


def test_the_fresh_install_cr_holds_the_values_internal_tls(pulled):
    """What installer#1145 (0.3.704) changed: the CR a fresh install writes carries internalTls."""
    cr = installer_render.self_cr(pulled, {**_values(), "registryAuth": {"username": "x"}})
    assert cr["spec"]["internalTls"]["certManagerNamespace"] == CRN and cr["spec"]["features"]["internalTls"] is True


def _without_the_pick(tmp_path, pulled) -> str:
    """The recorded installer with internalTls dropped from its fresh-install pick, as 0.3.703 and older have it."""
    import shutil
    root = tmp_path / "older"
    shutil.copytree(pulled, root)
    sb = root / "installer" / "templates" / "self-bootstrap.yaml"
    text = sb.read_text()
    assert text.count('"registryAuth" "internalTls" }}') == 1
    sb.write_text(text.replace('"registryAuth" "internalTls" }}', '"registryAuth" }}'))
    return str(root)


def test_falsifier_an_installer_that_drops_internal_tls_is_refused_before_any_cluster(tmp_path, pulled):
    with pytest.raises(installer_render.Refused) as e:
        installer_render.check(_without_the_pick(tmp_path, pulled), _values(), CRN)
    assert "does not hold the run's spec.internalTls.certManagerNamespace='cert-manager' (the CR: None)" in str(e.value)


def _with_spec_edit(tmp_path, pulled, line: str) -> str:
    """The recorded installer whose fresh-install CR is edited by one template line after its pick."""
    import shutil
    root = tmp_path / "edited"
    shutil.copytree(pulled, root)
    sb = root / "installer" / "templates" / "self-bootstrap.yaml"
    text = sb.read_text()
    at = '{{- $_ := set $spec "bootstrap" (dict "coreProvider" (dict "enabled" false)) }}\n'
    assert text.count(at) == 1
    sb.write_text(text.replace(at, at + line + "\n"))
    return str(root)


@pytest.mark.parametrize("line,why", [
    ('{{- $_ := set $spec "features" (omit $spec.features "internalTls") }}',
     "spec.features.internalTls=True (the CR: None)"),
    ('{{- $_ := set $spec "features" (set (deepCopy $spec.features) "internalTls" false) }}',
     "spec.features.internalTls=True (the CR: False)"),
    ('{{- $_ := unset $spec "features" }}', "spec.features.internalTls=True (the CR: None)"),
    ('{{- $_ := set $spec "componentValues" (omit $spec.componentValues "snowplow") }}',
     "spec.componentValues.snowplow.image.tag='1.12.74' (the CR: None)"),
    ('{{- $_ := set $spec "exposure" (dict "type" "LoadBalancer") }}', "spec.exposure.type='Gateway' (the CR: 'LoadBalancer')"),
], ids=["feature dropped", "feature off", "features gone", "a component's values gone", "another value"])
def test_falsifier_a_cr_that_does_not_hold_the_runs_values_is_refused(tmp_path, pulled, line, why):
    """#1598 review B1: the CR decides what is installed; what it does not hold of the run's values is named."""
    with pytest.raises(installer_render.Refused) as e:
        installer_render.check(_with_spec_edit(tmp_path, pulled, line), _values(), CRN)
    assert str(e.value).startswith("the CR a fresh install of this installer (composition.krateo.io/v"), str(e.value)
    assert why in str(e.value), str(e.value)


def test_the_install_only_values_are_not_wanted_of_the_cr(pulled):
    """bootstrap.coreProvider.enabled is true in the values and false in the CR by design; core-provider is the engine
    subchart's: the recorded installer's CR passes with both in the run's values."""
    v = _values()
    assert v["bootstrap"]["coreProvider"]["enabled"] is True and "core-provider" in v
    cr = installer_render.self_cr(pulled, {**v, "registryAuth": {**v["registryAuth"], "username": "x"}})
    assert cr["spec"]["bootstrap"]["coreProvider"]["enabled"] is False and "core-provider" not in cr["spec"]
    assert installer_render.effective_spec(v, cr, CRN) is cr["spec"]


@pytest.mark.parametrize("rec", [
    {}, {"kind": "installer_render_check", "ok": True, "rendered": []}, {"kind": "installer_render_check", "ok": False,
                                                                        "rendered": ["a"]},
    {"kind": "installer_render_check", "ok": True}, {"kind": "something", "ok": True, "rendered": ["a"]},
    {"kind": "installer_render_check", "ok": True, "rendered": "a"}, [], None, "x", 3, ["a"],
    {"kind": "installer_render_check", "ok": "yes", "rendered": ["a"]}])
def test_falsifier_only_a_render_that_passed_names_what_to_expect(tmp_path, rec):
    p = tmp_path / "r.json"
    p.write_text(json.dumps(rec))
    with pytest.raises(installer_render.Refused, match="is not the record of a composition render that passed"):
        installer_render.rendered_names(str(p))
    p.write_text(json.dumps({"kind": "installer_render_check", "ok": True, "rendered": ["b", "a"]}))
    assert installer_render.rendered_names(str(p)) == ["b", "a"]


@pytest.mark.parametrize("crn,why", [
    ("elsewhere", "internalTls.certManagerNamespace is 'cert-manager', and cert-manager reads cluster CA Secrets from "
                  "'elsewhere'"),
    ("", "features.internalTls is on and cert-manager's --cluster-resource-namespace is not known"),
])
def test_falsifier_a_namespace_that_is_not_where_cert_manager_reads_is_refused(pulled, crn, why):
    with pytest.raises(installer_render.Refused) as e:
        installer_render.check(pulled, _values(), crn)
    assert why in str(e.value)


def test_not_held_names_every_leaf_the_cr_does_not_hold():
    want = {"certManagerNamespace": "cert-manager", "deep": {"a": 1, "b": 2}}
    assert installer_render.not_held(want, {"certManagerNamespace": "cert-manager", "deep": {"a": 1, "b": 2, "c": 3}}, "t") == []
    assert installer_render.not_held(want, {"deep": {"a": 1}}, "t") == [
        "t.certManagerNamespace='cert-manager' (the CR: None)", "t.deep.b=2 (the CR: None)"]
    assert installer_render.not_held(want, None, "t") == [
        "t.certManagerNamespace='cert-manager' (the CR: None)", "t.deep.a=1 (the CR: None)", "t.deep.b=2 (the CR: None)"]


# ─── the composition render ─────────────────────────────────────────────────

def test_the_runs_effective_spec_renders_every_enabled_component(pulled):
    rec = installer_render.check(pulled, _values(), CRN)
    assert rec["ok"] and rec["rendered"] == rec["enabled"] and len(rec["rendered"]) > 40
    assert {"trust-manager", "internal-ca-bundle", "cert-manager-issuers", "cert-manager"} <= set(rec["rendered"])
    assert rec["cr_api_version"] == "composition.krateo.io/v" + ia.RECORDED.replace(".", "-")
    ln = installer_render.lines(rec)
    assert ln[0].startswith(f"installer render: rendered {len(rec['rendered'])} of {len(rec['rendered'])} enabled")
    assert ln[1] == ('installer internalTls in that CR: {"certManagerNamespace": "cert-manager"}; cert-manager\'s '
                     "--cluster-resource-namespace: 'cert-manager'")
    # the bypass is in the check's in-memory copy only: the pulled chart s_install installs is untouched
    assert not any(n.startswith("00-bench") for n in os.listdir(os.path.join(pulled, "installer", "templates")))


@pytest.mark.parametrize("case", ["no_cert_manager_namespace", "no_target_namespaces"])
def test_falsifier_what_the_installer_refuses_is_refused_before_any_cluster(pulled, case):
    values = _values()
    if case == "no_cert_manager_namespace":
        values.pop("internalTls")
    else:
        values["componentValues"].pop("internal-ca-bundle")
    with pytest.raises(installer_render.Refused) as e:
        installer_render.check(pulled, values, CRN)
    why = str(e.value)
    assert why.startswith("the installer refuses to render its components with the run's effective spec"), why
    assert GUARDS["certManagerNamespace" if case == "no_cert_manager_namespace" else "target"] in why, why
    assert "installer/templates/compositions.yaml:" in why, why


def test_falsifier_a_bypass_that_does_not_take_cannot_pass(pulled, monkeypatch):
    """Under a name that sorts after _helpers.tpl the chart's own gate wins and emits nothing offline: refused,
    never read as a pass."""
    monkeypatch.setattr(installer_render, "BYPASS_TEMPLATE", "templates/zz-bench-render-check.tpl")
    with pytest.raises(installer_render.Refused, match=r"^rendered 0 components"):
        installer_render.check(pulled, _values(), CRN)


def test_falsifier_a_component_enabled_but_not_rendered_is_named(pulled, monkeypatch):
    monkeypatch.setattr(installer_render, "BYPASS", installer_render.BYPASS.replace(
        '{{- if eq (include "inst.compEnabled" (list $top $c)) "true" -}}',
        '{{- if and (ne $c.name "trust-manager") (eq (include "inst.compEnabled" (list $top $c)) "true") -}}'))
    with pytest.raises(installer_render.Refused, match=r"^rendered (\d+) of (\d+) enabled components; not rendered: "
                                                       r"trust-manager$"):
        installer_render.check(pulled, _values(), CRN)


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
    rec = installer_render.check(pulled, _values(), CRN)
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


# ─── run606.sh: the render before any cluster, internalTls installed from the values ──

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


def test_run606_renders_before_any_cluster_installs_internal_tls_from_the_values_and_never_edits_the_cr(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "run complete" in log, log[-3000:]
    assert log.index("installer render: rendered ") < log.index("=== step up ===")
    v = yaml.safe_load((tmp / "out" / "installer-values.yaml").read_text())
    assert v["features"]["internalTls"] is True and v["internalTls"] == {"certManagerNamespace": "cert-manager"}
    rec = json.loads((tmp / "out" / "installer-render.json").read_text())
    assert rec["internal_tls"] == {"certManagerNamespace": "cert-manager"}
    from tests.test_run606_real_sequence import _calls
    calls = [(c["tool"], c["argv"]) for c in _calls(state)]
    install = [a for t, a in calls if t == "helm" and "install" in a and "installer" in a and "template" not in a]
    assert len(install) == 1 and str(tmp / "out" / "installer-values.yaml") in install[0], install
    # nothing writes to the Installer CR after the install
    writes = ("patch", "apply", "replace", "edit", "annotate", "label")
    assert [a for t, a in calls if t == "kubectl" and any(w in a for w in writes)
            and any(str(x).startswith("installers.") for x in a)] == []
    assert not (tmp / "out" / "installer-cr-patch.json").exists()
    _no_registry_username_kept(tmp, state)


def _no_registry_username_kept(tmp, state):
    """The installer is installed with the registry username, piped in: no file the run keeps holds it."""
    from tests.conftest import FAKE_GHCR_USER
    assert json.loads((state / "helm-registry-auth.json").read_text())["username"] == FAKE_GHCR_USER   # it was used
    kept = [f for f in (tmp / "out").rglob("*") if f.is_file() and FAKE_GHCR_USER.encode() in f.read_bytes()]
    assert kept == []


def test_falsifier_run606_refuses_an_installer_that_drops_internal_tls_before_up(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1", FAKE_INSTALLER_DROPS_TLS="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0, log[-3000:]
    assert ("registry FAILED: installer render: the CR a fresh install of this installer") in log, log[-3000:]
    assert "does not hold the run's spec.internalTls.certManagerNamespace='cert-manager' (the CR: None)" in log
    assert "=== step up ===" not in log


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


@pytest.mark.parametrize("env_extra,why", [
    ({"FAKE_LIVE_DROPS": "settings-page"}, "rendered, not declared: settings-page"),
    ({"FAKE_LIVE_ADDS": "surprise"}, "declared, not rendered: surprise"),
])
def test_falsifier_a_live_cr_that_declares_another_set_fails_the_platform_step(harness, env_extra, why):  # noqa: F811
    """#1598 review B1: what the live Installer CR declares is held to what this run rendered: a feature lost (or gained)
    on the way to the cluster is named, and never only shortens what is waited for."""
    env, state, _, tmp = harness
    p = _run606(env, **env_extra)               # fails at the first poll that reads the CR, not at the timeout
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0, log[-3000:]
    reason = (tmp / "out" / "platform-reason.txt").read_text()
    assert reason.startswith("the live Installer CR is not the run's: it declares ") and why in reason, reason
    assert "TIMEOUT after" not in log
    assert "=== step env ===" not in log


def test_run606s_dry_pass_renders_the_cr_the_values_write(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    assert "installer render: rendered " in log and "installer internalTls in that CR: " in log
