"""Bench run 4: install the installer from a LOCAL chart (installer#483 branch
fix/482-alert-crd-gate), preflight the branch before any cluster exists, and
assert the #483 outcome (verify483) before the 50K measurement.

Uses the fake gcloud/kubectl/helm harness from test_run606_real_sequence.
"""

from __future__ import annotations

import json
import subprocess

import pytest
import yaml

from bench import diag606, installer_src
from tests.test_run606_real_sequence import (  # noqa: F401  (fixture import)
    BENCH_DIR, KCTX, RUN_NAME, _calls, harness)


def _chart(tmp_path, *, feature="portal", obs_deps=("alert-provider-crd",),
           version="0.0.0-pr483", vendored=True):
    chart = tmp_path / "installer-src" / "chart"
    (chart / "files").mkdir(parents=True)
    (chart / "Chart.yaml").write_text(yaml.safe_dump({
        "apiVersion": "v2", "name": "installer", "version": version,
        "dependencies": [{"name": "core-provider", "version": "2.13.24"}]}))
    if vendored:
        (chart / "charts").mkdir()
        (chart / "charts" / "core-provider-2.13.24.tgz").write_bytes(b"x")
    (chart / "files" / "component-pins.yaml").write_text(yaml.safe_dump({"components": [
        {"name": "alert-provider-crd", "feature": feature},
        {"name": "alert-provider", "feature": "specialistAgents"},
        {"name": "krateo-observability", "feature": "portal", "deps": list(obs_deps)},
    ]}))
    return chart


def _env483(env, chart):
    env = dict(env)
    env.pop("INSTALLER_VERSION", None)
    env.update(INSTALLER_CHART_DIR=str(chart), VERIFY483="1")
    return env


# ─── preflight (pure) ───────────────────────────────────────────────────────


def test_preflight_accepts_the_483_branch_shape(tmp_path):
    res = installer_src.preflight(str(_chart(tmp_path)))
    assert res["ok"], res


@pytest.mark.parametrize("kw,needle", [
    ({"feature": "specialistAgents"}, "alert-provider-crd.feature"),
    ({"obs_deps": ("clickhouse-operator",)}, "krateo-observability.deps"),
    ({"version": "CHART_VERSION"}, "not stamped"),
    ({"vendored": False}, "not vendored"),
])
def test_preflight_refuses_a_wrong_branch(tmp_path, kw, needle):
    res = installer_src.preflight(str(_chart(tmp_path, **kw)))
    assert not res["ok"] and any(needle in f for f in res["failures"]), res


# ─── verify483 (pure) ───────────────────────────────────────────────────────


def _s(name, ready=True, synced=True, kind="X"):
    return {"kind": kind, "name": name,
            "Ready": {"status": "True" if ready else "False"},
            "Synced": {"status": "True" if synced else "False"}}


def test_evaluate483_pass_and_item5_recorded_only():
    leaves = [_s("authn"), _s("krateo-observability"), _s("alert-provider-crd")]
    r = diag606.evaluate483([_s("installer")], leaves,
                            [_s("authn"), _s("alert-provider-crd")], True,
                            [{"metadata": {"name": "sre-pod-crashloop"}}])
    assert r["verdict"] == "PASS"
    assert r["recorded"]["as_expected"] is True
    # item 5 never gates: no Alert CRs at all still PASSes 1-4
    r = diag606.evaluate483([_s("installer")], leaves,
                            [_s("alert-provider-crd")], True, [])
    assert r["verdict"] == "PASS" and r["recorded"]["as_expected"] is False


@pytest.mark.parametrize("mut,item", [
    (lambda L, C, crd: L.__setitem__(1, _s("krateo-observability", synced=False)), 1),
    (lambda L, C, crd: crd.__setitem__(0, False), 2),
    (lambda L, C, crd: C.pop(), 3),
    (lambda L, C, crd: C.append(_s("alert-provider")), 4),
    (lambda L, C, crd: L.append(_s("alert-provider")), 4),
])
def test_evaluate483_each_gating_item_fails_alone(mut, item):
    L = [_s("authn"), _s("krateo-observability"), _s("alert-provider-crd")]
    C = [_s("authn"), _s("alert-provider-crd")]
    crd = [True]
    mut(L, C, crd)
    r = diag606.evaluate483([_s("installer")], L, C, crd[0], [])
    assert r["verdict"] == "FAIL"
    assert [c["item"] for c in r["checks"] if not c["pass"]] == [item]


def test_chart_inspector_error_is_kept_verbatim():
    long_msg = ("create failed: generating RBAC using chart-inspector: 500 - "
                + "x" * 900 + " no matches for kind \"Alert\" in version "
                "\"observability.krateo.io/v1alpha1\" ensure CRDs are installed first")
    s = diag606.summarize_conditions({"kind": "KrateoObservability", "metadata": {
        "name": "krateo-observability"}, "status": {"conditions": [
            {"type": "Synced", "status": "False", "message": long_msg}]}})
    errs = diag606.chart_inspector_errors([s])
    assert len(errs) == 1 and long_msg in errs[0]


# ─── run606.sh end to end (fake tools) ──────────────────────────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_local_chart_run_passes_verify483_and_continues_to_measurement(harness):
    env, state, ambient, tmp = harness
    chart = _chart(tmp)
    env = _env483(env, chart)
    env["FAKE_483"] = "pass"
    env["FAKE_KOG"] = "1"         # run 3's private-registry 401 — known, unrelated
    p = _run(env)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    assert "installer source: LOCAL chart" in out
    # both layers read: the Installer's Ready.message names the CD child
    assert "1 of 41 managed children are not ready" in out
    assert ("ALLOWED(known): compositiondefinitions/krateo-system/github-provider-kog"
            "[response status code 401]") in out
    assert "[platform] verdict READY (with allow-listed known-unrelated" in out
    v = json.loads((tmp / "out" / "verify483.json").read_text())
    assert v["verdict"] == "PASS"
    assert [a["id"] for a in v["known_unrelated_allowed"]] == [
        "compositiondefinitions/krateo-system/github-provider-kog"]
    assert "[verify483] KNOWN-UNRELATED not ready (allowed)" in out
    assert v["recorded"]["sre_pod_crashloop_present"] is True
    assert "[verify483] (5) RECORDED" in out
    # it went on into the measurement
    assert "=== step data ===" in p.stderr and "=== step measure ===" in p.stderr
    helm_install = [c["argv"] for c in _calls(state)
                    if c["tool"] == "helm" and "install" in c["argv"]]
    assert len(helm_install) == 1
    assert str(chart) in helm_install[0] and "--version" not in helm_install[0]
    pre = json.loads((tmp / "out" / "installer-preflight.json").read_text())
    assert pre["ok"]


def test_wrong_branch_refuses_before_any_cloud_call(harness):
    env, state, ambient, tmp = harness
    env = _env483(env, _chart(tmp, feature="specialistAgents"))
    p = _run(env)
    assert p.returncode != 0
    assert "installer preflight REFUSED" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]
    assert "teardown" not in p.stderr


def test_gate_too_wide_fails_verify483_with_diagnostics_and_teardown(harness):
    env, state, ambient, tmp = harness
    env = _env483(env, _chart(tmp))
    env["FAKE_483"] = "wide"
    p = _run(env)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "[verify483] (4) FAIL alert-provider ABSENT" in out
    assert "=== step data ===" not in p.stderr          # no measurement
    diag = tmp / "out" / "diagnostics"
    assert (diag / "focus-483.json").exists()
    assert (diag / "chart-inspector-errors.txt").exists()
    focus = json.loads((diag / "focus-483.json").read_text())
    assert focus["components"]["alert-provider-crd"]["component_cr"]["Ready"]["status"] == "True"
    gcl = [c["argv"][:4] for c in _calls(state) if c["tool"] == "gcloud"]
    assert ["container", "clusters", "delete", RUN_NAME] in gcl
