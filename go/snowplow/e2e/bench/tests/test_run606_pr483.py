"""Bench run 5: install the RELEASED installer (0.3.425, #483 merged) the
ordinary OCI way, preflight the registry chart (resolves + carries the #483
pins) before any cluster exists, and assert the #483 outcome (verify483,
headline item 4) before the 50K measurement. Local-chart mode is refused:
it cannot work for this installer (run 4; self-bootstrap.yaml:215).

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


def _env483(env, chart=None):
    """OCI-version mode (run 5). `chart` is accepted and ignored for callers
    written against the run-4 local-chart signature."""
    env = dict(env)
    for k in ("INSTALLER_CHART_DIR", "INSTALLER_GIT_REF"):
        env.pop(k, None)
    env.update(INSTALLER_VERSION="0.3.425", VERIFY483="1")
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


def _inst(*names):
    """The Installer CR's summary, declaring (spec.components, feature on) the
    component CRs named — what diag606._leaf_summaries attaches as `declares`."""
    s = _s("installer")
    s["declares"] = [{"kind": "X", "name": n, "deps": []} for n in names]
    return s


DECLARED = ("authn", "krateo-observability", "alert-provider-crd")


def test_evaluate483_pass_and_item5_recorded_only():
    leaves = [_s("authn"), _s("krateo-observability"), _s("alert-provider-crd")]
    r = diag606.evaluate483([_inst(*DECLARED)], leaves,
                            [_s("authn"), _s("alert-provider-crd")], True,
                            [{"metadata": {"name": "sre-pod-crashloop"}}])
    assert r["verdict"] == "PASS"
    assert r["recorded"]["as_expected"] is True
    # item 5 never gates: no Alert CRs at all still PASSes 1-4
    r = diag606.evaluate483([_inst(*DECLARED)], leaves,
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
    r = diag606.evaluate483([_inst(*DECLARED)], L, C, crd[0], [])
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



# ─── run606.sh end to end (fake tools), OCI-version mode ────────────────────


def _run(env):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                          capture_output=True, text=True, timeout=300)


def test_oci_run_passes_verify483_headline_first_and_continues(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env["FAKE_483"] = "pass"
    env["FAKE_KOG"] = "1"         # run 3's private-registry 401 — known, unrelated
    p = _run(env)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    assert "registry preflight OK, #483 pins present" in out
    # both layers read: the Installer's Ready.message names the CD child
    assert "1 of 41 managed children are not ready" in out
    assert ("ALLOWED(known): compositiondefinitions/krateo-system/github-provider-kog"
            "[response status code 401]") in out
    assert "[platform] verdict READY (with allow-listed known-unrelated" in out
    # HEADLINE: item 4 printed FIRST among the verify483 result lines
    vlines = [ln for ln in out.splitlines() if ln.startswith("[verify483]")]
    assert vlines[0] == "[verify483] HEADLINE (4) PASS alert-provider ABSENT (present = gate too wide)"
    assert vlines[1].startswith("[verify483] (4) PASS")
    v = json.loads((tmp / "out" / "verify483.json").read_text())
    assert v["verdict"] == "PASS" and v["headline"]["result"] == "PASS"
    assert v["checks"][0]["item"] == 4
    assert sorted(c["item"] for c in v["checks"]) == [1, 2, 3, 4, 6]
    # the definition 401s, so its declared CR is never created: both are excused
    assert [a["id"] for a in v["known_unrelated_allowed"]] == [
        "compositiondefinitions/krateo-system/github-provider-kog",
        "declared/GithubProviderKog/github-provider-kog"]
    assert v["recorded"]["sre_pod_crashloop_present"] is True
    assert "[verify483] (5) RECORDED" in out
    # it went on into the measurement
    assert "=== step data ===" in p.stderr and "=== step measure ===" in p.stderr
    helm = [c["argv"] for c in _calls(state) if c["tool"] == "helm"]
    assert helm[0][:2] == ["show", "chart"] and "0.3.425" in helm[0]
    install = [a for a in helm if "install" in a]
    assert len(install) == 1
    assert "oci://ghcr.io/krateo-platformops/charts/installer" in install[0]
    assert install[0][install[0].index("--version") + 1] == "0.3.425"
    pre = json.loads((tmp / "out" / "installer-preflight.json").read_text())
    assert pre["ok"] and pre["resolved_version"] == "0.3.425"
    assert pre["requirements"]


@pytest.mark.parametrize("envkv,needle", [
    ({"FAKE_PINS": "bad"}, "alert-provider-crd.feature"),
    ({"FAKE_CHART_MISSING": "1"}, "helm show chart"),
])
def test_registry_preflight_refuses_before_any_cluster_call(harness, envkv, needle):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(envkv)
    p = _run(env)
    assert p.returncode != 0
    assert "installer registry preflight REFUSED" in p.stderr and needle in p.stderr
    calls = _calls(state)
    assert not [c for c in calls if c["tool"] in ("gcloud", "kubectl")]
    assert all(c["argv"][:1] in (["show"], ["pull"]) for c in calls if c["tool"] == "helm")
    assert "teardown" not in p.stderr


def test_pins_are_only_required_with_verify483(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env["VERIFY483"] = "0"
    env["FAKE_PINS"] = "bad"           # an older installer: fine without VERIFY483
    p = _run(env)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    assert "=== step verify483 ===" not in p.stderr


@pytest.mark.parametrize("var", ["INSTALLER_GIT_REF", "INSTALLER_CHART_DIR"])
def test_local_chart_mode_is_refused(harness, var):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env[var] = "fix/482-alert-crd-gate" if var == "INSTALLER_GIT_REF" else str(tmp)
    p = _run(env)
    assert p.returncode == 2
    assert "self-bootstrap.yaml:215" in p.stderr
    assert not _calls(state)


def test_gate_too_wide_fails_verify483_with_diagnostics_and_teardown(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env["FAKE_483"] = "wide"
    p = _run(env)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "[verify483] HEADLINE (4) FAIL alert-provider ABSENT" in out
    assert "=== step data ===" not in p.stderr          # no measurement
    diag = tmp / "out" / "diagnostics"
    assert (diag / "focus-483.json").exists()
    assert (diag / "chart-inspector-errors.txt").exists()
    readme = (diag / "README.txt").read_text()
    assert "self-bootstrap.yaml:215" in readme and "chart-inspector clean" in readme
    focus = json.loads((diag / "focus-483.json").read_text())
    assert focus["components"]["alert-provider-crd"]["component_cr"]["Ready"]["status"] == "True"
    gcl = [c["argv"][:4] for c in _calls(state) if c["tool"] == "gcloud"]
    assert ["container", "clusters", "delete", RUN_NAME] in gcl


def test_oci_preflight_unit(tmp_path):
    """oci_preflight against a fake `helm` runner (no subprocess)."""
    import types

    def runner(argv, **kw):
        if argv[:3] == ["helm", "show", "chart"]:
            return types.SimpleNamespace(returncode=0, stdout="version: 0.3.425\n", stderr="")
        if argv[:2] == ["helm", "pull"]:
            d = argv[argv.index("-d") + 1]
            src = _chart(tmp_path, version="0.3.425")
            import shutil
            shutil.copytree(src, f"{d}/installer")
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(argv)
    res = installer_src.oci_preflight("oci://x/installer", "0.3.425",
                                      str(tmp_path / "pulled"), run=runner)
    assert res["ok"], res

    def wrong_version(argv, **kw):
        return types.SimpleNamespace(returncode=0, stdout="version: 0.3.424\n", stderr="")
    res = installer_src.oci_preflight("oci://x/installer", "0.3.425",
                                      str(tmp_path / "p2"), run=wrong_version)
    assert not res["ok"] and "resolved version" in res["failures"][0]
