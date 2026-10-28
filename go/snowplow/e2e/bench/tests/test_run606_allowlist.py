"""Run-4 amendment: readiness across BOTH layers (CompositionDefinitions and
component CRs, as named by the Installer CR's Ready.message), an allow-list
keyed by EXACT child id AND reason (github-provider-kog's private-registry
401), and a bundle whose readiness fields are queryable in both shapes."""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import diag606
from tests.test_run606_pr483 import _chart, _env483
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

KOG = "compositiondefinitions/krateo-system/github-provider-kog"
# Run 3's REAL Synced message for this CD (from its diagnostics bundle). The
# allow-list reason must match THIS text — "401 unauthorized" would not.
RUN3_KOG_MSG = (
    "observe failed: error getting chart info: failed to get chart after 5 attempts: "
    "failed to get chart: failed to get oci://ghcr.io/krateo-blueprints/charts/"
    "github-provider-kog: failed to resolve reference ghcr.io/krateo-blueprints/charts/"
    "github-provider-kog:0.3.2: HEAD \"https://ghcr.io/v2/krateo-blueprints/charts/"
    "github-provider-kog/manifests/0.3.2\": GET \"https://ghcr.io/token?scope="
    "repository%3Akrateo-blueprints%2Fcharts%2Fgithub-provider-kog%3Apull&service="
    "ghcr.io\": response status code 401: unauthorized")


def _sid(resource, name, ready=True, msg=""):
    return diag606.summarize_conditions({"kind": "K", "metadata": {
        "name": name, "namespace": "krateo-system"}, "status": {"conditions": [
            {"type": "Ready", "status": "True" if ready else "False", "message": msg}]}},
        resource)


def _inst(ready=True, msg="", declares=diag606.REQUIRED_READY):
    """The Installer CR's summary with its `declares` (diag606._leaf_summaries
    attaches it from spec.components): the component CRs it will create."""
    s = _sid("installers", "installer", ready, msg)
    s["declares"] = [{"kind": "K", "name": n, "deps": []} for n in declares]
    return s


def test_parse_installer_message_children():
    msg = (f"2 of 41 managed children are not ready: {KOG}, "
           "krateoobservabilities/krateo-system/krateo-observability")
    assert diag606.parse_not_ready_children(msg) == [
        KOG, "krateoobservabilities/krateo-system/krateo-observability"]
    assert diag606.parse_not_ready_children("") == []


def test_platform_state_allows_only_the_exact_child_with_the_exact_reason():
    inst = [_inst(False, f"1 of 41 managed children are not ready: {KOG}")]
    leaves = [_sid("x", n) for n in diag606.REQUIRED_READY]
    kog401 = _sid("compositiondefinitions", "github-provider-kog", False, RUN3_KOG_MSG)
    st = diag606.platform_state(inst, leaves, [kog401])
    assert st["ok"] and [a["id"] for a in st["allowed_not_ready"]] == [KOG]

    # same child, different reason -> unexpected
    kog500 = _sid("compositiondefinitions", "github-provider-kog", False, "500 boom")
    st = diag606.platform_state(inst, leaves, [kog500])
    assert not st["ok"] and st["unexpected_not_ready"][0]["id"] == KOG

    # any OTHER not-ready child, even one named ONLY by the Installer message
    inst2 = [_inst(False, f"2 of 41 managed children are not ready: {KOG}, "
                          "compositiondefinitions/krateo-system/oasgen-provider")]
    st = diag606.platform_state(inst2, leaves, [kog401])
    assert not st["ok"]
    assert [u["id"] for u in st["unexpected_not_ready"]] == [
        "compositiondefinitions/krateo-system/oasgen-provider"]

    # a required component not Ready -> not ok even with nothing unexpected
    leaves2 = [_sid("x", "snowplow", False)] + leaves[1:]
    st = diag606.platform_state([_inst()], leaves2, [])
    assert not st["ok"] and st["required_ready"]["snowplow"] is False


def test_allow_list_env_override(monkeypatch):
    monkeypatch.setenv("BENCH_ALLOW_NOT_READY", "a/b/c=x y; d/e/f=z")
    assert diag606.allow_list_from_env() == (("a/b/c", "x y"), ("d/e/f", "z"))
    monkeypatch.setenv("BENCH_ALLOW_NOT_READY", "")
    assert diag606.allow_list_from_env() == ()
    monkeypatch.delenv("BENCH_ALLOW_NOT_READY")
    assert diag606.allow_list_from_env() == diag606.DEFAULT_ALLOW_NOT_READY


def test_evaluate483_item1_is_observability_only_and_item6_gates_others():
    obs = _sid("krateoobservabilities", "krateo-observability")
    obs["Synced"] = {"status": "True"}
    kog = _sid("compositiondefinitions", "github-provider-kog", False, RUN3_KOG_MSG)
    cds = [_sid("compositiondefinitions", "alert-provider-crd"), kog]
    r = diag606.evaluate483([_inst(False, f"1 of 41 managed children are not ready: {KOG}",
                                   declares=("krateo-observability",))],
                            [obs], cds, True, [])
    assert r["verdict"] == "PASS", r["checks"]
    assert [a["id"] for a in r["known_unrelated_allowed"]] == [KOG]
    cds.append(_sid("compositiondefinitions", "oasgen-provider", False, "timeout"))
    r = diag606.evaluate483([_inst(declares=("krateo-observability",))], [obs], cds, True, [])
    assert r["verdict"] == "FAIL"
    assert [c["item"] for c in r["checks"] if not c["pass"]] == [6]


def test_bundle_shape_is_queryable_both_ways():
    s = _sid("compositiondefinitions", "github-provider-kog", False, "401 unauthorized")
    assert s["Ready"]["status"] == "False" and s["id"] == KOG            # flattened
    assert any(c["type"] == "Ready" and c["status"] == "False"           # raw API shape
               for c in s["status"]["conditions"])
    assert ".status.conditions[]" in diag606.BUNDLE_README
    assert ".Ready.status" in diag606.BUNDLE_README


@pytest.mark.parametrize("envkv,needle", [
    ({"FAKE_KOG": "1", "FAKE_KOG_REASON": "500 internal error"}, KOG),
    ({"FAKE_KOG": "1", "FAKE_EXTRA_NOT_READY": "1"},
     "compositiondefinitions/krateo-system/oasgen-provider"),
    # run 17: the Installer CR is Ready and every CR that exists is Ready, but
    # it declares a page (deps: [portal]) it has not created yet
    ({"FAKE_DECLARED_LATER": "1"}, "declared/DashboardPage/dashboard-page"),
])
def test_platform_fails_on_any_child_outside_the_allow_list(harness, envkv, needle):
    env, state, ambient, tmp = harness
    env = _env483(env, _chart(tmp))
    env["FAKE_483"] = "pass"
    env.update(envkv)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert f"NOT READY: {needle}" in out
    assert "=== step verify483 ===" not in p.stderr
    assert "=== step data ===" not in p.stderr
    diag = tmp / "out" / "diagnostics"
    assert (diag / "README.txt").exists()
    st = json.loads((diag / "platform-state.json").read_text())
    assert needle in [u["id"] for u in st["unexpected_not_ready"]]
