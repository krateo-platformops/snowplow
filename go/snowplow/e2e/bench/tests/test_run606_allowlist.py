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


_OBSERVED = {"assertedReady": 0, "noEvidenceBelow": 0, "noReadyCondition": 1, "notObserved": 0}


def _cr(name, ready, reason="", synced="True", resource="internalcabundles", children=_OBSERVED):
    conds = [{"type": "Ready", "status": ready, "reason": reason}] if ready is not None else []
    conds += [{"type": "Synced", "status": synced, "reason": "ReconcileSuccess"}] if synced is not None else []
    status = {"conditions": conds}
    if children is not None:
        status["unevaluatedChildren"] = children
    return diag606.summarize_conditions({"kind": "K", "metadata": {"name": name, "namespace": "krateo-system"},
                                         "status": status}, resource)


CA = "internalcabundles/krateo-system/internal-ca-bundle"


def _required():
    return [_sid("x", n) for n in diag606.REQUIRED_READY]


def _held(st, ident=CA):
    """`ident` holds the platform, and nothing says it was accepted: not the record, not the line."""
    assert not st["ok"] and [u["id"] for u in st["unexpected_not_ready"]] == [ident], st["unexpected_not_ready"]
    assert st["not_health_evaluated"] == [] and "accepted" not in diag606.platform_line(st)


def test_a_reconciled_component_whose_health_cannot_be_read_does_not_hold_the_platform():
    """krateo-057's InternalCaBundle, read-only 2026-11-24: Ready=Unknown/ZeroEvidence, Synced=True,
    unevaluatedChildren.notObserved 0; its Installer CR: Ready=True, two children not health-evaluated."""
    inst = [_inst(True, "Composition is up-to-date (2 of 151 managed children not health-evaluated)")]
    st = diag606.platform_state(inst, _required() + [_cr("internal-ca-bundle", "Unknown", "ZeroEvidence")], [])
    assert st["ok"] and st["not_health_evaluated"] == [CA] and st["unexpected_not_ready"] == []
    assert "reconciled, health not evaluated (accepted): " + CA in diag606.platform_line(st)


@pytest.mark.parametrize("what,cr", [
    ("no Ready condition (the rehearsal of 2026-11-24)", _cr("internal-ca-bundle", None, synced=None)),
    ("a child could not be observed", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence",
                                          children={**_OBSERVED, "notObserved": 1})),
    ("every child could not be observed", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence",
                                              children={"noReadyCondition": 0, "notObserved": 3})),
    ("it does not say why", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", children=None)),
    ("its reasons carry no notObserved", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence",
                                             children={"noReadyCondition": 1})),
    ("notObserved is null", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", children={"notObserved": None})),
    ("notObserved is false", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", children={"notObserved": False})),
    ("its reasons are not an object", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", children=[0])),
    ("the children are forbidden", _cr("internal-ca-bundle", "Unknown", "ChildrenForbidden")),
    ("the inventory was never recorded", _cr("internal-ca-bundle", "Unknown", "InventoryNotRecorded")),
    ("its own observe failed", _cr("internal-ca-bundle", "Unknown", "ObserveFailed")),
    ("not observed", _cr("internal-ca-bundle", "Unknown", "NotObserved")),
    ("Unknown with no reason", _cr("internal-ca-bundle", "Unknown", "")),
    ("the reason spelled otherwise", _cr("internal-ca-bundle", "Unknown", "ZeroEvidenceYet")),
    ("False with the reason", _cr("internal-ca-bundle", "False", "ZeroEvidence")),
    ("not reconciled", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", synced="False")),
    ("reconcile unknown", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", synced="Unknown")),
    ("never reconciled", _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", synced=None)),
])
def test_falsifier_every_other_state_still_holds_the_platform(what, cr):
    _held(diag606.platform_state([_inst()], _required() + [cr], []))


#: core-provider 2.13.127's own forms (childhealth.go: rollupManagedChildren, withUnobservable, withUnreadable, joinCap)
def _not_ready(names, more=""):
    return f"{len(names)} of 138 managed children are not ready: " + ", ".join(names) + more


def _failing(names, more=""):
    return "managed children not healthy: " + ", ".join(names) + more


def _unobservable(names, more=""):
    return (f"Composition is up-to-date ({len(names)} of 138 managed children could NOT BE OBSERVED by their own controller, "
            "whose Observe failed; the error is on each child's Synced condition: " + ", ".join(names) + more + ")")


def _unreadable(names, more=""):
    return (f"Composition is up-to-date ({len(names)} of 138 managed children could NOT BE READ \u2014 this controller's "
            "ServiceAccount was refused access, which is a gap in the generated RBAC rather than a problem with the "
            "children: " + ", ".join(names) + more + ")")


@pytest.mark.parametrize("msg", [
    _not_ready([CA]), _not_ready(["x/krateo-system/other", CA]), _failing([CA]),
    _failing(["x/krateo-system/other (Ready=False: boom)", CA + " (Ready=False: no default config found)"]),
    _failing(["x/krateo-system/other"]) + "; also unhealthy: " + CA,
    _unobservable([CA]), _unreadable(["x/krateo-system/other", CA]), _unreadable([CA]),
], ids=["not-ready", "not-ready-list", "failing", "failing-detail", "also-unhealthy", "unobservable", "unreadable-list", "unreadable"])
def test_falsifier_one_the_installer_names_in_any_form_still_holds_the_platform(msg):
    """The Installer's read and ours can disagree for a moment; what it names wins."""
    st = diag606.platform_state([_inst(False, msg)], _required() + [_cr("internal-ca-bundle", "Unknown", "ZeroEvidence")], [])
    assert not st["ok"] and CA in [u["id"] for u in st["unexpected_not_ready"]]
    assert st["not_health_evaluated"] == [] and "accepted" not in diag606.platform_line(st)


_THREE = ["x/krateo-system/a", "x/krateo-system/b", "x/krateo-system/c"]


@pytest.mark.parametrize("msg", [_not_ready(_THREE, " (+4 more)"), _failing(_THREE, " (+1 more)"),
                                 _unobservable(_THREE, " (+12 more)"), _unreadable(_THREE, " (+7 more)")],
                         ids=["not-ready", "failing", "unobservable", "unreadable"])
def test_falsifier_none_is_accepted_while_the_installer_leaves_names_out(msg):
    """joinCap prints three names and a count: an id that is not printed may be one of the others."""
    st = diag606.platform_state([_inst(False, msg)], _required() + [_cr("internal-ca-bundle", "Unknown", "ZeroEvidence")], [])
    assert not st["ok"] and CA in [u["id"] for u in st["unexpected_not_ready"]]
    assert st["not_health_evaluated"] == []


def test_a_name_that_only_contains_the_id_is_not_the_id():
    """Its neighbour named by the Installer (same id plus a suffix or a prefix) says nothing about it."""
    inst = [_inst(False, _failing([CA + "-copy", "more" + CA, CA + ".v2", "x/" + CA]))]
    st = diag606.platform_state(inst, _required() + [_cr("internal-ca-bundle", "Unknown", "ZeroEvidence")], [])
    assert st["ok"] and st["not_health_evaluated"] == [CA]


def test_verify483_item_6_accepts_it_and_never_a_required_one():
    """evaluate483 reads the same set: a required component at ZeroEvidence fails item 6."""
    obs = _sid("krateoobservabilities", "krateo-observability")
    others = [x for x in _required() if x["name"] != "snowplow"]

    def item6(leaves):
        r = diag606.evaluate483([_inst()], leaves + [obs], [], True, [])
        return next(c for c in r["checks"] if c["item"] == 6)
    ca = _cr("internal-ca-bundle", "Unknown", "ZeroEvidence")
    assert item6(_required() + [ca])["pass"] is True
    c = item6(others + [_cr("snowplow", "Unknown", "ZeroEvidence", resource="snowplows"), ca])
    assert c["pass"] is False
    assert [u["id"] for u in c["detail"]["unexpected"]] == ["snowplows/krateo-system/snowplow"]


def test_the_summary_carries_why_a_composition_is_not_evaluated_only_when_the_api_says_it():
    assert _cr("x", "Unknown", "ZeroEvidence")["status"]["unevaluatedChildren"] == _OBSERVED
    assert "unevaluatedChildren" not in _cr("x", "Unknown", "ZeroEvidence", children=None)["status"]


def test_falsifier_the_acceptance_is_never_for_a_definition_or_a_required_component():
    cd = _cr("internal-ca-bundle", "Unknown", "ZeroEvidence", resource="compositiondefinitions")
    _held(diag606.platform_state([_inst()], _required(), [cd]), "compositiondefinitions/krateo-system/internal-ca-bundle")
    for name in diag606.REQUIRED_READY:
        leaves = [x for x in _required() if x["name"] != name] + [_cr(name, "Unknown", "ZeroEvidence", resource="xs")]
        st = diag606.platform_state([_inst()], leaves, [])
        _held(st, f"xs/krateo-system/{name}")
        assert st["required_ready"][name] is False


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
