"""Run 17 (installer 0.3.459): both arms passed the platform gate at 24/24
component CRs Ready, with the Installer CR at Ready=True "Composition is
up-to-date". Then the preflight refused: "(sidebar menu) -> None: the
shell rendered no sidebar Menu with routes".

The cause is the gate, not snowplow and not the menu discovery. 0.3.459
declares 38 components (spec.components, feature on). The installer emits a
layer only after that layer's deps are Ready, one reconcile at a time
(templates/compositions.yaml Pass B). portal-agents and the 13 page
compositions split out of the portal chart all have deps: [portal], so none
of them existed yet when the gate looked. Since portal 1.8.75 the sidebar is
the Menu krateo-system/sidebar-nav, whose apiRef RESTAction builds it per
caller at request time. Its namespaces stage is a userAccessFilter stage:
snowplow lists namespaces as its ServiceAccount and keeps those where the
caller may LIST flexes. Its pages stage then lists the flexes carrying
krateo.io/nav-path in each, as the caller. Each page chart brings its own
namespace, its page flexes AND the RoleBindings that let `devs` list them
(devs-read-<page>-pages; 057 2026-10-28). So the preflight's devs bench
user read a Menu with no items: the namespaces, pages and bindings it would
see did not exist yet.

The gate now requires every DECLARED component to exist and be Ready, and
the preflight records what the Menu was made of when it yields no route.
"""

from __future__ import annotations

import json
import pathlib

from bench import diag606, load606
from tests.test_navmodel import Fake, canon  # noqa: F401  (canon: the fake's key)

FIX = pathlib.Path(__file__).parent / "fixtures"
RUN17 = json.loads((FIX / "installer_0_3_459_run17.json").read_text())
NAV = json.loads((FIX / "portal_1_8_84_sidebar_nav_devs.json").read_text())


def _installer(ready=True, msg="Composition is up-to-date"):
    return {"kind": "Installer", "metadata": {"name": "installer", "namespace": "krateo-system"},
            "spec": {"features": RUN17["features"], "components": RUN17["components"]},
            "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False",
                                       "reason": "Available", "message": msg}]}}


def _leaf(kind, name, ready=True, reason="Available"):
    return diag606.summarize_conditions(
        {"kind": kind, "metadata": {"name": name, "namespace": "krateo-system"},
         "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False",
                                    "reason": reason, "message": ""}]}}, kind.lower() + "s")


def _inst_summary():
    o = _installer()
    s = diag606.summarize_conditions(o, "installers")
    s["declares"] = diag606.declared_components(o)
    return s


# ─── the gate, on run 17's own Installer CR ──────────────────────────────────


def test_declared_components_follow_the_installers_feature_rule():
    d = diag606.declared_components(_installer())
    assert len(RUN17["components"]) == 65 and len(d) == 38
    names = {c["name"] for c in d}
    assert {"portal", "portal-agents", "dashboard-page", "settings-page"} <= names
    # feature off (agentGateway: false) is not created; no feature = on
    assert "agentgateway" not in names and RUN17["features"]["agentGateway"] is False
    o = _installer()
    o["spec"] = {"features": {"x": False}, "components": [
        {"name": "a", "kind": "A"}, {"name": "b", "kind": "B", "feature": "x"},
        {"name": "c", "kind": "C", "registerOnly": True}]}
    assert [c["name"] for c in diag606.declared_components(o)] == ["a"]
    assert diag606.declared_components({"spec": {}}) is None


def test_run17_gate_passed_on_24_of_38_and_now_waits():
    """The REPRODUCTION. At 09:45:46 the 24 CRs status.managed names existed,
    all Ready, and the Installer CR said up-to-date. The old gate read that
    as READY. Now the 14 declared, not-yet-created components keep it waiting."""
    present = [_leaf(x["kind"], x["name"]) for x in RUN17["present_at_gate"]]
    assert len(present) == 24
    st = diag606.platform_state([_inst_summary()], present, [], allow=diag606.DEFAULT_ALLOW_NOT_READY)
    assert all(st["required_ready"].values())          # snowplow/frontend/authn/portal all Ready
    assert not st["ok"]
    missing = sorted(u["id"] for u in st["unexpected_not_ready"])
    assert len(missing) == 14 and "declared/PortalAgents/portal-agents" in missing
    assert "declared/DashboardPage/dashboard-page" in missing
    assert all("deps: portal" in u["message"] for u in st["unexpected_not_ready"])
    assert st["declared_created"] == "24/38"
    assert "declared CRs created 24/38" in diag606.platform_line(st)
    # the layer arrives, still Creating (the diagnostics' 24/38 Ready): not yet
    later = present + [_leaf(c["kind"], c["name"], False, "Creating")
                       for c in diag606.declared_components(_installer())
                       if (c["kind"], c["name"]) not in {(x["kind"], x["name"]) for x in RUN17["present_at_gate"]}]
    assert not diag606.platform_state([_inst_summary()], later, [])["ok"]
    # every declared component Ready: READY
    done = [_leaf(c["kind"], c["name"]) for c in diag606.declared_components(_installer())]
    st = diag606.platform_state([_inst_summary()], done, [])
    assert st["ok"] and st["declared_created"] == "38/38"


def test_an_unreadable_declaration_fails_closed():
    s = diag606.summarize_conditions(_installer(), "installers")
    s["declares"] = None
    st = diag606.platform_state([s], [_leaf("Snowplow", n) for n in diag606.REQUIRED_READY], [],
                                required=())
    assert not st["ok"] and st["unexpected_not_ready"][0]["reason"] == "DeclarationUnreadable"


def test_a_declared_component_whose_definition_is_allow_listed_may_be_missing():
    """github-provider-kog's definition 401s on the private registry (run 3): its
    CR can never be created, and the allow-list excuses its definition, so its
    absence is excused too. The same absence without that definition's
    allow-listed reason is not."""
    kog_cd = diag606.summarize_conditions(
        {"kind": "CompositionDefinition", "metadata": {"name": "github-provider-kog",
                                                       "namespace": "krateo-system"},
         "status": {"conditions": [{"type": "Ready", "status": "False", "reason": "Creating",
                                    "message": "failed to get chart: response status code 401: unauthorized"}]}},
        "compositiondefinitions")
    done = [_leaf(c["kind"], c["name"]) for c in diag606.declared_components(_installer())
            if c["name"] != "github-provider-kog"]
    st = diag606.platform_state([_inst_summary()], done, [kog_cd])
    assert st["ok"], st["unexpected_not_ready"]
    assert {a["id"] for a in st["allowed_not_ready"]} == {
        "declared/GithubProviderKog/github-provider-kog",
        "compositiondefinitions/krateo-system/github-provider-kog"}
    kog_cd["status"]["conditions"][0]["message"] = "timeout"
    kog_cd["Ready"]["message"] = "timeout"
    assert not diag606.platform_state([_inst_summary()], done, [kog_cd])["ok"]


def test_wait_installer_replays_run17_and_waits_for_the_page_layer():
    """Three polls, as run 17's cluster went: (1) 24 present, the Installer up
    to date; (2) the deps-on-portal layer created, Creating; (3) all Ready."""
    declared = diag606.declared_components(_installer())
    first = {(x["kind"], x["name"]) for x in RUN17["present_at_gate"]}
    polls = iter([
        [(c, True) for c in declared if (c["kind"], c["name"]) in first],
        [(c, (c["kind"], c["name"]) in first) for c in declared],
        [(c, True) for c in declared],
    ])
    seen = []

    class K:
        def composition_kinds(self):
            return ["installers.composition.krateo.io"] + [
                f"{c['kind'].lower()}s.composition.krateo.io" for c in declared]

        def items(self, kind, *a):
            if kind.startswith("installers."):
                self.now = next(polls)
                return [_installer()]
            if kind.startswith("compositiondefinitions."):
                return []
            return [{"kind": c["kind"], "metadata": {"name": c["name"], "namespace": "krateo-system"},
                     "status": {"conditions": [{"type": "Ready", "status": "True" if ok else "False",
                                                "reason": "Available" if ok else "Creating"}]}}
                    for c, ok in self.now if f"{c['kind'].lower()}s." == kind.split(".")[0] + "."]

    assert diag606.wait_installer(K(), timeout_s=100, interval_s=0, log=seen.append, allow=(),
                                  clock=lambda: 0, sleep=lambda _: None)
    lines = [m for m in seen if "CRs ready" in m]
    assert len(lines) == 3
    assert "declared CRs created 24/38" in lines[0] and "NOT READY: declared/" in lines[0]
    assert "declared CRs created 38/38" in lines[1] and "Creating" in lines[1]
    assert seen[-1].startswith("[platform] verdict READY")


# ─── the preflight, on the 0.3.459 shell ─────────────────────────────────────


def P(resource, name, ns="krateo-system"):
    return (f"/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1"
            f"&name={name}&namespace={ns}&resource={resource}")


INIT = ("/call?resource=layouts&apiVersion=widgets.templates.krateo.io/v1beta1"
        "&name=app-shell&namespace=krateo-system")


def W(kind, refs: dict, wd: dict | None = None):
    return {"kind": kind, "status": {"widgetData": wd or {}, "resourcesRefs": {"items": [
        {"id": i, "path": p, "verb": "GET", "allowed": True} for i, p in refs.items()]}}}


RA = ("/call?apiVersion=templates.krateo.io%2Fv1&resource=restactions"
      "&name=sidebar-nav&namespace=krateo-system")


class StagedFake(Fake):
    """The fake portal, with the X-Snowplow-Stage-Outcomes a RESTAction /call carries."""

    def __init__(self, tree, outcomes="[]"):
        super().__init__(tree)
        self.outcomes = outcomes

    def get(self, path, token):
        r = super().get(path, token)
        if canon(path) == canon(RA):
            r.stage_outcomes = self.outcomes
        return r


def _shell_0_3_459(items, refs):
    """app-shell Layout → sider-content Flex → {brand-logo Image, sidebar-nav
    Menu} (057's live CRs). The Menu's items and resourcesRefs come from
    restaction sidebar-nav (widgetDataTemplate items = .items;
    resourcesRefsTemplate over .refs → flexes in each page's own namespace)."""
    tree = {
        INIT: W("Layout", {"sider-content": P("flexes", "sider-content"), "app-theme": P("themes", "app-theme")},
                {"hasSider": True, "sider": {"resourceRefId": "sider-content"}, "effects": ["app-theme"]}),
        P("flexes", "header-context"): W("Flex", {}),
        "/list?category=builders&ns=krateo-system": {"items": []},
        P("themes", "app-theme"): W("Theme", {}),
        P("flexes", "sider-content"): W("Flex", {"brand": P("images", "brand-logo"), "nav": P("menus", "sidebar-nav")},
                                        {"items": [{"resourceRefId": "brand"}, {"resourceRefId": "nav"}]}),
        P("images", "brand-logo"): W("Image", {}),
        P("menus", "sidebar-nav"): {**W("Menu", {r["id"]: P("flexes", r["name"], r["namespace"]) for r in refs},
                                        {"allowedResources": ["flexes"], "items": items, "mode": "inline"}),
                                    "spec": {"apiRef": {"name": "sidebar-nav", "namespace": "krateo-system"}}},
        RA: {"items": items, "refs": refs},
    }
    for r in refs:
        tree[P("flexes", r["name"], r["namespace"])] = W("Flex", {})
    return tree


def test_preflight_refuses_the_0_3_459_shell_while_no_page_exists():
    """What run 17's preflight got: the page layer not yet created, so
    sidebar-nav finds no flex with krateo.io/nav-path, and the Menu has no items."""
    fake = StagedFake(_shell_0_3_459([], []))
    out = load606.preflight(fake, "t", load606.Portal(init=INIT))
    assert not out["ok"]
    assert out["refused"] == [{"path": "(sidebar menu)", "status": None,
                               "body": "the shell rendered no sidebar Menu with routes"}]
    assert out["first_load_shell"]["statuses"] == {"200": out["first_load_shell"]["calls"]}
    # the evidence run 17 lacked: the Menu came back with no item and no error,
    # and its apiRef RESTAction, read as the same identity, ran every stage
    m = out["menu"]
    assert (m["found"], m["items"], m["resources_refs"], m["routes"], m["error"]) == (True, 0, 0, 0, None)
    assert m["api_ref"] == {"name": "sidebar-nav", "namespace": "krateo-system"}
    assert m["api_ref_call"] == {"path": RA, "status": 200, "stage_outcomes": "[]"}
    assert fake.calls.count(RA) == 1
    # a failed stage is named
    bad = '[{"name":"pages","ok":false,"reason":"Forbidden"}]'
    out = load606.preflight(StagedFake(_shell_0_3_459([], []), bad), "t", load606.Portal(init=INIT))
    assert out["menu"]["api_ref_call"]["stage_outcomes"] == bad


def test_preflight_reads_the_0_3_459_menu_once_the_pages_exist():
    """The menu discovery already reads the template-built Menu: a devs user
    gets the five untemplated routes that 057's sidebar-nav gives the group,
    each served from its page's own namespace."""
    fake = StagedFake(_shell_0_3_459(NAV["items"], NAV["refs"]))
    out = load606.preflight(fake, "t", load606.Portal(init=INIT))
    assert out["ok"], out["refused"]
    assert out["menu"]["items"] == len(NAV["items"]) and "api_ref_call" not in out["menu"]
    assert RA not in fake.calls                     # with routes, no extra read
    assert [r["path"] for r in out["routes"]] == ["/dashboard", "/compositions", "/blueprints",
                                                  "/event-health", "/search"]
    ns = {r["path"]: r["endpoint"].split("namespace=")[1].split("&")[0] for r in out["routes"]}
    assert ns["/dashboard"] == "krateo-dashboard" and ns["/event-health"] == "krateo-system"
