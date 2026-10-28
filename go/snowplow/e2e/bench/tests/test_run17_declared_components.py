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
import subprocess
import sys

import pytest
import yaml

from bench import diag606, load606, load606_job
from tests.test_navmodel import Fake, canon  # noqa: F401  (canon: the fake's key)
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401

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
    # the allow-listed definition failing for a DIFFERENT reason excuses nothing
    kog_cd["status"]["conditions"][0]["message"] = "timeout"
    kog_cd["Ready"]["message"] = "timeout"
    st = diag606.platform_state([_inst_summary()], done, [kog_cd])
    assert not st["ok"]
    assert {u["id"] for u in st["unexpected_not_ready"]} == {
        "declared/GithubProviderKog/github-provider-kog",
        "compositiondefinitions/krateo-system/github-provider-kog"}
    assert st["allowed_not_ready"] == []


def test_a_ready_definition_does_not_excuse_its_missing_cr():
    """#946 review F3: a definition that is READY serves its CRD, so the
    installer can create its CR: a missing CR is not excused, even when the
    definition's conditions still carry the allow-listed text (a Synced
    message from an earlier failed attempt)."""
    kog_cd = diag606.summarize_conditions(
        {"kind": "CompositionDefinition", "metadata": {"name": "github-provider-kog",
                                                       "namespace": "krateo-system"},
         "status": {"conditions": [
             {"type": "Ready", "status": "True", "reason": "Available", "message": ""},
             {"type": "Synced", "status": "True", "reason": "ReconcileSuccess",
              "message": "earlier: response status code 401: unauthorized"}]}},
        "compositiondefinitions")
    done = [_leaf(c["kind"], c["name"]) for c in diag606.declared_components(_installer())
            if c["name"] != "github-provider-kog"]
    st = diag606.platform_state([_inst_summary()], done, [kog_cd])
    assert not st["ok"]
    assert [u["id"] for u in st["unexpected_not_ready"]] == ["declared/GithubProviderKog/github-provider-kog"]


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
    assert m["api_ref_call"] == {"path": RA, "status": 200, "stage_outcomes": [],
                                 "stage_outcomes_header": "present"}
    assert fake.calls.count(RA) == 1
    # a failed stage is named (snowplow's own value: issue904_stored_outcomes_test.go:52)
    bad = '[{"name":"sa","ok":false,"reason":"Forbidden"}]'
    out = load606.preflight(StagedFake(_shell_0_3_459([], []), bad), "t", load606.Portal(init=INIT))
    assert out["menu"]["api_ref_call"]["stage_outcomes"] == [{"name": "sa", "reason": "Forbidden"}]
    # 1.12.56: a stored RESTAction's /call carries no header (inline resolves only)
    out = load606.preflight(StagedFake(_shell_0_3_459([], []), None), "t", load606.Portal(init=INIT))
    assert out["menu"]["api_ref_call"]["stage_outcomes"] is None
    assert out["menu"]["api_ref_call"]["stage_outcomes_header"] == "absent"


def test_stage_outcomes_parse_snowplows_shapes_only():
    """The shapes StageOutcomes.HeaderValue renders (resolvers/restactions/api/
    stage_outcomes.go): failed / skipped stages, [], the truncated form."""
    p = load606.parse_stage_outcomes
    assert p('[{"name":"sa","ok":false,"reason":"Forbidden"}]') == [{"name": "sa", "reason": "Forbidden"}]
    assert p('[{"name":"w","ok":false,"reason":"Skipped","dependsOn":"r"}]') == [
        {"name": "w", "reason": "Skipped", "dependsOn": "r"}]
    assert p('[{"name":"","ok":false,"reason":"Aborted"}]') == [{"name": "", "reason": "Aborted"}]
    assert p("[]") == []
    assert p('{"truncated":true,"failed":7}') == {"truncated": True, "failed": 7}
    assert p(None) is None
    assert p("not json") == {"unparsed": "not json"}
    assert p('[{"name":"x","reason":"Some free text"}]')["unparsed"]       # not a reason code


FORBIDDEN = ('restactions "sidebar-nav": pages: flexes.widgets.templates.krateo.io is forbidden: '
             'User "scaleuser-0000" cannot list resource "flexes" in API group '
             '"widgets.templates.krateo.io" in the namespace "krateo-dashboard"')


def _error_menu_tree():
    tree = _shell_0_3_459([], [])
    tree[P("menus", "sidebar-nav")]["status"]["error"] = FORBIDDEN
    return tree


def test_the_menu_error_is_recorded_with_its_identity_redacted():
    """#946 review F1: a Menu's status.error is the apiRef resolve's raw
    err.Error(); a Forbidden names the user. The record keeps its meaning,
    never the name, and so does the summary run606 prints."""
    out = load606.preflight(StagedFake(_error_menu_tree()), "t", load606.Portal(init=INIT))
    err = out["menu"]["error"]
    assert 'User "<redacted>" cannot list resource "flexes"' in err
    assert "scaleuser-0000" not in json.dumps(out)
    p = subprocess.run([sys.executable, "-m", "bench.load606", "preflight-summary"], cwd=BENCH_DIR,
                       input=json.dumps(out) + "\n", capture_output=True, text=True, timeout=60)
    assert "error=" in p.stdout and 'User "<redacted>"' in p.stdout and "scaleuser-0000" not in p.stdout


def test_the_preflight_job_redacts_with_the_shipped_identity_module(tmp_path):
    """The Job runs load606.py ALONE from its ConfigMap: the rendered Job must
    ship bench/identity.py beside it, so the same import redacts in-cluster.
    Laid out exactly as the Job mounts it; run with the repo's bench out of reach."""
    docs = list(yaml.safe_load_all(load606_job.render_preflight(
        snowplow_url="http://s", authn_url="http://a", user="u", password="p", init=INIT,
        frontend_ns="krateo-system", builders_ns="krateo-system")))
    cm = next(d for d in docs if d and d["kind"] == "ConfigMap")
    job = next(d for d in docs if d and d["kind"] == "Job")
    vol = job["spec"]["template"]["spec"]["volumes"][0]["configMap"]
    mount = tmp_path / "opt-load606"
    for it in vol["items"]:
        (mount / it["path"]).parent.mkdir(parents=True, exist_ok=True)
        (mount / it["path"]).write_text(cm["data"][it["key"]])
    assert (mount / "bench" / "identity.py").exists()
    (mount / "probe.py").write_text(
        "import json, load606\n"
        f"ERR = {FORBIDDEN!r}\n"
        "class C:\n"
        "    def get(self, path, token):\n"
        "        if 'app-shell' in path:\n"
        "            doc = {'kind': 'Layout', 'status': {'widgetData': {'sider': {'resourceRefId': 'm'}},\n"
        "                   'resourcesRefs': {'items': [{'id': 'm', 'path': '/call?menu', 'verb': 'GET', 'allowed': True}]}}}\n"
        "        elif path == '/call?menu':\n"
        "            doc = {'kind': 'Menu', 'status': {'error': ERR, 'widgetData': {'items': []}}}\n"
        "        else:\n"
        "            doc = {}\n"
        "        return load606.Response(200, json.dumps(doc).encode(), 1.0)\n"
        "print(json.dumps(load606.preflight(C(), 't', load606.Portal(init='/call?app-shell'))))\n")
    p = subprocess.run([sys.executable, "-E", "-s", str(mount / "probe.py")], cwd=tmp_path,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    rec = json.loads(p.stdout)
    assert 'User "<redacted>"' in rec["menu"]["error"] and "scaleuser-0000" not in p.stdout


def test_route_bodies_naming_a_user_are_redacted_inside_the_records_lists():
    """#946 delta T1: the record's LISTS carry raw answer bodies too: a route's
    404 lands in `refused`, a 5xx in `product_errors`. Both name the user;
    neither the record nor run606's printed summary may."""
    tree = _part(2)
    ref = {r["id"]: P("flexes", r["name"], r["namespace"]) for r in NAV["refs"]}
    dash, comps = ref["page-dashboard"], ref["page-compositions"]
    body = ('flexes.widgets.templates.krateo.io "page-{}" is forbidden: User "scaleuser-0000" '
            'cannot get resource "flexes" in API group "widgets.templates.krateo.io"')
    fake = Fake(tree, status={dash: 404, comps: 500},
                bodies={dash: body.format("dashboard"), comps: body.format("compositions")})
    out = load606.preflight(fake, "t", load606.Portal(init=INIT))
    assert [x["path"] for x in out["refused"]] == ["/dashboard"]
    assert [x["path"] for x in out["product_errors"]] == ["/compositions"]
    for x in out["refused"] + out["product_errors"]:
        assert 'User "<redacted>" cannot get resource "flexes"' in x["body"]
    assert "scaleuser-0000" not in json.dumps(out)
    p = subprocess.run([sys.executable, "-m", "bench.load606", "preflight-summary"], cwd=BENCH_DIR,
                       input=json.dumps(out) + "\n", capture_output=True, text=True, timeout=60)
    assert "REFUSED /dashboard -> 404" in p.stdout and "PRODUCT ERROR" in p.stdout
    assert 'User "<redacted>"' in p.stdout and "scaleuser-0000" not in p.stdout


# ─── R1: the menu read twice ─────────────────────────────────────────────────


class ChangingFake(StagedFake):
    """The Menu answers `first` on its first read and `then` afterwards: a
    platform still converging (a page CR Ready before snowplow serves it)."""

    def __init__(self, first, then):
        super().__init__(first)
        self.then, self.menu_reads = then, 0

    def get(self, path, token):
        if canon(path) == canon(P("menus", "sidebar-nav")):
            self.menu_reads += 1
            if self.menu_reads > 1:
                self.tree = {canon(k): v for k, v in self.then.items()}
        return super().get(path, token)


def _part(n):
    items = [i for i in NAV["items"] if "{" not in i["path"]][:n]
    ids = {i["resourceRefId"] for i in items}
    return _shell_0_3_459(items, [r for r in NAV["refs"] if r["id"] in ids])


def test_a_menu_that_changes_between_two_reads_refuses_at_preflight():
    fake = ChangingFake(_part(2), _part(4))
    out = load606.preflight(fake, "t", load606.Portal(init=INIT))
    assert fake.menu_reads == 2 and not out["ok"]
    r = [x for x in out["refused"] if x["path"] == "(sidebar menu)"]
    assert len(r) == 1 and "changed during the preflight: 2 routes, then 4" in r[0]["body"]
    assert out["menu"]["reread"]["added"] == ["/blueprints", "/event-health"]
    # the same menu twice: no refusal, the re-read recorded
    out = load606.preflight(ChangingFake(_part(4), _part(4)), "t", load606.Portal(init=INIT))
    assert out["ok"] and out["menu"]["reread"]["same"] is True


def _pick(paths):
    items = [i for i in NAV["items"] if i["path"] in paths]
    ids = {i["resourceRefId"] for i in items}
    return _shell_0_3_459(items, [r for r in NAV["refs"] if r["id"] in ids])


def test_a_menu_that_swaps_a_route_at_the_same_count_refuses_at_preflight():
    """#946 delta T2: the SET is compared, not the count: two routes, then two
    routes with one swapped for another."""
    out = load606.preflight(ChangingFake(_pick({"/dashboard", "/compositions"}),
                                         _pick({"/dashboard", "/blueprints"})),
                            "t", load606.Portal(init=INIT))
    assert not out["ok"]
    rr = out["menu"]["reread"]
    assert (rr["routes"], rr["added"], rr["removed"], rr["same"]) == (2, ["/blueprints"], ["/compositions"], False)
    r = [x for x in out["refused"] if x["path"] == "(sidebar menu)"]
    assert len(r) == 1 and "2 routes, then 2" in r[0]["body"]
    assert "added ['/blueprints'], removed ['/compositions']" in r[0]["body"]


# ─── R2 / R3 through run606.sh ───────────────────────────────────────────────


def _run(harness, **env_extra):
    env, state, ambient, tmp = harness
    env = dict(env)
    env.update(PREFLIGHT_POLL_S="0.1", **env_extra)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    return p, p.stdout + p.stderr


def test_no_composite_page_refuses_at_preflight_not_at_measure(harness):
    """#946 review R2: k6_config raises on a composite burst without a
    composite route only at the measure step, after data and reboot."""
    p, out = _run(harness, FAKE_PREFLIGHT="nocomposite")
    assert p.returncode != 0
    assert "PREFLIGHT REFUSED: COMPOSITE_BURST_USERS=200 but the live menu has no composite page" in out
    assert "=== step data ===" not in p.stderr
    assert "will record FAILED navigations" not in out


def test_no_composite_page_is_fine_without_a_composite_burst(harness):
    p, out = _run(harness, FAKE_PREFLIGHT="nocomposite", COMPOSITE_BURST_USERS="0")
    assert "PREFLIGHT REFUSED" not in out and "=== step data ===" in p.stderr


CORE_0_3_459 = '{{- define "inst.coreFeatures" -}}{{- end -}}'     # pulled _helpers.tpl:185, verbatim


@pytest.mark.parametrize("body,want", [
    (CORE_0_3_459, ()),
    ('{{- define "inst.coreFeatures" -}}portal oasgenProvider{{- end -}}', ("portal", "oasgenProvider")),
])
def test_core_features_are_read_from_the_pulled_chart(tmp_path, body, want):
    chart = tmp_path / "pulled" / "installer"
    (chart / "templates").mkdir(parents=True)
    (chart / "Chart.yaml").write_text("name: installer\n")
    (chart / "templates" / "_helpers.tpl").write_text("{{/* x */}}\n" + body + "\n")
    assert diag606.core_features(str(tmp_path / "pulled")) == want
    assert diag606.core_features(str(chart)) == want


def test_core_features_fail_closed_on_a_rule_this_gate_does_not_mirror(tmp_path):
    with pytest.raises(ValueError, match="no pulled installer chart there"):
        diag606.core_features(str(tmp_path / "never-pulled"))
    chart = tmp_path / "installer"
    (chart / "templates").mkdir(parents=True)
    (chart / "Chart.yaml").write_text("name: installer\n")
    with pytest.raises(ValueError, match="0 inst.coreFeatures defines"):
        diag606.core_features(str(chart))
    (chart / "templates" / "_helpers.tpl").write_text(
        '{{- define "inst.coreFeatures" -}}{{ .Values.x }}{{- end -}}')
    with pytest.raises(ValueError, match="not a static list"):
        diag606.core_features(str(chart))


def test_a_core_feature_is_declared_whatever_spec_features_says():
    o = _installer()          # run 17: coreAgents is false in spec.features
    assert RUN17["features"]["coreAgents"] is False
    assert not {"kagent", "kagent-crds"} & {c["name"] for c in diag606.declared_components(o)}
    assert {"kagent", "kagent-crds"} <= {c["name"] for c in diag606.declared_components(o, ("coreAgents",))}


def test_run606_reads_core_features_from_the_pulled_chart(harness):
    """The harness's Installer declares agentgateway under feature
    agentGateway, off in spec.features: a pulled chart naming agentGateway a
    core feature makes it declared, and it was never created."""
    p, out = _run(harness, FAKE_CORE_FEATURES="agentGateway", PLATFORM_TIMEOUT_S="1",
                  PLATFORM_POLL_S="0.2")
    assert p.returncode != 0
    assert "installer core features (inst.coreFeatures): agentGateway" in out
    assert "NOT READY: declared/Agentgateway/agentgateway" in out
    assert "=== step preflight ===" not in p.stderr


def test_run606_refuses_a_core_feature_rule_it_cannot_read(harness):
    p, out = _run(harness, FAKE_CORE_FEATURES="dynamic")
    assert p.returncode != 0
    assert "refusing:" in out and "inst.coreFeatures is not a static list" in out
    assert "=== step preflight ===" not in p.stderr


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
