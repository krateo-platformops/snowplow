"""The SPA navigation model, preflight classification and adaptive logins.

Rules come from the frontend ui/src (cited in bench/load606.py); the fake
portal below has run 6's live shape: app-shell Layout → sider-content Flex
→ {brand Image, sidebar-nav Menu}; app-theme as a Layout effect; the Menu's
resourcesRefs point at every page, which a closure walk would fetch and the
SPA does not.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import subprocess
import threading
import time

import pytest

from bench import load606
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401

NS = "krateo-system"


def P(resource, name):
    return (f"/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1"
            f"&name={name}&namespace={NS}&resource={resource}")


INIT = ("/call?resource=layouts&apiVersion=widgets.templates.krateo.io/v1beta1"
        "&name=app-shell&namespace=krateo-system")
HEADER = ("/call?resource=flexes&apiVersion=widgets.templates.krateo.io/v1beta1"
          "&name=header-context&namespace=krateo-system")
BUILDERS = "/list?category=builders&ns=krateo-system"


def W(kind, refs: dict, wd: dict | None = None, denied=()):
    items = [{"id": i, "path": p, "verb": "GET", "allowed": i not in denied}
             for i, p in refs.items()]
    return {"kind": kind, "status": {"widgetData": wd or {},
                                     "resourcesRefs": {"items": items}}}


TREE = {
    INIT: W("Layout", {"sider": P("flexes", "sider-content"), "theme": P("themes", "app-theme"),
                       "unused": P("flexes", "never-rendered")},
            {"sider": {"resourceRefId": "sider"}, "effects": ["theme"]}),
    HEADER: W("Flex", {"proj": P("selects", "project-switcher")},
              {"items": [{"resourceRefId": "proj"}]}),
    P("selects", "project-switcher"): W("Select", {}),
    BUILDERS: {"items": []},
    P("themes", "app-theme"): W("Theme", {}),
    P("flexes", "sider-content"): W("Flex", {"brand": P("images", "brand-logo"),
                                            "nav": P("menus", "sidebar-nav")},
                                    {"items": [{"resourceRefId": "brand"}, {"resourceRefId": "nav"}]}),
    P("images", "brand-logo"): W("Image", {}),
    P("menus", "sidebar-nav"): W("Menu", {
        "dash": P("flexes", "page-dashboard"), "comps": P("flexes", "page-compositions"),
        "secret": P("flexes", "page-admin")},
        {"items": [
            {"path": "/dashboard", "resourceRefId": "dash", "order": 1},
            {"path": "/compositions", "order": 2},                       # convention
            {"path": "/compositions/{namespace}/{name}", "page": "composition-detail", "order": 3},
            {"path": "/admin", "resourceRefId": "secret", "order": 4},   # denied below
            {"type": "divider", "order": 5}]},
        denied=("secret",)),
    P("flexes", "page-dashboard"): W("Flex", {"a": P("piecharts", "pie"), "b": P("tables", "tbl"),
                                             "drawer": P("forms", "agent-create")},
                                     {"items": [{"resourceRefId": "a"}, {"resourceRefId": "b"}],
                                      "widgetActions": {"openDrawer": [{"id": "x", "resourceRefId": "drawer"}]}}),
    P("piecharts", "pie"): W("PieChart", {}),
    P("tables", "tbl"): W("Table", {"c1": P("buttons", "row-btn")},
                          {"dataSource": [[{"valueKey": "k", "kind": "widget", "resourceRefId": "c1"}]]}),
    P("buttons", "row-btn"): W("Button", {}),
    P("forms", "agent-create"): W("Form", {}),
    P("flexes", "page-compositions"): W("Tabs", {"t1": P("tables", "comps-a"), "t2": P("tables", "comps-b")},
                                        {"items": [{"label": "A", "resourceRefId": "t1"},
                                                   {"label": "B", "resourceRefId": "t2"}]}),
    P("tables", "comps-a"): W("Table", {}),
    P("tables", "comps-b"): W("Table", {}),
    P("flexes", "page-admin"): W("Flex", {}),
}


def canon(path: str) -> str:
    """snowplow reads /call params by name; their order and %2F-encoding do
    not matter. The fake must not care either."""
    import urllib.parse as up
    base, _, qs = path.partition("?")
    return base + "?" + "&".join(f"{k}={v}" for k, v in sorted(up.parse_qsl(qs)))


class Fake:
    def __init__(self, tree=TREE, status=None, bodies=None):
        self.tree = {canon(k): v for k, v in tree.items()}
        self.status = {canon(k): v for k, v in (status or {}).items()}
        self.bodies = {canon(k): v for k, v in (bodies or {}).items()}
        self.calls, self.lock = [], threading.Lock()

    def get(self, path, token):
        with self.lock:
            self.calls.append(path)
        k = canon(path)
        st = self.status.get(k, 200 if k in self.tree else 404)
        body = (self.bodies.get(k) or json.dumps(self.tree.get(k, {}))).encode()
        return load606.Response(st, body, 1.0)


PORTAL = load606.Portal(init=INIT)


# ─── eager_refs: what a rendered widget fetches ─────────────────────────────


def test_layout_fetches_slots_and_effects_not_unreferenced_refs():
    assert load606.eager_refs(TREE[INIT]) == [P("flexes", "sider-content"), P("themes", "app-theme")]


def test_menu_fetches_nothing_on_render():
    assert load606.eager_refs(TREE[P("menus", "sidebar-nav")]) == []


def test_tabs_fetch_only_the_active_first_tab():
    assert load606.eager_refs(TREE[P("flexes", "page-compositions")]) == [P("tables", "comps-a")]


def test_actions_are_not_fetched_until_clicked_and_table_cells_are():
    assert load606.eager_refs(TREE[P("flexes", "page-dashboard")]) == [
        P("piecharts", "pie"), P("tables", "tbl")]
    assert load606.eager_refs(TREE[P("tables", "tbl")]) == [P("buttons", "row-btn")]


def test_denied_refs_are_never_fetched():
    doc = W("Flex", {"x": P("flexes", "x")}, {"items": [{"resourceRefId": "x"}]}, denied=("x",))
    assert load606.eager_refs(doc) == []


# ─── menu_routes: the user's own sidebar ────────────────────────────────────


def test_menu_routes_from_live_menu():
    routes = load606.menu_routes(TREE[P("menus", "sidebar-nav")], NS)
    assert [r["path"] for r in routes] == ["/dashboard", "/compositions",
                                          "/compositions/{namespace}/{name}"]
    assert routes[0]["endpoint"] == P("flexes", "page-dashboard")
    assert "name=page-compositions" in routes[1]["endpoint"]          # convention slug
    assert "name=page-composition-detail" in routes[2]["endpoint"] and routes[2]["templated"]
    assert all("page-admin" not in r["endpoint"] for r in routes)     # denied → hidden


# ─── first load vs route click vs the closure walk ──────────────────────────


def test_first_load_fetches_exactly_what_the_spa_renders():
    c = Fake()
    with cf.ThreadPoolExecutor(6) as pool:
        shell, routes, content = load606.first_load(c, "t", PORTAL, pool=pool)
    fetched = set(c.calls)
    assert fetched == {INIT, BUILDERS, HEADER, P("selects", "project-switcher"),
                       P("themes", "app-theme"), P("flexes", "sider-content"),
                       P("images", "brand-logo"), P("menus", "sidebar-nav"),
                       # `/` redirects to the FIRST route → its eager tree:
                       P("flexes", "page-dashboard"), P("piecharts", "pie"),
                       P("tables", "tbl"), P("buttons", "row-btn")}
    assert len(c.calls) == len(fetched)                 # nothing fetched twice
    assert [r["path"] for r in routes] == ["/dashboard", "/compositions"]  # templated skipped
    assert content.calls == 4


def test_closure_walk_is_much_larger_and_is_not_a_navigation():
    c = Fake()
    with cf.ThreadPoolExecutor(6) as pool:
        wall, calls, errors, nbytes, slowest, samples = load606.walk_page(c, "t", INIT, pool=pool)
    assert P("flexes", "page-admin") not in c.calls     # denied still never fetched
    assert P("forms", "agent-create") in c.calls        # an action target: only the closure fetches it
    assert calls > 12


def test_driver_first_load_then_route_clicks():
    c = Fake()
    recs = []
    d = load606.Driver(c, {"u1": "t"}, PORTAL, think_s=0.03, duration_s=0.5,
                       nav_workers=2, call_workers=8, sink=recs.append)
    d.run()
    assert recs[0]["nav"] == "first_load" and recs[0]["visit"] == 1
    assert recs[0]["page"] == "/dashboard" and recs[0]["routes"] == 2
    later = recs[1:]
    assert later and all(r["nav"] == "route" for r in later)
    assert c.calls.count(INIT) == 1                     # the shell is never re-fetched
    assert c.calls.count(P("menus", "sidebar-nav")) == 1
    assert P("tables", "comps-b") not in c.calls        # inactive tab never mounted
    for page in {r["page"] for r in recs}:
        visits = [r["visit"] for r in recs if r["page"] == page]
        assert visits == list(range(1, len(visits) + 1))


# ─── classify + preflight ───────────────────────────────────────────────────


@pytest.mark.parametrize("status,body,expected", [
    (200, b"{}", "ok"),
    (404, b"", "config"),
    (500, b'{"message":"no matches for widgets.templates.krateo.io/v1beta1, Resource=pages"}', "config"),
    (500, b'{"message":"resolve apiRef ...: unexpected end of JSON input"}', "product"),
    (403, b"", "forbidden"),
    (503, b"", "product"),
])
def test_classify(status, body, expected):
    assert load606.classify(load606.Response(status, body, 1.0)) == expected


def test_preflight_refuses_stale_init_in_one_request():
    stale = "/call?resource=pages&apiVersion=widgets.templates.krateo.io/v1beta1&name=dashboard-page&namespace=krateo-system"
    c = Fake(status={stale: 500}, bodies={stale: '{"message":"no matches for widgets.templates.krateo.io/v1beta1, Resource=pages"}'})
    res = load606.preflight(c, "t", load606.Portal(init=stale))
    assert res["ok"] is False and res["refused"][0]["path"] == stale
    assert c.calls == [stale]


def test_preflight_records_product_5xx_and_does_not_refuse():
    page = P("flexes", "page-compositions")
    c = Fake(status={page: 500}, bodies={page: '{"message":"resolve apiRef: unexpected end of JSON input"}'})
    res = load606.preflight(c, "t", PORTAL, full_walk=True)
    assert res["ok"] is True
    assert [e["path"] for e in res["product_errors"]] == ["/compositions"]
    assert res["full_walk_report_only"]["calls"] > res["first_load_shell"]["calls"]


def test_preflight_refuses_an_unserved_route_page():
    page = P("flexes", "page-compositions")
    c = Fake(status={page: 404})
    res = load606.preflight(c, "t", PORTAL)
    assert res["ok"] is False and res["refused"][0]["path"] == "/compositions"


def test_portal_from_config_json():
    cfg = {"api": {"INIT": INIT, "BUILDERS_NAMESPACE": "b"}, "params": {"FRONTEND_NAMESPACE": "f"}}
    assert load606.portal_from_config_json(json.dumps(cfg)) == {
        "init": INIT, "frontend_ns": "f", "builders_ns": "b"}
    with pytest.raises(ValueError):
        load606.portal_from_config_json(json.dumps({"api": {"INIT": "https://x"}}))


# ─── adaptive logins ────────────────────────────────────────────────────────


def _authn(base_s=0.004, knee=8, fail_once=()):
    """A fake authn whose per-login latency stays at base up to `knee`
    concurrent logins, then grows linearly (a CSR issuer saturating)."""
    inflight = [0]
    lock = threading.Lock()
    failed = set()

    def login(u):
        with lock:
            inflight[0] += 1
            n = inflight[0]
        try:
            if u in fail_once and u not in failed:
                failed.add(u)
                raise RuntimeError("503")
            time.sleep(base_s * max(1.0, n / knee))
            return "tok-" + u
        finally:
            with lock:
                inflight[0] -= 1
    return login


def test_adaptive_login_ramps_up_backs_off_and_finishes():
    users = [f"scaleuser-{i:04d}" for i in range(1, 401)]
    logs = []
    toks, curve, failed = load606.adaptive_login(users, _authn(), log=logs.append)
    assert not failed and len(toks) == 400
    concs = [r["concurrency"] for r in curve if r["phase"] != "baseline"]
    assert max(concs) >= 8                                # it ramped up
    assert max(concs) <= 64
    # it does not run away past the authn knee: once latency grows it halves
    grew = [r for r in curve if r.get("ratio_to_baseline") and r["ratio_to_baseline"] > 1.6]
    assert grew, "the fake authn saturates above 8 — the curve must show it"
    i = curve.index(grew[0])
    assert curve[i + 1]["concurrency"] < grew[0]["concurrency"] if i + 1 < len(curve) else True
    assert any("[login] baseline" in m for m in logs)
    assert sum("logged in; concurrency" in m for m in logs) >= 400 // 50 - 1


def test_adaptive_login_retries_failures_and_backs_off_on_errors():
    users = [f"scaleuser-{i:04d}" for i in range(1, 61)]
    bad = {"scaleuser-0010", "scaleuser-0011", "scaleuser-0040"}
    toks, curve, failed = load606.adaptive_login(users, _authn(fail_once=bad), log=lambda m: None)
    assert not failed and len(toks) == 60
    err_rows = [r for r in curve if r.get("errors")]
    assert err_rows
    j = curve.index(err_rows[0])
    if j + 1 < len(curve):
        assert curve[j + 1]["concurrency"] <= max(1, err_rows[0]["concurrency"] // 2)


def test_login_report_is_an_authn_finding():
    rep = load606.login_report([{"users": 3, "failed": 0, "curve": [
        {"phase": "baseline", "concurrency": 1, "median_ms": 3200.0},
        {"phase": "round1", "concurrency": 2, "median_ms": 3300.0, "batch_wall_ms": 3300}]}])
    assert rep["baseline_single_login_ms"] == 3200.0
    assert "authn" in rep["finding"]


# ─── run606.sh: stale roots fail in seconds, before data or logins ──────────


def test_stale_roots_fail_in_seconds_before_stamping(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_PREFLIGHT="refused", PREFLIGHT_POLL_S="0.1")
    t0 = time.time()
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    elapsed_preflight = None
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "PREFLIGHT REFUSED" in out
    assert "[preflight] REFUSED /dashboard -> 500: no matches for" in out
    assert "=== step data ===" not in p.stderr                  # nothing stamped
    assert not (tmp / "out" / "data-proof.json").exists()
    assert not any("scaleuser-0001" in json.dumps(c) for c in _calls(state))  # no logins/users
    assert time.time() - t0 < 60
    gcl = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert ["container", "clusters", "delete"] in gcl


def test_preflight_ok_path_records_product_errors_and_reads_init_from_config(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out[-3000:]
    assert "portal boot root (config.json INIT): /call?resource=layouts" in out
    assert "PRODUCT ERROR (recorded, not fatal) /agents/new -> 500" in out
    assert "full closure walk (report-only, NOT a navigation): 590 calls" in out
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert "name=app-shell" in job and "dashboard-page" not in job
    verdict = json.loads((tmp / "out" / "verdict.json").read_text())
    assert verdict["report"]["preflight"]["product_errors"][0]["path"] == "/agents/new"
