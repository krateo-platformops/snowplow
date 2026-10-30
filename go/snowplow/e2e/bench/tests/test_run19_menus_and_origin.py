"""Run 17 attempt 5 was INVALID on two validity checks, both bench-side:

- k6.route_table: 9 users' menus differed from "the run's route table". They
  were the 9 platform admins k6 drives (scaleuser-0002..0010). The portal
  filters its sidebar by RBAC, so an admin, with cluster-wide read, sees
  pages a devs user does not. Each user now clicks from its OWN menu, as the
  SPA's sidebar offers it, and its menu is checked against its MENU CLASS's
  table: that class's preflight, signed in as the class's lowest-index k6
  user. The class comes from the groups (rbac_model.menu_class), with no
  special case.
- browser.live_refresh: no /notifications stream opened. The SPA was on
  four localhost origins, and from frontend 1.8.11 it reads /notifications
  with fetch + Bearer, a cross-origin request whose preflight sse-proxy
  answers 401. The bench probe counted only an EventSource. The browser now
  loads the portal from ONE origin, as krateo-057's gateway serves it (/
  the SPA, /auth, /content, /events). The run refuses a config.json that
  names several origins. The probe counts a /notifications fetch whose
  response is ok, and still counts an older SPA's EventSource.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
import yaml

from bench import k6_job, k6eval, load606, load606_job, rbac_model
from tests.test_k6_830 import INIT, NODE, NS, needs_node
from tests.test_run606_real_sequence import BENCH_DIR

M = rbac_model.RbacModel(users=1000, namespaces=50)
ADMINS = "bench-platform-admins+devs"


def _class(i, m=M):
    return rbac_model.menu_class(rbac_model.user_groups(i, m), m)


# ─── the menu class comes from the groups' grants ───────────────────────────

def test_a_users_menu_class_is_its_groups_less_those_confined_to_bench_data():
    assert _class(1) == _class(10) == ADMINS                  # the admins' cluster-wide read
    assert _class(11) == _class(1000) == "devs"               # one or two teams: data namespaces only
    assert any(len(rbac_model.user_groups(i, M)) > 2 for i in range(11, 200))    # cross-team users exist
    assert {_class(i) for i in range(1, 1001)} == {ADMINS, "devs"}
    assert rbac_model.grant_scope("bench-team-07", M) == ("bench-ns-07",)
    assert rbac_model.grant_scope(rbac_model.ADMIN_GROUP, M) == ("*",)
    assert rbac_model.grant_scope("devs", M) is None           # the platform's own grant: kept
    # a group the model does not bind is the platform's: it may change the menu, so it is kept
    assert rbac_model.menu_class(["devs", "bench-team-01", "someone-elses"], M) == "devs+someone-elses"
    none = rbac_model.RbacModel(users=20, namespaces=2, admins=0)
    assert rbac_model.menu_class(["devs", rbac_model.ADMIN_GROUP], none) == f"{rbac_model.ADMIN_GROUP}+devs"


def test_each_class_is_preflighted_as_its_lowest_index_k6_user():
    sessions = k6_job.browser_sessions(M, 5, last=799)
    browser = [s["index"] for s in sessions]
    reps = k6_job.menu_class_representatives(_class, 1000, 200, browser)
    nav, burst = k6_job.k6_users(1000, 200, browser)
    assert set(reps) == {ADMINS, "devs"}
    for c, i in reps.items():
        assert i in nav + burst and i not in browser and _class(i) == c
        assert all(_class(j) != c for j in nav + burst if j < i)
    assert reps[ADMINS] == min(i for i in nav if i <= 10)       # user 1 is a browser session


def _args(**kw):
    base = ["--users", "1000", "--namespaces", "50", "--browser-sessions", "5", "--burst-users", "200",
            "--snowplow-url", "http://snowplow:8081", "--authn-url", "http://authn:8082", "--password", "pw",
            "--init", INIT, "--driver", "k6", "--think-s", "60", "--duration-s", "600"]
    for k, v in kw.items():
        for x in (v if isinstance(v, list) else [v]):
            base += [f"--{k.replace('_', '-')}"] + ([] if x is True else [str(x)])
    return base


def test_class_reps_prints_each_class_and_its_user(capsys):
    assert load606_job.main(_args(class_reps=True)) == 0
    lines = [ln.split("\t") for ln in capsys.readouterr().out.splitlines()]
    assert [c for c, _ in lines] == sorted([ADMINS, "devs"])
    assert all(u.startswith("scaleuser-") for _, u in lines)


def test_a_class_preflight_job_has_its_own_name():
    y = load606_job.render_preflight(snowplow_url="s", authn_url="a", user="u", password="p", init=INIT,
                                     frontend_ns=NS, builders_ns=NS, job_name="preflight606-c2")
    job = [d for d in yaml.safe_load_all(y) if d and d["kind"] == "Job"][0]
    assert job["metadata"]["name"] == "preflight606-c2"
    assert job["spec"]["template"]["metadata"]["labels"]["app"] == "preflight606-c2"
    # the CLI run606's classes step calls
    p = subprocess.run([sys.executable, "-m", "bench.load606_job", "--preflight-only", "--preflight-job-name",
                        "preflight606-c1", "--preflight-user", "u", "--snowplow-url", "s", "--authn-url", "a",
                        "--password", "p", "--init", INIT], capture_output=True, text=True, cwd=BENCH_DIR, timeout=60)
    assert p.returncode == 0, p.stderr
    job = [d for d in yaml.safe_load_all(p.stdout) if d and d["kind"] == "Job"][0]
    assert job["metadata"]["name"] == "preflight606-c1"


# ─── k6's config: each user's class, each class's table ─────────────────────

def _r(*paths):
    return [{"path": p, "endpoint": f"/call?name=page{p.replace('/', '-')}"} for p in paths]


DEVS, ADMIN = _r("/dashboard", "/compositions"), _r("/dashboard", "/compositions", "/marketplace")


def _cfg(**kw):
    args = dict(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS, routes=DEVS, users=1000,
                burst_users=200, think_s=60, duration_s=60, burst_at_s=30, composite_routes=["/compositions"],
                menu_classes={"devs": DEVS, ADMINS: ADMIN}, class_of=_class, browser_users=[1, 20, 30, 40, 50])
    return k6_job.k6_config(**dict(args, **kw))


def test_the_config_carries_each_users_class_and_each_class_table():
    c = _cfg()
    assert c["userClass"]["2"] == ADMINS and c["userClass"]["11"] == "devs"
    assert set(c["userClass"]) == {str(i) for i in c["navUserIds"] + c["burstUserIds"]}
    assert c["menuClasses"] == {"devs": DEVS, ADMINS: ADMIN}
    with pytest.raises(ValueError, match=r"no preflight route table: \['bench-platform-admins\+devs'\]"):
        _cfg(menu_classes={"devs": DEVS})
    with pytest.raises(ValueError, match="no preflight route table"):
        _cfg(menu_classes={"devs": DEVS, ADMINS: []})
    with pytest.raises(ValueError, match="composite routes not in menu class 'devs'"):
        _cfg(menu_classes={"devs": _r("/dashboard"), ADMINS: ADMIN}, routes=ADMIN)


def _render(tmp_path, *extra, **kw):
    pf = tmp_path / "preflight.jsonl"
    rec = lambda rs: json.dumps({"kind": "preflight", "routes": [dict(r, status=200) for r in rs]})  # noqa: E731
    pf.write_text(rec(DEVS) + "\n")
    (tmp_path / "c1.jsonl").write_text(rec(ADMIN) + "\n")
    (tmp_path / "c2.jsonl").write_text(rec(DEVS) + "\n")
    argv = _args(preflight_file=pf, composite_route="/compositions",
                 class_preflight=[f"{ADMINS}={tmp_path / 'c1.jsonl'}", f"devs={tmp_path / 'c2.jsonl'}"], **kw)
    p = subprocess.run([sys.executable, "-m", "bench.load606_job", *argv, *extra], capture_output=True,
                       text=True, cwd=BENCH_DIR, timeout=120)
    return p


def _docs(text):
    return [d for d in yaml.safe_load_all(text) if d]


def test_the_browser_loads_the_portal_from_its_origin_with_no_forwarder(tmp_path):
    p = _render(tmp_path, browser_origin="http://10.0.0.9",
                browser_snowplow_url="http://10.0.0.9/content")
    assert p.returncode == 0, p.stderr
    docs = _docs(p.stdout)
    bcm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "k6browser")
    bcfg = json.loads(bcm["data"]["browser-config.json"])
    assert bcfg["frontendUrl"] == "http://10.0.0.9" and bcfg["snowplowUrl"] == "http://10.0.0.9/content"
    assert "forward.py" not in bcm["data"]
    job = next(d for d in docs if d["kind"] == "Job" and d["metadata"]["name"] == "k6browser")
    assert "initContainers" not in job["spec"]["template"]["spec"]          # no localhost forwarder
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    assert "http://10.0.0.9/" in script and "127.0.0.1" not in script       # it waits for the origin
    k6cm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "k6")
    cfg = json.loads(k6cm["data"]["config.json"])
    assert cfg["menuClasses"] == {ADMINS: ADMIN, "devs": DEVS} and cfg["userClass"]["2"] == ADMINS


def test_without_an_origin_or_a_forward_the_render_refuses(tmp_path):
    p = _render(tmp_path)
    assert p.returncode != 0 and "--browser-origin" in p.stderr
    assert _render(tmp_path, browser_forward="8080=frontend.krateo-system.svc:8080").returncode == 0


def test_the_plan_counts_the_forwarder_only_when_there_is_one():
    with_f = k6_job.k6_plan_requests(viewers=True)
    without = k6_job.k6_plan_requests(viewers=True, forwarder=False)
    assert len(with_f) == len(without) + 1 and (k6_job.FORWARD_CPU, k6_job.FORWARD_MEMORY) not in without


# ─── the portal's origin, as 057 serves it ──────────────────────────────────

SAME = {"api": {"SNOWPLOW_API_BASE_URL": "http://10.0.0.9/content", "AUTHN_API_BASE_URL": "http://10.0.0.9/auth",
                "EVENTS_API_BASE_URL": "http://10.0.0.9/events", "EVENTS_PUSH_API_BASE_URL": "http://10.0.0.9/events/"}}


def _origin(api):
    return load606.portal_origin(json.dumps({"api": api}))


def test_one_origin_at_057s_paths_passes():
    r = load606.portal_origin(json.dumps(SAME))
    assert r["origin"] == "http://10.0.0.9" and r["snowplow_url"] == "http://10.0.0.9/content"
    tls = {k: v.replace("http://10.0.0.9", "https://portal.krateo.dev") for k, v in SAME["api"].items()}
    assert _origin(tls)["origin"] == "https://portal.krateo.dev"            # 057's own config


@pytest.mark.parametrize("change,why", [
    ({"EVENTS_PUSH_API_BASE_URL": "http://10.0.0.10/events"}, "2 origins"),
    ({"EVENTS_PUSH_API_BASE_URL": "http://10.0.0.9:8083/events"}, "2 origins"),      # a port is an origin
    ({"AUTHN_API_BASE_URL": "https://10.0.0.9/auth"}, "2 origins"),                   # so is a scheme
    ({"SNOWPLOW_API_BASE_URL": "http://10.0.0.9/snowplow"}, "not '/content'"),
    ({"EVENTS_API_BASE_URL": "/events"}, "not an absolute URL"),
    ({"AUTHN_API_BASE_URL": ""}, "not an absolute URL"),
])
def test_anything_but_one_origin_at_057s_paths_is_refused(change, why):
    r = _origin(dict(SAME["api"], **change))
    assert r["origin"] is None and why in r["why"]


def test_the_cli_exports_the_origin_or_says_why_not(tmp_path):
    ok = subprocess.run([sys.executable, "-m", "bench.load606", "portal-origin"], input=json.dumps(SAME),
                        capture_output=True, text=True, cwd=BENCH_DIR, timeout=60)
    assert ok.returncode == 0 and "export PORTAL_ORIGIN=http://10.0.0.9" in ok.stdout
    run17 = {"api": {"SNOWPLOW_API_BASE_URL": "http://localhost:8081", "AUTHN_API_BASE_URL": "http://localhost:8082",
                     "EVENTS_API_BASE_URL": "http://localhost:8083",
                     "EVENTS_PUSH_API_BASE_URL": "http://localhost:8083"}}
    bad = subprocess.run([sys.executable, "-m", "bench.load606", "portal-origin"], input=json.dumps(run17),
                         capture_output=True, text=True, cwd=BENCH_DIR, timeout=60)
    assert bad.returncode == 1 and "3 origins" in bad.stderr and bad.stdout == ""


def test_the_installer_values_expose_the_portal_as_057_does():
    src = (BENCH_DIR / "run606.sh").read_text()
    block = src[src.index('cat > "$OUT/installer-values.yaml" <<EOF'):]
    block = block[block.index("\n") + 1:block.index("\nEOF\n")]
    v = yaml.safe_load(block.replace("${SNOWPLOW_TAG}", "x"))
    assert v["exposure"] == {"type": "Gateway", "gatewayMode": "path", "gatewayRef": {"name": "krateo-agent-gateway"},
                             "internalDataPlane": True, "platformPort": 80}
    assert v["features"]["agentGateway"] is True
    assert "BROWSER_FORWARDS" not in src and "--browser-forward" not in src


# ─── the route table check, per class (k6eval) ──────────────────────────────

def _o(points, count):
    return {"name": "k6", "stream": True, "points": {"route_table_mismatch": points, "page_loads": []},
            "times": {}, "frames": {"start": 1, "end": 1}, "truncated": False, "exit": {"rc": 0},
            "summary": {"metrics": {"route_table_mismatch": {"count": count}}, "config": {}}}


def test_the_route_table_check_names_each_class():
    c = next(x for x in k6eval.k6_checks(_o([(1, {"class": ADMINS})] * 9, 9)) if x["check"] == "k6.route_table")
    assert c["result"] == "FAIL" and c["by_class"] == {ADMINS: 9}
    assert f"9 of menu class {ADMINS}" in c["why"] and "menu class's table" in c["why"]
    ok = next(x for x in k6eval.k6_checks(_o([], 0)) if x["check"] == "k6.route_table")
    assert ok["result"] == "PASS" and "by_class" not in ok


# ─── navigate.js under node: clicks from the user's own menu ────────────────

_STUBS = {
    "index.js": "export function sleep (s) { globalThis.__clock += s * 1000 }\n",
    "execution.js": "export default globalThis.__exec\n",
    "http.js": "export default { batch: () => [] }\n",
    "data.js": "export class SharedArray { constructor (n, f) { return f() } }\n",
    "metrics.js": "function rec (name, v, t) { (globalThis.__M[name] = globalThis.__M[name] || []).push([v, t || {}]) }\n"
                  "export class Counter { constructor (n) { this.n = n } add (v, t) { rec(this.n, v, t) } }\n"
                  "export class Trend { constructor (n) { this.n = n } add (v, t) { rec(this.n, v, t) } }\n",
}
# spa.js's real sameMenu / userRoutes / pickRoute; a first load that returns
# globalThis.__menu (failing when __fail), and a walk that records its endpoint
_SPA = """export { sameMenu, userRoutes, pickRoute } from './spa_real.mjs'
export function firstLoad (portal, fetch, deepLink) {
  globalThis.__clock += 100
  const routes = globalThis.__fail ? [] : globalThis.__menu
  return { shell: { calls: 1, errors: 0, armed: [] }, content: null, routes,
           target: routes.length ? routes[0] : null }
}
export function firstLoadFailed (fl) { return !fl.routes.length }
export function walkTree (endpoints, fetch) {
  globalThis.__clock += 50
  ;(globalThis.__walked = globalThis.__walked || []).push(endpoints[0])
  return { calls: 1, errors: 0, armed: [] }
}
"""


def _navigate(tmp_path, user_index, menu, fail=False):
    pkg = tmp_path / "node_modules" / "k6"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "package.json").write_text(json.dumps({
        "name": "k6", "type": "module",
        "exports": {("." if n == "index.js" else f"./{n[:-3]}"): f"./{n}" for n in _STUBS}}))
    for n, src in _STUBS.items():
        (pkg / n).write_text(src)
    (tmp_path / "navigate.mjs").write_text(
        (BENCH_DIR / "k6" / "navigate.js").read_text().replace("'./spa.js'", "'./spa.mjs'"))
    (tmp_path / "spa_real.mjs").write_text((BENCH_DIR / "k6" / "spa.js").read_text())
    (tmp_path / "spa.mjs").write_text(_SPA)
    cfg = _cfg(users=30, burst_users=0, composite_routes=[], browser_users=[1], duration_s=30, think_s=1)
    (tmp_path / "config.json").write_text(json.dumps(dict(cfg, viewers=False)))
    (tmp_path / "tokens.json").write_text("[]")
    i = cfg["navUserIds"].index(user_index)
    (tmp_path / "globals.mjs").write_text(
        "import { readFileSync } from 'node:fs'\n"
        "globalThis.__ENV = { K6_CONFIG: 'config.json', TOKENS_FILE: 'tokens.json' }\n"
        "globalThis.open = (p) => readFileSync(p, 'utf8')\n"
        "globalThis.__clock = 1800000000000\nDate.now = () => globalThis.__clock\n"
        f"globalThis.__menu = {json.dumps(menu)}\nglobalThis.__fail = {json.dumps(fail)}\n"
        "globalThis.__M = {}\n"
        f"globalThis.__exec = {{ scenario: {{ name: 'navigate', iterationInTest: {i} }} }}\n")
    (tmp_path / "run.mjs").write_text(
        "const m = await import('./navigate.mjs')\n"
        "m.navigate({ t0: 1800000000000 })\n"
        "process.stdout.write(JSON.stringify({ M: globalThis.__M, walked: globalThis.__walked || [] }))\n")
    p = subprocess.run([NODE, "--import", "./globals.mjs", "run.mjs"], capture_output=True, text=True,
                       timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


@needs_node
def test_an_admin_clicks_its_own_menu_and_matches_its_class(tmp_path):
    r = _navigate(tmp_path, 2, list(reversed(ADMIN)))            # menu order differs: same menu
    assert "route_table_mismatch" not in r["M"]
    assert len(r["walked"]) > 5 and set(r["walked"]) == {x["endpoint"] for x in ADMIN}   # /marketplace too


@needs_node
def test_a_menu_that_is_not_the_classs_table_is_counted_with_its_class(tmp_path):
    r = _navigate(tmp_path, 2, DEVS)                              # an admin shown the devs menu
    assert r["M"]["route_table_mismatch"] == [[1, {"class": ADMINS}]]
    assert set(r["walked"]) <= {x["endpoint"] for x in DEVS}      # it clicks what it was shown
    r = _navigate(tmp_path / "d", 11, ADMIN)                      # a devs user shown the admin menu
    assert r["M"]["route_table_mismatch"] == [[1, {"class": "devs"}]]


@needs_node
def test_a_user_whose_first_load_failed_clicks_its_classs_table(tmp_path):
    r = _navigate(tmp_path, 2, ADMIN, fail=True)
    assert "route_table_mismatch" not in r["M"] and len(r["M"]["first_load_failed"]) == 1
    assert set(r["walked"]) == {x["endpoint"] for x in ADMIN}


# ─── the live-update probe (pageload.OBSERVER) under node ───────────────────

_PROBE = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const out = { streamOf: cases.urls.map((u) => pl.sseStreamOf(u, 'http://10.0.0.9/dashboard')) }
const listeners = {}
class FakeES {
  constructor (url) { this.url = url; listeners[url] = [] }
  addEventListener (ev, f) { if (ev === 'open') listeners[this.url].push(f) }
}
globalThis.window = {
  location: { href: 'http://10.0.0.9/dashboard', pathname: '/dashboard' },
  fetch: (u) => Promise.resolve({ ok: cases.ok[String(u)] !== false }),
  EventSource: FakeES, addEventListener () {}, PerformanceObserver: undefined,
}
globalThis.document = { querySelectorAll: () => [], addEventListener () {} }
globalThis.history = { pushState () {}, replaceState () {} }
globalThis.MutationObserver = class { observe () {} }
globalThis.performance = { now: () => 0 }
;(0, eval)(pl.OBSERVER)
for (const u of cases.fetched) await window.fetch(u)
await new Promise((r) => setTimeout(r, 10))
for (const u of cases.sources) { new window.EventSource(u); for (const f of listeners[u]) f() }
out.sse = window.__benchSse
process.stdout.write(JSON.stringify(out))
"""


def _probe(tmp_path, cases):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_PROBE)
    (tmp_path / "cases.json").write_text(json.dumps(cases))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "cases.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


@needs_node
def test_a_notifications_fetch_that_answered_ok_is_an_opened_stream(tmp_path):
    out = _probe(tmp_path, {
        "urls": ["http://10.0.0.9/events/notifications", "/events/notifications/", "http://localhost:8083/notifications",
                 "/content/refreshes", "/call?name=notifications&resource=panels", "/call?resource=x&name=refreshes",
                 "/content/call?x=/notifications", "::bad"],
        "fetched": ["http://10.0.0.9/events/notifications", "http://10.0.0.9/events/notifications",
                    "http://10.0.0.9/content/refreshes", "/call?name=notifications", "http://10.0.0.9/denied/notifications"],
        "ok": {"http://10.0.0.9/denied/notifications": False},
        "sources": ["http://localhost:8083/notifications?access_token=x"],   # an older SPA's EventSource
    })
    assert out["streamOf"] == ["notifications", "notifications", "notifications", "refreshes", None, None, None, None]
    # two ok /notifications fetches + the EventSource 'open'; the 401 one is not a stream
    assert out["sse"] == {"refreshes": 1, "notifications": 3}
