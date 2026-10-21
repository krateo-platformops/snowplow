"""Run 11's k6 + real-Chrome load (#830).

  * k6/spa.js is a port of load606.py's SPA model: under node, it must fetch
    exactly what load606.first_load / walk fetch on the same portal (the
    model's own tests in test_navmodel.py cite the frontend);
  * bench/k6eval.py judges the k6 and browser outputs: a stream that cannot
    prove it is whole, dropped arrival-rate iterations, errors, a route table
    that is not the users' own, too few browser sessions -> INVALID;
  * bench/k6_job.py renders the Jobs and refuses a plan the loadgen node
    cannot hold; login-tokens writes the tokens pod-locally.
"""

from __future__ import annotations

import concurrent.futures as cf
import gzip
import http.server
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
from pathlib import Path

import pytest
import yaml

from bench import k6_job, k6eval, load606, load606_job, rbac_model
from bench.load606 import SCALEUSER_PASSWORD
from tests.test_navmodel import INIT, NS, PORTAL, TREE, Fake, P, W, canon
from tests.test_run606_real_sequence import _calls, harness  # noqa: F401

BENCH_DIR = Path(__file__).resolve().parent.parent
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


# ─── spa.js parity with load606.py (node) ───────────────────────────────────

_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as spa from './spa.mjs'
const inp = JSON.parse(readFileSync(process.argv[2], 'utf8'))
function canon (path) {
  const [base, qs] = path.split('?')
  const kv = [...new URLSearchParams(qs || '')].sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)
  return base + '?' + kv.map(([k, v]) => `${k}=${v}`).join('&')
}
const calls = []
const fetchLevel = (paths) => paths.map((p) => {
  calls.push(p)
  const doc = inp.tree[canon(p)]
  return doc === undefined ? { status: 404, body: '{}' } : { status: 200, body: JSON.stringify(doc) }
})
const out = { eager: {}, menus: {}, slugs: {} }
for (const [k, d] of Object.entries(inp.docs)) out.eager[k] = spa.eagerRefs(d)
for (const [k, d] of Object.entries(inp.menus)) out.menus[k] = spa.menuRoutes(d, inp.ns)
for (const s of inp.slugs) out.slugs[s] = spa.routeSlug(s)
out.header = spa.headerContext(inp.portal.init)
const fl = spa.firstLoad(inp.portal, fetchLevel, inp.deepLink || null)
out.first = { calls: calls.slice(), routes: fl.routes, shellCalls: fl.shell.calls,
              contentCalls: fl.content ? fl.content.calls : null,
              target: fl.target ? fl.target.path : null, failed: spa.firstLoadFailed(fl) }
calls.length = 0
const w = spa.walkTree(inp.walkRoots, fetchLevel)
out.walk = { calls: calls.slice(), depth: w.depth, levels: w.levels }
out.fnv = inp.fnv.map((s) => spa.fnv1a(s))
process.stdout.write(JSON.stringify(out))
"""

MENU_TIES = W("Menu", {"r": P("flexes", "page-r")}, {"items": [
    {"path": "/b", "order": 2}, {"path": "/a", "order": 1}, {"path": "/c", "order": 2},
    {"path": "/x/{name}", "order": 3}, {"path": "/ns", "namespace": "other-ns", "order": 4},
    {"path": "/r", "resourceRefId": "r", "order": 0}, {"path": "/nested/deep/page"}]})


def _run_spa(tmp_path, spa_src: str, **extra) -> dict:
    (tmp_path / "spa.mjs").write_text(spa_src)
    (tmp_path / "driver.mjs").write_text(_DRIVER)
    inp = {"tree": {canon(k): v for k, v in TREE.items()},
           "docs": {k: v for k, v in TREE.items() if isinstance(v, dict) and "kind" in v},
           "menus": {"live": TREE[P("menus", "sidebar-nav")], "ties": MENU_TIES},
           "ns": NS, "slugs": ["/", "/dashboard", "/compositions/{namespace}/{name}", "/a/b/c"],
           "portal": {"init": INIT, "frontendNs": NS, "buildersNs": NS},
           "walkRoots": [P("flexes", "page-dashboard"), P("flexes", "page-compositions")],
           "fnv": ["", "a", "foobar", "17#3"], **extra}
    (tmp_path / "in.json").write_text(json.dumps(inp))
    p = subprocess.run([NODE, str(tmp_path / "driver.mjs"), str(tmp_path / "in.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


def _spa_src() -> str:
    return (BENCH_DIR / "k6" / "spa.js").read_text()


@needs_node
def test_spa_js_matches_load606_model(tmp_path):
    out = _run_spa(tmp_path, _spa_src())
    for k, doc in TREE.items():
        if isinstance(doc, dict) and "kind" in doc:
            assert out["eager"][k] == load606.eager_refs(doc), k
    for name, doc in (("live", TREE[P("menus", "sidebar-nav")]), ("ties", MENU_TIES)):
        assert out["menus"][name] == load606.menu_routes(doc, NS), name
    for s in ("/", "/dashboard", "/compositions/{namespace}/{name}", "/a/b/c"):
        assert out["slugs"][s] == load606._route_slug(s)
    assert out["header"] == PORTAL.header_context()
    # order ties keep menu order; no order = 0
    assert [r["path"] for r in out["menus"]["ties"]][:5] == [
        "/r", "/nested/deep/page", "/a", "/b", "/c"]


@needs_node
def test_spa_js_first_load_and_walk_fetch_what_load606_fetches(tmp_path):
    out = _run_spa(tmp_path, _spa_src())
    c = Fake()
    with cf.ThreadPoolExecutor(6) as pool:
        shell, routes, content = load606.first_load(c, "t", PORTAL, pool=pool)
    assert sorted(out["first"]["calls"]) == sorted(c.calls)
    assert out["first"]["shellCalls"] == shell.calls
    assert out["first"]["contentCalls"] == content.calls
    assert [{"path": r["path"], "endpoint": r["endpoint"]} for r in out["first"]["routes"]] == \
        [{"path": r["path"], "endpoint": r["endpoint"]} for r in routes]
    assert out["first"]["target"] == routes[0]["path"] and out["first"]["failed"] is False
    c2 = Fake()
    with cf.ThreadPoolExecutor(6) as pool:
        w = load606.walk(c2, "t", [P("flexes", "page-dashboard"), P("flexes", "page-compositions")],
                         pool=pool)
    assert sorted(out["walk"]["calls"]) == sorted(c2.calls)
    assert out["walk"]["depth"] == w.depth
    # level by level: a child only after its parent arrived
    assert out["walk"]["levels"][0] == [P("flexes", "page-dashboard"), P("flexes", "page-compositions")]
    assert P("buttons", "row-btn") in out["walk"]["levels"][2]


@needs_node
def test_spa_js_deep_link_targets_the_composite_route(tmp_path):
    out = _run_spa(tmp_path, _spa_src(), deepLink="/compositions")
    assert out["first"]["target"] == "/compositions"
    assert P("tables", "comps-a") in out["first"]["calls"]
    assert P("flexes", "page-dashboard") not in out["first"]["calls"]


@needs_node
def test_spa_js_fnv1a_vectors(tmp_path):
    out = _run_spa(tmp_path, _spa_src())
    # FNV-1a 32-bit reference values
    assert out["fnv"][:3] == [0x811C9DC5, 0xE40C292C, 0xBF9CF968]

    def fnv(s):
        h = 0x811C9DC5
        for ch in s.encode():
            h = ((h ^ ch) * 0x01000193) & 0xFFFFFFFF
        return h
    assert out["fnv"][3] == fnv("17#3")


@needs_node
def test_spa_js_parity_detects_a_drifted_port(tmp_path):
    """The parity test must fail on a port that drifts: Tabs mounting every
    tab instead of only the active one."""
    src = _spa_src().replace(
        "if (items.length && isObj(items[0]) && items[0].resourceRefId) ids.push(items[0].resourceRefId)",
        "for (const t of items) if (isObj(t) && t.resourceRefId) ids.push(t.resourceRefId)")
    assert src != _spa_src()
    out = _run_spa(tmp_path, src)
    k = P("flexes", "page-compositions")
    assert out["eager"][k] != load606.eager_refs(TREE[k])


@needs_node
@pytest.mark.parametrize("script", ["spa.js", "navigate.js", "browser.js"])
def test_k6_scripts_parse(tmp_path, script):
    (tmp_path / "s.mjs").write_text((BENCH_DIR / "k6" / script).read_text())
    p = subprocess.run([NODE, "--check", str(tmp_path / "s.mjs")], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr


# ─── k6eval: the k6 / browser outputs ───────────────────────────────────────


def _point(metric, value, **tags):
    return {"metric": metric, "type": "Point",
            "data": {"time": "2026-10-21T10:00:00Z", "value": value, "tags": tags}}


def _write_out(d: Path, name: str, points: list[dict], summary: dict | None, rc: int | None,
               frames=("start", "end"), cut=False):
    d.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps({"metric": "bench_frame", "type": "Metric", "data": {}})]
    if "start" in frames:
        lines.append(json.dumps(_point("bench_frame", 1, frame="start")))
    lines += [json.dumps(p) for p in points]
    # the lines the evaluator skips unparsed
    lines += [json.dumps(_point("http_req_waiting", 1.5, status="200"))] * 3
    if "end" in frames:
        lines.append(json.dumps(_point("bench_frame", 1, frame="end")))
    body = "\n".join(lines) + "\n"
    if cut:
        body = body[:-20]
    with gzip.open(d / f"{name}.json.gz", "wt") as fh:
        fh.write(body)
    if summary is not None:
        (d / f"{name}-summary.json").write_text(json.dumps(summary))
    if rc is not None:
        (d / f"{name}-exit.json").write_text(json.dumps({"kind": f"{name}_exit", "rc": rc, "t": 1}))


def _k6_summary(loads, *, dropped=None, iters=None, failed_rate=0.0, mismatch=0):
    iters = iters or {"navigate": loads - 2, "composite_burst": 2, "burst_navigate": 5}
    dropped = dropped or {}
    m = {"page_loads": {"count": loads}, "http_req_failed": {"rate": failed_rate},
         "route_table_mismatch": {"count": mismatch}}
    for sc, n in iters.items():
        m[f"iterations{{scenario:{sc}}}"] = {"count": n}
        m[f"dropped_iterations{{scenario:{sc}}}"] = {"count": dropped.get(sc, 0)}
    return {"kind": "k6_summary", "metrics": m}


def _k6_points(n):
    pts = []
    for i in range(n):
        kind = "composite_first_load" if i < 2 else ("first_load" if i < 5 else "route")
        tags = {"kind": kind, "route": f"/r{i % 3}", "visit": "first" if i < 7 else "warm"}
        pts += [_point("page_load_ms", 100 + i, **tags), _point("page_loads", 1, **tags),
                _point("http_req_duration", 10 + i, level=str(i % 2), status="200")]
    return pts


def _check(checks, name):
    return next(c for c in checks if c["check"] == name)


def test_k6_out_complete_passes(tmp_path):
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20), 0)
    o = k6eval.read_k6_out(str(tmp_path), "k6")
    checks = k6eval.k6_checks(o)
    assert all(c["result"] == "PASS" for c in checks), checks
    assert {c["check"] for c in checks} >= {
        "k6.stream_complete", "k6.dropped_iterations[navigate]",
        "k6.dropped_iterations[composite_burst]", "k6.error_rate", "k6.route_table"}
    rep = k6eval.k6_report(o)
    assert rep["page_load_by_kind_visit"]["route/warm"]["n"] == 13
    assert set(rep["call_ms_by_level"]) == {"0", "1"}


@pytest.mark.parametrize("mutate,why", [
    ({"frames": ("start",)}, "no end frame"),
    ({"frames": ("end",)}, "no start frame"),
    ({"cut": True}, "cut"),
    ({"rc": 1}, "exited rc=1"),
    ({"rc": None}, "no exit record"),
    ({"summary": None}, "no summary"),
    ({"summary_loads": 21}, "the summary counts 21 page loads, the stream holds 20"),
])
def test_k6_stream_that_cannot_prove_it_is_whole_fails(tmp_path, mutate, why):
    kw = {"rc": 0, "frames": ("start", "end"), "cut": False}
    kw.update({k: v for k, v in mutate.items() if k in kw})
    summary = (None if "summary" in mutate
               else _k6_summary(mutate.get("summary_loads", 20)))
    _write_out(tmp_path, "k6", _k6_points(20), summary, kw["rc"], frames=kw["frames"],
               cut=kw["cut"])
    c = _check(k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6")), "k6.stream_complete")
    assert c["result"] == "FAIL" and why in c["why"], c


def test_k6_nothing_collected_fails(tmp_path):
    assert k6eval.read_k6_out(str(tmp_path), "k6") is None
    c = k6eval.k6_checks(None)
    assert c == [{"check": "k6.stream_complete", "value": None, "bar": "collected",
                  "result": "FAIL", "why": "no k6 output was collected"}]


def test_dropped_iterations_are_a_validity_check(tmp_path):
    # navigate: 2 of 1000 dropped (0.2 % > 0.1 %); the burst: ANY drop
    _write_out(tmp_path, "k6", _k6_points(20),
               _k6_summary(20, iters={"navigate": 998, "composite_burst": 199, "burst_navigate": 50},
                           dropped={"navigate": 2, "composite_burst": 1}), 0)
    checks = k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6"))
    nav = _check(checks, "k6.dropped_iterations[navigate]")
    assert nav["result"] == "FAIL" and nav["value"] == 0.002 and "2 of 1000" in nav["why"]
    burst = _check(checks, "k6.dropped_iterations[composite_burst]")
    assert burst["result"] == "FAIL" and burst["value"] == 1
    assert _check(checks, "k6.dropped_iterations[burst_navigate]")["result"] == "PASS"
    # a scenario that scheduled nothing never ran: not a pass
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20, iters={"navigate": 0}), 0)
    c = _check(k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6")),
               "k6.dropped_iterations[navigate]")
    assert c["result"] == "FAIL" and "scheduled no navigate iteration" in c["why"]
    # at the bar: 1 of 1000 passes
    _write_out(tmp_path, "k6", _k6_points(20),
               _k6_summary(20, iters={"navigate": 999}, dropped={"navigate": 1}), 0)
    checks = k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6"))
    assert _check(checks, "k6.dropped_iterations[navigate]")["result"] == "PASS"


def test_k6_error_rate_and_route_table(tmp_path):
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20, failed_rate=0.02, mismatch=3), 0)
    checks = k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6"))
    assert _check(checks, "k6.error_rate")["result"] == "FAIL"
    rt = _check(checks, "k6.route_table")
    assert rt["result"] == "FAIL" and rt["value"] == 3
    s = _k6_summary(20)
    del s["metrics"]["http_req_failed"]
    _write_out(tmp_path, "k6", _k6_points(20), s, 0)
    assert _check(k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6")),
                  "k6.error_rate")["result"] == "FAIL"           # no data is not a pass


def _browser_points(sessions=("s1", "s2", "s3"), loads=21, timeouts=0):
    pts = []
    for s in sessions:
        pts.append(_point("browser_logins", 1, session=s))
        for i in range(loads):
            tags = {"session": s, "route": f"/r{i % 4}", "visit": "first" if i < 4 else "warm",
                    "method": "login" if i == 0 else "click"}
            pts += [_point("browser_page_load_ms", 200.0 + i, **tags),
                    _point("browser_page_loads", 1, **tags)]
    pts += [_point("browser_page_timeouts", 1, session=sessions[0])] * timeouts
    return pts


def _browser_summary(n):
    return {"kind": "browser_summary", "metrics": {"browser_page_loads": {"count": n}}}


def test_browser_out_and_navs(tmp_path):
    _write_out(tmp_path, "browser", _browser_points(), _browser_summary(63), 0)
    o = k6eval.read_k6_out(str(tmp_path), "browser")
    checks = k6eval.browser_checks(o, 5)
    assert all(c["result"] == "PASS" for c in checks), checks
    assert _check(checks, "browser.sessions")["value"] == 3
    navs = k6eval.browser_navs(o)
    assert len(navs) == 63 and sum(1 for n in navs if n["visit"] == 1) == 12
    assert {n["session"] for n in navs} == {"s1", "s2", "s3"}


def test_too_few_browser_sessions_or_loads_fail(tmp_path):
    _write_out(tmp_path, "browser", _browser_points(("s1", "s2")), _browser_summary(42), 0)
    checks = k6eval.browser_checks(k6eval.read_k6_out(str(tmp_path), "browser"), 5)
    assert _check(checks, "browser.sessions")["result"] == "FAIL"
    _write_out(tmp_path, "browser", _browser_points(loads=19), _browser_summary(57), 0)
    checks = k6eval.browser_checks(k6eval.read_k6_out(str(tmp_path), "browser"), 5)
    assert _check(checks, "browser.page_loads[s1]")["result"] == "FAIL"
    assert k6eval.browser_checks(None, 5)[0]["result"] == "FAIL"


def _evaluate_cli(tmp_path, k6dir, bdir):
    fresh = tmp_path / "fresh.jsonl"
    fresh.write_text(json.dumps({"kind": "fresh", "label": "list_dep@1", "converged": True,
                                 "latency_ms": 100, "polls": 1, "poll_ms": 100}) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(fresh),
                        "--k6-out", str(k6dir), "--browser-out", str(bdir),
                        "--browser-sessions", "3"],
                       capture_output=True, text=True, cwd=BENCH_DIR, timeout=120)
    return p.returncode, json.loads(p.stdout)


def test_evaluate_judges_browser_page_loads_and_k6_validity(tmp_path):
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_out(bdir, "browser", _browser_points(), _browser_summary(63), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir)
    names = {c["check"]: c for c in v["validity"]}
    for n in ("k6.stream_complete", "k6.dropped_iterations[navigate]", "k6.error_rate",
              "k6.route_table", "browser.stream_complete", "browser.sessions"):
        assert names[n]["result"] == "PASS", names[n]
    # the load606 driver's own checks do not apply
    assert not any(c.startswith("driver.") for c in names)
    assert _check(v["checks"], "browser.page_timeouts")["result"] == "PASS"
    rep = v["report"]
    assert rep["navs"] == 63                       # the north-star bars judge these
    assert rep["page_loads_are"].startswith("real Chrome")
    assert rep["browser"]["page_load_by_session"]["s1"]["n"] == 21
    assert rep["k6_report_only"]["page_load_by_kind_visit"]["route/warm"]["n"] == 13
    # a dropped burst iteration makes the run INVALID with its reason
    _write_out(k6dir, "k6", _k6_points(20),
               _k6_summary(20, dropped={"composite_burst": 1}), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir)
    assert rc == 3 and v["verdict"] == "INVALID"
    assert any("composite_burst" in r for r in v["invalid_reasons"]), v["invalid_reasons"]


def test_evaluate_without_browser_output_is_invalid(tmp_path):
    k6dir = tmp_path / "k6"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, tmp_path / "nobrowser")
    assert rc == 3
    reasons = " ".join(v["invalid_reasons"])
    assert "no browser output was collected" in reasons
    assert "no real-browser page load was recorded" in reasons


def test_browser_timeouts_fail_the_verdict(tmp_path):
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_out(bdir, "browser", _browser_points(timeouts=1), _browser_summary(63), 0)
    _, v = _evaluate_cli(tmp_path, k6dir, bdir)
    t = _check(v["checks"], "browser.page_timeouts")
    assert t["result"] == "FAIL" and t["value"] == 1


# ─── k6_job: sizing and rendering ───────────────────────────────────────────


def test_k6_plan_needs_the_larger_loadgen_node():
    with pytest.raises(ValueError, match="over budget on n2-standard-8"):
        k6_job.loadgen_fit("n2-standard-8", k6_job.k6_plan_requests())
    fit = k6_job.loadgen_fit("n2-standard-16", k6_job.k6_plan_requests())
    assert fit["cpu_m"] == 8350 and fit["cpu_m"] <= fit["budget_cpu_m"]
    with pytest.raises(ValueError, match="unknown loadgen machine type"):
        k6_job.loadgen_fit("e2-small", k6_job.k6_plan_requests())


def test_gke_loadgen_machine_is_configurable():
    from bench import gke
    spec = gke.default_spec("snowplow-bench-1021-1200", owner="t")
    assert next(p for p in spec.extra_pools if p.name == "loadgen").machine_type == "n2-standard-8"
    spec = gke.default_spec("snowplow-bench-1021-1200", owner="t",
                            loadgen_machine_type="n2-standard-16")
    assert next(p for p in spec.extra_pools if p.name == "loadgen").machine_type == "n2-standard-16"


def test_browser_sessions_are_distinct_identity_classes_outside_the_burst():
    m = rbac_model.RbacModel(users=1000, namespaces=50)
    s = k6_job.browser_sessions(m, 5, last=799)
    keys = {tuple(sorted(x["groups"])) for x in s}
    assert len(s) == 5 and len(keys) == 5
    assert s[0]["index"] == 1 and all(x["index"] <= 799 for x in s)
    assert sum(1 for x in s if len([g for g in x["groups"] if g.startswith("bench-team-")]) > 1) >= 1
    small = k6_job.browser_sessions(m, 5, last=12)       # never a burst user
    assert small and all(x["index"] <= 12 for x in small)


def _routes():
    return [{"path": "/dashboard", "endpoint": P("flexes", "page-dashboard")},
            {"path": "/compositions", "endpoint": P("flexes", "page-compositions")}]


def test_k6_config_drives_every_identity_once():
    cfg = k6_job.k6_config(snowplow_url="http://snowplow:8081/", init=INIT, frontend_ns=NS,
                           builders_ns=NS, routes=_routes(), users=1000, burst_users=200,
                           think_s=60, duration_s=1800, burst_at_s=900,
                           composite_routes=["/compositions"], browser_users=[1, 20, 11, 12, 13])
    nav, burst = cfg["navUserIds"], cfg["burstUserIds"]
    assert burst == list(range(801, 1001)) and len(nav) == 795
    assert not set(nav) & set(burst) and not {1, 20, 11, 12, 13} & set(nav)
    assert len(nav) + len(burst) + 5 == 1000
    assert cfg["snowplowUrl"] == "http://snowplow:8081" and cfg["maxVUs"] == 795
    assert cfg["preAllocatedVUs"] == 132                  # 795 / 60 s x 10
    # k6 refuses preAllocatedVUs > maxVUs (the kind proof's first run, rc 104)
    small = k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                             routes=_routes(), users=60, burst_users=10, think_s=5,
                             duration_s=1, burst_at_s=1, composite_routes=["/dashboard"],
                             browser_users=[1, 2, 3])
    assert small["preAllocatedVUs"] <= small["maxVUs"] == 47
    with pytest.raises(ValueError, match="outside the burst"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), users=1000, burst_users=200, think_s=60,
                         duration_s=1, burst_at_s=1, composite_routes=["/dashboard"], browser_users=[900])
    with pytest.raises(ValueError, match=r"not in the route table: \['/observability'\]"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), users=1000, burst_users=200, think_s=60,
                         duration_s=1, burst_at_s=1, composite_routes=["/observability"])
    with pytest.raises(ValueError, match="no route table"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=[], users=10, burst_users=0, think_s=1, duration_s=1,
                         burst_at_s=1, composite_routes=[])
    with pytest.raises(ValueError, match="think time"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), users=10, burst_users=0, think_s=0, duration_s=1,
                         burst_at_s=1, composite_routes=[])


def _preflight(tmp_path):
    f = tmp_path / "preflight.jsonl"
    rows = [{"path": r["path"], "endpoint": r["endpoint"], "status": 200} for r in _routes()]
    rows.append({"path": "/broken", "endpoint": P("flexes", "page-broken"), "status": 500})
    f.write_text("preflight: noise\n" + json.dumps({"kind": "preflight", "ok": True,
                                                    "routes": rows}) + "\n")
    return f


def _render(tmp_path, *extra):
    import contextlib
    import io
    buf = io.StringIO()
    argv = ["--users", "1000", "--driver", "k6", "--preflight-file", str(_preflight(tmp_path)),
            "--snowplow-url", "http://snowplow.krateo-system.svc:8081",
            "--authn-url", "http://authn.krateo-system.svc:8082", "--password", "pw-" + "bench",
            "--init", INIT, "--think-s", "60", "--duration-s", "1800",
            "--burst-users", "200", "--burst-at-s", "900", "--composite-route", "/compositions",
            "--fresh-rates", "1", "--mem-duration-s", "60", *extra]
    with contextlib.redirect_stdout(buf):
        assert load606_job.main(argv) == 0
    return list(yaml.safe_load_all(buf.getvalue()))


def test_render_k6_driver(tmp_path):
    docs = _render(tmp_path)
    by = {(d["kind"], d["metadata"]["name"]): d for d in docs}
    # fresh606 and mem606 are unchanged beside the k6 Jobs; no load606 driver
    assert {("Job", "k6load"), ("Job", "k6browser"), ("Job", "fresh606"), ("Job", "mem606"),
            ("ConfigMap", "load606"), ("ConfigMap", "k6"), ("ConfigMap", "k6browser")} <= set(by)
    assert ("Job", "load606") not in by
    k6 = by[("Job", "k6load")]["spec"]["template"]["spec"]
    c = k6["containers"][0]
    assert c["image"] == k6_job.K6_IMAGE and "@sha256:" in c["image"]
    assert c["readinessProbe"]["exec"]["command"] == ["test", "-f", "/out/done"]
    mounts = {m["name"]: m for m in c["volumeMounts"]}
    assert mounts["work"]["mountPath"] == "/work" and mounts["work"]["readOnly"] is True
    env = {e["name"]: e["value"] for e in c["env"]}
    assert env["TOKENS_FILE"] == "/work/tokens.json" and not env["TOKENS_FILE"].startswith("/out")
    login = k6["initContainers"][0]
    assert "login-tokens" in login["args"][0] and "--out /work/tokens.json" in login["args"][0]
    assert "/out" not in [m["mountPath"] for m in login["volumeMounts"]]
    assert {v["name"]: v for v in k6["volumes"]}["work"] == {"name": "work", "emptyDir": {}}
    assert k6["securityContext"]["runAsUser"] == k6_job.K6_UID
    cfg = json.loads(by[("ConfigMap", "k6")]["data"]["config.json"])
    assert [r["path"] for r in cfg["routes"]] == ["/dashboard", "/compositions"]  # 500 left out
    b = by[("Job", "k6browser")]["spec"]["template"]
    assert b["metadata"]["annotations"]["kubectl.kubernetes.io/default-container"] == "browser"
    names = [x["name"] for x in b["spec"]["containers"]]
    assert names == ["browser", "forward"]
    assert b["spec"]["containers"][0]["image"] == k6_job.K6_BROWSER_IMAGE
    assert b["spec"]["containers"][1]["command"][-3:] == [
        "8080=frontend.krateo-system.svc:8080", "8081=snowplow.krateo-system.svc:8081",
        "8082=authn.krateo-system.svc:8082"]
    bcfg = json.loads(by[("ConfigMap", "k6browser")]["data"]["browser-config.json"])
    browser_users = {int(s["user"].rsplit("-", 1)[1]) for s in bcfg["sessions"]}
    assert len(bcfg["sessions"]) == 5 and not browser_users & set(cfg["navUserIds"])
    for d in docs:                               # every pod on the loadgen pool
        if d["kind"] == "Job":
            assert d["spec"]["template"]["spec"]["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}


def test_render_k6_refuses_the_small_node_and_a_missing_route_table(tmp_path):
    with pytest.raises(ValueError, match="over budget"):
        _render(tmp_path, "--loadgen-machine-type", "n2-standard-8")
    import contextlib
    import io
    with pytest.raises(ValueError, match="no preflight route table"), \
            contextlib.redirect_stdout(io.StringIO()):
        load606_job.main(["--driver", "k6", "--preflight-file", str(tmp_path / "none.jsonl"),
                          "--snowplow-url", "u", "--authn-url", "u", "--password", "p",
                          "--init", INIT, "--think-s", "60", "--duration-s", "60",
                          "--burst-users", "0"])


# ─── login-tokens ───────────────────────────────────────────────────────────


class _Authn(http.server.BaseHTTPRequestHandler):
    deny: set = set()

    def do_GET(self):
        import base64
        user = base64.b64decode(self.headers["Authorization"].split()[1]).decode().split(":")[0]
        if self.path != "/basic/login" or user in self.deny:
            self.send_response(401)
            self.end_headers()
            return
        body = json.dumps({"accessToken": "tok-for-" + user[::-1]}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture
def authn():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Authn)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    _Authn.deny = set()
    srv.shutdown()


def _login_tokens(authn, out):
    return subprocess.run(
        [sys.executable, "-m", "bench.load606", "login-tokens",
         "--authn-url", f"http://127.0.0.1:{authn.server_address[1]}", "--password", "x",
         "--user-start", "1", "--user-end", "6", "--max-concurrency", "4", "--out", str(out)],
        capture_output=True, text=True, cwd=BENCH_DIR, timeout=120)


def test_login_tokens_writes_a_private_file_in_user_order(tmp_path, authn):
    out = tmp_path / "tokens.json"
    p = _login_tokens(authn, out)
    assert p.returncode == 0, p.stderr
    assert json.loads(out.read_text()) == [
        "tok-for-" + f"scaleuser-{i:04d}"[::-1] for i in range(1, 7)]
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o600
    assert "tok-for-" not in p.stdout + p.stderr            # never logged


def test_login_tokens_refuses_a_partial_population(tmp_path, authn):
    _Authn.deny = {"scaleuser-0004"}
    out = tmp_path / "tokens.json"
    p = _login_tokens(authn, out)
    assert p.returncode == 2 and "refusing a partial population" in p.stderr
    assert not out.exists()


def test_preflight_routes_keeps_only_servable_routes(tmp_path):
    assert load606_job.preflight_routes(str(_preflight(tmp_path))) == _routes()
    f = tmp_path / "empty.jsonl"
    f.write_text("nothing\n")
    with pytest.raises(ValueError, match="no preflight record"):
        load606_job.preflight_routes(str(f))


def test_navigate_js_reads_no_url_tags_and_keeps_tokens_out_of_out():
    src = (BENCH_DIR / "k6" / "navigate.js").read_text()
    assert "systemTags: ['status', 'method', 'scenario', 'expected_response', 'error_code']" in src
    assert "/work/tokens.json" in src and "/out/tokens" not in src


# ─── run606.sh with LOAD_DRIVER=k6 (fake gcloud / kubectl) ─────────────────


def _run606_k6(harness, **env):
    e, state, _, tmp = harness
    e = dict(e, LOAD_DRIVER="k6", USERS="20", THINK_S="1", COMPOSITE_BURST_USERS="5",
             BROWSER_SESSIONS="3", **env)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=e,
                       capture_output=True, text=True, timeout=300)
    return p, _calls(state), tmp / "out"


def test_run606_k6_end_to_end(harness):
    p, calls, out = _run606_k6(harness)
    log = p.stdout + p.stderr
    assert p.returncode == 0, log[-4000:]
    # the loadgen node is the n2-standard-16 the k6 plan needs
    pools = [c["argv"] for c in calls if c["tool"] == "gcloud" and c["argv"][:3] ==
             ["container", "node-pools", "create"]]
    lg = next(a for a in pools if a[3] == "loadgen")
    assert lg[lg.index("--machine-type") + 1] == "n2-standard-16"
    # k6 + browser Jobs applied, beside fresh606 and mem606; no load606 driver
    jobs = {d["metadata"]["name"] for d in yaml.safe_load_all((out / "loadgen-job.yaml").read_text())
            if d and d["kind"] == "Job"}
    assert jobs == {"k6load", "k6browser", "fresh606", "mem606"}
    assert "driver done: 1/1 k6load pod(s) Ready, every run exited 0" in log
    assert "driver done: 1/1 k6browser pod(s) Ready, every run exited 0" in log
    # collected under OUT/k6 and OUT/browser, judged by the verdict
    assert (out / "k6" / "k6.json.gz").exists() and (out / "browser" / "browser.json.gz").exists()
    v = json.loads((out / "verdict.json").read_text())
    val = {c["check"]: c["result"] for c in v["validity"]}
    for n in ("k6.stream_complete", "k6.dropped_iterations[navigate]",
              "k6.dropped_iterations[composite_burst]", "k6.error_rate", "k6.route_table",
              "browser.stream_complete", "browser.sessions"):
        assert val[n] == "PASS", (n, v["validity"])
    assert v["report"]["navs"] == 60                       # the browsers' page loads
    jp = [json.loads(x) for x in (out / "job-pods.jsonl").read_text().splitlines()]
    assert {r["job"] for r in jp} >= {"k6load", "k6browser", "fresh606", "mem606"}
    assert SCALEUSER_PASSWORD not in log
    assert SCALEUSER_PASSWORD not in (out / "run.log").read_text()


def test_run606_k6_dropped_burst_is_invalid(harness):
    p, _, out = _run606_k6(harness, FAKE_K6_OUT="dropped")
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    assert any("composite_burst" in r for r in v["invalid_reasons"])
    assert "INVALID: " in (out / "run.log").read_text()


def test_run606_k6_missing_browser_output_is_invalid(harness):
    p, _, out = _run606_k6(harness, FAKE_K6_OUT="nobrowser")
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    assert "could not copy k6browser's results (/out)" in (out / "run.log").read_text()
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    assert any("no browser output was collected" in r for r in v["invalid_reasons"])
