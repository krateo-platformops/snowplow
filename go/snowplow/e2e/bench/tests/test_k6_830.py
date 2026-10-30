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
import itertools
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
import time
import urllib.parse
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
  const rf = (inp.refresh || {})[canon(p)]
  const refresh = rf ? { key: rf[0], cls: rf[1] } : null
  return doc === undefined ? { status: 404, body: '{}', refresh }
    : { status: 200, body: JSON.stringify(doc), refresh }
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
out.walk = { calls: calls.slice(), depth: w.depth, levels: w.levels, armed: w.armed }
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


def _refresh_map():
    """Every tree widget answers with refresh headers, except one without any
    and one with a class snowplow does not stamp (neither is armed)."""
    m = {canon(k): ["key-" + str(i), "widgets"] for i, k in enumerate(TREE)}
    m[canon(P("piecharts", "pie"))] = ["key-x", "nonsense"]
    del m[canon(P("tables", "tbl"))]
    m[canon(P("flexes", "gone"))] = ["key-gone", "widgets"]      # on a 404: never armed
    return m


class _RefreshFake(Fake):
    def get(self, path, token):
        r = super().get(path, token)
        rf = _refresh_map().get(canon(path))
        if r.status == 200 and rf:
            r.refresh = tuple(rf)
        return r


@needs_node
def test_spa_js_arms_what_load606_walk_arms(tmp_path):
    """The widgets a tab arms (#830 viewer feed): every 200 answer that carried
    a refresh key and a known class, as load606.walk's `armed`."""
    roots = [P("flexes", "page-dashboard"), P("flexes", "page-compositions"), P("flexes", "gone")]
    out = _run_spa(tmp_path, _spa_src(), refresh=_refresh_map(), walkRoots=roots)
    c = _RefreshFake()
    with cf.ThreadPoolExecutor(6) as pool:
        w = load606.walk(c, "t", roots, pool=pool)
    py = sorted((e["path"], e["key"]) for e in w.armed)
    js = sorted((e["path"], e["key"]) for e in out["walk"]["armed"])
    assert js == py and len(js) >= 4
    assert P("piecharts", "pie") not in {p for p, _ in js}       # unknown class
    assert P("tables", "tbl") not in {p for p, _ in js}          # no headers
    assert P("flexes", "gone") not in {p for p, _ in js}         # not a 200
    assert {e["cls"] for e in out["walk"]["armed"]} == {"widgets"}


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


def _cpu_file(d: Path, container: str, request_m: int, cores: list[float], limit_m=None):
    """<container>-cpu.jsonl as k6_job._cpu_sampler writes it: cumulative
    usage every 5 s, one window per `cores` value."""
    recs = [{"kind": "cpu_start", "container": container, "request_m": request_m,
             "limit_m": limit_m, "interval_s": 5}]
    used = 0.0
    recs.append({"kind": "cpu", "t": 1000.0, "usage_usec": 0, "throttled_usec": 0})
    for i, c in enumerate(cores):
        used += c * 5
        recs.append({"kind": "cpu", "t": 1000.0 + 5 * (i + 1), "usage_usec": int(used * 1e6),
                     "throttled_usec": 0})
    recs.append({"kind": "cpu_end"})
    (d / f"{container}-cpu.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))


def _write_out(d: Path, name: str, points: list[dict], summary: dict | None, rc: int | None,
               frames=("start", "end"), cut=False, cpu=True):
    d.mkdir(parents=True, exist_ok=True)
    if cpu:
        for c in (("k6", "viewer") if name == "k6" else (name,)):
            _cpu_file(d, c, {"k6": 4000, "viewer": 1000, "browser": 3000}[c], [1.0] * 6,
                      limit_m=None if c == "browser" else 1000)
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


def _k6_summary(loads, *, dropped=None, iters=None, failed_rate=0.0, mismatch=0,
                burst=2, burst_loads=None):
    iters = iters or {"navigate": loads - 2, "composite_burst": 2, "burst_navigate": 5}
    dropped = dropped or {}
    m = {"page_loads": {"count": loads}, "http_req_failed": {"rate": failed_rate},
         "route_table_mismatch": {"count": mismatch},
         "page_loads{scenario:composite_burst}": {"count": burst if burst_loads is None
                                                  else burst_loads}}
    for sc, n in iters.items():
        m[f"iterations{{scenario:{sc}}}"] = {"count": n}
        m[f"dropped_iterations{{scenario:{sc}}}"] = {"count": dropped.get(sc, 0)}
    return {"kind": "k6_summary", "config": {"burstUsers": burst}, "metrics": m}


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
        "k6.composite_burst_complete", "k6.error_rate", "k6.route_table"}
    # per-vu-iterations never drops: no vacuous drop check for the burst (#831 gate)
    assert "k6.dropped_iterations[composite_burst]" not in {c["check"] for c in checks}
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
    # navigate: 2 of 1000 dropped (0.2 % > 0.1 %)
    _write_out(tmp_path, "k6", _k6_points(20),
               _k6_summary(20, iters={"navigate": 998, "composite_burst": 199, "burst_navigate": 50},
                           dropped={"navigate": 2}), 0)
    checks = k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6"))
    nav = _check(checks, "k6.dropped_iterations[navigate]")
    assert nav["result"] == "FAIL" and nav["value"] == 0.002 and "2 of 1000" in nav["why"]
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


def test_composite_burst_must_be_whole(tmp_path):
    """All burst users' page loads ran (#831 gate: per-vu-iterations never
    drops, so a drop count proved nothing; one cut by maxDuration is missing)."""
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20, burst=200, burst_loads=199), 0)
    c = _check(k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6")),
               "k6.composite_burst_complete")
    assert c["result"] == "FAIL" and c["value"] == 199 and "199 of the 200" in c["why"]
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20, burst=200, burst_loads=200), 0)
    assert _check(k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6")),
                  "k6.composite_burst_complete")["result"] == "PASS"
    _write_out(tmp_path, "k6", _k6_points(20), _k6_summary(20, burst=0), 0)      # no burst
    assert "k6.composite_burst_complete" not in {
        c["check"] for c in k6eval.k6_checks(k6eval.read_k6_out(str(tmp_path), "k6"))}


def test_cpu_stats_and_the_browser_headroom_check(tmp_path):
    _cpu_file(tmp_path, "browser", 3000, [1.0, 1.2, 2.0, 1.1, 1.0])
    st = k6eval.cpu_stats(k6eval.read_cpu(str(tmp_path), "browser"))
    assert st["windows"] == 5 and st["max_cores"] == 2.0 and st["request_m"] == 3000
    o = {"cpu": {"browser": k6eval.read_cpu(str(tmp_path), "browser")}}
    c = k6eval.cpu_check(o, "browser")
    assert c["result"] == "PASS" and c["bar"] == "p95 <= 2.4 cores (0.8 x request)"
    # Chrome at its request most of the run: starved, the timings can't be judged
    _cpu_file(tmp_path, "browser", 3000, [2.9] * 6)
    c = k6eval.cpu_check({"cpu": {"browser": k6eval.read_cpu(str(tmp_path), "browser")}}, "browser")
    assert c["result"] == "FAIL" and "CPU-starved" in c["why"]
    # no samples: unknown is not a pass
    assert k6eval.cpu_check({"cpu": {}}, "browser")["result"] == "FAIL"
    _cpu_file(tmp_path, "browser", 3000, [1.0, 1.0])               # 2 windows < 3
    assert k6eval.cpu_check({"cpu": {"browser": k6eval.read_cpu(str(tmp_path), "browser")}},
                            "browser")["result"] == "FAIL"


def test_widget_error_states_are_gated(tmp_path):
    # 1 error state over 63 loads = 1.6 % > 1 %; forbidden/notfound never count
    _write_out(tmp_path, "browser", _browser_points(error_states=1), _browser_summary(63), 0)
    c = k6eval.widget_error_check(k6eval.read_k6_out(str(tmp_path), "browser"))
    assert c["result"] == "FAIL" and c["value"]["loads_with_error_state"] == 1
    _write_out(tmp_path, "browser", _browser_points(), _browser_summary(63), 0)
    c = k6eval.widget_error_check(k6eval.read_k6_out(str(tmp_path), "browser"))
    assert c["result"] == "PASS" and c["value"]["loads_with_error_state"] == 0


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


def _browser_points(sessions=("s1", "s2", "s3"), loads=21, timeouts=0,
                    sse=("refreshes", "notifications"), error_states=0):
    pts = []
    for s in sessions:
        pts.append(_point("browser_logins", 1, session=s))
        pts.append(_point("browser_signin_ms", 3100.0, session=s))
        for stream in ("refreshes", "notifications"):
            pts.append(_point("browser_sse_opened", 2 if stream in sse else 0,
                              session=s, stream=stream))
        for i in range(loads):
            tags = {"session": s, "route": f"/r{i % 4}", "visit": "first" if i < 4 else "warm",
                    "method": "login" if i == 0 else "click"}
            pts += [_point("browser_page_load_ms", 200.0 + i, **tags),
                    _point("browser_page_loads", 1, **tags)]
    for _ in range(timeouts):                    # pageload.recordOps: a timeout is a load too
        t = {"session": sessions[0], "route": "/r0", "visit": "warm", "method": "click",
             "skeleton": "timeout", "timeout": "yes"}
        pts += [_point("browser_page_timeouts", 1, session=sessions[0], route="/r0"),
                _point("browser_page_load_ms", 60000.0, **t), _point("browser_page_loads", 1, **t)]
    pts += [_point("browser_widget_states", 1, session=sessions[0], route="/r1", state="error")
            ] * error_states
    pts += [_point("browser_widget_states", 2, session=sessions[0], route="/r1", state="forbidden")]
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


def test_a_session_without_live_updates_fails(tmp_path):
    # the sse-proxy firehose never opened (e.g. :8083 not forwarded)
    _write_out(tmp_path, "browser", _browser_points(sse=("refreshes",)), _browser_summary(63), 0)
    c = _check(k6eval.browser_checks(k6eval.read_k6_out(str(tmp_path), "browser"), 3),
               "browser.live_refresh[s1]")
    assert c["result"] == "FAIL" and "/notifications" in c["why"]
    assert c["value"] == {"refreshes": 2, "notifications": 0}
    _write_out(tmp_path, "browser", _browser_points(sse=()), _browser_summary(63), 0)
    c = _check(k6eval.browser_checks(k6eval.read_k6_out(str(tmp_path), "browser"), 3),
               "browser.live_refresh[s2]")
    assert c["result"] == "FAIL" and "/refreshes, /notifications" in c["why"]


# ─── the viewer sidecar's output ────────────────────────────────────────────


def _write_viewer(d: Path, users=("u1", "u2", "u3", "u4", "u5"), *, ended_by="feed_end",
                  rc=0, start=True, end=True, cut=False):
    d.mkdir(parents=True, exist_ok=True)
    recs = [{"kind": "viewer_start", "t": 1}] if start else []
    for i, u in enumerate(users):
        recs.append({"kind": "viewer_armed", "user": u, "nav": f"navigate:{i}", "t": 2,
                     "arm_lag_ms": 10.0 + i})
    recs += [{"kind": "push_refetch", "user": users[0], "key": "k", "path": "/call?x",
              "t_event": 3, "start_lag_ms": 1.0, "wall_ms": 20.0, "status": 200},
             {"kind": "viewer_dead", "t": 4, "tabs_dead": 0, "tabs_armed": len(users)},
             {"kind": "viewer_stats", "t": 5, "streams_opened": len(users), "frames": 1}]
    if end:
        recs.append({"kind": "viewer_end", "t": 6, "ok": rc == 0, "rc": rc, "ended_by": ended_by,
                     "navs": 20})
    body = "".join(json.dumps(r) + "\n" for r in recs)
    (d / "viewer.jsonl").write_text(body[:-15] if cut else body)
    if rc is not None:
        (d / "viewer-exit.json").write_text(json.dumps({"kind": "viewer_exit", "rc": rc}))


def _k6o(tmp_path, failed_first=0):
    pts = _k6_points(20) + [_point("first_load_failed", 1, kind="first_load")] * failed_first
    _write_out(tmp_path, "k6", pts, _k6_summary(20), 0)
    return k6eval.read_k6_out(str(tmp_path), "k6")


def test_viewer_complete_and_armed_passes(tmp_path):
    k6o = _k6o(tmp_path)                 # 5 users reached a working portal
    _write_viewer(tmp_path)
    v = k6eval.read_viewer_out(str(tmp_path))
    checks = k6eval.viewer_checks(v, k6o)
    assert [(c["check"], c["result"]) for c in checks] == [
        ("viewer.stream_complete", "PASS"), ("viewer.armed_users", "PASS")]
    assert checks[1]["value"] == {"armed": 5, "active": 5, "share": 1.0}
    rep = k6eval.viewer_report(v)
    assert rep["arms"] == 5 and rep["arm_lag_p50_ms"] == 12.0 and rep["arm_lag_max_ms"] == 14.0
    assert rep["push_refetch"]["refetches"] == 1 and rep["stats"]["streams_opened"] == 5


@pytest.mark.parametrize("kw,why", [
    ({"start": False}, "no viewer_start first"),
    ({"end": False}, "no viewer_end"),
    ({"ended_by": "k6 exited without feed_end: the feed is cut"}, "the viewers stopped on: k6 exited"),
    ({"rc": 3}, "exited rc=3"),
    ({"rc": None}, "no exit record"),
    ({"cut": True}, "cut"),
])
def test_viewer_stream_that_is_not_whole_fails(tmp_path, kw, why):
    _write_viewer(tmp_path, **kw)
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)), _k6o(tmp_path))[0]
    assert c["result"] == "FAIL" and why in c["why"], c


def test_viewer_armed_users_bar(tmp_path):
    # 4 armed of the 5 k6 brought in: 0.8 < 0.9
    _write_viewer(tmp_path, users=("u1", "u2", "u3", "u4"))
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)), _k6o(tmp_path))[1]
    assert c["result"] == "FAIL" and c["value"]["share"] == 0.8
    # one of k6's first loads failed: 4 of 4 active
    c = k6eval.viewer_checks(k6eval.read_viewer_out(str(tmp_path)),
                             _k6o(tmp_path, failed_first=1))[1]
    assert c["result"] == "PASS" and c["value"]["active"] == 4
    assert k6eval.viewer_checks(None, None)[0]["result"] == "FAIL"


def _evaluate_cli(tmp_path, k6dir, bdir, *extra):
    fresh = tmp_path / "fresh.jsonl"
    fresh.write_text(json.dumps({"kind": "fresh", "label": "list_dep@1", "converged": True,
                                 "latency_ms": 100, "polls": 1, "poll_ms": 100}) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(fresh),
                        "--k6-out", str(k6dir), "--browser-out", str(bdir),
                        "--browser-sessions", "3", *extra],
                       capture_output=True, text=True, cwd=BENCH_DIR, timeout=120)
    return p.returncode, json.loads(p.stdout)


def test_evaluate_judges_browser_page_loads_and_k6_validity(tmp_path):
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_viewer(k6dir)
    _write_out(bdir, "browser", _browser_points(), _browser_summary(63), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir)
    names = {c["check"]: c for c in v["validity"]}
    for n in ("k6.stream_complete", "k6.dropped_iterations[navigate]", "k6.error_rate",
              "k6.route_table", "browser.stream_complete", "browser.sessions",
              "browser.live_refresh[s1]", "viewer.stream_complete", "viewer.armed_users"):
        assert names[n]["result"] == "PASS", names[n]
    assert all(c["result"] == "PASS" for c in v["validity"]
               if c["check"].startswith(("k6.", "browser.", "viewer."))), v["validity"]
    # the load606 driver's own checks do not apply
    assert not any(c.startswith("driver.") for c in names)
    assert _check(v["checks"], "browser.page_timeouts")["result"] == "PASS"
    assert _check(v["checks"], "browser.widget_errors")["result"] == "PASS"
    assert _check(v["validity"], "browser.cpu_headroom")["result"] == "PASS"
    rep = v["report"]
    assert rep["navs"] == 63                       # the north-star bars judge these
    assert rep["page_loads_are"].startswith("real Chrome")
    assert rep["browser"]["page_load_by_session"]["s1"]["n"] == 21
    assert rep["k6_report_only"]["page_load_by_kind_visit"]["route/warm"]["n"] == 13
    assert rep["viewer_report_only"]["arms"] == 5
    # a burst user's page load missing makes the run INVALID with its reason
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20, burst=2, burst_loads=1), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir)
    assert rc == 3 and v["verdict"] == "INVALID"
    assert any("composite_burst_complete" in r for r in v["invalid_reasons"]), v["invalid_reasons"]


def test_evaluate_viewer_output_is_required_unless_no_viewers(tmp_path):
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_out(bdir, "browser", _browser_points(), _browser_summary(63), 0)
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir)
    assert rc == 3 and any("no viewer output was collected" in r for r in v["invalid_reasons"])
    rc, v = _evaluate_cli(tmp_path, k6dir, bdir, "--no-viewers")      # VIEWERS=0
    assert not any(c["check"].startswith("viewer.") for c in v["validity"])


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
    # k6 4 + viewer sidecar 1 + Chrome 3 + fresh606 1 + mem606 0.25
    assert fit["cpu_m"] == 9250 and fit["cpu_m"] <= fit["budget_cpu_m"]
    assert k6_job.loadgen_fit("n2-standard-16",
                              k6_job.k6_plan_requests(viewers=False))["cpu_m"] == 8250
    with pytest.raises(ValueError, match="unknown loadgen machine type"):
        k6_job.loadgen_fit("e2-small", k6_job.k6_plan_requests())


def _gke_allocatable(vcpu: int, mem_gib: int) -> dict:
    """GKE's reservation (cloud.google.com/kubernetes-engine/docs/concepts/
    plan-node-sizes), derived here independently of k6_job's table."""
    cpu_res = sum(min(max(vcpu - lo, 0), hi - lo) * pct * 1000 for lo, hi, pct in
                  ((0, 1, .06), (1, 2, .01), (2, 4, .005), (4, 10 ** 6, .0025)))
    mem_res = sum(min(max(mem_gib - lo, 0), hi - lo) * pct for lo, hi, pct in
                  ((0, 4, .25), (4, 8, .20), (8, 16, .10), (16, 128, .06), (128, 10 ** 6, .02)))
    return {"cpu_m": int(vcpu * 1000 - cpu_res),
            "memory_mi": int(mem_gib * 1024 - int(mem_res * 1024) - 100)}


def test_loadgen_allocatable_is_gkes_formula():
    """#831 gate: an inflated table let a plan 'fit' a node it would not."""
    assert k6_job.LOADGEN_ALLOCATABLE["n2-standard-8"] == _gke_allocatable(8, 32)
    assert k6_job.LOADGEN_ALLOCATABLE["n2-standard-16"] == _gke_allocatable(16, 64)


def test_gke_loadgen_machine_is_configurable():
    from bench import gke
    spec = gke.default_spec("snowplow-bench-1021-1200", owner="t")
    assert next(p for p in spec.extra_pools if p.name == "loadgen").machine_type == "n2-standard-8"
    spec = gke.default_spec("snowplow-bench-1021-1200", owner="t",
                            loadgen_machine_type="n2-standard-16")
    assert next(p for p in spec.extra_pools if p.name == "loadgen").machine_type == "n2-standard-16"


def test_browser_sessions_are_singleton_classes_no_k6_user_shares():
    m = rbac_model.RbacModel(users=1000, namespaces=50)
    s = k6_job.browser_sessions(m, 5, last=799)
    # an admin, then cross-team users: each class its own (1,000 users / 50 ns)
    assert [x["index"] for x in s] == [1, 20, 30, 40, 50]
    classes = {}
    for i in range(1, 1001):
        classes.setdefault(tuple(sorted(rbac_model.user_groups(i, m))), []).append(i)
    for x in s:
        assert classes[tuple(sorted(x["groups"]))] == [x["index"]] and x["cold"] is True
    cfg = k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                           routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=1000, burst_users=200, think_s=60,
                           duration_s=1, burst_at_s=1, composite_routes=["/dashboard"],
                           browser_users=[x["index"] for x in s])
    k6_classes = {tuple(sorted(rbac_model.user_groups(i, m)))
                  for i in cfg["navUserIds"] + cfg["burstUserIds"]}
    assert not k6_classes & {tuple(sorted(x["groups"])) for x in s}
    # too few singletons: a shared class is taken and the label says so
    tiny = rbac_model.RbacModel(users=4, namespaces=2, admins=0, cross_team_every=1000)
    t = k6_job.browser_sessions(tiny, 2)
    assert [x["cold"] for x in t] == [False, False]
    assert k6_job.first_visit_label(t).startswith("first for this tab, not cold")


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
                           builders_ns=NS, routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=1000, burst_users=200,
                           think_s=60, duration_s=1800, burst_at_s=900,
                           composite_routes=["/compositions"], browser_users=[1, 20, 11, 12, 13])
    nav, burst = cfg["navUserIds"], cfg["burstUserIds"]
    assert burst == list(range(801, 1001)) and len(nav) == 795
    assert not set(nav) & set(burst) and not {1, 20, 11, 12, 13} & set(nav)
    assert len(nav) + len(burst) + 5 == 1000
    assert cfg["snowplowUrl"] == "http://snowplow:8081"
    # run 12: one VU per user (navigate.js sessions), no arrival-rate VU pool
    assert "maxVUs" not in cfg and "preAllocatedVUs" not in cfg
    with pytest.raises(ValueError, match="outside the burst"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=1000, burst_users=200, think_s=60,
                         duration_s=1, burst_at_s=1, composite_routes=["/dashboard"], browser_users=[900])
    with pytest.raises(ValueError, match=r"not in the route table: \['/observability'\]"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=1000, burst_users=200, think_s=60,
                         duration_s=1, burst_at_s=1, composite_routes=["/observability"])
    with pytest.raises(ValueError, match="no route table"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=[], menu_classes={}, class_of=_one_class, users=10, burst_users=0, think_s=1, duration_s=1,
                         burst_at_s=1, composite_routes=[])
    with pytest.raises(ValueError, match="think time"):
        k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                         routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=10, burst_users=0, think_s=0, duration_s=1,
                         burst_at_s=1, composite_routes=[])


def _preflight(tmp_path):
    f = tmp_path / "preflight.jsonl"
    rows = [{"path": r["path"], "endpoint": r["endpoint"], "status": 200} for r in _routes()]
    rows.append({"path": "/broken", "endpoint": P("flexes", "page-broken"), "status": 500})
    f.write_text("preflight: noise\n" + json.dumps({"kind": "preflight", "ok": True,
                                                    "routes": rows}) + "\n")
    return f


# the portal's one origin (run 19: the browser loads it as 057's users do)
ORIGIN = ("--browser-origin", "http://10.0.0.9", "--browser-snowplow-url", "http://10.0.0.9/content")


def _render(tmp_path, *extra):
    import contextlib
    import io
    buf = io.StringIO()
    argv = ["--users", "1000", "--driver", "k6", "--preflight-file", str(_preflight(tmp_path)),
            "--snowplow-url", "http://snowplow.krateo-system.svc:8081",
            "--authn-url", "http://authn.krateo-system.svc:8082", "--password", "pw-" + "bench",
            "--init", INIT, "--think-s", "60", "--duration-s", "1800",
            "--burst-users", "200", "--burst-at-s", "900", "--composite-route", "/compositions",
            "--fresh-rates", "1", "--mem-duration-s", "60",
            # each menu class's table (run606 s_classes): here the one portal menu
            "--class-preflight", f"devs={tmp_path / 'preflight.jsonl'}",
            "--class-preflight", f"bench-platform-admins+devs={tmp_path / 'preflight.jsonl'}",
            *(extra or ORIGIN)]
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
    tpl = by[("Job", "k6load")]["spec"]["template"]
    assert tpl["metadata"]["annotations"]["kubectl.kubernetes.io/default-container"] == "k6"
    k6 = tpl["spec"]
    assert [x["name"] for x in k6["containers"]] == ["k6", "viewer"]
    c = k6["containers"][0]
    assert "--log-format raw --console-output /feed/nav.jsonl" in c["args"][0]
    vw = k6["containers"][1]
    assert "view-feed" in vw["args"][0] and "--stop-file /out/done" in vw["args"][0]
    assert "> /out/viewer-done" in vw["args"][0] and "> /out/done" not in vw["args"][0]
    assert vw["readinessProbe"]["exec"]["command"] == ["test", "-f", "/out/viewer-done"]
    vmounts = {m["name"]: m for m in vw["volumeMounts"]}
    assert vmounts["work"]["readOnly"] is True and vmounts["feed"]["readOnly"] is True
    assert vmounts["out"]["mountPath"] == "/out"
    assert vw["resources"]["requests"] == {"cpu": "1", "memory": "2Gi"}
    # #831 gate M1: k6 and the viewer are LIMITED (Chrome keeps its CPU); every
    # container samples its own CPU into /out
    assert vw["resources"]["limits"] == {"cpu": "1"}
    assert c["resources"]["limits"] == {"cpu": "4"} and c["resources"]["requests"]["cpu"] == "4"
    assert {e["name"]: e["value"] for e in c["env"]}["GOMAXPROCS"] == "4"
    assert "/out/k6-cpu.jsonl" in c["args"][0] and "/sys/fs/cgroup/cpu.stat" in c["args"][0]
    assert "/out/viewer-cpu.jsonl" in vw["args"][0]
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
    assert cfg["viewers"] is True
    b = by[("Job", "k6browser")]["spec"]["template"]
    assert b["metadata"]["annotations"]["kubectl.kubernetes.io/default-container"] == "browser"
    assert [x["name"] for x in b["spec"]["containers"]] == ["browser"]
    assert b["spec"]["containers"][0]["image"] == k6_job.K6_BROWSER_IMAGE
    # run 19: no sidecar; run 11's race is covered by the browser waiting for
    # an HTTP answer from the portal's ORIGIN before it starts
    assert "initContainers" not in b["spec"]
    args = b["spec"]["containers"][0]["args"][0]
    assert "for u in http://10.0.0.9/; do" in args
    assert args.index("wait_origin && ") < args.index("k6 run --quiet")
    bcfg = json.loads(by[("ConfigMap", "k6browser")]["data"]["browser-config.json"])
    assert bcfg["frontendUrl"] == "http://10.0.0.9" and bcfg["snowplowUrl"] == "http://10.0.0.9/content"
    browser_users = {int(s["user"].rsplit("-", 1)[1]) for s in bcfg["sessions"]}
    assert len(bcfg["sessions"]) == 5 and not browser_users & set(cfg["navUserIds"])
    # singleton identity classes: no k6 user shares their group set (#831 gate)
    assert sorted(browser_users) == [1, 20, 30, 40, 50]
    assert bcfg["firstVisit"].startswith("cold for the identity class")
    assert "pageload.js" in by[("ConfigMap", "k6browser")]["data"]
    bc = b["spec"]["containers"][0]
    assert "/out/browser-cpu.jsonl" in bc["args"][0] and "limits" not in bc["resources"]
    for d in docs:                               # every pod on the loadgen pool
        if d["kind"] == "Job":
            assert d["spec"]["template"]["spec"]["nodeSelector"] == {"bench.krateo.io/pool": "loadgen"}


def test_render_k6_without_viewers(tmp_path):
    docs = _render(tmp_path, "--no-viewers", *ORIGIN)
    by = {(d["kind"], d["metadata"]["name"]): d for d in docs}
    k6 = by[("Job", "k6load")]["spec"]["template"]["spec"]
    assert [x["name"] for x in k6["containers"]] == ["k6"]
    assert "--console-output" not in k6["containers"][0]["args"][0]
    assert json.loads(by[("ConfigMap", "k6")]["data"]["config.json"])["viewers"] is False


def test_render_k6_refuses_the_small_node_and_a_missing_route_table(tmp_path):
    with pytest.raises(ValueError, match="over budget"):
        _render(tmp_path, "--loadgen-machine-type", "n2-standard-8", *ORIGIN)
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


# ─── view-feed: the k6 pod's viewer sidecar ─────────────────────────────────


class _Snowplow(http.server.BaseHTTPRequestHandler):
    """/call answers with refresh headers (key-<name>); /refreshes streams one
    `refresh` frame for its first widget, then keepalives until closed."""
    protocol_version = "HTTP/1.1"
    subs: list = []

    def do_GET(self):
        name = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("name", ["?"])[0]
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Snowplow-Refresh-Key", "key-" + name)
        self.send_header("X-Snowplow-Refresh-Class", "widgets")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        coords = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).subs.append((self.headers.get("Authorization"), [c["name"] for c in coords]))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.close_connection = True
        try:
            time.sleep(0.2)
            self.wfile.write(f"event: refresh\ndata: key-{coords[0]['name']}\n\n".encode())
            self.wfile.flush()
            while True:
                time.sleep(0.5)
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except OSError:
            pass

    def log_message(self, *a):
        pass


@pytest.fixture
def snowplow():
    _Snowplow.subs = []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Snowplow)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()


def _armed(*names):
    return [{"path": P("flexes", n), "key": "key-" + n, "cls": "widgets"} for n in names]


def _view_feed(tmp_path, snowplow, lines, *, stop=False):
    toks = [f"tok-{i}" for i in range(1, 4)]
    (tmp_path / "tokens.json").write_text(json.dumps(toks))
    feed, out, stopf = tmp_path / "nav.jsonl", tmp_path / "viewer.jsonl", tmp_path / "done"
    p = subprocess.Popen(
        [sys.executable, "-m", "bench.load606", "view-feed",
         "--snowplow-url", f"http://127.0.0.1:{snowplow.server_address[1]}",
         "--tokens", str(tmp_path / "tokens.json"), "--feed", str(feed),
         "--stop-file", str(stopf), "--out", str(out), "--poll-s", "0.05"],
        cwd=BENCH_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(0.3)                                    # the feed appears after the sidecar
    with open(feed, "a") as fh:
        for ln in lines:
            fh.write(json.dumps(ln) + "\n")
            fh.flush()
            time.sleep(0.4)                            # frames + refetches happen meanwhile
    if stop:
        stopf.write_text("0\n")
    _, err = p.communicate(timeout=60)
    return p.returncode, [json.loads(x) for x in out.read_text().splitlines()], err


def test_view_feed_arms_each_users_tab_on_what_k6_received(tmp_path, snowplow):
    t = time.time()
    rc, recs, err = _view_feed(tmp_path, snowplow, [
        {"kind": "feed_start", "t": t},
        {"kind": "nav", "nav": "navigate:0", "user": 1, "route": "/dashboard", "t_end": t, "ok": True,
         "shell": _armed("sider"), "page": _armed("page-dashboard", "tbl")},
        {"kind": "nav", "nav": "navigate:1", "user": 2, "route": "/dashboard", "t_end": t,
         "ok": False, "shell": _armed("sider"), "page": []},  # no working portal: not armed
        {"kind": "nav", "nav": "navigate:3", "user": 3, "route": "/dashboard", "t_end": t,
         "ok": True, "shell": [], "page": _armed("page-other")},
        {"kind": "nav", "nav": "navigate:2", "user": 1, "route": "/compositions", "t_end": t,
         "ok": True, "page": _armed("page-compositions")},  # the shell stays armed
        {"kind": "feed_end", "t": t}])
    assert rc == 0, err
    kinds = [r["kind"] for r in recs]
    assert kinds[0] == "viewer_start" and kinds[-1] == "viewer_end"
    end = recs[-1]
    assert end["ended_by"] == "feed_end" and end["navs"] == 4 and end["navs_failed"] == 1
    armed = [r for r in recs if r["kind"] == "viewer_armed"]
    assert {r["user"] for r in armed} == {"scaleuser-0001", "scaleuser-0003"}
    assert ("Bearer tok-3", ["page-other"]) in _Snowplow.subs      # each user's own token
    assert all(r["arm_lag_ms"] >= 0 for r in armed)
    # the second nav re-armed the user's ONE stream on shell + new page, with its token
    assert _Snowplow.subs[0] == ("Bearer tok-1", ["sider", "page-dashboard", "tbl"])
    assert _Snowplow.subs[-1] == ("Bearer tok-1", ["sider", "page-compositions"])
    # a frame refetched the widget like the SPA (push_refetch), then close
    pr = [r for r in recs if r["kind"] == "push_refetch"]
    assert pr and all(r["status"] == 200 for r in pr) and pr[0]["key"] == "key-sider"
    stats = next(r for r in recs if r["kind"] == "viewer_stats")
    assert stats["streams_opened"] == 3 and stats["frames"] >= 1
    rep = k6eval.viewer_report({"records": recs, "truncated": False, "exit": None})
    assert rep["arms"] == len(armed)


def test_view_feed_without_feed_end_is_a_cut_feed(tmp_path, snowplow):
    t = time.time()
    rc, recs, _ = _view_feed(tmp_path, snowplow, [
        {"kind": "feed_start", "t": t},
        {"kind": "nav", "nav": "navigate:0", "user": 3, "route": "/dashboard", "t_end": t,
         "ok": True, "shell": _armed("sider"), "page": []}], stop=True)
    assert rc == 3
    assert recs[-1]["kind"] == "viewer_end" and "the feed is cut" in recs[-1]["ended_by"]


def test_preflight_routes_keeps_only_servable_routes(tmp_path):
    assert load606_job.preflight_routes(str(_preflight(tmp_path))) == _routes()
    f = tmp_path / "empty.jsonl"
    f.write_text("nothing\n")
    with pytest.raises(ValueError, match="no preflight record"):
        load606_job.preflight_routes(str(f))


# ─── pageload.js: the judged instrument's logic, under node ─────────────────

_PAGELOAD_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const out = {
  settled: cases.settled.map((c) => pl.settledNow(c.b, c.here, c.now, c.arg)),
  loads: cases.loads.map((c) => pl.loadOf(c.b, c.here, c.ids)),
  ops: cases.ops.map((c) => pl.recordOps(c.r, c.tags, c.timeout)),
  quiet: pl.QUIET_MS,
  // shipped into the page as source text: must not carry `export`
  sources: [pl.settledNow.toString(), pl.loadOf.toString()],
}
process.stdout.write(JSON.stringify(out))
"""


def _pageload(tmp_path, cases, src=None):
    (tmp_path / "pageload.mjs").write_text(src or (BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_PAGELOAD_DRIVER)
    (tmp_path / "cases.json").write_text(json.dumps(cases))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "cases.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


def _b(**kw):
    return dict({"count": 0, "goneAt": 0, "navT0": 0, "saw": False, "armLogin": False}, **kw)


@needs_node
def test_pageload_settled_needs_the_quiet_window_and_the_route(tmp_path):
    q = {"path": "/dashboard", "quiet": 1000}
    out = _pageload(tmp_path, {"settled": [
        {"b": _b(saw=True, goneAt=400), "here": "/dashboard", "now": 1399, "arg": q},   # 999 ms quiet
        {"b": _b(saw=True, goneAt=400), "here": "/dashboard", "now": 1400, "arg": q},   # 1000 ms
        {"b": _b(count=2, saw=True), "here": "/dashboard", "now": 9000, "arg": q},      # skeletons left
        {"b": _b(), "here": "/other", "now": 9000, "arg": q},                         # wrong route
        {"b": _b(), "here": "/dashboard", "now": 999, "arg": q},     # no skeleton yet: wait the window
        # the sign-in arrival: never while still on /login, or `/`, or before the app left /login
        {"b": _b(armLogin=True), "here": "/dashboard", "now": 9000, "arg": {"path": None, "quiet": 1000}},
        {"b": _b(), "here": "/login", "now": 9000, "arg": {"path": None, "quiet": 1000}},
        {"b": _b(), "here": "/", "now": 9000, "arg": {"path": None, "quiet": 1000}},
        {"b": _b(navT0=500), "here": "/dashboard", "now": 1500, "arg": {"path": None, "quiet": 1000}},
        {"b": None, "here": "/dashboard", "now": 9000, "arg": q},
    ], "loads": [], "ops": []})
    assert out["settled"] == [False, True, False, False, False, False, False, False, True, False]
    assert out["quiet"] == 1000
    assert not any(s.lstrip().startswith("export") for s in out["sources"])


@needs_node
def test_pageload_load_time_and_widget_states(tmp_path):
    out = _pageload(tmp_path, {"settled": [], "ops": [], "loads": [
        {"b": _b(saw=True, navT0=100, goneAt=503), "here": "/err",
         "ids": ["widget-error", "widget-forbidden", "widget-forbidden", "widget-other"]},
        {"b": _b(saw=False, navT0=100, goneAt=0), "here": "/x", "ids": []},     # app cache: 0 ms
    ]})
    assert out["loads"][0] == {"ms": 403, "saw": True, "path": "/err",
                               "states": {"error": 1, "timeout": 0, "forbidden": 2, "notfound": 0}}
    assert out["loads"][1]["ms"] == 0 and out["loads"][1]["saw"] is False


@needs_node
def test_pageload_a_timeout_counts_as_a_load_and_a_timeout(tmp_path):
    tags = {"session": "s1", "route": "/r", "visit": "warm", "method": "click"}
    out = _pageload(tmp_path, {"settled": [], "loads": [], "ops": [
        {"r": None, "tags": tags, "timeout": 60000},
        {"r": {"ms": 403, "saw": True, "path": "/r", "states": {"error": 1, "timeout": 0}},
         "tags": tags, "timeout": 60000},
    ]})
    timeout_ops, ok_ops = out["ops"]
    assert [o[0] for o in timeout_ops] == ["browser_page_timeouts", "browser_page_load_ms",
                                           "browser_page_loads"]
    assert timeout_ops[1][1] == 60000 and timeout_ops[1][2]["timeout"] == "yes"
    assert [(o[0], o[1]) for o in ok_ops] == [("browser_page_load_ms", 403), ("browser_page_loads", 1),
                                              ("browser_widget_states", 1)]
    assert ok_ops[2][2]["state"] == "error" and ok_ops[0][2]["skeleton"] == "yes"


def test_a_slow_sign_in_with_fast_pages_passes(tmp_path):
    """#831 gate (High): 3 sessions, each a 3.4 s sign-in and 20 pages at
    250 ms, recorded as browser.js now records them: the sign-in is
    browser_signin_ms, the first page is timed from leaving /login. The
    zero-cold bars must PASS: authn's login is not a page load."""
    pts = []
    for s in ("s1", "s2", "s3"):
        pts.append(_point("browser_signin_ms", 3400.0, session=s))
        pts.append(_point("browser_page_load_ms", 250.0, session=s, route="/dashboard",
                          visit="first", method="login"))
        for i in range(20):
            pts.append(_point("browser_page_load_ms", 250.0, session=s, route=f"/r{i % 10}",
                              visit="first" if i < 10 else "warm", method="click"))
    _write_out(tmp_path, "browser", pts, None, 0)
    o = k6eval.read_k6_out(str(tmp_path), "browser")
    res = load606.evaluate(k6eval.browser_navs(o), [], [])
    got = {c["check"]: c["result"] for c in res["checks"]
           if c["check"].startswith(("zero_cold", "cold_nav", "warm_nav"))}
    assert got and all(r == "PASS" for r in got.values()), got
    rep = k6eval.browser_report(o)
    assert rep["signin_by_session"]["s1"]["p50_ms"] == 3400.0
    # one flat value too (run 11's report reader found none)
    assert rep["signin_ms"] == {"n": 3, "p50_ms": 3400.0, "p95_ms": 3400.0, "max_ms": 3400.0}


def test_mem606_captures_prewarm_and_learned_identity_counters():
    """Run 12 must show why first visits are cold: the default mem606 prefixes
    (run606 MEM_VARS_PREFIXES = `load606 prefixes`) carry them."""
    p = subprocess.run([sys.executable, "-m", "bench.load606", "prefixes"], capture_output=True,
                       text=True, cwd=BENCH_DIR, check=True)
    got = p.stdout.split()
    for pre in ("snowplow_learned_classes", "snowplow_learned_clientconfig_secrets",
                "snowplow_prewarm_complete", "snowplow_phase1_units",
                "snowplow_phase1_readiness_exit", "snowplow_prewarm_engine"):
        assert pre in got, pre
    assert subprocess.run([sys.executable, "-m", "bench.load606", "check-prefixes", *got],
                          cwd=BENCH_DIR).returncode == 0


# ─── browser.js in real Chrome against a fixture SPA (docker) ──────────────


def needs_docker(test):
    """A test that runs containers. Marked `docker`: on the shared developer
    host, where Docker (like kind) is not to be used, run the suite with
    `-m "not docker"`; CI (bench-tests.yaml) runs them. Docker's availability
    is checked at the test's setup (tests/conftest.py `_require_docker`), so a
    deselected test never calls `docker`, not even `docker info` at import."""
    return pytest.mark.docker(pytest.mark.usefixtures("_require_docker")(test))


def _run_browser_fixture(tmp_path, k6dir=None, signin_ms=3400, skel_ms=150, page_ms=250, fresh=(),
                         duration_s=12):
    """k6/browser.js in the pinned k6 browser image against
    tests/fixtures/k6_browser_fixture.py, on a private docker network."""
    net = f"k6brfix-{os.getpid()}"
    spa = f"{net}-spa"
    out = tmp_path / "out"
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    (tmp_path / "cfg.json").write_text(json.dumps({
        "sessions": [{"name": "s1", "user": "fixture-user", "offset": 0, "fresh": list(fresh)}],
        "routes": ["/dashboard", "/compositions", "/never", "/err"], "password": "x",
        "thinkS": 0.3, "durationS": duration_s, "quietMs": 1000, "pageTimeoutMs": 3000,
        "signinTimeoutMs": 10000, "frontendUrl": f"http://{spa}:8080", "snowplowUrl": f"http://{spa}:8080",
        "firstVisit": "test"}))
    subprocess.run(["docker", "network", "create", net], capture_output=True, check=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", spa, "--network", net,
                        "-v", f"{BENCH_DIR / 'tests' / 'fixtures'}:/f:ro", "python:3.12-slim",
                        "python3", "-u", "/f/k6_browser_fixture.py", "8080", str(signin_ms),
                        str(skel_ms), str(page_ms)], capture_output=True, check=True)
        time.sleep(1)
        p = subprocess.run(
            ["docker", "run", "--rm", "--network", net,
             "-v", f"{k6dir or BENCH_DIR / 'k6'}:/opt/k6:ro", "-v", f"{tmp_path / 'cfg.json'}:/cfg.json:ro",
             "-v", f"{out}:/out", "-e", "BROWSER_CONFIG=/cfg.json",
             "-e", "SUMMARY_FILE=/out/browser-summary.json", "-e", "K6_BROWSER_HEADLESS=true",
             "-e", "K6_BROWSER_ARGS=no-sandbox", k6_job.K6_BROWSER_IMAGE,
             "run", "--quiet", "--out", "json=/out/browser.json.gz", "/opt/k6/browser.js"],
            capture_output=True, text=True, timeout=240)
        assert p.returncode == 0, p.stderr[-3000:]
    finally:
        subprocess.run(["docker", "rm", "-f", spa], capture_output=True)
        subprocess.run(["docker", "network", "rm", net], capture_output=True)
    (out / "browser-exit.json").write_text(json.dumps({"kind": "browser_exit", "rc": 0}))
    return k6eval.read_k6_out(str(out), "browser")


@needs_docker
@pytest.mark.parametrize("image", [k6_job.K6_IMAGE, k6_job.PY_IMAGE])
def test_cpu_sampler_runs_in_the_job_images(tmp_path, image):
    """The wrapper's cgroup sampler (busybox sh in the k6 images, dash in the
    python one) writes a framed, increasing CPU series the evaluator reads."""
    out = tmp_path / "out"
    out.mkdir()
    out.chmod(0o777)
    burn = "i=0; while [ $i -lt 400000 ]; do i=$((i+1)); done; sleep 6"
    script = k6_job._wrapper(burn, "probe", 0, cpu=("2", "2"))
    p = subprocess.run(["docker", "run", "--rm", "--cpus", "2", "--user", "12345",
                        "-v", f"{out}:/out", "--entrypoint", "/bin/sh", image, "-c", script],
                       capture_output=True, text=True, timeout=180)
    assert p.returncode == 0, p.stderr
    recs = k6eval.read_cpu(str(out), "probe")
    # --cpus 2: the kernel's limit, read from cpu.max, is 2000m
    assert recs[0] == {"kind": "cpu_start", "container": "probe", "request_m": 2000,
                       "limit_m": 2000, "cgroup_limit_m": 2000, "interval_s": 5}
    assert recs[-1] == {"kind": "cpu_end"}
    pts = [r for r in recs if r["kind"] == "cpu"]
    assert len(pts) >= 2 and pts[-1]["usage_usec"] > pts[0]["usage_usec"] > 0
    assert all(b["t"] > a["t"] for a, b in itertools.pairwise(pts))
    st = k6eval.cpu_stats(recs)
    assert st["windows"] >= 1 and st["p95_cores"] > 0
    assert (out / "probe-exit.json").exists() and (out / "done").read_text().strip() == "0"


def _browser_pod_in_docker(tmp_path, *, origin_delay_s, wrapper=True, login_tries=6,
                           origin_wait_s=60, origin=True):
    """The k6browser pod, emulated: the fixture SPA (the portal's origin)
    starts `origin_delay_s` late (run 11: Chrome navigated before what it
    loads the portal through answered); the browser container runs the
    RENDERED browser args (with `wrapper`) or a bare `k6 run` (without: run
    11's container). 3 sessions against the fixture SPA at its own origin."""
    net = f"k6race-{os.getpid()}-{origin_delay_s}-{int(wrapper)}-{login_tries}"
    spa = f"{net}-spa"
    out, k6dir = tmp_path / "out", tmp_path / "k6"
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    k6dir.mkdir()
    for n in ("browser.js", "pageload.js"):
        shutil.copy(BENCH_DIR / "k6" / n, k6dir / n)
    sessions = [{"name": f"s{i}", "user": f"fixture-{i}", "offset": i} for i in (1, 2, 3)]
    cfg = k6_job.browser_config(sessions=[dict(s, cold=True) for s in sessions],
                                routes=["/dashboard", "/compositions"], password="x",
                                think_s=0.3, duration_s=6, page_timeout_ms=5000,
                                frontend_url=f"http://{spa}:8080", snowplow_url=f"http://{spa}:8080")
    cfg.update(loginTries=login_tries, loginRetryMs=2000, signinTimeoutMs=10000)
    (k6dir / "browser-config.json").write_text(json.dumps(cfg))
    yaml_text = k6_job.render_browser(config=cfg, linger_s=0, origin_wait_s=origin_wait_s)
    job = next(d for d in yaml.safe_load_all(yaml_text) if d and d["kind"] == "Job")
    browser = job["spec"]["template"]["spec"]["containers"][0]
    script = (browser["args"][0] if wrapper
              else "k6 run --quiet --out json=/out/browser.json.gz /opt/k6/browser.js")
    env = [x for e in browser["env"] for x in ("-e", f"{e['name']}={e['value']}")]
    spa_cmd = (f"sleep {origin_delay_s}; exec python3 -u /f/k6_browser_fixture.py 8080 300 50 100"
               if origin else "sleep 600")
    subprocess.run(["docker", "network", "create", net], capture_output=True, check=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", spa, "--network", net,
                        "-v", f"{BENCH_DIR / 'tests' / 'fixtures'}:/f:ro", "python:3.12-slim",
                        "sh", "-c", spa_cmd], capture_output=True, check=True)
        p = subprocess.run(["docker", "run", "--rm", "--network", net,
                            "-v", f"{k6dir}:/opt/k6:ro", "-v", f"{out}:/out", *env,
                            "--entrypoint", "/bin/sh", k6_job.K6_BROWSER_IMAGE, "-c", script],
                           capture_output=True, text=True, timeout=300)
    finally:
        subprocess.run(["docker", "rm", "-f", spa], capture_output=True)
        subprocess.run(["docker", "network", "rm", net], capture_output=True)
    if not (out / "browser-exit.json").exists():                # the bare k6 run has no wrapper
        (out / "browser-exit.json").write_text(json.dumps({"kind": "browser_exit",
                                                           "rc": p.returncode}))
    return p, out


@needs_docker
def test_run11_race_reproduced_without_the_gate(tmp_path):
    """Run 11, reproduced: Chrome navigates while the portal's origin does
    not answer yet, no wait, no retry -> sessions lost, and browser.sessions
    keeps such a run INVALID."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=6, wrapper=False, login_tries=1)
    assert "ERR_" in p.stdout + p.stderr
    o = k6eval.read_k6_out(str(out), "browser")
    c = _check(k6eval.browser_checks(o, 3), "browser.sessions")
    assert c["result"] == "FAIL" and c["value"] == 0


@needs_docker
def test_origin_late_at_session_start_the_browser_waits(tmp_path):
    """The same race with the rendered browser container: its wrapper waits
    for the portal's origin to answer, then all 3 sessions sign in."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=6)
    assert p.returncode == 0, p.stderr[-2000:]
    fw = json.loads((out / "browser-forward.json").read_text())
    assert fw["ok"] is True and fw["waited_s"] >= 4
    o = k6eval.read_k6_out(str(out), "browser")
    c = _check(k6eval.browser_checks(o, 3), "browser.sessions")
    assert c["result"] == "PASS" and c["value"] == 3
    assert k6eval.browser_report(o)["forward_wait"]["ok"] is True


@needs_docker
def test_origin_late_the_login_retry_alone_recovers(tmp_path):
    """Without the wrapper's wait, browser.js's bounded login retry alone
    carries the sessions over an origin that answers 6 s late."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=6, wrapper=False, login_tries=6)
    assert "retrying in 2000 ms" in p.stdout + p.stderr
    o = k6eval.read_k6_out(str(out), "browser")
    assert _check(k6eval.browser_checks(o, 3), "browser.sessions")["value"] == 3


@needs_docker
def test_origin_never_answers_the_browser_does_not_start(tmp_path):
    """Bounded: no answer within ORIGIN_WAIT_S -> no browser run, rc 3, and
    the run is INVALID with the reason."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=0, origin=False, origin_wait_s=5)
    assert p.returncode == 3 and "did not answer within 5s: not starting the browser" in p.stderr
    fw = json.loads((out / "browser-forward.json").read_text())
    assert fw["kind"] == "forward_wait" and fw["ok"] is False and fw["url"].endswith(":8080/")
    c = k6eval.browser_checks(k6eval.read_k6_out(str(out), "browser"), 3)[0]
    assert c["check"] == "browser.stream_complete" and c["result"] == "FAIL" and "rc=3" in c["why"]


@needs_docker
def test_browser_js_in_chrome_against_the_fixture_spa(tmp_path):
    o = _run_browser_fixture(tmp_path)
    loads = [(t["method"], t["route"], t["timeout"], v) for v, t in o["points"]["browser_page_load_ms"]]
    # the sign-in (3.4 s) is NOT in the first page's time: 150 ms skeleton delay + 250 ms page
    login = [x for x in loads if x[0] == "login"]
    assert len(login) == 1 and login[0][1] == "/dashboard" and 380 <= login[0][3] <= 480, loads
    signin = [v for v, _ in o["points"]["browser_signin_ms"]]
    assert len(signin) == 1 and 3300 <= signin[0] <= 4500, signin
    # a click is timed from the click itself (in the page, not k6's CDP round
    # trip before it), through the skeleton, to its end: 150 + 250 ms (the
    # quiet window waits for a skeleton that appears 150 ms in)
    clicks = [x for x in loads if x[0] == "click" and x[2] == "no"]
    assert clicks and all(380 <= x[3] <= 480 for x in clicks), loads
    # /never times out: a load at its timeout value AND a timeout
    never = [x for x in loads if x[1] == "/never"]
    assert never and all(x[2] == "yes" and x[3] == 3000 for x in never), loads
    assert len(o["points"]["browser_page_timeouts"]) == len(never)
    assert {t["state"] for _, t in o["points"]["browser_widget_states"]} == {"error"}
    sse = {t["stream"]: v for v, t in o["points"]["browser_sse_opened"]}
    assert sse["refreshes"] >= 1 and sse["notifications"] >= 1
    # the stream is whole and the evaluator reads it
    assert k6eval.browser_checks(o, 1)[0]["result"] == "PASS"
    # no request URL (the SPA injects the user into /call extras) and no user
    # name anywhere in the stream
    with gzip.open(tmp_path / "out" / "browser.json.gz", "rt") as fh:
        raw = fh.read()
    assert '"url":' not in raw and '"name":"http' not in raw and "fixture-user" not in raw


@needs_docker
def test_in_chrome_a_refused_call_and_the_open_streams_are_points(tmp_path):
    """k6/browser.js in the pinned k6 browser image, real Chrome, against the
    fixture SPA: its refused /call (port 8099) is a `failed` point with Chrome's
    error, its live streams `never_completed`, its answered /call a
    browser_call_ms point; no point carries the extras. Pinned EXACTLY (#924
    review M4): every other request the page made — the document, the
    answered /call — finished, so k6 2.3.0 fired requestfinished for each,
    requestfailed with Request.failure() for the refused one, and response for
    the two streams."""
    o = _run_browser_fixture(tmp_path)
    inc = sorted((t["outcome"], t["request"], t["call_resource"], t["answered"], t["error"])
                 for _, t in o["points"]["browser_request_incomplete_ms"])
    assert inc == [("failed", "call", "boom", "no", "net::ERR_CONNECTION_REFUSED"),
                   ("never_completed", "eventsource", "", "yes", ""),     # /notifications, open
                   ("never_completed", "fetch", "", "yes", "")], inc       # /refreshes, open
    assert all(v >= 0 for v, _ in o["points"]["browser_request_incomplete_ms"])
    assert not o["points"].get("browser_request_failed_start_unseen")    # every start was seen
    calls = [t for _, t in o["points"]["browser_call_ms"]]
    assert any(t["call_resource"] == "tables" and t["call_status"] == "200" for t in calls), calls
    with gzip.open(tmp_path / "out" / "browser.json.gz", "rt") as fh:
        assert "FIXTURE-SECRET" not in fh.read()


@needs_docker
def test_in_chrome_each_session_switches_to_its_fresh_identity_classes(tmp_path):
    """#935: a session signs in as its initial user, visits every route twice
    (first, then warm), then signs in as its next fresh class in a new browser
    context and visits every route for the first time again. The real read
    path end to end (#939 review LOW): the previous class's K_user token,
    the Bearer header, the two expvar names — the fixture's /debug/vars
    (401 without a token) learns a class the first time its token reads,
    so every fresh class is proven cold."""
    o = _run_browser_fixture(tmp_path, signin_ms=300, fresh=["fixture-fresh-1", "fixture-fresh-2"],
                             duration_s=40)
    loads = [(t["class"], t["identity"], t["route"], t["visit"]) for _, t in o["points"]["browser_page_load_ms"]]
    classes = sorted({c for c, *_ in loads})
    assert classes[:2] == ["0", "1"], loads
    for c in classes[:-1]:                 # every class but the last switched after two visits of each route
        routes = [r for k, _, r, _ in loads if k == c]
        assert all(routes.count(r) >= 2 for r in ("/dashboard", "/compositions", "/never", "/err")), loads
    first1 = sorted(r for k, i, r, v in loads if k == "1" and v == "first")
    assert first1 == ["/compositions", "/dashboard", "/err", "/never"], loads
    assert {i for k, i, *_ in loads if k == "1"} == {"fresh"} and {i for k, i, *_ in loads if k == "0"} == {"initial"}
    fc = [t for _, t in o["points"]["browser_fresh_class"]]
    assert sorted(t["class"] for t in fc) == [c for c in classes if c != "0"]
    assert all(t["registered_before"] and int(t["registered_after"]) == int(t["registered_before"]) + 1
               and t["t_before"] for t in fc), fc
    from bench import figures
    r = figures.fresh_first_visits(o, [], load606._phase_of)
    assert r["cold_proven"] == r["classes"] == len(fc) and r["by_route"]["/dashboard"]["all"]["n"] >= 1, r
    with gzip.open(tmp_path / "out" / "browser.json.gz", "rt") as fh:
        assert "fixture-fresh" not in fh.read()            # a class's user name is never a tag


_K6_STUBS = {
    "http.js": "export default { batch: () => [] }\n",
    "execution.js": "export default { scenario: {} }\n",
    "data.js": "export class SharedArray { constructor (n, f) { return f() } }\n",
    "metrics.js": "export class Counter { add () {} }\nexport class Trend { add () {} }\n",
    "index.js": "export function sleep () {}\n",
}


@needs_node
def test_navigate_js_options_under_node(tmp_path):
    """navigate.js's real options object (k6 modules stubbed): Chrome's 6 per
    host (#831 gate: a 60 survived), the arrival executors and their VUs."""
    pkg = tmp_path / "node_modules" / "k6"
    pkg.mkdir(parents=True)
    (pkg / "package.json").write_text(json.dumps({
        "name": "k6", "type": "module",
        "exports": {("." if n == "index.js" else f"./{n[:-3]}"): f"./{n}" for n in _K6_STUBS}}))
    for n, src in _K6_STUBS.items():
        (pkg / n).write_text(src)
    for n in ("navigate.js", "spa.js"):
        (tmp_path / n.replace(".js", ".mjs")).write_text(
            (BENCH_DIR / "k6" / n).read_text().replace("'./spa.js'", "'./spa.mjs'"))
    cfg = k6_job.k6_config(snowplow_url="u", init=INIT, frontend_ns=NS, builders_ns=NS,
                           routes=_routes(), menu_classes={"devs": _routes()}, class_of=_one_class, users=1000, burst_users=200, think_s=60,
                           duration_s=1800, burst_at_s=900, composite_routes=["/dashboard"],
                           browser_users=[1, 20, 30, 40, 50])
    (tmp_path / "config.json").write_text(json.dumps(cfg))
    (tmp_path / "tokens.json").write_text("[]")
    (tmp_path / "globals.mjs").write_text(
        "import { readFileSync } from 'node:fs'\n"
        "globalThis.__ENV = { K6_CONFIG: 'config.json', TOKENS_FILE: 'tokens.json' }\n"
        "globalThis.open = (p) => readFileSync(p, 'utf8')\n")
    (tmp_path / "opts.mjs").write_text(
        "const m = await import('./navigate.mjs')\nprocess.stdout.write(JSON.stringify(m.options))\n")
    p = subprocess.run([NODE, "--import", "./globals.mjs", "opts.mjs"], capture_output=True,
                       text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    o = json.loads(p.stdout)
    assert o["batchPerHost"] == 6
    nav = o["scenarios"]["navigate"]
    # run 12: one VU per user, one iteration = its whole session
    assert nav["executor"] == "per-vu-iterations" and nav["iterations"] == 1
    assert nav["vus"] == 795 and nav["maxDuration"] == f"{1800 + 300}s"
    cb = o["scenarios"]["composite_burst"]
    assert cb["vus"] == 200 and cb["startTime"] == "900s" and cb["iterations"] == 1
    assert set(o["scenarios"]) == {"navigate", "composite_burst"}
    for sc in ("navigate", "composite_burst"):
        for m in ("arrivals", "arrivals_late", "first_loads", "clicks_deferred"):
            assert f"{m}{{scenario:{sc}}}" in o["thresholds"]
    assert "url" not in o["systemTags"] and "name" not in o["systemTags"]


PROOF = BENCH_DIR / "tests" / "fixtures" / "k6proof-831"


def test_kind_proof_outputs_pass_every_k6_browser_and_viewer_check(tmp_path):
    """The kind proof's collected /out (#831), as the reviewer can inspect it:
    real frontend image 1.8.8 in real Chrome (k6 browser), the real Jobs and
    images; FAKE snowplow / authn / sse-proxy (portal chart 1.8.77 widgets,
    60 ms per /call, a refresh frame every 2 s). 60 users, 10 in the burst,
    think 5 s, 150 s, 3 sessions. Sanitised: user names -> user-NNNN in
    viewer.jsonl (gzipped here); the streams drop the report-only per-request
    metrics (http_req_* but duration/failed, data_*, vus, iterations); no
    token exists in any of it (asserted)."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    shutil.copytree(PROOF / "k6", k6dir)
    shutil.copytree(PROOF / "browser", bdir)
    with gzip.open(k6dir / "viewer.jsonl.gz", "rt") as fi:
        (k6dir / "viewer.jsonl").write_text(fi.read())
    _, v = _evaluate_cli(tmp_path, k6dir, bdir)
    val = [c for c in v["validity"] if c["check"].startswith(("k6.", "browser.", "viewer."))]
    assert len(val) == 17 and all(c["result"] == "PASS" for c in val), val
    # only the fake portal's missing snowplow pod records make it INVALID
    assert v["invalid_reasons"] == [
        "snowplow.restarts_during_measure: restart state unknown (missing before/after pod record)"]
    rep = v["report"]
    assert rep["navs"] == 99 and rep["browser"]["timeouts"] == 0
    assert all(s["p50_ms"] < 1000 for s in rep["browser"]["signin_by_session"].values())
    assert rep["viewer_report_only"]["arms"] == 1592
    assert rep["viewer_report_only"]["push_refetch"]["non_200"] == 0
    # k6 ran under its kernel limit (scaled to 1.5 CPU for the 8-CPU kind node)
    assert rep["load_generator_cpu_report_only"]["k6"]["cgroup_limit_m"] == 1500
    for p in list(PROOF.rglob("*")):
        if p.is_file():
            if p.suffix == ".gz":
                with gzip.open(p, "rt") as fh:
                    raw = fh.read()
            else:
                raw = p.read_text()
            assert "scaleuser-" not in raw and "proof." not in raw and "Bearer" not in raw, p


def test_navigate_js_reads_no_url_tags_and_keeps_tokens_out_of_out():
    src = (BENCH_DIR / "k6" / "navigate.js").read_text()
    assert "systemTags: ['status', 'method', 'scenario', 'expected_response', 'error_code']" in src
    assert "/work/tokens.json" in src and "/out/tokens" not in src
    # the viewer feed (exercised end to end only by the kind proof): the refresh
    # headers k6 received, the first load's shell + page, a click's page
    assert "refresh: refreshOf(r)" in src
    assert "shell: fl.shell.armed, page: fl.content ? fl.content.armed : []" in src
    assert "page: w.armed" in src and "kind: 'feed_end'" in src


# ─── run606.sh with LOAD_DRIVER=k6 (fake gcloud / kubectl) ─────────────────


def _run606_k6(harness, **env):
    e, state, _, tmp = harness
    # PREFLIGHT_TIMEOUT_S: a preflight Job that never answers fails in seconds, not 300
    e = dict(e, LOAD_DRIVER="k6", USERS="20", THINK_S="1", COMPOSITE_BURST_USERS="5",
             BROWSER_SESSIONS="3", PREFLIGHT_TIMEOUT_S="10", PREFLIGHT_POLL_S="0.5", **env)
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
              "k6.composite_burst_complete", "k6.error_rate", "k6.route_table",
              "browser.cpu_headroom",
              "browser.stream_complete", "browser.sessions", "browser.live_refresh[s1]",
              "viewer.stream_complete", "viewer.armed_users"):
        assert val[n] == "PASS", (n, v["validity"])
    assert all(r == "PASS" for c, r in val.items() if c.startswith(("k6.", "browser.", "viewer.")))
    assert v["report"]["viewer_report_only"]["arms"] == 3
    # run 19: the browser loads the portal from the one origin config.json names; no forwarder
    docs = [d for d in yaml.safe_load_all((out / "loadgen-job.yaml").read_text()) if d]
    fwd = next(d for d in docs if d["kind"] == "Job" and d["metadata"]["name"] == "k6browser")
    assert "initContainers" not in fwd["spec"]["template"]["spec"]
    bcm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "k6browser")
    assert json.loads(bcm["data"]["browser-config.json"])["frontendUrl"] == "http://10.0.0.9"
    # each menu class k6 drives got its own preflight (run606 s_classes)
    # each menu class's walk, as its OWN identity (#999 review N2): its Job applied, its own table
    plan = json.loads((out / "menu-classes.json").read_text())["classes"]
    assert [c["class"] for c in plan] == ["bench-platform-admins+devs", "devs"]
    for k, c in enumerate(plan, 1):
        assert f"menu class {c['class']}: preflight as its own identity" in log
        job = next(d for d in yaml.safe_load_all((out / f"class-preflight-{k}-job.yaml").read_text())
                   if d and d["kind"] == "Job")
        assert job["metadata"]["name"] == f"preflight606-c{k}"
        assert f'--user "{c["user"]}"' in job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    applied = (state_dir(harness) / "applied-jobs").read_text().split()
    assert {"preflight606", "preflight606-c1", "preflight606-c2"} <= set(applied)
    cfg = json.loads(next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "k6")
                     ["data"]["config.json"])
    assert set(cfg["menuClasses"]) == {"bench-platform-admins+devs", "devs"}
    # each class's table is ITS Job's walk (the fake gives each Job a route of its own), never the run's
    for k, c in enumerate(plan, 1):
        paths = [r["path"] for r in cfg["menuClasses"][c["class"]]]
        assert paths == ["/dashboard", "/observability", "/compositions", f"/only-c{k}"], (c["class"], paths)
    assert [r["path"] for r in cfg["routes"]] == ["/dashboard", "/observability", "/compositions"]
    # the identities are deleted after their walks (and again at collect)
    dels = [c["argv"] for c in calls if c["tool"] == "kubectl" and "delete" in c["argv"]]
    assert any("bench.krateo.io/menu-class=true" in a for d in dels for a in d)
    assert v["report"]["navs"] == 60                       # the browsers' page loads
    jp = [json.loads(x) for x in (out / "job-pods.jsonl").read_text().splitlines()]
    assert {r["job"] for r in jp} >= {"k6load", "k6browser", "fresh606", "mem606"}
    assert SCALEUSER_PASSWORD not in log
    assert SCALEUSER_PASSWORD not in (out / "run.log").read_text()
    # the fake mem606 stream has no #765 B shadow: the run log says it is missing
    assert "WARNING: snowplow_deps_list_narrow absent in all" in (out / "run.log").read_text()


def test_run606_k6_dropped_burst_is_invalid(harness):
    p, _, out = _run606_k6(harness, FAKE_K6_OUT="dropped")
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    assert any("composite_burst_complete" in r for r in v["invalid_reasons"])
    assert "INVALID: " in (out / "run.log").read_text()


@pytest.mark.parametrize("mode,reason", [
    ("starved", "browser.cpu_headroom"),
    ("viewercut", "the viewers stopped on: k6 exited without feed_end"),
    ("nosse", "no live-update stream opened: /refreshes, /notifications"),
])
def test_run606_k6_viewers_and_live_updates_are_validity(harness, mode, reason):
    p, _, out = _run606_k6(harness, FAKE_K6_OUT=mode)
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    assert any(reason in r for r in v["invalid_reasons"]), v["invalid_reasons"]


def test_run606_k6_without_viewers(harness):
    """VIEWERS=0: no sidecar is rendered, and the verdict asks for no tabs."""
    p, _, out = _run606_k6(harness, VIEWERS="0")
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    k6 = next(d for d in yaml.safe_load_all((out / "loadgen-job.yaml").read_text())
              if d and d["kind"] == "Job" and d["metadata"]["name"] == "k6load")
    assert [c["name"] for c in k6["spec"]["template"]["spec"]["containers"]] == ["k6"]
    v = json.loads((out / "verdict.json").read_text())
    assert not any(c["check"].startswith("viewer.") for c in v["validity"])


def test_run606_k6_missing_browser_output_is_invalid(harness):
    p, _, out = _run606_k6(harness, FAKE_K6_OUT="nobrowser")
    assert p.returncode == 0, (p.stdout + p.stderr)[-4000:]
    assert "could not copy k6browser's results (/out)" in (out / "run.log").read_text()
    v = json.loads((out / "verdict.json").read_text())
    assert v["verdict"] == "INVALID"
    assert any("no browser output was collected" in r for r in v["invalid_reasons"])


def _one_class(i):
    """Every user in one menu class (the tests' portal has one menu)."""
    return "devs"



def state_dir(harness):
    """The fake tools' state dir (the harness's second member)."""
    return harness[1]


def test_run606_k6_a_refused_menu_class_preflight_stops_the_run(harness):
    """#999 review B2 (M25): the admins' class preflight alone refuses: the
    run stops at the classes step, naming the class, before the reboot and
    the load; the run's own preflight passed."""
    p, calls, out = _run606_k6(harness, FAKE_PREFLIGHT_REFUSE_JOB="preflight606-c1")
    log = p.stdout + p.stderr
    assert p.returncode != 0
    assert "preflight OK (the run;" in log
    assert "PREFLIGHT REFUSED: /dashboard -> 500" in log and "(menu class bench-platform-admins+devs)" in log
    assert "=== step reboot ===" not in log and "=== step measure ===" not in log
