"""#839 gate: navigate.js's lag instruments, made to FIRE. The real session
code runs under node with k6 stubbed: a fake clock (Date.now), and a k6
`sleep` that can oversleep, as a starved k6 does.

- An arrival is late against its wall-clock slot, whether the VU overslept
  or started after the slot (a lag measured from the VU's own start would
  read 0).
- A route click is late against its intended time (previous page end + think
  time); without it a starved k6 just clicks later and the offered load
  drops unseen — run 10's hidden lag, moved into the clicks.
- The burst's slot is the run's start + BURST_AT_S, not the burst
  scenario's own (possibly late) start."""

from __future__ import annotations

import json
import subprocess

from bench import k6_job
from tests.test_k6_830 import INIT, NODE, NS, needs_node
from tests.test_run606_real_sequence import BENCH_DIR

ROUTES = [{"path": "/a", "endpoint": "/route?p=/a"}, {"path": "/b", "endpoint": "/route?p=/b"}]
T0 = 1_800_000_000_000                 # setup's end (epoch ms)
THINK_MS, FIRST_MS, CLICK_MS = 2000, 3000, 200

STUBS = {
    "index.js": "export function sleep (s) {\n"
                "  const extra = globalThis.__oversleep.length ? globalThis.__oversleep.shift() : 0\n"
                "  globalThis.__clock += s * 1000 + extra\n}\n",
    "execution.js": "export default globalThis.__exec\n",
    "http.js": "export default { batch: () => [] }\n",
    "data.js": "export class SharedArray { constructor (n, f) { return f() } }\n",
    "metrics.js": "function rec (name, v) { (globalThis.__M[name] = globalThis.__M[name] || []).push(v) }\n"
                  "export class Counter { constructor (n) { this.n = n } add (v) { rec(this.n, v) } }\n"
                  "export class Trend { constructor (n) { this.n = n } add (v) { rec(this.n, v) } }\n",
}
SPA = """
export function firstLoad (portal, fetch, deepLink) {
  globalThis.__clock += globalThis.__firstMs
  return { shell: { calls: 1, errors: 0, armed: [] }, content: null,
           target: { path: deepLink || '/a' }, routes: globalThis.__routes, ok: true }
}
export function firstLoadFailed (fl) { return false }
export function sameMenu (routes, table) { return true }
export function userRoutes (fl, failed, table) { return failed ? table : fl.routes }
export function pickRoute (routes, user, visit) { return routes[visit %% routes.length] }
export function walkTree (endpoints, fetch) {
  globalThis.__clock += %d
  return { calls: 1, errors: 0, armed: [] }
}
""" % CLICK_MS


def _session(tmp_path, *, fn="navigate", i=0, clock0=T0, oversleep=(), scenario_start=None):
    """Run navigate.js's `fn` for VU `i` from `clock0`; returns its metrics."""
    pkg = tmp_path / "node_modules" / "k6"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "package.json").write_text(json.dumps({
        "name": "k6", "type": "module",
        "exports": {("." if n == "index.js" else f"./{n[:-3]}"): f"./{n}" for n in STUBS}}))
    for n, src in STUBS.items():
        (pkg / n).write_text(src)
    (tmp_path / "navigate.mjs").write_text(
        (BENCH_DIR / "k6" / "navigate.js").read_text().replace("'./spa.js'", "'./spa.mjs'"))
    (tmp_path / "spa.mjs").write_text(SPA)
    cfg = k6_job.k6_config(snowplow_url="http://s", init=INIT, frontend_ns=NS, builders_ns=NS,
                           routes=ROUTES, menu_classes={"devs": ROUTES}, class_of=lambda i: "devs", users=6, burst_users=2, think_s=THINK_MS / 1000,
                           duration_s=20, burst_at_s=8, composite_routes=["/b"], viewers=False)
    (tmp_path / "config.json").write_text(json.dumps(cfg))
    (tmp_path / "tokens.json").write_text("[]")
    name = "composite_burst" if fn == "compositeBurst" else "navigate"
    (tmp_path / "globals.mjs").write_text(
        "import { readFileSync } from 'node:fs'\n"
        "globalThis.__ENV = { K6_CONFIG: 'config.json', TOKENS_FILE: 'tokens.json' }\n"
        "globalThis.open = (p) => readFileSync(p, 'utf8')\n"
        f"globalThis.__clock = {clock0}\n"
        "Date.now = () => globalThis.__clock\n"
        f"globalThis.__oversleep = {json.dumps(list(oversleep))}\n"
        f"globalThis.__firstMs = {FIRST_MS}\n"
        f"globalThis.__routes = {json.dumps(ROUTES)}\n"
        "globalThis.__M = {}\n"
        f"globalThis.__exec = {{ scenario: {{ name: '{name}', iterationInTest: {i}, "
        f"startTime: {scenario_start or clock0} }} }}\n")
    (tmp_path / "run.mjs").write_text(
        "const m = await import('./navigate.mjs')\n"
        f"m.{fn}({{ t0: {T0} }})\n"
        "process.stdout.write(JSON.stringify(globalThis.__M))\n")
    p = subprocess.run([NODE, "--import", "./globals.mjs", "run.mjs"], capture_output=True,
                       text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout), cfg


def _sum(m, k):
    return sum(m.get(k, []))


@needs_node
def test_on_time_nothing_is_late(tmp_path):
    m, cfg = _session(tmp_path, i=1)                  # slot T0 + 500 ms, the VU waits for it
    assert _sum(m, "arrivals") == 1 and _sum(m, "arrivals_late") == 0
    assert m["arrival_lag_ms"] == [0]
    # first load ends at slot + 3 s, then a click every 2.2 s until T0 + 20 s
    assert _sum(m, "clicks") == 7 and _sum(m, "clicks_late") == 0
    assert set(m["click_lag_ms"]) == {0}
    assert _sum(m, "clicks_open_schedule") == 9      # one per 2 s from the slot
    assert _sum(m, "clicks_deferred") == 1           # slot + 2 s fell inside the 3 s first load


@needs_node
def test_a_vu_that_oversleeps_its_arrival_is_late(tmp_path):
    m, _ = _session(tmp_path, i=1, oversleep=[1500])
    assert _sum(m, "arrivals_late") == 1 and m["arrival_lag_ms"] == [1500]


@needs_node
def test_a_vu_that_starts_after_its_slot_is_late(tmp_path):
    """Measured from the VU's own start this would read 0."""
    m, _ = _session(tmp_path, i=1, clock0=T0 + 500 + 1500)
    assert _sum(m, "arrivals_late") == 1 and m["arrival_lag_ms"] == [1500]


@needs_node
def test_clicks_3_s_late_are_late(tmp_path):
    m, _ = _session(tmp_path, i=0, oversleep=[3000] * 20)   # slot = T0: every sleep is a click's
    assert _sum(m, "clicks") > 0 and _sum(m, "clicks_late") == _sum(m, "clicks")
    assert min(m["click_lag_ms"]) == 3000
    assert _sum(m, "arrivals_late") == 0


@needs_node
def test_a_late_burst_scenario_is_late_against_the_runs_clock(tmp_path):
    """k6 started the burst scenario 1.5 s late: its own start is no anchor."""
    late = T0 + 8000 + 1500
    m, _ = _session(tmp_path, fn="compositeBurst", clock0=late, scenario_start=late)
    assert _sum(m, "arrivals_late") == 1 and m["arrival_lag_ms"] == [1500]
