"""A page load ends when the page is FINAL (owner's decision, 2026-11-26).

Run 31: every warm load (2,767 of 2,767 in each of three arms) was recorded as exactly 0 ms: a revisit paints from
the portal's React Query cache with no `[data-widget-renderer]` skeleton, and the load's end was the skeleton's.
Yet ~97 % of them made ~12 own /call requests, the last answered p95 ~210-235 ms after the click. A load now ends
at the LAST of: its last skeleton gone, its last own request answered, its last DOM change; and it is settled once
QUIET_MS have passed since that end with no own request still pending (streams never count).

The OBSERVER runs under node here with a fake clock, a fake fetch whose responses the test resolves, and a fake
MutationObserver the test fires: the in-page instrument itself, not b-state fed to loadOf by hand.
"""

from __future__ import annotations

import json
import subprocess

from bench import k6eval
from tests.test_k6_830 import NODE, needs_node
from tests.test_run606_real_sequence import BENCH_DIR

_DRIVER = r"""
import { readFileSync } from 'node:fs'
import * as pl from './pageload.mjs'
const sc = JSON.parse(readFileSync(process.argv[2], 'utf8'))
let clock = 0
let skeletons = 0
let moCallback = null
let poCallback = null
const clickListeners = []
const pending = {}
const body = { contains: (n) => n.inBody !== false }
globalThis.window = {
  location: { href: 'http://10.0.0.9/dashboard', pathname: '/dashboard' },
  fetch: (u) => new Promise((resolve, reject) => { pending[String(u)] = { resolve, reject } }),
  addEventListener () {},
  PerformanceObserver: class { constructor (cb) { poCallback = cb } observe () {} },
}
globalThis.document = { querySelectorAll: () => ({ length: skeletons }), body,
                        addEventListener: (ev, f) => { if (ev === 'click') clickListeners.push(f) } }
globalThis.history = { pushState () {}, replaceState () {} }
globalThis.MutationObserver = class { constructor (cb) { moCallback = cb } observe () {} }
globalThis.performance = { now: () => clock, timeOrigin: 1e12 }
;(0, eval)(pl.OBSERVER)
const flush = async () => { for (let i = 0; i < 5; i++) await Promise.resolve() }
const arg = { path: '/dashboard', quiet: pl.QUIET_MS }
const out = { probes: [] }
for (const s of sc.steps) {
  clock = s.at
  if (s.op === 'click') { window.__bench.armClick = true; for (const f of clickListeners) f() }
  else if (s.op === 'fetch') { window.fetch(s.url) }
  else if (s.op === 'answer') { pending[s.url].resolve({ ok: true, headers: { get: () => (s.type || 'application/json') } }) }
  else if (s.op === 'entry') { poCallback({ getEntries: () => [Object.assign({ initiatorType: 'fetch', duration: 1 }, s.e)] }) }
  else if (s.op === 'skeletons') { skeletons = s.n; moCallback([{ target: {} }]) }
  else if (s.op === 'mutate') { moCallback([{ target: { inBody: s.inBody !== false } }]) }
  else if (s.op === 'probe') {
    out.probes.push({ at: s.at, settled: pl.settledNow(window.__bench, '/dashboard', s.at, arg) })
  }
  await flush()
}
clock = sc.loadAt
out.load = pl.loadOf(window.__bench, '/dashboard', [])
process.stdout.write(JSON.stringify(out))
"""


def run(tmp_path, steps, load_at, src=None):
    (tmp_path / "pageload.mjs").write_text(src or (BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_DRIVER)
    (tmp_path / "sc.json").write_text(json.dumps({"steps": steps, "loadAt": load_at}))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "sc.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    o = json.loads(p.stdout)
    return o["load"], {x["at"]: x["settled"] for x in o["probes"]}


def _warm(extra=()):
    """A revisit: the click at 5000, no skeleton ever, own requests at +10 and +40 ms, answered +120 and +210 ms."""
    return [{"op": "click", "at": 5000},
            {"op": "fetch", "at": 5010, "url": "/call?a"}, {"op": "fetch", "at": 5040, "url": "/call?b"},
            {"op": "answer", "at": 5120, "url": "/call?a"}, {"op": "answer", "at": 5210, "url": "/call?b"}, *extra]


@needs_node
def test_a_warm_load_without_a_skeleton_ends_at_its_last_request_and_settles_after_the_quiet_window(tmp_path):
    load, s = run(tmp_path, _warm([{"op": "probe", "at": 5100}, {"op": "probe", "at": 5209},
                                   {"op": "probe", "at": 6209}, {"op": "probe", "at": 6210}]), 6210)
    assert load["saw"] is False
    assert load["ms"] == 210 and load["ended"] == "request"      # the click at 5000, the last answer at 5210
    # not settled while a request is pending (5100), nor before the quiet window (6209); at 6210 = 210 + 1000
    assert s == {5100: False, 5209: False, 6209: False, 6210: True}


@needs_node
def test_a_pending_request_blocks_settling_however_long_the_page_is_quiet(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?slow"},
             {"op": "probe", "at": 9000}]
    _, s = run(tmp_path, steps, 9000)
    assert s == {9000: False}


@needs_node
def test_a_load_whose_only_pending_request_is_an_sse_stream_settles(tmp_path):
    steps = _warm([{"op": "fetch", "at": 5020, "url": "http://10.0.0.9/content/refreshes"},
                   {"op": "fetch", "at": 5030, "url": "/events/notifications"},
                   {"op": "probe", "at": 6209}, {"op": "probe", "at": 6210}])
    load, s = run(tmp_path, steps, 6210)      # the two streams are never answered
    assert s == {6209: False, 6210: True}
    assert load["ms"] == 210 and load["ended"] == "request"


@needs_node
def test_a_load_with_no_request_and_no_skeleton_ends_at_its_last_mutation(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "mutate", "at": 5300}, {"op": "mutate", "at": 5400},
             {"op": "probe", "at": 6399}, {"op": "probe", "at": 6400}]
    load, s = run(tmp_path, steps, 6400)
    assert load["ms"] == 400 and load["ended"] == "mutation" and load["saw"] is False
    assert s == {6399: False, 6400: True}


@needs_node
def test_a_change_outside_the_app_tree_is_not_the_pages_change(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "mutate", "at": 5300}, {"op": "mutate", "at": 5900, "inBody": False}]
    load, _ = run(tmp_path, steps, 6400)      # a style library writing into <head> at 5900
    assert load["ms"] == 300 and load["ended"] == "mutation"


@needs_node
def test_a_load_that_changes_nothing_and_asks_for_nothing_ends_at_the_click(tmp_path):
    load, s = run(tmp_path, [{"op": "click", "at": 5000}, {"op": "probe", "at": 5999}, {"op": "probe", "at": 6000}], 6000)
    assert load["ms"] == 0 and load["ended"] == "start" and s == {5999: False, 6000: True}


@needs_node
def test_a_first_load_whose_skeleton_is_gone_before_a_late_request_ends_at_the_request(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "skeletons", "at": 5001, "n": 3},
             {"op": "fetch", "at": 5100, "url": "/call?x"},
             {"op": "skeletons", "at": 5300, "n": 0},                  # the skeleton is gone at 5300 ...
             {"op": "answer", "at": 5800, "url": "/call?x"},           # ... a late request answers at 5800
             {"op": "probe", "at": 6799}, {"op": "probe", "at": 6800}]
    load, s = run(tmp_path, steps, 6800)
    assert load["saw"] is True and load["ms"] == 800 and load["ended"] == "request"
    assert s == {6799: False, 6800: True}


@needs_node
def test_a_first_load_ending_at_its_skeleton_says_so(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "skeletons", "at": 5001, "n": 3},
             {"op": "fetch", "at": 5100, "url": "/call?x"}, {"op": "answer", "at": 5200, "url": "/call?x"},
             {"op": "skeletons", "at": 5700, "n": 0}]
    load, _ = run(tmp_path, steps, 6700)
    assert load["ms"] == 700 and load["ended"] == "skeleton"


@needs_node
def test_a_request_started_before_the_click_is_not_the_loads_own(tmp_path):
    steps = [{"op": "fetch", "at": 4000, "url": "/call?old"}, {"op": "click", "at": 5000},
             {"op": "answer", "at": 5600, "url": "/call?old"}, {"op": "probe", "at": 6000}]
    load, s = run(tmp_path, steps, 6000)      # the previous page's refresh: neither pending nor an end for this load
    assert load["ms"] == 0 and s == {6000: True}


@needs_node
def test_a_requests_last_byte_is_its_end_when_it_comes_after_the_headers(tmp_path):
    """The fetch promise settles at the response headers; its Resource Timing entry carries the last byte. A stream's
    entry (it ends when the stream closes) and a script's are not data requests of the load."""
    steps = [{"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5100, "url": "/call?a"},
             {"op": "entry", "at": 5400, "e": {"name": "/call?a", "startTime": 5010, "responseEnd": 5400}},
             {"op": "entry", "at": 5401, "e": {"name": "/content/refreshes", "startTime": 5020, "responseEnd": 9000}},
             {"op": "entry", "at": 5402, "e": {"name": "/main.js", "startTime": 5030, "responseEnd": 9500,
                                               "initiatorType": "script"}},
             {"op": "entry", "at": 5403, "e": {"name": "/call?old", "startTime": 4000, "responseEnd": 9600}}]
    load, _ = run(tmp_path, steps, 6500)
    assert load["ms"] == 400 and load["ended"] == "request"


@needs_node
def test_a_request_of_the_previous_page_still_pending_does_not_hold_the_next_load(tmp_path):
    steps = [{"op": "fetch", "at": 4000, "url": "/call?old"}, {"op": "click", "at": 5000}, {"op": "probe", "at": 6000}]
    _, s = run(tmp_path, steps, 6000)         # never answered: not started since this load's click
    assert s == {6000: True}


# ─── browser.warm_loads_final: the blind spot that hid the defect ──────────────────────────────────

def _stream(loads):
    """loads: [(load_id, wall_ms, [last-request-end offsets (from_load_ms + response_end)...], visit)]"""
    pts = {"browser_page_load_ms": [], "browser_call_ms": []}
    md = {"browser_page_load_ms": [], "browser_call_ms": []}
    for lid, wall, ends, visit in loads:
        pts["browser_page_load_ms"].append((wall, {"visit": visit, "timeout": "no", "session": "s1"}))
        md["browser_page_load_ms"].append({"load_id": lid, "vu": "1"})
        for e in ends:
            pts["browser_call_ms"].append((e, {"during": "load", "visit": visit, "session": "s1"}))
            md["browser_call_ms"].append({"call_timing": json.dumps(
                {"from_load_ms": 10.0, "response_end": e - 10.0, "load_id": lid})})
    return {"points": pts, "metadata": md, "times": {}}


def test_run_31s_shape_every_warm_load_at_0_ms_fails_the_check():
    c = k6eval.warm_final_check(_stream([(f"1:{i}", 0.0, [180.0, 210.0], "warm") for i in range(5)]))
    assert c["result"] == "FAIL" and "0 ms" in c["why"]
    assert c["value"]["warm_loads_at_0_ms"] == 5


def test_all_warm_loads_at_0_ms_fail_even_when_no_request_was_recorded():
    """zero is not detection: with no call to contradict them, all-zero warm loads are still the blind spot"""
    c = k6eval.warm_final_check(_stream([(f"1:{i}", 0.0, [], "warm") for i in range(5)]))
    assert c["result"] == "FAIL" and "0 ms" in c["why"] and c["value"]["ended_before_last_request"] == 0


def test_a_warm_load_reporting_a_time_before_its_last_request_answered_fails():
    c = k6eval.warm_final_check(_stream([("1:1", 300.0, [210.0], "warm"), ("1:2", 150.0, [210.0], "warm")]))
    assert c["result"] == "FAIL" and c["value"]["ended_before_last_request"] == 1 and "1:2" in c["why"]


def test_warm_loads_ending_at_or_after_their_last_request_pass():
    c = k6eval.warm_final_check(_stream([("1:1", 210.0, [210.0], "warm"), ("1:2", 400.0, [210.0, 380.0], "warm"),
                                         ("1:3", 0.0, [], "warm"), ("1:4", 0.0, [900.0], "first")]))
    assert c["result"] == "PASS", c           # a first load's time is not this check's; a no-request 0 is fine
    assert c["value"]["warm_loads_with_requests"] == 2


def test_no_warm_load_is_no_check_here_driver_warm_navs_owns_that():
    assert k6eval.warm_final_check(_stream([("1:1", 900.0, [800.0], "first")])) is None
    assert k6eval.warm_final_check(None) is None


@needs_node
def test_the_load_points_carry_what_ended_the_load(tmp_path):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "d.mjs").write_text(
        "import * as pl from './pageload.mjs'\n"
        "const r = { ms: 210, saw: false, ended: 'request', states: {}, t0: 1e12 }\n"
        "process.stdout.write(JSON.stringify(pl.recordOps(r, {route: '/x'}, 60000, 0)))\n")
    p = subprocess.run([NODE, str(tmp_path / "d.mjs")], capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    ops = json.loads(p.stdout)
    assert ops[0][0] == "browser_page_load_ms" and ops[0][1] == 210
    assert ops[0][2]["skeleton"] == "no" and ops[0][2]["ended_by"] == "request"


def test_browser_checks_carries_the_warm_final_check():
    o = _stream([(f"1:{i}", 0.0, [180.0, 210.0], "warm") for i in range(3)])
    o.update({"stream": True, "frames": {"start": 1, "end": 1}, "truncated": False, "summary": {}, "exit": {"rc": 0}})
    o["points"].update({"browser_logins": [], "browser_page_loads": [], "browser_sse_opened": []})
    c = [x for x in k6eval.browser_checks(o, 5) if x["check"] == "browser.warm_loads_final"]
    assert len(c) == 1 and c[0]["result"] == "FAIL"
