"""A page load ends when the page is FINAL (owner's decision, 2026-11-26), and a live update does not extend it
(owner's ruling, 2026-11-28).

Run 31: every warm load (2,767 of 2,767 in each of three arms) was recorded as exactly 0 ms: a revisit paints from
the portal's React Query cache with no `[data-widget-renderer]` skeleton, and the load's end was the skeleton's.
Yet ~97 % of them made ~12 own /call requests, the last answered p95 ~210-235 ms after the click. A load now ends
at the LAST of: its last skeleton gone, its last own /call answered (body read to the end), its last counted DOM
change; and it is settled once QUIET_MS have passed since that end with no own request still pending. Streams never
count, nor does anything that is not a /call (the OTel exporter's POST), nor the re-fetch a `/refreshes` frame
causes: a /call whose answer carries the X-Snowplow-Refresh-Key of a refresh frame that reached the page since the
click, before the request started.

The OBSERVER runs under node here with a fake clock, a fake fetch whose answers and bodies the test releases, a fake
MutationObserver and PerformanceObserver the test fires: the in-page instrument itself, not b-state fed to loadOf.
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
const chan = {}
const enc = new TextEncoder()
const channel = (url) => (chan[url] = chan[url] || { queue: [], waiters: [], closed: false })
const reader = (url) => ({
  read: () => {
    const c = channel(url)
    if (c.queue.length) return Promise.resolve({ done: false, value: enc.encode(c.queue.shift()) })
    if (c.closed) return Promise.resolve({ done: true })
    return new Promise((res) => c.waiters.push(res))
  },
})
const push = (url, text) => {
  const c = channel(url)
  if (c.waiters.length) c.waiters.shift()({ done: false, value: enc.encode(text) }); else c.queue.push(text)
}
const close = (url) => { const c = channel(url); c.closed = true; while (c.waiters.length) c.waiters.shift()({ done: true }) }
const esInst = []
class FakeES {
  constructor (url) { this.url = url; this.l = {}; esInst.push(this) }
  addEventListener (ev, f) { (this.l[ev] = this.l[ev] || []).push(f) }
}
const body = { contains: (n) => n.inBody !== false }
globalThis.window = {
  location: { href: 'http://10.0.0.9/dashboard', pathname: '/dashboard' },
  fetch: (u) => new Promise((resolve, reject) => { pending[String(u)] = { resolve, reject } }),
  addEventListener () {}, EventSource: FakeES,
  PerformanceObserver: class { constructor (cb) { poCallback = cb } observe () {} },
}
globalThis.document = { querySelectorAll: () => ({ length: skeletons }), body,
                        addEventListener: (ev, f) => { if (ev === 'click') clickListeners.push(f) } }
globalThis.history = { pushState () {}, replaceState () {} }
globalThis.MutationObserver = class { constructor (cb) { moCallback = cb } observe () {} }
globalThis.performance = { now: () => clock, timeOrigin: 1e12 }
;(0, eval)(pl.OBSERVER)
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve() }
const arg = { path: '/dashboard', quiet: pl.QUIET_MS }
const out = { probes: [] }
const resp = (url, type, key) => ({
  ok: true,
  headers: { get: (h) => { const k = h.toLowerCase(); return k === 'content-type' ? type : (k === 'x-snowplow-refresh-key' ? (key || null) : null) } },
  clone: () => ({ body: { getReader: () => reader(url) } }),
})
for (const s of sc.steps) {
  clock = s.at
  if (s.op === 'click') { window.__bench.armClick = true; for (const f of clickListeners) f() }
  else if (s.op === 'fetch') { window.fetch(s.url, s.init) }
  else if (s.op === 'answer') {                       // the headers; the body too unless `end` is false
    pending[s.url].resolve(resp(s.url, s.type || 'application/json', s.key))
    if (s.end !== false) close(s.url)
  }
  else if (s.op === 'end') { close(s.url) }
  else if (s.op === 'es') { new window.EventSource(s.url) }
  else if (s.op === 'esframe') { for (const e of esInst) for (const f of (e.l.refresh || [])) f({ data: s.key }) }
  else if (s.op === 'reject') { pending[s.url].reject(new Error('network')) }
  else if (s.op === 'frame') { push(s.url, s.text) }
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
out.calls = window.__benchCalls
process.stdout.write(JSON.stringify(out))
"""


def run_full(tmp_path, steps, load_at, src=None):
    (tmp_path / "pageload.mjs").write_text(src or (BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "drv.mjs").write_text(_DRIVER)
    (tmp_path / "sc.json").write_text(json.dumps({"steps": steps, "loadAt": load_at}))
    p = subprocess.run([NODE, str(tmp_path / "drv.mjs"), str(tmp_path / "sc.json")],
                       capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    o = json.loads(p.stdout)
    return o, {x["at"]: x["settled"] for x in o["probes"]}


def run(tmp_path, steps, load_at, src=None):
    o, probes = run_full(tmp_path, steps, load_at, src)
    return o["load"], probes


def _warm(extra=()):
    """A revisit: the click at 5000, no skeleton ever, own requests at +10 and +40 ms, answered +120 and +210 ms."""
    return [{"op": "click", "at": 5000},
            {"op": "fetch", "at": 5010, "url": "/call?a"}, {"op": "fetch", "at": 5040, "url": "/call?b"},
            {"op": "answer", "at": 5120, "url": "/call?a"}, {"op": "answer", "at": 5210, "url": "/call?b"}, *extra]


def _stream_open(url="http://10.0.0.9/content/refreshes", at=4000):
    """The page's /refreshes stream, opened before the click and held open."""
    return [{"op": "fetch", "at": at, "url": url},
            {"op": "answer", "at": at + 1, "url": url, "type": "text/event-stream", "end": False}]


def _frame(key, at, url="http://10.0.0.9/content/refreshes"):
    return {"op": "frame", "at": at, "url": url, "text": f"event: refresh\ndata: {key}\n\n"}


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
def test_a_requests_end_is_its_body_read_to_the_end_not_its_headers(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5100, "url": "/call?a", "end": False}, {"op": "probe", "at": 6500},
             {"op": "end", "at": 5400, "url": "/call?a"}, {"op": "probe", "at": 6399}, {"op": "probe", "at": 6400}]
    load, s = run(tmp_path, steps, 6400)
    assert load["ms"] == 400 and load["ended"] == "request"
    assert s == {6500: False, 6399: False, 6400: True}      # the 6500 probe ran with the body still open


@needs_node
def test_a_rejected_fetch_never_stays_pending(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?down"},
             {"op": "reject", "at": 5100, "url": "/call?down"}, {"op": "probe", "at": 6099}, {"op": "probe", "at": 6100}]
    load, s = run(tmp_path, steps, 6100)
    assert s == {6099: False, 6100: True}
    assert load["ms"] == 100 and load["ended"] == "request"       # a failed request has ended


@needs_node
def test_a_load_whose_only_pending_request_is_an_sse_stream_settles(tmp_path):
    steps = _warm([{"op": "fetch", "at": 5020, "url": "http://10.0.0.9/content/refreshes"},
                   {"op": "fetch", "at": 5030, "url": "/events/notifications"},
                   {"op": "probe", "at": 6209}, {"op": "probe", "at": 6210}])
    load, s = run(tmp_path, steps, 6210)      # the two streams are never answered
    assert s == {6209: False, 6210: True}
    assert load["ms"] == 210 and load["ended"] == "request"


@needs_node
def test_a_post_to_something_that_is_not_snowplow_is_not_an_own_request(tmp_path):
    """The portal's OTel exporter POSTs its span batch (default every 5 s) through fetch: neither pending nor an end."""
    otel = "http://collector.example:4318/v1/traces"
    steps = _warm([{"op": "fetch", "at": 5100, "url": otel, "init": {"method": "POST"}},     # never answered
                   {"op": "fetch", "at": 5150, "url": "/v1/traces/call-not"},               # not a /call either
                   {"op": "probe", "at": 6209}, {"op": "probe", "at": 6210}])
    load, s = run(tmp_path, steps, 6210)
    assert s == {6209: False, 6210: True} and load["ms"] == 210
    # answered late, inside the quiet window: still not an end
    steps = _warm([{"op": "fetch", "at": 5100, "url": otel}, {"op": "answer", "at": 5900, "url": otel},
                   {"op": "probe", "at": 6210}])
    load, s = run(tmp_path, steps, 6210)
    assert load["ms"] == 210 and s == {6210: True}


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
    assert load["final"]["skeletonMs"] == 300 and load["final"]["requestMs"] == 800


@needs_node
def test_a_first_load_ending_at_its_skeleton_says_so(tmp_path):
    steps = [{"op": "click", "at": 5000}, {"op": "skeletons", "at": 5001, "n": 3},
             {"op": "fetch", "at": 5100, "url": "/call?x"}, {"op": "answer", "at": 5200, "url": "/call?x"},
             {"op": "skeletons", "at": 5700, "n": 0}]
    load, _ = run(tmp_path, steps, 6700)
    assert load["ms"] == 700 and load["ended"] == "skeleton"


@needs_node
def test_the_skeletons_end_is_when_the_last_one_went_not_the_last_tick_after_it(tmp_path):
    """goneAt moves only when the count falls to 0; later changes (here one outside the tree, not counted) leave it."""
    steps = [{"op": "click", "at": 5000}, {"op": "skeletons", "at": 5001, "n": 3}, {"op": "skeletons", "at": 5300, "n": 0},
             {"op": "mutate", "at": 5900, "inBody": False}]
    load, _ = run(tmp_path, steps, 7000)
    assert load["ms"] == 300 and load["ended"] == "skeleton" and load["final"]["skeletonMs"] == 300


@needs_node
def test_a_request_started_before_the_click_is_not_the_loads_own(tmp_path):
    steps = [{"op": "fetch", "at": 4000, "url": "/call?old"}, {"op": "click", "at": 5000},
             {"op": "answer", "at": 5600, "url": "/call?old"}, {"op": "probe", "at": 6000}]
    load, s = run(tmp_path, steps, 6000)      # the previous page's refresh: neither pending nor an end for this load
    assert load["ms"] == 0 and s == {6000: True}


@needs_node
def test_a_request_of_the_previous_page_still_pending_does_not_hold_the_next_load(tmp_path):
    steps = [{"op": "fetch", "at": 4000, "url": "/call?old"}, {"op": "click", "at": 5000}, {"op": "probe", "at": 6000}]
    _, s = run(tmp_path, steps, 6000)         # never answered: not started since this load's click
    assert s == {6000: True}


# ─── a live update does not extend a load (2026-11-28) ─────────────────────────────────────────────

@needs_node
def test_a_live_update_refetch_during_a_load_does_not_extend_it(tmp_path):
    """Own /call a (key K) answered at 5210. A refresh frame for K reaches the page at 5300; the portal re-fetches
    /call?live, answered (K again) at 5700. That re-fetch is the live update's: the load still ends at 5210."""
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"},
             _frame("K", 5300), {"op": "fetch", "at": 5310, "url": "/call?live"},
             {"op": "answer", "at": 5700, "url": "/call?live", "key": "K"},
             {"op": "mutate", "at": 5720},                         # the render the live answer causes
             {"op": "probe", "at": 6209}, {"op": "probe", "at": 6210}]
    load, s = run(tmp_path, steps, 6210)
    assert load["ms"] == 210 and load["ended"] == "request"
    assert s == {6209: False, 6210: True}
    assert load["final"]["liveRefetches"] == 1 and load["final"]["liveFrames"] == 1


@needs_node
def test_the_own_request_that_carries_the_same_key_but_started_before_the_frame_stays_own(tmp_path):
    """The key alone does not make a request live: the frame must have reached the page BEFORE the request started."""
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             _frame("K", 5100), {"op": "answer", "at": 5400, "url": "/call?a", "key": "K"}]
    load, _ = run(tmp_path, steps, 6400)
    assert load["ms"] == 400 and load["ended"] == "request" and load["final"]["liveRefetches"] == 0


@needs_node
def test_a_frame_for_another_key_makes_no_request_live(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, _frame("OTHER", 5050),
             {"op": "fetch", "at": 5100, "url": "/call?a"}, {"op": "answer", "at": 5300, "url": "/call?a", "key": "K"}]
    load, _ = run(tmp_path, steps, 6300)
    assert load["ms"] == 300 and load["final"]["liveRefetches"] == 0 and load["final"]["liveFrames"] == 1


@needs_node
def test_a_frame_from_before_the_click_does_not_make_this_loads_request_live(tmp_path):
    steps = [*_stream_open(), _frame("K", 4500), {"op": "click", "at": 5000},
             {"op": "fetch", "at": 5100, "url": "/call?a"}, {"op": "answer", "at": 5300, "url": "/call?a", "key": "K"}]
    load, _ = run(tmp_path, steps, 6300)
    assert load["ms"] == 300 and load["final"]["liveRefetches"] == 0 and load["final"]["liveFrames"] == 0


@needs_node
def test_a_pending_live_refetch_holds_the_settling_only_until_its_answer_names_it(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"}, _frame("K", 5300),
             {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "probe", "at": 6300},     # in flight: unknown yet
             {"op": "answer", "at": 6400, "url": "/call?live", "key": "K"}, {"op": "probe", "at": 6401}]
    load, s = run(tmp_path, steps, 6401)
    assert s == {6300: False, 6401: True} and load["ms"] == 210


@needs_node
def test_a_change_after_a_live_answer_is_the_live_updates_a_change_while_own_work_is_pending_is_not(tmp_path):
    base = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
            {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"}, _frame("K", 5300),
            {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "answer", "at": 5400, "url": "/call?live", "key": "K"}]
    load, _ = run(tmp_path, [*base, {"op": "mutate", "at": 5420}], 7000)
    assert load["ms"] == 210                                       # the live render is not the load's change
    # a new own request starts after the live answer: the changes while it is pending count again
    load, _ = run(tmp_path, [*base, {"op": "fetch", "at": 5500, "url": "/call?next"}, {"op": "mutate", "at": 5600},
                             {"op": "answer", "at": 5700, "url": "/call?next"}], 7000)
    assert load["ms"] == 700 and load["ended"] == "request"
    load, _ = run(tmp_path, [*base, {"op": "fetch", "at": 5500, "url": "/call?next"}, {"op": "mutate", "at": 5600},
                             {"op": "answer", "at": 5650, "url": "/call?next"}, {"op": "mutate", "at": 5660}], 7000)
    assert load["ms"] == 660 and load["ended"] == "mutation"       # own answered last: its draw counts again
    # a request of the PREVIOUS page still in flight is not own work pending: the live render still does not count
    load, _ = run(tmp_path, [{"op": "fetch", "at": 4500, "url": "/call?old"}, *base, {"op": "mutate", "at": 5420}], 7000)
    assert load["ms"] == 210


@needs_node
def test_a_load_a_live_update_reached_says_so(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"}, _frame("K", 5300), _frame("K2", 5301),
             {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "answer", "at": 5400, "url": "/call?live", "key": "K"}]
    load, _ = run(tmp_path, steps, 6400)
    assert load["final"] == {"skeletonMs": None, "requestMs": 210, "mutationMs": None, "liveRefetches": 1, "liveFrames": 2}
    ops = _record_ops(tmp_path, load, {"route": "/x", "visit": "warm"})
    assert ops[0][2]["live_update"] == "yes" and ops[0][2]["ended_by"] == "request"
    quiet, _ = run(tmp_path, _warm(), 6300)
    assert _record_ops(tmp_path, quiet, {"route": "/x", "visit": "warm"})[0][2]["live_update"] == "no"
    fin = json.loads(_node_json(tmp_path, "pl.loadFinalOf(JSON.parse(process.argv[2]))", json.dumps(load)))
    assert fin == {"skeleton_ms": None, "request_ms": 210, "mutation_ms": None, "live_refetches": 1, "live_frames": 2}


@needs_node
def test_a_live_refetchs_resource_timing_entry_is_flagged_live_and_an_own_ones_is_not(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"}, _frame("K", 5300),
             {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "answer", "at": 5400, "url": "/call?live", "key": "K"},
             {"op": "entry", "at": 5405, "e": {"name": "http://10.0.0.9/call?live", "startTime": 5311, "responseEnd": 5400}},
             {"op": "entry", "at": 5406, "e": {"name": "http://10.0.0.9/call?a", "startTime": 5011, "responseEnd": 5210}}]
    o, _ = run_full(tmp_path, steps, 6400)
    assert [(c["live"]) for c in o["calls"]] == [True, False]


@needs_node
def test_a_refresh_frame_split_over_two_reads_is_one_frame(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"},
             {"op": "frame", "at": 5300, "url": "http://10.0.0.9/content/refreshes", "text": "event: refresh\ndata: "},
             {"op": "frame", "at": 5301, "url": "http://10.0.0.9/content/refreshes", "text": "K\n\n"},
             {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "answer", "at": 5700, "url": "/call?live", "key": "K"}]
    load, _ = run(tmp_path, steps, 6700)
    assert load["ms"] == 210 and load["final"]["liveRefetches"] == 1


@needs_node
def test_an_older_spas_refresh_event_source_names_a_live_key_too(tmp_path):
    steps = [{"op": "es", "at": 4000, "url": "http://10.0.0.9/content/refreshes"}, {"op": "click", "at": 5000},
             {"op": "fetch", "at": 5010, "url": "/call?a"}, {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"},
             {"op": "esframe", "at": 5300, "key": "K"}, {"op": "fetch", "at": 5310, "url": "/call?live"},
             {"op": "answer", "at": 5700, "url": "/call?live", "key": "K"}]
    load, _ = run(tmp_path, steps, 6700)
    assert load["ms"] == 210 and load["final"]["liveRefetches"] == 1 and load["final"]["liveFrames"] == 1


@needs_node
def test_a_live_calls_timing_metadata_says_so(tmp_path):
    c = '{fetch: 1, connectStart: 1, connectEnd: 1, requestStart: 2, responseStart: 3, responseEnd: 9, start: 5, live: %s}'
    got = [json.loads(_node_json(tmp_path, f"pl.callTiming({c % v}, 0, 'v:1')", "")) for v in ("true", "false")]
    assert got[0]["live"] is True and "live" not in got[1]


@needs_node
def test_the_next_load_does_not_inherit_the_last_ones_live_answer_or_live_counts(tmp_path):
    steps = [*_stream_open(), {"op": "click", "at": 5000}, {"op": "fetch", "at": 5010, "url": "/call?a"},
             {"op": "answer", "at": 5210, "url": "/call?a", "key": "K"}, _frame("K", 5300),
             {"op": "fetch", "at": 5310, "url": "/call?live"}, {"op": "answer", "at": 5400, "url": "/call?live", "key": "K"},
             {"op": "click", "at": 8000}, {"op": "mutate", "at": 8100}]       # the next click, its first render
    load, _ = run(tmp_path, steps, 9200)
    assert load["ms"] == 100 and load["ended"] == "mutation"
    assert load["final"]["liveRefetches"] == 0 and load["final"]["liveFrames"] == 0


@needs_node
def test_refresh_frames_are_read_across_chunks_and_only_refresh_frames_name_a_key(tmp_path):
    out = _node_json(tmp_path, "JSON.stringify(["
                     "pl.refreshKeysOf('event: refresh\\ndata: A\\n\\n: keepalive\\n\\nevent: refresh\\ndata: B\\n\\nevent: res'),"
                     "pl.refreshKeysOf('event: resync\\ndata: pending-overflow\\n\\n'),"
                     "pl.refreshKeysOf('event: refresh\\ndata: v1-key:with:colons\\n\\nevent: refresh\\ndata:')])", "")
    a, b, c = json.loads(out)
    assert a == {"keys": ["A", "B"], "rest": "event: res"} and b["keys"] == []
    assert c == {"keys": ["v1-key:with:colons"], "rest": "event: refresh\ndata:"}


@needs_node
def test_only_a_frame_before_the_request_with_its_key_since_the_click_makes_a_refetch(tmp_path):
    t = json.loads(_node_json(tmp_path, "JSON.stringify(["
                   "pl.isLiveRefetch('K', 100, [{key:'K', at:50}], 0),"       # frame before the request: live
                   "pl.isLiveRefetch('K', 100, [{key:'K', at:150}], 0),"      # frame after it started: not
                   "pl.isLiveRefetch('K', 100, [{key:'J', at:50}], 0),"       # another key
                   "pl.isLiveRefetch('', 100, [{key:'', at:50}], 0),"         # an answer with no key
                   "pl.isLiveRefetch('K', 100, [{key:'K', at:50}], 60)])", ""))  # frame before the click
    assert t == [True, False, False, False, False]


@needs_node
def test_a_change_counts_unless_the_latest_answer_was_live_and_nothing_own_is_pending(tmp_path):
    t = json.loads(_node_json(tmp_path, "JSON.stringify([pl.countsAsChange(0, null), pl.countsAsChange(0, 'own'),"
                   "pl.countsAsChange(0, 'live'), pl.countsAsChange(2, 'live')])", ""))
    assert t == [True, True, False, True]


# ─── the ended tie rules, on b-state ───────────────────────────────────────────────────────────────

def _b(**kw):
    return dict({"navT0": 100, "saw": False, "goneAt": 0, "reqEnd": 0, "lastMut": 0, "count": 0, "armLogin": False}, **kw)


@needs_node
def test_ended_says_start_when_nothing_after_the_click_is_the_latest(tmp_path):
    cases = {
        "nothing": _b(navT0=0),                                    # every end 0 = the click at 0: not a skeleton's
        "request at the click": _b(reqEnd=100),                    # an end exactly AT the click is no request's
        "mutation at the click": _b(reqEnd=50, lastMut=100),       # nor a mutation's (a stale request end below it)
        "skeleton": _b(saw=True, goneAt=300, reqEnd=300, lastMut=300),      # ties: skeleton first
        "request over mutation": _b(reqEnd=300, lastMut=300),
        "mutation": _b(lastMut=300, reqEnd=200),
    }
    got = json.loads(_node_json(tmp_path, "JSON.stringify(Object.fromEntries(Object.entries(JSON.parse(process.argv[2]))"
                                "  .map(([k, b]) => [k, pl.loadOf(b, '/x', []).ended])))", json.dumps(cases)))
    assert got == {"nothing": "start", "request at the click": "start", "mutation at the click": "start",
                   "skeleton": "skeleton", "request over mutation": "request", "mutation": "mutation"}


def _node_json(tmp_path, expr, arg):
    (tmp_path / "pageload.mjs").write_text((BENCH_DIR / "k6" / "pageload.js").read_text())
    (tmp_path / "e.mjs").write_text(f"import * as pl from './pageload.mjs'\nprocess.stdout.write(String({expr}))\n")
    p = subprocess.run([NODE, str(tmp_path / "e.mjs"), arg], capture_output=True, text=True, timeout=60, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    return p.stdout


def _record_ops(tmp_path, load, tags):
    return json.loads(_node_json(tmp_path, "JSON.stringify(pl.recordOps(JSON.parse(process.argv[2]), "
                                 f"{json.dumps(tags)}, 60000, 0))", json.dumps(load)))


@needs_node
def test_the_load_points_carry_what_ended_the_load_and_first_loads_carry_the_old_series(tmp_path):
    r = {"ms": 210, "saw": True, "ended": "request", "states": {}, "t0": 1e12,
         "final": {"skeletonMs": 120, "requestMs": 210, "mutationMs": None, "liveRefetches": 0, "liveFrames": 0}}
    ops = _record_ops(tmp_path, r, {"route": "/x", "visit": "first"})
    assert ops[0][0] == "browser_page_load_ms" and ops[0][1] == 210 and ops[0][2]["ended_by"] == "request"
    old = [o for o in ops if o[0] == "browser_first_load_skeleton_ms"]
    assert len(old) == 1 and old[0][1] == 120                       # the first load, the OLD way: its skeleton's end
    warm = _record_ops(tmp_path, r, {"route": "/x", "visit": "warm"})
    assert not [o for o in warm if o[0] == "browser_first_load_skeleton_ms"]
    # a first load that never showed a skeleton was 0 ms the old way
    r2 = dict(r, saw=False, final=dict(r["final"], skeletonMs=None))
    assert [o[1] for o in _record_ops(tmp_path, r2, {"visit": "first"}) if o[0] == "browser_first_load_skeleton_ms"] == [0]


# ─── browser.warm_loads_final: the blind spot that hid the defect ──────────────────────────────────

def _stream(loads):
    """loads: [(load_id, wall_ms, calls, visit[, timeout])]; a call is its response end (offset 10 ms from the load's
    start) or (from_load_ms, response_end, live)."""
    pts = {"browser_page_load_ms": [], "browser_call_ms": []}
    md = {"browser_page_load_ms": [], "browser_call_ms": []}
    for lid, wall, calls, visit, *to in loads:
        pts["browser_page_load_ms"].append((wall, {"visit": visit, "timeout": to[0] if to else "no", "session": "s1"}))
        md["browser_page_load_ms"].append({"load_id": lid, "vu": "1"})
        for c in calls:
            frm, end, live = (10.0, c - 10.0, False) if not isinstance(c, tuple) else (c + (False,))[:3]
            pts["browser_call_ms"].append((end, {"during": "load", "visit": visit, "session": "s1"}))
            m = {"from_load_ms": frm, "response_end": end, "load_id": lid, **({"live": True} if live else {})}
            md["browser_call_ms"].append({"call_timing": json.dumps(m)})
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


def test_a_requests_end_is_its_start_offset_plus_its_response_end():
    """from_load_ms 100 + response_end 200 = 300 ms after the click: a load of 250 ended before it; of 300 did not."""
    assert k6eval.warm_final_check(_stream([("1:1", 250.0, [(100.0, 200.0)], "warm")]))["result"] == "FAIL"
    assert k6eval.warm_final_check(_stream([("1:1", 300.0, [(100.0, 200.0)], "warm")]))["result"] == "PASS"


def test_a_warm_load_that_timed_out_is_not_judged_here():
    """its recorded time is the bound or how long it was watched, not an end the instrument saw"""
    only = k6eval.warm_final_check(_stream([("1:1", 60000.0, [210.0], "warm", "yes")]))
    assert only is None
    c = k6eval.warm_final_check(_stream([("1:1", 90.0, [210.0], "warm", "yes"), ("1:2", 400.0, [210.0], "warm")]))
    assert c["result"] == "PASS" and c["value"]["warm_loads"] == 1


def test_a_live_updates_refetch_is_not_compared_with_the_loads_time():
    c = k6eval.warm_final_check(_stream([("1:1", 210.0, [210.0, (5000.0, 400.0, True)], "warm")]))
    assert c["result"] == "PASS", c


def test_warm_loads_ending_at_or_after_their_last_request_pass():
    c = k6eval.warm_final_check(_stream([("1:1", 210.0, [210.0], "warm"), ("1:2", 400.0, [210.0, 380.0], "warm"),
                                         ("1:3", 0.0, [], "warm"), ("1:4", 0.0, [900.0], "first")]))
    assert c["result"] == "PASS", c           # a first load's time is not this check's; a no-request 0 is fine
    assert c["value"]["warm_loads_with_requests"] == 2


def test_no_warm_load_is_no_check_here_the_nav_bar_owns_that():
    assert k6eval.warm_final_check(_stream([("1:1", 900.0, [800.0], "first")])) is None
    assert k6eval.warm_final_check(None) is None


def test_browser_checks_carries_the_warm_final_check():
    o = _stream([(f"1:{i}", 0.0, [180.0, 210.0], "warm") for i in range(3)])
    o.update({"stream": True, "frames": {"start": 1, "end": 1}, "truncated": False, "summary": {}, "exit": {"rc": 0}})
    o["points"].update({"browser_logins": [], "browser_page_loads": [], "browser_sse_opened": []})
    c = [x for x in k6eval.browser_checks(o, 5) if x["check"] == "browser.warm_loads_final"]
    assert len(c) == 1 and c[0]["result"] == "FAIL"


# ─── the report: what ended the loads, the live updates, and the old first-load series ─────────────

def test_the_report_splits_ended_by_per_visit_counts_live_update_loads_and_keeps_the_old_first_load_series():
    pts = {"browser_page_load_ms": [
        (210.0, {"visit": "warm", "ended_by": "request", "live_update": "no", "timeout": "no"}),
        (400.0, {"visit": "warm", "ended_by": "mutation", "live_update": "yes", "timeout": "no"}),
        (900.0, {"visit": "first", "ended_by": "request", "live_update": "no", "timeout": "no"}),
        (700.0, {"visit": "first", "ended_by": "skeleton", "live_update": "yes", "timeout": "no"}),
        (60000.0, {"visit": "first", "timeout": "yes"})],
        "browser_first_load_skeleton_ms": [(600.0, {}), (700.0, {})]}
    r = k6eval.final_page_report(pts)
    assert r["load_definition"].startswith("final-page")
    w, f = r["page_load_by_visit_ended_by"]["warm"], r["page_load_by_visit_ended_by"]["first"]
    assert w["n"] == 2 and w["ended_by"] == {"mutation": 1, "request": 1} and w["live_update_share"] == 0.5
    assert f["n"] == 2 and f["ended_by_share"] == {"request": 0.5, "skeleton": 0.5} and f["live_update_loads"] == 1
    assert r["first_load_skeleton_end"]["n"] == 2 and r["first_load_skeleton_end"]["max_ms"] == 700.0
    old = k6eval.final_page_report({"browser_page_load_ms": [(0.0, {"visit": "warm", "timeout": "no"})]})
    assert old["load_definition"].startswith("skeleton end") and old["first_load_skeleton_end"] is None
