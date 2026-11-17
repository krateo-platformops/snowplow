"""The owner's ruling of 2026-11-17, "Measure it precisely": browser.cpu_starvation charged each 5 s window's WHOLE
Chrome-container cgroup stall to EVERY page load overlapping it, though 5 sessions render at once (run28 N1 and run
27c N1B read INVALID on cold_nav_p95 by it). Now each load is charged only its OWN renderer process's run-queue wait
(/proc/<pid>/task/*/schedstat run_delay) over the samples that bracket it, the renderer named by Chrome's own
frame -> process record (k6/renderers.js); a load whose renderer is not known is UNKNOWN (the check undecided), never
the container bound; evidence without per-renderer data keeps the container rule, and the check says which judged.

Every case goes through the evaluate CLI; the real-Chrome case runs the rendered browser container (k6 browser,
Chrome, the renderer sampler and renderers.js) against the fixture SPA.
"""

from __future__ import annotations

import json

import pytest

from bench import k6eval, load606
from tests.test_k6_830 import (T_POINTS, _browser_points, _browser_pod_in_docker, _browser_summary, _check,
                               _cpu_file, _evaluate_cli, _k6_points, _k6_summary, _write_out, _write_viewer,
                               needs_docker)

SESSIONS = ("s1", "s2", "s3")
PIDS = {"s1": 101, "s2": 102, "s3": 103}
UPTIME0 = 1000.0                 # _cpu_file's first sample: its anchor puts uptime 1000 at T_POINTS - 15
START = "88123"                  # each renderer process's starttime (/proc/<pid>/stat field 22): its series key


def _uptime(epoch: float) -> float:
    return epoch - (T_POINTS - 15) + UPTIME0


def _case(tmp_path, *, renderer=True, stalled=(), map_after=True, udd_of=None, stall=None, lost=(), idle=None,
          ops=None, node=True):
    """Warm loads at 600 ms (over the 500 ms bar) for 3 sessions, inside 20 s of browser-container stall (every
    window stalled 5 s: the container rule explains the FAIL away). `renderer`: the run also recorded per-renderer
    evidence: each session's renderer (PIDS) read by renderers.js before and (`map_after`) after its loads, and
    sampled every second; the renderers of `stalled` sessions waited for a CPU all through their loads, the others
    none (an idle page's renderer). `lost`: those sessions ended early (at the points' time, a login-form timeout);
    `idle`: the node's idle ticks per CPU window (0 = no idle CPU); `ops`: the cluster's operations record (none
    recorded by default: every lost session is judged on its renderer alone)."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_viewer(k6dir)
    pts = _browser_points(sessions=SESSIONS, base_ms=600.0)
    for p in pts:
        if p["metric"].startswith("browser_page_load") or p["metric"] == "browser_page_loads":
            p["data"]["tags"]["class"] = "0"
        if p["metric"] == "browser_session_end" and p["data"]["tags"]["session"] in lost:
            p["data"]["tags"].update(early="yes", cause=f"session {p['data']['tags']['session']}: the login form "
                                                        "could not be filled: timed out")
    _write_out(bdir, "browser", pts, _browser_summary(63), 0)
    _cpu_file(bdir, "browser", 3000, [1.0] * 20, stall=stall if stall is not None else [5.0] * 20, idle=idle,
              node=node)
    if renderer:
        loads = [p["data"]["tags"] for p in pts if p["metric"] == "browser_page_load_ms"]
        first, last = min(int(t["t0"]) for t in loads) / 1000, max(int(t["t0"]) for t in loads) / 1000 + 1
        udd = {s: f"/tmp/k6browser-data-{s}" for s in SESSIONS}
        rows = [{"kind": "renderer_udd", "pid": PIDS[s], "start": START, "udd": (udd_of or udd)[s]} for s in SESSIONS]
        for i in range(60):                                   # every second from T_POINTS - 15
            t = T_POINTS - 15 + i
            # a stalled renderer waits for a CPU all the time its loads run (1 s of run_delay per second); every
            # renderer's counter is live: it advanced (2 ms) while its page first rendered, before the loads
            rows.append({"kind": "renderer", "t": _uptime(t), "rd": {
                f"{PIDS[s]}:{START}": (int(1e9 * min(max(0.0, t - (first - 1)), last + 1 - (first - 1)))
                                       if s in stalled else 0) + (1000 if t < first - 8 else 2001000)
                for s in SESSIONS}})
        (bdir / "browser-renderers.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        maps = []
        for s in SESSIONS:
            maps.append({"kind": "renderer_map", "t0": first - 6, "t1": first - 5.9, "udd": udd[s], "target": "T" + s,
                         "session": s, "class": "0", "pid": PIDS[s]})
            if map_after:
                maps.append({"kind": "renderer_map", "t0": last + 2, "t1": last + 2.1, "udd": udd[s],
                             "target": "T" + s, "session": s, "class": "0", "pid": PIDS[s]})
        (bdir / "renderer-map.jsonl").write_text("".join(json.dumps(m) + "\n" for m in maps))
    if not lost:
        return _evaluate_cli(tmp_path, k6dir, bdir)
    rec = tmp_path / "cluster-operations.json"
    rec.write_text(json.dumps({"kind": "cluster_operations", "cluster": "c", "region": "europe-west4",
                               "read_at": T_POINTS + 3600, "readable": True, "unreadable": None,
                               "operations": ops or []}))
    return _evaluate_cli(tmp_path, k6dir, bdir, "--cluster-operations", str(rec))


def test_the_container_rule_still_judges_evidence_without_per_renderer_data(tmp_path):
    """Old evidence: today's rule, named. The container's stall explains the 600 ms loads away: INVALID."""
    _, v = _case(tmp_path, renderer=False)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["value"]["rule"] == k6eval.RULE_CONTAINER and c["result"] == "FAIL", c
    assert "rule: container window" in c["why"]


def test_a_container_stall_does_not_land_on_an_idle_pages_renderer(tmp_path):
    """The ruling's unit case: the same container stall, and each page's own renderer waited for nothing: no load is
    charged the stall, the 600 ms loads FAIL the warm bar as measured, and the check PASSes under the per-renderer
    rule. Mutation guarded: charging the container window (possible_delay_ms) in per-renderer mode turns this RED."""
    _, v = _case(tmp_path)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["value"]["rule"] == k6eval.RULE_RENDERER and c["result"] == "PASS", c
    assert c["value"]["page_delay_ms_max"] == 0.0
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "FAIL"


def test_a_stall_on_the_pages_own_renderer_is_charged_to_its_loads(tmp_path):
    """The positive control: s1's renderer waited 4 s across its loads: its loads (600 ms) may all be the load
    generator's; s2's and s3's renderers waited for nothing, so their 600 ms loads stand: warm p95 still FAILs
    without the stall, nothing is decided by it."""
    _, v = _case(tmp_path, stalled=("s1",))
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["value"]["rule"] == k6eval.RULE_RENDERER and c["value"]["page_delay_ms_max"] > 0, c
    assert c["result"] == "PASS", c
    _, v = _case(tmp_path / "all", stalled=SESSIONS)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "FAIL" and "warm_nav_p95_ms" in c["why"] and "rule: per renderer" in c["why"], c


@pytest.mark.parametrize("kw,said", [
    ({"map_after": False}, "its renderer was not read both before and after it"),
    ({"udd_of": {"s1": "/tmp/k6browser-data-s2", "s2": "/tmp/k6browser-data-s1", "s3": "/tmp/k6browser-data-s3"}},
     "the pid Chrome named is not a renderer the sampler saw under that Chrome's --user-data-dir"),
])
def test_a_load_whose_renderer_is_not_known_is_undecided_never_the_container_bound(tmp_path, kw, said):
    """Mutation guarded: renderer_delay_ms falling back to the container window (or to 0) for an unplaced load turns
    this RED."""
    _, v = _case(tmp_path, **kw)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "UNMEASURED" and said in c["why"] and "rule: per renderer" in c["why"], c
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "INCONCLUSIVE"


def _read(t0, t1=None, pid=7, target="T", session="s", cls="0", udd="u"):
    return {"kind": "renderer_map", "t0": t0, "t1": t0 + 0.1 if t1 is None else t1, "pid": pid, "target": target,
            "session": session, "class": cls, "udd": udd}


def _st(reads, series=None, udd=None, slack=0.0):
    """renderer_state's shape, by hand: series {(pid, start): [(t, ns)]}, udd {(pid, start): udd}."""
    series = series if series is not None else {(7, "1"): [(float(t), 1000 * t) for t in range(0, 21)]}
    return {"slack_s": slack, "renderer": {"anchored": True, "recorded": {"renderers": True, "renderer_map": True},
                                           "series": series, "udd": udd if udd is not None else {k: "u" for k in series},
                                           "reads": sorted(reads, key=lambda m: m["t0"])}}


LOAD = {"session": "s", "class": "0", "start": 5.0, "wall_ms": 2000.0}      # 5.0 .. 7.0


def test_a_load_between_two_reads_of_its_renderer_is_charged_its_series_between_its_bracketing_samples():
    st = _st([_read(1), _read(9)])
    # samples at 5 (<= start) and 7 (>= end): 1000 ns/s x 2 s
    assert k6eval.renderer_delay_ms(LOAD, st) == (pytest.approx(2000 / 1e6), "")


@pytest.mark.parametrize("reads,series,udd,slack,want", [
    # L1: a read that only STARTED before the load (it ended after the load began) is not a read before it
    ([_read(4.9, 5.5), _read(9)], None, None, 0.0, "its renderer was not read both before and after it"),
    # L2: the same pid, another page target, in a read between: the renderer is not known to be the page's
    ([_read(1), _read(6, target="other"), _read(9)], None, None, 0.0, "its renderer changed while it loaded"),
    # a FAILED read of its Chrome between the two breaks the chain (it is never dropped)
    ([_read(1), {"kind": "renderer_map", "t0": 6, "t1": 6.1, "pid": None, "udd": "u", "why": "x"}, _read(9)],
     None, None, 0.0, "a read of its Chrome between the two failed"),
    # (pid, starttime): pid 7 was another process at the read after the load (a reused pid)
    ([_read(1), _read(9)], {(7, "1"): [(t, 1000 * t) for t in range(0, 8)],
                            (7, "2"): [(t, 50 * t) for t in range(8, 21)]}, None, 0.0,
     "pid 7 was another process at the read after the load"),
    # liveness: a series that never advanced is not shown live
    ([_read(1), _read(9)], {(7, "1"): [(float(t), 4242) for t in range(0, 21)]}, None, 0.0,
     "its renderer's run_delay never advanced over the run: the counter is not shown live"),
    # L5: a summed run_delay that decreased across the load (a thread exited) is flagged, never clamped to 0
    ([_read(1), _read(9)], {(7, "1"): [(float(t), 10000 - 100 * t) for t in range(0, 21)]}, None, 0.0,
     "its renderer's summed run_delay decreased across it (a thread exited with its run_delay)"),
])
def test_a_load_whose_renderer_or_its_stall_is_not_known_is_unknown(reads, series, udd, slack, want):
    """Mutants pinned: L1 (a read that only started before counts as before), L2 (the target check dropped), a
    failed read dropped, pid instead of (pid, starttime), no liveness check, L5 (a negative delta kept)."""
    assert k6eval.renderer_delay_ms(LOAD, _st(reads, series, udd, slack)) == (None, want)


def test_the_stall_is_capped_at_the_loads_own_time():
    """L4: the cap. 1 ms of run_delay per ms around a 2 s load: 2 s of bracket (5..7) is the load's whole time."""
    st = _st([_read(1), _read(9)], {(7, "1"): [(float(t), int(2e9) * t) for t in range(0, 21)]})
    assert k6eval.renderer_delay_ms(LOAD, st) == (2000.0, "")


def test_the_anchors_slack_widens_the_bracket_outward_never_inward():
    """L6: with 1 s of slack the bracket is the sample at or before 4.0 and the one at or after 8.0 (4 s of a renderer
    waiting 1000 ns/s), never 6 .. 6."""
    st = _st([_read(1), _read(9.5)], slack=1.0)
    assert k6eval.renderer_delay_ms(LOAD, st) == (pytest.approx(4000 / 1e6), "")


# ─── real Chrome: the frame -> renderer mapping, from the rendered browser container ─────────────────────────

@needs_docker
def test_in_chrome_each_sessions_page_maps_to_a_renderer_of_its_own_chrome(tmp_path):
    """The rendered k6browser pod against the fixture SPA, 3 sessions for 22 s: the browser container, and the
    renderers sidecar beside it (its own cgroup; the browser's process namespace, network and /tmp). renderers.js
    reads which renderer renders each session's page only when the page asks (before a class's sign-in, after its
    login load, after its last load: never on a timer, never inside a load); the sampler records each renderer's run_delay by
    (pid, starttime) and its Chrome's --user-data-dir. Exact: every read names a pid whose sampled series lies under
    the --user-data-dir of the very Chrome whose endpoint named it, each session its own pid, the same pid in every
    read of a class; and EVERY load is charged a measured stall (browser.js waits for a read before a class's
    sign-in and after its last load: no load is UNKNOWN)."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=0, duration_s=22)
    assert p.returncode == 0, p.stderr[-3000:]
    assert (out / "renderers-done").exists()                   # the sidecar ran to the browser's end
    watch = json.loads((out / "renderer-watch.json").read_text())
    assert watch["browsers"] == 3 and watch["of"] == 3, watch
    o = k6eval.read_k6_out(str(out), "browser")
    udd = {(r["pid"], str(r["start"])): r["udd"] for r in o["renderers"] if r["kind"] == "renderer_udd"}
    samples = [r for r in o["renderers"] if r["kind"] == "renderer"]
    reads = [m for m in o["renderer_map"] if m.get("kind") == "renderer_map" and m.get("pid") is not None]
    assert samples and reads, (o["renderers"][:5], o["renderer_map"][:5])
    assert all(":" in k for r in samples for k in r["rd"])     # (pid, starttime) keys
    for m in reads:
        assert m["udd"] in {u for (pid, _), u in udd.items() if pid == m["pid"]}, (m, udd)
    by_key: dict = {}
    for m in reads:
        by_key.setdefault((m["session"], m["class"]), set()).add(m["pid"])
    assert {s for s, _ in by_key} == {"s1", "s2", "s3"}, by_key
    assert all(len(v) == 1 for v in by_key.values()), by_key   # one renderer per page, read after read
    assert len({next(iter(v)) for v in by_key.values()}) == len(by_key)   # never another session's
    assert len({m["udd"] for m in reads}) == 3                 # one Chrome per session (k6: one per VU)
    st = k6eval.starvation(o, "browser")
    assert st["rule"] == k6eval.RULE_RENDERER
    navs = k6eval.browser_navs(o)
    placed = [k6eval.renderer_delay_ms(n, st) for n in navs]
    assert navs and all(d is not None for d, _ in placed), [w for _, w in placed if w]
    assert all(0 <= d <= n["wall_ms"] for n, (d, _) in zip(navs, placed))
    # reads only where asked, never more: each read names its ask, and each ask of a class is read once
    assert all(m.get("ask") in ("before", "login", "after") for m in reads), reads
    per: dict = {}
    for m in reads:
        per[(m["session"], m["class"], m["ask"])] = per.get((m["session"], m["class"], m["ask"]), 0) + 1
    assert per and all(v == 1 for v in per.values()), per
    # the wait, with the watcher running: every ask ended on its read (none reached its bound); the after-wait then
    # holds the page for the sampler's next sample
    # (2 x RENDERER_SAMPLE_S), so the read after a class's last load has a sample of its renderer after it
    from bench import k6_job
    waits = o["points"]["browser_renderer_wait_ms"]
    assert {(t["session"], t["side"]) for _, t in waits} >= {(s, w) for s in ("s1", "s2", "s3")
                                                             for w in ("before", "after")}, waits
    hold = 2 * k6_job.RENDERER_SAMPLE_S * 1000
    assert all(t["read"] == "yes" and v <= 2 * k6_job.RENDERER_READ_MS
               + (hold if t["side"] == "after" else 0) for v, t in waits), waits
    assert all(v >= hold for v, t in waits if t["side"] == "after"), waits


@needs_docker
def test_in_chrome_a_renderer_wait_is_outside_every_measured_time(tmp_path):
    """A watcher stated but none running (a bare `k6 run`): each class's before-wait runs to its full bound
    (rendererReadWaitMs, 20 s), and still the sign-in (3.4 s) and the first page (150 + 250 ms) are timed exactly as
    without one: the wait is never part of a measured time (mutation: starting the sign-in's clock before the wait
    turns this RED)."""
    from bench import k6_job
    from tests.test_k6_830 import _run_browser_fixture
    o = _run_browser_fixture(tmp_path, renderer_watch=True, duration_s=30, routes=("/dashboard", "/compositions"))
    waits = o["points"]["browser_renderer_wait_ms"]
    before = [v for v, t in waits if t["side"] == "before"]
    bound = 2 * k6_job.RENDERER_READ_MS
    assert before and all(t["read"] == "no" for _, t in waits), waits
    assert all(bound <= v <= bound + 2000 for v in before), before
    login = [v for v, t in o["points"]["browser_page_load_ms"] if t["method"] == "login"]
    assert len(login) == 1 and 380 <= login[0] <= 480, login
    signin = [v for v, _ in o["points"]["browser_signin_ms"]]
    assert len(signin) == 1 and 3300 <= signin[0] <= 4500, signin


# ─── a lost session, judged by cause: the load generator measured saturated is an environment event (#1517) ──
# the owner's ruling (2026-11-17): "saturation = environment, stated honestly"

#: the CPU windows (5 s from T_POINTS - 15): the third, [T - 5, T], holds s2's loss window; idle 0 = no idle CPU
SATURATED_AT_LOSS = [400, 400, 0] + [400] * 17
SATURATED = "load generator saturated (no idle CPU) during the session's window"


def _sessions(v):
    return (next(c for c in v["validity"] if c["check"] == "environment.browser_sessions"),
            next((c for c in v["checks"] if c["check"] == "browser.sessions_lost"), None))


@pytest.mark.parametrize("stalled", [("s2",), ()], ids=["its-renderer-waited", "its-renderer-did-not-wait"])
def test_a_session_lost_while_the_load_generator_was_saturated_is_the_environment(tmp_path, stalled):
    """No control-plane operation in its window, but the node had not one idle CPU tick in a window overlapping
    it: INVALID by environment, named so, with no causal claim, whether or not its renderer's run_delay moved (on a
    saturated node every process waits) (mutation: dropping the saturation events turns this a product FAIL)."""
    _, v = _case(tmp_path, lost=("s2",), stalled=stalled, idle=SATURATED_AT_LOSS)
    ses, lost = _sessions(v)
    assert ses["result"] == "FAIL" and ses["why"].startswith("INVALID by environment: 1 of 3 "), ses
    assert SATURATED in ses["why"] and "waited" not in ses["why"] and lost is None


def test_a_session_lost_on_an_unsaturated_load_generator_is_a_product_fail(tmp_path):
    _, v = _case(tmp_path, lost=("s2",), stalled=("s2",))
    ses, lost = _sessions(v)
    assert ses["result"] == "PASS" and lost["result"] == "FAIL", (ses, lost)
    assert "no environment event in its window" in lost["why"]


def test_a_session_lost_with_the_nodes_cpu_unmeasured_is_undecided(tmp_path):
    """Re: per-renderer evidence, but the node's idle ticks were not sampled: undecided, never "no event"
    (mutation: reading an unmeasured node as unsaturated turns this a product FAIL)."""
    _, v = _case(tmp_path, lost=("s2",), node=False)
    ses, lost = _sessions(v)
    assert ses["result"] == "FAIL" and lost is None, (ses, lost)
    assert ses["why"].startswith("INVALID by environment (undecided): s2: ") and \
        "the loadgen node's CPU was not measured" in ses["why"], ses["why"]


def test_old_evidence_judges_a_lost_session_on_control_plane_events_only(tmp_path):
    """No per-renderer data (a bench before #1517): the node's saturation is not a session's environment event;
    with no control-plane operation in the window the loss is a product FAIL (the owner's ruling)."""
    _, v = _case(tmp_path, renderer=False, lost=("s2",), idle=SATURATED_AT_LOSS)
    ses, lost = _sessions(v)
    assert ses["result"] == "PASS" and lost["result"] == "FAIL", (ses, lost)


def _st(*wins, node=True, anchored=True, slack=0.0):
    return {"node": node, "anchored": anchored, "slack_s": slack,
            "windows": [{"t0": a, "t1": b, "saturated": sat} for a, b, sat in wins]}


@pytest.mark.parametrize("wins, w0, w1, want", [
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 8, 12, 1),       # inside one saturated window
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 9.9, 14, 1),     # a PARTIAL overlap counts
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 10, 14, 1),      # touching its end: one shared instant
    ([(0, 5, False), (5, 10, False), (10, 15, False)], 1, 14, 0),      # covered, never saturated: no event
], ids=["inside", "partial", "touching", "covered-unsaturated"])
def test_saturation_overlaps_mean_one_shared_instant(wins, w0, w1, want):
    got, why = k6eval.saturation_events(_st(*wins), w0, w1)
    assert why == "" and len(got) == want and all(e["what"] == SATURATED for e in got), (got, why)


@pytest.mark.parametrize("st, w0, w1, why", [
    (_st((0, 5, False), node=False), 1, 4, "the loadgen node's CPU was not measured"),
    (_st((0, 5, False), anchored=False), 1, 4, "the CPU samples have no wall-clock anchor"),
    (_st((0, 5, False), (5, 10, False)), 1, 14, "the loadgen node's CPU samples do not cover the whole session window"),
    (_st((0, 5, False), (8, 12, False)), 1, 11, "the loadgen node's CPU samples do not cover the whole session window"),
    (_st(), 1, 4, "the loadgen node's CPU samples do not cover the whole session window"),
], ids=["node-unmeasured", "unanchored", "window-past-the-samples", "gap-in-the-samples", "no-samples"])
def test_saturation_that_cannot_be_told_is_undecided_never_no_event(st, w0, w1, why):
    """Rd / Re: an unmeasured node, unanchored samples, or a stretch of the session window no sample covers, with
    no saturation seen elsewhere in it: (None, why), never [] (mutations: each check dropped turns its case RED)."""
    assert k6eval.saturation_events(st, w0, w1) == (None, why)


def test_the_anchors_error_widens_each_window_outward():
    """A window 0.05 s short of the session's start overlaps it once widened by the anchor's 0.1 s error."""
    got, _ = k6eval.saturation_events(_st((0, 4.95, True), (4.95, 10, False), slack=0.1), 5, 9)
    assert len(got) == 1


def test_a_renderer_is_known_alive_from_its_process_start_not_only_from_its_first_sample():
    """A class's first read can come before the sampler's first sample of a renderer created just before it (the pod
    test, 2026-11-17: read at +0.0 s, first sample at +0.4 s): its process's own start (starttime / CLK_TCK, on the
    samples' uptime) shows it held the pid through the read. One that started after the read did not (mutation:
    ignoring the start leaves the first case UNKNOWN; reading the start as "alive" without the hz the sampler
    recorded is not done: no hz, no start)."""
    o = {"renderers": [{"kind": "renderer_clock", "hz": 100},
                       {"kind": "renderer", "t": 5.4, "rd": {"10:500": 1}},
                       {"kind": "renderer", "t": 6.4, "rd": {"10:500": 2}}]}
    r = k6eval.renderer_state(o, 100.0)                       # epoch = uptime + 100: the process started at 105.0
    assert r["started"] == {(10, "500"): 105.0}
    assert k6eval._series_of(r, 10, 105.1, 105.2) == ((10, "500"), "")
    assert k6eval._series_of(r, 10, 104.0, 104.5)[0] is None  # before the process existed: not its pid then
    no_hz = k6eval.renderer_state({"renderers": o["renderers"][1:]}, 100.0)
    assert no_hz["started"] == {} and k6eval._series_of(no_hz, 10, 105.1, 105.2)[0] is None


def test_render_browser_refuses_a_config_without_its_renderer_watch():
    """The k6browser pod always has its renderers sidecar: a config that says no watcher (no wait for a read) is
    refused, never rendered into a measured pod (mutation: dropping the refusal turns this RED)."""
    from bench import k6_job
    kw = dict(sessions=[{"name": "s1", "user": "u", "offset": 0, "cold": True}], routes=["/a"], password="x",
              think_s=1, duration_s=10, frontend_url="http://f", snowplow_url="http://s", authn_url="http://a",
              debug_user="d")
    assert "renderers" in k6_job.render_browser(config=k6_job.browser_config(**kw), linger_s=0)
    with pytest.raises(ValueError, match="the k6browser pod has a renderers watcher"):
        k6_job.render_browser(config=k6_job.browser_config(**kw, renderer_watch=False), linger_s=0)
    with pytest.raises(ValueError, match="the k6browser pod has a renderers watcher"):
        k6_job.render_browser(config=dict(k6_job.browser_config(**kw), rendererReadWaitMs=0), linger_s=0)


# ─── the sidecar's sampler, under a POSIX sh, against a process that rewrites its command line as Chromium does ──

@pytest.mark.skipif(not __import__("os").path.isdir("/proc/self/task") or __import__("shutil").which("perl") is None,
                    reason="needs Linux /proc and perl (it rewrites $0 into one space-joined string, as Chromium does)")
def test_the_sampler_keys_each_renderer_by_pid_and_starttime_and_reads_its_run_delay(tmp_path):
    """A process whose command line reads `/usr/lib/chromium/chromium --type=renderer --user-data-dir=...` (one
    space-joined string) is sampled under the key "<pid>:<starttime>" (its /proc/<pid>/stat field 22), with its
    --user-data-dir; a shell whose command line merely holds the text is not a renderer."""
    import subprocess
    import time

    from bench import k6_job
    fake = subprocess.Popen(["perl", "-e", '$0 = "/usr/lib/chromium/chromium --type=renderer '
                             '--user-data-dir=/tmp/k6browser-data-t1 --renderer-client-id=5"; '
                             'while (1) { $x++ for 1 .. 100000; select(undef, undef, undef, 0.05) }'])
    other = subprocess.Popen(["perl", "-e", '$0 = "/bin/sh -c echo --type=renderer"; sleep 30'])
    try:
        time.sleep(0.5)
        with open(f"/proc/{fake.pid}/stat") as fh:
            start = fh.read().rsplit(") ", 1)[1].split()[19]
        script = k6_job._renderer_sampler(1).replace("/out/", f"{tmp_path}/")
        p = subprocess.run(["sh", "-c", script + "rsample\nsleep 1\nrsample\n"], capture_output=True, text=True,
                           timeout=60)
        assert p.returncode == 0, p.stderr
        rows = [json.loads(x) for x in (tmp_path / "browser-renderers.jsonl").read_text().splitlines()]
    finally:
        fake.kill()
        other.kill()
    # judged on this test's own two processes only: the host's /proc holds whatever else runs beside it (a shared
    # build VM: another job's fake renderers), where the pod's own process namespace would not
    mine = (fake.pid, other.pid)
    udd = [r for r in rows if r["kind"] == "renderer_udd" and r["pid"] in mine]
    assert [(r["pid"], str(r["start"]), r["udd"]) for r in udd] == [(fake.pid, start, "/tmp/k6browser-data-t1")], rows
    samples = [dict(r, rd={k: v for k, v in r["rd"].items() if int(k.split(":")[0]) in mine})
               for r in rows if r["kind"] == "renderer"]
    assert len(samples) == 2 and all(list(s["rd"]) == [f"{fake.pid}:{start}"] for s in samples), samples
    key = f"{fake.pid}:{start}"
    assert samples[1]["rd"][key] >= samples[0]["rd"][key] >= 0
