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
import yaml

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
          ops=None, node=True, helper_cpu=True, watch=None, omit=(), per_thread=False, exiting=(), churn=(), spawn=()):
    """Warm loads at 600 ms (over the 500 ms bar) for 3 sessions, inside 20 s of browser-container stall (every
    window stalled 5 s: the container rule explains the FAIL away). `renderer`: the run also recorded per-renderer
    evidence: each session's renderer (PIDS) read by renderers.js before and (`map_after`) after its loads, and
    sampled every second; the renderers of `stalled` sessions waited for a CPU all through their loads, the others
    none (an idle page's renderer). `lost`: those sessions ended early (at the points' time, a login-form timeout);
    `idle`: the node's idle ticks per CPU window (0 = no idle CPU); `ops`: the cluster's operations record (none
    recorded by default: every lost session is judged on its renderer alone). The run's recorded loadgen manifest
    states the renderer watch (#1525 review): `watch` True (default: as `renderer`), or False (absent: a bench
    before #1517); `omit`: those per-renderer files removed after they were written. `per_thread`: the samples also
    hold each renderer THREAD's own run_delay ("th", a sampler since 2026-11-18): the renderer's main thread holds
    the whole stall; the renderers of `exiting` sessions also had a thread holding 3 s of run_delay (accrued before
    the loads) that exits halfway through them, so their summed "rd" drops by 3 s; those of `churn` sessions start a
    thread at every sample that is gone by the next (no run_delay of its own); those of `spawn` sessions start 20
    threads at once halfway through the loads, 1 ms of run_delay each, which stay."""
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
    if renderer and helper_cpu:
        # the renderers sidecar's own cgroup (Diego 2026-11-17: the helper measured directly): 0.04 then 0.06
        # cores, a minute each, over 5 minutes
        _cpu_file(bdir, "renderers", 250, ([0.04] * 12 + [0.06] * 12) * 2 + [0.04] * 12, limit_m=250)
    if renderer:
        loads = [p["data"]["tags"] for p in pts if p["metric"] == "browser_page_load_ms"]
        first, last = min(int(t["t0"]) for t in loads) / 1000, max(int(t["t0"]) for t in loads) / 1000 + 1
        udd = {s: f"/tmp/k6browser-data-{s}" for s in SESSIONS}
        rows = [{"kind": "renderer_udd", "pid": PIDS[s], "start": START, "udd": (udd_of or udd)[s]} for s in SESSIONS]
        mid = (first + last) / 2
        for i in range(60):                                   # every second from T_POINTS - 15
            t = T_POINTS - 15 + i
            # a stalled renderer waits for a CPU all the time its loads run (1 s of run_delay per second); every
            # renderer's counter is live: it advanced (2 ms) while its page first rendered, before the loads
            main = {s: (int(1e9 * min(max(0.0, t - (first - 1)), last + 1 - (first - 1))) if s in stalled else 0)
                    + (1000 if t < first - 8 else 2001000) for s in SESSIONS}
            th = {s: {f"{PIDS[s]}:{START}": main[s]} for s in SESSIONS}
            for s in exiting:
                if t < mid:
                    th[s][f"{PIDS[s] + 5000}:{int(START) + 7}"] = 3_000_000_000
            for s in churn:
                th[s][f"{20000 + i}:{int(START) + 100 + i}"] = 0
            for s in spawn:
                if t >= mid:
                    th[s].update({f"{30000 + j}:{int(START) + 900}": 1_000_000 for j in range(20)})
            row = {"kind": "renderer", "t": _uptime(t),
                   "rd": {f"{PIDS[s]}:{START}": sum(th[s].values()) for s in SESSIONS}}
            if per_thread:
                row["th"] = {f"{PIDS[s]}:{START}": th[s] for s in SESSIONS}
            rows.append(row)
        (bdir / "browser-renderers.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        maps = []
        for s in SESSIONS:
            maps.append({"kind": "renderer_map", "t0": first - 6, "t1": first - 5.9, "udd": udd[s], "target": "T" + s,
                         "session": s, "class": "0", "pid": PIDS[s]})
            if map_after:
                maps.append({"kind": "renderer_map", "t0": last + 2, "t1": last + 2.1, "udd": udd[s],
                             "target": "T" + s, "session": s, "class": "0", "pid": PIDS[s]})
        (bdir / "renderer-map.jsonl").write_text("".join(json.dumps(m) + "\n" for m in maps))
    for f in omit:
        (bdir / f).unlink()
    watch = renderer if watch is None else watch
    cfg = {"sessions": [{"name": s} for s in SESSIONS], **({"rendererWatch": True} if watch else {})}
    mf = tmp_path / "loadgen-job.yaml"
    mf.write_text(yaml.safe_dump({"kind": "ConfigMap", "metadata": {"name": "k6browser"},
                                  "data": {"browser-config.json": json.dumps(cfg)}}))
    if not lost:
        return _evaluate_cli(tmp_path, k6dir, bdir, "--loadgen-job", str(mf))
    rec = tmp_path / "cluster-operations.json"
    rec.write_text(json.dumps({"kind": "cluster_operations", "cluster": "c", "region": "europe-west4",
                               "read_at": T_POINTS + 3600, "readable": True, "unreadable": None,
                               "operations": ops or []}))
    return _evaluate_cli(tmp_path, k6dir, bdir, "--cluster-operations", str(rec), "--loadgen-job", str(mf))


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


# ─── per-thread run_delay (rehf1, 2026-11-18): a renderer thread that exits is measured, with its bound ──────────
# rehf1: 8 wall-time checks INCONCLUSIVE and browser.cpu_starvation UNMEASURED from 2 of 5172 loads whose renderer's
# SUMMED run_delay decreased (a thread exited with its run_delay). The sampler now records each thread's own
# run_delay, keyed by (tid, the thread's own starttime), and the evaluator sums each thread's own delta.

def test_a_renderer_thread_that_exits_mid_load_is_measured_with_its_bound(tmp_path):
    """s1's renderer loses a thread holding 3 s of run_delay halfway through its loads. Per thread: every load is
    MEASURED (the thread keeps its last value: no stall is charged for it); the exit is a signal, its under-count
    bound one sampling period (1 s, the gap between the thread's last sample and the next). The bound cannot flip a
    verdict here (judged again with it, warm p95 still FAILs: the loads that carry it are too few to move the p95),
    so the check is DECIDED (PASS: the idle renderers take nothing from the 600 ms loads) and warm p95 FAILs as
    measured."""
    _, v = _case(tmp_path, per_thread=True, exiting=("s1",))
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "PASS", c
    th = c["value"]["renderer_threads"]
    assert th["loads_with_thread_exit"] >= 1 and th["loads_with_thread_start"] == 0, th
    assert th["undercount_bound_ms_max"] == pytest.approx(1000.0, abs=1), th
    assert th["loads_measured"] == 63, th
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "FAIL"


def test_old_evidence_with_only_the_summed_run_delay_keeps_the_exit_unknown(tmp_path):
    """The same renderers recorded by a sampler before per-thread samples (only "rd"): the drop is not
    distinguishable from a broken counter, so those loads stay UNKNOWN and the wall-time checks INCONCLUSIVE
    (rehf1's shape: its evidence can only be read this way)."""
    _, v = _case(tmp_path, per_thread=False, exiting=("s1",))
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "UNMEASURED", c
    assert "its renderer's summed run_delay decreased across it (a thread exited with its run_delay)" in c["why"]
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "INCONCLUSIVE"


def test_an_exit_bound_that_could_flip_a_verdict_leaves_the_check_unmeasured_and_named(tmp_path):
    """Every renderer starts a thread at every sample that is gone by the next: every load's bracket holds an exited
    thread whose run_delay after its last sample is unknown, up to 1 s each (2 s for a load whose bracket spans two
    sample gaps, each with a thread that exited). Judged on the measured stall (0), warm p95
    FAILs without it as with it; judged on measured + bound (capped at the 600 ms loads), it would PASS. The two
    disagree, so the outcome is not known: UNMEASURED, naming the bound, the loads and the check, and warm p95
    INCONCLUSIVE (mutations: judging the measured stall alone reads PASS; judging measured + bound alone reads FAIL)."""
    _, v = _case(tmp_path, per_thread=True, churn=SESSIONS)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "UNMEASURED", c
    assert c["why"].startswith("UNMEASURED: the under-count bound of 63 page load(s) whose renderer lost a thread "
                               "inside it (up to 2000.0 ms each"), c["why"]
    assert "could flip warm_nav_p95_ms (FAIL without the measured stall, PASS without it and the bound)" in c["why"], \
        c["why"]
    th = c["value"]["renderer_threads"]
    assert th["loads_with_thread_exit"] == 63 and th["loads_with_thread_start"] == 63, th
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "INCONCLUSIVE"


def test_a_renderer_starting_twenty_threads_at_once_is_measured_whole_with_no_bound(tmp_path):
    """reviewer-1480's case (real Chrome: ~20 threads spawned at once inside 2.1% of loads, measured ~0.1 ms, bound
    ~22 s at f33a1dc1): every renderer starts 20 threads halfway through the loads, 1 ms of run_delay each. A new
    thread's run_delay began at 0, so its first sampled value is all of it: measured (20 x 1 ms on the loads whose
    bracket holds the start), no bound, the check DECIDED (PASS: 580 ms is still over the bar) and warm p95 FAILs as
    measured (mutations: a start bound per new thread re-added reads UNMEASURED; counting a new thread from its first
    sample measures 0)."""
    _, v = _case(tmp_path, per_thread=True, spawn=SESSIONS)
    c = _check(v["validity"], "browser.cpu_starvation")
    assert c["result"] == "PASS", c
    th = c["value"]["renderer_threads"]
    assert th["loads_with_thread_start"] >= 1 and th["loads_with_thread_exit"] == 0, th
    assert th["undercount_bound_ms_max"] == 0.0 and th["undercount_bound_ms_total"] == 0.0, th
    assert c["value"]["page_delay_ms_max"] == 20.0, c["value"]
    assert _check(v["checks"], "warm_nav_p95_ms")["result"] == "FAIL"


def _thr(series_ns, extra):
    """renderer_state's per-thread shape by hand: at each integer t in 0..20 the main thread ("7", "1") at
    `series_ns`(t), plus each extra thread {(tid, start): {t: ns}} where it has a sample."""
    at = {}
    for t in range(0, 21):
        snap = {("7", "1"): series_ns(t)}
        for k, vals in extra.items():
            if t in vals:
                snap[k] = vals[t]
        at[float(t)] = snap
    return at


def _st_thr(extra, main=lambda t: 1000 * t):
    threads = _thr(main, extra)
    series = {(7, "1"): [(t, sum(s.values())) for t, s in sorted(threads.items())]}
    st = _st([_read(1), _read(9)], series)
    st["renderer"].update(threads={(7, "1"): threads}, per_thread=True)
    return st


@pytest.mark.parametrize("extra,want", [
    # an exited thread keeps its last sampled value: +1000 ns from t5 to t6, then gone (bound: t6 -> t7, 1 s)
    ({("12", "80"): {t: 1000 * t for t in range(0, 7)}},
     {"ms": pytest.approx(3000 / 1e6), "bound_ms": 1000.0, "exited": 1, "started": 0}),
    # a thread first seen mid-load started inside the bracket, its run_delay from 0: its last value (5100 ns) is all
    # of it, no bound
    ({("11", "70"): {t: 5000 + 100 * (t - 6) for t in range(6, 21)}},
     {"ms": pytest.approx(7100 / 1e6), "bound_ms": 0.0, "exited": 0, "started": 1}),
    # a tid reused with another starttime is another thread: (9, 50) exits after t5 (bound 1 s), (9, 60) starts at
    # t6 (100 ns at t7, all of it)
    ({("9", "50"): {t: 9000 for t in range(0, 6)}, ("9", "60"): {t: 100 * (t - 6) for t in range(6, 21)}},
     {"ms": pytest.approx(2100 / 1e6), "bound_ms": 1000.0, "exited": 1, "started": 1}),
], ids=["exited-keeps-its-last-value", "new-thread-whole-from-zero", "tid-reused-new-starttime"])
def test_each_threads_own_delta_is_summed_with_the_bound_stated(extra, want):
    """LOAD 5.0 .. 7.0, bracketed by the samples at 5 and 7; the main thread waits 1000 ns/s (2000 ns). Mutants
    pinned: an exited thread dropped (summing only the threads alive at the end), a new thread counted from its
    first sample (losing what it waited before it), a start bound re-added, a thread keyed by its tid alone (the
    reused tid reads as one thread going backwards: UNKNOWN), the exit bound not added."""
    got = k6eval.renderer_delay(LOAD, _st_thr(extra))
    assert got["why"] == "" and {k: got[k] for k in want} == want, got
    assert got["upper_ms"] == pytest.approx(min(LOAD["wall_ms"], got["ms"] + want["bound_ms"]))


def test_a_threads_own_run_delay_going_backwards_stays_unknown():
    """The same tid AND starttime with a lower run_delay at a later sample is a broken counter, not an exit: UNKNOWN,
    never clamped (mutation: clamping the decrease to 0 reads MEASURED)."""
    got = k6eval.renderer_delay(LOAD, _st_thr({("13", "90"): {5: 8000, 6: 7000, 7: 9000}}))
    assert got["ms"] is None and got["why"] == ("thread 13 (starttime 90) of its renderer: its own run_delay went "
                                                "backwards across it (a broken counter, not an exit)"), got


def test_a_bracketing_sample_without_its_per_thread_record_is_unknown():
    """Per-thread evidence where one bracketing sample lacks the renderer's threads: UNKNOWN, never the summed rule."""
    st = _st_thr({})
    del st["renderer"]["threads"][(7, "1")][6.0]
    got = k6eval.renderer_delay(LOAD, st)
    assert got["ms"] is None and got["why"] == "a sample of its renderer that brackets it has no per-thread record"


def test_renderer_state_reads_each_threads_key_and_value():
    o = {"renderers": [{"kind": "renderer", "t": 5.0, "rd": {"10:500": 7}, "th": {"10:500": {"10:500": 3, "11:530": 4}}},
                       {"kind": "renderer", "t": 6.0, "rd": {"10:500": 3}, "th": {"10:500": {"10:500": 3}}}]}
    r = k6eval.renderer_state(o, 100.0)
    assert r["per_thread"] is True
    assert r["threads"] == {(10, "500"): {105.0: {("10", "500"): 3, ("11", "530"): 4}, 106.0: {("10", "500"): 3}}}
    old = {"renderers": [{k: v for k, v in r_.items() if k != "th"} for r_ in o["renderers"]]}
    assert k6eval.renderer_state(old, 100.0)["per_thread"] is False


def test_a_reused_tid_read_from_the_samples_is_another_thread():
    """From the sampler's own records through renderer_state: tid 9 with starttime 50 exits after t5 and a new
    thread gets tid 9 with starttime 60 at t6, its run_delay starting low. Two threads (one exited, one started,
    the stall measured), never one thread going backwards (mutation: keying a thread by its tid alone reads UNKNOWN)."""
    rows = [{"kind": "renderer_udd", "pid": 7, "start": "1", "udd": "u"}]
    for t in range(0, 21):
        th = {"7:1": 1000 * t}
        th.update({"9:50": 9000} if t <= 5 else {"9:60": 100 * (t - 6)})
        rows.append({"kind": "renderer", "t": float(t), "rd": {"7:1": sum(th.values())}, "th": {"7:1": th}})
    r = k6eval.renderer_state({"renderers": rows, "renderer_map": [_read(1), _read(9)]}, 0.0)
    got = k6eval.renderer_delay(LOAD, {"slack_s": 0.0, "renderer": r})
    assert got["why"] == "" and got["ms"] == pytest.approx(2100 / 1e6), got
    assert (got["exited"], got["started"], got["bound_ms"]) == (1, 1, 1000.0), got


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
    o["renderer_watch"] = k6eval.WATCH_ON                          # the pod's config (render_browser: always on)
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


def _sat_st(*wins, node=True, anchored=True, slack=0.0):
    return {"node": node, "anchored": anchored, "slack_s": slack,
            "windows": [{"t0": a, "t1": b, "saturated": sat} for a, b, sat in wins]}


@pytest.mark.parametrize("wins, w0, w1, want", [
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 8, 12, 1),       # inside one saturated window
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 9.9, 14, 1),     # a PARTIAL overlap counts
    ([(0, 5, False), (5, 10, True), (10, 15, False)], 10, 14, 1),      # touching its end: one shared instant
    ([(0, 5, False), (5, 10, False), (10, 15, False)], 1, 14, 0),      # covered, never saturated: no event
], ids=["inside", "partial", "touching", "covered-unsaturated"])
def test_saturation_overlaps_mean_one_shared_instant(wins, w0, w1, want):
    got, why = k6eval.saturation_events(_sat_st(*wins), w0, w1)
    assert why == "" and len(got) == want and all(e["what"] == SATURATED for e in got), (got, why)


@pytest.mark.parametrize("st, w0, w1, why", [
    (_sat_st((0, 5, False), node=False), 1, 4, "the loadgen node's CPU was not measured"),
    (_sat_st((0, 5, False), anchored=False), 1, 4, "the CPU samples have no wall-clock anchor"),
    (_sat_st((0, 5, False), (5, 10, False)), 1, 14, "the loadgen node's CPU samples do not cover the whole session window"),
    (_sat_st((0, 5, False), (8, 12, False)), 1, 11, "the loadgen node's CPU samples do not cover the whole session window"),
    (_sat_st(), 1, 4, "the loadgen node's CPU samples do not cover the whole session window"),
], ids=["node-unmeasured", "unanchored", "window-past-the-samples", "gap-in-the-samples", "no-samples"])
def test_saturation_that_cannot_be_told_is_undecided_never_no_event(st, w0, w1, why):
    """Rd / Re: an unmeasured node, unanchored samples, or a stretch of the session window no sample covers, with
    no saturation seen elsewhere in it: (None, why), never [] (mutations: each check dropped turns its case RED)."""
    assert k6eval.saturation_events(st, w0, w1) == (None, why)


def test_the_anchors_error_widens_each_window_outward():
    """A window 0.05 s short of the session's start overlaps it once widened by the anchor's 0.1 s error."""
    got, _ = k6eval.saturation_events(_sat_st((0, 4.95, True), (4.95, 10, False), slack=0.1), 5, 9)
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
    # per thread: the single-threaded process's one thread, its tid the pid and its starttime the process's
    for r in rows:
        if r["kind"] == "renderer" and key in r["rd"]:
            assert r["th"][key] == {f"{fake.pid}:{start}": r["rd"][key]}, r


@pytest.mark.skipif(not __import__("os").path.isdir("/proc/self/task"), reason="needs Linux /proc")
def test_the_sampler_records_each_thread_by_tid_and_its_own_starttime_and_drops_an_exited_one(tmp_path):
    """A renderer-shaped process (a python3 named `chromium`, `--type=renderer` among its arguments, on one CPU with a
    CPU hog beside it) starts a thread 0.3 s in that exits 1.2 s later, and, BETWEEN the sampler's two samples, a
    thread that spins 0.4 s behind the hog (it waits for the CPU) and stays. Sampled while the first runs, "th" holds
    the main thread and it, under its own tid and its OWN starttime (not the process's); sampled after, the main
    thread and the new one, the exited one gone; "rd" is their sum each time. Over those two real samples the
    evaluator counts the new thread's WHOLE first value (its run_delay began at 0), with no bound for it, and bounds
    only the exited one by the gap (mutations: keying a thread by the process's starttime; counting a new thread
    from its first sample)."""
    import os
    import subprocess
    import sys
    import time

    from bench import k6_job
    cpu = sorted(os.sched_getaffinity(0))[-1]
    exe = tmp_path / "chromium"
    exe.symlink_to(os.path.realpath(sys.executable))
    code = (f"import os,threading,time\nos.sched_setaffinity(0, {{{cpu}}})\n"
            "def spin(s):\n e = time.monotonic() + s\n while time.monotonic() < e: pass\n"
            "t1 = threading.Thread(target=lambda: time.sleep(1.2)); time.sleep(0.3); t1.start()\n"
            "time.sleep(1.0)\n"
            "t2 = threading.Thread(target=lambda: (spin(0.4), time.sleep(60))); t2.start()\n"
            "while True: time.sleep(0.05)\n")
    hog = subprocess.Popen([sys.executable, "-c", f"import os,time\nos.sched_setaffinity(0, {{{cpu}}})\n"
                            "e = time.monotonic() + 4\nwhile time.monotonic() < e: pass\n"])
    fake = subprocess.Popen([str(exe), "-I", "-c", code, "--type=renderer", "--user-data-dir=/tmp/k6browser-data-t2"])
    try:
        time.sleep(0.8)
        with open(f"/proc/{fake.pid}/stat") as fh:
            start = fh.read().rsplit(") ", 1)[1].split()[19]
        tids = sorted(os.listdir(f"/proc/{fake.pid}/task"), key=int)
        assert len(tids) == 2, tids
        script = k6_job._renderer_sampler(1).replace("/out/", f"{tmp_path}/")
        p = subprocess.run(["sh", "-c", script + "rsample\nsleep 1.5\nrsample\n"], capture_output=True, text=True,
                           timeout=60)
        assert p.returncode == 0, p.stderr
        rows = [json.loads(x) for x in (tmp_path / "browser-renderers.jsonl").read_text().splitlines()]
    finally:
        fake.kill()
        hog.kill()
    key = f"{fake.pid}:{start}"
    samples = [r for r in rows if r["kind"] == "renderer" and key in r["rd"]]
    assert len(samples) == 2, rows
    first, second = samples[0]["th"][key], samples[1]["th"][key]
    main = f"{fake.pid}:{start}"
    old = [k for k in first if k != main]
    assert main in first and len(old) == 1, first
    tid, tstart = old[0].split(":")
    assert tid == tids[1] and int(tstart) > int(start), (old, start)      # its own starttime, 0.3 s later
    new = [k for k in second if k != main]
    assert main in second and len(new) == 1 and new[0] not in first, (first, second)   # the exited one gone
    assert second[new[0]] > 0, second                                     # it waited behind the hog
    assert all(s["rd"][key] == sum(s["th"][key].values()) for s in samples), samples
    snaps = [{tuple(k.split(":")): v for k, v in s["th"][key].items()} for s in samples]
    times = [s["t"] for s in samples]
    got, why = k6eval._thread_delta(times, snaps)
    assert why == "" and (got["started"], got["exited"]) == (1, 1), got
    assert got["ns"] == second[main] - first[main] + second[new[0]], (got, first, second)
    assert got["bound_ms"] == pytest.approx((times[1] - times[0]) * 1000), got


def _stat_line(pid: int, start: int) -> str:
    """A /proc/<pid>/stat line whose field 22 (starttime) is `start`, its comm holding a space and a paren."""
    return f"{pid} (chromium (x) y) " + " ".join(["S"] + ["0"] * 18 + [str(start)] + ["0"] * 10) + "\n"


@pytest.mark.skipif(__import__("shutil").which("awk") is None, reason="needs awk")
def test_the_sampler_counts_no_thread_whose_stat_it_could_not_read(tmp_path):
    """reviewer-1480's Q7: a thread whose stat could not be read (it exited between the listing and the read) but
    whose schedstat was is in neither "th" nor "rd": the sampler's own awk program on a tree of files shaped as
    /proc (mutation: counting every schedstat line, the stat guard dropped, turns this RED)."""
    import subprocess

    from bench import k6_job
    script = k6_job._renderer_sampler(1)
    prog = script.split("awk -v t=\"$up\" '", 1)[1].split("' \\\n", 1)[0]
    root = tmp_path / "proc"
    (root / "10" / "task" / "10").mkdir(parents=True)
    (root / "10" / "task" / "11").mkdir(parents=True)
    (root / "10" / "stat").write_text(_stat_line(10, 500))
    (root / "10" / "task" / "10" / "stat").write_text(_stat_line(10, 500))
    (root / "10" / "task" / "10" / "schedstat").write_text("1 777 3\n")
    (root / "10" / "task" / "11" / "schedstat").write_text("1 999 3\n")        # its stat could not be read
    files = [root / "10" / "stat", root / "10" / "task" / "10" / "stat", root / "10" / "task" / "10" / "schedstat",
             root / "10" / "task" / "11" / "schedstat"]
    p = subprocess.run(["awk", "-v", "t=1.5", prog, *map(str, files)], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert json.loads(p.stdout) == {"kind": "renderer", "t": 1.5, "rd": {"10:500": 777},
                                    "th": {"10:500": {"10:500": 777}}}, p.stdout


#: The kernel premise of a new thread's measure (reviewer-1480, #1529): a new task's schedstat run_delay starts at 0
#: (sched_info is zeroed at fork), not at its parent's. The parent thread first waits behind 3 CPU hogs on its one
#: CPU for 1 s (it accrues hundreds of ms of run_delay); then a new thread reads its own schedstat at once, spins
#: 0.5 s behind the hogs and reads it again. Runs as its own process (it pins itself to one CPU); prints one JSON line.
#: The Docker arm runs it in the python image too (a container: the same kernel, its own namespaces and cgroup).
PREMISE_SCRIPT = """
import json, os, subprocess, sys, threading, time
cpu = sorted(os.sched_getaffinity(0))[0]
os.sched_setaffinity(0, {cpu})
def rd():
    with open(f"/proc/self/task/{threading.get_native_id()}/schedstat") as f:
        return int(f.read().split()[1])
def spin(s):
    e = time.monotonic() + s
    while time.monotonic() < e:
        pass
hog = "import os, time\\nos.sched_setaffinity(0, {%d})\\ne = time.monotonic() + 4\\nwhile time.monotonic() < e: pass" % cpu
hogs = [subprocess.Popen([sys.executable, "-c", hog]) for _ in range(3)]
time.sleep(0.2)
spin(1.0)
parent = rd()
out = {}
def child():
    out["first"] = rd()
    out["first_after_s"] = time.monotonic() - t0
    spin(0.5)
    out["later"] = rd()
t0 = time.monotonic()
c = threading.Thread(target=child)
c.start()
c.join()
for h in hogs:
    h.kill()
print(json.dumps({"parent_ns": parent, "child_first_ns": out["first"], "child_first_after_s": out["first_after_s"],
                  "child_later_ns": out["later"]}))
"""


def check_premise(r: dict) -> None:
    """The premise, from PREMISE_SCRIPT's line: the parent waited (>= 100 ms of run_delay); the new thread's first
    value is no more than its own life so far (+2 ms for the clock reads) and under a tenth of the parent's: it did
    not start from the parent's; and it only grew while it waited."""
    assert r["parent_ns"] >= 100e6, r
    assert r["child_first_ns"] <= r["child_first_after_s"] * 1e9 + 2e6, r
    assert r["child_first_ns"] < r["parent_ns"] / 10, r
    assert r["child_later_ns"] > r["child_first_ns"], r


@pytest.mark.skipif(not __import__("os").path.isdir("/proc/self/task"), reason="needs Linux /proc")
def test_the_kernel_starts_a_new_threads_run_delay_at_zero():
    """The kernel premise _thread_delta counts a new thread's whole value on, proven on the running kernel (the
    build VM's; the Docker arm also runs PREMISE_SCRIPT in a container)."""
    import subprocess
    import sys
    p = subprocess.run([sys.executable, "-c", PREMISE_SCRIPT], capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    r = json.loads(p.stdout)
    print("PREMISE", json.dumps(r))
    check_premise(r)
