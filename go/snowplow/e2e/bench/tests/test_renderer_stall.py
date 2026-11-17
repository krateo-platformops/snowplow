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


def _uptime(epoch: float) -> float:
    return epoch - (T_POINTS - 15) + UPTIME0


def _case(tmp_path, *, renderer=True, stalled=(), map_after=True, udd_of=None, stall=None):
    """Warm loads at 600 ms (over the 500 ms bar) for 3 sessions, inside 20 s of browser-container stall (every
    window stalled 5 s: the container rule explains the FAIL away). `renderer`: the run also recorded per-renderer
    evidence: each session's renderer (PIDS) read by renderers.js before and (`map_after`) after its loads, and
    sampled every second; the renderers of `stalled` sessions waited for a CPU all through their loads, the others
    none (an idle page's renderer)."""
    k6dir, bdir = tmp_path / "k6", tmp_path / "browser"
    _write_out(k6dir, "k6", _k6_points(20), _k6_summary(20), 0)
    _write_viewer(k6dir)
    pts = _browser_points(sessions=SESSIONS, base_ms=600.0)
    for p in pts:
        if p["metric"].startswith("browser_page_load") or p["metric"] == "browser_page_loads":
            p["data"]["tags"]["class"] = "0"
    _write_out(bdir, "browser", pts, _browser_summary(63), 0)
    _cpu_file(bdir, "browser", 3000, [1.0] * 20, stall=stall if stall is not None else [5.0] * 20)
    if renderer:
        loads = [p["data"]["tags"] for p in pts if p["metric"] == "browser_page_load_ms"]
        first, last = min(int(t["t0"]) for t in loads) / 1000, max(int(t["t0"]) for t in loads) / 1000 + 1
        udd = {s: f"/tmp/k6browser-data-{s}" for s in SESSIONS}
        rows = [{"kind": "renderer_udd", "pid": PIDS[s], "udd": (udd_of or udd)[s]} for s in SESSIONS]
        for i in range(60):                                   # every second from T_POINTS - 15
            t = T_POINTS - 15 + i
            # a stalled renderer waits for a CPU all the time its loads run (1 s of run_delay per second)
            rows.append({"kind": "renderer", "t": _uptime(t), "rd": {
                str(PIDS[s]): (int(1e9 * min(max(0.0, t - (first - 1)), last + 1 - (first - 1)))
                               if s in stalled else 0) + 1000 for s in SESSIONS}})
        (bdir / "browser-renderers.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        maps = []
        for s in SESSIONS:
            maps.append({"kind": "renderer_map", "t0": first - 6, "t1": first - 5.9, "udd": udd[s], "target": "T" + s,
                         "session": s, "class": "0", "pid": PIDS[s]})
            if map_after:
                maps.append({"kind": "renderer_map", "t0": last + 2, "t1": last + 2.1, "udd": udd[s],
                             "target": "T" + s, "session": s, "class": "0", "pid": PIDS[s]})
        (bdir / "renderer-map.jsonl").write_text("".join(json.dumps(m) + "\n" for m in maps))
    return _evaluate_cli(tmp_path, k6dir, bdir)


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


def test_a_renderer_that_changed_while_the_load_ran_is_not_known():
    st = {"slack_s": 0.0, "renderer": {"anchored": True, "recorded": {"renderers": True, "renderer_map": True},
                                       "udd": {7: "u", 8: "u"}, "samples": [(0.0, {7: 0, 8: 0}), (10.0, {7: 5, 8: 5})],
                                       "maps": [{"t0": 1, "t1": 1.1, "session": "s", "class": "0", "pid": 7, "udd": "u",
                                                 "target": "T"},
                                                {"t0": 3, "t1": 3.1, "session": "s", "class": "0", "pid": 8, "udd": "u",
                                                 "target": "T"},
                                                {"t0": 8, "t1": 8.1, "session": "s", "class": "0", "pid": 8, "udd": "u",
                                                 "target": "T"}]}}
    n = {"session": "s", "class": "0", "start": 2.0, "wall_ms": 500.0}
    assert k6eval.renderer_delay_ms(n, st) == (None, "its renderer changed while it loaded")
    assert k6eval.renderer_delay_ms(dict(n, start=4.0), st)[0] == pytest.approx(5 / 1e6)


# ─── real Chrome: the frame -> renderer mapping, from the rendered browser container ─────────────────────────

@needs_docker
def test_in_chrome_each_sessions_page_maps_to_a_renderer_of_its_own_chrome(tmp_path):
    """The rendered k6browser container against the fixture SPA, 3 sessions for 22 s: renderers.js reads, every
    MAP_S, which renderer renders each session's page; the sampler records each renderer's run_delay and its
    Chrome's --user-data-dir. Exact: every read names a pid the sampler saw under the --user-data-dir of the very
    Chrome whose endpoint named it, each session its own pid, the same pid in every read of a class; and loads
    between two reads are charged that renderer's measured stall (no load of a read class is UNKNOWN)."""
    p, out = _browser_pod_in_docker(tmp_path, origin_delay_s=0, duration_s=22)
    assert p.returncode == 0, p.stderr[-3000:]
    watch = json.loads((out / "renderer-watch.json").read_text())
    assert watch["browsers"] == 3 and watch["of"] == 3, watch
    o = k6eval.read_k6_out(str(out), "browser")
    udd = {r["pid"]: r["udd"] for r in o["renderers"] if r["kind"] == "renderer_udd"}
    samples = [r for r in o["renderers"] if r["kind"] == "renderer"]
    reads = [m for m in o["renderer_map"] if m.get("kind") == "renderer_map" and m.get("pid") is not None]
    assert samples and reads, (o["renderers"][:5], o["renderer_map"][:5])
    for m in reads:
        assert udd.get(m["pid"]) == m["udd"], (m, udd)          # a renderer of the Chrome that named it
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
    known = [(n, d) for n, (d, _) in zip(navs, placed) if d is not None]
    assert {n["session"] for n, _ in known} == {"s1", "s2", "s3"}, [w for _, w in placed]
    assert all(0 <= d <= n["wall_ms"] for n, d in known)
    # the frames' record is Chrome's: its processId is a pid this container runs as --type=renderer
    assert set().union(*by_key.values()) <= set(udd)
