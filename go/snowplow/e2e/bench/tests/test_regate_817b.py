"""#817 re-gate at 21be306c (review818b): the probe polls, a passive tab does
not; the driver must count every rotation and every dead tab; close_all must
be final; a 200 with no refresh headers disarms; the hub's view_lock is
load-bearing. One arm per finding."""

from __future__ import annotations

import threading

from bench import load606
from tests.test_viewer_key_rotation_817 import PATH, _Client, _entry, _Pool, _RotatingCell, _Stream

COORDS = load606.refresh_coords(PATH, "restactions")


# ─── 1. the probe: arm generations, the passive control, bounded rotations ──


def _probe(cell, samples=6):
    recs = []
    _Stream.made = []
    clk = lambda: cell.clock[0]                                   # noqa: E731
    viewer = load606.ProbeViewer("http://s", "t", COORDS, cell.key(), recs.append,
                                 stream_factory=_Stream, clock=clk)
    viewer.control = load606.ProbeViewer("http://s", "t", COORDS, cell.key(), recs.append,
                                         stream_factory=_Stream, clock=clk, role="control")
    out = load606.freshness_probe(
        cell, "t", PATH, cell.mutate, samples=samples, poll_ms=0, gap_s=0, budget_s=30,
        sink=recs.append, push_since=viewer.push_since, viewer=viewer, clock=clk,
        sleep=lambda s: cell.clock.__setitem__(0, cell.clock[0] + s))
    return out, recs, viewer


def test_samples_carry_their_arm_generation_and_the_control_measures_819():
    cell = _RotatingCell(rotate_at=8)
    out, recs, _ = _probe(cell)
    assert [r["arm_gen"] for r in out] == [0, 0, 0, 1, 1, 1]
    assert [r["rearmed_during"] for r in out] == [False, False, True, False, False, False]
    # the polling probe is pushed throughout; the passive tab stops at the rotation
    assert all(r["push_ms"] is not None for r in out)
    assert [r["control_push_ms"] is not None for r in out] == [True, True, False,
                                                               False, False, False]
    rep = load606.push_report(out)[out[0]["label"]]
    assert rep["by_arm_gen"] == {"0": {"samples": 3, "pushed": 3},
                                 "1": {"samples": 3, "pushed": 3}}
    assert rep["rearmed_during_sample"] == 1
    assert rep["control"]["pushed"] == 2 and rep["control"]["no_push_after_rotation"] == 3


def test_the_exercised_gate_counts_only_what_a_passive_tab_got():
    cell = _RotatingCell(rotate_at=8)
    out, _, _ = _probe(cell)
    d = load606.client_delivery(out, [], [])
    assert (d["probe_push_samples"], d["probe_push_samples_polling"]) == (2, 6)
    # without a control: pushes on arm generation 0 only
    legacy = [{k: v for k, v in r.items() if k != "control_push_ms"} for r in out]
    assert load606.client_delivery(legacy, [], [])["probe_push_samples"] == 3


def test_each_rotation_is_bounded_by_the_last_poll_that_saw_the_old_key():
    cell = _RotatingCell(rotate_at=8)
    _, recs, _ = _probe(cell)
    (rot,) = [r for r in recs if r.get("state") == "key_rotated"]
    # polls advance the clock by 0.1 s: poll 7 saw the old key, poll 8 the new
    assert rot["role"] == "probe"
    assert 0 < rot["first_new_t"] - rot["last_old_t"] < 0.2
    rep = load606.fresh_viewer_report(recs)
    assert rep["key_rotations"] == [{"after": rot["last_old_t"], "by": rot["first_new_t"]}]


def test_probe_disarms_on_a_200_without_headers_and_rearms_when_they_return():
    recs, _Stream.made = [], []
    v = load606.ProbeViewer("http://s", "t", COORDS, "K1", recs.append, stream_factory=_Stream,
                            clock=lambda: 5.0)
    v.observe(load606.Response(500, b"", 1.0))                     # an error: unchanged
    assert v.key == "K1" and not _Stream.made[0].closed
    v.observe(load606.Response(200, b"{}", 1.0))                   # no headers: disarm
    assert v.key is None and v.stream is None and _Stream.made[0].closed
    v.observe(load606.Response(200, b"{}", 1.0, refresh=("K2", "restactions")))
    assert v.key == "K2" and len(_Stream.made) == 2
    assert [r["state"] for r in recs] == ["disarmed_no_headers", "rearmed"]


# ─── 2. the driver: rotations via navigation, dead tabs ────────────────────


def _hub(keys):
    _Stream.made = []
    pool = _Pool()
    return load606.ViewerHub(_Client(keys), pool, lambda r: None, stream_factory=_Stream,
                             clock=lambda: 100.0), pool


def test_a_navigation_that_carries_a_rotated_key_is_counted():
    a, b = _entry("a", "KA"), _entry("b", "KB")
    h, _ = _hub({a["path"]: "KA", b["path"]: "KB"})
    h.view("u1", "t", [a, b])
    h.view("u1", "t", [dict(a, key="KA2"), _entry("c", "KC")])     # shell widget a rotated
    assert h.stats["nav_key_rotations"] == 1 and "KA" in h.dead_keys


def test_a_tab_armed_on_a_rotated_away_key_is_counted_dead():
    a = _entry("a", "KA")
    h, pool = _hub({a["path"]: "KA2"})
    h.view("u1", "t", [a])
    h.view("u2", "t", [a])                                          # same identity class
    h.on_refresh("u1", "KA", 100.0)
    pool.drain()                                                    # u1 re-arms on KA2
    assert h.dead_tabs() == {"tabs_dead": 1, "tabs_armed": 2, "dead_keys_seen": 1}  # u2
    h.view("u2", "t", [dict(a, key="KA2")])                        # u2 navigates: healed
    assert h.dead_tabs()["tabs_dead"] == 0
    assert h.close_all()["tabs_dead_at_close"] == 0


def test_the_driver_samples_dead_tabs():
    from tests.test_run9_harness import _RefClient
    from tests.test_navmodel import PORTAL
    recs = []
    d = load606.Driver(_RefClient(), {"u1": "t"}, PORTAL, think_s=0.05, duration_s=0.3,
                       nav_workers=2, call_workers=8, sink=recs.append, viewers=True,
                       viewer_factory=lambda c, pool, sink, clock: load606.ViewerHub(
                           c, pool, sink, stream_factory=_Stream, clock=clock))
    d.run()
    dead = [r for r in recs if r["kind"] == "viewer_dead"]
    assert dead and set(dead[0]) >= {"tabs_dead", "tabs_armed", "dead_keys_seen"}
    stats = next(r for r in recs if r["kind"] == "viewer_stats")
    assert {"nav_key_rotations", "tabs_dead_at_close", "disarmed_no_headers"} <= set(stats)


# ─── 3. close_all is final ─────────────────────────────────────────────────


def test_a_refetch_that_lands_after_close_all_opens_no_stream():
    """review818b's reproduction: a frame-triggered refetch is in flight,
    close_all runs, the refetch answers with a rotated key."""
    a = _entry("a", "KA")
    h, pool = _hub({a["path"]: "KA2"})
    h.view("u1", "t", [a])
    h.on_refresh("u1", "KA", 100.0)                 # refetch queued, not run
    h.close_all()
    pool.drain()                                    # ...it answers now, rotated
    assert len(_Stream.made) == 1 and all(s.closed for s in _Stream.made)
    assert h.streams == {}


def test_a_navigation_that_lands_after_close_all_opens_no_stream():
    a = _entry("a", "KA")
    h, _ = _hub({a["path"]: "KA"})
    h.view("u1", "t", [a])
    h.close_all()
    h.view("u1", "t", [_entry("c", "KC")])          # a navigation still finishing
    assert len(_Stream.made) == 1 and all(s.closed for s in _Stream.made)
    assert h.streams == {}


# ─── 4. a 200 without refresh headers disarms the widget ───────────────────


def test_a_refetch_without_headers_disarms_the_widget_like_the_spa():
    a, b = _entry("a", "KA"), _entry("b", "KB")
    client = _Client({a["path"]: "KA", b["path"]: "KB"})
    client.get = lambda path, token: load606.Response(200, b"{}", 1.0)   # no headers
    _Stream.made = []
    pool = _Pool()
    h = load606.ViewerHub(client, pool, lambda r: None, stream_factory=_Stream,
                          clock=lambda: 100.0)
    h.view("u1", "t", [a, b])
    h.on_refresh("u1", "KA", 100.0)
    pool.drain()
    assert h.stats["disarmed_no_headers"] == 1
    assert h.paths["u1"] == {"KB": [b["path"]]}
    assert [c["name"] for c in _Stream.made[-1].coords] == ["b"]


def test_a_failed_refetch_without_headers_keeps_the_arm():
    a = _entry("a", "KA")
    client = _Client({a["path"]: "KA"})
    client.get = lambda path, token: load606.Response(503, b"", 1.0)
    _Stream.made = []
    pool = _Pool()
    h = load606.ViewerHub(client, pool, lambda r: None, stream_factory=_Stream,
                          clock=lambda: 100.0)
    h.view("u1", "t", [a])
    h.on_refresh("u1", "KA", 100.0)
    pool.drain()
    assert h.stats["disarmed_no_headers"] == 0 and len(_Stream.made) == 1


# ─── 5. view_lock is load-bearing ──────────────────────────────────────────


def test_a_rearm_racing_a_navigation_leaves_exactly_the_new_page_armed():
    """A refetch re-arms the OLD page; its stream start is slow. A navigation
    to a new page arrives meanwhile. Serialized (view_lock), the navigation
    runs after the re-arm and closes it: one open stream, the new page's.
    Without the lock the re-arm's stream lands last and the navigation's
    stream is never closed (two open streams, the old page's one current)."""
    a = _entry("a", "KA")
    started, release = threading.Event(), threading.Event()

    class Slow(_Stream):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            if any(c["name"] == "a" for c in self.coords) and len(_Stream.made) == 2:
                started.set()
                release.wait(5)                     # the re-arm's slow stream open

    _Stream.made = []
    pool = _Pool()
    h = load606.ViewerHub(_Client({a["path"]: "KA2"}), pool, lambda r: None,
                          stream_factory=Slow, clock=lambda: 100.0)
    h.view("u1", "t", [a])
    h.on_refresh("u1", "KA", 100.0)
    t_rearm = threading.Thread(target=pool.drain)
    t_rearm.start()
    assert started.wait(5)
    c = _entry("c", "KC")
    t_nav = threading.Thread(target=h.view, args=("u1", "t", [c]))
    t_nav.start()
    t_nav.join(0.3)                                 # without the lock it finishes now
    release.set()
    t_rearm.join(5)
    t_nav.join(5)
    live = [s for s in _Stream.made if not s.closed]
    assert len(live) == 1 and [x["name"] for x in live[0].coords] == ["c"]
    assert h.streams["u1"] is live[0]
