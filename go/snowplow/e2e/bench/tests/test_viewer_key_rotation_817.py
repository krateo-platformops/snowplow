"""#817 (b): run 9's annotation probe armed its viewer once (06:13:47Z) and
got pushes for samples 0 and 3 only; for the next 95 min the stream stayed
open (no eof / error, so keepalives kept arriving inside the 90 s read
timeout) yet snowplow never routed the cell to the viewed tier
(viewed_enqueue +0 in annotation@50 while the cell was still dirty-marked and
refreshed in the background: bench-fresh-listdep fills +10). The status
probe, armed at 07:51Z, was viewed for every sample.

The armed key and the cell's key had parted: a restactions cell's key folds
the requester's identity class (BindingUID, binding-set digest, RBAC
sub-generation) and /refreshes freezes its armed set at subscribe time. The
SPA follows a rotated key: every /call response's X-Snowplow-Refresh-Key goes
through recordRefreshHeaders and a changed key re-fires the arm hook. The
harness armed once and never looked again. These arms pin the SPA behaviour
in the probe and in the driver's viewers.
"""

from __future__ import annotations

import pytest

from bench import load606

PATH = load606.call_path("templates.krateo.io", "v1", "restactions",
                         load606.FRESH_RA_NAME, "bench-ns-01")


class _Stream:
    made: list = []

    def __init__(self, base, token, coords, on_refresh, on_state=None):
        self.coords, self.on_refresh, self.closed = coords, on_refresh, False
        _Stream.made.append(self)

    def start(self):
        return self

    def close(self):
        self.closed = True


class _RotatingCell:
    """The probe cell: its key rotates after `rotate_at` GETs; a mutate shows
    up `lag` GETs later and pushes a frame on the CURRENT key (snowplow only
    publishes to subscribers of the key it stores the cell under)."""

    def __init__(self, rotate_at, lag=3):
        self.n, self.rotate_at, self.lag = 0, rotate_at, lag
        self.marker, self.since = None, None
        self.clock = [1000.0]

    def key(self):
        return "K-old-0000000000000" if self.n < self.rotate_at else "K-new-1111111111111"

    def mutate(self, m):
        self.marker, self.since = m, self.n
        self.clock[0] += 0.01               # the write takes time: t0 is after the last frame
        return True

    def get(self, path, token):
        self.n += 1
        self.clock[0] += 0.1
        body = b"old"
        if self.marker and self.n - self.since >= self.lag:
            body = self.marker.encode()
            for s in _Stream.made:                     # the publish, on the cell's key
                if not s.closed:
                    s.on_refresh(self.key(), self.clock[0])
        return load606.Response(200, body, 1.0, refresh=(self.key(), "restactions"))


def _probe(cell, observe, samples=6):
    recs = []
    _Stream.made = []
    viewer = load606.ProbeViewer("http://s", "t", load606.refresh_coords(PATH, "restactions"),
                                 cell.key(), recs.append, stream_factory=_Stream,
                                 clock=lambda: cell.clock[0])
    out = load606.freshness_probe(
        cell, "t", PATH, cell.mutate, samples=samples, poll_ms=0, gap_s=0, budget_s=30,
        sink=recs.append, push_since=viewer.push_since,
        observe=viewer.observe if observe else None,
        clock=lambda: cell.clock[0], sleep=lambda s: cell.clock.__setitem__(0, cell.clock[0] + s))
    return out, recs, viewer


def test_probe_rearms_when_the_cells_key_rotates_and_keeps_its_pushes():
    cell = _RotatingCell(rotate_at=8)
    out, recs, viewer = _probe(cell, observe=True)
    assert all(r["converged"] for r in out)
    assert all(r["push_ms"] is not None for r in out), [r["push_ms"] for r in out]
    rot = [r for r in recs if r.get("state") == "key_rotated"]
    assert [(r["old_key"], r["new_key"]) for r in rot] == [("K-old-0000000000", "K-new-1111111111")]
    assert len(_Stream.made) == 2 and _Stream.made[0].closed and not _Stream.made[1].closed
    viewer.close()
    assert _Stream.made[1].closed


def test_without_following_the_key_the_probe_loses_its_viewer_like_run9():
    """The run-9 behaviour (arm once): after the rotation no sample is pushed,
    while every sample still converges by polling."""
    cell = _RotatingCell(rotate_at=8)
    out, recs, _ = _probe(cell, observe=False)
    pushed = [r["push_ms"] is not None for r in out]
    assert pushed[0] and not any(pushed[2:])
    assert all(r["converged"] for r in out)
    assert not [r for r in recs if r.get("state") == "key_rotated"]


def test_fresh_phases_feed_every_poll_to_the_armed_viewer():
    seen = []

    def probe(client, token, path, mutate, **kw):
        seen.append(kw["observe"])

    class V:
        def observe(self, r):
            pass

        def close(self):
            pass
    v = V()
    load606.fresh_phases(None, "t", lambda r: None, modes=["annotation"], rates=["0"],
                         mutators={"annotation": lambda *a: True}, paths={"annotation": "/a"},
                         targets=[], probe=("n", "p"), samples=1, poll_ms=100, gap_s=0,
                         viewer=True, push_since=lambda t: None, stream=v, probe_fn=probe,
                         healthy=lambda: True)
    assert seen == [v.observe]


# ─── the driver's viewers ───────────────────────────────────────────────────


class _Pool:
    def __init__(self):
        self.jobs = []

    def submit(self, fn, *a):
        self.jobs.append((fn, a))

    def drain(self):
        while self.jobs:
            fn, a = self.jobs.pop(0)
            fn(*a)


class _Client:
    base = "http://snowplow"

    def __init__(self, keys):
        self.keys = keys            # path -> the key snowplow answers with now

    def get(self, path, token):
        return load606.Response(200, b"{}", 3.0, refresh=(self.keys[path], "widgets"))


def _entry(name, key):
    path = f"/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1&name={name}" \
           f"&namespace=krateo-system&resource=tables"
    return {"path": path, "key": key, "coords": load606.refresh_coords(path, "widgets")}


@pytest.fixture
def hub():
    _Stream.made = []
    a, b = _entry("a", "KA"), _entry("b", "KB")
    client = _Client({a["path"]: "KA2", b["path"]: "KB"})     # a's key has rotated
    pool = _Pool()
    h = load606.ViewerHub(client, pool, lambda r: None, stream_factory=_Stream,
                          clock=lambda: 100.0)
    h.view("u1", "t", [a, b])
    return h, pool, a, b


def test_viewer_rearms_when_a_refetch_answers_with_a_rotated_key(hub):
    h, pool, a, b = hub
    h.on_refresh("u1", "KA", 100.0)
    pool.drain()
    assert h.stats["rearmed_key_changed"] == 1
    assert len(_Stream.made) == 2 and _Stream.made[0].closed
    assert h.paths["u1"] == {"KA2": [a["path"]], "KB": [b["path"]]}
    h.on_refresh("u1", "KA2", 101.0)                    # the rotated key now routes
    assert h.stats["frames_unmatched"] == 0


def test_viewer_does_not_rearm_a_page_it_navigated_away_from(hub):
    h, pool, a, b = hub
    h.on_refresh("u1", "KA", 100.0)                     # refetch queued...
    c = _entry("c", "KC")
    h.view("u1", "t", [c])                              # ...the user navigates first
    pool.drain()
    assert h.stats["rearmed_key_changed"] == 0
    assert h.paths["u1"] == {"KC": [c["path"]]}


def test_verdict_reports_the_probe_viewers_key_rotations():
    fv = [{"kind": "fresh_viewer", "armed": True, "class": "restactions"},
          {"kind": "fresh_viewer", "state": "open", "t": 1.0},
          {"kind": "fresh_viewer", "state": "key_rotated", "t": 7.0,
           "old_key": "a", "new_key": "b"},
          {"kind": "fresh_viewer", "state": "open", "t": 7.1}]
    rep = load606.evaluate([], [], [], fresh_viewers=fv)["report"]["fresh_viewer"]
    assert rep == {"arms": 1, "states": {"key_rotated": 1, "open": 2}, "key_rotations_t": [7.0]}


def test_unchanged_key_does_not_rearm(hub):
    h, pool, a, b = hub
    h.on_refresh("u1", "KB", 100.0)
    pool.drain()
    assert h.stats["rearmed_key_changed"] == 0 and len(_Stream.made) == 1
