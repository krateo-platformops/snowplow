"""Run-9 harness (coordinator rulings after run 8, 2026-10-19):

- #763: once measure has started, ANY failure collects the run's own
  results (job logs, heap profiles, pod-after, snowplow logs) and computes a
  verdict BEFORE the teardown.
- #764: the bench opens /refreshes viewers like the SPA (refreshSse.ts):
  one stream per user over the shell + current page, refetch on `refresh`
  (5 s throttle with one trailing catch-up, <= 6 in flight), and fresh606
  arms its probe cell. Without a viewer, the #710/#740 checks are NOT
  EXERCISED (INCONCLUSIVE), never PASS.
- #765 (architect-623 spec): mem606 captures deps_dirty_mark_*, the
  refresher counters and fills by reason; the report gives marks per event.
"""

from __future__ import annotations

import base64
import http.server
import json
import subprocess
import threading
import time
import urllib.parse

import pytest

from bench import load606, load606_job
from tests.test_navmodel import INIT, PORTAL, Fake, P
from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import BENCH_DIR, TEARDOWN_TAIL, _calls, harness  # noqa: F401
from tests.test_run8 import DELIV, PODS, _mem, _navs, q234

K = load606.RUN8_KEYS
K9 = load606.RUN9_KEYS
RA = load606.call_path("templates.krateo.io", "v1", "restactions", "bench-fresh-listdep",
                       "bench-ns-01")


# ─── #763: collect on measure failure ──────────────────────────────────────


def test_run606_measure_failure_collects_everything_before_teardown(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", FAKE_WAIT_FAIL="1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    err = p.stderr
    assert p.returncode != 0
    assert "measure FAILED: fresh606 not Complete" in err
    i_collect = err.index("collecting partial results before teardown")
    i_diag = err.index("=== diagnostics before teardown")
    i_down = err.index("=== teardown ")
    assert i_collect < i_diag < i_down                 # results first, then snapshot, then delete
    out = tmp / "out"
    for f in ("nav-load606-0.jsonl", "fresh-fresh606-0.jsonl", "mem-mem606-0.jsonl",
              "pod-after.jsonl", "snowplow.log", "verdict.json"):
        assert (out / f).exists() and (out / f).stat().st_size > 0, f
    assert list((out / "heap").rglob("*.pb.gz"))      # heap profiles survived
    v = json.loads((out / "verdict.json").read_text())
    assert v["report"]["navs"] == 2                    # the driver's records made it
    gcl = [c["argv"][:3] for c in _calls(state) if c["tool"] == "gcloud"]
    assert gcl[-3:] == TEARDOWN_TAIL


def test_run606_failure_before_measure_does_not_try_to_collect(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_PREFLIGHT="refused", PREFLIGHT_POLL_S="0.1")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode != 0
    assert "collecting partial results" not in p.stderr
    assert not (tmp / "out" / "verdict.json").exists()


# ─── #764: the SPA's protocol ──────────────────────────────────────────────


def test_refresh_coords_follow_build_refresh_coords():
    c = load606.refresh_coords(RA, "restactions")
    assert c == {"class": "restactions", "group": "templates.krateo.io", "version": "v1",
                 "resource": "restactions", "namespace": "bench-ns-01",
                 "name": "bench-fresh-listdep"}
    w = load606.refresh_coords(
        "/call?apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&name=t"
        "&namespace=ns&page=2&perPage=5&extras=%7B%22compositionId%22%3A%22x%22%7D", "widgets")
    assert (w["page"], w["perPage"], w["extras"]) == (2, 5, {"compositionId": "x"})
    assert load606.refresh_coords("/list?category=builders&ns=x", "widgets") is None
    assert load606.refresh_coords(RA, "apistage") is None          # never armable
    assert load606.refresh_coords("/call?resource=x&name=y", "widgets") is None


def test_parse_sse_skips_keepalives_and_reads_named_refresh_events():
    lines = [": keepalive\n", "\n", "event: refresh\n", "data: K1\n", "\n",
             "event: refresh\n", "data:K2\n", "\n", "data: plain\n", "\n"]
    assert list(load606.parse_sse(lines)) == [("refresh", "K1"), ("refresh", "K2"),
                                             (None, "plain")]


class _SSE(http.server.BaseHTTPRequestHandler):
    seen: list = []
    post_status = 200

    def _stream(self, coords):
        _SSE.seen.append({"method": self.command, "auth": self.headers.get("Authorization"),
                          "coords": coords})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b": keepalive\n\nevent: refresh\ndata: K1\n\n")
        self.wfile.flush()
        time.sleep(3)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"[]")
        if _SSE.post_status != 200:
            self.send_response(_SSE.post_status)
            self.end_headers()
            return
        self._stream(body)

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        sub = q["sub"][0]
        coords = json.loads(base64.urlsafe_b64decode(sub + "=" * (-len(sub) % 4)))
        self._stream(coords)

    def log_message(self, *a):
        pass


@pytest.mark.parametrize("post_status,method", [(200, "POST"), (405, "GET"), (404, "GET")])
def test_refresh_stream_posts_coords_and_falls_back_to_get(post_status, method):
    _SSE.seen, _SSE.post_status = [], post_status
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SSE)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    got = []
    coords = [load606.refresh_coords(RA, "restactions")]
    s = load606.RefreshStream(f"http://127.0.0.1:{srv.server_address[1]}", "tok", coords,
                              lambda k, t: got.append(k)).start()
    deadline = time.time() + 5
    while not got and time.time() < deadline:
        time.sleep(0.02)
    s.close()
    srv.shutdown()
    assert got and got[0] == "K1"
    assert _SSE.seen[0]["method"] == method and _SSE.seen[0]["auth"] == "Bearer tok"
    assert _SSE.seen[0]["coords"] == coords


def test_refresh_stream_caps_coordinates_at_512():
    s = load606.RefreshStream("http://x", "t", [{"n": i} for i in range(600)], lambda k, t: None)
    assert len(s.coords) == 512 and s.truncated == 88


class _Stream:
    made: list = []

    def __init__(self, base, token, coords, on_refresh, on_state=None):
        self.coords, self.on_refresh, self.closed = coords, on_refresh, False
        _Stream.made.append(self)

    def start(self):
        return self

    def close(self):
        self.closed = True


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

    def __init__(self):
        self.calls = []

    def get(self, path, token):
        self.calls.append(path)
        return load606.Response(200, b"{}", 3.0)


def _entry(name, key):
    path = P("tables", name)
    return {"path": path, "key": key, "coords": load606.refresh_coords(path, "widgets")}


def test_viewer_hub_one_stream_per_user_rearmed_on_navigation():
    _Stream.made = []
    clock = [100.0]
    hub = load606.ViewerHub(_Client(), _Pool(), lambda r: None, stream_factory=_Stream,
                            clock=lambda: clock[0])
    hub.view("u1", "t", [_entry("a", "KA"), _entry("a", "KA"), _entry("b", "KB")])
    first = _Stream.made[-1]
    assert len(first.coords) == 2                         # duplicate widget armed once
    hub.view("u1", "t", [_entry("c", "KC")])              # route click: re-arm
    assert first.closed and len(_Stream.made) == 2 and _Stream.made[-1].coords[0]["name"] == "c"
    hub.view("u1", "t", [])                               # nothing armable: no stream
    assert _Stream.made[-2].closed and hub.streams == {}
    assert hub.close_all()["streams_opened"] == 2


def test_viewer_hub_refetch_throttle_trailing_catchup_and_inflight_cap():
    _Stream.made = []
    clock = [100.0]
    recs, client, pool = [], _Client(), _Pool()
    hub = load606.ViewerHub(client, pool, recs.append, stream_factory=_Stream,
                            clock=lambda: clock[0], max_inflight=2)
    hub.view("u1", "t", [_entry(n, f"K{n}") for n in "abc"])
    hub.on_refresh("u1", "Ka", 100.0)
    hub.on_refresh("u1", "Ka", 101.0)                    # inside the 5 s window: deferred
    hub.on_refresh("u1", "Kb", 101.0)
    hub.on_refresh("u1", "Kc", 101.0)                    # 3rd in flight: queued (cap 2)
    hub.on_refresh("u1", "Kzz", 101.0)                   # unmatched
    assert len(pool.jobs) == 2
    pool.drain()                                          # the queued one runs as a slot frees
    assert len(client.calls) == 3 and client.calls.count(P("tables", "a")) == 1
    clock[0] = 106.0
    hub.flush_trailing()                                  # the deferred frame is not lost
    pool.drain()
    assert client.calls.count(P("tables", "a")) == 2
    st = hub.close_all()
    assert (st["frames"], st["frames_unmatched"], st["deferred"], st["refetches"]) == (5, 1, 1, 4)
    r = [x for x in recs if x["kind"] == "push_refetch"]
    assert len(r) == 4 and all(x["status"] == 200 and len(x["key"]) <= 16 for x in r)


class _RefClient(Fake):
    """navmodel's Fake, with snowplow's refresh headers on every /call."""
    base = "http://snowplow"

    def get(self, path, token):
        r = super().get(path, token)
        if r.status == 200 and path.startswith("/call"):
            r.refresh = ("key:" + load606._route_slug(path), "widgets")
        return r


def test_driver_arms_shell_plus_current_page_after_each_navigation():
    _Stream.made = []
    recs = []
    d = load606.Driver(_RefClient(), {"u1": "t"}, PORTAL, think_s=0.05, duration_s=0.5,
                       nav_workers=2, call_workers=8, sink=recs.append, viewers=True,
                       viewer_factory=lambda c, pool, sink, clock: load606.ViewerHub(
                           c, pool, sink, stream_factory=_Stream, clock=clock))
    d.run()
    navs = [r for r in recs if r.get("kind") == "nav"]
    assert navs and navs[0]["nav"] == "first_load"
    first = _Stream.made[0]
    names = {c["name"] for c in first.coords}
    assert "app-shell" in names                           # the shell stays armed
    routes = [r for r in navs if r["nav"] == "route"]
    assert routes                                         # a route click happened…
    assert len(_Stream.made) == 1 + len(routes)           # …and each one re-armed
    assert all(s.closed for s in _Stream.made[:-1])       # one live stream per user
    second = {c["name"] for c in _Stream.made[1].coords}
    assert "app-shell" in second                          # the shell stays armed across clicks
    assert any(r.get("kind") == "viewer_stats" for r in recs)


def test_drive_without_viewers_opens_none():
    d = load606.Driver(_RefClient(), {"u1": "t"}, PORTAL, think_s=0.05, duration_s=0.2,
                       nav_workers=2, call_workers=8, sink=lambda r: None)
    d.run()
    assert d.viewers is None


def test_fresh_probe_records_push_ms_from_its_armed_viewer():
    t = [0.0]

    class C:
        n = 0

        def get(self, path, token):
            C.n += 1
            return load606.Response(200, b"fresh-x-0" if C.n > 3 else b"old", 1.0)

    def clock():
        return t[0]

    def sleep(s):
        t[0] += s

    recs = load606.freshness_probe(C(), "t", RA, lambda m: True, samples=1, poll_ms=100,
                                   gap_s=0, label="list_dep@10", clock=clock, sleep=sleep,
                                   push_since=lambda t0: t0 + 0.25)
    r = recs[0]
    assert r["armed"] and r["push_ms"] == 250.0
    rep = load606.push_report(recs + [{"kind": "fresh", "label": "list_dep@10", "armed": True,
                                       "push_ms": None}])
    assert rep["list_dep@10"]["pushed"] == 1 and rep["list_dep@10"]["no_push_within_budget"] == 1


def test_probe_without_refresh_headers_records_that_nothing_was_armed():
    recs = []
    ps, stream = load606.arm_probe_viewer(_Client(), "t", RA, recs.append)
    assert ps is None and stream is None
    assert recs[0]["kind"] == "fresh_viewer" and recs[0]["armed"] is False


def test_job_renders_viewers_on_by_default_and_off_on_request():
    kw = dict(users=4, shards=1, think_s=1, duration_s=1, snowplow_url="http://s",
              authn_url="http://a", password="pw", init=INIT, src="#")
    assert "--no-viewers" not in load606_job.render(**kw)
    assert "--no-viewers" in load606_job.render(viewers=False, **kw)
    fk = dict(snowplow_url="http://s", authn_url="http://a", user="u", password="pw",
              rates=["10"], samples=1, gap_s=1, probe_name="p", namespaces=2,
              comp_group="g", comp_version="v", comp_plural="x")
    assert "--no-viewer" not in load606_job.render_fresh(**fk)
    assert "--no-viewer" in load606_job.render_fresh(viewer=False, **fk)


# ─── #764: no viewer ⇒ NOT EXERCISED, never PASS ───────────────────────────


def _unexercised(**over):
    base = q234(**{K["retry_viewers"]: 0, K["stale_serve"]: {"known_stale": 0},
                   K["stale_to_push"]: {"buckets": {"le_3000": 0, "le_10000": 0, "le_inf": 0},
                                        "count": 0, "sum_ms": 0, "max_ms": 0}})
    base.update(over)
    rep, checks = load606.run8_report(_navs(), [_mem(150, **base)], [], PODS)
    return rep, {c["check"]: c for c in checks}


def test_no_viewer_and_no_stale_serve_makes_dependent_passes_inconclusive():
    rep, c = _unexercised()
    assert rep["exercised_710_740"]["exercised"] is False
    assert c["run8.710_740_exercised"]["result"] == "INCONCLUSIVE"
    for name in ("run8.stale_to_push.max", "run8.stale_push_unarmed_total",
                 "run8.stale_overdue_total", "run8.740.parked_vs_overdue"):
        assert c[name]["result"] == "INCONCLUSIVE" and "NOT EXERCISED" in c[name]["why"], name


_NO = {"fresh": [], "push_refetches": [], "viewer_stats": []}


@pytest.mark.parametrize("serves,deliv,missing", [
    # P1 (reviewer-small): stale serves, but no client ever received a frame
    (40, _NO, ["no client /refreshes stream", "no refresh frame",
               "no armed probe sample received a push"]),
    # P2: client delivery evidence, but snowplow never served stale
    (0, DELIV, ["no stale serve"]),
    # streams opened, frames = 0 (an SSE path that buffers)
    (40, {"fresh": DELIV["fresh"], "push_refetches": [],
          "viewer_stats": [{"kind": "viewer_stats", "streams_opened": 1000, "frames": 0}]},
     ["no refresh frame"]),
    # R2a: frames reached the driver but triggered no refetch
    (40, {"fresh": DELIV["fresh"], "push_refetches": [],
          "viewer_stats": [{"kind": "viewer_stats", "streams_opened": 1000, "frames": 7}]},
     ["no refresh frame"]),
    # frames reached the driver, but the armed probe never saw a push
    (40, {"fresh": [{"kind": "fresh", "label": "list_dep@10", "armed": True, "push_ms": None}],
          "push_refetches": DELIV["push_refetches"], "viewer_stats": DELIV["viewer_stats"]},
     ["no armed probe sample received a push"]),
])
def test_exercised_needs_a_stale_serve_and_client_observed_delivery(serves, deliv, missing):
    base = q234(**{K["stale_serve"]: {"known_stale": serves},
                   K["retry_viewers"]: 9})          # server-side viewers never count
    rep, checks = load606.run8_report(_navs(), [_mem(150, **base)], [], PODS, **deliv)
    c = {x["check"]: x for x in checks}
    ex = rep["exercised_710_740"]
    assert ex["exercised"] is False and c["run8.710_740_exercised"]["result"] == "INCONCLUSIVE"
    for m in missing:
        assert any(x.startswith(m) for x in ex["missing"]), (m, ex["missing"])
    assert c["run8.stale_overdue_total"]["result"] == "INCONCLUSIVE"
    assert c["run8.stale_push_unarmed_total"]["result"] == "INCONCLUSIVE"


def test_exercised_with_serves_and_full_client_delivery():
    rep, checks = load606.run8_report(_navs(), [_mem(150, **q234(**{K["retry_viewers"]: 0}))],
                                      [], PODS, **DELIV)
    ex = rep["exercised_710_740"]
    assert ex["exercised"] is True and ex["missing"] == []
    assert (ex["streams_opened"], ex["frames"], ex["push_refetches"], ex["probe_push_samples"]) \
        == (3, 5, 1, 1)
    assert {x["check"]: x for x in checks}["run8.710_740_exercised"]["result"] == "PASS"


def test_a_real_failure_stays_a_failure_when_unexercised():
    _, c = _unexercised(**{K["stale_overdue"]: 1})
    assert c["run8.stale_overdue_total"]["result"] == "FAIL"


def test_a_viewer_makes_the_checks_count():
    rep, checks = load606.run8_report(_navs(), [_mem(150, **q234())], [], PODS, **DELIV)
    c = {x["check"]: x for x in checks}
    assert rep["exercised_710_740"]["exercised"] is True
    assert c["run8.710_740_exercised"]["result"] == "PASS"
    assert c["run8.stale_overdue_total"]["result"] == "PASS"


# ─── #765: marks per event, refresher counters, fills by reason ────────────


def test_run9_marks_per_event_and_refresher_rates():
    a = q234(**{K9["dirty_mark"]: {"by_entry_class": {"apistage": 100, "widgets": 10},
                                   "events_total": 10, "submit_source": {"watch": 10}},
                K9["enqueue"]: 1000, K9["completed"]: 900, K9["floored"]: 5,
                K9["queue_depth"]: 300,
                K9["fills_by_reason"]: {"absent": 10, "known_stale": 0, "refresh_forced": 100}})
    b = q234(**{K9["dirty_mark"]: {"by_entry_class": {"apistage": 4800, "widgets": 210},
                                   "events_total": 110, "submit_source": {"watch": 110}},
                K9["enqueue"]: 1450, K9["completed"]: 1350, K9["floored"]: 9,
                K9["queue_depth"]: 700,
                K9["fills_by_reason"]: {"absent": 12, "known_stale": 1, "refresh_forced": 2100}})
    rep, _ = load606.run8_report(_navs(), [_mem(100, **a), _mem(200, **b)], [], PODS, **DELIV)
    r9 = rep["run9_refresh"]
    assert r9["dirty_marks"]["events_delta"] == 100
    assert r9["dirty_marks"]["marks_per_event"] == 49.0         # (4700 + 200) / 100
    assert r9["dirty_marks"]["marks_per_event_by_class"]["apistage"] == 47.0
    assert r9["refresher"]["enqueue"]["per_s"] == 4.5 and r9["refresher"]["completed"]["delta"] == 450
    assert r9["queue_depth"] == {"p50": 300, "p90": 700, "max": 700}
    assert r9["fills_by_reason_delta"] == {"absent": 2, "known_stale": 1, "refresh_forced": 2000}


@pytest.mark.parametrize("name", sorted(K9))
def test_every_run9_key_missing_is_not_exposed_and_fails(name):
    full = q234()
    del full[K9[name]]
    rep, checks = load606.run8_report(_navs(), [_mem(150, **full)], [], PODS, **DELIV)
    if name == "queue_depth":
        # the top-level sample field is the fallback; absent too here → FAIL
        pass
    bad = [c for c in checks if c["result"] == "FAIL" and c.get("key") == K9[name]]
    assert bad and all("not exposed" in c["why"] for c in bad), name


def test_run9_keys_are_served_by_1_12_50_and_captured_by_default():
    from tests.test_run8_digest_q234 import FIXTURE
    inv = json.loads(FIXTURE.read_text())["keys"]
    assert set(K9.values()) <= set(inv)
    assert {"by_entry_class", "events_total"} <= set(inv[K9["dirty_mark"]])
    assert set(inv[K9["fills_by_reason"]]) == {"absent", "known_stale", "refresh_forced"}
    assert load606.prefixes_missing(load606.RUN8_VARS_PREFIXES) == []
    assert K9["dirty_mark"] in load606.prefixes_missing(
        [p for p in load606.RUN8_VARS_PREFIXES if p != "snowplow_deps_dirty_mark_"])


def test_run606_refuses_prefixes_that_miss_a_run9_key(harness):
    env, state, ambient, tmp = harness
    env["MEM_VARS_PREFIXES"] = " ".join(p for p in load606.RUN8_VARS_PREFIXES
                                        if p != "snowplow_deps_dirty_mark_")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 2 and "snowplow_deps_dirty_mark_attribution" in p.stderr
    assert not [c for c in _calls(state) if c["tool"] in ("gcloud", "kubectl", "helm")]


def test_run606_viewers_off_reaches_the_job_manifest(harness):
    env, state, ambient, tmp = harness
    env = _env483(env)
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", VIEWERS="0")
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, (p.stdout + p.stderr)[-2000:]
    job = (tmp / "out" / "loadgen-job.yaml").read_text()
    assert "--no-viewers" in job and "--no-viewer " in job
