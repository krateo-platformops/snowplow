"""#606 production-scale load driver, freshness probe, expvar sampler, verdict.

STDLIB ONLY and no `bench.*` imports: the file is shipped as a ConfigMap into
a python:3 pod on the bench cluster's `loadgen` pool, so 1000 virtual users
never share a node (or a laptop uplink) with what they measure.

What it does
------------
drive    N virtual users replay the portal SPA over HTTP: GET a page widget,
         follow every GET+allowed `status.resourcesRefs.items[].path`, level
         by level, `per_user_parallel` at a time (a browser's per-host limit),
         until the tree is exhausted. One NAVIGATION = one page walk; its wall
         time is first request start → last response end. Users think for an
         exponential delay between navigations (open loop: the schedule does
         not slow down when snowplow does — scheduling lag is recorded and a
         lagging driver INVALIDATES the run rather than flattering it).
fresh    Mutate a probe object (caller-supplied `mutate(marker)`, kubectl in
         practice) and poll a /call path every `poll_ms` until the decoded body
         contains the marker. Latency = patch-returned → marker visible.
churn    Background event generator: `rate` mutations/s spread over many
         compositions, the independent variable of the #603 fan-out law.
sample   GET /debug/vars every `interval` s, keep the refresh-economics
         counters (refresher, dirty-mark attribution, resolved cache).
evaluate Turn the JSONL outputs into the north-star verdict (see THRESHOLDS).

Honesty rules baked in (see feedback memories cited in the run plan):
  - the READ denominator is the driver's own request count, never a server
    counter that correlates with reads (#552 contaminates hit_total);
  - a counter that is absent reads as None and fails the verdict closed;
  - a driver that could not keep its schedule marks the run INVALID.
"""

from __future__ import annotations

import argparse
import base64
import calendar
import concurrent.futures
import gzip
import json
import math
import os
import random
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, asdict
from typing import Callable, Iterable

__all__ = [
    "THRESHOLDS",
    "HttpClient",
    "extract_refs",
    "walk_page",
    "Driver",
    "freshness_probe",
    "ChurnDriver",
    "summarize_vars",
    "economics",
    "pct",
    "evaluate",
    "list_dep_fixture_yaml",
    "call_path",
    "main",
]

# North-star bars (project_north_star: 1s cold / 500ms warm / 1s fresh; #506
# ruling: zero cold navigations is a REQUIREMENT). The driver's own validity
# bars are below them.
# The synthetic scale users' fixed TEST password (throwaway users on a
# throwaway cluster). Single definition: lifecycle._scaleuser_password() and
# run606.sh read it from here, and run606.sh redacts it from every log line.
SCALEUSER_PASSWORD = "ScaleTest2026!"

THRESHOLDS = {
    "warm_nav_p95_ms": 500,
    "cold_nav_p95_ms": 1000,
    "fresh_p95_ms": 1000,
    # Zero cold navigations, measured from the client because snowplow sends
    # no per-response cache-outcome signal: every FIRST navigation of a
    # (user, page) must itself meet the WARM bar, and no navigation at all may
    # exceed the cold bar.
    "first_nav_p95_ms_for_zero_cold": 500,
    "max_nav_ms_for_zero_cold": 1000,
    # Driver validity.
    "max_sched_lag_p95_ms": 1000,
    "max_error_rate": 0.01,
    "min_fresh_samples": 30,
    "min_warm_navs": 200,
}


def pct(values: Iterable[float], p: float) -> float | None:
    """Nearest-rank percentile; None on empty input."""
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None
    k = max(0, min(len(xs) - 1, math.ceil(p / 100.0 * len(xs)) - 1))
    return xs[k]


# ─── HTTP ───────────────────────────────────────────────────────────────────


def _decode(body: bytes, headers) -> bytes:
    enc = (headers.get("Content-Encoding") or "").lower() if headers else ""
    if enc == "gzip" or body[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(body)
        except OSError:
            return body
    return body


@dataclass
class Response:
    status: int
    body: bytes
    ms: float
    error: str = ""
    # (X-Snowplow-Refresh-Key, X-Snowplow-Refresh-Class) when snowplow keyed
    # the /call response — what the SPA arms its /refreshes stream with.
    refresh: tuple | None = None


REFRESH_HEADER_KEY = "X-Snowplow-Refresh-Key"
REFRESH_HEADER_CLASS = "X-Snowplow-Refresh-Class"
REFRESH_CLASSES = ("widgets", "widgetContent", "restactions")


def _refresh_headers(headers) -> tuple | None:
    key = headers.get(REFRESH_HEADER_KEY) if headers else None
    cls = headers.get(REFRESH_HEADER_CLASS) if headers else None
    return (key, cls) if key and cls in REFRESH_CLASSES else None


class HttpClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, token: str | None) -> Response:
        headers = {"Accept-Encoding": "gzip"}
        if token:
            headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(self.base + path, headers=headers)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                raw = r.read()
                return Response(r.status, _decode(raw, r.headers),
                                (time.perf_counter() - t0) * 1000,
                                refresh=_refresh_headers(r.headers))
        except urllib.error.HTTPError as e:
            return Response(e.code, b"", (time.perf_counter() - t0) * 1000,
                            f"http {e.code}")
        except Exception as e:  # noqa: BLE001 — every failure is a sample
            return Response(0, b"", (time.perf_counter() - t0) * 1000,
                            f"{type(e).__name__}: {e}")


# ─── /refreshes viewers (#764) ──────────────────────────────────────────────
#
# The portal arms ONE multiplexed /refreshes stream per tab over every widget
# it rendered, and refetches a widget's /call when a `refresh` frame carries
# its key (frontend ui/src/hooks/refreshSse.ts; snowplow
# docs/howto-frontend-live-refresh-sse.md). Without it, snowplow's #710
# stale-then-push path and #740 viewer-gated retries have no viewer and never
# run (run 8: stale_retry_viewers 0 in all 351 samples). The bench mirrors the
# SPA's protocol, not its own:
#   - coordinates from the widget's own /call query + the verbatim
#     X-Snowplow-Refresh-Class (refreshSse.ts buildRefreshCoords);
#   - POST /refreshes with the JSON coordinate array, Bearer auth; GET
#     ?sub=<base64url> when the server answers 404/405 (refreshSse.ts open());
#   - <= 512 coordinates per stream (MAX_WIDGETS);
#   - on `event: refresh`, refetch the matching widget(s): leading edge, one
#     trailing catch-up per 5 s window (REFRESH_THROTTLE_MS), at most 6
#     refetches in flight per tab (MAX_INFLIGHT_REFETCH);
#   - reconnect with capped, jittered backoff 1 s .. 30 s.
# SIZING: one stream (one reader thread) per virtual user = 1000 concurrent
# streams for the 1000-user run, i.e. one tab per user, the production shape.
# Threads are created with a 512 KiB stack (VIEWER_STACK_BYTES): ~1000
# threads fit easily in the driver pod (3 CPU / 4Gi); they are idle in a
# socket read between frames (keepalive every 20 s).

REFRESH_MAX_COORDS = 512
REFRESH_THROTTLE_S = 5.0
REFRESH_MAX_INFLIGHT = 6
REFRESH_BACKOFF_BASE_S = 1.0
REFRESH_BACKOFF_MAX_S = 30.0
VIEWER_STACK_BYTES = 512 * 1024
DEAD_TAB_SAMPLE_S = 60.0     # viewer_dead: tabs armed on a rotated-away key (#817)


def refresh_coords(path: str, cls: str) -> dict | None:
    """A widget's subscription coordinates from the /call query it was fetched
    with (refreshSse.ts buildRefreshCoords). None when they can't form one."""
    if cls not in REFRESH_CLASSES or not path.startswith("/call?"):
        return None
    q = urllib.parse.parse_qs(path.split("?", 1)[1])
    api = (q.get("apiVersion") or [""])[0]
    resource = (q.get("resource") or [""])[0]
    name = (q.get("name") or [""])[0]
    if not api or not resource or not name:
        return None
    group, _, version = api.rpartition("/")
    coords = {"class": cls, "group": group, "version": version, "resource": resource,
              "namespace": (q.get("namespace") or [""])[0], "name": name}
    for k in ("page", "perPage"):
        v = (q.get(k) or [None])[0]
        if v is not None:
            try:
                coords[k] = int(v)
            except ValueError:
                pass
    extras = (q.get("extras") or [None])[0]
    if extras:
        try:
            coords["extras"] = json.loads(extras)
        except ValueError:
            pass
    return coords


def _b64url(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def parse_sse(lines: Iterable[str]):
    """Yield (event, data) per SSE block; comments (`: keepalive`) skipped."""
    event, data = None, []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if event is not None or data:
                yield event, "\n".join(data) if data else None
            event, data = None, []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:][1:] if line[5:6] == " " else line[5:])


class RefreshStream:
    """One tab's /refreshes stream: a reader thread that calls
    on_refresh(key, t) per `refresh` frame, reconnecting until close()."""

    def __init__(self, base_url: str, token: str, coords: list[dict],
                 on_refresh: Callable[[str, float], None], *,
                 on_state: Callable[[str, dict], None] | None = None,
                 clock: Callable[[], float] = time.time):
        self.base = base_url.rstrip("/")
        self.token = token
        self.coords = coords[:REFRESH_MAX_COORDS]
        self.truncated = max(0, len(coords) - REFRESH_MAX_COORDS)
        self.on_refresh = on_refresh
        self.on_state = on_state or (lambda s, d: None)
        self.clock = clock
        self.closed = threading.Event()
        self._resp = None
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> "RefreshStream":
        self.thread.start()
        return self

    def _open(self):
        hdrs = {"Authorization": "Bearer " + self.token, "Accept": "text/event-stream"}
        body = json.dumps(self.coords).encode()
        req = urllib.request.Request(self.base + "/refreshes", data=body, method="POST",
                                     headers=dict(hdrs, **{"Content-Type": "application/json"}))
        try:
            return urllib.request.urlopen(req, timeout=90)
        except urllib.error.HTTPError as e:
            if e.code not in (404, 405):
                raise
        sub = _b64url(json.dumps(self.coords))
        return urllib.request.urlopen(
            urllib.request.Request(f"{self.base}/refreshes?sub={sub}", headers=hdrs), timeout=90)

    def _loop(self):
        attempt = 0
        while not self.closed.is_set():
            try:
                resp = self._open()
                with self._lock:
                    if self.closed.is_set():
                        resp.close()
                        return
                    self._resp = resp
                attempt = 0
                self.on_state("open", {"coords": len(self.coords), "truncated": self.truncated})
                lines = (b.decode("utf-8", "replace") for b in resp)
                for event, data in parse_sse(lines):
                    if event == "refresh" and data:
                        self.on_refresh(data, self.clock())
                if self.closed.is_set():
                    return
                self.on_state("eof", {})
            except Exception as e:  # noqa: BLE001 — a dropped stream reconnects
                if self.closed.is_set():
                    return
                self.on_state("error", {"error": f"{type(e).__name__}: {e}"[:200]})
            attempt += 1
            delay = min(REFRESH_BACKOFF_MAX_S, REFRESH_BACKOFF_BASE_S * 2 ** (attempt - 1))
            self.closed.wait(delay * random.uniform(0.75, 1.25))

    def close(self):
        self.closed.set()
        with self._lock:
            r, self._resp = self._resp, None
        if r is not None:
            abort_response(r)


def abort_response(resp) -> None:
    """Close a response that another thread is blocked reading, without
    waiting for it. HTTPResponse.close() takes the reader's buffer lock, so it
    returns only when the reader wakes: on the next frame or the 90 s read
    timeout (run 9: closing 1000 viewers one by one took 31 min, #817).
    Shutting the socket down first wakes the reader at once."""
    try:
        s = socket.socket(fileno=os.dup(resp.fileno()))
    except (OSError, ValueError, AttributeError):   # already closed
        s = None
    if s is not None:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            s.close()
    try:
        resp.close()
    except Exception:  # noqa: BLE001
        pass


class ViewerHub:
    """Every virtual user's open tab: the widgets of the shell plus the
    current page are armed on that user's single stream; a `refresh` frame
    refetches the widget like the SPA does (throttled, capped in flight) and
    records one `push_refetch` sample."""

    def __init__(self, client: HttpClient, pool: concurrent.futures.Executor,
                 sink: Callable[[dict], None], *,
                 stream_factory=RefreshStream,
                 throttle_s: float = REFRESH_THROTTLE_S,
                 max_inflight: int = REFRESH_MAX_INFLIGHT,
                 clock: Callable[[], float] = time.time,
                 on_open: Callable[[str, float], None] | None = None):
        self.client = client
        self.pool = pool
        self.sink = sink
        # on_open(user, t): the user's stream is open = the tab is armed
        # (#830: view-feed measures the arm lag from it)
        self.on_open = on_open
        self.stream_factory = stream_factory
        self.throttle_s = throttle_s
        self.max_inflight = max_inflight
        self.clock = clock
        self.lock = threading.Lock()
        # Serializes re-arms: a navigation's view and a refetch's key re-arm
        # must not interleave (the later one would arm a stale page).
        self.view_lock = threading.Lock()
        self.streams: dict[str, RefreshStream] = {}
        self.paths: dict[str, dict[str, list[str]]] = {}     # user -> key -> paths
        self.entries: dict[str, list[dict]] = {}              # user -> armed entries
        self.tokens: dict[str, str] = {}
        self.window: dict[tuple, dict] = {}                   # (user, path) -> {until, pending}
        self.inflight: dict[str, int] = {}
        self.queue: dict[str, list] = {}
        self.stats = {"streams_opened": 0, "frames": 0, "frames_unmatched": 0,
                      "refetches": 0, "deferred": 0, "rearmed_key_changed": 0,
                      "nav_key_rotations": 0, "disarmed_no_headers": 0}
        # Keys seen to rotate away (#817 re-gate): a key string names one
        # cell for one identity class, so a tab still armed on one gets no
        # frame for that widget again — a dead tab, whoever saw the rotation.
        self.dead_keys: set[str] = set()
        self.closed = False

    def view(self, user: str, token: str, entries: list[dict]) -> None:
        """The user's tab now shows exactly `entries` (a navigation): re-arm
        (one stream). A widget that stays on screen under a different key is a
        rotation the navigation picked up."""
        with self.view_lock:
            with self.lock:
                before = {e["path"]: e["key"] for e in self.entries.get(user) or []}
                for e in entries:
                    old = before.get(e["path"])
                    if old is not None and old != e["key"]:
                        self.stats["nav_key_rotations"] += 1
                        self.dead_keys.add(old)
            self._view(user, token, entries)

    def _update_entry(self, user: str, path: str, key: str | None) -> None:
        """A refetch of `path` answered 200 with a different refresh key, or
        with none. A different key: the cell's key rotated (it folds identity
        inputs that change, e.g. the RBAC sub-generation) and the armed one is
        dead — the SPA re-arms (refreshSse.ts recordRefreshHeaders -> the arm
        hook's entry.key). No key: the SPA deletes the entry and disarms the
        widget (refreshSse.ts:245-253). The tab does the same (#817)."""
        with self.view_lock:
            with self.lock:
                if self.closed:
                    return
                ents = self.entries.get(user) or []
                hit = [e for e in ents if e["path"] == path and e["key"] != key]
                if not hit:
                    return                      # re-armed meanwhile, or navigated away
                if key is None:
                    ents = [e for e in ents if e["path"] != path]
                    self.stats["disarmed_no_headers"] += 1
                else:
                    ents = [dict(e, key=key) if e["path"] == path else e for e in ents]
                    self.stats["rearmed_key_changed"] += 1
                    self.dead_keys.update(e["key"] for e in hit)
                token = self.tokens[user]
            self._view(user, token, ents)

    def dead_tabs(self) -> dict:
        """How many tabs are armed on a key seen to rotate away (no frame will
        come for it), now."""
        with self.lock:
            dead = {u for u, ents in self.entries.items()
                    if u in self.streams and any(e["key"] in self.dead_keys for e in ents)}
            return {"tabs_dead": len(dead), "tabs_armed": len(self.streams),
                    "dead_keys_seen": len(self.dead_keys)}

    def _view(self, user: str, token: str, entries: list[dict]) -> None:
        by_key: dict[str, list[str]] = {}
        coords, seen = [], set()
        for e in entries:
            by_key.setdefault(e["key"], [])
            if e["path"] not in by_key[e["key"]]:
                by_key[e["key"]].append(e["path"])
            c = json.dumps(e["coords"], sort_keys=True)
            if c not in seen:
                seen.add(c)
                coords.append(e["coords"])
        with self.lock:
            if self.closed:
                return                          # close_all ran: never open another stream
            old = self.streams.pop(user, None)
            self.paths[user] = by_key
            self.entries[user] = list(entries)
            self.tokens[user] = token
        if old is not None:
            old.close()
        if not coords:
            return
        s = self.stream_factory(self.client.base, token, coords,
                                lambda key, t, u=user: self.on_refresh(u, key, t),
                                on_state=lambda st, d, u=user: self._state(u, st, d))
        with self.lock:
            self.streams[user] = s
            self.stats["streams_opened"] += 1
        s.start()

    def _state(self, user, state, detail):
        if state != "open":
            self.sink({"kind": "viewer", "user": user, "state": state, "t": self.clock(), **detail})
        elif self.on_open is not None:
            self.on_open(user, self.clock())

    def on_refresh(self, user: str, key: str, t_event: float) -> None:
        with self.lock:
            self.stats["frames"] += 1
            paths = list((self.paths.get(user) or {}).get(key, []))
            if not paths:
                self.stats["frames_unmatched"] += 1
        for p in paths:
            self._schedule(user, key, p, t_event)

    def _schedule(self, user, key, path, t_event):
        now = self.clock()
        with self.lock:
            w = self.window.get((user, path))
            if w and now < w["until"]:
                w["pending"] = (key, t_event)        # one trailing catch-up
                self.stats["deferred"] += 1
                return
            self.window[(user, path)] = {"until": now + self.throttle_s, "pending": None}
            if self.inflight.get(user, 0) >= self.max_inflight:
                self.queue.setdefault(user, []).append((key, path, t_event))
                return
            self.inflight[user] = self.inflight.get(user, 0) + 1
        self.pool.submit(self._refetch, user, key, path, t_event)

    def _refetch(self, user, key, path, t_event):
        try:
            t_start = self.clock()
            r = self.client.get(path, self.tokens.get(user))
            # key is a cache-key hash: a prefix is enough to correlate, and no
            # body or identity is recorded.
            self.sink({"kind": "push_refetch", "user": user, "key": key[:16], "path": path[:200],
                       "t_event": t_event,
                       "start_lag_ms": round(max(0.0, t_start - t_event) * 1000, 1),
                       "wall_ms": round(r.ms, 1), "status": r.status})
            with self.lock:
                self.stats["refetches"] += 1
            if r.status == 200 and (r.refresh or (None,))[0] != key:
                self._update_entry(user, path, r.refresh[0] if r.refresh else None)
        finally:
            nxt = None
            with self.lock:
                self.inflight[user] = max(0, self.inflight.get(user, 1) - 1)
                q = self.queue.get(user)
                if q:
                    nxt = q.pop(0)
                    self.inflight[user] += 1
            if nxt is not None:
                self.pool.submit(self._refetch, user, *nxt)

    def flush_trailing(self) -> None:
        """Fire the trailing catch-up of every window that has expired."""
        now = self.clock()
        due = []
        with self.lock:
            for (u, p), w in list(self.window.items()):
                if now >= w["until"]:
                    del self.window[(u, p)]
                    if w["pending"]:
                        due.append((u, p, w["pending"]))
        for u, p, (key, t_event) in due:
            self._schedule(u, key, p, t_event)

    def close_all(self) -> dict:
        """Close every tab, for good: under view_lock with `closed` set, so a
        refetch still in flight cannot re-arm (open a stream nobody closes)."""
        with self.view_lock:
            dead = self.dead_tabs()
            with self.lock:
                self.closed = True
                streams = list(self.streams.values())
                self.streams.clear()
        for s in streams:
            s.close()
        return {**self.stats, "tabs_dead_at_close": dead["tabs_dead"],
                "dead_keys_seen": dead["dead_keys_seen"]}


def login(authn_url: str, user: str, password: str, timeout: float = 30) -> str:
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(authn_url.rstrip("/") + "/basic/login",
                                 headers={"Authorization": "Basic " + creds})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)["accessToken"]


# ─── SPA walk ───────────────────────────────────────────────────────────────


def extract_refs(doc) -> list[str]:
    """Paths the SPA would fetch next from one resolved widget.

    GET + allowed refs under status.resourcesRefs.items whose path is a
    snowplow /call. A ref already embedded server-side (`rendered`, #72
    inline children) is NOT fetched — the browser does not fetch it either.
    """
    if not isinstance(doc, dict):
        return []
    items = (((doc.get("status") or {}).get("resourcesRefs") or {})
             .get("items") or [])
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if (it.get("verb") or "GET").upper() != "GET" or not it.get("allowed"):
            continue
        if it.get("rendered"):
            continue
        p = it.get("path") or ""
        if p.startswith("/call"):
            out.append(p)
    return out


@dataclass
class NavResult:
    user: str
    page: str
    visit: int            # 1 = this user's first visit of this page
    start: float          # epoch seconds
    wall_ms: float
    calls: int
    errors: int
    bytes: int
    slowest_call_ms: float
    sched_lag_ms: float = 0.0
    error_samples: list = field(default_factory=list)


def _allowed_ref_paths(doc) -> dict[str, str]:
    """id -> /call path for every GET + allowed, not-yet-rendered ref."""
    if not isinstance(doc, dict):
        return {}
    items = (((doc.get("status") or {}).get("resourcesRefs") or {})
             .get("items") or [])
    out = {}
    for it in items:
        if not isinstance(it, dict) or not it.get("id"):
            continue
        if (it.get("verb") or "GET").upper() != "GET" or not it.get("allowed"):
            continue
        if it.get("rendered"):
            continue
        p = it.get("path") or ""
        if p.startswith("/call"):
            out[it["id"]] = p
    return out


# ─── What the SPA actually fetches (frontend ui/src, read 2026-10-14) ───────
#
# A widget's children are fetched ONLY when a container RENDERS them, i.e. when
# a render slot in widgetData names their resourceRefId and RefChild /
# WidgetRenderer mounts it (components/RefChild/RefChild.tsx:60-64). A
# `resourcesRefs` entry that no slot names is NOT fetched. Slots by kind:
#   Flex/Col/Row/Form/ButtonGroup/Steps/Filters/List/PageHeader/Card body:
#       widgetData.items[].resourceRefId           (Flex.tsx:15, Col.tsx:23,
#       Row.tsx:64, Form.tsx:584, ButtonGroup.tsx:27, Steps.tsx:21,
#       Filters.tsx:54, List.tsx:50, PageHeader.tsx:75, Card.tsx:211)
#   Card: also cover, extraRefId, footer[].resourceRefId (Card.tsx:34-54)
#   Table: dataSource[][].resourceRefId per rendered cell (Table.tsx:168-236)
#   Layout: header, content, footer, sider.resourceRefId, effects[]
#       (Layout.tsx:20-23, 41-47, 74)
#   Tabs: ONLY the active tab — antd mounts an inactive pane on first
#       activation; default = first item (Tabs.tsx:18-45)
# NOT fetched on render:
#   Menu: its items only REGISTER routes (Menu.tsx:35-43); a route's page is
#       fetched when the route is visited (WidgetPage.tsx:41, 90).
#   widgetActions / actions (openDrawer, navigate, rest): on click only
#       (Drawer.tsx:94, Modal.tsx:84 render the target when opened).

_LAYOUT_SLOTS = ("header", "content", "footer")
_NEVER_EAGER_KEYS = ("widgetActions", "actions")


def _collect_ref_ids(node, out: list) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in _NEVER_EAGER_KEYS:
                continue
            if k in ("resourceRefId", "extraRefId", "cover") and isinstance(v, str):
                out.append(v)
            else:
                _collect_ref_ids(v, out)
    elif isinstance(node, list):
        for v in node:
            _collect_ref_ids(v, out)


def eager_refs(doc) -> list[str]:
    """The /call paths the SPA fetches when it RENDERS this widget."""
    if not isinstance(doc, dict):
        return []
    kind = doc.get("kind")
    wd = (doc.get("status") or {}).get("widgetData") or {}
    paths = _allowed_ref_paths(doc)
    if kind == "Menu":
        return []
    ids: list[str] = []
    if kind == "Layout":
        ids += [wd.get(k) for k in _LAYOUT_SLOTS if isinstance(wd.get(k), str)]
        sider = wd.get("sider")
        if isinstance(sider, dict) and isinstance(sider.get("resourceRefId"), str):
            ids.append(sider["resourceRefId"])
        ids += [e for e in (wd.get("effects") or []) if isinstance(e, str)]
    elif kind == "Tabs":
        items = wd.get("items") or []
        if items and isinstance(items[0], dict) and items[0].get("resourceRefId"):
            ids.append(items[0]["resourceRefId"])
    else:
        _collect_ref_ids(wd, ids)
    seen, out = set(), []
    for i in ids:
        p = paths.get(i)
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


_TEMPLATE = re.compile(r"\{[^}]+\}")


def _route_slug(path: str) -> str:
    """navModel.ts routeSlug: drop {param} segments + leading '/', '/'→'-'."""
    s = re.sub(r"/?\{[^}]+\}", "", path or "").lstrip("/").replace("/", "-")
    return s or "home"


def menu_routes(menu_doc, frontend_ns: str) -> list[dict]:
    """Routes the sidebar Menu registers, in menu order (navModel.ts:56-78).

    content endpoint: the item's resourceRefId → that ref's path; else the
    convention flexes/page-<slug> in item.namespace or FRONTEND_NAMESPACE.
    An item whose resourceRefId was DENIED is not a visible route (RBAC gate,
    navModel.ts isNavEntryAllowed). Templated paths ({param}) cannot be
    navigated without a value and are returned with templated=True.
    """
    wd = ((menu_doc or {}).get("status") or {}).get("widgetData") or {}
    paths = _allowed_ref_paths(menu_doc)
    out = []
    items = sorted((i for i in (wd.get("items") or []) if isinstance(i, dict)),
                   key=lambda i: i.get("order", 0))
    for it in items:
        if it.get("type") == "divider" or not it.get("path"):
            continue
        ref = it.get("resourceRefId")
        if ref:
            if ref not in paths:
                continue                       # denied / dangling → not visible
            endpoint = paths[ref]
        else:
            ns = it.get("namespace") if it.get("namespace") is not None else frontend_ns
            endpoint = ("/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1"
                        f"&resource=flexes&name=page-{it.get('page') or _route_slug(it['path'])}"
                        f"&namespace={ns}")
        out.append({"path": it["path"], "endpoint": endpoint,
                    "templated": bool(_TEMPLATE.search(it["path"]))})
    return out


@dataclass
class WalkResult:
    wall_ms: float
    calls: int
    errors: int
    bytes: int
    slowest_ms: float
    samples: list
    statuses: dict
    docs: dict            # path -> parsed doc (only when keep_docs)
    depth: int = 0        # levels of the tree actually fetched (1 = root only)
    # The widgets a viewer would arm: [{"path", "key", "coords"}] for every
    # 200 response that carried the refresh headers (#764).
    armed: list = field(default_factory=list)


def walk(client: HttpClient, token: str, roots: list[str], *,
         pool: concurrent.futures.Executor, children=eager_refs,
         inject: dict | None = None, per_user_parallel: int = 6,
         max_depth: int = 12, max_calls: int = 5000,
         keep_docs: bool = False) -> WalkResult:
    """Fetch like the SPA: level by level (a child is requested only after its
    parent arrived), at most `per_user_parallel` in flight (a browser's
    per-host limit). `children(doc)` decides what a rendered widget fetches;
    `inject[path]` adds requests the app makes once `path` arrived (e.g. the
    Layout's HeaderChrome → header-context, Shell.tsx:26-33)."""
    inject = inject or {}
    seen = set(roots)
    level = list(roots)
    calls = errors = nbytes = 0
    slowest = 0.0
    samples: list[str] = []
    statuses: dict[str, int] = {}
    docs: dict[str, dict] = {}
    armed: list[dict] = []
    t0 = time.perf_counter()
    depth = 0
    while level and depth <= max_depth and calls < max_calls:
        nxt: list[str] = []
        for i in range(0, len(level), per_user_parallel):
            chunk = level[i:i + per_user_parallel]
            for path, resp in zip(chunk, pool.map(
                    lambda p: client.get(p, token), chunk)):
                calls += 1
                slowest = max(slowest, resp.ms)
                nbytes += len(resp.body)
                key = str(resp.status)
                statuses[key] = statuses.get(key, 0) + 1
                for extra in inject.get(path, ()):
                    if extra not in seen:
                        seen.add(extra)
                        nxt.append(extra)
                if resp.status != 200:
                    errors += 1
                    if len(samples) < 5:
                        samples.append(f"{resp.status} {path[:160]} "
                                       f"{(resp.body or b'')[:240].decode('utf-8', 'replace') or resp.error}")
                    continue
                ref = getattr(resp, "refresh", None)
                if ref:
                    coords = refresh_coords(path, ref[1])
                    if coords is not None:
                        armed.append({"path": path, "key": ref[0], "coords": coords})
                try:
                    doc = json.loads(resp.body or b"null")
                except ValueError:
                    continue
                if keep_docs:
                    docs[path] = doc
                for ref in children(doc):
                    if ref not in seen:
                        seen.add(ref)
                        nxt.append(ref)
        level = nxt
        depth += 1
    return WalkResult(round((time.perf_counter() - t0) * 1000, 1), calls, errors,
                      nbytes, round(slowest, 1), samples, statuses, docs, depth, armed)


def walk_page(client: HttpClient, token: str, root: str, *,
              pool: concurrent.futures.Executor, per_user_parallel: int = 6,
              max_depth: int = 8, max_calls: int = 2000):
    """The FULL CLOSURE walk: every allowed ref, recursively. NOT a navigation
    (it fetches everything the sidebar menu can reach — 590 calls / 8.4 MB on
    run 6). Kept only for the separately-labelled `full_walk` report metric.
    Returns (wall_ms, calls, errors, bytes, slowest, error_samples)."""
    r = walk(client, token, [root], pool=pool, children=extract_refs,
             per_user_parallel=per_user_parallel, max_depth=max_depth,
             max_calls=max_calls)
    return r.wall_ms, r.calls, r.errors, r.bytes, r.slowest_ms, r.samples


@dataclass
class Portal:
    """The portal's boot description, from the frontend's config.json."""
    init: str                     # api.INIT (the app-shell Layout)
    frontend_ns: str = "krateo-system"     # params.FRONTEND_NAMESPACE
    builders_ns: str | None = "krateo-system"   # api.BUILDERS_NAMESPACE

    def header_context(self) -> str:
        """Shell.tsx:28-29 — derived from INIT's namespace."""
        qs = urllib.parse.parse_qs(self.init.split("?", 1)[1] if "?" in self.init else "")
        ns = (qs.get("namespace") or ["krateo-system"])[0]
        return ("/call?resource=flexes&apiVersion=widgets.templates.krateo.io/v1beta1"
                f"&name=header-context&namespace={ns}")

    def builders_list(self) -> str | None:
        """clusterBuilders.ts:4-6,116-122 — one /list read per sign-in."""
        return f"/list?category=builders&ns={self.builders_ns}" if self.builders_ns else None


def first_load(client: HttpClient, token: str, portal: Portal, *,
               pool, per_user_parallel: int = 6) -> tuple[WalkResult, list[dict], WalkResult | None]:
    """(i) First load after sign-in:
      INIT Layout (Shell.tsx:92) + its eager slots; the Layout's header slot
      falls back to HeaderChrome → header-context (Shell.tsx:26-33, Layout.tsx:41);
      the Builders /list read (Shell.tsx:81-85); the sidebar Menu registers
      routes, then `/` redirects to the FIRST route (Menu.tsx:45-50), whose
      content page is fetched with its eager tree (WidgetPage.tsx:41, 90).
    Returns (shell walk, the user's routes, first route's content walk)."""
    roots = [portal.init] + ([portal.builders_list()] if portal.builders_list() else [])
    shell = walk(client, token, roots, pool=pool,
                 inject={portal.init: [portal.header_context()]},
                 per_user_parallel=per_user_parallel, keep_docs=True)
    menu = next((d for d in shell.docs.values()
                 if isinstance(d, dict) and d.get("kind") == "Menu"), None)
    routes = [r for r in menu_routes(menu, portal.frontend_ns) if not r["templated"]]
    content = None
    if routes:
        content = walk(client, token, [routes[0]["endpoint"]], pool=pool,
                       per_user_parallel=per_user_parallel)
    return shell, routes, content


def first_load_failed(shell: WalkResult, routes: list, content: WalkResult | None) -> bool:
    """A first load FAILED (and must be retried) when the user did not get a
    working portal: no routes (no shell / no menu), or any transport failure
    (status 0: refused, reset, timeout) in the shell or the first page. A
    product 5xx inside the page is a finding, not a failed arrival."""
    if not routes:
        return True
    for w in (shell, content):
        if w is not None and w.statuses.get("0"):
            return True
    return False


# ─── Driver ─────────────────────────────────────────────────────────────────


class Driver:
    """Open-loop virtual-user scheduler over the SPA navigation model.

    A user's FIRST navigation is the first load (shell + Builders read +
    redirect to the first menu route). Every later navigation is a ROUTE CLICK:
    only that route's content tree, since the shell stays mounted
    (Shell.tsx:61-69: "only the content swaps on navigation"). Routes come
    from the user's OWN sidebar menu (RBAC-filtered by snowplow). Think time
    is exponential; scheduling lag is recorded.
    """

    def __init__(self, client: HttpClient, tokens: dict[str, str],
                 portal: Portal, *,
                 think_s: float = 60.0, duration_s: float = 1800.0,
                 nav_workers: int = 128, call_workers: int = 512,
                 per_user_parallel: int = 6, seed: int = 606,
                 sink: Callable[[dict], None] | None = None,
                 retry_base_s: float = 5.0, retry_cap_s: float = 120.0,
                 composite_routes: list[str] | None = None,
                 burst_users: list[str] | None = None,
                 burst_at_s: float | None = None,
                 viewers: bool = False,
                 viewer_factory=None,
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep):
        self.client = client
        self.tokens = tokens
        self.portal = portal
        # COLD COMPOSITE-PAGE BURST (#710 ruling 3 / #720): a reserved cohort
        # that does NOTHING until burst_at_s, then all of them deep-link to a
        # composite page at the same instant: each one's first-ever load of
        # the shell AND of a nested widget tree.
        self.composite_routes = list(composite_routes or [])
        self.burst_users = [u for u in (burst_users or []) if u in tokens]
        self.burst_at_s = burst_at_s
        self.burst_pending: set[str] = set(self.burst_users)
        if self.burst_users:
            nav_workers = max(nav_workers, len(self.burst_users) + 64)
        self.think_s = think_s
        self.duration_s = duration_s
        self.nav_pool = concurrent.futures.ThreadPoolExecutor(nav_workers)
        self.call_pool = concurrent.futures.ThreadPoolExecutor(call_workers)
        self.per_user_parallel = per_user_parallel
        self.rng = random.Random(seed)
        self.sink = sink or (lambda rec: None)
        self.clock = clock
        self.sleep = sleep
        self.visits: dict[tuple[str, str], int] = {}
        self.routes: dict[str, list[dict]] = {}
        self.busy: set[str] = set()
        self.attempts: dict[str, int] = {}
        self.not_before: dict[str, float] = {}
        self.retry_base_s = retry_base_s
        self.retry_cap_s = retry_cap_s
        self.lock = threading.Lock()
        self.results: list[NavResult] = []
        # #764: each user's tab arms /refreshes over the shell + current page.
        self.shell_armed: dict[str, list] = {}
        self.viewers = None
        if viewers:
            self.viewers = (viewer_factory or ViewerHub)(client, self.call_pool, self.sink,
                                                        clock=clock)

    def _record(self, user, page, kind, started, due, w: WalkResult,
                extra: WalkResult | None = None, routes: int | None = None,
                failed: bool = False, attempt: int | None = None):
        with self.lock:
            if failed:
                visit = 0               # a failed first load is not a visit
            else:
                visit = self.visits.get((user, page), 0) + 1
                self.visits[(user, page)] = visit
        parts = [w] + ([extra] if extra else [])
        res = NavResult(
            user=user, page=page, visit=visit, start=started,
            wall_ms=round(sum(p.wall_ms for p in parts), 1),
            calls=sum(p.calls for p in parts), errors=sum(p.errors for p in parts),
            bytes=sum(p.bytes for p in parts),
            slowest_call_ms=max(p.slowest_ms for p in parts),
            sched_lag_ms=round(max(0.0, started - due) * 1000, 1),
            error_samples=sum((p.samples for p in parts), [])[:5])
        with self.lock:
            self.results.append(res)
        rec = {"kind": "nav", "nav": kind, **asdict(res)}
        if routes is not None:
            rec["routes"] = routes
        if failed:
            rec["first_load_failed"] = True
        if attempt is not None:
            rec["attempt"] = attempt
        self.sink(rec)

    def _nav(self, user: str, due: float) -> None:
        started = self.clock()
        tok = self.tokens[user]
        try:
            if user in self.burst_pending:
                self._composite_first_load(user, tok, started, due)
                return
            if user not in self.routes:
                shell, routes, content = first_load(
                    self.client, tok, self.portal, pool=self.call_pool,
                    per_user_parallel=self.per_user_parallel)
                failed = first_load_failed(shell, routes, content)
                with self.lock:
                    attempt = self.attempts.get(user, 0) + 1
                    self.attempts[user] = attempt
                    if failed:
                        # NOT first-loaded: retry with capped exponential
                        # backoff, so the arrival burst is REPLAYED once
                        # snowplow is back (run 7: users whose first load died
                        # with snowplow never arrived again).
                        self.not_before[user] = self.clock() + min(
                            self.retry_cap_s, self.retry_base_s * (2 ** (attempt - 1)))
                    else:
                        self.routes[user] = routes
                        self.not_before.pop(user, None)
                        self.shell_armed[user] = shell.armed
                if not failed and self.viewers is not None:
                    self.viewers.view(user, tok, shell.armed + (content.armed if content else []))
                page = routes[0]["path"] if routes else "(no-route)"
                self._record(user, page, "first_load", started, due, shell,
                             content, routes=len(routes), failed=failed,
                             attempt=attempt)
                return
            routes = self.routes[user]
            if not routes:
                return
            r = self.rng.choice(routes)
            w = walk(self.client, tok, [r["endpoint"]], pool=self.call_pool,
                     per_user_parallel=self.per_user_parallel)
            if self.viewers is not None:
                self.viewers.view(user, tok, self.shell_armed.get(user, []) + w.armed)
            self._record(user, r["path"], "route", started, due, w)
        finally:
            with self.lock:
                self.busy.discard(user)

    def _composite_first_load(self, user: str, tok: str, started: float, due: float) -> None:
        """Shell + a deep link straight to a composite page (no redirect to
        the first route): the user's first-ever load of a nested tree."""
        portal = self.portal
        roots = [portal.init] + ([portal.builders_list()] if portal.builders_list() else [])
        shell = walk(self.client, tok, roots, pool=self.call_pool,
                     inject={portal.init: [portal.header_context()]},
                     per_user_parallel=self.per_user_parallel, keep_docs=True)
        menu = next((d for d in shell.docs.values()
                     if isinstance(d, dict) and d.get("kind") == "Menu"), None)
        routes = [r for r in menu_routes(menu, portal.frontend_ns) if not r["templated"]]
        by_path = {r["path"]: r for r in routes}
        target = next((by_path[p] for p in self.composite_routes if p in by_path), None)
        content = None
        if target is not None:
            content = walk(self.client, tok, [target["endpoint"]], pool=self.call_pool,
                           per_user_parallel=self.per_user_parallel)
        failed = first_load_failed(shell, routes, content) or target is None
        with self.lock:
            self.burst_pending.discard(user)
            if not failed:
                self.routes[user] = routes          # joins normal navigation
                self.shell_armed[user] = shell.armed
        if not failed and self.viewers is not None:
            self.viewers.view(user, tok, shell.armed + (content.armed if content else []))
        page = target["path"] if target else "(no-composite-route)"
        self._record(user, page, "composite_first_load", started, due, shell, content,
                     routes=len(routes), failed=failed)

    def run(self) -> list[NavResult]:
        t_start = self.clock()
        end = t_start + self.duration_s
        # First loads are spread over one think period so 1000 users do not
        # all arrive at the same instant.
        nxt = {u: t_start + self.rng.uniform(0, self.think_s) for u in self.tokens}
        if self.burst_users and self.burst_at_s is not None:
            for u in self.burst_users:            # all at the SAME instant
                nxt[u] = t_start + self.burst_at_s
        futures = []
        next_dead_sample = t_start
        while True:
            now = self.clock()
            if now >= end:
                break
            for u, t in list(nxt.items()):
                if t > now:
                    continue
                nb = self.not_before.get(u)
                if nb is not None and nb > now:
                    nxt[u] = nb                 # backing off after a failed first load
                    continue
                with self.lock:
                    if u in self.busy:          # a person finishes one navigation first
                        continue
                    self.busy.add(u)
                futures.append(self.nav_pool.submit(self._nav, u, t))
                nxt[u] = t + self.rng.expovariate(1.0 / self.think_s)
            if self.viewers is not None:
                self.viewers.flush_trailing()
                if now >= next_dead_sample:
                    # #817 re-gate: tabs left armed on a key seen to rotate away
                    self.sink({"kind": "viewer_dead", "t": now, **self.viewers.dead_tabs()})
                    next_dead_sample = now + DEAD_TAB_SAMPLE_S
            soonest = min(nxt.values())
            self.sleep(max(0.0, min(0.05, soonest - self.clock())))
        for f in futures:
            f.result()
        if self.viewers is not None:
            self.sink({"kind": "viewer_stats", "t": self.clock(), **self.viewers.close_all()})
        self.nav_pool.shutdown(wait=True)
        self.call_pool.shutdown(wait=True)
        return self.results


# ─── Freshness ──────────────────────────────────────────────────────────────


def freshness_probe(client: HttpClient, token: str, path: str,
                    mutate: Callable[[str], bool], *, samples: int = 60,
                    poll_ms: int = 100, budget_s: float = 30.0,
                    gap_s: float = 5.0, label: str = "list_dep",
                    sink: Callable[[dict], None] | None = None,
                    healthy: Callable[[], bool] | None = None,
                    pause_poll_s: float = 5.0, max_pause_s: float = 3600.0,
                    push_since: Callable[[float], float | None] | None = None,
                    observe: Callable[["Response"], None] | None = None,
                    viewer: "ProbeViewer | None" = None,
                    deadline: float | None = None,
                    clock: Callable[[], float] = time.time,
                    sleep: Callable[[float], None] = time.sleep) -> list[dict]:
    """Mutate → poll until the marker is visible. One record per sample.

    The clock starts when `mutate` RETURNS (the apiserver has committed the
    write), so the latency is informer → dirty-mark → refresh → serve, not the
    driver's own network hop. A sample that never converges records
    latency_ms=None and converged=False; the verdict fails on it (it is not
    dropped). The poll interval bounds resolution: report it with the result.

    `healthy()` (snowplow /readyz): before each sample the probe PAUSES while
    snowplow is not Ready (a `fresh_pause` record says for how long), and a
    sample during which snowplow stopped being Ready is marked
    valid=False — run 7 recorded 30-second "unconverged" samples against a
    dead snowplow, which measure the outage, not freshness.
    """
    sink = sink or (lambda rec: None)
    healthy = healthy or (lambda: True)
    if viewer is not None and observe is None:
        observe = viewer.observe
    out = []
    for i in range(samples):
        if deadline is not None and clock() >= deadline:
            # The phase's wall-clock share is spent (slow mutates or polls):
            # stop HERE, recorded, so the whole probe stays inside the bound
            # run606's measure wait and the TTL budget are computed from.
            sink({"kind": "fresh_truncated", "label": label, "done": i, "of": samples,
                  "t": clock()})
            break
        if not healthy():
            p0 = clock()
            while not healthy() and clock() - p0 < max_pause_s:
                sleep(pause_poll_s)
            sink({"kind": "fresh_pause", "label": label, "t": p0,
                  "paused_s": round(clock() - p0, 1), "recovered": healthy()})
        marker = f"fresh-{label}-{int(clock() * 1000)}-{i}"
        if not mutate(marker):
            rec = {"kind": "fresh", "label": label, "marker": marker,
                   "converged": False, "latency_ms": None, "polls": 0,
                   "error": "mutate failed", "poll_ms": poll_ms}
            out.append(rec)
            sink(rec)
            continue
        t0 = clock()
        polls = 0
        lat = None
        gen0 = viewer.gen if viewer is not None else None
        while clock() - t0 < budget_s:
            polls += 1
            r = client.get(path, token)
            if observe is not None:
                observe(r)                  # the armed viewer follows the cell's key
            if r.status == 200 and marker.encode() in r.body:
                lat = round((clock() - t0) * 1000, 1)
                break
            sleep(poll_ms / 1000.0)
        rec = {"kind": "fresh", "label": label, "marker": marker,
               "converged": lat is not None, "latency_ms": lat,
               "polls": polls, "poll_ms": poll_ms, "t": t0}
        if push_since is not None:
            # #764: the probe's tab is ARMED on its cell; push_ms is mutate →
            # the first `refresh` frame for the cell (what the SPA reacts to).
            tp = push_since(t0)
            rec["armed"] = True
            rec["push_ms"] = (round((tp - t0) * 1000, 1)
                              if tp is not None and tp - t0 <= budget_s else None)
        if viewer is not None:
            # #817 re-gate: which arm this sample ran on, whether a POLL re-armed
            # it meanwhile (a passive tab could not have), and what the
            # never-re-armed control (the passive tab) received.
            rec["arm_gen"] = gen0
            rec["rearmed_during"] = viewer.gen != gen0
            if viewer.control is not None:
                tc = viewer.control_push_since(t0)
                rec["control_push_ms"] = (round((tc - t0) * 1000, 1)
                                          if tc is not None and tc - t0 <= budget_s else None)
        if not healthy():
            rec["valid"] = False
            rec["invalid_reason"] = "snowplow not Ready (/readyz) during the sample"
        out.append(rec)
        sink(rec)
        sleep(gap_s)
    return out


def wait_for_customer_load(client: HttpClient, token: str, *, min_delta: int = 100,
                           poll_s: float = 5.0, max_wait_s: float = 1800.0,
                           clock: Callable[[], float] = time.time,
                           sleep: Callable[[float], None] = time.sleep) -> dict:
    """Block until the load driver's navigations are reaching snowplow.

    The signal is snowplow's own L1 lookup counter (hit + miss) rising by
    `min_delta` from this probe's first reading. It is only a START gate (the
    refresher also adds lookups, #552, so it is never used as a measurement).
    Returns {started, waited_s, delta}; started=False after max_wait_s.
    """
    def total():
        v = read_vars(client, token)
        if not v:
            return None
        s = summarize_vars(v)
        if s["l1_hit"] is None:
            return None
        return s["l1_hit"] + s["l1_miss"]
    t0 = clock()
    base = None
    while clock() - t0 < max_wait_s:
        cur = total()
        if cur is not None:
            if base is None:
                base = cur
            elif cur - base >= min_delta:
                return {"started": True, "waited_s": round(clock() - t0, 1),
                        "delta": cur - base}
        sleep(poll_s)
    return {"started": False, "waited_s": round(clock() - t0, 1), "delta": None}


def readyz(client: HttpClient) -> bool:
    return client.get("/readyz", None).status == 200


class ChurnDriver:
    """`rate` mutations/s over `targets`, round-robin, in a background thread.

    `mutate(target, marker) -> bool`. Records how many it issued and how many
    failed, so the verdict can state the event rate it actually achieved.
    """

    def __init__(self, targets: list, rate: float,
                 mutate: Callable[[object, str], bool], workers: int = 16):
        if rate <= 0 or not targets:
            raise ValueError("rate must be > 0 and targets non-empty")
        self.targets = targets
        self.rate = rate
        self.mutate = mutate
        self.pool = concurrent.futures.ThreadPoolExecutor(workers)
        self.stop_evt = threading.Event()
        self.issued = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.t0 = 0.0
        self.t1 = 0.0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _one(self, target, marker):
        ok = False
        try:
            ok = bool(self.mutate(target, marker))
        finally:
            with self.lock:
                self.issued += 1
                if not ok:
                    self.failed += 1

    def _loop(self):
        self.t0 = time.time()
        i = 0
        while not self.stop_evt.is_set():
            target = self.targets[i % len(self.targets)]
            self.pool.submit(self._one, target, f"churn-{i}")
            i += 1
            due = self.t0 + i / self.rate
            self.stop_evt.wait(max(0.0, due - time.time()))
        self.t1 = time.time()

    def start(self):
        self.thread.start()
        return self

    def stop(self) -> dict:
        self.stop_evt.set()
        self.thread.join()
        self.pool.shutdown(wait=True)
        span = max(1e-9, self.t1 - self.t0)
        return {"issued": self.issued, "failed": self.failed,
                "target_rate": self.rate,
                "achieved_rate": round((self.issued - self.failed) / span, 2)}


# ─── /debug/vars ────────────────────────────────────────────────────────────


def _sum_l1(lookups) -> tuple[int | None, int | None]:
    if not isinstance(lookups, dict):
        return None, None
    hit = miss = 0
    for cell in lookups.values():
        if isinstance(cell, dict):
            hit += int(cell.get("hit_total", 0))
            miss += int(cell.get("miss_total", 0))
    return hit, miss


def summarize_vars(v: dict) -> dict:
    """Keep the refresh-economics fields of one /debug/vars document.

    Absent keys stay None (never 0): an absent counter is not a zero reading.
    """
    def g(k):
        return v.get(k)

    attr = g("snowplow_deps_dirty_mark_attribution") or {}
    by_class: dict[str, int] = {}
    for b in attr.get("by_bucket") or []:
        if isinstance(b, dict):
            by_class[b.get("class", "?")] = (by_class.get(b.get("class", "?"), 0)
                                             + int(b.get("count", 0)))
    rc = g("snowplow_resolved_cache") or {}
    l1_hit, l1_miss = _sum_l1(g("snowplow_dispatch_l1_lookups"))
    mem = g("memstats") or {}
    return {
        "refresher_enqueue": g("snowplow_refresher_enqueue_total"),
        "refresher_completed": g("snowplow_refresher_completed_total"),
        "refresher_dropped": g("snowplow_refresher_dropped_total"),
        "refresher_failed": g("snowplow_refresher_failed_total"),
        "refresher_queue_depth": g("snowplow_refresher_queue_depth"),
        "dirty_to_fresh_p95_ms": g("snowplow_refresher_dirty_to_fresh_ms_p95"),
        "dirty_to_fresh_max_ms": g("snowplow_refresher_dirty_to_fresh_ms_max"),
        "dirty_to_fresh_samples": g("snowplow_refresher_dirty_to_fresh_samples_total"),
        "marks_by_class": by_class if attr else None,
        "mark_events": attr.get("events_total") if attr else None,
        "marks_unattributed": attr.get("unattributed") if attr else None,
        "cache_entries": rc.get("entries"),
        "cache_bytes": rc.get("bytes"),
        "cache_hit": rc.get("hit_total"),
        "cache_miss": rc.get("miss_total"),
        "l1_hit": l1_hit,
        "l1_miss": l1_miss,
        "prewarm_complete": g("snowplow_prewarm_complete"),
        "heap_inuse": mem.get("HeapInuse"),
        "num_gc": mem.get("NumGC"),
    }


def read_vars(client: HttpClient, token: str) -> dict | None:
    r = client.get("/debug/vars", token)
    if r.status != 200:
        return None
    try:
        return json.loads(r.body)
    except ValueError:
        return None


def _delta(a, b):
    if a is None or b is None:
        return None
    return b - a


def economics(first: dict, last: dict, customer_calls: int) -> dict:
    """Refresh economics over [first, last] (two summarize_vars outputs).

    completions_per_read uses the DRIVER's request count as the denominator.
    """
    completed = _delta(first.get("refresher_completed"),
                       last.get("refresher_completed"))
    enq = _delta(first.get("refresher_enqueue"), last.get("refresher_enqueue"))
    m0 = first.get("marks_by_class") or None
    m1 = last.get("marks_by_class") or None
    marks = list_dep = None
    if m0 is not None and m1 is not None:
        marks = sum(m1.values()) - sum(m0.values())
        list_dep = m1.get("list_dep", 0) - m0.get("list_dep", 0)
    events = _delta(first.get("mark_events"), last.get("mark_events"))
    return {
        "customer_calls": customer_calls,
        "refresher_completed": completed,
        "refresher_enqueued": enq,
        "refresher_dropped": _delta(first.get("refresher_dropped"),
                                    last.get("refresher_dropped")),
        "completions_per_read": (round(completed / customer_calls, 3)
                                 if completed is not None and customer_calls
                                 else None),
        "dirty_marks": marks,
        "list_dep_marks": list_dep,
        "list_dep_share": (round(list_dep / marks, 4)
                           if marks else None),
        "mark_events": events,
        "marks_per_event": (round(marks / events, 2)
                            if marks is not None and events else None),
    }


# ─── Verdict ────────────────────────────────────────────────────────────────


def login_report(curves: list[dict]) -> dict | None:
    """Summary of the adaptive-login records. AUTHN FINDING, not snowplow's:
    each login issues an x509 certificate (CSR) — run 6 measured ~3.2 s per
    login, a user-facing sign-in latency."""
    if not curves:
        return None
    rows = [r for c in curves for r in c.get("curve", [])]
    base = next((r for r in rows if r.get("phase") == "baseline"), {})
    meds = [r["median_ms"] for r in rows if r.get("median_ms") is not None]
    return {"users": sum(c.get("users", 0) for c in curves),
            "failed": sum(c.get("failed", 0) for c in curves),
            "baseline_single_login_ms": base.get("median_ms"),
            "median_login_ms": pct(meds, 50),
            "max_concurrency": max((r.get("concurrency", 1) for r in rows), default=1),
            "total_batch_wall_s": round(sum(r.get("batch_wall_ms") or 0 for r in rows) / 1000, 1),
            "curve": rows,
            "finding": "authn: per-login latency (x509 CSR issued per login) is a user-facing number"}


def restart_check(pods: list[dict] | None) -> dict | None:
    """INVALID when snowplow restarted during the measurement.

    `pods` are `snowplow_pod` records taken before and after measure (restart
    count + lastState per container). None = not provided (unit callers):
    no check. Provided but incomplete = INVALID (unknown is not "no restart").
    """
    if pods is None:
        return None
    before = next((p for p in pods if p.get("phase") == "before"), None)
    after = next((p for p in pods if p.get("phase") == "after"), None)
    if before is None or after is None:
        return {"check": "snowplow.restarts_during_measure", "value": None,
                "bar": "== 0", "result": "FAIL",
                "why": "restart state unknown (missing before/after pod record)"}
    if before.get("pod") is None or after.get("pod") is None:
        # #758: a record with no live pod (none, or only Terminating ones) is
        # unknown on EITHER side — two such records are not "same pod, 0".
        return {"check": "snowplow.restarts_during_measure", "value": None,
                "bar": "== 0", "result": "FAIL",
                "why": "restart state unknown (no live snowplow pod in the "
                       f"{'before' if before.get('pod') is None else 'after'} record)"}
    if after.get("pod") != before.get("pod"):
        return {"check": "snowplow.restarts_during_measure", "value": None,
                "bar": "== 0", "result": "FAIL",
                "why": f"snowplow pod REPLACED during measure: {before.get('pod')} -> {after.get('pod')}"}
    delta = (after.get("restarts") or 0) - (before.get("restarts") or 0)
    if delta > 0:
        ls = (after.get("last_state") or {}).get("terminated") or {}
        return {"check": "snowplow.restarts_during_measure", "value": delta,
                "bar": "== 0", "result": "FAIL",
                "why": f"snowplow restarted {delta}x during measure: last {ls.get('reason')} "
                       f"(exit {ls.get('exitCode')}) at {ls.get('finishedAt')}"}
    return {"check": "snowplow.restarts_during_measure", "value": 0, "bar": "== 0",
            "result": "PASS"}


def pod_record(pod_list: dict, phase: str) -> dict:
    """`kubectl get pods -l <snowplow> -o json` → one `snowplow_pod` record."""
    items = pod_list.get("items") or []
    if not items:
        return {"kind": "snowplow_pod", "phase": phase, "pod": None, "restarts": None,
                "last_state": None, "t": time.time()}
    # The live pod, never a Terminating rollout leftover (deletionTimestamp
    # set): right after the reboot the old ReplicaSet's pod can still sort
    # first, and recording it would read as a replaced pod on another digest.
    live = [p for p in items if not (p.get("metadata") or {}).get("deletionTimestamp")]
    if not live:
        return {"kind": "snowplow_pod", "phase": phase, "pod": None, "restarts": None,
                "last_state": None, "terminating_only": True, "t": time.time()}
    p = live[0]
    cs = ((p.get("status") or {}).get("containerStatuses") or [{}])[0]
    return {"kind": "snowplow_pod", "phase": phase,
            "pod": (p.get("metadata") or {}).get("name"),
            "restarts": cs.get("restartCount", 0),
            "ready": cs.get("ready"),
            "last_state": cs.get("lastState") or {},
            "limits": (((p.get("spec") or {}).get("containers") or [{}])[0]
                       .get("resources") or {}).get("limits"),
            "image_id": cs.get("imageID"),
            "digest": image_digest_of(cs.get("imageID")),
            "t": time.time()}


def _termination(state: dict | None) -> dict | None:
    t = (state or {}).get("terminated")
    if not t:
        return None
    return {"reason": t.get("reason"), "exit_code": t.get("exitCode"),
            "finished_at": t.get("finishedAt")}


def job_pod_records(pod_list: dict, job: str) -> list[dict]:
    """`kubectl get pods -l job-name=<job> -o json` → one `job_pod` record per
    pod: phase, readiness and how each container ended (#817 review: a driver
    pod's death must reach verdict.json with its reason, not only run.log)."""
    items = pod_list.get("items") or []
    if not items:
        # A deployed Job with no pod at collect: recorded, so it cannot be silent.
        return [{"kind": "job_pod", "job": job, "t": time.time(), "pod": None,
                 "missing": True}]
    out = []
    for p in items:
        st = p.get("status") or {}
        sched = next((c for c in st.get("conditions") or []
                      if c.get("type") == "PodScheduled" and c.get("status") != "True"), None)
        out.append({"kind": "job_pod", "job": job, "t": time.time(),
                    "pod": (p.get("metadata") or {}).get("name"),
                    "phase": st.get("phase"),
                    # pod-level reason (Evicted, ...) and why it never scheduled;
                    # reasons are enum-like, the free-text messages are not kept
                    "reason": st.get("reason"),
                    "unscheduled": (sched or {}).get("reason") if sched else None,
                    "containers": [{"name": cs.get("name"), "ready": cs.get("ready"),
                                    "restarts": cs.get("restartCount", 0),
                                    "terminated": _termination(cs.get("state")),
                                    "last_terminated": _termination(cs.get("lastState"))}
                                   for cs in st.get("containerStatuses") or []]})
    return out


# A driver pod that never starts (#834 review): a native sidecar whose
# startupProbe never passes is restarted forever and holds its pod in Init
# (phase Pending, not Failed), as does a pod no node takes. The driver wait
# fails on it at once, with the reason, instead of at MEASURE_WAIT_S.
#   DRIVER_PENDING_S  a pod still Pending this long after it was created.
#                     Run 11's k6 login initContainer took ~70 s for 1,000
#                     users (driver-k6load-lj2bw.log); image pulls on a new
#                     node add a few minutes.
#   DRIVER_INIT_RESTARTS  an init container (the sidecar) restarted this
#                     often while the pod is still Pending: each restart is a
#                     startup that failed for its whole probe budget.
DRIVER_PENDING_S = 600
DRIVER_INIT_RESTARTS = 2


def _ts(s: str | None) -> float | None:
    try:
        return calendar.timegm(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")) if s else None
    except ValueError:
        return None


def driver_pod_stuck(pod_list: dict, now: float, *, pending_s: float = DRIVER_PENDING_S,
                     init_restarts: int = DRIVER_INIT_RESTARTS) -> str | None:
    """Why a driver Job's pod will not start, or None. Reasons only (enum-like
    states and counts), never a container's message."""
    for p in pod_list.get("items") or []:
        st = p.get("status") or {}
        if st.get("phase") != "Pending":
            continue
        name = (p.get("metadata") or {}).get("name")
        for cs in st.get("initContainerStatuses") or []:
            n = cs.get("restartCount", 0)
            if n >= init_restarts:
                last = _termination(cs.get("lastState"))
                return (f"pod {name} is still Pending and its init container {cs.get('name')} "
                        f"restarted {n} times" + (f" (last: {last['reason']}, exit "
                                                  f"{last['exit_code']})" if last else "")
                        + ": a sidecar whose startupProbe never passes, or a crashing init step")
        created = _ts((p.get("metadata") or {}).get("creationTimestamp"))
        if created is not None and now - created >= pending_s:
            sched = next((c for c in st.get("conditions") or []
                          if c.get("type") == "PodScheduled" and c.get("status") != "True"), None)
            waiting = [f"{cs.get('name')} {((cs.get('state') or {}).get('waiting') or {}).get('reason')}"
                       for cs in (st.get("initContainerStatuses") or [])
                       + (st.get("containerStatuses") or [])
                       if (cs.get("state") or {}).get("waiting")]
            return (f"pod {name} is still Pending {int(now - created)} s after it was created"
                    + (f" (unscheduled: {sched.get('reason')})" if sched else "")
                    + (f" (waiting: {', '.join(waiting)})" if waiting else ""))
    return None


def job_pod_checks(recs: list[dict]) -> list[dict]:
    """A validity check per in-cluster Job pod: one whose container died
    (non-zero exit, a restart, phase Failed) makes the run INVALID, and the
    verdict says how it died. A pod still lingering for collect is a PASS."""
    out = []
    for r in recs:
        why = []
        if r.get("missing"):
            out.append({"check": f"{r.get('job')}.pod", "value": None,
                        "bar": "the deployed Job has a pod at collect", "result": "FAIL",
                        "why": f"no {r.get('job')} pod at collect: it never started, "
                               f"or it was deleted"})
            continue
        if r.get("phase") == "Failed":
            why.append("the pod Failed" + (f" ({r['reason']})" if r.get("reason") else ""))
        elif r.get("phase") in ("Pending", "Unknown"):
            why.append(f"the pod is {r['phase']} at collect"
                       + (f" ({r['unscheduled']})" if r.get("unscheduled") else "")
                       + (f" ({r['reason']})" if r.get("reason") else "")
                       + ": it did not run, or its node is unreachable")
        for c in r.get("containers") or []:
            for label, t in (("terminated", c.get("terminated")),
                             ("earlier run terminated", c.get("last_terminated"))):
                if t and t.get("exit_code") not in (0, None):
                    why.append(f"container {c.get('name')} {label}: {t.get('reason')} "
                               f"(exit {t.get('exit_code')}) at {t.get('finished_at')}")
            if c.get("restarts"):
                why.append(f"container {c.get('name')} restarted {c['restarts']}x")
        out.append({"check": f"{r.get('job')}.pod[{r.get('pod')}]",
                    "value": {"phase": r.get("phase"),
                              "containers": r.get("containers")},
                    "bar": "ran; no container death or restart",
                    "result": "FAIL" if why else "PASS",
                    **({"why": "; ".join(why)} if why else {})})
    return out


# ─── image digest gate (run 8: a pod on any other digest makes the run VOID) ─

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def image_digest_of(image_id: str | None) -> str | None:
    """`ghcr.io/x/snowplow@sha256:<hex>` (or `docker-pullable://…@sha256:…`)
    → `sha256:<hex>`; None when the status carries no pulled digest."""
    if not image_id:
        return None
    tail = str(image_id).rsplit("@", 1)[-1]
    return tail if DIGEST_RE.match(tail) else None


def image_digest_gate(pod_list: dict, expected: str, require_settled: bool = False) -> dict:
    """`kubectl get pods -l <snowplow> -o json` → one `image_digest` record.

    MATCH only when the expected digest is well formed, at least one live
    snowplow pod exists, and EVERY container status of EVERY live pod reports
    exactly that digest (read from status.containerStatuses[].imageID — what
    the kubelet pulled, not the tag the spec asked for). Anything else —
    a different digest, an empty imageID, no pods — is VOID. Pods already
    terminating (deletionTimestamp set: the rollout's leftovers) are recorded
    but not judged.
    """
    rec = {"kind": "image_digest", "expected": expected, "t": time.time(),
           "pods": [], "result": "VOID", "why": None}
    if not DIGEST_RE.match(expected or ""):
        rec["why"] = "expected digest is not sha256:<64 hex>"
        return rec
    live = 0
    bad = []
    for p in pod_list.get("items") or []:
        meta = p.get("metadata") or {}
        terminating = bool(meta.get("deletionTimestamp"))
        statuses = (p.get("status") or {}).get("containerStatuses") or []
        if not terminating:
            live += 1
            if not statuses:
                bad.append(f"{meta.get('name')}: no container status")
        for cs in statuses:
            d = image_digest_of(cs.get("imageID"))
            row = {"pod": meta.get("name"), "container": cs.get("name"),
                   "image": cs.get("image"), "image_id": cs.get("imageID"),
                   "digest": d, "terminating": terminating, "match": d == expected}
            rec["pods"].append(row)
            if not terminating and d != expected:
                bad.append(f"{meta.get('name')}/{cs.get('name')}: {d or 'no digest'}")
    leftovers = sorted({r["pod"] for r in rec["pods"] if r["terminating"]})
    rec["terminating"] = leftovers
    if live == 0:
        rec["why"] = "no live snowplow pod"
    elif bad:
        rec["why"] = "digest mismatch: " + "; ".join(bad)
    elif leftovers and require_settled:
        # Not a verdict yet: the old ReplicaSet's pod is still Terminating
        # (`rollout status` returns before it is gone). run606 waits, bounded.
        rec["result"] = "UNSETTLED"
        rec["why"] = "still Terminating: " + ", ".join(leftovers)
    else:
        rec["result"] = "MATCH"
    return rec


def digest_check(expected: str | None, gates: list[dict],
                 pods: list[dict] | None) -> dict | None:
    """Validity check over the gate record(s) and the before/after pod records.
    None when no digest is expected (unit callers)."""
    if not expected:
        return None
    c = {"check": "snowplow.image_digest", "bar": f"== {expected}"}
    seen = [g for g in gates if g.get("kind") == "image_digest"]
    if not seen:
        return dict(c, value=None, result="FAIL",
                    why="VOID: no image-digest gate record (the gate did not run)")
    if any(g.get("result") != "MATCH" or g.get("expected") != expected for g in seen):
        bad = next(g for g in seen if g.get("result") != "MATCH" or g.get("expected") != expected)
        return dict(c, value=[r.get("digest") for r in bad.get("pods") or []], result="FAIL",
                    why=f"VOID: {bad.get('why') or 'gate expected a different digest'}")
    for pr in pods or []:
        if pr.get("pod") is not None and pr.get("digest") != expected:
            return dict(c, value=pr.get("digest"), result="FAIL",
                        why=f"VOID: snowplow pod ({pr.get('phase')}) runs {pr.get('digest')}")
    return dict(c, value=expected, result="PASS")


def memory_report(mem: list[dict]) -> dict | None:
    """Summary of the memwatch trajectory (metadata only)."""
    rows = [m for m in mem if m.get("kind") == "mem" and m.get("heap_inuse_mb") is not None]
    if not rows:
        return None
    heap = [m["heap_inuse_mb"] for m in rows]
    sysm = [m.get("sys_mb") for m in rows if m.get("sys_mb") is not None]
    first_ready = next((m for m in rows if m.get("readyz") == 200), None)
    return {"samples": len(rows),
            "heap_inuse_mb_at_first_ready": (first_ready or {}).get("heap_inuse_mb"),
            "heap_inuse_mb_max": max(heap), "sys_mb_max": max(sysm) if sysm else None,
            "not_ready_samples": sum(1 for m in mem if m.get("kind") == "mem" and m.get("readyz") != 200),
            "heap_profiles": [m.get("profile") for m in mem if m.get("kind") == "heap_profile"]}


# ─── CPU profiles, GC and scheduler (run 12) ───────────────────────────────
#
# Report-only, with one validity bar: the storm window's profiles are the
# reason the profiler exists, so a run whose storm window they do not cover
# is INVALID. `cpu_storm_min_coverage` allows ~2 of the 20 planned 30 s
# profiles to be lost (a failed request, a restart's gap).
CPU_STORM_MIN_COVERAGE = 0.9


def _merged(iv: list[tuple[float, float]], gap: float = 0.0) -> list[tuple[float, float]]:
    out: list[list[float]] = []
    for a, b in sorted(iv):
        if out and a <= out[-1][1] + gap:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def _covered(iv: list[tuple[float, float]], a: float, b: float) -> float:
    return sum(max(0.0, min(y, b) - max(x, a)) for x, y in _merged(iv))


def cpu_profile_windows(mem: list[dict]) -> dict | None:
    """The CPU-profile plan the mem606 stream announced (mem_start cpu_plan),
    the windows it meant (anchored where memwatch anchored them: the first
    load_detected sample, else mem_start) and how much of each the written
    profiles cover. None when the stream announced no plan."""
    start = next((r for r in mem if r.get("kind") == "mem_start" and r.get("cpu_plan")), None)
    if start is None:
        return None
    plan = start["cpu_plan"]
    anchor = next((r["t"] for r in mem if r.get("kind") == "mem" and r.get("load_detected")),
                  start["t"])
    end_t = next((r["t"] for r in mem if r.get("kind") == "mem_end"),
                 max((r.get("t") or 0) for r in mem))
    profs = sorted((r for r in mem if r.get("kind") == "cpu_profile" and r.get("profile")),
                   key=lambda r: r["t_start"])
    iv = [(r["t_start"], r["t_end"]) for r in profs]

    def window(a, b):
        b = min(b, end_t)
        span = b - a
        return {"t0": a, "t1": b, "planned_s": round(max(span, 0.0), 1),
                "covered_s": round(_covered(iv, a, b), 1),
                "coverage": round(_covered(iv, a, b) / span, 3) if span > 0 else None}
    burst_at = plan.get("burst_at_s")
    return {"plan": plan, "anchor_t": anchor,
            "anchor_is": ("the load first seen (mem sample load_detected)"
                          if anchor != start["t"] else "mem606's start (no load was seen)"),
            "storm": window(anchor, anchor + plan["storm_s"]),
            "burst": (window(anchor + burst_at - plan["burst_pre_s"],
                             anchor + burst_at + plan["burst_post_s"])
                      if burst_at is not None else None),
            "profiles": [{"profile": r["profile"], "window": r["window"],
                          "t_start": r["t_start"], "t_end": r["t_end"]} for r in profs],
            "failed": [{"window": r.get("window"), "t_start": r.get("t_start"),
                        "error": r.get("error")}
                       for r in mem if r.get("kind") == "cpu_profile" and not r.get("profile")]}


def cpu_storm_check(win: dict | None) -> dict | None:
    if win is None:
        return None
    cov = win["storm"]["coverage"]
    c = {"check": "mem606.cpu_profiles_storm", "value": cov,
         "bar": f">= {CPU_STORM_MIN_COVERAGE} of the storm window "
                f"({win['storm']['planned_s']} s) covered by CPU profiles"}
    if cov is not None and cov >= CPU_STORM_MIN_COVERAGE:
        return dict(c, result="PASS")
    return dict(c, result="FAIL",
                why=f"the login storm's CPU profiles are missing: {win['storm']['covered_s']} s "
                    f"of {win['storm']['planned_s']} s covered, {len(win['failed'])} profile "
                    f"request(s) failed; they are the reason the profiler runs")


def _lat(ms: list[float]) -> dict | None:
    return ({"n": len(ms), "p50_ms": pct(ms, 50), "p95_ms": pct(ms, 95), "max_ms": max(ms)}
            if ms else None)


def latency_by_profiling(loads: list[tuple[float, float]], win: dict) -> dict:
    """Page loads (start epoch, ms) that started inside a CPU profile vs
    outside one: everywhere, and in the steady part of the run (outside the
    storm and burst windows), where only the periodic profiles fall and the
    load is the same inside and out — the comparison that shows whether
    profiling itself moved latency."""
    iv = _merged([(p["t_start"], p["t_end"]) for p in win["profiles"]])
    busy = [(win["storm"]["t0"] - 1e12, win["storm"]["t1"])] + (
        [(win["burst"]["t0"], win["burst"]["t1"])] if win["burst"] else [])

    def inside(t, ivs):
        return any(a <= t < b for a, b in ivs)
    out = {}
    for name, rows in (("all", loads), ("steady", [x for x in loads if not inside(x[0], busy)])):
        out[name] = {"inside_profile": _lat([ms for t, ms in rows if inside(t, iv)]),
                     "outside_profile": _lat([ms for t, ms in rows if not inside(t, iv)])}
    return out


def _pauses(rows: list[dict]) -> dict | None:
    ps = [p for r in rows for p in (r.get("gc_pauses_ms") or [])]
    return ({"n": len(ps), "p50_ms": pct(ps, 50), "p95_ms": pct(ps, 95),
             "p99_ms": pct(ps, 99), "max_ms": max(ps)} if ps else None)


GC_BUCKET_S = 60


def gc_over_time(rows: list[dict], bucket_s: float = GC_BUCKET_S) -> list[dict]:
    """Per minute of the run: the GC pauses' p95 / max, how many GCs, the
    GC CPU fraction at the minute's last sample (Go's GCCPUFraction is
    cumulative since the process started: its slope is the minute's share),
    heap allocated and the next GC target at their peak."""
    if not rows:
        return []
    t0 = rows[0]["t"]
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        buckets.setdefault(int((r["t"] - t0) // bucket_s), []).append(r)
    out = []
    for k in sorted(buckets):
        b = buckets[k]
        ps = [x for r in b for x in (r.get("gc_pauses_ms") or [])]
        frac = [r["gc_cpu_fraction"] for r in b if r.get("gc_cpu_fraction") is not None]
        heap = [r["heap_alloc_mb"] for r in b if r.get("heap_alloc_mb") is not None]
        nxt = [r["next_gc_mb"] for r in b if r.get("next_gc_mb") is not None]
        out.append({"t": t0 + k * bucket_s, "gcs": len(ps),
                    "pause_p95_ms": pct(ps, 95), "pause_max_ms": max(ps) if ps else None,
                    "gc_cpu_fraction": frac[-1] if frac else None,
                    "heap_alloc_mb_max": max(heap) if heap else None,
                    "next_gc_mb_max": max(nxt) if nxt else None})
    return out


def gc_sched_report(mem: list[dict], win: dict | None) -> dict | None:
    """GC pause distribution, GC CPU share and goroutines from the mem
    samples (each sample carries the pauses since the previous one), by
    window when the profiling plan defines them."""
    rows = [r for r in mem if r.get("kind") == "mem" and "gc_pauses_ms" in r]
    if not rows:
        return None
    phases = {"all": rows}
    if win is not None:
        st, bu = win["storm"], win["burst"]
        phases["storm"] = [r for r in rows if r["t"] < st["t1"]]
        if bu:
            phases["burst"] = [r for r in rows if bu["t0"] <= r["t"] < bu["t1"]]
        phases["steady"] = [r for r in rows if r["t"] >= st["t1"]
                            and not (bu and bu["t0"] <= r["t"] < bu["t1"])]
    gor = {k: [r["goroutines"] for r in v if r.get("goroutines") is not None]
           for k, v in phases.items()}
    frac = [r["gc_cpu_fraction"] for r in rows if r.get("gc_cpu_fraction") is not None]
    sched = [r["sched_latencies"] for r in rows if r.get("sched_latencies") is not None]
    return {"gc_pauses": {k: _pauses(v) for k, v in phases.items()},
            "gc_pauses_lost": sum(int(r.get("gc_pauses_lost") or 0) for r in rows),
            "gc_cpu_fraction_last": frac[-1] if frac else None,
            "over_time": gc_over_time(rows),
            "goroutines": {k: ({"p50": pct(v, 50), "max": max(v)} if v else None)
                           for k, v in gor.items()},
            "sched_latencies": (sched[-1] if sched else
                                f"not exposed: no {SCHED_LATENCY_KEY} expvar on snowplow's "
                                f"/debug/vars")}


# What a CPU profile costs snowplow, measured on kind (2026-10-22, snowplow
# 1.12.52, CPU limit 2): a fixed open-loop load (authenticated /debug/vars,
# 2000 req/s, ~0.36 cores) with and without one 30 s profile in flight, 10
# ABBA pairs per arm. Two clean arms; a third ran beside an unrelated busy
# process and is not counted. Reported with every verdict; the run's own
# inside / outside comparison is report.profiling.latency.
CPU_PROFILE_OVERHEAD_KIND = {
    "measured": "2026-10-22, kind, snowplow 1.12.52, 2000 req/s /debug/vars, 10 ABBA pairs x 2 arms",
    "cpu_overhead_pct_mean_by_arm": [0.67, -1.8],
    "cpu_overhead_pct_median_pair_by_arm": [0.93, -0.87],
    "per_pair_noise_pct": "-8.8 .. +9.1",
    "request_p95_ms": {"no_profile": [0.61, 0.63], "profiling": [0.62, 0.60]},
    "request_p99_ms": {"no_profile": [2.41, 4.62], "profiling": [7.4, 3.62]},
    "reading": "below the measurement's resolution (|mean| < 2 % at ~0.36 cores); "
               "p95 unchanged, p99 mixed",
}


def profiling_report(mem: list[dict], navs: list[dict],
                     k6_loads: list[tuple[float, float, dict]] | None = None) -> dict | None:
    win = cpu_profile_windows(mem)
    gc = gc_sched_report(mem, win)
    if win is None and gc is None:
        return None
    out = {"cpu": None, "gc_sched": gc, "overhead_measured_on_kind": CPU_PROFILE_OVERHEAD_KIND}
    if win is not None:
        by_window = {}
        for p in win["profiles"]:
            by_window[p["window"]] = by_window.get(p["window"], 0) + 1
        out["cpu"] = dict(win, written_by_window=by_window,
                          # back-to-back profiles as one span (a request's
                          # turnaround apart)
                          profiled_windows=[list(x) for x in _merged(
                              [(p["t_start"], p["t_end"]) for p in win["profiles"]], gap=1.0)])
        out["latency"] = {
            "page_loads": latency_by_profiling(
                [(n["start"], n["wall_ms"]) for n in navs if n.get("start") is not None], win),
            **({"k6_page_loads": latency_by_profiling([(t, ms) for t, ms, _ in k6_loads], win)}
               if k6_loads else {})}
    return out


# ─── Run 8 (#710 rulings, issuecomment-6187374111) ─────────────────────────
#
# Key names as given by architect-ch from the CODE (PR #719 @ 7d8cb8de and
# main eb055b77), not notes. #719's rework, #720 and PR-2b are NOT BUILT yet:
# a key that no sample carries is "not exposed" and FAILS — it never reads as
# a pass. Every check names its key and the snowplow build it was read from.
RUN8_KEYS = {
    "fill_over_ceiling": "snowplow_apistage_content_fill_admission_over_ceiling_bytes",
    "fill_over_ceiling_peak": "snowplow_apistage_content_fill_admission_over_ceiling_bytes_peak",
    "nested_over_ceiling": "snowplow_nested_resolve_admission_over_ceiling_bytes",
    "nested_over_ceiling_peak": "snowplow_nested_resolve_admission_over_ceiling_bytes_peak",
    "stale_to_push": "snowplow_apistage_stale_to_push_ms",
    "stale_overdue": "snowplow_apistage_stale_overdue_total",
    "admission_wait": "snowplow_apistage_content_fill_admission_wait_ms",
    "dirty_to_fresh_p95": "snowplow_refresher_dirty_to_fresh_ms_p95",
    "dirty_to_fresh_max": "snowplow_refresher_dirty_to_fresh_ms_max",
    "build_info": "snowplow_build_info",
    # Q2 — stale serves and their pushes (#710 PR-2b). Names and shapes
    # checked against the /debug/vars the 1.12.50 tree (e47cd719) serves:
    # tests/fixtures/debug_vars_keys_1.12.50.json.
    "stale_serve": "snowplow_apistage_stale_serve_total",            # {"known_stale": n}
    "stale_serve_age": "snowplow_apistage_stale_serve_age_ms",       # histogram
    "stale_push": "snowplow_apistage_stale_push_total",
    "stale_push_unarmed": "snowplow_apistage_stale_push_unarmed_total",
    "stale_withdrawn": "snowplow_apistage_stale_serve_withdrawn_total",  # {"unarmable": n}
    "resolved_cache": "snowplow_resolved_cache",                     # stat -> value map
    # Q3 — users never queue: per-class admission (background/customer/debt).
    "admission_queued": "snowplow_apistage_content_fill_admission_queued",        # class -> gauge
    "admission_timeout": "snowplow_apistage_content_fill_admission_timeout_total",  # class -> n
    "admission_weight": "snowplow_apistage_content_fill_admission_inflight_weight",
    "admission_weight_peak": "snowplow_apistage_content_fill_admission_inflight_weight_peak",
    "customer_inflight": "snowplow_customer_resolve_inflight",
    # Q4 — #740 viewer-gated retries and parked debt.
    "debt_open": "snowplow_apistage_stale_debt_open",
    "debt_parked": "snowplow_apistage_stale_debt_parked",
    "debt_oldest_age": "snowplow_apistage_stale_debt_oldest_age_ms",
    "debt_retries": "snowplow_apistage_stale_debt_retries_total",
    "retry_total": "snowplow_stale_retry_total",                 # kind -> source -> n
    "retry_chains": "snowplow_stale_retry_chains_active",
    "retry_viewers": "snowplow_stale_retry_viewers",
    "retry_backoff": "snowplow_stale_retry_backoff_ms",
    "retry_abandoned": "snowplow_stale_retry_abandoned_total",   # {"no_viewer", "evicted"}
    "retry_resumed": "snowplow_stale_retry_resumed_total",
}
# Members of the snowplow_resolved_cache map the Q2 report reads.
RUN8_RESOLVED_CACHE_STATS = ("hit_total", "stale_served_total", "stale_served_age_ms_max",
                             "known_stale_served_total")
# The /debug/vars prefixes memwatch must capture so every key above is seen.
# run606's MEM_VARS_PREFIXES default IS this tuple (printed by `prefixes`), and
# `check-prefixes` refuses a run-8 run whose prefixes miss any RUN8_KEYS value.
RUN8_VARS_PREFIXES = (
    "snowplow_apistage_content_fill",     # PR-0 family + #719 admission_*
    "snowplow_nested_resolve_",           # #720 nested gate
    "snowplow_apistage_stale_",           # PR-2b stale_to_push / overdue / debt
    "snowplow_refresher_",                # dirty_to_fresh windowed gauges + run-9 counters
    "snowplow_deps_dirty_mark_",          # run 9 (#765): marks per event
    "snowplow_resolved_cache",
    "snowplow_informer_dispatch_shape",
    "snowplow_build_info",
    "snowplow_customer_resolve_inflight",  # Q3
    "snowplow_stale_retry_",               # Q4 (#740)
    # #817: when does an identity-bound key rotate? (publish_seq, subgen
    # bumps / epoch, the refresher's drift declines, the armed-key gauges)
    "snowplow_rbac_",
    "snowplow_l1_identity_class_drift",
    "snowplow_refresh_broadcaster",
    # run 12: why are first visits cold? learned identity classes (#262),
    # prewarm completion and engine, phase-1 units and readiness exit
    # (expvar keys at 1.12.52: cache/learned_identities.go,
    # cache/prewarm_complete_metric.go, dispatchers/prewarm_engine_metrics.go,
    # dispatchers/phase1_walk_pagination_metrics.go, phase1_readiness_exit.go)
    "snowplow_learned_classes",
    "snowplow_learned_clientconfig_secrets",   # the learned-classes alarm's denominator
    "snowplow_prewarm_complete",
    "snowplow_phase1_units",
    "snowplow_phase1_readiness_exit",
    "snowplow_prewarm_engine",
)
# The identity-class inputs whose movement rotates every identity-bound key
# that folds them (rbac/identity_class.go IdentityClassOf), and the refresher's
# own count of cells it found minted under a stale class.
ROTATION_KEYS = ("snowplow_rbac_publish_seq", "snowplow_rbac_subgen_bumps_total",
                 "snowplow_rbac_subgen_epoch", "snowplow_l1_identity_class_drift_declined_total")


# Carried in every verdict's report, so a number read across runs comes with
# what changed under it.
HARNESS_NOTES = (
    "run 10+ (#817): the driver's offered load and scheduling lag are NOT comparable "
    "with runs <= 9. There, closing a /refreshes viewer blocked until its reader "
    "woke (next frame or the 90 s read timeout), on every navigation's re-arm and "
    "for 31 min at close_all; each blocked close held a navigation worker and left "
    "that tab unarmed. Viewers now also re-arm when their cell's refresh key rotates.",
)


def rotation_report(mem: list[dict]) -> dict | None:
    """#817 report-only: every mem sample at which an identity-class input
    moved, with the values. These counters are GLOBAL: a change dates a
    CANDIDATE rotation for some identity, not proof that a given user's key
    rotated (the probe's own key_rotated records are that proof). An armed
    /refreshes key goes dead when its cell's key rotates (the armed set is
    frozen per connection); this dates the candidates so a viewer that
    stopped being pushed can be matched to one."""
    rows = sorted((m for m in mem if m.get("kind") == "mem"
                   and any(k in (m.get("extra") or {}) for k in ROTATION_KEYS)),
                  key=lambda m: m.get("t") or 0)
    if not rows:
        return None
    changes, prev = [], None
    for m in rows:
        cur = {k: _num((m.get("extra") or {}).get(k)) for k in ROTATION_KEYS}
        if prev is not None and cur != prev:
            changes.append({"t": m.get("t"), **{k: v for k, v in cur.items()
                                                 if v != prev.get(k)}})
        prev = cur
    first = {k: _num((rows[0].get("extra") or {}).get(k)) for k in ROTATION_KEYS}
    return {"first": first, "last": prev, "changes": changes}
# Bars: the #710 / #740 rulings, written here — never derived from the
# instrument under test.
STALE_TO_PUSH_P95_BAR_MS = 3000
STALE_TO_PUSH_MAX_BAR_MS = 10000
STALE_TO_PUSH_GOAL_MS = 1000
STALE_PUSH_UNARMED_BAR = 0
STALE_OVERDUE_BAR = 0
BYPASS_QUEUED_BAR = 0
BYPASS_TIMEOUT_BAR = 0
BURST_WINDOW_S = 120.0


# Run 9 (#765 spec, issuecomment-6266045747): what run 8 could not measure —
# dirty marks per event, the refresher's own throughput counters, and fills
# by reason. Names checked against the 1.12.50 /debug/vars
# (tests/fixtures/debug_vars_keys_1.12.50.json).
RUN9_KEYS = {
    "dirty_mark": "snowplow_deps_dirty_mark_attribution",   # by_entry_class, events_total, …
    "enqueue": "snowplow_refresher_enqueue_total",
    "completed": "snowplow_refresher_completed_total",
    "floored": "snowplow_refresher_floored_total",
    "queue_depth": "snowplow_refresher_queue_depth",
    "remarked": "snowplow_refresher_dirty_ended_unfresh_remarked_total",
    "d2f_samples": "snowplow_refresher_dirty_to_fresh_samples_total",
    "fills_by_reason": "snowplow_apistage_content_fill_total",  # {absent, known_stale, refresh_forced}
}


def prefixes_missing(prefixes) -> list[str]:
    """The RUN8_KEYS / RUN9_KEYS values no prefix in `prefixes` captures."""
    ps = tuple(p for p in prefixes if p)
    need = set(RUN8_KEYS.values()) | set(RUN9_KEYS.values())
    return sorted(k for k in need if not ps or not k.startswith(ps))


def _hist_cum(h: dict, bound_ms: int) -> int | None:
    b = (h or {}).get("buckets") or {}
    v = b.get(f"le_{bound_ms}")
    return int(v) if v is not None else None


def hist_fraction_le(h: dict, bound_ms: int) -> float | None:
    """Fraction of observations <= bound (cumulative buckets)."""
    count = (h or {}).get("count") or 0
    cum = _hist_cum(h, bound_ms)
    if not count or cum is None:
        return None
    return round(cum / count, 4)


def hist_p95_bound(h: dict) -> str | None:
    """The bucket bound the p95 falls under: "<= 2500", or "> 10000"."""
    count = (h or {}).get("count") or 0
    b = (h or {}).get("buckets") or {}
    if not count:
        return None
    bounds = sorted(int(k[3:]) for k in b if k.startswith("le_") and k[3:].isdigit())
    for x in bounds:
        if b[f"le_{x}"] >= 0.95 * count:
            return f"<= {x}"
    return f"> {bounds[-1]}" if bounds else None


def _parse_quantity(q) -> int | None:
    if q is None:
        return None
    q = str(q)
    units = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4,
             "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3}
    for u, m in units.items():
        if q.endswith(u):
            try:
                return int(float(q[:-len(u)]) * m)
            except ValueError:
                return None
    try:
        return int(q)
    except ValueError:
        return None


def _extra_values(mem: list[dict], key: str, t0=None, t1=None) -> list:
    out = []
    for m in mem:
        if m.get("kind") != "mem" or key not in (m.get("extra") or {}):
            continue
        t = m.get("t") or 0
        if t0 is not None and not (t0 <= t < t1):
            continue
        out.append(m["extra"][key])
    return out


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _window_peaks(mem, tops, t0, t1) -> dict:
    ms = [m for m in mem if m.get("kind") == "mem" and t0 <= (m.get("t") or 0) < t1]
    ts = [t for t in tops if t0 <= (t.get("t") or 0) < t1]
    heap = [m["heap_inuse_mb"] for m in ms if m.get("heap_inuse_mb") is not None]
    sysm = [m["sys_mb"] for m in ms if m.get("sys_mb") is not None]
    rss = [t["rss_bytes"] for t in ts if t.get("rss_bytes") is not None]
    out = {"samples": len(ms), "heap_inuse_mb_peak": max(heap) if heap else None,
           "sys_mb_peak": max(sysm) if sysm else None,
           "rss_bytes_peak": max(rss) if rss else None}
    for gate in ("fill", "nested"):
        vals = [_num(v) for k in (f"{gate}_over_ceiling", f"{gate}_over_ceiling_peak")
                for v in _extra_values(mem, RUN8_KEYS[k], t0, t1)]
        vals = [v for v in vals if v is not None]
        out[f"{gate}_over_ceiling_bytes_peak"] = max(vals) if vals else None
    return out


def _cumulative_points(sample: dict) -> dict:
    """Every cumulative-since-boot number in one mem sample: memstats NumGC,
    each captured *_total / *_peak scalar, map sums of *_total maps, and each
    histogram's count. A pod restart resets all of them."""
    out = {}
    if sample.get("num_gc") is not None:
        out["memstats.NumGC"] = sample["num_gc"]
    for k, v in (sample.get("extra") or {}).items():
        if isinstance(v, dict) and "count" in v and "buckets" in v:
            out[k + ".count"] = v.get("count") or 0
        elif k.endswith(("_total", "_peak")):
            if _num(v) is not None:
                out[k] = v
            elif isinstance(v, dict):
                vals = [x for x in v.values() if _num(x) is not None]
                if vals:
                    out[k + ".sum"] = sum(vals)
        elif isinstance(v, dict) and k == RUN8_KEYS["admission_wait"]:
            for cls, hh in v.items():
                if isinstance(hh, dict) and "count" in hh:
                    out[f"{k}.{cls}.count"] = hh.get("count") or 0
    return out


def detect_restart(mem: list[dict], pods: list[dict] | None) -> dict | None:
    """A snowplow restart during the run, from ANY of: a drop in a cumulative
    counter between consecutive samples, a change of build_info, or the pod's
    restartCount. Returns {t, signals, ...} or None."""
    signals, t_first = [], None
    rows = sorted((m for m in mem if m.get("kind") == "mem" and m.get("vars_status", 200) == 200),
                  key=lambda m: m.get("t") or 0)
    prev, prev_build = None, None
    for m in rows:
        cur = _cumulative_points(m)
        if prev is not None:
            drops = [k for k, v in cur.items() if k in prev and v < prev[k]]
            if drops:
                signals.append({"t": m.get("t"), "signal": "counter reset",
                                "keys": sorted(drops)[:8]})
                t_first = t_first if t_first is not None else m.get("t")
        build = (m.get("extra") or {}).get(RUN8_KEYS["build_info"])
        if build is not None and prev_build is not None and build != prev_build:
            signals.append({"t": m.get("t"), "signal": "build_info changed"})
            t_first = t_first if t_first is not None else m.get("t")
        prev = cur or prev
        prev_build = build if build is not None else prev_build
    if pods:
        before = next((x for x in pods if x.get("phase") == "before"), None)
        after = next((x for x in pods if x.get("phase") == "after"), None)
        if before and after and (after.get("restarts") or 0) > (before.get("restarts") or 0):
            ls = (after.get("last_state") or {}).get("terminated") or {}
            signals.append({"t": ls.get("finishedAt"), "signal": "pod restartCount",
                            "reason": ls.get("reason"), "exit_code": ls.get("exitCode")})
            if t_first is None and ls.get("finishedAt"):
                # restartCount is the ONLY signal (e.g. no sample landed after
                # the restart): the termination time is the restart time, so
                # the "peak before the restart" fields are still computed.
                import datetime as _dt
                try:
                    t_first = _dt.datetime.fromisoformat(
                        str(ls["finishedAt"]).replace("Z", "+00:00")).timestamp()
                except ValueError:
                    pass
    if not signals:
        return None
    return {"t": t_first, "signals": signals}


def _series(mem: list[dict], key: str) -> list:
    """The values of one captured expvar, in sample-time order."""
    rows = sorted((m for m in mem if m.get("kind") == "mem" and key in (m.get("extra") or {})),
                  key=lambda m: m.get("t") or 0)
    return [m["extra"][key] for m in rows]


def _member(v, name):
    return _num(v.get(name)) if isinstance(v, dict) else None


def _first_last_delta(vals: list) -> dict:
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"first": None, "last": None, "delta": None}
    return {"first": vals[0], "last": vals[-1],
            "delta": vals[-1] - vals[0] if len(vals) >= 2 else None}


def _max(vals):
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


def _map_delta(series: list) -> dict:
    """Per-member first→last delta of a flat (or kind→source nested) count map."""
    out = {}
    maps = [s for s in series if isinstance(s, dict)]
    if not maps:
        return out
    first, last = maps[0], maps[-1]
    for k, v in last.items():
        if isinstance(v, dict):
            out[k] = _map_delta([first.get(k) or {}, v]) if len(maps) >= 2 else dict(v)
        elif _num(v) is not None:
            out[k] = v - (_num(first.get(k)) or 0) if len(maps) >= 2 else v
    return out


def _q2_stale(mem: list[dict], chk) -> dict:
    """Q2 — stale serves: share of customer hits and age (resolved cache), the
    apistage stale-serve/push family; push_unarmed is GATED at 0."""
    out = {}
    key = RUN8_KEYS["resolved_cache"]
    rc = [v for v in _series(mem, key) if isinstance(v, dict)]
    missing = [s for s in RUN8_RESOLVED_CACHE_STATS if not any(s in v for v in rc)]
    if missing:
        chk("run8.stale_served.share_of_hits", key, None, "reported", "FAIL",
            "not exposed: " + ", ".join(missing))
    else:
        hit = _first_last_delta([_member(v, "hit_total") for v in rc])
        ss = _first_last_delta([_member(v, "stale_served_total") for v in rc])
        share = (round(ss["delta"] / hit["delta"], 4)
                 if hit["delta"] and ss["delta"] is not None else None)
        reasons = sorted({k for v in rc for k in v
                          if k.startswith("known_stale_served_") and k.endswith("_total")
                          and k != "known_stale_served_total"})
        out["resolved_cache"] = {
            "hit_total_delta": hit["delta"], "stale_served_total_delta": ss["delta"],
            "stale_served_share_of_hits": share,
            # windowed (~one OTLP interval) gauge: the max over every sample
            "stale_served_age_ms_max": _max([_member(v, "stale_served_age_ms_max") for v in rc]),
            "known_stale_served_total_delta":
                _first_last_delta([_member(v, "known_stale_served_total") for v in rc])["delta"],
            "known_stale_served_by_reason_delta":
                {r: _first_last_delta([_member(v, r) for v in rc])["delta"] for r in reasons}}
        chk("run8.stale_served.share_of_hits", key, share, "reported (no bar)", "PASS",
            None if share is not None else "no customer hits between samples")
    for name, k, member in (("apistage_stale_serve", "stale_serve", "known_stale"),
                            ("apistage_stale_push", "stale_push", None),
                            ("apistage_stale_withdrawn", "stale_withdrawn", "unarmable")):
        key = RUN8_KEYS[k]
        s = _series(mem, key)
        vals = [_member(v, member) if member else _num(v) for v in s]
        if not [v for v in vals if v is not None]:
            chk(f"run8.{name}_total", key, None, "reported", "FAIL", "not exposed")
            continue
        out[name + "_total"] = _first_last_delta(vals)
        chk(f"run8.{name}_total", key, out[name + "_total"]["delta"], "reported", "PASS")
    key = RUN8_KEYS["stale_serve_age"]
    hs = [h for h in _series(mem, key) if isinstance(h, dict)]
    if not hs:
        chk("run8.stale_serve_age_ms", key, None, "reported", "FAIL", "not exposed")
    else:
        out["stale_serve_age_ms"] = {"count": hs[-1].get("count"),
                                     "p95_bucket": hist_p95_bound(hs[-1]),
                                     "max_ms": hs[-1].get("max_ms")}
        chk("run8.stale_serve_age_ms", key, out["stale_serve_age_ms"], "reported", "PASS")
    key = RUN8_KEYS["stale_push_unarmed"]
    vals = [v for v in (_num(x) for x in _series(mem, key)) if v is not None]
    if not vals:
        chk("run8.stale_push_unarmed_total", key, None, f"== {STALE_PUSH_UNARMED_BAR}",
            "FAIL", "not exposed")
    else:
        out["stale_push_unarmed_total"] = vals[-1]
        chk("run8.stale_push_unarmed_total", key, vals[-1], f"== {STALE_PUSH_UNARMED_BAR}",
            "PASS" if vals[-1] <= STALE_PUSH_UNARMED_BAR else "FAIL",
            "a stale serve pushed with no armed subscriber: the user is never told")
    return out


_BYPASS = ("customer", "debt")


def _q3_admission(mem: list[dict], chk) -> dict:
    """Q3 — users never queue. Per class (background/customer/debt), all from
    /debug/vars maps: queued (gauge), timeouts, admissions (wait-histogram
    count: a bypass flight records one 0 ms wait at admit)."""
    out = {}
    kq, kt, kw = (RUN8_KEYS["admission_queued"], RUN8_KEYS["admission_timeout"],
                  RUN8_KEYS["admission_wait"])
    qs = [v for v in _series(mem, kq) if isinstance(v, dict)]
    ts = [v for v in _series(mem, kt) if isinstance(v, dict)]
    ws = [v for v in _series(mem, kw) if isinstance(v, dict)]
    note = ("structural: a bypass class is admitted before it can enqueue, so 0 is the "
            "expected reading, not a detection; the user-side evidence is nav latency")
    if not qs or not all(c in qs[-1] for c in _BYPASS):
        chk("run8.users_never_queue.queued", kq, None, f"== {BYPASS_QUEUED_BAR}", "FAIL",
            "not exposed")
    else:
        mx = {c: _max([_member(q, c) for q in qs]) for c in ("background",) + _BYPASS}
        out["queued_max"] = mx
        bad = any((mx[c] or 0) > BYPASS_QUEUED_BAR for c in _BYPASS)
        chk("run8.users_never_queue.queued", kq, {c: mx[c] for c in _BYPASS},
            f"== {BYPASS_QUEUED_BAR}", "FAIL" if bad else "PASS", note)
    if not ts or not all(c in ts[-1] for c in _BYPASS):
        chk("run8.users_never_queue.timeout", kt, None, f"== {BYPASS_TIMEOUT_BAR}", "FAIL",
            "not exposed")
    else:
        last = {c: _member(ts[-1], c) for c in ("background",) + _BYPASS}
        out["timeout_total"] = last
        bad = any((last[c] or 0) > BYPASS_TIMEOUT_BAR for c in _BYPASS)
        chk("run8.users_never_queue.timeout", kt, {c: last[c] for c in _BYPASS},
            f"== {BYPASS_TIMEOUT_BAR}", "FAIL" if bad else "PASS", note)
    if ws:
        last = ws[-1]
        adm = {c: (last.get(c) or {}).get("count") for c in ("background",) + _BYPASS}
        bg = last.get("background") or {}
        lowest = _hist_cum(bg, 10)
        out["bypass_vs_throttled"] = {
            "customer_bypass_admissions": adm["customer"],
            "debt_bypass_admissions": adm["debt"],
            "background_admissions": adm["background"],
            # background flights that waited past the lowest bucket (10 ms)
            "background_waited_over_10ms": (bg.get("count") - lowest
                                            if bg.get("count") is not None and lowest is not None
                                            else None),
            "background_timeouts": (out.get("timeout_total") or {}).get("background"),
            "per_class_wait": {c: {"count": (last.get(c) or {}).get("count"),
                                   "p95_bucket": hist_p95_bound(last.get(c) or {}),
                                   "max_ms": (last.get(c) or {}).get("max_ms")}
                               for c in ("background",) + _BYPASS},
            "note": ("a customer/debt max_ms > 0 is a queued background flight a user "
                     "joined (promoted, admitted at once), timed from the leader's start")}
        chk("run8.admission.bypass_vs_throttled", kw, out["bypass_vs_throttled"],
            "reported (no bar)", "PASS")
    for k in ("customer_inflight", "admission_weight", "admission_weight_peak"):
        key = RUN8_KEYS[k]
        vals = [_num(v) for v in _series(mem, key)]
        if k == "customer_inflight":
            vals += [_num(m.get("customer_resolve_inflight")) for m in mem
                     if m.get("kind") == "mem"]
        out[k + "_max"] = _max(vals)
        if out[k + "_max"] is None:
            chk(f"run8.{k}", key, None, "reported", "FAIL", "not exposed")
    return out


def _q4_retry(mem: list[dict], chk, overdue_vals: list) -> dict:
    """Q4 — #740: debts parked (nobody viewing) vs overdue (viewed, late), and
    the viewer-gated retry engine. overdue is gated by the caller."""
    out = {"stale_overdue_total_last": overdue_vals[-1] if overdue_vals else None}
    for k in ("debt_open", "debt_parked", "debt_oldest_age", "retry_chains",
              "retry_viewers", "retry_backoff"):
        key = RUN8_KEYS[k]
        vals = [_num(v) for v in _series(mem, key)]
        vals = [v for v in vals if v is not None]
        if not vals:
            chk(f"run8.740.{k}", key, None, "reported", "FAIL", "not exposed")
            continue
        out[k] = {"max": max(vals), "last": vals[-1]}
    for k in ("debt_retries", "retry_resumed"):
        key = RUN8_KEYS[k]
        vals = [_num(v) for v in _series(mem, key)]
        if not [v for v in vals if v is not None]:
            chk(f"run8.740.{k}", key, None, "reported", "FAIL", "not exposed")
            continue
        out[k] = _first_last_delta(vals)
    for k in ("retry_total", "retry_abandoned"):
        key = RUN8_KEYS[k]
        s = _series(mem, key)
        if not [v for v in s if isinstance(v, dict)]:
            chk(f"run8.740.{k}", key, None, "reported", "FAIL", "not exposed")
            continue
        out[k + "_delta"] = _map_delta(s)
    # Viewer gate. snowplow_stale_retry_total counts retries SCHEDULED
    # (stale_retry.go:227-231, scheduleLocked), not attempts run; each wake
    # re-checks for a viewer and ABANDONS the chain when there is none
    # (stale_retry.go:258-262, abandoned_total{no_viewer}). So "retries rose
    # while no viewer was sampled" is NOT evidence of retries run without a
    # viewer (run 8's 84 "suspect intervals" were exactly that misreading).
    # What /debug/vars can show is scheduled vs abandoned; an attempt run
    # without a viewer is only visible in the logs.
    sched = (out.get("retry_total_delta") or {}).get("genuine") or {}
    aband = out.get("retry_abandoned_delta") or {}
    n_sched = sum(v for v in sched.values() if _num(v) is not None) if sched else None
    out["viewer_gate"] = {
        "genuine_retries_scheduled": n_sched,
        "abandoned_no_viewer": aband.get("no_viewer"),
        "abandoned_evicted": aband.get("evicted"),
        "viewers_max": (out.get("retry_viewers") or {}).get("max"),
        "note": ("stale_retry_total counts retries SCHEDULED, not run; a wake with no "
                 "viewer is abandoned (abandoned_total{no_viewer}). An attempt run "
                 "without a viewer is not observable here: read the snowplow logs.")}
    parked = (out.get("debt_parked") or {}).get("max")
    chk("run8.740.parked_vs_overdue", RUN8_KEYS["debt_parked"],
        {"parked_max": parked, "overdue_total": out["stale_overdue_total_last"]},
        "reported (parked is NOT overdue; overdue gated separately)",
        "PASS" if parked is not None else "FAIL", None if parked is not None else "not exposed")
    return out


def _run9_refresh(mem: list[dict], chk) -> dict:
    """Run 9 (#765): dirty marks per event, refresher throughput and fills by
    reason over the measured window (first → last sample). Report-only; a key
    no sample carries is "not exposed" and FAILS."""
    out = {}
    rows = sorted((m for m in mem if m.get("kind") == "mem"), key=lambda m: m.get("t") or 0)
    dt = (rows[-1]["t"] - rows[0]["t"]) if len(rows) >= 2 else None
    key = RUN9_KEYS["dirty_mark"]
    dm = [v for v in _series(mem, key) if isinstance(v, dict)]
    if not dm:
        chk("run9.dirty_marks_per_event", key, None, "reported", "FAIL", "not exposed")
    else:
        ev = _first_last_delta([_num(v.get("events_total")) for v in dm])
        cls = _map_delta([v.get("by_entry_class") or {} for v in dm])
        marks = sum(x for x in cls.values() if _num(x) is not None) if cls else None
        per = (round(marks / ev["delta"], 2) if ev["delta"] and marks is not None else None)
        out["dirty_marks"] = {"events_delta": ev["delta"], "marks_delta": marks,
                              "marks_per_event": per,
                              "marks_per_event_by_class": (
                                  {c: round(n / ev["delta"], 3) for c, n in cls.items()}
                                  if ev["delta"] else None),
                              "submit_source_delta": _map_delta(
                                  [v.get("submit_source") or {} for v in dm])}
        chk("run9.dirty_marks_per_event", key, per, "reported (no bar)", "PASS",
            None if per is not None else "no dirty-mark events between samples")
    thr = {}
    for k in ("enqueue", "completed", "floored", "remarked", "d2f_samples"):
        key = RUN9_KEYS[k]
        vals = [_num(v) for v in _series(mem, key)]
        if not [v for v in vals if v is not None]:
            chk(f"run9.refresher.{k}", key, None, "reported", "FAIL", "not exposed")
            continue
        d = _first_last_delta(vals)
        thr[k] = dict(d, per_s=round(d["delta"] / dt, 3) if d["delta"] is not None and dt else None)
    out["refresher"] = thr
    key = RUN9_KEYS["queue_depth"]
    q = [v for v in (_num(x) for x in _series(mem, key)) if v is not None]
    if not q:
        q = [v for v in (_num(m.get("refresher_queue_depth")) for m in rows) if v is not None]
    if not q:
        chk("run9.refresher.queue_depth", key, None, "reported", "FAIL", "not exposed")
    else:
        out["queue_depth"] = {"p50": pct(q, 50), "p90": pct(q, 90), "max": max(q)}
    key = RUN9_KEYS["fills_by_reason"]
    fr = _series(mem, key)
    if not [v for v in fr if isinstance(v, dict)]:
        chk("run9.fills_by_reason", key, None, "reported", "FAIL", "not exposed")
    else:
        out["fills_by_reason_delta"] = _map_delta(fr)
    return out


# Checks that only mean something once a viewer existed and a stale serve
# happened (#764). Without that, a PASS there is "never ran", not "healthy".
EXERCISE_DEPENDENT_CHECKS = ("run8.stale_to_push.p95", "run8.stale_to_push.max",
                             "run8.stale_push_unarmed_total", "run8.stale_overdue_total",
                             "run8.740.parked_vs_overdue")


def client_delivery(fresh: list[dict] | None, push_refetches: list[dict] | None,
                    viewer_stats: list[dict] | None) -> dict:
    """What the CLIENTS observed of #710's push path (#764): streams opened,
    `refresh` frames received, refetches they triggered, and armed probe
    samples whose push arrived (push_ms set). Server counters are not used:
    snowplow_stale_retry_viewers counts viewers on FAILING retry chains only,
    so it reads 0 in a healthy run with 1000 armed tabs."""
    vs = viewer_stats or []
    pr = push_refetches or []
    fr = [f for f in (fresh or []) if f.get("armed")]
    return {"streams_opened": sum(int(v.get("streams_opened") or 0) for v in vs),
            "frames": sum(int(v.get("frames") or 0) for v in vs),
            "push_refetches": len(pr),
            "probe_armed_samples": len(fr),
            # #817 re-gate: delivery to a PASSIVE tab, not to the polling probe
            # (which re-arms on every poll); the probe's own count is reported
            "probe_push_samples": sum(1 for f in fr if passive_push(f)),
            "probe_push_samples_polling": sum(1 for f in fr if f.get("push_ms") is not None)}


def _exercised_710_740(q2: dict, q4: dict, chk, checks: list[dict],
                       delivery: dict) -> dict:
    """Did the #710 stale-then-push path run END TO END? Exercised requires
    BOTH a stale serve (snowplow's apistage_stale_serve_total) AND client-
    observed delivery: viewers opened streams, `refresh` frames reached them
    and triggered refetches, and the armed probe saw pushes. Anything less
    turns every dependent PASS into INCONCLUSIVE (a FAIL stays a FAIL)."""
    serves = (q2.get("apistage_stale_serve_total") or {}).get("last")
    missing = []
    if not serves:
        missing.append("no stale serve (apistage_stale_serve_total 0)")
    if not delivery["streams_opened"]:
        missing.append("no client /refreshes stream")
    if not (delivery["frames"] and delivery["push_refetches"]):
        missing.append("no refresh frame reached a client (viewer_stats.frames / push_refetch)")
    if not delivery["probe_push_samples"]:
        missing.append("no armed probe sample received a push a passive tab would get "
                       "(control_push_ms, or push_ms on arm generation 0)")
    ok = not missing
    out = {"stale_serve_total_last": serves, **delivery,
           "server_retry_viewers_max": (q4.get("retry_viewers") or {}).get("max"),
           "exercised": ok, "missing": missing}
    if not ok:
        why = ("NOT EXERCISED (#764): " + "; ".join(missing)
               + " — this PASS would mean 'never ran'")
        for c in checks:
            if c["check"] in EXERCISE_DEPENDENT_CHECKS and c["result"] == "PASS":
                c["result"] = "INCONCLUSIVE"
                c["why"] = why + (f" ({c['why']})" if c.get("why") else "")
    chk("run8.710_740_exercised", RUN8_KEYS["stale_serve"], out,
        "a stale serve AND client-observed delivery",
        "PASS" if ok else "INCONCLUSIVE", None if ok else "; ".join(missing))
    return out


def run8_report(navs: list[dict], mem: list[dict], tops: list[dict],
                pods: list[dict] | None, *, fresh: list[dict] | None = None,
                push_refetches: list[dict] | None = None,
                viewer_stats: list[dict] | None = None) -> tuple[dict, list[dict]]:
    """(report, checks). Checks: PASS / FAIL / INCONCLUSIVE."""
    checks: list[dict] = []
    builds = [m["extra"].get(RUN8_KEYS["build_info"]) for m in mem
              if m.get("kind") == "mem" and RUN8_KEYS["build_info"] in (m.get("extra") or {})]
    build = builds[-1] if builds else None

    def chk(name, key, value, bar, result, why=None):
        c = {"check": name, "key": key, "value": value, "bar": bar,
             "result": result, "build": build}
        if why:
            c["why"] = why
        checks.append(c)

    # ── memory under the two bursts ──────────────────────────────────────
    limit = None
    for pr in pods or []:
        limit = _parse_quantity(((pr.get("limits") or {}).get("memory"))) or limit
    windows = {}
    for kind, label in (("first_load", "first_load_burst"),
                        ("composite_first_load", "composite_page_burst")):
        starts = [n["start"] for n in navs if n.get("nav") == kind]
        if not starts:
            windows[label] = None
            continue
        t0 = min(starts)
        w = _window_peaks(mem, tops, t0, t0 + BURST_WINDOW_S)
        w["window"] = [t0, t0 + BURST_WINDOW_S]
        w["navs"] = sum(1 for n in navs if n.get("nav") == kind
                        and t0 <= n["start"] < t0 + BURST_WINDOW_S)
        windows[label] = w
    overall = _window_peaks(mem, tops, float("-inf"), float("inf"))
    peak_rss = overall["rss_bytes_peak"]
    peak_sys = overall["sys_mb_peak"] * 1048576 if overall["sys_mb_peak"] else None
    risk = {"limit_bytes": limit,
            "peak_rss_bytes": peak_rss,
            "peak_rss_share_of_limit": round(peak_rss / limit, 3) if peak_rss and limit else None,
            "peak_go_sys_bytes": peak_sys,
            "peak_go_sys_share_of_limit": round(peak_sys / limit, 3) if peak_sys and limit else None,
            "peak_heap_inuse_mb": overall["heap_inuse_mb_peak"],
            "windows": windows,
            "note": "Diego decides the accepted memory risk from these shares (#710 ruling 2)."}
    if windows.get("composite_page_burst") is None:
        chk("run8.composite_page_burst_ran", None, None, "ran", "FAIL",
            "no composite_first_load navigation recorded")
    if windows.get("first_load_burst") is None:
        chk("run8.first_load_burst_ran", None, None, "ran", "FAIL",
            "no first_load navigation recorded")
    for gate, k in (("content_fill", "fill"), ("nested_resolve", "nested")):
        key = RUN8_KEYS[f"{k}_over_ceiling"]
        seen = _extra_values(mem, key) + _extra_values(mem, RUN8_KEYS[f"{k}_over_ceiling_peak"])
        if not seen:
            chk(f"run8.{gate}.over_ceiling_bytes", key, None, "exposed", "FAIL", "not exposed")
        else:
            vals = [w.get(f"{k}_over_ceiling_bytes_peak") for w in windows.values() if w]
            chk(f"run8.{gate}.over_ceiling_bytes", key, max([v for v in vals if v is not None], default=None),
                "reported (decision: Diego)", "PASS")

    # ── stale_to_push (D2): p95 <= 3 s, max <= 10 s; gap to the 1 s goal ──
    key = RUN8_KEYS["stale_to_push"]
    hs = _extra_values(mem, key)
    h = hs[-1] if hs else None
    stp = None
    if h is None:
        chk("run8.stale_to_push.p95", key, None, f"<= {STALE_TO_PUSH_P95_BAR_MS}", "FAIL", "not exposed")
        chk("run8.stale_to_push.max", key, None, f"<= {STALE_TO_PUSH_MAX_BAR_MS}", "FAIL", "not exposed")
    else:
        f2500, f5000, f1000 = (hist_fraction_le(h, 2500), hist_fraction_le(h, 5000),
                               hist_fraction_le(h, 1000))
        p95 = hist_p95_bound(h)
        stp = {"count": h.get("count"), "p95_bucket": p95, "max_ms": h.get("max_ms"),
               "fraction_le_1000": f1000, "fraction_le_2500": f2500,
               "gap_to_1s_goal": (None if f1000 is None else
                                  f"{round((1 - f1000) * 100, 1)}% of pushes slower than 1 s")}
        f3000 = hist_fraction_le(h, STALE_TO_PUSH_P95_BAR_MS)
        stp["fraction_le_3000"] = f3000
        if not h.get("count"):
            chk("run8.stale_to_push.p95", key, None, f"<= {STALE_TO_PUSH_P95_BAR_MS}",
                "INCONCLUSIVE", "no stale pushes observed")
        elif f3000 is not None:
            # 2b's bounds are exactly 1000/3000/10000 (architect-ch): with a
            # le_3000 bucket the bar is DECIDABLE.
            chk("run8.stale_to_push.p95", key, p95, f"<= {STALE_TO_PUSH_P95_BAR_MS}",
                "PASS" if f3000 >= 0.95 else "FAIL",
                f"fraction <= 3000 ms = {f3000}")
        elif f2500 is not None and f2500 >= 0.95:
            chk("run8.stale_to_push.p95", key, p95, f"<= {STALE_TO_PUSH_P95_BAR_MS}", "PASS")
        elif f5000 is not None and f5000 >= 0.95:
            chk("run8.stale_to_push.p95", key, p95, f"<= {STALE_TO_PUSH_P95_BAR_MS}", "INCONCLUSIVE",
                "p95 <= 5000 (bucket), INCONCLUSIVE vs 3 s")
        else:
            chk("run8.stale_to_push.p95", key, p95, f"<= {STALE_TO_PUSH_P95_BAR_MS}", "FAIL")
        mx = h.get("max_ms")
        chk("run8.stale_to_push.max", key, mx, f"<= {STALE_TO_PUSH_MAX_BAR_MS}",
            "PASS" if mx is not None and mx <= STALE_TO_PUSH_MAX_BAR_MS else "FAIL")

    # ── stale_overdue_total: GATED at 0 (#740: parked debt is NOT overdue) ─
    # Read as the absolute last value — cumulative since the pre-measure
    # reboot, so it is stricter than the in-run delta (also reported).
    key = RUN8_KEYS["stale_overdue"]
    ov_vals = [_num(v) for v in _series(mem, key)]
    ov_vals = [v for v in ov_vals if v is not None]
    overdue = (ov_vals[-1] - ov_vals[0]) if len(ov_vals) >= 2 else (ov_vals[0] if ov_vals else None)
    if not ov_vals:
        chk("run8.stale_overdue_total", key, None, f"== {STALE_OVERDUE_BAR}", "FAIL", "not exposed")
    else:
        chk("run8.stale_overdue_total", key, ov_vals[-1], f"== {STALE_OVERDUE_BAR}",
            "PASS" if ov_vals[-1] <= STALE_OVERDUE_BAR else "FAIL",
            f"since the reboot; in-run delta {overdue}")

    q2 = _q2_stale(mem, chk)
    q3 = _q3_admission(mem, chk)
    q4 = _q4_retry(mem, chk, ov_vals)
    exercised = _exercised_710_740(q2, q4, chk, checks,
                                   client_delivery(fresh, push_refetches, viewer_stats))
    run9 = _run9_refresh(mem, chk)

    # ── background admission wait (and customer: users never queue) ──────
    key = RUN8_KEYS["admission_wait"]
    ws = _extra_values(mem, key)
    wait = None
    if not ws:
        chk("run8.admission_wait_ms.background", key, None, "reported", "FAIL", "not exposed")
    else:
        last = ws[-1] if isinstance(ws[-1], dict) else {}
        wait = {cls: {"count": hh.get("count"), "p95_bucket": hist_p95_bound(hh),
                      "max_ms": hh.get("max_ms")} for cls, hh in last.items()
                if isinstance(hh, dict)}
        chk("run8.admission_wait_ms.background", key, wait.get("background"), "reported",
            "PASS" if "background" in wait else "FAIL",
            None if "background" in wait else "no background class in the map")

    # ── refresher dirty_to_fresh (windowed gauges: max over the whole run) ─
    d2f = {}
    for k in ("dirty_to_fresh_p95", "dirty_to_fresh_max"):
        key = RUN8_KEYS[k]
        vals = [_num(v) for v in _extra_values(mem, key)]
        vals = [v for v in vals if v is not None]
        d2f[k + "_max_over_samples"] = max(vals) if vals else None
        if not vals:
            chk(f"run8.refresher.{k}", key, None, "reported", "FAIL", "not exposed")
    # A restart RESETS every cumulative-since-boot key (histograms, *_total,
    # *_peak): a last-sample read would report only the post-restart tail and
    # could PASS. An OOMKill is the run-8 HEADLINE result, so every run-8
    # check becomes RESTARTED → FAIL, with the restart and the pre-restart peak.
    restart = detect_restart(mem, pods)
    if restart is not None:
        t_r = restart["t"]
        before = [m for m in mem if m.get("kind") == "mem" and t_r is not None
                  and (m.get("t") or 0) < t_r]
        heap_b = [m["heap_inuse_mb"] for m in before if m.get("heap_inuse_mb") is not None]
        sys_b = [m["sys_mb"] for m in before if m.get("sys_mb") is not None]
        rss_b = [t["rss_bytes"] for t in tops if t_r is not None and (t.get("t") or 0) < t_r
                 and t.get("rss_bytes") is not None]
        restart["peak_heap_inuse_mb_before"] = max(heap_b) if heap_b else None
        restart["peak_sys_mb_before"] = max(sys_b) if sys_b else None
        restart["peak_rss_bytes_before"] = max(rss_b) if rss_b else None
        if restart["peak_rss_bytes_before"] and limit:
            restart["peak_rss_share_of_limit_before"] = round(restart["peak_rss_bytes_before"] / limit, 3)
        for c in checks:
            if c["result"] != "FAIL":
                c["result"] = "FAIL"
            c["why"] = ("RESTARTED: snowplow restarted during the run; cumulative counters "
                        "reset, so this reading covers only part of the run"
                        + (f" ({c['why']})" if c.get("why") and not c["why"].startswith("RESTARTED") else ""))
        checks.insert(0, {"check": "run8.no_restart", "key": None, "value": restart["signals"],
                          "bar": "no restart", "result": "FAIL", "build": build,
                          "why": "RESTARTED — the OOMKill/restart is the run-8 headline result"})
    report = {"memory_risk": risk, "stale_to_push": stp, "stale_overdue_delta": overdue,
              "stale_overdue_last": ov_vals[-1] if ov_vals else None,
              "q2_stale": q2, "q3_users_never_queue": q3, "q4_retry_740": q4,
              "exercised_710_740": exercised, "run9_refresh": run9,
              "restart": restart,
              "admission_wait_ms": wait, "refresher_dirty_to_fresh": d2f, "build": build,
              "keys": RUN8_KEYS}
    return report, checks


def push_report(fresh: list[dict]) -> dict:
    """Per label, the ARMED probe's mutate → `refresh` frame time (#764).
    Report-only: the #710 bar is snowplow's own stale_to_push histogram."""
    out = {}
    for label in sorted({f.get("label", "?") for f in fresh if f.get("armed")}):
        rows = [f for f in fresh if f.get("label") == label and f.get("armed")
                and f.get("valid") is not False]
        push = [f["push_ms"] for f in rows if f.get("push_ms") is not None]
        out[label] = {"samples": len(rows), "pushed": len(push),
                      "no_push_within_budget": len(rows) - len(push),
                      "p50_ms": pct(push, 50), "p95_ms": pct(push, 95),
                      "max_ms": max(push) if push else None}
        # #817 re-gate: the probe re-arms on a POLL, which a passive tab never
        # does. Split its pushes by arm generation (0 = the arm a passive tab
        # also holds), and report the never-re-armed control beside it.
        gens = sorted({f["arm_gen"] for f in rows if f.get("arm_gen") is not None})
        if gens:
            out[label]["by_arm_gen"] = {
                str(g): {"samples": sum(1 for f in rows if f.get("arm_gen") == g),
                         "pushed": sum(1 for f in rows if f.get("arm_gen") == g
                                       and f.get("push_ms") is not None)}
                for g in gens}
            out[label]["rearmed_during_sample"] = sum(1 for f in rows if f.get("rearmed_during"))
        ctl = [f for f in rows if "control_push_ms" in f]
        if ctl:
            cpush = [f["control_push_ms"] for f in ctl if f["control_push_ms"] is not None]
            out[label]["control"] = {
                "samples": len(ctl), "pushed": len(cpush),
                "p50_ms": pct(cpush, 50), "p95_ms": pct(cpush, 95),
                # #819 measured: samples after the probe's key rotated
                # (arm_gen > 0) that the passive tab got NO push for
                "no_push_after_rotation": sum(1 for f in ctl if (f.get("arm_gen") or 0) > 0
                                              and f["control_push_ms"] is None)}
    return out


def fresh_viewer_report(fv: list[dict]) -> dict:
    """The probe's viewer (#817): its arms and stream states, every key
    rotation BOUNDED (the last poll that saw the old key .. the first that saw
    the new one) and every disarm; the passive control's states beside it."""
    def states(rows):
        return {s: sum(1 for v in rows if v.get("state") == s)
                for s in sorted({v["state"] for v in rows if v.get("state")})}
    probe = [v for v in fv if v.get("role", "probe") == "probe"]
    return {"arms": sum(1 for v in fv if v.get("armed") is True),
            "states": states(probe),
            "control_states": states([v for v in fv if v.get("role") == "control"]),
            "key_rotations": [{"after": v.get("last_old_t"), "by": v.get("first_new_t", v.get("t"))}
                              for v in probe if v.get("state") == "key_rotated"],
            "disarmed_no_headers_t": [v.get("t") for v in probe
                                      if v.get("state") == "disarmed_no_headers"]}


def passive_push(f: dict) -> bool:
    """A push a PASSIVE tab would have received: the never-re-armed control's,
    or (without a control) one on arm generation 0, before any re-arm."""
    if "control_push_ms" in f:
        return f["control_push_ms"] is not None
    return f.get("push_ms") is not None and (f.get("arm_gen") or 0) == 0


def push_refetch_report(rows: list[dict]) -> dict | None:
    """The driver's viewers: refetches triggered by `refresh` frames (#764)."""
    if not rows:
        return None
    ok = [r for r in rows if r.get("status") == 200]
    return {"refetches": len(rows), "non_200": len(rows) - len(ok),
            "users": len({r.get("user") for r in rows}),
            "wall_p95_ms": pct([r.get("wall_ms") for r in ok], 95),
            "start_lag_p95_ms": pct([r.get("start_lag_ms") for r in rows], 95)}


def _check(name, value, op, bar, checks):
    if value is None:
        checks.append({"check": name, "value": None, "bar": bar,
                       "result": "FAIL", "why": "no data"})
        return
    ok = value <= bar if op == "<=" else value >= bar
    checks.append({"check": name, "value": value, "bar": f"{op} {bar}",
                   "result": "PASS" if ok else "FAIL"})


def evaluate(navs: list[dict], fresh: list[dict], vars_series: list[dict],
             *, thresholds: dict | None = None,
             login_curves: list[dict] | None = None,
             preflights: list[dict] | None = None,
             snowplow_pods: list[dict] | None = None,
             mem: list[dict] | None = None,
             tops: list[dict] | None = None,
             run8: bool = False,
             expected_digest: str | None = None,
             image_digests: list[dict] | None = None,
             push_refetches: list[dict] | None = None,
             viewer_stats: list[dict] | None = None,
             fresh_truncated: list[dict] | None = None,
             driver_streams: list[dict] | None = None,
             fresh_viewers: list[dict] | None = None,
             mem_streams: list[dict] | None = None,
             job_pods: list[dict] | None = None,
             viewer_dead: list[dict] | None = None,
             k6: dict | None = None) -> dict:
    """North-star verdict from the run's JSONL records.

    Returns {"verdict": PASS|FAIL|INVALID, "checks": [...], "economics": ...}.
    INVALID (the driver, not snowplow, failed) wins over FAIL; every INVALID
    reason is spelled out in "invalid_reasons".
    `driver_streams` (driver_stream(), one per collected driver file) proves
    the navigation records are the driver's WHOLE output; None = not checked
    (callers that hand in records directly).
    `k6` (#830, run 11): {"k6", "browser": k6eval.read_k6_out(...), "sessions",
    "thresholds"}; `navs` are then the real-browser page loads.
    """
    if k6 is not None:
        # operator side only: this file also runs alone inside the Job pods
        from bench import k6eval
    th = dict(THRESHOLDS, **(thresholds or {}))
    checks: list[dict] = []
    validity: list[dict] = []

    first = [n["wall_ms"] for n in navs if n.get("visit") == 1]
    warm = [n["wall_ms"] for n in navs if (n.get("visit") or 0) > 1]
    calls = sum(int(n.get("calls", 0)) for n in navs)
    errors = sum(int(n.get("errors", 0)) for n in navs)

    _check("warm_nav_p95_ms", pct(warm, 95), "<=", th["warm_nav_p95_ms"], checks)
    _check("cold_nav_p95_ms", pct(first, 95), "<=", th["cold_nav_p95_ms"], checks)
    _check("zero_cold_navs.first_nav_p95_ms", pct(first, 95), "<=",
           th["first_nav_p95_ms_for_zero_cold"], checks)
    over = sum(1 for n in navs if n["wall_ms"] > th["max_nav_ms_for_zero_cold"])
    _check("zero_cold_navs.navs_over_cold_bar", over if navs else None, "<=", 0,
           checks)

    # Ruling 2026-10-14: freshness GATES at the 10 events/s churn step; the
    # other rates (1, 50) are REPORTED as the curve. Labels are
    # "list_dep@<rate>"; a label without "@" (single-rate run) gates.
    # Run 9: one label family per churn MODE ("list_dep" = annotation churn,
    # "list_dep_status" = status-condition churn); the 10/s step gates in
    # EVERY mode present. An annotation-only run gates exactly as before.
    gate_labels = set(th.get("fresh_gate_labels", ("list_dep@10",)))
    curve = {}
    labels = sorted({f.get("label", "?") for f in fresh})
    rated = [l for l in labels if "@" in l]
    gate_rate = th.get("fresh_gate_rate", "10")
    modes = sorted({l.split("@", 1)[0] for l in rated})
    gate_labels |= {f"{m}@{gate_rate}" for m in modes if m != "list_dep"}
    for m in modes:
        want = {g for g in gate_labels if g.split("@", 1)[0] == m}
        if not (want & set(rated)):
            checks.append({"check": f"fresh_p95_ms[{'/'.join(sorted(want))}]",
                           "value": None, "bar": f"<= {th['fresh_p95_ms']}",
                           "result": "FAIL", "why": "no samples at the gate churn rate"})
    for label in labels:
        # A sample taken while snowplow was not Ready measures the outage, not
        # freshness: it is excluded from the gate and counted in the report.
        rows = [f for f in fresh if f.get("label") == label and f.get("valid") is not False]
        lat = [f["latency_ms"] for f in rows if f.get("converged")]
        unconverged = sum(1 for f in rows if not f.get("converged"))
        if "@" in label and label not in gate_labels:
            curve[label] = {"samples": len(rows), "unconverged": unconverged,
                            "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95),
                            "max_ms": max(lat) if lat else None}
            continue
        if unconverged:
            # A sample that never converged is the worst latency there is; it
            # fails the bar rather than leaving the percentile.
            checks.append({"check": f"fresh_p95_ms[{label}]", "value": None,
                           "bar": f"<= {th['fresh_p95_ms']}", "result": "FAIL",
                           "why": f"{unconverged}/{len(rows)} samples never "
                                  f"converged"})
        else:
            _check(f"fresh_p95_ms[{label}]", pct(lat, 95), "<=",
                   th["fresh_p95_ms"], checks)
        _check(f"fresh_samples[{label}]", len(rows), ">=",
               th["min_fresh_samples"], validity)
    if not fresh:
        _check("fresh_p95_ms", None, "<=", th["fresh_p95_ms"], checks)

    if k6 is not None:
        # #830 run 11: navs are the REAL-BROWSER page loads; the load comes
        # from k6, whose own validity (stream, dropped iterations, errors,
        # route table) and the browser sessions' replace load606's driver checks.
        validity.extend(k6eval.k6_checks(k6.get("k6"), k6.get("thresholds")))
        validity.extend(k6eval.browser_checks(k6.get("browser"), k6.get("sessions"),
                                              k6.get("thresholds")))
        validity.append(k6eval.cpu_check(k6.get("browser"), "browser", k6.get("thresholds")))
        if k6.get("viewers", True):
            validity.extend(k6eval.viewer_checks(k6.get("viewer"), k6.get("k6"),
                                                 k6.get("thresholds")))
        timeouts = (k6eval.browser_report(k6.get("browser")) or {}).get("timeouts")
        checks.append({"check": "browser.page_timeouts", "value": timeouts, "bar": "== 0",
                       "result": "PASS" if timeouts == 0 else "FAIL",
                       **({} if timeouts == 0 else
                          {"why": "a real-browser page did not finish loading within its "
                                  "timeout: the worst page load there is"})})
        checks.append(k6eval.widget_error_check(k6.get("browser"), k6.get("thresholds")))
    else:
        _check("driver.sched_lag_p95_ms",
               pct([n.get("sched_lag_ms", 0) for n in navs], 95), "<=",
               th["max_sched_lag_p95_ms"], validity)
        _check("driver.error_rate", round(errors / calls, 4) if calls else None,
               "<=", th["max_error_rate"], validity)
        _check("driver.warm_navs", len(warm), ">=", th["min_warm_navs"], validity)
    if not navs:
        validity.append({"check": "driver.navs_recorded", "value": 0, "bar": ">= 1",
                         "result": "FAIL",
                         "why": ("no real-browser page load was recorded" if k6 is not None
                                 else "the driver recorded zero navigations") +
                                ": there are no page-load times to evaluate"})
    if driver_streams is not None and k6 is None:      # run 11 has no load606 driver
        validity.extend(driver_stream_checks(driver_streams))
    if mem_streams is not None:
        # Required once mem606 was deployed (it has a job_pod record): a
        # memory capture that never arrived is INVALID, not silently absent.
        validity.extend(framed_stream_checks(
            MEM_FRAMING, mem_streams,
            required=any(r.get("job") == "mem606" for r in job_pods or [])))
    validity.extend(job_pod_checks(job_pods or []))
    storm = cpu_storm_check(cpu_profile_windows(mem or []))
    if storm is not None:
        validity.append(storm)

    econ = None
    if len(vars_series) >= 2:
        econ = economics(vars_series[0], vars_series[-1], calls)
    q = [v.get("refresher_queue_depth") for v in vars_series
         if v.get("refresher_queue_depth") is not None]
    p95s = [v.get("dirty_to_fresh_p95_ms") for v in vars_series
            if v.get("dirty_to_fresh_p95_ms") is not None]
    report = {
        "navs": len(navs), "first_navs": len(first), "warm_navs": len(warm),
        "calls": calls, "errors": errors,
        "warm_p50_ms": pct(warm, 50), "warm_p95_ms": pct(warm, 95),
        "first_p50_ms": pct(first, 50), "first_p95_ms": pct(first, 95),
        "max_nav_ms": max((n["wall_ms"] for n in navs), default=None),
        "refresher_queue_depth_max": max(q) if q else None,
        "refresher_queue_depth_last": q[-1] if q else None,
        "server_dirty_to_fresh_p95_ms_max_window": max(p95s) if p95s else None,
        "server_dirty_to_fresh_p95_ms_median_window": pct(p95s, 50),
        "fresh_curve_report_only": curve,
        # churn modes present ("list_dep" = annotation, "list_dep_status" =
        # status condition); each gates at its own @10 label
        "fresh_modes": modes,
        "fresh_push_report_only": push_report(fresh),
        "push_refetch_report_only": push_refetch_report(push_refetches or []),
        # #817: the probe's viewer, incl. every time its cell's key rotated
        # under it (it re-arms, like the SPA; run 9 armed once and lost it).
        "fresh_viewer": fresh_viewer_report(fresh_viewers or []),
        # #817 re-gate: the driver's tabs left armed on a rotated-away key
        "viewer_dead_report_only": ({
            "samples": len(viewer_dead),
            "tabs_dead_max": max(int(v.get("tabs_dead") or 0) for v in viewer_dead),
            "tabs_dead_last": viewer_dead[-1].get("tabs_dead"),
            "dead_keys_seen_last": viewer_dead[-1].get("dead_keys_seen")}
            if viewer_dead else None),
        "fresh_truncated": fresh_truncated or [],
        "nav_kinds": {k: sum(1 for n in navs if n.get("nav") == k)
                      for k in sorted({n.get("nav") for n in navs if n.get("nav")})},
        "login": login_report(login_curves or []),
        "preflight": (preflights or [None])[-1],
    }

    restart = restart_check(snowplow_pods)
    if restart is not None:
        validity.append(restart)
    dig = digest_check(expected_digest, image_digests or [], snowplow_pods)
    if dig is not None:
        validity.append(dig)
    report["image_digest"] = {"expected": expected_digest, "gate": image_digests or None}
    report["snowplow_pod"] = snowplow_pods
    report["memory"] = memory_report(mem or [])
    report["key_rotation_report_only"] = rotation_report(mem or [])
    report["profiling"] = profiling_report(
        mem or [], navs,
        k6eval.k6_page_loads(k6.get("k6")) if k6 is not None else None)
    report["harness_notes"] = list(HARNESS_NOTES)
    if k6 is not None:
        report["page_loads_are"] = "real Chrome sessions (k6 browser), first visit vs warm per session"
        report["browser"] = k6eval.browser_report(k6.get("browser"))
        report["k6_report_only"] = k6eval.k6_report(k6.get("k6"))
        report["viewer_report_only"] = k6eval.viewer_report(k6.get("viewer"))
        report["load_generator_cpu_report_only"] = {
            c: k6eval.cpu_stats(((k6.get("k6") or {}).get("cpu") or {}).get(c)) for c in ("k6", "viewer")}
    report["first_loads_failed"] = sum(1 for n in navs if n.get("first_load_failed"))
    fv = [f for f in fresh if f.get("valid") is False]
    report["fresh_invalid_samples"] = len(fv)

    if run8:
        r8, r8_checks = run8_report(navs, mem or [], tops or [], snowplow_pods,
                                    fresh=fresh, push_refetches=push_refetches,
                                    viewer_stats=viewer_stats)
        report["run8"] = r8
        checks.extend(r8_checks)

    if any(c["result"] == "FAIL" for c in validity):
        verdict = "INVALID"
    elif any(c["result"] == "FAIL" for c in checks):
        verdict = "FAIL"
    elif any(c["result"] == "INCONCLUSIVE" for c in checks):
        # e.g. stale_to_push p95 lands in the le_5000 bucket: cannot claim <= 3 s.
        verdict = "INCONCLUSIVE"
    else:
        verdict = "PASS"
    invalid_reasons = [f"{c['check']}: " + (c.get("why") or f"{c.get('value')} vs {c.get('bar')}")
                       for c in validity if c["result"] == "FAIL"]
    return {"verdict": verdict, "invalid_reasons": invalid_reasons, "checks": checks,
            "validity": validity, "report": report, "economics": econ, "thresholds": th}


# ─── Fixtures + kubectl mutation (operator side) ────────────────────────────

FRESH_ANNOTATION = "snowplow-bench/fresh-marker"
FRESH_RA_NAME = "bench-fresh-listdep"


def list_dep_fixture_yaml(ns: str, group: str, version: str, plural: str,
                          ra_ns: str = "bench-ns-01") -> str:
    """A RESTAction whose only stage LISTs one namespace of compositions and
    projects each item's freshness annotation.

    Served through the informer, so an annotation patch on ANY composition in
    `ns` reaches this cell ONLY through a list_dep edge (keyed on GVR + ns) —
    the exact edge #603 narrows. No query string: #618, the informer LIST
    ignores ?labelSelector/limit and would serve the unselected list.
    """
    path = f"/apis/{group}/{version}/namespaces/{ns}/{plural}"
    jq = ('[.comps.items[] | {name: .metadata.name, marker: '
          f'(.metadata.annotations["{FRESH_ANNOTATION}"] // "")}}]')
    return f"""\
apiVersion: templates.krateo.io/v1
kind: RESTAction
metadata:
  name: {FRESH_RA_NAME}
  namespace: {ra_ns}
  labels:
    app: scaletest
spec:
  api:
  - name: comps
    path: "{path}"
    filter: '{jq}'
"""


# ─── Status-condition churn (run 9, #765 spec issuecomment-6266045747) ─────
#
# Run 8 churned annotations only. Production changes are mostly status
# (controllers flipping conditions), and Diego's C decision depends on how
# status churn performs. The status mode writes the marker into a condition of
# .status.conditions THROUGH THE STATUS SUBRESOURCE (as a controller does) and
# polls a sibling RESTAction that projects that condition, so the marker lives
# in the field the mode churns, exactly like architect-623's in-process
# harness. The compositions are not reconciled (data606 holds their
# controller), so nothing else rewrites the condition.
FRESH_STATUS_TYPE = "SnowplowBenchFresh"
FRESH_STATUS_RA_NAME = "bench-fresh-listdep-status"
FRESH_MODES = ("annotation", "status")
FRESH_MODE_LABEL = {"annotation": "list_dep", "status": "list_dep_status"}


def list_dep_status_fixture_yaml(ns: str, group: str, version: str, plural: str,
                                 ra_ns: str = "bench-ns-01") -> str:
    """The status-mode twin of list_dep_fixture_yaml: same LIST (same
    list_dep edge), projecting the SnowplowBenchFresh condition's message."""
    path = f"/apis/{group}/{version}/namespaces/{ns}/{plural}"
    jq = ('[.comps.items[] | {name: .metadata.name, marker: '
          f'([(.status.conditions // [])[] | select(.type == "{FRESH_STATUS_TYPE}") '
          '| .message][0] // "")}]')
    return f"""\
apiVersion: templates.krateo.io/v1
kind: RESTAction
metadata:
  name: {FRESH_STATUS_RA_NAME}
  namespace: {ra_ns}
  labels:
    app: scaletest
spec:
  api:
  - name: comps
    path: "{path}"
    filter: '{jq}'
"""


def with_fresh_condition(obj: dict, value: str, now: str) -> dict:
    """`obj` with its SnowplowBenchFresh condition set to `value`, FLIPPING
    the condition status (True <-> False) like a controller transition; every
    other condition is kept as is."""
    obj = json.loads(json.dumps(obj))
    status = obj.setdefault("status", {}) or {}
    obj["status"] = status
    conds = [c for c in (status.get("conditions") or []) if isinstance(c, dict)]
    prev = next((c for c in conds if c.get("type") == FRESH_STATUS_TYPE), None)
    new = {"type": FRESH_STATUS_TYPE,
           "status": "False" if (prev or {}).get("status") == "True" else "True",
           "reason": "BenchChurn", "message": value, "lastTransitionTime": now}
    status["conditions"] = [c for c in conds if c.get("type") != FRESH_STATUS_TYPE] + [new]
    return obj


def incluster_status_condition(group: str, version: str, plural: str, ns: str,
                               name: str, value: str, *, sa_dir: str | None = None,
                               api: str = "https://kubernetes.default.svc",
                               timeout: float = 30, attempts: int = 3) -> bool:
    """GET the object, set the SnowplowBenchFresh condition, PUT it to the
    /status subresource with its resourceVersion (409 → re-read, retry)."""
    import ssl
    sa_dir = sa_dir or _SA_DIR
    try:
        with open(os.path.join(sa_dir, "token")) as fh:
            tok = fh.read().strip()
        ctx = ssl.create_default_context(cafile=os.path.join(sa_dir, "ca.crt"))
        url = f"{api}/apis/{group}/{version}/namespaces/{ns}/{plural}/{name}"
        hdr = {"Authorization": "Bearer " + tok}
        for _ in range(attempts):
            with urllib.request.urlopen(urllib.request.Request(url, headers=hdr),
                                        timeout=timeout, context=ctx) as r:
                obj = json.loads(r.read())
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            body = json.dumps(with_fresh_condition(obj, value, now)).encode()
            req = urllib.request.Request(url + "/status", data=body, method="PUT",
                                         headers=dict(hdr, **{"Content-Type": "application/json"}))
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
                    return 200 <= r.status < 300
            except urllib.error.HTTPError as e:
                if e.code != 409:
                    return False
        return False
    except Exception:  # noqa: BLE001 — a failed mutate is recorded, not raised
        return False


def kubectl_status_condition(kubeconfig: str, context: str, resource: str, ns: str,
                             name: str, value: str, timeout: int = 30) -> bool:
    """Operator-side twin: `kubectl get -o json` + `kubectl replace
    --subresource=status`, explicit kubeconfig + context."""
    import subprocess
    base = ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}", "-n", ns]
    try:
        g = subprocess.run(base + ["get", resource, name, "-o", "json"],
                           capture_output=True, timeout=timeout)
        if g.returncode != 0:
            return False
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        body = json.dumps(with_fresh_condition(json.loads(g.stdout), value, now))
        r = subprocess.run(base + ["replace", "--subresource=status", "-f", "-"],
                           input=body.encode(), capture_output=True, timeout=timeout)
        return r.returncode == 0
    except Exception:  # noqa: BLE001
        return False


def call_path(group: str, version: str, plural: str, name: str, ns: str) -> str:
    return (f"/call?apiVersion={group}%2F{version}&resource={plural}"
            f"&name={name}&namespace={ns}")


def kubectl_annotate(kubeconfig: str, context: str, resource: str, ns: str,
                     name: str, value: str, timeout: int = 30) -> bool:
    """`kubectl annotate --overwrite` with an EXPLICIT kubeconfig + context:
    the operator's ambient context is never consulted."""
    import subprocess
    argv = ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}",
            "annotate", "--overwrite", resource, name, "-n", ns,
            f"{FRESH_ANNOTATION}={value}"]
    try:
        return subprocess.run(argv, capture_output=True,
                              timeout=timeout).returncode == 0
    except Exception:  # noqa: BLE001
        return False


# ─── CLI ────────────────────────────────────────────────────────────────────


def _jsonl_sink(path: str):
    lock = threading.Lock()
    fh = open(path, "a", buffering=1)

    def _w(rec):
        with lock:
            fh.write(json.dumps(rec, default=str) + "\n")
    return _w


def _read_jsonl(path: str) -> list[dict]:
    """Records from a JSONL file. `kubectl logs` interleaves the driver's
    stderr diagnostics with its JSONL; non-JSON lines are skipped and counted
    (a `{"kind": "skipped"}` record) so nothing disappears silently."""
    out = []
    skipped = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if not line.startswith("{"):
                skipped += 1
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                skipped += 1
    if skipped:
        out.append({"kind": "skipped", "path": path, "lines": skipped})
    return out


# A framed stream (#817): a file one in-cluster writer produces, which must
# prove it is that writer's WHOLE output: its start record first, its end
# record (how many items it wrote) last, then the pod wrapper's exit record.
# `kinds` are the record kinds only that writer emits: a file holding any of
# them is that writer's stream.
@dataclass(frozen=True)
class Framing:
    name: str           # check prefix
    who: str            # subject of a reason
    kinds: frozenset
    start: str
    end: str
    exit: str
    item: str           # the counted record kind
    count: str          # the end record's count field
    noun: str           # what `count` counts
    lost: str           # what was lost when the counts differ


DRIVER_KINDS = frozenset({"drive_start", "drive_end", "drive_exit", "nav", "login_curve",
                          "viewer_config", "viewer", "viewer_stats", "viewer_dead",
                          "push_refetch", "vars"})
DRIVER_FRAMING = Framing("driver", "the driver", DRIVER_KINDS, "drive_start", "drive_end",
                         "drive_exit", "nav", "navs", "navigations", "navigation")
MEM_FRAMING = Framing("mem606", "the mem606 sampler",
                      frozenset({"mem_start", "mem_end", "mem_exit", "mem", "heap_profile",
                                 "cpu_profile"}),
                      "mem_start", "mem_end", "mem_exit", "mem", "samples",
                      "memory samples", "memory sample")


def framed_stream(f: Framing, path: str, recs: list[dict]) -> dict | None:
    """The framing of one collected file for writer `f`: its first record, its
    end / exit records and how many items it holds. None when the file holds
    no record of that writer."""
    own = [r for r in recs if r.get("kind") in f.kinds]
    if not own:
        return None
    return {"path": os.path.basename(path), "first": own[0].get("kind"),
            "first_t": own[0].get("t", own[0].get("t_event")),
            "end": next((r for r in own if r["kind"] == f.end), None),
            "exit": next((r for r in own if r["kind"] == f.exit), None),
            "items": sum(1 for r in own if r["kind"] == f.item)}


def framed_stream_checks(f: Framing, streams: list[dict], *, required: bool) -> list[dict]:
    """One validity check per stream; a stream that cannot prove it is the
    writer's whole output makes the run INVALID, with the reason. Run 9: the
    driver's stream started mid-run (container log rotation cut its head) and
    held no navigation at all; the verdict only said `warm_navs 0`."""
    if not streams:
        if not required:
            return []
        return [{"check": f"{f.name}.stream", "value": None,
                 "bar": f"one collected {f.name} stream", "result": "FAIL",
                 "why": f"no {f.name} output was collected: no file holds a {f.name} record"}]
    out = []
    for s in streams:
        why = []
        if s["first"] != f.start:
            why.append(f"the stream starts mid-run (first record: {s['first']} at "
                       f"t={s['first_t']}): its head is missing, as when the container "
                       f"log rotated before it was read")
        end, ex = s["end"], s["exit"]
        if end is None:
            why.append(f"no {f.end} record: {f.who} did not finish, or the "
                       f"stream's tail is missing")
        else:
            if end.get(f.count) != s["items"]:
                why.append(f"{f.end} counts {end.get(f.count)} {f.noun}, the stream "
                           f"holds {s['items']}: {f.lost} records were lost")
            if not end.get("ok", True):
                why.append(f"{f.who} failed: {end.get('reason')}")
        if ex is None:
            why.append(f"no {f.exit} record: {f.who}'s pod died before its "
                       f"wrapper recorded the exit code")
        elif ex.get("rc") != 0:
            why.append(f"{f.who} exited rc={ex.get('rc')}")
        out.append({"check": f"{f.name}.stream_complete[{s['path']}]",
                    "value": {f.count: s["items"],
                              f"{f.end}_{f.count}": (end or {}).get(f.count),
                              "exit_rc": (ex or {}).get("rc")},
                    "bar": f"{f.start} .. {f.end} (ok, all {f.count}) .. {f.exit} rc=0",
                    "result": "FAIL" if why else "PASS",
                    **({"why": "; ".join(why)} if why else {})})
    return out


def driver_stream(path: str, recs: list[dict]) -> dict | None:
    return framed_stream(DRIVER_FRAMING, path, recs)


def driver_stream_checks(streams: list[dict]) -> list[dict]:
    return framed_stream_checks(DRIVER_FRAMING, streams, required=True)


def classify(resp: "Response") -> str:
    """ok | config (a request the cluster cannot serve: 404, or snowplow's
    'no matches for <GVR>' for an unserved type — run 6) | forbidden |
    product (any other non-2xx: a FINDING, recorded, never a tooling stop)."""
    if 200 <= resp.status < 300:
        return "ok"
    body = (resp.body or b"")[:2000].decode("utf-8", "replace")
    if resp.status == 404 or "no matches for" in body:
        return "config"
    if resp.status == 403:
        return "forbidden"
    return "product"


def adaptive_login(users: list[str], login_fn: Callable[[str], str], *,
                   start: int = 2, cmax: int = 64, baseline_n: int = 3,
                   grow_below: float = 1.25, shrink_above: float = 1.6,
                   rounds: int = 3, progress_every: int = 50,
                   log: Callable[[str], None] = lambda m: print(m, file=sys.stderr, flush=True),
                   clock: Callable[[], float] = time.perf_counter):
    """Log every user in, with SELF-ADAPTING concurrency.

    Baseline = median of `baseline_n` serial logins. Then batches of size c:
    the batch's median per-login latency vs the baseline decides the next c —
    double while it stays below grow_below×baseline, halve on any error or
    above shrink_above×baseline, hold in between. Failed users are retried in
    later rounds. Progress every `progress_every` users. Returns
    (tokens, curve, failed): `curve` is the latency/concurrency record the
    report keeps (authn issues an x509 certificate per login — run 6 measured
    ~3.2s each — which is itself a finding for authn).
    """
    tokens: dict[str, str] = {}
    curve: list[dict] = []
    errors_seen: dict[str, str] = {}

    def one(u):
        t0 = clock()
        try:
            tok = login_fn(u)
            return u, tok, (clock() - t0) * 1000, None
        except Exception as e:  # noqa: BLE001
            return u, None, (clock() - t0) * 1000, f"{type(e).__name__}: {e}"

    pending = list(users)
    base_lat = []
    for u in pending[:baseline_n]:
        u, tok, ms, err = one(u)
        if tok:
            tokens[u] = tok
            base_lat.append(ms)
        else:
            errors_seen[u] = err or "?"
    baseline = pct(base_lat, 50) or 1.0
    curve.append({"phase": "baseline", "concurrency": 1, "n": len(base_lat),
                  "median_ms": round(baseline, 1), "errors": len(pending[:baseline_n]) - len(base_lat)})
    log(f"[login] baseline {baseline:.0f} ms/login (serial, n={len(base_lat)})")
    pending = [u for u in users if u not in tokens]
    c = max(1, start)
    last_mark = 0
    for rnd in range(rounds):
        failed: list[str] = []
        i = 0
        with concurrent.futures.ThreadPoolExecutor(cmax) as pool:
            while i < len(pending):
                batch = pending[i:i + c]
                i += len(batch)
                t0 = clock()
                res = list(pool.map(one, batch))
                wall = (clock() - t0) * 1000
                lats = [ms for _, tok, ms, _ in res if tok]
                errs = 0
                for u, tok, ms, err in res:
                    if tok:
                        tokens[u] = tok
                    else:
                        errs += 1
                        failed.append(u)
                        errors_seen[u] = err or "?"
                med = pct(lats, 50)
                curve.append({"phase": f"round{rnd + 1}", "concurrency": c,
                              "n": len(batch), "errors": errs,
                              "median_ms": round(med, 1) if med else None,
                              "p95_ms": round(pct(lats, 95), 1) if lats else None,
                              "batch_wall_ms": round(wall, 1),
                              "ratio_to_baseline": round(med / baseline, 2) if med else None})
                if errs or (med is not None and med > shrink_above * baseline):
                    c = max(1, c // 2)
                elif med is not None and med <= grow_below * baseline:
                    c = min(cmax, c * 2)
                done = len(tokens)
                if done // progress_every > last_mark // progress_every or done == len(users):
                    log(f"[login] {done}/{len(users)} logged in; concurrency {c}; "
                        f"batch median {med or 0:.0f} ms ({(med or 0) / baseline:.2f}x baseline); "
                        f"errors this batch {errs}")
                    last_mark = done
        pending = failed
        if not pending:
            break
        c = max(1, c // 2)
        log(f"[login] retry round {rnd + 2}: {len(pending)} users")
    return tokens, curve, pending


def portal_from_config_json(text: str) -> dict:
    """The frontend's config.json (ConfigMap frontend-config-vars) →
    {init, frontend_ns, builders_ns}. The boot root is api.INIT
    (ConfigContext.tsx:10, Shell.tsx:92) — never a hard-coded page name."""
    cfg = json.loads(text)
    api, params = cfg.get("api") or {}, cfg.get("params") or {}
    init = api.get("INIT") or ""
    if not init.startswith("/call"):
        raise ValueError(f"config.json api.INIT is not a /call path: {init!r}")
    return {"init": init,
            "frontend_ns": params.get("FRONTEND_NAMESPACE") or "krateo-system",
            "builders_ns": api.get("BUILDERS_NAMESPACE") or ""}


def _cmd_portal_config(a) -> int:
    p = portal_from_config_json(sys.stdin.read())
    import shlex
    print(f"export PORTAL_INIT={shlex.quote(p['init'])}")
    print(f"export PORTAL_FRONTEND_NS={shlex.quote(p['frontend_ns'])}")
    print(f"export PORTAL_BUILDERS_NS={shlex.quote(p['builders_ns'])}")
    return 0


def _cmd_preflight_composite(a) -> int:
    """preflight JSONL on stdin -> the composite route paths, one per line."""
    for line in sys.stdin:
        line = line.strip()
        if line.startswith("{"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("kind") == "preflight":
                for path in r.get("composite_routes") or []:
                    print(path)
    return 0


def _cmd_preflight_summary(a) -> int:
    rec = None
    for line in sys.stdin:
        line = line.strip()
        if line.startswith("{"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("kind") == "preflight":
                rec = r
    if rec is None:
        print("[preflight] no result line", flush=True)
        return 1
    print(f"[preflight] ok={rec.get('ok')} init_status={rec.get('init_status')} "
          f"routes={len(rec.get('routes') or [])} login_ms={rec.get('login_ms')}", flush=True)
    fl = rec.get("first_load_shell")
    if fl:
        print(f"[preflight] first-load shell: {fl.get('calls')} calls, "
              f"{fl.get('wall_ms')} ms, statuses {fl.get('statuses')}", flush=True)
    for x in rec.get("refused") or []:
        print(f"[preflight] REFUSED {x.get('path')} -> {x.get('status')}: "
              f"{(x.get('body') or '')[:200]}", flush=True)
    for x in rec.get("product_errors") or []:
        print(f"[preflight] PRODUCT ERROR (recorded, not fatal) {x.get('path')} -> "
              f"{x.get('status')}: {(x.get('body') or '')[:200]}", flush=True)
    print(f"[preflight] composite pages (nested trees, for the cold burst): "
          f"{rec.get('composite_routes') or 'NONE'}", flush=True)
    fw = rec.get("full_walk_report_only")
    if fw:
        print(f"[preflight] full closure walk (report-only, NOT a navigation): "
              f"{fw.get('calls')} calls, {fw.get('bytes')} bytes, {fw.get('wall_ms')} ms",
              flush=True)
    return 0 if rec.get("ok") else 2


def _portal_from_args(a) -> "Portal":
    return Portal(init=a.init, frontend_ns=a.frontend_ns,
                  builders_ns=a.builders_ns or None)


def composite_routes(rows: list[dict], *, min_depth: int = 3, top: int = 3) -> list[str]:
    """Composite pages = menu routes whose eagerly-rendered tree is NESTED
    (page → container → widgets: depth >= min_depth), ranked by how many
    widgets the first render fetches. Derived from the live menu at
    preflight (#710 ruling 3: run 8 must include a cold composite-page burst;
    the nested-resolve gate no longer queues customers)."""
    comp = [r for r in rows if (r.get("tree_depth") or 0) >= min_depth and not r.get("tree_errors")]
    comp.sort(key=lambda r: (-(r.get("tree_calls") or 0), r["path"]))
    return [r["path"] for r in comp[:top]]


def preflight(client: HttpClient, token: str, portal: "Portal", *,
              full_walk: bool = False, per_user_parallel: int = 6) -> dict:
    """Seconds-scale check BEFORE stamping data or mass logins.

    Refuses (ok=False) on a CONFIG error: INIT or a menu route's content page
    is 404 or an unserved type. A product 5xx / 403 is RECORDED and does not
    refuse. Also records the navigation shape (first-load calls, routes) and,
    with full_walk, the closure-walk size as a separately labelled metric.
    """
    out: dict = {"kind": "preflight", "init": portal.init, "refused": [],
                 "product_errors": [], "forbidden": [], "routes": []}
    with concurrent.futures.ThreadPoolExecutor(per_user_parallel) as pool:
        r = client.get(portal.init, token)
        cls = classify(r)
        out["init_status"] = r.status
        if cls != "ok":
            body = (r.body or b"")[:400].decode("utf-8", "replace")
            (out["refused"] if cls == "config" else out["product_errors"]).append(
                {"path": portal.init, "status": r.status, "body": body})
            out["ok"] = not out["refused"]
            return out
        shell, routes, content = first_load(client, token, portal, pool=pool,
                                            per_user_parallel=per_user_parallel)
        out["first_load_shell"] = {"calls": shell.calls, "errors": shell.errors,
                                   "wall_ms": shell.wall_ms, "statuses": shell.statuses}
        if not routes:
            out["refused"].append({"path": "(sidebar menu)", "status": None,
                                   "body": "the shell rendered no sidebar Menu with routes"})
        for rt in routes:
            rr = client.get(rt["endpoint"], token)
            c = classify(rr)
            row = {"path": rt["path"], "endpoint": rt["endpoint"], "status": rr.status}
            if c == "ok":
                tree = walk(client, token, [rt["endpoint"]], pool=pool,
                            per_user_parallel=per_user_parallel)
                row.update({"tree_calls": tree.calls, "tree_depth": tree.depth,
                            "tree_bytes": tree.bytes, "tree_errors": tree.errors})
            out["routes"].append(row)
            if c == "config":
                out["refused"].append({**row, "body": (rr.body or b"")[:400].decode("utf-8", "replace")})
            elif c == "product":
                out["product_errors"].append({**row, "body": (rr.body or b"")[:400].decode("utf-8", "replace")})
            elif c == "forbidden":
                out["forbidden"].append(row)
        out["composite_routes"] = composite_routes(out["routes"])
        if full_walk:
            wall, calls, errs, nbytes, slowest, samples = walk_page(
                client, token, portal.init, pool=pool, max_depth=12, max_calls=5000)
            out["full_walk_report_only"] = {"calls": calls, "errors": errs,
                                            "bytes": nbytes, "wall_ms": round(wall, 1)}
    out["ok"] = not out["refused"]
    return out


def _cmd_preflight(a) -> int:
    client = HttpClient(a.snowplow_url)
    t0 = time.perf_counter()
    try:
        tok = login(a.authn_url, a.user, a.password)
    except Exception as e:  # noqa: BLE001
        print(json.dumps({"kind": "preflight", "ok": False,
                          "refused": [{"path": "/basic/login", "status": None,
                                       "body": f"{type(e).__name__}: {e}"}]}), flush=True)
        return 2
    login_ms = (time.perf_counter() - t0) * 1000
    res = preflight(client, tok, _portal_from_args(a), full_walk=a.full_walk)
    res["login_ms"] = round(login_ms, 1)
    print(json.dumps(res, default=str), flush=True)
    if not res["ok"]:
        print("preflight REFUSED: " + "; ".join(
            f"{x['path']} -> {x['status']}" for x in res["refused"]), file=sys.stderr, flush=True)
        return 2
    return 0


def _cmd_login_tokens(a) -> int:
    """#830: sign users start..end in through authn (the adaptive ramp, capped
    at --max-concurrency) and write their bearer tokens, in user order, to
    --out — a file INSIDE the k6 pod (an emptyDir), never collected or logged.
    Exit 2 when any user could not sign in: a partial population is refused,
    as `drive` refuses it."""
    users = [f"{a.user_prefix}{i:04d}" for i in range(a.user_start, a.user_end + 1)]
    tokens, curve, failed = adaptive_login(
        users, lambda u: login(a.authn_url, u, a.password), cmax=a.max_concurrency)
    print(json.dumps({"kind": "login_curve", "users": len(users), "failed": len(failed),
                      "cmax": a.max_concurrency, "curve": curve}), flush=True)
    if failed:
        print(f"only {len(tokens)}/{len(users)} users signed in; refusing a partial population",
              file=sys.stderr, flush=True)
        return 2
    fd = os.open(a.out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump([tokens[u] for u in users], fh)
    return 0


def _feed_entries(armed: list[dict]) -> list[dict]:
    """A feed line's widgets ({path, key, cls}) as ViewerHub entries; a widget
    whose /call cannot form coordinates is not armed (as in `walk`)."""
    out = []
    for e in armed or []:
        coords = refresh_coords(e.get("path") or "", e.get("cls") or "")
        if coords is not None and e.get("key"):
            out.append({"path": e["path"], "key": e["key"], "coords": coords})
    return out


def _cmd_view_feed(a) -> int:
    """#830 (run 11, option A): the users' open tabs, as a sidecar of the k6
    pod. Tails k6's navigation feed (k6/navigate.js: one JSON line per page
    load, with the refresh key/class each widget actually answered with) and
    re-arms that user's /refreshes stream on exactly those widgets, the shell's
    plus the page's, through ViewerHub — so frames, refetches, rotation re-arms
    and close are the run 9/10 driver's. Writes a framed stream to --out:
    viewer_start, viewer_armed (arm lag = k6 nav end -> stream open),
    viewer / push_refetch / viewer_dead (ViewerHub), viewer_stats, viewer_end.
    Ends at the feed's feed_end, or when --stop-file appears (k6 exited) and
    the feed is drained: then rc 3, the feed was cut."""
    sink = _jsonl_sink(a.out)
    sink({"kind": "viewer_start", "t": time.time()})
    with open(a.tokens) as fh:
        tokens = json.load(fh)
    threading.stack_size(VIEWER_STACK_BYTES)        # one reader thread per tab (#764)
    client = HttpClient(a.snowplow_url)
    pool = concurrent.futures.ThreadPoolExecutor(a.refetch_workers)
    lock = threading.Lock()
    pending: dict[str, tuple[str, float]] = {}       # user -> (nav, k6 nav end)

    def on_open(user: str, t: float) -> None:
        with lock:
            p = pending.pop(user, None)
        if p is not None:
            sink({"kind": "viewer_armed", "user": user, "nav": p[0], "t": t,
                  "arm_lag_ms": round(max(0.0, t - p[1]) * 1000, 1)})

    hub = ViewerHub(client, pool, sink, on_open=on_open)
    shell: dict[str, list[dict]] = {}
    seen = {"navs": 0, "navs_failed": 0, "bad_lines": 0, "feed_start": False}
    ended_by = None
    fh, buf = None, ""
    stopping = False
    next_dead = time.time() + DEAD_TAB_SAMPLE_S
    while ended_by is None:
        if fh is None and os.path.exists(a.feed):
            fh = open(a.feed)  # noqa: SIM115 — tailed across the loop, closed below
        chunk = fh.readline() if fh is not None else ""
        if chunk:
            buf += chunk
            if not buf.endswith("\n"):
                continue                         # a line still being written
            line, buf = buf, ""
            try:
                rec = json.loads(line)
            except ValueError:
                seen["bad_lines"] += 1
                continue
            kind = rec.get("kind")
            if kind == "feed_start":
                seen["feed_start"] = True
            elif kind == "feed_end":
                ended_by = "feed_end"
            elif kind == "nav":
                seen["navs"] += 1
                if not rec.get("ok"):
                    seen["navs_failed"] += 1     # no working portal: nothing to arm
                    continue
                idx = int(rec["user"])
                user = f"{a.user_prefix}{idx:04d}"
                if "shell" in rec:               # a first load: the shell stays mounted
                    shell[user] = _feed_entries(rec["shell"])
                entries = shell.get(user, []) + _feed_entries(rec.get("page"))
                if entries:
                    with lock:
                        pending[user] = (str(rec.get("nav")), float(rec.get("t_end") or 0))
                hub.view(user, tokens[idx - 1], entries)
            continue
        if stopping:
            ended_by = "k6 exited without feed_end: the feed is cut"
            break
        hub.flush_trailing()
        now = time.time()
        if now >= next_dead:
            sink({"kind": "viewer_dead", "t": now, **hub.dead_tabs()})
            next_dead = now + DEAD_TAB_SAMPLE_S
        if os.path.exists(a.stop_file):
            stopping = True                      # drain what is left, then stop
            continue
        time.sleep(a.poll_s)
    if fh is not None:
        fh.close()
    stats = hub.close_all()
    sink({"kind": "viewer_stats", "t": time.time(), **stats})
    pool.shutdown(wait=True, cancel_futures=True)
    rc = 0 if ended_by == "feed_end" else 3
    sink({"kind": "viewer_end", "t": time.time(), "ok": rc == 0, "rc": rc,
          "ended_by": ended_by, **seen})
    return rc


def _cmd_drive(a) -> int:
    """The driver's stream is framed so the evaluator can prove it complete
    (#817): `drive_start` is its first record and `drive_end` (navigation
    count, ok, reason) its last; the pod wrapper appends `drive_exit`. Run 9
    lost every navigation to a stream cut at its head, and nothing said so."""
    sink = _jsonl_sink(a.out)
    sink({"kind": "drive_start", "t": time.time(), "user_start": a.user_start,
          "user_end": a.user_end, "duration_s": a.duration_s, "think_s": a.think_s})
    navs: list[NavResult] = []
    try:
        rc, reason = _drive(a, sink, navs)
    except Exception as e:  # noqa: BLE001 — recorded in drive_end, then re-raised
        sink(drive_end_record(navs, 1, f"driver crashed: {type(e).__name__}: {e}"[:300]))
        raise
    if rc == 0 and not navs:
        rc, reason = 3, "the driver recorded zero navigations"
    if rc != 0:
        print(f"FATAL: {reason}", file=sys.stderr, flush=True)
    sink(drive_end_record(navs, rc, reason))
    return rc


def drive_end_record(navs: list["NavResult"], rc: int, reason: str | None) -> dict:
    return {"kind": "drive_end", "t": time.time(), "ok": rc == 0, "rc": rc,
            "reason": reason, "navs": len(navs)}


def _drive(a, sink, navs: list["NavResult"]) -> tuple[int, str | None]:
    client = HttpClient(a.snowplow_url)
    portal = _portal_from_args(a)
    users = [f"{a.user_prefix}{i:04d}" for i in range(a.user_start, a.user_end + 1)]
    # Guard in seconds, BEFORE the login burst: the first user's INIT must be
    # servable (the run606 preflight already checked; this keeps `drive`
    # standalone-safe — run 6 spent 54 min logging in before discovering it).
    try:
        tok0 = login(a.authn_url, users[0], a.password)
    except Exception as e:  # noqa: BLE001
        return 2, f"first login failed: {e}"
    r0 = client.get(portal.init, tok0)
    if classify(r0) == "config":
        return 2, f"INIT {portal.init} -> {r0.status}: not servable on this cluster; refusing"
    tokens, curve, failed = adaptive_login(
        users[1:], lambda u: login(a.authn_url, u, a.password))
    tokens[users[0]] = tok0
    sink({"kind": "login_curve", "users": len(users), "failed": len(failed),
          "curve": curve})
    if failed:
        return 2, f"only {len(tokens)}/{len(users)} users logged in; refusing a partial population"
    probe_user = users[0]
    stop = threading.Event()

    def _sampler():
        while not stop.is_set():
            v = read_vars(client, tokens[probe_user])
            sink({"kind": "vars", "t": time.time(),
                  **(summarize_vars(v) if v else {"unreadable": True})})
            stop.wait(a.vars_interval)

    th = None
    if a.vars_interval > 0:
        th = threading.Thread(target=_sampler, daemon=True)
        th.start()
    burst_users = users[-a.burst_users:] if a.burst_users > 0 else []
    if burst_users and not a.composite_route:
        print("composite burst requested but no --composite-route (none found at "
              "preflight): the burst records FAILED navigations", file=sys.stderr)
    viewers = not a.no_viewers
    if viewers:
        # one reader thread per user's /refreshes stream (#764 sizing)
        threading.stack_size(VIEWER_STACK_BYTES)
    sink({"kind": "viewer_config", "enabled": viewers, "users": len(tokens),
          "max_coords": REFRESH_MAX_COORDS, "throttle_s": REFRESH_THROTTLE_S,
          "max_inflight_per_user": REFRESH_MAX_INFLIGHT})
    d = Driver(client, tokens, portal, think_s=a.think_s, duration_s=a.duration_s,
               nav_workers=a.nav_workers, call_workers=a.call_workers,
               sink=sink, composite_routes=a.composite_route or [],
               burst_users=burst_users,
               burst_at_s=a.burst_at_s if burst_users else None,
               viewers=viewers)
    try:
        d.run()
    finally:
        navs.extend(d.results)
        stop.set()
        if th:
            th.join(timeout=a.vars_interval + 30)
    return 0, None


_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


def incluster_annotate(group: str, version: str, plural: str, ns: str,
                       name: str, value: str, *, sa_dir: str = _SA_DIR,
                       api: str = "https://kubernetes.default.svc",
                       timeout: float = 30) -> bool:
    """Merge-patch the freshness annotation through the Kubernetes API using
    the pod's ServiceAccount (the fresh606 Job). No kubectl, no kubeconfig."""
    import ssl
    try:
        with open(os.path.join(sa_dir, "token")) as fh:
            tok = fh.read().strip()
        ctx = ssl.create_default_context(cafile=os.path.join(sa_dir, "ca.crt"))
        body = json.dumps({"metadata": {"annotations": {FRESH_ANNOTATION: value}}}).encode()
        req = urllib.request.Request(
            f"{api}/apis/{group}/{version}/namespaces/{ns}/{plural}/{name}",
            data=body, method="PATCH",
            headers={"Authorization": "Bearer " + tok,
                     "Content-Type": "application/merge-patch+json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return 200 <= r.status < 300
    except Exception:  # noqa: BLE001 — a failed mutate is recorded, not raised
        return False


def _cmd_fresh(a) -> int:
    """Freshness probe at one or more churn rates (labels list_dep@<rate>).

    --mutator kubectl    operator machine; explicit --kubeconfig/--context.
    --mutator incluster  inside the fresh606 Job; Kubernetes API via the
                         pod's ServiceAccount.
    """
    if a.mutator == "kubectl":
        if not a.kubeconfig or not a.context:
            print("--mutator kubectl needs --kubeconfig and --context", file=sys.stderr)
            return 2
        if "krateo-057" in a.context:
            print("refusing: krateo-057 is never a bench target (#606)", file=sys.stderr)
            return 3

        def patch(ns, name, value):
            return kubectl_annotate(a.kubeconfig, a.context, f"{a.plural}.{a.group}",
                                    ns, name, value)

        def patch_status(ns, name, value):
            return kubectl_status_condition(a.kubeconfig, a.context, f"{a.plural}.{a.group}",
                                            ns, name, value)
    else:
        def patch(ns, name, value):
            return incluster_annotate(a.group, a.version, a.plural, ns, name, value)

        def patch_status(ns, name, value):
            return incluster_status_condition(a.group, a.version, a.plural, ns, name, value)
    mutators = {"annotation": patch, "status": patch_status}

    client = HttpClient(a.snowplow_url)
    token = None
    for attempt in range(5):
        try:
            token = login(a.authn_url, a.user, a.password)
            break
        except Exception as e:  # noqa: BLE001
            print(f"fresh: login attempt {attempt + 1} failed: {e}", file=sys.stderr)
            time.sleep(2 * (attempt + 1))
    if token is None:
        return 2
    sink = _jsonl_sink(a.out)
    # The probe object is excluded from churn: churn writes the same
    # annotation key and would overwrite the probe's marker.
    targets = [(f"bench-ns-{n:02d}", f"{a.name_prefix}{n:02d}-{k:02d}")
               for n in range(1, a.churn_namespaces + 1)
               for k in range(1, a.churn_per_ns + 1)
               if (f"bench-ns-{n:02d}", f"{a.name_prefix}{n:02d}-{k:02d}")
               != (a.namespace, a.probe_name)]
    path = call_path("templates.krateo.io", "v1", "restactions",
                     FRESH_RA_NAME, a.ra_namespace)
    # Run 9: churn MODES as separate labelled phases, annotation first. An
    # annotation-only run is unchanged: same path, labels, order and records.
    modes = list(a.modes or ["annotation"])
    paths = {"annotation": path,
             "status": call_path("templates.krateo.io", "v1", "restactions",
                                 FRESH_STATUS_RA_NAME, a.ra_namespace)}
    push_since = None
    stream = None
    if not a.no_viewer:
        push_since, stream = arm_probe_viewer(client, token, paths[modes[0]], sink)
    # Start only once the driver's navigations reach snowplow (run 7's first
    # samples ran during the login phase, with no read load at all).
    if a.wait_for_load:
        gate = wait_for_customer_load(client, token, min_delta=a.load_min_delta,
                                      max_wait_s=a.load_max_wait_s)
        sink({"kind": "fresh_start_gate", **gate})
    rates = a.rates if a.rates else [a.churn_rate]
    fresh_phases(client, token, sink, modes=modes, rates=rates, mutators=mutators,
                 paths=paths, targets=targets, probe=(a.namespace, a.probe_name),
                 samples=a.samples, poll_ms=a.poll_ms, gap_s=a.gap_s,
                 viewer=not a.no_viewer, push_since=push_since, stream=stream)
    return 0


FRESH_BUDGET_S = 30.0        # freshness_probe's per-sample convergence budget
FRESH_PHASE_SLACK = 1.25     # phase share = samples x (budget + gap) x slack


def fresh_phase_budget_s(samples: int, gap_s: float) -> float:
    """A phase's wall-clock share: every sample at its full convergence
    budget plus the gap, +25 % for mutate / last-poll overrun. A phase that
    needs more is truncated (recorded) instead of overrunning the bounds the
    measure wait and the TTL budget are computed from (reviewer-small (d))."""
    return samples * (FRESH_BUDGET_S + gap_s) * FRESH_PHASE_SLACK


def fresh_phases(client, token, sink, *, modes, rates, mutators, paths, targets, probe,
                 samples, poll_ms, gap_s, viewer, push_since=None, stream=None,
                 probe_fn=None, churn_factory=None, arm=None, healthy=None,
                 clock: Callable[[], float] = time.time):
    """Every (mode, rate) phase in order: modes outer, rates inner. Labels are
    "list_dep@<rate>" (annotation, unchanged from run 8) and
    "list_dep_status@<rate>" (status). The probe and the churn of a phase use
    that mode's mutator; the probe polls that mode's RESTAction. The viewer
    for the first mode is armed by the caller (before the start gate, as in
    run 8); each later mode re-arms on its own path."""
    probe_fn = probe_fn or freshness_probe
    churn_factory = churn_factory or ChurnDriver
    arm = arm or arm_probe_viewer
    healthy = healthy or (lambda: readyz(client))
    for i, mode in enumerate(modes):
        mut = mutators[mode]
        if i > 0 and viewer:
            if stream is not None:
                stream.close()
            push_since, stream = arm(client, token, paths[mode], sink)
        for rate in rates:
            rate = float(rate)
            label = f"{FRESH_MODE_LABEL[mode]}@{rate:g}"
            churn = None
            if rate > 0:
                churn = churn_factory(targets, rate,
                                      lambda t, m, mut=mut: mut(t[0], t[1], m)).start()
            probe_fn(
                client, token, paths[mode],
                lambda m, mut=mut: mut(probe[0], probe[1], m),
                samples=samples, poll_ms=poll_ms, gap_s=gap_s,
                label=label, sink=sink, healthy=healthy,
                push_since=push_since, observe=getattr(stream, "observe", None),
                viewer=stream if isinstance(stream, ProbeViewer) else None,
                deadline=clock() + fresh_phase_budget_s(samples, gap_s))
            if churn:
                sink({"kind": "churn", "label": label, **churn.stop()})
    if stream is not None:
        stream.close()


def arm_probe_viewer(client: HttpClient, token: str, path: str, sink,
                     stream_factory=RefreshStream):
    """Arm a /refreshes viewer on the probe cell, like the SPA would after
    rendering it. Returns (push_since, stream); (None, None) when snowplow did
    not stamp the refresh headers (then nothing can be armed — recorded)."""
    r = client.get(path, token)
    ref = r.refresh
    coords = refresh_coords(path, ref[1]) if ref else None
    if coords is None:
        sink({"kind": "fresh_viewer", "armed": False, "status": r.status,
              "why": "no X-Snowplow-Refresh-Key/Class on the probe's /call"})
        return None, None
    viewer = ProbeViewer(client.base, token, coords, ref[0], sink, stream_factory)
    # The passive-tab CONTROL (#817 re-gate): armed once on the same cell and
    # never re-armed, like an SPA tab that is open but not refetching. After a
    # key rotation its pushes stop: that is #819, measured directly.
    viewer.control = ProbeViewer(client.base, token, coords, ref[0], sink, stream_factory,
                                 role="control")
    sink({"kind": "fresh_viewer", "armed": True, "class": ref[1]})
    return viewer.push_since, viewer


class ProbeViewer:
    """The probe tab's /refreshes viewer on its one cell.

    Like the SPA (refreshSse.ts recordRefreshHeaders -> the arm hook's
    entry.key dependency), it RE-ARMS when a /call of the cell answers with a
    different X-Snowplow-Refresh-Key, and disarms when a successful /call
    carries no refresh headers (`observe`, fed every poll): the cell's key
    folds identity inputs that can change while the tab is open (binding UID,
    binding-set digest, RBAC sub-generation), and snowplow's armed set is
    frozen per connection. Run 9: the annotation probe's arm went dead minutes
    in, and 95 min of samples ran with no viewer, unrecorded (#817).

    The probe POLLS (every 100 ms), which a passive tab never does, so every
    arm has a generation (`gen`): samples are tagged with it, and `control`
    (role "control", never re-armed) is the passive-tab baseline."""

    def __init__(self, base: str, token: str, coords: dict, key: str, sink,
                 stream_factory=RefreshStream, clock: Callable[[], float] = time.time,
                 role: str = "probe"):
        self.base, self.token, self.coords = base, token, coords
        self.sink, self.stream_factory, self.clock = sink, stream_factory, clock
        self.role = role
        self.lock = threading.Lock()
        self.events: list[float] = []
        self.key = None
        self.stream = None
        self.gen = -1
        self.last_seen_t = None          # the last poll that still saw self.key
        self.control: "ProbeViewer | None" = None
        self._arm(key)

    def _record(self, state: str, **detail) -> None:
        self.sink({"kind": "fresh_viewer", "role": self.role, "state": state,
                   "t": self.clock(), **detail})

    def _arm(self, key: str | None) -> None:
        stream = None
        if key is not None:
            def on_refresh(k, t, armed=key):
                if k == armed:
                    with self.lock:
                        self.events.append(t)
            stream = self.stream_factory(
                self.base, self.token, [self.coords], on_refresh,
                on_state=lambda st, d: self._record(st, **d)).start()
        with self.lock:
            old, self.stream, self.key = self.stream, stream, key
            self.gen += 1
        if old is not None:
            old.close()

    def observe(self, resp: "Response") -> None:
        """One /call response of the cell: re-arm if its key rotated, disarm
        if a successful response carries no refresh headers (as the SPA)."""
        if resp.status != 200:
            return                       # the SPA records headers on success only
        new = resp.refresh[0] if resp.refresh else None
        now = self.clock()
        with self.lock:
            old, seen = self.key, self.last_seen_t
            if new == old:
                self.last_seen_t = now
                return
            self.last_seen_t = now
        if new is None:
            self._record("disarmed_no_headers", old_key=old[:16], last_old_t=seen)
        else:
            # The rotation happened between the last poll that saw the old key
            # and this one: both are recorded, so its time is bounded.
            self._record("key_rotated" if old else "rearmed",
                         old_key=(old or "")[:16], new_key=new[:16],
                         last_old_t=seen, first_new_t=now)
        self._arm(new)

    def push_since(self, t0: float) -> float | None:
        with self.lock:
            return next((t for t in self.events if t >= t0), None)

    def control_push_since(self, t0: float) -> float | None:
        return self.control.push_since(t0) if self.control else None

    def close(self) -> None:
        with self.lock:
            s, self.stream = self.stream, None
        if s is not None:
            s.close()
        if self.control is not None:
            self.control.close()


_MEM_FIELDS = (("HeapAlloc", "heap_alloc_mb"), ("HeapInuse", "heap_inuse_mb"),
               ("HeapIdle", "heap_idle_mb"), ("HeapReleased", "heap_released_mb"),
               ("HeapSys", "heap_sys_mb"), ("Sys", "sys_mb"), ("NextGC", "next_gc_mb"),
               ("StackInuse", "stack_inuse_mb"))


def gc_pauses(memstats: dict, prev_num_gc: int | None) -> tuple[list[float], int]:
    """The GC pauses (ms) since the sample that saw `prev_num_gc` GCs, from
    memstats' 256-entry ring (PauseNs[(k+255)%256] is GC number k's pause),
    and how many of them the ring no longer holds (more than 256 since)."""
    ring, num = memstats.get("PauseNs") or [], memstats.get("NumGC")
    if prev_num_gc is None or num is None or len(ring) != 256 or num < prev_num_gc:
        return [], 0
    first = max(prev_num_gc + 1, num - 255)
    return ([round(ring[(k + 255) % 256] / 1e6, 3) for k in range(first, num + 1)],
            first - prev_num_gc - 1)


_GOROUTINE_TOTAL = re.compile(rb"^goroutine profile: total (\d+)")
# Scheduler latency is a runtime/metrics histogram (/sched/latencies:seconds).
# snowplow publishes no runtime/metrics on /debug/vars (origin/main 7b3fdcdd),
# so a sample carries it only once an expvar of that name exists.
SCHED_LATENCY_KEY = "/sched/latencies:seconds"


def mem_sample(client: HttpClient, token: str | None,
               extra_prefixes: tuple[str, ...] = (),
               prev_num_gc: int | None = None) -> dict:
    """One memory sample: /readyz + /debug/vars memstats and a few counters,
    plus every expvar whose name starts with one of `extra_prefixes` (e.g. the
    #710 PR-0 fill counters — heap profiles cannot carry pprof labels, so the
    per-GVR / fill-reason attribution has to come from expvar during the
    burst). METADATA ONLY — never cache bodies.
    GC and scheduler evidence (run 12): the GC pauses since the previous
    sample (`prev_num_gc`), the GC's CPU share, and the goroutine count (the
    first line of /debug/pprof/goroutine?debug=1; nothing else of that body
    is kept)."""
    rec = {"kind": "mem", "t": time.time(),
           "readyz": client.get("/readyz", None).status}
    r = client.get("/debug/vars", token)
    rec["vars_status"] = r.status
    if r.status == 200:
        try:
            v = json.loads(r.body)
        except ValueError:
            return rec
        m = v.get("memstats") or {}
        for k, out in _MEM_FIELDS:
            if k in m:
                rec[out] = round(m[k] / 1048576, 1)
        rec["num_gc"] = m.get("NumGC")
        rec["gc_pauses_ms"], rec["gc_pauses_lost"] = gc_pauses(m, prev_num_gc)
        rec["gc_pause_total_ms"] = (round(m["PauseTotalNs"] / 1e6, 3)
                                    if m.get("PauseTotalNs") is not None else None)
        rec["gc_cpu_fraction"] = m.get("GCCPUFraction")
        rec["sched_latencies"] = v.get(SCHED_LATENCY_KEY)
        rec["heap_objects"] = m.get("HeapObjects")
        rec["customer_resolve_inflight"] = v.get("snowplow_customer_resolve_inflight")
        rec["refresher_queue_depth"] = v.get("snowplow_refresher_queue_depth")
        rc = v.get("snowplow_resolved_cache") or {}
        rec["cache_entries"], rec["cache_bytes"] = rc.get("entries"), rc.get("bytes")
        rec["prewarm_complete"] = v.get("snowplow_prewarm_complete")
        hit, miss = _sum_l1(v.get("snowplow_dispatch_l1_lookups"))
        rec["l1_lookups"] = (hit + miss) if hit is not None else None
        if extra_prefixes:
            rec["extra"] = {k: v[k] for k in sorted(v) if k.startswith(tuple(extra_prefixes))}
        g = client.get("/debug/pprof/goroutine?debug=1", token)
        n = _GOROUTINE_TOTAL.match(g.body) if g.status == 200 else None
        rec["goroutines"] = int(n.group(1)) if n else None
    return rec


# ─── CPU profiles (run 12) ──────────────────────────────────────────────────
#
# Run 11's storm analysis had heap profiles only: CPU cost could only be
# inferred from allocations. mem606 also takes CPU profiles
# (/debug/pprof/profile?seconds=N) on its own thread, so its memstats cadence
# holds while a profile runs:
#   storm     back to back from the sampler's start until `storm_s` after the
#             load is first seen (the login storm);
#   burst     back to back from `burst_pre_s` before to `burst_post_s` after
#             the composite burst (`burst_at_s` after the load is first seen);
#   periodic  one profile every `every_s` otherwise.
# Each one is a `cpu_profile` record with the window it covers; the files are
# cpu-NN-HHMMSS.pb.gz next to the heap profiles.


@dataclass(frozen=True)
class CpuPlan:
    profile_s: int = 30
    storm_s: float = 600.0
    every_s: float = 300.0
    burst_at_s: float | None = None
    burst_pre_s: float = 60.0
    burst_post_s: float = 240.0

    def window(self, now: float, anchor: float) -> str | None:
        """The back-to-back window `now` lies in (anchor = when the load was
        first seen, until then the sampler's start), None outside them."""
        if now < anchor + self.storm_s:
            return "storm"
        if self.burst_at_s is not None and \
                anchor + self.burst_at_s - self.burst_pre_s <= now < \
                anchor + self.burst_at_s + self.burst_post_s:
            return "burst"
        return None

    def next_window_start(self, now: float, anchor: float) -> float | None:
        b = None if self.burst_at_s is None else anchor + self.burst_at_s - self.burst_pre_s
        return b if b is not None and b > now else None


def cpu_profiler(client: HttpClient, plan: CpuPlan, *, token_fn: Callable[[], str | None],
                 anchor_fn: Callable[[], float], stopped: Callable[[], bool],
                 out_dir: str, sink: Callable[[dict], None],
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep,
                 retry_s: float = 5.0) -> int:
    """Take CPU profiles by `plan` until `stopped()`; returns how many were
    written. `client` must time out later than plan.profile_s."""
    n = 0
    last = None
    while not stopped():
        now, anchor = clock(), anchor_fn()
        window = plan.window(now, anchor)
        if window is None:
            if last is None or now - last >= plan.every_s:
                window = "periodic"
            else:
                due = [t for t in (last + plan.every_s, plan.next_window_start(now, anchor))
                       if t is not None]
                # at least 0.1 s: `now + (due - now)` can round below `due`
                sleep(max(0.1, min(min(due) - now, retry_s)))
                continue
        token = token_fn()
        if not token:
            sleep(retry_s)
            continue
        t0 = clock()
        r = client.get(f"/debug/pprof/profile?seconds={plan.profile_s}", token)
        t1 = clock()
        rec = {"kind": "cpu_profile", "t": t1, "t_start": t0, "t_end": t1,
               "seconds": plan.profile_s, "window": window, "status": r.status}
        if r.status == 200 and r.body:
            n += 1
            name = f"cpu-{n:02d}-{time.strftime('%H%M%S', time.gmtime(t0))}.pb.gz"
            with open(os.path.join(out_dir, name), "wb") as fh:
                fh.write(r.body)
            rec.update(profile=name, bytes=len(r.body))
            last = t0
            sink(rec)
        else:
            rec.update(profile=None, error=r.error or f"http {r.status}")
            sink(rec)
            sleep(retry_s)
    return n


def memwatch(client: HttpClient, login_fn: Callable[[], str], *, duration_s: float,
             interval_s: float = 15.0, profile_every_s: float = 120.0,
             out_dir: str = "/out", sink: Callable[[dict], None] = print,
             extra_prefixes: tuple[str, ...] = (),
             burst_interval_s: float = 5.0, burst_window_s: float = 60.0,
             load_min_delta: int = 100,
             stop_file: str | None = None,
             cpu: CpuPlan | None = None, cpu_client: HttpClient | None = None,
             clock: Callable[[], float] = time.time,
             sleep: Callable[[float], None] = time.sleep) -> int:
    """The standard run-time memory capture (run 7 → #710): a memstats sample
    every `interval_s`, and a heap profile (/debug/pprof/heap) at the FIRST
    Ready, every `profile_every_s` while Ready, and at every not-Ready → Ready
    transition (a restart). Profiles are written to out_dir; their names are
    emitted as `heap_profile` records.

    The stream is framed like the driver's (#817): `mem_start` first,
    `mem_end` (how many mem records) last, so a cut file is detected. It ends
    at duration_s, or as soon as `stop_file` exists (the collect step asks for
    the final stream before it copies it).

    With `cpu`, CPU profiles are taken on a second thread (cpu_profiler, on
    `cpu_client`, whose timeout outlasts a profile) by that plan, anchored at
    the load first seen; mem_start carries the plan, mem_end the count."""
    os.makedirs(out_dir, exist_ok=True)
    token = None
    end = clock() + duration_s
    samples = 0
    lock, closed = threading.Lock(), [False]
    raw_sink = sink

    def sink(rec):      # two writers; nothing follows mem_end
        with lock:
            if not closed[0]:
                raw_sink(rec)
    t_start = clock()
    sink({"kind": "mem_start", "t": t_start, "duration_s": duration_s,
          "interval_s": interval_s, **({"cpu_plan": asdict(cpu)} if cpu else {})})
    state = {"token": None, "anchor": t_start}
    stop_cpu = threading.Event()
    cpu_n = [0]
    cpu_thread = None
    if cpu:
        def run_cpu():
            cpu_n[0] = cpu_profiler(cpu_client or client, cpu,
                                    token_fn=lambda: state["token"],
                                    anchor_fn=lambda: state["anchor"],
                                    stopped=stop_cpu.is_set, out_dir=out_dir, sink=sink,
                                    sleep=stop_cpu.wait)
        # started by the loop once a sample authenticated, so the first
        # profile does not wait out the profiler's retry
        cpu_thread = threading.Thread(target=run_cpu, name="cpu-profiler", daemon=True)
    prev_gc = None
    # Burst resolution (#710: heap went 0.6 -> 6.35 GB in 16 s, so 15 s
    # samples see at most one point inside it): sample every burst_interval_s
    # until the load is seen (L1 lookups +load_min_delta from the first
    # reading) and for burst_window_s after that, then every interval_s.
    l1_base = None
    burst_until = None
    was_ready = False
    last_prof = None
    n = 0
    while clock() < end and not (stop_file and os.path.exists(stop_file)):
        if token is None:
            try:
                token = login_fn()
            except Exception as e:  # noqa: BLE001
                sink({"kind": "mem", "t": clock(), "login_error": type(e).__name__})
                samples += 1
        rec = mem_sample(client, token, extra_prefixes, prev_gc)
        rec["t"] = clock()
        prev_gc = rec.get("num_gc", prev_gc)
        if rec.get("vars_status") == 401:
            token = None
        state["token"] = token
        if cpu_thread is not None and token and cpu_thread.ident is None:
            cpu_thread.start()
        l1 = rec.get("l1_lookups")
        if l1 is not None:
            if l1_base is None:
                l1_base = l1
            elif burst_until is None and l1 - l1_base >= load_min_delta:
                burst_until = clock() + burst_window_s
                rec["load_detected"] = True
                state["anchor"] = rec["t"]
        fast = burst_until is None or clock() < burst_until
        rec["interval_s"] = burst_interval_s if fast else interval_s
        sink(rec)
        samples += 1
        ready = rec.get("readyz") == 200
        if ready and token and (not was_ready or last_prof is None
                                or clock() - last_prof >= profile_every_s):
            r = client.get("/debug/pprof/heap", token)
            if r.status == 200 and r.body:
                n += 1
                name = f"heap-{n:02d}-{time.strftime('%H%M%S', time.gmtime(clock()))}" \
                       f"{'-ready' if not was_ready else ''}.pb.gz"
                with open(os.path.join(out_dir, name), "wb") as fh:
                    fh.write(r.body)
                sink({"kind": "heap_profile", "t": clock(), "profile": name,
                      "bytes": len(r.body), "at": "ready" if not was_ready else "load"})
                last_prof = clock()
        was_ready = ready
        sleep(rec["interval_s"])
    stop_cpu.set()
    if cpu_thread is not None and cpu_thread.ident is not None:
        # a profile in flight ends within profile_s; past that, mem_end closes
        # the stream and a late record is dropped
        cpu_thread.join(cpu.profile_s + 30)
    sink({"kind": "mem_end", "t": clock(), "samples": samples, "profiles": n,
          **({"cpu_profiles": cpu_n[0]} if cpu else {}),
          "stopped": bool(stop_file and os.path.exists(stop_file))})
    closed[0] = True
    with open(os.path.join(out_dir, "done"), "w") as fh:
        fh.write("1")
    return n


def _cmd_memwatch(a) -> int:
    client = HttpClient(a.snowplow_url)
    sink = _jsonl_sink(a.out)
    cpu = (CpuPlan(profile_s=a.cpu_profile_s, storm_s=a.cpu_storm_s, every_s=a.cpu_every_s,
                   burst_at_s=a.burst_at_s) if a.cpu_profile_s > 0 else None)
    memwatch(client, lambda: login(a.authn_url, a.user, a.password),
             duration_s=a.duration_s, interval_s=a.interval_s,
             profile_every_s=a.profile_every_s, out_dir=a.out_dir, sink=sink,
             extra_prefixes=tuple(a.vars_prefix or ()), stop_file=a.stop_file,
             cpu=cpu, cpu_client=HttpClient(a.snowplow_url, timeout=a.cpu_profile_s + 30))
    # Stay up so the collect step can copy the profiles out (kubectl cp). The
    # mem606 Job's wrapper lingers itself (--linger-s 0), so a crash here
    # still leaves the stream copyable.
    time.sleep(a.linger_s)
    return 0


def _cmd_driver_stuck(a) -> int:
    try:
        pods = json.loads(sys.stdin.read() or "{}")
    except ValueError:
        return 0                         # an unreadable listing proves nothing
    why = driver_pod_stuck(pods, time.time(), pending_s=a.pending_s,
                           init_restarts=a.init_restarts)
    if why:
        print(why)
        return 1
    return 0


def _cmd_pod_record(a) -> int:
    print(json.dumps(pod_record(json.loads(sys.stdin.read() or "{}"), a.phase)))
    return 0


def _cmd_digest_gate(a) -> int:
    rec = image_digest_gate(json.loads(sys.stdin.read() or "{}"), a.expected,
                            require_settled=a.require_settled)
    print(json.dumps(rec))
    return {"MATCH": 0, "UNSETTLED": 4}.get(rec["result"], 3)


def _cmd_check_prefixes(a) -> int:
    missing = prefixes_missing(a.prefixes)
    if missing:
        print("run-8 keys no MEM_VARS_PREFIXES entry captures: " + " ".join(missing),
              file=sys.stderr)
        return 2
    return 0


def _cmd_evaluate(a) -> int:
    recs = []
    streams = []
    mem_streams = []
    for p in a.inputs:
        rs = _read_jsonl(p)
        recs.extend(rs)
        s = driver_stream(p, rs)
        if s is not None:
            streams.append(s)
        ms = framed_stream(MEM_FRAMING, p, rs)
        if ms is not None:
            mem_streams.append(ms)
    navs = [r for r in recs if r.get("kind") == "nav"]
    k6 = None
    if a.k6_out is not None or a.browser_out is not None:
        # #830 run 11: page loads come from the real browsers; k6 is the load
        from bench import k6eval
        k6 = {"k6": k6eval.read_k6_out(a.k6_out, "k6") if a.k6_out else None,
              "browser": k6eval.read_k6_out(a.browser_out, "browser") if a.browser_out else None,
              "viewer": k6eval.read_viewer_out(a.k6_out) if a.k6_out else None,
              "viewers": not a.no_viewers,
              "sessions": a.browser_sessions}
        navs = k6eval.browser_navs(k6["browser"])
    fresh = [r for r in recs if r.get("kind") == "fresh"]
    vs = sorted((r for r in recs if r.get("kind") == "vars"
                 and not r.get("unreadable")), key=lambda r: r.get("t", 0))
    tops = []
    for r in recs:
        if r.get("kind") == "top":
            parts = (r.get("raw") or "").split()
            r["rss_bytes"] = _parse_quantity(parts[2]) if len(parts) >= 3 else None
            tops.append(r)
    result = evaluate(navs, fresh, vs, tops=tops, run8=a.run8,
                      login_curves=[r for r in recs if r.get("kind") == "login_curve"],
                      preflights=[r for r in recs if r.get("kind") == "preflight"],
                      snowplow_pods=[r for r in recs if r.get("kind") == "snowplow_pod"],
                      mem=[r for r in recs if r.get("kind") in
                           ("mem_start", "mem", "heap_profile", "cpu_profile", "mem_end")],
                      expected_digest=a.expect_digest,
                      image_digests=[r for r in recs if r.get("kind") == "image_digest"],
                      push_refetches=[r for r in recs if r.get("kind") == "push_refetch"],
                      viewer_stats=[r for r in recs if r.get("kind") == "viewer_stats"],
                      fresh_truncated=[r for r in recs if r.get("kind") == "fresh_truncated"],
                      driver_streams=streams,
                      fresh_viewers=[r for r in recs if r.get("kind") == "fresh_viewer"],
                      mem_streams=mem_streams,
                      job_pods=[r for r in recs if r.get("kind") == "job_pod"],
                      viewer_dead=sorted((r for r in recs if r.get("kind") == "viewer_dead"),
                                         key=lambda r: r.get("t") or 0),
                      k6=k6)
    print(json.dumps(result, indent=2, default=str))
    return {"PASS": 0, "FAIL": 1, "INVALID": 3, "INCONCLUSIVE": 4}[result["verdict"]]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="load606")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("drive")
    d.add_argument("--snowplow-url", required=True)
    d.add_argument("--authn-url", required=True)
    d.add_argument("--password", required=True)
    d.add_argument("--user-prefix", default="scaleuser-")
    d.add_argument("--user-start", type=int, default=1)
    d.add_argument("--user-end", type=int, default=1000)
    d.add_argument("--composite-route", action="append", default=None,
                   help="composite page path (from preflight; repeatable, ranked)")
    d.add_argument("--burst-users", type=int, default=0,
                   help="reserve the LAST N users for the cold composite-page burst")
    d.add_argument("--burst-at-s", type=float, default=600.0,
                   help="seconds after the drive starts when the burst cohort arrives together")
    d.add_argument("--init", required=True,
                   help="the frontend config.json api.INIT (the app-shell Layout)")
    d.add_argument("--frontend-ns", default="krateo-system")
    d.add_argument("--builders-ns", default="krateo-system")
    d.add_argument("--think-s", type=float, default=60.0)
    d.add_argument("--duration-s", type=float, default=1800.0)
    d.add_argument("--nav-workers", type=int, default=128)
    d.add_argument("--call-workers", type=int, default=512)
    d.add_argument("--vars-interval", type=float, default=15.0)
    d.add_argument("--no-viewers", action="store_true",
                   help="do NOT open /refreshes viewers (#764: then #710/#740 cannot run)")
    d.add_argument("--out", required=True)
    d.set_defaults(func=_cmd_drive)
    f = sub.add_parser("fresh")
    f.add_argument("--snowplow-url", required=True)
    f.add_argument("--authn-url", required=True)
    f.add_argument("--user", required=True)
    f.add_argument("--password", required=True)
    f.add_argument("--kubeconfig", default=None)
    f.add_argument("--context", default=None)
    f.add_argument("--mutator", choices=["kubectl", "incluster"], default="kubectl")
    f.add_argument("--rates", nargs="*", default=None,
                   help="churn rates to step through (overrides --churn-rate)")
    f.add_argument("--group", default="composition.krateo.io")
    f.add_argument("--version", required=True)
    f.add_argument("--plural", required=True)
    f.add_argument("--namespace", default="bench-ns-01")
    f.add_argument("--probe-name", required=True)
    f.add_argument("--ra-namespace", default="bench-ns-01")
    f.add_argument("--samples", type=int, default=60)
    f.add_argument("--poll-ms", type=int, default=100)
    f.add_argument("--gap-s", type=float, default=5.0)
    f.add_argument("--wait-for-load", action="store_true",
                   help="start only after the driver's navigations reach snowplow")
    f.add_argument("--load-min-delta", type=int, default=100)
    f.add_argument("--load-max-wait-s", type=float, default=1800.0)
    f.add_argument("--churn-rate", type=float, default=0.0)
    f.add_argument("--churn-namespaces", type=int, default=50)
    f.add_argument("--churn-per-ns", type=int, default=20)
    f.add_argument("--name-prefix", default="bench-app-")
    f.add_argument("--modes", nargs="*", choices=list(FRESH_MODES), default=None,
                   help="churn modes as separate phases (default: annotation only)")
    f.add_argument("--no-viewer", action="store_true",
                   help="do NOT arm a /refreshes viewer on the probe cell (#764)")
    f.add_argument("--out", required=True)
    f.set_defaults(func=_cmd_fresh)
    pf = sub.add_parser("preflight")
    pf.add_argument("--snowplow-url", required=True)
    pf.add_argument("--authn-url", required=True)
    pf.add_argument("--user", required=True)
    pf.add_argument("--password", required=True)
    pf.add_argument("--init", required=True)
    pf.add_argument("--frontend-ns", default="krateo-system")
    pf.add_argument("--builders-ns", default="krateo-system")
    pf.add_argument("--full-walk", action="store_true",
                    help="also record the closure walk (report-only metric)")
    pf.set_defaults(func=_cmd_preflight)
    pc = sub.add_parser("portal-config",
                        help="config.json on stdin -> PORTAL_* shell exports")
    pc.set_defaults(func=_cmd_portal_config)
    ps = sub.add_parser("preflight-summary", help="preflight JSONL on stdin -> summary")
    ps.set_defaults(func=_cmd_preflight_summary)
    pcx = sub.add_parser("preflight-composite", help="preflight JSONL on stdin -> composite routes")
    pcx.set_defaults(func=_cmd_preflight_composite)
    mw = sub.add_parser("memwatch", help="in-cluster memstats + heap profile sampler")
    mw.add_argument("--snowplow-url", required=True)
    mw.add_argument("--authn-url", required=True)
    mw.add_argument("--user", required=True)
    mw.add_argument("--password", required=True)
    mw.add_argument("--duration-s", type=float, required=True)
    mw.add_argument("--interval-s", type=float, default=15.0)
    mw.add_argument("--profile-every-s", type=float, default=120.0)
    mw.add_argument("--out-dir", default="/out")
    mw.add_argument("--linger-s", type=float, default=3600.0)
    mw.add_argument("--stop-file", default=None,
                    help="end the capture early once this file exists (collect)")
    mw.add_argument("--vars-prefix", action="append", default=None,
                    help="also capture every expvar starting with this prefix (repeatable)")
    mw.add_argument("--out", default="/dev/stdout")
    mw.add_argument("--cpu-profile-s", type=int, default=30,
                    help="CPU profile length (s); 0 = no CPU profiles")
    mw.add_argument("--cpu-storm-s", type=float, default=600.0,
                    help="back-to-back CPU profiles until this long after the load is seen")
    mw.add_argument("--cpu-every-s", type=float, default=300.0,
                    help="one CPU profile every this many seconds outside the windows")
    mw.add_argument("--burst-at-s", type=float, default=None,
                    help="the composite burst, seconds after the load is seen")
    mw.set_defaults(func=_cmd_memwatch)
    ds = sub.add_parser("driver-stuck", help="kubectl get pods -l job-name=<job> -o json on "
                        "stdin -> exit 1 + the reason when a driver pod will not start")
    ds.add_argument("--pending-s", type=float, default=DRIVER_PENDING_S)
    ds.add_argument("--init-restarts", type=int, default=DRIVER_INIT_RESTARTS)
    ds.set_defaults(func=lambda a: _cmd_driver_stuck(a))
    jp = sub.add_parser("job-pod-record",
                        help="kubectl get pods -l job-name=<job> -o json on stdin -> job_pod records")
    jp.add_argument("--job", required=True)
    jp.set_defaults(func=lambda a: print("\n".join(
        json.dumps(r) for r in job_pod_records(json.loads(sys.stdin.read() or "{}"), a.job)))
        or 0)
    lt = sub.add_parser("login-tokens", help="#830: sign users in; bearer tokens to a pod-local file")
    lt.add_argument("--authn-url", required=True)
    lt.add_argument("--password", required=True)
    lt.add_argument("--user-prefix", default="scaleuser-")
    lt.add_argument("--user-start", type=int, required=True)
    lt.add_argument("--user-end", type=int, required=True)
    lt.add_argument("--max-concurrency", type=int, default=64)
    lt.add_argument("--out", required=True)
    lt.set_defaults(func=_cmd_login_tokens)
    vf = sub.add_parser("view-feed", help="#830: open tabs for k6's navigations (sidecar)")
    vf.add_argument("--snowplow-url", required=True)
    vf.add_argument("--tokens", required=True, help="login-tokens' file (user order)")
    vf.add_argument("--user-prefix", default="scaleuser-")
    vf.add_argument("--feed", required=True, help="k6's console feed (JSON lines)")
    vf.add_argument("--stop-file", required=True, help="appears when k6 has exited")
    vf.add_argument("--out", required=True)
    vf.add_argument("--refetch-workers", type=int, default=64)
    vf.add_argument("--poll-s", type=float, default=0.2)
    vf.set_defaults(func=_cmd_view_feed)
    pr = sub.add_parser("pod-record", help="kubectl get pods -o json on stdin -> snowplow_pod record")
    pr.add_argument("--phase", required=True, choices=["before", "after"])
    pr.set_defaults(func=_cmd_pod_record)
    dg = sub.add_parser("digest-gate",
                        help="kubectl get pods -o json on stdin -> image_digest record; exit 3 = VOID")
    dg.add_argument("--expected", required=True, help="sha256:<64 hex>")
    dg.add_argument("--require-settled", action="store_true",
                    help="exit 4 (UNSETTLED) while any snowplow pod is still Terminating")
    dg.set_defaults(func=_cmd_digest_gate)
    cp = sub.add_parser("check-prefixes", help="exit 2 unless the prefixes capture every RUN8_KEYS value")
    cp.add_argument("prefixes", nargs="*")
    cp.set_defaults(func=_cmd_check_prefixes)
    vp = sub.add_parser("prefixes", help="print RUN8_VARS_PREFIXES (run606's default)")
    vp.set_defaults(func=lambda a: print(" ".join(RUN8_VARS_PREFIXES)) or 0)
    e = sub.add_parser("evaluate")
    e.add_argument("inputs", nargs="+")
    e.add_argument("--expect-digest", default=None,
                   help="INVALID (VOID) unless the image-digest gate and pod records show this digest")
    e.add_argument("--k6-out", default=None,
                   help="#830: the k6 pod's collected /out (k6.json.gz, summary, exit)")
    e.add_argument("--browser-out", default=None,
                   help="#830: the browser pod's collected /out; its page loads are the verdict's")
    e.add_argument("--browser-sessions", type=int, default=None)
    e.add_argument("--no-viewers", action="store_true",
                   help="#830: the run had no open tabs (VIEWERS=0): no viewer checks")
    e.add_argument("--run8", action="store_true",
                   help="add the #710 run-8 memory-risk / stale_to_push report and checks")
    e.set_defaults(func=_cmd_evaluate)
    a = p.parse_args(argv)
    return int(a.func(a))


if __name__ == "__main__":
    sys.exit(main())
