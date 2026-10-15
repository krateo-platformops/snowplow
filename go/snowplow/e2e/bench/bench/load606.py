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
import concurrent.futures
import gzip
import json
import math
import os
import random
import re
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
                                (time.perf_counter() - t0) * 1000)
        except urllib.error.HTTPError as e:
            return Response(e.code, b"", (time.perf_counter() - t0) * 1000,
                            f"http {e.code}")
        except Exception as e:  # noqa: BLE001 — every failure is a sample
            return Response(0, b"", (time.perf_counter() - t0) * 1000,
                            f"{type(e).__name__}: {e}")


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
                      nbytes, round(slowest, 1), samples, statuses, docs, depth)


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
            soonest = min(nxt.values())
            self.sleep(max(0.0, min(0.05, soonest - self.clock())))
        for f in futures:
            f.result()
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
    out = []
    for i in range(samples):
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
        while clock() - t0 < budget_s:
            polls += 1
            r = client.get(path, token)
            if r.status == 200 and marker.encode() in r.body:
                lat = round((clock() - t0) * 1000, 1)
                break
            sleep(poll_ms / 1000.0)
        rec = {"kind": "fresh", "label": label, "marker": marker,
               "converged": lat is not None, "latency_ms": lat,
               "polls": polls, "poll_ms": poll_ms, "t": t0}
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
    p = items[0]
    cs = ((p.get("status") or {}).get("containerStatuses") or [{}])[0]
    return {"kind": "snowplow_pod", "phase": phase,
            "pod": (p.get("metadata") or {}).get("name"),
            "restarts": cs.get("restartCount", 0),
            "ready": cs.get("ready"),
            "last_state": cs.get("lastState") or {},
            "limits": (((p.get("spec") or {}).get("containers") or [{}])[0]
                       .get("resources") or {}).get("limits"),
            "t": time.time()}


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
}
# The /debug/vars prefixes memwatch must capture so every key above is seen
# (also used as run606's MEM_VARS_PREFIXES default).
RUN8_VARS_PREFIXES = (
    "snowplow_apistage_content_fill",     # PR-0 family + #719 admission_*
    "snowplow_nested_resolve_",           # #720 nested gate
    "snowplow_apistage_stale_",           # PR-2b stale_to_push / overdue / debt
    "snowplow_refresher_dirty_to_fresh",  # windowed gauges: poll the whole run
    "snowplow_resolved_cache",
    "snowplow_informer_dispatch_shape",
    "snowplow_build_info",
)
STALE_TO_PUSH_P95_BAR_MS = 3000
STALE_TO_PUSH_MAX_BAR_MS = 10000
STALE_TO_PUSH_GOAL_MS = 1000
BURST_WINDOW_S = 120.0


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


def run8_report(navs: list[dict], mem: list[dict], tops: list[dict],
                pods: list[dict] | None) -> tuple[dict, list[dict]]:
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

    # ── stale_overdue_total ──────────────────────────────────────────────
    key = RUN8_KEYS["stale_overdue"]
    vals = [_num(v) for v in _extra_values(mem, key)]
    vals = [v for v in vals if v is not None]
    overdue = (vals[-1] - vals[0]) if len(vals) >= 2 else (vals[0] if vals else None)
    chk("run8.stale_overdue_total", key, overdue, "reported",
        "FAIL" if not vals else "PASS", None if vals else "not exposed")

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
              "restart": restart,
              "admission_wait_ms": wait, "refresher_dirty_to_fresh": d2f, "build": build,
              "keys": RUN8_KEYS}
    return report, checks


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
             run8: bool = False) -> dict:
    """North-star verdict from the run's JSONL records.

    Returns {"verdict": PASS|FAIL|INVALID, "checks": [...], "economics": ...}.
    INVALID (the driver, not snowplow, failed) wins over FAIL.
    """
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
    gate_labels = set(th.get("fresh_gate_labels", ("list_dep@10",)))
    curve = {}
    labels = sorted({f.get("label", "?") for f in fresh})
    rated = [l for l in labels if "@" in l]
    if rated and not (gate_labels & set(rated)):
        checks.append({"check": f"fresh_p95_ms[{'/'.join(sorted(gate_labels))}]",
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

    _check("driver.sched_lag_p95_ms",
           pct([n.get("sched_lag_ms", 0) for n in navs], 95), "<=",
           th["max_sched_lag_p95_ms"], validity)
    _check("driver.error_rate", round(errors / calls, 4) if calls else None,
           "<=", th["max_error_rate"], validity)
    _check("driver.warm_navs", len(warm), ">=", th["min_warm_navs"], validity)

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
        "nav_kinds": {k: sum(1 for n in navs if n.get("nav") == k)
                      for k in sorted({n.get("nav") for n in navs if n.get("nav")})},
        "login": login_report(login_curves or []),
        "preflight": (preflights or [None])[-1],
    }

    restart = restart_check(snowplow_pods)
    if restart is not None:
        validity.append(restart)
    report["snowplow_pod"] = snowplow_pods
    report["memory"] = memory_report(mem or [])
    report["first_loads_failed"] = sum(1 for n in navs if n.get("first_load_failed"))
    fv = [f for f in fresh if f.get("valid") is False]
    report["fresh_invalid_samples"] = len(fv)

    if run8:
        r8, r8_checks = run8_report(navs, mem or [], tops or [], snowplow_pods)
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
    return {"verdict": verdict, "checks": checks, "validity": validity,
            "report": report, "economics": econ, "thresholds": th}


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


def _cmd_drive(a) -> int:
    client = HttpClient(a.snowplow_url)
    portal = _portal_from_args(a)
    users = [f"{a.user_prefix}{i:04d}" for i in range(a.user_start, a.user_end + 1)]
    sink = _jsonl_sink(a.out)
    # Guard in seconds, BEFORE the login burst: the first user's INIT must be
    # servable (the run606 preflight already checked; this keeps `drive`
    # standalone-safe — run 6 spent 54 min logging in before discovering it).
    try:
        tok0 = login(a.authn_url, users[0], a.password)
    except Exception as e:  # noqa: BLE001
        print(f"first login failed: {e}", file=sys.stderr)
        return 2
    r0 = client.get(portal.init, tok0)
    if classify(r0) == "config":
        print(f"INIT {portal.init} -> {r0.status}: not servable on this cluster; refusing",
              file=sys.stderr)
        return 2
    tokens, curve, failed = adaptive_login(
        users[1:], lambda u: login(a.authn_url, u, a.password))
    tokens[users[0]] = tok0
    sink({"kind": "login_curve", "users": len(users), "failed": len(failed),
          "curve": curve})
    if failed:
        print(f"only {len(tokens)}/{len(users)} users logged in; refusing a "
              f"partial population", file=sys.stderr)
        return 2
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
    Driver(client, tokens, portal, think_s=a.think_s, duration_s=a.duration_s,
           nav_workers=a.nav_workers, call_workers=a.call_workers,
           sink=sink, composite_routes=a.composite_route or [],
           burst_users=burst_users,
           burst_at_s=a.burst_at_s if burst_users else None).run()
    stop.set()
    if th:
        th.join(timeout=a.vars_interval + 30)
    return 0


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
    else:
        def patch(ns, name, value):
            return incluster_annotate(a.group, a.version, a.plural, ns, name, value)

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
    # Start only once the driver's navigations reach snowplow (run 7's first
    # samples ran during the login phase, with no read load at all).
    if a.wait_for_load:
        gate = wait_for_customer_load(client, token, min_delta=a.load_min_delta,
                                      max_wait_s=a.load_max_wait_s)
        sink({"kind": "fresh_start_gate", **gate})
    rates = a.rates if a.rates else [a.churn_rate]
    for rate in rates:
        rate = float(rate)
        label = f"list_dep@{rate:g}"
        churn = None
        if rate > 0:
            churn = ChurnDriver(targets, rate,
                                lambda t, m: patch(t[0], t[1], m)).start()
        freshness_probe(
            client, token, path, lambda m: patch(a.namespace, a.probe_name, m),
            samples=a.samples, poll_ms=a.poll_ms, gap_s=a.gap_s,
            label=label, sink=sink, healthy=lambda: readyz(client))
        if churn:
            sink({"kind": "churn", "label": label, **churn.stop()})
    return 0


_MEM_FIELDS = (("HeapAlloc", "heap_alloc_mb"), ("HeapInuse", "heap_inuse_mb"),
               ("HeapIdle", "heap_idle_mb"), ("HeapReleased", "heap_released_mb"),
               ("HeapSys", "heap_sys_mb"), ("Sys", "sys_mb"), ("NextGC", "next_gc_mb"),
               ("StackInuse", "stack_inuse_mb"))


def mem_sample(client: HttpClient, token: str | None,
               extra_prefixes: tuple[str, ...] = ()) -> dict:
    """One memory sample: /readyz + /debug/vars memstats and a few counters,
    plus every expvar whose name starts with one of `extra_prefixes` (e.g. the
    #710 PR-0 fill counters — heap profiles cannot carry pprof labels, so the
    per-GVR / fill-reason attribution has to come from expvar during the
    burst). METADATA ONLY — never cache bodies."""
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
    return rec


def memwatch(client: HttpClient, login_fn: Callable[[], str], *, duration_s: float,
             interval_s: float = 15.0, profile_every_s: float = 120.0,
             out_dir: str = "/out", sink: Callable[[dict], None] = print,
             extra_prefixes: tuple[str, ...] = (),
             burst_interval_s: float = 5.0, burst_window_s: float = 60.0,
             load_min_delta: int = 100,
             clock: Callable[[], float] = time.time,
             sleep: Callable[[float], None] = time.sleep) -> int:
    """The standard run-time memory capture (run 7 → #710): a memstats sample
    every `interval_s`, and a heap profile (/debug/pprof/heap) at the FIRST
    Ready, every `profile_every_s` while Ready, and at every not-Ready → Ready
    transition (a restart). Profiles are written to out_dir; their names are
    emitted as `heap_profile` records."""
    os.makedirs(out_dir, exist_ok=True)
    token = None
    end = clock() + duration_s
    # Burst resolution (#710: heap went 0.6 -> 6.35 GB in 16 s, so 15 s
    # samples see at most one point inside it): sample every burst_interval_s
    # until the load is seen (L1 lookups +load_min_delta from the first
    # reading) and for burst_window_s after that, then every interval_s.
    l1_base = None
    burst_until = None
    was_ready = False
    last_prof = None
    n = 0
    while clock() < end:
        if token is None:
            try:
                token = login_fn()
            except Exception as e:  # noqa: BLE001
                sink({"kind": "mem", "t": clock(), "login_error": type(e).__name__})
        rec = mem_sample(client, token, extra_prefixes)
        rec["t"] = clock()
        if rec.get("vars_status") == 401:
            token = None
        l1 = rec.get("l1_lookups")
        if l1 is not None:
            if l1_base is None:
                l1_base = l1
            elif burst_until is None and l1 - l1_base >= load_min_delta:
                burst_until = clock() + burst_window_s
                rec["load_detected"] = True
        fast = burst_until is None or clock() < burst_until
        rec["interval_s"] = burst_interval_s if fast else interval_s
        sink(rec)
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
    with open(os.path.join(out_dir, "done"), "w") as fh:
        fh.write("1")
    return n


def _cmd_memwatch(a) -> int:
    client = HttpClient(a.snowplow_url)
    sink = _jsonl_sink(a.out)
    memwatch(client, lambda: login(a.authn_url, a.user, a.password),
             duration_s=a.duration_s, interval_s=a.interval_s,
             profile_every_s=a.profile_every_s, out_dir=a.out_dir, sink=sink,
             extra_prefixes=tuple(a.vars_prefix or ()))
    # Stay up so the collect step can copy the profiles out (kubectl cp).
    time.sleep(a.linger_s)
    return 0


def _cmd_pod_record(a) -> int:
    print(json.dumps(pod_record(json.loads(sys.stdin.read() or "{}"), a.phase)))
    return 0


def _cmd_evaluate(a) -> int:
    recs = []
    for p in a.inputs:
        recs.extend(_read_jsonl(p))
    navs = [r for r in recs if r.get("kind") == "nav"]
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
                      mem=[r for r in recs if r.get("kind") in ("mem", "heap_profile")])
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
    mw.add_argument("--vars-prefix", action="append", default=None,
                    help="also capture every expvar starting with this prefix (repeatable)")
    mw.add_argument("--out", default="/dev/stdout")
    mw.set_defaults(func=_cmd_memwatch)
    pr = sub.add_parser("pod-record", help="kubectl get pods -o json on stdin -> snowplow_pod record")
    pr.add_argument("--phase", required=True, choices=["before", "after"])
    pr.set_defaults(func=_cmd_pod_record)
    e = sub.add_parser("evaluate")
    e.add_argument("inputs", nargs="+")
    e.add_argument("--run8", action="store_true",
                   help="add the #710 run-8 memory-risk / stale_to_push report and checks")
    e.set_defaults(func=_cmd_evaluate)
    a = p.parse_args(argv)
    return int(a.func(a))


if __name__ == "__main__":
    sys.exit(main())
