// pageload.js — the judged instrument of browser.js (#830/#831), as pure
// functions with no k6 import, so node runs them (tests/test_k6_830.py) and
// browser.js ships `settledNow` / `loadOf` into the page as source text.
//
// "Page loaded": no `[data-widget-renderer]` element left for QUIET_MS.
// WidgetLoading, the skeleton every pending widget shows (components/
// WidgetRenderer/WidgetRenderer.tsx:131-134 and the Suspense fallback :69), is
// the ONLY element carrying that attribute (components/WidgetStates/
// WidgetStates.tsx:7-10), so "none left" means every widget in the tree has
// its data or a final state. The load ENDS when the last skeleton disappeared
// (not at the end of the quiet window). QUIET_MS (1000) exceeds
// useProgressiveLoading's 500 ms batch reveal (hooks/useProgressiveLoading.ts
// :13-33), so a list still revealing is not "loaded".
//
// The sign-in is NOT a page load (#831 gate, High): authn's login (run 10: a
// median ~3.1 s, an x509 certificate per login) happens on /login. The first
// page's clock starts when the app LEAVES /login — Login.tsx:92 navigates
// once authn answered — and the submit -> leave time is recorded apart, as
// `signinMs` (report only).

export const QUIET_MS = 1000

// Installed in every document before the app runs (context.addInitScript).
export const OBSERVER = `(() => {
  const b = window.__bench = { count: 0, goneAt: 0, navT0: 0, saw: false,
                               armLogin: false, submitAt: 0, signinMs: null, armClick: false };
  const sel = '[data-widget-renderer]';
  const tick = () => {
    const n = document.querySelectorAll(sel).length;
    const now = performance.now();
    if (n > 0) b.saw = true;
    if (n === 0 && b.count > 0) b.goneAt = now;
    b.count = n;
  };
  new MutationObserver(tick).observe(document, { childList: true, subtree: true });
  // the first page's clock: the moment the app leaves /login
  const leave = () => {
    if (!b.armLogin || window.location.pathname === '/login') return;
    const now = performance.now();
    b.armLogin = false;
    b.signinMs = now - b.submitAt;
    b.navT0 = now; b.goneAt = 0;
    b.count = document.querySelectorAll(sel).length;
    b.saw = b.count > 0;
  };
  for (const k of ['pushState', 'replaceState']) {
    const orig = history[k];
    history[k] = function () { const r = orig.apply(this, arguments); leave(); return r; };
  }
  window.addEventListener('popstate', leave);
  // a route click's clock starts AT the click, in the page (not when k6's
  // driver, a CDP round trip earlier, armed it)
  document.addEventListener('click', () => {
    if (!b.armClick) return;
    b.armClick = false;
    b.navT0 = performance.now(); b.saw = false; b.goneAt = 0;
  }, true);
  // live updates: each SSE stream counted when it OPENS (an answered request)
  const sse = window.__benchSse = { refreshes: 0, notifications: 0 };
  const f = window.fetch;
  window.fetch = function (input, init) {
    const url = String((input && input.url) || input);
    const p = f.apply(this, arguments);
    if (url.indexOf('/refreshes') >= 0) {
      p.then((r) => { if (r.ok) sse.refreshes++ }, () => {});
    }
    return p;
  };
  const ES = window.EventSource;
  if (ES) {
    window.EventSource = function (url, opts) {
      const es = new ES(url, opts);
      if (String(url).indexOf('/notifications') >= 0) {
        es.addEventListener('open', () => { sse.notifications++ });
      }
      return es;
    };
    window.EventSource.prototype = ES.prototype;
    ['CONNECTING', 'OPEN', 'CLOSED'].forEach((k) => { window.EventSource[k] = ES[k] });
  }
  // #919: every /call the page fetches, from its own Resource Timing entry
  // (callOf); browser.js drains window.__benchCalls into browser_call_ms
  const calls = window.__benchCalls = [];
  const callOf = ${callOf.toString()};
  if (window.PerformanceObserver) {
    try {
      new window.PerformanceObserver((list) => {
        for (const e of list.getEntries()) {
          const c = callOf(e.name, window.location.href, e.duration, e.responseStatus);
          if (c) calls.push(c);
        }
      }).observe({ type: 'resource', buffered: true });
    } catch (e) { /* no Resource Timing: no call recorded, the evaluator says none came */ }
  }
})()`

// #919: one /call request as the page saw it — its Resource Timing entry
// (`duration`: start to the response's last byte; `responseStatus`, 0 when
// the browser does not expose it) — tagged with the /call's resource, name
// and namespace: those query parameters ONLY, never the rest of the URL (the
// SPA puts the signed-in user into /call's extras). Not a /call: null.
// k6 browser's own per-request metric (browser_http_req_duration) cannot
// carry them: its one per-request tag hook (page.on('metric') tag()) names
// a URL pattern with a fixed string and needs the `url` system tag, which
// would put the extras into every point. Self-contained, like settledNow
// (shipped into the page as source text).
function callOf (url, base, duration, status) {
  let u
  try { u = new URL(url, base) } catch (e) { return null }
  if (!/\/call$/.test(u.pathname)) return null
  const q = u.searchParams
  return {
    resource: q.get('resource') || '',
    name: q.get('name') || '',
    namespace: q.get('namespace') || '',
    ms: duration,
    status: status || 0,
  }
}

// The metric points the calls drained from one page write: browser_call_ms,
// tagged with the page's own tags (session, route, visit, during) and the
// call's resource, name, namespace and status.
export function callOps (calls, tags) {
  return (calls || []).map((c) => ['browser_call_ms', c.ms, Object.assign({}, tags, {
    call_resource: c.resource, call_name: c.name, call_namespace: c.namespace, call_status: String(c.status),
  })])
}

// A request's per-request tags (#919): `request` tells a /call from any
// other request — "call", else Chrome's resource type (document, script,
// stylesheet, fetch, eventsource, ...) — and a /call carries its resource /
// name / namespace parameters, the same /call tags as browser_call_ms. No
// path and no other query parameter: a pathname can carry a name, and the
// query carries the extras (the user). Plain string work: k6's runtime has no
// URL global.
export function requestTags (url, resourceType) {
  const s = String(url || '').split('#')[0]
  const qi = s.indexOf('?')
  const path = (qi < 0 ? s : s.slice(0, qi)).replace(/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/[^/]*/, '')
  const call = /\/call$/.test(path)
  const q = {}
  if (call && qi >= 0) {
    for (const kv of s.slice(qi + 1).split('&')) {
      const i = kv.indexOf('=')
      const k = i < 0 ? kv : kv.slice(0, i)
      if (k === 'resource' || k === 'name' || k === 'namespace') {
        try { q[k] = decodeURIComponent((i < 0 ? '' : kv.slice(i + 1)).replace(/\+/g, ' ')) } catch (e) { q[k] = '' }
      }
    }
  }
  return {
    request: call ? 'call' : String(resourceType || 'other').toLowerCase(),
    call_resource: q.resource || '',
    call_name: q.name || '',
    call_namespace: q.namespace || '',
  }
}

// #919 (N7): the browser requests that never answered. k6's browser module
// emits no duration and no failed point for a request that failed at network
// level (network_manager.go emitResponseMetrics runs on a response only; run
// 16 B: 99,504 browser_data_sent vs 94,497 durations), so browser.js keeps
// this ledger from the page's own events — request, response,
// requestfinished, requestfailed — and emits one browser_request_incomplete_ms
// point for each request that FAILED (refused, reset, timed out, aborted —
// the page's own cancel included: Chrome's errorText says which) or NEVER
// COMPLETED by the end of the session. Value: ms from the request's start to
// its failure, or to the session's end. Point time: when it was emitted — the
// failure, or the session's end — so the request STARTED at the point's time
// minus its value. Tags: requestTags, outcome (failed | never_completed),
// error, answered ("yes" when its response headers had arrived: a live
// stream still open is a never_completed request that WAS answered; "no": no
// answer at all).
// A failed request whose start the ledger never saw has no duration: it is a
// browser_request_failed_start_unseen count (1, the same tags), never a
// made-up 0 ms.
// k6 hands a new Request object to each event, so a request is matched by
// method + URL, oldest start first: the counts are exact; two identical
// requests (same method and URL) in flight at once may swap durations.
export function requestLedger () {
  const open = new Map()                  // method + URL -> [{t0, type, answered}], oldest first
  const key = (method, url) => `${method} ${url}`
  const take = (k) => {
    const q = open.get(k)
    const e = q && q.length ? q.shift() : null
    if (q && !q.length) open.delete(k)
    return e
  }
  const tags = (url, type, outcome, error, answered) =>
    Object.assign(requestTags(url, type), { outcome, error, answered: answered ? 'yes' : 'no' })
  let ended = false                       // the session's end: unfinished() reported the rest
  return {
    started (method, url, t, resourceType) {
      if (ended) return
      const k = key(method, url)
      if (!open.has(k)) open.set(k, [])
      open.get(k).push({ t0: t, type: resourceType, answered: false })
    },
    // its response headers arrived (the oldest such request not yet answered)
    responded (method, url) {
      const e = (open.get(key(method, url)) || []).find((x) => !x.answered)
      if (e) e.answered = true
    },
    finished (method, url) { take(key(method, url)) },
    // the point for a failed request; null after the session's end: closing
    // the page aborts what is still open, and unfinished() reported it
    failed (method, url, t, error, resourceType) {
      if (ended) return null
      const e = take(key(method, url))
      if (!e) {
        return ['browser_request_failed_start_unseen', 1,
          Object.assign(tags(url, resourceType, 'failed', error || 'unknown', false), { start_seen: 'false' })]
      }
      return ['browser_request_incomplete_ms', t - e.t0, tags(url, e.type, 'failed', error || 'unknown', e.answered)]
    },
    // every request still open at `t` (the session's end): never completed
    unfinished (t) {
      ended = true
      const out = []
      for (const [k, q] of open) {
        const url = k.slice(k.indexOf(' ') + 1)
        for (const e of q) out.push(['browser_request_incomplete_ms', t - e.t0, tags(url, e.type, 'never_completed', '', e.answered)])
      }
      open.clear()
      return out
    },
    size () { let n = 0; for (const q of open.values()) n += q.length; return n },
  }
}

// Is the page loaded at `now`? Self-contained: browser.js sends its source
// into the page (declared WITHOUT `export`: k6's goja keeps the keyword in
// Function.prototype.toString, which the page cannot parse; exported below). arg = {path, quiet}; path null = the sign-in arrival, which
// lands wherever the user's own menu sends it, once the app has left /login.
function settledNow (b, here, now, arg) {
  if (!b) return false
  if (arg.path) {
    if (here !== arg.path) return false
  } else if (b.armLogin || here === '/login' || here === '/') {
    return false
  }
  if (b.count > 0) return false
  const end = b.saw ? b.goneAt : b.navT0
  return now - end >= arg.quiet
}

// The load just settled: its time, where it landed, and the widgets' end
// states (data-testid widget-error / -timeout / -forbidden / -notfound,
// WidgetStates.tsx:34,54,75,89). Self-contained, like settledNow.
function loadOf (b, here, testids) {
  const end = b.saw ? b.goneAt : b.navT0
  const states = { error: 0, timeout: 0, forbidden: 0, notfound: 0 }
  for (const id of testids) {
    const k = String(id).replace(/^widget-/, '')
    if (k in states) states[k]++
  }
  return { ms: end - b.navT0, saw: b.saw, path: here, states }
}

// The metric points one load writes. A page that did not settle within its
// timeout is a TIMEOUT and still a load, at its timeout value, so it counts in
// the p95 as well as in the timeout check (#831 gate). skeleton=no: painted
// from the app's own React Query cache (App.tsx:25-30), no request.
export function recordOps (r, tags, timeoutMs) {
  if (r === null) {
    const t = Object.assign({}, tags, { skeleton: 'timeout', timeout: 'yes' })
    return [['browser_page_timeouts', 1, tags], ['browser_page_load_ms', timeoutMs, t],
      ['browser_page_loads', 1, t]]
  }
  const t = Object.assign({}, tags, { skeleton: r.saw ? 'yes' : 'no', timeout: 'no' })
  const ops = [['browser_page_load_ms', r.ms, t], ['browser_page_loads', 1, t]]
  for (const k of Object.keys(r.states || {})) {
    if (r.states[k]) ops.push(['browser_widget_states', r.states[k], Object.assign({}, t, { state: k })])
  }
  return ops
}

export { callOf, loadOf, settledNow }
