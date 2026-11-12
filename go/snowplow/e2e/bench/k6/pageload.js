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
  // live updates: each SSE stream counted when it OPENS (an answered request).
  // The SPA reads both with fetch + Bearer (refreshSse.ts; sseClient.ts from
  // frontend 1.8.11, 18df61d): a fetch opened its stream when the response
  // is ok AND an event stream. An ok response of another type is not one:
  // behind a path-mode gateway a path no route claims falls to the SPA,
  // whose nginx answers 200 text/html (try_files ... /index.html; #999
  // review B1). An older SPA opens /notifications as an EventSource: counted
  // at its 'open' (the browser opens one only on text/event-stream).
  const sse = window.__benchSse = { refreshes: 0, notifications: 0 };
  const sseStreamOf = ${sseStreamOf.toString()};
  const f = window.fetch;
  window.fetch = function (input, init) {
    const s = sseStreamOf(String((input && input.url) || input), window.location.href);
    const p = f.apply(this, arguments);
    if (s) {
      p.then((r) => {
        const type = (r.headers && r.headers.get && r.headers.get('content-type')) || '';
        if (r.ok && type.toLowerCase().indexOf('text/event-stream') === 0) sse[s]++
      }, () => {});
    }
    return p;
  };
  const ES = window.EventSource;
  if (ES) {
    window.EventSource = function (url, opts) {
      const es = new ES(url, opts);
      const s = sseStreamOf(String(url), window.location.href);
      if (s) {
        es.addEventListener('open', () => { sse[s]++ });
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
  const timingOf = ${timingOf.toString()};
  if (window.PerformanceObserver) {
    try {
      new window.PerformanceObserver((list) => {
        for (const e of list.getEntries()) {
          const c = callOf(e.name, window.location.href, e.duration, e.responseStatus);
          if (c) calls.push(Object.assign(c, timingOf(e, window.performance && window.performance.timeOrigin)));
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
// The live-update stream a request opens: 'refreshes' (snowplow's
// per-widget stream) or 'notifications' (sse-proxy's firehose), by the URL's
// LAST path segment, wherever the portal mounts it (localhost:8083/
// notifications, or /events/notifications behind a path-mode gateway);
// anything else (a /call named "notifications-…" among it): null.
// Self-contained, shipped into the page as source text.
function sseStreamOf (url, base) {
  let u
  try { u = new URL(url, base) } catch (e) { return null }
  const m = /\/(refreshes|notifications)\/?$/.exec(u.pathname)
  return m ? m[1] : null
}

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

// #1089: how the browser fetched one /call, from the same Resource Timing
// entry: the protocol it negotiated (nextHopProtocol: 'http/1.1', 'h2', 'h3';
// '' when not exposed), its start on the wall clock (epoch ms: timeOrigin +
// startTime, the clock a load's t0 is on), and fetchStart, connectStart,
// connectEnd, requestStart, responseStart, responseEnd RELATIVE TO ITS
// startTime. A field the browser does not expose reads 0 (a cross-origin
// entry without Timing-Allow-Origin): null here, never a made-up offset. On a
// reused connection connectStart === connectEnd, so requestStart − fetchStart
// is the time the request waited for that connection. Never the URL.
// Self-contained, shipped into the page as source text.
function timingOf (e, origin) {
  const s = e && typeof e.startTime === 'number' ? e.startTime : null
  const rel = (v) => (s !== null && typeof v === 'number' && v > 0 ? Math.round((v - s) * 10) / 10 : null)
  return {
    protocol: e && typeof e.nextHopProtocol === 'string' ? e.nextHopProtocol : '',
    start: s !== null && typeof origin === 'number' ? origin + s : null,
    fetch: rel(e && e.fetchStart),
    connectStart: rel(e && e.connectStart),
    connectEnd: rel(e && e.connectEnd),
    requestStart: rel(e && e.requestStart),
    responseStart: rel(e && e.responseStart),
    responseEnd: rel(e && e.responseEnd),
  }
}

// A load's start on the wall clock (epoch ms): the page's own (loadOf's t0),
// or for a load that never settled the driver's clock just before the action
// (#1043 review B1). recordOps tags every point of the load with it; the
// load's calls carry it in their timing metadata.
export function loadStart (r, actionAt) {
  return r === null ? actionAt : r.t0
}

// #1089: one call's timing as k6 sample METADATA (callOps' fourth element;
// browser.js sets it on the VU for that one add): per-call numbers must never
// be tags (each distinct tag set is a time series k6 keeps for the whole run).
// One key, call_timing, a JSON string: the Resource Timing offsets (ms from
// the call's startTime); for a load's own calls also from_load_ms (the call's
// start minus its load's start: the waterfall) and load_id (which load: the
// same id browser.js puts on the load's own page-load points; Chrome's request
// samples are placed in a load by time instead, see browser.js nextLoadId).
// null: no timing recorded (a browser.js before #1089).
export function callTiming (c, loadT0, loadId) {
  if (!c || !('fetch' in c)) return null
  const m = { fetch: c.fetch, connect_start: c.connectStart, connect_end: c.connectEnd,
              request_start: c.requestStart, response_start: c.responseStart, response_end: c.responseEnd }
  if (Number.isFinite(loadT0) && typeof c.start === 'number' && loadId) {
    m.from_load_ms = Math.round((c.start - loadT0) * 10) / 10
    m.load_id = String(loadId)
  }
  return JSON.stringify(m)
}

// The metric points the calls drained from one page write: browser_call_ms,
// tagged with the page's own tags (session, route, visit, during) and the
// call's resource, name, namespace, status and (#1089) negotiated protocol;
// the fourth element is its call_timing metadata (callTiming). `loadT0` /
// `loadId`: the load these calls belong to (during=load), else undefined.
export function callOps (calls, tags, loadT0, loadId) {
  return (calls || []).map((c) => ['browser_call_ms', c.ms, Object.assign({}, tags, {
    call_resource: c.resource, call_name: c.name, call_namespace: c.namespace, call_status: String(c.status),
    call_protocol: typeof c.protocol === 'string' ? c.protocol : '',
  }), callTiming(c, loadT0, loadId)])
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

// #935: when a session switches to its next identity class. A class's
// purpose is one FIRST visit of every route, and the run's warm bars still
// need WARM visits: so a class stays until every route has been visited twice
// (once first, once warm) — two route cycles, 2 x len(routes) x thinkS. The
// session's last class stays to the end.
export function switchDue (visits, routes, last) {
  if (last) return false
  return routes.every((r) => (visits[r] || 0) >= 2)
}

// #935: the browser_fresh_class point's tags — snowplow's learned-class
// counters before the fresh sign-in and after its first page ('' when a read
// failed), and the clock before it (epoch seconds), the window evaluate
// proves the class cold in.
export function freshClassTags (before, after, tBefore) {
  const v = (x, k) => (x && typeof x[k] === 'number' ? String(x[k]) : '')
  return {
    registered_before: v(before, 'registered'), registered_after: v(after, 'registered'),
    seeded_before: v(before, 'seeded'), seeded_after: v(after, 'seeded'), t_before: String(Math.floor(tBefore)),
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
  // t0: the load's own start on the wall clock (epoch ms), from the same clock its time is
  // measured on: where its interval sits against the CPU windows (#1042, #1043 review B1)
  return { ms: end - b.navT0, saw: b.saw, path: here, states, t0: performance.timeOrigin + b.navT0 }
}

// The metric points one load writes. A page that did not settle within its
// timeout is a TIMEOUT and still a load, at its timeout value, so it counts in
// the p95 as well as in the timeout check (#831 gate). skeleton=no: painted
// from the app's own React Query cache (App.tsx:25-30), no request.
// Every point of a load carries its start, t0 (epoch ms, integer): the page's
// own (loadOf), or for a load that never settled, `actionAt`, the driver's
// clock just before the action (the same system clock) (#1043 review B1).
export function recordOps (r, tags, timeoutMs, actionAt) {
  const t0 = loadStart(r, actionAt)
  const at = Number.isFinite(t0) ? { t0: String(Math.round(t0)) } : {}
  if (r === null) {
    const t = Object.assign({}, tags, at, { skeleton: 'timeout', timeout: 'yes' })
    return [['browser_page_timeouts', 1, Object.assign({}, tags, at)], ['browser_page_load_ms', timeoutMs, t],
      ['browser_page_loads', 1, t]]
  }
  const t = Object.assign({}, tags, at, { skeleton: r.saw ? 'yes' : 'no', timeout: 'no' })
  const ops = [['browser_page_load_ms', r.ms, t], ['browser_page_loads', 1, t]]
  for (const k of Object.keys(r.states || {})) {
    if (r.states[k]) ops.push(['browser_widget_states', r.states[k], Object.assign({}, t, { state: k })])
  }
  return ops
}

// Run 26d (frontend 1.8.40: 0 of 5 sessions signed in): the login form contract, frontend#659
// (ui/src/pages/Login/LoginForm/LoginForm.tsx loginFormNameOf; pinned by
// ui/src/pages/Login/AuthMethods/AuthMethods.ids.test.tsx). Basic login is the form named
// `login_basic`, whose field ids antd prefixes with that name, submitted by that form's own
// submit control (antd htmlType='submit', exactly one per form). Never a label, a button's text or
// autocomplete: with two strategies on the page the labels repeat, and the button's text is not
// part of the contract (1.8.36-1.8.39 render "Sign In", 1.8.40 "Sign in").
// bench/login_contract.py SELECTORS holds the same strings and refuses a frontend older than the
// first release carrying them (1.8.36).
export const LOGIN = {
  form: 'form#login_basic',
  username: '#login_basic_username',
  password: '#login_basic_password',
  submit: 'button[type="submit"]',      // inside `form`
}

// The time after which a session's first sign-in can no longer land, from the script's own
// bounds (browser.js uses each of them on that path): the login page's navigation (gotoLogin:
// `tries` attempts of pageTimeoutMs each, `retryMs` apart), the two fills and the submit click
// (pageTimeoutMs each), the arrival (settled: pageTimeoutMs + signinTimeoutMs) and its record
// (readMs). browser.js's sign-in gate: no session signed in by then, and none can.
export function signinBoundMs (cfg, tries, retryMs, readMs) {
  const page = cfg.pageTimeoutMs
  const signin = cfg.signinTimeoutMs || 60000
  return tries * page + (tries - 1) * retryMs + 3 * page + page + signin + readMs
}

export { callOf, loadOf, settledNow, sseStreamOf, timingOf }
