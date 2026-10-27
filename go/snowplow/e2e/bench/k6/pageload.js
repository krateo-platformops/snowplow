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
