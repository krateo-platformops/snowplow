// pageload.js — the judged instrument of browser.js (#830/#831), as pure
// functions with no k6 import, so node runs them (tests/test_k6_830.py) and
// browser.js ships `settledNow` / `loadOf` into the page as source text.
//
// "Page loaded" = the page is FINAL (owner's decision, 2026-11-26): from the click until the page's OWN data
// requests have answered AND the page has stopped changing, for QUIET_MS. One definition for every load, first
// and warm alike. A load ends at the LAST of: its last `[data-widget-renderer]` skeleton gone, its last own
// request answered, its last DOM change; and it is settled once QUIET_MS have passed since that end with no own
// request still pending. A revisit is painted from the portal's React Query cache with no skeleton, yet it still
// refreshes its data with /call requests: before this, `saw` false made every warm load 0 ms by construction
// (run 31: 2,767 of 2,767), so the warm navigation bar was not measured at all. Its `ended` field says what
// ended the load: 'skeleton', 'request', 'mutation', or 'start' (nothing changed after the click).
// Long-lived streams (/refreshes, /notifications: sseStreamOf) never count as own requests.
//
// OWN requests are the page's data requests to snowplow as the URL shows them: a path ending in /call (callOf).
// Anything else (the OTel exporter's batch POST, config, assets) is not named and not counted.
// LIVE UPDATES DO NOT EXTEND A LOAD (owner's ruling, 2026-11-28; they count as freshness, measured apart). A
// /call is a live-update re-fetch, not the load's own, when its response's X-Snowplow-Refresh-Key equals the key
// of a `refresh` frame the page's /refreshes stream carried since the click, the frame arriving before the request
// started (isLiveRefetch). That is snowplow's own link: refreshes.go writes `event: refresh\ndata: <l1Key>` and
// the dispatchers stamp the same l1Key on the /call answer (setRefreshKeyHeader); the portal's refreshSse.ts
// dispatchRefresh(data) re-fetches the widgets whose last answer carried that key. No time window is involved.
// A DOM change counts toward the load unless the latest answered data request was a live re-fetch and no own
// request is pending (countsAsChange): the render a live answer causes is the live update's.
//
// The skeleton part: no `[data-widget-renderer]` element left.
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
                               armLogin: false, submitAt: 0, signinMs: null, armClick: false,
                               reqEnd: 0, lastMut: 0, open: [], lastAnswer: null, liveEv: [],
                               liveFrames: 0, liveRefetches: 0, liveFirst: 0, callClass: [],
                               armedAt: {}, loadKeys: {} };
  const sel = '[data-widget-renderer]';
  const sseStreamOf = ${sseStreamOf.toString()};
  const callOf = ${callOf.toString()};
  const timingOf = ${timingOf.toString()};
  const refreshKeysOf = ${refreshKeysOf.toString()};
  const isLiveRefetch = ${isLiveRefetch.toString()};
  const countsAsChange = ${countsAsChange.toString()};
  // a new load starts: what the last one saw of live updates and answers is not this one's
  const restart = (now) => {
    b.navT0 = now; b.saw = false; b.goneAt = 0; b.lastAnswer = null;
    b.liveFrames = 0; b.liveRefetches = 0; b.liveFirst = 0; b.liveEv = []; b.loadKeys = {};
  };
  const tick = (records) => {
    const n = document.querySelectorAll(sel).length;
    const now = performance.now();
    // the app's tree: a change under <body> (not the <head> a style library writes into)
    if ((!records || !document.body || records.some((r) => document.body.contains(r.target))) &&
        countsAsChange(b.open.filter((o) => o.t >= b.navT0).length, b.lastAnswer)) b.lastMut = now;
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
    restart(now);
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
    restart(performance.now());
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
  const f = window.fetch;
  // a refresh frame arrived: the key it names (isLiveRefetch matches it to a /call's answer by the key alone)
  const liveFrame = (key) => {
    const now = performance.now();
    if (now >= b.navT0) { b.liveFrames++; b.liveEv.push({ key, at: now }); }
  };
  // read a cloned response to its end; chunk() sees each decoded piece. Never throws, never blocks the app's own read.
  const drain = (clone, chunk) => {
    try {
      const reader = clone.body.getReader();
      const dec = new TextDecoder();
      const step = () => reader.read().then((x) => {
        if (x.done) return;
        chunk(x.value === undefined ? '' : dec.decode(x.value, { stream: true }));
        return step();
      });
      return step().catch(() => {});
    } catch (e) { return Promise.resolve(); }
  };
  window.fetch = function (input, init) {
    const url = String((input && input.url) || input);
    const s = sseStreamOf(url, window.location.href);
    const p = f.apply(this, arguments);
    if (s) {
      p.then((r) => {
        const type = (r.headers && r.headers.get && r.headers.get('content-type')) || '';
        if (r.ok && type.toLowerCase().indexOf('text/event-stream') === 0) {
          sse[s]++
          // the refresh frames' keys: what a live update is, to tell its re-fetches from the load's own requests
          if (s === 'refreshes' && r.clone) {
            let buf = '';
            drain(r.clone(), (text) => {
              const x = refreshKeysOf(buf + text);
              buf = x.rest;
              for (const k of x.keys) liveFrame(k);
            });
          }
        }
      }, () => {});
    } else if (callOf(url, window.location.href, 0, 0) !== null) {
      // an own request candidate: a /call. Pending until its body has been read to the end (or it failed).
      const tok = { t: performance.now() };
      b.open.push(tok);
      const done = () => {
        const i = b.open.indexOf(tok);
        if (i >= 0) b.open.splice(i, 1);
        if (tok.t < b.navT0) return;
        if (tok.live) { b.liveRefetches++; b.lastAnswer = 'live'; }
        else { b.reqEnd = Math.max(b.reqEnd, performance.now()); b.lastAnswer = 'own'; }
      };
      p.then((r) => {
        // at the answer's headers: is it a live update's re-fetch? (its Resource Timing entry is flagged the same)
        const key = (r.headers && r.headers.get && r.headers.get('X-Snowplow-Refresh-Key')) || '';
        tok.live = tok.t >= b.navT0 && isLiveRefetch(key, tok.t, b.liveEv, b.navT0, b.armedAt);
        // a live-read request that is the load's FIRST for its key may be the load's own request that followed an
        // armed key's frame (the one residual of the key rule): counted, so the report can bound it
        if (tok.live && key && !b.loadKeys[key]) b.liveFirst++;
        if (tok.t >= b.navT0 && key) b.loadKeys[key] = true;
        // from now on this key is armed (the portal arms a widget for refreshes from an answer carrying its key)
        if (key && b.armedAt[key] === undefined) b.armedAt[key] = performance.now();
        try { b.callClass.push({ url: new URL(url, window.location.href).href, t: tok.t, live: tok.live }); } catch (e) { /* no URL */ }
        if (r.clone) drain(r.clone(), () => {}).then(done); else done();
      }, done);
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
        if (s === 'refreshes') es.addEventListener('refresh', (e) => { if (e && e.data) liveFrame(String(e.data)) });
      }
      return es;
    };
    window.EventSource.prototype = ES.prototype;
    ['CONNECTING', 'OPEN', 'CLOSED'].forEach((k) => { window.EventSource[k] = ES[k] });
  }
  // the entry of a /call IS the request the fetch wrapper classified: the same URL, the nearest start (a pairing of
  // one request's two records, not an attribution). A call it never classified is not a live re-fetch.
  const takeLive = (name, startTime) => {
    let best = -1;
    b.callClass.forEach((c, i) => {
      if (c.url === name && (best < 0 || Math.abs(c.t - startTime) < Math.abs(b.callClass[best].t - startTime))) best = i;
    });
    return best < 0 ? false : b.callClass.splice(best, 1)[0].live;
  };
  // #919: every /call the page fetches, from its own Resource Timing entry
  // (callOf); browser.js drains window.__benchCalls into browser_call_ms
  const calls = window.__benchCalls = [];
  if (window.PerformanceObserver) {
    try {
      new window.PerformanceObserver((list) => {
        for (const e of list.getEntries()) {
          const c = callOf(e.name, window.location.href, e.duration, e.responseStatus);
          if (c) calls.push(Object.assign(c, timingOf(e, window.performance && window.performance.timeOrigin),
            { live: takeLive(e.name, e.startTime) }));
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

// The l1Keys of the `refresh` frames complete in a stream buffer, and the rest (an unfinished frame). snowplow's
// frame (handlers/refreshes.go): `event: refresh\ndata: <l1Key>\n\n`; `: keepalive` comments and `resync`
// frames carry no key. Self-contained, shipped into the page as source text.
function refreshKeysOf (buffer) {
  const keys = []
  let rest = buffer
  let at = rest.indexOf('\n\n')
  while (at !== -1) {
    let event = ''
    let data = ''
    for (const line of rest.slice(0, at).split('\n')) {
      if (line.indexOf('event:') === 0) event = line.slice(6).trim()
      else if (line.indexOf('data:') === 0) data = line.slice(5).replace(/^ /, '')
    }
    if (event === 'refresh' && data) keys.push(data)
    rest = rest.slice(at + 2)
    at = rest.indexOf('\n\n')
  }
  return { keys, rest }
}

// Is a /call that started at `startedAt` and whose answer carried `key` (X-Snowplow-Refresh-Key) the re-fetch of
// a live update? Yes when a refresh frame naming that key reached the page since the load's click (`navT0`) and
// before the request started, AND an earlier answer in this document had already carried that key (`armedAt`: key
// to the moment of its first answer): the portal arms a widget for refreshes only from an answer carrying its key
// (refreshSse.ts:381), so a key no answer has carried yet cannot be frame-triggered (a first load is exact). No key
// (not cache-keyed): never. `events`: [{key, at}] of this load. Self-contained.
function isLiveRefetch (key, startedAt, events, navT0, armedAt) {
  if (!key || armedAt[key] === undefined) return false
  return events.some((e) => e.key === key && e.at >= navT0 && e.at <= startedAt && e.at > armedAt[key])
}

// Does a DOM change count toward the load? Yes while an own request is pending, and unless the latest answered data
// request was a live re-fetch (the render it causes is the live update's). `pending`: own requests in flight.
// Self-contained.
function countsAsChange (pending, lastAnswer) {
  return pending > 0 || lastAnswer !== 'live'
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
  if (c.live) m.live = true            // the re-fetch of a live update (isLiveRefetch): not the load's own
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
  if ((b.open || []).some((o) => o.t >= b.navT0)) return false
  const end = Math.max(b.navT0, b.saw ? b.goneAt : 0, b.reqEnd || 0, b.lastMut || 0)
  return now - end >= arg.quiet
}

// The load just settled: its time, where it landed, and the widgets' end
// states (data-testid widget-error / -timeout / -forbidden / -notfound,
// WidgetStates.tsx:34,54,75,89). Self-contained, like settledNow.
function loadOf (b, here, testids) {
  const skel = b.saw ? b.goneAt : 0
  const end = Math.max(b.navT0, skel, b.reqEnd || 0, b.lastMut || 0)
  // what ended the load: the latest of the three; a tie goes to the skeleton (its removal is itself a change),
  // then the request
  const ended = b.saw && skel === end ? 'skeleton'
    : (b.reqEnd || 0) === end && end > b.navT0 ? 'request'
    : (b.lastMut || 0) === end && end > b.navT0 ? 'mutation' : 'start'
  const states = { error: 0, timeout: 0, forbidden: 0, notfound: 0 }
  for (const id of testids) {
    const k = String(id).replace(/^widget-/, '')
    if (k in states) states[k]++
  }
  // t0: the load's own start on the wall clock (epoch ms), from the same clock its time is
  // measured on: where its interval sits against the CPU windows (#1042, #1043 review B1)
  // the three ends, ms from the click (null: none after it) and the live updates that reached the load
  const off = (v) => (v > b.navT0 ? v - b.navT0 : null)
  const finalOf = { skeletonMs: b.saw ? skel - b.navT0 : null, requestMs: off(b.reqEnd || 0), mutationMs: off(b.lastMut || 0),
                    liveRefetches: b.liveRefetches || 0, liveFrames: b.liveFrames || 0,
                    liveFirst: b.liveFirst || 0 }
  return { ms: end - b.navT0, saw: b.saw, ended, final: finalOf, path: here, states, t0: performance.timeOrigin + b.navT0 }
}

// The metric points one load writes. A page that did not settle within its
// timeout is a TIMEOUT and still a load, at its timeout value, so it counts in
// the p95 as well as in the timeout check (#831 gate). skeleton=no: painted
// from the app's own React Query cache (App.tsx:25-30), no skeleton; its time is
// still when its own requests answered and the page stopped changing (`ended_by`).
// Every point of a load carries its start, t0 (epoch ms, integer): the page's
// own (loadOf), or for a load that never settled, `actionAt`, the driver's
// clock just before the action (the same system clock) (#1043 review B1).
// #1508: `watch` = { ms, cause } (browser.js settled): the load was watched past its bound `timeoutMs`, until
// it settled or the drive ended, so a load past the bound is no longer censored there. `ms` is how long it
// took from the action to the moment it settled (or to the end of the watch), on the clock the bound is
// judged on. Past the bound (watch.ms > timeoutMs, or no load at all): a timeout, tagged with its bound and
// `completed` yes (its value is its real time, it loaded: its widget states count) or no (its value is how
// long it was watched; `cause` says why the watch ended). Without `watch` (a caller before #1508): r null is
// a timeout censored at the bound, as before.
export function recordOps (r, tags, timeoutMs, actionAt, watch) {
  const t0 = loadStart(r, actionAt)
  const at = Number.isFinite(t0) ? { t0: String(Math.round(t0)) } : {}
  const past = watch ? watch.ms > timeoutMs : r === null
  if (r === null) {
    const t = Object.assign({}, tags, at, { skeleton: 'timeout', timeout: 'yes' },
      watch ? { bound: String(timeoutMs), completed: 'no', cause: String(watch.cause || 'not said') } : {})
    return [['browser_page_timeouts', 1, Object.assign({}, tags, at, watch ? { bound: String(timeoutMs) } : {})],
      ['browser_page_load_ms', watch ? Math.round(watch.ms) : timeoutMs, t], ['browser_page_loads', 1, t]]
  }
  if (past) {
    const t = Object.assign({}, tags, at, { skeleton: r.saw ? 'yes' : 'no', timeout: 'yes', bound: String(timeoutMs),
      completed: 'yes' })
    const ops = [['browser_page_timeouts', 1, Object.assign({}, tags, at, { bound: String(timeoutMs) })],
      ['browser_page_load_ms', Math.round(watch.ms), t], ['browser_page_loads', 1, t]]
    for (const k of Object.keys(r.states || {})) {
      if (r.states[k]) ops.push(['browser_widget_states', r.states[k], Object.assign({}, t, { state: k })])
    }
    return ops
  }
  const t = Object.assign({}, tags, at, { skeleton: r.saw ? 'yes' : 'no', timeout: 'no' },
    r.ended ? { ended_by: r.ended } : {},
    r.final ? { live_update: r.final.liveRefetches > 0 ? 'yes' : 'no' } : {})
  const ops = [['browser_page_load_ms', r.ms, t], ['browser_page_loads', 1, t]]
  // the first-load series the OLD way (its skeleton's end; 0 for a load that never showed one), so a run stays
  // comparable with run 31 and earlier, which ended every load there
  if (tags && tags.visit === 'first' && r.final) {
    ops.push(['browser_first_load_skeleton_ms', r.saw && r.final.skeletonMs !== null ? r.final.skeletonMs : 0, t])
  }
  for (const k of Object.keys(r.states || {})) {
    if (r.states[k]) ops.push(['browser_widget_states', r.states[k], Object.assign({}, t, { state: k })])
  }
  return ops
}

// A settled load's own ends and the live updates that reached it, as the JSON string browser.js sets on the load's
// page-load point as metadata `load_final` (never tags: per-load numbers). Null for a load that never settled.
export function loadFinalOf (r) {
  if (!r || !r.final) return null
  return JSON.stringify({ skeleton_ms: r.final.skeletonMs, request_ms: r.final.requestMs,
                          mutation_ms: r.final.mutationMs, live_refetches: r.final.liveRefetches,
                          live_frames: r.final.liveFrames, live_first: r.final.liveFirst || 0 })
}

// #1508: why the watch of a load ended without it settling: one line, at most 200 characters, never an
// identity (`secrets`: the session's users and the password, each replaced). A waitForFunction that ran
// out its timeout ran to the drive's end; anything else ended the watch early (the page's context went).
export function watchCause (e, watchedMs, secrets) {
  const line = redactAll(String(e).split('\n')[0], secrets)
  if (/timed out/i.test(line)) return `not settled by the drive's end (watched ${Math.round(watchedMs)} ms)`
  return `the watch ended: ${line}`.slice(0, 200)
}

export function redactAll (text, secrets) {
  let t = String(text)
  for (const x of secrets || []) {
    if (x) t = t.split(x).join('<redacted>')
  }
  return t
}

// #1508: the browser_session_end point's tags, one per session when its iteration ends: `early` yes when it
// ended before the drive's end (an error, or no identity class left to drive), with its `cause` (one line, at
// most 200 characters, never an identity).
export function sessionEndTags (session, k, early, cause, secrets) {
  return { session, class: String(k), early: early ? 'yes' : 'no',
    cause: redactAll(String(cause).split('\n')[0], secrets).slice(0, 200) }
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

export { callOf, countsAsChange, isLiveRefetch, loadOf, refreshKeysOf, settledNow, sseStreamOf, timingOf }
