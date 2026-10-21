// browser.js — run 11's TRUE page load (#830): real Chrome sessions through
// the real frontend while navigate.js runs the 1,000-user background load.
//
// Each VU is one person in one browser context:
//   - signs in through the UI: /login, the antd form `basic` (username,
//     password: pages/Login/LoginForm/LoginForm.tsx:20-34), submitted; the SPA
//     sends GET {AUTHN_API_BASE_URL}{path} with Basic auth (pages/Login/
//     Login.tsx:78-86), stores the payload in localStorage K_user (:90) and
//     navigates (:92) — `/` redirects to the first menu route (widgets/Menu/
//     Menu.tsx:45-50). That arrival is the session's FIRST LOAD.
//   - then clicks sidebar routes (Menu.tsx:96 onClick → navigate) every
//     THINK_S, recording each load as first visit or warm for this session.
//
// "Page loaded": the content has no `[data-widget-renderer]` element left for
// QUIET_MS. WidgetLoading — the skeleton every pending widget shows
// (components/WidgetRenderer/WidgetRenderer.tsx:131-134, and the Suspense
// fallback :69) — is the ONLY element carrying that attribute
// (components/WidgetStates/WidgetStates.tsx:7-10), so "none left" means every
// widget in the tree has its data or a final state. The load ENDS when the last
// skeleton disappeared (not at the end of the quiet window). QUIET_MS (1000)
// exceeds useProgressiveLoading's 500 ms batch reveal (hooks/
// useProgressiveLoading.ts:13-33), so a list still revealing is not "loaded".
// Widget end states (data-testid widget-error / -timeout / -forbidden /
// -notfound, WidgetStates.tsx:34,54,75,89) are counted per load.
//
// Live updates, the user's way (#830 re-gate): the SPA keeps two SSE streams,
// each counted per session when it OPENS (an answered request): snowplow's
// per-widget `/refreshes` (fetch POST, hooks/refreshSse.ts:572-578) and the
// sse-proxy's `/notifications` firehose (EventSource, hooks/sseClient.ts:49,
// subscribed at hooks/useLiveRefresh.ts:18-21; localhost:8083 is forwarded to
// it). `browser_sse_opened{session, stream}`; the evaluator wants >= 1 of each.
import { browser } from 'k6/browser'
import exec from 'k6/execution'
import { Counter, Trend } from 'k6/metrics'

const CFG = JSON.parse(open(__ENV.BROWSER_CONFIG || '/opt/k6/browser-config.json'))

const LOAD = new Trend('browser_page_load_ms', true)
const LOADS = new Counter('browser_page_loads')
const WIDGET_ERRORS = new Counter('browser_widget_errors')
const TIMEOUTS = new Counter('browser_page_timeouts')
const LOGINS = new Counter('browser_logins')
const FRAME = new Counter('bench_frame')
const SSE = new Counter('browser_sse_opened')

export const options = {
  scenarios: {
    ui: {
      executor: 'per-vu-iterations',
      vus: CFG.sessions.length,
      iterations: 1,
      maxDuration: `${CFG.durationS + 600}s`,
      options: { browser: { type: 'chromium' } },
    },
  },
  thresholds: {
    browser_page_loads: ['count>=0'],
    browser_logins: ['count>=0'],
    browser_sse_opened: ['count>=0'],
    browser_page_timeouts: ['count>=0'],
    browser_widget_errors: ['count>=0'],
    bench_frame: ['count>=0'],
  },
  summaryTrendStats: ['count', 'min', 'med', 'p(95)', 'max'],
}

// Installed in every document before the app runs: tracks the skeletons.
const OBSERVER = `(() => {
  const b = window.__bench = { count: 0, goneAt: 0, navT0: 0, saw: false };
  const tick = () => {
    const n = document.querySelectorAll('[data-widget-renderer]').length;
    const now = performance.now();
    if (n > 0) b.saw = true;
    if (n === 0 && b.count > 0) b.goneAt = now;
    b.count = n;
  };
  new MutationObserver(tick).observe(document, { childList: true, subtree: true });
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
})()`

async function markStart (page) {
  await page.evaluate(() => {
    const b = window.__bench
    b.navT0 = performance.now(); b.saw = false; b.goneAt = 0
  })
}

// Wait until the route is loaded; returns {ms, path, errors} or null on
// timeout. path null = wherever the app lands after sign-in (the user's OWN
// first menu route, which need not be the run's route table's first).
async function settled (page, path) {
  try {
    await page.waitForFunction((arg) => {
      const b = window.__bench
      const here = window.location.pathname
      if (!b || (arg.path ? here !== arg.path : (here === '/login' || here === '/'))) return false
      if (b.count > 0) return false
      const end = b.saw ? b.goneAt : b.navT0
      return performance.now() - end >= arg.quiet
    }, { polling: 50, timeout: CFG.pageTimeoutMs }, { path, quiet: CFG.quietMs })
  } catch (e) {
    return null
  }
  return await page.evaluate(() => {
    const b = window.__bench
    const end = b.saw ? b.goneAt : b.navT0
    return { ms: end - b.navT0, saw: b.saw, path: window.location.pathname,
             errors: document.querySelectorAll('[data-testid^="widget-"]').length }
  })
}

// skeleton=no: the route painted with no WidgetLoading at all — every widget
// came from the app's own React Query cache (App.tsx:25-30, staleTime 30 s),
// no request; the load is then ~0 ms. Tagged so a report can tell it apart.
function record (r, tags) {
  if (r === null) { TIMEOUTS.add(1, tags); return }
  const t = Object.assign({ skeleton: r.saw ? 'yes' : 'no' }, tags)
  LOAD.add(r.ms, t)
  LOADS.add(1, t)
  if (r.errors) WIDGET_ERRORS.add(r.errors, t)
}

export default async function () {
  const s = CFG.sessions[exec.vu.idInTest - 1]
  const context = await browser.newContext()
  await context.addInitScript(OBSERVER)
  const page = await context.newPage()
  const deadline = Date.now() + CFG.durationS * 1000
  const visited = new Set()
  try {
    await page.goto(`${CFG.frontendUrl}/login`, { waitUntil: 'load', timeout: CFG.pageTimeoutMs })
    await page.locator('#basic_username').fill(s.user)
    await page.locator('#basic_password').fill(CFG.password)
    await markStart(page)
    await page.locator('#basic_password').press('Enter')
    const first = await settled(page, null)
    const landed = first === null ? '(none)' : first.path
    if (first !== null) LOGINS.add(1, { session: s.name })
    record(first, { session: s.name, route: landed, visit: 'first', method: 'login' })
    visited.add(landed)
    let n = 0
    while (Date.now() < deadline) {
      await page.waitForTimeout(CFG.thinkS * 1000)
      const route = CFG.routes[(s.offset + (++n)) % CFG.routes.length]
      const item = page.locator(`li.ant-menu-item[data-menu-id$="-${route}"]`)
      let method = 'click'
      await markStart(page)
      if (await item.isVisible()) {
        await item.click()
      } else {
        // a route under a collapsed submenu: the same client-side navigation
        method = 'history'
        await page.evaluate((p) => {
          window.history.pushState({}, '', p)
          window.dispatchEvent(new PopStateEvent('popstate'))
        }, route)
      }
      const r = await settled(page, route)
      record(r, { session: s.name, route, visit: visited.has(route) ? 'warm' : 'first', method })
      visited.add(route)
    }
  } finally {
    try {
      const n = await page.evaluate(() => window.__benchSse || { refreshes: 0, notifications: 0 })
      SSE.add(n.refreshes, { session: s.name, stream: 'refreshes' })
      SSE.add(n.notifications, { session: s.name, stream: 'notifications' })
    } catch (e) { /* the page is gone: nothing counted, the evaluator says so */ }
    await page.close()
    await context.close()
  }
}

export function setup () { FRAME.add(1, { frame: 'start' }) }
export function teardown () { FRAME.add(1, { frame: 'end' }) }

export function handleSummary (data) {
  const keep = {}
  for (const [name, m] of Object.entries(data.metrics)) {
    if (/^(browser_page_load_ms|browser_page_loads|browser_widget_errors|browser_page_timeouts|browser_logins|browser_sse_opened|bench_frame)(\{|$)/.test(name)) keep[name] = m.values
  }
  return { [__ENV.SUMMARY_FILE || '/out/browser-summary.json']: JSON.stringify({
    kind: 'browser_summary', sessions: CFG.sessions.map((x) => x.name), metrics: keep }) }
}
