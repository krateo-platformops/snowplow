// browser.js — run 11's TRUE page load (#830): real Chrome sessions through
// the real frontend while navigate.js runs the 1,000-user background load.
// What "loaded" means, and how a load is recorded, is in pageload.js (pure,
// tested under node); this file drives Chrome.
//
// Each VU is one person in one browser context:
//   - signs in through the UI: /login, the antd form `basic` (username,
//     password: pages/Login/LoginForm/LoginForm.tsx:20-34), submitted; the SPA
//     sends GET {AUTHN_API_BASE_URL}{path} with Basic auth (pages/Login/
//     Login.tsx:78-86), stores the payload in localStorage K_user (:90) and
//     navigates (:92) — `/` redirects to the first menu route (widgets/Menu/
//     Menu.tsx:45-50). The page that lands is the session's FIRST LOAD, timed
//     from the moment the app left /login; the sign-in itself is
//     `browser_signin_ms` (report only).
//   - then clicks sidebar routes (Menu.tsx:96 onClick → navigate) every
//     THINK_S, recording each load as first visit or warm for this session.
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
import { OBSERVER, QUIET_MS, callOps, loadOf, recordOps, requestLedger, settledNow } from './pageload.js'

const CFG = JSON.parse(open(__ENV.BROWSER_CONFIG || '/opt/k6/browser-config.json'))

const METRICS = {
  browser_page_load_ms: new Trend('browser_page_load_ms', true),
  browser_page_loads: new Counter('browser_page_loads'),
  browser_widget_states: new Counter('browser_widget_states'),
  browser_page_timeouts: new Counter('browser_page_timeouts'),
}
const SIGNIN = new Trend('browser_signin_ms', true)
const LOGINS = new Counter('browser_logins')
const FRAME = new Counter('bench_frame')
const SSE = new Counter('browser_sse_opened')
// #919: each /call the page fetched, tagged with its resource, name and namespace
const CALL = new Trend('browser_call_ms', true)
// #919 (N7): each browser request that failed at network level or never
// completed (pageload.requestLedger): ms from its start to its failure or to
// the session's end; it started at the point's time minus its value
const INCOMPLETE = new Trend('browser_request_incomplete_ms', true)

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
    browser_widget_states: ['count>=0'],
    browser_call_ms: ['count>=0'],
    browser_request_incomplete_ms: ['count>=0'],
    bench_frame: ['count>=0'],
  },
  summaryTrendStats: ['count', 'min', 'med', 'p(95)', 'max'],
  // no url/name tags: the SPA puts the signed-in user into /call extras
  // (identity injection), and k6 browser tags every request with its URL
  systemTags: ['status', 'method', 'scenario', 'expected_response', 'error_code'],
}

// Before a route click: the in-page click listener (pageload.OBSERVER) starts
// the clock at the click itself; navT0 here is the fallback.
async function markStart (page) {
  await page.evaluate(() => {
    const b = window.__bench
    b.navT0 = performance.now(); b.saw = false; b.goneAt = 0; b.armClick = true
  })
}

// Wait until the route is loaded (pageload.settledNow, run in the page);
// returns pageload.loadOf's record, or null on timeout.
// The sign-in arrival may wait for authn first: its timeout adds
// signinTimeoutMs (the page itself still gets pageTimeoutMs).
async function settled (page, path) {
  const arg = JSON.stringify({ path, quiet: CFG.quietMs || QUIET_MS })
  const timeout = CFG.pageTimeoutMs + (path ? 0 : (CFG.signinTimeoutMs || 60000))
  try {
    await page.waitForFunction(
      `(${settledNow.toString()})(window.__bench, window.location.pathname, performance.now(), ${arg})`,
      { polling: 50, timeout })
  } catch (e) {
    console.warn(`page did not settle (${path || 'sign-in'}): ${e}`)
    return null
  }
  // page.evaluate takes only a function: run loadOf's source as an
  // expression (waitForFunction), keep its value on window, read it back
  await page.waitForFunction(
    `(window.__benchLoad = (${loadOf.toString()})(window.__bench, window.location.pathname, ` +
    `Array.from(document.querySelectorAll('[data-testid^="widget-"]')).map((e) => e.getAttribute('data-testid'))), true)`,
    { polling: 'raf', timeout: 10000 })
  return await page.evaluate(() => window.__benchLoad)
}

function record (r, tags) {
  for (const [metric, value, t] of recordOps(r, tags, CFG.pageTimeoutMs)) METRICS[metric].add(value, t)
}

// #919: the /calls the page made since the last drain (pageload.callOf), as
// browser_call_ms points with the page's tags: during=load right after a
// load settled (the load's own calls), during=between before the next click
// (live refreshes while the user thinks). A page that is gone: none drained.
async function drainCalls (page, tags) {
  let calls
  try {
    calls = await page.evaluate(() => (window.__benchCalls || []).splice(0))
  } catch (e) {
    return
  }
  for (const [, value, t] of callOps(calls, tags)) CALL.add(value, t)
}

// The first navigation, retried (run 11: a refused localhost port killed 4
// of 5 sessions outright). Bounded: GOTO_TRIES x GOTO_WAIT_MS (config
// loginTries / loginRetryMs; tests set 1 to reproduce run 11).
const GOTO_TRIES = CFG.loginTries || 6
const GOTO_WAIT_MS = CFG.loginRetryMs || 5000
async function gotoLogin (page) {
  for (let i = 1; ; i++) {
    try {
      return await page.goto(`${CFG.frontendUrl}/login`, { waitUntil: 'load', timeout: CFG.pageTimeoutMs })
    } catch (e) {
      if (i >= GOTO_TRIES) throw e
      console.warn(`login page attempt ${i} of ${GOTO_TRIES} failed (${e}); retrying in ${GOTO_WAIT_MS} ms`)
      await page.waitForTimeout(GOTO_WAIT_MS)
    }
  }
}

export default async function () {
  const s = CFG.sessions[exec.vu.idInTest - 1]
  const context = await browser.newContext()
  await context.addInitScript(OBSERVER)
  const page = await context.newPage()
  const deadline = Date.now() + CFG.durationS * 1000
  const visited = new Set()
  let shown = '(none)'     // the route on screen: tags the calls made between loads
  // #919 (N7): every request the page makes, until it finishes or fails
  // (k6 emits nothing for a request that failed at network level)
  const ledger = requestLedger()
  page.on('request', (req) => ledger.started(req.method(), req.url(), Date.now(), req.resourceType()))
  page.on('requestfinished', (req) => ledger.finished(req.method(), req.url()))
  page.on('requestfailed', (req) => {
    const f = req.failure()
    const op = ledger.failed(req.method(), req.url(), Date.now(), f && f.errorText, req.resourceType())
    if (op) INCOMPLETE.add(op[1], Object.assign({ session: s.name, route: shown }, op[2]))
  })
  try {
    await gotoLogin(page)
    await page.locator('#basic_username').fill(s.user)
    await page.locator('#basic_password').fill(CFG.password)
    // the first page's clock starts when the app leaves /login (pageload.js)
    await page.evaluate(() => {
      const b = window.__bench
      b.armLogin = true; b.submitAt = performance.now(); b.signinMs = null
    })
    await page.locator('#basic_password').press('Enter')
    const first = await settled(page, null)
    const landed = first === null ? '(none)' : first.path
    if (first !== null) {
      LOGINS.add(1, { session: s.name })
      const signin = await page.evaluate(() => window.__bench.signinMs)
      if (signin !== null) SIGNIN.add(signin, { session: s.name })
    }
    record(first, { session: s.name, route: landed, visit: 'first', method: 'login' })
    await drainCalls(page, { session: s.name, route: landed, visit: 'first', during: 'load' })
    visited.add(landed)
    shown = landed
    let n = 0
    while (Date.now() < deadline) {
      await page.waitForTimeout(CFG.thinkS * 1000)
      const route = CFG.routes[(s.offset + (++n)) % CFG.routes.length]
      const item = page.locator(`li.ant-menu-item[data-menu-id$="-${route}"]`)
      let method = 'click'
      await drainCalls(page, { session: s.name, route: shown, during: 'between' })
      await markStart(page)
      if (await item.isVisible()) {
        await item.click()
      } else {
        // a route under a collapsed submenu: the same client-side navigation
        method = 'history'
        await page.evaluate((p) => {
          const b = window.__bench
          b.navT0 = performance.now(); b.saw = false; b.goneAt = 0; b.armClick = false
          window.history.pushState({}, '', p)
          window.dispatchEvent(new PopStateEvent('popstate'))
        }, route)
      }
      const r = await settled(page, route)
      const visit = visited.has(route) ? 'warm' : 'first'
      record(r, { session: s.name, route, visit, method })
      await drainCalls(page, { session: s.name, route, visit, during: 'load' })
      visited.add(route)
      shown = route
    }
  } finally {
    await drainCalls(page, { session: s.name, route: shown, during: 'between' })
    for (const [, value, t] of ledger.unfinished(Date.now())) {
      INCOMPLETE.add(value, Object.assign({ session: s.name, route: shown }, t))
    }
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
    if (/^(browser_page_load_ms|browser_page_loads|browser_widget_states|browser_page_timeouts|browser_logins|browser_signin_ms|browser_sse_opened|browser_call_ms|browser_request_incomplete_ms|bench_frame)(\{|$)/.test(name)) keep[name] = m.values
  }
  return { [__ENV.SUMMARY_FILE || '/out/browser-summary.json']: JSON.stringify({
    kind: 'browser_summary', sessions: CFG.sessions.map((x) => x.name),
    firstVisit: CFG.firstVisit, metrics: keep }) }
}
