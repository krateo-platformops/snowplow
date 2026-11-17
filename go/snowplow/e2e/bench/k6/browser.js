// browser.js — run 11's TRUE page load (#830): real Chrome sessions through
// the real frontend while navigate.js runs the 1,000-user background load.
// What "loaded" means, and how a load is recorded, is in pageload.js (pure,
// tested under node); this file drives Chrome.
//
// Each VU is one person in one browser context:
//   - signs in through the UI: /login, the antd form `login_basic` (username,
//     password; frontend#659's contract, pageload.LOGIN, frontend >= 1.8.36:
//     bench/login_contract.py refuses an older one), submitted by its own
//     (single) submit control, whatever its text; the SPA
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
// sse-proxy's `/notifications` firehose (fetch + Bearer from frontend 1.8.11,
// hooks/sseClient.ts; an EventSource before; under the portal's origin at
// /events, as on 057). pageload.sseStreamOf tells them apart.
// `browser_sse_opened{session, stream}`; the evaluator wants >= 1 of each.
import { browser } from 'k6/browser'
import encoding from 'k6/encoding'
import exec from 'k6/execution'
import http from 'k6/http'
import { Counter, Trend } from 'k6/metrics'
import {
  LOGIN, OBSERVER, QUIET_MS, callOps, freshClassTags, loadOf, loadStart, recordOps, requestLedger, sessionEndTags,
  settledNow, signinBoundMs, switchDue, watchCause,
} from './pageload.js'

const CFG = JSON.parse(open(__ENV.BROWSER_CONFIG || '/opt/k6/browser-config.json'))

// The first navigation, retried (run 11: a refused localhost port killed 4
// of 5 sessions outright). Bounded: GOTO_TRIES x GOTO_WAIT_MS (config
// loginTries / loginRetryMs; tests set 1 to reproduce run 11).
const GOTO_TRIES = CFG.loginTries || 6
const GOTO_WAIT_MS = CFG.loginRetryMs || 5000
// a settled page's record, read back from the page (settled)
const LOAD_READ_MS = 10000
// Run 26d's sign-in gate: frontend 1.8.40 renamed the login form, every session's sign-in threw,
// and the run measured on with 0 browser sessions. Each session adds 0 to browser_first_signins
// when it starts (a Counter with no sample is not judged while the test runs) and 1 when its
// first sign-in lands; if none has landed by SIGNIN_BOUND_MS, the bound past which none can
// (pageload.signinBoundMs), k6 aborts the whole test (abortOnFail: exit 99, the summary still
// written). run606.sh's measure step sees the browser driver end before the drive and fails by name.
const SIGNIN_BOUND_MS = signinBoundMs(CFG, GOTO_TRIES, GOTO_WAIT_MS, LOAD_READ_MS)
const SIGNED_IN = new Counter('browser_first_signins')

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
// a failed request whose start the ledger never saw: counted, no duration
const UNSEEN = new Counter('browser_request_failed_start_unseen')
// #935: one point per fresh identity class: snowplow's learned-class counters
// around its sign-in (the proof it was cold) and the clock before it
const FRESH = new Counter('browser_fresh_class')
// #1508: one point per session when its iteration ends (pageload.sessionEndTags): whether it ended before the
// drive's end, and why. Run 27c N2 lost 2 of 5 sessions to an uncaught login-form error and the verdict never
// said so: evaluate makes an arm whose session ended early INVALID by environment, naming it
const SESSION_END = new Counter('browser_session_end')
// #1517: how long each class waited for a renderer read (side: before its sign-in / after its last load; read: yes,
// or no when none came within rendererReadWaitMs). Never part of a load: the sign-in clock starts after it
const RENDERER_WAIT = new Trend('browser_renderer_wait_ms', true)
const LEDGER_METRICS = { browser_request_incomplete_ms: INCOMPLETE, browser_request_failed_start_unseen: UNSEEN }

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
    bench_frame: ['count>=0'],
    browser_request_failed_start_unseen: ['count>=0'],
    browser_fresh_class: ['count>=0'],
    browser_session_end: ['count>=0'],
    // the only threshold that can fail: see SIGNIN_BOUND_MS
    browser_first_signins: [{ threshold: 'count>0', abortOnFail: true, delayAbortEval: `${Math.ceil(SIGNIN_BOUND_MS / 1000)}s` }],
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

// Wait until the route is loaded (pageload.settledNow, run in the page).
// The sign-in arrival may wait for authn first: its bound adds
// signinTimeoutMs (the page itself still gets pageTimeoutMs).
// #1508: the load is watched PAST its bound, until it settles or the drive
// ends (`deadline`), so a load past the bound is recorded with how long it
// really took, or as never completed with why the watch ended; never
// censored at the bound. Returns { load: pageload.loadOf's record or null,
// bound, ms: from this call to the moment it settled or the watch ended,
// cause: why it never settled (null when it did) }.
async function settled (page, path, deadline, secrets) {
  const arg = JSON.stringify({ path, quiet: CFG.quietMs || QUIET_MS })
  const bound = CFG.pageTimeoutMs + (path ? 0 : (CFG.signinTimeoutMs || 60000))
  const from = Date.now()
  const watch = Math.max(bound, deadline - from)
  try {
    await page.waitForFunction(
      `(${settledNow.toString()})(window.__bench, window.location.pathname, performance.now(), ${arg})`,
      { polling: 50, timeout: watch })
  } catch (e) {
    const ms = Date.now() - from
    console.warn(`page did not settle (${path || 'sign-in'}) within ${ms} ms (its bound ${bound} ms): ${e}`)
    return { load: null, bound, ms, cause: watchCause(e, ms, secrets) }
  }
  const ms = Date.now() - from
  if (ms > bound) console.warn(`page settled (${path || 'sign-in'}) after ${ms} ms, past its bound of ${bound} ms`)
  // page.evaluate takes only a function: run loadOf's source as an
  // expression (waitForFunction), keep its value on window, read it back
  await page.waitForFunction(
    `(window.__benchLoad = (${loadOf.toString()})(window.__bench, window.location.pathname, ` +
    `Array.from(document.querySelectorAll('[data-testid^="widget-"]')).map((e) => e.getAttribute('data-testid'))), true)`,
    { polling: 'raf', timeout: LOAD_READ_MS })
  return { load: await page.evaluate(() => window.__benchLoad), bound, ms, cause: null }
}

// `res`: settled()'s answer. `actionAt`: the driver's clock (epoch ms) just before the action, the start of a
// load that never settled (a settled load carries the page's own start)
// #1089: the load's id rides on its own points' metadata (load_id), set for
// these synchronous adds only
function record (res, tags, actionAt, loadId) {
  const md = exec.vu.metrics.metadata
  md.load_id = loadId
  try {
    const watch = { ms: res.ms, cause: res.cause }
    for (const [metric, value, t] of recordOps(res.load, tags, res.bound, actionAt, watch)) METRICS[metric].add(value, t)
  } finally {
    delete md.load_id
  }
}

// #919: the /calls the page made since the last drain (pageload.callOf), as
// browser_call_ms points with the page's tags: during=load right after a
// load settled (the load's own calls), during=between before the next click
// (live refreshes while the user thinks). A page that is gone: none drained.
// #1089: each call's Resource Timing (pageload.callTiming) rides as the
// sample's call_timing METADATA, set on the VU for that one synchronous add
// and removed at once (per-call numbers as tags would each be a time series);
// `loadT0` / `loadId`: the load these calls belong to (pageload.loadStart;
// nextLoadId), for a load's own calls only.
async function drainCalls (page, tags, loadT0, loadId) {
  let calls
  try {
    calls = await page.evaluate(() => (window.__benchCalls || []).splice(0))
  } catch (e) {
    return
  }
  const md = exec.vu.metrics.metadata
  for (const [, value, t, timing] of callOps(calls, tags, loadT0, loadId)) {
    if (timing === null) {
      CALL.add(value, t)
      continue
    }
    md.call_timing = timing
    try {
      CALL.add(value, t)
    } finally {
      delete md.call_timing
    }
  }
}

// #1089: the join of Chrome's own request samples (browser_http_req_duration)
// with a load's /calls, never by URL. k6 v2.3.0 stamps a browser request
// sample with the VU's tags and metadata when its RESPONSE is emitted
// (network_manager.go emitResponseMetrics: state.Tags.GetCurrentValues();
// capturing them when Chrome SENDS the request needs the experimental
// AsyncMetricContext feature flag, which the bench does not turn on). A
// per-load id on the VU would therefore be the load open when the response
// came: a late fetch of the previous load booked to this one. So only the VU
// is stamped, for the whole session (metadata `vu`, never changing), and
// bench/calltiming.py places each Chrome request in a load of that VU by its
// own start (the sample's time minus its duration) inside the load's window
// (its t0 and wall time). A load's id (`<vu>:<n>`, unique in the test) rides
// on its own page-load points (record) and its calls' call_timing.
let LOADS = 0
function nextLoadId () { return `${exec.vu.idInTest}:${++LOADS}` }

// #1517 review: ask renderers.js for a read of this page and wait (at most CFG.rendererReadWaitMs) until a read that
// STARTED after `afterMs` (epoch ms) named its renderer (it stamps window.__benchMapped with its start). Asked only
// where the renderer can have changed, and never inside a load: before a class's sign-in, after its login load
// (the class's one cross-document navigation: a same-document one keeps its document, and so its renderer), and
// after its last load, so every load has a read on both sides with no read in Chrome between (the watcher's cost
// is inside the browser's cgroup). The ask is a call of window.__benchRead, the binding renderers.js gives the page
// (Runtime.bindingCalled reaches only the watcher: the app sees no DOM, title or event change; the page's title is
// the portal's own, set from its routes, and would race an ask carried there). Asked once, from the page, as soon
// as the binding is there; nothing is polled by the watcher. A read that never comes (no watcher, a page gone)
// leaves its loads with no known renderer: the evaluator says UNKNOWN
let READS = 0
async function awaitRead (page, afterMs, tags) {
  if (!CFG.rendererWatch) return
  const t0 = Date.now()
  const ask = `bench-read:${tags.side}:${exec.vu.idInTest}:${++READS}`
  let read = 'yes'
  try {
    await page.waitForFunction(`(() => {
      if (typeof window.__benchRead !== 'function') return false
      if (window.__benchAsked !== ${JSON.stringify(ask)}) { window.__benchAsked = ${JSON.stringify(ask)}; window.__benchRead(${JSON.stringify(ask)}) }
      return (window.__benchMapped || 0) > ${afterMs}
    })()`, { polling: 200, timeout: CFG.rendererReadWaitMs })
  } catch (e) { read = 'no' /* none came within its bound: said by the evaluator, per load */ }
  // after a class's last load the page closes next, and its renderer with it: the read places this pid only with a
  // sample of it AFTER the read (no other process took the pid in between), so the page stays open for the
  // sampler's next sample (renderers.js stamps the page once its read is over; CFG.rendererSampleWaitMs: two of
  // the sampler's periods)
  if (read === 'yes' && tags.side === 'after') await page.waitForTimeout(CFG.rendererSampleWaitMs)
  RENDERER_WAIT.add(Date.now() - t0, Object.assign({ read }, tags))
}

// The first navigation, retried within GOTO_TRIES x GOTO_WAIT_MS (above).
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

// The bench's debug reader (bench/rbac_model.py THE DEBUG READER): snowplow
// #1365 authorizes /debug by RBAC, and no session's user holds a /debug grant
// (the measurement keeps the customer's RBAC shape), so the counters are read
// as this one identity, logged in once per VU at authn and again after a 401.
// null when it cannot log in (the class is then not proven).
let DEBUG_TOKEN = null
function debugToken (renew) {
  if (renew) DEBUG_TOKEN = null
  if (DEBUG_TOKEN || !CFG.authnUrl || !CFG.debugUser) return DEBUG_TOKEN
  try {
    const r = http.get(`${CFG.authnUrl}/basic/login`, {
      headers: { Authorization: `Basic ${encoding.b64encode(`${CFG.debugUser}:${CFG.password}`)}` },
      tags: { bench: 'debug_reader_login' }, timeout: '30s',
    })
    if (r.status === 200) DEBUG_TOKEN = r.json().accessToken || null
  } catch (e) {
    DEBUG_TOKEN = null
  }
  return DEBUG_TOKEN
}

// #935: snowplow's learned-class counters (/debug/vars, as the debug reader:
// the counters are snowplow's, whoever reads them): the proof a fresh class
// was one snowplow had not learned. `token` is the session's own: no token
// yet means no sign-in to judge, and null; null too when the read fails (the
// class is then not proven).
function learnedCounts (token) {
  if (!token || !CFG.snowplowUrl) return null
  try {
    let r = null
    for (const renew of [false, true]) {
      const dt = debugToken(renew)
      if (!dt) return null
      r = http.get(`${CFG.snowplowUrl}/debug/vars`, {
        headers: { Authorization: `Bearer ${dt}` }, tags: { bench: 'learned_classes' }, timeout: '30s',
      })
      if (r.status !== 401) break
    }
    if (r.status !== 200) return null
    const v = r.json()
    return { registered: v.snowplow_learned_classes_registered, seeded: v.snowplow_learned_classes_seeded }
  } catch (e) {
    return null
  }
}

async function tokenOf (page) {
  try {
    return await page.evaluate(() => JSON.parse(window.localStorage.getItem('K_user') || '{}').accessToken || null)
  } catch (e) {
    return null
  }
}

export default async function () {
  exec.vu.metrics.metadata.vu = String(exec.vu.idInTest)      // #1089: see nextLoadId
  const s = CFG.sessions[exec.vu.idInTest - 1]
  SIGNED_IN.add(0, { session: s.name })                       // the gate judges from here (SIGNIN_BOUND_MS)
  const deadline = Date.now() + CFG.durationS * 1000
  // #935: the session's identity classes, its initial user first, then each
  // FRESH class (bench/fresh_classes.py); one browser context each, like a
  // new person: its own sign-in, its own app cache, its own first visits
  const users = [s.user].concat(s.fresh || [])
  const secrets = users.concat([CFG.password])
  let token = null
  let k = 0
  let failed = null          // why the last class's sign-in never completed (#1508), null when it did
  try {
    for (; k < users.length && Date.now() < deadline; k++) {
      const res = await runClass(s, users[k], k, k === users.length - 1, deadline, token, secrets)
      token = res.token
      failed = res.signinFailed
    }
  } catch (e) {
    // #1508: the session ends here (k6 logs the error as uncaught): said with its cause, never silently
    SESSION_END.add(1, sessionEndTags(s.name, k, true, e, secrets))
    throw e
  }
  const early = Date.now() < deadline
  const last = Math.max(0, k - 1)
  SESSION_END.add(1, sessionEndTags(s.name, last, early, !early ? 'the drive is over'
    : failed ? `class ${last}'s sign-in never completed (${failed}), and no identity class is left`
      : 'no identity class left before the drive\'s end', secrets))
}

// One identity class: its own context, its sign-in, then its navigations. Returns { token, signinFailed }:
// #1508: a sign-in that never completed signed nobody in, so the class's navigations are not run (they
// would each 'time out' by construction: run 27c N2's s2); signinFailed says why, and the session goes on
// with its next class
async function runClass (s, user, k, last, deadline, prevToken, secrets) {
  const context = await browser.newContext()
  await context.addInitScript(OBSERVER)
  // 2026-11-17 ("Measure it precisely"): the page says whose it is, so renderers.js can name the renderer process
  // that renders this session's page (Chrome's own frame -> process record) and charge each load its own stall
  await context.addInitScript(`window.__benchSession = ${JSON.stringify({ session: s.name, class: String(k) })}`)
  const page = await context.newPage()
  const visits = {}
  const base = { session: s.name, class: String(k), identity: k === 0 ? 'initial' : 'fresh' }
  let shown = '(none)'     // the route on screen: tags the calls made between loads
  let token = prevToken
  let signinFailed = null
  // #919 (N7): every request the page makes, until it finishes or fails
  // (k6 emits nothing for a request that failed at network level)
  const ledger = requestLedger()
  page.on('request', (req) => ledger.started(req.method(), req.url(), Date.now(), req.resourceType()))
  page.on('response', (res) => ledger.responded(res.request().method(), res.url()))
  page.on('requestfinished', (req) => ledger.finished(req.method(), req.url()))
  page.on('requestfailed', (req) => {
    const f = req.failure()
    const op = ledger.failed(req.method(), req.url(), Date.now(), f && f.errorText, req.resourceType())
    if (op) LEDGER_METRICS[op[0]].add(op[1], Object.assign({ session: s.name, route: shown }, op[2]))
  })
  try {
    await gotoLogin(page)
    // #935: snowplow's learned classes right before a fresh class's sign-in
    const tBefore = Date.now() / 1000
    const before = k > 0 ? learnedCounts(prevToken) : null
    const opts = { timeout: CFG.pageTimeoutMs }
    const form = page.locator(LOGIN.form)
    const submit = form.locator(LOGIN.submit)
    try {
      await page.locator(LOGIN.username).fill(user, opts)
      await page.locator(LOGIN.password).fill(CFG.password, opts)
    } catch (e) {
      // named, and without the identity (k6's error quotes the value it was filling): the
      // session ends here; default() records it with this message (browser_session_end, #1508)
      // and k6 logs it as uncaught
      throw new Error(`session ${s.name}: the login form ${LOGIN.form} (${LOGIN.username}, ${LOGIN.password}) ` +
        `could not be filled: ${String(e).split(user).join('<user>').split(CFG.password).join('<password>')}`)
    }
    // the form is rendered (its fields took the fills): it holds exactly one submit control, or
    // which one submits is not the contract's to say
    const submits = await submit.count()
    if (submits !== 1) {
      throw new Error(`session ${s.name}: the login form ${LOGIN.form} holds ${submits} ${LOGIN.submit} ` +
        'controls, not exactly 1: which one signs in is not the contract\'s')
    }
    // #1517 review: a renderer read of this page BEFORE its first load (the sign-in arrival) starts
    await awaitRead(page, 0, { session: s.name, side: 'before' })
    // the first page's clock starts when the app leaves /login (pageload.js)
    await page.evaluate(() => {
      const b = window.__bench
      b.armLogin = true; b.submitAt = performance.now(); b.signinMs = null
    })
    const loginAt = Date.now()
    const firstId = nextLoadId()
    try {
      await submit.click(opts)
    } catch (e) {
      throw new Error(`session ${s.name}: the ${LOGIN.submit} control of ${LOGIN.form} could not be clicked: ` +
        `${String(e).split(user).join('<user>').split(CFG.password).join('<password>')}`)
    }
    const arrival = await settled(page, null, deadline, secrets)
    const first = arrival.load
    const landed = first === null ? '(none)' : first.path
    if (first !== null) {
      LOGINS.add(1, { session: s.name })
      if (k === 0) SIGNED_IN.add(1, { session: s.name })
      const signin = await page.evaluate(() => window.__bench.signinMs)
      if (signin !== null) SIGNIN.add(signin, { session: s.name })
    }
    record(arrival, Object.assign({ route: landed, visit: 'first', method: 'login' }, base), loginAt, firstId)
    await drainCalls(page, Object.assign({ route: landed, visit: 'first', during: 'load' }, base),
      loadStart(first, loginAt), firstId)
    if (first === null) {
      // #1508: reported once (the login load above, timed out with its cause); nothing of this class runs on
      signinFailed = arrival.cause
      return { token, signinFailed }
    }
    token = (await tokenOf(page)) || prevToken
    if (k > 0) {
      const after = learnedCounts(token)
      FRESH.add(1, Object.assign({}, base, freshClassTags(before, after, tBefore)))
    }
    visits[landed] = (visits[landed] || 0) + 1
    shown = landed
    // #1517 review: the login load was the class's one cross-document navigation, so its renderer can have changed:
    // a read now, after it (outside every load), brackets the same-document loads that follow
    await awaitRead(page, Date.now(), { session: s.name, side: 'login' })
    let n = 0
    while (Date.now() < deadline && !switchDue(visits, CFG.routes, last)) {
      await page.waitForTimeout(CFG.thinkS * 1000)
      const route = CFG.routes[(s.offset + (++n)) % CFG.routes.length]
      const item = page.locator(`li.ant-menu-item[data-menu-id$="-${route}"]`)
      let method = 'click'
      await drainCalls(page, Object.assign({ route: shown, during: 'between' }, base))
      const actionAt = Date.now()
      const loadId = nextLoadId()
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
      const res = await settled(page, route, deadline, secrets)
      const r = res.load
      const visit = visits[route] ? 'warm' : 'first'
      record(res, Object.assign({ route, visit, method }, base), actionAt, loadId)
      await drainCalls(page, Object.assign({ route, visit, during: 'load' }, base), loadStart(r, actionAt), loadId)
      visits[route] = (visits[route] || 0) + 1
      shown = route
    }
  } finally {
    // #1517 review: the context stays open until renderers.js has read the page AFTER its last load
    await awaitRead(page, Date.now(), { session: s.name, side: 'after' })
    await drainCalls(page, Object.assign({ route: shown, during: 'between' }, base))
    for (const [, value, t] of ledger.unfinished(Date.now())) {
      INCOMPLETE.add(value, Object.assign({ session: s.name, route: shown }, t))
    }
    try {
      const sse = await page.evaluate(() => window.__benchSse || { refreshes: 0, notifications: 0 })
      SSE.add(sse.refreshes, { session: s.name, stream: 'refreshes' })
      SSE.add(sse.notifications, { session: s.name, stream: 'notifications' })
    } catch (e) { /* the page is gone: nothing counted, the evaluator says so */ }
    await page.close()
    await context.close()
  }
  return { token, signinFailed }
}

export function setup () { FRAME.add(1, { frame: 'start' }) }
export function teardown () { FRAME.add(1, { frame: 'end' }) }

export function handleSummary (data) {
  const keep = {}
  for (const [name, m] of Object.entries(data.metrics)) {
    if (/^(browser_page_load_ms|browser_page_loads|browser_widget_states|browser_page_timeouts|browser_logins|browser_signin_ms|browser_sse_opened|browser_call_ms|browser_request_incomplete_ms|browser_request_failed_start_unseen|browser_fresh_class|browser_session_end|bench_frame)(\{|$)/.test(name)) keep[name] = m.values
  }
  return { [__ENV.SUMMARY_FILE || '/out/browser-summary.json']: JSON.stringify({
    kind: 'browser_summary', sessions: CFG.sessions.map((x) => x.name),
    firstVisit: CFG.firstVisit, metrics: keep }) }
}
