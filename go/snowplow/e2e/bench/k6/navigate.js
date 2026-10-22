// navigate.js — run 11's background load (#830): 1,000 users replaying the
// portal SPA's /call trees against snowplow.
//
// Each user is one tab, driven as the SPA drives it (run 12): ONE VU per user
// (per-vu-iterations, one iteration = the user's whole session). The user
// ARRIVES on an open schedule — navigating user k of |NAV| at k * THINK_S /
// |NAV| after the scenario starts, the composite-burst users all at
// BURST_AT_S — and its first load (shell + first route, spa.firstLoad, or the
// deep link) starts then, whatever the server is doing; a start later than
// LATE_MS is counted (`arrivals_late`), never a hidden lag (run 10's single
// Python driver queued its arrival for ~2 minutes). Its route clicks (the
// route spa.pickRoute(routes, user, visit) chooses) start only after its first
// load COMPLETED, each THINK_S after the previous page ended: run 11 started
// them on a fixed rate whatever the first load was doing, and 36 % of the
// storm's calls were clicks of users whose dashboard had not loaded yet. Every
// click the old open schedule would have started while the first load still
// ran is counted (`clicks_deferred`), so the load the dependency removed is
// reported, not hidden.
// NAV and BURST (config navUserIds / burstUserIds, 1-based) leave out the
// real-Chrome sessions' users: each of the 1,000 identities is driven by ONE
// client, so k6 never pre-warms a browser user's own first visits.
//
// A page load = every /call of the route's tree, level by level, each level
// one http.batch capped at 6 per host (Chrome's HTTP/1.1 per-host limit;
// snowplow is plain http) — the SPA's own requests (spa.js cites the
// frontend). Recorded as k6 metrics into --out json=/out/k6.json.gz, framed:
// a `bench_frame{frame:start}` point first (setup), `{frame:end}` last
// (teardown), and handleSummary's /out/k6-summary.json with the totals the
// evaluator matches against the stream.
//
// The open tabs (#830 ruling, option A): after every page load k6 writes one
// line to its console, which `k6 run --log-format raw --console-output
// /feed/nav.jsonl` turns into a JSON-lines feed on an emptyDir shared with the
// viewer sidecar (load606.py view-feed): {kind: nav, nav, user, route, t_end,
// ok, shell?, page} with the refresh key and class each widget ACTUALLY
// answered with. The sidecar re-arms that user's /refreshes stream on exactly
// those widgets — no schedule to predict, no second walk. Framed by
// feed_start (setup) and feed_end (teardown).
import http from 'k6/http'
import exec from 'k6/execution'
import { sleep } from 'k6'
import { SharedArray } from 'k6/data'
import { Counter, Trend } from 'k6/metrics'
import { firstLoad, firstLoadFailed, pickRoute, walkTree } from './spa.js'

const CFG = JSON.parse(open(__ENV.K6_CONFIG || '/opt/k6/config.json'))
// Bearer tokens obtained by the pod's login initContainer (never under /out).
const TOKENS = new SharedArray('tokens', () => JSON.parse(open(__ENV.TOKENS_FILE || '/work/tokens.json')))

const PAGE = new Trend('page_load_ms', true)
const LOADS = new Counter('page_loads')
const CALLS = new Counter('page_calls')
const NAV_ERRORS = new Counter('page_errors')
const FIRST_FAILED = new Counter('first_load_failed')
const ROUTE_MISMATCH = new Counter('route_table_mismatch')
const FRAME = new Counter('bench_frame')
const ARRIVALS = new Counter('arrivals')               // users whose first load started
const ARRIVALS_LATE = new Counter('arrivals_late')     // ... more than LATE_MS after their slot
const ARRIVAL_LAG = new Trend('arrival_lag_ms', true)
const FIRST_LOADS = new Counter('first_loads')         // first loads recorded (per scenario)
const CLICKS_DEFERRED = new Counter('clicks_deferred')
const LATE_MS = 1000

const NAV = CFG.navUserIds
const BURST = CFG.burstUserIds
const navRate = NAV.length / CFG.thinkS
// A page that starts by the end of the run may take this long to finish
// (spa.js / fetcher: 60 s per request, a few levels deep).
const GRACE_S = 300

// One VU per user; a session ends when its last page (started before
// durationS) is done.
const scenarios = {
  navigate: {
    executor: 'per-vu-iterations',
    exec: 'navigate',
    vus: NAV.length,
    iterations: 1,
    maxDuration: `${CFG.durationS + GRACE_S}s`,
  },
}
if (BURST.length > 0) {
  scenarios.composite_burst = {
    executor: 'per-vu-iterations',
    exec: 'compositeBurst',
    vus: BURST.length,
    iterations: 1,
    startTime: `${CFG.burstAtS}s`,
    maxDuration: `${Math.max(1, CFG.durationS - CFG.burstAtS) + GRACE_S}s`,
  }
}

// Always-true thresholds force the per-scenario submetrics into the summary,
// which the evaluator matches against the stream.
const thresholds = {}
for (const s of Object.keys(scenarios)) {
  for (const m of ['iterations', 'page_loads', 'arrivals', 'arrivals_late', 'first_loads',
    'clicks_deferred']) {
    thresholds[`${m}{scenario:${s}}`] = ['count>=0']
  }
}
thresholds['page_loads'] = ['count>=0']

export const options = {
  scenarios,
  thresholds,
  batchPerHost: 6,
  // Every request writes ~9 points to /out/k6.json.gz; keep their tags small
  // (no url/name: a page's calls are told apart by its route and level tags).
  systemTags: ['status', 'method', 'scenario', 'expected_response', 'error_code'],
  discardResponseBodies: false,
  summaryTrendStats: ['count', 'min', 'med', 'p(95)', 'max'],
  setupTimeout: '60s',
  teardownTimeout: '60s',
}

function feed (rec) {
  if (CFG.viewers) console.log(JSON.stringify(rec))
}

function navId (user, visit) {
  return `${exec.scenario.name}:${user}:${visit}`
}

function refreshOf (r) {
  const key = r.headers['X-Snowplow-Refresh-Key']
  const cls = r.headers['X-Snowplow-Refresh-Class']
  return key && cls ? { key, cls } : null
}

function headers (user) {
  return { Authorization: `Bearer ${TOKENS[user - 1]}` }
}

// One level of the tree: one http.batch, at most 6 in flight per host.
function fetcher (user, tags) {
  return (paths, depth) => {
    const reqs = paths.map((p) => ({
      method: 'GET',
      url: CFG.snowplowUrl + p,
      params: { headers: headers(user), tags: Object.assign({ level: String(depth) }, tags),
                timeout: '60s' },
    }))
    return http.batch(reqs).map((r) => ({ status: r.status, body: r.body || '', refresh: refreshOf(r) }))
  }
}

const PORTAL = { init: CFG.init, frontendNs: CFG.frontendNs, buildersNs: CFG.buildersNs }

function record (start, kind, route, visit, calls, errors) {
  const tags = { kind, route, visit }
  PAGE.add(Date.now() - start, tags)
  LOADS.add(1, tags)
  CALLS.add(calls, tags)
  if (errors) NAV_ERRORS.add(errors, tags)
}

// The user's routes must be the run's route table (preflight's): route clicks
// pick from it. Any difference is counted (a validity check), never assumed.
function checkRoutes (routes) {
  const mine = routes.map((r) => `${r.path} ${r.endpoint}`).sort().join('\n')
  const table = CFG.routes.map((r) => `${r.path} ${r.endpoint}`).sort().join('\n')
  if (mine !== table) ROUTE_MISMATCH.add(1)
}

function doFirstLoad (user, deepLink) {
  const start = Date.now()
  const kind = deepLink ? 'composite_first_load' : 'first_load'
  const fl = firstLoad(PORTAL, fetcher(user, { kind }), deepLink)
  const calls = fl.shell.calls + (fl.content ? fl.content.calls : 0)
  const errors = fl.shell.errors + (fl.content ? fl.content.errors : 0)
  const route = fl.target ? fl.target.path : '(none)'
  record(start, kind, route, 'first', calls, errors)
  FIRST_LOADS.add(1)
  const failed = firstLoadFailed(fl)
  if (failed) FIRST_FAILED.add(1, { kind })
  else checkRoutes(fl.routes)
  feed({ kind: 'nav', nav: navId(user, 0), user, route, t_end: Date.now() / 1000, ok: !failed,
         shell: fl.shell.armed, page: fl.content ? fl.content.armed : [] })
}

function routeClick (user, visit, firstPath) {
  // Has this user visited the route before? Its earlier choices are a pure
  // function of (user, visit): the first-load route at visit 0, then pickRoute.
  const route = pickRoute(CFG.routes, user, visit)
  let seen = route.path === firstPath
  for (let v = 1; v < visit && !seen; v++) seen = pickRoute(CFG.routes, user, v).path === route.path
  const start = Date.now()
  const w = walkTree([route.endpoint], fetcher(user, { kind: 'route' }))
  record(start, 'route', route.path, seen ? 'warm' : 'first', w.calls, w.errors)
  feed({ kind: 'nav', nav: navId(user, visit), user, route: route.path, t_end: Date.now() / 1000,
         ok: true, page: w.armed })
}

// One user's tab: arrive at `slotMs` (epoch ms), first load, then a route
// click THINK_S after each page completed, until the run's end. `endMs`:
// no page starts after it.
function session (user, slotMs, endMs, deepLink, firstPath) {
  const wait = slotMs - Date.now()
  if (wait > 0) sleep(wait / 1000)
  const lag = Math.max(0, Date.now() - slotMs)
  ARRIVALS.add(1)
  ARRIVAL_LAG.add(lag)
  if (lag > LATE_MS) ARRIVALS_LATE.add(1)
  doFirstLoad(user, deepLink)
  const thinkMs = CFG.thinkS * 1000
  const loaded = Date.now()
  // the open schedule's clicks (slot + v * THINK_S) that fell inside the
  // first load: a browser cannot click a page it does not have yet
  const deferred = Math.max(0, Math.ceil((Math.min(loaded, endMs) - slotMs) / thinkMs) - 1)
  if (deferred > 0) CLICKS_DEFERRED.add(deferred)
  let next = loaded + thinkMs
  for (let visit = 1; next < endMs; visit++) {
    sleep(Math.max(0, next - Date.now()) / 1000)
    routeClick(user, visit, firstPath)
    next = Date.now() + thinkMs
  }
}

function runEndMs () {
  return exec.scenario.startTime + (CFG.durationS - (exec.scenario.name === 'composite_burst'
    ? CFG.burstAtS : 0)) * 1000
}

export function navigate () {
  // per-vu-iterations, 1 each: iterationInTest is 0..NAV-1, one per user;
  // the first loads arrive spread over the first THINK_S, as before
  const i = exec.scenario.iterationInTest
  session(NAV[i], exec.scenario.startTime + i * 1000 / navRate, runEndMs(), null,
          CFG.routes[0].path)
}

export function compositeBurst () {
  // all at BURST_AT_S (the scenario's start), each deep-linking to a composite route
  const i = exec.scenario.iterationInTest
  const route = CFG.compositeRoutes[i % CFG.compositeRoutes.length]
  session(BURST[i], exec.scenario.startTime, runEndMs(), route, route)
}

export function setup () {
  FRAME.add(1, { frame: 'start' })
  feed({ kind: 'feed_start', t: Date.now() / 1000 })
}
export function teardown () {
  FRAME.add(1, { frame: 'end' })
  feed({ kind: 'feed_end', t: Date.now() / 1000 })
}

export function handleSummary (data) {
  const keep = {}
  for (const [name, m] of Object.entries(data.metrics)) {
    if (/^(page_load_ms|page_loads|page_calls|page_errors|first_load_failed|route_table_mismatch|bench_frame|dropped_iterations|iterations|arrivals|arrivals_late|arrival_lag_ms|first_loads|clicks_deferred|http_reqs|http_req_failed|http_req_duration|http_req_blocked|vus_max)(\{|$)/.test(name)) {
      keep[name] = m.values
    }
  }
  const out = { kind: 'k6_summary', config: { users: CFG.users, navUsers: NAV.length,
    burstUsers: BURST.length,
    thinkS: CFG.thinkS, durationS: CFG.durationS, burstAtS: CFG.burstAtS,
    navRate, routes: CFG.routes.length }, metrics: keep }
  return { [__ENV.SUMMARY_FILE || '/out/k6-summary.json']: JSON.stringify(out) }
}
