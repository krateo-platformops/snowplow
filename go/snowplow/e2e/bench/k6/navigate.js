// navigate.js — run 11's background load (#830): 1,000 users replaying the
// portal SPA's /call trees against snowplow, under k6's ARRIVAL-RATE
// executors, so a late start is a counted `dropped_iterations`, never a hidden
// lag (run 10's single Python driver queued its arrival for ~2 minutes).
//
// Iteration i of `navigate` is user NAV[i mod |NAV|]: its first iteration
// is the first load (shell + first route, spa.firstLoad), every later one a
// route click on the route spa.pickRoute(routes, user, visit) chooses — each
// user navigates once per THINK_S on average, and the first loads arrive spread
// over the first THINK_S. `composite_burst`: the reserved BURST users deep-link
// to a composite route all at once at BURST_AT_S, then join `burst_navigate`.
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

const NAV = CFG.navUserIds
const BURST = CFG.burstUserIds
const navRate = NAV.length / CFG.thinkS
const burstRate = BURST.length / CFG.thinkS

const scenarios = {
  navigate: {
    executor: 'constant-arrival-rate',
    exec: 'navigate',
    rate: Math.round(navRate * 1000),
    timeUnit: '1000s',
    duration: `${CFG.durationS}s`,
    preAllocatedVUs: CFG.preAllocatedVUs,
    maxVUs: CFG.maxVUs,
  },
}
if (BURST.length > 0) {
  scenarios.composite_burst = {
    executor: 'per-vu-iterations',
    exec: 'compositeBurst',
    vus: BURST.length,
    iterations: 1,
    startTime: `${CFG.burstAtS}s`,
    maxDuration: `${CFG.thinkS * 5}s`,
  }
  scenarios.burst_navigate = {
    executor: 'constant-arrival-rate',
    exec: 'burstNavigate',
    rate: Math.round(burstRate * 1000),
    timeUnit: '1000s',
    startTime: `${CFG.burstAtS + CFG.thinkS}s`,
    duration: `${Math.max(1, CFG.durationS - CFG.burstAtS - CFG.thinkS)}s`,
    preAllocatedVUs: Math.min(BURST.length,
      Math.max(10, Math.ceil(CFG.preAllocatedVUs * BURST.length / CFG.users))),
    maxVUs: BURST.length,
  }
}

// Always-true thresholds force the per-scenario submetrics into the summary,
// which the evaluator matches against the stream.
const thresholds = {}
for (const s of Object.keys(scenarios)) {
  thresholds[`dropped_iterations{scenario:${s}}`] = ['count>=0']
  thresholds[`iterations{scenario:${s}}`] = ['count>=0']
  thresholds[`page_loads{scenario:${s}}`] = ['count>=0']
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

function navId () {
  return `${exec.scenario.name}:${exec.scenario.iterationInTest}`
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
  const failed = firstLoadFailed(fl)
  if (failed) FIRST_FAILED.add(1, { kind })
  else checkRoutes(fl.routes)
  feed({ kind: 'nav', nav: navId(), user, route, t_end: Date.now() / 1000, ok: !failed,
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
  feed({ kind: 'nav', nav: navId(), user, route: route.path, t_end: Date.now() / 1000, ok: true,
         page: w.armed })
}

export function navigate () {
  const i = exec.scenario.iterationInTest
  const user = NAV[i % NAV.length]
  const visit = Math.floor(i / NAV.length)
  if (visit === 0) doFirstLoad(user, null)
  else routeClick(user, visit, CFG.routes[0].path)
}

export function compositeBurst () {
  // per-vu-iterations, 1 each: iterationInTest is 0..BURST-1, one per user
  const i = exec.scenario.iterationInTest
  doFirstLoad(BURST[i], CFG.compositeRoutes[i % CFG.compositeRoutes.length])
}

export function burstNavigate () {
  const i = exec.scenario.iterationInTest
  const b = i % BURST.length
  routeClick(BURST[b], 1 + Math.floor(i / BURST.length),
             CFG.compositeRoutes[b % CFG.compositeRoutes.length])
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
    if (/^(page_load_ms|page_loads|page_calls|page_errors|first_load_failed|route_table_mismatch|bench_frame|dropped_iterations|iterations|http_reqs|http_req_failed|http_req_duration|http_req_blocked|vus_max)(\{|$)/.test(name)) {
      keep[name] = m.values
    }
  }
  const out = { kind: 'k6_summary', config: { users: CFG.users, navUsers: NAV.length,
    burstUsers: BURST.length,
    thinkS: CFG.thinkS, durationS: CFG.durationS, burstAtS: CFG.burstAtS,
    navRate, burstRate, routes: CFG.routes.length }, metrics: keep }
  return { [__ENV.SUMMARY_FILE || '/out/k6-summary.json']: JSON.stringify(out) }
}
