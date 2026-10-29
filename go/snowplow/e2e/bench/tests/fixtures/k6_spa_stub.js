// spa.js's interface, stubbed for navigate.js's scheduling test: a first load
// is ONE request to /first (the fixture answers it slowly), a route click one
// request to /route?p=<path>. navigate.js's own scheduling, metrics and feed
// run unchanged around it.
const ROUTES = [{ path: '/a', endpoint: '/route?p=/a' }, { path: '/b', endpoint: '/route?p=/b' }]

export function firstLoad (portal, fetch, deepLink) {
  const r = fetch(['/first'], 0)[0]
  const ok = r.status === 200
  return { shell: { calls: 1, errors: ok ? 0 : 1, armed: [] }, content: null,
           target: { path: deepLink || ROUTES[0].path }, routes: ROUTES, ok }
}

export function firstLoadFailed (fl) {
  return !fl.ok
}

export function pickRoute (routes, user, visit) {
  return routes[visit % routes.length]
}

export function sameMenu (routes, table) {
  const key = (rs) => (rs || []).map((r) => `${r.path} ${r.endpoint}`).sort().join('\n')
  return key(routes) === key(table)
}

export function userRoutes (fl, failed, classTable) {
  return failed ? classTable : fl.routes
}

export function walkTree (endpoints, fetch) {
  const r = fetch(endpoints, 0)
  return { calls: r.length, errors: r.filter((x) => x.status !== 200).length, armed: [] }
}
