// spa.js — what the portal SPA fetches, as pure functions (no k6 imports), so
// the same file runs under k6 (navigate.js) and under node for the parity test
// against load606.py (tests/test_k6_spa_parity.py). A line-for-line port of
// load606.py's model (#830), which cites the frontend ui/src:
//
// A widget's children are fetched ONLY when a container RENDERS them: a render
// slot in widgetData names their resourceRefId (components/RefChild/RefChild.tsx
// :60-64). Slots by kind:
//   Flex/Col/Row/Form/ButtonGroup/Steps/Filters/List/PageHeader/Card body:
//       widgetData.items[].resourceRefId   (Flex.tsx:15, Col.tsx:23, Row.tsx:64,
//       Form.tsx:584, ButtonGroup.tsx:27, Steps.tsx:21, Filters.tsx:54,
//       List.tsx:50, PageHeader.tsx:75, Card.tsx:211)
//   Card: also cover, extraRefId, footer[].resourceRefId (Card.tsx:34-54)
//   Table: dataSource[][].resourceRefId per rendered cell (Table.tsx:168-236)
//   Layout: header, content, footer, sider.resourceRefId, effects[]
//       (Layout.tsx:20-23, 41-47, 74)
//   Tabs: ONLY the active tab, default the first (Tabs.tsx:18-45)
// NOT fetched on render: Menu items only REGISTER routes (Menu.tsx:35-43; a
// route's page is fetched on visit, WidgetPage.tsx:41, 90); widgetActions /
// actions run on click only (Drawer.tsx:94, Modal.tsx:84). A ref the user may
// not GET (resourcesRefs.items[].allowed false) is never fetched
// (WidgetRenderer.tsx:51-52).

const LAYOUT_SLOTS = ['header', 'content', 'footer']
const NEVER_EAGER_KEYS = ['widgetActions', 'actions']

function isObj (v) { return v !== null && typeof v === 'object' && !Array.isArray(v) }

export function allowedRefPaths (doc) {
  const out = {}
  if (!isObj(doc)) return out
  const items = (((doc.status || {}).resourcesRefs || {}).items) || []
  for (const it of items) {
    if (!isObj(it) || !it.id) continue
    if (String(it.verb || 'GET').toUpperCase() !== 'GET' || !it.allowed) continue
    if (it.rendered) continue
    const p = it.path || ''
    if (p.startsWith('/call')) out[it.id] = p
  }
  return out
}

function collectRefIds (node, out) {
  if (Array.isArray(node)) {
    for (const v of node) collectRefIds(v, out)
  } else if (isObj(node)) {
    for (const [k, v] of Object.entries(node)) {
      if (NEVER_EAGER_KEYS.includes(k)) continue
      if ((k === 'resourceRefId' || k === 'extraRefId' || k === 'cover') && typeof v === 'string') {
        out.push(v)
      } else {
        collectRefIds(v, out)
      }
    }
  }
}

// The /call paths the SPA fetches when it RENDERS this widget.
export function eagerRefs (doc) {
  if (!isObj(doc)) return []
  const kind = doc.kind
  const wd = (doc.status || {}).widgetData || {}
  const paths = allowedRefPaths(doc)
  if (kind === 'Menu') return []
  const ids = []
  if (kind === 'Layout') {
    for (const k of LAYOUT_SLOTS) if (typeof wd[k] === 'string') ids.push(wd[k])
    if (isObj(wd.sider) && typeof wd.sider.resourceRefId === 'string') ids.push(wd.sider.resourceRefId)
    for (const e of (wd.effects || [])) if (typeof e === 'string') ids.push(e)
  } else if (kind === 'Tabs') {
    const items = wd.items || []
    if (items.length && isObj(items[0]) && items[0].resourceRefId) ids.push(items[0].resourceRefId)
  } else {
    collectRefIds(wd, ids)
  }
  const seen = new Set()
  const out = []
  for (const i of ids) {
    const p = paths[i]
    if (p && !seen.has(p)) { seen.add(p); out.push(p) }
  }
  return out
}

// navModel.ts routeSlug: drop {param} segments + leading '/', '/' -> '-'.
export function routeSlug (path) {
  const s = String(path || '').replace(/\/?\{[^}]+\}/g, '').replace(/^\/+/, '').replace(/\//g, '-')
  return s || 'home'
}

// The routes the sidebar Menu registers, in menu order (navModel.ts:56-78).
export function menuRoutes (menuDoc, frontendNs) {
  const wd = (((menuDoc || {}).status) || {}).widgetData || {}
  const paths = allowedRefPaths(menuDoc)
  const items = (wd.items || []).filter(isObj).map((it, idx) => ({ it, idx }))
  items.sort((a, b) => ((a.it.order || 0) - (b.it.order || 0)) || (a.idx - b.idx))
  const out = []
  for (const { it } of items) {
    if (it.type === 'divider' || !it.path) continue
    let endpoint
    if (it.resourceRefId) {
      if (!(it.resourceRefId in paths)) continue          // denied / dangling: not visible
      endpoint = paths[it.resourceRefId]
    } else {
      const ns = (it.namespace !== undefined && it.namespace !== null) ? it.namespace : frontendNs
      endpoint = '/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1' +
        `&resource=flexes&name=page-${it.page || routeSlug(it.path)}&namespace=${ns}`
    }
    out.push({ path: it.path, endpoint, templated: /\{[^}]+\}/.test(it.path) })
  }
  return out
}

// Shell.tsx:28-29: header-context, derived from INIT's namespace.
export function headerContext (init) {
  const m = /[?&]namespace=([^&]*)/.exec(init)
  const ns = m ? decodeURIComponent(m[1]) : 'krateo-system'
  return '/call?resource=flexes&apiVersion=widgets.templates.krateo.io/v1beta1' +
    `&name=header-context&namespace=${ns}`
}

// clusterBuilders.ts:4-6,116-122: one /list read per sign-in.
export function buildersList (buildersNs) {
  return buildersNs ? `/list?category=builders&ns=${buildersNs}` : null
}

// The refresh classes snowplow stamps on a /call answer (load606.REFRESH_CLASSES).
export const REFRESH_CLASSES = ['widgets', 'widgetContent', 'restactions']

// Level-by-level walk of a widget tree. `fetchLevel(paths)` returns, in order,
// one {status, body, refresh} per path (the caller decides HOW: k6 http.batch,
// node test fixtures); `refresh` = {key, cls} from the X-Snowplow-Refresh-Key /
// -Class headers, or null. Mirrors load606.walk: a child is requested only
// after its parent arrived; `inject[path]` adds what the app requests once
// `path` arrived; `armed` lists the widgets a tab arms on its /refreshes
// stream ({path, key, cls}: every 200 answer that carried both headers).
export function walkTree (roots, fetchLevel, opts = {}) {
  const inject = opts.inject || {}
  const maxDepth = opts.maxDepth === undefined ? 12 : opts.maxDepth
  const maxCalls = opts.maxCalls === undefined ? 5000 : opts.maxCalls
  const keepDocs = !!opts.keepDocs
  const seen = new Set(roots)
  let level = roots.slice()
  const res = { calls: 0, errors: 0, bytes: 0, depth: 0, levels: [], docs: {}, statuses: {}, armed: [] }
  while (level.length && res.depth <= maxDepth && res.calls < maxCalls) {
    const next = []
    const got = fetchLevel(level, res.depth)
    res.levels.push(level.slice())
    for (let i = 0; i < level.length; i++) {
      const path = level[i]
      const r = got[i] || { status: 0, body: '' }
      res.calls++
      res.bytes += (r.body || '').length
      res.statuses[String(r.status)] = (res.statuses[String(r.status)] || 0) + 1
      for (const extra of (inject[path] || [])) {
        if (!seen.has(extra)) { seen.add(extra); next.push(extra) }
      }
      if (r.status !== 200) { res.errors++; continue }
      if (r.refresh && r.refresh.key && REFRESH_CLASSES.includes(r.refresh.cls)) {
        res.armed.push({ path, key: r.refresh.key, cls: r.refresh.cls })
      }
      let doc = null
      try { doc = JSON.parse(r.body || 'null') } catch (e) { continue }
      if (keepDocs) res.docs[path] = doc
      for (const ref of eagerRefs(doc)) {
        if (!seen.has(ref)) { seen.add(ref); next.push(ref) }
      }
    }
    level = next
    res.depth++
  }
  return res
}

// The first load after sign-in (load606.first_load): INIT Layout + eager slots
// (Shell.tsx:92), header-context (Shell.tsx:26-33, Layout.tsx:41), the
// Builders /list read (Shell.tsx:81-85); the Menu registers routes and `/`
// redirects to the FIRST route (Menu.tsx:45-50), whose page tree is fetched.
// `deepLink` (the composite burst) replaces "first route" by that route.
export function firstLoad (portal, fetchLevel, deepLink) {
  const roots = [portal.init]
  const bl = buildersList(portal.buildersNs)
  if (bl) roots.push(bl)
  const inject = {}
  inject[portal.init] = [headerContext(portal.init)]
  const shell = walkTree(roots, fetchLevel, { inject, keepDocs: true })
  const menu = Object.values(shell.docs).find(d => isObj(d) && d.kind === 'Menu') || null
  const routes = menuRoutes(menu, portal.frontendNs).filter(r => !r.templated)
  let target = routes.length ? routes[0] : null
  if (deepLink) target = routes.find(r => r.path === deepLink) || null
  const content = target ? walkTree([target.endpoint], fetchLevel) : null
  return { shell, routes, target, content }
}

// load606.first_load_failed: no routes, or a transport failure (status 0).
export function firstLoadFailed (fl) {
  if (!fl.routes.length || !fl.target) return true
  for (const w of [fl.shell, fl.content]) {
    if (w && w.statuses['0']) return true
  }
  return false
}

// Deterministic choice shared with any non-k6 companion (FNV-1a 32-bit): the
// route a user visits on its n-th navigation.
export function fnv1a (s) {
  let h = 0x811c9dc5
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 0x01000193) >>> 0
  }
  return h >>> 0
}

export function pickRoute (routes, user, visit) {
  return routes.length ? routes[fnv1a(`${user}#${visit}`) % routes.length] : null
}
