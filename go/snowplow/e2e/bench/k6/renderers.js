// renderers.js: which Chrome renderer process renders each real-browser session's page (the owner's ruling of
// 2026-11-17, "Measure it precisely": a page load is charged only its OWN renderer's CPU stall).
//
// k6 browser exposes no CDP session to the script, so this is a second CDP client, run in the k6browser pod's
// renderers SIDECAR, outside the measured browser container's cgroup (k6_job.renderers_script starts it once every
// session's Chrome wrote its DevToolsActivePort in the shared /tmp).
// Each k6 VU runs its own Chrome (its own --user-data-dir); each Chrome has several renderer processes (the page's,
// a spare, a WebUI one), so neither the VU nor the newest renderer says which process renders the page. Chrome
// itself says it: a trace started in a page's session opens with TracingStartedInBrowser, whose `frames` list
// every frame of that page with the processId hosting it (the record DevTools' Performance panel maps frames to
// processes with). Read 2026-11-17 on grafana/k6:2.3.0-with-browser (Chrome 152): the page target's id is its
// main frame's id, and that frame's processId is the pid of a `--type=renderer` process of the same
// --user-data-dir in the container's /proc.
//
// A page is read only when it ASKS (#1517 review: the reads' own work runs in Chrome, inside the measured browser
// container's cgroup, so it is done only where the page's renderer can have changed, never inside a load):
// browser.js asks before a class's sign-in, after its login load (the class's one cross-document navigation; a
// same-document navigation keeps its document and so its renderer) and after its last load, by calling the
// binding window.__benchRead('bench-read:<side>:...') this watcher gives each page (Runtime.addBinding on a session
// attached once, when the page is created, Target.setDiscoverTargets): between asks the watcher sends Chrome
// nothing. A read, on that session: read the page's own marker (window.__benchSession, set by browser.js's init script: its session and identity class),
// start and at once end a trace, take the outermost main frame's processId, stamp the page (window.__benchMapped:
// the read's start). One line per read on stdout (--console-output): {"kind": "renderer_map", "t0", "t1"
// (epoch s: the read lies between them), "udd", "target", "ask", "session", "class", "pid"}; a page whose marker
// or frame cannot be read is a line with pid null and `why`. Nothing here is a guess: a load the evaluator cannot
// place between two reads of the same (pid, starttime) process has no renderer, and its stall is UNKNOWN.
import { WebSocket } from 'k6/websockets'

const TARGETS = JSON.parse(__ENV.CDP_TARGETS || '[]')
// one page's read (attach, marker, trace, stamp, detach): past it, a line with its why (k6_job.RENDERER_READ_MS)
const READ_MS = Number(__ENV.READ_MS || 10000)

export const options = { scenarios: { map: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: '1000h' } } }

function emit (o) { console.log(JSON.stringify(Object.assign({ kind: 'renderer_map' }, o))) }

// One Chrome: a websocket, its pending calls by id, and the pages of the current poll read one after another
function watch (target) {
  const ws = new WebSocket(target.url)
  // the watcher's own events (its connection, each page it attached): evidence, not reads (kind renderer_cdp)
  const note = (event, o) => console.log(JSON.stringify(Object.assign(
    { kind: 'renderer_cdp', t: Date.now() / 1000, udd: target.udd, event }, o || {})))
  let id = 0
  const waiting = {}                // id -> callback(result, error)
  const traces = {}                 // sessionId -> callback(frames)
  const call = (method, params, sessionId, cb) => {
    const n = ++id
    waiting[n] = cb
    ws.send(JSON.stringify(Object.assign({ id: n, method, params: params || {} }, sessionId ? { sessionId } : {})))
  }
  let busy = false
  const onReply = (e) => {     // replies to calls, and the traces of reads
    const m = JSON.parse(e.data)
    if (m.id && waiting[m.id]) {
      const cb = waiting[m.id]
      delete waiting[m.id]
      cb(m.result, m.error)
      return
    }
    if (m.method === 'Tracing.dataCollected' && traces[m.sessionId]) {
      for (const ev of m.params.value || []) {
        if (ev.name === 'TracingStartedInBrowser') traces[m.sessionId].frames = ((ev.args || {}).data || {}).frames || []
      }
    }
    if (m.method === 'Tracing.tracingComplete' && traces[m.sessionId]) {
      const t = traces[m.sessionId]
      delete traces[m.sessionId]
      t.done(t.frames || null)
    }
  }
  // Each page is attached ONCE, when it is created (a flat session kept for the page's life), and given the
  // binding window.__benchRead: the page calls it to ask for a read (Runtime.bindingCalled on that session). No
  // domain is enabled on the session, nothing is polled: between asks the watcher sends Chrome nothing. The page
  // cannot see the ask: no DOM, no title, no event of its own (the portal sets document.title from its routes,
  // ui/src/hooks/useDocumentTitle.ts, so a title would race it)
  const pages = {}                  // sessionId -> targetId
  const onPage = (info) => {
    if (!info || info.type !== 'page') return
    call('Target.attachToTarget', { targetId: info.targetId, flatten: true }, null, (r, err) => {
      if (err || !r) {
        return emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, target: info.targetId,
          pid: null, why: `attach: ${JSON.stringify(err)}` })
      }
      pages[r.sessionId] = info.targetId
      note('attached', { target: info.targetId })
      // Runtime.bindingCalled reaches a session only with its Runtime domain enabled (2026-11-17, the pod test 3 of
      // 3 with no ask delivered without it); the domain's other events (contexts, console) are dropped unread here
      call('Runtime.enable', {}, r.sessionId, (_, err1) => {
        if (err1) note('runtime-enable-failed', { target: info.targetId, error: JSON.stringify(err1) })
        call('Runtime.addBinding', { name: '__benchRead' }, r.sessionId, (_, err2) => {
          if (err2) emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, target: info.targetId,
            pid: null, why: `addBinding: ${JSON.stringify(err2)}` })
          else note('binding', { target: info.targetId })
        })
      })
    })
  }
  // One read, on the page's own session. It ends exactly once (`finish`); a trace still running when its read ran
  // out is ended before the next read starts (no trace of a read overlaps another)
  const readPage = (sid, targetId, ask, next) => {
    const t0 = Date.now() / 1000
    const row = { t0, udd: target.udd, target: targetId, ask: String(ask || '').split(':')[1] || null,
      session: null, class: null, pid: null }
    let finished = false
    let tracing = false
    const finish = (why) => {
      if (finished) return
      finished = true
      delete traces[sid]
      if (why) row.why = why
      row.pid = why ? null : row.pid
      if (why || row.t1 === undefined) row.t1 = Date.now() / 1000
      emit(row)
      if (!tracing || !why) return next()
      let done = false
      const go = () => { if (!done) { done = true; next() } }
      setTimeout(go, READ_MS)                         // an end that never answers: the read is still over
      call('Tracing.end', {}, sid, go)
    }
    setTimeout(() => finish(`no answer within ${READ_MS} ms`), READ_MS)
    call('Runtime.evaluate', { expression: 'JSON.stringify(window.__benchSession || null)', returnByValue: true },
      sid, (ev, err2) => {
        if (finished) return
        const mark = ev && ev.result && typeof ev.result.value === 'string' ? JSON.parse(ev.result.value) : null
        if (err2 || !mark) return finish('the page carries no session marker')
        row.session = String(mark.session)
        row.class = String(mark.class)
        traces[sid] = {
          done: (frames) => {
            tracing = false
            if (finished) return
            const main = (frames || []).filter((f) => f.frame === targetId && f.isOutermostMainFrame)
            if (main.length !== 1 || typeof main[0].processId !== 'number') {
              return finish(`the trace names ${main.length} outermost main frame(s) for the page`)
            }
            row.pid = main[0].processId
            row.t1 = Date.now() / 1000           // the read is over: the stamp below comes after it
            // the stamp is the read's START: a read that began after a load ended is the one after it
            call('Runtime.evaluate', { expression: `window.__benchMapped = ${Math.round(t0 * 1000)}`,
              returnByValue: true }, sid, () => finish(null))
          },
        }
        tracing = true
        call('Tracing.start', { traceConfig: { includedCategories: ['disabled-by-default-devtools.timeline'] },
          transferMode: 'ReportEvents' }, sid, (_, err3) => {
          if (finished) return
          if (err3) { tracing = false; return finish(`tracing: ${JSON.stringify(err3)}`) }
          call('Tracing.end', {}, sid, () => {})
        })
      })
  }
  // One read at a time per Chrome, in ask order; each ask is served once
  const served = {}                 // sessionId -> the last ask read
  const queue = []
  const next = () => {
    if (busy || !queue.length) return
    busy = true
    const [sid, ask] = queue.shift()
    readPage(sid, pages[sid], ask, () => { busy = false; next() })
  }
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data)
    if (m.method === 'Target.targetCreated') return onPage((m.params || {}).targetInfo)
    if (m.method === 'Target.detachedFromTarget') { delete pages[(m.params || {}).sessionId]; return }
    if (m.method === 'Runtime.bindingCalled' && m.params && m.params.name === '__benchRead' && pages[m.sessionId]) {
      const ask = String(m.params.payload || '')
      if (served[m.sessionId] === ask) return
      served[m.sessionId] = ask
      queue.push([m.sessionId, ask])
      return next()
    }
    onReply(e)
  }
  ws.onopen = () => {
    note('open')
    call('Target.setDiscoverTargets', { discover: true }, null, (r, err) => {
      if (err) emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, pid: null,
        why: `setDiscoverTargets: ${JSON.stringify(err)}` })
    })
  }
  ws.onerror = (e) => note('error', { error: String((e && e.error) || e) })
  ws.onclose = () => emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, pid: null, why: 'the CDP connection closed' })
}

export default function () {
  for (const t of TARGETS) watch(t)
}
