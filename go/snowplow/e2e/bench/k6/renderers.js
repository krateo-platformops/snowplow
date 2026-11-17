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
// Every MAP_S seconds, for each Chrome (CDP_TARGETS: [{udd, url}]) and each of its page targets: attach (a flat
// session), read the page's own marker (window.__benchSession, set by browser.js's init script: its session and
// identity class), start and at once end a trace, take the outermost main frame's processId, stamp the page
// (window.__benchMapped: browser.js waits for a read before a class's sign-in and after its last load), detach. One line
// per page and poll on stdout (--console-output): {"kind": "renderer_map", "t0", "t1" (epoch s: the read lies
// between them), "udd", "target", "session", "class", "pid"}; a page whose marker or frame cannot be read is a line
// with pid null and `why`. Nothing here is a guess: a load the evaluator cannot place between two reads of the
// same pid has no renderer, and its stall is UNKNOWN.
import { WebSocket } from 'k6/websockets'

const TARGETS = JSON.parse(__ENV.CDP_TARGETS || '[]')
const MAP_S = Number(__ENV.MAP_S || 5)
// one page's read (attach, marker, trace, stamp, detach): past it, a line with its why (k6_job.RENDERER_READ_MS)
const READ_MS = Number(__ENV.READ_MS || 10000)

export const options = { scenarios: { map: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: '1000h' } } }

function emit (o) { console.log(JSON.stringify(Object.assign({ kind: 'renderer_map' }, o))) }

// One Chrome: a websocket, its pending calls by id, and the pages of the current poll read one after another
function watch (target) {
  const ws = new WebSocket(target.url)
  let id = 0
  const waiting = {}                // id -> callback(result, error)
  const traces = {}                 // sessionId -> callback(frames)
  const call = (method, params, sessionId, cb) => {
    const n = ++id
    waiting[n] = cb
    ws.send(JSON.stringify(Object.assign({ id: n, method, params: params || {} }, sessionId ? { sessionId } : {})))
  }
  let busy = false
  ws.onmessage = (e) => {
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
  // One page's read. It ends exactly once (`finish`), and the next page is read only once this page's session is
  // DETACHED (detaching ends its trace, so no trace of a read that ran out of time overlaps the next read); a session
  // attached after the read already ended (its attach answered late) is detached as soon as it is known
  const readPage = (info, next) => {
    const t0 = Date.now() / 1000
    const row = { t0, udd: target.udd, target: info.targetId, session: null, class: null, pid: null }
    let finished = false
    let sid = null
    const detachThen = (s, then) => {
      if (!s) return then()
      let done = false
      const go = () => { if (!done) { done = true; then() } }
      setTimeout(go, READ_MS)                         // a detach that never answers: the read is still over
      call('Target.detachFromTarget', { sessionId: s }, null, go)
    }
    const finish = (why) => {
      if (finished) return
      finished = true
      if (sid) delete traces[sid]
      if (why) row.why = why
      row.pid = why ? null : row.pid
      if (why || row.t1 === undefined) row.t1 = Date.now() / 1000
      emit(row)
      detachThen(sid, next)
    }
    setTimeout(() => finish(`no answer within ${READ_MS} ms`), READ_MS)
    call('Target.attachToTarget', { targetId: info.targetId, flatten: true }, null, (r, err) => {
      if (finished) {                                  // answered after the read ran out: detach it, nothing else
        if (r && r.sessionId) call('Target.detachFromTarget', { sessionId: r.sessionId }, null, () => {})
        return
      }
      if (err || !r) return finish(`attach: ${JSON.stringify(err)}`)
      sid = r.sessionId
      call('Runtime.evaluate', { expression: 'JSON.stringify(window.__benchSession || null)', returnByValue: true },
        sid, (ev, err2) => {
          if (finished) return
          const mark = ev && ev.result && typeof ev.result.value === 'string' ? JSON.parse(ev.result.value) : null
          if (err2 || !mark) return finish('the page carries no session marker')
          row.session = String(mark.session)
          row.class = String(mark.class)
          traces[sid] = {
            done: (frames) => {
              if (finished) return
              const main = (frames || []).filter((f) => f.frame === info.targetId && f.isOutermostMainFrame)
              if (main.length !== 1 || typeof main[0].processId !== 'number') {
                return finish(`the trace names ${main.length} outermost main frame(s) for the page`)
              }
              row.pid = main[0].processId
              row.t1 = Date.now() / 1000           // the read is over: the stamp below comes after it
              // the page learns it was read (browser.js waits for a read before its class's sign-in and after its
              // class's last load: every load has a read on both sides)
              // the stamp is the read's START: a read that began after a load ended is the one after it
              call('Runtime.evaluate', { expression: `window.__benchMapped = ${Math.round(t0 * 1000)}`, returnByValue: true }, sid,
                () => finish(null))
            },
          }
          call('Tracing.start', { traceConfig: { includedCategories: ['disabled-by-default-devtools.timeline'] },
            transferMode: 'ReportEvents' }, sid, (_, err3) => {
            if (finished) return
            if (err3) return finish(`tracing: ${JSON.stringify(err3)}`)
            call('Tracing.end', {}, sid, () => {})
          })
        })
    })
  }
  const poll = () => {
    if (busy) return                // the previous poll still reads: never two traces at once
    busy = true
    call('Target.getTargets', {}, null, (r, err) => {
      if (err || !r) {
        emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, pid: null, why: 'getTargets failed' })
        busy = false
        return
      }
      const pages = r.targetInfos.filter((x) => x.type === 'page')
      const step = (i) => {
        if (i >= pages.length) { busy = false; return }
        readPage(pages[i], () => step(i + 1))
      }
      step(0)
    })
  }
  ws.onopen = () => { poll(); setInterval(poll, MAP_S * 1000) }
  ws.onclose = () => emit({ t0: Date.now() / 1000, t1: Date.now() / 1000, udd: target.udd, pid: null, why: 'the CDP connection closed' })
}

export default function () {
  for (const t of TARGETS) watch(t)
}
