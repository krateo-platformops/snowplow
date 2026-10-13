// prewarm_pass_resume.go — #643 PR-C: the gvr-discovered discovery epoch and the
// resume (pass-done) set.
//
// WHY. Before #643 every discovered GVR enqueued its own gvr-discovered pass,
// and every pass ignored its GVR and re-ran the whole seed; a cut pass then
// restarted from unit zero. On 057: 51 GVRs, one full pass each, none ever
// finishing (pending_depth 41–59 for 29h).
//
// COALESCE (change 2). The hook bumps a monotonic discovery EPOCH and enqueues
// ONE payload-free gvr-discovered item; the workqueue folds N enqueues into one
// pending pass (one re-run if a pass is in flight). The epoch, not the queue
// item, is what records that new dep edges are needed.
//
// RESUME (change 3). A cut gvr-discovered pass is requeued; the next attempt
// skips a unit iff an earlier attempt of the SAME scope already re-resolved and
// re-Put it in THIS epoch, and that Put is provably still the cell's truth:
//
//  1. the record's epoch — read at the unit's RESOLVE START (#375
//     epoch-at-entry) — equals the current epoch: every GVR discovered up to
//     now was registered before that resolve began, so its dep edges are on the
//     cell (F4-C3 / I-3). A GVR discovered since makes every record stale.
//  2. the store's current entry under the key is the SAME *ResolvedEntry object
//     this pass Put (GetNoTouch applies the read path's TTL / max-age
//     liveness): not evicted, not expired, not re-Put by anyone else (a
//     refresher or /call resolve that may have started before the GVR).
//     Identity is the pointer AND the cell's putSeq read right after our Put.
//     No code mutates a stored entry's body or deps in place (audit in the PR;
//     pinned by TestIssue643_StoredEntryBodyIsNeverMutatedInPlace), and any
//     re-Put — even of the same pointer — takes a new putSeq.
//  3. cache.InputsMovedSincePut(key) is false: no dirty window is open AND the
//     cell is not KNOWN-STALE (#547: a refresh that ended without a Put leaves
//     the pointer unchanged while the inputs moved). Zero-cold: a resumed pass
//     never skips a unit whose inputs changed since the Put it would trust.
//
// Any doubt → the unit is resolved. Boot keeps its own fresh-skip; keepwarm its
// age-skip; rbac-shift its new-sub-gen liveness skip.
//
// LIFECYCLE. Engine-lived per scope key, reused across AddRateLimited requeues,
// cleared on genuine completion (err == nil and the epoch unchanged since the
// pass began) and on a config-vars redrive. If the epoch moved during a clean
// pass the set is kept — its records are stale anyway, and the coalesced re-run
// redoes every unit.

package dispatchers

import (
	"context"
	"expvar"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// passDoneRecord is one unit an attempt of the scope re-resolved and re-Put.
type passDoneRecord struct {
	epoch  uint64
	entry  *cache.ResolvedEntry
	putSeq uint64
}

// passDoneSet is the engine-lived resume set for one scope key, plus the epoch
// source it is judged against (the owning engine's discovery epoch).
type passDoneSet struct {
	epoch *atomic.Uint64
	mu    sync.Mutex
	m     map[string]passDoneRecord
	marks uint64 // monotonic accepted marks (C4 progress measure)
}

func newPassDoneSet(epoch *atomic.Uint64) *passDoneSet {
	return &passDoneSet{epoch: epoch, m: map[string]passDoneRecord{}}
}

func (s *passDoneSet) currentEpoch() uint64 {
	if s == nil || s.epoch == nil {
		return 0
	}
	return s.epoch.Load()
}

func (s *passDoneSet) mark(key string, epoch uint64, entry *cache.ResolvedEntry, putSeq uint64) {
	if s == nil || key == "" || entry == nil {
		return
	}
	s.mu.Lock()
	s.m[key] = passDoneRecord{epoch: epoch, entry: entry, putSeq: putSeq}
	s.marks++
	s.mu.Unlock()
}

// marksTotal is the monotonic count of accepted marks — a new key OR a
// replaced (redone) record. Every mark is an accepted Put of a fresh resolve,
// so its delta across an attempt is that attempt's PROGRESS (C4): a redo of a
// stale record is progress even though it adds no key.
func (s *passDoneSet) marksTotal() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marks
}

func (s *passDoneSet) lookup(key string) (passDoneRecord, bool) {
	if s == nil {
		return passDoneRecord{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.m[key]
	return r, ok
}

func (s *passDoneSet) len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

type ctxKeyPassDoneSetType struct{}

var ctxKeyPassDoneSet = ctxKeyPassDoneSetType{}

func withPassDoneSet(ctx context.Context, s *passDoneSet) context.Context {
	if ctx == nil || s == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyPassDoneSet, s)
}

func passDoneSetFromContext(ctx context.Context) *passDoneSet {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyPassDoneSet).(*passDoneSet)
	return s
}

// resolveStartEpoch carries the discovery epoch read at a unit's resolve start
// from the seed primitive's entry to its terminal Put.
type ctxKeyResolveStartEpochType struct{}

var ctxKeyResolveStartEpoch = ctxKeyResolveStartEpochType{}

// withResolveStartEpoch stamps the current epoch on ctx (no done set → no-op).
func withResolveStartEpoch(ctx context.Context) context.Context {
	s := passDoneSetFromContext(ctx)
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyResolveStartEpoch, s.currentEpoch())
}

// markPassDone records key as re-resolved and re-Put by this attempt, at the
// epoch its resolve started under. Called only after an accepted terminal Put.
func markPassDone(resCtx context.Context, key string, entry *cache.ResolvedEntry) {
	s := passDoneSetFromContext(resCtx)
	if s == nil {
		return
	}
	epoch, ok := resCtx.Value(ctxKeyResolveStartEpoch).(uint64)
	if !ok {
		return // no resolve-start epoch: never claim a unit done
	}
	// The putSeq of the cell AS our entry: read after the Put; if another Put
	// landed in between, the stored pointer is no longer ours and rule 2 fails
	// on the pointer anyway (nobody else holds our pointer to re-Put it).
	cur, live := cache.ResolvedCache().GetNoTouch(key)
	seq, seqOK := cache.ResolvedCache().CellPutSeq(key)
	if !live || cur != entry || !seqOK {
		return // not provably our Put: never claim the unit done
	}
	s.mark(key, epoch, entry, seq)
}

// passDoneSkip is the resume consult (rules 1–3 in the header). True iff key
// may be skipped by this attempt.
func passDoneSkip(ctx context.Context, handle cacheHandle, key string) bool {
	s := passDoneSetFromContext(ctx)
	r, ok := s.lookup(key)
	if !ok {
		return false
	}
	if !passDoneEpochRuleFn(r.epoch, s.currentEpoch()) {
		return false
	}
	if !passDoneSameEntryRuleFn(handle, key, r) {
		return false
	}
	if passDoneDirtyRuleFn(key) {
		return false
	}
	return true
}

// The three rules, as seams ONLY so the PR-C mutation arms can drop one at a
// time and prove each is load-bearing. Production never reassigns them.
var (
	passDoneEpochRuleFn     = func(recorded, current uint64) bool { return recorded == current }
	passDoneSameEntryRuleFn = func(handle cacheHandle, key string, r passDoneRecord) bool {
		cur, live := handle.GetNoTouch(key)
		if !live || cur == nil || cur != r.entry {
			return false
		}
		seq, ok := cache.ResolvedCache().CellPutSeq(key)
		return ok && seq == r.putSeq
	}
	passDoneDirtyRuleFn = cache.InputsMovedSincePut
)

// gvrDiscoveredLogCap bounds the GVR list a pass logs (the count is exact).
const gvrDiscoveredLogCap = 20

// onGVRDiscovered is the engine's GVR-discovered hook body: bump the discovery
// epoch BEFORE the enqueue (the epoch is what invalidates every resume record),
// note the GVR for the next pass's log, and enqueue ONE payload-free item — the
// workqueue folds N discoveries into one pending pass (one re-run if a pass is
// in flight). Non-blocking (the cache fires it synchronously).
func (e *prewarmEngine) onGVRDiscovered(gvr schema.GroupVersionResource) {
	e.gvrEpoch.Add(1)
	e.noteDiscoveredGVR(gvr)
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered})
}

// NoteGVRDiscoveredForTest drives the PROCESS engine's real hook body once —
// the epoch-gauge OTLP falsifier's way to move the series. Production MUST NOT
// use it.
func NoteGVRDiscoveredForTest(gvr schema.GroupVersionResource) {
	prewarmEngineSingleton().onGVRDiscovered(gvr)
}

// noteDiscoveredGVR records gvr for the next pass's log line.
func (e *prewarmEngine) noteDiscoveredGVR(gvr schema.GroupVersionResource) {
	e.gvrPendingMu.Lock()
	if e.gvrPending == nil {
		e.gvrPending = map[string]struct{}{}
	}
	e.gvrPending[gvr.String()] = struct{}{}
	e.gvrPendingMu.Unlock()
}

// drainDiscoveredGVRs takes the GVRs discovered since the last pass: the exact
// count and a capped, sorted list for the log.
func (e *prewarmEngine) drainDiscoveredGVRs() (int, []string) {
	e.gvrPendingMu.Lock()
	pending := e.gvrPending
	e.gvrPending = nil
	e.gvrPendingMu.Unlock()
	list := make([]string, 0, len(pending))
	for g := range pending {
		list = append(list, g)
	}
	sort.Strings(list)
	n := len(list)
	if len(list) > gvrDiscoveredLogCap {
		list = list[:gvrDiscoveredLogCap]
	}
	return n, list
}

// passDoneSetFor returns the engine-lived resume set for scope key, creating it
// on first use. Reused across the scope's requeues.
func (e *prewarmEngine) passDoneSetFor(key string) *passDoneSet {
	e.passDoneMu.Lock()
	defer e.passDoneMu.Unlock()
	if e.passDone == nil {
		e.passDone = map[string]*passDoneSet{}
	}
	s, ok := e.passDone[key]
	if !ok {
		s = newPassDoneSet(&e.gvrEpoch)
		e.passDone[key] = s
	}
	return s
}

// clearPassDoneSet drops scope key's resume set (genuine completion, redrive).
func (e *prewarmEngine) clearPassDoneSet(key string) {
	e.passDoneMu.Lock()
	delete(e.passDone, key)
	e.passDoneMu.Unlock()
}

// GVRDiscoveryEpoch is the process engine's discovery epoch (the OTLP gauge).
func GVRDiscoveryEpoch() uint64 { return prewarmEngineSingleton().gvrEpoch.Load() }

var gvrEpochExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	gvrEpochExpvarOnce.Do(func() {
		expvar.Publish("snowplow_gvr_discovery_epoch", expvar.Func(func() any { return GVRDiscoveryEpoch() }))
	})
}
