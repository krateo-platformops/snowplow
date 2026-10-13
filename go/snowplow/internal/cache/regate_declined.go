// regate_declined.go — #677: the refresher's KNOWN-DECLINED mark for a cell
// whose every refresh fails ONLY on a correct per-identity re-gate deny.
//
// THE DEFECT (#677, measured on krateo-057 1.12.46). A background refresh
// re-resolves an identity-bound cell under its cohort's representative with
// snowplow's SA transport. Branch C (api/internal_dispatch.go) re-gates each
// GET-by-name for that identity; a cohort that is correctly denied one step
// (a SubjectAccessReview per identity agreed with the in-process evaluator on
// 1,076 of 1,076 pairs) gets a synthetic Forbidden, the stage-error sink
// fires, and the re-Put is declined. Nothing remembered that the decline is
// DETERMINISTIC: every list_dep dirty mark on any OTHER step of the same
// RESTAction (compositions, incidents, ...) re-ran the whole resolve and the
// same deny, ~117/min from one pod, for a result that can never be Put.
//
// THE MARK. When a refresher re-resolve declines and EVERY stage error it
// recorded is a per-identity re-gate deny (RegateDenySink count == stage-error
// count, no external touch), the key is marked. While the mark holds, the
// refresher skips the re-resolve and returns without a Put, so the dequeue
// still ends KNOWN-STALE (#547 bookkeeping, refresher.settleDequeue): the cell
// is served and counted, and revalidated by the next read under the REQUESTER
// (that demand path never consults this set).
//
// WHAT RETIRES IT. A deny can only change when one of these changes, and each
// retires the mark (any doubt resolves toward resolving):
//
//	(a) RBAC — the published RBAC snapshot sequence (RBACGen) moved since the
//	    value read BEFORE the resolve. Every published rebuild bumps it,
//	    including a Role / ClusterRole RULE edit, which the per-subject
//	    sub-generation does not see. A publish that landed mid-resolve also
//	    retires (the deny may have been evaluated on the older snapshot).
//	(b) definition — the uid + resourceVersion of the cell's own CR and, for a
//	    widget, its apiRef RESTAction, read informer-only at mark and at
//	    consult. Unreadable at consult retires; unreadable at mark never marks.
//	(c) GONE — any recorded definition coordinate deleted (notifyObjectGone).
//	(d) Put — ANY accepted L1 Put of the key, at putCoreLocked, the same
//	    chokepoint #656's declined-external Put-unmark uses.
//	(e) evicted — the key left the store (the mark must not outlive its key).
//
// SCOPE. Refresher only. A /call, the demand revalidation, the cohort seed and
// the prewarm engine never consult or set it. It is a separate set from #656's
// declined-external set; neither consult reads the other's.
//
// DOCUMENTED LIMIT. A stage whose call path is DATA-dependent (an iterator over
// a list) could stop issuing the denied call when its data changes. Such a
// change does not retire the mark; the cell stays known-stale until the next
// read revalidates it under the requester, a Put, or one of (a)-(c). That is
// the #547 posture this cell already had, since its refresh was declined
// anyway, so it introduces no new staleness window over today's behaviour
// except that a refresh that WOULD now succeed waits for a read.

package cache

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- the per-resolve re-gate deny sink ------------------------------------

type ctxKeyRegateDenySinkType struct{}

var ctxKeyRegateDenySink = ctxKeyRegateDenySinkType{}

// RegateDenySink counts the stage errors of one resolve that were a
// per-identity re-gate deny (api branch C). It is bumped at the SAME site, and
// for the same item, as the StageErrorSink bump, so Count()==stage Count()
// means "every stage error was a re-gate deny".
type RegateDenySink struct{ n atomic.Int64 }

// Bump records one re-gate deny. nil-receiver-safe.
func (s *RegateDenySink) Bump() {
	if s != nil {
		s.n.Add(1)
	}
}

// Count returns the number of re-gate denies recorded. nil-receiver-safe.
func (s *RegateDenySink) Count() int64 {
	if s == nil {
		return 0
	}
	return s.n.Load()
}

// WithRegateDenySink installs a fresh sink on ctx. Installed ONLY by the
// refresher (resolveAndPopulateL1).
func WithRegateDenySink(ctx context.Context) (context.Context, *RegateDenySink) {
	s := &RegateDenySink{}
	if ctx == nil {
		return ctx, s
	}
	return context.WithValue(ctx, ctxKeyRegateDenySink, s), s
}

// RegateDenySinkFromContext returns the sink on ctx, or nil.
func RegateDenySinkFromContext(ctx context.Context) *RegateDenySink {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyRegateDenySink).(*RegateDenySink)
	return s
}

// --- the mark ---------------------------------------------------------------

// DefinitionVersion is one definition object a mark depends on, with the
// identity and version read at mark time.
type DefinitionVersion struct {
	Key             DepKey
	UID             string
	ResourceVersion string
}

// RegateDeclinedMark is the set of facts a known-declined mark holds under.
type RegateDeclinedMark struct {
	// RBACPublishSeq is RBACGen() read BEFORE the resolve that declined.
	RBACPublishSeq uint64
	// Defs are the cell's own CR and, for a widget, its apiRef RESTAction.
	Defs []DefinitionVersion
}

var (
	regateDeclined     sync.Map // l1Key -> *RegateDeclinedMark
	regateDeclinedSize atomic.Int64

	regateDeclinedSetTotal  atomic.Uint64
	regateDeclinedSkipTotal atomic.Uint64
)

// MarkRegateDeclined records key as known-declined under m. A nil mark is
// refused (nothing to re-check means nothing to hold).
func MarkRegateDeclined(key string, m *RegateDeclinedMark) bool {
	if key == "" || m == nil {
		return false
	}
	if _, loaded := regateDeclined.Swap(key, m); !loaded {
		regateDeclinedSize.Add(1)
	}
	regateDeclinedSetTotal.Add(1)
	return true
}

// LookupRegateDeclined returns key's mark.
func LookupRegateDeclined(key string) (*RegateDeclinedMark, bool) {
	if key == "" {
		return nil, false
	}
	v, ok := regateDeclined.Load(key)
	if !ok {
		return nil, false
	}
	m, _ := v.(*RegateDeclinedMark)
	return m, true
}

// RetireRegateDeclined drops key's mark and counts it under reason. Returns
// whether a mark was held.
func RetireRegateDeclined(key, reason string) bool {
	if key == "" {
		return false
	}
	if _, loaded := regateDeclined.LoadAndDelete(key); loaded {
		regateDeclinedSize.Add(-1)
		RecordRegateDeclinedRetired(reason, 1)
		return true
	}
	return false
}

// NoteRegateDeclinedSkip counts one refresher re-resolve skipped on a held mark.
func NoteRegateDeclinedSkip() { regateDeclinedSkipTotal.Add(1) }

// unmarkRegateDeclinedOnPut is retire rule (d), called from putCoreLocked for
// every accepted Put (every variant reaches it).
func unmarkRegateDeclinedOnPut(key string) {
	RetireRegateDeclined(key, RegateDeclinedRetiredPutUnmark)
}

// forgetRegateDeclinedOnEvict is retire rule (e), called from both eviction
// bodies (removeElementLocked and deleteForDep).
func forgetRegateDeclinedOnEvict(key string) {
	RetireRegateDeclined(key, RegateDeclinedRetiredEvicted)
}

// forgetRegateDeclinedCoordinate is retire rule (c): every mark that recorded
// the deleted coordinate as a definition.
func forgetRegateDeclinedCoordinate(gvr schema.GroupVersionResource, namespace, name string) {
	coord := DepKey{GVR: gvr, Namespace: namespace, Name: name}
	regateDeclined.Range(func(k, v any) bool {
		m, _ := v.(*RegateDeclinedMark)
		hit := m == nil
		if m != nil {
			for _, d := range m.Defs {
				if d.Key == coord {
					hit = true
					break
				}
			}
		}
		if hit {
			RetireRegateDeclined(k.(string), RegateDeclinedRetiredGone)
		}
		return true
	})
}

// RegateDeclinedLen is the number of marks currently held.
func RegateDeclinedLen() int64 { return regateDeclinedSize.Load() }

// RegateDeclinedSetTotal / RegateDeclinedSkipTotal are the two counters.
func RegateDeclinedSetTotal() uint64  { return regateDeclinedSetTotal.Load() }
func RegateDeclinedSkipTotal() uint64 { return regateDeclinedSkipTotal.Load() }

// The retire reasons — a CLOSED set, every cell pre-populated.
const (
	RegateDeclinedRetiredNilMark              = "nil_mark"
	RegateDeclinedRetiredRBACPublished        = "rbac_published"
	RegateDeclinedRetiredDefinitionUnreadable = "definition_unreadable"
	RegateDeclinedRetiredDefinitionChanged    = "definition_changed"
	RegateDeclinedRetiredPutUnmark            = "put_unmark"
	RegateDeclinedRetiredGone                 = "gone"
	RegateDeclinedRetiredEvicted              = "evicted"
)

// RegateDeclinedRetiredReasons is the closed reason set, in emit order.
var RegateDeclinedRetiredReasons = []string{
	RegateDeclinedRetiredNilMark,
	RegateDeclinedRetiredRBACPublished,
	RegateDeclinedRetiredDefinitionUnreadable,
	RegateDeclinedRetiredDefinitionChanged,
	RegateDeclinedRetiredPutUnmark,
	RegateDeclinedRetiredGone,
	RegateDeclinedRetiredEvicted,
}

var (
	regateRetiredMu sync.Mutex
	regateRetired   = newRegateRetired()
)

func newRegateRetired() map[string]uint64 {
	m := make(map[string]uint64, len(RegateDeclinedRetiredReasons))
	for _, r := range RegateDeclinedRetiredReasons {
		m[r] = 0
	}
	return m
}

// RecordRegateDeclinedRetired counts n retires under reason. A reason outside
// the closed set is dropped.
func RecordRegateDeclinedRetired(reason string, n int) {
	if n <= 0 {
		return
	}
	regateRetiredMu.Lock()
	if _, ok := regateRetired[reason]; ok {
		regateRetired[reason] += uint64(n)
	}
	regateRetiredMu.Unlock()
}

// RegateDeclinedRetiredCount is one {reason} cell.
type RegateDeclinedRetiredCount struct {
	Reason string
	Count  uint64
}

// RegateDeclinedRetiredSnapshot returns every reason cell, zeros included.
func RegateDeclinedRetiredSnapshot() []RegateDeclinedRetiredCount {
	regateRetiredMu.Lock()
	defer regateRetiredMu.Unlock()
	out := make([]RegateDeclinedRetiredCount, 0, len(RegateDeclinedRetiredReasons))
	for _, r := range RegateDeclinedRetiredReasons {
		out = append(out, RegateDeclinedRetiredCount{Reason: r, Count: regateRetired[r]})
	}
	return out
}

// ResetRegateDeclinedForTest empties the set and zeroes every counter. Tests only.
func ResetRegateDeclinedForTest() {
	regateDeclined.Range(func(k, _ any) bool { regateDeclined.Delete(k); return true })
	regateDeclinedSize.Store(0)
	regateDeclinedSetTotal.Store(0)
	regateDeclinedSkipTotal.Store(0)
	regateRetiredMu.Lock()
	regateRetired = newRegateRetired()
	regateRetiredMu.Unlock()
}

// CFG-1: published only with the cache on.
func init() {
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_refresher_regate_declined_marks", expvar.Func(func() any {
		return RegateDeclinedLen()
	}))
	expvar.Publish("snowplow_refresher_regate_declined_set_total", expvar.Func(func() any {
		return RegateDeclinedSetTotal()
	}))
	expvar.Publish("snowplow_refresher_regate_declined_skip_total", expvar.Func(func() any {
		return RegateDeclinedSkipTotal()
	}))
	expvar.Publish("snowplow_refresher_regate_declined_retired_total", expvar.Func(func() any {
		m := map[string]uint64{}
		for _, c := range RegateDeclinedRetiredSnapshot() {
			m[c.Reason] = c.Count
		}
		return m
	}))
}
