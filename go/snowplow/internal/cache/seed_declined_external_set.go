// seed_declined_external_set.go — the #132 F4b Lever A boot-scope
// "resolved-but-declined-external" marker set.
//
// THE DEFECT (docs/f4b-seed-overshoot-design-2026-07-14.md §3): the boot seed
// resolves ~9 external-backed "whale" widgets (observability/ClickHouse +
// marketplace, endpointRef≠None). Each resolve touches a genuine external
// endpoint, so the #102 GTTL-1 declineSeedPutOnError gate (correctly) DECLINES
// the handle.Put — an external-touch cell has no dep edge to invalidate it, so a
// warm entry would go stale silently. The cell is never warmed. On the NEXT
// resume pass, seedSkipDecision(seedModeBoot, ...) does handle.Get → MISS (never
// Put) → returns false → the whale is re-resolved from scratch, paying its full
// external round-trip again. GOTO decline, forever, every resume pass
// (search-results alone = ~42.5s/pass × 5 resume passes = ~210s of pure repeated
// external work the seed can never make progress on). This is the self-inflicted
// resume loop that keeps the boot scope requeuing.
//
// THE FIX (Lever A): give the seed a boot-scoped MEMORY that "this (widget,
// cohort) key was resolved and INTENTIONALLY declined (external) this boot." A
// resume pass consults the marker BEFORE handle.Get; a marked key is skipped
// (the cell is intentionally cold — re-resolving it every resume pass makes zero
// forward progress and only burns budget). The FIRST boot pass still resolves
// each external whale once (records nothing warm — the Put was going to be
// declined anyway; the cell is cold-on-first-/call regardless). Lever A changes
// ONLY whether the seed WASTES budget re-resolving a cell it will decline again.
// It never warms an un-invalidatable cell — the #102 decline is fully preserved.
//
// WHY A CONTEXT-CARRIED SET (not a process-global) — mirrors SeedResolveMemo /
// StageErrorSink / ExternalTouchedSink (the established per-seed-pass /
// per-resolve sink idiom):
//
//   - LIFETIME (#643 PR-B — SUPERSEDES the original per-boot-scope teardown):
//     there is ONE engine-wide, process-lived set (EngineDeclinedExternalSet,
//     declined_external_engine.go), installed by the engine's processScope on
//     EVERY scope kind (boot, gvr-discovered, keepwarm, rbac-shift,
//     learned-class) and no longer torn down when boot completes — background
//     passes need boot's marks, and their absence is what made every pass
//     re-dial every external key. A config-vars redrive still clears it. Its
//     marks retire themselves when the fact they recorded stops holding.
//     REQUEST HALF of C-F4B-3, unchanged (I-6): a user /call context and the
//     refresher never carry a set, so SeedDeclinedExternalSetFromContext returns
//     nil there and Marked() is always false on the request path — the /call
//     path re-resolves the external widget fresh, unaffected.
//
//   - KEY = THE FULL dispatchCacheLookupKey (C-F4B-2): the marker is keyed by the
//     EXACT `key` the Put/Get pair uses — the cohort-identity-folding
//     dispatchCacheLookupKey, NEVER per-(class,target). Cohort A resolving+
//     declining widget W marks A's key; cohort B's FIRST encounter of W derives a
//     DIFFERENT key (distinct RBAC identity) → NOT marked → B still resolves. A
//     per-target marker would be a cross-cohort correctness bug (B would skip a
//     widget it never resolved). Single-derivation: the caller passes the same
//     `key` seedSkipDecision consults and declineSeedPutOnError marks, so
//     marker-key correctness cannot drift from Put-key correctness
//     (feedback_consultation_mutation_is_not_key_correctness).
//
// TOGGLE (C-F4B / C-F4-8 pattern): the set is only ever installed on an engine
// scope context, which only exists when the prewarm engine runs, which only runs under
// ResolvedCacheEnabled() / the prewarm gate. A cache-off process never installs a
// set → SeedDeclinedExternalSetFromContext is nil everywhere → zero behavior
// change. WithSeedDeclinedExternalSet additionally no-ops the install under
// Disabled().

package cache

import (
	"context"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ctxKeySeedDeclinedExternalSetType is the typed empty-struct context key for
// the boot-scope declined-external marker set. Distinct unexported type — no
// cross-package raw-string-key collision (mirrors ctxKeySeedResolveMemoType).
type ctxKeySeedDeclinedExternalSetType struct{}

var ctxKeySeedDeclinedExternalSet = ctxKeySeedDeclinedExternalSetType{}

// SeedDeclinedExternalSet records the FULL cache keys the boot seed
// resolved-and-declined for an EXTERNAL touch WITHIN A SINGLE BOOT SCOPE. A
// resume pass consults it (Marked) before its handle.Get so a whale the same
// boot already resolved-and-declined is not re-resolved on the resume pass.
// Concurrency-safe (sync.Map): the seed fans widgets across cohort goroutines.
//
// Membership is keyed by the exact dispatchCacheLookupKey the Put/Get pair uses,
// so cohort identity is encoded in the key — a marker set by cohort A can never
// suppress cohort B's first resolve of the same widget (C-F4B-2).
type SeedDeclinedExternalSet struct {
	// key string -> *DeclinedExternalMark (#643: the facts the mark was taken
	// under, which Marked's caller re-checks; an empty mark from the plain Mark
	// carries none).
	m sync.Map

	marks uint64
	mu    sync.Mutex // guards marks (diagnostic counter only)

	// size is the number of keys currently held (#643: the
	// snowplow_prewarm_declined_external_marks gauge — the set is engine-wide
	// and process-lived, so its growth must be a reading).
	size atomic.Int64
}

// Mark records key as resolved-and-declined-external with no recorded facts.
// nil-receiver-safe (no-op) so an uninstalled call site never marks. Idempotent:
// a second Mark of the same key is a harmless no-op (LoadOrStore).
func (s *SeedDeclinedExternalSet) Mark(key string) {
	s.MarkWith(key, &DeclinedExternalMark{})
}

// MarkWith records key with the facts it was taken under (#643). A re-mark of a
// held key REPLACES its facts (the newer resolve's RA version, deps and stamp
// are the ones that hold now) without counting a new distinct key.
func (s *SeedDeclinedExternalSet) MarkWith(key string, mark *DeclinedExternalMark) {
	if s == nil || key == "" {
		return
	}
	if mark == nil {
		mark = &DeclinedExternalMark{}
	}
	if _, loaded := s.m.Swap(key, mark); !loaded {
		s.size.Add(1)
		s.mu.Lock()
		s.marks++
		s.mu.Unlock()
	}
}

// Lookup returns the mark held for key (#643). nil-receiver-safe.
func (s *SeedDeclinedExternalSet) Lookup(key string) (*DeclinedExternalMark, bool) {
	if s == nil || key == "" {
		return nil, false
	}
	v, ok := s.m.Load(key)
	if !ok {
		return nil, false
	}
	m, _ := v.(*DeclinedExternalMark)
	if m == nil {
		m = &DeclinedExternalMark{}
	}
	return m, true
}

// Unmark retires key's mark. Reports whether a mark was held. nil-receiver-safe.
func (s *SeedDeclinedExternalSet) Unmark(key string) bool {
	if s == nil || key == "" {
		return false
	}
	if _, loaded := s.m.LoadAndDelete(key); loaded {
		s.size.Add(-1)
		return true
	}
	return false
}

// Len is the number of keys currently marked. nil-receiver-safe.
func (s *SeedDeclinedExternalSet) Len() int64 {
	if s == nil {
		return 0
	}
	return s.size.Load()
}

// Reset drops every mark and zeroes the cumulative distinct-mark count (a
// config-vars redrive starts a new topology; tests). nil-receiver-safe.
func (s *SeedDeclinedExternalSet) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.marks = 0
	s.mu.Unlock()
	s.m.Range(func(k, _ any) bool {
		if _, loaded := s.m.LoadAndDelete(k); loaded {
			s.size.Add(-1)
		}
		return true
	})
}

// ForgetCoordinate retires every mark taken for the widget or RESTAction at
// (gvr, namespace, name) — the #643 GONE retire rule. Returns how many it
// dropped. A Range over the set: GONE is rare and the set is small.
func (s *SeedDeclinedExternalSet) ForgetCoordinate(gvr schema.GroupVersionResource, namespace, name string) int {
	if s == nil {
		return 0
	}
	coord := DepKey{GVR: gvr, Namespace: namespace, Name: name}
	n := 0
	s.m.Range(func(k, v any) bool {
		m, _ := v.(*DeclinedExternalMark)
		if m == nil || (m.Widget != coord && m.RA != coord) {
			return true
		}
		if _, loaded := s.m.LoadAndDelete(k); loaded {
			s.size.Add(-1)
			n++
		}
		return true
	})
	return n
}

// Marked reports whether key was already resolved-and-declined-external this
// boot scope. nil-receiver-safe: (nil).Marked is always false so a call site
// with no set installed behaves as "never skip on this account" (the resume pass
// falls through to the normal handle.Get liveness check).
func (s *SeedDeclinedExternalSet) Marked(key string) bool {
	if s == nil || key == "" {
		return false
	}
	_, ok := s.m.Load(key)
	return ok
}

// Marks returns the count of distinct keys marked (diagnostic — feeds the
// phase1.seed.skip.declined_external falsifier/summary line). nil-receiver-safe.
func (s *SeedDeclinedExternalSet) Marks() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marks
}

// NewSeedDeclinedExternalSet builds an empty declined-external marker set.
func NewSeedDeclinedExternalSet() *SeedDeclinedExternalSet {
	return &SeedDeclinedExternalSet{}
}

// WithSeedDeclinedExternalSet returns a child context carrying set. Installed
// ONLY by the prewarm engine's processScope, on every scope kind (#643), and by
// the pre-engine phase1 boot walk (#643 PR-B2, resolveNavigationRoot) — a user
// /call and the refresher never install one, so the set is a strict no-op on the
// request path (C-F4B-3's request half, I-6). Inert under Disabled(): returns
// ctx unchanged so a cache-off process carries no set.
func WithSeedDeclinedExternalSet(ctx context.Context, set *SeedDeclinedExternalSet) context.Context {
	if ctx == nil || set == nil || Disabled() {
		return ctx
	}
	return context.WithValue(ctx, ctxKeySeedDeclinedExternalSet, set)
}

// SeedDeclinedExternalSetFromContext returns the *SeedDeclinedExternalSet
// installed on ctx, or nil when none is present. A nil return MUST be treated as
// "no set — never skip on this account" (the methods are nil-receiver-safe). Off
// the engine's scope contexts this is always nil, which is what makes the marker
// provably untouched on the /call path and the refresher path.
func SeedDeclinedExternalSetFromContext(ctx context.Context) *SeedDeclinedExternalSet {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeySeedDeclinedExternalSet).(*SeedDeclinedExternalSet)
	return v
}
