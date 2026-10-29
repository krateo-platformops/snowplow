// stale_serve.go — #710 PR-2b: the cache-side seams of the D2 stale serve.
//
// Owner ruling (#710, issuecomment-6187374111): a customer hit on a
// KNOWN-STALE, identity-free content cell inside an ARMABLE customer resolve
// is served at once — stale — and its refill starts at once at top priority;
// when the fresh value lands the dependent customer cell is re-resolved and
// the armed tab pushed ("show then replace"). The content layer
// (restactions/api, apistage_stale_debt.go) owns the decision and the debt;
// this file holds what it needs from the cache package:
//
//   - StaleServeSink: the per-resolve carrier (like StageErrorSink). A
//     customer dispatcher whose response is ARMABLE (restactions, widgets)
//     installs it with its own L1 key and class; its presence is what makes a
//     resolve eligible, and the content layer notes on it every content cell
//     it served stale. The dispatcher reads it at its Put (StaleDerived).
//   - KnownStaleMark: a known-stale cell's mark seq — the epoch a refill must
//     start at or after to close the window (the closeKnownStaleOnPut rule).
//   - RunRefreshNow: re-resolve one L1 key at once through its class's
//     registered refresh handler, OUTSIDE the refresher queue — the debt
//     lane's dependent re-resolve, which must not wait behind background work
//     nor be dropped by its poison-pill bound.
//   - The write-point guarantee (#710 ruling on the unarmed residual,
//     issuecomment-6195582848): a stale-derived body is written ONLY behind an
//     accepted Put + the armed refresh header + an OWED-PUSH mark. The
//     dispatcher COMMITS the sink after its accepted Put, or WITHDRAWS it (and
//     re-runs the resolve with D2 disabled) when the Put was declined. The
//     owed mark carries the push to a tab that arms after the replacement was
//     published (/refreshes has no replay): SubscribeRefreshStream emits it at
//     arm time once the cell is no longer stale-derived (replaced or evicted).
package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// StaleServeSink is one armable customer resolve's stale-serve carrier.
type StaleServeSink struct {
	l1Key string
	class string
	mu    sync.Mutex
	// served is the content cells this resolve was served stale. Their reads
	// were metric-neutral; Commit counts them as the request's hits.
	served []staleServedRef
	// state is the dependent's write-point outcome; replaceOnCommit records a
	// content debt that cleared before the dispatcher committed (its Put comes
	// after the response is encoded, which can be after a fast refill).
	state           staleSinkState
	replaceOnCommit bool
	servedAt        time.Time
	// wrote (#905): a write stage of this resolve was sent. From then on no read
	// of it is served stale (NoteIfNoWrite refuses, under the lock BeginWrite
	// takes), so it can never be withdrawn and re-run, which would send the
	// write a second time.
	wrote bool
	// declaredWrite (#905, #913 re-review): a RESTAction resolved for this
	// request DECLARES a write stage. It is known from the spec before any stage
	// runs, so the request is never served stale from then on: a top-level
	// write RESTAction is never stale-derived at all, and the hold / withdraw /
	// re-run machinery only ever applies to a write first seen inside a nested
	// resolve, after a read was already served stale.
	declaredWrite bool
}

type staleServedRef struct {
	key string
}

type staleSinkState uint8

const (
	staleSinkPending   staleSinkState = iota // resolve running, not yet written
	staleSinkCommitted                       // accepted Put + armed header: replace + push
	staleSinkWithdrawn                       // Put declined: never written stale; re-run fresh
)

type ctxKeyStaleServeSinkType struct{}

var ctxKeyStaleServeSink = ctxKeyStaleServeSinkType{}

type ctxKeyStaleServeOffType struct{}

var ctxKeyStaleServeOff = ctxKeyStaleServeOffType{}

// WithStaleServeDisabled marks ctx as the write-point RE-RUN of a withdrawn
// stale serve: WithStaleServeSink installs nothing under it, so every content
// read reads through (#547) and the re-run can never re-enter D2.
func WithStaleServeDisabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyStaleServeOff, true)
}

// WithCustomerTriggeredRefresh marks ctx as refresh work a CUSTOMER started
// and is waiting on — a D2 debt's dependent re-resolve, a #547 demand
// revalidation. It runs through the refresher's resolve (which stamps the
// refresher origin, so every rule keyed on that origin would class it as
// background and queue it under pressure), but the owner ruling puts it "at
// top priority, ahead of all background work": the content-fill gate and the
// nested-resolve gate check this marker FIRST, before the origin rule.
func WithCustomerTriggeredRefresh(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyCustomerTriggeredRefresh, true)
}

// CustomerTriggeredRefreshFromContext reports WithCustomerTriggeredRefresh.
func CustomerTriggeredRefreshFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyCustomerTriggeredRefresh).(bool)
	return v
}

type ctxKeyCustomerTriggeredRefreshType struct{}

var ctxKeyCustomerTriggeredRefresh = ctxKeyCustomerTriggeredRefreshType{}

// StaleServeDisabled reports whether ctx is a withdrawn serve's re-run.
func StaleServeDisabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	off, _ := ctx.Value(ctxKeyStaleServeOff).(bool)
	return off
}

// WithStaleServeSink marks ctx as an ARMABLE customer resolve producing l1Key
// of class (restactions, widgets) — eligible for the D2 stale serve — and
// returns the sink its content reads note stale serves on. Install it ONLY on
// a customer dispatch path whose stored cell is armed for /refreshes; never on
// a background, inert or identity-free resolve.
func WithStaleServeSink(ctx context.Context, l1Key, class string) (context.Context, *StaleServeSink) {
	if ctx == nil || l1Key == "" || StaleServeDisabled(ctx) {
		return ctx, nil
	}
	s := &StaleServeSink{l1Key: l1Key, class: class}
	return context.WithValue(ctx, ctxKeyStaleServeSink, s), s
}

// StaleServeSinkFromContext returns ctx's sink, or nil. A sink is in scope only
// while the resolve still fills the sink's OWN cell: a nested resolve that
// re-scopes the L1 key (WithL1KeyContext*) fills an intermediate SUBSTRATE cell
// (the identity-bound RAFullList, a nested cached RESTAction). That cell is read
// as input by later resolves — including the debt's own dependent re-resolve —
// so a stale body baked into it would look fresh and the replace would serve
// the stale value again. Its reads therefore read through (#547), never D2.
func StaleServeSinkFromContext(ctx context.Context) *StaleServeSink {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyStaleServeSink).(*StaleServeSink)
	if s == nil || L1KeyFromContext(ctx) != s.l1Key {
		return nil
	}
	return s
}

// StaleServeSinkForWrite (#905) is the customer request's sink WHATEVER L1 key
// the resolve is filling now. A write anywhere under the request — in a nested
// resolve that re-scoped to its own cell included — is sent or held for the
// request as a whole, because it is the request that is withdrawn and re-run.
func StaleServeSinkForWrite(ctx context.Context) *StaleServeSink {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyStaleServeSink).(*StaleServeSink)
	return s
}

// NoteIfNoWrite (#905) is Note, atomic with BeginWrite: it records the stale
// serve of contentKey and returns true, unless a write of this resolve was
// already sent, in which case it records nothing and returns false (the read
// must read through). Under the sink's lock, so a stale read and a write
// running in parallel (nested resolve:true items of one stage) can never both
// happen: either the stale serve is noted first and the write is held, or the
// write is begun first and the read is not served stale. Nil-safe (true).
func (s *StaleServeSink) NoteIfNoWrite(contentKey string) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wrote {
		return false
	}
	s.served = append(s.served, staleServedRef{key: contentKey})
	return true
}

// L1Key is the dependent customer cell's key. Nil-safe.
func (s *StaleServeSink) L1Key() string {
	if s == nil {
		return ""
	}
	return s.l1Key
}

// Class is the dependent customer cell's class. Nil-safe.
func (s *StaleServeSink) Class() string {
	if s == nil {
		return ""
	}
	return s.class
}

// Note records that this resolve was served contentKey stale. The read itself
// was metric-neutral (a stale serve may still be withdrawn at the write point,
// and a withdrawn pass must not count): Commit counts it. Nil-safe.
func (s *StaleServeSink) Note(contentKey string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.served = append(s.served, staleServedRef{key: contentKey})
	s.mu.Unlock()
}

// StaleDerived reports whether this resolve was served any content stale —
// the customer cell it Puts is then StaleDerived. Nil-safe.
func (s *StaleServeSink) StaleDerived() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.served) > 0
}

// BeginWrite (#905) is asked before a write stage item of this resolve is
// sent. A customer resolve that is stale-derived (D2) is withdrawn and re-run
// at its write point whenever its Put is declined — and a sent write always
// declines it (an external touch) — so a write sent from it would be sent again
// by the re-run. BeginWrite therefore answers:
//   - false when the resolve is already stale-derived: the write must not be
//     sent from this pass (its re-run, with D2 disabled, sends it once);
//   - true otherwise, and records that a write was sent: a parallel read that
//     reaches serveStaleWithDebt afterwards is refused (NoteIfNoWrite), so the
//     request is never served stale after its write and never re-run. (A read
//     that STARTS after the write never gets that far: the RESTAction that
//     sent it marked the sink DeclaredWrite before its first stage.)
//
// Nil-safe: no sink (no D2 for this resolve) → true.
func (s *StaleServeSink) BeginWrite() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.served) > 0 {
		return false
	}
	s.wrote = true
	return true
}

// NoteDeclaredWrite records that a RESTAction resolved for this request
// declares a write stage (see declaredWrite). Nil-safe.
func (s *StaleServeSink) NoteDeclaredWrite() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.declaredWrite = true
	s.mu.Unlock()
}

// DeclaredWrite reports whether a RESTAction resolved for this request declares
// a write stage (NoteDeclaredWrite). Nil-safe.
func (s *StaleServeSink) DeclaredWrite() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.declaredWrite
}

// NoteServedAt records the first stale serve's time (the stale_to_push
// origin). Nil-safe.
func (s *StaleServeSink) NoteServedAt(t time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.servedAt.IsZero() {
		s.servedAt = t
	}
	s.mu.Unlock()
}

// ServedAt is the first stale serve's time. Nil-safe.
func (s *StaleServeSink) ServedAt() time.Time {
	if s == nil {
		return time.Time{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.servedAt
}

// Commit is the dispatcher's accepted-Put outcome for a stale-derived
// response: it sets the key's owed-push mark and reports whether a content
// debt already cleared (the caller then replaces the cell now). False after a
// withdrawal or a previous commit.
func (s *StaleServeSink) Commit() (replaceNow bool) {
	if s == nil {
		return false
	}
	s.mu.Lock()
	if s.state != staleSinkPending {
		s.mu.Unlock()
		return false
	}
	s.state = staleSinkCommitted
	replaceNow = s.replaceOnCommit
	served := append([]staleServedRef(nil), s.served...)
	servedAt := s.servedAt
	s.mu.Unlock()
	MarkStaleOwed(s.l1Key, servedAt)
	// The stale bodies are now written: count their reads as this request's
	// hits, exactly once (a withdrawn pass never reaches here), through the one
	// customer hit funnel — Get (hit_total, LRU warmth, the #354/#547
	// serve-while-dirty note; TestIssue354_P3_StaleServeNoteCoversEveryCountedHit).
	if c := ResolvedCache(); c != nil {
		for _, r := range served {
			//serve-probe: counts a committed stale serve's deferred hit; the body was already served
			c.Get(r.key)
		}
	}
	return replaceNow
}

// Withdraw is the dispatcher's declined-Put outcome: the stale body is never
// written and no push is owed. True on the first call only.
func (s *StaleServeSink) Withdraw() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != staleSinkPending {
		return false
	}
	s.state = staleSinkWithdrawn
	return true
}

// DebtCleared is a content debt's outcome for this dependent: true means
// replace the committed cell now; a pending dependent is replaced at its
// Commit instead, a withdrawn one never.
func (s *StaleServeSink) DebtCleared() (replaceNow bool) {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case staleSinkCommitted:
		return true
	case staleSinkPending:
		s.replaceOnCommit = true
	}
	return false
}

// --- owed-push marks ----------------------------------------------------------

// The owed marks: every key a stale-derived body was written under, until a
// refresh for it reaches an armed subscriber after its cell stopped being
// stale-derived. A mark SURVIVES the cell's eviction (a fresh replacement
// evicted before the tab arms must still be announced: the refetch is a miss,
// i.e. a fresh fill). Bounded by the L1 store's own entry budget — oldest
// dropped first, counted on staleOwedDropped (a guarantee failure, expected 0).
var (
	staleOwedMu    sync.Mutex
	staleOwed      = map[string]*list.Element{} // value: *staleOwedMark
	staleOwedOrder = list.New()

	// staleOwedArmRecorder records stale_to_push for an owed push delivered at
	// arm time (the tab armed after the replacement was published): the
	// histogram lives with the D2 family (restactions/api), which installs it.
	staleOwedArmRecorder atomic.Pointer[func(servedAt time.Time)]

	staleOwedDropped   atomic.Uint64
	staleOwedArmEmits  atomic.Uint64
	staleOwedDelivered atomic.Uint64
)

// staleOwedMark is one owed push: its key and the EARLIEST stale serve still
// owed (the stale_to_push origin for an arm-time delivery).
type staleOwedMark struct {
	key      string
	servedAt time.Time
}

// SetStaleOwedArmRecorder installs the arm-time stale_to_push recorder.
func SetStaleOwedArmRecorder(f func(servedAt time.Time)) { staleOwedArmRecorder.Store(&f) }

// MarkStaleOwed sets key's owed-push mark, keeping the earliest servedAt still
// owed (a zero servedAt means "now").
func MarkStaleOwed(key string, servedAt time.Time) {
	if key == "" {
		return
	}
	if servedAt.IsZero() {
		servedAt = time.Now()
	}
	limit := defaultResolvedCacheMaxEntries
	if c := ResolvedCache(); c != nil && c.maxEntries > 0 {
		limit = c.maxEntries
	}
	staleOwedMu.Lock()
	defer staleOwedMu.Unlock()
	if e, ok := staleOwed[key]; ok {
		if m := e.Value.(*staleOwedMark); servedAt.Before(m.servedAt) {
			m.servedAt = servedAt
		}
		staleOwedOrder.MoveToBack(e)
		return
	}
	staleOwed[key] = staleOwedOrder.PushBack(&staleOwedMark{key: key, servedAt: servedAt})
	for staleOwedOrder.Len() > limit {
		front := staleOwedOrder.Front()
		staleOwedOrder.Remove(front)
		delete(staleOwed, front.Value.(*staleOwedMark).key)
		staleOwedDropped.Add(1)
	}
}

// StaleOwed reports whether key carries an owed-push mark.
func StaleOwed(key string) bool {
	staleOwedMu.Lock()
	defer staleOwedMu.Unlock()
	_, ok := staleOwed[key]
	return ok
}

// staleOwedReady reports whether key's owed push can be honoured now: a mark
// exists and the cell is no longer stale-derived (replaced, or absent — a
// refetch of an absent cell is a fresh fill).
func staleOwedReady(key string) bool {
	if !StaleOwed(key) {
		return false
	}
	if c := ResolvedCache(); c != nil {
		//serve-probe: owed-push readiness — reads StaleDerived only, serves no body
		if e, ok := c.GetNoTouch(key); ok && e != nil && e.StaleDerived {
			return false
		}
	}
	return true
}

func clearStaleOwed(key string) bool {
	_, ok := takeStaleOwed(key)
	return ok
}

// takeStaleOwed clears key's mark and returns its earliest owed servedAt.
func takeStaleOwed(key string) (time.Time, bool) {
	staleOwedMu.Lock()
	defer staleOwedMu.Unlock()
	e, ok := staleOwed[key]
	if !ok {
		return time.Time{}, false
	}
	staleOwedOrder.Remove(e)
	delete(staleOwed, key)
	return e.Value.(*staleOwedMark).servedAt, true
}

// noteStaleOwedArmEmit is the arm-time delivery of an owed push: it clears
// the mark, counts the emit, and records stale_to_push = now − servedAt (the
// tab showed the stale body all that time — the slowest corrections, which a
// publish-time-only record would drop).
func noteStaleOwedArmEmit(key string) {
	servedAt, ok := takeStaleOwed(key)
	if !ok {
		return
	}
	staleOwedArmEmits.Add(1)
	if f := staleOwedArmRecorder.Load(); f != nil {
		(*f)(servedAt)
	}
}

// noteRefreshFanout clears key's owed mark once a refresh reached >= 1 armed
// subscriber after the cell stopped being stale-derived (offerRefresh never
// drops, #484). Called by fanoutRefresh outside the hub lock.
func noteRefreshFanout(key string, offered int) {
	if offered == 0 || !staleOwedReady(key) {
		return
	}
	if clearStaleOwed(key) {
		staleOwedDelivered.Add(1)
	}
}

// StaleOwedStats is the owed-mark family: open marks, marks dropped by the
// capacity bound (a guarantee failure), pushes emitted at arm time, and marks
// cleared by a delivered publish.
type StaleOwedStats struct {
	Open, Dropped, ArmEmits, Delivered uint64
}

// StaleOwedStatsSnapshot reads the owed-mark family.
func StaleOwedStatsSnapshot() StaleOwedStats {
	staleOwedMu.Lock()
	open := uint64(len(staleOwed))
	staleOwedMu.Unlock()
	return StaleOwedStats{Open: open, Dropped: staleOwedDropped.Load(),
		ArmEmits: staleOwedArmEmits.Load(), Delivered: staleOwedDelivered.Load()}
}

// ResetStaleOwedForTest drops every mark and zeroes the family. Production
// MUST NOT call it.
func ResetStaleOwedForTest() {
	staleOwedMu.Lock()
	staleOwed = map[string]*list.Element{}
	staleOwedOrder = list.New()
	staleOwedMu.Unlock()
	staleOwedDropped.Store(0)
	staleOwedArmEmits.Store(0)
	staleOwedDelivered.Store(0)
}

// KnownStaleMark returns key's known-stale window: the dep-event seq of its
// latest mark (a fill whose resolve began at or after it read the post-mark
// state and closes the window — closeKnownStaleOnPut) and the window's start.
// ok=false: key is not known-stale.
func KnownStaleMark(key string) (seq uint64, t0 time.Time, ok bool) {
	s, ok := knownStaleStamp(key)
	if !ok {
		return 0, time.Time{}, false
	}
	return s.seq, s.t0, true
}

// ErrRefreshNowNoEntry / ErrRefreshNowNoHandler: RunRefreshNow had nothing to
// re-resolve (the cell is gone, legacy, or its class has no handler).
var (
	ErrRefreshNowNoEntry   = errors.New("refresh now: no resident entry with inputs")
	ErrRefreshNowNoHandler = errors.New("refresh now: no refresh handler for the entry's class")
)

// RunRefreshNow re-resolves l1Key at once through its class's registered
// refresh handler, outside the refresher queue. The handler Puts and, on an
// accepted Put, publishes the /refreshes signal itself (resolve_populate.go).
func RunRefreshNow(ctx context.Context, l1Key string) error {
	c := ResolvedCache()
	if c == nil {
		return ErrRefreshNowNoEntry
	}
	entry, ok := c.GetNoTouch(l1Key)
	if !ok || entry == nil || entry.Inputs == nil {
		return ErrRefreshNowNoEntry
	}
	r := refresherSingleton()
	r.handlersMu.RLock()
	fn := r.handlers[entry.Inputs.CacheEntryClass]
	r.handlersMu.RUnlock()
	if fn == nil {
		return ErrRefreshNowNoHandler
	}
	return runRefreshFunc(ctx, fn, l1Key, *entry.Inputs)
}

// staleServeQuiesce stops and joins the D2 goroutines (debt refills, push
// deliveries), which outlive their request by design and read the process
// singletons. Registered once by the D2 owner (restactions/api); called ONLY by
// the test resets of those singletons, so a reset never tears a store down
// under a running debt. Never called on a production path.
var staleServeQuiesce atomic.Pointer[func()]

// RegisterStaleServeQuiesce installs the D2 stop-and-join (see
// staleServeQuiesce).
func RegisterStaleServeQuiesce(f func()) { staleServeQuiesce.Store(&f) }

// quiesceStaleServeForTest runs the registered D2 stop-and-join, if any.
func quiesceStaleServeForTest() {
	// #740 — the viewer-gated chains outlive their request too (a pending wake
	// reads these singletons). Stop them first so no new attempt starts; the D2
	// quiesce then unblocks and joins the attempts already running (they may be
	// parked on its stop channel) before it joins its own goroutines.
	stopStaleRetryChainsForTest()
	if f := staleServeQuiesce.Load(); f != nil {
		(*f)()
	}
	resetStaleRetryForTest()
}
