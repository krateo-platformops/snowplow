// refresh_broadcaster.go — Ship 1 (live-refresh-coherence, option A).
//
// A purpose-built per-key fan-out hub for the live-refresh signal. After
// the refresher commits a fresh entry to L1 (resolve_populate.go:291) it
// calls PublishRefresh(l1Key); the hub fans that l1Key out to every SSE
// connection (internal/handlers/refreshes.go) that armed it, so the
// frontend learns precisely when to refetch — and the refetch is a
// guaranteed L1 HIT (no apiserver read). Design:
// docs/live-refresh-coherence-design-2026-06-18.md §2.
//
// 1.12.6 item 7 (C11, design-1.12.6-item7-refreshes.md §6.3): the hub fans
// out through its reverse index. keyRefs (a per-key COUNT) became keySubs
// (l1Key -> the subscribers armed for it), so a publish visits only the
// connections armed for that key — O(armed-for-key), typically 1–5 —
// instead of every live connection (O(|subs|), 1000 map lookups per
// published key at fleet scale). The refcount is len(keySubs[k]).
//
// PRIOR ART (feedback_check_k8s_clientgo_prior_art): we BORROW the proven
// per-watcher discipline of k8s.io/apimachinery/pkg/watch.Broadcaster
// (mux.go: per-watcher buffered channel + DropIfChannelFull so one slow
// consumer never blocks the producer), but NOT the type — watch.Broadcaster
// fans EVERY event to EVERY watcher with no per-key/per-subject routing,
// which would leak the cluster-wide churn set to all 1000 users. This hub
// adds per-key subscription routing + a per-key subscriber reverse-index.
//
// CONCURRENCY (feedback_shared_vs_copy_is_a_concurrency_change): PublishRefresh
// is called from the refresher worker goroutine(s), off the customer request
// path; SubscribeRefresh / unsub / Arm / Disarm run on /refreshes connection
// goroutines. All shared state is guarded by mu (subs + keySubs) and the
// coalesce map by cmu. The fan-out loop holds only an RLock and does NO I/O
// inside it (a full sink takes the non-blocking default: arm). Exercised by
// the -race falsifier 9.6.
//
// REMOVABILITY (project_cache_off_is_transparent_fallback, project_caching_
// is_provisional): refreshHub() returns nil under RefreshSSEEnabled()==false
// (which is false whenever CACHE_ENABLED=false). Every public entry point is
// nil-safe and no-ops in that state. The whole layer is gated by one env
// toggle (REFRESH_SSE_ENABLED) and is cleanly switch-off-able.

package cache

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// envRefreshSSEEnabled is the Ship 1 per-feature toggle for the
	// live-refresh SSE layer (broadcaster + /refreshes). Default ON when
	// the cache subsystem is on, mirroring WidgetContentL1Enabled; explicit
	// "false"/"0"/"no" disables it (broadcaster becomes a no-op, /refreshes
	// an idle stream) without losing L1. Gated UNDER ResolvedCacheEnabled().
	envRefreshSSEEnabled = "REFRESH_SSE_ENABLED"

	// envRefreshCoalesceWindowMS bounds per-key fan-out under churn: a
	// second emit for the same key within the window is coalesced away
	// (the frontend refetches on the next signal, and a refetch is
	// idempotent, so a coalesced duplicate is harmless). Design §2.3.
	envRefreshCoalesceWindowMS = "REFRESH_COALESCE_WINDOW_MS"

	// defaultRefreshCoalesceWindowMS — 250ms (design §2.3 / §7). The
	// frontend ALSO throttles per widget (~5s); server coalescing is the
	// cheaper first line.
	defaultRefreshCoalesceWindowMS int64 = 250

	// refreshSubChanCap is the per-connection buffered-channel depth. A
	// full channel DROPS (coalesce-by-design); the value is the burst the
	// hub absorbs before a slow consumer starts shedding duplicate signals.
	// Borrowed sizing intent from watch.Broadcaster's per-watcher queue.
	refreshSubChanCap = 64
)

// RefreshSSEEnabled reports whether the Ship 1 live-refresh SSE layer is
// active. TWO gates, both must hold (same shape as WidgetContentL1Enabled):
//
//  1. ResolvedCacheEnabled() — CACHE_ENABLED=true AND RESOLVED_CACHE_ENABLED
//     !=false (the refresher + L1 store the signal rides on).
//  2. REFRESH_SSE_ENABLED!="false" — the per-feature toggle, default ON.
//
// When false the broadcaster is a no-op and /refreshes is an idle stream —
// transparent fallback (project_cache_off_is_transparent_fallback).
func RefreshSSEEnabled() bool {
	if !ResolvedCacheEnabled() {
		return false
	}
	switch os.Getenv(envRefreshSSEEnabled) {
	case "false", "0", "no":
		return false
	default:
		return true
	}
}

// refreshCoalesceWindowFn is a (var-seam) indirection over the coalesce-window
// resolution so the L11 falsifier can transiently install a wrong-shaped
// resolver (one that coerces an explicit 0 into the 250ms DEFAULT, coalescing
// away a burst) to observe RED, then restore the real fn. Production NEVER
// reassigns it. Repo idiom: resolveOnceFn / nestedCallResolver.
var refreshCoalesceWindowFn = refreshCoalesceWindow

// refreshCoalesceWindow returns the active per-key coalesce window. A value
// <= 0 disables coalescing (every emit fans out) — an EXPLICIT 0 must be
// honoured as "off", NOT silently coerced to the default. Read fresh per emit
// so a deployer can re-tune at pod start (matches the rateFloor env-read idiom).
func refreshCoalesceWindow() time.Duration {
	return time.Duration(int64FromEnv(envRefreshCoalesceWindowMS, defaultRefreshCoalesceWindowMS)) * time.Millisecond
}

// refreshSub is one SSE connection's sink. ch is buffered; a full channel
// DROPS (the refresher never blocks on a slow consumer). keys is the set of
// l1Keys this connection is armed for; it is consulted under the hub mu.
type refreshSub struct {
	id   uint64
	hub  *refreshBroadcaster
	keys map[string]struct{}
	ch   chan string

	// per-connection attribution (C12), read by RefreshSubSnapshotsForTest.
	delivered atomic.Uint64
}

// refreshBroadcaster is the per-key fan-out hub. One process-singleton,
// lazily built; nil when the layer is disabled (refreshHub()).
type refreshBroadcaster struct {
	mu   sync.RWMutex
	subs map[uint64]*refreshSub
	// keySubs is the per-key subscriber reverse-index — l1Key -> the
	// connections armed for it (C11; formerly keyRefs, a count). Maintained
	// in lockstep with subs[*].keys under mu. Backs HasRefreshSubscriber's
	// O(1) presence check and the fan-out (PublishRefresh visits only
	// keySubs[l1Key]). A key with no armed connection has no entry (the
	// inner map is deleted when it empties), so len(keySubs) is the
	// armed_keys gauge.
	keySubs map[string]map[uint64]*refreshSub
	next    uint64

	// coalesce state: per-key last-emit timestamp, guarded by cmu (a
	// separate lock so coalescing never contends with the fan-out RLock).
	cmu      sync.Mutex
	lastEmit map[string]time.Time
}

var (
	refreshHubInstance *refreshBroadcaster
	refreshHubMu       sync.Mutex // guards lazy construction + reset-for-test
)

// refreshHub returns the process-wide broadcaster, or nil when the
// live-refresh layer is disabled (RefreshSSEEnabled()==false, which is also
// false under cache-off). Nil-safe: every caller nil-checks and no-ops.
//
// Lazy construction is a plain mutex-guarded double-check (NOT sync.Once) so
// resetRefreshBroadcasterForTest can rebuild the singleton between tests — the
// same discipline resetRefresherForTest uses (refresher.go).
func refreshHub() *refreshBroadcaster {
	if !RefreshSSEEnabled() {
		return nil
	}
	refreshHubMu.Lock()
	defer refreshHubMu.Unlock()
	if refreshHubInstance == nil {
		refreshHubInstance = &refreshBroadcaster{
			subs:     map[uint64]*refreshSub{},
			keySubs:  map[string]map[uint64]*refreshSub{},
			lastEmit: map[string]time.Time{},
		}
	}
	return refreshHubInstance
}

// armLocked / disarmLocked maintain keySubs in lockstep with s.keys. Caller
// holds h.mu (write).
func (h *refreshBroadcaster) armLocked(s *refreshSub, l1Key string) {
	if _, ok := s.keys[l1Key]; ok {
		return
	}
	s.keys[l1Key] = struct{}{}
	m := h.keySubs[l1Key]
	if m == nil {
		m = map[uint64]*refreshSub{}
		h.keySubs[l1Key] = m
	}
	m[s.id] = s
}

func (h *refreshBroadcaster) disarmLocked(s *refreshSub, l1Key string) {
	if _, ok := s.keys[l1Key]; !ok {
		return
	}
	delete(s.keys, l1Key)
	if m := h.keySubs[l1Key]; m != nil {
		delete(m, s.id)
		if len(m) == 0 {
			delete(h.keySubs, l1Key)
		}
	}
}

// coalesced reports whether an emit for l1Key should be suppressed because
// the previous emit for the SAME key was within the coalesce window. On a
// non-suppressed emit it stamps lastEmit[l1Key]=now. A window <= 0 disables
// coalescing entirely (always returns false). Design §2.3.
func (h *refreshBroadcaster) coalesced(l1Key string, now time.Time) bool {
	win := refreshCoalesceWindowFn()
	if win <= 0 {
		return false
	}
	h.cmu.Lock()
	defer h.cmu.Unlock()
	if last, ok := h.lastEmit[l1Key]; ok && now.Sub(last) < win {
		return true
	}
	h.lastEmit[l1Key] = now
	return false
}

// PublishRefresh announces that l1Key was just committed to L1. Non-blocking:
// it fans the key out to every connection armed for it; a full sink is
// dropped (refreshDroppedTotal bump) rather than blocking the refresher
// goroutine. No-op when the layer is disabled or no hub exists.
//
// Called from internal/handlers/dispatchers/resolve_populate.go:291,
// immediately after c.Put — strictly post-commit, on the refresher path
// only, so it fires only when L1 actually changed (design §1.1).
func PublishRefresh(l1Key string) {
	h := refreshHub()
	if h == nil {
		return // disabled / cache-off — transparent no-op
	}
	now := time.Now()
	if h.coalesced(l1Key, now) {
		refreshCoalescedTotal.Add(1)
		return
	}
	h.mu.RLock()
	// C11: visit ONLY the subscribers armed for this key (the reverse
	// index), not every live connection. fanoutVisitsForTest is the S2
	// discriminating instrument — an absolute count of subscribers
	// examined, which an O(|subs|) loop cannot keep small.
	for _, s := range h.keySubs[l1Key] {
		fanoutVisitsForTest.Add(1)
		select {
		case s.ch <- l1Key:
			refreshDeliveredTotal.Add(1)
			s.delivered.Add(1)
			noteSinkDepth(len(s.ch))
		default:
			// Slow consumer: its buffer is full. Drop — the next committed
			// refresh for this key re-signals and the frontend's refetch is
			// idempotent. A dropped TERMINAL signal degrades to the frontend
			// 5s throttle (falsifier 9.8), never indefinite stale.
			refreshDroppedTotal.Add(1)
		}
	}
	h.mu.RUnlock()
	refreshPublishedTotal.Add(1)
}

// SubscribeRefresh registers a connection armed for the given validated
// l1Key set (the caller — handlers.Refreshes — has already re-derived these
// keys under the connection's authenticated identity; §5). Returns the sink
// channel to read signals from and an idempotent unsubscribe func that the
// handler MUST defer.
//
// When the layer is disabled it returns a closed channel + a no-op unsub, so
// the handler degrades to a clean idle stream (transparent fallback).
func SubscribeRefresh(armedKeys map[string]struct{}) (<-chan string, func()) {
	h := refreshHub()
	if h == nil {
		ch := make(chan string)
		close(ch)
		return ch, func() {}
	}
	s := &refreshSub{
		hub:  h,
		keys: make(map[string]struct{}, len(armedKeys)),
		ch:   make(chan string, refreshSubChanCap),
	}
	h.mu.Lock()
	h.next++
	s.id = h.next
	h.subs[s.id] = s
	for k := range armedKeys {
		h.armLocked(s, k)
	}
	h.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subs[s.id]; ok {
				for k := range s.keys {
					h.disarmLocked(s, k)
				}
				delete(h.subs, s.id)
			}
			h.mu.Unlock()
			// Do NOT close s.ch — PublishRefresh may hold the RLock and be
			// mid-send under a concurrent publish. The handler stops reading
			// after unsub; the channel is GC'd with the sub. (watch.Broadcaster
			// uses the same "producer never sends to a closed chan" discipline.)
		})
	}
	return s.ch, unsub
}

// ArmKey adds l1Key to a live connection's armed set. Idempotent.
//
// NO PRODUCTION CALLER (design-1.12.6-item7-refreshes.md §1.3, TRACED):
// SubscribeRefresh returns only (sink, unsub) and never hands out the
// *refreshSub, so the handler cannot reach this — the armed set is FROZEN
// per connection and the SPA re-arms by dropping and re-opening the stream.
// Kept (not deleted) as the natural home for a future incremental-arm
// protocol; exercised only by the broadcaster's own tests.
func (s *refreshSub) ArmKey(l1Key string) {
	if s == nil || s.hub == nil {
		return
	}
	s.hub.mu.Lock()
	s.hub.armLocked(s, l1Key)
	s.hub.mu.Unlock()
}

// DisarmKey removes l1Key from a live connection's armed set. Idempotent.
// Same status as ArmKey: no production caller (design §1.3), kept for a
// future incremental-arm protocol, exercised only by the broadcaster's tests.
func (s *refreshSub) DisarmKey(l1Key string) {
	if s == nil || s.hub == nil {
		return
	}
	s.hub.mu.Lock()
	s.hub.disarmLocked(s, l1Key)
	s.hub.mu.Unlock()
}

// HasRefreshSubscriber reports whether >=1 connection is armed for l1Key.
// O(1) map read under RLock. Nil-safe: cache-off / disabled / no hub -> false.
func HasRefreshSubscriber(l1Key string) bool {
	h := refreshHub()
	if h == nil {
		return false
	}
	h.mu.RLock()
	n := len(h.keySubs[l1Key])
	h.mu.RUnlock()
	return n > 0
}

// RefreshSubscriberCount returns the current number of live /refreshes
// connections. Read-only — used by /debug/vars + tests for connection-scale
// observability (design §10).
func RefreshSubscriberCount() int {
	h := refreshHub()
	if h == nil {
		return 0
	}
	h.mu.RLock()
	n := len(h.subs)
	h.mu.RUnlock()
	return n
}

// RefreshArmedKeyCount returns the number of distinct l1Keys with >=1 armed
// connection (len(keySubs)) — the memory-scale gauge for the reverse index
// (C12 armed_keys).
func RefreshArmedKeyCount() int {
	h := refreshHub()
	if h == nil {
		return 0
	}
	h.mu.RLock()
	n := len(h.keySubs)
	h.mu.RUnlock()
	return n
}

// --- counters (atomic.Uint64; same idiom as cluster_list_metrics.go) --------

var (
	// refreshPublishedTotal counts PublishRefresh calls that fanned out
	// (post-coalesce, hub present).
	refreshPublishedTotal atomic.Uint64
	// refreshDeliveredTotal counts individual (key -> subscriber) sends that
	// succeeded.
	refreshDeliveredTotal atomic.Uint64
	// refreshDroppedTotal counts (key -> subscriber) sends dropped because
	// the subscriber's buffer was full (slow consumer).
	refreshDroppedTotal atomic.Uint64
	// refreshCoalescedTotal counts emits suppressed by the per-key coalesce
	// window.
	refreshCoalescedTotal atomic.Uint64

	// refreshMaxSinkDepth is the high-water mark of a subscriber sink's
	// occupancy after a send (0..refreshSubChanCap) — consumer lag.
	refreshMaxSinkDepth atomic.Int64

	// fanoutVisitsForTest counts subscribers EXAMINED by a fan-out loop —
	// the S2 discriminating probe (an O(|subs|) loop cannot keep it small).
	// Test-only reader; production never resets it.
	fanoutVisitsForTest atomic.Uint64
)

func noteSinkDepth(depth int) {
	d := int64(depth)
	for {
		cur := refreshMaxSinkDepth.Load()
		if d <= cur || refreshMaxSinkDepth.CompareAndSwap(cur, d) {
			return
		}
	}
}

// RefreshFanoutVisitsForTest returns the S2 probe. Test-only.
func RefreshFanoutVisitsForTest() uint64 {
	return fanoutVisitsForTest.Load()
}

// RefreshBroadcasterCounters returns (published, delivered, dropped,
// coalesced) for post-deploy inspection + tests.
func RefreshBroadcasterCounters() (published, delivered, dropped, coalesced uint64) {
	return refreshPublishedTotal.Load(),
		refreshDeliveredTotal.Load(),
		refreshDroppedTotal.Load(),
		refreshCoalescedTotal.Load()
}

// resetRefreshBroadcasterForTest tears the singleton down and zeroes the
// counters. Test-only — production code MUST NOT call this. Mirrors
// resetRefresherForTest's singleton-rebuild discipline.
func resetRefreshBroadcasterForTest() {
	refreshHubMu.Lock()
	refreshHubInstance = nil
	refreshHubMu.Unlock()
	refreshPublishedTotal.Store(0)
	refreshDeliveredTotal.Store(0)
	refreshDroppedTotal.Store(0)
	refreshCoalescedTotal.Store(0)
	refreshMaxSinkDepth.Store(0)
	fanoutVisitsForTest.Store(0)
}

// ResetRefreshBroadcasterForTest is the exported wrapper for cross-package
// tests (internal/handlers/dispatchers' emit-seam falsifiers). Production
// code MUST NOT call it. Mirrors ResetRefresherForTest.
func ResetRefreshBroadcasterForTest() {
	resetRefreshBroadcasterForTest()
}
