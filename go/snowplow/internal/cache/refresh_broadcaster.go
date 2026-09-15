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
// 1.12.6 item 7 (C10 + C11, design-1.12.6-item7-refreshes.md §6.2/§6.3):
//
//   - C11 — the hub fans out through its reverse index. keyRefs (a per-key
//     COUNT) became keySubs (l1Key -> the subscribers armed for it), so a
//     publish visits only the connections armed for that key — O(armed-for-
//     key), typically 1–5 — instead of every live connection (O(|subs|),
//     1000 map lookups per published key at fleet scale). The refcount is
//     len(keySubs[k]).
//   - C10 — a DELETE-semantics eviction publishes too, via a DISTINCT entry
//     point (PublishEviction) that is paced per subscriber by a token bucket
//     (rate R keys/s, burst B, both env knobs) with a dedup'd pending set:
//     excess keys are DEFERRED and drained in order, never dropped. The
//     entry is already gone, so a late notification is strictly better
//     than none; the pending set is bounded by the connection's armed set.
//     PublishRefresh (the refresher's post-commit path, which induces L1
//     HITs) is untouched and un-bucketed.
//
// PRIOR ART (feedback_check_k8s_clientgo_prior_art): we BORROW the proven
// per-watcher discipline of k8s.io/apimachinery/pkg/watch.Broadcaster
// (mux.go: per-watcher buffered channel + DropIfChannelFull so one slow
// consumer never blocks the producer), but NOT the type — watch.Broadcaster
// fans EVERY event to EVERY watcher with no per-key/per-subject routing,
// which would leak the cluster-wide churn set to all 1000 users. This hub
// adds per-key subscription routing + a per-key subscriber reverse-index.
// The eviction pacing borrows golang.org/x/time/rate (the token bucket
// client-go's flowcontrol uses) rather than re-implementing one.
//
// CONCURRENCY (feedback_shared_vs_copy_is_a_concurrency_change): PublishRefresh
// is called from the refresher worker goroutine(s), off the customer request
// path; PublishEviction from the dep-event worker (deps.go runEvictionBatch)
// and the refresher's self-404 drop point (deps.go EvictSelfGone), both AFTER
// the store lock (resolved.go deleteForDep) is released; SubscribeRefresh /
// unsub / Arm / Disarm run on /refreshes connection goroutines. Shared hub
// state is guarded by mu (subs + keySubs), the coalesce map by cmu, and each
// subscriber's pacing state by its own pmu. Lock order: h.mu -> s.pmu; the
// per-subscriber drain goroutine takes only s.pmu and never h.mu, and the
// hub takes no store lock, so there is no inversion against the
// d.storeMu -> c.mu eviction path (S10). The fan-out loops hold only an
// RLock and do NO I/O inside it (a full sink takes the non-blocking default).
// Exercised by the -race falsifiers 9.6 and S10.
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

	"golang.org/x/time/rate"
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

	// envRefreshEvictionPublishRatePerSecond / envRefreshEvictionPublishBurst
	// are the C10 per-subscriber token bucket (1.12.6 item 7, design §6.2.3):
	// eviction-driven refresh frames are released to ONE connection at most
	// `burst` immediately and then `rate` per second; the rest are deferred
	// (never dropped) and drained in order. They bound the fleet-wide
	// refetch storm a mass DELETE would otherwise induce (1000 tabs × 30
	// widgets × the SPA's ×4 404-retry ≈ 120 000 requests in one tick).
	//
	// PROVISIONAL (Diego, 2026-09-14): 5 keys/s and burst 10 were chosen
	// without production data — a 30-widget page converges in <= 6 s, a
	// 512-widget page in ~100 s; the fleet-wide arrival rate is bounded at
	// 1000 × 5 = 5 000 notifications/s. Re-tune from the C12 counters that
	// justify them: snowplow_refresh_broadcaster.evict_deferred (how often
	// the bound engaged) and evict_published (the eviction volume).
	// Values < 1 fall back to the default: a zero rate would deliver
	// nothing and a zero burst would defer everything.
	envRefreshEvictionPublishRatePerSecond = "REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND"
	envRefreshEvictionPublishBurst         = "REFRESH_EVICTION_PUBLISH_BURST"
	defaultRefreshEvictionPublishRate      = 5
	defaultRefreshEvictionPublishBurst     = 10

	// refreshSubChanCap is the per-connection buffered-channel depth. A
	// full channel DROPS (coalesce-by-design) on the refresher path; the
	// value is the burst the hub absorbs before a slow consumer starts
	// shedding duplicate signals. Borrowed sizing intent from
	// watch.Broadcaster's per-watcher queue. The eviction path never drops:
	// a full sink defers the key instead.
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

// refreshEvictionBucket returns the C10 per-subscriber bucket parameters,
// read at subscribe time (a re-tune applies to new connections at pod
// start, matching the coalesce-window idiom). Values < 1 fall back to the
// defaults — see the env constants for why.
func refreshEvictionBucket() (rate.Limit, int) {
	r := intFromEnv(envRefreshEvictionPublishRatePerSecond, defaultRefreshEvictionPublishRate)
	if r < 1 {
		r = defaultRefreshEvictionPublishRate
	}
	b := intFromEnv(envRefreshEvictionPublishBurst, defaultRefreshEvictionPublishBurst)
	if b < 1 {
		b = defaultRefreshEvictionPublishBurst
	}
	return rate.Limit(r), b
}

// refreshSub is one SSE connection's sink. ch is buffered; on the refresher
// path a full channel DROPS (the refresher never blocks on a slow consumer);
// on the eviction path a full channel DEFERS. keys is the set of l1Keys this
// connection is armed for; it is consulted under the hub mu.
type refreshSub struct {
	id   uint64
	hub  *refreshBroadcaster
	keys map[string]struct{}
	ch   chan string

	// subscribedAt feeds stream_seconds_total at unsub (C12).
	subscribedAt time.Time

	// C10 eviction pacing, all guarded by pmu (lock order: hub.mu -> pmu).
	pmu     sync.Mutex
	limiter *rate.Limiter
	// pending is the dedup'd set of eviction keys deferred by the bucket
	// (or a full sink), order their FIFO release order. A key is in
	// pending iff it is in order (order may briefly carry a key that
	// DisarmKey removed from pending; the drain skips those). Bounded by
	// the armed set: only keys in keys are offered, and dedup keeps one
	// slot per key.
	pending  map[string]struct{}
	order    []string
	draining bool          // a drain goroutine is live for this sub
	closed   bool          // unsub ran; nothing more is queued or sent
	done     chan struct{} // closed at unsub; wakes the drain goroutine

	// per-connection attribution (C12), read by RefreshSubSnapshotForTest.
	delivered     atomic.Uint64
	evictDeferred atomic.Uint64
}

// refreshBroadcaster is the per-key fan-out hub. One process-singleton,
// lazily built; nil when the layer is disabled (refreshHub()).
type refreshBroadcaster struct {
	mu   sync.RWMutex
	subs map[uint64]*refreshSub
	// keySubs is the per-key subscriber reverse-index — l1Key -> the
	// connections armed for it (C11; formerly keyRefs, a count). Maintained
	// in lockstep with subs[*].keys under mu. Backs HasRefreshSubscriber's
	// O(1) presence check and BOTH fan-outs (PublishRefresh /
	// PublishEviction visit only keySubs[l1Key]). A key with no armed
	// connection has no entry (the inner map is deleted when it empties),
	// so len(keySubs) is the armed_keys gauge.
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
// only, so it fires only when L1 actually changed (design §1.1). NOT paced:
// a refresher-driven refetch is an L1 HIT (feedback_customer_priority_
// over_refresher — pacing the eviction path must never slow this one).
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

// PublishEviction announces that l1Key was just EVICTED from L1 by a
// DELETE-semantics path (deps.go runEvictionBatch — an informer DELETE /
// objAbsent — or EvictSelfGone — the refresher's definite self-404). The
// armed frontend must refetch so the deleted resource stops rendering from
// a body that no longer exists (1.12.6 item 7, loss mode L1).
//
// It reuses the existing `refresh` frame (one key per frame; no wire change)
// but NOT PublishRefresh: it is neither coalesced nor dropped. Each armed
// subscriber paces it through its own token bucket (refreshEvictionBucket)
// — within budget the key goes straight to the sink; otherwise, or when the
// sink is full, it is DEFERRED into the subscriber's dedup'd pending set and
// released in order by that subscriber's drain goroutine. Never dropped: the
// entry is gone, so a late notification is strictly better than none.
//
// Cost with no subscriber armed for the key: one RLock + one map read, and
// no counter moves — a 50K-composition bench teardown with no browser
// attached publishes nothing (S1c). TTL / LRU / max-age evictions do NOT
// call this (design §6.2.5, S1d): they are capacity events, not object
// deletions, and publishing them would be a periodic refetch storm.
func PublishEviction(l1Key string) {
	h := refreshHub()
	if h == nil {
		return // disabled / cache-off — transparent no-op
	}
	h.mu.RLock()
	targets := h.keySubs[l1Key]
	if len(targets) == 0 {
		h.mu.RUnlock()
		return // nobody armed: zero cost, zero signal (S1c)
	}
	for _, s := range targets {
		fanoutVisitsForTest.Add(1)
		s.offerEviction(l1Key)
	}
	h.mu.RUnlock()
	refreshEvictPublishedTotal.Add(1)
}

// offerEviction hands one evicted key to this subscriber: immediate when the
// bucket and the sink allow, deferred otherwise. Caller holds h.mu (read);
// takes s.pmu (lock order h.mu -> s.pmu).
func (s *refreshSub) offerEviction(l1Key string) {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	if s.closed {
		return
	}
	if _, dup := s.pending[l1Key]; dup {
		return // already queued for this connection — one slot per key
	}
	// While a backlog is draining every new key joins the queue (FIFO
	// fairness; the drain goroutine is the only token consumer then).
	if !s.draining && s.limiter.Allow() {
		select {
		case s.ch <- l1Key:
			refreshDeliveredTotal.Add(1)
			s.delivered.Add(1)
			noteSinkDepth(len(s.ch))
			return
		default:
			// Full sink: fall through and defer — never drop an eviction.
		}
	}
	s.pending[l1Key] = struct{}{}
	s.order = append(s.order, l1Key)
	refreshEvictDeferredTotal.Add(1)
	s.evictDeferred.Add(1)
	notePendingHighWater(len(s.pending))
	if !s.draining {
		s.draining = true
		go s.drainEvictions()
	}
}

// drainFullSinkRetry is how long the drain goroutine waits before retrying
// a send into a full sink. The connection goroutine reads the sink
// continuously, so a full sink is transient (it means the consumer is
// behind by refreshSubChanCap frames).
const drainFullSinkRetry = 5 * time.Millisecond

// drainEvictions releases this subscriber's deferred eviction keys in FIFO
// order at the bucket's rate, then exits (a new backlog starts a new
// goroutine). It takes only s.pmu, never h.mu, and blocks only on its own
// timers / done — never on the publisher and never on the sink: every send
// is non-blocking and happens under pmu with closed==false, so nothing is
// ever sent after unsub (S10 "no deliver-after-unsub").
func (s *refreshSub) drainEvictions() {
	for {
		s.pmu.Lock()
		if s.closed {
			s.pmu.Unlock()
			return
		}
		var l1Key string
		for len(s.order) > 0 {
			k := s.order[0]
			s.order = s.order[1:]
			if _, live := s.pending[k]; live {
				l1Key = k
				break
			}
			// disarmed while pending — nothing to deliver for it
		}
		if l1Key == "" {
			s.draining = false
			s.order = nil
			s.pmu.Unlock()
			return
		}
		delay := s.limiter.Reserve().Delay()
		s.pmu.Unlock()

		if !s.sleepOrDone(delay) {
			return
		}
		for !s.trySendDeferred(l1Key) {
			if !s.sleepOrDone(drainFullSinkRetry) {
				return
			}
		}
	}
}

// sleepOrDone waits d (no-op when d <= 0); false when the subscriber was
// closed meanwhile.
func (s *refreshSub) sleepOrDone(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-s.done:
			return false
		default:
			return true
		}
	}
	tm := time.NewTimer(d)
	select {
	case <-tm.C:
		return true
	case <-s.done:
		tm.Stop()
		return false
	}
}

// trySendDeferred attempts one non-blocking send of a deferred key under
// pmu. Returns true when the key is delivered OR no longer needs delivery
// (closed / disarmed meanwhile); false when the sink was full (retry).
func (s *refreshSub) trySendDeferred(l1Key string) bool {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	if s.closed {
		return true
	}
	if _, live := s.pending[l1Key]; !live {
		return true // disarmed during the wait
	}
	select {
	case s.ch <- l1Key:
		// Delivered; the pending slot is released only now so a re-eviction
		// of the same key during the wait stayed deduplicated.
		delete(s.pending, l1Key)
		refreshDeliveredTotal.Add(1)
		s.delivered.Add(1)
		noteSinkDepth(len(s.ch))
		return true
	default:
		return false
	}
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
	limit, burst := refreshEvictionBucket()
	s := &refreshSub{
		hub:          h,
		keys:         make(map[string]struct{}, len(armedKeys)),
		ch:           make(chan string, refreshSubChanCap),
		subscribedAt: time.Now(),
		limiter:      rate.NewLimiter(limit, burst),
		pending:      map[string]struct{}{},
		done:         make(chan struct{}),
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
			s.close()
			// Do NOT close s.ch — a publisher may hold the RLock and be
			// mid-send under a concurrent publish. The handler stops reading
			// after unsub; the channel is GC'd with the sub. (watch.Broadcaster
			// uses the same "producer never sends to a closed chan" discipline.)
		})
	}
	return s.ch, unsub
}

// close marks the subscriber dead for the eviction path (no more queueing,
// the drain goroutine exits) and folds its lifetime into the C12 stream
// counters. Idempotent under pmu.
func (s *refreshSub) close() {
	s.pmu.Lock()
	if s.closed {
		s.pmu.Unlock()
		return
	}
	s.closed = true
	s.pending = nil
	s.order = nil
	close(s.done)
	s.pmu.Unlock()
	refreshStreamNanosTotal.Add(time.Since(s.subscribedAt).Nanoseconds())
	refreshStreamsClosedTotal.Add(1)
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
// A pending (deferred) eviction for the key is withdrawn too, so the pending
// set never outgrows the armed set.
func (s *refreshSub) DisarmKey(l1Key string) {
	if s == nil || s.hub == nil {
		return
	}
	s.hub.mu.Lock()
	s.hub.disarmLocked(s, l1Key)
	s.pmu.Lock()
	if s.pending != nil {
		delete(s.pending, l1Key)
	}
	s.pmu.Unlock()
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
	// succeeded, on BOTH the refresher and the eviction path.
	refreshDeliveredTotal atomic.Uint64
	// refreshDroppedTotal counts (key -> subscriber) sends dropped because
	// the subscriber's buffer was full (slow consumer) — refresher path only;
	// the eviction path defers instead.
	refreshDroppedTotal atomic.Uint64
	// refreshCoalescedTotal counts emits suppressed by the per-key coalesce
	// window.
	refreshCoalescedTotal atomic.Uint64

	// C10/C12 — eviction path.
	//
	// refreshEvictPublishedTotal counts PublishEviction calls that reached
	// >=1 armed subscriber (the eviction volume the frontend is told about).
	refreshEvictPublishedTotal atomic.Uint64
	// refreshEvictDeferredTotal counts (key -> subscriber) eviction signals
	// the bucket (or a full sink) deferred instead of sending immediately —
	// the number that proves the bound engaged (design §6.2.3).
	refreshEvictDeferredTotal atomic.Uint64

	// C12 — stream lifetime. stream_seconds_total / streams_closed_total is
	// the mean stream lifetime; the instrument S8 found missing at every hop.
	refreshStreamNanosTotal   atomic.Int64
	refreshStreamsClosedTotal atomic.Uint64
	// refreshMaxSinkDepth is the high-water mark of a subscriber sink's
	// occupancy after a send (0..refreshSubChanCap) — consumer lag.
	refreshMaxSinkDepth atomic.Int64

	// fanoutVisitsForTest counts subscribers EXAMINED by a fan-out loop —
	// the S2 discriminating probe (an O(|subs|) loop cannot keep it small).
	// Test-only reader; production never resets it.
	fanoutVisitsForTest atomic.Uint64
	// evictPendingHighWaterForTest is the largest per-subscriber pending
	// set observed — S9's "pending never exceeds armed" instrument.
	evictPendingHighWaterForTest atomic.Int64
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

func notePendingHighWater(n int) {
	d := int64(n)
	for {
		cur := evictPendingHighWaterForTest.Load()
		if d <= cur || evictPendingHighWaterForTest.CompareAndSwap(cur, d) {
			return
		}
	}
}

// RefreshBroadcasterCounters returns (published, delivered, dropped,
// coalesced) for post-deploy inspection + tests.
func RefreshBroadcasterCounters() (published, delivered, dropped, coalesced uint64) {
	return refreshPublishedTotal.Load(),
		refreshDeliveredTotal.Load(),
		refreshDroppedTotal.Load(),
		refreshCoalescedTotal.Load()
}

// RefreshBroadcasterStats is the C12 point-in-time snapshot behind
// /debug/vars snowplow_refresh_broadcaster and the OTel mirror
// (internal/metrics). Identity-free aggregates only.
type RefreshBroadcasterStats struct {
	Published, Delivered, Dropped, Coalesced uint64
	Subscribers, ArmedKeys                   int
	MaxSinkDepth                             int64
	EvictPublished, EvictDeferred            uint64
	StreamSecondsTotal                       float64
	StreamsClosedTotal                       uint64
}

// RefreshBroadcasterStatsSnapshot reads every broadcaster gauge/counter at
// once (one RLock for the two hub-size gauges, atomics for the rest).
func RefreshBroadcasterStatsSnapshot() RefreshBroadcasterStats {
	st := RefreshBroadcasterStats{
		Published:          refreshPublishedTotal.Load(),
		Delivered:          refreshDeliveredTotal.Load(),
		Dropped:            refreshDroppedTotal.Load(),
		Coalesced:          refreshCoalescedTotal.Load(),
		MaxSinkDepth:       refreshMaxSinkDepth.Load(),
		EvictPublished:     refreshEvictPublishedTotal.Load(),
		EvictDeferred:      refreshEvictDeferredTotal.Load(),
		StreamSecondsTotal: float64(refreshStreamNanosTotal.Load()) / float64(time.Second),
		StreamsClosedTotal: refreshStreamsClosedTotal.Load(),
	}
	if h := refreshHub(); h != nil {
		h.mu.RLock()
		st.Subscribers = len(h.subs)
		st.ArmedKeys = len(h.keySubs)
		h.mu.RUnlock()
	}
	return st
}

// RefreshSubSnapshot is one live connection's pacing state, for the S9 /
// S12 arms (cross-package: internal/handlers drives the real handler).
type RefreshSubSnapshot struct {
	Armed, Pending int
	Delivered      uint64
	EvictDeferred  uint64
}

// RefreshSubSnapshotsForTest returns every live subscriber's pacing state.
// Test-only — production code MUST NOT call it.
func RefreshSubSnapshotsForTest() []RefreshSubSnapshot {
	h := refreshHub()
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]RefreshSubSnapshot, 0, len(h.subs))
	for _, s := range h.subs {
		s.pmu.Lock()
		out = append(out, RefreshSubSnapshot{
			Armed:         len(s.keys),
			Pending:       len(s.pending),
			Delivered:     s.delivered.Load(),
			EvictDeferred: s.evictDeferred.Load(),
		})
		s.pmu.Unlock()
	}
	return out
}

// RefreshEvictPendingHighWaterForTest returns the largest per-subscriber
// pending set observed since the last reset. Test-only.
func RefreshEvictPendingHighWaterForTest() int64 {
	return evictPendingHighWaterForTest.Load()
}

// RefreshFanoutVisitsForTest returns the S2 probe. Test-only.
func RefreshFanoutVisitsForTest() uint64 {
	return fanoutVisitsForTest.Load()
}

// resetRefreshBroadcasterForTest tears the singleton down and zeroes the
// counters. Live subscribers of the torn-down hub are closed so their drain
// goroutines exit. Test-only — production code MUST NOT call this. Mirrors
// resetRefresherForTest's singleton-rebuild discipline.
func resetRefreshBroadcasterForTest() {
	refreshHubMu.Lock()
	old := refreshHubInstance
	refreshHubInstance = nil
	refreshHubMu.Unlock()
	if old != nil {
		old.mu.Lock()
		for _, s := range old.subs {
			s.close()
		}
		old.mu.Unlock()
	}
	refreshPublishedTotal.Store(0)
	refreshDeliveredTotal.Store(0)
	refreshDroppedTotal.Store(0)
	refreshCoalescedTotal.Store(0)
	refreshEvictPublishedTotal.Store(0)
	refreshEvictDeferredTotal.Store(0)
	refreshStreamNanosTotal.Store(0)
	refreshStreamsClosedTotal.Store(0)
	refreshMaxSinkDepth.Store(0)
	fanoutVisitsForTest.Store(0)
	evictPendingHighWaterForTest.Store(0)
}

// ResetRefreshBroadcasterForTest is the exported wrapper for cross-package
// tests (internal/handlers/dispatchers' emit-seam falsifiers). Production
// code MUST NOT call it. Mirrors ResetRefresherForTest.
func ResetRefreshBroadcasterForTest() {
	resetRefreshBroadcasterForTest()
}
