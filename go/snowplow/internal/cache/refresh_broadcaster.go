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
// #484 (a quiet key's LAST change must reach the tab): the refresher path
// used to lose a signal two ways, and the SPA repairs neither (no polling,
// no refetch on focus), so an open tab stayed stale until navigation:
//
//   - TRAILING-EDGE COALESCING. A coalesced emit is not a duplicate when it
//     announces a newer commit. Every leading emit opens a per-key window;
//     a signal suppressed inside it marks the window, and the window's end
//     emits EXACTLY ONE trailing signal (then opens a fresh window, so a
//     continuously-changing key settles at one emit per window). A window
//     exists only for an ARMED key and is torn down on the key's last
//     disarm, on an eviction of the key (the eviction frame supersedes it)
//     and on hub reset, so the armed-timer count is bounded by armed keys.
//   - PER-SUBSCRIBER REFRESH PENDING SET (modelled on the eviction path's,
//     but un-paced: customer-priority). A full sink DEFERS the key into a
//     dedup'd FIFO set drained by the subscriber's own goroutine — never
//     dropped. The set is a subset of the armed set (<= refreshSubMaxEntries
//     per connection), so it cannot outgrow it. Its overflow rule is a
//     STALL rule: when the drain cannot place a key for a full liveness
//     interval (RefreshLivenessInterval, the heartbeat contract) while the
//     consumer made no progress at all, the subscriber is force-RESYNCED —
//     both pending sets are released and the handler ends the stream with an
//     `event: resync` frame; the SPA's transport-loss path (refreshSse.ts
//     scheduleRetry → revalidateOnConnect → revalidateArmed) re-fetches every
//     armed widget, so no key is ever silently lost.
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
// subscriber's pacing state by its own pmu. Lock order: h.mu -> s.pmu and
// h.mu -> cmu (cmu and pmu are never nested; nothing takes h.mu under
// either); the per-subscriber drain goroutines take only s.pmu and never
// h.mu, the coalesce timers take cmu and release it before fanning out, and
// the hub takes no store lock, so there is no inversion against the
// d.storeMu -> c.mu eviction path (S10). The fan-out loops hold only an
// RLock and do NO I/O inside it; every sink send is a non-blocking
// select/default under pmu, so no lock is ever held across a blocking send
// (PublishRefresh is also reached from the customer cold-fill,
// publishIfSubscribed). Exercised by the -race falsifiers 9.6, S10 and the
// #484 arms (refresh_484_*_test.go).
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

	// refreshSubChanCap is the per-connection buffered-channel depth: the
	// burst the hub absorbs before a slow consumer's keys start deferring
	// into its pending sets. Borrowed sizing intent from watch.Broadcaster's
	// per-watcher queue. Since #484 NEITHER path drops: a full sink defers
	// the key (refresh pending set / eviction pending set).
	refreshSubChanCap = 64

	// RefreshLivenessInterval is the /refreshes liveness contract: the SSE
	// heartbeat period (handlers.refreshHeartbeatInterval), the per-write
	// deadline (a client that cannot accept one frame within it is gone),
	// and the pending-drain stall bound (a subscriber whose consumer made no
	// progress for it is force-resynced, #484). One interval, not three
	// knobs: each is "the client did not keep up with the stream's own
	// heartbeat". 20s beats the server IdleTimeout (30s) with margin.
	RefreshLivenessInterval = 20 * time.Second
)

// refreshPendingStallBound is the #484 overflow rule's bound. A var only so
// the force-resync arms can shorten it (SetRefreshPendingStallBoundForTest);
// production never reassigns it.
var refreshPendingStallBound atomic.Int64

func init() { refreshPendingStallBound.Store(int64(RefreshLivenessInterval)) }

// SetRefreshPendingStallBoundForTest shortens the stall bound for an arm and
// returns the restore func. Test-only — production MUST NOT call it.
func SetRefreshPendingStallBoundForTest(d time.Duration) (restore func()) {
	prev := refreshPendingStallBound.Swap(int64(d))
	return func() { refreshPendingStallBound.Store(prev) }
}

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

// refreshSub is one SSE connection's sink. ch is buffered; a full channel
// DEFERS on both paths (#484: the refresher path used to drop) — the producer
// never blocks on a slow consumer. keys is the set of l1Keys this connection
// is armed for; it is consulted under the hub mu.
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

	// #484 refresh pending set (guarded by pmu): refresher-path keys a full
	// sink deferred, dedup'd, FIFO in rorder (same invariant as pending /
	// order). NOT bucket-paced. Subset of keys.
	rpending  map[string]struct{}
	rorder    []string
	rdraining bool // a refresh drain goroutine is live for this sub
	// resynced: the stall rule fired; nothing more is queued or sent and
	// resync is closed so the handler ends the stream (the SPA re-validates
	// on reconnect).
	resynced bool
	resync   chan struct{}

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

	// coalesce state (#484 trailing edge): one window per ARMED key with an
	// emit inside the last coalesce interval, guarded by cmu (a separate lock
	// so coalescing never contends with the fan-out RLock). Lock order:
	// h.mu -> cmu (disarmLocked tears a window down under h.mu); nothing
	// takes h.mu while holding cmu. stopped is set at hub reset: every window
	// is stopped and a late timer fire is inert.
	cmu     sync.Mutex
	windows map[string]*coalesceWindow
	stopped bool
}

// coalesceWindow is one key's open coalesce window. Exactly one timer is
// armed per window; suppressed records that a signal arrived inside it, so
// its end owes exactly one trailing emit.
type coalesceWindow struct {
	timer      *time.Timer
	suppressed bool
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
			subs:    map[uint64]*refreshSub{},
			keySubs: map[string]map[uint64]*refreshSub{},
			windows: map[string]*coalesceWindow{},
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
			// Nobody is armed for the key any more: its coalesce window has
			// no one to owe a trailing emit to (lock order h.mu -> cmu).
			h.stopWindow(l1Key)
		}
	}
}

// coalesced reports whether an emit for l1Key should be suppressed because a
// coalesce window for the SAME key is open (the previous emit was within the
// window). A suppressed signal marks the window so its end emits exactly one
// trailing signal (#484 — a coalesced emit announces a NEWER commit, so it is
// not a duplicate). A non-suppressed emit opens a window. A window <= 0
// disables coalescing entirely (always false), and a key nobody is armed for
// gets no window (there is no fan-out to bound and nobody to owe a trailing
// emit to), so live windows <= armed keys. Design §2.3.
func (h *refreshBroadcaster) coalesced(l1Key string) bool {
	win := refreshCoalesceWindowFn()
	if win <= 0 {
		return false
	}
	h.mu.RLock()
	armed := len(h.keySubs[l1Key]) > 0
	h.mu.RUnlock()
	if !armed {
		return false
	}
	h.cmu.Lock()
	defer h.cmu.Unlock()
	if h.stopped {
		return false
	}
	if w := h.windows[l1Key]; w != nil {
		w.suppressed = true
		return true
	}
	w := &coalesceWindow{}
	w.timer = time.AfterFunc(win, func() { h.windowEnd(l1Key, w) })
	h.windows[l1Key] = w
	return false
}

// windowEnd closes l1Key's window. A window that saw a suppressed signal
// emits exactly ONE trailing signal and opens a fresh window (so a key that
// keeps changing settles at one emit per window); one that saw none is
// released. A fire for a window that was torn down meanwhile (last disarm,
// eviction, hub reset) is inert: identity check under cmu, and fan-out goes
// through the live reverse index, which no longer holds an unsubscribed sink.
func (h *refreshBroadcaster) windowEnd(l1Key string, w *coalesceWindow) {
	h.cmu.Lock()
	if h.stopped || h.windows[l1Key] != w {
		h.cmu.Unlock()
		return
	}
	if !w.suppressed {
		delete(h.windows, l1Key)
		h.cmu.Unlock()
		return
	}
	w.suppressed = false
	if win := refreshCoalesceWindowFn(); win > 0 {
		w.timer = time.AfterFunc(win, func() { h.windowEnd(l1Key, w) })
	} else {
		delete(h.windows, l1Key)
	}
	h.cmu.Unlock()
	refreshTrailingEmittedTotal.Add(1)
	h.fanoutRefresh(l1Key) // no cmu held: lock order h.mu -> cmu
}

// stopWindow tears l1Key's coalesce window down (timer stopped; a fire
// already in flight is inert via windowEnd's identity check). Caller may hold
// h.mu (lock order h.mu -> cmu).
func (h *refreshBroadcaster) stopWindow(l1Key string) {
	h.cmu.Lock()
	if w := h.windows[l1Key]; w != nil {
		w.timer.Stop()
		delete(h.windows, l1Key)
	}
	h.cmu.Unlock()
}

// PublishRefresh announces that l1Key was just committed to L1. Non-blocking:
// it fans the key out to every connection armed for it; a full sink DEFERS
// the key into that connection's refresh pending set (#484) rather than
// blocking the refresher goroutine or dropping. No-op when the layer is
// disabled or no hub exists.
//
// Called from internal/handlers/dispatchers/resolve_populate.go,
// immediately after the commit — strictly post-commit, so it fires only when
// L1 actually changed (design §1.1) — and from the cold-dispatch
// publishIfSubscribed. NOT paced: a refresher-driven refetch is an L1 HIT
// (feedback_customer_priority_over_refresher — pacing the eviction path must
// never slow this one).
func PublishRefresh(l1Key string) {
	h := refreshHub()
	if h == nil {
		return // disabled / cache-off — transparent no-op
	}
	if h.coalesced(l1Key) {
		refreshCoalescedTotal.Add(1)
		return
	}
	h.fanoutRefresh(l1Key)
}

// fanoutRefresh offers l1Key to every subscriber armed for it. C11: visit
// ONLY the subscribers armed for this key (the reverse index), not every live
// connection. fanoutVisitsForTest is the S2 discriminating instrument — an
// absolute count of subscribers examined, which an O(|subs|) loop cannot keep
// small. Holds only the RLock (+ each sub's pmu, briefly); no I/O and no
// blocking send.
func (h *refreshBroadcaster) fanoutRefresh(l1Key string) {
	h.mu.RLock()
	for _, s := range h.keySubs[l1Key] {
		fanoutVisitsForTest.Add(1)
		s.offerRefresh(l1Key)
	}
	h.mu.RUnlock()
	refreshPublishedTotal.Add(1)
}

// offerRefresh hands one committed key to this subscriber: straight into the
// sink when it has room and no refresh backlog is draining, otherwise into
// the dedup'd refresh pending set (never dropped, #484). A key already queued
// for this connection — refresh-pending, or eviction-pending (the eviction
// frame is the same `refresh` frame and supersedes it) — is absorbed: one
// frame per key per backlog. Caller holds h.mu (read); takes s.pmu (lock
// order h.mu -> s.pmu). The send is non-blocking, so pmu is never held
// across a blocking operation.
func (s *refreshSub) offerRefresh(l1Key string) {
	s.pmu.Lock()
	defer s.pmu.Unlock()
	if s.closed || s.resynced {
		return
	}
	_, rdup := s.rpending[l1Key]
	_, edup := s.pending[l1Key]
	if rdup || edup {
		refreshPendingCoalescedTotal.Add(1)
		return
	}
	// While a backlog drains every new key joins the queue (FIFO fairness).
	if !s.rdraining {
		select {
		case s.ch <- l1Key:
			refreshDeliveredTotal.Add(1)
			s.delivered.Add(1)
			noteSinkDepth(len(s.ch))
			return
		default:
			// Full sink: defer — never drop the key's last change (#484).
		}
	}
	s.rpending[l1Key] = struct{}{}
	s.rorder = append(s.rorder, l1Key)
	refreshDeferredTotal.Add(1)
	noteRefreshPendingHighWater(len(s.rpending))
	if !s.rdraining {
		s.rdraining = true
		go s.drainRefreshes()
	}
}

// drainRefreshes releases this subscriber's deferred refresh keys in FIFO
// order as soon as the sink has room (un-paced), then exits (a new backlog
// starts a new goroutine). It takes only s.pmu, never h.mu, and blocks only
// on its own retry timer / done — every send is non-blocking under pmu with
// closed==false, so nothing is sent after unsub.
//
// The overflow rule (#484): if the consumer makes NO progress — no frame of
// any kind leaves this subscriber's sink (delivered does not move) — for the
// stall bound (RefreshLivenessInterval), the subscriber is force-resynced.
func (s *refreshSub) drainRefreshes() {
	var stalledSince time.Time
	var seen uint64
	for {
		s.pmu.Lock()
		if s.closed || s.resynced {
			s.rdraining = false
			s.pmu.Unlock()
			return
		}
		for len(s.rorder) > 0 {
			if _, live := s.rpending[s.rorder[0]]; live {
				break
			}
			s.rorder = s.rorder[1:] // disarmed / superseded by an eviction
		}
		if len(s.rorder) == 0 {
			s.rdraining = false
			s.rorder = nil
			s.pmu.Unlock()
			return
		}
		l1Key := s.rorder[0]
		select {
		case s.ch <- l1Key:
			s.rorder = s.rorder[1:]
			delete(s.rpending, l1Key)
			refreshDeliveredTotal.Add(1)
			s.delivered.Add(1)
			noteSinkDepth(len(s.ch))
			stalledSince = time.Time{}
			s.pmu.Unlock()
			continue
		default:
		}
		now := time.Now()
		if d := s.delivered.Load(); stalledSince.IsZero() || d != seen {
			stalledSince, seen = now, d // first miss, or the consumer moved
		} else if now.Sub(stalledSince) >= time.Duration(refreshPendingStallBound.Load()) {
			s.forceResyncLocked()
			s.rdraining = false
			s.pmu.Unlock()
			return
		}
		s.pmu.Unlock()
		if !s.sleepOrDone(drainFullSinkRetry) {
			return
		}
	}
}

// forceResyncLocked applies the #484 overflow rule: the subscriber's consumer
// is wedged, so instead of holding its backlog indefinitely it is released —
// BOTH pending sets are emptied, nothing more is queued for this
// subscriber, and resync is closed so the handler writes `event: resync` and
// ends the stream. The SPA treats the end as a transport loss and, on the
// reconnect, re-fetches every armed widget (refreshSse.ts scheduleRetry sets
// revalidateOnConnect; connect() runs revalidateArmed), so every released key
// is repaired, none is silently lost. Caller holds s.pmu.
func (s *refreshSub) forceResyncLocked() {
	if s.resynced || s.closed {
		return
	}
	s.resynced = true
	s.rpending = nil
	s.rorder = nil
	s.pending = nil
	s.order = nil
	close(s.resync)
	refreshPendingOverflowResyncTotal.Add(1)
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
	// #484: the eviction frame supersedes any trailing refresh the key's
	// coalesce window still owes (the entry is gone; one frame says so).
	h.stopWindow(l1Key)
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
	if s.closed || s.resynced {
		return
	}
	// #484: an eviction supersedes a deferred refresh of the same key — both
	// are the same `refresh` frame, so the connection gets ONE.
	delete(s.rpending, l1Key)
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
		if s.closed || s.resynced {
			s.draining = false
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
	if s.closed || s.resynced {
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

// RefreshStream is one subscription: Keys carries the l1Keys to signal;
// Resync is closed when the #484 overflow rule fires (the handler then writes
// `event: resync` and ends the stream so the SPA re-validates every armed
// widget on reconnect); Unsub is idempotent and the handler MUST defer it.
type RefreshStream struct {
	Keys   <-chan string
	Resync <-chan struct{}
	Unsub  func()
}

// SubscribeRefresh is SubscribeRefreshStream without the resync channel —
// the shape the in-process falsifiers read. The /refreshes handler uses
// SubscribeRefreshStream.
func SubscribeRefresh(armedKeys map[string]struct{}) (<-chan string, func()) {
	st := SubscribeRefreshStream(armedKeys)
	return st.Keys, st.Unsub
}

// SubscribeRefreshStream registers a connection armed for the given validated
// l1Key set (the caller — handlers.Refreshes — has already re-derived these
// keys under the connection's authenticated identity; §5).
//
// When the layer is disabled it returns a closed Keys channel, a nil Resync
// (never fires) and a no-op unsub, so the handler degrades to a clean idle
// stream (transparent fallback).
func SubscribeRefreshStream(armedKeys map[string]struct{}) RefreshStream {
	h := refreshHub()
	if h == nil {
		ch := make(chan string)
		close(ch)
		return RefreshStream{Keys: ch, Unsub: func() {}}
	}
	limit, burst := refreshEvictionBucket()
	s := &refreshSub{
		hub:          h,
		keys:         make(map[string]struct{}, len(armedKeys)),
		ch:           make(chan string, refreshSubChanCap),
		subscribedAt: time.Now(),
		limiter:      rate.NewLimiter(limit, burst),
		pending:      map[string]struct{}{},
		rpending:     map[string]struct{}{},
		done:         make(chan struct{}),
		resync:       make(chan struct{}),
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
	return RefreshStream{Keys: s.ch, Resync: s.resync, Unsub: unsub}
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
	s.rpending = nil
	s.rorder = nil
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
	delete(s.pending, l1Key)  // nil-safe
	delete(s.rpending, l1Key) // #484: the refresh pending set never outgrows the armed set either
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
	// refreshDroppedTotal counted (key -> subscriber) sends dropped on a full
	// sink. Structurally 0 since #484 — a full sink DEFERS on both paths
	// (refreshDeferredTotal); nothing increments it. Kept so the series and
	// RefreshBroadcasterCounters' shape survive; a nonzero value would be a
	// regression of the never-drop invariant.
	refreshDroppedTotal atomic.Uint64
	// refreshDeferredTotal counts refresher-path (key -> subscriber) signals
	// a full sink deferred into the subscriber's refresh pending set (#484).
	refreshDeferredTotal atomic.Uint64
	// refreshTrailingEmittedTotal counts trailing-edge emits: windows that
	// had a suppressed signal and re-announced the key at their end (#484).
	refreshTrailingEmittedTotal atomic.Uint64
	// refreshPendingCoalescedTotal counts refresher-path signals absorbed
	// because the key was already queued for that connection (refresh- or
	// eviction-pending) — delivered once, not lost (#484).
	refreshPendingCoalescedTotal atomic.Uint64
	// refreshPendingOverflowResyncTotal counts subscribers force-resynced by
	// the #484 overflow (stall) rule: stream ended with `event: resync`, the
	// SPA re-validates every armed widget on reconnect.
	refreshPendingOverflowResyncTotal atomic.Uint64
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
	// refreshPendingHighWaterForTest is the largest per-subscriber REFRESH
	// pending set observed (#484: never exceeds the armed set).
	refreshPendingHighWaterForTest atomic.Int64
)

func noteRefreshPendingHighWater(n int) {
	d := int64(n)
	for {
		cur := refreshPendingHighWaterForTest.Load()
		if d <= cur || refreshPendingHighWaterForTest.CompareAndSwap(cur, d) {
			return
		}
	}
}

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
	// 1.12.6 C7: the `stat` tag is the /debug/vars key; `kind:"gauge"` marks a
	// current value (everything else is a monotonic counter). The OTLP
	// instrument name is derived: snowplow_refresh_broadcaster_<stat>, with
	// "_total" appended to a counter that does not already carry it
	// (StatFamily.OTelInstrumentName). Expvar, OTLP, docs guard and parity
	// arm all read these tags — nothing here is copied by hand.
	Published             uint64  `stat:"published" desc:"Live-refresh signals published."`
	Delivered             uint64  `stat:"delivered" desc:"Live-refresh signals delivered to subscribers."`
	Dropped               uint64  `stat:"dropped" desc:"Live-refresh signals dropped on a full sink. Structurally 0 since #484 (a full sink defers, see deferred); nonzero is a regression."`
	Deferred              uint64  `stat:"deferred" desc:"Refresher-path signals a full subscriber sink deferred into its refresh pending set (never dropped, #484)."`
	TrailingEmitted       uint64  `stat:"trailing_emitted" desc:"Trailing-edge emits: coalesce windows that had a suppressed signal re-announced the key at window end (#484)."`
	PendingCoalesced      uint64  `stat:"pending_coalesced" desc:"Refresher-path signals absorbed because the key was already queued for that subscriber (delivered once, #484)."`
	PendingOverflowResync uint64  `stat:"pending_overflow_resync" desc:"Subscribers force-resynced by the pending-drain stall rule: stream ended with a resync frame so the SPA re-validates every armed widget (#484)."`
	Coalesced             uint64  `stat:"coalesced" desc:"Live-refresh signals coalesced."`
	Subscribers           int     `stat:"subscribers" kind:"gauge" desc:"Current live-refresh subscriber count."`
	ArmedKeys             int     `stat:"armed_keys" kind:"gauge" desc:"Distinct L1 keys with at least one armed live-refresh subscriber (reverse-index size)."`
	MaxSinkDepth          int64   `stat:"max_sink_depth" kind:"gauge" desc:"High-water mark of a subscriber sink after a send (consumer lag, 0..64)."`
	EvictPublished        uint64  `stat:"evict_published" desc:"Eviction-driven live-refresh publishes that reached at least one subscriber."`
	EvictDeferred         uint64  `stat:"evict_deferred" desc:"Eviction-driven signals deferred by the per-subscriber token bucket (the bound engaged)."`
	StreamSecondsTotal    float64 `stat:"stream_seconds_total" desc:"Accumulated /refreshes stream lifetime in seconds (divide by streams_closed_total for the mean)."`
	StreamsClosedTotal    uint64  `stat:"streams_closed_total" desc:"/refreshes streams that ended."`
}

// refreshBroadcasterStatsOverride, when set, replaces the live snapshot.
// TEST-ONLY seam for the 1.12.6 C7 parity arms; nil in production.
var refreshBroadcasterStatsOverride atomic.Pointer[RefreshBroadcasterStats]

// SetRefreshBroadcasterStatsForTest installs (or, with nil, clears) a
// snapshot override. Production callers MUST NOT use it.
func SetRefreshBroadcasterStatsForTest(s *RefreshBroadcasterStats) {
	refreshBroadcasterStatsOverride.Store(s)
}

// RefreshBroadcasterStatsSnapshot reads every broadcaster gauge/counter at
// once (one RLock for the two hub-size gauges, atomics for the rest).
func RefreshBroadcasterStatsSnapshot() RefreshBroadcasterStats {
	if o := refreshBroadcasterStatsOverride.Load(); o != nil {
		return *o
	}
	st := RefreshBroadcasterStats{
		Published:             refreshPublishedTotal.Load(),
		Delivered:             refreshDeliveredTotal.Load(),
		Dropped:               refreshDroppedTotal.Load(),
		Coalesced:             refreshCoalescedTotal.Load(),
		Deferred:              refreshDeferredTotal.Load(),
		TrailingEmitted:       refreshTrailingEmittedTotal.Load(),
		PendingCoalesced:      refreshPendingCoalescedTotal.Load(),
		PendingOverflowResync: refreshPendingOverflowResyncTotal.Load(),
		MaxSinkDepth:          refreshMaxSinkDepth.Load(),
		EvictPublished:        refreshEvictPublishedTotal.Load(),
		EvictDeferred:         refreshEvictDeferredTotal.Load(),
		StreamSecondsTotal:    float64(refreshStreamNanosTotal.Load()) / float64(time.Second),
		StreamsClosedTotal:    refreshStreamsClosedTotal.Load(),
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
	// #484: the refresh pending set's size and whether the stall rule fired.
	RefreshPending int
	Resynced       bool
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
			Armed:          len(s.keys),
			Pending:        len(s.pending),
			Delivered:      s.delivered.Load(),
			EvictDeferred:  s.evictDeferred.Load(),
			RefreshPending: len(s.rpending),
			Resynced:       s.resynced,
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

// RefreshPendingHighWaterForTest returns the largest per-subscriber REFRESH
// pending set observed since the last reset (#484). Test-only.
func RefreshPendingHighWaterForTest() int64 {
	return refreshPendingHighWaterForTest.Load()
}

// RefreshCoalesceWindowsForTest returns the number of open coalesce windows —
// each holds exactly one armed timer, so this IS the armed-timer count the
// #484 lifecycle arms bound (a leak check that does not lean on goroutine
// counts). Test-only.
func RefreshCoalesceWindowsForTest() int {
	h := refreshHub()
	if h == nil {
		return 0
	}
	h.cmu.Lock()
	defer h.cmu.Unlock()
	return len(h.windows)
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
		// #484: stop every coalesce window; a timer already firing is inert.
		old.cmu.Lock()
		old.stopped = true
		for k, w := range old.windows {
			w.timer.Stop()
			delete(old.windows, k)
		}
		old.cmu.Unlock()
	}
	refreshPublishedTotal.Store(0)
	refreshDeliveredTotal.Store(0)
	refreshDroppedTotal.Store(0)
	refreshCoalescedTotal.Store(0)
	refreshDeferredTotal.Store(0)
	refreshTrailingEmittedTotal.Store(0)
	refreshPendingCoalescedTotal.Store(0)
	refreshPendingOverflowResyncTotal.Store(0)
	refreshPendingHighWaterForTest.Store(0)
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
