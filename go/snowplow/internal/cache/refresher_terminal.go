// refresher_terminal.go — 1.12.6 C4: terminal semantics for every refresh
// outcome (design-1.12.6-event-hardening §6).
//
// THE RULE. No refresh outcome may end in "forget the key and keep the
// entry". Every path ends in exactly one of:
//
//   - re-Put    fresh bytes stored (the success path, unchanged);
//   - evict     the entry's basis is gone or unverifiable — the self-object
//     404 (1.12.5) and, from 1.12.6, EVERY deterministic failure
//     that exhausts the requeue budget (403 / 500 / timeout /
//     parse failure / apistage not-servable), behind the breaker
//     below;
//   - suppress  the key is explicitly marked refresh-by-traffic-only,
//     counted, and cleared by the next real Put.
//
// Before 1.12.6 the drop point (processNext) Forgot a non-404 key and left the
// stale body resident until TTL, and the three decline gates in
// resolveAndPopulateL1 re-resolved the same key every ~3 min forever (#191:
// 956 WARNs from 23 keys in 4.5 h).
//
// THE BREAKER (§6.3). An apiserver outage fails every refresh; after the
// budget every dirty-marked key reaches the drop point, and evicting them all
// is a cold-cache storm on top of an outage. REFRESH_DROP_EVICT_MAX_PER_MINUTE
// (default "64") is a token bucket on drop-point evictions: over budget the
// key keeps today's drop-to-TTL, refresh_drop_evict_suspended_total ticks and
// ONE WARN per suspension window names the suspected outage. "0" disables
// drop-eviction for non-404 failures entirely (today's behaviour byte-for-
// byte). The breaker is NOT a rate limit: it is the discriminator between a
// correctness regime (a handful of deterministic failures — evicting is right)
// and an availability regime (a mass failure — evicting would turn a stale
// portal into a broken one).
//
// SUPPRESSION (§6.4, #191). A per-key marker held HERE (package state), never
// on the resident *ResolvedEntry (Get returns the live pointer; mutating it is
// a data race). Set at each decline site after K consecutive declines
// (REFRESH_SUPPRESS_AFTER_DECLINES, default "3"; the structurally-permanent
// declines — external endpoint, UAF cell, unsupported handler kind — use K=1).
// Consulted in processNext right after the entry Get and before the rate
// floor: a suppressed key is Forgotten and skipped — no resolve, no WARN.
// Cleared by ResolvedCacheStore.Put (a real Put is by definition the "next
// real Put" the suppression waits for) and by every store eviction, so the
// marker cannot outlive its key.
//
// Package-level rather than on the refresher struct so that the store's Put /
// eviction path can clear a marker WITHOUT constructing the refresher
// singleton (whose workqueues spawn goroutines — the C1 N1 off-path lesson).
package cache

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// envRefreshDropEvictMaxPerMinute — the drop-point eviction breaker.
	// String-typed (installer plumbing is string-only); unset/unparseable →
	// default; "0" disables drop-eviction for non-404 failures.
	envRefreshDropEvictMaxPerMinute     = "REFRESH_DROP_EVICT_MAX_PER_MINUTE"
	defaultRefreshDropEvictMaxPerMinute = 64

	// envRefreshSuppressAfterDeclines — consecutive stage-error declines of
	// one key before it is marked refresh-by-traffic-only.
	envRefreshSuppressAfterDeclines     = "REFRESH_SUPPRESS_AFTER_DECLINES"
	defaultRefreshSuppressAfterDeclines = 3
)

// ErrRefreshUnsupported — item 4 row 4: the resolver seam has no handler for
// the entry's kind. It never will for this entry, so the decline site
// suppresses the key (K=1) instead of skipping it to TTL forever.
var ErrRefreshUnsupported = errors.New("refresh unsupported for this entry kind")

// ErrContentNotServable — item 4 row 9: an apistage CONTENT entry whose single
// K8s call is not pivot-servable right now (pre-sync informer, metadata-only
// GVR). Pre-1.12.6 this was a (nil, nil) skip-to-TTL with no 404
// discrimination at all — the one carrier with a single layer where every
// other class has two. Typed, it enters the normal requeue budget and reaches
// the drop point like any other deterministic failure.
var ErrContentNotServable = errors.New("apistage content call not servable")

// RefreshDropEvictMaxPerMinute returns the breaker budget; 0 = drop-eviction
// disabled for non-404 failures.
func RefreshDropEvictMaxPerMinute() int {
	n := intFromEnv(envRefreshDropEvictMaxPerMinute, defaultRefreshDropEvictMaxPerMinute)
	if n < 0 {
		return defaultRefreshDropEvictMaxPerMinute
	}
	return n
}

// RefreshSuppressAfterDeclines returns K, the consecutive-decline threshold
// for the stage-error suppression; never below 1.
func RefreshSuppressAfterDeclines() int {
	n := intFromEnv(envRefreshSuppressAfterDeclines, defaultRefreshSuppressAfterDeclines)
	if n < 1 {
		return defaultRefreshSuppressAfterDeclines
	}
	return n
}

// dropEvictBreaker is a token bucket: capacity == refill-per-minute == the
// knob. One token per drop-point eviction; refill is continuous.
type dropEvictBreaker struct {
	mu         sync.Mutex
	tokens     float64
	last       time.Time
	perMinute  int
	suspended  bool // inside a suspension window (one WARN per window)
	nowFn      func() time.Time
	warnTotal  atomic.Uint64 // suspension-window WARNs emitted (one per window)
	suspTotal  atomic.Uint64 // refresh_drop_evict_suspended_total
	evictTotal atomic.Uint64 // refresh_drop_evict_total (non-404 drop-point evictions)
}

func newDropEvictBreaker(perMinute int) *dropEvictBreaker {
	b := &dropEvictBreaker{perMinute: perMinute, nowFn: time.Now}
	b.tokens = float64(perMinute)
	b.last = b.nowFn()
	return b
}

// allow consumes one token when available. It returns (allowed, firstSuspend):
// firstSuspend is true exactly once per suspension window — the caller emits
// the WARN then, and the window closes the next time a token is granted.
func (b *dropEvictBreaker) allow() (allowed bool, firstSuspend bool) {
	if b.perMinute <= 0 {
		return false, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.nowFn()
	elapsed := now.Sub(b.last).Minutes()
	if elapsed > 0 {
		b.tokens += elapsed * float64(b.perMinute)
		if b.tokens > float64(b.perMinute) {
			b.tokens = float64(b.perMinute)
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		b.suspended = false
		b.evictTotal.Add(1)
		return true, false
	}
	b.suspTotal.Add(1)
	if !b.suspended {
		b.suspended = true
		b.warnTotal.Add(1)
		return false, true
	}
	return false, false
}

// --- suppression ---------------------------------------------------------

type suppressState struct {
	reason   string
	declines int
}

var (
	// refreshSuppressed: l1Key -> reason. Presence == suppressed.
	refreshSuppressed sync.Map
	// refreshDeclines: l1Key -> *suppressState (consecutive-decline counter).
	refreshDeclines sync.Map

	refreshSuppressedSetTotal   atomic.Uint64 // keys marked suppressed
	refreshSuppressedSkipsTotal atomic.Uint64 // dequeues skipped because suppressed
	refreshDeclineNotedTotal    atomic.Uint64 // decline sites reached (all reasons)
)

// NoteRefreshDecline records one decline of l1Key at a refresh decline site.
// permanent == true marks the key immediately (structurally permanent
// declines: external endpoint, UAF cell, unsupported kind); otherwise the key
// is marked after REFRESH_SUPPRESS_AFTER_DECLINES consecutive declines. It
// returns true when the key is (now) suppressed. Idempotent for an already
// suppressed key.
func NoteRefreshDecline(l1Key, reason string, permanent bool) bool {
	if l1Key == "" {
		return false
	}
	refreshDeclineNotedTotal.Add(1)
	if _, already := refreshSuppressed.Load(l1Key); already {
		return true
	}
	v, _ := refreshDeclines.LoadOrStore(l1Key, &suppressState{})
	st := v.(*suppressState)
	// The per-key decline counter is only ever touched by the refresher
	// worker that dequeued this key (one dequeue in flight per key, the
	// workqueue's contract), so a plain increment is race-free.
	st.declines++
	st.reason = reason
	if permanent || st.declines >= RefreshSuppressAfterDeclines() {
		refreshSuppressed.Store(l1Key, reason)
		refreshDeclines.Delete(l1Key)
		refreshSuppressedSetTotal.Add(1)
		return true
	}
	return false
}

// RefreshSuppressedReason reports whether l1Key is suppressed and why.
func RefreshSuppressedReason(l1Key string) (string, bool) {
	v, ok := refreshSuppressed.Load(l1Key)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// clearRefreshSuppression forgets both the marker and the consecutive-decline
// counter for l1Key. Called by the store on every Put (the "next real Put")
// and on every eviction (the marker must not outlive its key). Cheap: two
// sync.Map deletes, no allocation.
func clearRefreshSuppression(l1Key string) {
	refreshSuppressed.Delete(l1Key)
	refreshDeclines.Delete(l1Key)
}

// resetRefreshTerminalForTest clears the suppression state and counters.
// Called from resetRefresherForTest.
func resetRefreshTerminalForTest() {
	refreshSuppressed.Range(func(k, _ any) bool { refreshSuppressed.Delete(k); return true })
	refreshDeclines.Range(func(k, _ any) bool { refreshDeclines.Delete(k); return true })
	refreshSuppressedSetTotal.Store(0)
	refreshSuppressedSkipsTotal.Store(0)
	refreshDeclineNotedTotal.Store(0)
}

// RefreshTerminalStats is the read-only snapshot of the C4 counters (expvar
// + OTLP + tests).
type RefreshTerminalStats struct {
	DropEvictTotal          uint64 // non-404 drop-point evictions performed
	DropEvictSuspendedTotal uint64 // drop-point evictions refused by the breaker
	DropEvictSuspendWarns   uint64 // suspension windows entered (one WARN each)
	DropEvictMaxPerMinute   int
	SuppressedSetTotal      uint64
	SuppressedSkipsTotal    uint64
	DeclineNotedTotal       uint64
	SuppressedKeys          int
	SuppressAfterDeclines   int
}

// RefreshTerminalStatsSnapshot reads the counters. It does NOT construct the
// refresher singleton: with no refresher (cache-off, or before the first
// enqueue) the breaker counters read zero.
func RefreshTerminalStatsSnapshot() RefreshTerminalStats {
	s := RefreshTerminalStats{
		DropEvictMaxPerMinute: RefreshDropEvictMaxPerMinute(),
		SuppressedSetTotal:    refreshSuppressedSetTotal.Load(),
		SuppressedSkipsTotal:  refreshSuppressedSkipsTotal.Load(),
		DeclineNotedTotal:     refreshDeclineNotedTotal.Load(),
		SuppressAfterDeclines: RefreshSuppressAfterDeclines(),
	}
	refreshSuppressed.Range(func(_, _ any) bool { s.SuppressedKeys++; return true })
	if r := refresherInstance; r != nil && r.dropEvict != nil {
		s.DropEvictTotal = r.dropEvict.evictTotal.Load()
		s.DropEvictSuspendedTotal = r.dropEvict.suspTotal.Load()
		s.DropEvictSuspendWarns = r.dropEvict.warnTotal.Load()
	}
	return s
}

// warnDropEvictSuspended is the one-per-window WARN.
func warnDropEvictSuspended(key string, perMinute int, fromCL bool) {
	slog.Warn("refresher.drop_evict_suspended",
		slog.String("subsystem", "cache"),
		slog.String("key_hash", key),
		slog.Int("max_per_minute", perMinute),
		slog.Bool("cluster_list_tier", fromCL),
		slog.String("effect", "more deterministic refresh failures than the breaker budget in one minute — "+
			"a mass failure (apiserver outage?) rather than a few dead objects; over-budget keys keep "+
			"today's drop-to-TTL and stay SERVED instead of being evicted into a cold-cache storm"),
	)
}
