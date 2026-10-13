// known_stale.go — #547: the serve-side half of the KNOWN-STALE state.
//
// THE INVARIANT. A resident cell whose latest dirty mark has not been closed by
// an accepted re-Put is KNOWN-STALE, and that fact outlives the failed refresh
// (dirty_window.go keeps the stamp as {t0, stale: <reason>}). This file is what
// the serve path does with it:
//
//   - DETECT. Every customer hit that delivers a known-stale body is counted on
//     known_stale_served_total{reason} inside ResolvedCacheStore.Get, the one
//     funnel every hit_total-counted hit goes through. The counter is bumped by
//     the very hits that deliver the stale body, so it cannot read zero while the
//     defect is happening: a user reloading a frozen card 43 times moves it by 43.
//     known_stale_resident is the gauge of cells currently in the state.
//
//   - REVALIDATE ON DEMAND (identity-bound classes). The hit still serves the
//     body — zero cold navigations — and hands the key to ONE singleflighted
//     revalidation per (key, t0) that re-runs the customer's own cold path under
//     the REQUESTER: their identity and their own credential, the read that is
//     known to work (the cold read is right where the background refresh fails —
//     #547 trace row 7). It runs DETACHED from the request (never blocks the
//     response) under a bounded deadline: DemandRevalidationDeadline, the
//     server's own write deadline, which is the longest any customer resolve may
//     run — no new knob. An accepted write closes the window as a real refresh
//     does (a dirty→fresh sample) and the cold path's own publishIfSubscribed
//     announces it over SSE. It bypasses refresh suppression (a demand is not the
//     background decline loop). If it fails (owner ruling 1, #547): the body
//     keeps being served and counted, the slot is released, and the next read or
//     change retries. Never an eviction.
//
//   - READ THROUGH (substrate cells). An identity-free content cell (apistage
//     content, the widgetContent shell) and the identity-bound RAFullList are
//     read AS INPUT by other resolves; a stale input would be baked into a new,
//     fresh-looking cell. A known-stale substrate cell is therefore never served:
//     the reader treats it as a MISS and takes its existing fall-through (owner
//     ruling 3: one apiserver call per read while the informer is not servable).
//     Counted on known_stale_read_through_total.
//
// No TTL, no new deletion, no knob, no class or resource special case: the
// served-vs-read-through split is by the READER's role (top-level customer serve
// vs resolve input), and the reasons are the closed set below.
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// knownStaleReason is why a cell is known-stale (the closed set).
type knownStaleReason uint8

const (
	knownStaleNone knownStaleReason = iota
	// knownStaleDeclined — a refresh returned without a Put (stage error,
	// empty full, external / UAF / sensitive gate, no Put outcome).
	knownStaleDeclined
	// knownStaleDropped — the poison-pill bound gave up on the key.
	knownStaleDropped
	// knownStaleSuppressed — the dequeue was skipped: the key is suppressed
	// (a structurally-permanent decline).
	knownStaleSuppressed
	// knownStaleNotServable — the dropped refresh failed because its content
	// call was not pivot-servable (ErrContentNotServable).
	knownStaleNotServable
	knownStaleReasons
)

// KnownStaleReasons is the CLOSED `reason` attribute set of
// snowplow_resolved_cache_known_stale_served_total.
var KnownStaleReasons = []string{"declined", "dropped", "suppressed", "not_servable"}

func (r knownStaleReason) String() string {
	if r == knownStaleNone || r >= knownStaleReasons {
		return ""
	}
	return KnownStaleReasons[r-1]
}

var (
	knownStaleServed            [knownStaleReasons]atomic.Uint64
	knownStaleReadThroughTotal  atomic.Uint64
	knownStaleRevalidationFresh atomic.Uint64
	knownStaleRevalidationFail  atomic.Uint64
)

// knownStaleStamp returns key's stamp when it is known-stale.
func knownStaleStamp(key string) (*dirtyStamp, bool) {
	r := refresherPeek()
	if r == nil || key == "" {
		return nil, false
	}
	v, ok := r.dirty.Load(key)
	if !ok {
		return nil, false
	}
	s := v.(*dirtyStamp)
	if s.stale == knownStaleNone {
		return nil, false
	}
	return s, true
}

// KnownStale reports whether key is known-stale: the start of its stale window
// and the reason.
func KnownStale(key string) (t0 time.Time, reason string, ok bool) {
	s, ok := knownStaleStamp(key)
	if !ok {
		return time.Time{}, "", false
	}
	return s.t0, s.stale.String(), true
}

// KnownStaleReadThrough is the substrate readers' gate (apistage content, the
// widgetContent shell, the RAFullList): true means key is known-stale and the
// reader must take its MISS fall-through instead of reading the cell. Counted.
func KnownStaleReadThrough(key string) bool {
	if _, ok := knownStaleStamp(key); !ok {
		return false
	}
	knownStaleReadThroughTotal.Add(1)
	return true
}

// noteKnownStaleServe counts one customer hit on a known-stale stamp.
func noteKnownStaleServe(s *dirtyStamp) {
	if s == nil || s.stale == knownStaleNone || s.stale >= knownStaleReasons {
		return
	}
	knownStaleServed[s.stale].Add(1)
}

// loadKnownStaleServed reads the served counters in KnownStaleReasons order.
func loadKnownStaleServed() (out [knownStaleReasons - 1]uint64) {
	for i := range out {
		out[i] = knownStaleServed[i+1].Load()
	}
	return out
}

// knownStaleResident counts the cells currently known-stale (a gauge; bounded
// by the resident key set, which bounds the dirty map).
func knownStaleResident() uint64 {
	r := refresherPeek()
	if r == nil {
		return 0
	}
	var n uint64
	r.dirty.Range(func(_, v any) bool {
		if v.(*dirtyStamp).stale != knownStaleNone {
			n++
		}
		return true
	})
	return n
}

// KnownStaleServedByReason returns the served counter per reason of the closed
// set (0 when never ticked).
func KnownStaleServedByReason() map[string]uint64 {
	out := make(map[string]uint64, len(KnownStaleReasons))
	for i, name := range KnownStaleReasons {
		out[name] = knownStaleServed[i+1].Load()
	}
	return out
}

// --- demand revalidation --------------------------------------------------

// demandInflight holds the one in-flight demand revalidation per key (value:
// the t0 it revalidates). Bounded by the known-stale key set.
var demandInflight sync.Map

type ctxKeyDemandRevalidationType struct{}

var ctxKeyDemandRevalidation = ctxKeyDemandRevalidationType{}

// ClaimDemandRevalidation claims key's single revalidation slot for the stale
// generation t0. false: one is already in flight.
func ClaimDemandRevalidation(key string, t0 time.Time) bool {
	if key == "" {
		return false
	}
	_, loaded := demandInflight.LoadOrStore(key, t0)
	return !loaded
}

// DemandRevalidationFromContext reports whether ctx is a demand revalidation
// (the re-entered customer cold path), and of which key.
func DemandRevalidationFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	k, ok := ctx.Value(ctxKeyDemandRevalidation).(string)
	return k, ok
}

// DemandRevalidationDeadline bounds one demand revalidation: the server's write
// deadline (the store's #189 tombstone window, which main.go sets from the
// http.Server WriteTimeout — the longest a customer resolve may run). The
// default applies when the window is unset or disabled.
func DemandRevalidationDeadline() time.Duration {
	if c := ResolvedCache(); c != nil {
		c.mu.Lock()
		d := c.tombstoneTTL
		c.mu.Unlock()
		if d > 0 {
			return d
		}
	}
	return time.Duration(defaultResolvedCacheTombstoneTTLSeconds) * time.Second
}

// BeginDemandRevalidation marks ctx as the demand revalidation of key (claimed
// with ClaimDemandRevalidation for t0) and installs the write-outcome sink the
// customer Put reports into (PutIfGen → NoteRefreshWrite). settle MUST be called
// once the revalidation returns: it releases the slot and, on an accepted
// un-remarked write, closes the known-stale window as fresh. It reports whether
// the cell is fresh.
func BeginDemandRevalidation(ctx context.Context, key string, t0 time.Time) (context.Context, func() bool) {
	ctx, o := withRefreshOutcome(ctx, key)
	ctx = context.WithValue(ctx, ctxKeyDemandRevalidation, key)
	var once sync.Once
	var fresh bool
	return ctx, func() bool {
		once.Do(func() {
			demandInflight.CompareAndDelete(key, t0)
			if o.accepted.Load() && !o.remarked.Load() {
				closeKnownStaleFresh(key, t0)
				knownStaleRevalidationFresh.Add(1)
				fresh = true
				return
			}
			knownStaleRevalidationFail.Add(1)
		})
		return fresh
	}
}

// closeKnownStaleFresh ends key's known-stale window t0 after an accepted demand
// write: the same close (and dirty→fresh sample) a refresher re-Put makes. A
// stamp that is in flight is left to its dequeue; a stamp re-opened for another
// t0 is not this generation's.
func closeKnownStaleFresh(key string, t0 time.Time) {
	r := refresherPeek()
	if r == nil {
		return
	}
	for {
		cur, ok := r.dirty.Load(key)
		if !ok {
			return
		}
		s := cur.(*dirtyStamp)
		if s.stale == knownStaleNone || s.inflight || !s.t0.Equal(t0) {
			return
		}
		if r.dirty.CompareAndDelete(key, cur) {
			r.sampleFresh(t0)
			return
		}
	}
}

// AddKnownStaleServedForTest moves the served counter of one reason of the
// closed set through its production atomic, so a cross-package arm can prove
// the OTLP mirror reads the LIVE counter. false: not a reason of the set.
// Production callers MUST NOT use this.
func AddKnownStaleServedForTest(reason string, n uint64) bool {
	for i, name := range KnownStaleReasons {
		if name == reason {
			knownStaleServed[i+1].Add(n)
			return true
		}
	}
	return false
}

// resetKnownStaleForTest clears the package counters and slots.
func resetKnownStaleForTest() {
	for i := range knownStaleServed {
		knownStaleServed[i].Store(0)
	}
	knownStaleReadThroughTotal.Store(0)
	knownStaleRevalidationFresh.Store(0)
	knownStaleRevalidationFail.Store(0)
	demandInflight.Range(func(k, _ any) bool { demandInflight.Delete(k); return true })
}
