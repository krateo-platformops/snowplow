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
//   - REVALIDATE IN THE BACKGROUND (the identity-free widget SHELL, owner
//     ruling): the shell is a SERVED body, so it is served instantly like a
//     per-cohort cell, and its revalidation is handed to the refresher under the
//     refresher's own identity for identity-free cells (RevalidateKnownStaleIn-
//     Background) — never resolved under the requester, never read through.
//
//   - READ THROUGH (substrate cells). An apistage content cell and the
//     identity-bound RAFullList are read AS INPUT by other resolves; a stale
//     input would be baked into a new, fresh-looking cell. A known-stale
//     substrate cell is therefore never served:
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
	// knownStaleRevalidationSkip counts reads of a known-stale cell whose key is
	// SUPPRESSED for a structurally-permanent reason (external endpoint, UAF,
	// unsupported kind, sensitive resource): the requester's cold path declines
	// the Put by the same gate, so a revalidation could never succeed — it is
	// skipped and counted instead (team ruling (b), #547).
	knownStaleRevalidationSkip atomic.Uint64
	// knownStaleRevalidationRerun counts the reruns an ABSORBED read earned
	// (#740), per slot of the closed set KnownStaleRevalidationSlots.
	knownStaleRevalidationRerun [revalSlotKinds]atomic.Uint64
)

// revalSlotKind indexes the two revalidation slots (#740).
type revalSlotKind uint8

const (
	revalSlotBackground revalSlotKind = iota
	revalSlotDemand
	revalSlotKinds
)

// KnownStaleRevalidationSlots is the CLOSED `slot` attribute set of
// snowplow_resolved_cache_known_stale_revalidation_rerun_total, in
// revalSlotKind order.
var KnownStaleRevalidationSlots = []string{"background", "demand"}

// KnownStaleRevalidationRerunBySlot returns the rerun counter per slot of the
// closed set (0 when never ticked).
func KnownStaleRevalidationRerunBySlot() map[string]uint64 {
	out := make(map[string]uint64, len(KnownStaleRevalidationSlots))
	for i, name := range KnownStaleRevalidationSlots {
		out[name] = knownStaleRevalidationRerun[i].Load()
	}
	return out
}

// loadKnownStaleRevalidationRerun reads the rerun counters in slot order.
func loadKnownStaleRevalidationRerun() (out [revalSlotKinds]uint64) {
	for i := range out {
		out[i] = knownStaleRevalidationRerun[i].Load()
	}
	return out
}

// revalSlot is one key's revalidation slot, held from the claim until the
// attempt settles. A read that finds it held is ABSORBED into that attempt and
// arms rerun (#740, architect ruling): the attempt may have read its inputs
// before the state that reader needs (the trace: it read the refresher
// identity's deny just before the identity recovered), and with no new change
// event nothing else would retry before the NEXT read, which on a low-traffic
// cell is unbounded. One rerun per settle with an absorbed read: driven by
// reads, never self-looping, no timer, no knob.
type revalSlot struct {
	mu       sync.Mutex
	rerun    bool // a read was absorbed into the attempt in flight
	released bool // the slot left the map; an absorber claims a fresh one
	// done is closed when the slot is released (#740): a stale-retry attempt
	// that joined an attempt in flight waits on it.
	done chan struct{}
}

func newRevalSlot() *revalSlot { return &revalSlot{done: make(chan struct{})} }

// revalSlotDone returns the done channel of key's slot in m, if one is held.
func revalSlotDone(m *sync.Map, key string) (<-chan struct{}, bool) {
	v, ok := m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*revalSlot).done, true
}

// claimRevalSlot claims key's slot in m, or, when one is held, arms its rerun:
// the read is absorbed. true: claimed (the caller starts the attempt).
func claimRevalSlot(m *sync.Map, key string) bool {
	for {
		v, loaded := m.LoadOrStore(key, newRevalSlot())
		if !loaded {
			return true
		}
		s := v.(*revalSlot)
		s.mu.Lock()
		if s.released {
			// Settled between the Load and the lock: the slot is gone, so this
			// read starts an attempt of its own rather than arming a dead slot.
			s.mu.Unlock()
			continue
		}
		s.rerun = true
		s.mu.Unlock()
		return false
	}
}

// settleRevalSlot ends one attempt on key's slot in m. rerun: a read was absorbed
// into it and key is still known-stale, so the flag is cleared, the slot STAYS
// claimed (a read during the rerun arms it again) and the caller re-runs at
// once. Otherwise the slot is released. held: there was a slot to settle.
func settleRevalSlot(m *sync.Map, key string) (held, rerun bool) {
	v, ok := m.Load(key)
	if !ok {
		return false, false
	}
	s := v.(*revalSlot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rerun {
		if _, _, still := KnownStale(key); still {
			s.rerun = false
			return true, true
		}
	}
	s.released = true
	m.CompareAndDelete(key, s)
	close(s.done)
	return true, false
}

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

// KnownStaleReadThrough is the substrate readers' gate (apistage content and
// the RAFullList; never the served widget shell): true means key is known-stale and the
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

// demandInflight holds the one in-flight demand revalidation per key (value: its
// *revalSlot). Bounded by the known-stale key set.
var demandInflight sync.Map

type ctxKeyDemandRevalidationType struct{}

var ctxKeyDemandRevalidation = ctxKeyDemandRevalidationType{}

// ClaimDemandRevalidation claims key's single revalidation slot for the stale
// generation t0. false: one is already in flight — this read is absorbed into it
// and earns it one rerun (#740) — or, team ruling (b), the key is suppressed for
// a structurally-permanent reason, so no revalidation can succeed: that skip is
// counted (known_stale_revalidation_skipped_total).
func ClaimDemandRevalidation(key string, t0 time.Time) bool {
	if key == "" {
		return false
	}
	if _, suppressed := RefreshSuppressedReason(key); suppressed {
		knownStaleRevalidationSkip.Add(1)
		return false
	}
	return claimRevalSlot(&demandInflight, key)
}

// bgRevalidationInflight holds the one queued background revalidation per
// identity-free key (RevalidateKnownStaleInBackground); released when the
// refresher's dequeue of that key settles.
var bgRevalidationInflight sync.Map

// RevalidateKnownStaleInBackground is the identity-free twin of the demand
// revalidation (owner ruling, #547): a known-stale SHARED widget shell is SERVED
// instantly and revalidated in the background by the refresher, under the
// refresher's own identity for identity-free cells (the SA) — never under the
// requester, whose identity is not the cell's. One queued revalidation per key;
// a read that finds it queued is absorbed and earns it one rerun (#740); a
// structurally-permanent suppression skips it (counted). Never blocks.
func RevalidateKnownStaleInBackground(key string) {
	_, _, ok := KnownStale(key)
	if !ok {
		return
	}
	if _, suppressed := RefreshSuppressedReason(key); suppressed {
		knownStaleRevalidationSkip.Add(1)
		return
	}
	// KnownStale already proved a refresher exists (the stamp lives on it); Peek,
	// never construct one on a serve path (the C1 N1 off-path lesson).
	r := refresherPeek()
	if r == nil {
		return
	}
	if !claimRevalSlot(&bgRevalidationInflight, key) {
		return
	}
	r.enqueueRevalidation(key)
}

// backgroundRetrySpec is key's viewer-gated chain on the background slot: each
// attempt claims the slot (or, when one is in flight, joins it as an absorbed
// read would), queues the refresher revalidation and waits for the slot to be
// released. Its viewer is the shell's own armed key.
func backgroundRetrySpec(key string) StaleRetrySpec {
	return StaleRetrySpec{
		Source: StaleRetryBackground,
		Attempt: func(ctx context.Context) StaleRetryOutcome {
			r := refresherPeek()
			if r == nil {
				return StaleRetryGenuine
			}
			if claimRevalSlot(&bgRevalidationInflight, key) {
				r.enqueueRevalidation(key)
			}
			if done, ok := revalSlotDone(&bgRevalidationInflight, key); ok {
				ctx, cancel := context.WithTimeout(ctx, DemandRevalidationDeadline())
				defer cancel()
				select {
				case <-done:
				case <-ctx.Done():
				}
			}
			if _, _, still := KnownStale(key); still {
				return StaleRetryGenuine
			}
			return StaleRetryFresh
		},
		Dependents: func() []string { return []string{key} },
		StillStale: func() (bool, bool) { return knownStaleRetryState(key) },
	}
}

// knownStaleRetryState is a #547 cell's chain predicate: still known-stale and
// not suppressed (stale), and resident.
func knownStaleRetryState(key string) (stale, resident bool) {
	if c := ResolvedCache(); c == nil || !c.Has(key) {
		return false, false
	}
	if _, suppressed := RefreshSuppressedReason(key); suppressed {
		return false, true
	}
	_, _, ks := KnownStale(key)
	return ks, true
}

// enqueueRevalidation queues key's background revalidation on its own tier.
func (r *refresher) enqueueRevalidation(key string) {
	if _, isCL := r.clusterListKeys.Load(key); isCL {
		r.enqueueClusterList(key)
	} else {
		r.enqueue(key)
	}
}

// settleBackgroundRevalidation settles key's background revalidation slot and
// counts the attempt's outcome (fresh: the key is no longer known-stale). Called
// by the refresher once a dequeue of key has settled. A read absorbed into the
// attempt while the key is still known-stale earns one rerun (#740): the slot
// stays claimed and the key is re-queued at once — the workqueue re-adds it
// after this dequeue's Done.
func settleBackgroundRevalidation(key string, dur time.Duration) {
	held, rerun := settleRevalSlot(&bgRevalidationInflight, key)
	if !held {
		return
	}
	_, _, still := KnownStale(key)
	if still {
		knownStaleRevalidationFail.Add(1)
	} else {
		knownStaleRevalidationFresh.Add(1)
	}
	if !rerun {
		if still {
			// #740 — a revalidation that settled still known-stale (and earned no
			// rerun) opens or joins the cell's viewer-gated chain. A refused
			// replace ends the window evicted, so a still-known-stale settle is
			// always a genuine failure (Q4); a suppressed key opens no chain.
			if _, suppressed := RefreshSuppressedReason(key); !suppressed {
				StaleRetryFailed(key, StaleRetryGenuine, dur, backgroundRetrySpec(key))
			}
		}
		return
	}
	knownStaleRevalidationRerun[revalSlotBackground].Add(1)
	if r := refresherPeek(); r != nil {
		r.enqueueRevalidation(key)
	}
}

// DemandRevalidationsInFlight is the number of demand revalidations currently
// claimed (each one holds its key's slot until its settle runs, after the
// re-entered handler has returned). Bounded by the known-stale key set.
func DemandRevalidationsInFlight() int {
	n := 0
	demandInflight.Range(func(_, _ any) bool { n++; return true })
	return n
}

// BackgroundRevalidationsInFlight is the number of background revalidations
// currently queued or running (each holds its key's slot until the refresher's
// dequeue of that key settles). Bounded by the known-stale key set.
func BackgroundRevalidationsInFlight() int {
	n := 0
	bgRevalidationInflight.Range(func(_, _ any) bool { n++; return true })
	return n
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

// BeginDemandRevalidation marks ctx as one demand revalidation attempt of key
// (its slot claimed with ClaimDemandRevalidation for t0) and installs the
// write-outcome sink the customer Put reports into (PutIfGen → NoteRefreshWrite).
// settle MUST be called once the attempt returns: it counts the outcome and
// settles the slot. The window itself is closed by the write, through the same
// rule as any other accepted Put (endKnownStaleOnPut). fresh reports whether
// the write was accepted un-remarked. rerun (#740): a read was absorbed into this
// attempt and key is still known-stale — the slot STAYS claimed and the caller
// runs one more attempt at once, beginning it with another
// BeginDemandRevalidation; otherwise the slot is released.
func BeginDemandRevalidation(ctx context.Context, key string, t0 time.Time) (context.Context, func() DemandSettle) {
	ctx, o := withRefreshOutcome(ctx, key)
	ctx = context.WithValue(ctx, ctxKeyDemandRevalidation, key)
	var once sync.Once
	var out DemandSettle
	return ctx, func() DemandSettle {
		once.Do(func() {
			if o.accepted.Load() && !o.remarked.Load() {
				knownStaleRevalidationFresh.Add(1)
				out.Fresh = true
			} else {
				knownStaleRevalidationFail.Add(1)
			}
			out.Refused = o.refused.Load()
			_, out.Rerun = settleRevalSlot(&demandInflight, key)
			if out.Rerun {
				knownStaleRevalidationRerun[revalSlotDemand].Add(1)
			}
		})
		return out
	}
}

// DemandSettle is one demand attempt's settle: Fresh (an accepted, un-remarked
// write), Rerun (#740: an absorbed read earned one more attempt; the slot stays
// claimed) and Refused (the Put was refused because the generation moved: a
// TRANSIENT failure for the viewer-gated chain, Q4).
type DemandSettle struct{ Fresh, Rerun, Refused bool }

// DemandSlotDone returns the done channel of key's demand slot, if one is held:
// a stale-retry attempt that found the slot held waits on it (#740).
func DemandSlotDone(key string) (<-chan struct{}, bool) { return revalSlotDone(&demandInflight, key) }

// KnownStaleRetryState is the #547 cells' viewer-gated chain predicate (#740):
// still known-stale and not suppressed, and resident.
func KnownStaleRetryState(key string) (stale, resident bool) { return knownStaleRetryState(key) }

// endKnownStaleOnPut ends key's KNOWN-STALE state on an ACCEPTED Put whose
// resolve began at or after the window's latest mark (#547): that body was read
// from the post-mark state, so the cell is fresh, whichever carrier wrote it — a
// demand revalidation, a substrate read-through refill (apistage content MISS,
// the RAFullList live resolve), a seed or keep-warm re-Put, the refresher's own
// terminal re-Put. ctx must carry the resolve's #375 dep-gen sink (its startSeq
// is the resolve-entry epoch); a Put with no sink proves nothing and ends
// nothing. The store's Put methods call it INSIDE the critical section that
// publishes the body (#734), so a reader that sees the new body also sees the
// state ended: before #734 an in-flight stamp kept its flag until the refresher
// dequeue settled, and a reader in that gap counted a known-stale serve of the
// fresh body, started a demand revalidation of it, or read a fresh substrate
// cell through as a MISS. Only sync.Map operations: safe under c.mu.
//
// A stamp at rest is closed: the window ends, and sample reports the t0 whose
// dirty→fresh sample the caller records once c.mu is released. An IN-FLIGHT
// stamp keeps its window: only the known-stale flag ends, and the refresher
// dequeue still classifies and samples the window by its own outcome. Called
// before the Put's #375 remark: a dep that moved during the resolve then
// re-opens a fresh window through the ordinary mark.
func endKnownStaleOnPut(ctx context.Context, key string) (t0 time.Time, sample bool) {
	r := refresherPeek()
	if r == nil || key == "" {
		return time.Time{}, false
	}
	start, ok := DepGenStartSeqFromContext(ctx)
	if !ok {
		return time.Time{}, false
	}
	for {
		cur, ok := r.dirty.Load(key)
		if !ok {
			return time.Time{}, false
		}
		s := cur.(*dirtyStamp)
		if s.stale == knownStaleNone || start < s.seq {
			return time.Time{}, false
		}
		if s.inflight {
			if r.dirty.CompareAndSwap(key, cur, &dirtyStamp{t0: s.t0, inflight: true, next: s.next, seq: s.seq}) {
				return time.Time{}, false
			}
			continue
		}
		if r.dirty.CompareAndDelete(key, cur) {
			return s.t0, true
		}
	}
}

// sampleFreshAfterPut records the dirty→fresh sample of a window an accepted Put
// closed (endKnownStaleOnPut). Called after c.mu is released: the sample feeds
// the OTLP histogram hook.
func sampleFreshAfterPut(t0 time.Time) {
	if r := refresherPeek(); r != nil {
		r.sampleFresh(t0)
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

// AddKnownStaleRevalidationRerunForTest moves the rerun counter of one slot of
// the closed set through its production atomic, so a cross-package arm can
// prove the OTLP mirror reads the LIVE counter. false: not a slot of the set.
// Production callers MUST NOT use this.
func AddKnownStaleRevalidationRerunForTest(slot string, n uint64) bool {
	for i, name := range KnownStaleRevalidationSlots {
		if name == slot {
			knownStaleRevalidationRerun[i].Add(n)
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
	knownStaleRevalidationSkip.Store(0)
	for i := range knownStaleRevalidationRerun {
		knownStaleRevalidationRerun[i].Store(0)
	}
	demandInflight.Range(func(k, _ any) bool { demandInflight.Delete(k); return true })
	bgRevalidationInflight.Range(func(k, _ any) bool { bgRevalidationInflight.Delete(k); return true })
}
