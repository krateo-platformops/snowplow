// apistage_fill_admission.go — #710 PR-2a: bound the memory of concurrent
// content fills.
//
// PR-1 made N readers of ONE content key cost one fill. A burst across MANY
// distinct big lists still runs one fill per list at once, each decoding its
// whole envelope (~10× the raw bytes). This is the third instance of the shared
// memory-admission pattern (nested_resolve_bound.go, dispatchers/seed_bound.go):
// a fill is admitted only while the aggregate estimated weight of the fills in
// flight fits under cache.AdmissionCeiling() — (GOMEMLIMIT − live heap) −
// GOMEMLIMIT/8, sampled fresh at each admission.
//
// C1 (deadlock-safety): this gate has its OWN mutex, cond and counters; it
// shares only the ceiling calc. LOCK ORDER: nested → fill, never fill →
// nested. A customer tree holding a nested-resolve admission may wait here for
// one of its content fills; a fill leader NEVER enters the nested gate (nor the
// seed gate) while it holds or waits for a fill admission, so the two gates
// cannot wait on each other.
//
// WHO PAYS, AND WHEN. Admission is taken per FLIGHT, by its leader, BEFORE the
// dispatch builds the envelope: a flight that waits holds nothing but a ticket,
// so K queued distinct keys cost K tickets, not K parked envelopes. The weight
// it waits with is PREDICTED (predictContentFillRaw): the key's own resident
// cell's raw size, else the informer partition's item count × the GVR's raw
// bytes per item, else the GVR's largest observed raw, else a seed. After the
// dispatch the held weight is RECONCILED in place to the exact raw × expansion
// — never released and re-queued, so an admitted flight never waits twice. The
// admission is released after the Put: from then on the stored tree is live
// heap, which the ceiling itself reads. Joiners pay nothing.
//
// WEIGHT = raw × decode expansion(GVR). The expansion is SELF-CALIBRATING and,
// unlike the nested bound's one-shot C46-1, CONTINUOUS: there is one weight per
// GVR while list sizes move. Seeded at contentFillExpansionSeed; the GVR's
// first measured fill (live-heap delta across parse + Put ÷ raw) replaces the
// seed; after that an ASYMMETRIC average rises at once to a larger sample and
// decays slowly toward a smaller one. A concurrent live-heap delta is noisy
// both ways — other fills' allocations inflate it (safe), a partial GC shrinks
// it — and a symmetric average would ratchet down on the shrunken samples. A
// sample ≤ 0 is ignored; samples are clamped to [1, contentFillExpansionMax].
//
// NEVER DROPS. A flight that does not fit QUEUES; an empty gate always admits
// (guaranteed progress). The wait is bounded by the leader's detached fill
// ctx — the server write deadline. Past it the flight fails with
// ErrContentFillAdmissionTimeout (a 503, D1): its readers get a stage error,
// nothing is stored, nothing is served stale, nothing falls through to an
// apiserver LIST. A JOINER of a flight that failed this way, whose own request
// is still alive, starts a new flight rather than inherit someone else's
// deadline.
//
// PRIORITY. Strict by class, FIFO within a class by enqueue order: a flight
// admits only when no queued flight has a higher class, or the same class and
// an earlier ticket — on every admit path, the empty-gate escape included.
// customer read-through > background (refresher re-resolve, prewarm). A
// flight's class is the MAX over its participants: a customer that joins a
// queued background flight PROMOTES it (keeping its enqueue order), or
// coalescing would bypass the priority. No aging (C5's rule in the nested
// bound); background starvation is visible on admission_wait_ms{class}.
// #710 PR-2b adds the debt class above customer.
//
// TRANSPARENT when GOMEMLIMIT is unset (AdmissionCeiling's unlimited): admit at
// once; release still calibrates.
package api

import (
	"context"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ErrContentFillAdmissionTimeout is a content fill that could not be admitted
// within its deadline (D1: the request errors; nothing stale, nothing dropped).
// It is snowplow's own 503 StatusError — the honest status for "out of memory
// headroom right now" — built here, never classified from an upstream body.
var ErrContentFillAdmissionTimeout error = apierrors.NewServiceUnavailable("apistage content fill: memory admission not granted before the deadline")

// fillAdmissionClass is a fill's admission priority; higher admits first.
type fillAdmissionClass uint8

const (
	fillClassBackground fillAdmissionClass = iota
	fillClassCustomer
	fillAdmissionClasses
)

// fillAdmissionClassNames is the closed `class` attribute set, in class order.
var fillAdmissionClassNames = [fillAdmissionClasses]string{"background", "customer"}

func (c fillAdmissionClass) String() string {
	if c >= fillAdmissionClasses {
		return fillAdmissionClassNames[fillClassBackground]
	}
	return fillAdmissionClassNames[c]
}

// ContentFillAdmissionClasses returns the closed `class` set, in a fixed order.
func ContentFillAdmissionClasses() []string { return fillAdmissionClassNames[:] }

// contentFillClassOf classifies a reader: a refresher re-resolve, a prewarm or
// any other background resolve is background; everything else is a customer
// read-through.
func contentFillClassOf(ctx context.Context) fillAdmissionClass {
	if _, refresher := cache.RefreshTriggerGVRFromContext(ctx); refresher ||
		cache.BackgroundResolveFromContext(ctx) || cache.ApistagePrewarmFromContext(ctx) {
		return fillClassBackground
	}
	return fillClassCustomer
}

const (
	// contentFillExpansionSeed is the decode expansion used for a GVR until its
	// first fill is measured. Under a runtime soft limit (GC working against
	// GOMEMLIMIT, as in the pod) the F-MEM arm measures 9-15× live-heap growth
	// per raw envelope byte, in line with PR-0's ~10.4×; the seed sits above.
	contentFillExpansionSeed = 16.0
	// contentFillExpansionMax clamps one sample.
	contentFillExpansionMax = 64.0
	// contentFillExpansionDecay is how far one SMALLER sample moves the
	// expansion down (a larger one replaces it at once).
	contentFillExpansionDecay = 0.05
	// contentFillBytesPerItemAlpha weights a new raw-bytes-per-item sample.
	contentFillBytesPerItemAlpha = 0.2
	// contentFillSeedRaw is the predicted raw size of a fill with nothing else
	// to go on (no resident cell, no partition count, no GVR history).
	contentFillSeedRaw = 1 << 20
)

// fillTicket is one flight's place in the gate. Every field is guarded by the
// gate's mutex.
type fillTicket struct {
	gvr       string
	class     fillAdmissionClass
	seq       uint64 // enqueue order; 0 = not yet queued
	queued    bool
	admitted  bool
	predicted int64 // predicted raw bytes
	raw       int64 // the raw bytes the held weight is computed from
	weight    int64 // the weight held (or waited for)
	enqueued  time.Time
	liveAtAdm int64
}

// contentFillGate is the process-wide content-fill semaphore.
type contentFillGate struct {
	mu             sync.Mutex
	cond           *sync.Cond
	inFlightWeight int64
	inFlightCount  int
	queue          map[*fillTicket]struct{}
	nextSeq        uint64
	// Per-GVR calibration and prediction state.
	expansion    map[string]float64 // absent = the seed
	bytesPerItem map[string]float64
	maxRaw       map[string]int64
	predictRatio map[string]float64 // EWMA of exact / predicted raw
}

var (
	contentFillGateMu   sync.Mutex
	contentFillGateInst *contentFillGate
)

func theContentFillGate() *contentFillGate {
	contentFillGateMu.Lock()
	defer contentFillGateMu.Unlock()
	if contentFillGateInst == nil {
		g := &contentFillGate{
			queue:        map[*fillTicket]struct{}{},
			expansion:    map[string]float64{},
			bytesPerItem: map[string]float64{},
			maxRaw:       map[string]int64{},
			predictRatio: map[string]float64{},
		}
		g.cond = sync.NewCond(&g.mu)
		contentFillGateInst = g
	}
	return contentFillGateInst
}

func newFillTicket(class fillAdmissionClass) *fillTicket { return &fillTicket{class: class} }

// expansionLocked returns gvr's decode expansion. Caller holds g.mu.
func (g *contentFillGate) expansionLocked(gvr string) float64 {
	if e, ok := g.expansion[gvr]; ok {
		return e
	}
	return contentFillExpansionSeed
}

// calibrateLocked folds one measured expansion sample. Caller holds g.mu.
func (g *contentFillGate) calibrateLocked(gvr string, sample float64) {
	if sample <= 0 {
		return
	}
	sample = min(max(sample, 1), contentFillExpansionMax)
	cur, measured := g.expansion[gvr]
	switch {
	case !measured, sample >= cur:
		// The first measurement replaces the seed; a larger one is taken at once.
		g.expansion[gvr] = sample
	default:
		g.expansion[gvr] = cur + contentFillExpansionDecay*(sample-cur)
	}
}

// predictContentFillRaw predicts a fill's raw envelope size before its
// dispatch. resident is the key's current cell (nil when absent); partition is
// the informer partition's item count (ok=false when unknown).
func predictContentFillRaw(gvr string, resident *cache.ResolvedEntry, partitionItems int, partitionOK bool) int64 {
	if resident != nil && len(resident.RawJSON) > 0 {
		return int64(len(resident.RawJSON))
	}
	g := theContentFillGate()
	g.mu.Lock()
	defer g.mu.Unlock()
	if bpi, ok := g.bytesPerItem[gvr]; ok && partitionOK {
		return max(int64(bpi*float64(partitionItems)), 1)
	}
	if m, ok := g.maxRaw[gvr]; ok {
		return m
	}
	return contentFillSeedRaw
}

// predictFillRaw gathers the predictor's inputs for one content key: the
// resident cell (a known-stale or refresh-forced fill replaces one) and, for a
// LIST, the informer partition's item count (a GET-by-name is one item).
func predictFillRaw(store *cache.ResolvedCacheStore, key string, gvr schema.GroupVersionResource, namespace string, isList bool) int64 {
	var resident *cache.ResolvedEntry
	if store != nil {
		//serve-probe: admission size prediction — reads len(RawJSON) only, serves no body
		if e, ok := store.GetNoTouch(key); ok {
			resident = e
		}
	}
	items, ok := 1, true
	if isList {
		items, ok = cache.Global().PartitionLen(gvr, namespace)
	}
	return predictContentFillRaw(gvr.String(), resident, items, ok)
}

// bestLocked reports whether t heads the queue: no queued ticket has a higher
// class, or the same class and an earlier enqueue. Caller holds g.mu.
func (g *contentFillGate) bestLocked(t *fillTicket) bool {
	for u := range g.queue {
		if u == t {
			continue
		}
		if u.class > t.class || (u.class == t.class && u.seq < t.seq) {
			return false
		}
	}
	return true
}

func (g *contentFillGate) dequeueLocked(t *fillTicket) {
	if t.queued {
		delete(g.queue, t)
		t.queued = false
		contentFillAdmissionQueued[t.class].Add(-1)
		g.cond.Broadcast()
	}
}

// promote raises a flight's class to c (a participant of a higher class
// joined it), keeping its enqueue order. A queued ticket is re-ordered at once.
func (g *contentFillGate) promote(t *fillTicket, c fillAdmissionClass) {
	if t == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if c <= t.class {
		return
	}
	if t.queued {
		contentFillAdmissionQueued[t.class].Add(-1)
		contentFillAdmissionQueued[c].Add(1)
		g.cond.Broadcast()
	}
	t.class = c
}

// admit waits (bounded by ctx) until t's flight, weighing predictedRaw ×
// expansion, may run. On ctx expiry it returns ErrContentFillAdmissionTimeout.
func (g *contentFillGate) admit(ctx context.Context, t *fillTicket, gvr string, predictedRaw int64) error {
	t0 := time.Now()
	g.mu.Lock()
	t.gvr = gvr
	t.predicted = predictedRaw
	t.raw = predictedRaw
	t.weight = int64(float64(predictedRaw) * g.expansionLocked(gvr))
	g.nextSeq++
	t.seq = g.nextSeq
	t.enqueued = t0
	t.queued = true
	g.queue[t] = struct{}{}
	contentFillAdmissionQueued[t.class].Add(1)
	for {
		ceiling, unlimited := cache.AdmissionCeiling()
		fits := g.inFlightWeight+t.weight <= ceiling
		if (unlimited || g.inFlightCount == 0 || fits) && g.bestLocked(t) {
			g.dequeueLocked(t)
			t.admitted = true
			g.inFlightWeight += t.weight
			g.inFlightCount++
			contentFillAdmissionInflightWeight.Store(g.inFlightWeight)
			storeMax(&contentFillAdmissionInflightWeightPeak, g.inFlightWeight)
			class := t.class
			g.mu.Unlock()
			recordContentFillAdmissionWait(class, time.Since(t0).Milliseconds())
			t.liveAtAdm = cache.AdmissionLiveHeapSample()
			return nil
		}
		g.waitLocked(ctx)
		if ctx.Err() != nil {
			g.dequeueLocked(t)
			class := t.class
			g.mu.Unlock()
			contentFillAdmissionTimeout[class].Add(1)
			recordContentFillAdmissionWait(class, time.Since(t0).Milliseconds())
			return ErrContentFillAdmissionTimeout
		}
	}
}

// reconcile replaces an admitted flight's predicted weight with the exact one,
// in place (never re-queued), and records the prediction's accuracy.
func (g *contentFillGate) reconcile(t *fillTicket, exactRaw int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !t.admitted {
		return
	}
	w := int64(float64(exactRaw) * g.expansionLocked(t.gvr))
	g.inFlightWeight = max(g.inFlightWeight+w-t.weight, 0)
	t.weight, t.raw = w, exactRaw
	contentFillAdmissionInflightWeight.Store(g.inFlightWeight)
	storeMax(&contentFillAdmissionInflightWeightPeak, g.inFlightWeight)
	if t.predicted > 0 {
		r := float64(exactRaw) / float64(t.predicted)
		if cur, ok := g.predictRatio[t.gvr]; ok {
			g.predictRatio[t.gvr] = cur + contentFillBytesPerItemAlpha*(r-cur)
		} else {
			g.predictRatio[t.gvr] = r
		}
	}
	g.maxRaw[t.gvr] = max(g.maxRaw[t.gvr], exactRaw)
}

// noteItems records a decoded LIST's raw bytes per item for gvr: the cold-key
// predictor's input.
func (g *contentFillGate) noteItems(gvr string, raw int64, items int) {
	if items <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	bpi := float64(raw) / float64(items)
	if cur, ok := g.bytesPerItem[gvr]; ok {
		g.bytesPerItem[gvr] = cur + contentFillBytesPerItemAlpha*(bpi-cur)
	} else {
		g.bytesPerItem[gvr] = bpi
	}
}

// release ends an admitted flight. After its Put (calibrate) the live-heap
// delta calibrates the GVR's expansion; an early exit (nothing decoded)
// releases without calibrating. Wakes the queue. Idempotent.
func (g *contentFillGate) release(t *fillTicket, calibrate bool) {
	after := cache.AdmissionLiveHeapSample()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !t.admitted {
		return
	}
	t.admitted = false
	if calibrate && t.raw > 0 {
		g.calibrateLocked(t.gvr, float64(after-t.liveAtAdm)/float64(t.raw))
	}
	g.inFlightWeight = max(g.inFlightWeight-t.weight, 0)
	g.inFlightCount = max(g.inFlightCount-1, 0)
	contentFillAdmissionInflightWeight.Store(g.inFlightWeight)
	g.cond.Broadcast()
}

// waitLocked blocks on g.cond until a release, a promotion or ctx ends. g.mu
// is held on entry and on return. sync.Cond has no ctx integration, so a
// watcher Broadcasts on ctx.Done; the caller re-checks ctx.Err.
func (g *contentFillGate) waitLocked(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		case <-stop:
		}
	}()
	g.cond.Wait()
	close(stop)
}

// contentFillCalibrationSnapshot returns the per-GVR expansion and prediction
// ratio (exact / predicted raw).
func contentFillCalibrationSnapshot() (expansion, predictRatio map[string]float64) {
	g := theContentFillGate()
	g.mu.Lock()
	defer g.mu.Unlock()
	expansion = make(map[string]float64, len(g.expansion))
	for k, v := range g.expansion {
		expansion[k] = v
	}
	predictRatio = make(map[string]float64, len(g.predictRatio))
	for k, v := range g.predictRatio {
		predictRatio[k] = v
	}
	return expansion, predictRatio
}

// --- test seams ---------------------------------------------------------------

// resetContentFillGateForTest drops the gate and its calibration. Test-only.
func resetContentFillGateForTest() {
	contentFillGateMu.Lock()
	contentFillGateInst = nil
	contentFillGateMu.Unlock()
	for i := range contentFillAdmissionQueued {
		contentFillAdmissionQueued[i].Store(0)
	}
	contentFillAdmissionInflightWeight.Store(0)
	contentFillAdmissionInflightWeightPeak.Store(0)
}

// contentFillGateStateForTest returns (inFlightWeight, inFlightCount). Test-only.
func contentFillGateStateForTest() (int64, int) {
	g := theContentFillGate()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlightWeight, g.inFlightCount
}

// admitContentFillForTest admits a standalone ticket of raw bytes for gvr at
// class and returns its release. Test-only.
func admitContentFillForTest(ctx context.Context, gvr string, raw int64, class fillAdmissionClass) (func(), *fillTicket, error) {
	g := theContentFillGate()
	t := newFillTicket(class)
	if err := g.admit(ctx, t, gvr, raw); err != nil {
		return nil, t, err
	}
	return func() { g.release(t, true) }, t, nil
}
