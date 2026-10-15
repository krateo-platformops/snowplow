// apistage_fill_admission.go — #710 PR-2a: bound the memory of concurrent
// content fills.
//
// PR-1 made N readers of ONE content key cost one fill. A burst across MANY
// distinct big lists still runs one fill per list at once, each decoding its
// whole envelope (~10× the raw bytes, PR-0's measurement). This is the third
// instance of the shared memory-admission pattern (nested_resolve_bound.go,
// dispatchers/seed_bound.go): a fill is admitted only while the aggregate
// estimated weight of the fills in flight fits under cache.AdmissionCeiling()
// — (GOMEMLIMIT − live heap) − GOMEMLIMIT/8, sampled fresh at each admission.
//
// C1 (deadlock-safety): this gate has its OWN mutex, cond and counters; it
// shares only the ceiling calc. A goroutine holding a nested or seed admission
// that enters a content fill waits on a DIFFERENT semaphore, never on itself.
//
// WHO PAYS. Admission is taken per FLIGHT, by its leader, after the dispatch
// built the envelope (its exact raw size is known) and before the parse; it is
// released when the flight completes, after the Put — from then on the stored
// tree is live heap, which the ceiling itself reads. Joiners pay nothing: a
// queued flight absorbs every new arrival for its key.
//
// WEIGHT = raw envelope bytes × decode expansion(GVR). The expansion is
// SELF-CALIBRATING (the C46-1 pattern): seeded conservatively high
// (contentFillExpansionSeed), replaced by the GVR's first measured sample — the
// live-heap delta across parse + Put over the raw bytes — then an EWMA of later
// samples. A sample ≤ 0 (GC ran mid-fill) is ignored; samples are clamped to
// [1, contentFillExpansionMax]. No env knob.
//
// NEVER DROPS. A fill that does not fit QUEUES. inFlight == 0 admits
// unconditionally (guaranteed progress: the lone fill always runs). The wait
// is bounded by the leader's detached fill ctx — the server write deadline
// (DemandRevalidationDeadline); past it the flight fails with
// ErrContentFillAdmissionTimeout and every reader in it gets that error as a
// stage error (D1, owner ruling). It never falls through to an apiserver LIST,
// which would rebuild the memory the gate exists to bound.
//
// PRIORITY. Strict, by class: an admission waits while any HIGHER class has a
// waiter, on every admit path including the inFlight == 0 escape (so a lower
// class cannot win the race for a just-freed slot). customer read-through >
// background (refresher re-resolve, prewarm, seed). The ordering is generic over
// the classes; #710 PR-2b adds the debt class above customer.
//
// TRANSPARENT when GOMEMLIMIT is unset (AdmissionCeiling's unlimited): admit at
// once, release is a no-op apart from calibration.
package api

import (
	"context"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ErrContentFillAdmissionTimeout is a content fill that could not be admitted
// within its deadline (D1: the request errors; nothing stale, nothing dropped).
// It is a 503 StatusError, so the #709 stage failure reports 503 Service
// Unavailable — the honest status for "the server is out of memory headroom
// right now" — rather than a 500.
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

// contentFillClassOf classifies a fill by the reader that leads it: a
// refresher re-resolve, a prewarm or any other background resolve is
// background; everything else is a customer read-through.
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
	// GOMEMLIMIT, as in the pod) the F-MEM arm measures 11-12× live-heap growth
	// per raw envelope byte, in line with PR-0's ~10.4×; without the soft limit
	// unswept decode garbage reads ~30×. The seed sits above the soft-limit
	// figure, so an unmeasured GVR over-reserves.
	contentFillExpansionSeed = 16.0
	// contentFillExpansionMax clamps one sample: a heap delta inflated by
	// concurrent unrelated allocation must not freeze a GVR out.
	contentFillExpansionMax = 64.0
	// contentFillExpansionEWMAAlpha weights a new sample after the first.
	contentFillExpansionEWMAAlpha = 0.2
)

// contentFillGate is the process-wide content-fill semaphore.
type contentFillGate struct {
	mu             sync.Mutex
	cond           *sync.Cond
	inFlightWeight int64
	inFlightCount  int
	waiting        [fillAdmissionClasses]int
	// expansion is the calibrated decode expansion per GVR (absent = seed).
	expansion map[string]float64
}

var (
	contentFillGateMu   sync.Mutex
	contentFillGateInst *contentFillGate
)

func theContentFillGate() *contentFillGate {
	contentFillGateMu.Lock()
	defer contentFillGateMu.Unlock()
	if contentFillGateInst == nil {
		g := &contentFillGate{expansion: map[string]float64{}}
		g.cond = sync.NewCond(&g.mu)
		contentFillGateInst = g
	}
	return contentFillGateInst
}

// expansionLocked returns gvr's decode expansion. Caller holds g.mu.
func (g *contentFillGate) expansionLocked(gvr string) float64 {
	if e, ok := g.expansion[gvr]; ok {
		return e
	}
	return contentFillExpansionSeed
}

// calibrateLocked folds one measured sample into gvr's expansion. Caller holds
// g.mu.
func (g *contentFillGate) calibrateLocked(gvr string, sample float64) {
	if sample <= 0 {
		return
	}
	sample = min(max(sample, 1), contentFillExpansionMax)
	cur, measured := g.expansion[gvr]
	if !measured {
		g.expansion[gvr] = sample // the first measurement replaces the seed
		return
	}
	g.expansion[gvr] = cur + contentFillExpansionEWMAAlpha*(sample-cur)
}

// higherWaitingLocked reports a waiter in a class above c. Caller holds g.mu.
func (g *contentFillGate) higherWaitingLocked(c fillAdmissionClass) bool {
	for k := c + 1; k < fillAdmissionClasses; k++ {
		if g.waiting[k] > 0 {
			return true
		}
	}
	return false
}

// admitContentFill admits one flight of rawBytes for gvr at class, waiting
// (bounded by ctx) while it does not fit. release must be called exactly once
// after the flight's Put; it re-samples the live heap to calibrate gvr's
// expansion and wakes the waiters. Idempotent.
func admitContentFill(ctx context.Context, gvr string, rawBytes int64, class fillAdmissionClass) (release func(), err error) {
	g := theContentFillGate()
	t0 := time.Now()
	g.mu.Lock()
	weight := int64(float64(rawBytes) * g.expansionLocked(gvr))
	counted := false
	unwait := func() {
		if counted {
			g.waiting[class]--
			counted = false
			contentFillAdmissionQueued[class].Add(-1)
			// A lower class yielding to this waiter must re-check.
			g.cond.Broadcast()
		}
	}
	for {
		ceiling, unlimited := cache.AdmissionCeiling()
		fits := g.inFlightWeight+weight <= ceiling
		if (unlimited || g.inFlightCount == 0 || fits) && !g.higherWaitingLocked(class) {
			unwait()
			g.inFlightWeight += weight
			g.inFlightCount++
			contentFillAdmissionInflightWeight.Store(g.inFlightWeight)
			storeMax(&contentFillAdmissionInflightWeightPeak, g.inFlightWeight)
			g.mu.Unlock()
			break
		}
		if !counted {
			g.waiting[class]++
			counted = true
			contentFillAdmissionQueued[class].Add(1)
		}
		g.waitLocked(ctx)
		if ctx.Err() != nil {
			unwait()
			g.mu.Unlock()
			contentFillAdmissionTimeout[class].Add(1)
			recordContentFillAdmissionWait(time.Since(t0).Milliseconds())
			return nil, ErrContentFillAdmissionTimeout
		}
	}
	recordContentFillAdmissionWait(time.Since(t0).Milliseconds())

	before := cache.AdmissionLiveHeapSample()
	var once sync.Once
	return func() {
		once.Do(func() {
			after := cache.AdmissionLiveHeapSample()
			g.mu.Lock()
			if rawBytes > 0 {
				g.calibrateLocked(gvr, float64(after-before)/float64(rawBytes))
			}
			g.inFlightWeight = max(g.inFlightWeight-weight, 0)
			g.inFlightCount = max(g.inFlightCount-1, 0)
			contentFillAdmissionInflightWeight.Store(g.inFlightWeight)
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}, nil
}

// waitLocked blocks on g.cond until a release or ctx ends. g.mu is held on
// entry and on return. sync.Cond has no ctx integration, so a watcher
// Broadcasts on ctx.Done; the caller re-checks ctx.Err.
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

// contentFillExpansionSnapshot returns the calibrated expansion per GVR.
func contentFillExpansionSnapshot() map[string]float64 {
	g := theContentFillGate()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]float64, len(g.expansion))
	for k, v := range g.expansion {
		out[k] = v
	}
	return out
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
