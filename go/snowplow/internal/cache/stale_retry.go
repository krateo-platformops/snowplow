// stale_retry.go — #740 (owner ruling; architect spec issuecomment-6202304466,
// answers issuecomment-6250211062): after a GENUINELY failed refresh of a
// known-stale cell, keep serving it (#547) AND keep retrying it with a growing
// backoff, but ONLY while someone is viewing it; stop when nobody is, resume
// when someone starts.
//
// ONE CHAIN PER CELL. Every trigger (a slot attempt that settled still
// known-stale, a D2 refill that failed, …) JOINS the cell's chain; two viewers
// or a burst of reads never start a second one. A join replaces the chain's
// spec, so the newest same-key request is the template (Q3).
//
// SCHEDULE, one rule for every source (Q4): refused (the generation moved — a
// newer change exists) ⇒ TRANSIENT: retry at once, then #690's 10 ms → 5 s on
// repeats; everything else (transport, timeout, 5xx, deny, a declined Put) ⇒
// GENUINE: exponential from max(1 s, the failed attempt's own duration),
// doubling, capped at staleRetryCap (60 s, the owner's "about a minute": a
// design constant, never an env knob). The floor self-adapts: an upstream that
// takes 5 s to fail is never retried more often than every 5 s. A suppressed
// key never opens a chain (the callers skip it, as they skip its revalidation).
//
// VIEWER GATE. Before every genuine retry the chain checks its viewers: a live
// /refreshes connection armed on a key of its dependent set (the cell itself
// for the #547 slots; the dependents' L1 keys for a D2 debt). No viewer → the
// chain is ABANDONED: the cell stays known-stale and served (#547), and an arm
// of a dependent RESUMES it with an immediate attempt. Recent reads are not a
// viewer (architect ruling): a tab with no armed subscription cannot receive
// the push, and its next read revalidates anyway.
//
// CANCELLATION. Eager: a connection's close stops the pending wake of every
// chain left without a viewer. Lazy: each wake re-checks the viewers before its
// attempt (a frozen armed set says nothing about OTHER connections), race-free
// against a concurrent arm because the arm resumes. An abandoned chain holds no
// goroutine and no timer.
//
// A chain ends on a fresh commit, on abandonment (no viewer, or its cell
// evicted), or when another carrier healed the cell; its spec (and the request
// template inside it) is dropped then, so a held credential never outlives it.
// Every attempt is customer-triggered refresh work (a viewer is waiting on it):
// it bypasses the admission cap at both gates (WithCustomerTriggeredRefresh).
// Nothing here logs a body, a user or an object name: keys are hashes.
package cache

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"
	"time"
)

// StaleRetryOutcome is what one chain attempt reports.
type StaleRetryOutcome uint8

const (
	// StaleRetryFresh: the attempt committed a fresh body; the chain ends.
	StaleRetryFresh StaleRetryOutcome = iota
	// StaleRetryTransient: the Put was refused because the generation moved.
	StaleRetryTransient
	// StaleRetryGenuine: the attempt genuinely failed.
	StaleRetryGenuine
)

// StaleRetrySource is the closed `source` set of snowplow_stale_retry_total.
type StaleRetrySource uint8

const (
	StaleRetryBackground StaleRetrySource = iota // the identity-free shell's background revalidation
	StaleRetryDemand                             // an identity-bound cell's demand revalidation
	StaleRetryDebt                               // a D2 debt's refill or dependent replace
	staleRetrySources
)

// StaleRetrySources / StaleRetryKinds are the closed label sets, in order.
var (
	StaleRetrySources = []string{"background", "demand", "debt"}
	StaleRetryKinds   = []string{"transient", "genuine"}
)

// staleRetryCap is the genuine-failure backoff cap: the owner's "about a minute".
const staleRetryCap = 60 * time.Second

// staleRetryFloor is the genuine-failure backoff floor.
const staleRetryFloor = time.Second

// staleRetryTransientMin / Max: #690's backoff for repeated transient refusals.
const (
	staleRetryTransientMin = 10 * time.Millisecond
	staleRetryTransientMax = 5 * time.Second
)

// StaleRetrySpec is one cell's chain:
//   - Attempt re-runs the cell's revalidation under ctx (customer-triggered,
//     bounded by the caller) and reports its outcome;
//   - Dependents is the cell's dependent set (the keys a tab arms to view it);
//   - StillStale reports whether the cell still needs a refresh (stale) and is
//     still resident; false/false ends the chain (healed / evicted).
type StaleRetrySpec struct {
	Source     StaleRetrySource
	Attempt    func(ctx context.Context) StaleRetryOutcome
	Dependents func() []string
	StillStale func() (stale, resident bool)
	// Release, if set, runs when the chain stops holding this spec — it ends or
	// is abandoned — so a request template (and its credential) never outlives
	// the chain that needed it (#740 Q3). Called under the engine lock: it must
	// not call back into the engine.
	Release func()
}

type staleRetryChain struct {
	key  string
	spec StaleRetrySpec

	step      time.Duration // the next genuine backoff step
	tstep     time.Duration // the next transient backoff step
	lastStep  time.Duration // the wake currently pending (backoff_ms)
	timer     staleRetryTimer
	running   bool
	abandoned bool
}

var (
	staleRetryMu     sync.Mutex
	staleRetryChains = map[string]*staleRetryChain{}
	staleRetryWG     sync.WaitGroup

	staleRetryTotal          [2][staleRetrySources]atomic.Uint64 // [kind-1][source]
	staleRetryAbandonedNoV   atomic.Uint64
	staleRetryAbandonedEvict atomic.Uint64
	staleRetryResumedTotal   atomic.Uint64
)

// --- the seamed clock -----------------------------------------------------------

// StaleRetryTimer is a stoppable pending wake.
type StaleRetryTimer interface{ Stop() bool }

type staleRetryTimer = StaleRetryTimer

// StaleRetryClock reads time and schedules wakes; tests install a manual one
// (SetStaleRetryClockForTest).
type StaleRetryClock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) StaleRetryTimer
}

var staleRetryClock atomic.Pointer[StaleRetryClock]

// SetStaleRetryClockForTest installs c (nil: the real clock). Production MUST
// NOT use it.
func SetStaleRetryClockForTest(c StaleRetryClock) {
	if c == nil {
		staleRetryClock.Store(nil)
		return
	}
	staleRetryClock.Store(&c)
}

func staleRetryNow() time.Time {
	if c := staleRetryClock.Load(); c != nil {
		return (*c).Now()
	}
	return time.Now()
}

func staleRetryAfter(d time.Duration, f func()) StaleRetryTimer {
	if c := staleRetryClock.Load(); c != nil {
		return (*c).AfterFunc(d, f)
	}
	return time.AfterFunc(d, f)
}

// --- the engine -----------------------------------------------------------------

// StaleRetryFailed reports that a revalidation of key settled still known-stale
// after an attempt of the given kind that took dur. It joins key's chain or
// opens it. A chain that is scheduled or running keeps its schedule and takes
// spec as its newest template; an abandoned one is rescheduled.
func StaleRetryFailed(key string, kind StaleRetryOutcome, dur time.Duration, spec StaleRetrySpec) {
	if key == "" || kind == StaleRetryFresh || spec.Attempt == nil || spec.StillStale == nil || spec.Dependents == nil {
		return
	}
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	c := staleRetryChains[key]
	if c != nil && !c.abandoned {
		c.spec = spec
		return
	}
	if c == nil {
		c = &staleRetryChain{key: key}
		staleRetryChains[key] = c
	}
	c.spec = spec
	c.scheduleLocked(kind, dur)
}

// StaleRetryRetemplate replaces the spec of key's chain if one exists (an
// absorbed read carries a newer request, Q3). No chain: a no-op.
func StaleRetryRetemplate(key string, spec StaleRetrySpec) {
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	if c := staleRetryChains[key]; c != nil && spec.Attempt != nil && spec.StillStale != nil && spec.Dependents != nil {
		c.spec = spec
	}
}

// scheduleLocked arms c's next wake after a failure of kind that took dur.
func (c *staleRetryChain) scheduleLocked(kind StaleRetryOutcome, dur time.Duration) {
	var d time.Duration
	switch kind {
	case StaleRetryTransient:
		staleRetryTotal[0][c.spec.Source].Add(1)
		d, c.tstep = c.tstep, nextTransientStep(c.tstep)
	default:
		kind = StaleRetryGenuine
		staleRetryTotal[1][c.spec.Source].Add(1)
		c.tstep = 0
		if c.step == 0 {
			c.step = staleRetryFloor
		}
		d = min(max(c.step, dur), staleRetryCap)
		c.step = min(2*d, staleRetryCap)
	}
	c.abandoned = false
	c.lastStep = d
	c.timer = staleRetryAfter(d, func() { c.wake(kind, false) })
}

func nextTransientStep(cur time.Duration) time.Duration {
	if cur == 0 {
		return staleRetryTransientMin
	}
	return min(2*cur, staleRetryTransientMax)
}

// wake runs one retry of c, unless the cell no longer needs it or, for a
// genuine retry not forced by an arm, nobody is viewing it.
func (c *staleRetryChain) wake(kind StaleRetryOutcome, resumed bool) {
	staleRetryMu.Lock()
	if staleRetryChains[c.key] != c || c.abandoned || c.running {
		staleRetryMu.Unlock()
		return
	}
	c.timer = nil
	stale, resident := c.spec.StillStale()
	switch {
	case !resident:
		c.endLocked()
		staleRetryAbandonedEvict.Add(1)
		staleRetryMu.Unlock()
		return
	case !stale:
		c.endLocked() // healed by another carrier
		staleRetryMu.Unlock()
		return
	case kind == StaleRetryGenuine && !resumed && !c.viewedLocked():
		c.abandonLocked()
		staleRetryAbandonedNoV.Add(1)
		staleRetryMu.Unlock()
		return
	}
	c.running = true
	spec := c.spec
	staleRetryWG.Add(1)
	staleRetryMu.Unlock()
	c.run(spec)
}

// run performs one attempt and schedules what follows from its outcome.
func (c *staleRetryChain) run(spec StaleRetrySpec) {
	defer staleRetryWG.Done()
	ctx := WithCustomerTriggeredRefresh(context.Background()) // a viewer is waiting on it
	start := staleRetryNow()
	out := spec.Attempt(ctx)
	dur := staleRetryNow().Sub(start)
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	c.running = false
	if staleRetryChains[c.key] != c {
		return
	}
	if out == StaleRetryFresh {
		c.endLocked()
		return
	}
	if stale, resident := c.spec.StillStale(); !stale || !resident {
		if !resident {
			staleRetryAbandonedEvict.Add(1)
		}
		c.endLocked()
		return
	}
	c.scheduleLocked(out, dur)
}

// viewedLocked reports whether a live /refreshes connection is armed on a key
// of c's dependent set.
func (c *staleRetryChain) viewedLocked() bool {
	for _, k := range c.spec.Dependents() {
		if HasRefreshSubscriber(k) {
			return true
		}
	}
	return false
}

// abandonLocked stops c's pending wake and keeps c, abandoned, so an arm of a
// dependent can resume it. Bounded by the known-stale set: StillStale ends it
// at its next look once the cell healed or left.
func (c *staleRetryChain) abandonLocked() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.abandoned = true
	if c.spec.Release != nil {
		c.spec.Release()
	}
}

// endLocked drops c and its spec (the request template with it).
func (c *staleRetryChain) endLocked() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if staleRetryChains[c.key] == c {
		delete(staleRetryChains, c.key)
	}
	if c.spec.Release != nil {
		c.spec.Release()
	}
	c.spec = StaleRetrySpec{}
}

// noteStaleRetryArm resumes, with an immediate attempt, every abandoned chain
// with a dependent among armed (a /refreshes connection armed them).
func noteStaleRetryArm(armed map[string]struct{}) {
	if len(armed) == 0 {
		return
	}
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	for _, c := range staleRetryChains {
		if !c.abandoned || c.running || !c.dependsOnAny(armed) {
			continue
		}
		if stale, resident := c.spec.StillStale(); !stale || !resident {
			c.endLocked()
			continue
		}
		c.abandoned = false
		c.step = 0 // a new viewer: the schedule starts over
		c.lastStep = 0
		staleRetryResumedTotal.Add(1)
		c.timer = staleRetryAfter(0, func() { c.wake(StaleRetryGenuine, true) })
	}
}

// noteStaleRetryDisarm stops, eagerly, the pending wake of every chain left
// with no viewer once a /refreshes connection closed.
func noteStaleRetryDisarm(disarmed map[string]struct{}) {
	if len(disarmed) == 0 {
		return
	}
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	for _, c := range staleRetryChains {
		if c.abandoned || c.running || !c.dependsOnAny(disarmed) {
			continue
		}
		if !c.viewedLocked() {
			c.abandonLocked()
			staleRetryAbandonedNoV.Add(1)
		}
	}
}

func (c *staleRetryChain) dependsOnAny(keys map[string]struct{}) bool {
	for _, k := range c.spec.Dependents() {
		if _, ok := keys[k]; ok {
			return true
		}
	}
	return false
}

// StaleRetryStats is the engine's state: the single source of the expvar keys
// and the OTLP callback.
type StaleRetryStats struct {
	// Retries is snowplow_stale_retry_total, [kind][source] in
	// StaleRetryKinds × StaleRetrySources order.
	Retries           [2][staleRetrySources]uint64
	ChainsActive      int64
	Viewers           int64
	AbandonedNoViewer uint64
	AbandonedEvicted  uint64
	Resumed           uint64
	// BackoffMS is the longest wake currently pending: sitting at 60000 is a
	// persistent upstream failure that someone is looking at.
	BackoffMS int64
}

// StaleRetryStatsSnapshot reads the engine.
func StaleRetryStatsSnapshot() StaleRetryStats {
	var s StaleRetryStats
	for k := range s.Retries {
		for src := range s.Retries[k] {
			s.Retries[k][src] = staleRetryTotal[k][src].Load()
		}
	}
	s.AbandonedNoViewer = staleRetryAbandonedNoV.Load()
	s.AbandonedEvicted = staleRetryAbandonedEvict.Load()
	s.Resumed = staleRetryResumedTotal.Load()
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	for _, c := range staleRetryChains {
		if c.abandoned {
			continue
		}
		s.ChainsActive++
		for _, k := range c.spec.Dependents() {
			if HasRefreshSubscriber(k) {
				s.Viewers++
			}
		}
		s.BackoffMS = max(s.BackoffMS, c.lastStep.Milliseconds())
	}
	return s
}

// StaleRetryTotalFor reads one (kind, source) cell of the retry counter.
func (s StaleRetryStats) StaleRetryTotalFor(kind StaleRetryOutcome, src StaleRetrySource) uint64 {
	if kind != StaleRetryTransient && kind != StaleRetryGenuine || src >= staleRetrySources {
		return 0
	}
	return s.Retries[kind-1][src]
}

// StaleRetryChainOpen reports whether key has a chain that is scheduled or
// running (not abandoned).
func StaleRetryChainOpen(key string) bool {
	open, abandoned := StaleRetryChainOpenForTest(key)
	return open && !abandoned
}

// StaleRetryChainOpenForTest reports whether key has a chain (abandoned or not).
// Production MUST NOT use it.
func StaleRetryChainOpenForTest(key string) (open, abandoned bool) {
	staleRetryMu.Lock()
	defer staleRetryMu.Unlock()
	c := staleRetryChains[key]
	if c == nil {
		return false, false
	}
	return true, c.abandoned
}

// WaitStaleRetryForTest joins every attempt in flight. Production MUST NOT use it.
func WaitStaleRetryForTest() { staleRetryWG.Wait() }

// ResetStaleRetryForTest stops every chain's pending wake, joins the attempts
// in flight and zeroes the engine. Production MUST NOT use it.
func ResetStaleRetryForTest() { resetStaleRetryForTest() }

func resetStaleRetryForTest() {
	stopStaleRetryChainsForTest()
	staleRetryWG.Wait()
	for k := range staleRetryTotal {
		for src := range staleRetryTotal[k] {
			staleRetryTotal[k][src].Store(0)
		}
	}
	staleRetryAbandonedNoV.Store(0)
	staleRetryAbandonedEvict.Store(0)
	staleRetryResumedTotal.Store(0)
}

// stopStaleRetryChainsForTest stops every pending wake and drops every chain
// (no new attempt can start); attempts already running finish on their own.
func stopStaleRetryChainsForTest() {
	staleRetryMu.Lock()
	for _, c := range staleRetryChains {
		if c.timer != nil {
			c.timer.Stop()
		}
		if c.spec.Release != nil {
			c.spec.Release()
		}
	}
	staleRetryChains = map[string]*staleRetryChain{}
	staleRetryMu.Unlock()
}

var staleRetryExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerStaleRetryExpvar()
}

// registerStaleRetryExpvar publishes the snowplow_stale_retry_* family (CFG-1:
// from a Disabled()-gated init, so cache-off has no keys). The OTLP twins read
// the same StaleRetryStatsSnapshot.
func registerStaleRetryExpvar() {
	staleRetryExpvarOnce.Do(func() {
		expvar.Publish("snowplow_stale_retry_total", expvar.Func(func() any {
			s := StaleRetryStatsSnapshot()
			out := make(map[string]map[string]uint64, len(StaleRetryKinds))
			for k, kind := range StaleRetryKinds {
				out[kind] = make(map[string]uint64, len(StaleRetrySources))
				for src, name := range StaleRetrySources {
					out[kind][name] = s.Retries[k][src]
				}
			}
			return out
		}))
		expvar.Publish("snowplow_stale_retry_chains_active", expvar.Func(func() any {
			return StaleRetryStatsSnapshot().ChainsActive
		}))
		expvar.Publish("snowplow_stale_retry_viewers", expvar.Func(func() any {
			return StaleRetryStatsSnapshot().Viewers
		}))
		expvar.Publish("snowplow_stale_retry_abandoned_total", expvar.Func(func() any {
			s := StaleRetryStatsSnapshot()
			return map[string]uint64{"no_viewer": s.AbandonedNoViewer, "evicted": s.AbandonedEvicted}
		}))
		expvar.Publish("snowplow_stale_retry_resumed_total", expvar.Func(func() any {
			return StaleRetryStatsSnapshot().Resumed
		}))
		expvar.Publish("snowplow_stale_retry_backoff_ms", expvar.Func(func() any {
			return StaleRetryStatsSnapshot().BackoffMS
		}))
	})
}

// RegisterStaleRetryExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterStaleRetryExpvarForTest() { registerStaleRetryExpvar() }

// StaleRetryAbandonedReasons is the closed `reason` set of
// snowplow_stale_retry_abandoned_total.
var StaleRetryAbandonedReasons = []string{"no_viewer", "evicted"}

// AddStaleRetryForTest moves the engine's counters through their production
// atomics, so a cross-package arm can prove the OTLP mirror reads the LIVE
// values. Production MUST NOT use it.
func AddStaleRetryForTest(kind StaleRetryOutcome, src StaleRetrySource, retries, noViewer, evicted, resumed uint64) {
	if (kind == StaleRetryTransient || kind == StaleRetryGenuine) && src < staleRetrySources {
		staleRetryTotal[kind-1][src].Add(retries)
	}
	staleRetryAbandonedNoV.Add(noViewer)
	staleRetryAbandonedEvict.Add(evicted)
	staleRetryResumedTotal.Add(resumed)
}
