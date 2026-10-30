// pip_stage_timing.go — Ship 0.30.192 + 0.30.193 PURE-ADDITIVE INSTRUMENTATION.
//
// Per-stage timing sink for PIP seed cohort restactions. Ship 0.30.193
// extends the 0.30.192 stage-level capture (ClusterListUsed,
// IteratorElapsedMs, IteratorCalls) with FOUR new checkpoint families
// designed to pinpoint the 91s wall-clock gap between iterator end and
// stage end observed on the sentinel cohort's allCompositions stage at
// 0.30.192:
//
//   C3: per-call apistageContentServe hit/miss/elapsed
//   C4: populateCohortGateMemo permitAll/input/kept/elapsed
//   Defensive: dispatch + parse + put + envelope bytes (cluster_list.go)
//
// CONCURRENCY: one PIPStageTimingSink is created per seedOneRestaction /
// seedOneWidget invocation and attached to that goroutine's context.
// The inner-call iterator runs an errgroup with up to GOMAXPROCS workers
// — these workers DO concurrently call AccumulateContentServe and
// AccumulateMemoPopulate. The sink's existing sync.Mutex now protects
// BOTH:
//   - currentStage pointer reassignment by the parent goroutine between
//     stages (BeginStage / EndStage)
//   - worker-side AccumulateContentServe / AccumulateMemoPopulate /
//     AccumulateDefensive that read currentStage and mutate the struct.
// At GOMAXPROCS-typical 16 workers and per-call work ~10ms, mutex
// contention is sub-µs — negligible vs the 73s iterator wall-clock the
// stage is trying to attribute.
//
// PURE-ADDITIVE: nil-safe everywhere — a ctx without WithPIPStageTimingSink
// returns a nil *PIPStageTimingSink from PIPStageTimingSinkFrom, and
// every sink-method short-circuits on a nil receiver. Production paths
// (the live /call request, the SA content-warm, the L1 refresher) carry
// no sink and pay zero overhead beyond the nil-receiver branch.
// REFRESHER-SAFETY (feedback_refresher_populate_amplification): the
// L1 refresher does NOT install a sink → PIPStageTimingSinkFrom returns
// nil → every Accumulate* call is a nil-receiver no-op → ZERO refresher
// amplification.

package cache

import (
	"context"
	"sync"
)

// PIPStageTiming captures per-stage cost-attribution data for ONE
// spec.api[] stage inside a seedOneRestaction resolve. Ship 0.30.193
// extends the 0.30.192 fields with content-serve / memo-populate /
// defensive-prefetch sub-stage timings.
type PIPStageTiming struct {
	// StageID is the spec.api[].name of the stage (e.g.
	// "compositions-by-ns"). Used in log diff between cohorts.
	StageID string `json:"stage_id"`

	// ElapsedMs is the wall-clock from stage-start to stage-end (the
	// outer for-loop iteration in resolve.go's stage loop). Includes
	// endpoint resolution + cluster-list gate check + iterator dispatch
	// + apistage-content serve.
	ElapsedMs int64 `json:"elapsed_ms"`

	// ClusterListAttempted is true when attemptClusterListCollapse was
	// reached for the stage. Since Ship S.1 removed the per-RA opt-in
	// (formerly ClusterListWhenAllowed), the resolver evaluates the gate
	// for EVERY iterator stage, so this is true whenever the stage had an
	// iterator the resolver could attempt to collapse.
	ClusterListAttempted bool `json:"cl_attempt"`

	// ClusterListUsed reports the outcome of the gate: true means the
	// helper returned useClusterList=true and tmp was collapsed to a
	// single cluster-scope call. False means a deny — see
	// ClusterListDenyGate.
	ClusterListUsed bool `json:"cl_used"`

	// ClusterListDenyGate is the gate number (1-9) that triggered the
	// false return from attemptClusterListCollapse — see cluster_list.go.
	// 0 means no deny (gate passed, ClusterListUsed=true) OR the helper
	// was not attempted. Values:
	//   1 — collapse INERT (Ship S.1-re sequencing gate; S.2 flips clusterListCollapseEnabled)
	//   2 — cache-off / snapshot pre-readiness / apistage substrate absent
	//   3 — no iterator on the stage
	//   4 — GVR derivation failed (iterator path not namespace-scoped)
	//   5 — RBAC permission deny
	//   6 — dispatch returned non-servable (rare — pre-sync informer)
	//   7 — shape check failed (cluster-list multi-element check)
	//   8 — the cell is cold: per-item fallback, async populate scheduled
	//   9 — the stage's verb is not a plain GET: a collapse would replace its
	//       calls with a GET they are not (#1011)
	ClusterListDenyGate int `json:"cl_deny_gate"`

	// IteratorCalls is the number of inner-call elements the bounded
	// errgroup dispatched for this stage. For a cluster-list-collapsed
	// stage this is 1. For a per-NS iterator path with no collapse, this
	// is the number of namespaces matched by the iterator query.
	IteratorCalls int `json:"iter_calls"`

	// IteratorElapsedMs is the wall-clock from g.SetLimit() to
	// g.Wait() for this stage's inner-call errgroup. The dominant cost
	// of a high-fan-out iterator stage; isolates iteration cost from
	// endpoint-resolution / gate-check cost.
	IteratorElapsedMs int64 `json:"iter_elapsed_ms"`

	// ContentHits / ContentMisses — Ship 0.30.193 Checkpoint 3. Counts
	// of apistageContentServe outcomes (hit on store.Get, miss
	// triggering dispatchViaInformer + Put). Accumulated by workers.
	ContentHits   int `json:"content_hits"`
	ContentMisses int `json:"content_misses"`

	// ContentElapsedMs — Ship 0.30.193 Checkpoint 3. Aggregate
	// wall-clock cost of all apistageContentServe invocations for this
	// stage (sum of per-call elapsed). For an N-fanout stage the
	// per-call cost serialised would sum to N × per_call_ms; under the
	// bounded errgroup the WALL-clock is IteratorElapsedMs and this
	// figure is the SUM of CPU-busy time across workers — a
	// throughput-shape indicator.
	ContentElapsedMs int64 `json:"content_elapsed_ms"`

	// ApistageMemoPermitAll — Ship 0.30.193 Checkpoint 4. The last-seen
	// CohortNSACL verdict for this stage's content (true = cluster-wide
	// list grant; false = namespace-scoped). When a stage produces N
	// content entries (iterator path with no collapse) this captures the
	// verdict of the most-recent populate; for the cluster-list collapse
	// path there is one entry so the value is unambiguous.
	ApistageMemoPermitAll bool `json:"memo_permit_all"`

	// ApistageMemoInputCount — Ship 0.30.193 Checkpoint 4. The
	// parsed.items length the LAST populateCohortGateMemo call observed
	// for this stage. For 29,907-composition clusters this confirms the
	// memo populate is in fact walking 29,907 items.
	ApistageMemoInputCount int `json:"memo_input_count"`

	// ApistageMemoKeptCount — Ship 0.30.193 Checkpoint 4. The post-ACL
	// kept-set size from the LAST populateCohortGateMemo call. For
	// permitAll=true this equals input_count (every item kept); for
	// permitAll=false it is the per-namespace-grant filtered subset.
	ApistageMemoKeptCount int `json:"memo_kept_count"`

	// ApistageMemoPopulateElapsedMs — Ship 0.30.193 Checkpoint 4.
	// Aggregate wall-clock cost of all populateCohortGateMemo calls for
	// this stage. If this dominates the 91s gap, the long-pole fix is
	// to move populate off the cold path (lazy populate, or strip the
	// RBAC fast-path on >10K items).
	ApistageMemoPopulateElapsedMs int64 `json:"memo_populate_elapsed_ms"`

	// DefensiveDispatchElapsedMs — Ship 0.30.193 Defensive prefetch
	// breakdown. The dispatchViaInformer call inside
	// attemptClusterListCollapse (cluster_list.go:213) that produces the
	// raw cluster-scope envelope. Cluster-list collapse path only;
	// non-collapse stages leave this 0.
	DefensiveDispatchElapsedMs int64 `json:"defensive_dispatch_elapsed_ms"`

	// DefensiveParseElapsedMs — Ship 0.30.193 Defensive prefetch
	// breakdown. The parseListEnvelope call inside
	// attemptClusterListCollapse (cluster_list.go:268) — the SECOND
	// unmarshal whose cost was never measured. If this dominates, the
	// fix is to dedup the double-unmarshal between validateClusterListShape
	// and parseListEnvelope.
	DefensiveParseElapsedMs int64 `json:"defensive_parse_elapsed_ms"`

	// DefensivePutElapsedMs — Ship 0.30.193 Defensive prefetch
	// breakdown. The apistageStore.Put call inside
	// attemptClusterListCollapse (cluster_list.go:273) — the LRU insert
	// of the validated envelope.
	DefensivePutElapsedMs int64 `json:"defensive_put_elapsed_ms"`

	// DefensiveEnvelopeBytes — Ship 0.30.193 Defensive prefetch
	// breakdown. The byte length of the dispatched cluster-scope
	// envelope (cluster_list.go:213's rawEnvelope). At 29,907 compositions
	// this is expected to be ~100 MB; a smaller value means the
	// dispatch returned a subset (informer not fully synced).
	DefensiveEnvelopeBytes int64 `json:"defensive_envelope_bytes"`
}

// PIPStageTimingSink collects per-stage timings for a single
// seedOneRestaction / seedOneWidget call. Constructed by the caller
// and attached to ctx via WithPIPStageTimingSink. The resolver reads
// the sink from ctx via PIPStageTimingSinkFrom and appends one entry
// per spec.api[] stage.
//
// Ship 0.30.193 extends the sink to support worker-side accumulation
// into a "current stage" pointer via BeginStage / EndStage. Workers
// call AccumulateContentServe / AccumulateMemoPopulate /
// AccumulateDefensive — all of these acquire mu, read s.current, and
// mutate fields in-place. If s.current is nil (no stage in progress
// or sink not wired) every Accumulate* is a no-op.
type PIPStageTimingSink struct {
	mu      sync.Mutex
	stages  []PIPStageTiming
	current *PIPStageTiming // pointer to the in-flight stage; nil between stages
}

// NewPIPStageTimingSink constructs an empty sink. Returns a non-nil
// pointer; callers attach it to ctx and pass that ctx to Resolve.
func NewPIPStageTimingSink() *PIPStageTimingSink {
	return &PIPStageTimingSink{stages: make([]PIPStageTiming, 0, 8)}
}

// BeginStage registers a *PIPStageTiming as the in-flight stage for
// this sink. Workers' Accumulate* calls write into *stage under mu.
// Nil-safe: a nil receiver or nil stage is a no-op.
//
// Concurrency contract: the resolver's stage loop is single-threaded
// at the BeginStage / EndStage boundaries (the parent goroutine of
// the errgroup). BeginStage MUST be called BEFORE g.Go workers can
// start; EndStage MUST be called AFTER g.Wait() returns. Between
// BeginStage and EndStage, workers may concurrently call Accumulate*
// — the sink's mu serialises those writes.
func (s *PIPStageTimingSink) BeginStage(stage *PIPStageTiming) {
	if s == nil || stage == nil {
		return
	}
	s.mu.Lock()
	s.current = stage
	s.mu.Unlock()
}

// EndStage commits the in-flight stage to the stages slice and clears
// the current pointer. The committed entry is a COPY of *stage at the
// moment of EndStage (after the parent goroutine has set ElapsedMs +
// IteratorElapsedMs). Nil-safe.
//
// The copy happens under mu so a late worker that races with EndStage
// either:
//   - lands its write BEFORE EndStage clears current (write captured)
//   - finds s.current == nil and no-ops
// In both cases the committed entry is well-formed; there is no
// torn-struct read.
func (s *PIPStageTimingSink) EndStage() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.current != nil {
		s.stages = append(s.stages, *s.current)
		s.current = nil
	}
	s.mu.Unlock()
}

// Append adds a finalised PIPStageTiming entry to the sink directly,
// bypassing the BeginStage/EndStage flow. Used by the 0.30.192 stage
// loop for stage entries that completed before Ship 0.30.193 wired
// BeginStage/EndStage — kept for back-compat. Nil-safe.
func (s *PIPStageTimingSink) Append(t PIPStageTiming) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, t)
}

// AccumulateContentServe records ONE apistageContentServe outcome
// into the in-flight stage. Called from worker goroutines after the
// hit/miss branch resolves. Nil-safe (no sink, no current stage).
func (s *PIPStageTimingSink) AccumulateContentServe(hit bool, elapsedMs int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return
	}
	if hit {
		s.current.ContentHits++
	} else {
		s.current.ContentMisses++
	}
	s.current.ContentElapsedMs += elapsedMs
}

// AccumulateMemoPopulate records ONE populateCohortGateMemo outcome
// into the in-flight stage. permitAll / inputCount / keptCount are
// overwritten (the LAST populate's values win — at the cluster-list
// collapse path there is exactly one populate per stage). elapsedMs
// is accumulated (multi-populate stages sum). Nil-safe.
func (s *PIPStageTimingSink) AccumulateMemoPopulate(permitAll bool, inputCount, keptCount int, elapsedMs int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return
	}
	s.current.ApistageMemoPermitAll = permitAll
	s.current.ApistageMemoInputCount = inputCount
	s.current.ApistageMemoKeptCount = keptCount
	s.current.ApistageMemoPopulateElapsedMs += elapsedMs
}

// AccumulateDefensive records the defensive-prefetch sub-stage timings
// from attemptClusterListCollapse into the in-flight stage. envBytes
// is overwritten (one defensive prefetch per stage); dispatch / parse
// / put elapsed are accumulated (back-compat for any future
// multi-defensive flow). Nil-safe.
func (s *PIPStageTimingSink) AccumulateDefensive(envBytes int64, dispatchMs, parseMs, putMs int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return
	}
	s.current.DefensiveEnvelopeBytes = envBytes
	s.current.DefensiveDispatchElapsedMs += dispatchMs
	s.current.DefensiveParseElapsedMs += parseMs
	s.current.DefensivePutElapsedMs += putMs
}

// Snapshot returns a defensive copy of the sink's stages slice. The
// caller (phase1_pip_seed.go's deferred log line) reads it ONCE at the
// end of seedOneRestaction. A nil receiver returns nil.
func (s *PIPStageTimingSink) Snapshot() []PIPStageTiming {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PIPStageTiming, len(s.stages))
	copy(out, s.stages)
	return out
}

// ctxKeyPIPStageTimingSinkType is the typed context key for the
// PIPStageTimingSink seam. Follows the same anonymous-struct-typed-key
// pattern as ctxKeyApistagePrewarmType and friends.
type ctxKeyPIPStageTimingSinkType struct{}

var ctxKeyPIPStageTimingSink = ctxKeyPIPStageTimingSinkType{}

// WithPIPStageTimingSink returns a child context carrying the given
// sink. A nil ctx is returned unchanged (matches the WithApistagePrewarm
// nil-safety contract).
func WithPIPStageTimingSink(ctx context.Context, sink *PIPStageTimingSink) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyPIPStageTimingSink, sink)
}

// PIPStageTimingSinkFrom returns the sink attached to ctx, or nil when
// no sink is wired. Nil-safe on a nil ctx. The resolver's stage loop
// uses this to optionally record timing — the production /call path
// never installs a sink, so the resolver pays one nil-check per stage
// and nothing else.
func PIPStageTimingSinkFrom(ctx context.Context) *PIPStageTimingSink {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeyPIPStageTimingSink).(*PIPStageTimingSink)
	return v
}
