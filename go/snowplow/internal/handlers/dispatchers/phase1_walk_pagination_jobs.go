// phase1_walk_pagination_jobs.go — Path 3.2.2.b (0.30.221): deferred
// apiRef pagination.
//
// PROBLEM the file solves
//
// Path 3.2.2 (0.30.220) ran iterateApiRefPages INLINE on the Phase 1
// walker goroutine. At 50K-composition scale the per-widget pagination
// (up to 500 pages × ~125–500ms each) extended the walk past the 360s
// kubelet startup probe budget — pod never became Ready, restart loop.
// The MECHANISM was empirically validated correct (5× more walk events;
// new per-composition CRs reached); only the SCHEDULING was wrong.
//
// THE FIX — collect-then-drain
//
// During the Phase 1 walk, when the page-1 resolve sees an
// apiRef+resourcesRefsTemplate widget whose `.slice.continue==true`,
// the walker COLLECTS a paginationJob into a shared, mutex-guarded
// collector instead of paginating inline. Phase1Warmup completes Step 4
// (walk) → Step 7 (informer sync barrier) → Step 8 (MarkPhase1Done —
// /readyz flips to 200) UNAFFECTED.
//
// AFTER MarkPhase1Done, phase1WarmupWith launches a SINGLE bounded
// background goroutine (symmetric with the existing PIP seed at Step
// 7.6 — see phase1_walk.go:563-596) that drains every collected job
// through the unchanged iterateApiRefPages mechanism. The drain runs
// under a fresh context.Background-derived seed context with its own
// timeout (paginationDrainTimeout) so it OUTLIVES Phase1Warmup's ctx
// (which main.go cancels the instant Phase1Warmup returns).
//
// INVARIANTS preserved from Path 3.2.2:
//   - Mechanism unchanged — iterateApiRefPages is the same code path.
//   - Per-widget bound: the OPERATIVE terminator is the blueprint's
//     `.slice.continue==false`; phase1MaxApiRefPages is the liveness
//     backstop (raised to 20,000 in #156 / 0.30.256 so it does not
//     truncate the real population — see its doc).
//   - Data-driven predicate (isApiRefTemplateDriven on widget SHAPE);
//     no hardcoded GVRs.
//   - Bounded by parent ctx — cancellation on pod shutdown propagates
//     into iterateApiRefPages' per-page ctx.Err() check.
//
// REFRESHER AMPLIFICATION DISCIPLINE (feedback_refresher_populate_
// amplification): the drain Puts new identity-free widgetContent L1
// cells. The L1 refresher then exercises those cells every cycle. We
// instrument paginationDrainTimeout deliberately bounded (5 min, same
// shape as pipGlobalTimeout / 8 min) so a runaway apiRef cannot leak
// goroutines past the bounded budget; the per-job liveness backstop
// (phase1MaxApiRefPages) further bounds the work each apiRef widget can
// produce. #156 raises the backstop to cover the FULL declared list, so
// the drain now Puts MORE cells of the SAME existing widgetContent class
// (entry-count growth, not a new populate LAYER — the 0.30.185 distinction);
// the per-page customer-priority yield below keeps that one-time growth off
// the customer's latency budget.
//
// PRIOR ART: client-go has no equivalent — apiserver pagination is a
// snowplow concern. The collect-then-drain pattern mirrors the PIP seed
// (Step 7.6 background) which has been in production since 0.30.179.

package dispatchers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// paginationDrainTimeout is the absolute wall-clock budget for the
// background pagination drain. Bounded so a pathological apiRef
// (resolver always reports .slice.continue=true) cannot leave the drain
// goroutine running until process exit. At today's apiRef widget count
// (2: compositions-page-datagrid, blueprints-page-datagrid) × 500 pages
// × ~500ms wall-clock each (worst case observed on 0.30.220) the drain
// completes well inside 5 min; the timeout is a backstop, not the
// expected exit path.
//
// Code-time constant per Diego's "no new env vars / flags" mandate.
const paginationDrainTimeout = 5 * time.Minute

// apiRefPaginationJob carries everything the deferred pagination needs
// to call iterateApiRefPages from the background drain goroutine. The
// fields are captured by-value (or by-pointer to immutable state) at
// COLLECT time so the drain does not depend on the walker's mutable
// state (the walker is gone by the time the drain runs).
type apiRefPaginationJob struct {
	// In is the page-1 widget CR. The drain re-fetches a fresh copy
	// via objects.Get for each subsequent page (the resolver mutates
	// its input), so we keep In only to read its (ns, name) — but
	// holding the full unstructured is cheap and matches the original
	// inline call shape.
	In *unstructured.Unstructured

	// GVR is the widget's GroupVersionResource — threaded to
	// populateWidgetContentL1 so the seed cell matches the serve-time
	// dispatcher.
	GVR schema.GroupVersionResource

	// Page1Res is the page-1 resolved envelope. iterateApiRefPages
	// reads its .slice.continue signal to decide whether to start
	// paginating; we keep the full envelope (not just the bool) so
	// the isApiRefTemplateDriven shape check inside iterateApiRefPages
	// stays the canonical predicate (no duplication of logic at
	// collect time).
	Page1Res *unstructured.Unstructured

	// Depth is the walker depth at which this widget was reached.
	// Subsequent-page child recursion continues from depth+1 — same
	// as the page-1 children loop.
	Depth int

	// Pagination is the pagination the first page was resolved AND keyed
	// with (the walk's pg, #966). The drain resolves and keys each next page
	// with Pagination.pageAfter(n); the re-collection re-resolves the first
	// page with Pagination itself.
	Pagination prewarmPagination

	// AuthnNS is the authn namespace — threaded to widgets.Resolve.
	AuthnNS string
}

// jobKey returns the dedupe key for a job. Two roots may reach the
// same apiRef widget; we paginate only once per (gvr, ns, name) —
// idempotent at the L1 layer (Put is by content key), but skipping the
// duplicate work saves the up-to-500-page wall clock.
func (j apiRefPaginationJob) jobKey() string {
	if j.In == nil {
		return ""
	}
	return j.GVR.String() + "|" + j.In.GetNamespace() + "|" + j.In.GetName()
}

// apiRefPaginationCollector is the mutex-guarded set of pagination jobs
// collected during the Phase 1 walk. The walker writes via
// collectApiRefPaginationJob; the drain reads via Drain after
// MarkPhase1Done.
//
// Concurrency: the walk is single-threaded per root and roots resolve
// sequentially (phase1WarmupWith), but the mutex makes the collector
// safe regardless of how the walk is scheduled — same shape as
// contentPrewarmHarvester / navWidgetHarvester.
type apiRefPaginationCollector struct {
	mu   sync.Mutex
	jobs map[string]apiRefPaginationJob

	// pendingRecollect holds the COLLECTION-ROBUSTNESS candidates (Task
	// #318 Step 1, design option (c)): widgets the walk reached that ARE
	// eligible (isApiRefTemplateDriven) but whose page-1 resolve did NOT
	// signal continuation. On a healthy boot this is empty; on a POST-STORM
	// boot the datagrid's page-1 resolve can see a transiently short/empty
	// apiRef RA (the same data-availability window the empty-shell guard
	// defends) → no continuation → the job is (correctly) NOT collected.
	// Pre-#318 that condition was SILENT — the page-2..N coverage dropped to
	// literally zero with no signal. We record the candidate here so the
	// post-MarkPhase1Done drain can re-resolve page-1 after the informer
	// settles (recollectPendingApiRefPaginationJobs) and, if continuation now
	// fires, collect the job. Keyed by jobKey so duplicate eligible-no-continue
	// observations across roots coalesce. A candidate that later collects
	// normally (a second walk reached it with continuation) is removed.
	pendingRecollect map[string]apiRefPaginationJob
}

// newApiRefPaginationCollector returns an empty collector.
func newApiRefPaginationCollector() *apiRefPaginationCollector {
	return &apiRefPaginationCollector{
		jobs:             map[string]apiRefPaginationJob{},
		pendingRecollect: map[string]apiRefPaginationJob{},
	}
}

// collectApiRefPaginationJob appends j to the collector iff the page-1
// envelope is shape-eligible (apiRef+resourcesRefsTemplate) AND the
// resolver signals continuation. Nil-safe: a nil collector is a no-op
// (matches Phase 1's flag-off posture). Idempotent for duplicate
// (gvr, ns, name) — first write wins.
//
// The two predicates are checked HERE, not inside the drain, so the
// collector's `jobs` map contains ONLY work that would actually
// paginate. Empty collector at drain time => background goroutine is a
// fast no-op; matches the byte-identical-when-flag-off posture.
func (c *apiRefPaginationCollector) collect(j apiRefPaginationJob) {
	if c == nil || j.In == nil || j.Page1Res == nil {
		return
	}
	// #966 — a first page resolved (and keyed) unpaginated has no next page:
	// its body is the whole list, exactly as an unpaginated /call's. Nothing
	// to drain, nothing to re-collect.
	if !j.Pagination.paginated() {
		return
	}
	if !isApiRefTemplateDriven(j.Page1Res.Object) {
		return
	}
	key := j.jobKey()
	if key == "" {
		return
	}
	if !resolverWantsContinue(j.Page1Res) {
		// COLLECTION ROBUSTNESS (Task #318 Step 1, design option (c)): the
		// widget IS eligible (apiRef+template) but page-1 produced NO
		// continuation. On a healthy boot this is the normal end-of-list and
		// nothing more is needed. On a POST-STORM boot it can be a transiently
		// short/empty apiRef RA → the page-2..N coverage would silently drop to
		// zero. Record the candidate (keyed by jobKey, so cross-root duplicates
		// coalesce) + bump the observable counter so the drain can re-resolve
		// page-1 after the informer settles. NOT collected as a job — page-1
		// genuinely said "no more" at walk time; only a retry that NOW sees
		// continuation produces a job (recollectPendingApiRefPaginationJobs).
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, alreadyJob := c.jobs[key]; alreadyJob {
			// A continuing observation already collected this widget — the
			// eligible-no-continue observation is stale, ignore it.
			return
		}
		if _, seen := c.pendingRecollect[key]; !seen {
			c.pendingRecollect[key] = j
			bumpPrewarmEligibleNoContinue()
		}
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.jobs[key]; seen {
		// First-write-wins. Same widget reached via two roots is rare
		// (the visited-set in phase1Walker.walk already prevents
		// re-traversal within a root) but cross-root collisions are
		// possible. Idempotent — same widget would Put the same cells.
		return
	}
	c.jobs[key] = j
	// A continuing observation supersedes any earlier eligible-no-continue
	// candidate for the same widget — drop it so the drain does not also
	// re-collect a widget already queued for the normal drain.
	delete(c.pendingRecollect, key)
}

// drain returns a snapshot of the collected jobs and clears the
// collector. Called by the background drain goroutine after
// MarkPhase1Done. Idempotent — a second call returns an empty slice.
func (c *apiRefPaginationCollector) drain() []apiRefPaginationJob {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.jobs) == 0 {
		return nil
	}
	out := make([]apiRefPaginationJob, 0, len(c.jobs))
	for _, j := range c.jobs {
		out = append(out, j)
	}
	c.jobs = map[string]apiRefPaginationJob{}
	return out
}

// count returns the current job count without draining. Used by the
// drain logger for the "jobs_collected" field and by tests.
func (c *apiRefPaginationCollector) count() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

// pendingRecollectCount returns the number of eligible-but-no-continuation
// candidates currently queued for re-collection (Task #318 Step 1). Used by
// recollectPendingApiRefPaginationJobs' logger and by tests. Nil-safe.
func (c *apiRefPaginationCollector) pendingRecollectCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingRecollect)
}

// drainPendingRecollect returns a snapshot of the eligible-but-no-continuation
// candidates and clears them. Symmetric with drain(): the snapshot is what the
// re-collection pass retries; clearing makes the pass one-shot (a candidate
// that retries successfully collects a job; one that still has no continuation
// is dropped — items beyond page 1 fall back to the per-user serve path, the
// same posture as a widget that legitimately ends at page 1). Nil-safe.
func (c *apiRefPaginationCollector) drainPendingRecollect() []apiRefPaginationJob {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pendingRecollect) == 0 {
		return nil
	}
	out := make([]apiRefPaginationJob, 0, len(c.pendingRecollect))
	for _, j := range c.pendingRecollect {
		out = append(out, j)
	}
	c.pendingRecollect = map[string]apiRefPaginationJob{}
	return out
}

// drainApiRefPaginationJobs runs the deferred pagination AFTER
// MarkPhase1Done. Each job is processed sequentially through the
// unchanged iterateApiRefPages mechanism. The drain context is
// independent of Phase1Warmup's ctx so a normal Phase 1 completion
// (main.go cancels p1Ctx the instant Phase1Warmup returns) does NOT
// kill an in-flight drain.
//
// Concurrency: jobs run sequentially. Per-job parallelism would compound
// the refresher-amplification risk (each apiRef widget materialises new
// L1 cells the refresher then exercises) — serial drain keeps the
// post-readiness CPU shape predictable. iterateApiRefPages' own per-page
// work is bounded by the blueprint's `.slice.continue` halt (the operative
// terminator) + the phase1MaxApiRefPages liveness backstop.
//
// Customer priority (#156 / 0.30.256): each job is preceded by an
// engineYieldCheckpoint so a customer /call arriving mid-drain defers the
// drain at job granularity; the per-PAGE yield inside iterateApiRefPages
// covers within-job bursts. Same cooperative yield + customerInFlight()
// atomic the engine uses; NO shared budget
// (feedback_customer_priority_over_refresher). Without this, raising the
// page backstop would turn the drain into a long-running CPU consumer that
// competes with customer /call for the whole window.
//
// Cancellation: parent ctx cancel propagates into iterateApiRefPages'
// per-page ctx.Err() check (same path the inline 0.30.220 used). On pod
// shutdown the drain goroutine returns at the next page boundary —
// never leaks past the bounded drain budget.
//
// Errors: log-only. A pagination failure on one widget MUST NOT prevent
// draining the next. The page-1 envelope (Put before MarkPhase1Done) is
// always correct for that widget — pagination is a NICE-TO-HAVE that
// fills cells for items 6..N; missing pages fall back to the existing
// per-user serve-time path.
//
// Panic recovery: the caller (phase1WarmupWith) wraps the goroutine in
// a defer-recover so a single bad job cannot crash the process. This
// function itself does NOT recover — letting a panic bubble to the
// recover at the goroutine boundary so it is logged once with the
// full panic value.
func drainApiRefPaginationJobs(
	ctx context.Context,
	jobs []apiRefPaginationJob,
	saEP endpoints.Endpoint,
	saRC *rest.Config,
) {
	log := xcontext.Logger(ctx)
	if log == nil {
		log = slog.Default()
	}
	start := time.Now()
	if len(jobs) == 0 {
		log.Info("phase1.pagination_drain.empty",
			slog.String("subsystem", "cache"),
			slog.String("phase", "post_phase1_done"),
		)
		return
	}

	log.Info("phase1.pagination_drain.start",
		slog.String("subsystem", "cache"),
		slog.String("phase", "post_phase1_done"),
		slog.Int("jobs_collected", len(jobs)),
		slog.String("drain_budget", paginationDrainTimeout.String()),
	)

	// SA-credentialed context for the drain — mirrors withPhase1SAContext
	// but anchored on the drain's own ctx (NOT the dead Phase1Warmup
	// ctx). iterateApiRefPages calls objects.Get and widgets.Resolve, both
	// of which read the SA endpoint/restConfig from this ctx.
	drainCtx := withPhase1SAContext(ctx, saEP, saRC)

	completed := 0
	for _, j := range jobs {
		if err := ctx.Err(); err != nil {
			log.Warn("phase1.pagination_drain.ctx_cancel",
				slog.String("subsystem", "cache"),
				slog.Int("completed", completed),
				slog.Int("remaining", len(jobs)-completed),
				slog.Any("err", err),
				slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
			)
			return
		}
		// P8 customer-priority yield (#156 / 0.30.256): step aside for any
		// in-flight customer /call BEFORE starting the next job. The per-page
		// yield inside iterateApiRefPages handles bursts arriving mid-job;
		// this checkpoint handles bursts arriving between jobs. Fast no-op
		// when no customer is in flight (prewarm_engine.go:436-438). Uses the
		// drainCtx (SA-credentialed) so the yield observes the drain's own
		// cancellation, symmetric with the loop's ctx.Err() guard.
		engineYieldCheckpoint(drainCtx)
		// #871 — measurement only: one job is one drain unit (pprof label +
		// counter); its pages, their child walks and their stage goroutines
		// carry snowplow_work=drain.
		workclass.Do(drainCtx, workclass.Drain, func(jobCtx context.Context) {
			drainOneApiRefPaginationJob(jobCtx, j, saRC)
		})
		completed++
	}

	log.Info("phase1.pagination_drain.complete",
		slog.String("subsystem", "cache"),
		slog.String("phase", "post_phase1_done"),
		slog.Int("jobs_completed", completed),
		slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
	)
}

// drainOneApiRefPaginationJob runs one collected job through the unchanged
// iterateApiRefPages mechanism (split out of drainApiRefPaginationJobs for the
// #871 drain work-class bracket only).
func drainOneApiRefPaginationJob(ctx context.Context, j apiRefPaginationJob, saRC *rest.Config) {
	// Run the original 3.2.2 mechanism, unchanged. Returns no error
	// (logs internally); a failure leaves the page-1 cell correct
	// and proceeds to the next job.
	iterateApiRefPages(
		ctx,
		// The walker is gone by the time the drain runs. Construct a
		// minimal walker shell for the SAME helpers iterateApiRefPages
		// uses (visited set + harvesters). The visited set is
		// per-drain so children discovered by THIS drain are
		// internally deduped; we do NOT share with the original
		// walker's visited (the walker's visited is no longer
		// referenced post-MarkPhase1Done). Harvesters are nil — the
		// drain runs AFTER MarkPhase1Done so the content-pass / PIP-
		// seed harvesters have already been drained; populating them
		// now would have no consumer.
		// Ship 0.30.232: type-safe construction via newPhase1Walker.
		// Ship 0.30.230 fix-at-root preserved — the drain shell walker
		// MUST carry the SA *rest.Config so the page-N
		// widgets.Resolve at phase1_walk_pagination.go's literal
		// receives non-nil rc downstream of opts.RC. No harvesters
		// (drain post-MarkPhase1Done; consumers already drained).
		newPhase1Walker(saRC, j.AuthnNS),
		j.In,
		j.GVR,
		j.Page1Res,
		j.Depth,
		j.Pagination,
		j.AuthnNS,
	)
}

// paginationRecollectDelay is the informer-settle wait before the
// re-collection pass re-resolves page-1 for the eligible-but-no-continuation
// candidates (Task #318 Step 1). The post-storm zero-collection trigger is a
// transiently short/empty apiRef RA at boot; the wait lets the informer catch
// up before the retry. Bounded + ctx-cancellable (the wait short-circuits on
// drain ctx cancel). Code-time constant per Diego's "no new env vars / flags"
// mandate; a package var (not const) ONLY so the unit falsifier can set it to 0
// (recollectDelayForTest) — production never mutates it.
var paginationRecollectDelay = 15 * time.Second

// recollectDelayForTest overrides paginationRecollectDelay for unit tests so
// the re-collection falsifier does not wait the production settle window.
// Production code MUST NOT call this.
func recollectDelayForTest(d time.Duration) (restore func()) {
	prev := paginationRecollectDelay
	paginationRecollectDelay = d
	return func() { paginationRecollectDelay = prev }
}

// recollectPendingApiRefPaginationJobs is the COLLECTION-ROBUSTNESS retry
// (Task #318 Step 1, design option (c)). It composes with the post-MarkPhase1Done
// drain goroutine — it is NOT a new loop and spawns NO goroutine of its own.
//
// For each eligible-but-no-continuation candidate the walk recorded (a widget
// that IS apiRef+template-driven but whose page-1 resolve produced no
// continuation — the post-storm short/empty-list window), it waits an
// informer-settle delay then re-resolves PAGE 1 through the SAME
// paginationFetchPageFn / paginationResolvePageFn seams the drain uses. If the
// settled re-resolve NOW signals continuation, it hands the job back to
// collector.collect (idempotent — dedup by jobKey + the same shape predicates),
// so the caller's subsequent collector.drain() picks it up. A candidate that
// STILL has no continuation is dropped (drainPendingRecollect cleared it) —
// items beyond page 1 fall back to the per-user serve-time path, the same
// posture as a widget that legitimately ends at page 1.
//
// Bounded + log-only + ctx-aware, exactly like the drain:
//   - the settle wait short-circuits on ctx cancel;
//   - a per-candidate engineYieldCheckpoint keeps the retry off the customer's
//     latency budget (feedback_customer_priority_over_refresher);
//   - a fetch/resolve failure on one candidate is logged and the pass moves on.
//
// saRC is the SA *rest.Config the re-resolve passes into
// widgets.ResolveOptions.RC. It MUST be non-nil in production for the SAME
// reason the drain threads w.rc (phase1_walker_new.go): a nil rc 500s in
// crdschema.ValidateObjectStatus → cache.GVRFor → discoverPluralInfo
// ("plurals discovery: nil *rest.Config") — the nil-rc class behind six HARD
// REVERTs (0.30.226→231). The launcher passes the same SA rc the drain uses.
// Unit tests swap paginationResolvePageFn so they pass nil safely (the fake
// seam ignores opts.RC).
//
// nil-collector / no-candidate => fast no-op (matches the empty-drain posture).
func recollectPendingApiRefPaginationJobs(ctx context.Context, c *apiRefPaginationCollector, saRC *rest.Config) {
	if c == nil {
		return
	}
	log := xcontext.Logger(ctx)
	if log == nil {
		log = slog.Default()
	}

	candidates := c.drainPendingRecollect()
	if len(candidates) == 0 {
		return
	}
	// #871 — measurement only: the re-collection pass is one drain unit.
	ctx, endWork := workclass.Begin(ctx, workclass.Drain)
	defer endWork()

	log.Info("phase1.pagination_recollect.start",
		slog.String("subsystem", "cache"),
		slog.String("phase", "post_phase1_done"),
		slog.Int("candidates", len(candidates)),
		slog.String("settle_delay", paginationRecollectDelay.String()),
	)

	// Informer-settle wait — bounded, ctx-cancellable. The post-storm trigger
	// is data-availability lag; the delay lets the apiRef RA's informer catch
	// up before the retry. Skipped when the delay is non-positive (unit tests).
	if paginationRecollectDelay > 0 {
		t := time.NewTimer(paginationRecollectDelay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			log.Warn("phase1.pagination_recollect.ctx_cancel_during_settle",
				slog.String("subsystem", "cache"),
				slog.Int("candidates", len(candidates)),
				slog.Any("err", ctx.Err()),
			)
			return
		case <-t.C:
		}
	}

	recollected := 0
	for _, cand := range candidates {
		if err := ctx.Err(); err != nil {
			log.Warn("phase1.pagination_recollect.ctx_cancel",
				slog.String("subsystem", "cache"),
				slog.Int("recollected", recollected),
				slog.Any("err", err),
			)
			return
		}
		// Customer priority: defer the retry for any in-flight customer /call.
		engineYieldCheckpoint(ctx)

		ns := cand.In.GetNamespace()
		name := cand.In.GetName()
		ref := templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: name, Namespace: ns},
			Resource:   cand.GVR.Resource,
			APIVersion: cand.GVR.GroupVersion().String(),
		}
		got := paginationFetchPageFn(ctx, ref)
		if got.Err != nil || got.Unstructured == nil {
			log.Warn("phase1.pagination_recollect.fetch_failed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", cand.GVR.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Any("err", got.Err),
			)
			continue
		}
		// Re-resolve the FIRST page (we are re-trying the first page's
		// continuation decision, not advancing). #966: with the candidate's own
		// pagination — the one its first page was resolved and keyed with.
		res, err := paginationResolvePageFn(ctx, widgets.ResolveOptions{
			In:      got.Unstructured,
			RC:      saRC, // SA rest.Config — see the function doc (nil-rc 500 class).
			AuthnNS: cand.AuthnNS,
			PerPage: cand.Pagination.PerPage,
			Page:    cand.Pagination.Page,
		})
		if err != nil || res == nil {
			log.Warn("phase1.pagination_recollect.resolve_failed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", cand.GVR.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Any("err", err),
			)
			continue
		}
		// If the settled re-resolve NOW signals continuation, hand the job back
		// to collect (it re-checks both shape predicates and dedups by jobKey,
		// landing it in c.jobs for the caller's drain). If it STILL has no
		// continuation we DROP it (the snapshot was already cleared by
		// drainPendingRecollect) — one-shot retry; items beyond page 1 fall back
		// to the per-user serve-time path until the next boot re-walk. We do NOT
		// re-queue here, which would re-bump the eligible-no-continue counter and
		// could loop the candidate across passes.
		if !resolverWantsContinue(res) {
			continue
		}
		cand.Page1Res = res
		c.collect(cand)
		recollected++
	}

	log.Info("phase1.pagination_recollect.complete",
		slog.String("subsystem", "cache"),
		slog.String("phase", "post_phase1_done"),
		slog.Int("candidates", len(candidates)),
		slog.Int("recollected", recollected),
	)
}
