// phase1_walk_pagination.go — Path 3.2.2 (0.30.220): walker pagination
// for apiRef+resourcesRefsTemplate widgets.
//
// PROBLEM the file solves
//
// The F2 INIT walker (phase1_walk.go) resolves each navigation widget
// exactly ONCE at the declared/default `(perPage, page)` tuple. For an
// apiRef+resourcesRefsTemplate-driven widget the resolver fans the
// template over the apiRef RA's sliced result — page 1 only — and the
// walker reads `status.resourcesRefs.items[]` to discover children. With
// perPage=5 (the bounded `PREWARM_PAGE_LIMIT` default per Ship 0.30.127),
// the compositions-page-datagrid produces 5 of ~50K composition rows; the
// downstream per-composition widget CRs (panels, buttons, markdowns) are
// never reached, never resolved, never populated in `widgetContent` L1.
// Customer hits manifest as `widget-content-miss-per-user-fallback`
// fallthrough cells (3,437 buttons + 3,401 markdowns observed on 0.30.219).
//
// THE FIX — page iteration over apiRef-template-driven widgets
//
// After the existing page-1 resolve+populate, IF the widget is
// apiRef+resourcesRefsTemplate-driven AND the resolver's own
// `status.resourcesRefs.slice.continue == true` signal fires, the walker
// continues with `page = 2, 3, …` until ANY of:
//   - the resolver returns `.slice.continue == false` (the OPERATIVE
//     terminator — the blueprint's declared pagination),
//   - the resolved items list is empty,
//   - `page > phase1MaxApiRefPages` (the LIVENESS BACKSTOP — an anomaly
//     exit, logged loud; see the constant's doc),
//   - the parent context is cancelled / timed out,
//   - `objects.Get` fails to re-fetch a fresh CR copy for the next page.
//
// For each additional page, the walker:
//   1. Calls `widgets.Resolve` on a FRESH CR copy (the resolver mutates
//      its input — we must NOT reuse the page-1 copy).
//   2. Calls `populateWidgetContentL1` to Put the page's envelope under
//      the matching identity-free key (KEY tuple decoupled from the
//      RESOLUTION tuple per Ship 0.30.187 D2 — the seed must match the
//      dispatcher's serve-time lookup).
//   3. Walks each new child ref (`status.resourcesRefs.items[]`)
//      discovered on this page, honouring the SAME `visited` set the
//      page-1 walk uses so a shared subtree is resolved once.
//
// DATA-DRIVEN — NO HARDCODED GVRs
//
// The pagination fires on the SHAPE predicate `isApiRefTemplateDriven`:
// both `spec.apiRef.name != ""` AND `spec.resourcesRefsTemplate` non-
// empty. This is the SAME shape `shouldSkipEmptyWidgetShell` and
// `isRBACSensitiveApiRefWidget` already key on (`widget_content.go`).
// No widget-name, no GVR literal. New per-composition widget GVRs the
// page reveals are discovered by parsing each child ref's `/call?` Path
// via the existing `util.ParseCallPathToObjectRef` — the same machinery
// the page-1 recursion already uses.
//
// COST BOUNDS
//
//   - Number of widgets paginated: only the apiRef+resourcesRefsTemplate
//     widgets the walker reaches from the INIT roots. At today's
//     cluster: compositions-page-datagrid, blueprints-page-datagrid,
//     plus any nested template-driven widgets that themselves drive a
//     list. Counted at runtime via `phase1.walk.apiref_pagination`.
//   - Per-widget pages: the OPERATIVE terminator is the blueprint's
//     declared pagination — the resolver's `.slice.continue==false`
//     (feedback_prewarm_walk_no_sampling_caps: bound by the declared
//     slice, NEVER a magic-number sample cap). `phase1MaxApiRefPages`
//     is a LIVENESS BACKSTOP ONLY (#156 / 0.30.256): it defends against
//     a pathological apiRef RA that reports `.slice.continue==true`
//     forever, and is set high enough (20,000 pages = 100K items at
//     perPage=5) that the real population cannot reach it in normal
//     operation. Hitting it is now an ANOMALY (logged loud Warn), not the
//     expected exit. A code-time constant per Diego's "no new env vars /
//     flags" mandate (project_single_cache_flag_direction).
//   - Per-page recursion: unchanged from today — `walkShouldRecurse`
//     gate (verb=="GET"), `w.visited` cycle-set, `phase1MaxWalkDepth`
//     cap. Each new composition reached spawns the same depth-bounded
//     subtree the page-1 walk would have produced for it.
//   - REFRESHER AMPLIFICATION (the 0.30.185 HARD REVERT lesson): the
//     resulting L1 entries are visible to the refresher. #156 raises the
//     backstop so MORE cells of the SAME existing widgetContent class are
//     populated (entry-count growth, not a new populate LAYER — the key
//     0.30.185 distinction). The 50K bench probe captures the refresher
//     cycle-cost delta BEFORE this ships.
//
// CONCURRENCY
//
// The walker is single-threaded per root (phase1_walk.go's
// resolveNavigationRoot does not goroutine-fan). `iterateApiRefPages`
// runs strictly in-line on the same goroutine, so `w.visited` access
// is safe without a mutex — same property the page-1 recursion relies
// on. Returns nothing: a pagination error is logged + the loop exits;
// the page-1 envelope already populated remains correct.
//
// CUSTOMER PRIORITY (#156 / 0.30.256)
//
// Raising the backstop to cover the FULL declared list (10K+ pages at
// 50K) turns the per-widget loop into a long-running CPU consumer. Each
// page iteration therefore opens with `engineYieldCheckpoint(ctx)` — the
// SAME cooperative yield + `customerInFlight()` atomic the prewarm engine
// uses, NO shared budget (feedback_customer_priority_over_refresher). A
// fast no-op when no customer is in flight.
//
// PREWARM GATE
//
// Wired through `cache.PrewarmEnabled()` — the same gate that controls
// whether Phase1Warmup runs at all (main.go won't even schedule the
// walker otherwise). #57: PrewarmEnabled() is implicit-on-cache; no
// prewarm-specific env var.

package dispatchers

import (
	"context"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/maps"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// phase1MaxApiRefPages is the per-widget apiRef-page LIVENESS BACKSTOP — a
// pure anti-runaway ceiling, NOT a coverage/sample cap. The OPERATIVE
// terminator for the page loop is the blueprint's declared pagination
// (`.slice.continue==false`, read via resolverWantsContinue); this constant
// only fires for a pathological apiRef RA that reports `.slice.continue==true`
// forever (a goroutine-leak / infinite-walk risk the file header calls out).
// Per feedback_prewarm_walk_no_sampling_caps a magic-number SAMPLE cap is
// forbidden; a liveness backstop set far above the real population is the
// honest bound, and hitting it is an ANOMALY (logged loud Warn at the loop
// exit) rather than the expected exit path.
//
// CEILING DERIVATION (#156 / 0.30.256 — Diego holds veto on this number):
//
//	perPage              = prewarmPageLimit() default = defaultPrewarmPageLimit = 5
//	max_realistic_items  = 50,000   (ratified production scope: 1000 users +
//	                                  50,000 compositions — project_production_scale,
//	                                  canonical 2026-05-14, supersedes the design
//	                                  doc's stale "10K"; the compositions-page-datagrid's
//	                                  compositions-list RA lists all compositions)
//	pages_for_population  = ceil(50,000 / 5)               = 10,000 pages
//	safety_margin         = 2x   (growth headroom + a slightly-larger-than-50K
//	                               cluster + a deployment that lowers perPage below 5)
//	phase1MaxApiRefPages  = 10,000 x 2                     = 20,000 pages
//	                      = 20,000 x 5 = 100,000 items covered at perPage=5
//
// 50K BENCH cross-check (SCALE=50000, ~49K panels, perPage=5):
//
//	bench worst case      = ceil(49,000 / 5)               = 9,800 pages/widget
//
// 20,000 clears the 9,800-page bench worst case ~2x over — comfortably, so a
// well-behaved RA exits on `.slice.continue==false` long before the backstop.
//
// 20,000 stays under the TestPhase1MaxApiRefPages_BoundedSane "absurdly large"
// guard (> 100,000) so a buggy unbounded RA still terminates. Exposed via a
// test-only override (phase1MaxApiRefPagesForTest) so unit tests need not
// materialise a 20,000-page mock pagination.
const phase1MaxApiRefPages = 20_000

// phase1MaxApiRefPagesForTest, when non-zero, overrides phase1MaxApiRefPages.
// Production code MUST NOT set this; the only caller is the test in
// phase1_walk_pagination_test.go. Kept as a file-level var (not a build
// tag) so the test does not have to vendor a copy of iterateApiRefPages.
var phase1MaxApiRefPagesForTest int

// maxApiRefPages returns the per-widget page backstop honouring the test
// override. Production callers always observe phase1MaxApiRefPages.
func maxApiRefPages() int {
	if phase1MaxApiRefPagesForTest > 0 {
		return phase1MaxApiRefPagesForTest
	}
	return phase1MaxApiRefPages
}

// paginationFetchPageFn / paginationResolvePageFn are the page-fetch and
// page-resolve seams iterateApiRefPages drives. Production code observes the
// real objects.Get / widgets.Resolve; the only swapper is the falsifier in
// phase1_walk_pagination_test.go (same 1-line `var fooFn = foo` pattern the
// package uses for seedCohortFn / seedOneWidgetFn). The seam lets the page
// loop's continuation / backstop / yield behaviour be unit-falsified without
// a live apiserver or a 20,000-page mock.
//
// paginationFetchPageFn re-fetches a FRESH widget CR copy for page N (the
// resolver mutates its input, so the previous page's envelope cannot be
// reused). paginationResolvePageFn resolves that copy at (perPage, page).
var (
	paginationFetchPageFn = func(ctx context.Context, ref templatesv1.ObjectReference) objects.Result {
		return objects.Get(ctx, ref)
	}
	paginationResolvePageFn = func(ctx context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		return widgets.Resolve(ctx, opts)
	}
)

// isApiRefTemplateDriven reports whether a widget CR (the resolver's
// input shape — its `.Object` map at the start of widgets.Resolve, OR
// the resolved-envelope shape which still carries the original spec)
// is an apiRef-driven widget whose resourcesRefs list is FANNED OVER a
// template (so paginating apiRef pages produces new resourcesRefs items).
//
// SHAPE-BASED (feedback_no_special_cases): true IFF
//   - spec.apiRef.name != "" (a non-empty external RA data source), AND
//   - spec.resourcesRefsTemplate non-empty (the items list is built by
//     fanning a template over the apiRef RA's data — so each apiRef page
//     produces a new chunk of items).
//
// A widget with apiRef but no resourcesRefsTemplate renders entirely
// from status.widgetData (e.g. a piechart/table over an aggregating RA)
// — paginating apiRef pages would re-populate the same identity-free
// cell with cumulative widgetData rather than new children to recurse
// into; that shape is correctly handled by the existing
// `isRBACSensitiveApiRefWidget` per-cohort routing.
//
// Accessor errors are treated as "absent" (the conservative direction:
// no pagination, fall back to the current page-1-only behaviour).
func isApiRefTemplateDriven(obj map[string]any) bool {
	if obj == nil {
		return false
	}
	apiRef, err := widgets.GetApiRef(obj)
	if err != nil || apiRef.Name == "" {
		return false
	}
	rrt, _ := widgets.GetResourcesRefsTemplate(obj)
	return len(rrt) > 0
}

// resolverWantsContinue reads `status.resourcesRefs.slice.continue` from
// a resolved widget envelope. The resolver sets this to true when the
// resolved items count equals or exceeds the requested perPage —
// internal/resolvers/widgets/resolve.go:124-134. Returns false on any
// shape mismatch (absent map, wrong type) — the conservative direction
// stops pagination.
func resolverWantsContinue(res *unstructured.Unstructured) bool {
	if res == nil {
		return false
	}
	slice, ok, err := maps.NestedMap(res.Object, "status", "resourcesRefs", "slice")
	if !ok || err != nil {
		return false
	}
	cont, ok := slice["continue"].(bool)
	if !ok {
		return false
	}
	return cont
}

// drainKeyPageFor derives the dispatcher-lookup KEY page for a drain page,
// given page-1's KEY page (keyPage, captured at collect time via
// deriveSeedKeyTuple) and the drain's RESOLUTION page (page = 2, 3, …).
// Task #318 Step 1.
//
// The KEY page is decoupled from the RESOLUTION page (Ship 0.30.187 D2): the
// resolution page advances the apiRef RA's slice; the KEY page must match what
// the dispatcher's paginationInfo (helpers.go:53-79) returns at serve time for
// the request that fetches the SAME deep page. Two cases, no widget/GVR
// special-case (the discriminant is the captured KEY tuple, not the resource):
//
//   - keyPage <= 0 (page-1 reached with NO declared slice — the root datagrid
//     keys at (-1, -1)): the frontend's deep-page request carries ?page=N, so
//     paginationInfo returns page=N. The drain's resolution page IS that N, so
//     the KEY page == the resolution page.
//   - keyPage >= 1 (page-1 reached at a declared ?page=K&perPage=M slice): the
//     drain advances from the declared start, so the KEY page advances from K
//     in lockstep with the resolution page (which starts at 2 for the first
//     EXTRA page after page-1): keyPage + (page - 1).
//
// BOTH the WithL1KeyContext install (:403) and the populateWidgetContentL1 Put
// (:446) call this with the SAME (keyPage, page) so they hash to one cell —
// closing the AC-G.5 detached-entry defect the pre-#318 split re-introduced.
func drainKeyPageFor(keyPage, page int) int {
	if keyPage <= 0 {
		// No declared slice on page-1 (root). The serve-time KEY page for the
		// deep page is the resolution page (the frontend sends ?page=<page>).
		return page
	}
	// Declared slice starting at page keyPage. The drain's first EXTRA page
	// (resolution page 2) is the page AFTER the declared start, so advance
	// keyPage by (page-1).
	return keyPage + (page - 1)
}

// iterateApiRefPages — Path 3.2.2 (0.30.220) — drives apiRef pagination
// AFTER the existing page-1 walk has resolved + populated. Inputs:
//
//   - ctx           — the SA-credentialed walker context (the same one
//     the caller's page-1 walk runs under). Pagination
//     stops on ctx cancel / deadline.
//   - w             — the per-root walker, providing `visited` cycle
//     state and the harvesters (kept consistent with
//     the page-1 path).
//   - in            — the page-1 widget CR. Used ONLY to read its
//     metadata (namespace, name) — we re-fetch a fresh
//     copy for each subsequent page via objects.Get
//     because the resolver mutates its input.
//   - gvr           — the widget's GVR; threaded to populateWidgetContentL1
//     so the seed cell matches the serve-time dispatcher.
//   - page1Res      — the page-1 resolved envelope. Used to read the
//     resolver's `.slice.continue` signal; we do NOT
//     re-Put it (the caller already did).
//   - depth         — the walker depth; subsequent-page recursion
//     continues from depth+1, the same as the page-1
//     children loop.
//   - perPage       — the RESOLUTION perPage (passed unchanged for
//     pages 2..N — the apiRef RA is paginated by adv-
//     ancing PAGE, not by changing perPage).
//   - keyPerPage    — the KEY perPage tuple. Per Ship 0.30.187 D2,
//     decoupled from `perPage`; passed straight through
//     to `populateWidgetContentL1` so each page's seed
//     cell matches the dispatcher's serve-time lookup
//     for the SAME page.
//   - keyPage       — page-1's KEY page tuple (Task #318 Step 1). The
//     per-page KEY page is derived from this base via
//     drainKeyPageFor(keyPage, page) so the page cell's
//     two key sites (the WithL1KeyContext install and the
//     populateWidgetContentL1 Put) hash to the SAME cell
//     AND follow deriveSeedKeyTuple semantics — instead
//     of the pre-#318 split where the install used the raw
//     loop counter and the Put used the RESOLUTION perPage
//     (the AC-G.5 detached-entry defect for the page cell).
//   - authnNS       — the authn namespace; threaded to widgets.Resolve.
//
// Returns no error: any pagination failure is logged and stops the
// loop; the page-1 envelope (already Put) remains correct. The walker's
// primary purpose (informer discovery) is unaffected.
//
// SAFETY: gated on isApiRefTemplateDriven(page1Res) AND
// resolverWantsContinue(page1Res). If either is false, the function is
// a no-op — page-1-only behaviour, byte-identical to the pre-3.2.2
// posture.
func iterateApiRefPages(
	ctx context.Context,
	w *phase1Walker,
	in *unstructured.Unstructured,
	gvr schema.GroupVersionResource,
	page1Res *unstructured.Unstructured,
	depth int,
	perPage int,
	keyPerPage int,
	keyPage int,
	authnNS string,
) {
	if in == nil || page1Res == nil || w == nil {
		return
	}
	// Predicate: only widgets whose items list grows with apiRef pages.
	// Use the page-1 RESOLVED envelope — it still carries the original
	// spec.apiRef + spec.resourcesRefsTemplate (the resolver augments
	// status without stripping spec).
	if !isApiRefTemplateDriven(page1Res.Object) {
		return
	}
	if !resolverWantsContinue(page1Res) {
		return
	}

	log := xcontext.Logger(ctx)
	if log == nil {
		log = slog.Default()
	}

	ns := in.GetNamespace()
	name := in.GetName()
	maxPages := maxApiRefPages()

	// pagesWalked is the count of EXTRA pages beyond page 1 the walker
	// resolves for this widget — emitted at completion for observability.
	pagesWalked := 0

	// reachedBackstop records whether the loop exited because it hit the
	// liveness backstop (page > maxPages) WITHOUT the resolver ever
	// signalling `.slice.continue==false`. That is an ANOMALY (#156): a
	// well-behaved RA terminates the loop on its declared pagination long
	// before the backstop. Set true only if we exhaust maxPages with the
	// resolver still asking to continue (see the loop tail).
	reachedBackstop := false

	// The page-1 walk ran at PAGE=1. We start at PAGE=2 and increment.
	// Each iteration: re-fetch a fresh CR copy (resolver mutates input),
	// resolve at this page, populate, recurse children, decide whether
	// to continue. The OPERATIVE terminator is the resolver's
	// `.slice.continue==false` (the blueprint's declared pagination); the
	// loop bound `maxPages` is the liveness backstop only.
	for page := 2; page <= maxPages; page++ {
		// P8 customer-priority yield (#156 / 0.30.256): raising the page
		// backstop to cover the FULL declared list (10K+ pages at 50K) turns
		// this drain into a long-running CPU consumer, so the loop MUST step
		// aside for any in-flight customer /call — same cooperative yield the
		// engine uses, same customerInFlight() atomic, NO shared budget
		// (feedback_customer_priority_over_refresher). A fast no-op when no
		// customer is in flight (prewarm_engine.go:436-438).
		engineYieldCheckpoint(ctx)

		if err := ctx.Err(); err != nil {
			log.Info("phase1.walk.apiref_pagination.ctx_cancel",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Int("page", page),
				slog.Int("pages_walked", pagesWalked),
				slog.Any("err", err),
			)
			return
		}

		// Re-fetch a FRESH CR copy. The resolver mutates opts.In in
		// place (status.* nested fields), so the previous page's
		// envelope cannot be reused. We fetch by (ns, name) via the
		// SAME ObjectReference shape the page-1 path used; objects.Get
		// rides the SA-credentialed transport on ctx.
		ref := templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: name, Namespace: ns},
			Resource:   gvr.Resource,
			APIVersion: gvr.GroupVersion().String(),
		}
		got := paginationFetchPageFn(ctx, ref)
		if got.Err != nil {
			log.Warn("phase1.walk.apiref_pagination.fetch_failed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Int("page", page),
				slog.Any("err", got.Err),
			)
			return
		}
		if got.Unstructured == nil {
			log.Warn("phase1.walk.apiref_pagination.fetch_nil",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Int("page", page),
			)
			return
		}

		// Install the widgetContent L1 key on the resolveCtx (Ship G
		// AC-G.5 — see phase1_walk.go:910-931 for the rationale). The
		// page cell's KEY tuple is (keyPerPage, keyPageForThisPage) —
		// decoupled from the RESOLUTION (perPage, page) per Ship
		// 0.30.187 D2 and symmetric with the page-1 site
		// (phase1_walk.go:1146). keyPageForThisPage advances page-1's
		// KEY page (keyPage) in lockstep with the drain's resolution
		// page so each deep page lands in its OWN cell while staying on
		// the deriveSeedKeyTuple model.
		//
		// Task #318 Step 1: pre-#318 this site used the RESOLUTION-loop
		// counter `page` as the KEY page and the Put at :446 used the
		// RESOLUTION `perPage` — so the two key sites disagreed (the
		// AC-G.5 detached-entry defect for the page cell). Both sites now
		// compute keyPageForThisPage + keyPerPage identically.
		keyPageForThisPage := drainKeyPageFor(keyPage, page)
		wcKey, _ := widgetContentL1Key(gvr, ns, name, keyPerPage, keyPageForThisPage)
		// #323 — capture the content cell's generation BEFORE the page resolve
		// below (paginationResolvePageFn), so populateWidgetContentL1's post-readyz
		// PutIfGen refuses (rather than resurrecting) if a DELETE lands during this
		// keep-warm / gvr-discovered re-resolve. Cheap + nil-safe.
		wcGen0 := cache.ResolvedCache().CaptureGen(wcKey)
		resolveCtx := ctx
		if wcKey != "" {
			resolveCtx = cache.WithL1KeyContext(resolveCtx, wcKey)
		}
		// Task #318 Step 1 — Cache-A sink parity. Install a stage-error
		// sink on the resolve ctx so the populate below (passed resolveCtx,
		// NOT the bare ctx) can decline to seed a partial-with-errors shell
		// for the recursed leaf-CHILD cells (the drain's only serveable
		// value, design §1d) — symmetric with the page-1 site
		// (phase1_walk.go:1162/1226), the request paths (widgets.go,
		// restactions.go), and the refresher (resolve_populate.go). For the
		// RBAC-sensitive datagrid PAGE cell the gate is moot (the Put is
		// already declined upstream at widget_content.go:213) but harmless.
		resolveCtx, _ = cache.WithStageErrorSink(resolveCtx)
		// #398 — the page resolve feeds the identity-free content cell below;
		// a sensitive read under it must decline that Put.
		resolveCtx, _ = cache.WithSensitiveTouchedSink(resolveCtx)

		res, err := paginationResolvePageFn(resolveCtx, widgets.ResolveOptions{
			In:      got.Unstructured,
			RC:      w.rc,
			AuthnNS: authnNS,
			PerPage: perPage,
			Page:    page,
		})
		if err != nil {
			log.Warn("phase1.walk.apiref_pagination.resolve_failed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Int("page", page),
				slog.Any("err", err),
			)
			return
		}
		if res == nil {
			return
		}

		// #156 observability: one extra page resolved. The page cell is a
		// PLANNED unit (we are about to hand it to populate) and — because
		// res is non-nil here — a SEEDED unit (the seed attempt reached the
		// populate call; populateWidgetContentL1 may still decline the Put
		// internally, accounted for by the widget_content skip counters —
		// see phase1_walk_pagination_metrics.go).
		bumpPrewarmApiRefPagesTotal()
		bumpPrewarmUnitsPlanned(1)
		// NB (counters-hygiene 2026-07-04): bumpPrewarmUnitsSeeded increments
		// snowplow_phase1_units_seeded, which counts apiRef CONTENT-PAGINATION
		// page cells (here) — NOT top-level per-identity seed units (that's
		// snowplow_phase1_bindingset_seed_resolves_total). Name reads "seed
		// units"; semantics are content-pagination cells. See the declaration
		// doc in phase1_walk_pagination_metrics.go.
		bumpPrewarmUnitsSeeded()

		// Put this page's envelope under the matching content L1 key.
		// populateWidgetContentL1 idempotently handles the RBAC-
		// sensitive + empty-shell guards (already enforced for page 1)
		// — for page N these are the same conditions, so behaviour is
		// uniform.
		//
		// Task #318 Step 1: pass the KEY tuple (keyPerPage,
		// keyPageForThisPage) — BYTE-IDENTICAL to the :403 install key
		// above — NOT the RESOLUTION (perPage, page); and pass resolveCtx
		// (which carries the stage-error sink + the L1 key) NOT the bare
		// ctx, so the Cache-A gate inside populateWidgetContentL1 sees this
		// resolve's sink (parity with phase1_walk.go:1226).
		populateWidgetContentL1(resolveCtx, gvr, got.Unstructured, keyPerPage, keyPageForThisPage, res, wcGen0)
		pagesWalked++

		// Recurse into the children this page produced. Honours the
		// SAME w.visited set as the page-1 children loop, so a child
		// that already appeared (e.g. a widget shared across pages of
		// a paginated apiRef RA — rare but possible) is not re-walked.
		children := extractResourcesRefsItems(res.Object)
		for _, child := range children {
			if err := ctx.Err(); err != nil {
				return
			}
			if !walkShouldRecurse(child) {
				continue
			}
			childRef, ok := objects.ParseCallPathToObjectRef(child.Path)
			if !ok {
				continue
			}
			key := navWidgetEndpointKey(childRef)
			if _, seen := w.visited[key]; seen {
				continue
			}
			w.visited[key] = struct{}{}

			// #156: each NEW child the deeper page reveals is a planned seed
			// unit (the walk descends it via w.walk below, which seeds the
			// child's own widgetContent cell). Counted once per unique child
			// (post visited-set dedup) so units_planned tracks the real
			// reachable widget-CR count, not re-walks.
			bumpPrewarmUnitsPlanned(1)

			// Child pagination defaults — same posture as the page-1
			// children loop in phase1_walk.go:1057-1083: honour the
			// child's declared slice, else use page=1 + bounded
			// perPage. Key tuple via the same deriveSeedKeyTuple
			// helper the page-1 path uses (paginationInfo-symmetric).
			childPage, childPerPage := 1, prewarmPageLimit()
			if p, pp, hasPg := util.ParseCallPathPagination(child.Path); hasPg {
				childPage, childPerPage = p, pp
			}
			childKeyPerPage, childKeyPage := deriveSeedKeyTuple(child.Path)

			childGot := objects.Get(ctx, childRef)
			if childGot.Err != nil || childGot.Unstructured == nil {
				continue
			}
			_ = w.walk(ctx, childGot.Unstructured, childGot.GVR,
				depth+1, childPage, childPerPage,
				childKeyPerPage, childKeyPage)
		}

		// Decide whether to continue. The resolver's
		// `.slice.continue == true` is the canonical (OPERATIVE) terminator —
		// when it flips to false (or the items list comes back empty) we have
		// reached the end of the blueprint's declared pagination.
		if !resolverWantsContinue(res) {
			break
		}
		// The resolver still wants more. If this was the last permitted page
		// (page == maxPages), the loop is about to exit on the LIVENESS
		// BACKSTOP rather than the declared terminator — an anomaly we surface
		// loud below.
		if page == maxPages {
			reachedBackstop = true
		}
	}

	// Hitting the backstop is an ANOMALY (#156): a well-behaved apiRef RA
	// terminates on `.slice.continue==false` far below maxPages (20,000 pages
	// = 100K items at perPage=5, ~2x the 50K production population). Reaching
	// it means either the RA reports `.slice.continue==true` forever (a
	// resolver bug / pathological data source) OR the real population genuinely
	// exceeds the derived ceiling (a scale-policy signal Diego should
	// re-evaluate). EITHER way it means coverage was TRUNCATED — log loud so it
	// is actionable, not silent.
	if reachedBackstop {
		log.Warn("phase1.walk.apiref_pagination.backstop_hit",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("ns", ns),
			slog.String("name", name),
			slog.Int("pages_walked", pagesWalked),
			slog.Int("max_pages", maxPages),
			slog.String("effect", "apiRef page liveness backstop reached while the resolver still "+
				"signalled .slice.continue==true — coverage TRUNCATED for this widget. Items beyond "+
				"the backstop fall back to the per-user serve-time path. Investigate: pathological "+
				"apiRef RA (continue==true forever) OR population exceeds the derived ceiling "+
				"(re-evaluate phase1MaxApiRefPages vs production scope)."),
		)
	}

	log.Info("phase1.walk.apiref_pagination.completed",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("ns", ns),
		slog.String("name", name),
		slog.Int("pages_walked", pagesWalked),
		slog.Int("max_pages", maxPages),
		slog.Bool("backstop_hit", reachedBackstop),
	)
}
