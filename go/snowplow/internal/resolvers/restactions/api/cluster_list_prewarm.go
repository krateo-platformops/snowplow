// cluster_list_prewarm.go — Path 3.2 / 0.30.218 — PIP boot pre-warm
// for the cluster_list cell roster.
//
// At pod start, BEFORE MarkPhase1Done flips /readyz to 200, the
// dispatcher subsystem walks every harvested RESTAction, finds every
// stage with a per-NS iterator (DependsOn.Iterator non-empty +
// ns-scoped target GVR), dedupes by GVR, and populates the apistage L1
// cell for each one in parallel under SA identity. This eliminates the
// first-customer cold-fallback path: every customer /call after boot
// hits a warm cell.
//
// CELL ROSTER cardinality: ~10-15 distinct GVRs at 50K prod scale
// (cluster_list-eligible RAs across portal + krateo internal — the
// per-namespace iterators in compositions / panels / configmaps /
// widgets / apirefs etc.). Each cell costs ~50-2,000ms to populate
// (envelope-size-dependent). Total wall-clock with GOMAXPROCS=8
// parallelism: ~5s on the empirical 0.30.217 measurements.
//
// TIMEOUT: 60s hard cap. If the pre-warm overruns, MarkPhase1Done
// still fires (readiness not blocked); cold-fallback covers the gap
// until cells warm in the background.

package api

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ClusterListPrewarmTimeout is the hard ceiling on Step 7.5 wall-clock.
// If pre-warm exceeds this, the caller proceeds anyway (MarkPhase1Done
// fires); per-NS iterator fallback covers the gap until cells warm via
// the cold-fallback async populate path. Exported so phase1 callers can
// reference it. Path 3.2 / 0.30.218.
const ClusterListPrewarmTimeout = 60 * time.Second

// ClusterListCellRoster enumerates the (apiCall, GVR) tuples that need
// pre-warm. Built by EnumerateClusterListCells; consumed by
// PrewarmClusterListCells.
type ClusterListCellRoster struct {
	Cells []ClusterListCellEntry
}

// ClusterListCellEntry is one cell in the roster — a single
// (apiCall, target GVR) tuple. The apiCall is needed for clusterCall
// construction (path template + endpoint resolution); the GVR is the
// key the cell is stored under.
type ClusterListCellEntry struct {
	APICall *templates.API
	GVR     schema.GroupVersionResource
}

// EnumerateClusterListCells walks every RESTAction in restActions and
// every stage's `path` template, statically extracting a target GVR
// from each apiserver path that resolves to a LIST shape (no per-object
// name suffix). Returns the deduped (apiCall, GVR) roster.
//
// Path 3.2.1 / 0.30.219 — supersedes the iterator-jq-probe-only
// algorithm from Path 3.2 / 0.30.218 which produced cells=0 in
// production because the dominant RA stage shape is a LITERAL
// cluster-scope LIST (no DependsOn.Iterator), and the few iterator
// stages that exist (e.g. compositions-list/allCompositions) reference
// parent-stage output the empty-dict jq probe cannot resolve. The new
// algorithm covers BOTH cases:
//
//   1. Literal apiserver paths (cluster-scope OR namespace-scope with
//      hardcoded ns): ParseAPIServerPathToDep extracts the GVR
//      directly. No jq evaluation required. Covers the majority of
//      harvested-RA stages (all-routes, blueprints-list,
//      blueprints-panels, compositions-panels, compositions-get-ns-
//      and-crd, sidebar-nav-menu-items in the 0.30.218 RA roster).
//
//   2. Iterator stages with parent-derived templates (Path like
//      `${ ".../<group>/<ver>/<plural>" }` where the substitution comes
//      from a prior stage): the empty-dict jq probe FAILS — these stay
//      cold-fallback-served until the refresher's lazy populate fires
//      at first customer touch.
//
// All registered cells share the cluster-scope LIST key
// `contentKeyInputs(gvr, "", "")` — the apistage cache contract is
// identity-free for ClassApistage (per ResolvedKeyInputs.CacheEntryClass).
// Cluster-scope paths and namespace-scope paths register the SAME cell
// because populateClusterListCellSync always issues a cluster-scope
// LIST (buildClusterListCall constructs `/apis/<g>/<v>/<plural>` or
// `/api/<v>/<plural>` regardless of source). Pre-warming the cell also
// triggers EnsureResourceType for the GVR, which initialises the
// dynamic informer — so subsequent GET-by-name customer /call hits the
// warm informer path (no first-touch LIST+watch tax).
//
// ctx: SA-identity context — currently unused by the static extraction
// path but kept on the signature for symmetry with the original
// API + future jq-probe fallbacks.
//
// Dedup: keyed on gvr.String(). The first apiCall encountered for a
// given GVR wins (it's the path-template carrier; downstream
// buildClusterListCall ignores the path field and constructs the
// canonical cluster-scope path from the GVR).
//
// Returns an empty roster on nil/empty input. Per-stage parse failures
// are NEVER fatal — one broken stage must not block the rest of the
// roster from pre-warm.
func EnumerateClusterListCells(
	ctx context.Context,
	log *slog.Logger,
	restActions []*templates.RESTAction,
) ClusterListCellRoster {
	_ = ctx // kept for symmetry; the static extraction path doesn't dispatch
	roster := ClusterListCellRoster{}
	if len(restActions) == 0 {
		return roster
	}
	seenGVR := map[string]bool{}
	for _, ra := range restActions {
		if ra == nil {
			continue
		}
		for _, apiCall := range ra.Spec.API {
			if apiCall == nil {
				continue
			}
			gvr, ok := extractClusterListGVRFromStage(apiCall)
			if !ok {
				continue
			}
			key := gvr.String()
			if seenGVR[key] {
				continue
			}
			seenGVR[key] = true
			roster.Cells = append(roster.Cells, ClusterListCellEntry{
				APICall: apiCall,
				GVR:     gvr,
			})
		}
	}
	if log != nil {
		log.Info("cluster_list.prewarm.enumerated",
			slog.String("subsystem", "cache"),
			slog.Int("ra_count", len(restActions)),
			slog.Int("cells", len(roster.Cells)),
		)
	}
	return roster
}

// extractClusterListGVRFromStage statically extracts a target GVR from
// an RA stage's path template. Returns (gvr, true) when the path
// resolves to a parsable apiserver LIST shape, (zero, false) otherwise.
//
// Rules (all conservative — false-negatives are acceptable, false-
// positives are not):
//
//   1. Non-GET stages are skipped. The cluster_list cell models a LIST
//      response; write verbs must never seed it.
//   2. Inter-RA `/call?...` paths are skipped — not apiserver paths.
//   3. Paths with unresolved `${...}` JQ template fragments are skipped
//      — we can't know the target GVR without running the iterator's
//      first-element jq evaluation, and parent-derived iterators have
//      no parent output at boot.
//   4. ParseAPIServerPathToDep must succeed AND return name=="" (LIST
//      form). A GET-by-name path (`/apis/.../<plural>/<name>`) targets
//      one object and is NOT a cluster-LIST seed candidate.
//   5. UserAccessFilter stages are accepted — the cluster-LIST seed
//      under SA scope still loads the apistage cell; the per-user
//      refilter applies at customer /call serve time, not at seed.
//
// The returned GVR is what populateClusterListCellSync will dispatch
// against — always cluster-scope (buildClusterListCall strips any
// namespace segment from the source path).
func extractClusterListGVRFromStage(apiCall *templates.API) (schema.GroupVersionResource, bool) {
	if apiCall == nil {
		return schema.GroupVersionResource{}, false
	}
	// Only GET stages seed cluster-list cells. apiCall.Verb is *string
	// with empty/nil meaning GET per the templates schema (`Verb is the
	// request method (GET if omitempty)`).
	if apiCall.Verb != nil {
		v := strings.ToUpper(strings.TrimSpace(*apiCall.Verb))
		if v != "" && v != "GET" && v != "HEAD" {
			return schema.GroupVersionResource{}, false
		}
	}
	path := strings.TrimSpace(apiCall.Path)
	if path == "" {
		return schema.GroupVersionResource{}, false
	}
	// Inter-RA call (snowplow's own /call?... routing) — not an
	// apiserver path; cluster-list mechanism doesn't apply.
	if strings.HasPrefix(path, "/call") || strings.HasPrefix(path, "/call?") {
		return schema.GroupVersionResource{}, false
	}
	// Unresolved JQ template fragments are a deal-breaker — without
	// the iterator's parent-stage output, the GVR is undefined.
	// ParseAPIServerPathToDep also rejects these, but the explicit
	// check makes the algorithm contract clear at the call site.
	if strings.Contains(path, "${") {
		return schema.GroupVersionResource{}, false
	}
	gvr, _, name, ok := cache.ParseAPIServerPathToDep(path)
	if !ok {
		return schema.GroupVersionResource{}, false
	}
	// LIST form only. A non-empty name segment targets one object —
	// populateClusterListCellSync dispatches a cluster-scope LIST,
	// which would mismatch a GET-by-name's content key.
	if name != "" {
		return schema.GroupVersionResource{}, false
	}
	// gvr.Resource MUST be set — defence in depth: an empty resource
	// is a malformed parse and would yield an unusable cluster path.
	if gvr.Resource == "" {
		return schema.GroupVersionResource{}, false
	}
	return gvr, true
}

// PrewarmClusterListCells populates every cell in the roster in
// parallel (bounded by parallelism arg). Each cell is populated via
// populateClusterListCellSync. Returns the count of successfully
// populated cells + total cells attempted (so the caller can log
// "warmed X of Y").
//
// ctx MUST carry the SA endpoint + REST config + a deadline (the
// 60s timeout cap, set by the caller via context.WithTimeout). On
// deadline exceeded, the errgroup returns and partially-populated
// state is acceptable — the cold-fallback path covers any unwarmed
// cell on first customer touch.
//
// parallelism is typically runtime.GOMAXPROCS(0); the same bound the
// PIP cohort errgroup uses.
//
// ep is the SA endpoint passed through to buildClusterListCall. NOT
// per-cohort (cluster_list cells are identity-free).
//
// apistageStore is the L1 store the populated entries are Put into.
// Required (panics on nil — production wires it from
// cache.ResolvedCache()).
//
// Returns (populated, attempted, err). err is set to ctx.Err() ONLY on
// deadline exceeded — per-cell failures are logged + counted in
// `attempted - populated` but NOT propagated (one broken cell must not
// abort the rest).
func PrewarmClusterListCells(
	ctx context.Context,
	log *slog.Logger,
	roster ClusterListCellRoster,
	ep endpoints.Endpoint,
	apistageStore *cache.ResolvedCacheStore,
	parallelism int,
) (populated int, attempted int, err error) {
	if apistageStore == nil {
		log.Warn("cluster_list.prewarm.skipped",
			slog.String("subsystem", "cache"),
			slog.String("reason", "nil apistage store — cache off?"),
		)
		return 0, 0, nil
	}
	if len(roster.Cells) == 0 {
		log.Info("cluster_list.prewarm.empty_roster",
			slog.String("subsystem", "cache"),
			slog.String("hint", "no iterator stages with ns-scope target GVR — nothing to pre-warm"),
		)
		return 0, 0, nil
	}

	if parallelism <= 0 {
		parallelism = 4
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)

	var mu sync.Mutex
	popOK := 0

	for i := range roster.Cells {
		cell := roster.Cells[i] // capture loop var for closure
		g.Go(func() error {
			// Honor deadline.
			if gctx.Err() != nil {
				return gctx.Err()
			}
			contentKey := cache.ComputeKey(contentKeyInputs(cell.GVR, "", ""))
			// If the cell is already warm (from a prior boot's
			// refresher OR from a concurrent populate), skip.
			// #376 — GetNoTouch: this is a PREWARM already-warm probe (not a customer
			// serve) on the shared resolved store; it must not stamp lastRead/hitTotal,
			// else the populate sweep fakes #315/#316 warmth on the content set and
			// contaminates the warm_lastread gauge. Output-neutral: (existing, hit) is
			// identical, only the warmth perturbation is dropped.
			if existing, hit := apistageStore.GetNoTouch(contentKey); hit && existing != nil {
				mu.Lock()
				popOK++
				mu.Unlock()
				// Register in cluster_list tier so future
				// dirty-marks route to the high-priority queue.
				cache.RegisterClusterListKey(contentKey)
				return nil
			}
			// Build the cluster-scope call.
			clusterCall := buildClusterListCall(cell.APICall, ep, cell.GVR)

			if populateClusterListCellSync(gctx, log, cell.APICall, cell.GVR, contentKey, clusterCall, apistageStore) {
				mu.Lock()
				popOK++
				mu.Unlock()
			}
			return nil // never propagate per-cell errors
		})
	}

	err = g.Wait()
	if err == context.DeadlineExceeded || err == context.Canceled {
		log.Warn("cluster_list.prewarm.timeout",
			slog.String("subsystem", "cache"),
			slog.Int("populated", popOK),
			slog.Int("attempted", len(roster.Cells)),
			slog.Any("err", err),
			slog.String("effect", "MarkPhase1Done fires regardless; remaining cells lazy-warm via cold-fallback async populate"),
		)
	}
	return popOK, len(roster.Cells), err
}
