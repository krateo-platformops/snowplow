// phase1_clusterlist_prewarm.go — Path 3.2 / 0.30.218 — Step 7.5
// cluster_list cell pre-warm.
//
// Runs BEFORE MarkPhase1Done (Step 8) — populates every cluster_list-
// eligible cell in parallel under SA identity, with a 60s hard timeout
// cap. On timeout, readiness still flips (the cluster_list cold-fallback
// path covers the gap until cells warm in the background).
//
// MECHANISM (per design §3):
//   1. Enumerate the cell roster — iterate every harvested RESTAction,
//      walk api[], find stages with non-empty DependsOn.Iterator that
//      derive to a ns-scope target GVR. Dedupe by GVR.
//   2. For each unique GVR, build the cluster-scope httpcall.RequestOptions
//      via api.PrewarmClusterListCells (which calls populateClusterListCellSync).
//   3. Bounded errgroup at runtime.GOMAXPROCS(0). 60s ctx deadline.
//
// PRE-PHASE-1-DONE ORDERING. This pass MUST run AFTER Step 7
// (WaitAllInformersSynced — the cluster-scope LIST goes through the
// dynamic informer pivot, which requires the informer be HasSynced),
// and BEFORE Step 8 (MarkPhase1Done). The 503 readiness gate covers
// customer traffic during this window.
//
// FLAG-OFF: when PREWARM_PIP_ENABLED=false (the seed is opted out of)
// AND no apistage store is reachable, the pre-warm is skipped — the
// cluster_list cold-fallback path still works at /call time.
//
// Per `feedback_no_special_cases`: no hardcoded GVR / RA / cohort
// branching. The roster is purely a function of the harvested RA set;
// every cell's GVR is derived from the iterator's resolved path.

package dispatchers

import (
	"context"
	"log/slog"
	goruntime "runtime"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/apis"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// clusterListPrewarmFn is the Step 7.5 invocation hook. Returns nil
// on success or timeout (timeout is best-effort, not a fatal); never
// errors out the way pipSeedFn does. phase1WarmupWith calls this BEFORE
// MarkPhase1Done. Path 3.2 / 0.30.218.
type clusterListPrewarmFn func(ctx context.Context)

// makeClusterListPrewarmFn constructs the Step 7.5 hook. h is the F2
// content-prewarm harvester (drained for the harvested RESTAction set).
// saEP is the SA endpoint used to build the cluster-scope call URL.
// authnNS is the namespace from which we Get RESTAction CRs (same as
// the F2 content pass).
//
// Returns a closure that:
//   1. Resolves every harvested RA reference to its RESTAction CR.
//   2. Enumerates the cell roster via api.EnumerateClusterListCells.
//   3. Populates the roster via api.PrewarmClusterListCells, bounded
//      by a 60s deadline.
//
// Returns nil when cache or apistage L1 is OFF (no store to populate).
func makeClusterListPrewarmFn(h *contentPrewarmHarvester, saEP endpoints.Endpoint, saRC *rest.Config, authnNS string) clusterListPrewarmFn {
	return func(parent context.Context) {
		log := slog.Default()
		start := time.Now()

		if !cache.ApistageL1Enabled() {
			log.Info("cluster_list.prewarm.skipped",
				slog.String("subsystem", "cache"),
				slog.String("reason", "apistage L1 disabled — cluster_list cells have no storage substrate"),
			)
			return
		}
		apistageStore := cache.ResolvedCache()
		if apistageStore == nil {
			log.Info("cluster_list.prewarm.skipped",
				slog.String("subsystem", "cache"),
				slog.String("reason", "resolved cache store nil — cache off"),
			)
			return
		}

		// #1233 — the prewarm writes cells: run it on a complete RBAC snapshot, or
		// its writes would be refused and its cells left cold.
		if err := cache.WaitRBACSnapshotComplete(parent); err != nil {
			log.WarnContext(parent, "cluster_list.prewarm.skipped",
				slog.String("subsystem", "cache"),
				slog.String("reason", "the RBAC snapshot did not complete before the prewarm's deadline"),
				slog.Any("err", err),
			)
			return
		}

		refs := h.snapshot()
		if len(refs) == 0 {
			log.Info("cluster_list.prewarm.no_restactions_harvested",
				slog.String("subsystem", "cache"),
				slog.String("hint", "no RESTActions reached by the Phase-1 walker — nothing to pre-warm"),
			)
			return
		}

		// Fetch each harvested RA's CR. We use the same SA-context
		// shape the F2 content pass uses so objects.Get authenticates
		// as SA.
		saCtx := withContentPrewarmSAContext(parent, saEP, saRC)
		var restActions []*templatesv1.RESTAction
		for _, ref := range refs {
			if parent.Err() != nil {
				return // shutdown
			}
			got := objects.Get(saCtx, ref)
			if got.Err != nil || got.Unstructured == nil {
				log.Debug("cluster_list.prewarm.ra_fetch_failed",
					slog.String("subsystem", "cache"),
					slog.String("ra", ref.Namespace+"/"+ref.Name),
				)
				continue
			}
			scheme := k8sruntime.NewScheme()
			if err := apis.AddToScheme(scheme); err != nil {
				continue
			}
			var cr templatesv1.RESTAction
			if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(
				got.Unstructured.Object, &cr); err != nil {
				continue
			}
			restActions = append(restActions, &cr)
		}

		if len(restActions) == 0 {
			log.Info("cluster_list.prewarm.no_restactions_resolved",
				slog.String("subsystem", "cache"),
				slog.Int("harvested", len(refs)),
				slog.String("hint", "all harvested refs failed to fetch — falling back to runtime cold-miss async populate"),
			)
			return
		}

		// Enumerate the cell roster under SA context (deriveTargetGVR
		// runs jq against an empty dict — no parent stage outputs
		// needed for ns-scope GVR derivation in 99% of cases).
		roster := api.EnumerateClusterListCells(saCtx, log, restActions)
		log.Info("cluster_list.prewarm.roster_enumerated",
			slog.String("subsystem", "cache"),
			slog.Int("ra_count", len(restActions)),
			slog.Int("cells", len(roster.Cells)),
		)
		if len(roster.Cells) == 0 {
			return
		}

		// Bound the pre-warm wall-clock by ClusterListPrewarmTimeout
		// (60s, per design §3.3 hard cap). On deadline the partial
		// state is acceptable — the cold-fallback path covers any
		// unwarmed cell.
		prewarmCtx, cancel := context.WithTimeout(saCtx, api.ClusterListPrewarmTimeout)
		defer cancel()

		populated, attempted, err := api.PrewarmClusterListCells(
			prewarmCtx, log, roster, saEP, apistageStore, goruntime.GOMAXPROCS(0))

		log.Info("cluster_list.prewarm.completed",
			slog.String("subsystem", "cache"),
			slog.Int("populated", populated),
			slog.Int("attempted", attempted),
			slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
			slog.Bool("timed_out", err == context.DeadlineExceeded),
		)
	}
}

