// rbac_snapshot_expvar.go — Ship 0.30.249 / Task #250 Block 2a.
//
// Read-only expvar exposure of the RBAC snapshot publish-sequence
// counter (`rbacSnapshotPublishSeq`, rbac_snapshot.go:238 / RBACGen()
// at rbac_snapshot.go:251). The counter is incremented exactly once
// per successful rebuildRBACSnapshot publish (rbac_snapshot.go:474).
//
// PURPOSE: Phase 6 #250 needs a mechanism-independent probe to verify
// snowplow's subject-index actually rebuilt after a RoleBinding
// ADD/DELETE — separate from any /call evaluator path. The bench
// stage S8 (RB-add) / S9 (RB-remove) inner gate at
// e2e/bench/bench/phases.py polls this counter pre/post mutation
// and requires a delta >= 1 within 30s. Probe A in the four-way
// breakdown at docs/task-250-phase6-rbac-stages-design.md §6.
//
// KEY: snowplow_rbac_publish_seq — uint64 returned by RBACGen()
// (rbac_snapshot.go:251). 0 is the sentinel "no snapshot published"
// value (cache=off OR pre-readiness on cache=on).
//
// REGISTRATION: idempotent via sync.Once; called from main.go's HTTP
// bootstrap (next to mux.Handle("GET /debug/vars", expvar.Handler())
// at main.go:788). Unconditionally wired so the bench probe receives
// `0` under cache=off rather than a missing-key error — failing the
// probe closed in cache=off is the correct behaviour because the
// snapshot never publishes there, so the probe SHOULD timeout
// awaiting a delta.
//
// Mirrors the sister-shape idiom at bindings_by_gvr_metrics.go:42-82
// (expvar.Publish + expvar.Func + sync.Once).

package cache

import (
	"expvar"
	"sync"
)

// rbacSnapshotExpvarOnce guards RegisterRBACSnapshotExpvar so the
// registration body runs at most once per process. expvar.Publish
// panics on a duplicate key; sync.Once prevents that under repeated
// invocation from tests or accidental dual call sites.
var rbacSnapshotExpvarOnce sync.Once

// RegisterRBACSnapshotExpvar publishes the snowplow_rbac_publish_seq
// expvar key. Idempotent under repeated invocation. Called from
// main.go's HTTP mux bootstrap; safe to call from tests via the
// public name.
//
// The value is sourced from RBACGen() (rbac_snapshot.go:251) — a
// single atomic.Uint64.Load. No per-scrape allocation other than
// the int boxing expvar performs at JSON-encode time.
func RegisterRBACSnapshotExpvar() {
	rbacSnapshotExpvarOnce.Do(func() {
		expvar.Publish("snowplow_rbac_publish_seq", expvar.Func(func() any {
			return RBACGen()
		}))
		// #691 — the stuck-rebuild detectors.
		expvar.Publish("snowplow_rbac_rebuild_panics_total", expvar.Func(func() any {
			return RBACRebuildPanicsTotal()
		}))
		expvar.Publish("snowplow_rbac_unpublished_event_age_seconds", expvar.Func(func() any {
			return RBACUnpublishedEventAge().Seconds()
		}))
		expvar.Publish("snowplow_rbac_last_successful_publish_age_seconds", expvar.Func(func() any {
			return RBACLastPublishAge().Seconds()
		}))
		// #687 — rebuilds the post-release re-check picked up.
		expvar.Publish("snowplow_rbac_rebuild_post_release_pickups_total", expvar.Func(func() any {
			return RBACRebuildPostReleasePickups()
		}))
	})
}
