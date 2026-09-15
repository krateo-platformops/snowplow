// relist_bridge.go — 1.12.6 C2: bridge the informer swap on a CRD schema
// relist (design-1.12.6-event-hardening §4).
//
// THE GAP (§4.1, trace addendum 4 §1). triggerCRDSchemaRelist tears the
// per-GVR informer down (RemoveResourceType closes its stop channel and
// purges its state — anything in its DeltaFIFO or in flight on its watch is
// dropped with no handler run) and builds a FRESH one. A fresh informer's
// knownObjects indexer is empty, so DeltaFIFO.Replace() synthesises NO
// Deleted delta for an object that vanished before the new LIST: the object
// simply never appears, and no DELETE is ever generated for it. The 1.12.5
// pre-sync and post-sync dirty-marks reach such an entry only through a
// refresh whose own re-fetch must then 404 — a two-hop path that depends on
// a healthy refresh path and costs one re-resolve per DEPENDENT of the GVR.
//
// THE BRIDGE (§4.2): snapshot the OLD indexer's key set before teardown,
// wait for the NEW informer to sync, diff (before \ after) and ENQUEUE each
// missing coordinate on the C1 dep-event queue. The worker then probes the
// (now servable) informer, reads objAbsent, and evicts the self entry through
// the one decision site — no new eviction path, no store walk, no lock-order
// hazard: the bridge takes rw.mu twice (two IndexerKeys reads) and never
// touches the store mutex. This is what client-go's Replace() does WITHIN one
// informer, done ACROSS the swap.
//
// ADDITIVE ONLY (PM condition C10). The 1.12.5 post-sync re-fire
// (refireRelistDirtyMarkAfterSync) is UNTOUCHED and runs alongside: its
// enqueues are dirty-marks, the bridge's are coordinates, and both land on
// idempotent paths, so the overlap costs one extra probe per missing key.
// The re-fire is removed in a LATER, soak-gated commit once
// relist_bridge_timeout_total stays at zero across real CRD schema changes
// while relist_bridge_enqueued_total moves — a revert of C2 must leave the
// stopgap standing, which is why this file adds and never edits.
//
// THE TIMEOUT PATH IS A SILENT NO-OP BY DESIGN — and counted. If the fresh
// informer does not sync inside the bound, the bridge cannot diff and gives
// up (WARN + relist_bridge_timeout_total). The re-fire covers that case
// exactly as before. Non-zero on a healthy cluster = the bridge is NOT
// covering the window it was built for, and the stopgap must stay.
//
// COST (§4.4): two ListKeys per relisted GVR (the relisted CRD's object
// count, hundreds for a widget GVR — the 50K compositions are not
// CRD-schema-relisted), once per relist. No relist storm: the bridge fires
// at the relist's own cadence.

package cache

import (
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// envRelistBridgeTimeoutSeconds bounds how long the bridge waits for the
// relisted informer to sync before giving up (string-typed chart knob, see
// values.yaml). Default matches the 1.12.5 post-sync re-fire's wait: a
// relisted informer that has not synced in two minutes is not going to.
// intFromEnv returns the default for unset AND unparseable, so a typo
// degrades to the default rather than to zero.
const (
	envRelistBridgeTimeoutSeconds     = "DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS"
	defaultRelistBridgeTimeoutSeconds = 120
)

// relistBridgeTimeout returns the configured sync wait bound.
func relistBridgeTimeout() time.Duration {
	s := intFromEnv(envRelistBridgeTimeoutSeconds, defaultRelistBridgeTimeoutSeconds)
	if s <= 0 {
		s = defaultRelistBridgeTimeoutSeconds
	}
	return time.Duration(s) * time.Second
}

// relistBridgeMaxLoggedKeys caps how many synthesized coordinates one relist
// INFO line names — enough to identify the batch, never a dump.
const relistBridgeMaxLoggedKeys = 5

// bridgeRelistDeletes waits for the relisted GVR's replacement informer to
// sync, diffs the pre-teardown key set against the fresh indexer and
// enqueues every missing coordinate on the dep-event queue.
//
// Runs on its own goroutine: the relist loop executes on the single
// CRD-lifecycle worker, and blocking there would stall every other CRD
// event. Accounted on c.workerWG so the bridge's stop path waits for it;
// panic-guarded because a goroutine panic takes the process down.
func (c *crdDiscovery) bridgeRelistDeletes(rw *ResourceWatcher, w *depWatch, gvr schema.GroupVersionResource, before []string, syncCh <-chan struct{}) {
	defer c.workerWG.Done()
	defer func() {
		if rec := recover(); rec != nil {
			c.panicsRecovered.Add(1)
			slog.Error("cache.crd_discovery.relist_bridge.panic",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()
	c.relistBridgeRuns.Add(1)

	if syncCh == nil {
		// EnsureResourceType returned no channel (passthrough / nil watcher):
		// there is no fresh informer to diff against.
		c.relistBridgeAborted.Add(1)
		return
	}

	wait := relistBridgeTimeout()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-syncCh:
	case <-c.stopCh:
		return
	case <-timer.C:
		c.relistBridgeTimeout.Add(1)
		slog.Warn("cache.crd_discovery.relist_bridge.timeout",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.Duration("waited", wait),
			slog.Int("before_count", len(before)),
			slog.String("effect", "the relisted informer never synced, so the bridge could not diff the "+
				"old key set against the new LIST and enqueued NOTHING — entries whose objects "+
				"were deleted inside the teardown window are covered only by the 1.12.5 "+
				"post-sync re-fire (if that synced) or their TTL. Non-zero on a healthy cluster "+
				"means the bridge is not covering the window it replaces the re-fire for."),
		)
		return
	}
	select {
	case <-c.stopCh:
		return
	default:
	}

	after, ok := rw.IndexerKeys(gvr)
	if !ok {
		// Registered before, gone now: the GVR was removed again while we
		// waited (a CRD DELETE racing the relist). Its entries are covered by
		// OnResourceTypeRemoved; nothing to diff.
		c.relistBridgeAborted.Add(1)
		return
	}
	present := make(map[string]struct{}, len(after))
	for _, k := range after {
		present[k] = struct{}{}
	}
	synthesized := 0
	var logged []string
	for _, k := range before {
		if _, still := present[k]; still {
			continue
		}
		ns, name := "", k
		if i := strings.IndexByte(k, '/'); i >= 0 {
			ns, name = k[:i], k[i+1:]
		}
		if name == "" {
			continue
		}
		w.submitDepEvent(rw, depEventKey{gvr: gvr, namespace: ns, name: name})
		synthesized++
		if len(logged) < relistBridgeMaxLoggedKeys {
			logged = append(logged, k)
		}
	}
	if synthesized > 0 {
		c.relistBridgeEnqueued.Add(uint64(synthesized))
	}
	slog.Info("cache.crd_discovery.relist_bridge.diffed",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.Int("before_count", len(before)),
		slog.Int("after_count", len(after)),
		slog.Int("synthesized", synthesized),
		slog.Any("sample", logged),
		slog.String("hint", "objects present in the old indexer and absent from the relisted LIST were "+
			"enqueued as coordinates; the dep-event worker probes the fresh informer (ABSENT) and "+
			"evicts their self entries — the DELETE a fresh informer never synthesises (#187, 1.12.6 C2)"),
	)
}
