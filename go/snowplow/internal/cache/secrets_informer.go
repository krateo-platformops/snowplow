// secrets_informer.go — Ship D.2 (0.30.143). Stock-client-go
// kubeinformers.SharedInformerFactory infrastructure for the
// AUTHN_NAMESPACE-scoped Secrets cache. Sibling to the dynamic
// factory in watcher.go (kept separate so the dynamic factory's
// cluster-scope is never widened to include Secrets — see
// secrets_snapshot.go header).
//
// LIFECYCLE.
//
//   - StartSecretsInformer (called from main.go after SetRESTConfig):
//     builds kubernetes.NewForConfig(rc), constructs the namespace-
//     scoped factory, wires the snapshot event handler, installs the
//     SetWatchErrorHandler for conjunct-3 servability, starts the
//     factory, waits for cache sync (bounded), then publishes the
//     initial snapshot synchronously.
//   - SecretsCacheServable: two-conjunct predicate (started AND
//     HasSynced AND !watchBroken AND !cache.Disabled()). Consulted
//     by FromInformerSecret BEFORE the snapshot lookup. Design §2.4
//     elides conjuncts 1 (registered) and 4 (resourceTypeConfirmed)
//     because Secrets are a core/v1 type — always served by every
//     conformant apiserver — and the informer is a process-wide
//     singleton (always wired or always absent, never racy).
//   - AssertSecretsInformerWired: boot-time assertion in the
//     AssertRBACSnapshotWired lineage. PM-ratified test/prod
//     asymmetry: panic in test (env.TestMode()==true), log ERROR +
//     increment snowplow_assertion_violations_total{check=
//     "secrets_informer_wired"} in prod. The pod stays up in prod.
package cache

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/plumbing/env"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
)

// secretsInformerState holds the sibling infrastructure for the
// AUTHN_NAMESPACE Secrets cache. Wired exactly once at startup via
// StartSecretsInformer; subsequent reads are lock-free (atomic loads
// + indexer reads).
//
// Lifetime: process-scoped. The factory's stop channel is bound to
// the cacheCtx in main.go — process shutdown closes it.
var (
	// secretsInformerStarted is set to true after the initial cache
	// sync completes in StartSecretsInformer. Read by
	// SecretsCacheServable.
	secretsInformerStarted atomic.Bool

	// secretsInformerHandle holds the *informers.GenericInformer-ish
	// pieces we need to read post-start: the indexer (for the
	// rebuild walk) and the HasSynced predicate (for servability).
	// Set once under secretsInformerInitMu, read lock-free
	// afterwards via the atomic.Pointer.
	secretsInformerHandlePtr atomic.Pointer[secretsInformerHandle]

	// secretsWatchBroken is set true when the reflector drops its
	// WATCH (conjunct-3 servability signal). Cleared on a
	// successful relist via the discovery-style recovery hook (see
	// installSecretsWatchErrorHandler below).
	secretsWatchBroken atomic.Bool

	// secretsCacheNamespace holds the AUTHN_NAMESPACE the informer
	// was constructed with. Set once at StartSecretsInformer time;
	// read lock-free afterwards. Empty string means "no informer
	// wired".
	secretsCacheNamespaceStr atomic.Pointer[string]

	// secretsInformerWired is the snowplow-side flag set by
	// StartSecretsInformer; AssertSecretsInformerWired panics (or
	// log+counts) when this stays false in cache=on mode.
	secretsInformerWired atomic.Bool

	// secretsInformerAssertionDisabled, when true, makes
	// AssertSecretsInformerWired a no-op. TEST-ONLY — same posture
	// as rbacSnapshotAssertionDisabled (rbac_snapshot.go).
	secretsInformerAssertionDisabled atomic.Bool

	// secretsAssertionViolationsTotal — production-mode counter
	// bumped by AssertSecretsInformerWired when the informer is
	// unwired in non-test mode. Mirrors assertionViolationsTotal
	// (fallthrough_assert.go) shape.
	secretsAssertionViolationsTotal atomic.Uint64
)

// secretsInformerHandle bundles the two things rebuildSecretsSnapshot
// and SecretsCacheServable need post-start: the indexer (for List())
// and the HasSynced predicate. Held as an atomic.Pointer so the
// initial Store happens-before any rebuild can run.
type secretsInformerHandle struct {
	indexer   clientcache.Indexer
	hasSynced func() bool
}

// secretsInformerIndexer returns the indexer, or nil if not yet
// wired. Used by rebuildSecretsSnapshot (secrets_snapshot.go).
func secretsInformerIndexer() clientcache.Indexer {
	h := secretsInformerHandlePtr.Load()
	if h == nil {
		return nil
	}
	return h.indexer
}

// SecretsCacheNamespace returns the AUTHN_NAMESPACE the Secrets
// informer was constructed with. Empty string means the cache is not
// wired (StartSecretsInformer was never called, or was called with
// an empty namespace).
//
// FromInformerSecret consults this to refuse lookups whose ref
// namespace doesn't match — those are uncacheable by construction
// (the informer's WithNamespace option scopes the LIST/WATCH).
func SecretsCacheNamespace() string {
	p := secretsCacheNamespaceStr.Load()
	if p == nil {
		return ""
	}
	return *p
}

// SecretsCacheServable reports whether the Secrets cache can vouch
// for a lookup. Returns false when:
//
//   - cache=off (Disabled()==true) — cache mechanism inert,
//   - the informer has not been started (e.g. AUTHN_NAMESPACE empty
//     at boot),
//   - the initial LIST has not completed (HasSynced()==false),
//   - the WATCH has been dropped (secretsWatchBroken==true).
//
// Returns true otherwise — the snapshot is current and
// FromInformerSecret can serve. Lock-free (atomic loads + a
// HasSynced call which client-go documents as concurrency-safe).
func SecretsCacheServable() bool {
	if Disabled() {
		return false
	}
	if !secretsInformerStarted.Load() {
		return false
	}
	if secretsWatchBroken.Load() {
		return false
	}
	h := secretsInformerHandlePtr.Load()
	if h == nil || h.hasSynced == nil {
		return false
	}
	return h.hasSynced()
}

// StartSecretsInformer constructs the AUTHN_NAMESPACE-scoped typed
// Secrets informer and wires it for the Ship D.2 snapshot. Idempotent
// — re-calls after a successful start are no-ops (the
// secretsInformerStarted flag gates re-entry).
//
// Errors at construction time (client build, factory start) leave
// the cache inert — FromInformerSecret then falls back to upstream
// FromSecret on every call (the pre-D.2 behavior). Soft-fail by
// design (design §2.7).
//
// When `namespace == ""`, the informer is NOT started — the caller
// (main.go) gates on the AUTHN_NAMESPACE flag. SecretsCacheServable
// then stays false and every call falls through to upstream.
func StartSecretsInformer(ctx context.Context, rc *rest.Config, namespace string) error {
	if Disabled() {
		// Cache-off — invariant inert. Honors
		// project_caching_is_provisional.
		return nil
	}
	if namespace == "" {
		// AUTHN_NAMESPACE empty — production sets it via the chart;
		// dev/test path may legitimately leave it blank. Log + no-op.
		slog.Info("cache.secrets.informer.no_authn_ns",
			slog.String("subsystem", "cache"),
			slog.String("hint", "AUTHN_NAMESPACE empty — Secrets cache not wired (upstream FromSecret on every call)"),
		)
		return nil
	}
	if secretsInformerStarted.Load() {
		// Idempotent re-call.
		return nil
	}

	// Test-injection path takes precedence: SetSecretsClientForTest
	// installs a fake clientset that the factory uses INSTEAD of
	// kubernetes.NewForConfig(rc). When unset, build the production
	// client from rc.
	client, err := buildSecretsClient(rc)
	if err != nil {
		return fmt.Errorf("cache.secrets.informer: build client: %w", err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // resync period 0 — pure event-driven (matches existing dynamic factory policy at watcher.go:317-322)
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			// Same bounded-paging policy as the dynamic factory's
			// listOptionsTweak (watcher.go:35-37) — keep the
			// initial LIST page-bounded to avoid an OOM in the
			// pathological "thousands of Secrets in
			// AUTHN_NAMESPACE" scenario.
			opts.Limit = listPageLimit
		}),
	)

	gi := factory.Core().V1().Secrets()
	inf := gi.Informer()

	// Snapshot event handler — every ADD/UPDATE/DELETE schedules a
	// rebuild via the dirty-bit + tryLock pattern. Handler body
	// returns immediately; the indexer walk runs on the detached
	// rebuild goroutine.
	if _, err := inf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ interface{}) { scheduleSecretsRebuild() },
		UpdateFunc: func(_, _ interface{}) { scheduleSecretsRebuild() },
		DeleteFunc: func(_ interface{}) { scheduleSecretsRebuild() },
	}); err != nil {
		// Informer already started — pre-Start install must succeed;
		// log and proceed (we'll still publish on the bounded sync
		// wait below).
		slog.Warn("cache.secrets.informer.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
		)
	}

	// Conjunct-3 servability — SetWatchErrorHandler flips
	// secretsWatchBroken when the reflector drops its WATCH.
	// Recovery: a successful relist clears it
	// (clearSecretsWatchBrokenOnSync below, called from the
	// initial-sync wait + periodic re-sync hook).
	installSecretsWatchErrorHandler(inf)

	// Publish the handle BEFORE Start so the first event-driven
	// rebuild can find the indexer. The factory's Start is non-
	// blocking; the actual LIST/WATCH runs on goroutines spawned
	// from this call.
	secretsInformerHandlePtr.Store(&secretsInformerHandle{
		indexer:   inf.GetIndexer(),
		hasSynced: inf.HasSynced,
	})

	// Record the namespace BEFORE Start so FromInformerSecret's
	// namespace-match guard sees the correct value even during the
	// bounded sync wait.
	ns := namespace
	secretsCacheNamespaceStr.Store(&ns)

	// Start the factory. ctx.Done() is the stop signal — process
	// shutdown closes cacheCtx, which propagates here.
	factory.Start(stopCh(ctx))

	// Bounded WaitForCacheSync — 30 s default (the design §2.7
	// references the same pattern as main.go:247-260 for RBAC).
	// Soft-fail: a timeout leaves the cache servable=false (the
	// HasSynced predicate continues to return false until the
	// sync completes), and FromInformerSecret falls through.
	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()
	if ok := clientcache.WaitForCacheSync(syncCtx.Done(), inf.HasSynced); !ok {
		slog.Warn("cache.secrets.informer.initial_sync_timeout",
			slog.String("subsystem", "cache"),
			slog.String("namespace", namespace),
			slog.String("hint", "Secrets cache will become servable when HasSynced flips; "+
				"FromInformerSecret falls back to upstream until then"),
		)
		// Note: we do NOT return an error. The factory is started;
		// HasSynced may flip true after the function returns. The
		// cache will start serving once the sync completes; no
		// crash, no data loss.
	}

	// Publish the initial snapshot synchronously so the very first
	// FromInformerSecret call after StartSecretsInformer returns
	// sees a populated snapshot (rather than the pre-readiness nil).
	rebuildSecretsSnapshot()

	secretsInformerStarted.Store(true)
	secretsInformerWired.Store(true)

	slog.Info("cache.secrets.informer.started",
		slog.String("subsystem", "cache"),
		slog.String("namespace", namespace),
		slog.Int("initial_count", initialSecretCount()),
	)
	return nil
}

// initialSecretCount reads the freshly-published snapshot's
// cardinality for the startup log line. Cheap; doesn't appear on the
// hot path.
func initialSecretCount() int {
	s := secretsSnap.Load()
	if s == nil {
		return 0
	}
	return len(s.ByName)
}

// installSecretsWatchErrorHandler wires the conjunct-3 WATCH-error
// handler onto the Secrets informer. Mirrors
// installWatchErrorHandler in servable.go:103-143 — same closure
// shape (set the broken flag + WARN once).
//
// Recovery: a future enhancement could clear secretsWatchBroken on a
// LastSyncResourceVersion advance (the discovery-style ticker
// pattern). For Ship D.2 the simpler "broken stays sticky until pod
// restart" posture is acceptable — the informer's own ListAndWatch
// re-arm will re-populate the indexer; servability resumes only
// after a deliberate restart (an operator signal that the cache is
// degraded). Documented as Ship D.2 out-of-scope (§7 X1's
// "diagnostic" mitigation can be added later).
func installSecretsWatchErrorHandler(inf clientcache.SharedIndexInformer) {
	handler := func(_ *clientcache.Reflector, err error) {
		// 1.12.7 — count EVERY error, ABOVE the CAS. This one-shot is sticky
		// until pod restart, so an increment below it would record exactly one
		// error for the life of the process however long the watch stays
		// broken.
		recordWatchError()
		if !secretsWatchBroken.CompareAndSwap(false, true) {
			// Already broken — suppress duplicate WARN.
			return
		}
		slog.Warn("cache.secrets.watch.broken",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
			slog.String("effect", "SecretsCacheServable=false until pod restart; "+
				"FromInformerSecret falls through to upstream"),
		)
	}
	if err := inf.SetWatchErrorHandler(handler); err != nil {
		// Reachable only if the informer already started — the
		// caller guarantees against (installed pre-Start). Log a
		// WARN so the regression is visible without failing the
		// boot.
		slog.Warn("cache.secrets.watch.set_error_handler_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
		)
	}
}

// AssertSecretsInformerWired panics (in test mode) or logs ERROR +
// increments the assertion-violations counter (in prod) when the
// Secrets informer was not wired despite cache=on mode.
//
// PM-ratified test/prod asymmetry — same posture as
// AssertReadPathsScoped (fallthrough_assert.go) and
// AssertRBACSnapshotWired (rbac_snapshot.go).
//
// Called from main.go after StartSecretsInformer returns. A wiring
// regression that bypasses StartSecretsInformer (or whose
// StartSecretsInformer call no-ops on a missing AUTHN_NAMESPACE)
// surfaces here.
func AssertSecretsInformerWired() {
	if secretsInformerAssertionDisabled.Load() {
		return
	}
	if Disabled() {
		// Cache-off mode — invariant inert. Mirrors the
		// fallthrough_assert.go gate.
		return
	}
	if SecretsCacheNamespace() == "" {
		// AUTHN_NAMESPACE empty — the informer was deliberately
		// not started. Production must set it; the assertion fires
		// to surface the misconfiguration. Same posture as
		// AssertReadPathsScoped on a missing required route.
		if env.TestMode() {
			panic("cache.AssertSecretsInformerWired: AUTHN_NAMESPACE is empty in cache=on mode — " +
				"Secrets cache is NOT wired (every /call falls back to apiserver FromSecret)")
		}
		secretsAssertionViolationsTotal.Add(1)
		slog.Error("cache.secrets.informer.assertion_violation",
			slog.String("subsystem", "cache"),
			slog.String("check", "secrets_informer_wired"),
			slog.String("hint", "AUTHN_NAMESPACE is empty; Secrets cache is offline. "+
				"Set authn-namespace via the chart values."),
		)
		return
	}
	if !secretsInformerWired.Load() {
		if env.TestMode() {
			panic("cache.AssertSecretsInformerWired: Secrets informer was not wired " +
				"despite cache=on + AUTHN_NAMESPACE set — StartSecretsInformer was not called " +
				"or returned an error")
		}
		secretsAssertionViolationsTotal.Add(1)
		slog.Error("cache.secrets.informer.assertion_violation",
			slog.String("subsystem", "cache"),
			slog.String("check", "secrets_informer_wired"),
			slog.String("namespace", SecretsCacheNamespace()),
			slog.String("hint", "StartSecretsInformer did not complete — F-3 cache offline"),
		)
	}
}

// SecretsAssertionViolationsTotal returns the cumulative count of
// secrets-informer assertion violations observed in production mode.
// Exported for the AC-D2.3 test gate.
func SecretsAssertionViolationsTotal() uint64 {
	return secretsAssertionViolationsTotal.Load()
}

// stopCh returns a stop channel that closes when ctx is canceled.
// Used by factory.Start which takes a <-chan struct{}.
func stopCh(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

// buildSecretsClient builds a typed kubernetes.Interface from the
// passed-in rest config. Routed through this helper so test code can
// inject a fake clientset via SetSecretsClientForTest.
//
// In production: main.go passes a *rest.Config and we call
// kubernetes.NewForConfig. In tests: the override returns the fake
// clientset without touching rc.
func buildSecretsClient(rc *rest.Config) (kubernetes.Interface, error) {
	// Test injection path takes precedence so a fake client backs
	// the informer in unit tests without requiring a live apiserver.
	if cli := secretsTestClient.Load(); cli != nil && *cli != nil {
		return *cli, nil
	}
	if rc == nil {
		return nil, fmt.Errorf("StartSecretsInformer: rest.Config is nil and no test client set")
	}
	return kubernetes.NewForConfig(rc)
}

// secretsTestClient holds an injectable fake clientset for tests.
// Set via SetSecretsClientForTest; nil in production. Atomic.Pointer
// so SetSecretsClientForTest is goroutine-safe (the AC-D2.5 race
// test sets it on one goroutine and reads it on others).
var secretsTestClient atomic.Pointer[kubernetes.Interface]

// SetSecretsClientForTest injects a fake kubernetes.Interface that
// buildSecretsClient will use INSTEAD of kubernetes.NewForConfig(rc).
// TEST-ONLY — production code MUST NOT call it. Returns a restore
// closure (clears the override).
func SetSecretsClientForTest(cli kubernetes.Interface) func() {
	secretsTestClient.Store(&cli)
	return func() {
		var none kubernetes.Interface
		secretsTestClient.Store(&none)
	}
}

// ResetSecretsInformerForTest clears the package-level informer
// state. TEST-ONLY — production code MUST NOT call it. Mirrors
// ResetSecretsSnapshotForTest pattern.
func ResetSecretsInformerForTest() {
	secretsInformerStarted.Store(false)
	secretsWatchBroken.Store(false)
	secretsInformerHandlePtr.Store(nil)
	var noneNS string
	secretsCacheNamespaceStr.Store(&noneNS)
	secretsInformerWired.Store(false)
	secretsAssertionViolationsTotal.Store(0)
	secretsInformerAssertionDisabled.Store(false)
}

// DisableSecretsInformerAssertionForTest sets the assertion-disabled
// flag so AssertSecretsInformerWired is a no-op. TEST-ONLY — same
// posture as DisableRBACSnapshotForTest. Returns a restore closure.
func DisableSecretsInformerAssertionForTest() func() {
	prev := secretsInformerAssertionDisabled.Swap(true)
	return func() {
		secretsInformerAssertionDisabled.Store(prev)
	}
}

// ForceSecretsCacheReadyForTest synthesizes the post-startup state of
// the Secrets cache so SecretsCacheServable returns true and
// SecretsCacheNamespace returns the passed-in namespace — without
// requiring the test to actually start an informer + fake clientset.
// TEST-ONLY — production code MUST NOT call it.
//
// Used by the AC-D2 namespace-mismatch test and any test that wants
// to exercise FromInformerSecret's cache-hit path against a
// hand-published snapshot.
func ForceSecretsCacheReadyForTest(namespace string) {
	ns := namespace
	secretsCacheNamespaceStr.Store(&ns)
	// Synthesize a handle whose HasSynced returns true so the
	// servability gate flips.
	secretsInformerHandlePtr.Store(&secretsInformerHandle{
		indexer:   nil, // not needed for the lookup path
		hasSynced: func() bool { return true },
	})
	secretsInformerStarted.Store(true)
	secretsInformerWired.Store(true)
	secretsWatchBroken.Store(false)
}
