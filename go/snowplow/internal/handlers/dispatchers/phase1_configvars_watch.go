// phase1_configvars_watch.go — boot-race-tolerant prewarm (shape A,
// docs/prewarm-boot-race-tolerant-2026-07-03.md §2.2).
//
// THE TRIGGER INVERSION. Phase-1 prewarm used to start eagerly at boot and
// read the frontend `*-config-vars` ConfigMap exactly once (phase1_roots.go
// readFrontendConfig). On a fresh install where snowplow boots BEFORE the
// frontend has created that ConfigMap, the one-shot read finds nothing, the
// walk gives up warming, and the pod serves the first navigation COLD for
// its whole lifetime (the rt8rv 2026-07-03 boot race).
//
// The fix makes the APPEARANCE of the config-vars ConfigMap the event that
// DRIVES the prewarm walk. A namespaced, single-object (field-selected)
// ConfigMap informer is registered at Phase-1 start on the process-lifetime
// cacheCtx; its AddFunc (ConfigMap appeared) and UpdateFunc (config.json
// changed) enqueue the engine's scopeKindBoot re-drive. rePrewarmBoot then
// reads config-vars (now present), walks the nav roots, harvests, and seeds
// — the whole path already exists (prewarm_engine_boot.go). Idempotent by
// the "boot" dedup key (prewarm_engine.go:206), and the engine worker
// outlives boot (bound to cacheCtx via SetEngineProcessContext), so the
// re-drive fires whether the ConfigMap arrives before OR after the readiness
// backstop — self-heal with zero pod restart.
//
// NO NEW ENV, NO HARDCODED NAME. The watched ConfigMap name/namespace are
// the SAME config the eager read uses: name = FRONTEND_CONFIG_CONFIGMAP
// (default frontend-config-vars, phase1_roots.go:66-71), namespace = the
// AUTHN_NAMESPACE passed to Phase1Warmup. The name/namespace NEVER appear as
// Go literals here (feedback_no_special_cases): a single-object field
// selector is built from frontendConfigConfigMapName() + authnNS.
//
// HOOK-MUST-NOT-BLOCK. AddFunc/UpdateFunc ONLY call the O(1) non-blocking
// enqueueScope (prewarm_engine.go:273-282) — no inline walk work. The walk
// runs on the engine worker goroutine under its customer-priority yield +
// bounded queue (prewarm_engine_boot.go:105-108 contract).
//
// RBAC. The snowplow SA holds native `*/*` get/list/watch (phase1_walk.go:50
// cites the ClusterRoleBinding), so a namespaced ConfigMap WATCH is already
// authorized. No chart/RBAC change.

package dispatchers

import (
	"context"
	"log/slog"
	"sync/atomic"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"

	corev1 "k8s.io/api/core/v1"
)

// configVarsWatchStarted guards StartConfigVarsWatch against a double start
// (idempotent per process). The first successful start wins; later calls are
// no-ops. Kept package-level (not on a struct) to mirror the other Phase-1
// singletons (seedTokenProvider, engineProcessCtx).
var configVarsWatchStarted atomic.Bool

// configVarsWatchClientForTest injects a fake typed clientset so the
// falsifier can drive AddFunc/UpdateFunc off a real client-go informer
// without a live apiserver. nil in production. Mirrors
// cache.SetSecretsClientForTest.
var configVarsWatchClientForTest atomic.Pointer[kubernetes.Interface]

// SetConfigVarsWatchClientForTest installs a fake kubernetes.Interface that
// StartConfigVarsWatch uses INSTEAD of kubernetes.NewForConfig(rc). TEST-ONLY
// — production MUST NOT call it. Returns a restore closure.
func SetConfigVarsWatchClientForTest(cli kubernetes.Interface) func() {
	configVarsWatchClientForTest.Store(&cli)
	return func() {
		var none kubernetes.Interface
		configVarsWatchClientForTest.Store(&none)
	}
}

// ResetConfigVarsWatchForTest clears the started guard. TEST-ONLY.
func ResetConfigVarsWatchForTest() {
	configVarsWatchStarted.Store(false)
	var none kubernetes.Interface
	configVarsWatchClientForTest.Store(&none)
}

// configVarsEnqueuedTotal counts scopeKindBoot enqueues driven by a
// config-vars ConfigMap event (Add or Update). The falsifier reads this to
// prove the informer — not the eager read — drove the re-walk. Distinct from
// the engine's own enqueuedTotal (which also counts the boot + GVR-discovered
// enqueues).
var configVarsEnqueuedTotal atomic.Uint64

// ConfigVarsEnqueuedTotal exposes the config-vars-driven boot re-enqueue
// count (falsifier + telemetry).
func ConfigVarsEnqueuedTotal() uint64 { return configVarsEnqueuedTotal.Load() }

// configVarsSkippedTotal counts config-vars UPDATE events the data-change gate
// (#106, configVarsDataChanged) SUPPRESSED because Data + BinaryData were
// byte-identical to the previously-delivered state — i.e. a metadata-only write
// (the KrateoFrontend CDC re-apply bumps annotations/managedFields with a fresh
// krateo.io/traceparent span each reconcile, ~1/min on fresh2). Steady-state
// rate == the CDC churn rate, so a climbing value PROVES the gate is actively
// firing (distinguishes gate-working from informer-dead / churn-stopped).
// Exact mirror of the enqueued-counter shape above; exposed via expvar
// snowplow_phase1_configvars_skipped_total (phase1_pip_metrics.go).
var configVarsSkippedTotal atomic.Uint64

// ConfigVarsSkippedTotal exposes the count of UPDATE events suppressed by the
// data-change gate (falsifier + telemetry).
func ConfigVarsSkippedTotal() uint64 { return configVarsSkippedTotal.Load() }

// buildConfigVarsWatchClient builds the typed clientset the informer uses —
// the injected fake in tests, else kubernetes.NewForConfig(rc). Mirrors
// cache.buildSecretsClient.
func buildConfigVarsWatchClient(rc *rest.Config) (kubernetes.Interface, error) {
	if cli := configVarsWatchClientForTest.Load(); cli != nil && *cli != nil {
		return *cli, nil
	}
	return kubernetes.NewForConfig(rc)
}

// matchesConfigVars reports whether an informer event object IS the config-vars
// ConfigMap (name match). The field selector already scopes the LIST/WATCH to
// metadata.name==cmName at the apiserver (production), so this is a cheap
// defense-in-depth guard, NOT walk work — it keeps the trigger correct even if
// the field selector is ever weakened or the informer is backed by a client
// that does not enforce field selectors. O(1) name compare, non-blocking.
func matchesConfigVars(obj interface{}, cmName string) bool {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		// A DeletedFinalStateUnknown or unexpected type — we do not enqueue on
		// it (no DeleteFunc anyway). Only Add/Update carry *ConfigMap here.
		return false
	}
	return cm.Name == cmName
}

// configVarsDataChanged reports whether the config-vars ConfigMap's CONSUMED
// content changed between the informer's previously-delivered state (oldObj) and
// the new one (newObj) — #106 data-change redrive gate (design
// docs/configvars-update-datagate-design-2026-07-07.md).
//
// WHY. UpdateFunc used to enqueue a full boot re-walk on ANY object write. The
// KrateoFrontend CDC re-apply bumps annotations/managedFields (a fresh
// krateo.io/traceparent span each reconcile) → new ResourceVersion → informer
// UPDATE ~1/min, each paying the ~2.3-min walk at 60K while config.json is
// byte-identical. This predicate gates the enqueue on the actual consumed
// content so metadata-only churn stops driving the walk.
//
// SCOPE = FULL Data + BinaryData (superset of the single consumed key
// Data["config.json"], phase1_roots.go frontendConfigDataKey), never narrower
// than consumed: a future walker reading a sibling key must NOT silently go
// stale, and a redundant coalesced walk on a real frontend deploy touching a
// sibling key is self-limiting. Matches the Helm checksum/config prior-art.
//
// FAIL-OPEN. If EITHER object is not a *corev1.ConfigMap (unexpected type /
// tombstone) the function returns true (== the pre-#106 unconditional-enqueue
// behavior) — never suppress a redrive on doubt (a suppressed genuine redrive is
// the cold-first-nav class this watcher exists to kill, and it fails silent).
// O(1)-ish map compare over a 1-key ConfigMap; the HOOK-MUST-NOT-BLOCK contract
// holds (no parse, no apiserver read).
func configVarsDataChanged(oldObj, newObj interface{}) bool {
	oldCM, oldOK := oldObj.(*corev1.ConfigMap)
	newCM, newOK := newObj.(*corev1.ConfigMap)
	if !oldOK || !newOK {
		return true // fail OPEN — pre-#106 behavior; never suppress on doubt
	}
	return !apiequality.Semantic.DeepEqual(oldCM.Data, newCM.Data) ||
		!apiequality.Semantic.DeepEqual(oldCM.BinaryData, newCM.BinaryData)
}

// enqueueBootReDrive is the AddFunc/UpdateFunc body: it ONLY calls the
// engine's non-blocking enqueueScope(scopeKindBoot) (after the O(1) name
// guard). The scopeKindBoot key coalesces (prewarm_engine.go:206), so a burst
// of ConfigMap events collapses to at most one pending boot scope — behavioral
// neutrality when the object is already present at boot (one Add at sync → one
// coalescing enqueue).
//
// HOOK-MUST-NOT-BLOCK: no walk work here — enqueueScope is O(1).
func enqueueBootReDrive(reason string) {
	configVarsEnqueuedTotal.Add(1)
	bootScope := prewarmScope{kind: scopeKindBoot}
	// #132 F4b Lever A / #643 — a config-vars redrive is a NEW TOPOLOGY (the frontend
	// config.json / nav set changed). CLEAR the engine-wide declined-external
	// marker set (kept deliberately by #643: a rare event, and the conservative
	// answer to "can a config change alter external-ness?") so the redrive re-resolves each external
	// whale ONCE under the new nav set, rather than staying suppressed across the
	// topology change (a stale suppression would leave a now-navigable external
	// widget cold-forever). A plain F.4 deadline-cut requeue does NOT come through
	// here (it AddRateLimited's inside processScope and keeps the set) — only a
	// genuine config change clears.
	prewarmEngineSingleton().clearDeclinedExternalSet(bootScope.key(), "config-vars-redrive")
	// #105 — a config-vars redrive is a NEW TOPOLOGY: also clear the engine-lived
	// boot-convergence state so the redrive re-arms with empty seeded/failed sets
	// and a zeroed no-progress counter. A target given up under the OLD topology
	// (converged-with-skips) gets a genuine fresh attempt under the new nav set,
	// and the set-delta is computed against the new topology's first pass rather
	// than a stale prior. A plain F.4 deadline-cut requeue does NOT come through
	// here (it keeps the state) — only a genuine config change clears.
	prewarmEngineSingleton().clearBootConvergenceState(bootScope.key())
	// #135 F4b Lever B — RESET the boot scope's workqueue requeue count so this
	// genuine topology change re-dequeues at attempt==0 → rePrewarmBootScoped
	// WALKS the new config.json nav set instead of REUSING the prior topology's
	// harvester snapshot. MUST precede enqueueScope (a plain queue.Add that does
	// NOT reset NumRequeues): without it, a redrive landing mid-F.4-requeue-streak
	// (NumRequeues>0) leaves attempt>0 → Lever B reuses the STALE nav set across
	// the topology change. A plain F.4 deadline-cut resume does NOT come through
	// here (it AddRateLimited's in processScope), so its attempt>0 fast-path reuse
	// is preserved — only a genuine config change forces the re-walk. Paired with
	// clearBootConvergenceState above: a topology change resets BOTH the #105
	// convergence sets AND the #135 requeue-attempt count on the same event.
	prewarmEngineSingleton().forgetScope(bootScope)
	prewarmEngineSingleton().enqueueScope(bootScope)
	slog.Info("prewarm.configvars.boot_redrive_enqueued",
		slog.String("subsystem", "cache"),
		slog.String("reason", reason),
		slog.String("effect", "config-vars ConfigMap event drove a scopeKindBoot re-walk (coalesces on the boot key); "+
			"F4b declined-external set cleared so whales re-resolve once under the new topology"),
	)
}

// StartConfigVarsWatch registers a single-object, namespaced, field-selected
// ConfigMap informer for (authnNS, frontendConfigConfigMapName()). On the
// ConfigMap's appearance (AddFunc) or a config.json change (UpdateFunc) it
// enqueues a scopeKindBoot re-drive on the prewarm engine.
//
// LIFETIME. ctx MUST be the process-lifetime cacheCtx (NOT p1Ctx): the
// informer must stay live so a ConfigMap that lands LONG after the Phase-1
// budget / readiness backstop still drives a self-heal re-walk (§2.2, §2.6).
// The engine worker it enqueues to is also cacheCtx-bound
// (SetEngineProcessContext), so the enqueue always targets a live worker.
//
// Idempotent (configVarsWatchStarted). Soft-fail: a client-build error leaves
// the eager one-shot read as the only trigger — degraded, not fatal (the pod
// still serves cold and the eager read still covers the deps-warm-at-boot
// case).
//
// MUST be called only under cache.PrewarmEnabled() + PrewarmEngineEnabled()
// (main.go enforces) — the enqueue is meaningless without a running engine.
func StartConfigVarsWatch(ctx context.Context, rc *rest.Config, authnNS string) {
	log := slog.Default()
	if authnNS == "" {
		// No namespace to scope the informer to — the eager read has the
		// same dependency, so this mirrors that degrade. Production sets
		// AUTHN_NAMESPACE via the chart.
		log.Info("prewarm.configvars.watch_skipped",
			slog.String("subsystem", "cache"),
			slog.String("reason", "authn namespace empty — no config-vars namespace to watch"),
		)
		return
	}
	if !configVarsWatchStarted.CompareAndSwap(false, true) {
		return // idempotent re-call
	}

	client, err := buildConfigVarsWatchClient(rc)
	if err != nil {
		log.Warn("prewarm.configvars.watch_client_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
			slog.String("effect", "config-vars watch not wired; eager one-shot read remains the only trigger (degraded, not fatal)"),
		)
		configVarsWatchStarted.Store(false) // allow a later retry
		return
	}

	cmName := frontendConfigConfigMapName()

	// Single-object field selector: metadata.name == cmName. Combined with
	// the namespace scope below this is the smallest possible watch — ONE
	// object in ONE namespace, not a cluster-wide ConfigMap watch. The name
	// is config (frontendConfigConfigMapName), never a Go literal.
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // resync period 0 — pure event-driven (matches the dynamic + secrets factories)
		informers.WithNamespace(authnNS),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", cmName).String()
		}),
	)

	inf := factory.Core().V1().ConfigMaps().Informer()

	if _, err := inf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
		// AddFunc: config-vars appeared (or the initial-list replay when it
		// is already present at boot). Drives the boot re-walk. UNGATED
		// (#106 C106-3a): first appearance / initial-LIST replay always
		// redrives — it is the boot-race self-heal trigger itself and has no
		// prior state to diff.
		AddFunc: func(obj interface{}) {
			if matchesConfigVars(obj, cmName) {
				enqueueBootReDrive("configmap_added")
			}
		},
		// UpdateFunc: re-drives ONLY when the consumed content actually changed
		// (#106 data-change gate). The KrateoFrontend CDC re-apply bumps
		// annotations/managedFields (fresh traceparent span each reconcile) →
		// UPDATE ~1/min with byte-identical config.json; without the gate each
		// paid a full ~2.3-min boot re-walk and starved the keepwarm sweep. The
		// gate compares the informer's own delivered (old,new) pair — last-seen
		// ≡ last-redriven, no stored hash, so a flap A→B→A redrives on BOTH
		// events by construction (design §"No stored hash").
		UpdateFunc: func(oldObj, newObj interface{}) {
			if !matchesConfigVars(newObj, cmName) {
				return
			}
			if !configVarsDataChanged(oldObj, newObj) {
				configVarsSkippedTotal.Add(1)
				slog.Info("prewarm.configvars.redrive_skipped",
					slog.String("subsystem", "cache"),
					slog.String("reason", "data_unchanged_metadata_only"),
					slog.String("configmap", cmName),
					slog.String("effect", "config-vars UPDATE was metadata-only (Data+BinaryData byte-identical); "+
						"boot re-walk SUPPRESSED (#106 CDC-churn gate) — the walk reads the live CM, nothing to re-warm"),
				)
				return
			}
			enqueueBootReDrive("configmap_data_changed")
		},
		// No DeleteFunc: a deleted config-vars ConfigMap has no roots to
		// re-walk — nothing to enqueue. delete→recreate is covered by the
		// unconditional AddFunc (recreate == ADD).
	}); err != nil {
		log.Warn("prewarm.configvars.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
		)
	}

	// Start the factory bound to ctx (cacheCtx) — process shutdown closes
	// it. Non-blocking; the LIST/WATCH runs on spawned goroutines. We do NOT
	// WaitForCacheSync here: the AddFunc replay on first sync is exactly the
	// signal we want, and blocking boot on it would re-introduce a startup
	// stall (the 0.30.220 lesson) — the whole point is that prewarm start is
	// event-driven, not boot-blocking.
	factory.Start(cacheStopCh(ctx))

	log.Info("prewarm.configvars.watch_started",
		slog.String("subsystem", "cache"),
		slog.String("namespace", authnNS),
		slog.String("configmap", cmName),
		slog.String("lifetime", "process (cacheCtx)"),
		slog.String("selector", "metadata.name single-object"),
	)
}

// cacheStopCh returns a stop channel that closes when ctx is cancelled, for
// SharedInformerFactory.Start (which takes a <-chan struct{}). Local sibling
// of cache.stopCh (unexported there; the dispatchers package cannot reach it
// without a new export, so we keep a tiny local copy).
func cacheStopCh(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}
