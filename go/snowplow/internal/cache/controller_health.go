// controller_health.go — Ship Resilience-1 (0.30.162). Observability-
// only subsystem that surfaces the state of upstream Kubernetes
// controllers (Deployment pod-restart age + Service Endpoints
// readiness + admission-webhook configuration) as expvar gauges on
// /debug/vars, so operators diagnosing a "snowplow is not working"
// report can distinguish SCENARIO 5 (downstream apiserver pressure
// from a crash-looping upstream controller) from a snowplow defect
// in seconds rather than ~2 hours.
//
// Read-only across the watch surface — no writes to apiserver. The
// only per-rebuild apiserver read is a bounded Pod LIST (label-
// selector-scoped) for the pod-restart conjunct; everything else
// reads the typed informer indexer.
//
// Discovery is mechanism-uniform per feedback_no_special_cases: the
// controller set is auto-discovered from MutatingWebhookConfiguration
// and ValidatingWebhookConfiguration webhooks[].clientConfig.service
// references. NO hardcoded controller-name table.
//
// LIFECYCLE.
//
//   - StartControllerHealthInformer (called from main.go after
//     SetRESTConfig): builds kubernetes.NewForConfig(rc), constructs
//     one namespace-scoped factory per CONTROLLER_HEALTH_NAMESPACES
//     entry for the Deployment + Endpoints watches, plus one
//     cluster-scoped factory for the admissionregistration watches.
//     Wires event handlers that schedule a snapshot rebuild,
//     installs the SetWatchErrorHandler, starts the factories,
//     waits for cache sync (bounded), publishes the initial snapshot.
//
//   - Idempotent: subsequent calls after a successful start are
//     no-ops (controllerHealthStarted gates re-entry).
//
//   - CACHE_ENABLED=false → no-op at the top of Start (per
//     project_caching_is_provisional). No typed clientset built, no
//     factory constructed, no goroutine spawned, no WaitForCacheSync
//     call. Both expvar gauges return empty maps.
//
//   - Empty namespace list → cluster-scoped admissionregistration
//     watches still run (so webhook_failurepolicy gauge is
//     populated); per-namespace Deployment + Endpoints watches do
//     not. The webhook discovery still happens; controller-health
//     entries without a matching watched Deployment carry
//     Reason="unwired".
//
// CONCURRENCY.
//   - Snapshot publish via atomic.Pointer (same shape as
//     secretsInformerHandlePtr).
//   - Single-writer rebuild via tryLock + dirty-bit (mirrors
//     scheduleSecretsRebuild).
//   - prevRestarts is held inside controllerHealthHandle and only
//     touched on the single writer goroutine; readers (expvar.Func
//     callers) load the immutable snapshot pointer.
package cache

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
)

// ControllerHealthEntry is the per-controller value published in
// snowplow_upstream_controller_health. Immutable after Store.
type ControllerHealthEntry struct {
	Healthy            int    `json:"Healthy"`
	Reason             string `json:"Reason"`
	PodRestartCount    int    `json:"PodRestartCount"`
	EndpointReadyCount int    `json:"EndpointReadyCount"`
	Namespace          string `json:"Namespace"`
	Name               string `json:"Name"`
	LastObserved       int64  `json:"LastObserved"`
}

// WebhookFailurePolicyEntry is the per-webhook value published in
// snowplow_upstream_webhook_failurepolicy. Immutable after Store.
type WebhookFailurePolicyEntry struct {
	Policy        string `json:"Policy"`        // "Fail" | "Ignore"
	Configuration string `json:"Configuration"` // owning *WebhookConfiguration name
	Type          string `json:"Type"`          // "Mutating" | "Validating"
}

// ControllerHealthSnapshot is the immutable view published atomically
// by every rebuild. Readers MUST treat the maps as read-only.
type ControllerHealthSnapshot struct {
	// Controllers keys "<namespace>/<name>" → entry. Stable key for
	// grep tooling.
	Controllers map[string]ControllerHealthEntry
	// Webhooks keys webhook-entry name (the webhook's `.name` field
	// inside webhooks[]) → entry.
	Webhooks map[string]WebhookFailurePolicyEntry
}

// Reason enum (closed, cardinality-budgeted).
const (
	controllerReasonHealthy     = ""
	controllerReasonPodRestart  = "pod-restart-within-window"
	controllerReasonNoEndpoints = "endpoints-zero-ready"
	controllerReasonBoth        = "both"
	controllerReasonUnwired     = "unwired"
)

// controllerHealthHandle bundles the indexers needed for the rebuild
// walk plus the typed clientset used for the per-controller Pod LIST.
// Published once via atomic.Pointer at end of Start (post bounded
// WaitForCacheSync).
type controllerHealthHandle struct {
	client kubernetes.Interface

	// nsFactories — one per CONTROLLER_HEALTH_NAMESPACES entry, for
	// W1 (Deployments) + W2 (Endpoints).
	nsFactories map[string]informers.SharedInformerFactory

	// clusterFactory hosts W3 (MutatingWebhookConfigurations) + W4
	// (ValidatingWebhookConfigurations).
	clusterFactory informers.SharedInformerFactory

	// hasSyncedFns is the union of every informer's HasSynced
	// predicate, used by ControllerHealthCacheServable.
	hasSyncedFns []clientcache.InformerSynced

	// prevRestarts retains the previous rebuild's per-pod restart
	// count map per controller, keyed "ns/name" → map[podName]int.
	// Guarded by prevMu so concurrent rebuilds (the synchronous
	// initial publish vs. an event-handler-scheduled rebuild) can
	// share the prev state safely. In production a single rebuild
	// goroutine touches this at a time (tryLock pattern in
	// scheduleControllerHealthRebuild); the mutex covers the
	// transient overlap during Start where the synchronous final
	// rebuild may race a just-spawned scheduled rebuild.
	prevMu       sync.Mutex
	prevRestarts map[string]map[string]int

	// namespaces is the watch-scope set captured at Start time.
	namespaces []string
}

var (
	// controllerHealthStarted is set true after the initial cache
	// sync completes in StartControllerHealthInformer.
	controllerHealthStarted atomic.Bool

	// controllerHealthHandlePtr holds the post-Start handle. Loaded
	// lock-free by the rebuild goroutine + every reader.
	controllerHealthHandlePtr atomic.Pointer[controllerHealthHandle]

	// controllerHealthWatchBroken is set true when ANY of the four
	// reflectors drops its WATCH. Snapshot stays at last-published.
	controllerHealthWatchBroken atomic.Bool

	// controllerHealthSnap is the published immutable snapshot.
	// nil until first rebuild publishes (pre-readiness sentinel).
	controllerHealthSnap atomic.Pointer[ControllerHealthSnapshot]

	// controllerHealthRebuildLock + ...Dirty: single-writer atomic
	// tryLock + dirty-bit. Mirrors scheduleSecretsRebuild.
	controllerHealthRebuildLock  atomic.Bool
	controllerHealthRebuildDirty atomic.Bool

	// controllerHealthPublishSeq bumps on every successful publish.
	// Log-correlation only; not load-bearing.
	controllerHealthPublishSeq atomic.Uint64

	// controllerHealthTestClient: injectable fake clientset for
	// unit tests. nil in production. atomic.Pointer for thread
	// safety in the race test.
	controllerHealthTestClient atomic.Pointer[kubernetes.Interface]
)

// ControllerHealthSnapshotLoad returns the latest published
// snapshot, or nil if no snapshot has been published yet
// (pre-readiness / cache=off). Lock-free single atomic load.
func ControllerHealthSnapshotLoad() *ControllerHealthSnapshot {
	return controllerHealthSnap.Load()
}

// ControllerHealthCacheServable reports whether the controller-
// health snapshot can be trusted by an external consumer (e.g. a
// reporting tool). Returns false when:
//   - Disabled() (cache=off),
//   - StartControllerHealthInformer was not called,
//   - any reflector dropped its WATCH (watchBroken sticky),
//   - any informer is not yet HasSynced.
func ControllerHealthCacheServable() bool {
	if Disabled() {
		return false
	}
	if !controllerHealthStarted.Load() {
		return false
	}
	if controllerHealthWatchBroken.Load() {
		return false
	}
	h := controllerHealthHandlePtr.Load()
	if h == nil {
		return false
	}
	for _, fn := range h.hasSyncedFns {
		if fn == nil || !fn() {
			return false
		}
	}
	return true
}

// StartControllerHealthInformer constructs the Resilience-1 watch
// surface and wires it for the expvar gauges. Idempotent — re-calls
// after a successful start are no-ops.
//
// Errors at construction time leave the subsystem inert — the gauges
// publish empty maps; operators see the wiring error in startup log.
// Soft-fail by design (matches secrets_informer.go precedent).
//
// CACHE_ENABLED=false → no-op (AC-R1.4). No client built, no factory
// constructed, no goroutine spawned.
//
// namespaces is the watch-scope set parsed from
// CONTROLLER_HEALTH_NAMESPACES. Empty slice → cluster-scoped
// webhook watches still run (so webhook gauge is populated);
// per-namespace Deployment + Endpoints watches do not.
func StartControllerHealthInformer(ctx context.Context, rc *rest.Config, namespaces []string) error {
	if Disabled() {
		// Cache-off — invariant inert per
		// project_caching_is_provisional. No typed clientset, no
		// factory, no goroutine.
		return nil
	}
	if controllerHealthStarted.Load() {
		// Idempotent re-call.
		return nil
	}

	client, err := buildControllerHealthClient(rc)
	if err != nil {
		return fmt.Errorf("cache.controller_health: build client: %w", err)
	}

	// Cluster-scoped factory for W3+W4 (admissionregistration is
	// cluster-scoped — no WithNamespace).
	clusterFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // event-driven; matches secrets_informer.go
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.Limit = listPageLimit
		}),
	)

	// Per-namespace factory for W1+W2 (Deployments + Endpoints).
	nsFactories := make(map[string]informers.SharedInformerFactory, len(namespaces))
	for _, ns := range namespaces {
		nsFactories[ns] = informers.NewSharedInformerFactoryWithOptions(
			client,
			0,
			informers.WithNamespace(ns),
			informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
				opts.Limit = listPageLimit
			}),
		)
	}

	// Collect informers + install event handlers + watch-error
	// handlers. Every Add/Update/Delete schedules a snapshot
	// rebuild via scheduleControllerHealthRebuild — the handler
	// body returns immediately.
	var hasSyncedFns []clientcache.InformerSynced
	for _, f := range nsFactories {
		depInf := f.Apps().V1().Deployments().Informer()
		epInf := f.Core().V1().Endpoints().Informer()
		installControllerHealthHandlers(depInf)
		installControllerHealthHandlers(epInf)
		installControllerHealthWatchErrorHandler(depInf)
		installControllerHealthWatchErrorHandler(epInf)
		hasSyncedFns = append(hasSyncedFns, depInf.HasSynced, epInf.HasSynced)
	}
	mwcInf := clusterFactory.Admissionregistration().V1().MutatingWebhookConfigurations().Informer()
	vwcInf := clusterFactory.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer()
	installControllerHealthHandlers(mwcInf)
	installControllerHealthHandlers(vwcInf)
	installControllerHealthWatchErrorHandler(mwcInf)
	installControllerHealthWatchErrorHandler(vwcInf)
	hasSyncedFns = append(hasSyncedFns, mwcInf.HasSynced, vwcInf.HasSynced)

	// Publish the handle BEFORE Start so the first event-driven
	// rebuild can find the indexers.
	nsCopy := append([]string(nil), namespaces...)
	controllerHealthHandlePtr.Store(&controllerHealthHandle{
		client:         client,
		nsFactories:    nsFactories,
		clusterFactory: clusterFactory,
		hasSyncedFns:   hasSyncedFns,
		prevRestarts:   make(map[string]map[string]int),
		namespaces:     nsCopy,
	})

	// Start every factory. Process shutdown closes ctx.Done() →
	// every reflector's Run returns → no goroutine leak.
	for _, f := range nsFactories {
		f.Start(stopCh(ctx))
	}
	clusterFactory.Start(stopCh(ctx))

	// Bounded WaitForCacheSync — 30s, same budget as
	// secrets_informer.go. Soft-fail on timeout (factories continue
	// syncing in background; the post-sync rebuild will publish
	// once events deliver).
	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()
	if ok := clientcache.WaitForCacheSync(syncCtx.Done(), hasSyncedFns...); !ok {
		slog.Warn("cache.controller_health.initial_sync_timeout",
			slog.String("subsystem", "cache"),
			slog.Any("namespaces", namespaces),
			slog.String("hint", "controller-health gauges will populate when HasSynced flips"),
		)
		// Do NOT return an error. The factories are started;
		// HasSynced may flip true after this returns.
	}

	// Publish the initial snapshot synchronously.
	rebuildControllerHealthSnapshot()

	controllerHealthStarted.Store(true)

	slog.Info("cache.controller_health.started",
		slog.String("subsystem", "cache"),
		slog.Any("namespaces", namespaces),
		slog.Int("initial_controllers", initialControllerCount()),
		slog.Int("initial_webhooks", initialWebhookCount()),
	)
	return nil
}

// installControllerHealthHandlers wires the snapshot-rebuild
// scheduler onto every event from the given informer. The handler
// body returns immediately; the indexer walk runs on the detached
// rebuild goroutine.
func installControllerHealthHandlers(inf clientcache.SharedIndexInformer) {
	if _, err := inf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ interface{}) { scheduleControllerHealthRebuild() },
		UpdateFunc: func(_, _ interface{}) { scheduleControllerHealthRebuild() },
		DeleteFunc: func(_ interface{}) { scheduleControllerHealthRebuild() },
	}); err != nil {
		slog.Warn("cache.controller_health.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
		)
	}
}

// installControllerHealthWatchErrorHandler wires the sticky-broken
// WATCH-error handler. Mirrors installSecretsWatchErrorHandler.
func installControllerHealthWatchErrorHandler(inf clientcache.SharedIndexInformer) {
	handler := controllerHealthWatchErrorHandler()
	if err := inf.SetWatchErrorHandler(handler); err != nil {
		slog.Warn("cache.controller_health.watch.set_error_handler_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
		)
	}
}

// controllerHealthWatchErrorHandler builds the controller-health reflector
// error handler. Named rather than inline (1.12.7 review C-2) so an arm can
// drive the REAL handler twice and pin that the count sits ABOVE the sticky
// one-shot.
func controllerHealthWatchErrorHandler() func(*clientcache.Reflector, error) {
	return func(_ *clientcache.Reflector, err error) {
		// 1.12.7 — count EVERY error, ABOVE the CAS. This one-shot is sticky
		// until pod restart, so an increment below it would record exactly one
		// error for the life of the process however long the watch stays
		// broken.
		recordWatchError()
		if !controllerHealthWatchBroken.CompareAndSwap(false, true) {
			return // duplicate WARN suppressed
		}
		slog.Warn("cache.controller_health.watch.broken",
			slog.String("subsystem", "cache"),
			slog.Any("err", err),
			slog.String("effect", "ControllerHealthCacheServable=false until pod restart; gauge values stay at last-published"),
		)
	}
}

// scheduleControllerHealthRebuild flips the dirty bit; if no rebuild
// is in flight, spawns ONE goroutine that drains the bit by looping.
// Mirrors scheduleSecretsRebuild precisely.
func scheduleControllerHealthRebuild() {
	controllerHealthRebuildDirty.Store(true)
	if !controllerHealthRebuildLock.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer controllerHealthRebuildLock.Store(false)
		defer func() {
			if r := recover(); r != nil {
				controllerHealthRebuildDirty.Store(true)
				slog.Error("cache.controller_health.rebuild_panic",
					slog.String("subsystem", "cache"),
					slog.Any("recovered", r),
					slog.String("stack", string(debug.Stack())),
				)
			}
		}()
		for {
			controllerHealthRebuildDirty.Store(false)
			rebuildControllerHealthSnapshot()
			if !controllerHealthRebuildDirty.Load() {
				return
			}
		}
	}()
}

// rebuildControllerHealthSnapshot walks every informer indexer once,
// computes the controller-health + webhook-failurepolicy snapshots,
// and publishes them atomically. Single-writer (scheduleControllerHealthRebuild
// gates re-entry).
//
// Algorithm:
//
//  1. Walk W3 + W4 (MWC + VWC). For every webhook entry: record the
//     failurePolicy in `webhooks` and seed a controller-candidate
//     (ns, name) tuple from clientConfig.service IF the namespace
//     falls within the watch scope.
//  2. For each candidate (ns, name): look up the Deployment + Endpoints
//     in the ns-factory's indexer (key "ns/name"). If both missing →
//     skip (the controller's webhook references a service that does
//     not exist in our watched ns set, expected when discovery
//     scope is narrow). If Deployment present: compute
//     PodRestartCount via a label-selector-scoped Pod LIST. Compute
//     EndpointReadyCount from Endpoints.Subsets.
//  3. Compute Healthy + Reason via the §2.3 enum.
//  4. Store the immutable snapshot.
func rebuildControllerHealthSnapshot() {
	h := controllerHealthHandlePtr.Load()
	if h == nil {
		return
	}

	now := time.Now().UnixNano()

	// Step 1 — webhook walk. Build:
	//   webhooks map (for the gauge)
	//   candidates map "ns/name" → empty (set of controllers to watch)
	webhooks := map[string]WebhookFailurePolicyEntry{}
	candidates := map[string]struct {
		ns   string
		name string
	}{}

	collectWebhook := func(cfgName, webhookName string, fp *admissionv1.FailurePolicyType, svc *admissionv1.ServiceReference, typ string) {
		policy := ""
		if fp != nil {
			policy = string(*fp)
		}
		webhooks[webhookName] = WebhookFailurePolicyEntry{
			Policy:        policy,
			Configuration: cfgName,
			Type:          typ,
		}
		if svc == nil {
			return
		}
		// Seed a candidate ONLY if the namespace falls within the
		// configured watch-scope set. That keeps the
		// per-controller cardinality bounded by chart config.
		if !h.inScope(svc.Namespace) {
			return
		}
		key := svc.Namespace + "/" + svc.Name
		candidates[key] = struct {
			ns   string
			name string
		}{ns: svc.Namespace, name: svc.Name}
	}

	if h.clusterFactory != nil {
		mwcIdx := h.clusterFactory.Admissionregistration().V1().MutatingWebhookConfigurations().Informer().GetIndexer()
		for _, it := range mwcIdx.List() {
			cfg, ok := it.(*admissionv1.MutatingWebhookConfiguration)
			if !ok {
				continue
			}
			for _, wh := range cfg.Webhooks {
				wh := wh
				collectWebhook(cfg.Name, wh.Name, wh.FailurePolicy, wh.ClientConfig.Service, "Mutating")
			}
		}
		vwcIdx := h.clusterFactory.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer().GetIndexer()
		for _, it := range vwcIdx.List() {
			cfg, ok := it.(*admissionv1.ValidatingWebhookConfiguration)
			if !ok {
				continue
			}
			for _, wh := range cfg.Webhooks {
				wh := wh
				collectWebhook(cfg.Name, wh.Name, wh.FailurePolicy, wh.ClientConfig.Service, "Validating")
			}
		}
	}

	// Step 2+3 — per-candidate controller-health.
	controllers := map[string]ControllerHealthEntry{}
	for key, c := range candidates {
		entry := ControllerHealthEntry{
			Namespace:    c.ns,
			Name:         c.name,
			LastObserved: now,
		}

		f, ok := h.nsFactories[c.ns]
		if !ok {
			// Webhook references an ns we don't watch — skip the
			// per-controller probe; the webhook still appears in
			// the webhooks gauge above (operators see the
			// failurePolicy without a controller-health row).
			continue
		}

		// Deployment lookup via indexer (key "ns/name").
		depIdx := f.Apps().V1().Deployments().Informer().GetIndexer()
		depObj, depExists, _ := depIdx.GetByKey(key)
		if !depExists {
			entry.Healthy = 0
			entry.Reason = controllerReasonUnwired
			controllers[key] = entry
			continue
		}
		dep, _ := depObj.(*appsv1.Deployment)

		// Endpoints lookup via indexer (same key).
		epIdx := f.Core().V1().Endpoints().Informer().GetIndexer()
		epObj, epExists, _ := epIdx.GetByKey(key)
		var ep *corev1.Endpoints
		if epExists {
			ep, _ = epObj.(*corev1.Endpoints)
		}

		// Conjunct B — Endpoints readiness.
		entry.EndpointReadyCount = countReadyEndpoints(ep)
		conjunctB := entry.EndpointReadyCount == 0

		// Conjunct A — pod restart delta over 5-min window. Pod
		// LIST is bounded by Deployment-selector + ns. Per design
		// §2.3 this is an apiserver READ; no WATCH/INFORMER added
		// (avoids 50K-pod cardinality at production scale).
		curRestarts, currentTotal := h.podRestartCounts(dep)
		conjunctA := h.swapPrevRestarts(key, curRestarts)
		entry.PodRestartCount = currentTotal

		// §2.3 enum.
		switch {
		case conjunctA && conjunctB:
			entry.Healthy = 0
			entry.Reason = controllerReasonBoth
		case conjunctA:
			entry.Healthy = 0
			entry.Reason = controllerReasonPodRestart
		case conjunctB:
			entry.Healthy = 0
			entry.Reason = controllerReasonNoEndpoints
		default:
			entry.Healthy = 1
			entry.Reason = controllerReasonHealthy
		}

		controllers[key] = entry
	}

	snap := &ControllerHealthSnapshot{
		Controllers: controllers,
		Webhooks:    webhooks,
	}
	controllerHealthSnap.Store(snap)
	controllerHealthPublishSeq.Add(1)

	slog.Debug("cache.controller_health.snapshot.published",
		slog.String("subsystem", "cache"),
		slog.Int("controllers", len(controllers)),
		slog.Int("webhooks", len(webhooks)),
		slog.Uint64("seq", controllerHealthPublishSeq.Load()),
	)
}

// swapPrevRestarts atomically swaps the per-controller previous-
// restart-count map with `cur` and returns true iff any pod's
// restart count advanced (Conjunct A delta). Guards the prevRestarts
// state under prevMu so the synchronous initial publish in Start
// can race an event-handler-scheduled rebuild without UB.
func (h *controllerHealthHandle) swapPrevRestarts(key string, cur map[string]int) bool {
	h.prevMu.Lock()
	defer h.prevMu.Unlock()
	prev := h.prevRestarts[key]
	fired := podRestartDelta(prev, cur) > 0
	h.prevRestarts[key] = cur
	return fired
}

// inScope reports whether ns is in the configured watch-scope set.
func (h *controllerHealthHandle) inScope(ns string) bool {
	if h == nil {
		return false
	}
	for _, w := range h.namespaces {
		if w == ns {
			return true
		}
	}
	return false
}

// podRestartCounts issues a label-selector-scoped Pod LIST and
// returns a map[podName]restartCountSum + the aggregate total. Per
// design §2.3 Conjunct A: this is a fresh API call (NOT informer-
// served) so we avoid adding a Pods informer (the data-plane Pods
// at production scale would blow up cardinality). The LIST is
// bounded by Deployment.Spec.Selector and ns; ≤ ~50 pods per
// controller in practice; one LIST per rebuild per controller.
//
// READ-ONLY — no write verbs. HG-2 discharge.
func (h *controllerHealthHandle) podRestartCounts(dep *appsv1.Deployment) (map[string]int, int) {
	out := map[string]int{}
	total := 0
	if h == nil || h.client == nil || dep == nil {
		return out, total
	}
	if dep.Spec.Selector == nil {
		return out, total
	}
	sel := labels.SelectorFromSet(labels.Set(dep.Spec.Selector.MatchLabels))
	pods, err := h.client.CoreV1().Pods(dep.Namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: sel.String(),
		Limit:         listPageLimit,
	})
	if err != nil {
		// Pod LIST failure → leave the delta map empty so the
		// next rebuild can re-establish a baseline. The controller
		// is still observable via Conjunct B (endpoints).
		slog.Debug("cache.controller_health.pod_list_failed",
			slog.String("subsystem", "cache"),
			slog.String("ns", dep.Namespace),
			slog.String("name", dep.Name),
			slog.Any("err", err),
		)
		return out, total
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		var sum int
		for _, cs := range p.Status.ContainerStatuses {
			sum += int(cs.RestartCount)
		}
		out[p.Name] = sum
		total += sum
	}
	return out, total
}

// podRestartDelta returns the count of pods whose RestartCount
// advanced between two rebuilds. A pod present in cur but absent
// from prev does NOT count as a restart (new pod, baseline
// establishment).
func podRestartDelta(prev, cur map[string]int) int {
	if len(prev) == 0 {
		return 0
	}
	d := 0
	for name, c := range cur {
		if p, ok := prev[name]; ok && c > p {
			d++
		}
	}
	return d
}

// countReadyEndpoints returns the number of ready endpoint
// addresses across every subset. notReadyAddresses is informational
// and does NOT count (§2.3 Conjunct B).
func countReadyEndpoints(ep *corev1.Endpoints) int {
	if ep == nil {
		return 0
	}
	n := 0
	for _, s := range ep.Subsets {
		n += len(s.Addresses)
	}
	return n
}

// buildControllerHealthClient builds a typed kubernetes.Interface
// from the passed-in rest config, OR returns the test-injected fake
// clientset (set via SetControllerHealthClientForTest). Same shape
// as buildSecretsClient.
func buildControllerHealthClient(rc *rest.Config) (kubernetes.Interface, error) {
	if cli := controllerHealthTestClient.Load(); cli != nil && *cli != nil {
		return *cli, nil
	}
	if rc == nil {
		return nil, fmt.Errorf("StartControllerHealthInformer: rest.Config is nil and no test client set")
	}
	return kubernetes.NewForConfig(rc)
}

// SetControllerHealthClientForTest injects a fake clientset for unit
// tests. TEST-ONLY — production MUST NOT call. Returns a restore
// closure (clears the override).
func SetControllerHealthClientForTest(cli kubernetes.Interface) func() {
	controllerHealthTestClient.Store(&cli)
	return func() {
		var none kubernetes.Interface
		controllerHealthTestClient.Store(&none)
	}
}

// ResetControllerHealthForTest clears every package-level state.
// TEST-ONLY.
func ResetControllerHealthForTest() {
	controllerHealthStarted.Store(false)
	controllerHealthWatchBroken.Store(false)
	controllerHealthHandlePtr.Store(nil)
	controllerHealthSnap.Store(nil)
	controllerHealthRebuildLock.Store(false)
	controllerHealthRebuildDirty.Store(false)
	controllerHealthPublishSeq.Store(0)
	var none kubernetes.Interface
	controllerHealthTestClient.Store(&none)
}

// RebuildControllerHealthSnapshotForTest synchronously runs the
// rebuild walk so tests can assert on the published snapshot
// without waiting for an event handler to schedule. TEST-ONLY.
func RebuildControllerHealthSnapshotForTest() {
	rebuildControllerHealthSnapshot()
}

// ControllerHealthPublishSeqForTest returns the publish-seq
// counter for test correlation. TEST-ONLY.
func ControllerHealthPublishSeqForTest() uint64 {
	return controllerHealthPublishSeq.Load()
}

func initialControllerCount() int {
	s := controllerHealthSnap.Load()
	if s == nil {
		return 0
	}
	return len(s.Controllers)
}

func initialWebhookCount() int {
	s := controllerHealthSnap.Load()
	if s == nil {
		return 0
	}
	return len(s.Webhooks)
}
