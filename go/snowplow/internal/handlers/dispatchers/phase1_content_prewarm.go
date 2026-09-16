// phase1_content_prewarm.go — Ship 0.30.125 (F2): the SA-driven
// content-population pass.
//
// THE NORTH-STAR SHIP. F1 (0.30.119) built the identity-free content L1;
// 0.30.123/124 made an SA-credentialed nested /call resolve correctly.
// F2 adds a new Step 7.5 to Phase-1 startup: after WaitAllInformersSynced
// and BEFORE MarkPhase1Done — i.e. behind the 503 readiness gate, so the
// pod goes Ready only once the content cache is warm — it resolves the
// data-source RESTAction set under the snowplow ServiceAccount and
// populates F1's content layer. The first real /call by any never-seen
// user is then a content_hit with zero resolve — cold navigations vanish.
//
// THE HARVEST (7.5a): the data-source RESTAction set is each widget's
// spec.apiRef. It is harvested DURING the existing discovery walk (the
// phase1Walker records every resolved widget's apiRef into a shared
// contentPrewarmHarvester) — NO second traversal. The set is small (one
// apiRef per navigation page / datagrid, tens total), derived purely
// from the resolved navigation — no hardcoded resource list.
//
// THE POPULATION RESOLVE (7.5b): for each harvested RESTAction a
// dedicated resolve (NOT through the widget walker) — objects.Get →
// FromUnstructured → restactions.Resolve with PerPage:-1/Page:-1 (F1's
// content key is (gvr,ns,name), pagination-free, so a full resolve warms
// exactly the entries any-perPage /call hits). The resolve runs under
// withContentPrewarmSAContext.
//
// OOM MITIGATIONS (load-bearing — uncapping the iterator does the full
// per-namespace fan-out, #159 territory). There are exactly TWO; both
// bound the TRANSIENT (the real #159 risk). Resident cost is bounded
// separately by F1's content-L1 LRU maxBytes.
//   1. SERIAL content pass — the 7.5b loop resolves harvested RESTActions
//      one at a time (sequential for, no errgroup). Behind the 503 gate:
//      no latency budget, trade wall-clock for peak RSS.
//   2. SERIAL inner-call fan-out — withContentPrewarmSAContext sets
//      cache.WithPrewarmIterSerial so iterParallelism returns 1.
//
// PREWARM_CONTENT_MAX_BYTES is NOT an OOM mitigation and NOT a circuit-
// breaker. By the time a resolved envelope's size is known, F1's
// apistageContentServe has ALREADY populated every per-K8s-call content
// entry — there is nothing left to skip, and skipping the Put would be a
// correctness regression (a cold miss that defeats F2). It is purely an
// OBSERVABILITY SIGNAL: an oversize resolved envelope emits a WARN
// (content.prewarm.envelope_oversize) so an operator can tune
// PREWARM_CONTENT_MAX_BYTES against the real data-source sizes. Size-
// keyed, no resource literal.
//
// Gated by PREWARM_CONTENT_ENABLED (chart default false — the production
// default is FAL-4's call). Flag-off Step 7.5 is a no-op and startup is
// byte-identical to 0.30.124.

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/apis"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const (
	// envPrewarmContentMaxBytes is the per-envelope OBSERVABILITY
	// threshold (bytes) — NOT a circuit-breaker, NOT an OOM mitigation. A
	// prewarmed RESTAction whose resolved envelope exceeds it emits a WARN
	// so an operator can tune the value against real data-source sizes.
	// The content entries were already populated per-K8s-call during the
	// resolve; nothing is skipped.
	envPrewarmContentMaxBytes = "PREWARM_CONTENT_MAX_BYTES"

	// defaultPrewarmContentMaxBytes — ~32 MiB. compositions-list's
	// resolved envelope at 50K scale is ~26 MB, so 32 MiB admits the
	// real headline data source while still tripping on a pathological
	// over-large one.
	defaultPrewarmContentMaxBytes = 32 * 1024 * 1024

	// envPrewarmPageLimit (Ship 0.30.127) is the bounded per-widget
	// pagination DEFAULT the Phase-1 discovery walk resolves a widget
	// under WHEN that widget's `/call` Path carries no explicit
	// page/perPage `slice`. A widget that DOES declare a slice overrides
	// it. It is never -1 — -1 is the unbounded value that, with the
	// iterator cap removed, drives the 49K-row resolution storm.
	envPrewarmPageLimit = "PREWARM_PAGE_LIMIT"

	// defaultPrewarmPageLimit — a small bounded default. The discovery
	// walk only needs to traverse navigation STRUCTURE + discover
	// informers (GVR-scoped); a handful of resolved rows per widget
	// exercises the apiRef chain as well as the full LIST would.
	defaultPrewarmPageLimit = 5
)

// PrewarmContentEnabled reports whether the F2 Step-7.5 content pass runs.
// FOLDED 2026-07-03 (docs/prewarm-engine-implicit-on-cache-2026-07-03.md): the
// standalone PREWARM_CONTENT_ENABLED env read is RETIRED (registered in
// cache.retiredFlags). It is now IMPLICIT-ON-CACHE — the content pass runs
// whenever prewarm runs, mirroring #57. (The sizing knobs PREWARM_CONTENT_MAX_BYTES
// / PREWARM_PAGE_LIMIT are NOT folded — they are observability/pagination
// params, not on/off gates.)
func PrewarmContentEnabled() bool {
	return cache.PrewarmEnabled() // implicit-on-cache (#57); was env "PREWARM_CONTENT_ENABLED"=="true"
}

// prewarmContentMaxBytes returns the per-envelope oversize-WARN
// observability threshold, defaulting to defaultPrewarmContentMaxBytes.
func prewarmContentMaxBytes() int {
	return env.Int(envPrewarmContentMaxBytes, defaultPrewarmContentMaxBytes)
}

// prewarmPageLimit returns the bounded per-widget pagination default the
// Phase-1 discovery walk uses for a widget whose `/call` Path declares no
// explicit page/perPage slice (Ship 0.30.127). A non-positive env value
// falls back to defaultPrewarmPageLimit — the walk is NEVER unbounded.
func prewarmPageLimit() int {
	n := env.Int(envPrewarmPageLimit, defaultPrewarmPageLimit)
	if n <= 0 {
		return defaultPrewarmPageLimit
	}
	return n
}

// contentPrewarmHarvester accumulates the deduplicated data-source
// RESTAction set harvested during the Phase-1 discovery walk (Step 7.5a).
// The phase1Walker writes into it as it resolves each widget; the Step
// 7.5b content pass drains it. Concurrency: the walk is single-threaded
// per root and roots resolve sequentially, but the mutex makes the
// harvester safe regardless of how the walk is scheduled.
type contentPrewarmHarvester struct {
	mu   sync.Mutex
	refs map[string]templatesv1.ObjectReference
}

// newContentPrewarmHarvester returns an empty harvester.
func newContentPrewarmHarvester() *contentPrewarmHarvester {
	return &contentPrewarmHarvester{refs: map[string]templatesv1.ObjectReference{}}
}

// harvestApiRef records a widget's spec.apiRef as a data-source
// RESTAction reference. Nil-safe (a nil harvester / nil widget is a
// no-op — flag-off Phase 1 passes no harvester). Deduplicated by
// (namespace,name): the same RESTAction reached from multiple widgets is
// resolved once.
func (h *contentPrewarmHarvester) harvestApiRef(w *unstructured.Unstructured) {
	if h == nil || w == nil {
		return
	}
	name, ns, ok := readApiRef(w)
	if !ok {
		return
	}
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: name, Namespace: ns},
		// apiRef always targets a RESTAction — the GVR is fixed
		// (restActionGVR, deps_extract.go). Render the apiVersion the
		// objects.Get fetch expects.
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	h.mu.Lock()
	h.refs[ns+"/"+name] = ref
	h.mu.Unlock()
}

// forgetCoordinate drops the harvested apiRef for (gvr, ns, name). 1.12.7 F6b
// — the ONLY removal path on this harvester, driven exclusively by an
// authoritative GONE verdict from the dep tracker
// (cache.RegisterGoneForgetHook). Returns the number of entries dropped.
//
// SCOPED TO RESTActions. Every ref this harvester holds targets a RESTAction —
// harvestApiRef stamps restActionGVR unconditionally — so a verdict for any
// other kind cannot match and is declined here rather than being allowed to
// delete a same-named ref of a different kind.
func (h *contentPrewarmHarvester) forgetCoordinate(gvr schema.GroupVersionResource, ns, name string) int {
	if h == nil || name == "" || gvr != restActionGVR {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	k := ns + "/" + name
	if _, ok := h.refs[k]; !ok {
		return 0
	}
	delete(h.refs, k)
	return 1
}

// snapshot returns a copy of the harvested reference set, stable for the
// content pass to iterate.
func (h *contentPrewarmHarvester) snapshot() []templatesv1.ObjectReference {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]templatesv1.ObjectReference, 0, len(h.refs))
	for _, ref := range h.refs {
		out = append(out, ref)
	}
	return out
}

// withContentPrewarmSAContext builds the context the F2 content pass
// resolves under. It is a clone of withPhase1SAContext (SA identity,
// canonical system:serviceaccount: username, the SA-transport seam:
// WithUserConfig / WithInternalEndpoint / WithInternalRESTConfig) plus
// the content-pass markers:
//
//   - cache.WithApistagePrewarm — so apistageContentServe populates the
//     identity-free content L1 and skips the per-user gate (there is no
//     requester; the resolved dict is discarded, the warmed content
//     entry is the whole point).
//   - cache.WithPrewarmIterSerial — so iterParallelism returns 1 (OOM
//     mitigation 2 / Fork B's serial side): the content pass materializes
//     the 49K-row JSON and is the genuine OOM risk, so its inner-call
//     fan-out runs serially. Behind the 503 readiness gate there is no
//     latency budget to protect.
//
// The `dependsOn.iterator` runs FULLY for every resolve since Ship
// 0.30.127 deleted phase1IteratorCap — the content pass warms the WHOLE
// data set; the discovery walk relies on the resolver's default bounded
// errgroup as its storm guard (Fork B — see withPhase1SAContext).
func withContentPrewarmSAContext(ctx context.Context, saEP endpoints.Endpoint, saRC *rest.Config) context.Context {
	opts := []xcontext.WithContextFunc{
		xcontext.WithUserConfig(saEP),
		xcontext.WithLogger(slog.Default()),
	}
	if u, ok := phase1SAUsername(saEP.Token); ok {
		opts = append(opts, xcontext.WithUserInfo(jwtutil.UserInfo{Username: u}))
	}
	rctx := xcontext.BuildContext(ctx, opts...)
	rctx = cache.WithInternalEndpoint(rctx, &saEP)
	rctx = cache.WithInternalRESTConfig(rctx, saRC)
	rctx = cache.WithApistagePrewarm(rctx)   // populate content L1, skip the per-user gate
	rctx = cache.WithPrewarmIterSerial(rctx) // OOM mitigation 2 / Fork B serial — serial inner-call fan-out
	// #130 F2 — attach the shared watcher so the content pass's inner LIST calls
	// (dispatchViaInternalRESTConfig, resolve.go:849) serve a servable GVR's LIST
	// from the synced informer indexer instead of a live ~23s/60K paged apiserver
	// LIST. BYTE-IDENTICAL to the discovery walk's withPhase1SAContext
	// (phase1_walk.go:1029): without it the internal-dispatch informer-serve branch
	// (internal_dispatch.go:380, `ServeWatcherFromContext(ctx); haveRW`) is never
	// entered, so the content-prewarm benchapps LIST always took the live paged
	// LIST (the 3× residual traced in boot-1.7.5-t0.log). ctx is rebuilt from
	// scratch above (xcontext.BuildContext), so the watcher cannot be inherited
	// from the caller — it MUST be attached here. PREWARM-SCOPED: only this SA
	// content context carries it; per-user /call never does, so the customer
	// dispatch path is byte-identical. nil-safe (cache.Global() may be nil under
	// CACHE_ENABLED=false → WithServeWatcher returns ctx unchanged → the live LIST
	// runs as today).
	rctx = cache.WithServeWatcher(rctx, cache.Global())
	// Boot-readiness external-fetch wall-clock bound (external_seed_bound.go /
	// resolve.go:1106) — stamp the SAME boot-prewarm-walk scope the discovery
	// walk carries (withPhase1SAContext, phase1_walk.go:1084). The content pass
	// resolves the obs-* apiRef data sources (external ClickHouse widgets)
	// SERIALLY BEFORE MarkPhase1Done, so without a scope marker the
	// external-fetch bound would no-op on this vector and it could overrun the
	// 480s readiness budget alone. Reusing the walk scope (rather than a new
	// constant) is behavior-neutral for the content pass: the ONLY behavioral
	// consumer of ScopeBootPrewarmWalk is apiref.shouldServeRAFullList
	// (widgets/apiref/resolve.go:89), which reads the scope ONLY on a PAGINATED
	// resolve (perPage>0 && page>0). The content pass resolves at PerPage:-1/
	// Page:-1 (prewarmOneRESTAction, :394), and that -1/-1 pagination propagates
	// VERBATIM down the nested-widget path it CAN reach shouldServeRAFullList
	// through (restactions.Resolve → maybeResolveInProcess resolve_inprocess.go:174
	// → ResolveNestedCall nested_call.go:251 → widgets.Resolve widgets/resolve.go:253
	// → apiref.Resolve), so every resolve arrives as shouldServeRAFullList(ctx,-1,-1)
	// → IsPaginatedResolve(-1,-1)==false → short-circuits at the FIRST conjunct
	// BEFORE the scope is ever read. The walk-scope stamp therefore cannot alter
	// any content-pass behavior other than engaging the external-fetch bound it
	// is added for. (One benign non-behavioral side effect: the content pass now
	// records apiserver-fallthrough metrics under the boot-prewarm-walk label —
	// RecordApiserverFallthrough is "NO BEHAVIOUR CHANGE", a metrics-attribution
	// shift onto a label the discovery walk already emits.) Structural (an
	// existing scope constant, no host/name literal — feedback_no_special_cases).
	// nil-safe / inert under cache-off (WithFallthroughScope returns rctx
	// unchanged when Disabled()).
	rctx = cache.WithFallthroughScope(rctx, cache.ScopeBootPrewarmWalk)
	return rctx
}

// runContentPrewarmPass is Step 7.5b: the serial SA content-population
// resolve over the harvested data-source RESTAction set. Called by
// phase1WarmupWith between Step 7 (WaitAllInformersSynced) and Step 8
// (MarkPhase1Done) when PREWARM_CONTENT_ENABLED is set.
//
// It resolves each harvested RESTAction ONE AT A TIME (OOM mitigation 1
// — sequential, no errgroup): behind the 503 gate there is no latency
// budget, so it trades wall-clock for peak RSS. A single resolve failure
// is logged and the pass continues — a broken data source must not block
// warming the rest, and a missed entry is simply lazy-warmed on first
// request.
func runContentPrewarmPass(ctx context.Context, h *contentPrewarmHarvester,
	saEP endpoints.Endpoint, saRC *rest.Config, authnNS string) {

	log := slog.Default()
	refs := h.snapshot()
	if len(refs) == 0 {
		log.Info("content.prewarm.skipped",
			slog.String("subsystem", "cache"),
			slog.String("reason", "no apiRef data-source RESTActions harvested from the navigation walk"),
		)
		return
	}

	start := time.Now()
	maxBytes := prewarmContentMaxBytes()
	rctx := withContentPrewarmSAContext(ctx, saEP, saRC)

	// warmed counts data sources whose content entries were populated
	// (oversize ones are still warm — see the oversize branch). oversize
	// counts how many of those tripped the observability WARN. failed
	// counts resolve failures (those alone leave a content entry cold).
	var warmed, oversize, failed int
	log.Info("content.prewarm.started",
		slog.String("subsystem", "cache"),
		slog.Int("data_sources", len(refs)),
		slog.Int("max_bytes", maxBytes),
	)

	for _, ref := range refs {
		if ctx.Err() != nil {
			// Phase-1 budget exhausted — stop the pass; whatever is left
			// is lazy-warmed on first request.
			log.Warn("content.prewarm.ctx_cancelled",
				slog.String("subsystem", "cache"),
				slog.Int("warmed", warmed),
				slog.Int("remaining", len(refs)-warmed-failed),
			)
			break
		}
		envelopeBytes, err := prewarmOneRESTAction(rctx, ref, authnNS)
		if err != nil {
			failed++
			log.Warn("content.prewarm.resolve_failed",
				slog.String("subsystem", "cache"),
				slog.String("restaction", ref.Namespace+"/"+ref.Name),
				slog.Any("err", err),
				slog.String("effect", "content entry stays cold — lazy-warmed on first request"),
			)
			continue
		}
		if envelopeBytes > maxBytes {
			// OVERSIZE-DATA-SOURCE OBSERVABILITY SIGNAL — NOT an OOM
			// mitigation, NOT a circuit-breaker. By the time the resolved
			// envelope's size is known, F1's apistageContentServe has
			// ALREADY populated every per-K8s-call content entry during
			// the resolve. There is nothing to skip — and skipping the Put
			// would be a correctness regression (a cold miss that defeats
			// F2). This branch only WARNs so an operator can tune
			// PREWARM_CONTENT_MAX_BYTES against the real data-source
			// sizes. The entry IS warm; the pass continues normally.
			oversize++
			log.Warn("content.prewarm.envelope_oversize",
				slog.String("subsystem", "cache"),
				slog.String("restaction", ref.Namespace+"/"+ref.Name),
				slog.Int("envelope_bytes", envelopeBytes),
				slog.Int("max_bytes", maxBytes),
				slog.String("note", "oversize data source — review PREWARM_CONTENT_MAX_BYTES; "+
					"the content entry IS warm (not skipped)"),
			)
			// NOTE — no `continue`: the entry is warm, count it as warmed.
		}
		warmed++
	}

	log.Info("content.prewarm.completed",
		slog.String("subsystem", "cache"),
		slog.Int("data_sources", len(refs)),
		slog.Int("warmed", warmed),
		slog.Int("oversize_warned", oversize),
		slog.Int("failed", failed),
		slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
	)
}

// prewarmOneRESTAction resolves ONE data-source RESTAction under the
// content-prewarm SA context. The resolve's side effect — F1's
// apistageContentServe populating the identity-free content L1 for each
// inner K8s call — is the whole point; the resolved envelope size is
// returned only so the caller can emit the oversize-data-source WARN.
//
// rctx already carries withContentPrewarmSAContext (SA identity, SA
// transport, WithApistagePrewarm, WithPrewarmIterSerial).
//
// PerPage:-1 is DELIBERATE here — and, unlike the discovery walk, NOT a
// bug. The content cache keys K8s calls (gvr,ns,name); the warmed
// ENTRIES are slice-invariant. PerPage only narrows the K8s-call SET
// when a stage iterator is gated on dict["slice"], which NO current
// data-source RESTAction does (compositions-panels iterates namespaces;
// compositions-list has no slice). Resolving at -1 warms the COMPLETE
// first-paint-reachable content set; a bounded PerPage here would warm
// fewer entries and re-introduce cold misses. (The discovery walk is the
// opposite case — it must honour the declared per-widget slice; see
// phase1_walk.go.)
func prewarmOneRESTAction(rctx context.Context, ref templatesv1.ObjectReference, authnNS string) (int, error) {
	got := objects.Get(rctx, ref)
	if got.Err != nil {
		return 0, fmt.Errorf("fetch RESTAction %s/%s: %s", ref.Namespace, ref.Name, got.Err.Message)
	}
	if got.Unstructured == nil {
		return 0, fmt.Errorf("fetch RESTAction %s/%s: nil object", ref.Namespace, ref.Name)
	}

	scheme := runtime.NewScheme()
	if err := apis.AddToScheme(scheme); err != nil {
		return 0, fmt.Errorf("add apis to scheme: %w", err)
	}
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		got.Unstructured.Object, &cr); err != nil {
		return 0, fmt.Errorf("unstructured -> RESTAction %s/%s: %w", ref.Namespace, ref.Name, err)
	}

	// Thread the content L1 key so each inner K8s call's content entry
	// records its dep edge (the content path's dep recording — F1 wires
	// it; this keeps it wired for the prewarm resolve too).
	keyCtx := cache.WithL1KeyContext(rctx,
		cache.ComputeKey(cache.ResolvedKeyInputs{
			CacheEntryClass: "restactions",
			Group:           restActionGVR.Group,
			Version:         restActionGVR.Version,
			Resource:        restActionGVR.Resource,
			Namespace:       ref.Namespace,
			Name:            ref.Name,
		}))

	res, err := restactions.Resolve(keyCtx, restactions.ResolveOptions{
		In: &cr,
		// Ship 0.30.230 fix-at-root: SArc threaded from ctx — the
		// content prewarm runs under withContentPrewarmSAContext which
		// installs the SA rc via cache.WithInternalRESTConfig upstream.
		SArc:    rcFromCtx(keyCtx),
		AuthnNS: authnNS,
		PerPage: -1,
		Page:    -1,
	})
	if err != nil {
		return 0, fmt.Errorf("resolve RESTAction %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	// Size of the resolved envelope's status — the oversize-WARN signal.
	if res != nil && res.Status != nil {
		return len(res.Status.Raw), nil
	}
	return 0, nil
}
