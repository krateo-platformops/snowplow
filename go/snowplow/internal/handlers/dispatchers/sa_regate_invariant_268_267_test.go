//go:build falsifier_268_267

// sa_regate_invariant_268_267_test.go — #268/#269 Part 2 drift-guard, invariant
// table (architect's form; TL ship-choice: keep the 3-conjunct MustRegateSADial and
// ENCODE its production-equivalence per producer here).
//
// MustRegateSADial = BackgroundResolve && saCredentialOnContext && !ServesUnnarrowed.
// It is production-equivalent to the design's 2-conjunct (saCred && !ServesUnnarrowed)
// ONLY IF every production SA-transport producer satisfies:
//
//	ServesUnnarrowed(ctx) || BackgroundResolveFromContext(ctx)
//
// This test asserts that invariant for each directly-callable SA-transport builder,
// exercising the REAL builder (no hand-rolled ctx mirror) under its production
// precondition (cache-on, so the nil-safe WithServeWatcher actually attaches). The
// WithInternal* producer census (internal/rbac/sa_producer_census_268_267_test.go)
// is the companion tripwire: it fails if a producer is added/removed/moved, forcing
// a new row here. Census = enforced-in-CI; this table = the verified classification.
//
// TWO producers are covered ELSEWHERE, not re-driven here:
//   - the refresher rctx (resolve_populate.go builds it; resolveOnceProd:496 stamps
//     WithBackgroundResolve): its BackgroundResolve is the LOAD-BEARING case (a REAL
//     per-user representative → !ServesUnnarrowed). It is exercised end-to-end by
//     TestRefresherLeak_Site3_DispatchCR_ServedUnderSA (sa_serve_refresher_leak_268_test.go),
//     which drives the real resolveAndPopulateL1→resolveOnceProd→objects.Get and
//     TRIPS if line 496 is dropped (the denied RA is then served under the SA rc).
//   - cluster_list's populateCtx (async, package api): ServesUnnarrowed via no-UserInfo.

package dispatchers

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	"k8s.io/client-go/rest"
)

// saProducerCtx describes one directly-callable SA-transport ctx-builder.
type saProducerCtx struct {
	name  string
	build func(saEP endpoints.Endpoint, saRC *rest.Config) context.Context
	// unnarrowed records whether this producer is expected to be ServesUnnarrowed
	// (true) or to rely on BackgroundResolve (false). All three callable builders
	// are ServesUnnarrowed; the BackgroundResolve case is the refresher, covered by
	// the step-4 arm (see the file header).
	unnarrowed bool
}

func saProducerBuilders() []saProducerCtx {
	return []saProducerCtx{
		{
			name:       "withPhase1SAContext",
			unnarrowed: true,
			build: func(saEP endpoints.Endpoint, saRC *rest.Config) context.Context {
				return withPhase1SAContext(context.Background(), saEP, saRC)
			},
		},
		{
			name:       "withContentPrewarmSAContext",
			unnarrowed: true,
			build: func(saEP endpoints.Endpoint, saRC *rest.Config) context.Context {
				return withContentPrewarmSAContext(context.Background(), saEP, saRC)
			},
		},
		{
			// The SENSITIVE row: withCohortSeedContext installs a REAL cohort
			// identity (non-SA), so ServeWatcher (cache-on) is its SOLE
			// ServesUnnarrowed mechanism. Dropping it flips the invariant false
			// (see TestCohortSeedContext_ServeWatcherIsSoleUnnarrowedMechanism).
			name:       "withCohortSeedContext",
			unnarrowed: true,
			build: func(saEP endpoints.Endpoint, saRC *rest.Config) context.Context {
				cohort := seedTarget{BindingUID: "uid-cohort", Username: "cohort-rep", Groups: []string{"tenant-a"}}
				return withCohortSeedContext(context.Background(), cohort, saEP, saRC)
			},
		},
	}
}

func saLeakEndpointAndRC(t *testing.T) (endpoints.Endpoint, *rest.Config) {
	t.Helper()
	ep := endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: saLeakToken}
	return ep, saLeakRC(ep.ServerURL)
}

// TestSAProducerContexts_SatisfyRegateInvariant is the core drift-guard: each
// callable SA-transport builder's ctx must satisfy ServesUnnarrowed || BackgroundResolve
// AND actually carry an SA credential (else the assertion is vacuous —
// feedback_arm_that_cannot_fail_is_not_coverage). Run cache-on so the nil-safe
// WithServeWatcher attaches the watcher exactly as in production.
func TestSAProducerContexts_SatisfyRegateInvariant(t *testing.T) {
	// refresherDeniedWatcher (sa_serve_refresher_leak_268_test.go) turns CACHE_ENABLED
	// on and installs a synced watcher as cache.Global(), so WithServeWatcher(ctx,
	// cache.Global()) attaches a non-nil watcher — the production cache-on precondition.
	refresherDeniedWatcher(t)
	if cache.Global() == nil {
		t.Fatalf("precondition: cache.Global() must be non-nil (cache-on) so WithServeWatcher attaches")
	}

	saEP, saRC := saLeakEndpointAndRC(t)

	for _, p := range saProducerBuilders() {
		t.Run(p.name, func(t *testing.T) {
			ctx := p.build(saEP, saRC)

			// Non-vacuity: this MUST be an SA-credentialed context, else the guard
			// never consults it and the invariant assertion proves nothing.
			_, hasEP := cache.InternalEndpointFromContext(ctx)
			_, hasRC := cache.InternalRESTConfigFromContext(ctx)
			if !hasEP && !hasRC {
				t.Fatalf("%s: ctx carries no SA credential (InternalEndpoint/InternalRESTConfig) — not an SA producer; the invariant would be vacuous", p.name)
			}

			serves := rbac.ServesUnnarrowed(ctx)
			bg := cache.BackgroundResolveFromContext(ctx)
			t.Logf("%s: ServesUnnarrowed=%v BackgroundResolve=%v", p.name, serves, bg)

			if !serves && !bg {
				t.Fatalf("DRIFT (#268/#269 Part 2): %s attaches an SA credential but is NEITHER ServesUnnarrowed NOR BackgroundResolve.\n"+
					"  MustRegateSADial would go false for it (BackgroundResolve && saCred && !ServesUnnarrowed), so an SA-served read under a real identity would NOT be re-gated — a leak.\n"+
					"  Restore the ServeWatcher/canonical-SA identity, or set WithBackgroundResolve on this builder's ctx.", p.name)
			}
			if p.unnarrowed && !serves {
				t.Errorf("%s: architect-verified classification is ServesUnnarrowed, but ServesUnnarrowed(ctx)=false (invariant held only via BackgroundResolve=%v). Re-verify the classification and the sa_regate table.", p.name, bg)
			}
		})
	}
}

// TestSAProducerInvariant_Control_UngatedShapeIsCaught proves the invariant
// assertion CAN fail (feedback_arm_that_cannot_fail_is_not_coverage): an SA
// credential on a ctx carrying a REAL non-SA identity, with NO ServeWatcher and NO
// BackgroundResolve, VIOLATES ServesUnnarrowed || BackgroundResolve. Crucially the
// 3-conjunct MustRegateSADial does NOT re-gate this shape (it requires
// BackgroundResolve), so a PRODUCER of it would leak — which is exactly why the
// census + invariant table must PREVENT it from existing. This is a deliberate
// counter-example, NOT a production producer.
func TestSAProducerInvariant_Control_UngatedShapeIsCaught(t *testing.T) {
	saEP, saRC := saLeakEndpointAndRC(t)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "real-user", Groups: []string{"tenant-a"}}),
	)
	ctx = cache.WithInternalEndpoint(ctx, &saEP)
	ctx = cache.WithInternalRESTConfig(ctx, saRC)
	// Intentionally NO WithServeWatcher, NO WithBackgroundResolve, NO
	// WithApistageContentResolve, and a non-SA username.

	serves := rbac.ServesUnnarrowed(ctx)
	bg := cache.BackgroundResolveFromContext(ctx)
	t.Logf("control: ServesUnnarrowed=%v BackgroundResolve=%v (want both false)", serves, bg)

	if serves || bg {
		t.Fatalf("control is not a genuine invariant-violating shape: ServesUnnarrowed=%v BackgroundResolve=%v — both must be false so the positive arms' assertion (ServesUnnarrowed || BackgroundResolve) is provably non-trivial", serves, bg)
	}
	// The 3-conjunct MustRegateSADial does NOT fire for this shape (it requires
	// BackgroundResolve; sa_regate.go deliberately excludes a non-background real-user
	// + SA-credential dispatch as not production-reachable). That is PRECISELY why the
	// drift-guard invariant is load-bearing: a PRODUCER of this shape would be served
	// UN-re-gated (a leak). The census + invariant table guarantee no such producer
	// exists — the guard does not, and is not meant to, catch it at runtime.
	if rbac.MustRegateSADial(ctx) {
		t.Fatalf("unexpected: MustRegateSADial fired for the non-background invariant-violating shape; the 3-conjunct should NOT cover it (that shape must be prevented by the census/table, not caught by the guard)")
	}
}

// TestCohortSeedContext_ServeWatcherIsSoleUnnarrowedMechanism proves ServeWatcher
// is LOAD-BEARING for withCohortSeedContext: cache-off (cache.Global()==nil, so the
// nil-safe WithServeWatcher no-ops), the cohort ctx carries a REAL non-SA identity
// and is therefore NOT ServesUnnarrowed. Production forbids a cache-off cohort seed
// (the seed never runs cache-off — phase1_pip_seed.go), so this is only reachable as
// a control; it is what makes the cache-on cohort row in
// TestSAProducerContexts_SatisfyRegateInvariant sensitive to a dropped ServeWatcher.
func TestCohortSeedContext_ServeWatcherIsSoleUnnarrowedMechanism(t *testing.T) {
	prev := cache.Global()
	cache.SetGlobal(nil)
	t.Cleanup(func() { cache.SetGlobal(prev) })
	if cache.Global() != nil {
		t.Fatalf("precondition: cache.Global() must be nil for this cache-off control")
	}

	saEP, saRC := saLeakEndpointAndRC(t)
	cohort := seedTarget{BindingUID: "uid-cohort", Username: "cohort-rep", Groups: []string{"tenant-a"}}
	ctx := withCohortSeedContext(context.Background(), cohort, saEP, saRC)

	if _, ok := cache.ServeWatcherFromContext(ctx); ok {
		t.Fatalf("cache-off: expected no ServeWatcher on the cohort ctx (WithServeWatcher(nil) should no-op)")
	}
	if rbac.ServesUnnarrowed(ctx) {
		t.Fatalf("cache-off cohort ctx unexpectedly ServesUnnarrowed — the cache-on row would not be sensitive to a dropped ServeWatcher")
	}
	t.Logf("cache-off cohort ctx: ServesUnnarrowed=false (ServeWatcher is the sole unnarrowed mechanism, as expected)")
}
