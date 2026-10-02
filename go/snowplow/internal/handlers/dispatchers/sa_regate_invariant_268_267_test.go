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
	// (true) or to rely on BackgroundResolve (false). The Phase-1 walk and the
	// content prewarm are ServesUnnarrowed (snowplow SA identity or none); the
	// cohort seed (#425) and the refresher rely on BackgroundResolve.
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
			// identity (non-SA), so it is NOT ServesUnnarrowed even with its
			// ServeWatcher (#425 — that exemption served SA-fetched data under the
			// cohort key). Its own WithBackgroundResolve stamp is what keeps the
			// invariant (see TestCohortSeedContext_IsNarrowedAndRegated).
			name:       "withCohortSeedContext",
			unnarrowed: false,
			build: func(saEP endpoints.Endpoint, saRC *rest.Config) context.Context {
				cohort := seedTarget{BindingUID: "uid-cohort", Username: "cohort-rep", Groups: []string{"tenant-a"}}
				return withCohortSeedContext(context.Background(), cohort, saEP, saRC)
			},
		},
	}
}

// invSAEndpointAndRC is the production SA credential shape: the projected SA token
// (a JWT whose `sub` is the snowplow SA username) on both the endpoint and the
// *rest.Config. Default tags (#428 C3): CI must enforce these production guards.
func invSAEndpointAndRC(t *testing.T) (endpoints.Endpoint, *rest.Config) {
	t.Helper()
	tok := s425SAToken()
	ep := endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: tok}
	return ep, &rest.Config{Host: ep.ServerURL, BearerToken: tok}
}

// invCacheOn installs a synced cache-on watcher as cache.Global() so the nil-safe
// WithServeWatcher attaches, exactly as in production.
func invCacheOn(t *testing.T) {
	t.Helper()
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: true})
}

// TestSAProducerContexts_SatisfyRegateInvariant is the core drift-guard: each
// callable SA-transport builder's ctx must satisfy ServesUnnarrowed || BackgroundResolve
// AND actually carry an SA credential (else the assertion is vacuous —
// feedback_arm_that_cannot_fail_is_not_coverage). Run cache-on so the nil-safe
// WithServeWatcher attaches the watcher exactly as in production.
func TestSAProducerContexts_SatisfyRegateInvariant(t *testing.T) {
	// invCacheOn installs a synced watcher as cache.Global(), so WithServeWatcher(ctx,
	// cache.Global()) attaches a non-nil watcher — the production cache-on precondition.
	invCacheOn(t)
	if cache.Global() == nil {
		t.Fatalf("precondition: cache.Global() must be non-nil (cache-on) so WithServeWatcher attaches")
	}

	saEP, saRC := invSAEndpointAndRC(t)

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
			if !p.unnarrowed && serves {
				t.Errorf("%s: a REAL narrowing identity is classified ServesUnnarrowed — SA-fetched reads under it are served un-gated (#425).", p.name)
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
	saEP, saRC := invSAEndpointAndRC(t)

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

// TestCohortSeedContext_IsNarrowedAndRegated (#425) proves the cohort seed ctx —
// cache-on, so it DOES carry a ServeWatcher — is a narrowing subject: it is NOT
// ServesUnnarrowed (the ServeWatcher exempts only snowplow's own SA identity), and
// rbac.MustRegateSADial fires for it from the builder alone (its own
// WithBackgroundResolve stamp), so objects.getFromAPIServer and branch E re-gate
// the seed's SA-credentialed dials as well as branch C. RED if the ServeWatcher
// clause is reverted to "any ServeWatcher" or the stamp is dropped.
func TestCohortSeedContext_IsNarrowedAndRegated(t *testing.T) {
	invCacheOn(t)
	if cache.Global() == nil {
		t.Fatalf("precondition: cache.Global() must be non-nil (cache-on) so WithServeWatcher attaches")
	}
	saEP, saRC := invSAEndpointAndRC(t)
	cohort := seedTarget{BindingUID: "uid-cohort", Username: "cohort-rep", Groups: []string{"tenant-a"}}
	ctx := withCohortSeedContext(context.Background(), cohort, saEP, saRC)

	if _, ok := cache.ServeWatcherFromContext(ctx); !ok {
		t.Fatalf("precondition: the cache-on cohort ctx must carry a ServeWatcher (else this arm cannot see the #425 clause)")
	}
	if rbac.ServesUnnarrowed(ctx) {
		t.Fatalf("#425: the cohort seed ctx (REAL cohort identity) is ServesUnnarrowed — branch C would serve SA-fetched data un-gated and the seed would cache it under the cohort key")
	}
	if !rbac.MustRegateSADial(ctx) {
		t.Fatalf("the cohort seed ctx is not re-gated at the SA dial sites — withCohortSeedContext must stamp WithBackgroundResolve")
	}
}
