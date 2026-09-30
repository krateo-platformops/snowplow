// issue214_seed_prewarm_marker_test.go — #214 arm-3 (enumerate-all-roots
// coverage): the per-user widget seed (seedOneWidget, the confirmed source of
// the 546-lines/12h resourceRef-denial WARN spam) must hand the RESOLVER a ctx
// marked WithPrewarmPath, so resourcesrefs.resolveOne Debug+counts a denial
// instead of WARN-ing. This is the "missed prewarm root" guard: if the choke
// mark ever stops reaching the resolver, this arm goes RED even though arms
// 1/2 (which mark the ctx directly) stay green.
//
// Mirrors TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink: it drives the
// REAL seedOneWidget end-to-end with only the resolver seamed (hermetic — the
// seam receives the exact ctx the resolve runs under), so it observes the
// choke mark's PROPAGATION, which a direct resolveOne test cannot.

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

func TestIssue214_SeedOneWidget_ResolveCtxIsPrewarmMarked(t *testing.T) {
	a1BuildTwoTenantWatcher(t)

	entry := navWidgetEntry{
		W:          h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}),
		GVR:        h1WidgetGVR,
		PerPage:    -1,
		Page:       -1,
		KeyPerPage: -1,
		KeyPage:    -1,
	}

	// A cohort representative that presents the GROUP the shared CRB grants, so
	// the seed derives a live BindingUID and reaches the resolve (else vacuous).
	seedCtx := withCohortSeedContext(context.Background(),
		seedTarget{Username: a1Alice, Groups: []string{a1Group}}, endpoints.Endpoint{}, nil)

	key, handle, inputs := dispatchCacheLookupKey(seedCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(seedCtx, entry.W.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: the seed cohort must derive a live, non-empty-BindingUID widgets key — otherwise "+
			"seedOneWidget short-circuits (#95) and this arm proves nothing; key=%q handle=%v inputs=%+v",
			key, handle != nil, inputs)
	}

	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })

	var resolverRan, prewarmMarked bool
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		resolverRan = true
		prewarmMarked = cache.PrewarmPathFromContext(ctx)
		return h1WidgetUnstructured(map[string]any{}), nil
	}

	if err := seedOneWidget(seedCtx, entry, h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneWidget returned %v; want nil", err)
	}
	if !resolverRan {
		t.Fatal("PRECONDITION: seedOneWidget never reached the resolve — it short-circuited, so this arm is vacuous")
	}
	if !prewarmMarked {
		t.Fatal("#214 arm-3 RED: the widget seed ran the resolve under a ctx WITHOUT WithPrewarmPath — the per-user " +
			"seed's resourceRef denials would WARN-spam (the missed-root failure mode this arm guards).")
	}

	// NEGATIVE CONTROL: PrewarmPathFromContext is not vacuously true — an
	// unmarked (serve-style) context reports false.
	if cache.PrewarmPathFromContext(context.Background()) {
		t.Fatal("control: an unmarked context must NOT report prewarm-path — else the discriminator is vacuous")
	}
}
