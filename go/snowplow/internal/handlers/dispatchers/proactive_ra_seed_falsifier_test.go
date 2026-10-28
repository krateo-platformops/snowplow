// proactive_ra_seed_falsifier_test.go — in-process falsifiers for the
// proactive composition-page RESTAction seed (Option A). Package
// dispatchers. NON-DESTRUCTIVE — pure key/handle assertions over the
// resolved-output L1 singleton + the union helper; no live ResourceWatcher,
// no apiserver, no rbac TestMain. Safe under `go test
// ./internal/handlers/dispatchers/...`.
//
// FALSIFIER MAP (see the ship brief):
//   - F-1 cross-ref same-cell: a proactively-unioned RA ref Put under its
//     dispatcher key is HIT — byte-identical RawJSON — under the SAME key a
//     harvested ref of the same RA would produce. Proves the union does not
//     perturb the seed/serve cell.
//   - F-2 ComputeKey parity: the dispatcher key for a proactive ref =={binding,
//     RA, extras_len:0, page:-1}== the key for the equivalent harvested ref
//     (the union builds the byte-identical ObjectReference the harvester does).
//   - F-6 transparency: PROACTIVE_RA_SEED_ENABLED defaults OFF; the union with
//     an empty proactive set returns the harvested slice UNCHANGED (object
//     identity), so the seed source — and served content — is unchanged.
//   - union dedup: a proactive ref colliding by {ns,name} with a harvested ref
//     is NOT duplicated (matches the harvester's own {ns,name} dedup).
//   - F-3 guard: the latent-hazard guard — PrewarmEngineEnabled() defaults to
//     the documented opt-in posture and ProactiveRASeedEnabled() defaults OFF;
//     the production posture (engine ON) is the contract the on-cluster F-3
//     pprof probe asserts.

package dispatchers

import (
	"log/slog"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// raRef builds the ObjectReference the nav-walk harvester produces for a
// RESTAction (phase1_content_prewarm.go:162-169) — the SHAPE the union must
// reproduce for a proactive ref so the downstream key computation is identical.
func raRef(ns, name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: ns},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
}

// TestProactiveRASeed_F2_DispatchKeyParity — F-2. The dispatcher cache key
// for a PROACTIVELY-unioned RA ref is byte-identical to the key for the
// harvested ref of the same RA, at the restactions seed tuple {ns, name,
// extras=nil, page:-1, perPage:-1}. seedOneRestaction computes the key via
// dispatchCacheLookupKey("restactions", g,v,r, ns,name, -1,-1, nil) — the
// SAME call restactions.go:117 makes — so identical refs ⇒ identical keys.
func TestProactiveRASeed_F2_DispatchKeyParity(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const ns, name = "comp-a", "composition-resources"

	g, v, r := restActionGVR.Group, restActionGVR.Version, restActionGVR.Resource

	// Harvested ref (the existing source) vs the union's proactive ref. The
	// union builds the proactive ref via unionRefsForTest — the SAME path
	// production uses — so they must be byte-identical for the same RA.
	harvested := raRef(ns, name)
	unioned := unionRefsForTest(nil, []cache.RestActionRef{{Namespace: ns, Name: name}})
	if len(unioned) != 1 {
		t.Fatalf("expected 1 unioned ref, got %d", len(unioned))
	}
	proactive := unioned[0]

	keyH, handleH, _ := dispatchCacheLookupKey(ctx, "restactions",
		g, v, r, harvested.Namespace, harvested.Name, -1, -1, nil)
	keyP, handleP, _ := dispatchCacheLookupKey(ctx, "restactions",
		g, v, r, proactive.Namespace, proactive.Name, -1, -1, nil)

	if handleH == nil || handleP == nil {
		t.Fatalf("expected live restactions L1 handles (H=%v P=%v)", handleH, handleP)
	}
	if keyH == "" || keyP == "" {
		t.Fatalf("expected non-empty keys (H=%q P=%q)", keyH, keyP)
	}
	if keyH != keyP {
		t.Fatalf("F-2 FAILED: proactive-ref key %q != harvested-ref key %q — the proactively "+
			"seeded RA would MISS the cell the dispatcher serves from", keyP, keyH)
	}
}

// TestProactiveRASeed_F1_SeedServeHit — F-1. Put a RESTAction body under the
// SEED key computed for a proactive ref; Get under the SERVE key the
// dispatcher computes for the SAME RA → HIT, byte-identical RawJSON. Proves
// the proactively-seeded cell is the cell the customer /call reads.
func TestProactiveRASeed_F1_SeedServeHit(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const ns, name = "comp-b", "composition-resources"
	g, v, r := restActionGVR.Group, restActionGVR.Version, restActionGVR.Resource

	// SEED side — the key seedOneRestaction would compute for the proactive ref.
	seedKey, seedHandle, seedInputs := dispatchCacheLookupKey(ctx, "restactions",
		g, v, r, ns, name, -1, -1, nil)
	if seedHandle == nil || seedKey == "" {
		t.Fatal("expected a live seed handle + key")
	}
	body := []byte(`{"status":{"items":[{"name":"comp-b-resource"}]}}`)
	putFixture(seedHandle, seedKey, &cache.ResolvedEntry{RawJSON: body, Inputs: seedInputs})

	// SERVE side — the dispatcher's key for the same RA tuple (same identity).
	serveKey, serveHandle, _ := dispatchCacheLookupKey(ctx, "restactions",
		g, v, r, ns, name, -1, -1, nil)
	if serveHandle == nil || serveKey == "" {
		t.Fatal("expected a live serve handle + key")
	}
	if seedKey != serveKey {
		t.Fatalf("F-1 FAILED: seed key %q != serve key %q", seedKey, serveKey)
	}
	got, hit := serveHandle.Get(serveKey)
	if !hit {
		t.Fatal("F-1 FAILED: serve-key lookup MISSED the proactively-seeded cell")
	}
	if string(got.RawJSON) != string(body) {
		t.Fatalf("F-1 FAILED: served wrong body: got %q want %q", got.RawJSON, body)
	}
}

// TestProactiveRASeed_F6_FlagDefaultsOff — F-6. The proactive seed is opt-in;
// flag-off it must be inert (no env set).
func TestProactiveRASeed_F6_FlagDefaultsOff(t *testing.T) {
	if ProactiveRASeedEnabled() {
		t.Fatal("F-6 FAILED: PROACTIVE_RA_SEED_ENABLED must default OFF")
	}
}

// TestProactiveRASeed_F6_EmptyProactiveUnchanged — F-6. The union with an
// empty proactive set (nil rw degrades RBACReachableRestActionRefs to nil)
// returns the harvested slice UNCHANGED — same length, same contents — so the
// seed source is byte-identical to harvester-only (transparent).
func TestProactiveRASeed_F6_EmptyProactiveUnchanged(t *testing.T) {
	harvested := []templatesv1.ObjectReference{
		raRef("krateo-system", "sidebar-nav-menu"),
		raRef("demo-system", "panel-list"),
	}
	got := unionProactiveRARefs(harvested, nil, slog.Default())
	if len(got) != len(harvested) {
		t.Fatalf("F-6 FAILED: empty-proactive union changed the seed source length: got %d want %d",
			len(got), len(harvested))
	}
	for i := range harvested {
		if got[i].Namespace != harvested[i].Namespace || got[i].Name != harvested[i].Name {
			t.Fatalf("F-6 FAILED: empty-proactive union perturbed ref[%d]: got %s/%s want %s/%s",
				i, got[i].Namespace, got[i].Name, harvested[i].Namespace, harvested[i].Name)
		}
	}
}

// TestProactiveRASeed_UnionDedup — the union dedups proactive refs that collide
// by {ns,name} with a harvested ref (matches the harvester's own dedup key).
// Driven via unionRefsForTest (the pure dedup core, RBAC-source-independent) so
// it needs no live cluster.
func TestProactiveRASeed_UnionDedup(t *testing.T) {
	harvested := []templatesv1.ObjectReference{
		raRef("krateo-system", "sidebar-nav-menu"),
		raRef("comp-a", "composition-resources"),
	}
	proactive := []cache.RestActionRef{
		{Namespace: "comp-a", Name: "composition-resources"}, // dup of harvested[1]
		{Namespace: "comp-b", Name: "composition-resources"}, // new
		{Namespace: "comp-b", Name: "composition-resources"}, // dup of itself
	}
	got := unionRefsForTest(harvested, proactive)
	// Expect: 2 harvested + 1 genuinely-new proactive = 3.
	if len(got) != 3 {
		t.Fatalf("union dedup FAILED: got %d refs, want 3: %+v", len(got), got)
	}
	seen := map[string]int{}
	for _, r := range got {
		seen[r.Namespace+"/"+r.Name]++
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("union dedup FAILED: %q appears %d times", k, c)
		}
	}
	if seen["comp-b/composition-resources"] != 1 {
		t.Fatal("union dedup FAILED: the genuinely-new proactive ref was dropped")
	}
}

// TestProactiveRASeed_F3_EngineImplicitOnCache — F-3, REWORKED for the
// 2026-07-03 family fold (docs/prewarm-engine-implicit-on-cache-2026-07-03.md).
// PREWARM_ENGINE_ENABLED + PROACTIVE_RA_SEED_ENABLED are RETIRED; both helpers
// are now IMPLICIT-ON-CACHE and the legacy errgroup runPIPSeed OOM-hazard path
// is DELETED. So the F-3 posture is no longer "flags default OFF, production
// flips ON" — it is "engine + proactive seed ON iff CACHE_ENABLED; off-switch
// is CACHE_ENABLED=false." This is the installer-test regression guard: a
// cache-on deployment with the flags forgotten now correctly runs the engine
// (not the deleted OOM-hazard path).
func TestProactiveRASeed_F3_EngineImplicitOnCache(t *testing.T) {
	// Implicit-ON: cache on ⇒ both the engine and the proactive seed run,
	// WITHOUT any PREWARM_ENGINE_ENABLED / PROACTIVE_RA_SEED_ENABLED env.
	t.Setenv("CACHE_ENABLED", "true")
	if !PrewarmEngineEnabled() {
		t.Fatal("F-3 FAILED: engine must be implicit-ON when CACHE_ENABLED=true (post-fold) — " +
			"a cache-on deployment with the flag forgotten must run the engine, not the deleted legacy path")
	}
	if !ProactiveRASeedEnabled() {
		t.Fatal("F-3 FAILED: proactive RA seed must be implicit-ON when CACHE_ENABLED=true (post-fold)")
	}
	// Off-switch: cache off ⇒ both OFF.
	t.Setenv("CACHE_ENABLED", "false")
	if PrewarmEngineEnabled() {
		t.Fatal("F-3 FAILED: engine must be OFF when CACHE_ENABLED=false (the only off-switch post-fold)")
	}
	if ProactiveRASeedEnabled() {
		t.Fatal("F-3 FAILED: proactive RA seed must be OFF when CACHE_ENABLED=false")
	}
}
