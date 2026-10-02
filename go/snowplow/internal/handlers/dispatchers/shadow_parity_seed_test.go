// shadow_parity_seed_test.go — #275 Step 2E falsifiers for the POPULATE-side
// (boot-seed) shadow-parity hook.
//
// #266 installs the shadow context only at the two SERVE entries, so the boot
// seed resolves are off-hook — a boot walk fires ZERO checks for the COHORT
// (populator) identities that KEY L1 cells (the v7-key #254 leak vector). 2E
// installs the same DARK context on the seed resolve ctx (which already carries
// the cohort identity via withCohortSeedContext) and classifies each completed
// resolve into one of four buckets, with the sum-integrity invariant
// b1+b2+b3+b4 == populate_seed_installs_total as the false-green guard.
//
// These drive the REAL seedOneRestaction / seedOneWidget primitives (fetch →
// convert → gates → tail) against the a1 two-tenant fake cluster, with only the
// resolve tail seamed — exactly as the a1 UAF arms do. The seam fires the same
// per-object EvaluateRBAC calls a real UAF refilter/nested call makes, under the
// shadow-bearing resolve ctx, so the dark hook observes the cohort's verdicts.
//
// Control matrix:
//   F-2E-LOADBEARING — the real seed INSTALLS on every target (installs==#targets)
//                      and the boot walk moves checks_allow 0->>0; RED with the
//                      seed-site wiring reverted (installs stays 0).
//   F-2E-IDENTITY    — a cohort DENIED on the target classifies rbac_denied (b2) +
//                      checks_deny, NOT allow-bearing; sc.identity=={cohort}. The
//                      anti-leak linchpin: the SA transport identity is not measured.
//   F-2E-CALIBRATION — one drive into each of the four buckets; each lands in
//                      exactly its bucket AND b1+b2+b3+b4==installs.
//   F-2E-DARK        — seed shadow-ON vs OFF: byte-identical Put body + identical key.
//   F-2E-PANIC       — a recovered install derive-panic bumps dark_panic, is NOT
//                      counted in installs or any bucket (INVARIANT b), and the seed
//                      still COMPLETES + Puts.
//   F-2E-R2          — the F2 content-prewarm classes are identity-free, so skipping
//                      the populate hook there leaves no per-identity leak unmeasured.

package dispatchers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ───────────────────────────── seed-2E harness ─────────────────────────────

// seed2ECohortCtx builds the boot-seed cohort ctx for a portal-GROUP member —
// the group the shared a1 CRB grants get restactions/panels to, so
// dispatchCacheLookupKey derives a NON-EMPTY BindingUID and the seed proceeds past
// the #95 empty-binding guard (the username-only seedCohortCtx short-circuits
// before ever reaching the install).
func seed2ECohortCtx(user string) context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: user, Groups: []string{a1Group}}, endpoints.Endpoint{}, nil)
}

func resetShadowPopulateCounters() {
	shadowPopulateSeedInstallsTotal.Store(0)
	shadowPopulateAllowBearingTotal.Store(0)
	shadowPopulateRBACDeniedTotal.Store(0)
	shadowPopulateRBACGatedNoCheckTotal.Store(0)
	shadowPopulatePassthroughTotal.Store(0)
}

// reset2E zeroes both the serve counters (shared resetShadowCounters) and the
// five 2E populate counters.
func reset2E() {
	resetShadowCounters()
	resetShadowPopulateCounters()
}

// seed2EFetchedRA is the objects.Result seedObjectsGetFn hands back for a NON-UAF
// RESTAction. Non-UAF ⇒ it reaches the resolve+Put tail (where 2E installs).
//   - dGated=true  → one concrete apiserver read step (get /api/v1/namespaces),
//     so DeriveRESTActionAccessDomain yields ≥1 class ⇒ dGated.
//   - dGated=false → a wholly-templated (external/opaque) path ⇒ classifyStepPath
//     !apiserver ⇒ NO class ⇒ classify lands in populate_passthrough (b4).
//
// `name` keys a distinct cell per drive so seedSkipDecision never skips a repeat.
func seed2EFetchedRA(name string, dGated bool) objects.Result {
	step := map[string]any{"name": "list-namespaces", "path": "/api/v1/namespaces"}
	if !dGated {
		step = map[string]any{"name": "opaque", "path": "${.getpath}"}
	}
	return objects.Result{
		GVR: h1RAGVR,
		Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": name, "namespace": h1NS},
			"spec":       map[string]any{"api": []any{step}},
		}},
	}
}

func seed2ERef(name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
		Resource:   h1RAGVR.Resource,
	}
}

// drive2ERestaction seeds ONE non-UAF RA under `cohort`, with the resolve+Put tail
// seamed to fire one get-configmaps EvaluateRBAC call per ns in `nsChecks`, under
// the SHADOW-BEARING resolve ctx (resCtx) — the shape a real UAF refilter / nested
// call makes. It returns the *shadowContext the resolve ran under (nil if none was
// installed), so a caller can assert the measured identity.
func drive2ERestaction(t *testing.T, cohort context.Context, name string, dGated bool, nsChecks []string) *shadowContext {
	t.Helper()
	origPut := seedRestactionResolveAndPutFn
	origGet := seedObjectsGetFn
	t.Cleanup(func() {
		seedRestactionResolveAndPutFn = origPut
		seedObjectsGetFn = origGet
	})

	seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return seed2EFetchedRA(name, dGated)
	}
	var seen *shadowContext
	seedRestactionResolveAndPutFn = func(
		_, resCtx context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, _ string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		if sc, ok := resCtx.Value(shadowCtxKey).(*shadowContext); ok {
			seen = sc
		}
		ui, _ := xcontext.UserInfo(resCtx)
		for _, ns := range nsChecks {
			if _, _, err := rbac.EvaluateRBAC(resCtx, rbac.EvaluateOptions{
				Username: ui.Username, Groups: ui.Groups,
				Verb: "get", Group: "", Resource: "configmaps", Namespace: ns,
			}); err != nil {
				t.Fatalf("seam EvaluateRBAC(%s, %s): %v", ui.Username, ns, err)
			}
		}
		return nil
	}
	if err := seedOneRestaction(cohort, "cohort-2e", seed2ERef(name), h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction(%s): %v", name, err)
	}
	return seen
}

func seed2EWidgetEntry() navWidgetEntry {
	return navWidgetEntry{
		W:          h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}),
		GVR:        h1WidgetGVR,
		PerPage:    -1,
		Page:       -1,
		KeyPerPage: -1,
		KeyPage:    -1,
	}
}

// ───────────────────────── F-2E-LOADBEARING ─────────────────────────

func TestF2E_LoadBearing_RealSeedInstallsAndChecksAllowMoves(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.ResetRequesterProfileMemoForTest()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	// --- toggle OFF: the seed runs the SAME real path, but 2E is fully dark. ---
	rbac.SetShadowParityEnabled(false)
	reset2E()
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "lb-off", true, []string{a1TenantA})
	if in, ca := shadowPopulateSeedInstallsTotal.Load(), shadowChecksAllowTotal.Load(); in != 0 || ca != 0 {
		t.Fatalf("toggle-off: 2E must be fully dark (installs=%d checks_allow=%d, want 0/0) — deploying 2E changes nothing until the toggle flips", in, ca)
	}

	// --- toggle ON: the REAL seedOneRestaction installs on EVERY target and the
	//     boot walk moves checks_allow 0 -> >0 (RED with the seed-site wiring gone). ---
	rbac.SetShadowParityEnabled(true)
	reset2E()
	const targets = 3
	for i := 0; i < targets; i++ {
		drive2ERestaction(t, seed2ECohortCtx(a1Alice), "lb-on-"+strconv.Itoa(i), true, []string{a1TenantA})
	}
	if got := shadowPopulateSeedInstallsTotal.Load(); got != targets {
		t.Fatalf("RED (populate off-hook): the real seed never installs the shadow context. installs=%d, want %d", got, targets)
	}
	if shadowChecksAllowTotal.Load() == 0 {
		t.Fatal("RED (populate off-hook): checks_allow_total stayed 0 — the boot walk fired no allow-bearing cohort check")
	}
}

// ───────────────────────── F-2E-IDENTITY (anti-leak linchpin) ─────────────────────────

func TestF2E_Identity_CohortDeniedClassifiesDeniedNotSA(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	rbac.ResetRequesterProfileMemoForTest()
	reset2E()

	// carol is a portal-GROUP member (so she holds the dispatch grant → non-empty
	// BindingUID → the seed proceeds) but has NO configmap RoleBinding in either
	// tenant, so every per-object check DENIES for her.
	const denied = "carol"
	allowBefore, denyBefore := shadowChecksAllowTotal.Load(), shadowChecksDenyTotal.Load()

	sc := drive2ERestaction(t, seed2ECohortCtx(denied), "identity-denied", true, []string{a1TenantA, a1TenantB})
	if sc == nil {
		t.Fatal("precondition: the seed resolve carried NO shadow context (nothing installed) — the arm proves nothing")
	}

	if got := shadowPopulateRBACDeniedTotal.Load(); got != 1 {
		t.Fatalf("a cohort DENIED on the target must classify populate_rbac_denied (b2); got %d", got)
	}
	if got := shadowPopulateAllowBearingTotal.Load(); got != 0 {
		t.Fatalf("a denied cohort must NOT classify allow-bearing (b1) — reading the permissive SA identity instead of the cohort is exactly the leak that would; got %d", got)
	}
	if got := shadowChecksAllowTotal.Load() - allowBefore; got != 0 {
		t.Fatalf("a denied cohort's checks must all be DENY; checks_allow moved by %d", got)
	}
	if got := shadowChecksDenyTotal.Load() - denyBefore; got != 2 {
		t.Fatalf("the two per-object checks must both DENY under the cohort; checks_deny delta=%d, want 2", got)
	}
	// THE anti-leak assertion: the measured identity is the COHORT, never the SA.
	if sc.identity.Username != denied {
		t.Fatalf("F-2E-IDENTITY: sc.identity.Username=%q, want the cohort %q (reading the SA transport identity is the leak vector this arm exists to catch)", sc.identity.Username, denied)
	}
	// #424: the seed resolves the cohort as an AUTHENTICATED identity — its
	// effective groups are the cohort groups plus system:authenticated
	// (rbac.WithAuthenticatedGroup), never anything of the SA's.
	want := rbac.WithAuthenticatedGroup([]string{a1Group})
	if len(sc.identity.Groups) != len(want) || sc.identity.Groups[0] != want[0] || sc.identity.Groups[1] != want[1] {
		t.Fatalf("F-2E-IDENTITY: sc.identity.Groups=%v, want the cohort's effective groups %v", sc.identity.Groups, want)
	}
}

// ───────────────────────── F-2E-CALIBRATION (4 buckets + sum) ─────────────────────────

func TestF2E_Calibration_FourBucketsAndSumEqualsInstalls(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	rbac.ResetRequesterProfileMemoForTest()
	reset2E()

	// b1 allow-bearing: alice is permitted get configmaps in tenant-a.
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "cal-allow", true, []string{a1TenantA})
	// b2 rbac-denied: carol (portal group, no configmap RB) is denied everywhere.
	drive2ERestaction(t, seed2ECohortCtx("carol"), "cal-deny", true, []string{a1TenantA, a1TenantB})
	// b3 gated-no-check: dGated, but the resolve fired ZERO checks (memo hit / empty).
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "cal-nocheck", true, nil)
	// b4 passthrough: NOT dGated (external-only step ⇒ no class).
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "cal-passthrough", false, nil)

	b1 := shadowPopulateAllowBearingTotal.Load()
	b2 := shadowPopulateRBACDeniedTotal.Load()
	b3 := shadowPopulateRBACGatedNoCheckTotal.Load()
	b4 := shadowPopulatePassthroughTotal.Load()
	installs := shadowPopulateSeedInstallsTotal.Load()

	if b1 != 1 || b2 != 1 || b3 != 1 || b4 != 1 {
		t.Fatalf("each drive must land in exactly its bucket: b1(allow)=%d b2(denied)=%d b3(gated-no-check)=%d b4(passthrough)=%d, want 1/1/1/1", b1, b2, b3, b4)
	}
	if b1+b2+b3+b4 != installs {
		t.Fatalf("SUM-INTEGRITY VIOLATED: b1+b2+b3+b4=%d != installs=%d (a silently-missed resolve)", b1+b2+b3+b4, installs)
	}
	if installs != 4 {
		t.Fatalf("four real seed drives must install four times; installs=%d", installs)
	}
	if shadowChecksAllowTotal.Load() < b1 {
		t.Fatalf("checks_allow_total (%d) must be >= the allow-bearing bucket b1 (%d)", shadowChecksAllowTotal.Load(), b1)
	}
}

// ───────────────────────── F-2E-DARK (byte + key parity) ─────────────────────────

func TestF2E_Dark_WidgetPutAndKeyByteIdenticalOnVsOff(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.ResetRequesterProfileMemoForTest()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	entry := seed2EWidgetEntry()
	cohort := seed2ECohortCtx(a1Alice)
	key, handle, inputs := dispatchCacheLookupKey(cohort, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(cohort, entry.W.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("precondition: a live widgets key with a non-empty BindingUID; key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}

	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(_ context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		out := h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}})
		if err := unstructured.SetNestedField(out.Object, "same-for-everyone", "status", "widgetData", "rows"); err != nil {
			t.Fatalf("seam: %v", err)
		}
		return out, nil
	}

	run := func(on bool) (string, string) {
		t.Helper()
		rbac.SetShadowParityEnabled(on)
		cache.ResetResolvedCacheForTest() // clear the seeded liveness so the seed re-Puts
		reset2E()
		// Re-derive the handle+key AFTER the reset so it binds to the LIVE store
		// (ResetResolvedCacheForTest detaches the pre-reset handle). The key is
		// deterministic, so both runs derive the identical key.
		rk, rh, _ := dispatchCacheLookupKey(cohort, "widgets",
			h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
			h1NS, h1WName, -1, -1, effectiveKeyExtras(cohort, entry.W.Object, nil))
		if err := seedOneWidget(cohort, entry, h1NS, seedModeBoot); err != nil {
			t.Fatalf("seedOneWidget(on=%v): %v", on, err)
		}
		e, ok := rh.Get(rk)
		if !ok {
			t.Fatalf("seedOneWidget(on=%v) did not Put the cell — cannot compare bytes", on)
		}
		sum := sha256.Sum256(e.RawJSON)
		return rk, hex.EncodeToString(sum[:])
	}

	offKey, offHash := run(false)
	onKey, onHash := run(true)
	if offKey != onKey || offHash != onHash {
		t.Fatalf("F-2E-DARK: shadow-ON perturbed the seed Put/key — off=(%q,%s) on=(%q,%s). The populate path's verdict/byte/key must be untouched.", offKey, offHash, onKey, onHash)
	}
	// The on-run must have really run 2E (else the parity claim is vacuous).
	if shadowPopulateSeedInstallsTotal.Load() == 0 {
		t.Fatal("F-2E-DARK: the shadow-ON run installed nothing — the byte-parity claim would be vacuous")
	}
}

// ───────────────────────── F-2E-PANIC (invariant b) ─────────────────────────

func TestF2E_Panic_DerivePanicNotCountedSeedCompletes(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	rbac.ResetRequesterProfileMemoForTest()
	reset2E()

	entry := seed2EWidgetEntry()
	cohort := seed2ECohortCtx(a1Alice)
	key, handle, _ := dispatchCacheLookupKey(cohort, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(cohort, entry.W.Object, nil))
	if handle == nil || key == "" {
		t.Fatalf("precondition: a live widgets key; key=%q handle=%v", key, handle != nil)
	}

	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(_ context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		out := h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}})
		if err := unstructured.SetNestedField(out.Object, "x", "status", "widgetData", "rows"); err != nil {
			t.Fatalf("seam: %v", err)
		}
		return out, nil
	}

	darkBefore := shadowDarkPanicTotal.Load()
	shadowDerivePanicForTest.Store(true)
	t.Cleanup(func() { shadowDerivePanicForTest.Store(false) })

	if err := seedOneWidget(cohort, entry, h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneWidget with a derive-panic: %v — the seed must COMPLETE (the dark panic is recovered)", err)
	}
	shadowDerivePanicForTest.Store(false)

	if got := shadowDarkPanicTotal.Load() - darkBefore; got < 1 {
		t.Fatalf("a recovered install derive-panic must bump dark_panic_total; delta=%d", got)
	}
	if got := shadowPopulateSeedInstallsTotal.Load(); got != 0 {
		t.Fatalf("INVARIANT b: a derive-panicked install must NOT be counted in installs; got %d", got)
	}
	if b := shadowPopulateAllowBearingTotal.Load() + shadowPopulateRBACDeniedTotal.Load() +
		shadowPopulateRBACGatedNoCheckTotal.Load() + shadowPopulatePassthroughTotal.Load(); b != 0 {
		t.Fatalf("a derive-panicked resolve must land in NO bucket (sum-integrity holds via the dark_panic backstop); bucket sum=%d", b)
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatal("the seed must still Put the cell despite the recovered dark panic — the dark carrier never blocks the seed")
	}
}

// ───────────────────────── F-2E-R2 (content-prewarm is identity-free) ─────────────────────────

func TestF2E_R2_ContentPrewarmClassesAreIdentityFree(t *testing.T) {
	// The F2 content-prewarm walker (deliberately NOT hooked by 2E) populates only
	// the identity-free classes whose ComputeKey SKIPS the identity fold — so no
	// per-identity leak hides there and skipping the populate hook leaves nothing
	// unmeasured.
	if !isIdentityFreeClass(cache.CacheEntryClassWidgetContent) {
		t.Fatal("R2: the widget-content class must be identity-free")
	}
	if !isIdentityFreeClass(cache.CacheEntryClassApistage) {
		t.Fatal("R2: the apistage class must be identity-free")
	}
	// The per-user seed classes (where 2E DOES install) must NOT be identity-free.
	if isIdentityFreeClass("restactions") || isIdentityFreeClass("widgets") {
		t.Fatal("R2: the per-user restactions/widgets classes must NOT be identity-free — those are exactly where 2E measures cohort parity")
	}
}
