// refresh_subscription_registry_test.go — L-KEYPARITY-FULLREGISTRY
// (docs/test-blindspot-analysis-2026-07-24.md, class 2 — key divergence).
// Widens the #67 standing per-class key-parity invariant to a REGISTRY that
// enumerates EVERY CacheEntryClass, so "a new class ships with zero parity arm"
// is a hard test failure — not a silent gap (the #64 / #107 divergence class,
// which broke silently across six builds).
//
// TWO PARITY KINDS, one per class — because the classes do NOT all key the same
// way, and asserting a single shape for all of them would fabricate a FALSE RED
// (feedback_falsifier_must_actually_run... / the author-shares-blind-spot trap
// the analysis itself names):
//
//   SUBSCRIPTION parity (sub == emit) — the THREE armable classes the /call
//   dispatcher stamps on the wire via X-Snowplow-Refresh-Class (restactions,
//   widgets, widgetContent; setRefreshKeyHeader sites in restactions.go /
//   widgets.go). A browser arms these by re-sending coords; the subscription
//   key MUST equal the emit key. This is the existing #67 invariant
//   (TestFalsifier67_SubscriptionKeyInvariant_PerClass) — referenced here, not
//   duplicated.
//
//   SEED==DISPATCH parity — the TWO substrate classes (apistage, raFullList)
//   that are NEVER armed by a ?sub= (the dispatcher stamps no refresh-class for
//   them). Their meaningful parity is "the cell the boot seed warms == the cell
//   a live /call warms": both derivations must produce field-equal
//   ResolvedKeyInputs. apistage is populated IDENTITY-FREE (contentKeyInputs:
//   empty BindingUID, RBACSubGen 0, no pagination — apistage.go:64); raFullList
//   uses cache.RAFullListKeyInputs (forces PerPage/Page 0). Asserting a
//   sub==emit shape for these would FALSE-RED, because DeriveSubscriptionKey
//   would fold the requester's BindingUID that the identity-free / page-
//   independent populate path never carries. The seed==dispatch arm is the
//   farch_seed_parity arm (F-ARCH-1) generalised to these classes.
//
// SPA-MODEL-DRIFT CAVEAT (permanent, per feedback_curl_probes_inadmissible...):
// the emit/dispatch derivations here are the SERVER's real functions, but the
// seed-vs-portal arm's request shape mirrors the SPA buildExtrasParam by hand
// (see farch_seed_parity_test.go / farch_f6_keyextras_test.go). Real-Chrome
// DISPATCH_KEY_DIAG key_hash equality stays the DECISIVE post-deploy arm; this
// hermetic registry is the pre-merge floor that would have caught #64/#107.

package dispatchers

import (
	"reflect"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// parityKind classifies HOW a CacheEntryClass's key-parity is asserted.
type parityKind int

const (
	// paritySubscription — sub key == emit key (the armable classes; #67).
	paritySubscription parityKind = iota
	// paritySeedDispatch — seed-key == dispatch-key (the substrate classes).
	paritySeedDispatch
)

// keyParityRegistry is the CANONICAL enumeration of every CacheEntryClass and
// the parity KIND that governs it. A new class added to internal/cache MUST be
// added here with its parity kind, or TestKeyParityRegistry_EveryClassCovered
// fails (closes "a new class ships with zero parity arm"). The three armable
// classes are the two bare-literal classes (classRestActions / classWidgets —
// refresh_subscription.go:52) plus cache.CacheEntryClassWidgetContent; the two
// substrate classes are cache.CacheEntryClassApistage / RAFullList.
var keyParityRegistry = map[string]parityKind{
	classRestActions:                  paritySubscription, // "restactions"
	classWidgets:                      paritySubscription, // "widgets"
	cache.CacheEntryClassWidgetContent: paritySubscription, // "widgetContent"
	cache.CacheEntryClassApistage:      paritySeedDispatch, // "apistage"
	cache.CacheEntryClassRAFullList:    paritySeedDispatch, // "raFullList"
}

// allCacheEntryClasses is the GROUND-TRUTH set of CacheEntryClass string values
// that exist in internal/cache. It is asserted equal to the registry's key set
// by TestKeyParityRegistry_EveryClassCovered. Whoever adds a new
// CacheEntryClass constant to internal/cache must add it here AND to
// keyParityRegistry with its parity kind — the two-list cross-check is the
// registry-drift guard (a new constant absent from either list fails the test).
//
// The three armable classes have no named cache constant (they are the bare
// literals classRestActions / classWidgets and the widgetContent constant); the
// two substrate classes are the named apistage / raFullList constants. This
// list is the union — every string ever written as ResolvedKeyInputs.
// CacheEntryClass by any Put site.
var allCacheEntryClasses = []string{
	classRestActions,                   // dispatchers literal "restactions"
	classWidgets,                       // dispatchers literal "widgets"
	cache.CacheEntryClassWidgetContent, // "widgetContent"
	cache.CacheEntryClassApistage,      // "apistage"
	cache.CacheEntryClassRAFullList,    // "raFullList"
}

// TestKeyParityRegistry_EveryClassCovered is the registry-completeness assert:
// every CacheEntryClass value has an entry in keyParityRegistry (a parity kind),
// and the registry has no phantom classes. A new class added to internal/cache
// without a registry entry — the "ships with zero parity arm" gap #64/#107
// exemplify — fails here.
func TestKeyParityRegistry_EveryClassCovered(t *testing.T) {
	// Every ground-truth class must be in the registry.
	for _, class := range allCacheEntryClasses {
		if _, ok := keyParityRegistry[class]; !ok {
			t.Errorf("CacheEntryClass %q has NO key-parity registry entry — a new class must declare its "+
				"parity kind (paritySubscription for an armable class, paritySeedDispatch for a substrate "+
				"class). This closes 'a new class ships with zero parity arm' (#64/#107).", class)
		}
	}
	// The registry must have no class the ground-truth list omits (catches a
	// stale registry entry for a removed class).
	truth := map[string]bool{}
	for _, c := range allCacheEntryClasses {
		truth[c] = true
	}
	for class := range keyParityRegistry {
		if !truth[class] {
			t.Errorf("keyParityRegistry has entry %q not in allCacheEntryClasses — remove the stale entry "+
				"or add the class to the ground-truth list.", class)
		}
	}
	// Coverage counts must match (belt-and-suspenders on the two-list cross-check).
	if len(keyParityRegistry) != len(allCacheEntryClasses) {
		t.Fatalf("registry/ground-truth size mismatch: registry=%d truth=%d — the two lists must enumerate "+
			"the SAME class set", len(keyParityRegistry), len(allCacheEntryClasses))
	}
}

// TestKeyParityRegistry_SubscriptionClassesAreArmable pins the architectural
// fact the parity KINDS rest on: EXACTLY the three subscription-parity classes
// are the ones DeriveSubscriptionKey accepts (armable by a browser ?sub=), and
// the two seed-dispatch classes are NOT independently armable via the
// dispatchCacheLookupKey subscription arms. If a future change makes apistage /
// raFullList armable (a new setRefreshKeyHeader stamp), this test flips and
// forces re-classifying them to paritySubscription + adding a sub==emit arm.
func TestKeyParityRegistry_SubscriptionClassesAreArmable(t *testing.T) {
	// The three subscription-parity classes are exactly {restactions, widgets,
	// widgetContent} — the classes the existing #67 invariant loop covers
	// (refresh_subscription_invariant_test.go:108) and the ONLY classes the
	// /call dispatcher stamps as a refresh-class on the wire.
	wantSub := map[string]bool{
		classRestActions:                   true,
		classWidgets:                       true,
		cache.CacheEntryClassWidgetContent: true,
	}
	gotSub := map[string]bool{}
	for class, kind := range keyParityRegistry {
		if kind == paritySubscription {
			gotSub[class] = true
		}
	}
	if !reflect.DeepEqual(gotSub, wantSub) {
		t.Fatalf("subscription-parity class set drifted: got %v want %v. The armable set is fixed by the "+
			"setRefreshKeyHeader stamp sites (restactions/widgets/widgetContent); if a substrate class "+
			"became armable, add a sub==emit arm and re-classify it.", gotSub, wantSub)
	}
}

// apistageContentKeyInputs reconstructs the EXACT ResolvedKeyInputs literal the
// production apistage populate builder (api.contentKeyInputs, apistage.go:64)
// emits for a content cell — reproduced here because contentKeyInputs is
// package-api-private. This is the SEED/populate-side derivation.
func apistageContentKeyInputs(gvr schema.GroupVersionResource, namespace, name string) cache.ResolvedKeyInputs {
	return cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassApistage,
		Group:           gvr.Group,
		Version:         gvr.Version,
		Resource:        gvr.Resource,
		Namespace:       namespace,
		Name:            name,
	}
}

// TestKeyParity_SeedEqualsDispatch_Apistage — the apistage SEED==DISPATCH arm,
// driven from TWO INDEPENDENT real entry points (arch hard requirement — not a
// tautology):
//
//   - SEED side: the populate builder receives its (gvr, ns, name) DIRECTLY
//     (apistage.go:609 stores under contentKeyInputs(gvr, ns, name); the boot
//     seed / cluster-list prewarm warm the same cell the same way).
//   - /CALL side: the read/lookup builder derives (gvr, ns, name) by PARSING a
//     real apiserver PATH string via cache.ParseAPIServerPathToDep (the
//     production /call entry — resolve_inprocess.go:125, apistage.go:487), then
//     keys with contentKeyInputs. The path is the SEPARATE input the /call
//     brings; the seed never sees a path.
//
// The meaningful invariant: "the cell the seed warms == the cell a real /call
// PATH lookup lands on." It DISCRIMINATES because the two sides derive their
// coordinates from DIFFERENT inputs (direct tuple vs a parsed URL) — a path
// parser that drops the namespace segment (the class-3 by-name / cluster-list
// collapse family, memory project_class3_clusterlist_byname_collapse) diverges
// the coords → different key → the seed cell is unreachable by the /call. The
// RED sub-arm proves that.
func TestKeyParity_SeedEqualsDispatch_Apistage(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	const ns, name = "team-a", "cm-1"

	// SEED side — direct coords into the populate builder.
	seed := apistageContentKeyInputs(gvr, ns, name)

	// /CALL side — derive coords from a REAL apiserver path (the independent
	// input the /call brings), through the production parser.
	callPath := "/api/v1/namespaces/" + ns + "/configmaps/" + name
	pGVR, pNS, pName, ok := cache.ParseAPIServerPathToDep(callPath)
	if !ok {
		t.Fatalf("apistage /call side: ParseAPIServerPathToDep(%q) failed", callPath)
	}
	dispatch := apistageContentKeyInputs(pGVR, pNS, pName)

	// PRE-HASH field equality (names the diverging field), then digest.
	assertKeyInputsFieldEqual(t, "apistage/seed-vs-dispatch", &seed, &dispatch)

	// Identity-free + page-independent invariants the #58 serve-time gate and
	// the shared-substrate sharing rely on.
	if seed.BindingUID != "" || seed.RBACSubGen != 0 {
		t.Fatalf("apistage cell must be identity-FREE (BindingUID=%q RBACSubGen=%d) — folding identity "+
			"breaks the shared-substrate invariant the serve-time gate depends on", seed.BindingUID, seed.RBACSubGen)
	}
	if seed.PerPage != 0 || seed.Page != 0 {
		t.Fatalf("apistage content cell is page-independent (PerPage=%d Page=%d must be 0)", seed.PerPage, seed.Page)
	}
}

// TestKeyParity_SeedEqualsDispatch_Apistage_RED is the DISCRIMINATING RED arm:
// it flips ONE coordinate (the namespace) on the /call side — modelling a path
// parser that lands the cell in the WRONG namespace (the class-3 collapse
// family) — and asserts the seed key and the /call key now DIVERGE. If the arm
// stayed GREEN here, the seed==dispatch assertion above would be a tautology
// (proving nothing); this proves it catches a real coordinate divergence.
func TestKeyParity_SeedEqualsDispatch_Apistage_RED(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	seed := apistageContentKeyInputs(gvr, "team-a", "cm-1")

	// A path that resolves the SAME object but in a DIFFERENT namespace (the
	// injected divergence — one flipped input field on one side).
	wrongPath := "/api/v1/namespaces/team-b/configmaps/cm-1"
	pGVR, pNS, pName, ok := cache.ParseAPIServerPathToDep(wrongPath)
	if !ok {
		t.Fatalf("RED setup: ParseAPIServerPathToDep(%q) failed", wrongPath)
	}
	dispatch := apistageContentKeyInputs(pGVR, pNS, pName)

	if cache.ComputeKey(seed) == cache.ComputeKey(dispatch) {
		t.Fatalf("RED arm did NOT discriminate: a team-a seed key equals a team-b /call key — the "+
			"seed==dispatch parity assertion would be a tautology (namespace divergence not caught). "+
			"seed.ns=%q dispatch.ns=%q", seed.Namespace, dispatch.Namespace)
	}
	if seed.Namespace == dispatch.Namespace {
		t.Fatalf("RED arm setup broken: expected divergent namespaces, got seed=%q dispatch=%q", seed.Namespace, dispatch.Namespace)
	}
}

// TestKeyParity_SeedEqualsDispatch_RAFullList — the raFullList SEED==DISPATCH
// arm. raFullList is IDENTITY-BOUND (folds BindingUID) but PAGE-INDEPENDENT
// (cache.RAFullListKeyInputs forces PerPage/Page 0 + extrasMinusSlice — the
// slice is applied at serve time, ra_full_list_slice.go). The single prod
// builder is fed by TWO INDEPENDENT request shapes (arch hard requirement):
//
//   - SEED side: warmed WITHOUT a browser request — a DIFFERENT requested page
//     in the slice extra (page 1), the boot-seed shape.
//   - /CALL side: a live browser request for a DIFFERENT page (page 9) of the
//     SAME (RA × binding × non-slice extras).
//
// The meaningful invariant is the raFullList DEDUPE: "the cell a page-1 seed
// warms == the cell a page-9 /call reads." It DISCRIMINATES the slice-stripping
// (extrasMinusSlice) — if extrasMinusSlice failed to strip the slice, page 1
// and page 9 would key differently and the seed cell would be unreachable by
// the /call (the extras-xlen divergence family). The RED sub-arm proves the arm
// catches a NON-slice extras divergence (the dimension the seed↔/call can
// genuinely disagree on).
func TestKeyParity_SeedEqualsDispatch_RAFullList(t *testing.T) {
	const g, v, r, ns, name = "templates.krateo.io", "v1", "restactions", "demo-system", "ra-1"
	const bindingUID = "C:test-crb-uid"

	// SEED side — page 1 requested (the boot-seed shape; no browser).
	seedExtras := map[string]any{"region": "eu", "slice": map[string]any{"perPage": 10, "page": 1}}
	seed := cache.RAFullListKeyInputsForTest(g, v, r, ns, name, bindingUID, seedExtras)

	// /CALL side — a DIFFERENT requested page (9) of the SAME cell.
	callExtras := map[string]any{"region": "eu", "slice": map[string]any{"perPage": 50, "page": 9}}
	dispatch := cache.RAFullListKeyInputsForTest(g, v, r, ns, name, bindingUID, callExtras)

	assertKeyInputsFieldEqual(t, "raFullList/seed-vs-dispatch", &seed, &dispatch)
	if seed.PerPage != 0 || seed.Page != 0 {
		t.Fatalf("raFullList must force PerPage/Page 0 (page-independent cell); got %d/%d", seed.PerPage, seed.Page)
	}
	if cache.ComputeKey(seed) != cache.ComputeKey(dispatch) {
		t.Fatalf("raFullList SEED==DISPATCH: page-1 seed key != page-9 dispatch key — the page-independent "+
			"dedupe broke (extrasMinusSlice failed to strip the slice; different pages of the same "+
			"RA×binding must share ONE cell)")
	}
}

// TestKeyParity_SeedEqualsDispatch_RAFullList_RED is the DISCRIMINATING RED arm:
// it flips a NON-slice extra (region eu → us) on the /call side — the dimension
// the seed and a real /call can genuinely disagree on (the extras-xlen
// divergence family, #107). The seed key and the /call key MUST diverge; a
// GREEN result here would mean the arm ignores non-slice extras and the parity
// assertion is a tautology.
func TestKeyParity_SeedEqualsDispatch_RAFullList_RED(t *testing.T) {
	const g, v, r, ns, name = "templates.krateo.io", "v1", "restactions", "demo-system", "ra-1"
	const bindingUID = "C:test-crb-uid"

	seedExtras := map[string]any{"region": "eu", "slice": map[string]any{"perPage": 10, "page": 1}}
	seed := cache.RAFullListKeyInputsForTest(g, v, r, ns, name, bindingUID, seedExtras)

	// One flipped NON-slice extra on the /call side (region eu → us).
	callExtras := map[string]any{"region": "us", "slice": map[string]any{"perPage": 50, "page": 9}}
	dispatch := cache.RAFullListKeyInputsForTest(g, v, r, ns, name, bindingUID, callExtras)

	if cache.ComputeKey(seed) == cache.ComputeKey(dispatch) {
		t.Fatalf("RED arm did NOT discriminate: a region=eu seed key equals a region=us /call key — the "+
			"seed==dispatch parity assertion would be a tautology (non-slice extras divergence not "+
			"caught, the #107 extras-xlen class).")
	}
}
