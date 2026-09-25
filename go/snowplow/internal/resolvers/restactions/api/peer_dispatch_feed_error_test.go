// peer_dispatch_feed_error_test.go — Build backlog #6 falsifiers.
//
// THE DEFECT (TRACED + PM-confirmed): four served-path branches in
// dispatchOneCall (resolve.go) do a raw `return err` on a feedBytes/feedValue
// error, instead of routing it through recordItemError (Option C-A, #313).
// A feed* error is REACHABLE via the per-item stage FILTER zero-yield
// (handler.go:154 `jq filter %q yielded no value`) and multi-yield
// (handler.go:142 ErrMultiYield) — both converge on jsonHandlerCore where the
// filter runs. On the raw `return err` the worker returns non-nil → g.Wait()
// non-nil → runStage returns stop=true → the whole resolve abandons the stage
// AND all downstream stages, serving partial content (the #313 C-A violation).
//
// The 4 PEER served branches (find-by-code, MAIN line numbers differ from the
// audit's #35-branch refs):
//   1. nested /call          — feedBytes(statusRaw)   after nestedCallResolver
//   2. apistage gated env.    — feedValue(gatedVal)    after apistageContentServe
//   3. informer-served        — feedBytes(raw)         after dispatchViaInformer
//   4. internal-rest-config   — feedBytes(raw)         after dispatchViaInternalRESTConfig
//
// The external-endpoint branch (httpcall.Do / httpFetchAllowingNonJSON) is
// already #313-correct (it routes the feed error through recordItemError) and
// is NOT touched here (#35, parked separately).
//
// ORACLE = the #313 Option C-A contract on HEAD: a per-item iterator error
// (here a per-item FILTER zero-/multi-yield) must NOT skip remaining items or
// downstream stages.  No pre-0.30.128 git-worktree oracle — these served
// branches never went through httpcall.Do, so a pre-0.30.128 baseline would
// FALSELY show no-truncation (architect-confirmed).
//
// PER-SITE DRIVER (mirrors resolve_iter_continue_test.go + the F1 fixtures):
// a 2-stage RA. Stage 1 is an iterator whose served-branch IS the site under
// test; its `filter` ZERO-YIELDS on exactly ONE item (jq `select(...)` that
// drops the failing item's name) and yields the sibling items. Stage 2
// (`downstream`, no iterator) writes dict["downstream"].
//
// CONTENT assertions (NOT HTTP-200 / key-count — feedback_validate_content_not_just_status):
//   RED on the unfixed branch: dict["downstream"] ABSENT (truncated),
//                              dict[errorKey] ABSENT.
//   GREEN after fix:           dict["downstream"] PRESENT,
//                              dict[errorKey] carries the feed error,
//                              the successful sibling item present in dict[id].
//
// json.Unmarshal sub-mode (malformed bytes) is UNREACHABLE — every dispatch
// source emits valid JSON — so it is NOT exercised here
// (feedback_no_fake_production_scenarios).

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// pdfeNamespaces are the iterator elements every site fans over. The element
// at index pdfeFailIdx is the one whose served response the stage filter
// ZERO-YIELDS on (so its feed* call errors), proving the non-truncation
// contract: the other namespaces (siblings) must still merge AND the
// downstream stage must still run.
var pdfeNamespaces = []string{"ns-a", "ns-b", "ns-c", "ns-d"}

const pdfeFailIdx = 2 // "ns-c" is the failing element

// pdfeVals builds the iterator element array carried in Extras:
// {"vals":[{"ns":"ns-a"},...]}.
func pdfeVals() []any {
	out := make([]any, 0, len(pdfeNamespaces))
	for _, ns := range pdfeNamespaces {
		out = append(out, map[string]any{"ns": ns})
	}
	return out
}

// pdfeResolveCtx builds the base resolve context (UserInfo required by
// Resolve). The username is admin-broad so every site's RBAC gate is a
// transparent pass-through — the only per-item failure under test is the
// FILTER zero-yield, never an RBAC drop.
func pdfeResolveCtx(username string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: username}),
	)
}

// pdfeAssertNoTruncation is the shared CONTENT oracle. dict is the full
// Resolve output, id is the iterator stage id, sibling is a substring that
// must appear in the merged dict[id] (a successful sibling item), and errKey
// is the stage's ErrorKey.
//
//	GREEN (fixed): downstream present, dict[errKey] carries the feed error,
//	               sibling present in dict[id].
//	RED (unfixed): downstream absent (truncated) AND dict[errKey] absent.
func pdfeAssertNoTruncation(t *testing.T, dict map[string]any, id, errKey, sibling string) {
	t.Helper()

	_, downstreamPresent := dict["downstream"]
	_, errPresent := dict[errKey]

	if !downstreamPresent {
		t.Errorf("TRUNCATION: dict[\"downstream\"] absent — the per-item feed error truncated "+
			"the whole resolve (raw `return err` instead of recordItemError). dict keys=%v", keysOf(dict))
	}
	if !errPresent {
		t.Errorf("dict[%q] absent — the feed error was not recorded via recordItemError. "+
			"dict keys=%v", errKey, keysOf(dict))
	}

	// The recorded error must be an accumulating slice (W-A) carrying the
	// feed error (the zero-yield message).
	if errPresent {
		if es, ok := dict[errKey].([]any); !ok {
			t.Errorf("dict[%q] is %T, want []any (accumulating slice)", errKey, dict[errKey])
		} else if len(es) == 0 {
			t.Errorf("dict[%q] empty slice; want >=1 feed error", errKey)
		} else {
			joined := fmt.Sprintf("%v", es)
			// The two reachable feed (filter) errors: zero-yield
			// (handler.go:154 "jq filter %q yielded no value") and
			// multi-yield (ErrMultiYield — "jq query yielded more than one
			// value, expected exactly one").
			if !containsAny(joined, "yielded no value", "more than one value") {
				t.Errorf("dict[%q] does not carry the feed (filter) error: %v", errKey, es)
			}
		}
	}

	// A successful sibling must be present in the merged stage output.
	if sibling != "" {
		if !containsAny(fmt.Sprintf("%v", dict[id]), sibling) {
			t.Errorf("successful sibling %q absent from dict[%q]: %v", sibling, id, dict[id])
		}
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// SITE 1 — RETIRED (2026-06-22 unified ship). The /call loopback dispatch
// branch (feedBytes(statusRaw)) was retired (dead code — corpus audit
// confirmed zero live loopback paths). Its feed-error-no-truncate coverage
// (#313 Option C-A) is fully subsumed by Sites 2/3/4 below, which feed via the
// SAME feedBytes/recordItemError triad through feedRawOrResolved. The two
// former Site-1 functions (ZeroYield/MultiYield over a /call?resource= path)
// were deleted with the branch they exercised.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// shared informer/apistage fixture (sites 2 & 3)
// ---------------------------------------------------------------------------

var pdfeGVR = schema.GroupVersionResource{
	Group:    "widgets.krateo.io",
	Version:  "v1",
	Resource: "widgets",
}

// pdfeWidget builds an unstructured widget named after its namespace so the
// per-item filter can target exactly one item by content.
func pdfeWidget(ns string) runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.krateo.io/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      "widget-" + ns,
		},
	}}
}

// pdfeAdminUser is granted a cluster-wide wildcard so the dispatch RBAC gate
// is transparent — the only per-item failure under test is the filter
// zero-yield.
const pdfeAdminUser = "pdfe-admin"

func pdfeAdminRBACSeed() []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "pdfe-admin-role"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "pdfe-admin-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: pdfeAdminUser}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "pdfe-admin-role"},
		},
	}
}

// pdfeListKinds is the dynamicfake LIST-kind map (widgets + RBAC kinds the
// watcher constructor eagerly registers).
func pdfeListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		pdfeGVR: "WidgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
}

// pdfeNewWatcher stands up a synced cache=on watcher seeded with one widget
// per namespace plus the admin RBAC. apistageOn selects the apistage-enabled
// vs apistage-disabled dispatch branch (site 2 vs site 3). Post #57-fold the
// api-stage L1 is on iff ResolvedCacheEnabled(), so the off-arm disables it
// via RESOLVED_CACHE_ENABLED=false (the master gate it now folds under) —
// still exercising the apistage-disabled path.
func pdfeNewWatcher(t *testing.T, apistageOn bool) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	if apistageOn {
		t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	} else {
		t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	}
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	var seed []runtime.Object
	for _, ns := range pdfeNamespaces {
		seed = append(seed, pdfeWidget(ns))
	}
	seed = append(seed, pdfeAdminRBACSeed()...)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, pdfeListKinds(), seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

	added, syncCh := rw.EnsureResourceType(pdfeGVR)
	if !added {
		t.Fatalf("EnsureResourceType(widgets): want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("widgets informer did not sync within 5s")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// pdfeGetByNameStage builds an iterator stage that GET-by-names one widget per
// namespace from the informer/apistage cache. The filter zero-yields on the
// failing namespace's widget (by name) and yields the siblings.
func pdfeGetByNameStage(id string, multiYield bool) *templates.API {
	var filter string
	if multiYield {
		// `.id | .x[]` over a 2-element array → ErrMultiYield. The widget has
		// no `.x`; we instead build a synthetic 2-element via the stage path
		// returning the bare object — so for multi-yield we filter `.id |
		// (.metadata, .metadata)` which yields twice. Simpler: wrap object in
		// a 2-element array via `[.id.metadata, .id.metadata] | .[]`.
		filter = fmt.Sprintf(`[.%s.metadata, .%s.metadata] | .[]`, id, id)
	} else {
		failName := "widget-" + pdfeNamespaces[pdfeFailIdx]
		filter = fmt.Sprintf(`.%s | select(.metadata.name != "%s")`, id, failName)
	}
	return &templates.API{
		Name: id,
		// GET-by-name per namespace; name=widget-<ns> exists in the seed.
		Path:      `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets/widget-" + .ns }`,
		Verb:      ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{Iterator: ptr.To(".vals")},
		Filter:    ptr.To(filter),
		ErrorKey:  ptr.To("error"),
	}
}

// pdfeResolveCached drives the REAL api.Resolve over the informer/apistage
// cache as pdfeAdminUser, with the downstream stage GET-by-name'ing an extra
// widget (ns-a) so its presence proves the loop continued.
func pdfeResolveCached(t *testing.T, watcher *cache.ResourceWatcher, stage *templates.API) map[string]any {
	t.Helper()
	id := stage.Name
	downstream := &templates.API{
		Name:      "downstream",
		Path:      `/apis/widgets.krateo.io/v1/namespaces/ns-a/widgets/widget-ns-a`,
		Verb:      ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{Name: id},
		Filter:    ptr.To(".downstream"),
	}
	ctx := pdfeResolveCtx(pdfeAdminUser)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	return Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage, downstream},
		Extras:              map[string]any{"vals": pdfeVals()},
		Watcher:             watcher,
		RESTActionNamespace: "default",
		RESTActionName:      "peer-feed-cached",
	})
}

// ---------------------------------------------------------------------------
// SITE 2 — apistage gated envelope (feedValue(gatedVal))
// ---------------------------------------------------------------------------
//
// apistage ON (folded under RESOLVED_CACHE_ENABLED, #57) → the
// apistageContentServe path feeds a decoded gated value via feedValue. The
// failing item's filter zero-yields →
// feedValue returns the error → site 2's raw `return err` truncates.
func TestPeerFeedError_Site2_ApistageContent_ZeroYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, true) // apistage ON
	dict := pdfeResolveCached(t, rw, pdfeGetByNameStage("site2", false))
	pdfeAssertNoTruncation(t, dict, "site2", "error", "widget-ns-a")
}

func TestPeerFeedError_Site2_ApistageContent_MultiYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, true)
	dict := pdfeResolveCached(t, rw, pdfeGetByNameStage("site2", true))
	pdfeAssertNoTruncation(t, dict, "site2", "error", "")
}

// ---------------------------------------------------------------------------
// resolve:true substitution through the APISTAGE-CONTENT arm (PM gate, the
// production-default arm). apistage ON (folded under RESOLVED_CACHE_ENABLED)
// makes a single-CR GET-by-name of a widgets-resource CR land on the apistage-content
// block (resolve.go), which has the feedValue(gatedVal) vs feedBytes(substituted)
// FORK. These prove the fork wires correctly: when maybeResolveInProcess
// substitutes, the apistage arm feeds the SUBSTITUTED resolved bytes (NOT the
// raw gated value), and the result matches the informer arm for the same CR.
// ---------------------------------------------------------------------------

// pdfeInProcSeamSentinel is the canned envelope the seam stub returns; its
// presence in the stage output proves the apistage arm fed feedBytes(substituted)
// and NOT feedValue(gatedVal) (the raw widget, which has no such marker).
const pdfeInProcSeamSentinel = `{"kind":"Widget","apiVersion":"widgets.krateo.io/v1","status":{"inProcessResolved":"sentinel-value"}}`

// pdfeResolveOneGetByName drives api.Resolve for a SINGLE (non-iterator)
// resolve:true GET-by-name stage of widget-<ns> over the given watcher, under
// the supplied outer L1 key. Returns the resolved dict.
func pdfeResolveOneGetByName(t *testing.T, watcher *cache.ResourceWatcher, ns, outerKey string, resolve *bool) map[string]any {
	t.Helper()
	stage := &templates.API{
		Name:    "ref",
		Path:    "/apis/widgets.krateo.io/v1/namespaces/" + ns + "/widgets/widget-" + ns,
		Verb:    ptr.To(http.MethodGet),
		Resolve: resolve,
		Filter:  ptr.To(".ref"),
	}
	ctx := pdfeResolveCtx(pdfeAdminUser)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	if outerKey != "" {
		ctx = cache.WithL1KeyContext(ctx, outerKey)
	}
	return Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		Watcher:             watcher,
		RESTActionNamespace: "default",
		RESTActionName:      "apistage-inproc-resolve",
	})
}

// TestInProcessResolve_ApistageArm_FeedsSubstituted is the PM-gate falsifier:
// a resolve:true single-CR GET-by-name lands on the APISTAGE-CONTENT arm
// (apistage ON via RESOLVED_CACHE_ENABLED) and the arm feeds the SUBSTITUTED
// resolved envelope — proving the feedValue(gatedVal) → feedBytes(substituted)
// fork wires correctly (the riskiest of the 3 served arms). The seam is
// stubbed to a sentinel so the substitution is unambiguous and there is ZERO
// outbound HTTP.
//
// RED if the apistage arm fed feedValue(gatedVal) (the raw widget) when did==true.
func TestInProcessResolve_ApistageArm_FeedsSubstituted(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, true) // APISTAGE arm (production default)

	// Stub the in-process resolve seam to a sentinel envelope.
	prev := nestedCallResolver
	var seamCalls int
	nestedCallResolver = func(_ context.Context, ref templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		seamCalls++
		if ref.Resource != "widgets" {
			t.Errorf("apistage arm: seam ref.Resource = %q, want widgets", ref.Resource)
		}
		return []byte(pdfeInProcSeamSentinel), nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	dict := pdfeResolveOneGetByName(t, rw, "ns-a", "", ptr.To(true))

	if seamCalls == 0 {
		t.Fatalf("apistage arm: resolve:true did NOT invoke the in-process seam — "+
			"a GET-by-name on the apistage arm fed the raw gated value instead of "+
			"substituting; dict=%#v", dict)
	}
	ref, ok := dict["ref"].(map[string]any)
	if !ok {
		t.Fatalf("apistage arm: dict[ref] not a map: %#v", dict["ref"])
	}
	status, _ := ref["status"].(map[string]any)
	if status == nil || status["inProcessResolved"] != "sentinel-value" {
		t.Fatalf("apistage arm FORK BUG: the SUBSTITUTED resolved envelope was NOT fed "+
			"(feedValue(gatedVal) fed the raw widget instead of feedBytes(substituted)). "+
			"got dict[ref]=%#v", ref)
	}
}

// TestInProcessResolve_ApistageArm_DepOnOuterKey mirrors I-7 on the apistage
// arm: a resolve:true GET-by-name under an outer L1 key records the referenced
// widget's apiserver-path dep edge on the OUTER key (the dep site fires
// regardless of which serve arm dispatched, and regardless of whether the
// substitution seam is wired). RED if the apistage arm dropped the L1 key on
// the way to the substitution. The seam is stubbed (the substitution itself is
// covered above + end-to-end with the real seam in the dispatchers package);
// here we isolate that the apistage arm preserves the OUTER L1 key for the
// dep recording (the proposal §5 / I-7 caveat on this specific arm).
func TestInProcessResolve_ApistageArm_DepOnOuterKey(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, true)

	prev := nestedCallResolver
	nestedCallResolver = func(_ context.Context, _ templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		return []byte(pdfeInProcSeamSentinel), nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	const outerKey = "apistage-outer-L1-key"
	dict := pdfeResolveOneGetByName(t, rw, "ns-c", outerKey, ptr.To(true))
	if dict["ref"] == nil {
		t.Fatalf("apistage arm dep test: no output — resolve:true did not run; dict=%#v", dict)
	}

	matches := cache.Deps().CollectMatchesForTest(pdfeGVR, "ns-c", "widget-ns-c")
	if _, ok := matches[outerKey]; !ok {
		t.Fatalf("apistage arm: NO dep edge from the OUTER L1 key %q to the referenced "+
			"widget (gvr=%s ns=ns-c name=widget-ns-c) — editing the widget would not "+
			"dirty-mark the outer entry. matches=%#v", outerKey, pdfeGVR, matches)
	}
}

// TestInProcessResolve_ApistageContentCachesRaw_NoResolveContamination is the
// CORRECTNESS counterpart the architect flagged: the apistage CONTENT cache
// (keyed (class,gvr,ns,name) — NOT on `resolve`, contentKeyInputs apistage.go)
// stores the RAW dispatched envelope, and the resolve:true substitution is a
// PER-REQUEST transform applied DOWNSTREAM of the content serve. So two
// resolves of the SAME CR — one resolve:true, one resolve:false — share the
// same content entry (raw) yet diverge correctly: true → substituted envelope,
// false → raw CR. A bug that cached the RESOLVED bytes at the content layer
// (or keyed it without `resolve`) would CONTAMINATE: the 2nd (resolve:false)
// call would hit a cached resolved entry and wrongly return the substituted
// shape. This asserts the content cache stays resolve-agnostic and consistent
// with the informer / internal-rest-config arms (all feed raw; substitution is
// uniformly downstream).
//
// Both resolves run against the SAME watcher so the 1st populates the content
// entry and the 2nd is a content HIT — the contamination window.
func TestInProcessResolve_ApistageContentCachesRaw_NoResolveContamination(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, true) // APISTAGE arm

	prev := nestedCallResolver
	nestedCallResolver = func(_ context.Context, _ templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		return []byte(pdfeInProcSeamSentinel), nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	// 1st: resolve:true → populates the content entry (raw) + substitutes.
	trueDict := pdfeResolveOneGetByName(t, rw, "ns-d", "", ptr.To(true))
	trueRef, _ := trueDict["ref"].(map[string]any)
	trueStatus, _ := trueRef["status"].(map[string]any)
	if trueStatus == nil || trueStatus["inProcessResolved"] != "sentinel-value" {
		t.Fatalf("contamination test: resolve:true did not substitute on the apistage arm; got %#v", trueRef)
	}

	// 2nd: resolve:false on the SAME CR → content HIT (raw entry already
	// stored). Must return the RAW widget — NOT the substituted sentinel.
	falseDict := pdfeResolveOneGetByName(t, rw, "ns-d", "", ptr.To(false))
	falseRef, ok := falseDict["ref"].(map[string]any)
	if !ok {
		t.Fatalf("contamination test: dict[ref] not a map on resolve:false: %#v", falseDict["ref"])
	}
	// The RAW widget has metadata.name=widget-ns-d and NO status.inProcessResolved.
	if fs, _ := falseRef["status"].(map[string]any); fs != nil && fs["inProcessResolved"] == "sentinel-value" {
		t.Fatalf("CONTENT-CACHE CONTAMINATION: resolve:false hit a cached RESOLVED entry on the "+
			"apistage arm — the content cache must store RAW (resolve-agnostic) bytes, with the "+
			"substitution applied per-request downstream. got dict[ref]=%#v", falseRef)
	}
	meta, _ := falseRef["metadata"].(map[string]any)
	if meta == nil || meta["name"] != "widget-ns-d" {
		t.Fatalf("contamination test: resolve:false did not return the raw widget CR; got %#v", falseRef)
	}
}

// ---------------------------------------------------------------------------
// SITE 3 — informer-served (feedBytes(raw))
// ---------------------------------------------------------------------------
//
// apistage OFF → the dispatchViaInformer branch feeds raw bytes via feedBytes.
// The failing item's filter zero-yields → feedBytes returns the error → site
// 3's raw `return err` truncates.
func TestPeerFeedError_Site3_Informer_ZeroYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, false) // apistage OFF → informer branch
	dict := pdfeResolveCached(t, rw, pdfeGetByNameStage("site3", false))
	pdfeAssertNoTruncation(t, dict, "site3", "error", "widget-ns-a")
}

func TestPeerFeedError_Site3_Informer_MultiYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	rw := pdfeNewWatcher(t, false)
	dict := pdfeResolveCached(t, rw, pdfeGetByNameStage("site3", true))
	pdfeAssertNoTruncation(t, dict, "site3", "error", "")
}

// ---------------------------------------------------------------------------
// SITE 4 — internal-rest-config (feedBytes(raw))
// ---------------------------------------------------------------------------
//
// cache.WithInternalRESTConfig + a TLS httptest apiserver (the SAME real
// client-go transport path internal_dispatch_paged_test.go drives, via
// internalClientFor — no new fake-client seam) → the
// dispatchViaInternalRESTConfig branch serves GET-by-name and feeds raw bytes
// via feedBytes. The failing item's filter zero-yields → feedBytes errors →
// site 4's raw `return err` truncates.
//
// CACHE_ENABLED stays unset → resolverUseInformer() false → the informer block
// is skipped; the internal-rest-config block (gated on the context
// *rest.Config) owns the GET.
func TestPeerFeedError_Site4_InternalRESTConfig_ZeroYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	dict := pdfeResolveInternalRC(t, false)
	pdfeAssertNoTruncation(t, dict, "site4", "error", "widget-ns-a")
}

func TestPeerFeedError_Site4_InternalRESTConfig_MultiYield_NoTruncate(t *testing.T) {
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	dict := pdfeResolveInternalRC(t, true)
	pdfeAssertNoTruncation(t, dict, "site4", "error", "")
}

// pdfeInternalRCFixture is a TLS apiserver answering GET-by-name for
// /apis/widgets.krateo.io/v1/namespaces/<ns>/widgets/widget-<ns> with the bare
// widget object (the exact wire shape client-go's dynamic Get reads).
func pdfeInternalRCFixture(t *testing.T) (*rest.Config, func()) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Path: /apis/widgets.krateo.io/v1/namespaces/<ns>/widgets/widget-<ns>
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		name := parts[len(parts)-1]
		ns := ""
		if len(parts) >= 2 {
			ns = parts[len(parts)-3]
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w,
			`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget",`+
				`"metadata":{"namespace":%q,"name":%q}}`, ns, name)
	}))
	caPEM := pemEncodeCert(srv.Certificate())
	rc := &rest.Config{
		Host:            srv.URL,
		BearerToken:     "fake-sa-jwt",
		TLSClientConfig: rest.TLSClientConfig{CAData: caPEM},
	}
	return rc, srv.Close
}

// pdfeResolveInternalRC drives api.Resolve with a TLS apiserver attached via
// cache.WithInternalRESTConfig, so the internal-rest-config dispatch branch
// owns the per-item GET-by-name.
func pdfeResolveInternalRC(t *testing.T, multiYield bool) map[string]any {
	t.Helper()
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	rc, closeSrv := pdfeInternalRCFixture(t)
	t.Cleanup(closeSrv)

	id := "site4"
	stage := pdfeGetByNameStage(id, multiYield)

	downstream := &templates.API{
		Name:      "downstream",
		Path:      `/apis/widgets.krateo.io/v1/namespaces/ns-a/widgets/widget-ns-a`,
		Verb:      ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{Name: id},
		Filter:    ptr.To(".downstream"),
	}

	// The internal-rest-config path IS the SA-credentialed path (the SA
	// *rest.Config is what makes branch C reachable). Drive it under a
	// canonical ServiceAccount identity so branch C's per-user RBAC re-gate
	// (fix/sa-config-fallthrough-rbac-regate) exempts the serve via clause d —
	// the only per-item failure under test here is the FILTER zero-/multi-yield,
	// never an RBAC drop. (An end-user identity would be narrowed and, with no
	// RBAC/SAR grant wired in this hermetic fixture, denied — masking the
	// feed-error path this Site exercises. The narrowing ALLOW/DENY behaviour
	// itself is covered by internal_dispatch_sa_regate_test.go.)
	ctx := pdfeResolveCtx("system:serviceaccount:krateo-system:snowplow")
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	ctx = cache.WithInternalRESTConfig(ctx, rc)
	return Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage, downstream},
		Extras:              map[string]any{"vals": pdfeVals()},
		RESTActionNamespace: "default",
		RESTActionName:      "peer-feed-site4",
	})
}
