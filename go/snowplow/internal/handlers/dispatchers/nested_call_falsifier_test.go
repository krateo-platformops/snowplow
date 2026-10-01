// nested_call_falsifier_test.go — falsifiers for the in-process RA/widget
// resolver SEAM (dispatchers.ResolveNestedCall).
//
// THE MECHANISM (post-2026-06-22): the seam resolves a referenced
// RESTAction/Widget CR IN-PROCESS — objects.Get → checkDispatchRBAC →
// restactions.Resolve / widgets.Resolve → encodeResolvedJSON. It is invoked by
// the api resolver's DIRECT-APISERVER-PATH + `resolve: true` branch
// (maybeResolveInProcess). (It was introduced Ship 0.30.123 #155 for the /call
// loopback; that loopback DISPATCH BRANCH + the RESOLVER_INPROCESS_NESTED_CALL
// flag were RETIRED 2026-06-22 — the resolve LOGIC here survived. The former
// loopback-trigger falsifiers F2/F4F6 were retired with the branch; their seam
// properties are preserved by the direct-seam falsifiers below + the
// resolve:true direct-path falsifiers in api/ and inprocess_resolve_falsifier_test.go.)
//
// SURVIVING DIRECT-SEAM FALSIFIERS (drive ResolveNestedCall directly):
//   F1 — the in-process result is the FULL RESTAction envelope
//        {kind,apiVersion,spec,status} — not the bare status (0.30.124 shape).
//   F3 — an authorized resolve returns CORRECT non-empty content; the
//        layer-(b) stage-error sink stays 0.
//   F4 — the depth-8 recursion cap returns a bounded `depth limit exceeded`
//        error with nil bytes (no stack overflow / hang); cap-1 proceeds.
//   F5 — a denied dispatch (identity not RBAC-authorized for the inner CR)
//        surfaces a 403-class error, NOT empty content (the load-bearing
//        in-process RBAC gate).
//
// F3/F5 drive the real ResolveNestedCall against the watcher harness; F1/F4
// drive it directly. No /call api-step path is used (the loopback trigger is
// gone).

package dispatchers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// --- shared fixtures ------------------------------------------------------

// loopbackStage / nestedResolveJWTLess / nestedCallLoopbackPath RETIRED
// 2026-06-22 — they drove the /call?resource= loopback DISPATCH BRANCH (now
// removed, dead code per corpus audit). The seam they exercised
// (ResolveNestedCall) SURVIVES as the in-process resolver behind the
// direct-apiserver-path + resolve:true mechanism, and is tested DIRECTLY by
// the F1/F4_Real/RBAC falsifiers below (which call ResolveNestedCall without a
// /call api-step path) plus the new direct-path resolve:true falsifiers.

// --- F1 — HEADLINE: in-process nested /call returns the FULL envelope ----

// TestF1_NestedCall_ReturnsFullEnvelope is the headline falsifier and the
// Ship 0.30.124 capturable pre-fix→post-fix artifact.
//
// It drives the REAL ResolveNestedCall (no stub) against a seeded inner
// RESTAction whose resolved status is a top-level ARRAY — exactly the
// shape of the real compositions-get-ns-and-crd RESTAction that broke
// 0.30.123.
//
// THE CONTENT-SHAPE CONTRACT: the real HTTP GET /call?resource=restactions
// is routed by the handlers.Dispatcher middleware to restActionHandler
// .ServeHTTP, whose response body is encodeResolvedJSON(res) — the WHOLE
// RESTAction envelope {kind, apiVersion, metadata, spec, status}. A
// consuming stage's `dependsOn.iterator: ".<id>.status"` does `.status`
// on the nested result, so the in-process path MUST deliver that
// envelope, not the bare status.
//
//   - PRE-FIX (0.30.123, return res.Status.Raw): ResolveNestedCall
//     returns the BARE array `[{"ns":"team-a"},...]`. The envelope keys
//     kind/spec are absent, .status would fail on an array — the empty-
//     result defect. This test FAILS on the pre-fix code.
//   - POST-FIX (0.30.124, return encodeResolvedJSON(res)): ResolveNestedCall
//     returns the full envelope; kind/spec/status are all present and the
//     array sits under .status. This test PASSES.
func TestF1_NestedCall_ReturnsFullEnvelope(t *testing.T) {
	const ns, name = "krateo-system", "inner-restaction"
	// Seed the watcher with the ARRAY-STATUS inner RESTAction — the
	// compositions-get-ns-and-crd shape.
	newNestedCallWatcherWithInner(t, ns, name,
		nestedInnerRESTActionArrayStatus(ns, name),
		nestedCallRoleBinding(ns, "sa-prewarmer"))

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "sa-prewarmer"}),
	)
	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err != nil {
		t.Fatalf("F1: JWT-less nested /call must resolve cleanly, got error: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("F1: in-process nested /call produced EMPTY content")
	}

	// The returned bytes MUST be the full RESTAction envelope — the same
	// shape the HTTP /call delivers. Decode and assert the envelope keys.
	var envelope map[string]any
	if uerr := json.Unmarshal(raw, &envelope); uerr != nil {
		t.Fatalf("F1: nested /call result is not a JSON object — got %s "+
			"(0.30.123 returned the bare status array; the fix must return the "+
			"full envelope): %v", raw, uerr)
	}
	for _, key := range []string{"kind", "apiVersion", "spec", "status"} {
		if _, ok := envelope[key]; !ok {
			t.Fatalf("F1 CONTENT-SHAPE DEFECT: nested /call result is missing the "+
				"envelope key %q — ResolveNestedCall returned the BARE status, not "+
				"the full RESTAction envelope. A consuming stage's "+
				"`dependsOn.iterator: \".<id>.status\"` would fail on this shape "+
				"(the 0.30.123 empty-result defect).\n got: %s", key, raw)
		}
	}
	// The inner array content must sit UNDER .status — a stage doing
	// `.status` on this envelope gets the array, exactly as the HTTP path.
	statusBytes, _ := json.Marshal(envelope["status"])
	if !strings.Contains(string(statusBytes), "team-a") {
		t.Fatalf("F1: the inner resolved array must be reachable under .status; "+
			"got status=%s", statusBytes)
	}
}

// --- F2 — the loopback branch feeds the nested bytes through verbatim ----

// TestF2_NestedCall_LoopbackBranchPassesBytesVerbatim asserts the
// resolve.go loopback branch feeds the bytes the nested resolver returned
// into the stage's ResponseHandler EXACTLY — no mutation, no re-wrap.
// This is a unit test of the loopback branch's pass-through; the nested
// resolver itself is stubbed, so the stub returns what the REAL
// TestF2 (LoopbackBranchPassesBytesVerbatim) and TestF4F6
// (DepthLimitBoundedError via a /call api-step) RETIRED 2026-06-22 — both
// drove the /call loopback DISPATCH BRANCH through an api-step path, which was
// removed (dead code, corpus audit). The seam properties they checked —
// verbatim envelope pass-through and the surfaced (non-empty) depth-cap error
// — are preserved by TestF1_NestedCall_ReturnsFullEnvelope and
// TestF4_RealResolveNestedCall_DepthCap (which drive ResolveNestedCall
// directly), plus the new direct-path resolve:true falsifiers.

// TestF4_RealResolveNestedCall_DepthCap drives the REAL ResolveNestedCall
// recursion bound directly: a context already at NestedCallMaxDepth must
// return a bounded `depth limit exceeded` error WITHOUT objects.Get,
// WITHOUT resolving — and never panic / overflow.
func TestF4_RealResolveNestedCall_DepthCap(t *testing.T) {
	ctx := cache.WithNestedCallDepth(context.Background(), cache.NestedCallMaxDepth())
	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: "x", Namespace: "n"},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err == nil {
		t.Fatalf("F4: ResolveNestedCall at the depth cap must return an error, got nil")
	}
	if !strings.Contains(err.Error(), "depth limit exceeded") {
		t.Fatalf("F4: depth-cap error = %q; want a `depth limit exceeded` message", err)
	}
	if raw != nil {
		t.Fatalf("F4: depth-cap must return nil bytes, got %d", len(raw))
	}
}

// TestF4_DepthCapBoundaryBelowCapProceeds asserts a depth ONE BELOW the
// cap does NOT short-circuit on the depth check (it proceeds to
// objects.Get — which then fails hermetically with a fetch error, NOT a
// depth error). This proves the cap is an upper bound, not off-by-one.
func TestF4_DepthCapBoundaryBelowCapProceeds(t *testing.T) {
	ctx := cache.WithNestedCallDepth(context.Background(), cache.NestedCallMaxDepth()-1)
	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: "x", Namespace: "n"},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}
	_, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	// It WILL error (no cluster), but the error must NOT be the depth cap
	// — at depth cap-1 the call must proceed past the depth guard.
	if err != nil && strings.Contains(err.Error(), "depth limit exceeded") {
		t.Fatalf("F4: at depth cap-1 the call must NOT hit the depth limit; got %v", err)
	}
}

// --- F3 / F5 watcher harness ---------------------------------------------

// nestedCallInnerGVR is the inner RESTAction's GVR.
var nestedCallInnerGVR = schema.GroupVersionResource{
	Group:    "templates.krateo.io",
	Version:  "v1",
	Resource: "restactions",
}

// nestedInnerRESTAction builds an unstructured inner RESTAction CR with a
// single trivial api stage (no inner /call — it just resolves to a small
// dict). It is the target of the nested /call.
func nestedInnerRESTAction(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
		"spec": map[string]any{
			// Top-level filter projects a constant — the inner resolve
			// produces deterministic non-empty content with no real
			// apiserver call (the api stage list is empty, so dict is {}
			// and the filter yields the constant object).
			"filter": `{"resolved":true,"name":"` + name + `"}`,
			"api":    []any{},
		},
	}}
}

// nestedInnerRESTActionArrayStatus builds an inner RESTAction whose
// top-level filter yields a top-level JSON ARRAY — modelling the REAL
// compositions-get-ns-and-crd RESTAction, whose resolved status is an
// array. This is the Ship 0.30.124 content-shape fixture: it is exactly
// the shape that broke 0.30.123 (a consuming stage's
// `dependsOn.iterator: ".<id>.status"` does `.status` on the nested
// result; if ResolveNestedCall returns the bare array instead of the
// full envelope, `.status` on an array fails and the result is empty).
func nestedInnerRESTActionArrayStatus(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
		"spec": map[string]any{
			// A filter that yields a top-level ARRAY — restactions.Resolve
			// writes this verbatim into Status.Raw, so the inner resolve's
			// BARE status is `[{"ns":"..."}]`.
			"filter": `[{"ns":"team-a"},{"ns":"team-b"}]`,
			"api":    []any{},
		},
	}}
}

// nestedCallClusterRole grants `get` on templates.krateo.io restactions.
func nestedCallGetRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "nested-call-restaction-getter"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{nestedCallInnerGVR.Group},
				Resources: []string{nestedCallInnerGVR.Resource},
				Verbs:     []string{"get"}},
		},
	}
}

// nestedCallRoleBinding binds user to the getter ClusterRole in ns.
func nestedCallRoleBinding(ns, user string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "nested-call-binding-" + user},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: user},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole",
			Name: "nested-call-restaction-getter",
		},
	}
}

// newNestedCallWatcher builds a cache=on watcher seeded with the DEFAULT
// inner RESTAction CR (nestedInnerRESTAction) plus the RBAC grants. The
// inner restactions informer is registered + synced so objects.Get
// serves it.
func newNestedCallWatcher(t *testing.T, ns, name string, bindings ...*rbacv1.RoleBinding) *cache.ResourceWatcher {
	t.Helper()
	return newNestedCallWatcherWithInner(t, ns, name, nestedInnerRESTAction(ns, name), bindings...)
}

// newNestedCallWatcherWithInner is newNestedCallWatcher with the inner
// RESTAction CR supplied explicitly — so a test can seed a specific
// status shape (e.g. nestedInnerRESTActionArrayStatus for the Ship
// 0.30.124 content-shape falsifier).
func newNestedCallWatcherWithInner(t *testing.T, ns, name string,
	inner *unstructured.Unstructured, bindings ...*rbacv1.RoleBinding) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// #57: informer pivot is implicit under CACHE_ENABLED (RESOLVER_USE_INFORMER retired).
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	seed := []k8sruntime.Object{inner, nestedCallGetRole()}
	for _, b := range bindings {
		seed = append(seed, b)
	}

	scheme := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		nestedCallInnerGVR: "RESTActionList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop() })

	added, syncCh := rw.EnsureResourceType(nestedCallInnerGVR)
	if !added {
		t.Fatalf("EnsureResourceType(restactions): want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("inner restactions informer did not sync")
	}
	syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(syncCtx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	cache.SetGlobal(rw)
	// #385: synchronously publish THIS watcher's RBAC snapshot into the global
	// rbacSnap. WaitForCacheSync only guarantees informer HasSynced, not that this
	// watcher's async initial publish (waitAndPublishInitialRBACSnapshot) has
	// landed — so in the shared package binary the global can still hold a
	// stale/foreign snapshot from a neighbour test (missing this test's seeded
	// bindings) → spurious RBAC deny → the nested resolve falls through to the
	// per-user path → "user *Endpoint not found in context" (#385). Built FROM rw
	// (a leaked neighbour cannot satisfy it) and synchronous (no publish race).
	// The watcher's OWN async initial publish may still land afterwards; that is
	// benign (same rw, same content).
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("#385: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	// #385 post-condition: the live global snapshot now grants each seeded
	// binding's subject — proves the own-publish landed and cleared any stale one.
	for _, b := range bindings {
		for _, s := range b.Subjects {
			opts := rbac.EvaluateOptions{
				Verb: "get", Group: nestedCallInnerGVR.Group,
				Resource: nestedCallInnerGVR.Resource, Namespace: b.Namespace,
			}
			if s.Kind == rbacv1.GroupKind {
				opts.Groups = []string{s.Name}
			} else {
				opts.Username = s.Name
			}
			if allowed, _, err := rbac.EvaluateRBAC(context.Background(), opts); err != nil || !allowed {
				t.Fatalf("#385 post-condition: after own-publish the global RBAC snapshot must grant %s %q get %s in ns %q (allowed=%v err=%v) — stale/foreign snapshot not cleared",
					s.Kind, s.Name, nestedCallInnerGVR.Resource, b.Namespace, allowed, err)
			}
		}
	}
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// --- F5 — denied dispatch surfaces an error, NOT empty -------------------

// TestF5_NestedCall_DeniedDispatchIsErrorNotEmpty drives the REAL
// ResolveNestedCall for an identity with NO RBAC grant for the inner
// RESTAction. The dispatch MUST fail closed — ResolveNestedCall returns
// an ERROR and nil bytes, NEVER empty-but-valid content (which would
// mask the denial). Two fail-closed gates can catch it:
//   - objects.Get's informer-serve filterGetByRBAC (Tag 0.30.101) — for
//     a denied user the informer GET is refused and, with no user
//     Endpoint to fall through to, objects.Get returns a fetch error;
//   - if the GET somehow succeeded, ResolveNestedCall's own
//     checkDispatchRBAC (the load-bearing in-process RBAC gate) denies
//     with an explicit "forbidden" error.
//
// Either way the contract holds: a denied nested /call is an error with
// nil bytes — never silent empty content.
func TestF5_NestedCall_DeniedDispatchIsErrorNotEmpty(t *testing.T) {
	const ns, name = "krateo-system", "inner-restaction"
	// NO RoleBinding for "denied-user" — the inner RESTAction exists but
	// the identity has no `get` grant.
	newNestedCallWatcher(t, ns, name)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "denied-user"}),
	)
	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err == nil {
		t.Fatalf("F5: a denied nested /call must FAIL CLOSED with an ERROR — "+
			"got nil error with %d bytes (the denial was masked as content)", len(raw))
	}
	if raw != nil {
		t.Fatalf("F5: a denied nested /call must return nil bytes, got %d — "+
			"NEVER empty-but-valid content (that masks the RBAC denial)", len(raw))
	}
	// The error must be a denial / fetch-failure — NOT a resolve that
	// produced empty content. (In a real cluster the apiserver fall-through
	// surfaces a literal 403; hermetically the informer-serve RBAC refusal
	// + absent user Endpoint surfaces a fetch error — both are fail-closed.)
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "forbidden") && !strings.Contains(low, "fetch") &&
		!strings.Contains(low, "endpoint") {
		t.Fatalf("F5: denied dispatch error = %q; want a fail-closed denial / "+
			"fetch-failure error, not a resolve that swallowed the denial", err)
	}
}

// TestF5_RealCheckDispatchRBAC_DeniesUnauthorized is the focused
// companion to F5: it asserts the load-bearing checkDispatchRBAC gate
// itself denies an unauthorized identity. ResolveNestedCall calls this
// gate AFTER objects.Get; this test exercises the gate directly so the
// "single most important correctness line" is covered even though, in
// the hermetic objects.Get path, the informer-serve RBAC filter denies
// first.
func TestF5_RealCheckDispatchRBAC_DeniesUnauthorized(t *testing.T) {
	const ns, name = "krateo-system", "inner-restaction"
	newNestedCallWatcher(t, ns, name, nestedCallRoleBinding(ns, "authorized-user"))

	deniedCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "denied-user"}),
	)
	if checkDispatchRBAC(deniedCtx, nestedCallInnerGVR, ns) {
		t.Fatalf("F5: checkDispatchRBAC ALLOWED an identity with no `get` grant — " +
			"the in-process nested /call RBAC gate is the single load-bearing " +
			"correctness line and must deny")
	}
	authedCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "authorized-user"}),
	)
	if !checkDispatchRBAC(authedCtx, nestedCallInnerGVR, ns) {
		t.Fatalf("F5: checkDispatchRBAC DENIED an identity that holds the `get` grant")
	}
}

// --- F3 — exportJwt RESTAction refreshes with correct non-empty content --

// TestF3_NestedCall_AuthorizedResolveNonEmpty drives the REAL
// ResolveNestedCall for an AUTHORIZED identity and asserts it returns
// CORRECT, NON-EMPTY content (not (nil,nil) skip-to-TTL, not empty). This
// is the F2-unblock property: a JWT-less / SA-credentialed nested /call
// of an (exportJwt) RESTAction now resolves real content. It also
// confirms the layer-(b) stage-error sink stays 0 — a clean resolve
// records no stage error, so the refresher's error-aware Put-gate would
// NOT decline the Put (the backstop is intact but does not false-fire).
func TestF3_NestedCall_AuthorizedResolveNonEmpty(t *testing.T) {
	const ns, name = "krateo-system", "inner-restaction"
	newNestedCallWatcher(t, ns, name, nestedCallRoleBinding(ns, "authorized-user"))

	// A stage-error sink on ctx — the layer-(b) seam. A clean nested
	// resolve must leave it at 0.
	base := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "authorized-user"}),
	)
	ctx, sink := cache.WithStageErrorSink(base)

	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err != nil {
		t.Fatalf("F3: authorized nested /call must resolve cleanly, got error: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("F3: authorized nested /call produced EMPTY content — the F2-unblock " +
			"property requires real non-empty content, not a skip-to-TTL")
	}
	// Ship 0.30.124 content-shape contract: the result MUST be the full
	// RESTAction envelope, and the inner resolved content sits under
	// .status — exactly as the HTTP /call delivers it.
	var envelope map[string]any
	if uerr := json.Unmarshal(raw, &envelope); uerr != nil {
		t.Fatalf("F3: nested /call result is not a JSON object (full envelope): %v\n got %s",
			uerr, raw)
	}
	for _, key := range []string{"kind", "apiVersion", "spec", "status"} {
		if _, ok := envelope[key]; !ok {
			t.Fatalf("F3: nested /call result missing envelope key %q — must return "+
				"the full RESTAction envelope, not the bare status; got %s", key, raw)
		}
	}
	statusBytes, _ := json.Marshal(envelope["status"])
	if !strings.Contains(string(statusBytes), "resolved") {
		t.Fatalf("F3: the inner resolved content must be reachable under .status; "+
			"got status=%s", statusBytes)
	}
	// Layer (b): a clean resolve records NO stage error.
	if got := sink.Count(); got != 0 {
		t.Fatalf("F3: layer-(b) stage-error sink = %d after a CLEAN nested resolve; "+
			"want 0 (the error-aware Put-gate must not false-fire on a good resolve)", got)
	}
}

// --- RC-2 / RC-5 / RC-6 — #79 resolve-cycle self-reference cycle-stop --------

// TestRC2_SelfReferenceReturnsRawCR is the #79 crux RED arm. When the target
// node is ALREADY on the descent path (self-reference: A resolve:true-refs A),
// ResolveNestedCall must STOP the cycle by returning the RAW CR (resolve:false
// semantics) — NOT recurse into restactions.Resolve, NOT hit the depth-8 error.
// We seed an inner RA whose recursive resolve would yield a distinct
// (filter-projected) status, mark its node as an ancestor on ctx, and assert
// the returned bytes are the RAW CR (its status is the UNRESOLVED spec.filter
// string, not the resolved projection) with NO error and NO depth-limit.
//
// This is the on-cluster fsa-yN-composition-resources self-reference that
// exploded 250×/composition on 1.5.20 (#77 re-enabled the recursion). Neuter
// the cycle-stop (remove the NestedResolveAncestorPresent branch) → this RA
// recurses to the depth-8 backstop instead of stopping at reentry.
func TestRC2_SelfReferenceReturnsRawCR(t *testing.T) {
	const ns, name = "demo-system", "fsa-y7-composition-resources"
	// Inner RA with an ARRAY-yielding filter — a RECURSIVE resolve would write
	// the projected array into status; the RAW CR's status carries no such
	// projection (spec.filter is the literal string). So raw-vs-resolved is
	// distinguishable.
	newNestedCallWatcherWithInner(t, ns, name,
		nestedInnerRESTActionArrayStatus(ns, name),
		nestedCallRoleBinding(ns, "authorized-user"))

	base := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "authorized-user"}),
	)
	// Mark THIS node as already-an-ancestor — i.e. we are resolving A from
	// within A's own resolve chain (the self-reference). node identity is the
	// FETCHED GVR.Resource/ns/name (matches ResolveNestedCall's construction).
	node := nestedCallInnerGVR.Resource + "/" + ns + "/" + name
	ctx := cache.WithNestedResolveAncestor(base, node)

	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err != nil {
		t.Fatalf("RC-2: self-reference must cycle-STOP cleanly (raw CR), got error: %v", err)
	}
	if strings.Contains(err2s(err), "depth limit exceeded") {
		t.Fatalf("RC-2: self-reference must stop at reentry, NOT recurse to the depth-8 backstop")
	}
	if len(raw) == 0 {
		t.Fatalf("RC-2: cycle-stop must return the raw CR, got empty")
	}
	var envelope map[string]any
	if uerr := json.Unmarshal(raw, &envelope); uerr != nil {
		t.Fatalf("RC-2: cycle-stop result is not a JSON object: %v\n got %s", uerr, raw)
	}
	// RAW CR: the status must NOT carry the resolved array projection — the raw
	// object has spec.filter as a literal string and an unresolved/empty status.
	// The resolved form would put the array under .status; the raw form does
	// not. Assert the spec.filter literal survived (raw), i.e. we returned the
	// object as fetched.
	specBytes, _ := json.Marshal(envelope["spec"])
	if !strings.Contains(string(specBytes), "team-a") {
		t.Fatalf("RC-2: expected the RAW CR (spec.filter literal intact), got spec=%s", specBytes)
	}
	statusBytes, _ := json.Marshal(envelope["status"])
	if strings.Contains(string(statusBytes), "team-a") {
		t.Fatalf("RC-2: status carries the RESOLVED projection — the cycle did NOT stop "+
			"(it recursed and resolved instead of returning the raw CR); status=%s", statusBytes)
	}
}

// TestRC5_CycleStopStillEnforcesRBAC — the cycle-stop happens AFTER
// objects.Get + checkDispatchRBAC (RC-5: stop != bypass). An UNAUTHORIZED
// identity hitting a self-reference must STILL get a denial error, never the raw
// CR. (Proves the stop did not short-circuit ahead of the RBAC gate.)
func TestRC5_CycleStopStillEnforcesRBAC(t *testing.T) {
	const ns, name = "demo-system", "fsa-y7-composition-resources"
	// No RoleBinding for "denied-user" → objects.Get / checkDispatchRBAC denies.
	newNestedCallWatcherWithInner(t, ns, name,
		nestedInnerRESTActionArrayStatus(ns, name))

	base := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "denied-user"}),
	)
	node := nestedCallInnerGVR.Resource + "/" + ns + "/" + name
	ctx := cache.WithNestedResolveAncestor(base, node) // ancestor present (would cycle-stop)

	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
	if err == nil {
		t.Fatalf("RC-5: an UNAUTHORIZED self-reference must be DENIED (error), not " +
			"cycle-stopped to the raw CR — the stop must sit AFTER the RBAC gate")
	}
	if raw != nil {
		t.Fatalf("RC-5: denied cycle-stop must return nil bytes, got %d", len(raw))
	}
}

func err2s(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
