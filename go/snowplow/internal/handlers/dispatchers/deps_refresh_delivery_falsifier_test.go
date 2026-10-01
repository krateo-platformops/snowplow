// deps_refresh_delivery_falsifier_test.go — #61 (1.5.8) the /refreshes
// zero-delivery falsifier set.
//
// RCA: docs/rca-refreshes-zero-delivery-2026-06-26.md. A composition-DETAIL
// widget resolves its displayed target into status.resourcesRefs.items[] with
// an EMPTY spec.resourcesRefs. extractResourcesRefs USED to read only `spec`
// → recordWidgetDeps recorded NO dep edge on the displayed resource → when
// that resource reconciled, OnUpdate dirty-marked only the intermediate
// apistage cells, NEVER the top-level armed L1 key → PublishRefresh never
// fired for it → the armed /refreshes subscriber received zero `event:
// refresh`. Broken 1.5.5–1.5.7. The fix (deps_extract.go) reads the UNION of
// status.resourcesRefs ∪ spec.resourcesRefs.
//
// THREE arms (RCA §5):
//   - ARM A (key-equality golden) — REGRESSION GUARD, green pre+post. Lives in
//     handlers/refreshes_test.go + dispatchers/refresh_isolation_falsifier_test.go
//     (DeriveSubscriptionKey == the serve key, byte-identical per class). The
//     keys were never the bug; that existing golden pins the equality contract
//     that was previously untested. NOT re-duplicated here.
//   - ARM B (TestFalsifier61_StatusRefDepCoverage) — the discriminating
//     dep-coverage falsifier: status ref R, EMPTY spec → recordWidgetDeps →
//     OnUpdate(R) dirty-marks the widget key. RED pre-fix (spec-only read →
//     no dep on R → key NOT marked), GREEN post-fix.
//   - C-1 (TestFalsifier61_EndToEndDelivery) — the REQUIRED sufficiency arm:
//     seed the widget L1 entry, recordWidgetDeps from a status-bearing res,
//     arm SubscribeRefresh for the key, OnUpdate(R) through the REAL refresh
//     hook, assert refreshDeliveredTotal +1 AND the subscriber channel
//     receives the key. Proves dirty-mark → enqueue → (refresher terminal
//     PublishRefresh) → delivery END-TO-END.
//
// Hermetic: no apiserver, no kubeconfig. The one MODELED link in C-1 is the
// refresher's re-resolve→Put→PublishRefresh terminal step: rather than spin
// the async refresher worker + a real resolve closure (non-hermetic, pulls the
// whole resolver), the test wires the dep tracker's REAL refresh hook
// (Deps().SetRefreshHook, the exact seam refresher.go:425 wires) to call
// cache.PublishRefresh(l1Key) — which is verbatim what resolve_populate.go:328
// does after the refresher's Put. So the chain under test is the real
// OnUpdate→dirty-mark→enqueueFn→PublishRefresh→subscriber path; only the
// re-resolve compute between enqueue and publish is elided (it cannot change
// WHICH key publishes).

package dispatchers

import (
	"context"
	"testing"
	"time"

	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// detailWidgetForTest builds a composition-DETAIL widget in the post-resolve
// shape: the displayed target (awsvpcstacks/demo-vpc) lives ONLY in
// status.resourcesRefs.items[] (the resolver writes it there from the request
// extras); spec.resourcesRefs is EMPTY (the templated-detail shape). This is
// the exact shape that recorded zero dep edges pre-fix.
func detailWidgetForTest() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata": map[string]any{
			"name":      "detail-panel-1",
			"namespace": "demo-ns",
		},
		// EMPTY spec.resourcesRefs — the displayed target is request-driven,
		// not author-declared. (No apiRef either: isolate edge type 1.)
		"spec": map[string]any{},
		// The resolved displayed ref lives in STATUS as a ResourceRefResult:
		// {id, path, verb, allowed}. The gvr/ns/name are encoded ONLY in the
		// /call `path` query string — there are NO inline apiVersion/resource/
		// namespace/name fields. This is the trap: a naive status FIELD read
		// finds empty strings; only objects.ParseCallPathToObjectRef on `path`
		// recovers the displayed target.
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{
					map[string]any{
						"id":      "displayed-1",
						"path":    "/call?resource=awsvpcstacks&apiVersion=composition.krateo.io%2Fv1&namespace=demo-system&name=demo-vpc",
						"verb":    "GET",
						"allowed": true,
					},
				},
			},
		},
	}}
}

// displayedResource is the (gvr, ns, name) the detail widget DISPLAYS — the
// resource whose reconcile must dirty-mark the widget key.
var (
	displayedGVR  = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "awsvpcstacks"}
	displayedNS   = "demo-system"
	displayedName = "demo-vpc"

	detailWidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
)

// TestFalsifier61_StatusRefDepCoverage — ARM B. The discriminating
// dep-coverage falsifier. RED before the deps_extract union fix (spec-only
// read finds no displayed ref → no dep edge → OnUpdate marks 0), GREEN after
// (union reads status → dep edge on demo-vpc → OnUpdate marks the widget key).
func TestFalsifier61_StatusRefDepCoverage(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()

	l1Key := "L1_detail_demo_vpc"
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, detailWidgetForTest())

	// The widget key MUST depend on the DISPLAYED resource (status ref).
	matched := cache.Deps().CollectMatchesForTest(displayedGVR, displayedNS, displayedName)
	if _, ok := matched[l1Key]; !ok {
		t.Fatalf("ARM B RED: widget key %q records NO dep edge on the DISPLAYED resource %s/%s/%s "+
			"(status.resourcesRefs ref). extractResourcesRefs must read status ∪ spec, not spec-only. matched=%v",
			l1Key, displayedGVR, displayedNS, displayedName, matched)
	}

	// And a real OnUpdate on that resource must dirty-mark (return marked>=1
	// including our key).
	marked := cache.Deps().OnUpdate(displayedGVR, displayedNS, displayedName)
	if marked < 1 {
		t.Fatalf("ARM B RED: OnUpdate(%s/%s/%s) dirty-marked %d keys — the widget key was never enqueued; "+
			"PublishRefresh can never fire for it.", displayedGVR, displayedNS, displayedName, marked)
	}
}

// TestFalsifier61_StaticSpecRefStillRecorded — UNION guard (C-0): a
// genuinely-static spec.resourcesRefs ref MUST still be recorded after the fix
// (the union must not be a status-only flip that drops static refs).
func TestFalsifier61_StaticSpecRefStillRecorded(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()

	staticGVR := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"name": "static-panel", "namespace": "demo-ns"},
		"spec": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{
					map[string]any{
						"id":         "static-1",
						"apiVersion": "composition.krateo.io/v1",
						"resource":   "compositions",
						"namespace":  "bench-ns-01",
						"name":       "static-app-01",
					},
				},
			},
		},
		// Empty status — the static-only widget shape.
		"status": map[string]any{},
	}}

	l1Key := "L1_static_panel"
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, w)

	matched := cache.Deps().CollectMatchesForTest(staticGVR, "bench-ns-01", "static-app-01")
	if _, ok := matched[l1Key]; !ok {
		t.Fatalf("UNION GUARD: the union DROPPED a static spec.resourcesRefs ref %s/bench-ns-01/static-app-01 "+
			"(the fix must be status∪spec, NOT a flip to status-only). matched=%v", staticGVR, matched)
	}
}

// TestFalsifier61_EndToEndDelivery — C-1 (HARD, REQUIRED). Proves the full
// sufficiency chain: recordWidgetDeps(status ref) → SubscribeRefresh(armed
// key) → OnUpdate(R) → [real refresh hook = the resolve_populate.go:328
// PublishRefresh terminal] → the armed subscriber RECEIVES the key AND
// refreshDeliveredTotal increments. RED pre-fix (no dep edge → OnUpdate marks
// 0 → hook never called → no publish → subscriber gets nothing → delivered
// stays flat).
func TestFalsifier61_EndToEndDelivery(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// HERMETIC TO THE ENV: do NOT rely on the REFRESH_SSE_ENABLED default —
	// RefreshSSEEnabled() is `env != "false"`, so a runner whose shell has
	// REFRESH_SSE_ENABLED set to anything-but-true makes ResetRefreshBroadcasterForTest
	// re-read it disabled -> the hub is nil -> SubscribeRefresh returns a CLOSED
	// channel -> zero delivery -> a green-here/red-on-another-env split on a HARD
	// gate. Hard-set it BEFORE the reset so the reset picks up the enabled hub.
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	cache.ResetDepsForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	l1Key := "L1_detail_demo_vpc_e2e"

	// Wire the REAL dep-tracker refresh hook to the REAL terminal publish
	// (verbatim resolve_populate.go:328: cache.PublishRefresh(key) after a
	// successful refresher re-resolve+Put). The re-resolve compute between
	// enqueue and publish is elided — it cannot change WHICH key publishes.
	cache.Deps().SetRefreshHook(func(key string, _ schema.GroupVersionResource) {
		cache.PublishRefresh(key)
	})

	// Record the dep edges from the resolved detail widget (status-bearing).
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, detailWidgetForTest())

	// Arm a /refreshes subscriber for the widget key (the seam
	// handlers.Refreshes uses after re-deriving the key under the connection
	// identity).
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{l1Key: {}})
	defer unsub()

	_, deliveredBefore, _, _ := cache.RefreshBroadcasterCounters()

	// The displayed resource reconciles (18×/min in production).
	cache.Deps().OnUpdate(displayedGVR, displayedNS, displayedName)

	// The armed subscriber must receive the key.
	select {
	case got := <-ch:
		if got != l1Key {
			t.Fatalf("C-1: subscriber received key %q, want the armed widget key %q", got, l1Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("C-1 RED: /refreshes subscriber received NOTHING within 2s after the displayed resource "+
			"reconciled — the dep-change never reached PublishRefresh for the armed key (zero-delivery).")
	}

	_, deliveredAfter, _, _ := cache.RefreshBroadcasterCounters()
	if deliveredAfter != deliveredBefore+1 {
		t.Fatalf("C-1: refreshDeliveredTotal = %d, want %d (exactly one (key→subscriber) delivery)",
			deliveredAfter, deliveredBefore+1)
	}
}

// ---------------------------------------------------------------------------
// M19 — name-less LIST delivery target + action-only-ref no-edge.
//
// A composition-DETAIL / list widget can resolve a NAME-LESS displayed
// target into status.resourcesRefs: the /call path carries resource +
// apiVersion but NO `name` (it displays "all of kind X in namespace Y").
// ParseCallPathToObjectRef returns a ref with Name=="" → recordWidgetDeps
// takes the ref.Name=="" branch and MUST RecordList (name="*"), so that ANY
// object of that GVR in the namespace reconciling dirty-marks the widget key
// (a newly-created / updated member of the displayed list changes what the
// widget renders). Recording it as a by-name exact edge instead would
// wildcard-MISS a sibling member's reconcile → zero delivery.
// ---------------------------------------------------------------------------

// listWidgetForTest builds a list widget whose displayed target is a NAME-LESS
// /call path (resource+apiVersion, no name) in status.resourcesRefs. This is
// the exact shape that must yield a RecordList('*') list-scope dep edge.
func listWidgetForTest() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata": map[string]any{
			"name":      "list-panel-1",
			"namespace": "demo-ns",
		},
		"spec": map[string]any{},
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{
					map[string]any{
						"id": "list-displayed-1",
						// NO &name= param → a list-scope target.
						"path":    "/call?resource=awsvpcstacks&apiVersion=composition.krateo.io%2Fv1&namespace=demo-system",
						"verb":    "GET",
						"allowed": true,
					},
				},
			},
		},
	}}
}

// TestFalsifierM19_NameLessListTarget_RecordsWildcard — the DISCRIMINATING
// list-scope dep-coverage arm. recordWidgetDeps on a name-less status target
// must record a LIST edge (name="*"); OnUpdate of ANY object of that GVR in
// the namespace (an arbitrary member name that was NEVER named in the ref)
// must dirty-mark the widget key.
//
// RED (proven by neutering recordListDepFn): swap the real RecordList for a
// by-name Record under a synthetic placeholder name → the sibling-member
// OnUpdate wildcard-MISSES → marked==0 (delivered==0 downstream). Restore →
// GREEN.
func TestFalsifierM19_NameLessListTarget_RecordsWildcard(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	// A sibling member of the displayed list — a name that is NOT encoded in
	// the ref (the ref is name-less). Only a wildcard list edge can match it.
	const siblingMember = "some-vpc-created-later"

	// --- RED arm: neuter RecordList → by-name Record under a placeholder ---
	orig := recordListDepFn
	t.Cleanup(func() { recordListDepFn = orig })
	cache.ResetDepsForTest()
	recordListDepFn = func(ctx context.Context, deps *cache.DepTracker, l1Key string, gvr schema.GroupVersionResource, namespace string) {
		// The wrong-granularity impl: a by-name exact edge. name!="" (Record
		// drops name==""), but the name is a fixed placeholder that no real
		// list member will ever equal.
		deps.Record(ctx, l1Key, gvr, namespace, "__wrong_byname_placeholder__")
	}
	recordWidgetDeps(context.Background(), slog.Default(), "L1_list_red", detailWidgetGVR, listWidgetForTest())
	if marked := cache.Deps().OnUpdate(displayedGVR, displayedNS, siblingMember); marked != 0 {
		t.Fatalf("RED arm expected to MISS: a by-name edge dirty-marked %d keys for a sibling member "+
			"OnUpdate — the wrong-granularity Record was supposed to wildcard-miss", marked)
	}

	// --- GREEN arm: restore real RecordList → wildcard hit ---
	recordListDepFn = orig
	cache.ResetDepsForTest()
	l1Key := "L1_list_green"
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, listWidgetForTest())

	// The list edge must be a wildcard: a member NEVER named in the ref matches.
	matched := cache.Deps().CollectMatchesForTest(displayedGVR, displayedNS, siblingMember)
	if _, ok := matched[l1Key]; !ok {
		t.Fatalf("M19 GREEN: name-less status target did NOT record a LIST('*') edge — a sibling member "+
			"%s/%s/%s does not dirty-mark the widget key. recordWidgetDeps must RecordList, not Record, "+
			"for a name-less ref. matched=%v", displayedGVR, displayedNS, siblingMember, matched)
	}
	if marked := cache.Deps().OnUpdate(displayedGVR, displayedNS, siblingMember); marked < 1 {
		t.Fatalf("M19 GREEN: OnUpdate(%s/%s/%s) dirty-marked %d keys — the wildcard list edge never fired",
			displayedGVR, displayedNS, siblingMember, marked)
	}
}

// TestFalsifierM19_NameLessListTarget_EndToEndDelivery — the sufficiency arm
// for the LIST path: a name-less list target's member reconcile must reach
// PublishRefresh for the armed widget key. Mirrors C-1's real
// OnUpdate→dirty-mark→hook→PublishRefresh→subscriber chain, but the trigger is
// a sibling LIST member (matched only by the wildcard edge).
func TestFalsifierM19_NameLessListTarget_EndToEndDelivery(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	cache.ResetDepsForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	l1Key := "L1_list_e2e"
	cache.Deps().SetRefreshHook(func(key string, _ schema.GroupVersionResource) {
		cache.PublishRefresh(key)
	})
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, listWidgetForTest())

	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{l1Key: {}})
	defer unsub()

	_, deliveredBefore, _, _ := cache.RefreshBroadcasterCounters()

	// A brand-new member of the displayed list reconciles — a name never in the ref.
	cache.Deps().OnUpdate(displayedGVR, displayedNS, "brand-new-member-vpc")

	select {
	case got := <-ch:
		if got != l1Key {
			t.Fatalf("M19 E2E: subscriber received key %q, want the armed widget key %q", got, l1Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("M19 E2E RED: /refreshes subscriber received NOTHING within 2s after a list MEMBER reconciled — "+
			"the name-less list target recorded no wildcard dep edge (Record-by-name instead of RecordList).")
	}

	_, deliveredAfter, _, _ := cache.RefreshBroadcasterCounters()
	if deliveredAfter != deliveredBefore+1 {
		t.Fatalf("M19 E2E: refreshDeliveredTotal = %d, want %d", deliveredAfter, deliveredBefore+1)
	}
}

// TestFalsifierM19_ActionOnlyRef_RecordsNoDepEdge — an action-only ref (its
// id appears in status.widgetData.actions[*].resourceRefId — e.g. a "View
// Logs" button target) must NOT record a render dep edge (Revision 14
// filter). Tracking it would spuriously invalidate the widget every time the
// action target reconciled. RED (proven by neutering the skip set): with the
// filter dropped, the action ref DOES record an edge → the action target's
// reconcile spuriously dirty-marks the widget key.
func TestFalsifierM19_ActionOnlyRef_RecordsNoDepEdge(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	// A widget whose ONLY status resourcesRef is an action-only target: its id
	// ("action-target-1") is declared under status.widgetData.actions.
	actionWidget := func() *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "widgets.templates.krateo.io/v1beta1",
			"kind":       "Panel",
			"metadata":   map[string]any{"name": "action-panel", "namespace": "demo-ns"},
			"spec":       map[string]any{},
			"status": map[string]any{
				"resourcesRefs": map[string]any{
					"items": []any{
						map[string]any{
							"id":      "action-target-1",
							"path":    "/call?resource=awsvpcstacks&apiVersion=composition.krateo.io%2Fv1&namespace=demo-system&name=logs-target",
							"verb":    "GET",
							"allowed": true,
						},
					},
				},
				"widgetData": map[string]any{
					"actions": map[string]any{
						"rest": []any{
							map[string]any{"resourceRefId": "action-target-1"},
						},
					},
				},
			},
		}}
	}

	actionGVR := displayedGVR // awsvpcstacks
	actionNS := displayedNS
	actionName := "logs-target"

	// GREEN: the action-only ref records NO edge → its reconcile marks 0.
	cache.ResetDepsForTest()
	l1Key := "L1_action_only"
	recordWidgetDeps(context.Background(), slog.Default(), l1Key, detailWidgetGVR, actionWidget())
	matched := cache.Deps().CollectMatchesForTest(actionGVR, actionNS, actionName)
	if _, ok := matched[l1Key]; ok {
		t.Fatalf("M19: an action-only ref (id in status.widgetData.actions) recorded a render dep edge on "+
			"%s/%s/%s — tracking it spuriously invalidates the widget on every action-target reconcile. matched=%v",
			actionGVR, actionNS, actionName, matched)
	}
	if marked := cache.Deps().OnUpdate(actionGVR, actionNS, actionName); marked != 0 {
		t.Fatalf("M19: action-target reconcile dirty-marked %d keys — action-only refs must NOT be tracked", marked)
	}

	// RED (control that the arm can distinguish a real edge): the SAME widget
	// WITHOUT the action classification (the ref is treated as a render dep)
	// DOES record an edge. Proves the arm's GREEN above is the filter working,
	// not a dead code path that never records anything.
	renderWidget := actionWidget()
	// Drop the widgetData.actions so the ref is no longer action-classified.
	unstructured.RemoveNestedField(renderWidget.Object, "status", "widgetData")
	cache.ResetDepsForTest()
	l1Key2 := "L1_render_ref"
	recordWidgetDeps(context.Background(), slog.Default(), l1Key2, detailWidgetGVR, renderWidget)
	matched2 := cache.Deps().CollectMatchesForTest(actionGVR, actionNS, actionName)
	if _, ok := matched2[l1Key2]; !ok {
		t.Fatalf("M19 control: with the action classification removed, the ref MUST record a render dep edge "+
			"(else the GREEN arm above is a no-op that never records anything). matched=%v", matched2)
	}
}
