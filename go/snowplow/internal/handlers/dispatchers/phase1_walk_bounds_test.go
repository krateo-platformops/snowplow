// phase1_walk_bounds_test.go — unit coverage for the recursion-safety
// bound the Phase 1 widget-tree walker enforces: the recursion-depth
// truncation bound (phase1MaxWalkDepth).
//
// Ship 0.30.127: the per-GVR data-fan-out sample cap
// (phase1PerGVRSampleLimit / phase1Walker.gvrSamples) was DELETED — that
// count-cap heuristic pruned distinct navigation widgets by a
// sibling-count guess and silently starved the Dashboard branch. The
// data-fan-out bound is now the DECLARED per-ref pagination (each widget
// resolves under the `slice` its ref declares, or unpaginated as the browser
// requests it — #966 retired the bounded PREWARM_PAGE_LIMIT default; see
// walk()'s pg). The former
// TestPhase1Walk_PerGVRSampleBound, which pinned that removed heuristic,
// was removed with it.
//
// The depth bound is exercised through the REAL phase1Walker.walk: the
// depth gate returns BEFORE widgets.Resolve is reached, so a direct
// walk(depth > phase1MaxWalkDepth, ...) call hits the cap with no
// cluster contact (widgets.Resolve builds a live apiserver client the
// package's fake dynamic client cannot serve).

package dispatchers

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestPhase1Walk_MaxDepthTruncation asserts the recursion-depth cap. The
// portal navigation tree is shallow (~5 levels); phase1MaxWalkDepth (=32)
// is a defensive guard against a pathological CR graph the visited-set
// fails to dedupe. walk MUST cap — return nil without recursing — once
// depth exceeds the bound, and log phase1.walk.max_depth.
//
// This drives the REAL phase1Walker.walk: the depth gate
// (`if depth > phase1MaxWalkDepth`) returns before widgets.Resolve is
// reached, so the call needs no cluster. A regression that drops or
// loosens the cap would let walk fall through to widgets.Resolve at
// unbounded depth and recurse without limit — and fail here.
func TestPhase1Walk_MaxDepthTruncation(t *testing.T) {
	// Ship 0.30.232: type-safe construction via newPhase1Walker. The
	// depth-cap gate fires before any rc consumer runs, so nil rc is
	// safe here (the constructor accepts nil; runtime safety lives at
	// the consumer sites). See phase1_walker_new.go for the contract.
	w := newPhase1Walker(nil, "krateo-system")
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Widget",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "deep-widget"},
	}}

	// At depth == phase1MaxWalkDepth+1 the depth gate fires: walk returns
	// nil immediately, never touching widgets.Resolve. If the cap were
	// removed walk would call the resolver and fail (no apiserver).
	// Signature (#966): walk(ctx, in, gvr, depth, pg) — one pagination for
	// the resolve and the key; which one the depth-gate test passes is
	// irrelevant since the gate fires before any resolve or cell write.
	if err := w.walk(context.Background(), widget, schema.GroupVersionResource{}, phase1MaxWalkDepth+1, unpaginatedPrewarm); err != nil {
		t.Fatalf("walk past the depth cap must return nil (truncate), got err: %v", err)
	}

	// The cap must not have recursed: nothing visited.
	if len(w.visited) != 0 {
		t.Errorf("depth-capped walk recursed — visited = %v, want empty", w.visited)
	}

	// A deep nil widget must also be safe (defensive).
	if err := w.walk(context.Background(), nil, schema.GroupVersionResource{}, phase1MaxWalkDepth+5, unpaginatedPrewarm); err != nil {
		t.Errorf("walk(nil widget, deep) must return nil, got: %v", err)
	}

	// Sanity-pin the constant: the cap is a small defensive bound, not an
	// accidental unbounded value.
	if phase1MaxWalkDepth != 32 {
		t.Errorf("phase1MaxWalkDepth = %d, want 32 (defensive recursion-safety bound)", phase1MaxWalkDepth)
	}
}
