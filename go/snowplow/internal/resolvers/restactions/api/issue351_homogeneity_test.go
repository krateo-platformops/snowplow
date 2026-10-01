// issue351_homogeneity_test.go — #351: cluster-list collapse must derive the
// target GVR from EVERY iterator element, not element 0 only.
//
// DEFECT (pre-fix): deriveTargetGVRForClusterList evaluated the path template
// against the FIRST iterator element only and returned that GVR, assuming GVR
// homogeneity "by construction". A heterogeneous ns-scoped iterator therefore
// collapsed to ONE cluster-wide LIST of element-0's kind and SILENTLY DROPPED
// the rest (incomplete content); lazyRegisterInnerCallPaths then registered an
// informer for that one GVR only, so the dropped kinds' DELETEs went untracked.
//
// FIX (Option A — homogeneity gate): walk all elements; collapse (ok=true) only
// when every element yields the SAME ns-scoped LIST GVR; any divergence → false
// → the existing per-element fan-out (resolve.go) fetches every kind and
// registers every informer.
//
// These are the DERIVE-level arms (the exact locus of the fix). The CONTENT +
// informer-coverage arms (that the dropped kind actually reappears in the served
// result and gets an informer) live in issue351_homogeneity_e2e_test.go.
//
//   A heterogeneous ns-scoped iterator (2 distinct GVRs) → decline (ok=false).
//     RED pre-fix: element-0-only returns (widgets, true).
//   B positive control: homogeneous multi-namespace iterator → still collapses
//     (ok=true, correct GVR). Guards against the fix over-rejecting the shape the
//     collapse exists for. GREEN pre- and post-fix.
//   D divergence on a LATER element (0..4 identical, 5 different) → decline.
//     RED for ANY early-exit / sampling-cap implementation (not just element 0).

package api

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue351_A_HeterogeneousNsScopedIteratorDeclinesCollapse(t *testing.T) {
	// Two elements resolve to DIFFERENT ns-scoped LIST GVRs (same group/version,
	// different resource) — the resource segment is a free jq template, so this
	// is fully expressible (the one-path-edit-away portal-compositions form).
	apiCall := &templates.API{
		Name: "heteroCompositions",
		Path: `${ "/apis/composition.krateo.io/v1/namespaces/" + .ns + "/" + .plural }`,
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[{"ns":"ns1","plural":"widgets"},{"ns":"ns1","plural":"gadgets"}]`),
		},
	}
	gvr, ok := deriveTargetGVRForClusterList(
		context.Background(), clusterListLogger(t), apiCall, map[string]any{})
	if ok {
		t.Fatalf("RED #351: a HETEROGENEOUS ns-scoped iterator (widgets, gadgets) must NOT collapse "+
			"(got ok=true, gvr=%s) — collapsing to ONE cluster-wide LIST of element-0's kind silently "+
			"drops every other kind from the served content. Element-0-only derivation is the bug.", gvr)
	}
}

func TestIssue351_B_HomogeneousMultiNamespaceStillCollapses(t *testing.T) {
	// Same resource across every element; only the namespace varies — the
	// legitimate collapse target (a user's widgets across their namespaces).
	apiCall := &templates.API{
		Name: "widgetsAcrossNamespaces",
		Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`,
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[{"ns":"ns1"},{"ns":"ns2"},{"ns":"ns3"}]`),
		},
	}
	gvr, ok := deriveTargetGVRForClusterList(
		context.Background(), clusterListLogger(t), apiCall, map[string]any{})
	if !ok {
		t.Fatalf("#351 positive control: a HOMOGENEOUS multi-namespace iterator MUST still collapse " +
			"(got ok=false) — the fix must not over-reject the shape the collapse was built for.")
	}
	want := schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1", Resource: "widgets"}
	if gvr != want {
		t.Fatalf("#351 positive control: collapsed gvr=%s want %s", gvr, want)
	}
}

func TestIssue351_D_DivergenceOnLaterElementDeclinesCollapse(t *testing.T) {
	// Elements 0..4 resolve to the SAME GVR (widgets); element 5 diverges
	// (gadgets). An element-0-only or any early-exit/sampling implementation
	// sees only the leading homogeneous run and wrongly collapses — so this arm
	// is RED for them even though arm A's element-1 divergence is not.
	apiCall := &templates.API{
		Name: "divergeLate",
		Path: `${ "/apis/composition.krateo.io/v1/namespaces/" + .ns + "/" + .plural }`,
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[` +
				`{"ns":"ns0","plural":"widgets"},` +
				`{"ns":"ns1","plural":"widgets"},` +
				`{"ns":"ns2","plural":"widgets"},` +
				`{"ns":"ns3","plural":"widgets"},` +
				`{"ns":"ns4","plural":"widgets"},` +
				`{"ns":"ns5","plural":"gadgets"}]`),
		},
	}
	gvr, ok := deriveTargetGVRForClusterList(
		context.Background(), clusterListLogger(t), apiCall, map[string]any{})
	if ok {
		t.Fatalf("RED #351: divergence on a LATER element (element 5 = gadgets) must decline collapse "+
			"(got ok=true, gvr=%s) — the gate must walk ALL elements, not stop at a leading homogeneous "+
			"run. Any early-exit/sampling-cap implementation ships the drop for late-diverging iterators.", gvr)
	}
}

func TestIssue351_E_MixedShapeIteratorDeclinesCollapse(t *testing.T) {
	// Gate bar point 2: the ns-scoped-LIST shape guard (ns!="", name=="") applies
	// to EVERY element, not just element 0. A later element that is cluster-scope
	// or by-name is a shape divergence -> decline; do not collapse a partially-ns
	// iterator. Symmetric with arms A/D (GVR divergence).
	t.Run("later_by_name", func(t *testing.T) {
		apiCall := &templates.API{
			Name: "mixedByName",
			Path: `${ "/apis/g.io/v1/namespaces/" + .ns + "/widgets" + (if .name then "/" + .name else "" end) }`,
			DependsOn: &templates.Dependency{
				Iterator: ptr.To(`[{"ns":"ns1"},{"ns":"ns1","name":"w1"}]`),
			},
		}
		if gvr, ok := deriveTargetGVRForClusterList(context.Background(), clusterListLogger(t), apiCall, map[string]any{}); ok {
			t.Fatalf("RED #351: an iterator mixing a ns-LIST (el0) with a by-name GET (el1) must decline collapse (got ok=true, gvr=%s)", gvr)
		}
	})
	t.Run("later_cluster_scope", func(t *testing.T) {
		apiCall := &templates.API{
			Name: "mixedClusterScope",
			Path: `${ if .ns then "/apis/g.io/v1/namespaces/" + .ns + "/widgets" else "/apis/g.io/v1/widgets" end }`,
			DependsOn: &templates.Dependency{
				Iterator: ptr.To(`[{"ns":"ns1"},{}]`),
			},
		}
		if gvr, ok := deriveTargetGVRForClusterList(context.Background(), clusterListLogger(t), apiCall, map[string]any{}); ok {
			t.Fatalf("RED #351: an iterator mixing a ns-LIST (el0) with a cluster-scope LIST (el1) must decline collapse (got ok=true, gvr=%s)", gvr)
		}
	})
}
