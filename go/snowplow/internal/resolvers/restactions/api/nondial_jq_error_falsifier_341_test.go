//go:build unit
// +build unit

// nondial_jq_error_falsifier_341_test.go — #341 falsifiers for the two NON-dial
// evalJQ-swallow sites now routed through evalJQE + SURFACED (a per-site counter
// + DEBUG). Per the #341 framing: assert the jq error is SURFACED/COUNTED and
// the non-dial fail-safe behaviour is UNCHANGED (no collapse / a byte-identical
// name-miss) — NOT that a bad dial is prevented (these sites never dial).
package api

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func nondial341Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestFalsifier341_ClusterListGVRProbe_JQError_Surfaced — a jq error in the
// cluster_list GVR-derivation probe path must SURFACE (bump the per-site
// counter); the fail-safe stays (no collapse, derivedOK=false). A valid probe
// path does NOT bump (discrimination). RED pre-#341: evalJQ swallowed it → the
// counter stays 0.
func TestFalsifier341_ClusterListGVRProbe_JQError_Surfaced(t *testing.T) {
	ctx := context.Background()
	log := nondial341Logger()
	dict := map[string]any{}

	// Bad: the path jq expression errors (divide by zero). Self-contained literal
	// iterator (matches the cluster_list test convention) → firstElement is
	// {"ns":"team-a"}, so the probe reaches the path eval where the jq error fires.
	badStage := &templates.API{
		Name:      "cluster_list_jq_error",
		Path:      "${1/0}",
		DependsOn: &templates.Dependency{Iterator: ptr.To(`[{"ns":"team-a"}]`)},
	}
	before := NondialJQErrorByReason(nondialClusterListGVRProbe)
	_, derivedOK := deriveTargetGVRForClusterList(ctx, log, badStage, dict)

	if derivedOK {
		t.Fatalf("#341 fail-safe: a jq-error probe must NOT collapse (derivedOK=true) — behaviour must stay no-collapse")
	}
	if d := NondialJQErrorByReason(nondialClusterListGVRProbe) - before; d != 1 {
		t.Fatalf("#341 (RED pre-fix): a jq error in the cluster_list GVR probe must SURFACE — bump %q by 1; got delta %d (pre-#341 evalJQ swallowed it silently)", nondialClusterListGVRProbe, d)
	}

	// Control: a VALID namespace-scoped probe path does not bump; it collapses.
	okStage := &templates.API{
		Name:      "cluster_list_ok",
		Path:      `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`,
		DependsOn: &templates.Dependency{Iterator: ptr.To(`[{"ns":"team-a"}]`)},
	}
	beforeOK := NondialJQErrorByReason(nondialClusterListGVRProbe)
	_, okDerived := deriveTargetGVRForClusterList(ctx, log, okStage, dict)
	if !okDerived {
		t.Fatalf("#341 control: a valid namespace-scoped probe path must collapse (derivedOK=true); got false")
	}
	if d := NondialJQErrorByReason(nondialClusterListGVRProbe) - beforeOK; d != 0 {
		t.Fatalf("#341 control: a VALID probe path must NOT bump the jq-error counter; got delta %d", d)
	}
}

// TestFalsifier341_EndpointRefName_JQError_Surfaced — a jq error in a templated
// endpointRef.name must SURFACE (bump the per-site counter) while the behaviour
// stays BYTE-IDENTICAL: the §1.2 honest-error posture — the same sanitized name
// (from the error string) → a downstream Secret miss, no new error class. A
// valid template does NOT bump.
func TestFalsifier341_EndpointRefName_JQError_Surfaced(t *testing.T) {
	r := newResolveRun(context.Background(), ResolveOptions{}, nondial341Logger(), jwtutil.UserInfo{})

	before := NondialJQErrorByReason(nondialEndpointRefName)
	got, templated, err := r.evalEndpointRef(&templates.Reference{Name: "${1/0}"})

	if err != nil {
		t.Fatalf("#341 honest-error posture: a jq-error name must NOT become a new error class; got err=%v", err)
	}
	if !templated {
		t.Fatalf("#341: a templated name must report templated=true")
	}
	if d := NondialJQErrorByReason(nondialEndpointRefName) - before; d != 1 {
		t.Fatalf("#341 (RED pre-fix): a jq error in a templated endpointRef.name must SURFACE — bump %q by 1; got delta %d (pre-#341 evalJQ swallowed it silently)", nondialEndpointRefName, d)
	}
	// Byte-identical: the returned name is the SAME sanitized error-string name as
	// pre-#341 (MakeDNS1123Compatible(evalJQ(...))) — pure observability, no
	// behaviour change.
	wantName := kubeutil.MakeDNS1123Compatible(evalJQ("${1/0}", r.dict))
	if got == nil || got.Name != wantName {
		t.Fatalf("#341 byte-identical: the errored name must equal the pre-#341 sanitized value %q; got %+v", wantName, got)
	}

	// Control: a VALID templated name does not bump; resolves normally.
	beforeOK := NondialJQErrorByReason(nondialEndpointRefName)
	gotOK, _, errOK := r.evalEndpointRef(&templates.Reference{Name: `${ "resolved-name" }`})
	if errOK != nil {
		t.Fatalf("#341 control: a valid templated name must resolve; got err=%v", errOK)
	}
	if d := NondialJQErrorByReason(nondialEndpointRefName) - beforeOK; d != 0 {
		t.Fatalf("#341 control: a VALID templated name must NOT bump the jq-error counter; got delta %d", d)
	}
	if gotOK == nil || gotOK.Name != "resolved-name" {
		t.Fatalf("#341 control: valid name must resolve to \"resolved-name\"; got %+v", gotOK)
	}
}
