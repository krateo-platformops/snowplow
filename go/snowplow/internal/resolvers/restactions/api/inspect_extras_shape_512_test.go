package api

import (
	"context"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// TestInspect512_ExtrasAreSeededAtTopLevelLikeTheDispatcher — the #512
// falsifier.
//
// WHY THE EXISTING ARMS COULD NOT CATCH THIS. InspectReadSet used to seed
// `dict["extras"] = extras` while the dispatcher seeds
// `dict = maps.DeepCopyJSON(opts.Extras)` — extras at TOP LEVEL. The package's
// own arms read `.extras.namespaces` / `.extras.plurals`, i.e. they agreed with
// the implementation and disagreed with production, so the divergence was
// invisible to the suite. The consequence was not cosmetic: NO templated path
// had ever rendered a real value through this pass, because every author-written
// template reads extras at top level.
//
// This arm drives the shape of the REAL in-repo fixture,
// testdata/widgets/button.extras.apiref.yaml:26:
//
//	path: ${ "/api/v1/namespaces/" + .nsName }
//
// RED before the fix: `.nsName` against {"extras":{"nsName":"demo-system"}} is
// null, so the path renders "/api/v1/namespaces/" and the row that comes back is
// the cluster-scoped `list namespaces` — NOT the by-name read the dispatcher
// would issue. The assertion below is on the NAMESPACE the row carries, which is
// the bit that moves.
func TestInspect512_ExtrasAreSeededAtTopLevelLikeTheDispatcher(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "pods-in-the-extras-namespace",
					// Top-level `.nsName`, exactly as an author writes it and as
					// the dispatcher serves it. `pods` (not `namespaces`) so the
					// assertion cannot be satisfied by the collapsed-path row.
					Path: `${ "/api/v1/namespaces/" + .nsName + "/pods" }`,
				},
			},
		},
	}

	rows, unresolved, err := InspectReadSet(context.Background(), ra, map[string]any{"nsName": "demo-system"})
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("#512: a top-level extras template must be materializable, got unresolved %+v", unresolved)
	}

	var got *Resource
	for i := range rows {
		if rows[i].Resource == "pods" {
			got = &rows[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("#512 RED: no `pods` row — the extras template did not render. rows=%+v", rows)
	}
	if got.Namespace != "demo-system" {
		t.Errorf("#512 RED: the row carries namespace %q, want %q — extras were not seeded at top level, "+
			"so the read-set was computed against a path the dispatcher would never request",
			got.Namespace, "demo-system")
	}
	if got.Verb != "list" {
		t.Errorf("#512: a nameless collection path must enumerate as `list`, got %q", got.Verb)
	}
}

// TestInspect512_SliceInTheQueryIsHarmlessInThePath pins the deliberate
// NON-parity with the dispatcher, and pins it to what actually happens rather
// than to what I first assumed.
//
// The dispatcher seeds dict["slice"] from the paging parameters; /rbac takes
// none, so `.slice.*` is absent here. My first version of this arm asserted the
// stage must therefore be UNRESOLVABLE. It is not, and should not be: a
// `.slice.*` reference in the QUERY renders the literal "null", the parser
// strips the query before classifying, and the stage enumerates its resource
// correctly. That is the right outcome — a read-set is per-RESOURCE, not
// per-page, so the limit is irrelevant to what RBAC must grant.
//
// What this arm therefore protects is that NOT seeding slice costs the read-set
// nothing on the query path, so nobody "fixes" it later by inventing paging
// values at enumeration time.
func TestInspect512_SliceInTheQueryIsHarmlessInThePath(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "paged",
					Path: `${ "/api/v1/namespaces/" + .nsName + "/pods?limit=" + (.slice.perPage | tostring) }`,
				},
			},
		},
	}

	rows, unresolved, err := InspectReadSet(context.Background(), ra, map[string]any{"nsName": "demo-system"})
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("#512: an unseeded .slice.* in the QUERY must not make a stage unresolvable "+
			"(the query is stripped before classification; a read-set is per-resource): %+v", unresolved)
	}
	found := false
	for _, r := range rows {
		if r.Resource == "pods" && r.Namespace == "demo-system" && r.Verb == "list" {
			found = true
		}
	}
	if !found {
		t.Errorf("#512: the paged stage must still enumerate {pods, demo-system, list}; rows=%+v", rows)
	}
}
