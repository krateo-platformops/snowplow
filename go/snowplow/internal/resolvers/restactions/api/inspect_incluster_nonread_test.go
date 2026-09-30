// inspect_nonread_incluster_arm_test.go — #179 fast-follow (arch nit): the
// in-cluster emit site now sets NonReadVerb by construction too. The in-cluster
// verb is get/list, so it is ALWAYS false today — this arm pins that the
// invariant NonReadVerb == !uafVerbIsRead(Verb) holds at the IN-CLUSTER site
// (not just the UAF site), so a future change that emits a non-read verb there
// cannot ship unflagged. In-process (shares fakeDiscoveryServer); no kind.

package api

import (
	"context"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func TestInspect_InClusterRow_NonReadVerbFalseByConstruction(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{Name: "ns-list", Path: "/api/v1/namespaces"},                           // collection → list
				{Name: "one-pod", Path: "/api/v1/namespaces/krateo-system/pods/my-pod"}, // by-name → get
			},
		},
	}
	rows, unresolved, err := InspectReadSet(context.Background(), ra, nil)
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("expected zero unresolved, got %+v", unresolved)
	}
	if len(rows) == 0 {
		t.Fatalf("expected in-cluster rows, got none")
	}
	for _, r := range rows {
		if r.NonReadVerb {
			t.Fatalf("#179 FAIL: in-cluster row %+v is flagged NonReadVerb, but the in-cluster verb is "+
				"get/list by construction", r)
		}
		// The invariant, row by row: the flag is exactly !uafVerbIsRead(Verb).
		if r.NonReadVerb != !uafVerbIsRead(r.Verb) {
			t.Fatalf("#179 FAIL: invariant broken — row %+v has NonReadVerb=%v but !uafVerbIsRead(%q)=%v",
				r, r.NonReadVerb, r.Verb, !uafVerbIsRead(r.Verb))
		}
	}
}
