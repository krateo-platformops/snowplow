// inspect_nonread_flag_falsifier_test.go — #179: InspectReadSet must not
// amplify a non-read userAccessFilter verb into a Role suggestion silently.
//
// THE DEFECT (#179, A-4 review). inspectUAFStage copies uaf.Verb verbatim into
// the read-set row (inspect.go). For a RESTAction written straight to etcd
// (bypassing admission and the 1.12.3 warn-only guard), a UAF verb like
// `create`/`deletecollection` becomes a suggested Role granting a WRITE verb —
// the endpoint amplifies a bad verb into a Role suggestion.
//
// THE FIX — FLAG, NOT BOUND. We keep the verb verbatim (bounding it to
// get/list/watch would SILENTLY corrupt the read-set for the three LIVE portal
// pickers that use `verb: create` legitimately — refilter.go:77-85 documents
// why non-read UAF verbs are deliberately warn-only, not rejected) and add a
// `nonReadVerb: true` flag, computed from the SAME single-source predicate the
// refilter warn uses (uafVerbIsRead, refilter.go:103). The caller (core-provider)
// can then refuse to mint a Role for a flagged row without snowplow either
// amplifying the verb OR corrupting the read-set.
//
// Falsifier-FIRST (feedback_falsifier_first_before_ship): written BEFORE the
// inspect.go change. It does not compile against the unfixed tree (Resource has
// no NonReadVerb field) — the RED-for-the-right-reason signal that the flag is
// absent. In-process / httptest only (shares fakeDiscoveryServer); NEVER a
// remote kubeconfig.

package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// TestInspect_UAFNonReadVerb_Flagged is the DISCRIMINATING falsifier: a UAF
// stage with a non-read verb (deletecollection) emits the verb VERBATIM and
// sets NonReadVerb=true. A read-only emit, a bounded verb, or a missing flag
// FAILS.
func TestInspect_UAFNonReadVerb_Flagged(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "ns",
					Path: "/api/v1/namespaces",
					UserAccessFilter: &templates.UserAccessFilterSpec{
						Verb:     "deletecollection",
						Group:    "",
						Resource: "namespaces",
					},
				},
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
	row, ok := findRow(rows, "", "namespaces", "deletecollection")
	if !ok {
		t.Fatalf("#179 FAIL: the UAF verb must still be emitted VERBATIM (deletecollection) — "+
			"bounding it would corrupt the read-set for legit `verb: create` portal pickers; got %+v", rows)
	}
	if !row.NonReadVerb {
		t.Fatalf("#179 FAIL: a non-read UAF verb (deletecollection) must set NonReadVerb=true so the "+
			"caller can refuse to mint a write-granting Role; got %+v", row)
	}
}

// TestInspect_UAFReadVerb_NotFlagged is the NEGATIVE CONTROL: a UAF stage with
// a read verb (watch) must NOT set the flag — the fix must not over-flag.
func TestInspect_UAFReadVerb_NotFlagged(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "apps",
					Path: "/apis/composition.krateo.io/v1/fireworksapps",
					UserAccessFilter: &templates.UserAccessFilterSpec{
						Verb:     "watch",
						Group:    "composition.krateo.io",
						Resource: "fireworksapps",
					},
				},
			},
		},
	}
	rows, _, err := InspectReadSet(context.Background(), ra, nil)
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	row, ok := findRow(rows, "composition.krateo.io", "fireworksapps", "watch")
	if !ok {
		t.Fatalf("expected a watch row for fireworksapps; got %+v", rows)
	}
	if row.NonReadVerb {
		t.Fatalf("#179 FAIL: a read UAF verb (watch) must NOT set NonReadVerb; got %+v", row)
	}
}

// TestInspect_NonReadVerb_JSONShape pins the wire contract: a non-read row
// carries "nonReadVerb":true, and a read row OMITS the field entirely
// (omitempty) so every existing (read-verb) response stays byte-identical.
func TestInspect_NonReadVerb_JSONShape(t *testing.T) {
	nonRead := Resource{Group: "", Resource: "namespaces", Verb: "deletecollection", NonReadVerb: true}
	b, err := json.Marshal(nonRead)
	if err != nil {
		t.Fatalf("marshal non-read row: %v", err)
	}
	if !strings.Contains(string(b), `"nonReadVerb":true`) {
		t.Fatalf("#179 FAIL: a non-read row must serialize nonReadVerb:true; got %s", b)
	}

	read := Resource{Group: "", Resource: "pods", Verb: "list"}
	b, err = json.Marshal(read)
	if err != nil {
		t.Fatalf("marshal read row: %v", err)
	}
	if strings.Contains(string(b), "nonReadVerb") {
		t.Fatalf("#179 FAIL: a read row must OMIT nonReadVerb (omitempty) so existing responses "+
			"stay byte-identical; got %s", b)
	}
}

// TestInspect_NonReadVerbFlag_SurvivesDedupe guards the dedupe trap:
// dedupeSortResources rebuilds each row from a field-by-field key literal, so a
// new field is silently dropped unless it is added to that literal. Two stages
// that both emit the same non-read UAF row collapse to ONE row that MUST still
// carry NonReadVerb=true.
func TestInspect_NonReadVerbFlag_SurvivesDedupe(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "ns-a",
					Path: "/api/v1/namespaces",
					UserAccessFilter: &templates.UserAccessFilterSpec{
						Verb: "deletecollection", Group: "", Resource: "namespaces",
					},
				},
				{
					Name: "ns-b",
					Path: "/api/v1/namespaces",
					UserAccessFilter: &templates.UserAccessFilterSpec{
						Verb: "deletecollection", Group: "", Resource: "namespaces",
					},
				},
			},
		},
	}
	rows, _, err := InspectReadSet(context.Background(), ra, nil)
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	var count int
	for _, r := range rows {
		if r.Group == "" && r.Resource == "namespaces" && r.Verb == "deletecollection" {
			count++
			if !r.NonReadVerb {
				t.Fatalf("#179 FAIL: the deduped non-read row lost its NonReadVerb flag "+
					"(dedupeSortResources must carry it in the key literal); got %+v", r)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one deduped namespaces/deletecollection row, got %d (rows=%+v)", count, rows)
	}
}
