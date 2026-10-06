package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/client-go/rest"
)

// discovery504 serves the five groups the #504 stages read. The SHARED
// fakeDiscoveryServer cannot: it knows core/v1 (namespaces, pods, configmaps),
// apps/v1 (deployments) and composition.krateo.io/v1 only — no `services`, no
// `forms`, no `computeinstances`. That gap is not incidental: when the 1.12.17
// baseline for this issue was first captured, three of the five stages came back
// "discovery validation failed" and it looked like a partial pre-existing
// failure, when in fact the harness simply did not serve them. An arm that
// reused the shared helper would assert 2 of 5 and call it a pass.
func discovery504(t *testing.T) *rest.Config {
	t.Helper()
	mux := http.NewServeMux()
	j := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("/apis", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[
		  {"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}},
		  {"name":"acmefeedback.example.io","versions":[{"groupVersion":"acmefeedback.example.io/v1alpha1","version":"v1alpha1"}],"preferredVersion":{"groupVersion":"acmefeedback.example.io/v1alpha1","version":"v1alpha1"}},
		  {"name":"compute.cnrm.cloud.google.com","versions":[{"groupVersion":"compute.cnrm.cloud.google.com/v1beta1","version":"v1beta1"}],"preferredVersion":{"groupVersion":"compute.cnrm.cloud.google.com/v1beta1","version":"v1beta1"}}
		]}`)
	})
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIVersions","versions":["v1"]}`)
	})
	mux.HandleFunc("/api/v1", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[
		  {"name":"namespaces","namespaced":false,"kind":"Namespace","verbs":["get","list"]},
		  {"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]},
		  {"name":"services","namespaced":true,"kind":"Service","verbs":["get","list"]}
		]}`)
	})
	mux.HandleFunc("/apis/apps/v1", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"apps/v1","resources":[
		  {"name":"deployments","namespaced":true,"kind":"Deployment","verbs":["get","list"]}
		]}`)
	})
	mux.HandleFunc("/apis/acmefeedback.example.io/v1alpha1", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"acmefeedback.example.io/v1alpha1","resources":[
		  {"name":"forms","namespaced":true,"kind":"Form","verbs":["get","list"]}
		]}`)
	})
	mux.HandleFunc("/apis/compute.cnrm.cloud.google.com/v1beta1", func(w http.ResponseWriter, r *http.Request) {
		j(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"compute.cnrm.cloud.google.com/v1beta1","resources":[
		  {"name":"computeinstances","namespaced":true,"kind":"ComputeInstance","verbs":["get","list"]}
		]}`)
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return &rest.Config{
		Host:            srv.URL,
		BearerToken:     "fake-sa-jwt",
		TLSClientConfig: rest.TLSClientConfig{CAData: pemEncodeCert(srv.Certificate())},
	}
}

// ra504 is the real blueprint from the issue: five status-projection stages
// produced by the Blueprint Composer, every one interpolating the composition's
// namespace, which does not exist when core-provider calls /rbac.
func ra504() *templates.RESTAction {
	p := func(s string) string { return s }
	return &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{Name: "arch", Path: p(`${ "/api/v1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/configmaps/" + ((.compositionName // .name) // "") + "-architecture" }`)},
				{Name: "o0", Path: p(`${ "/apis/apps/v1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/deployments" }`)},
				{Name: "o1", Path: p(`${ "/api/v1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/services" }`)},
				{Name: "o2", Path: p(`${ "/apis/acmefeedback.example.io/v1alpha1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/forms" }`)},
				{Name: "o3", Path: p(`${ "/apis/compute.cnrm.cloud.google.com/v1beta1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/computeinstances" }`)},
			},
		},
	}
}

// inspectWith504Discovery drives the real endpoint boundary with the #504
// blueprint and the discovery set its five stages need.
func inspectWith504Discovery(t *testing.T) ([]Resource, []Unresolved, error) {
	t.Helper()
	withInspectSARESTConfig(t, discovery504(t))
	return InspectReadSet(context.Background(), ra504(), nil)
}

// TestIssue504_TheRealBlueprintEnumerates is THE falsifier.
//
// It drives InspectReadSet(ctx, ra, nil) — the real endpoint boundary, empty
// dict, NO extras, exactly as core-provider calls it before any composition
// instance exists. RED on origin/main with five
// "stage produced no request path" entries and a 422.
func TestIssue504_TheRealBlueprintEnumerates(t *testing.T) {
	rows, unresolved, err := inspectWith504Discovery(t)
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("#504 RED: every stage must enumerate; unresolved=%+v", unresolved)
	}

	want := []Resource{
		{Group: "", Version: "v1", Resource: "configmaps", NamespaceUnknown: true, Verb: "get"},
		{Group: "", Version: "v1", Resource: "services", NamespaceUnknown: true, Verb: "list"},
		{Group: "acmefeedback.example.io", Version: "v1alpha1", Resource: "forms", NamespaceUnknown: true, Verb: "list"},
		{Group: "apps", Version: "v1", Resource: "deployments", NamespaceUnknown: true, Verb: "list"},
		{Group: "compute.cnrm.cloud.google.com", Version: "v1beta1", Resource: "computeinstances", NamespaceUnknown: true, Verb: "list"},
	}
	if len(rows) != len(want) {
		t.Fatalf("#504: got %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("#504 row %d:\n got  %+v\n want %+v", i, rows[i], want[i])
		}
	}

	// Ruling 2, called out separately because it is the row that would otherwise
	// 403 at the first /call: `arch` is a BY-NAME read whose name interpolated to
	// empty, so it must carry `get`. Rendering it would have produced `list`,
	// which does not authorize a by-name GET.
	for _, r := range rows {
		if r.Resource == "configmaps" && r.Verb != "get" {
			t.Errorf("#504 ruling 2: a by-name read with a collapsed name must be `get`, got %q", r.Verb)
		}
	}
	// Ruling 1: every row says its namespace is unknown rather than pretending to
	// be cluster-scoped.
	for _, r := range rows {
		if !r.NamespaceUnknown {
			t.Errorf("#504 ruling 1: row %+v must be marked NamespaceUnknown", r)
		}
		if r.Namespace != "" {
			t.Errorf("#504: an unknown namespace must not invent a value, got %q", r.Namespace)
		}
	}
}

// TestIssue504_CollapsedResourceStaysUnresolvable is Ruling 3: a render that
// LOOKS like bare discovery but whose template carried a resource segment is a
// collapsed resource, and must fail loud rather than contribute zero rows with a
// 200 (the silent under-grant this file forbids).
func TestIssue504_CollapsedResourceStaysUnresolvable(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{Name: "disco", Path: `${ "/apis/apps/v1/" + (.kind // "") }`},
			},
		},
	}
	rows, unresolved, err := InspectReadSet(context.Background(), ra, nil)
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) == 0 {
		t.Errorf("#504 ruling 3: a collapsed RESOURCE must be UNRESOLVABLE, not zero-rows-200; rows=%+v", rows)
	}
}

// TestIssue504_UnknownNamespaceDoesNotMergeWithClusterScoped pins the dedupe
// half of Ruling 1. Without NamespaceUnknown in the key, a namespaced read with
// an unknown scope is byte-identical to a genuinely cluster-scoped read of the
// same GVR and the two collapse into one row — after which nothing downstream
// can tell them apart.
func TestIssue504_UnknownNamespaceDoesNotMergeWithClusterScoped(t *testing.T) {
	in := []Resource{
		{Group: "", Version: "v1", Resource: "namespaces", Verb: "list"},
		{Group: "", Version: "v1", Resource: "namespaces", NamespaceUnknown: true, Verb: "list"},
	}
	out := dedupeSortResources(in)
	if len(out) != 2 {
		t.Fatalf("#504 ruling 1: an unknown-namespace row must NOT merge with a cluster-scoped row; got %d rows: %+v", len(out), out)
	}
	if out[0].NamespaceUnknown || !out[1].NamespaceUnknown {
		t.Errorf("#504: the sort must be total over NamespaceUnknown (known first); got %+v", out)
	}
}

// TestIssue504_LiteralDiscoveryPathStillEnumeratesCleanly is the ACCEPTANCE arm
// whose absence let a regression ship (#515 gate F1).
//
// Ruling 3's arm only asserted that a COLLAPSED resource becomes unresolvable —
// a one-directional discriminator. Nothing asserted the other direction: that a
// LITERAL discovery path keeps working. It did not: skeletonizeTemplatedPath
// leaves a literal unchanged and the templated() predicate counts an empty
// segment as non-static, so a literal trailing slash ("/apis/apps/v1/", no jq
// anywhere) was flagged as a collapsed resource and 422'd a path that answers
// 200 on main.
//
// A guard that can only fail in one direction is not coverage for the other.
//
// The shape list below carries no QUERY, which is how #520 — the same mistake
// reading a string that still had the query attached — survived this arm. The
// query-bearing shapes live in inspect_520_query_strip_falsifier_test.go.
func TestIssue504_LiteralDiscoveryPathStillEnumeratesCleanly(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	for _, path := range []string{
		"/apis/apps/v1",  // bare group discovery, no trailing slash
		"/apis/apps/v1/", // WITH a trailing slash — the F1 regression shape
		"/api/v1",        // core discovery
		"/api/v1/",       // core, trailing slash
	} {
		ra := &templates.RESTAction{
			Spec: templates.RESTActionSpec{
				API: []*templates.API{{Name: "disco", Path: path}},
			},
		}
		rows, unresolved, err := InspectReadSet(context.Background(), ra, nil)
		if err != nil {
			t.Fatalf("path %q: InspectReadSet errored: %v", path, err)
		}
		if len(unresolved) != 0 {
			t.Errorf("#515 F1 REGRESSION: literal discovery path %q must stay RESOLVABLE "+
				"(it contains no template at all); got unresolved=%+v", path, unresolved)
		}
		if len(rows) != 0 {
			t.Errorf("path %q: a discovery path contributes no read-set rows; got %+v", path, rows)
		}
	}
}

// TestIssue504_PartiallySkippedIteratorKeepsTheRowsThatRendered is the other
// missing arm (#515 gate F2).
//
// An iterator stage expands to one option per element. When one element's path
// fails to render — a jq error, or a surviving ${...} that the #293 guard
// refuses — the rows that DID render must survive. Failing the whole stage threw
// them away: 2 rows + 200 became 0 rows + 422, which is #504's own symptom
// wearing a different shape.
//
// Whether a partially-skipped stage SHOULD fail loud is a fourth ruling nobody
// has made; this arm pins the pre-#504 behaviour until someone does, so the
// question cannot be settled silently by a refactor.
func TestIssue504_PartiallySkippedIteratorKeepsTheRowsThatRendered(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{
				{
					Name: "per-ns",
					// One element renders cleanly; the other carries a literal
					// "${" so its rendered path trips the #293 unrendered-template
					// guard — a NON-emptyInterp skip, which is the class that
					// used to fail the whole stage.
					Path: `${ "/apis/apps/v1/namespaces/" + (.) + "/deployments" }`,
					DependsOn: &templates.Dependency{
						Iterator: ptrStr(".namespaces"),
					},
				},
			},
		},
	}
	rows, unresolved, err := InspectReadSet(context.Background(), ra,
		map[string]any{"namespaces": []any{"ns-a", "${ still-a-template }"}})
	if err != nil {
		t.Fatalf("InspectReadSet errored: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("#515 F2: a stage whose other elements rendered must not be failed whole; unresolved=%+v", unresolved)
	}
	var got []string
	for _, r := range rows {
		if r.Resource == "deployments" {
			got = append(got, r.Namespace)
		}
	}
	if len(got) != 1 || got[0] != "ns-a" {
		t.Errorf("#515 F2 REGRESSION: the element that DID render must survive the one that did not; "+
			"got namespaces %v (rows=%+v)", got, rows)
	}
}
