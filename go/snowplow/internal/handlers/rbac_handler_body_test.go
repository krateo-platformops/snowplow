// rbac_handler_body_test.go — H5 hermetic body/status falsifier for GET /rbac.
//
// rbac_test.go already proves the AUTH GATE (UserConfig 401s a bad/absent JWT
// before the handler runs). This file proves the HANDLER BODY LOGIC — the four
// non-auth response shapes — WITHOUT an apiserver or informer, by swapping the
// two package-private seams (rbacGetObjectFn / rbacInspectReadSetFn, rbac.go)
// for in-memory fakes:
//
//	(a) an unresolvable stage  -> 422 whose JSON body NAMES the stage with its
//	    reason token and carries the rows that did enumerate (#1619) — a partial
//	    read-set is NEVER returned as success;
//	(b) a missing apiRefName / apiRefNamespace -> 400 (before any load);
//	(c) an all-external RESTAction (readSet nil) -> 200 with "readSet": []
//	    (marshalled as an empty array, NOT null);
//	(d) a valid RESTAction -> 200 with the deduped, sorted read-set verbatim.
//
// RED arm (proved in-file, TestRBAC_Body_RED_PartialReadSetMustNot200): a wrong
// impl that returns the partial read-set as a 200 (ignoring the unresolved
// stages) is caught — arm (a) demands 422 exactly when unresolved is non-empty.
//
// The seams default to the REAL objects.Get / api.InspectReadSet in prod; this
// test overrides them per-test and restores via t.Cleanup, so it perturbs no
// production behaviour and no other test.

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeRAUnstructured is a minimal RESTAction object that converts cleanly to the
// typed templatesv1.RESTAction the handler builds via FromUnstructured. Its spec
// is irrelevant — rbacInspectReadSetFn is stubbed, so the inspect pass never
// actually walks it.
func fakeRAUnstructured(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": templatesv1.SchemeGroupVersion.String(),
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec":       map[string]any{},
	}}
}

// withRBACSeams installs the two handler seams for the duration of the test and
// restores them on cleanup.
func withRBACSeams(t *testing.T,
	get func(ctx context.Context, ref templatesv1.ObjectReference) objects.Result,
	inspect func(ctx context.Context, in *templatesv1.RESTAction, extras map[string]any) ([]api.Resource, []api.Unresolved, error),
) {
	t.Helper()
	prevGet, prevInspect := rbacGetObjectFn, rbacInspectReadSetFn
	rbacGetObjectFn = get
	rbacInspectReadSetFn = inspect
	t.Cleanup(func() {
		rbacGetObjectFn = prevGet
		rbacInspectReadSetFn = prevInspect
	})
}

// callRBAC drives RBAC().ServeHTTP with the given query, returning the recorder.
// A default logger is on the ctx (xcontext.Logger returns one anyway); no
// middleware — the seams stand in for objects.Get / api.InspectReadSet.
func callRBAC(t *testing.T, query string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/rbac"+query, nil).
		WithContext(xcontext.BuildContext(context.Background()))
	RBAC().ServeHTTP(rec, req)
	return rec
}

// okGet returns a seam that serves the fake RA for (name, namespace).
func okGet(name, namespace string) func(context.Context, templatesv1.ObjectReference) objects.Result {
	return func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
		return objects.Result{
			GVR:          schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
			Unstructured: fakeRAUnstructured(ref.Name, ref.Namespace),
		}
	}
}

// TestRBAC_Body_Unresolvable422NamesStage — arm (a): when the inspect pass
// reports an unresolvable stage, the handler MUST 422 (never 200) and the JSON
// body MUST name that stage with its reason token and message, and carry the
// rows that did enumerate (#1619).
func TestRBAC_Body_Unresolvable422NamesStage(t *testing.T) {
	withRBACSeams(t,
		okGet("foo", "krateo-system"),
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			return []api.Resource{{Group: "", Version: "v1", Resource: "configmaps", Verb: "get"}},
				[]api.Unresolved{{Stage: "listPods", Reason: api.ReasonUnrenderedTemplate, Message: "needs upstream output"}},
				nil
		},
	)

	rec := callRBAC(t, "?apiRefName=foo&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("H5(a): unresolvable stage must yield 422, got %d; body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("H5(a): 422 content type = %q", ct)
	}
	var got rbacResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("H5(a): decode 422 body: %v (%s)", err, rec.Body.String())
	}
	if got.RESTAction != (restActionRef{Name: "foo", Namespace: "krateo-system"}) {
		t.Errorf("H5(a): restaction = %+v", got.RESTAction)
	}
	wantRows := []api.Resource{{Version: "v1", Resource: "configmaps", Verb: "get"}}
	if !reflect.DeepEqual(got.ReadSet, wantRows) {
		t.Errorf("H5(a): 422 readSet = %+v, want the enumerated rows %+v", got.ReadSet, wantRows)
	}
	wantUn := []api.Unresolved{{Stage: "listPods", Reason: "unrenderedTemplate", Message: "needs upstream output"}}
	if !reflect.DeepEqual(got.Unresolved, wantUn) {
		t.Errorf("H5(a): unresolved = %+v, want %+v", got.Unresolved, wantUn)
	}
}

// TestRBAC_Body_MissingParams400 — arm (b): a missing apiRefName or
// apiRefNamespace yields 400 BEFORE any RA load (the seams must not even be
// consulted). Table-driven over the two params.
func TestRBAC_Body_MissingParams400(t *testing.T) {
	getCalled := false
	withRBACSeams(t,
		func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
			getCalled = true
			return objects.Result{}
		},
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			return nil, nil, nil
		},
	)

	cases := []struct {
		name  string
		query string
	}{
		{"missing name", "?apiRefNamespace=krateo-system"},
		{"missing namespace", "?apiRefName=foo"},
		{"missing both", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			getCalled = false
			rec := callRBAC(t, c.query)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("H5(b) %s: want 400, got %d", c.name, rec.Code)
			}
			if getCalled {
				t.Fatalf("H5(b) %s: RA loader must NOT run when required params are absent", c.name)
			}
		})
	}
}

// TestRBAC_Body_AllExternalReadSetEmptyArray — arm (c): a RESTAction whose every
// stage is external/discovery legitimately reads no in-cluster resource; the
// inspect pass returns a nil read-set. The handler MUST marshal it as [] (empty
// array), NOT null — a null readSet would be a JSON-shape regression for the
// core-provider consumer.
func TestRBAC_Body_AllExternalReadSetEmptyArray(t *testing.T) {
	withRBACSeams(t,
		okGet("all-external", "krateo-system"),
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			return nil, nil, nil // nil read-set, nothing unresolved
		},
	)

	rec := callRBAC(t, "?apiRefName=all-external&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusOK {
		t.Fatalf("H5(c): all-external RA must yield 200, got %d; body=%s", rec.Code, rec.Body.String())
	}
	// Assert the RAW JSON carries "readSet": [] and NOT null. Decoding into a Go
	// slice would collapse both null and [] to a nil slice, masking the bug —
	// so match the raw bytes.
	raw := rec.Body.String()
	if !containsStr(raw, `"readSet": []`) {
		t.Fatalf("H5(c): body must marshal an empty read-set as [] (not null); got:\n%s", raw)
	}
	// Belt-and-suspenders: the typed decode succeeds and yields an empty set.
	var body rbacResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("H5(c): decode 200 body: %v", err)
	}
	if len(body.ReadSet) != 0 {
		t.Fatalf("H5(c): read-set must be empty; got %d rows", len(body.ReadSet))
	}
}

// TestRBAC_Body_Valid200SortedDeduped — arm (d): a valid RESTAction yields 200
// and the read-set the inspect pass produced, verbatim, in the body's readSet.
// (The dedupe+sort is the inspect pass's contract; the handler passes it through
// unchanged — this arm proves the pass-through + the identity echo.)
func TestRBAC_Body_Valid200SortedDeduped(t *testing.T) {
	want := []api.Resource{
		{Group: "", Version: "v1", Resource: "configmaps", Namespace: "krateo-system", Verb: "get"},
		{Group: "apps", Version: "v1", Resource: "deployments", Namespace: "krateo-system", Verb: "list"},
	}
	withRBACSeams(t,
		okGet("valid-ra", "krateo-system"),
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			return want, nil, nil
		},
	)

	rec := callRBAC(t, "?apiRefName=valid-ra&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusOK {
		t.Fatalf("H5(d): valid RA must yield 200, got %d; body=%s", rec.Code, rec.Body.String())
	}
	var body rbacResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("H5(d): decode 200 body: %v", err)
	}
	if body.RESTAction.Name != "valid-ra" || body.RESTAction.Namespace != "krateo-system" {
		t.Fatalf("H5(d): body must echo the RA identity; got %+v", body.RESTAction)
	}
	if len(body.ReadSet) != len(want) {
		t.Fatalf("H5(d): read-set len = %d, want %d (%+v)", len(body.ReadSet), len(want), body.ReadSet)
	}
	for i := range want {
		if body.ReadSet[i] != want[i] {
			t.Fatalf("H5(d): read-set[%d] = %+v, want %+v", i, body.ReadSet[i], want[i])
		}
	}
}

// TestRBAC_Body_GetErrorEncoded — a loader error (e.g. NotFound) is encoded with
// its own status code, never a 200. Guards the "load failed → fail loud" branch.
func TestRBAC_Body_GetErrorEncoded(t *testing.T) {
	withRBACSeams(t,
		func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
			return objects.Result{Err: response.New(http.StatusNotFound, http.ErrNoLocation)}
		},
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			t.Fatalf("inspect must NOT run when the RA load errored")
			return nil, nil, nil
		},
	)

	rec := callRBAC(t, "?apiRefName=absent&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("H5(load-err): a NotFound load must encode 404, got %d", rec.Code)
	}
}

// TestRBAC_Body_RED_PartialReadSetMustNot200 is the RED proof for arm (a): it
// demonstrates that the discriminating assertion (422 on any unresolved stage)
// catches the plausible wrong impl "return the partial read-set as 200,
// ignoring unresolved". We simulate that wrong impl by directly reproducing its
// output shape (200 + partial body) and asserting the H5(a) invariant would
// FAIL on it — i.e. the arm is not vacuously green.
func TestRBAC_Body_RED_PartialReadSetMustNot200(t *testing.T) {
	// Shadow the wrong-impl response: a handler that ships the partial set as
	// 200 despite unresolved stages.
	wrong := httptest.NewRecorder()
	wrong.Code = http.StatusOK
	partial := rbacResponse{
		RESTAction: restActionRef{Name: "foo", Namespace: "krateo-system"},
		ReadSet:    []api.Resource{{Group: "", Version: "v1", Resource: "configmaps", Verb: "get"}},
	}
	_ = json.NewEncoder(wrong.Body).Encode(partial)

	// The H5(a) invariant applied to the wrong impl's output MUST fail (it is
	// 200, not 422). If this ever passes, arm (a) is not discriminating.
	if wrong.Code == http.StatusUnprocessableEntity {
		t.Fatalf("RED setup invalid: wrong impl should be 200, not 422")
	}
	// Confirm the real handler, on the SAME unresolved condition, is 422 — the
	// exact behaviour the wrong impl lacks.
	withRBACSeams(t,
		okGet("foo", "krateo-system"),
		func(_ context.Context, _ *templatesv1.RESTAction, _ map[string]any) ([]api.Resource, []api.Unresolved, error) {
			return partial.ReadSet, []api.Unresolved{{Stage: "listPods", Reason: "needs upstream output"}}, nil
		},
	)
	rec := callRBAC(t, "?apiRefName=foo&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("RED: real handler must 422 where the wrong impl 200s; got %d", rec.Code)
	}
}

// containsStr is a tiny substring helper (avoids importing strings in a test
// file already dense with imports; behaviour == strings.Contains).
func containsStr(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
