package handlers

// #1619 — GET /rbac keeps the 422 but the body is JSON: the rows that did
// enumerate, and one {stage, reason, message} per stage that did not. The real
// InspectReadSet runs; only the RESTAction load and the SA discovery are faked.

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func rbac1619RA(stages ...map[string]any) func(context.Context, templatesv1.ObjectReference) objects.Result {
	list := make([]any, 0, len(stages))
	for _, s := range stages {
		list = append(list, s)
	}
	return func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
		return objects.Result{
			GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
			Unstructured: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": templatesv1.SchemeGroupVersion.String(),
				"kind":       "RESTAction",
				"metadata":   map[string]any{"name": ref.Name, "namespace": ref.Namespace},
				"spec":       map[string]any{"api": list},
			}},
		}
	}
}

func stage1619(name, path string, dependsOn ...string) map[string]any {
	s := map[string]any{"name": name, "path": path}
	if len(dependsOn) > 0 {
		s["dependsOn"] = map[string]any{"name": dependsOn[0]}
	}
	return s
}

const rbac1619Query = "?apiRefName=ra&apiRefNamespace=krateo-system"

type wire1619 struct {
	RESTAction restActionRef    `json:"restaction"`
	ReadSet    []map[string]any `json:"readSet"`
	Unresolved []api.Unresolved `json:"unresolved"`
}

func get1619(t *testing.T, stages ...map[string]any) (int, wire1619, []byte) {
	t.Helper()
	rbac1116Discovery(t) // core v1 with configmaps
	withRBACSeams(t, rbac1619RA(stages...), api.InspectReadSet)
	rec := callRBAC(t, rbac1619Query)
	var w wire1619
	if err := json.Unmarshal(rec.Body.Bytes(), &w); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	return rec.Code, w, rec.Body.Bytes()
}

// A mixed read set: a 422 with the resolved rows and the named unresolved
// stages, each with its token.
func TestRBAC1619_MixedReadSetIs422WithRowsAndTokens(t *testing.T) {
	code, w, _ := get1619(t,
		stage1619("good", "/api/v1/namespaces/ns1/configmaps/cd"),
		stage1619("raw", "/api/v1/namespaces/${ .x /configmaps"),
		stage1619("orphan", "/api/v1/namespaces/ns1/configmaps", "ghost"),
		stage1619("looped", "/api/v1/namespaces/ns1/configmaps", "looped"),
	)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", code)
	}
	if w.RESTAction != (restActionRef{Name: "ra", Namespace: "krateo-system"}) {
		t.Errorf("restaction = %+v", w.RESTAction)
	}
	wantRows := []map[string]any{{"group": "", "version": "v1", "resource": "configmaps", "namespace": "ns1", "verb": "get"}}
	if !reflect.DeepEqual(w.ReadSet, wantRows) {
		t.Errorf("readSet = %+v, want %+v", w.ReadSet, wantRows)
	}
	got := map[string]string{}
	for _, u := range w.Unresolved {
		got[u.Stage] = u.Reason
		if u.Message == "" {
			t.Errorf("no message on %+v", u)
		}
	}
	want := map[string]string{"raw": "unrenderedTemplate", "orphan": "unknownDependency", "looped": "cyclicDependency"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unresolved = %+v, want stage->token %v", w.Unresolved, want)
	}
}

// An unknown dependsOn used to come back as "cyclic dependency detected" and a
// 500. It is an unresolved stage of a 422, and a real cycle is distinct.
func TestRBAC1619_UnknownDependencyIsNot500AndNotACycle(t *testing.T) {
	code, w, _ := get1619(t, stage1619("orphan", "/api/v1/namespaces/ns1/configmaps", "ghost"))
	if code != http.StatusUnprocessableEntity || len(w.Unresolved) != 1 || w.Unresolved[0].Reason != "unknownDependency" {
		t.Fatalf("unknown dependency: %d %+v", code, w)
	}
	code, w, _ = get1619(t,
		stage1619("a", "/api/v1/namespaces/ns1/configmaps", "b"),
		stage1619("b", "/api/v1/namespaces/ns1/configmaps", "a"))
	if code != http.StatusUnprocessableEntity || len(w.Unresolved) != 2 || w.Unresolved[0].Reason != "cyclicDependency" {
		t.Fatalf("cycle: %d %+v", code, w)
	}
}

// The row shape of a 422 is the 200's: the same rows, encoded the same bytes, so
// one decoder reads both.
func TestRBAC1619_RowShapeIsIdenticalTo200(t *testing.T) {
	good := stage1619("good", "/api/v1/namespaces/ns1/configmaps/cd")
	code, _, ok := get1619(t, good)
	if code != http.StatusOK {
		t.Fatalf("want 200, got %d", code)
	}
	code, _, partial := get1619(t, good, stage1619("raw", "/api/v1/namespaces/${ .x /configmaps"))
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", code)
	}
	var a, b struct {
		ReadSet json.RawMessage `json:"readSet"`
	}
	if err := json.Unmarshal(ok, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(partial, &b); err != nil {
		t.Fatal(err)
	}
	if string(a.ReadSet) != string(b.ReadSet) || len(a.ReadSet) == 0 {
		t.Fatalf("readSet differs between 200 and 422:\n200: %s\n422: %s", a.ReadSet, b.ReadSet)
	}
	// And a 200 has no "unresolved" key at all.
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(ok, &keys); err != nil {
		t.Fatal(err)
	}
	if _, has := keys["unresolved"]; has {
		t.Errorf("a 200 must not carry unresolved: %s", ok)
	}
}
