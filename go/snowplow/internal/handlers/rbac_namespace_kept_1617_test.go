package handlers

// #1617 — GET /rbac end to end with the issue's case: extras give the
// namespace, only the object name comes from a prior stage. The real
// InspectReadSet runs; only the RESTAction load and the SA discovery are faked.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func rbac1617RA(stagePath string) func(context.Context, templatesv1.ObjectReference) objects.Result {
	return func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
		return objects.Result{
			GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
			Unstructured: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": templatesv1.SchemeGroupVersion.String(),
				"kind":       "RESTAction",
				"metadata":   map[string]any{"name": ref.Name, "namespace": ref.Namespace},
				"spec": map[string]any{"api": []any{
					// a different namespace, so the two rows do not dedupe
					map[string]any{"name": "prior", "path": "/api/v1/namespaces/ns2/configmaps/cd"},
					map[string]any{"name": "target", "dependsOn": map[string]any{"name": "prior"}, "path": stagePath},
				}},
			}},
		}
	}
}

func TestRBAC1617_NameFromPriorStageKeepsTheNamespace(t *testing.T) {
	rbac1116Discovery(t) // core v1 with configmaps
	withRBACSeams(t, rbac1617RA(`${ "/api/v1/namespaces/" + .compositionNamespace + "/configmaps/" + .prior.name }`), api.InspectReadSet)

	ej := url.QueryEscape(`{"compositionNamespace":"ns1"}`)
	rec := callRBAC(t, "?apiRefName=ra&apiRefNamespace=krateo-system&extras="+ej)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d %s", rec.Code, rec.Body.String())
	}
	var b struct {
		ReadSet []api.Resource `json:"readSet"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	want := []api.Resource{
		{Version: "v1", Resource: "configmaps", Namespace: "ns1", Verb: "get"},
		{Version: "v1", Resource: "configmaps", Namespace: "ns2", Verb: "get"},
	}
	if !reflect.DeepEqual(b.ReadSet, want) {
		t.Fatalf("want %+v, got %+v (%s)", want, b.ReadSet, rec.Body.String())
	}
	var raw map[string][]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	for _, row := range raw["readSet"] {
		if _, has := row["namespaceUnknown"]; has {
			t.Errorf("a known namespace must not carry namespaceUnknown: %v", row)
		}
		if _, has := row["name"]; has {
			t.Errorf("the name is not known before reads and must be absent: %v", row)
		}
	}
}
