package handlers

// #1116 (#1127 review finding 1) — GET /rbac enumerates a RESTAction's
// read-set from a dict seeded with the request extras, the same seed the
// resolver uses (api.InspectReadSet, #512 shape parity). A stage-named extras
// key made /rbac answer 200 with a read-set built from a forged stage output,
// for a /call that is always refused. The REAL InspectReadSet runs here: only
// the RESTAction load is faked.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// rbac1116RA: compdef reads a literal path; comps builds its path from
// compdef's output, so it cannot be enumerated without it.
func rbac1116RA(ref templatesv1.ObjectReference) objects.Result {
	return objects.Result{
		GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
		Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": templatesv1.SchemeGroupVersion.String(),
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": ref.Name, "namespace": ref.Namespace},
			"spec": map[string]any{"api": []any{
				map[string]any{"name": "compdef", "path": "/api/v1/namespaces/ns-a/configmaps/cd"},
				map[string]any{"name": "comps", "dependsOn": map[string]any{"name": "compdef"},
					"path": `${ "/apis/" + .compdef.status.apiVersion + "/namespaces/ns-a/" + .compdef.status.resource }`},
			}},
		}},
	}
}

// rbac1116Discovery is the SA's discovery: core v1 with configmaps, no groups.
func rbac1116Discovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/api/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","groups":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(api.SetServiceAccountRESTConfigForInspectForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: srv.URL}, nil
	}))
}

func TestRBAC1116_SeededStageKeyIs400NotAReadSet(t *testing.T) {
	rbac1116Discovery(t)
	withRBACSeams(t,
		func(_ context.Context, ref templatesv1.ObjectReference) objects.Result { return rbac1116RA(ref) },
		api.InspectReadSet)

	// No seed: comps cannot be enumerated, so /rbac refuses a partial read-set.
	rec := callRBAC(t, "?apiRefName=ra&apiRefNamespace=krateo-system")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("no seed: want 422 (comps unresolvable), got %d %s", rec.Code, rec.Body.String())
	}

	// Seeded compdef: the 400 /call answers, naming the key; no read-set.
	ej := url.QueryEscape(`{"compdef":{"status":{"apiVersion":"apps/v1","resource":"deployments"}}}`)
	rec = callRBAC(t, "?apiRefName=ra&apiRefNamespace=krateo-system&extras="+ej)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("seeded compdef: want 400, got %d %s", rec.Code, rec.Body.String())
	}
	var b struct {
		Reason  string `json:"reason"`
		Details struct {
			Causes []struct{ Reason, Field string } `json:"causes"`
		} `json:"details"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if b.Reason != "BadRequest" || len(b.Details.Causes) != 1 ||
		b.Details.Causes[0].Field != "extras.compdef" || b.Details.Causes[0].Reason != "ReservedExtrasKey" {
		t.Errorf("seeded compdef: want one ReservedExtrasKey cause on extras.compdef, got %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "deployments") {
		t.Errorf("the forged stage output reached the answer: %s", rec.Body.String())
	}
}

// The shared place holds the rule: InspectReadSet itself refuses, so every
// caller of the enumeration gets the refusal, not only GET /rbac.
func TestRBAC1116_InspectReadSetRefuses(t *testing.T) {
	got := rbac1116RA(templatesv1.ObjectReference{Reference: templatesv1.Reference{Name: "ra", Namespace: "ns"}})
	var ra templatesv1.RESTAction
	b, _ := json.Marshal(got.Unstructured.Object)
	if err := json.Unmarshal(b, &ra); err != nil {
		t.Fatal(err)
	}
	rs, un, err := api.InspectReadSet(context.Background(), &ra, map[string]any{"compdef": map[string]any{}})
	if _, ok := api.AsReservedExtrasError(err); !ok || rs != nil || un != nil {
		t.Fatalf("InspectReadSet: want the #1116 refusal and nothing enumerated, got rs=%v un=%v err=%v", rs, un, err)
	}
}
