package restactions

// #1116 — restactions.Resolve refuses extras that name a key the resolver
// writes BEFORE it runs anything, and returns the refusal as an
// *api.ReservedExtrasError: never an empty dict through the RESTAction's
// filter, which would answer 200.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/client-go/rest"
)

func TestReservedExtras1116_ResolveReturnsTheRefusal(t *testing.T) {
	var dials atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{
		API: []*templates.API{
			{Name: "stage1", Path: "/api/v1/namespaces/team-a/configmaps", ErrorKey: ptr.To("stage1Err")},
			{Name: "stage2", Path: `${ "/x/" + (.stage1.items[0].id | tostring) }`,
				EndpointRef: &templates.Reference{Name: "held-endpoint", Namespace: "krateo-system"},
				DependsOn:   &templates.Dependency{Name: "stage1"}},
		},
		Filter: ptr.To(`{ out: .stage2 }`),
	}}
	ra.SetNamespace("team-a")
	ra.SetName("chain")

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "u-a", Groups: []string{"g"}}))
	ctx, outcomes := api.WithStageOutcomes(ctx)
	_, err := Resolve(ctx, ResolveOptions{
		In:     ra,
		SArc:   &rest.Config{Host: srv.URL},
		Extras: map[string]any{"namespace": "team-a", "stage1": map[string]any{"items": []any{map[string]any{"id": 2002}}}, "error": "x"},
	})
	re, ok := api.AsReservedExtrasError(err)
	if !ok {
		t.Fatalf("Resolve: want *api.ReservedExtrasError, got %v", err)
	}
	if len(re.Keys) != 2 || re.Keys[0].Key != "error" || re.Keys[1].Key != "stage1" {
		t.Errorf("refused keys = %+v, want error (stage2's default errorKey) and stage1", re.Keys)
	}
	if ra.Status != nil {
		t.Errorf("a refused resolve set a status: %s", ra.Status.Raw)
	}
	if n := dials.Load(); n != 0 {
		t.Errorf("a refused resolve dialled %d time(s)", n)
	}
	if h := outcomes.HeaderValue(); !strings.Contains(h, `"reason":"Aborted"`) {
		t.Errorf("outcomes = %s, want the resolve-level Aborted row", h)
	}
}
