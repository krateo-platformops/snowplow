//go:build unit || integration

package api

// issue651_uaf_resources_from_test.go — #651 (b) RED-first arms.
//
// THE DEFECT (main 080ac5e6): a userAccessFilter whose resourcesFrom fails (a
// jq error, a non-array result, a non-string element) correctly fails CLOSED —
// every item is dropped and the stage is served {items: []} — but nothing marks
// it as an error: no stage-error sink bump and no stage outcome. The EMPTY
// answer is then indistinguishable from "no resources", and since #561 the
// identity-bound widgets cell (and the RAFullList cell) STORE a UAF-touched
// resolve, so an authoring error became a persisted 0-row widget.
//
// WHAT THE ARMS PIN, per identity over K=2 users x M=2 cohorts, stored AND
// caller-supplied provenance, four resourcesFrom failure shapes, through the
// real Resolve (the UAF stage dispatched on the SA endpoint seam):
//   - the fail-closed drop is preserved: dict[stage] is {items: []};
//   - the stage-error sink is bumped, its sample naming the stage — the signal
//     every L1 Put gate reads to decline;
//   - the caller-supplied stage outcome is Error;
//   - a downstream stage still runs.
// A resolving-resourcesFrom CONTROL proves the arm is not vacuous (sink 0).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

func newA651Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/crds":
			fmt.Fprint(w, `[{"plural":"anyplural"}]`)
		case "/list":
			fmt.Fprint(w, `{"kind":"AnyPluralList","items":[`+
				`{"metadata":{"name":"item-a","namespace":"ns-a"}},`+
				`{"metadata":{"name":"item-b","namespace":"ns-b"}}]}`)
		case "/down":
			fmt.Fprint(w, `{"ok":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","code":404}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func a651Stages(resourcesFrom string) []*templates.API {
	return []*templates.API{
		{Name: "crds", Path: "/crds", Verb: ptr.To(http.MethodGet)},
		{
			Name:      "items",
			Path:      "/list",
			Verb:      ptr.To(http.MethodGet),
			DependsOn: &templates.Dependency{Name: "crds"},
			UserAccessFilter: &templates.UserAccessFilterSpec{
				Verb:          "list",
				Group:         "example.test",
				ResourcesFrom: resourcesFrom,
				NamespaceFrom: ".metadata.namespace",
			},
		},
		{Name: "after", Path: "/down", Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Name: "items"}},
	}
}

type a651Result struct {
	dict     map[string]any
	sink     *cache.StageErrorSink
	outcomes *StageOutcomes
}

func a651Resolve(srvURL, user string, groups []string, prov Provenance, stages []*templates.API) a651Result {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srvURL})
	ctx, sink := cache.WithStageErrorSink(ctx)
	ctx, outcomes := WithStageOutcomes(ctx)
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               stages,
		RESTActionNamespace: "ns-651",
		RESTActionName:      "ra-651",
		Provenance:          prov,
	})
	return a651Result{dict: dict, sink: sink, outcomes: outcomes}
}

// a651SAEndpoint points the UAF stage's service-account dispatch at srv.
func a651SAEndpoint(t *testing.T, srvURL string) {
	t.Helper()
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srvURL, Token: "sa-651"}, nil
	}
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
}

func TestIssue651b_UnresolvedResourcesFromIsAStageError(t *testing.T) {
	srv := newA651Server(t)
	a651SAEndpoint(t, srv.URL)
	shapes := []struct{ name, q string }{
		{"runtime_error", "[ .crds[] | .plural + 1 ]"},
		{"parse_error", "[ (.crds"},
		{"non_array", `"anyplural"`},
		{"non_string_element", "[ 1 ]"},
	}
	provs := []struct {
		name string
		p    Provenance
	}{{"stored", ProvenanceStored}, {"caller_supplied", ProvenanceCallerSupplied}}
	type ident struct {
		user   string
		groups []string
	}
	var ids []ident
	for m := 0; m < 2; m++ {
		for k := 0; k < 2; k++ {
			ids = append(ids, ident{fmt.Sprintf("u651-c%d-k%d", m, k), []string{fmt.Sprintf("cohort-651-%d", m)}})
		}
	}
	for _, sh := range shapes {
		for _, pv := range provs {
			t.Run(sh.name+"/"+pv.name, func(t *testing.T) {
				results := make([]a651Result, len(ids))
				var wg sync.WaitGroup
				for i, id := range ids {
					wg.Add(1)
					go func(i int, id ident) {
						defer wg.Done()
						results[i] = a651Resolve(srv.URL, id.user, id.groups, pv.p, a651Stages(sh.q))
					}(i, id)
				}
				wg.Wait()
				for i, id := range ids {
					res := results[i]
					t.Run(id.user, func(t *testing.T) {
						b, _ := json.Marshal(res.dict["items"])
						if string(b) != `{"items":[]}` {
							t.Errorf("the fail-closed drop is not preserved: dict[items]=%s", b)
						}
						if res.sink.Count() == 0 {
							t.Errorf("#651 RED: stage-error sink NOT bumped — every L1 Put gate would store this empty answer")
						} else if st, _ := res.sink.Sample(); st != "items" {
							t.Errorf("sink sample names stage %q, want items", st)
						}
						if pv.p == ProvenanceCallerSupplied {
							want := `{"name":"items","ok":false,"reason":"Error"}`
							if hv := res.outcomes.HeaderValue(); !strings.Contains(hv, want) {
								t.Errorf("#651 RED: stage outcome for items is not Error: header=%s", hv)
							}
						}
						if _, ok := res.dict["after"]; !ok {
							t.Errorf("the downstream stage did not run")
						}
					})
				}
			})
		}
	}
}

func TestIssue651b_ResolvingResourcesFrom_Control(t *testing.T) {
	srv := newA651Server(t)
	a651SAEndpoint(t, srv.URL)
	for _, pv := range []Provenance{ProvenanceStored, ProvenanceCallerSupplied} {
		res := a651Resolve(srv.URL, "u651-control", []string{"cohort-651-0"}, pv, a651Stages("[ .crds[] | .plural ]"))
		if res.sink.Count() != 0 {
			st, msg := res.sink.Sample()
			t.Fatalf("control: sink=%d (%s: %s), want 0 — the arm cannot tell a resolving set from a failing one", res.sink.Count(), st, msg)
		}
		if _, ok := res.dict["items"]; !ok {
			t.Fatalf("control: dict[items] absent")
		}
		if pv == ProvenanceCallerSupplied {
			if hv := res.outcomes.HeaderValue(); strings.Contains(hv, `"name":"items","ok":false`) {
				t.Fatalf("control: items outcome failed: %s", hv)
			}
		}
	}
}
