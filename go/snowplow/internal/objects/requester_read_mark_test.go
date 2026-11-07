package objects

// requester_read_mark_test.go — #1229: a read that leaves snowplow under the
// requester's credential marks the resolve; a read under an internal driver's
// credential (the ServiceAccount's, on a seed, the walk or the refresher) does not.
// Both directions matter: unmarked, a requester's read can be stored under a key
// another identity shares; marked, every seed whose informer had not synced yet
// would lose its Put.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue1229_OnlyARequesterCredentialedReadMarksTheResolve(t *testing.T) {
	// The requester's server URL is kept as given (no in-cluster rewrite).
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	var reads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`))
		case "/api/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
				`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`))
		case "/api/v1/namespaces/ns/configmaps/x":
			reads.Add(1)
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: "x", Namespace: "ns"}, APIVersion: "v1", Resource: "configmaps",
	}
	for _, c := range []struct {
		name       string
		internal   bool
		wantMarked bool
	}{
		{name: "under_the_requester", internal: false, wantMarked: true},
		{name: "under_an_internal_driver", internal: true, wantMarked: false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "t"}))
			if c.internal {
				ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: srv.URL})
			}
			ctx, sink := cache.WithExternalTouchedSink(ctx)
			before := reads.Load()
			res := getFromAPIServer(ctx, ref)
			if res.Err != nil || res.Unstructured == nil || reads.Load() == before {
				t.Fatalf("PRECONDITION: the read must have left snowplow and succeeded (err=%v reads=%d)", res.Err, reads.Load()-before)
			}
			if marked := sink.Count() > 0; marked != c.wantMarked || res.RequesterDialled != c.wantMarked {
				t.Fatalf("marked=%v RequesterDialled=%v, want %v: only a read under the requester's credential marks the resolve",
					marked, res.RequesterDialled, c.wantMarked)
			}
			if sink.OnlyThirdParty() {
				t.Fatalf("a read under the requester's credential is never a third-party touch")
			}
		})
	}
}
