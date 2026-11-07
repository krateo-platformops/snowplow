package objects

// requester_read_mark_test.go — #1229: a read that leaves snowplow under the
// requester's credential marks the resolve; a read under an internal driver's
// credential (the ServiceAccount's, on a seed, the walk or the refresher) does not.
// Both directions matter: unmarked, a requester's read can be stored under a key
// another identity shares; marked, every seed whose informer had not synced yet
// would lose its Put.
//
// Two of the cases pin claims the #1230 review found no arm for:
//   - a refused read is marked too: the mark is placed before the dial;
//   - an internal configuration that is a nil pointer or of the wrong type is not
//     used: ClientConfigFor falls back to the requester's credential, and the read
//     is then marked. The mark and the client agree on whose credential it is.

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
		case "/api/v1/namespaces/ns/configmaps/refused":
			reads.Add(1)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ref := func(name string) templatesv1.ObjectReference {
		return templatesv1.ObjectReference{
			Reference: templatesv1.Reference{Name: name, Namespace: "ns"}, APIVersion: "v1", Resource: "configmaps",
		}
	}
	for _, c := range []struct {
		name       string
		internal   any // what the ctx carries as its internal configuration; nil = none
		object     string
		wantMarked bool
		wantCode   int // 0 = the read succeeds
	}{
		{name: "under_the_requester", object: "x", wantMarked: true},
		{name: "under_an_internal_driver", internal: &rest.Config{Host: srv.URL}, object: "x", wantMarked: false},
		{name: "refused_under_the_requester", object: "refused", wantMarked: true, wantCode: http.StatusForbidden},
		{name: "a_nil_internal_config_is_the_requester", internal: (*rest.Config)(nil), object: "x", wantMarked: true},
		{name: "a_wrong_typed_internal_config_is_the_requester", internal: "not a rest config", object: "x", wantMarked: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "t"}))
			if c.internal != nil {
				ctx = cache.WithInternalRESTConfig(ctx, c.internal)
			}
			ctx, sink := cache.WithExternalTouchedSink(ctx)
			before := reads.Load()
			res := getFromAPIServer(ctx, ref(c.object))
			if reads.Load() == before {
				t.Fatalf("PRECONDITION: the read must have left snowplow (err=%v)", res.Err)
			}
			if c.wantCode == 0 && (res.Err != nil || res.Unstructured == nil) {
				t.Fatalf("PRECONDITION: the read must succeed (err=%v)", res.Err)
			}
			if c.wantCode != 0 && (res.Err == nil || res.Err.Code != c.wantCode) {
				t.Fatalf("PRECONDITION: the read must be refused with %d (err=%v)", c.wantCode, res.Err)
			}
			if marked := sink.Count() > 0; marked != c.wantMarked || res.RequesterDialled != c.wantMarked {
				t.Fatalf("marked=%v RequesterDialled=%v, want %v: only a read under the requester's credential marks the resolve, refused or not",
					marked, res.RequesterDialled, c.wantMarked)
			}
			if sink.OnlyThirdParty() {
				t.Fatalf("a read under the requester's credential is never a third-party touch")
			}
		})
	}
}
