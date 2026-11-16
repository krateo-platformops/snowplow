package api

// issue903_write_classifier_test.go — #903 F7: ONE write classifier.
//
//   C1  isWriteVerb is exactly isSafeMethod's complement (the #902 audit trail's
//       classifier), over a table that includes the empty verb and lower case.
//   C2  the empty verb is classified as it is SENT: net/http sends "" as GET, so
//       a background resolve dials a `verb: ""` stage (a read) and does not
//       refuse it — and the upstream sees a GET.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue903_C1_OneWriteClassifier(t *testing.T) {
	for _, v := range []string{"", "GET", "get", "HEAD", "OPTIONS", "TRACE", "POST", "post", "PUT", "PATCH", "DELETE", "CONNECT"} {
		if isWriteVerb(ptr.To(v)) == isSafeMethod(v) {
			t.Errorf("verb %q: isWriteVerb=%v but isSafeMethod=%v — the two classifiers disagree", v, isWriteVerb(ptr.To(v)), isSafeMethod(v))
		}
	}
	if isWriteVerb(nil) {
		t.Error("a nil verb is GET")
	}
	if isWriteVerb(ptr.To("")) {
		t.Error(`verb "" is sent as GET: it is not a write`)
	}
	for _, v := range []string{"POST", "PUT", "PATCH", "DELETE", "CONNECT", "post"} {
		if !isWriteVerb(ptr.To(v)) {
			t.Errorf("verb %q must be a write", v)
		}
	}
}

func TestIssue903_C2_EmptyVerbIsSentAsGET_NotRefusedInTheBackground(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	var mu sync.Mutex
	methods := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	// A non-customer resolve (the prewarm path: no #268 SA re-gate on its reads).
	ctx := cache.WithPrewarmPath(ctxWithUser("cyberjoker", "devs"))
	if cache.NonCustomerWriteOrigin(ctx) == "" {
		t.Fatal("PRE: the ctx must be non-customer")
	}
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
	ctx, sink := cache.WithStageErrorSink(ctx)
	dict := Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: &rest.Config{Host: srv.URL}, AuthnNS: "krateo-system",
		Items: []*templates.API{{Name: "s", Path: "/things/1", Verb: ptr.To(""),
			ContinueOnError: ptr.To(true), ErrorKey: ptr.To("sErr")}},
		RESTActionNamespace: "ns-903", RESTActionName: "ra-903",
	})
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != http.MethodGet {
		t.Fatalf(`a verb "" stage must be dialed once, as GET; the upstream saw %v (sErr=%v, stage errors=%d)`,
			methods, dict["sErr"], sink.Count())
	}
	if sink.Count() != 0 {
		t.Fatalf(`a verb "" stage was refused as a write: %v`, dict["sErr"])
	}
}
