package api

// definite_notfound_1080_review_test.go — #1080 arm the PR #1100 review asked
// for (reviewer arm RV-N2x).
//
//	N2x  on branch C (the refresher and seed path), an SA dial that FAILS
//	     with a 5xx after the per-identity pre-dial ALLOWED the read stays a
//	     plain stage error, never a definite NotFound, even though the watcher
//	     reports the coordinate absent. Only the apiserver's NotFound, after
//	     an allowed pre-dial, is this identity's answer.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue1080_N2x_BranchC_5xxAfterAllowedPreDial_NotDefinite(t *testing.T) {
	watcher1080(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	var dials atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`)
	}))
	t.Cleanup(srv.Close)
	rc := &rest.Config{Host: srv.URL}
	if !cache.WatchedAbsent(cmGVR677, "team-a", "ghost") {
		t.Fatal("PRE: the watcher must report team-a/ghost absent, so only the dial's answer can decline")
	}
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx, s := withSinks681(ctx)
	_ = Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: []*templates.API{stage681("ghost", ghost1080)}, RESTActionNamespace: "krateo-system", RESTActionName: "ra1080x",
	})
	if dials.Load() == 0 {
		t.Fatal("PRE: the allowed pre-dial must lead to the SA dial")
	}
	if s.stage.Count() != 1 {
		t.Fatalf("PRE: want one stage error, got %d", s.stage.Count())
	}
	if s.forb.Stage() != 0 || cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatalf("N2x RED: a branch C 5xx after an allowed pre-dial was counted as a definite NotFound (forb.stage=%d)", s.forb.Stage())
	}
}
