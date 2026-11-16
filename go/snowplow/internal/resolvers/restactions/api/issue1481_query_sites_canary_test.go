package api

// issue1481_query_sites_canary_test.go — #1481 review B1: the remaining
// restactions/api sites where a stage path's query reached a log line.
//
//   N  the internal dispatch's null-collapse WARN (resolved_path).
//   T  the internal-rest-config dispatch's transport error: client-go's
//      *url.Error quotes the request URL, query included, and resolve logs and
//      records it.
//   I  the inspect enumeration's error for a path it cannot enumerate.
//
// (The branch-E refusal message is driven through the refresher in
// handlers/dispatchers issue1481_branche_query_canary_test.go.)

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

const x1481SiteCanary = "QSITECANARY1481"

type x1481Buf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *x1481Buf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *x1481Buf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// x1481DebugLogs is a DEBUG JSON logger, also installed as slog's default.
func x1481DebugLogs(t *testing.T) (*slog.Logger, *x1481Buf) {
	t.Helper()
	buf := &x1481Buf{}
	lg := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := slog.Default()
	slog.SetDefault(lg)
	t.Cleanup(func() { slog.SetDefault(prev) })
	return lg, buf
}

func x1481RequireNoCanary(t *testing.T, logs string) {
	t.Helper()
	for _, ln := range strings.Split(logs, "\n") {
		if strings.Contains(ln, x1481SiteCanary) {
			t.Errorf("RED: a log line carries the stage path's query value:\n%s", strings.ReplaceAll(ln, x1481SiteCanary, "<CANARY>"))
		}
	}
}

func TestIssue1481_N_NullCollapseWarnCarriesNoQueryValue(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	fixture, caPEM := newPagedListFixture(t, 3, int(internalDispatchListPageLimit))
	rc := &rest.Config{Host: fixture.server.URL, BearerToken: "fake-sa-jwt", TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}}
	// A trailing "/" is a folded name segment: the collapse the WARN names.
	path := "/apis/composition.krateo.io/v1-2-2/githubscaffoldingwithcompositionpages/?labelSelector=owner%3D" + x1481SiteCanary
	lg, buf := x1481DebugLogs(t)
	ctx := withSlogLogger(cache.WithInternalRESTConfig(context.Background(), rc), lg)
	_, _, _ = dispatchViaInternalRESTConfig(ownClusterCtx(ctx), httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path}})
	logs := buf.String()
	if !strings.Contains(logs, nullCollapseWARNMsg) {
		t.Fatalf("PRE: the collapse WARN did not fire. Log:\n%s", logs)
	}
	if !strings.Contains(logs, "labelSelector=") {
		t.Fatalf("PRE: the WARN no longer names the path's query keys:\n%s", logs)
	}
	x1481RequireNoCanary(t, logs)
}

func TestIssue1481_T_InternalDispatchTransportErrorCarriesNoQueryValue(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close() // nothing listens: the connection is refused
	rc := &rest.Config{Host: dead.URL, BearerToken: "fake-sa-jwt"}
	path := "/apis/composition.krateo.io/v1-2-2/githubscaffoldingwithcompositionpages?labelSelector=owner%3D" + x1481SiteCanary
	lg, buf := x1481DebugLogs(t)
	ctx := ownClusterCtx(withSlogLogger(cache.WithInternalRESTConfig(ctxWithUser("u1481"), rc), lg))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	_, _, ierr := dispatchViaInternalRESTConfig(ctx, httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path}})
	if ierr == nil || !strings.Contains(ierr.Error(), x1481SiteCanary) {
		t.Fatalf("PRE: the dispatch must fail with client-go's error quoting the URL, got %v", ierr)
	}
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC: rc, RESTActionNamespace: "krateo-system", RESTActionName: "ra-1481",
		Items: []*templates.API{{Name: "s", Path: path, Verb: ptr.To(http.MethodGet), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("sErr")}},
	})
	logs := buf.String()
	if !strings.Contains(logs, "api call response failure") || !strings.Contains(logs, "labelSelector=") {
		t.Fatalf("PRE: the failure line, with the URL's query keys, must be logged (dict %v):\n%s", dict, logs)
	}
	x1481RequireNoCanary(t, logs)
	if v, _ := dict["sErr"].(string); strings.Contains(v, x1481SiteCanary) {
		t.Errorf("RED: the recorded stage error carries the query value: %s", strings.ReplaceAll(v, x1481SiteCanary, "<CANARY>"))
	}
}

func TestIssue1481_I_InspectErrorCarriesNoQueryValue(t *testing.T) {
	lg, _ := x1481DebugLogs(t)
	// A rendered path that is not an apiserver path: inspect cannot enumerate it.
	stage := &templates.API{Name: "s", Path: "/zd/tickets?apikey=" + x1481SiteCanary, Verb: ptr.To(http.MethodGet)}
	_, err := inspectInClusterStage(context.Background(), &rest.Config{Host: "http://127.0.0.1:1"}, stage, map[string]any{}, lg)
	if err == nil || !strings.Contains(err.Error(), "is not an enumerable") || !strings.Contains(err.Error(), "apikey=") {
		t.Fatalf("PRE: want the not-enumerable error naming the path's query keys, got %v", err)
	}
	if strings.Contains(err.Error(), x1481SiteCanary) {
		t.Errorf("RED: the inspect error carries the query value: %s", strings.ReplaceAll(err.Error(), x1481SiteCanary, "<CANARY>"))
	}
}
