//go:build integration

// issue905_stale_write_once_test.go — #905: a customer RESTAction write is
// sent EXACTLY ONCE even when one of its GET stages is served stale (D2).
//
// The #710 D2 harness: the shared `found` content cell is KNOWN-STALE, and a
// customer GET /call of the RESTAction goes through the REAL restActionHandler
// (D2 sink, write point, withdraw + re-run). The RESTAction resolve seam runs
// the real api resolver over the `found` stage AND a POST stage to a counting
// upstream; the test owns the count.
//
//   W1  read then write: the first pass is served `found` stale, so its write
//       is held (rule a); the pass is withdrawn and the fresh re-run sends the
//       POST — exactly once. PRE: one stale decision, one withdrawal.
//       Mutation → RED: BeginWrite never refuses (2 POSTs).
//   W2  write then read: the POST is sent first, so the later `found` read is
//       never served stale (rule b) and the request is never re-run — exactly
//       one POST, zero stale decisions.
//       Mutation → RED: staleServeEligible ignores WriteSent (2 POSTs).
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// w905Seam is the RESTAction resolve seam: the `found` stage through the real
// api-stage resolver, plus a POST to up (before or after it).
func (h *k547Harness) w905Seam(up string, writeFirst bool) func(context.Context, restactions.ResolveOptions) (*templates.RESTAction, error) {
	return func(ctx context.Context, opts restactions.ResolveOptions) (*templates.RESTAction, error) {
		name, _ := opts.Extras["name"].(string)
		ns, _ := opts.Extras["namespace"].(string)
		rw := cache.Global()
		rctx := cache.WithServeWatcher(ctx, rw)
		// The `found` LIST is served from the content cell / informer and never
		// dials; the POST (no endpointRef) dials this endpoint, the upstream.
		rctx = cache.WithInternalEndpoint(rctx, &endpoints.Endpoint{ServerURL: up})
		found := &templates.API{Name: "found", Verb: ptr.To("GET"),
			Path: "/apis/" + k547CompGVR.Group + "/" + k547CompGVR.Version + "/" + k547CompGVR.Resource}
		create := &templates.API{Name: "create", Verb: ptr.To("POST"), Path: "/create",
			Payload: ptr.To(`{"n":1}`), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("createErr")}
		items := []*templates.API{found, create}
		if writeFirst {
			create.DependsOn = nil
			found.DependsOn = &templates.Dependency{Name: "create"}
			items = []*templates.API{create, found}
		} else {
			create.DependsOn = &templates.Dependency{Name: "found"}
		}
		dict := restactionsapi.Resolve(rctx, restactionsapi.ResolveOptions{
			RC: &rest.Config{Host: up}, Watcher: rw, Items: items,
			RESTActionNamespace: k547WNS, RESTActionName: k547RAName,
		})
		var conds []any
		if v, ok := dict["found"].(map[string]any); ok {
			list, _ := v["items"].([]any)
			for _, it := range list {
				m, _ := it.(map[string]any)
				md, _ := m["metadata"].(map[string]any)
				if md["name"] == name && md["namespace"] == ns {
					conds, _, _ = unstructured.NestedSlice(m, "status", "conditions")
				}
			}
		}
		if conds == nil {
			return nil, fmt.Errorf("composition %s/%s not in the found stage", ns, name)
		}
		raw, _ := json.Marshal(map[string]any{"conditions": conds})
		out := opts.In.DeepCopy()
		out.Status = &runtime.RawExtension{Raw: raw}
		return out, nil
	}
}

func w905Upstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"created":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &posts
}

func TestIssue905_W1_ReadThenWrite_StalePassHoldsTheWrite_SentOnce(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	h, _ := k710KnownStaleFound(t)
	up, posts := w905Upstream(t)
	t.Cleanup(setRestactionsResolveForTest(h.w905Seam(up.URL, false)))
	s0 := restactionsapi.StaleServeStatsSnapshot()

	r, code := h.getRA(0, k547Changed)

	st := restactionsapi.StaleServeStatsSnapshot()
	if st.ServeTotal-s0.ServeTotal != 1 || st.WithdrawnUnarmable-s0.WithdrawnUnarmable != 1 {
		t.Fatalf("PRE: the first pass must be served stale and withdrawn (serve %d, withdrawn %d) — else this arm is vacuous",
			st.ServeTotal-s0.ServeTotal, st.WithdrawnUnarmable-s0.WithdrawnUnarmable)
	}
	if n := posts.Load(); n != 1 {
		t.Fatalf("#905 RED: one customer request sent its POST %d time(s), want exactly 1", n)
	}
	if code != http.StatusOK || r != "v1" {
		t.Fatalf("served %q (code %d), want the fresh v1 from the re-run", r, code)
	}
}

func TestIssue905_W2_WriteThenRead_NeverServedStaleAfterTheWrite_SentOnce(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	h, _ := k710KnownStaleFound(t)
	up, posts := w905Upstream(t)
	t.Cleanup(setRestactionsResolveForTest(h.w905Seam(up.URL, true)))
	s0 := restactionsapi.StaleServeStatsSnapshot()

	r, code := h.getRA(0, k547Changed)

	st := restactionsapi.StaleServeStatsSnapshot()
	if n := posts.Load(); n != 1 {
		t.Fatalf("#905 RED: one customer request sent its POST %d time(s), want exactly 1 (stale decisions %d, withdrawn %d)",
			n, st.ServeTotal-s0.ServeTotal, st.WithdrawnUnarmable-s0.WithdrawnUnarmable)
	}
	if d := st.ServeTotal - s0.ServeTotal; d != 0 {
		t.Fatalf("a read after a sent write must read through, never be served stale: %d stale decision(s)", d)
	}
	if code != http.StatusOK || r != "v1" {
		t.Fatalf("served %q (code %d), want the fresh v1 (the read after the write read through)", r, code)
	}
}
