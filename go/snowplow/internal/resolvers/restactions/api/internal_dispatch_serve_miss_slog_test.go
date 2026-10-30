// internal_dispatch_serve_miss_slog_test.go — Task #130 F1 diagnostic slog.
//
// The internal_dispatch.list.serve_miss slog fires on the serve-branch
// MISS path — every exit within the ServeWatcherFromContext block that
// falls through to the live paged LIST — carrying the four servability
// conjuncts (registered, hasSynced, watchHealthy, typeConfirmed) captured
// under one read lock. It discriminates ALL miss reasons (sync timeout,
// unregistered, watch-broken, typeConfirmed=false) so the next boot log
// reads the fall-through reason directly.
//
// Arms:
//   MISS path emits with all four fields — the C1 never-synced fall-through
//     (registered=false). RED: revert the emit → no serve_miss line.
//   HIT path does NOT emit — a servable GVR serves from the informer, and
//     serve_miss must be absent (the flow returns before the miss emit).
//
// Diagnostic-only: neither arm changes the dispatch result (served / raw /
// error); they only assert the presence/absence of the observability line.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// serveMissLine parses the single serve_miss JSON log line out of buf and
// returns its decoded fields. Fails the test if zero or more than one line
// is present.
func serveMissLine(t *testing.T, buf string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, ln := range strings.Split(strings.TrimSpace(buf), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			continue
		}
		if m["msg"] == "internal_dispatch.list.serve_miss" {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one serve_miss line, got %d; log:\n%s", len(found), buf)
	}
	return found[0]
}

// TestServe1a_ServeMiss_EmitsConjunctSnapshotOnFallThrough is the MISS-path
// arm: a never-synced GVR forces the serve branch to fall through to the
// live LIST, and the serve_miss line must fire with all four conjunct
// fields present (registered=false here — the never-synced/unregistered
// exit). RED: revert the emit → serveMissLine finds zero lines.
func TestServe1a_ServeMiss_EmitsConjunctSnapshotOnFallThrough(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	// Never-synced watcher → the serve branch misses and falls through.
	rw := newServe1aWatcher(t, false)
	if rw.IsServable(serve1aGVR) {
		t.Fatal("precondition: never-synced GVR must not be servable")
	}

	const n = 20
	fixture, caPEM := newServe1aLiveFixture(t, n, int(internalDispatchListPageLimit))
	rc := &rest.Config{
		Host:            fixture.server.URL,
		BearerToken:     "fake-sa-jwt",
		TLSClientConfig: rest.TLSClientConfig{CAData: caPEM},
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx := cache.WithInternalRESTConfig(context.Background(), rc)
	ctx = cache.WithServeWatcher(ctx, rw)
	ctx = withSlogLogger(ctx, logger)

	_, served, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: serve1aListPath},
	})
	if err != nil {
		t.Fatalf("fall-through dispatch returned error: %v", err)
	}
	if !served {
		t.Fatal("expected served=true from the live-LIST fall-through")
	}

	m := serveMissLine(t, logBuf.String())
	// All four conjunct fields must be present (and JSON bools).
	for _, f := range []string{"registered", "hasSynced", "watchHealthy", "typeConfirmed"} {
		v, ok := m[f]
		if !ok {
			t.Fatalf("serve_miss line missing field %q; line: %+v", f, m)
		}
		if _, isBool := v.(bool); !isBool {
			t.Fatalf("serve_miss field %q must be a bool, got %T (%v)", f, v, v)
		}
	}
	// The never-synced/unregistered exit → registered=false, the discriminating
	// signal the boot operator reads.
	if m["registered"] != false {
		t.Fatalf("never-synced GVR: serve_miss registered must be false; line: %+v", m)
	}
	if m["gvr"] != serve1aGVR.String() {
		t.Fatalf("serve_miss gvr = %v, want %s", m["gvr"], serve1aGVR.String())
	}
}

// TestServe1a_ServeHit_DoesNotEmitServeMiss is the HIT-path arm: a
// servable, populated GVR serves from the informer and returns BEFORE the
// miss emit — serve_miss must be absent.
func TestServe1a_ServeHit_DoesNotEmitServeMiss(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	const n = 20
	items := make([]*unstructured.Unstructured, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, serve1aItem(i, "composition-"+itoa(i), "krateo-system"))
	}
	rw := newServe1aWatcher(t, true, items...)

	fixture, caPEM := newServe1aLiveFixture(t, n, int(internalDispatchListPageLimit))
	rc := &rest.Config{
		Host:            fixture.server.URL,
		BearerToken:     "fake-sa-jwt",
		TLSClientConfig: rest.TLSClientConfig{CAData: caPEM},
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx := cache.WithInternalRESTConfig(context.Background(), rc)
	ctx = cache.WithServeWatcher(ctx, rw)
	ctx = withSlogLogger(ctx, logger)

	_, served, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: serve1aListPath},
	})
	if err != nil {
		t.Fatalf("informer-serve dispatch returned error: %v", err)
	}
	if !served {
		t.Fatal("expected served=true from the informer-serve branch")
	}
	// HIT served from the informer — the serve_miss line must be absent.
	if strings.Contains(logBuf.String(), `"msg":"internal_dispatch.list.serve_miss"`) {
		t.Fatalf("serve_miss fired on the HIT path (should return before the miss emit); log:\n%s", logBuf.String())
	}
	// Sanity: it WAS the informer-serve (not a silent live LIST).
	if !strings.Contains(logBuf.String(), `"msg":"internal_dispatch.list.informer_served"`) {
		t.Fatalf("expected informer_served on the HIT path; log:\n%s", logBuf.String())
	}
}
