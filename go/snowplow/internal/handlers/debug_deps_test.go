package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// debug_deps_test.go — the edge-3 /debug/deps diagnostic (issue #277). It is
// metadata-only: it flags the serve-seam staleness class (a widget cell with no
// backing edge) WITHOUT ever emitting a resolved body.

func depsBackingGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps.krateo.io", Version: "v1", Resource: "compositions"}
}

func depsWidgetInputs(name string) *cache.ResolvedKeyInputs {
	return &cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "widgets",
		Namespace:       "krateo-system",
		Name:            name,
	}
}

func driveDebugDeps(t *testing.T, rawQuery string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://snowplow.example/debug/deps?"+rawQuery, nil)
	rec := httptest.NewRecorder()
	DebugDeps().ServeHTTP(rec, req)
	res := rec.Result()
	return res.StatusCode, rec.Body.String()
}

// TestDebugDeps_MissingBackingEdgeDetectorAndNoBodyLeak drives both ?key and
// ?filter=missing-backing-edges over two resident widget cells: one with a
// backing edge, one without. The detector flags only the edgeless cell, ?key
// reflects hasBackingEdge correctly, and NO resolved body ever appears in any
// response (only its sha256).
func TestDebugDeps_MissingBackingEdgeDetectorAndNoBodyLeak(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true")
	}
	d := cache.Deps()

	const bodyMarker = "example-body-content-that-must-never-leak"
	body := []byte(`{"payload":"` + bodyMarker + `"}`)

	restactionsGVR := schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	backing := depsBackingGVR()

	const backingKey = "L1_deps_has_backing"
	const edgelessKey = "L1_deps_no_backing"

	// Cell WITH a backing edge: templates edge + a real backing LIST edge.
	store.Put(backingKey, &cache.ResolvedEntry{RawJSON: body, Inputs: depsWidgetInputs("w-backing")})
	d.Record(backingKey, restactionsGVR, "krateo-system", "some-ra")
	d.RecordList(backingKey, backing, "krateo-system")

	// Cell WITHOUT a backing edge: only the RA-CR template edge (the edge-3
	// staleness class — served by the 4a fast path pre-fix).
	store.Put(edgelessKey, &cache.ResolvedEntry{RawJSON: body, Inputs: depsWidgetInputs("w-edgeless")})
	d.Record(edgelessKey, restactionsGVR, "krateo-system", "some-ra")

	// --- ?filter=missing-backing-edges ---
	code, out := driveDebugDeps(t, "filter=missing-backing-edges")
	if code != http.StatusOK {
		t.Fatalf("filter status=%d, want 200", code)
	}
	var fv depsFilterView
	if err := json.Unmarshal([]byte(out), &fv); err != nil {
		t.Fatalf("decode filter response: %v (%s)", err, out)
	}
	if !depsSampleHas(fv.Sample, edgelessKey) {
		t.Fatalf("missing-backing-edges did NOT flag the edgeless cell %q: %+v", edgelessKey, fv)
	}
	if depsSampleHas(fv.Sample, backingKey) {
		t.Fatalf("missing-backing-edges WRONGLY flagged the backed cell %q: %+v", backingKey, fv)
	}
	if strings.Contains(out, bodyMarker) {
		t.Fatalf("BODY LEAK: the filter response contained the resolved body marker")
	}

	// --- ?key for the edgeless cell: hasBackingEdge=false, sha256 present, no body ---
	code, out = driveDebugDeps(t, "key="+edgelessKey)
	if code != http.StatusOK {
		t.Fatalf("key status=%d, want 200", code)
	}
	var kv depsKeyView
	if err := json.Unmarshal([]byte(out), &kv); err != nil {
		t.Fatalf("decode key response: %v (%s)", err, out)
	}
	if kv.HasBackingEdge {
		t.Fatalf("?key on the edgeless cell reported hasBackingEdge=true: %+v", kv)
	}
	if !kv.Resident || kv.BodySha256 == "" {
		t.Fatalf("?key must report a resident cell with a bodySha256: %+v", kv)
	}
	if strings.Contains(out, bodyMarker) {
		t.Fatalf("BODY LEAK: the ?key response contained the resolved body marker")
	}

	// --- ?key for the backed cell: hasBackingEdge=true ---
	_, out = driveDebugDeps(t, "key="+backingKey)
	if err := json.Unmarshal([]byte(out), &kv); err != nil {
		t.Fatalf("decode key response: %v (%s)", err, out)
	}
	if !kv.HasBackingEdge {
		t.Fatalf("?key on the backed cell reported hasBackingEdge=false: %+v", kv)
	}
}

func depsSampleHas(sample []depsFilterSample, key string) bool {
	for _, s := range sample {
		if s.KeyHash == key {
			return true
		}
	}
	return false
}
