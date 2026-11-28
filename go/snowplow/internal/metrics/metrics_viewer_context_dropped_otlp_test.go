package metrics

// The viewer-context drop counter reaches OTLP per fixed viewer-context key
// (registered in registerInstruments, observed via the REAL callback over a
// ManualReader). Mirrors metrics_311_malformed_dial_otlp_test.go. Guards the
// CARDINALITY bound: the series count equals the key enum and every emitted
// attribute value is in it; a value-derived attribute would blow it.

import (
	"encoding/json"
	"expvar"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/viewercontext"
)

func TestViewerContextDroppedReachesOTLP(t *testing.T) {
	viewercontext.ResetDroppedForTest()
	t.Cleanup(viewercontext.ResetDroppedForTest)

	enum := viewercontext.DroppedKeyEnum()
	if len(enum) == 0 {
		t.Fatal("the drop counter has an empty key enum")
	}
	// A distinct count per key proves the callback reads the ACTUAL atomic.
	want := map[string]int64{}
	for i, k := range enum {
		for j := 0; j <= i+1; j++ {
			viewercontext.RecordDroppedForTest(k)
		}
		want[k] = int64(i + 2)
	}

	pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_viewer_context_dropped_total")
	if len(pts) != len(enum) {
		t.Fatalf("snowplow_viewer_context_dropped_total must reach OTLP as %d {viewer_key} series "+
			"(0 = unregistered / expvar-only); got %d", len(enum), len(pts))
	}
	enumSet := map[string]bool{}
	for _, k := range enum {
		enumSet[k] = true
	}
	got := map[string]int64{}
	for _, p := range pts {
		if len(p.attrs) != 1 {
			t.Fatalf("a drop point must carry exactly the {viewer_key} attribute, got %+v", p.attrs)
		}
		k, ok := p.attrs["viewer_key"]
		if !ok {
			t.Fatalf("a drop OTLP point carries no {viewer_key} attribute: %+v", p)
		}
		if !enumSet[k] {
			t.Fatalf("cardinality bound VIOLATED: key %q is not in the fixed enum %v", k, enum)
		}
		got[k] = p.value
	}
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("key %q OTLP value = %d, want %d: the callback is not reading the drop atomic", k, got[k], w)
		}
	}

	// /debug/vars publishes the SAME counts, as a map keyed by the same enum.
	// A bare number, or a map from another source, fails here.
	v := expvar.Get("snowplow_viewer_context_dropped_total")
	if v == nil {
		t.Fatal("snowplow_viewer_context_dropped_total is not published on expvar")
	}
	var ev map[string]uint64
	if err := json.Unmarshal([]byte(v.String()), &ev); err != nil {
		t.Fatalf("expvar value is not a map{key -> uint64}: %v (%s)", err, v.String())
	}
	if len(ev) != len(got) {
		t.Fatalf("expvar has %d keys, OTLP has %d: %v vs %v", len(ev), len(got), ev, got)
	}
	for k, n := range got {
		if int64(ev[k]) != n {
			t.Fatalf("key %q: expvar = %d, OTLP = %d; the two surfaces diverged", k, ev[k], n)
		}
	}
}
