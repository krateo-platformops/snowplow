// resolved_metadata_test.go — R1 §6 Mode 1 + PM F-2 falsifiers for the
// metadata-only /debug/apistage projection.
//
// F-2 (PM condition): a STRUCTURAL leak-guard TEST proving RangeMetadata
// cannot return resolved content — not just a comment. Two arms:
//
//   - STRUCTURAL: reflect over every field of ResolvedEntryMeta and assert
//     NONE is a content-bearing type (no []byte, no slice-of-pointer/struct,
//     no map, no unstructured). A future field that could carry RawJSON /
//     Items / Extras fails the test at the TYPE level, before any handler
//     ever runs. This is the load-bearing guard.
//   - BEHAVIORAL: Put an entry whose RawJSON + Items + Extras carry a unique
//     sentinel, run RangeMetadata, JSON-encode the emitted metadata, and
//     assert the sentinel NEVER appears — while the metadata coordinates
//     (class/gvr/path/age/counts) ARE correct.

package cache

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestRangeMetadata_StructurallyCannotLeakContent is the F-2 STRUCTURAL
// guard: ResolvedEntryMeta must contain only scalar metadata fields. Any
// field whose type could carry a resolved body (a byte slice, a slice of
// objects/pointers, a map, or an unstructured) is a leak vector and fails
// here — at the type level, independent of RangeMetadata's body.
func TestRangeMetadata_StructurallyCannotLeakContent(t *testing.T) {
	mt := reflect.TypeOf(ResolvedEntryMeta{})
	for i := 0; i < mt.NumField(); i++ {
		f := mt.Field(i)
		switch f.Type.Kind() {
		case reflect.String, reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// Scalar — safe. (Note: a []byte would be Kind()==Slice, caught below;
			// a lone Uint8 field is a harmless scalar, not a body.)
		default:
			t.Fatalf("F-2 STRUCTURAL leak: ResolvedEntryMeta.%s has type %s (kind %s) — "+
				"only scalar metadata fields are allowed; a slice/map/pointer/struct field "+
				"could carry resolved content (RawJSON/Items/Extras) and is a cross-user leak vector",
				f.Name, f.Type, f.Type.Kind())
		}
	}
}

// TestRangeMetadata_BehaviorallyEmitsNoBody is the F-2 BEHAVIORAL guard: a
// populated entry's body never reaches the emitted metadata, and the
// metadata coordinates are correct.
func TestRangeMetadata_BehaviorallyEmitsNoBody(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	const sentinel = "SECRET-DO-NOT-LEAK-7f3a9"
	// RawJSON, a parsed Item, and an Extras value all carry the sentinel.
	entry := &ResolvedEntry{
		RawJSON: []byte(`{"secretField":"` + sentinel + `","items":[1,2,3]}`),
		Items: []*unstructured.Unstructured{
			{Object: map[string]any{"metadata": map[string]any{"name": sentinel}}},
			{Object: map[string]any{"metadata": map[string]any{"name": "two"}}},
		},
		ItemsAPIVersion: "v1",
		ItemsKind:       "ConfigMapList",
		Inputs: &ResolvedKeyInputs{
			CacheEntryClass: CacheEntryClassApistage,
			Group:           "",
			Version:         "v1",
			Resource:        "configmaps",
			Namespace:       "",
			Name:            "",
			Stage:           "stage-hash-abc",
			Extras:          map[string]any{"compositionId": sentinel},
		},
	}
	c.put("opaque-key-1", entry)

	var got []ResolvedEntryMeta
	c.RangeMetadata(func(m ResolvedEntryMeta) bool {
		got = append(got, m)
		return true
	})
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 metadata row, got %d", len(got))
	}
	m := got[0]

	// Coordinates are correct (the diagnostic must be useful).
	if m.CacheEntryClass != CacheEntryClassApistage {
		t.Errorf("class = %q, want apistage", m.CacheEntryClass)
	}
	if m.Resource != "configmaps" || m.Version != "v1" {
		t.Errorf("gvr coords wrong: %+v", m)
	}
	if m.Path != "/api/v1/configmaps" {
		t.Errorf("path = %q, want /api/v1/configmaps", m.Path)
	}
	if m.Stage != "stage-hash-abc" {
		t.Errorf("stage = %q, want the opaque stage hash", m.Stage)
	}
	if m.ItemsCount != 2 {
		t.Errorf("itemsCount = %d, want 2 (the LENGTH, not the items)", m.ItemsCount)
	}
	if m.RawJSONBytes != len(entry.RawJSON) {
		t.Errorf("rawJSONBytes = %d, want %d (the LENGTH, not the body)", m.RawJSONBytes, len(entry.RawJSON))
	}

	// THE GUARD: the fully-serialized metadata must NOT contain the sentinel
	// from RawJSON, Items, or Extras anywhere.
	blob, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	if strings.Contains(string(blob), sentinel) {
		t.Fatalf("F-2 BEHAVIORAL leak: the sentinel %q (from RawJSON/Items/Extras) appeared in the "+
			"emitted metadata JSON: %s", sentinel, blob)
	}
}

// TestRangeMetadata_AgeAndTTL covers the age/ttl-remaining projection used
// to spot a stale entry (the R1 verification signal: after a composition
// update the entry's age resets).
func TestRangeMetadata_AgeAndTTL(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	old := &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		CreatedAt: time.Now().Add(-10 * time.Minute),
		Inputs:    &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Version: "v1", Resource: "configmaps"},
	}
	c.put("k-old", old)

	var m ResolvedEntryMeta
	c.RangeMetadata(func(x ResolvedEntryMeta) bool { m = x; return false })

	if m.AgeSeconds < 590 || m.AgeSeconds > 610 {
		t.Errorf("ageSeconds = %d, want ~600 (10 min)", m.AgeSeconds)
	}
	// ttl 1h - age 10min ≈ 3000s remaining.
	if m.TTLRemainingSeconds < 2980 || m.TTLRemainingSeconds > 3020 {
		t.Errorf("ttlRemainingSeconds = %d, want ~3000", m.TTLRemainingSeconds)
	}
}

// TestRangeMetadata_NilReceiver is the cache-off path: a nil store is a
// no-op (the handler reports cacheEnabled=false, zero entries).
func TestRangeMetadata_NilReceiver(t *testing.T) {
	var c *ResolvedCacheStore
	called := false
	c.RangeMetadata(func(ResolvedEntryMeta) bool { called = true; return true })
	if called {
		t.Fatalf("nil-receiver RangeMetadata must not invoke fn")
	}
}
