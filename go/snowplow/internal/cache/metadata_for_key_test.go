// metadata_for_key_test.go — 1.12.5 / #187 acceptance for the single-entry
// inspector behind /debug/apistage?key_hash=.
//
// The load-bearing arm is the NEGATIVE one. L1 cells are per-identity — the
// key folds BindingUID, so on krateo-057 the same widget was resident under
// admin, system:gke-common-webhooks and system:kubestore-collector. Returning
// a body to whoever holds the debug JWT would be a cross-identity read of rows
// that requester's own RBAC would have filtered; the /call path enforces that
// boundary and the debug path must not bypass it. So the inspector returns a
// SHA-256 instead, which still answers the question an invalidation bug
// raises — "is this the same body as before, or was it re-resolved?" — without
// revealing anything.
//
// The second arm pins the other half: inspecting an entry must not CHANGE it.
// A diagnostic that routed through Get would enforce TTL, bump the hit
// counters and move the entry to the LRU front, making the thing it measures
// look hotter than it is and potentially evicting it as a side effect of being
// looked at.

package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMetadataForKey_ReturnsAHashNeverTheBody(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	const secret = `{"children":["agents-header"],"tenantSecretish":"do-not-leak"}`
	store := newResolvedCache(10, 1<<20, time.Hour)
	inputs := &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "flexes",
		Namespace:       "krateo-system",
		Name:            "page-agents",
		BindingUID:      "uid-cohort-a",
	}
	const key = "L1_inspect_me"
	store.Put(key, &ResolvedEntry{RawJSON: []byte(secret), Inputs: inputs})

	meta, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatalf("MetadataForKey: resident entry not found")
	}

	// The response, as the handler would serialise it, must not contain the
	// body — asserted on the SERIALISED BYTES, not on a field list, so a
	// future field that smuggles content in fails here.
	blob, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, fragment := range []string{"do-not-leak", "agents-header", "children"} {
		if strings.Contains(string(blob), fragment) {
			t.Fatalf("#187 inspector LEAKED body content (%q) into the metadata response. "+
				"L1 cells are per-identity; a body dump to a debug-JWT holder is a "+
				"cross-identity read of rows their own RBAC would have filtered.\n%s",
				fragment, blob)
		}
	}

	// The hash must be the real hash of the real body — otherwise "same body
	// as before?" cannot be answered and the field is decoration.
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(secret)))
	if meta.BodySHA256 != want {
		t.Fatalf("BodySHA256 = %q, want %q", meta.BodySHA256, want)
	}
	// And it must discriminate: a different body must hash differently.
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"children":["agents-main"]}`), Inputs: inputs})
	after, _ := store.MetadataForKey(key)
	if after.BodySHA256 == want {
		t.Fatalf("BodySHA256 did not change across a re-Put with different content — the field "+
			"cannot answer \"was this re-resolved?\" (%s)", after.BodySHA256)
	}

	// The opaque per-identity discriminator is present: #187 needed to know
	// how many cohort-scoped copies of one widget were resident.
	if meta.BindingUID != "uid-cohort-a" {
		t.Fatalf("BindingUID = %q, want the stored cohort uid", meta.BindingUID)
	}
}

func TestMetadataForKey_DoesNotTouchTheEntry(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	store := newResolvedCache(10, 1<<20, time.Hour)
	inputs := &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "w"}
	store.Put("L1_a", &ResolvedEntry{RawJSON: []byte(`{"a":1}`), Inputs: inputs})
	store.Put("L1_b", &ResolvedEntry{RawJSON: []byte(`{"b":1}`), Inputs: inputs})

	beforeHits := store.Stats().HitTotal
	beforeMiss := store.Stats().MissTotal

	if _, ok := store.MetadataForKey("L1_a"); !ok {
		t.Fatalf("entry not found")
	}
	if _, ok := store.MetadataForKey("nope"); ok {
		t.Fatalf("absent key reported as found")
	}

	if got := store.Stats().HitTotal; got != beforeHits {
		t.Fatalf("inspecting an entry bumped hit_total (%d -> %d). A diagnostic must not make "+
			"the thing it measures look hotter than it is.", beforeHits, got)
	}
	if got := store.Stats().MissTotal; got != beforeMiss {
		t.Fatalf("inspecting a missing key bumped miss_total (%d -> %d)", beforeMiss, got)
	}
	if _, ok := store.Get("L1_b"); !ok {
		t.Fatalf("a neighbouring entry was evicted by an inspection")
	}
}
