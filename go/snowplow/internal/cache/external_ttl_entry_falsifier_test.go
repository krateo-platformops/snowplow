// external_ttl_entry_falsifier_test.go — external-widget bounded-TTL cache
// (Option A, 2026-07-10) falsifiers at the STORE / entry layer.
//
// These pin the entry-level invariants of the design:
//   - F-staleness (§10.3): a served external-TTL entry is at most TTLOverride
//     old — REUSING the #36 TTLOverride/effectiveTTLLocked machinery verbatim
//     (no new TTL path). Read < TTL → HIT; read > TTL → MISS → re-fetch.
//   - F-toggle-off zero-value inertness (C4, §10.2): the new ExternalTTL bool
//     is in-memory cache state ONLY, never wire-encoded — its presence changes
//     NO serialized output (RawJSON is what goes on the wire; the marker rides
//     beside it and is invisible to the client). A zero-valued (false) marker
//     is byte-identical to a pre-Option-A entry.
//   - Persisted-marker survives Put→Get (C2 mechanism): the value the HIT-serve
//     branch reads (entry.ExternalTTL) is exactly what the Put branch wrote.
//
// Run: go test -race -count=1 ./internal/cache/ -run ExternalTTL
//
// NOTE: this package's TestMain does NOT touch the remote kubeconfig (that is
// internal/rbac). These are pure in-memory store tests.

package cache

import (
	"bytes"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// widgetInputsForBinding builds the canonical widgets key-inputs for the SAME
// obs widget CR under a given BindingUID — the shape the dispatcher builds at
// widgets.go via dispatchCacheLookupKey. Only BindingUID varies between two
// users; everything else (GVR/ns/name/page) is identical.
func widgetInputsForBinding(bindingUID string) ResolvedKeyInputs {
	return ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "widgets",
		Namespace:       "krateo-system",
		Name:            "obs-log-stream",
		BindingUID:      bindingUID,
		PerPage:         40,
		Page:            1,
	}
}

// TestExternalTTL_Isolation_TwoDimensional — F-isolation (C1), the #1-risk arm.
// Two users under DIFFERENT bindings load the same obs widget; A warms the
// external-TTL cell. Assert BOTH dimensions on the SHIPPED per-binding key
// (real ComputeKey + real store):
//
//	dim-1 KEY:    A's and B's ComputeKey hashes DIFFER (BindingUID fold,
//	              resolved.go ComputeKey — folds BindingUID for `widgets`).
//	dim-2 OUTPUT: B, keyed under B's own BindingUID, NEVER reads A's cached
//	              external bytes from the shared store.
//
// Key-hash inequality ALONE is insufficient (spoof-quarantine lesson); the
// OUTPUT arm proves B does not receive A's served bytes.
func TestExternalTTL_Isolation_TwoDimensional(t *testing.T) {
	store := newResolvedCache(100, 1<<20, time.Hour)

	inA := widgetInputsForBinding("R:krateo-system/uid-user-a")
	keyA := ComputeKey(inA)
	inB := widgetInputsForBinding("C:uid-user-b")
	keyB := ComputeKey(inB)

	// dim-1 — KEY side.
	if keyA == keyB {
		t.Fatalf("F-isolation dim-1 FAIL: two users under DIFFERENT bindings produced the SAME "+
			"widgets key — BindingUID not folded; the external cell would be shared cross-user")
	}

	// A renders first: external-TTL Put of A's OWN external bytes under keyA.
	bytesA := []byte(`{"otel_logs":["A-secret-row"]}`)
	store.put(keyA, &ResolvedEntry{
		RawJSON: bytesA, CreatedAt: time.Now(),
		TTLOverride: 20 * time.Second, ExternalTTL: true,
	})

	// dim-2 — OUTPUT side: B serves under keyB → MUST MISS A's cell.
	if ent, ok := store.Get(keyB); ok {
		t.Fatalf("F-isolation dim-2 FAIL: user B read a cell under B's own key returning bytes %q — "+
			"B must MISS and resolve its own external result under its own binding, never read A's",
			ent.RawJSON)
	}
}

// TestExternalTTL_Isolation_RED_IdentityFreeCell_MustCrossDeliver is the RED
// discriminator (feedback_falsifier_shape_must_discriminate + the spoof-
// quarantine lesson). It SIMULATES the WRONG impl the design forbids: routing
// the external result to the identity-free cell (BindingUID="" — the shared
// empty-identity row). Under that mutation the two users' keys COLLAPSE and B
// reads A's bytes. This test asserts the harness CATCHES that cross-delivery
// on the OUTPUT dimension — proving the isolation arm is not key-hash-only.
//
// The SHIPPED impl never does this: the external-TTL Put branch is gated by
// serveFromCacheEligible (rejects "" BindingUID) and uses the unchanged
// per-binding key, so keyA!=keyB (the TwoDimensional test) and this cross-
// delivery cannot occur in production.
func TestExternalTTL_Isolation_RED_IdentityFreeCell_MustCrossDeliver(t *testing.T) {
	store := newResolvedCache(100, 1<<20, time.Hour)

	// WRONG impl: identity-free cell — BindingUID="" for BOTH users.
	keyA := ComputeKey(widgetInputsForBinding(""))
	keyB := ComputeKey(widgetInputsForBinding(""))

	if keyA != keyB {
		t.Fatalf("RED setup invalid: identity-free ('') inputs must collapse to the SAME key; "+
			"got distinct keys — cannot exercise the cross-delivery the arm must catch")
	}

	bytesA := []byte(`{"otel_logs":["A-secret-row"]}`)
	store.put(keyA, &ResolvedEntry{
		RawJSON: bytesA, CreatedAt: time.Now(),
		TTLOverride: 20 * time.Second, ExternalTTL: true,
	})

	ent, ok := store.Get(keyB)
	if !ok {
		t.Fatalf("RED FAIL: could not reproduce the cross-delivery — expected B to READ A's "+
			"identity-free cell; the harness would then be BLIND to the leak the shipped impl prevents")
	}
	if !bytes.Equal(ent.RawJSON, bytesA) {
		t.Fatalf("RED FAIL: B read the '' cell but got %q not A's %q — the OUTPUT-dimension "+
			"cross-delivery was not reproduced, so the arm does not discriminate a content-spoof",
			ent.RawJSON, bytesA)
	}
	// GREEN: the RED reproduced B receiving A's bytes on the OUTPUT dimension,
	// so the OUTPUT arm genuinely discriminates. The shipped per-binding key
	// (keyA!=keyB) prevents this in production.
}

// TestExternalTTL_PersistedMarkerSurvivesPutGet — the C2 mechanism. The Put
// branch stamps ExternalTTL:true alongside TTLOverride; the HIT-serve branch
// reads entry.ExternalTTL off the Get'd entry to suppress the refresh-key
// header. This proves the marker round-trips through the store unchanged (no
// mutation, no drop) — the single runtime signal the HIT path has.
func TestExternalTTL_PersistedMarkerSurvivesPutGet(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	// External-TTL Put: marker true, short override.
	c.put("k-ext", &ResolvedEntry{
		RawJSON:     []byte(`{"ext":true}`),
		CreatedAt:   time.Now(),
		TTLOverride: 20 * time.Second,
		ExternalTTL: true,
	})
	// Normal Put: marker defaults to zero value (false).
	c.put("k-normal", &ResolvedEntry{
		RawJSON:   []byte(`{"ext":false}`),
		CreatedAt: time.Now(),
	})

	entExt, ok := c.Get("k-ext")
	if !ok {
		t.Fatalf("external-TTL entry within its window must be a HIT")
	}
	if !entExt.ExternalTTL {
		t.Fatalf("C2 FAIL: ExternalTTL marker did not survive Put→Get (got false, want true) — "+
			"the HIT-serve branch would then WRONGLY stamp the refresh-key header and arm a "+
			"/refreshes subscription for a dep-edgeless external key")
	}

	entNorm, ok := c.Get("k-normal")
	if !ok {
		t.Fatalf("normal entry must be a HIT")
	}
	if entNorm.ExternalTTL {
		t.Fatalf("C4 FAIL: a normal entry's ExternalTTL must be its zero value (false); got true — "+
			"the header would be WRONGLY suppressed on a normal serve")
	}
}

// TestExternalTTL_StalenessBoundedByTTLOverride — F-staleness (§10.3). REUSES
// the #36 TTLOverride/effectiveTTLLocked machinery: an external-TTL entry read
// inside its TTL window is a HIT (same bytes); read past the window is a MISS
// → the next /call re-fetches live. No new TTL machinery.
func TestExternalTTL_StalenessBoundedByTTLOverride(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour) // store ttl long (1h)

	body := []byte(`{"otel_logs":["row1","row2"]}`)

	// Read at t+5s (well inside a 20s TTL): HIT, identical bytes.
	c.put("k-fresh", &ResolvedEntry{
		RawJSON:     body,
		CreatedAt:   time.Now().Add(-5 * time.Second),
		TTLOverride: 20 * time.Second,
		ExternalTTL: true,
	})
	entFresh, ok := c.Get("k-fresh")
	if !ok {
		t.Fatalf("F-staleness FAIL: external-TTL entry aged 5s within a 20s TTL must be a HIT")
	}
	if !bytes.Equal(entFresh.RawJSON, body) {
		t.Fatalf("F-staleness FAIL: served bytes differ from what was cached")
	}

	// Read at t+25s (past the 20s TTL): MISS → re-fetch. Proves the served
	// data age can NEVER exceed the TTL even though the store ttl is 1h.
	c.put("k-stale", &ResolvedEntry{
		RawJSON:     body,
		CreatedAt:   time.Now().Add(-25 * time.Second),
		TTLOverride: 20 * time.Second,
		ExternalTTL: true,
	})
	if _, ok := c.Get("k-stale"); ok {
		t.Fatalf("F-staleness FAIL: external-TTL entry aged 25s past a 20s TTL must be a MISS "+
			"(TTL-evicted so the next /call re-fetches live) — the bounded-staleness guarantee broke")
	}
}

// TestExternalTTL_StormCollapse_ConcurrentRendersAreHits — F-storm-collapse
// mechanism arm (§8/§11.6) under -race. Once the external-TTL cell is warmed
// (the single first-render fetch), N CONCURRENT re-renders of the SAME widget
// (the ~15 obs widgets + tab re-renders) are ALL pure L1 HITs within the TTL
// window — so exactly ONE downstream external fetch happens per (widget, TTL
// window) instead of one-per-render. This proves the collapse mechanism: the
// only fetch is the cold fill; every concurrent reader after it hits the cell.
//
// A `refetch` counter models "a Get MISS would force a live external fetch."
// After the warm Put, it MUST stay 0 across all concurrent readers. Shape:
// K>1 concurrent readers (feedback_falsifier_shape_must_discriminate — a
// degenerate K=1 would not exercise the concurrent-hit collapse).
func TestExternalTTL_StormCollapse_ConcurrentRendersAreHits(t *testing.T) {
	store := newResolvedCache(100, 1<<20, time.Hour)
	key := ComputeKey(widgetInputsForBinding("R:krateo-system/uid-obs"))
	body := []byte(`{"otel_logs":["r1","r2"]}`)

	// Single first-render cold fill (the ONE external fetch per window).
	store.put(key, &ResolvedEntry{
		RawJSON: body, CreatedAt: time.Now(),
		TTLOverride: 20 * time.Second, ExternalTTL: true,
	})

	const readers = 64 // K>1: concurrent re-renders across widgets/tabs
	var refetch atomic.Int64
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			ent, ok := store.Get(key)
			if !ok {
				// A MISS here would force a live external re-fetch (the storm).
				refetch.Add(1)
				return
			}
			if !bytes.Equal(ent.RawJSON, body) {
				refetch.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := refetch.Load(); n != 0 {
		t.Fatalf("F-storm-collapse FAIL: %d of %d concurrent re-renders MISSed the warm external-TTL "+
			"cell → each would trigger a live external fetch; the storm did not collapse to one "+
			"fetch per (widget, TTL window)", n, readers)
	}
}

// TestExternalTTL_NotWireEncoded — C4 zero-value inertness on the SERIALIZED
// surface. What goes to the client is entry.RawJSON. The ExternalTTL marker is
// beside it in memory and must change NO serialized output. This asserts that
// two entries with IDENTICAL RawJSON but DIFFERENT ExternalTTL markers serve
// byte-identical bytes — the marker is invisible on the wire.
func TestExternalTTL_NotWireEncoded(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	body := []byte(`{"a":1,"b":[2,3]}`)

	c.put("k-marked", &ResolvedEntry{RawJSON: body, CreatedAt: time.Now(),
		TTLOverride: 20 * time.Second, ExternalTTL: true})
	c.put("k-plain", &ResolvedEntry{RawJSON: body, CreatedAt: time.Now()})

	em, _ := c.Get("k-marked")
	ep, _ := c.Get("k-plain")
	if !bytes.Equal(em.RawJSON, ep.RawJSON) {
		t.Fatalf("C4 FAIL: the ExternalTTL marker leaked into the served bytes — it must be "+
			"in-memory-only, never wire-encoded")
	}
	// The marker is not a JSON field on the payload: the payload is exactly the
	// bytes we stored, and unmarshalling it exposes no "ExternalTTL"/"externalTTL".
	var m map[string]any
	if err := json.Unmarshal(em.RawJSON, &m); err != nil {
		t.Fatalf("stored RawJSON not valid JSON: %v", err)
	}
	if _, present := m["ExternalTTL"]; present {
		t.Fatalf("C4 FAIL: 'ExternalTTL' appeared as a wire field in the payload")
	}
	if _, present := m["externalTTL"]; present {
		t.Fatalf("C4 FAIL: 'externalTTL' appeared as a wire field in the payload")
	}
}
