// ra_full_list_uaf_cached_counter_592_test.go — #592 store-primitive arm for
// snowplow_ra_full_list_uaf_cached_total.
//
// WHY THIS FILE EXISTS: the apiref-side arms drive the whole serve path and pin
// that a UAF cell is stored, capped and isolated — but they never produce a
// generation-REFUSED write, so they left the counter's `stored &&` gate unarmed.
// A mutation dropping that gate (counting Puts ATTEMPTED rather than cells
// STORED) passed the entire apiref package. Found by mutating, armed here.
//
// It matters beyond tidiness: this counter is #592's acceptance signal, read off
// a live cluster to decide whether UAF carriers are caching again. A counter
// that counts refused writes reports cached bodies that do not exist, and it
// over-reports exactly when the cluster is churning (DELETE-evictions during
// resolves), which is when someone is most likely to be reading it.

package cache

import (
	"context"
	"testing"
	"time"
)

func TestS592_RAFullListUAFCachedCounter_CountsStoredNotAttempted(t *testing.T) {
	ResetUAFPutDeclineCountersForTest()
	t.Cleanup(ResetUAFPutDeclineCountersForTest)

	c := newResolvedCache(10, 1<<20, time.Hour)
	full := map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{}}
	uafIn := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, HasUAF: true}
	plainIn := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList}

	// ACCEPTED + HasUAF → counts.
	if !c.PutRAFullListIfGen(context.Background(), fixtureStoreVerdict, "rk-uaf", uafIn, full, c.CaptureGen("rk-uaf")) {
		t.Fatalf("setup: a cold-fill PutRAFullListIfGen must be accepted")
	}
	if n := RAFullListUAFCached(); n != 1 {
		t.Fatalf("an ACCEPTED write of a HasUAF cell must tick the counter once; got %d", n)
	}

	// ACCEPTED + non-UAF → does not count. (Pins that the counter measures the
	// UAF population and not raFullList writes in general — without this the
	// acceptance number silently becomes "all 4a traffic".)
	if !c.PutRAFullListIfGen(context.Background(), fixtureStoreVerdict, "rk-plain", plainIn, full, c.CaptureGen("rk-plain")) {
		t.Fatalf("setup: a cold-fill non-UAF PutRAFullListIfGen must be accepted")
	}
	if n := RAFullListUAFCached(); n != 1 {
		t.Fatalf("a non-UAF cell must NOT tick the UAF-cached counter; total moved to %d", n)
	}

	// REFUSED (generation moved by a real DELETE-evict) + HasUAF → must NOT count.
	gen0 := c.CaptureGen("rk-uaf")
	c.DeleteForTest("rk-uaf") // real deleteForDep — tombstones + bumps the gen
	if c.PutRAFullListIfGen(context.Background(), fixtureStoreVerdict, "rk-uaf", uafIn, full, gen0) {
		t.Fatalf("setup: a write carrying the pre-DELETE generation must be REFUSED — without the refusal this arm " +
			"cannot observe the property it exists for")
	}
	if n := RAFullListUAFCached(); n != 1 {
		t.Fatalf("#592 RED: a generation-REFUSED write stored NO cell but ticked the UAF-cached counter (total %d). "+
			"The counter is reporting cached userAccessFilter bodies that do not exist, and it over-reports precisely "+
			"when DELETE-evictions race resolves.", n)
	}
}

// TestS592_RAFullListUAFCap_AppliesToStoredEntry pins the cap at the STORE
// primitive, independently of the serve path that stamps HasUAF. The apiref arm
// asserts the same thing end to end; this one fails without the harness, so a
// future refactor of raFullListServe cannot take the cap down with it silently.
func TestS592_RAFullListUAFCap_AppliesToStoredEntry(t *testing.T) {
	// A non-zero knob, so "capped" is distinguishable from "knob off".
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "300")
	ResetUAFPutDeclineCountersForTest()
	t.Cleanup(ResetUAFPutDeclineCountersForTest)

	c := newResolvedCache(10, 1<<20, time.Hour)
	full := map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{}}

	if !c.PutRAFullListIfGen(context.Background(), fixtureStoreVerdict, "rk-uaf", ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassRAFullList, HasUAF: true,
	}, full, c.CaptureGen("rk-uaf")) {
		t.Fatalf("setup: cold-fill must be accepted")
	}
	entry, ok := c.Get("rk-uaf")
	if !ok {
		t.Fatalf("setup: the stored cell must be readable")
	}
	if want := UAFResolvedTTL(); entry.TTLOverride != want {
		t.Fatalf("#592 RED: a HasUAF raFullList cell carries TTLOverride=%v, want the short cap %v. The key stops "+
			"this cell LEAKING; only the cap bounds it going STALE, because the key folds the requester's bindings "+
			"and not the per-object verdicts the refilter re-evaluates.", entry.TTLOverride, want)
	}

	// The twin primitive must agree — PutRAFullList is test-only today, but a
	// divergence between the two would be invisible until something promoted it.
	c2 := newResolvedCache(10, 1<<20, time.Hour)
	c2.PutRAFullList(fixtureStoreVerdict, "rk-uaf2", ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassRAFullList, HasUAF: true,
	}, full)
	e2, ok := c2.Get("rk-uaf2")
	if !ok {
		t.Fatalf("setup: PutRAFullList must store")
	}
	if e2.TTLOverride != UAFResolvedTTL() {
		t.Fatalf("#592 RED: PutRAFullList and PutRAFullListIfGen disagree about the UAF cap (%v vs %v)",
			e2.TTLOverride, UAFResolvedTTL())
	}

	// And a non-UAF cell is untouched by any of it.
	if !c.PutRAFullListIfGen(context.Background(), fixtureStoreVerdict, "rk-plain", ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassRAFullList,
	}, full, c.CaptureGen("rk-plain")) {
		t.Fatalf("setup: non-UAF cold-fill must be accepted")
	}
	plain, _ := c.Get("rk-plain")
	if plain.TTLOverride != 0 {
		t.Fatalf("#592 RED: a non-UAF raFullList cell took TTLOverride=%v, want 0 — the cap is unconditional and the "+
			"whole class now churns at the short TTL", plain.TTLOverride)
	}
}
