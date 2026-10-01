// ra_full_list_ifgen_323_test.go — #323 store-primitive arm for
// PutRAFullListIfGen (the generation-guarded PutRAFullList variant): accept a
// cold-fill / no-eviction write, REFUSE a write whose raKey generation moved via
// a real DELETE-evict (the pre-delete full body is not resurrected).

package cache

import (
	"context"
	"testing"
	"time"
)

func TestPutRAFullListIfGen_S323_AcceptColdFill_RefuseAfterDelete(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	full := map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{}}

	// ACCEPT — a cold fill (key absent, gen 0) is stored.
	if !c.PutRAFullListIfGen(context.Background(), "rk", ResolvedKeyInputs{}, full, c.CaptureGen("rk")) {
		t.Fatalf("#323: a cold-fill PutRAFullListIfGen (no intervening removal) must be ACCEPTED")
	}
	if _, ok := c.Get("rk"); !ok {
		t.Fatalf("#323: accepted cold-fill must be readable")
	}

	// REFUSE — a real DELETE-evict between capture and write bumps the generation.
	gen0 := c.CaptureGen("rk")
	c.DeleteForTest("rk") // real deleteForDep — tombstones + bumps the gen
	if c.PutRAFullListIfGen(context.Background(), "rk", ResolvedKeyInputs{}, full, gen0) {
		t.Fatalf("#323: PutRAFullListIfGen carrying the pre-DELETE generation must be REFUSED")
	}
	if _, ok := c.Get("rk"); ok {
		t.Fatalf("#323: the DELETE-evicted RAFullList cell must NOT be resurrected")
	}
	if r := c.Stats().PutRefusedGenerationMovedTotal; r == 0 {
		t.Fatalf("#323: the refusal must bump put_refused_generation_moved_total")
	}

	// ACCEPT (no over-refusal) — an independent cold key with no eviction stores.
	if !c.PutRAFullListIfGen(context.Background(), "rk2", ResolvedKeyInputs{}, full, c.CaptureGen("rk2")) {
		t.Fatalf("#323: a second cold-fill (no eviction) must be ACCEPTED — no over-refusal")
	}
}
