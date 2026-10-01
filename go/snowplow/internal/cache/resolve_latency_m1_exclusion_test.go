package cache

// resolve_latency_m1_exclusion_test.go — #386 M1, PM gate condition 2: the p95
// resolve-latency estimator must measure the REAL resolve work ONLY. A workload
// made of skips, no-handler entries, rate-floor deferrals and customer-priority
// parks must leave p95 UNMOVED (==0) — only the ok=true handler resolve in
// processOne registers. This is the discriminating arm: if the sample were taken
// at the wrong site (dequeue-to-completion, or unconditionally) a skip/park-heavy
// run would inflate p95. Proven RED by moving the sample off the fn() bracket.
//
// NB filename: NO "_386"/"_arm"/GOOS/GOARCH token before _test.go — such a suffix
// is an implicit build constraint and silently excludes the file
// (reference_go_filename_goos_goarch_build_constraint).

import (
	"context"
	"testing"
	"time"
)

// TestIssue386_M1_ExcludesSkipNoHandlerAndFloor drives the non-resolve paths and
// asserts p95 stays 0, then drives real resolves and asserts it moves.
func TestIssue386_M1_ExcludesSkipNoHandlerAndFloor(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 1)
	defer cleanup()
	// A positive floor so the FLOOR arm below actually defers a fresh entry.
	t.Setenv(envRefresherRateFloorSeconds, "3600")

	c := ResolvedCache()
	r := refresherSingleton()
	ctx := context.Background()

	if v := RefresherP95ResolveMS(); v != 0 {
		t.Fatalf("#386 M1: baseline p95 = %v, want 0 (no resolve has run)", v)
	}

	// Arm SKIP (ok=false): the skipped_no_entry early-return precedes fn().
	for i := 0; i < 200; i++ {
		_ = r.processOne(ctx, "missing", nil, false)
	}
	if v := RefresherP95ResolveMS(); v != 0 {
		t.Fatalf("#386 M1: skipped_no_entry moved p95 to %v; want 0 — a skip must not time as a resolve", v)
	}

	// Arm NO-HANDLER (ok=true, no handler registered for the class): the
	// skipped_no_handler early-return precedes fn().
	nhInputs := ResolvedKeyInputs{CacheEntryClass: "no-such-class", Name: "nh"}
	nhEntry := &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &nhInputs}
	for i := 0; i < 200; i++ {
		_ = r.processOne(ctx, "nh", nhEntry, true)
	}
	if v := RefresherP95ResolveMS(); v != 0 {
		t.Fatalf("#386 M1: no-handler skip moved p95 to %v; want 0", v)
	}

	// Arm FLOOR (processNext defers a FRESH entry before processOne): put an
	// entry whose CreatedAt is ~now and enqueue it; with a 1h floor, processNext
	// Forget+AddAfter+flooredTotal++ and returns BEFORE processOne/fn().
	flInputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "fl"}
	flKey := ComputeKey(flInputs)
	c.Put(flKey, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &flInputs})
	// A handler exists for the class so ONLY the floor (not no-handler) can
	// explain an unmoved p95 on this arm.
	var floored int
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
		floored++ // must stay 0 on the floor arm
		return nil
	})
	for i := 0; i < 50; i++ {
		enqueueRefreshForTest(flKey)
		r.processNext(ctx)
	}
	if floored != 0 {
		t.Fatalf("#386 M1: floor arm invoked the handler %d time(s) — the entry was not floored, arm invalid", floored)
	}
	if v := RefresherP95ResolveMS(); v != 0 {
		t.Fatalf("#386 M1: rate-floor deferral moved p95 to %v; want 0 — a floored dequeue does no resolve", v)
	}

	// Arm REAL RESOLVE (ok=true, handler does a known amount of work): p95 MUST
	// move. Proves the sample fires on the real path (inclusion), so the zeros
	// above are exclusion, not a dead counter.
	const work = 15 * time.Millisecond
	RegisterRefreshFunc("restactions", func(context.Context, string, ResolvedKeyInputs) error {
		time.Sleep(work)
		return nil
	})
	realInputs := ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "real"}
	realEntry := &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &realInputs}
	for i := 0; i < 10; i++ {
		_ = r.processOne(ctx, "real", realEntry, true)
	}
	if v := RefresherP95ResolveMS(); v < 10 {
		t.Fatalf("#386 M1: real resolve did NOT move p95 (got %v ms, want >= ~%v) — the sample never fires", v, work)
	}
}

// TestIssue386_M1_ExcludesPark proves the customer-priority PARK (yieldToCustomer)
// is excluded: a resolve that waited a long park before running must record ~the
// resolve time, NOT park+resolve. If the sample bracketed the park, p95 would be
// dominated by the park.
func TestIssue386_M1_ExcludesPark(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 1)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "0") // no floor — we want the resolve to run

	c := ResolvedCache()
	r := refresherSingleton()
	ctx := context.Background()

	const work = 10 * time.Millisecond
	const park = 200 * time.Millisecond
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
		time.Sleep(work)
		return nil
	})

	// Park the worker: the hook reports a customer in flight, cleared after `park`.
	parkUntil := time.Now().Add(park)
	SetCustomerInflightHook(func() bool { return time.Now().Before(parkUntil) })
	defer SetCustomerInflightHook(nil)

	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "parked"}
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})

	// processNext yields (parks ~200ms) THEN resolves (~10ms). We time the whole
	// call to confirm the park really happened, but assert the RECORDED p95 is the
	// resolve time, not the park.
	start := time.Now()
	enqueueRefreshForTest(key)
	r.processNext(ctx)
	wall := time.Since(start)

	if wall < park {
		t.Fatalf("#386 M1: park arm wall %v < park %v — the worker did not actually park, arm invalid", wall, park)
	}
	v := RefresherP95ResolveMS()
	if v == 0 {
		t.Fatalf("#386 M1: park arm recorded no resolve (p95=0) — the resolve did not run after the park")
	}
	// The recorded p95 must be the resolve work (~10ms), far below park+work (~210ms).
	if v > float64((park+work)/2/time.Millisecond) {
		t.Fatalf("#386 M1: park arm p95 = %v ms includes the ~%v park (wall %v) — park time is NOT excluded",
			v, park, wall)
	}
}
