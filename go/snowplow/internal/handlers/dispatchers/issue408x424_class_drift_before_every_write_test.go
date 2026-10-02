// issue408x424_class_drift_before_every_write_test.go — #408 × #424 composition.
//
// #424's identity-class drift guard must run before EVERY seed terminal write. Since
// #408 there are two write carriers:
//   - the pre-readyz boot Put (PutThenRemark);
//   - the post-readyz gen-guarded Put (PutIfGen). This covers every non-boot mode, plus
//     boot units that are captured or written after /readyz.
// TestKey424_SeedTerminalPut_DeclinesOnClassDrift runs boot mode in whatever phase1
// state the package left behind, so it exercises only one of the two carriers. This arm
// pins both explicitly.
//
// For each /readyz state it mints the key, moves the cohort's RBAC class (a real grant),
// and calls seedTerminalPut. The arm asserts:
//   - the write is refused;
//   - no cell lands under the stale key;
//   - no #375 remark fires (a declined write must not enqueue a refresh).
// The control (no class change) must land through the carrier the state selects.
//
// NEUTER: the drift check moved after the PutThenRemark branch (guarding only PutIfGen)
// → the pre-readyz sub-arm goes RED.
package dispatchers

import (
	"context"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue408x424_ClassDriftDeclinesBeforeEveryWriteCarrier(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readyz  bool
		carrier string
	}{
		{"pre-readyz_boot_PutThenRemark", false, "PutThenRemark"},
		{"post-readyz_boot_PutIfGen", true, "PutIfGen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetPhase1DoneForTest()
			t.Cleanup(cache.ResetPhase1DoneForTest)
			a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
			dyn := psBuildWatcher(t, a)
			if tc.readyz {
				cache.MarkPhase1Done()
			}
			var mu sync.Mutex
			remarks := 0
			t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(string, string) {
				mu.Lock()
				remarks++
				mu.Unlock()
			}))
			cohort := withCohortSeedContext(context.Background(), seedTarget{Username: psCarol, Groups: []string{psGroup}},
				endpoints.Endpoint{}, nil)

			// Control first: an unchanged class lands through this state's carrier.
			ckey, chandle, cinputs := k424SeedKey(t, cohort)
			cg := seedTerminalGuardFor(seedModeBoot, chandle, ckey)
			if tc.readyz != cg.guarded {
				t.Fatalf("PRECONDITION: readyz=%v must select guarded=%v (got %+v)", tc.readyz, tc.readyz, cg)
			}
			rctx := cache.WithL1KeyContext(cohort, ckey)
			if !seedTerminalPut(rctx, chandle, ckey, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: cinputs}, cg) {
				t.Fatalf("CONTROL (%s): an unchanged class must be written via %s", tc.name, tc.carrier)
			}
			if _, hit := chandle.Get(ckey); !hit {
				t.Fatalf("CONTROL (%s): the cell must be present", tc.name)
			}
			chandle.(*cache.ResolvedCacheStore).DeleteForTest(ckey)

			// Drift: the class moves after the mint, before the terminal write.
			key, handle, inputs := k424SeedKey(t, cohort)
			g := seedTerminalGuardFor(seedModeBoot, handle, key)
			k424Grant(t, dyn, a, psCarol)
			mu.Lock()
			remarks = 0
			mu.Unlock()
			dctx := cache.WithL1KeyContext(cohort, key)
			if seedTerminalPut(dctx, handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs}, g) {
				t.Fatalf("#408x#424 RED (%s): seedTerminalPut wrote through %s under a key minted for the "+
					"cohort's PRE-grant class — the class-drift guard must run before every write carrier",
					tc.name, tc.carrier)
			}
			if _, hit := handle.Get(key); hit {
				t.Fatalf("#408x#424 RED (%s): a cell exists under the stale key", tc.name)
			}
			mu.Lock()
			defer mu.Unlock()
			if remarks != 0 {
				t.Fatalf("#408x#424 RED (%s): a declined write fired %d #375 remarks", tc.name, remarks)
			}
		})
	}
}
