// issue406x424_declined_put_records_no_layer_test.go — #406 × #424 composition. A seed
// terminal write that #424's identity-class drift guard DECLINES must not record
// layered sources: the cell was never stored, so no raKey edge may point at it, and a
// later raKey change must not remark it. The control (no class change) records exactly
// one edge. Both write carriers are covered (pre-readyz PutThenRemark, post-readyz
// PutIfGen), using the #408x#424 harness.
package dispatchers

import (
	"context"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue406x424_DeclinedSeedPutRecordsNoLayeredSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		readyz bool
	}{
		{"pre-readyz_boot_PutThenRemark", false},
		{"post-readyz_boot_PutIfGen", true},
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
			layered := 0
			t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(_ string, reason string) {
				if reason == "layered" {
					mu.Lock()
					layered++
					mu.Unlock()
				}
			}))
			cohort := withCohortSeedContext(context.Background(), seedTarget{Username: psCarol, Groups: []string{psGroup}},
				endpoints.Endpoint{}, nil)

			// A resident raKey the seed widget Go-sliced.
			raIn := cache.RAFullListKeyInputsForTest("templates.krateo.io", "v1", "restactions",
				"krateo-system", "ra-406x424-"+tc.name, "C:uid-406x424", nil)
			raKey := cache.ComputeKey(raIn)
			putRA := func(body string) {
				t.Helper()
				in := raIn
				cache.ResolvedCache().Put(raKey, &cache.ResolvedEntry{RawJSON: []byte(body), Inputs: &in})
			}
			putRA(`{"items":["v1"]}`)
			slice := func(ctx context.Context) {
				t.Helper()
				e, ok := cache.ResolvedCache().GetNoTouch(raKey)
				if !ok {
					t.Fatalf("setup: raKey not resident")
				}
				cache.ResolvedCache().NoteRAFullListSlice(ctx, raKey, e)
			}

			// Control: an unchanged class lands and records one edge.
			ckey, chandle, cinputs := k424SeedKey(t, cohort)
			cg := seedTerminalGuardFor(seedModeBoot, chandle, ckey)
			cctx := cache.WithL1KeyContext(cohort, ckey)
			slice(cctx)
			if !seedTerminalPut(cctx, chandle, ckey, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: cinputs}, cg) {
				t.Fatalf("CONTROL (%s): an unchanged class must be written", tc.name)
			}
			if _, cons := cache.ResolvedCache().RAFullListLayerStatsForTest(); cons != 1 {
				t.Fatalf("CONTROL (%s): the stored seed cell must record its raKey source, got %d edges", tc.name, cons)
			}
			chandle.(*cache.ResolvedCacheStore).DeleteForTest(ckey)
			if _, cons := cache.ResolvedCache().RAFullListLayerStatsForTest(); cons != 0 {
				t.Fatalf("setup: removing the control cell must drop its edge, got %d", cons)
			}

			// Drift: the class moves after the mint; the write is declined.
			key, handle, inputs := k424SeedKey(t, cohort)
			g := seedTerminalGuardFor(seedModeBoot, handle, key)
			dctx := cache.WithL1KeyContext(cohort, key)
			slice(dctx)
			k424Grant(t, dyn, a, psCarol)
			if seedTerminalPut(dctx, handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs}, g) {
				t.Fatalf("setup (%s): the drifted write must be declined (#424)", tc.name)
			}
			if recs, cons := cache.ResolvedCache().RAFullListLayerStatsForTest(); recs != 0 || cons != 0 {
				t.Fatalf("#406x#424 RED (%s): a DECLINED seed write recorded layered sources (%d / %d)", tc.name, recs, cons)
			}
			putRA(`{"items":["v2"]}`)
			mu.Lock()
			defer mu.Unlock()
			if layered != 0 {
				t.Fatalf("#406x#424 RED (%s): a raKey change remarked %d cells that were never stored", tc.name, layered)
			}
		})
	}
}
