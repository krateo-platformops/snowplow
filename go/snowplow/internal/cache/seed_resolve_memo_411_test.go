// seed_resolve_memo_411_test.go — #411 unit arms for the SeedResolveMemo
// freshness check: a stamped entry whose captured deps moved past its stamp is
// a MISS; replacement is monotone in stamp; Load/Store/bump are race-safe.

package cache

import (
	"fmt"
	"sync"
	"testing"

	pmaps "github.com/krateo-platformops/plumbing/maps"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var memo411GVR = schema.GroupVersionResource{Group: "apps.krateo.io", Version: "v1", Resource: "compositions"}

// TestSeedResolveMemo411_StaleEntryIsMiss — bucket arm + cold-coordinate arm.
// An entry stamped at seq S is served while its deps see no event; a bump of a
// captured dep's bucket (lastBumpSeq > S) turns it into a miss. A dep with NO
// bucket falls back to the per-GVR floor (#375 fix ii) — conservative.
func TestSeedResolveMemo411_StaleEntryIsMiss(t *testing.T) {
	ResetDepsForTest()
	d := Deps()
	ns := "ns-411"
	listDep := DepKey{GVR: memo411GVR, Namespace: ns, Name: listWildcard}
	d.RecordList(t.Context(), "L1_411_producer", memo411GVR, ns) // bucket exists

	m := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	stamp := DepGenEpochNow()
	m.Store("k", map[string]any{"v": "OLD"}, []DepKey{listDep}, stamp)
	if _, _, _, ok := m.Load("k"); !ok {
		t.Fatal("no dep event since the stamp: the entry must be served")
	}
	d.bumpCoordinateGen(memo411GVR, ns, "obj-0") // stamps the LIST bucket
	if _, _, _, ok := m.Load("k"); ok {
		t.Fatal("#411 RED: a captured dep's bucket moved past the stamp, yet the memo served the entry")
	}
	if got := m.StaleMisses(); got != 1 {
		t.Fatalf("StaleMisses=%d, want 1", got)
	}

	// Cold coordinate (no bucket): the per-GVR floor decides.
	cold := DepKey{GVR: schema.GroupVersionResource{Group: "x.io", Version: "v1", Resource: "things"}, Namespace: ns, Name: "t"}
	stamp2 := DepGenEpochNow()
	m.Store("cold", map[string]any{"v": 1}, []DepKey{cold}, stamp2)
	if _, _, _, ok := m.Load("cold"); !ok {
		t.Fatal("cold dep, no event on its GVR: must be served")
	}
	d.bumpCoordinateGen(cold.GVR, ns, "other") // raises the GVR floor; no bucket for cold
	if _, _, _, ok := m.Load("cold"); ok {
		t.Fatal("#411 RED: a cold dep's GVR floor moved past the stamp, yet the memo served the entry")
	}
}

// TestSeedResolveMemo411_ReplaceIsMonotone — a stale entry is replaced by the
// fresher producer; an OLDER-stamped racing producer can never overwrite a
// fresher entry; an equal-stamp producer keeps first-writer-wins.
func TestSeedResolveMemo411_ReplaceIsMonotone(t *testing.T) {
	ResetDepsForTest()
	m := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	m.Store("k", map[string]any{"v": "A"}, nil, 5)
	m.Store("k", map[string]any{"v": "A2"}, nil, 5) // equal stamp: first writer wins
	if b, _, _, _ := m.Load("k"); b["v"] != "A" {
		t.Fatalf("equal-stamp Store replaced the entry: %v", b)
	}
	m.Store("k", map[string]any{"v": "B"}, nil, 9) // fresher: replaces
	if b, _, _, _ := m.Load("k"); b["v"] != "B" {
		t.Fatalf("a fresher Store must replace the entry: %v", b)
	}
	m.Store("k", map[string]any{"v": "C"}, nil, 7) // older: discarded
	if b, _, _, _ := m.Load("k"); b["v"] != "B" {
		t.Fatalf("an OLDER-stamped Store overwrote a fresher entry: %v", b)
	}
}

// TestSeedResolveMemo411_ConcurrentLoadStoreBump_Race — the memo is shared by
// the seed's cohort goroutines in one pass while the informer bridge bumps
// generations. Run under -race: concurrent Load / Store / bumpCoordinateGen on
// the same keys must be data-race free, a served body is always one a producer
// stored (never torn), and the final entry carries the highest stamp stored.
func TestSeedResolveMemo411_ConcurrentLoadStoreBump_Race(t *testing.T) {
	ResetDepsForTest()
	d := Deps()
	ns := "ns-411-race"
	d.RecordList(t.Context(), "L1_411_race", memo411GVR, ns)
	dep := DepKey{GVR: memo411GVR, Namespace: ns, Name: listWildcard}
	m := NewSeedResolveMemo(pmaps.DeepCopyJSON)

	var wg sync.WaitGroup
	var maxMu sync.Mutex
	var maxStamp uint64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("k%d", i%4)
				if body, _, _, ok := m.Load(key); ok {
					if _, ok := body["stamp"].(float64); !ok {
						t.Errorf("served a body with no producer stamp: %v", body)
						return
					}
					continue
				}
				s := DepGenEpochNow()
				maxMu.Lock()
				if s > maxStamp {
					maxStamp = s
				}
				maxMu.Unlock()
				m.Store(key, pmaps.DeepCopyJSON(map[string]any{"stamp": float64(s)}), []DepKey{dep}, s)
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			d.bumpCoordinateGen(memo411GVR, ns, fmt.Sprintf("o%d", i))
		}
	}()
	wg.Wait()

	// After the storm, a Store at the current epoch (≥ every bump) must be the
	// served entry: monotone replacement converges on the freshest producer.
	now := DepGenEpochNow()
	m.Store("k0", map[string]any{"stamp": float64(now)}, []DepKey{dep}, now)
	b, _, _, ok := m.Load("k0")
	if !ok || b["stamp"] != float64(now) {
		t.Fatalf("after the storm the freshest producer's entry must be served: ok=%v body=%v (maxStamp=%d now=%d)", ok, b, maxStamp, now)
	}
}
