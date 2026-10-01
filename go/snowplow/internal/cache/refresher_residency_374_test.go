//go:build unit || integration

// refresher_residency_374_test.go — #374 #91 Lever C regression guard.
//
// Part C (the fan-out Has-before-isSelf probe) was DROPPED from #374 (TL ruling:
// Has() takes the same c.mu Lock as the Get-miss it replaced and ADDS a lock for
// resident keys; its missTotal-de-contamination benefit is delivered by #376's
// GetNoTouch conversion of the same isSelf Get). What remains relevant here is
// the #91 Lever C invariant the eventual post-#375 enqueue-drop must not break,
// and which #374's future-drop marker guards: a cell-less stuck-false RAFullList
// raKey MUST still fire the enqueue hook (where SubmitSliceabilityInvalidate
// clears the stuck-false sliceability memo). This arm pins that.

package cache

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func contains374(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// TestFalsifier374_91LeverC_StuckFalseRAKeyStillEnqueued — the #91 regression
// guard. A stuck-false RAFullList raKey has a dep edge but NO cell (verdict=false
// never Put). On a backing-object event it MUST still enter toMark / fire the
// enqueue hook — because the production hook (refresher.go SetRefreshHook) fires
// SubmitSliceabilityInvalidate(raKey) there, clearing the stuck-false
// sliceability memo so the next /call re-verifies. This holds today (the fan-out
// enqueues every matched key, resident or not); the arm + the future-drop marker
// guard it against a later residency-gated enqueue-drop that forgets to keep
// SubmitSliceabilityInvalidate firing (:475) when it gates r.enqueue (:473).
func TestFalsifier374_91LeverC_StuckFalseRAKeyStillEnqueued(t *testing.T) {
	d := newTestDepTracker(t, 1_000)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d.SetStore(store)
	gvr := gvrCompositions()
	ns, name := "ns", "backing-obj"

	// Stuck-false RAFullList raKey: dep edge recorded (ra_full_list.go:523), NO
	// cell (PutRAFullList only on verdict=true). THIS is the #91 Lever C shape.
	d.Record("raKey-stuck-false", gvr, ns, name)

	fired := []string{}
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { fired = append(fired, k) })

	d.OnObjectEvent(gvr, ns, name, objExists)

	if !contains374(fired, "raKey-stuck-false") {
		t.Fatalf("#91 Lever C REGRESSION: the cell-less stuck-false raKey must STILL fire the enqueue hook (that is where SubmitSliceabilityInvalidate clears the stuck-false sliceability memo) — the memo would never recover otherwise. fired=%v", fired)
	}
}
