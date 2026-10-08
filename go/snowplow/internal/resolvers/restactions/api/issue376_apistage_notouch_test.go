// issue376_apistage_notouch_test.go — #376: the apistage content SERVE
// (apistageContentServe) stamps #315/#316 warmth on a customer /call, but a refresher
// whole-RA re-resolve read of the same content cell is INTERNAL and must NOT stamp
// (else it fakes content-cell warmth + contaminates the hit/miss ratio — the #376 class
// at the content layer). Reuses the R1 refresh-bypass harness (same package).

package api

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue376_ApistageContentServe_RefresherReadIsNoTouch(t *testing.T) {
	_ = newR1RefreshBypassWatcher(t) // installs the cache.Global watcher + fresh object
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache nil")
	}
	putR1StaleContentEntry(store) // content cell resident (GET-by-name r1FsaGVR)

	// REFRESHER path: WithRefreshTriggerGVR for a DIFFERENT GVR → forceContentMiss=false
	// → the HIT is SERVED, but as an INTERNAL refresher read it must NOT stamp warmth.
	//
	// #552 — the ctx now carries WithBackgroundResolve as well, which is what the
	// PRODUCTION refresher ctx carries: resolveOnceProd stamps the background
	// marker on every refresher resolve (it is the single refresher resolve entry,
	// reached from the one refreshFunc closure registered for all five entry
	// classes), and refresher.go:1059 adds the trigger GVR on top at dequeue. This
	// fixture previously modelled the refresher with HALF its markers — a trigger
	// and no background stamp, a shape no production path builds — and that was
	// the gap #552 lived in: the read primitive was keyed on the trigger, so a
	// refresher dequeue with no trigger read as a customer. The primitive is now
	// keyed on background-ness, so the arm has to present the real refresher ctx.
	otherGVR := schema.GroupVersionResource{Group: "other.krateo.io", Version: "v1", Resource: "others"}
	h0, m0 := store.Stats().HitTotal, store.Stats().MissTotal
	refresherCtx := cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(r1Ctx()), otherGVR)
	_, served, ok := apistageContentServe(refresherCtx, store, r1GetCall(), false)
	if !ok || !served {
		t.Fatal("refresher-path content HIT must still serve (different-GVR trigger → forceContentMiss=false)")
	}
	if s := store.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
		t.Fatalf("#376: a refresher re-resolve content read must NOT stamp hit/miss (hit %d->%d miss %d->%d)", h0, s.HitTotal, m0, s.MissTotal)
	}

	// CUSTOMER path (no refresh marker): the SERVE stamps hit_total.
	_, servedC, okC := apistageContentServe(r1Ctx(), store, r1GetCall(), false)
	if !okC || !servedC {
		t.Fatal("customer content HIT must serve")
	}
	if s := store.Stats(); s.HitTotal != h0+1 {
		t.Fatalf("#376 control: a customer content serve must stamp hit_total (%d->%d)", h0, s.HitTotal)
	}
}
