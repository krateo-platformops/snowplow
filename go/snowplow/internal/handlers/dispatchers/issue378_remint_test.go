package dispatchers

// issue378_remint_test.go — #378 P1 falsifiers at the dispatcher boundary
// (brief issuecomment-5990884686). Harness: issue378_remint_harness_test.go.
//
//   F1   THE GATE. K=4 classes {restactions GET-by-name page=2&perPage=10 +
//        extras, widgets with keyExtras, raFullList, widgetContent (+ the nested
//        apistage cell that rides along)} × M=8 cells, kept warm by customer
//        reads and refreshed by real informer UPDATEs + #316. Customer GETs after
//        the ORIGINAL BornAt + maxAge: 32/32 HIT. RED on the parent: 32 misses.
//   F2a  a refresher re-Put whose BornAt age is < maxAge−L INHERITS BornAt.
//   F2b  a refresher re-Put inside [maxAge−L, maxAge) RESETS BornAt. RED on parent.
//   F2c  the seed modes (boot, keepwarm, gvr-discovered, #258 rbac-shift) writing
//        a resident in-window cell through the ONE terminal-Put mechanism inherit
//        BornAt (the store-level reflective twin, covering every write method, is
//        cache.TestIssue378_F2c_OnlyTheRefresherTerminalReMints).
//
// Every cell ages by REAL elapse under short env bounds; no SetBornAtForTest.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func r378SleepUntil(at time.Time) {
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// TestIssue378_F1_WarmCellsCrossTheCapWithoutAColdNavigation — the gate.
func TestIssue378_F1_WarmCellsCrossTheCapWithoutAColdNavigation(t *testing.T) {
	r378RunF1(t, nil)
}

// r378RunF1 is the F1 body. inWindow, when set, runs once inside the lead
// window (the F1-seed parent-SHA driver uses it).
func r378RunF1(t *testing.T, inWindow func(e *r378Env)) {
	const ttlS, maxAgeS, m = 8, 12, 8 // L = min(8, 6) = 6s → lead window [6s, 12s)
	maxAge := time.Duration(maxAgeS) * time.Second
	e := r378Setup(t, ttlS, maxAgeS, m)
	e.fill(t)
	e.startRefresher(t)
	born := e.maxBorn()
	completed0 := cache.RefresherStatsByStat()["completed"]

	stopWarm, stopChurn := make(chan struct{}), make(chan struct{})
	warm := e.keepWarm(t, time.Second, stopWarm)
	churn := e.churn(t, 1500*time.Millisecond, stopChurn)
	if inWindow != nil {
		r378SleepUntil(born.Add(maxAge/2 + 500*time.Millisecond))
		inWindow(e)
	}
	// Customer traffic stops well before the cap (its last read stays within the
	// TTL, so every cell is still WARM at the cap); churn stops just before it.
	r378SleepUntil(born.Add(maxAge - 1500*time.Millisecond))
	close(stopWarm)
	warm.Wait()
	r378SleepUntil(born.Add(maxAge - 800*time.Millisecond))
	close(stopChurn)
	churn.Wait()

	// Snapshot each cell's residency + birth BEFORE the cap (GetNoTouch is
	// metric-neutral and the cells are still inside the cap here).
	c := cache.ResolvedCache()
	type snap struct {
		resident bool
		born     time.Time
	}
	pre := map[string]snap{}
	for _, cell := range e.cells {
		ent, ok := c.GetNoTouch(cell.key)
		pre[cell.key] = snap{resident: ok}
		if ok {
			pre[cell.key] = snap{resident: true, born: ent.BornAt}
		}
	}
	t.Logf("F1: refresher completed Δ=%d across the run", cache.RefresherStatsByStat()["completed"]-completed0)

	// After the ORIGINAL BornAt + maxAge.
	r378SleepUntil(born.Add(maxAge + 500*time.Millisecond))
	s0 := cache.ResolvedCacheStatsByStat()
	hitsByClass := map[string]int{}
	for i := 0; i < m; i++ {
		for _, class := range r378Classes {
			h0 := r378CellHits(class)
			if err := e.serve(e.alice, class, i); err != nil {
				t.Errorf("F1: %v", err)
			}
			if r378CellHits(class)-h0 == 1 {
				hitsByClass[class]++
			}
		}
	}
	s1 := cache.ResolvedCacheStatsByStat()
	hits, misses := s1["hit_total"]-s0["hit_total"], s1["miss_total"]-s0["miss_total"]
	served := 0
	for _, n := range hitsByClass {
		served += n
	}
	want := len(r378Classes) * m
	t.Logf("F1: final GETs at original BornAt+maxAge+0.5s: served from the cell=%d/%d by class %v; hit_total Δ=%d "+
		"miss_total Δ=%d evict_max_age_total Δ=%d", served, want, hitsByClass, hits, misses,
		s1["evict_max_age_total"]-s0["evict_max_age_total"])
	if served != want || hits != int64(want) || misses != 0 {
		t.Errorf("#378 F1 RED: %d of %d customer GETs after the original BornAt+maxAge were COLD NAVIGATIONS (served from "+
			"the cell by class %v; hit_total Δ=%d, miss_total Δ=%d, evict_max_age_total Δ=%d) — warm cells the refresher "+
			"kept body-fresh still reached the C5 cap under a customer", want-served, want, hitsByClass, hits, misses,
			s1["evict_max_age_total"]-s0["evict_max_age_total"])
	}
	if v, ok := s1["evict_max_age_warm_customer_total"]; !ok {
		t.Errorf("#378 F1 RED: evict_max_age_warm_customer_total (the P5 trigger) is not published")
	} else if d := v - s0["evict_max_age_warm_customer_total"]; d != 0 {
		t.Errorf("#378 F1 RED: evict_max_age_warm_customer_total Δ=%d, want 0", d)
	}
	advanced := map[string]int{}
	for _, cell := range e.cells {
		p := pre[cell.key]
		if !p.resident {
			t.Errorf("#378 F1: %s cell %s was not resident under its ORIGINAL key before the cap (key changed or evicted)", cell.class, cell.key)
			continue
		}
		if p.born.After(cell.born) {
			advanced[cell.class]++
		}
	}
	for _, class := range r378Classes {
		if advanced[class] != m {
			t.Errorf("#378 F1 RED: %d/%d %s cells had their BornAt advanced (re-minted under the same key) before the cap",
				advanced[class], m, class)
		}
	}
	if v, ok := s1["remint_total"]; !ok || v == 0 {
		t.Errorf("#378 F1 RED: remint_total=%d (published=%v), want > 0", v, ok)
	}
}

// r378IdentityFreeCell puts one identity-free widgetContent cell (no RBAC
// representative needed) and returns its inputs and key.
func r378IdentityFreeCell(t *testing.T, ttlS, maxAgeS int, name string) (cache.ResolvedKeyInputs, string) {
	t.Helper()
	k423Env(t)
	r378Bounds(t, ttlS, maxAgeS)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	in := cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group: h1WidgetGVR.Group, Version: h1WidgetGVR.Version, Resource: h1WidgetGVR.Resource,
		Namespace: h1NS, Name: name, PerPage: -1, Page: -1}
	key := cache.ComputeKey(in)
	inCopy := in
	cache.ResolvedCache().Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"fill"}`), Inputs: &inCopy})
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(`{"v":"refreshed"}`), nil
	})
	t.Cleanup(restore)
	return in, key
}

// TestIssue378_F2a_RefresherRePutBeforeTheWindowInheritsBornAt.
func TestIssue378_F2a_RefresherRePutBeforeTheWindowInheritsBornAt(t *testing.T) {
	in, key := r378IdentityFreeCell(t, 60, 4, "f2a") // L = min(60, 2) = 2s → window [2s, 4s)
	c := cache.ResolvedCache()
	e0, _ := c.GetNoTouch(key)
	time.Sleep(500 * time.Millisecond) // age 0.5s < maxAge−L
	if err := resolveAndPopulateL1(context.Background(), in, nil, nil); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	e1, ok := c.GetNoTouch(key)
	if !ok || string(e1.RawJSON) != `{"v":"refreshed"}` {
		t.Fatalf("SETUP: the refresher terminal must have written the cell")
	}
	if !e1.BornAt.Equal(e0.BornAt) {
		t.Fatalf("#378 F2a RED: a refresher re-Put BEFORE the lead window reset BornAt (%v → %v) — that extends the C5 cap", e0.BornAt, e1.BornAt)
	}
}

// TestIssue378_F2b_RefresherRePutInsideTheWindowResetsBornAt.
func TestIssue378_F2b_RefresherRePutInsideTheWindowResetsBornAt(t *testing.T) {
	in, key := r378IdentityFreeCell(t, 60, 4, "f2b")
	c := cache.ResolvedCache()
	e0, _ := c.GetNoTouch(key)
	time.Sleep(2300 * time.Millisecond) // age 2.3s ∈ [maxAge−L, maxAge)
	remint0, _ := r378Stat("remint_total")
	before := time.Now()
	if err := resolveAndPopulateL1(context.Background(), in, nil, nil); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	e1, ok := c.GetNoTouch(key)
	if !ok || string(e1.RawJSON) != `{"v":"refreshed"}` {
		t.Fatalf("SETUP: the refresher terminal must have written the cell")
	}
	if !e1.BornAt.After(e0.BornAt) || e1.BornAt.Before(before) {
		t.Fatalf("#378 F2b RED: the refresher terminal re-Put INSIDE the lead window kept BornAt %v (born %v) — the cell will "+
			"reach the C5 cap and its next customer GET is a cold navigation", e1.BornAt, e0.BornAt)
	}
	if v := r378MustStat(t, "remint_total"); v-remint0 != 1 {
		t.Fatalf("#378 F2b: remint_total Δ=%d, want 1", v-remint0)
	}
}

// TestIssue378_F2c_SeedModesInheritBornAtInTheWindow — keepwarm, boot (pre- and
// post-readyz), gvr-discovered and #258 rbac-shift terminal writes of a RESIDENT
// in-window cell go through seedTerminalPut and INHERIT BornAt; plus the full
// gvr-discovered seedOneWidget path (it never skips).
func TestIssue378_F2c_SeedModesInheritBornAtInTheWindow(t *testing.T) {
	r378Bounds(t, 60, 4) // window [2s, 4s)
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)
	store := cache.ResolvedCache()
	modes := []seedScopeMode{seedModeBoot, seedModeKeepwarm, seedModeGVRDiscovered, seedModeRBACShift}
	keys := map[seedScopeMode]string{}
	born := map[seedScopeMode]time.Time{}
	for _, mode := range modes {
		k := "r378-f2c-" + mode.String()
		store.Put(k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":1}`)})
		e, _ := store.GetNoTouch(k)
		keys[mode], born[mode] = k, e.BornAt
	}
	e := reseedWidgetEntry()
	ctx := reseedSeedCtx()
	wKey, handle, inputs := reseedWidgetKey(t, ctx, e)
	handle.Put(wKey, &cache.ResolvedEntry{RawJSON: []byte(`{"seed":1}`), Inputs: inputs})
	wE0, _ := handle.GetNoTouch(wKey)

	time.Sleep(2300 * time.Millisecond) // every cell is inside the lead window

	for _, mode := range modes {
		k := keys[mode]
		g := seedTerminalGuardFor(mode, store, k)
		if !seedTerminalPut(context.Background(), store, k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":2}`)}, g) {
			t.Fatalf("%s: terminal Put refused on a live cell", mode)
		}
		got, ok := store.GetNoTouch(k)
		if !ok || string(got.RawJSON) != `{"v":2}` {
			t.Fatalf("%s: terminal Put did not write", mode)
		}
		if !got.BornAt.Equal(born[mode]) {
			t.Errorf("#378 F2c RED: seed mode %s RESET BornAt of a resident in-window cell (%v → %v); only the refresher "+
				"terminal may re-mint", mode, born[mode], got.BornAt)
		}
	}
	if err := seedOneWidget(ctx, e, h1NS, seedModeGVRDiscovered); err != nil {
		t.Fatalf("seedOneWidget(gvr-discovered): %v", err)
	}
	wE1, ok := handle.GetNoTouch(wKey)
	if !ok || wE1 == wE0 {
		t.Fatalf("SETUP: gvr-discovered must re-write the resident cell")
	}
	if !wE1.BornAt.Equal(wE0.BornAt) {
		t.Errorf("#378 F2c RED: the gvr-discovered seed re-Put of an in-window cell reset BornAt (%v → %v)", wE0.BornAt, wE1.BornAt)
	}
}
