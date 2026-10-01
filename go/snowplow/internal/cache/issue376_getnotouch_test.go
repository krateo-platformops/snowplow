// issue376_getnotouch_test.go — #376: internal cache reads must not fake #315/#316
// warmth or contaminate hit_total. GetNoTouch is Get minus the three hit-side
// customer-warmth effects (MoveToFront, lastRead stamp, hitTotal); it KEEPS the lazy
// TTL/maxAge evicts. Has is a side-effect-free residency probe (shared with #374).

package cache

import (
	"testing"
	"time"
)

func TestIssue376_GetNoTouch_NoStampNoHit(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "w"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in, SeededAtBoot: true})

	h0 := c.Stats().HitTotal
	if e, ok := c.GetNoTouch(key); !ok || e == nil {
		t.Fatal("GetNoTouch must return the resident entry")
	}
	if got := c.Stats().HitTotal; got != h0 {
		t.Fatalf("#376(iii): GetNoTouch must NOT bump hit_total; %d -> %d", h0, got)
	}
	if m, ok := c.MetadataForKey(key); !ok || m.LastReadSeconds != -1 {
		t.Fatalf("#376(iii): GetNoTouch must NOT stamp lastRead (still -1); got %d", m.LastReadSeconds)
	}
	if _, ok := c.Get(key); !ok {
		t.Fatal("Get must hit")
	}
	if got := c.Stats().HitTotal; got != h0+1 {
		t.Fatalf("#376(iii) control: Get must bump hit_total; %d -> %d", h0, got)
	}
	if m, _ := c.MetadataForKey(key); m.LastReadSeconds < 0 {
		t.Fatalf("#376(iii) control: Get must stamp lastRead (>=0); got %d", m.LastReadSeconds)
	}

	// Full metric neutrality (freshness-audit): a GetNoTouch that MISSES must not
	// bump miss_total either — an internal read is invisible to BOTH ratio counters.
	absent := ComputeKey(ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "ghost"})
	m0 := c.Stats().MissTotal
	if _, ok := c.GetNoTouch(absent); ok {
		t.Fatal("GetNoTouch on an absent key must miss")
	}
	if got := c.Stats().MissTotal; got != m0 {
		t.Fatalf("#376(iii): GetNoTouch must NOT bump miss_total (full metric neutrality); %d -> %d", m0, got)
	}
	if _, ok := c.Get(absent); ok {
		t.Fatal("Get on an absent key must miss")
	}
	if got := c.Stats().MissTotal; got != m0+1 {
		t.Fatalf("#376(iii) control: Get must bump miss_total on a miss; %d -> %d", m0, got)
	}
}

func TestIssue376_GetNoTouch_KeepsLazyEvicts(t *testing.T) {
	c := newResolvedCache(10, 1<<20, 50*time.Millisecond)
	c.maxEntryAge = time.Minute
	inA := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "maxage"}
	keyA := ComputeKey(inA)
	c.Put(keyA, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inA, BornAt: time.Now().Add(-2 * time.Hour), CreatedAt: time.Now()})
	beforeMax := c.Stats().EvictMaxAgeTotal
	beforeMiss := c.Stats().MissTotal
	if _, ok := c.GetNoTouch(keyA); ok {
		t.Fatal("#376(iv): GetNoTouch on a past-maxAge cell must MISS (evicted)")
	}
	if got := c.Stats().EvictMaxAgeTotal; got != beforeMax+1 {
		t.Fatalf("#376(iv): GetNoTouch must KEEP the lazy maxAge evict (evict_max_age %d -> %d)", beforeMax, got)
	}
	// ...but the evict is metric-neutral on the GetNoTouch path: the evict counter
	// moves (correctness), miss_total does NOT (freshness-audit full neutrality).
	if got := c.Stats().MissTotal; got != beforeMiss {
		t.Fatalf("#376(iv): a GetNoTouch evict must NOT bump miss_total; %d -> %d", beforeMiss, got)
	}
	inB := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "ttl"}
	keyB := ComputeKey(inB)
	c.Put(keyB, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inB, CreatedAt: time.Now().Add(-time.Hour)})
	beforeTTL := c.Stats().EvictTTLTotal
	if _, ok := c.GetNoTouch(keyB); ok {
		t.Fatal("#376(iv): GetNoTouch on a past-TTL cell must MISS (evicted)")
	}
	if got := c.Stats().EvictTTLTotal; got != beforeTTL+1 {
		t.Fatalf("#376(iv): GetNoTouch must KEEP the lazy TTL evict (evict_ttl %d -> %d)", beforeTTL, got)
	}
}

func TestIssue376_Has_NoSideEffects(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.maxEntryAge = time.Minute
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "has"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, BornAt: time.Now().Add(-2 * time.Hour), CreatedAt: time.Now()})
	h0, m0, e0 := c.Stats().HitTotal, c.Stats().MissTotal, c.Stats().EvictMaxAgeTotal
	if !c.Has(key) {
		t.Fatal("#376 Has: must report a resident key")
	}
	if !c.Has(key) {
		t.Fatal("#376 Has: must STILL report resident — Has must not evict (no side effects)")
	}
	s := c.Stats()
	if s.HitTotal != h0 || s.MissTotal != m0 || s.EvictMaxAgeTotal != e0 {
		t.Fatalf("#376 Has: must touch NO counter (hit %d->%d miss %d->%d evictMax %d->%d)",
			h0, s.HitTotal, m0, s.MissTotal, e0, s.EvictMaxAgeTotal)
	}
	if c.Has(ComputeKey(ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n", Name: "absent"})) {
		t.Fatal("#376 Has: must report false for an absent key")
	}
}

func TestIssue376_InternalReadStaysCold_No316Enqueue(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	gvr := gvrFlexes()
	const ns = "krateo-system"
	in := widgetInputs(gvr, ns, "coldbody")
	key := ComputeKey(*in)
	// customer-COLD (lastRead old) yet body-fresh/APPROACHING-TTL: Put old (cold-fill
	// lastRead=CreatedAt=-100s) then ReplaceIfGen CreatedAt=-16s (replace-in-place
	// inherits lastRead=-100s). Models a refresher-kept-fresh cell no customer read in 100s.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: in, CreatedAt: time.Now().Add(-100 * time.Second)})
	gen := c.CaptureGen(key)
	if !c.ReplaceIfGen(key, &ResolvedEntry{RawJSON: []byte(`{"v":"refreshed"}`), Inputs: in, CreatedAt: time.Now().Add(-16 * time.Second)}, gen) {
		t.Fatal("setup: ReplaceIfGen must succeed")
	}
	if m, _ := c.MetadataForKey(key); m.LastReadSeconds < 20 || m.TTLRemainingSeconds >= 5 {
		t.Fatalf("setup: want cold (LastReadSeconds>=20) + approaching (TTLRemaining<5); got lastRead=%d ttlRem=%d", m.LastReadSeconds, m.TTLRemainingSeconds)
	}

	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatal("GetNoTouch must hit (within TTL)")
	}
	if m, _ := c.MetadataForKey(key); m.LastReadSeconds < 20 {
		t.Fatalf("#376(i): GetNoTouch must NOT re-warm — lastRead must stay cold; got %d", m.LastReadSeconds)
	}
	before := c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before {
		t.Fatalf("#376(i): a customer-cold cell read only internally must NOT be #316-enqueued; %d -> %d", before, got)
	}

	if _, ok := c.Get(key); !ok {
		t.Fatal("Get must hit (within TTL)")
	}
	before = c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before+1 {
		t.Fatalf("#376(ii) control: after a CUSTOMER Get the warm approaching-TTL cell MUST be #316-enqueued; %d -> %d", before, got)
	}
}

// TestIssue376_WarmGauge_InternalReadNeitherBucket is the #376 scope-add arm: the
// warm_seeded / warm_lastread decomposition computed in reapPastMaxEntryAge's walk.
// A customer-cold cell read only internally (GetNoTouch, no lastRead stamp, not
// seeded) lands in NEITHER bucket once its cold-fill lastRead has aged past TTL; a
// later CUSTOMER Get stamps lastRead=now and moves it into warm_lastread. This is the
// live readout that makes #376 observable (warm_lastread → ~0 on an unbrowsed cluster).
func TestIssue376_WarmGauge_InternalReadNeitherBucket(t *testing.T) {
	c := newResolvedCache(10, 1<<20, 20*time.Second)
	c.maxEntryAge = time.Hour // nothing reaped; isolate the gauge
	gvr := gvrFlexes()
	const ns = "krateo-system"
	in := widgetInputs(gvr, ns, "warmgauge")
	key := ComputeKey(*in)
	// customer-COLD (lastRead aged past TTL) yet body within-TTL (resident): Put old
	// (cold-fill lastRead=CreatedAt=-100s) then ReplaceIfGen CreatedAt=-1s — replace-
	// in-place inherits the old lastRead=-100s while the body is freshly within TTL.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: in, CreatedAt: time.Now().Add(-100 * time.Second)})
	gen := c.CaptureGen(key)
	if !c.ReplaceIfGen(key, &ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: in, CreatedAt: time.Now().Add(-1 * time.Second)}, gen) {
		t.Fatal("setup: ReplaceIfGen must succeed")
	}
	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatal("setup: GetNoTouch must hit (body within TTL)")
	}

	c.reapPastMaxEntryAge()
	s := c.Stats()
	if s.WarmSeeded != 0 || s.WarmLastRead != 0 {
		t.Fatalf("#376 warm-gauge: an internal-only-read cell (cold lastRead, not seeded) must be in NEITHER bucket; warm_seeded=%d warm_lastread=%d", s.WarmSeeded, s.WarmLastRead)
	}

	if _, ok := c.Get(key); !ok {
		t.Fatal("control: customer Get must hit (body within TTL)")
	}
	c.reapPastMaxEntryAge()
	s = c.Stats()
	if s.WarmLastRead != 1 || s.WarmSeeded != 0 {
		t.Fatalf("#376 warm-gauge control: after a CUSTOMER Get the cell must land in warm_lastread; warm_seeded=%d warm_lastread=%d", s.WarmSeeded, s.WarmLastRead)
	}
}

func TestIssue376_IsSelfRepresentationUsesGetNoTouch(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	gvr := gvrFlexes()
	const ns = "krateo-system"
	in := widgetInputs(gvr, ns, "selfprobe")
	key := ComputeKey(*in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: in, SeededAtBoot: true})

	h0 := c.Stats().HitTotal
	_ = Deps().isSelfRepresentation(c, key, DepKey{GVR: gvr, Namespace: ns, Name: "selfprobe"})
	if got := c.Stats().HitTotal; got != h0 {
		t.Fatalf("WIRING #376: isSelfRepresentation must use GetNoTouch (no hit_total bump); %d -> %d", h0, got)
	}
	if m, _ := c.MetadataForKey(key); m.LastReadSeconds != -1 {
		t.Fatalf("WIRING #376: isSelfRepresentation must not stamp lastRead (still -1); got %d", m.LastReadSeconds)
	}
}
