//go:build unit

package cache

// issue1255_evict_unstorable_test.go — #1255 on the REAL store
// (evict_unstorable.go): a resolve whose decline proves its key unstorable
// evicts the cell it could not replace.
//
//   U1  the rule, by the decline's REASON: every permanent verdict (declared
//       write, external touch, stage error beside one) and the two non-verdict
//       proofs (sensitive read, UAF) evict an older cell; a transient verdict, an
//       "unobserved" verdict and a storing one do not.
//   U2  THE RACE, deterministic: a fresh Put whose resolve began after the
//       declining one began is never dropped; a body resolved before it is.
//   U2s THE RACE, concurrent (under -race): declining resolves and fresh Puts of
//       one key interleave; no eviction ever removes a body resolved after the
//       declining resolve began.
//   U3  the cell's own contract: an external-TTL cell is kept by an external
//       decline the opt-in turns into a store; a declared write still evicts it.
//   U4  an inert (dry-run) resolve, or one without a dep-gen sink, evicts nothing.
//   U5  the subscriber: the eviction publishes the refresh frame, and an open
//       stale serve of the key is replaced and closes PUSHED when that frame is
//       written — never left as cache.stale_push_unreplaced.
//   U6  a TTL-reset path found in code (that it is what 057 hit is inferred, not
//       proven): a widget cell Go-sliced from a raFullList cell (#406) is
//       remarked when that cell is evicted as unstorable (a decline makes no
//       raKey commit, so nothing else would).
//   U3b a transient failure of an opt-in (external-TTL) cell's own endpoint keeps
//       the cell (review of #1263, F1).
//   U7  every eviction is counted under its reason (the OTLP series).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func u1255Store(t *testing.T) *ResolvedCacheStore {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	CompleteRBACSnapshotForTest(t)
	ResetResolvedCacheForTest()
	t.Cleanup(ResetResolvedCacheForTest)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	c := ResolvedCache()
	Deps().SetStore(c)
	return c
}

// u1255Resolve is a resolve that begins NOW: its ctx carries a fresh dep-gen
// sink for key (startSeq = the current dep-event sequence).
func u1255Resolve(key string) context.Context {
	return WithL1KeyContext(context.Background(), key)
}

// u1255Change advances the dep-event sequence, as an object event does.
func u1255Change() { depEventSeq.Add(1) }

// u1255Put stores body under key from a resolve that began at ctx's start.
func u1255Put(t *testing.T, c *ResolvedCacheStore, ctx context.Context, key, body string, mut ...func(*ResolvedEntry)) *ResolvedEntry {
	t.Helper()
	e := &ResolvedEntry{RawJSON: []byte(body), Inputs: &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: key}}
	for _, m := range mut {
		m(e)
	}
	if !c.putIfGen(ctx, key, e, c.CaptureGen(key)) {
		t.Fatalf("PRECONDITION: Put of %s refused", key)
	}
	return e
}

func TestIssue1255_U1_TheRuleIsTheDeclineReason(t *testing.T) {
	permanent := map[string]PutVerdict{
		"declared_write":          {decision: PutDeclinePermanent, reason: PutReasonDeclaredWrite},
		"external_touched":        {decision: PutDeclinePermanent, reason: PutReasonExternal},
		"stage_error_beside_ext":  {decision: PutDeclinePermanent, reason: PutReasonStageError},
		"external_third_party_ok": {decision: PutDeclinePermanent, reason: PutReasonExternal, thirdPartyOnly: true},
	}
	keeps := map[string]PutVerdict{
		"stage_error_transient": {decision: PutDeclineTransient, reason: PutReasonStageError},
		"unobserved":            {reason: PutReasonUnobserved},
		"store":                 {decision: PutStore},
	}
	for name, v := range permanent {
		t.Run("evicts/"+name, func(t *testing.T) {
			c := u1255Store(t)
			u1255Put(t, c, u1255Resolve("k"), "k", `{"pre":"edit"}`)
			u1255Change()
			if !c.EvictOnPermanentDecline(u1255Resolve("k"), v, "k") {
				t.Fatalf("#1255 RED: a %s decline kept the pre-change cell", name)
			}
			if c.Has("k") || c.Stats().EvictUnstorableTotal != 1 {
				t.Fatalf("#1255 RED: resident=%v evict_unstorable=%d", c.Has("k"), c.Stats().EvictUnstorableTotal)
			}
			if c.Stats().EvictDeleteTotal != 0 {
				t.Fatal("#1255: the informer-DELETE discriminator must not move")
			}
		})
	}
	for _, reason := range []string{UnstorableSensitive, UnstorableUAF} {
		t.Run("evicts/"+reason, func(t *testing.T) {
			c := u1255Store(t)
			u1255Put(t, c, u1255Resolve("k"), "k", `{"pre":"edit"}`)
			u1255Change()
			if !c.EvictUnstorable(u1255Resolve("k"), "k", reason) || c.Has("k") {
				t.Fatalf("#1255 RED: a %s decline kept the pre-change cell", reason)
			}
		})
	}
	for name, v := range keeps {
		t.Run("keeps/"+name, func(t *testing.T) {
			c := u1255Store(t)
			prior := u1255Put(t, c, u1255Resolve("k"), "k", `{"pre":"edit"}`)
			u1255Change()
			if c.EvictOnPermanentDecline(u1255Resolve("k"), v, "k") {
				t.Fatalf("#1255 over-reach: a %s verdict evicted the cell", name)
			}
			if got, ok := c.GetNoTouch("k"); !ok || got != prior {
				t.Fatalf("#1255 over-reach: a %s verdict did not keep the prior entry", name)
			}
		})
	}
}

func TestIssue1255_U2_AFreshPutAfterTheDecliningResolveBeganIsNeverDropped(t *testing.T) {
	ext := PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}

	t.Run("fresh_put_spared", func(t *testing.T) {
		c := u1255Store(t)
		u1255Put(t, c, u1255Resolve("k"), "k", `{"v":"old"}`)
		u1255Change()
		declining := u1255Resolve("k") // the refresher's resolve begins…
		// …and, while it runs, a concurrent writer's resolve begins and stores a
		// newer valid body (e.g. the edit was reverted).
		fresh := u1255Put(t, c, u1255Resolve("k"), "k", `{"v":"fresh"}`)
		if c.EvictOnPermanentDecline(declining, ext, "k") {
			t.Fatal("#1255 RACE RED: the declining resolve dropped a body resolved after it began")
		}
		if got, ok := c.GetNoTouch("k"); !ok || got != fresh {
			t.Fatal("#1255 RACE RED: the fresh body is gone")
		}
	})
	t.Run("fresh_put_after_a_later_change_spared", func(t *testing.T) {
		c := u1255Store(t)
		u1255Put(t, c, u1255Resolve("k"), "k", `{"v":"old"}`)
		declining := u1255Resolve("k")
		u1255Change() // a change lands after the declining resolve began
		fresh := u1255Put(t, c, u1255Resolve("k"), "k", `{"v":"fresh"}`)
		if c.EvictOnPermanentDecline(declining, ext, "k") {
			t.Fatal("#1255 RACE RED: the declining resolve dropped a body resolved from a NEWER state than its own")
		}
		if got, _ := c.GetNoTouch("k"); got != fresh {
			t.Fatal("#1255 RACE RED: the fresh body is gone")
		}
	})
	t.Run("stale_concurrent_put_evicted", func(t *testing.T) {
		c := u1255Store(t)
		staleStart := u1255Resolve("k") // a writer began BEFORE the change…
		u1255Change()
		declining := u1255Resolve("k")
		// …and stored its pre-change body while the declining resolve ran: it is
		// no newer, so it must not survive (it would serve the pre-edit body).
		u1255Put(t, c, staleStart, "k", `{"v":"pre-edit"}`)
		if !c.EvictOnPermanentDecline(declining, ext, "k") {
			t.Fatal("#1255 RED: a pre-change body stored during the declining resolve survived it")
		}
	})
}

func TestIssue1255_U2s_ConcurrentDeclinesNeverDropANewerBody(t *testing.T) {
	c := u1255Store(t)
	ext := PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}
	const workers, rounds = 4, 300

	// Phase 1 — every declining resolve began BEFORE any body below was
	// resolved: whatever a writer stores is newer than all of them, so no
	// eviction may ever succeed, however the goroutines interleave.
	declining := make([]context.Context, workers)
	for i := range declining {
		declining[i] = u1255Resolve("k")
	}
	u1255Change()
	var evicted atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func(ctx context.Context) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if c.EvictOnPermanentDecline(ctx, ext, "k") {
					evicted.Add(1)
				}
			}
		}(declining[w])
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				e := &ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: &ResolvedKeyInputs{CacheEntryClass: "widgets"}}
				_ = c.putIfGen(u1255Resolve("k"), "k", e, c.CaptureGen("k"))
				u1255Change()
			}
		}()
	}
	wg.Wait()
	if n := evicted.Load(); n != 0 {
		t.Fatalf("#1255 RACE RED: %d eviction(s) by a resolve older than every body it removed", n)
	}
	if !c.Has("k") {
		t.Fatal("#1255 RACE RED: the newest body is gone")
	}

	// Phase 2 — the mirror: writers whose resolves all began before the
	// decliners' (a pre-change body landing late) interleave with decliners
	// that began after them; the store must end without a pre-change body
	// once a decliner runs last.
	stale := make([]context.Context, workers)
	for i := range stale {
		stale[i] = u1255Resolve("k")
	}
	u1255Change()
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func(ctx context.Context) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				e := &ResolvedEntry{RawJSON: []byte(`{"v":"pre-edit"}`), Inputs: &ResolvedKeyInputs{CacheEntryClass: "widgets"}}
				_ = c.putIfGen(ctx, "k", e, c.CaptureGen("k"))
			}
		}(stale[w])
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				c.EvictOnPermanentDecline(u1255Resolve("k"), ext, "k")
			}
		}()
	}
	wg.Wait()
	c.EvictOnPermanentDecline(u1255Resolve("k"), ext, "k")
	if c.Has("k") {
		t.Fatal("#1255 RED: a pre-change body stored during the declines survived the last one")
	}
}

func TestIssue1255_U3_AnExternalTTLCellKeepsItsOwnContract(t *testing.T) {
	c := u1255Store(t)
	extTTL := func(e *ResolvedEntry) { e.ExternalTTL = true; e.TTLOverride = 60 * time.Second }
	prior := u1255Put(t, c, u1255Resolve("k"), "k", `{"ext":1}`, extTTL)
	u1255Change()
	third := PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal, thirdPartyOnly: true}
	if c.EvictOnPermanentDecline(u1255Resolve("k"), third, "k") {
		t.Fatal("#1255 over-reach: an external-TTL cell was evicted by the very decline its opt-in stores")
	}
	if got, _ := c.GetNoTouch("k"); got != prior {
		t.Fatal("#1255 over-reach: the external-TTL cell is gone")
	}
	// A declared write is not something the opt-in stores: it evicts.
	write := PutVerdict{decision: PutDeclinePermanent, reason: PutReasonDeclaredWrite}
	if !c.EvictOnPermanentDecline(u1255Resolve("k"), write, "k") {
		t.Fatal("#1255 RED: a declared write kept an external-TTL cell")
	}
}

// U3b (review of #1263, F1) — a TRANSIENT failure of the opt-in cell's own
// third-party endpoint: the real sinks record the third-party touch and the
// endpoint's 5xx as a stage error, which PutGateVerdict classes as a permanent
// stage_error. The opt-in stores the clean retry, so the external-TTL cell is
// KEPT. The same verdict on a cell that did NOT opt in still evicts it (every
// resolve of that spec touches the endpoint, and nothing stores it), and so does
// a failure beside a touch that carried the requester (the opt-in refuses those,
// #1221).
func TestIssue1255_U3b_ATransientFailureOfTheOptInEndpointKeepsTheCell(t *testing.T) {
	failing := func(thirdParty bool) (context.Context, PutVerdict) {
		ctx := u1255Resolve("k")
		ctx, stage := WithStageErrorSink(ctx)
		ctx, _ = WithPerIdentityForbiddenSink(ctx)
		ctx, ext := WithExternalTouchedSink(ctx)
		if thirdParty {
			ext.BumpThirdParty()
		} else {
			ext.Bump()
		}
		stage.Bump("feed", "503 Service Unavailable")
		return ctx, PutGateVerdict(PutGateSinksFromContext(ctx), "widgets")
	}
	extTTL := func(e *ResolvedEntry) { e.ExternalTTL = true; e.TTLOverride = 60 * time.Second }

	t.Run("opt_in_kept", func(t *testing.T) {
		c := u1255Store(t)
		prior := u1255Put(t, c, u1255Resolve("k"), "k", `{"ext":1}`, extTTL)
		u1255Change()
		ctx, v := failing(true)
		if !v.Permanent() || v.Reason() != PutReasonStageError {
			t.Fatalf("PRECONDITION: a failed third-party call is a permanent stage_error, got permanent=%v reason=%s", v.Permanent(), v.Reason())
		}
		if c.EvictOnPermanentDecline(ctx, v, "k") {
			t.Fatal("#1255 F1 RED: a transient failure of the opt-in cell's own endpoint evicted it")
		}
		if got, _ := c.GetNoTouch("k"); got != prior {
			t.Fatal("#1255 F1 RED: the external-TTL cell is gone")
		}
	})
	t.Run("not_opted_in_evicted", func(t *testing.T) {
		c := u1255Store(t)
		u1255Put(t, c, u1255Resolve("k"), "k", `{"pre":"edit"}`)
		u1255Change()
		ctx, v := failing(true)
		if !c.EvictOnPermanentDecline(ctx, v, "k") {
			t.Fatal("#1255 RED: a stage error beside an external touch kept a cell that did not opt in")
		}
	})
	t.Run("requester_touch_evicted", func(t *testing.T) {
		c := u1255Store(t)
		u1255Put(t, c, u1255Resolve("k"), "k", `{"ext":1}`, extTTL)
		u1255Change()
		ctx, v := failing(false)
		if !c.EvictOnPermanentDecline(ctx, v, "k") {
			t.Fatal("#1255 RED: a failure beside a touch that carried the requester kept an external-TTL cell the opt-in would refuse")
		}
	})
}

// U7 — every eviction is counted under its reason (the OTLP
// snowplow_l1_evict_unstorable_total{reason} series), the closed set in full.
func TestIssue1255_U7_EvictionsAreCountedByReason(t *testing.T) {
	before := map[string]uint64{}
	for _, r := range EvictUnstorableReasons {
		before[r] = EvictUnstorableReasonCountForTest(r)
	}
	evict := func(f func(c *ResolvedCacheStore, ctx context.Context) bool) {
		t.Helper()
		c := u1255Store(t)
		u1255Put(t, c, u1255Resolve("k"), "k", `{"v":1}`)
		u1255Change()
		if !f(c, u1255Resolve("k")) {
			t.Fatal("PRECONDITION: the cell must be evicted")
		}
	}
	for _, r := range []PutReason{PutReasonDeclaredWrite, PutReasonExternal, PutReasonStageError} {
		evict(func(c *ResolvedCacheStore, ctx context.Context) bool {
			return c.EvictOnPermanentDecline(ctx, PutVerdict{decision: PutDeclinePermanent, reason: r}, "k")
		})
	}
	for _, r := range []string{UnstorableSensitive, UnstorableUAF, "something-new"} {
		evict(func(c *ResolvedCacheStore, ctx context.Context) bool { return c.EvictUnstorable(ctx, "k", r) })
	}
	for _, r := range EvictUnstorableReasons {
		if d := EvictUnstorableReasonCountForTest(r) - before[r]; d != 1 {
			t.Errorf("#1255: reason %q counted %d eviction(s), want 1", r, d)
		}
	}
	if got := EvictUnstorableByReason(); len(got) != len(EvictUnstorableReasons) {
		t.Fatalf("#1255: the closed reason set must be reported in full, got %d of %d", len(got), len(EvictUnstorableReasons))
	}
}

func TestIssue1255_U4_AnInertOrUnplacedResolveEvictsNothing(t *testing.T) {
	c := u1255Store(t)
	u1255Put(t, c, u1255Resolve("k"), "k", `{"v":1}`)
	u1255Change()
	ext := PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}
	if c.EvictOnPermanentDecline(WithInert(u1255Resolve("k")), ext, "k") {
		t.Fatal("#1255: an inert (dry-run) resolve evicted a cell")
	}
	if c.EvictOnPermanentDecline(context.Background(), ext, "k") {
		t.Fatal("#1255: a resolve with no dep-gen sink (no start to compare) evicted a cell")
	}
	if !c.Has("k") {
		t.Fatal("#1255: the cell is gone")
	}
}

func TestIssue1255_U5_TheEvictionReplacesAndPushesAnOpenStaleServe(t *testing.T) {
	c := u1255Store(t)
	var published []string
	orig := publishEvictionFn
	publishEvictionFn = func(k string) { published = append(published, k) }
	t.Cleanup(func() { publishEvictionFn = orig })
	ResetStalePushForTest()
	t.Cleanup(ResetStalePushForTest)

	u1255Put(t, c, u1255Resolve("k"), "k", `{"pre":"edit"}`)
	// An armed tab was served the cell stale.
	s := noteStaleServe("k", "subj", layerL1, StalePushNow(), nil)
	if s == nil {
		t.Fatal("PRECONDITION: the stale serve must open")
	}
	u1255Change()
	if !c.EvictOnPermanentDecline(u1255Resolve("k"), PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}, "k") {
		t.Fatal("PRECONDITION: the cell must be evicted")
	}
	if len(published) != 1 || published[0] != "k" {
		t.Fatalf("#1255 RED: the eviction must publish one refresh frame for the key, got %v", published)
	}
	pushed0 := stalePushStats.closes[layerL1][closePushedIdx].Load()
	stalePushFrameWritten("k", "subj") // the /refreshes handler wrote that frame
	if stalePushStats.closes[layerL1][closePushedIdx].Load() != pushed0+1 {
		t.Fatal("#1255 RED: the stale serve was not closed pushed by the eviction's frame — it would surface as cache.stale_push_unreplaced")
	}
}

func TestIssue1255_U6_AWidgetSlicedFromAnUnstorableRAFullListIsRemarked(t *testing.T) {
	c := u1255Store(t)
	var mu sync.Mutex
	remarked := map[string]int{}
	t.Cleanup(SetDepGenRemarkObserverForTest(func(k, reason string) {
		if reason == "layered" {
			mu.Lock()
			remarked[k]++
			mu.Unlock()
		}
	}))
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	raIn := RAFullListKeyInputsForTest("templates.krateo.io", "v1", "restactions", "ns", "ra-1255", "C:uid", nil)
	raKey := ComputeKey(raIn)
	if !c.putIfGen(u1255Resolve(raKey), raKey, &ResolvedEntry{RawJSON: []byte(`{"items":["pre-edit"]}`), Inputs: &raIn}, c.CaptureGen(raKey)) {
		t.Fatal("PRECONDITION: raKey Put refused")
	}
	// The widget's refresh ran first and Go-sliced the stale raKey: its accepted
	// re-Put stores the pre-edit body with a FRESH CreatedAt (a new TTL).
	const w = "widget-sliced"
	wctx := u1255Resolve(w)
	ra, _ := c.GetNoTouch(raKey)
	c.NoteRAFullListSlice(wctx, raKey, ra)
	u1255Put(t, c, wctx, w, `{"title":"pre-edit"}`)

	// Then the raKey refresh declines (the edit made the RESTAction external).
	u1255Change()
	if !c.EvictOnPermanentDecline(u1255Resolve(raKey), PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}, raKey) {
		t.Fatal("PRECONDITION: the raKey cell must be evicted")
	}
	mu.Lock()
	n := remarked[w]
	mu.Unlock()
	if n != 1 {
		t.Fatalf("#1255 RED: the widget sliced from the evicted raKey was remarked %d time(s), want 1 — "+
			"it keeps the pre-edit body for a fresh TTL", n)
	}
}
