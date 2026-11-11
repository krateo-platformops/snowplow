// refreshes_catchup_1324_test.go — #1324 wire-level arms: a /refreshes arm
// that carries `held` (the X-Snowplow-Refresh-Digest of the tab's /call
// answer) gets exactly one `event: refresh` frame for every key the tab is
// behind on, at once, and none for any other key.
//
// The REAL chain: middleware.RefreshAuth → handlers.Refreshes →
// validateSubscription (the held digests collected per derived key) →
// dispatchers.BehindKeys → RefreshStream.CatchUp → the bytes on the wire.
// The cells are identity-bound `widgets` cells, served verbatim by /call, so
// the digest the tab holds is cache.ContentDigest of the bytes it was served
// (refresh_digest.go; the per-identity widgetContent case is the dispatchers
// arm TestIssue1324_TwoIdentitiesOneKeyOnlyTheBehindOneCatchesUp).
//
//	C1  a change between /call and arm → exactly one catch-up frame.
//	C2  no change → no frame (scoped: a later publish reaches the stream).
//	C3  many keys → only the behind ones get a frame.
//	C4  the SPA's re-open (it aborts and re-opens its one stream on every
//	    widget mount/unmount): a change published while no stream was armed
//	    reaches the next stream as one frame.
//	C5  an absent `held` and `held: ""` → no catch-up (today's behaviour);
//	    a gone cell with a held digest → one frame.

package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// sub1324 builds a ?sub= for identity-bound widgets coords, one per name,
// each carrying held[name] when present (the key is omitted when absent).
func sub1324(names []string, held map[string]string) string {
	body := make([]map[string]any, 0, len(names))
	for _, n := range names {
		c := map[string]any{
			"class": "widgets", "group": panelGVR.Group, "version": panelGVR.Version,
			"resource": panelGVR.Resource, "namespace": evictTestNS, "name": n, "perPage": 5, "page": 1,
		}
		if h, ok := held[n]; ok {
			c["held"] = h
		}
		body = append(body, c)
	}
	raw, _ := json.Marshal(body)
	return base64.StdEncoding.EncodeToString(raw)
}

// put1324 stores body as the widgets cell under key (what /call serves verbatim).
func put1324(t *testing.T, key, name, body string) {
	t.Helper()
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("PRE: resolved cache disabled")
	}
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(body), Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets", Group: panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name,
	}})
}

// drain1324 collects the frames that arrive within d.
func drain1324(frames <-chan wireFrame, d time.Duration) []string {
	var got []string
	deadline := time.After(d)
	for {
		select {
		case f := <-frames:
			got = append(got, f.key)
		case <-deadline:
			sort.Strings(got)
			return got
		}
	}
}

func awaitUnarmed1324(t *testing.T, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for cache.HasRefreshSubscriber(key) {
		if time.Now().After(deadline) {
			t.Fatal("PRE: the closed stream never unsubscribed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRefreshes1324_C1_AChangeBetweenCallAndArmGetsExactlyOneCatchUpFrame(t *testing.T) {
	const name = "cu-c1"
	setup819(t, name)
	k := widgetsKey819(t, name)
	put1324(t, k, name, `{"v":1}`)
	held := cache.ContentDigest([]byte(`{"v":1}`)) // the /call answer's header
	put1324(t, k, name, `{"v":2}`)                 // the change lands before the arm
	before := cache.RefreshBroadcasterStatsSnapshot()

	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, sub1324([]string{name}, map[string]string{name: held}), []string{k})
	defer cancel()
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 1 || got[0] != k {
		t.Fatalf("C1 RED: frames %v on arm, want exactly one on %q — the change published before the arm reached nobody "+
			"and /refreshes keeps no replay (#1324)", got, k)
	}
	after := cache.RefreshBroadcasterStatsSnapshot()
	if after.ArmCatchUp-before.ArmCatchUp != 1 || after.ArmHeld-before.ArmHeld != 1 {
		t.Fatalf("C1: arm_catch_up Δ=%d arm_held Δ=%d, want 1/1", after.ArmCatchUp-before.ArmCatchUp, after.ArmHeld-before.ArmHeld)
	}
}

func TestRefreshes1324_C2_NoChangeNoFrame(t *testing.T) {
	const name = "cu-c2"
	setup819(t, name)
	k := widgetsKey819(t, name)
	put1324(t, k, name, `{"v":1}`)
	held := cache.ContentDigest([]byte(`{"v":1}`))

	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, sub1324([]string{name}, map[string]string{name: held}), []string{k})
	defer cancel()
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 0 {
		t.Fatalf("C2 RED: frames %v on an arm whose tab holds the current bytes, want none", got)
	}
	// Scope: the stream is live.
	cache.PublishRefresh(k)
	if f, ok := awaitFrame(t, frames, 3*time.Second); !ok || f.key != k {
		t.Fatalf("C2 PRE: the armed stream did not receive a publish (got %q ok=%v)", f.key, ok)
	}
}

func TestRefreshes1324_C3_ManyKeysOnlyTheBehindOnesGetAFrame(t *testing.T) {
	names := make([]string, 12)
	for i := range names {
		names[i] = fmt.Sprintf("cu-c3-%02d", i)
	}
	setup819(t, names...)
	keys := make([]string, len(names))
	held := map[string]string{}
	var want []string
	for i, n := range names {
		keys[i] = widgetsKey819(t, n)
		body := fmt.Sprintf(`{"panel":%q,"v":1}`, n)
		put1324(t, keys[i], n, body)
		held[n] = cache.ContentDigest([]byte(body))
		if i%4 == 1 { // three of twelve change before the arm
			put1324(t, keys[i], n, fmt.Sprintf(`{"panel":%q,"v":2}`, n))
			want = append(want, keys[i])
		}
	}
	sort.Strings(want)

	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, sub1324(names, held), keys)
	defer cancel()
	got := drain1324(frames, 700*time.Millisecond)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("C3 RED: catch-up frames %v, want exactly the %d behind keys %v", got, len(want), want)
	}
}

func TestRefreshes1324_C4_TheSPAReopenWindowIsCaughtUp(t *testing.T) {
	const name = "cu-c4"
	setup819(t, name)
	k := widgetsKey819(t, name)
	put1324(t, k, name, `{"v":1}`)
	held := cache.ContentDigest([]byte(`{"v":1}`))
	base := refreshServer(t)
	sub := sub1324([]string{name}, map[string]string{name: held})

	// The tab is armed and current.
	frames1, cancel1 := openArmedStreamSub(t, base, sub, []string{k})
	if got := drain1324(frames1, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("C4 PRE: frames %v on a current arm", got)
	}
	// Another widget mounts: the SPA aborts this stream to re-open it with the
	// new armed set. A change is published inside that window: nobody hears it.
	cancel1()
	awaitUnarmed1324(t, k)
	put1324(t, k, name, `{"v":2}`)
	cache.PublishRefresh(k)

	// The re-opened stream carries the same `held` (the tab still renders v1).
	frames2, cancel2 := openArmedStreamSub(t, base, sub, []string{k})
	defer cancel2()
	if got := drain1324(frames2, 500*time.Millisecond); len(got) != 1 || got[0] != k {
		t.Fatalf("C4 RED: frames %v on the re-opened stream, want exactly one on %q — the change published while the SPA "+
			"re-opened its stream was lost", got, k)
	}
}

func TestRefreshes1324_C5_AbsentOrEmptyHeldIsTodaysBehaviour_AGoneCellCatchesUp(t *testing.T) {
	names := []string{"cu-c5-absent", "cu-c5-empty", "cu-c5-gone"}
	setup819(t, names...)
	keys := make([]string, len(names))
	for i, n := range names {
		keys[i] = widgetsKey819(t, n)
		put1324(t, keys[i], n, `{"v":1}`)
	}
	old := cache.ContentDigest([]byte(`{"v":1}`))
	// The first two change before the arm, but their arm says nothing (absent)
	// or nothing useful (""): no catch-up, as before #1324.
	put1324(t, keys[0], names[0], `{"v":2}`)
	put1324(t, keys[1], names[1], `{"v":2}`)
	// The third's cell leaves the store: the refetch is a fill, so it is behind.
	cache.ResolvedCache().DeleteForTest(keys[2])

	base := refreshServer(t)
	before := cache.RefreshBroadcasterStatsSnapshot()
	frames, cancel := openArmedStreamSub(t, base, sub1324(names, map[string]string{names[1]: "", names[2]: old}), keys)
	defer cancel()
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 1 || got[0] != keys[2] {
		t.Fatalf("C5 RED: frames %v, want exactly one, on the gone cell %q (absent and empty held mean no catch-up)", got, keys[2])
	}
	if d := cache.RefreshBroadcasterStatsSnapshot().ArmHeld - before.ArmHeld; d != 1 {
		t.Fatalf("C5: arm_held Δ=%d, want 1 (an empty held is not a claim)", d)
	}
}
