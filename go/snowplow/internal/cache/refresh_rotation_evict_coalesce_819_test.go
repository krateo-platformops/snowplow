package cache

// refresh_rotation_evict_coalesce_819_test.go — #828 re-gate nit 1 (the
// reviewer's probe, adopted). A rotation whose keys already wait in the PACED
// eviction backlog must not inherit the eviction pace: left there they drain
// at 5/s, so most would still be queued when the SPA reconnects ~200 ms after
// the first frame and die with the old connection. The rotation moves each
// such key onto the unpaced path — and the tab still gets ONE frame per key.

import (
	"fmt"
	"testing"
	"time"
)

func TestIssue819_RotationOfKeysWithPacedEvictionsIsNotPaced(t *testing.T) {
	withRefreshLayer(t)
	const n = 40
	keys := make([]string, n)
	armed := map[string]struct{}{}
	for i := range keys {
		keys[i] = fmt.Sprintf("L1_rot_evict_%03d", i)
		armed[keys[i]] = struct{}{}
	}
	st := SubscribeRefreshStream(armed)
	defer st.Unsub()
	// Deferred evictions for every key: the first burst goes out, the rest
	// wait in the paced set.
	for _, k := range keys {
		PublishEviction(k)
	}
	got := map[string]int{}
	drain := func(d time.Duration) {
		until := time.After(d)
		for {
			select {
			case k := <-st.Keys:
				got[k]++
			case <-until:
				return
			}
		}
	}
	drain(50 * time.Millisecond)
	beforeRotation := len(got)
	sentBefore := map[string]bool{}
	for k := range got {
		sentBefore[k] = true
	}
	if beforeRotation >= n {
		t.Fatalf("precondition: the eviction burst delivered all %d keys — nothing left in the paced backlog", n)
	}

	st.Rotated(keys)
	drain(200 * time.Millisecond) // the SPA's reconnect debounce

	if len(got) < n {
		t.Fatalf("%d/%d rotated keys still queued behind the paced eviction backlog 200 ms after the rotation — "+
			"they would die with the connection at the SPA's reconnect", n-len(got), n)
	}
	drain(500 * time.Millisecond) // nothing more may arrive: the backlog entries were moved, not copied
	for k, c := range got {
		// A key whose eviction frame already went out gets the rotation's own
		// frame (a later event); a key still in the backlog gets ONE frame:
		// the eviction and the rotation are the same frame.
		want := 1
		if sentBefore[k] {
			want = 2
		}
		if c != want {
			t.Fatalf("key %q delivered %d times, want %d (pending eviction + rotation = one frame)", k, c, want)
		}
	}
	t.Logf("%d keys by the eviction burst; %d/%d within 200 ms of the rotation; backlogged keys once", beforeRotation, len(got), n)
}
