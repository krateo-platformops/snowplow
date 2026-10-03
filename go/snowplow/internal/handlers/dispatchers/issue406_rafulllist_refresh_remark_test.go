// issue406_rafulllist_refresh_remark_test.go — #406 carrier arm for the REFRESHER's
// raKey re-Put (resolveAndPopulateL1 → ReplaceIfGen), over the real store with the
// resolve seam stubbed: a widget that Go-sliced the raKey is remarked when the refresher
// re-Puts raKey with CHANGED bytes, and not when the bytes are identical.

package dispatchers

import (
	"context"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue406_RefresherRAFullListRePut_RemarksSlicedWidgetOnlyOnChange(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	c := cache.ResolvedCache()
	cache.Deps().SetStore(c)

	var mu sync.Mutex
	layered := map[string]int{}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
		if reason == "layered" {
			mu.Lock()
			layered[k]++
			mu.Unlock()
		}
	}))
	cache.Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	inputs := cache.RAFullListKeyInputs("templates.krateo.io", "v1", "restactions",
		"krateo-system", "ra-406-refresher", "C:uid-406", nil)
	inputs.RepresentativeUsername = "admin"
	raKey := cache.ComputeKey(inputs)
	in := inputs
	c.Put(raKey, &cache.ResolvedEntry{RawJSON: []byte(`{"items":["old"]}`), Inputs: &in})

	// The widget cell, served through the 4a fast path over that raKey body.
	const wKey = "L1_w406_refresher"
	wctx := cache.WithL1KeyContext(context.Background(), wKey)
	prior, ok := c.GetNoTouch(raKey)
	if !ok {
		t.Fatalf("setup: raKey not resident")
	}
	c.NoteRAFullListSlice(wctx, raKey, prior)
	wIn := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "w406"}
	if !c.PutIfGen(wctx, wKey, &cache.ResolvedEntry{RawJSON: []byte(`{"old":1}`), Inputs: &wIn}, c.CaptureGen(wKey)) {
		t.Fatalf("setup: widget PutIfGen refused")
	}

	body := `{"items":["old"]}`
	t.Cleanup(setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(body), nil
	}))
	refresh := func() {
		t.Helper()
		if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
			t.Fatalf("resolveAndPopulateL1: %v", err)
		}
		if e, ok := c.GetNoTouch(raKey); !ok || string(e.RawJSON) != body {
			t.Fatalf("setup: the refresher did not re-Put raKey with %s", body)
		}
	}

	refresh() // identical bytes
	mu.Lock()
	got := layered[wKey]
	mu.Unlock()
	if got != 0 {
		t.Fatalf("identical refresher re-Put of raKey remarked the widget %d times; want 0", got)
	}

	body = `{"items":["old","new"]}`
	refresh() // changed bytes
	mu.Lock()
	got = layered[wKey]
	mu.Unlock()
	if got != 1 {
		t.Fatalf("#406 RED: the refresher re-Put raKey with CHANGED bytes but the widget that Go-sliced the old "+
			"body was remarked %d times; want exactly 1", got)
	}
}
