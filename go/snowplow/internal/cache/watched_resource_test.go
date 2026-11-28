package cache

// watched_resource_test.go: WatchedResource, the rum ingest's closed-set check
// for the widget attributes of a browser span, over a REAL synced informer
// (depwatch_harness_test.go). It answers from the informers alone: a plural is
// known only when a servable informer of the group serves it, and a widget
// exists only when that informer's indexer holds it.

import "testing"

func TestWatchedResource_KnownPluralAndExistingObject(t *testing.T) {
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo", "w1", "x")

	g := gvr.Group
	for _, tc := range []struct {
		name                  string
		group, res, ns, nm    string
		wantKnown, wantExists bool
	}{
		{"plural and object", g, "flexes", "demo", "w1", true, true},
		{"plural only", g, "flexes", "", "", true, false},
		{"absent object", g, "flexes", "demo", "nope", true, false},
		{"wrong namespace", g, "flexes", "other", "w1", true, false},
		{"unknown plural", g, "tablez", "demo", "w1", false, false},
		{"other group, same plural", "example.com", "flexes", "demo", "w1", false, false},
	} {
		known, exists := rw.probeResource(tc.group, tc.res, tc.ns, tc.nm)
		if known != tc.wantKnown || exists != tc.wantExists {
			t.Errorf("%s: got known=%v exists=%v, want known=%v exists=%v", tc.name, known, exists, tc.wantKnown, tc.wantExists)
		}
	}
}

func TestWatchedResource_NilWatcherKnowsNothing(t *testing.T) {
	var rw *ResourceWatcher
	if known, exists := rw.probeResource("widgets.templates.krateo.io", "flexes", "demo", "w1"); known || exists {
		t.Errorf("nil watcher: known=%v exists=%v, want false false", known, exists)
	}
}
