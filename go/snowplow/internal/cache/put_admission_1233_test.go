package cache

// #1233 — the store admits no write while the RBAC snapshot is incomplete, for
// every write method and every writer, and admits every one of them once the
// snapshot is complete.

import (
	"context"
	"testing"
	"time"
)

type storeWrite1233 struct {
	name    string
	class   string
	replace bool // the write replaces a live cell: one is put first, under a complete snapshot
	noCtx   bool // the write takes no ctx, so it names no writer: it counts as serve
	do      func(ctx context.Context, c *ResolvedCacheStore, key string) bool
}

func entry1233(class string) *ResolvedEntry {
	return &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &ResolvedKeyInputs{CacheEntryClass: class}}
}

// contentKey1233 is a content cell's own key, the only key PutContentIfGen writes.
func contentKey1233(name string) (string, *ResolvedEntry) {
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Version: "v1", Resource: "configmaps", Namespace: "ns", Name: name}
	return ComputeKey(in), &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in}
}

var storeWrites1233 = []storeWrite1233{
	{name: "put", class: "widgets", noCtx: true, do: func(_ context.Context, c *ResolvedCacheStore, k string) bool {
		return c.put(k, entry1233("widgets"))
	}},
	{name: "PutResolved", class: "restactions", noCtx: true, do: func(_ context.Context, c *ResolvedCacheStore, k string) bool {
		return c.PutResolved(fixtureStoreVerdict, k, entry1233("restactions"))
	}},
	{name: "PutResolvedIfGen", class: "restactions", do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.PutResolvedIfGen(ctx, fixtureStoreVerdict, k, entry1233("restactions"), c.CaptureGen(k))
	}},
	{name: "PutResolvedThenRemark", class: "widgets", do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.PutResolvedThenRemark(ctx, fixtureStoreVerdict, k, entry1233("widgets"))
	}},
	{name: "PutResolvedEndingKnownStale", class: "widgets", do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.PutResolvedEndingKnownStale(ctx, fixtureStoreVerdict, k, entry1233("widgets"))
	}},
	{name: "ReplaceResolvedIfGen", class: "widgets", replace: true, do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.ReplaceResolvedIfGen(ctx, fixtureStoreVerdict, k, entry1233("widgets"), c.CaptureGen(k))
	}},
	{name: "ReplaceResolvedIfGenRefresh", class: "widgets", replace: true, do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.ReplaceResolvedIfGenRefresh(ctx, fixtureStoreVerdict, k, entry1233("widgets"), c.CaptureGen(k))
	}},
	{name: "PutRAFullList", class: CacheEntryClassRAFullList, noCtx: true, do: func(_ context.Context, c *ResolvedCacheStore, k string) bool {
		c.PutRAFullList(fixtureStoreVerdict, k, ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList}, map[string]any{"items": []any{}})
		_, ok := c.GetNoTouch(k)
		return ok
	}},
	{name: "PutRAFullListIfGen", class: CacheEntryClassRAFullList, do: func(ctx context.Context, c *ResolvedCacheStore, k string) bool {
		return c.PutRAFullListIfGen(ctx, fixtureStoreVerdict, k, ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList},
			map[string]any{"items": []any{}}, c.CaptureGen(k))
	}},
	{name: "PutContentIfGen", class: CacheEntryClassApistage, do: func(ctx context.Context, c *ResolvedCacheStore, _ string) bool {
		k, e := contentKey1233("cm")
		return c.PutContentIfGen(ctx, k, e, c.CaptureGen(k))
	}},
}

// writers1233 are the drivers that write the store, as RefDenialOrigin names
// them: a user's /call (serve), the content prewarm (prewarm-path), and each
// background driver.
var writers1233 = []string{RefDenialOriginServe, RefDenialOriginPrewarmPath, BackgroundOriginRefresher,
	BackgroundOriginCohortSeed, BackgroundOriginPrewarmEngineBoot}

func writerCtx1233(writer string) context.Context {
	switch writer {
	case RefDenialOriginServe:
		return context.Background()
	case RefDenialOriginPrewarmPath:
		return WithPrewarmPath(context.Background())
	}
	return WithBackgroundResolveOrigin(context.Background(), writer)
}

func setSnapshot1233(t *testing.T, s *RBACSnapshot) {
	t.Helper()
	prev := rbacSnap.Load()
	rbacSnap.Store(s)
	t.Cleanup(func() { rbacSnap.Store(prev) })
}

func TestIssue1233_TheStoreAdmitsNoWriteOnAnIncompleteSnapshot(t *testing.T) {
	states := []struct {
		name  string
		snap  *RBACSnapshot
		admit bool
	}{
		{"complete", &RBACSnapshot{Synced: true}, true},
		{"partial", &RBACSnapshot{}, false},
		{"none published", nil, false},
	}
	for _, w := range storeWrites1233 {
		for _, writer := range writers1233 {
			for _, st := range states {
				t.Run(w.name+"/"+writer+"/"+st.name, func(t *testing.T) {
					c := newResolvedCache(10, 1<<20, time.Hour)
					key := "k-" + w.name
					if w.name == "PutContentIfGen" {
						key, _ = contentKey1233("cm")
					}
					if w.replace {
						setSnapshot1233(t, &RBACSnapshot{Synced: true})
						if !c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":0}`), Inputs: &ResolvedKeyInputs{CacheEntryClass: w.class}}) {
							t.Fatal("setup: the live cell must be written under a complete snapshot")
						}
					}
					setSnapshot1233(t, st.snap)
					label := writer + "/" + w.class
					if w.noCtx {
						label = RefDenialOriginServe + "/" + w.class
					}
					before := PutDeclinedRBACIncompleteSnapshot()[label]

					wrote := w.do(writerCtx1233(writer), c, key)

					got, resident := c.GetNoTouch(key)
					declined := PutDeclinedRBACIncompleteSnapshot()[label] - before
					if st.admit {
						if !wrote || !resident || string(got.RawJSON) != `{"v":1}` && w.class != CacheEntryClassRAFullList {
							t.Fatalf("a %s write by %s under a complete snapshot must store: wrote=%v resident=%v", w.name, writer, wrote, resident)
						}
						if declined != 0 {
							t.Fatalf("a stored write counted %d declines", declined)
						}
						return
					}
					if wrote {
						t.Fatalf("#1233 RED: %s by %s stored a shared cell on a %s RBAC snapshot", w.name, writer, st.name)
					}
					if w.replace {
						if !resident || string(got.RawJSON) != `{"v":0}` {
							t.Fatalf("#1233 RED: %s by %s replaced the live cell on a %s snapshot", w.name, writer, st.name)
						}
					} else if resident {
						t.Fatalf("#1233 RED: %s by %s left a cell resident on a %s snapshot", w.name, writer, st.name)
					}
					if declined != 1 {
						t.Fatalf("the refusal must be counted once under %q, counted %d", label, declined)
					}
				})
			}
		}
	}
}

// An inert (dry-run) resolve is refused by the same admission, and is not
// counted as an RBAC decline, on a complete snapshot or a partial one (#1243
// review: the order of the two refusals must not move a dry run into the count).
func TestIssue1233_AnInertWriteIsRefusedButNotCountedAsAnRBACDecline(t *testing.T) {
	for _, st := range []struct {
		name string
		snap *RBACSnapshot
	}{{"complete", &RBACSnapshot{Synced: true}}, {"partial", &RBACSnapshot{}}, {"none published", nil}} {
		t.Run(st.name, func(t *testing.T) {
			setSnapshot1233(t, st.snap)
			c := newResolvedCache(10, 1<<20, time.Hour)
			before := PutDeclinedRBACIncompleteTotal()
			if c.PutResolvedIfGen(WithInert(context.Background()), fixtureStoreVerdict, "k", entry1233("restactions"), c.CaptureGen("k")) {
				t.Fatal("an inert resolve stored a cell")
			}
			if got := PutDeclinedRBACIncompleteTotal() - before; got != 0 {
				t.Fatalf("an inert refusal counted %d RBAC declines on a %s snapshot", got, st.name)
			}
		})
	}
}

// A resolve is admitted by the snapshot it BEGAN on (#1243 review, blocker 1):
// its sink, installed at its start on a partial snapshot, refuses its write
// after the snapshot completed. A resolve begun on the complete snapshot writes.
func TestIssue1233_AWriteIsAdmittedByTheSnapshotItsResolveBeganOn(t *testing.T) {
	setSnapshot1233(t, &RBACSnapshot{})
	partialCtx, partialSink := WithExternalTouchedSink(context.Background())
	setSnapshot1233(t, &RBACSnapshot{Synced: true})
	completeCtx, completeSink := WithExternalTouchedSink(context.Background())
	if !partialSink.StartedIncomplete() || completeSink.StartedIncomplete() {
		t.Fatalf("the sinks must record the snapshot at their start: partial=%v complete=%v",
			partialSink.StartedIncomplete(), completeSink.StartedIncomplete())
	}
	c := newResolvedCache(10, 1<<20, time.Hour)
	before := PutDeclinedRBACIncompleteSnapshot()[RefDenialOriginServe+"/restactions"]
	if c.PutResolvedIfGen(partialCtx, fixtureStoreVerdict, "k1", entry1233("restactions"), c.CaptureGen("k1")) {
		t.Fatal("#1233 RED: a resolve begun on a partial snapshot stored its body after the snapshot completed")
	}
	if got := PutDeclinedRBACIncompleteSnapshot()[RefDenialOriginServe+"/restactions"] - before; got != 1 {
		t.Fatalf("the refusal must be counted once, counted %d", got)
	}
	if !c.PutResolvedIfGen(completeCtx, fixtureStoreVerdict, "k2", entry1233("restactions"), c.CaptureGen("k2")) {
		t.Fatal("CONTROL: a resolve begun on the complete snapshot must store")
	}
	// A child created on the complete snapshot under a parent begun on the
	// partial one belongs to that resolve tree: it is refused too.
	childUnderPartial, _ := WithChildPutGateSinks(partialCtx)
	if c.PutResolvedIfGen(childUnderPartial, fixtureStoreVerdict, "k3", entry1233("restactions"), c.CaptureGen("k3")) {
		t.Fatal("#1233 RED: a nested resolve of a tree begun on a partial snapshot stored")
	}
}

// A memo hit is indistinguishable from the resolve it replaces (#912 N1): a body
// a producer resolved on a partial snapshot, replayed by a hitter that began on
// the complete one, is refused like the producer's would be.
func TestIssue1233_AMemoHitReplaysThatItsProducerBeganOnAPartialSnapshot(t *testing.T) {
	setSnapshot1233(t, &RBACSnapshot{})
	_, producer := WithPutGateEffectsCapture(context.Background())
	effects := producer.Effects()
	if !effects.StartedIncomplete {
		t.Fatal("the producer's capture must record that it began on a partial snapshot")
	}
	setSnapshot1233(t, &RBACSnapshot{Synced: true})
	hitterCtx, hitterSink := WithExternalTouchedSink(context.Background())
	if hitterSink.StartedIncomplete() {
		t.Fatal("PRECONDITION: the hitter began on the complete snapshot")
	}
	effects.Replay(hitterCtx)
	c := newResolvedCache(10, 1<<20, time.Hour)
	if c.PutResolvedIfGen(hitterCtx, fixtureStoreVerdict, "k", entry1233("widgets"), c.CaptureGen("k")) {
		t.Fatal("#1233 RED: a memo body produced on a partial snapshot was stored by the hitter after completion")
	}
	// A producer that began on the complete snapshot replays nothing of the kind.
	_, clean := WithPutGateEffectsCapture(context.Background())
	ctx2, _ := WithExternalTouchedSink(context.Background())
	clean.Effects().Replay(ctx2)
	if !c.PutResolvedIfGen(ctx2, fixtureStoreVerdict, "k2", entry1233("widgets"), c.CaptureGen("k2")) {
		t.Fatal("CONTROL: a memo body produced on the complete snapshot must store")
	}
}

// The count is published where 057 and run 22 read it.
func TestIssue1233_TheDeclineCountIsPublished(t *testing.T) {
	setSnapshot1233(t, &RBACSnapshot{})
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.PutResolved(fixtureStoreVerdict, "k", entry1233("widgets"))
	snap := PutDeclinedRBACIncompleteSnapshot()
	if snap["total"] == 0 || snap[RefDenialOriginServe+"/widgets"] == 0 {
		t.Fatalf("the snapshot does not carry the refusal: %v", snap)
	}
	for _, k := range PutDeclinedRBACIncompleteKeys() {
		if k == "total" {
			t.Fatal("a cell is named total")
		}
	}
}
