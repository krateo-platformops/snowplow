package cache

// stale_push_late_open_1198_test.go — #1198: a serve that opens AFTER the Put
// that replaced its cell.
//
// A hit reads its cell, and only then learns it served stale bytes and opens a
// serve (NoteStaleHit). A replacing Put that lands between the two finds no
// serve to mark, and no later Put is owed: the frame that follows closes
// nothing, the sample is lost, and the overdue scan then reports a viewer who
// HAS the fresh frame as a miss of the 10 s goal.
//
// These arms hold no timing: the window is the order of the calls. The hit's
// read is taken first and handed to NoteStaleHit after the Put, which is exactly
// what a Put landing in that window does.
//
// The two arms that must NOT replace pin the other direction: a late open asks
// the Put's own question of the cell, and a Put that would not have replaced an
// open serve does not replace a late one either.

import (
	"context"
	"testing"
	"time"
)

// readThenLateOpen is one hit whose serve opens after the window: read the
// cell, let inWindow run (the Put landing), then the hit's write point.
func (e *k1062Env) readThenLateOpen(t *testing.T, inWindow func()) {
	t.Helper()
	read, ok := e.c.GetNoTouch(e.key)
	if !ok {
		t.Fatalf("PRECONDITION: the cell is not resident")
	}
	inWindow()
	NoteStaleHit(context.Background(), e.key, read, func(context.Context) string { return "" })
}

func TestStalePush1198_ALateOpenAfterAFreshPutIsReplaced(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	for _, tc := range []struct {
		name, layer string
		setup       func(e *k1062Env, t *testing.T)
	}{
		{"stale_derived_cell", StaleLayerAPIStage, func(e *k1062Env, t *testing.T) {
			e.put(t, WithL1KeyContext(context.Background(), e.key), true)
		}},
		{"dirty_cell", StaleLayerL1, func(e *k1062Env, _ *testing.T) { e.mark() }},
		{"stale_derived_cell_with_an_open_window", StaleLayerAPIStage, func(e *k1062Env, t *testing.T) {
			e.put(t, WithL1KeyContext(context.Background(), e.key), true)
			e.mark()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newK1062(t)
			st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
			defer st.Unsub()
			tc.setup(e, t)

			e.readThenLateOpen(t, func() {
				e.put(t, WithL1KeyContext(context.Background(), e.key), false) // began after the mark
			})
			if s := k1062Layer(t, tc.layer); s.Serves != 1 {
				t.Fatalf("PRECONDITION: the hit opened no %s serve (%d)", tc.layer, s.Serves)
			}
			e.advance(1500 * time.Millisecond)
			written(st, e.key) // the replacement's frame

			s := k1062Layer(t, tc.layer)
			if s.ToPush.Count != 1 || s.ToPush.MaxMS != 1500 || s.Open != 0 || s.Closes["pushed"] != 1 {
				t.Fatalf("RED (#1198): the frame after a Put that landed before the serve opened closed nothing (open=%d pushed=%d %+v), want one 1500 ms sample",
					s.Open, s.Closes["pushed"], s.ToPush)
			}
			e.advance(11 * time.Second)
			ScanStalePushOverdueForTest()
			if got := k1062Layer(t, tc.layer).Unreplaced; got != 0 {
				t.Fatalf("RED (#1198): a viewer who has the fresh frame is counted unreplaced %d time(s)", got)
			}
		})
	}
}

// The serve is gone before it opens: the key left the store in the window, so a
// refetch is a fresh fill (the same rule as an eviction of an open serve).
func TestStalePush1198_ALateOpenAfterAnEvictionIsReplaced(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.put(t, WithL1KeyContext(context.Background(), e.key), true)
	e.readThenLateOpen(t, func() {
		entry, ok := e.c.GetNoTouch(e.key)
		if !ok || !e.c.EvictStaleDerived(e.key, entry) {
			t.Fatalf("PRECONDITION: the stale-derived cell was not evicted")
		}
	})
	e.advance(time.Second)
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerAPIStage); s.Open != 0 || s.ToPush.Count != 1 {
		t.Fatalf("RED (#1198): open=%d %+v, want the eviction to count as the replacement", s.Open, s.ToPush)
	}
}

// A Put that began before the mark carries the pre-change state, whichever side
// of the serve's open it lands on (TestIssue1062_L1b is the same Put after it).
func TestStalePush1198_ALatePutThatBeganBeforeTheMarkReplacesNothing(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	early := WithL1KeyContext(context.Background(), e.key) // the resolve begins …
	e.mark()                                               // … then the data moves
	e.readThenLateOpen(t, func() { e.put(t, early, false) })
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerL1); s.Serves != 1 || s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("RED (#1198): a Put that read the pre-change state replaced a late serve (serves=%d open=%d samples=%d)",
			s.Serves, s.Open, s.ToPush.Count)
	}
}

// A carrier with no resolve start proves nothing, as for an open serve.
func TestStalePush1198_ALatePutWithNoResolveStartReplacesNothing(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.readThenLateOpen(t, func() { e.put(t, context.Background(), false) })
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerL1); s.Serves != 1 || s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("RED (#1198): a Put with no resolve start replaced a late serve (serves=%d open=%d samples=%d)",
			s.Serves, s.Open, s.ToPush.Count)
	}
}

// A stale-derived Put in the window leaves stale bytes in the cell.
func TestStalePush1198_ALateStaleDerivedPutReplacesNothing(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.put(t, WithL1KeyContext(context.Background(), e.key), true)
	e.readThenLateOpen(t, func() { e.put(t, WithL1KeyContext(context.Background(), e.key), true) })
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerAPIStage); s.Serves != 1 || s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("RED (#1198): a stale-derived Put replaced a late serve (serves=%d open=%d samples=%d)",
			s.Serves, s.Open, s.ToPush.Count)
	}
}
