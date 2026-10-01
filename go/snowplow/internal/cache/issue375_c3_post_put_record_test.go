// issue375_c3_post_put_record_test.go — #375 C3 (review of #395): the window between an
// accepted gen-guarded Put's dep check and a dep Record that runs AFTER the Put.
//
// The content / customer handlers Record a cell's deps only on an accepted Put (#189:
// no edge for a refused Put): apistage content MISS (PutIfGen → Record), cluster_list
// populate, restactions.go and widgets.go. During [Put-check, Record) a COLD cell has no
// edge, so a dep event there dirty-marks nothing, and the Put-check already ran. The fix
// is structural in recordInternal: a Record for the key its sink's resolve already Put
// re-checks that dep against the check's seq, and remarks the key once if it moved.
//
// NEUTER: drop the recheckAfterPutRecord call in recordInternal → the window sub-arm
// goes RED (0 remarks).

package cache

import (
	"context"
	"testing"
)

func TestIssue375_C3_EventBetweenPutCheckAndPostPutRecord_RemarksOnce(t *testing.T) {
	t.Run("event-in-window_remarks-once", func(t *testing.T) {
		c, rl, hl := setup375(t)
		const K = "c3-window"
		ctx := WithL1KeyContext(context.Background(), K)
		if !c.PutIfGen(ctx, K, body375(`{}`), c.CaptureGen(K)) {
			t.Fatalf("setup: PutIfGen refused")
		}
		if n := rl.count(K); n != 0 {
			t.Fatalf("setup: the Put-check must not remark (no deps moved yet); got %d", n)
		}
		Deps().OnUpdate(g375A, "ns", "obj") // in [Put-check, Record): no edge yet → no dirty-mark
		if n := hl.residentMarks(K); n != 0 {
			t.Fatalf("setup: the event must find no edge on the cold cell; got %d resident marks", n)
		}
		Deps().Record(ctx, K, g375A, "ns", "obj") // the post-Put Record
		if n := rl.count(K); n != 1 {
			t.Fatalf("#375 C3 RED: a dep event between the Put-check and the post-Put Record must remark the key "+
				"exactly once; got %d (the cell stays stale with no pending mark)", n)
		}
		rl.mu.Lock()
		r := rl.reasons["moved_after_put"]
		rl.mu.Unlock()
		if r != 1 {
			t.Fatalf("#375 C3: want reason moved_after_put=1, got reasons=%v", rl.reasons)
		}
		hl.mu.Lock()
		defer hl.mu.Unlock()
		saw := false
		for _, ev := range hl.evs {
			if ev.key == K && ev.gvr == g375A && ev.resident {
				saw = true
			}
		}
		if !saw {
			t.Fatalf("#375 C3: the remark must reach the refresh hook for the RESIDENT key with the dep's GVR; evs=%v", hl.evs)
		}
	})

	t.Run("no-event_zero-remarks", func(t *testing.T) {
		c, rl, _ := setup375(t)
		const K = "c3-quiet"
		ctx := WithL1KeyContext(context.Background(), K)
		c.PutIfGen(ctx, K, body375(`{}`), c.CaptureGen(K))
		Deps().Record(ctx, K, g375A, "ns", "obj")
		Deps().Record(ctx, K, g375B, "ns", "obj2")
		if n := rl.count(K); n != 0 {
			t.Fatalf("#375 C3 anti-amp RED: no dep event, yet %d remarks", n)
		}
	})

	t.Run("put-check-already-remarked_no-double", func(t *testing.T) {
		c, rl, _ := setup375(t)
		const K = "c3-once"
		// A content cell: own coordinate pre-declared, Recorded only after the Put.
		ctx := WithContentDepGenSink(WithL1KeyContext(context.Background(), "outer-c3"), g375A, "ns", "obj")
		Deps().OnUpdate(g375A, "ns", "obj") // moves BEFORE the Put → the Put-check remarks
		if !c.PutIfGen(ctx, K, body375(`{}`), c.CaptureGen(K)) {
			t.Fatalf("setup: PutIfGen refused")
		}
		Deps().OnUpdate(g375A, "ns", "obj") // and again before the Record
		Deps().Record(ctx, K, g375A, "ns", "obj")
		if n := rl.count(K); n != 1 {
			t.Fatalf("#375 C3 RED: a Put whose check already remarked must not be remarked again by the post-Put "+
				"re-check (once per Put); got %d", n)
		}
	})

	t.Run("record-for-another-key_no-recheck", func(t *testing.T) {
		c, rl, _ := setup375(t)
		const K = "c3-put"
		ctx := WithL1KeyContext(context.Background(), K)
		c.PutIfGen(ctx, K, body375(`{}`), c.CaptureGen(K))
		Deps().OnUpdate(g375A, "ns", "obj")
		Deps().Record(ctx, "c3-other-key", g375A, "ns", "obj")
		if n := rl.count(K) + rl.count("c3-other-key"); n != 0 {
			t.Fatalf("#375 C3 precision RED: a Record for a key the sink did NOT Put must not re-check; got %d remarks", n)
		}
	})
}
