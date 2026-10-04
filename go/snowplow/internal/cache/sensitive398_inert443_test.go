package cache

import (
	"context"
	"testing"
)

// #443 x #398: the sensitive-resource Put decline and the inert (dry-run) flag
// are independent checks. Under the flag a sensitive resolve STILL declines
// (so an inert Put gate mutated off can never let a Secret through a
// DeclineSensitivePut call site), and only the decline counter is skipped
// (dry-run traffic is not a decline).
func TestS398_DeclineSensitivePutUnderInert443(t *testing.T) {
	for _, inert := range []bool{false, true} {
		ctx := context.Background()
		if inert {
			ctx = WithInert(ctx)
		}
		ctx, sink := WithSensitiveTouchedSink(ctx)
		if DeclineSensitivePut(ctx) {
			t.Fatalf("inert=%v: declined with no sensitive read", inert)
		}
		sink.Bump()
		before := SensitiveSkippedPutForTest()
		if !DeclineSensitivePut(ctx) {
			t.Fatalf("inert=%v: a sensitive resolve did not decline its Put — the flag hid the #398 decline", inert)
		}
		got := SensitiveSkippedPutForTest() - before
		want := uint64(1)
		if inert {
			want = 0
		}
		if got != want {
			t.Errorf("inert=%v: decline counter moved by %d, want %d", inert, got, want)
		}
	}
}
