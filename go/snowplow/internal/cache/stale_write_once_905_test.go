package cache

// stale_write_once_905_test.go — #905 review F5: the stale-serve note and the
// write are decided atomically on the request's sink, so nested resolve:true
// items running in parallel can never both be served stale AND send a write
// from the same pass (which the D2 re-run would then send again).
//
//   A1  order: a noted stale serve holds a later write; a begun write refuses a
//       later stale serve.
//   A2  race (run under -race): N readers and N writers on one sink — after
//       they settle, never (served stale AND a write sent).
//   A3  a nested resolve that re-scoped its L1 key still reaches the request's
//       sink for a write (StaleServeSinkForWrite), while its reads are out of
//       D2 scope (StaleServeSinkFromContext is nil there).

import (
	"context"
	"sync"
	"testing"
)

func TestIssue905_A1_NoteAndWriteAreExclusive(t *testing.T) {
	s := &StaleServeSink{l1Key: "k"}
	if !s.NoteIfNoWrite("c1") {
		t.Fatal("a first stale serve must be noted")
	}
	if s.BeginWrite() {
		t.Fatal("a write after a stale serve must be held")
	}
	w := &StaleServeSink{l1Key: "k"}
	if !w.BeginWrite() {
		t.Fatal("a first write must be sent")
	}
	if w.NoteIfNoWrite("c1") {
		t.Fatal("a stale serve after a sent write must be refused (read through)")
	}
	if w.StaleDerived() {
		t.Fatal("after a write: the sink must not be stale-derived")
	}
	var nilSink *StaleServeSink
	if !nilSink.BeginWrite() || !nilSink.NoteIfNoWrite("c") {
		t.Fatal("a nil sink never holds anything")
	}
}

func TestIssue905_A2_ParallelReadsAndWritesNeverBoth(t *testing.T) {
	for round := 0; round < 200; round++ {
		s := &StaleServeSink{l1Key: "k"}
		var wg sync.WaitGroup
		var mu sync.Mutex
		served, sent := 0, 0
		for i := 0; i < 8; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				if s.NoteIfNoWrite("c") {
					mu.Lock()
					served++
					mu.Unlock()
				}
			}()
			go func() {
				defer wg.Done()
				if s.BeginWrite() {
					mu.Lock()
					sent++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if served > 0 && sent > 0 {
			t.Fatalf("round %d: %d stale serve(s) AND %d write(s) from one pass — the re-run would send the write again", round, served, sent)
		}
	}
}

func TestIssue905_A3_NestedWriteReachesTheRequestSink(t *testing.T) {
	ctx := WithL1KeyContext(context.Background(), "outer")
	ctx, s := WithStaleServeSink(ctx, "outer", "restactions")
	if s == nil {
		t.Fatal("PRE: no sink")
	}
	nested := WithL1KeyContext(ctx, "nested-cell")
	if StaleServeSinkFromContext(nested) != nil {
		t.Fatal("PRE: a re-scoped nested resolve must be out of D2 scope for its reads")
	}
	if StaleServeSinkForWrite(nested) != s {
		t.Fatal("#905 F5 RED: a nested resolve's write does not reach the request's sink — the D2 re-run could send it twice")
	}
}
