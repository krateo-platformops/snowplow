package cache

// issue1321_review_test.go — #1321 review M1 and M3.
//
//	M1   a write whose caller takes no frame decision compares nothing: a
//	     GET-only content write over a resident #854 incremental list cell
//	     builds no envelope (ListCellStats.RawJSONBuilt does not move), so the
//	     envelope is never built, memoised and kept resident beside the
//	     incremental cell just for a compare nobody reads. On a26cb2b2 every
//	     accepted putIfGen ran sameBytesPrior, whose prior.Raw() built it.
//	     BenchmarkIssue1321_M1_PutOverAResidentPrior measures what the compare
//	     costs where it is consumed (one memcmp, no extra c.mu acquisition)
//	     and that the no-outcome write pays nothing for it.
//	M3   the ledger's identical close (noteStalePushPut) decides on the
//	     client's NEWEST serve, and only a Put that REPLACES that serve can
//	     prove the client holds current bytes. A Put that replaces an older
//	     serve but began before the newest serve's mark carries the newest
//	     serve's bytes and still proves nothing: the older serve stays open
//	     (replaced, waiting for a frame) and the newest stays open. Kills the
//	     review's Ma7 (dropping `entry.replaces(newest)`).

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestIssue1321_M1_AContentWriteOverAListCellBuildsNoEnvelope(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	_, cell, _ := c854AnchorWith(t, 5, c854Funcs(), nil)
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Group: c854GVR.Group, Version: c854GVR.Version,
		Resource: c854GVR.Resource, Namespace: "c854"}
	key := ComputeKey(in)
	ctx := context.Background()
	if !c.PutContentIfGen(ctx, key, NewListCellEntry(cell, &in), c.CaptureGen(key)) {
		t.Fatal("PRE: the content fill was refused")
	}
	b0 := ListCellStatsSnapshot().RawJSONBuilt
	// The re-fill of a resident list cell (a #547 revalidation of the content
	// key re-anchors over it): the prior is an incremental list cell.
	if !c.PutContentIfGen(ctx, key, NewListCellEntry(cell, &in), c.CaptureGen(key)) {
		t.Fatal("PRE: the content re-fill was refused")
	}
	if got := ListCellStatsSnapshot().RawJSONBuilt; got != b0 {
		t.Fatalf("M1 RED: a content write over a resident list cell built %d envelope(s) — nothing reads that write's "+
			"outcome, so it must compare nothing, and the envelope must not be built and kept resident for it", got-b0)
	}
	// Scope: the stat does count a build when a reader asks for the body.
	cur, _ := c.GetNoTouch(key)
	_ = cur.Raw()
	if got := ListCellStatsSnapshot().RawJSONBuilt; got != b0+1 {
		t.Fatalf("PRE: RawJSONBuilt %d → %d after a Raw() read, want +1 (the instrument must see a build)", b0, got)
	}
}

var bench1321Sink PutOutcome

// BenchmarkIssue1321_M1_PutOverAResidentPrior: the gen-guarded write over a
// resident prior of the same bytes, with and without a consumed outcome.
// "no_outcome" is what every content / raFullList-layer / fixture write pays
// (no compare: the same work as before #1321); "outcome" adds one memcmp of
// the body after the commit, against the entry the write replaced (no extra
// c.mu acquisition); "outcome_other_len" is a changed body of another length,
// decided by RawLen without reading either body.
func BenchmarkIssue1321_M1_PutOverAResidentPrior(b *testing.B) {
	CompleteRBACSnapshotForTest(b)
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		body := make([]byte, size)
		for i := range body {
			body[i] = byte('a' + i%26)
		}
		c := newResolvedCache(16, 1<<30, time.Hour)
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "bench1321", Name: "b"}
		key := ComputeKey(in)
		if !c.put(key, &ResolvedEntry{RawJSON: body, Inputs: &in}) {
			b.Fatal("PRE: the resident prior was refused")
		}
		// Two entries with equal bytes in separate buffers, alternated, so every
		// compare reads both bodies (memequal short-circuits on one buffer).
		sameA := &ResolvedEntry{RawJSON: append([]byte(nil), body...), Inputs: &in}
		sameB := &ResolvedEntry{RawJSON: append([]byte(nil), body...), Inputs: &in}
		alt := false
		same := func() *ResolvedEntry {
			alt = !alt
			if alt {
				return sameA
			}
			return sameB
		}
		other := &ResolvedEntry{RawJSON: append(append([]byte(nil), body...), ' '), Inputs: &in}
		ctx := context.Background()
		run := func(name string, f func() PutOutcome, want PutOutcome) {
			b.Run(strconv.Itoa(size>>10)+"KiB/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					bench1321Sink = f()
				}
				if bench1321Sink != want {
					b.Fatalf("PRE: %s reported %v, want %v (the write was not measured)", name, bench1321Sink, want)
				}
			})
		}
		run("no_outcome", func() PutOutcome {
			return c.putIfGenWrite(ctx, key, same(), c.CaptureGen(key), false)
		}, PutChanged)
		run("outcome", func() PutOutcome {
			return c.putIfGenOutcome(ctx, key, same(), c.CaptureGen(key))
		}, PutIdentical)
		// Alternate the two bodies so every write replaces the other length.
		flip := false
		run("outcome_other_len", func() PutOutcome {
			flip = !flip
			e := other
			if !flip {
				e = same()
			}
			return c.putIfGenOutcome(ctx, key, e, c.CaptureGen(key))
		}, PutChanged)
	}
}

func TestIssue1321_M3_APutThatDoesNotReplaceTheNewestServeClosesNothingIdentical(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()

	early := e.fresh1321() // resolve E begins before the first mark
	e.mark()               // mark 1
	e.hit(t)               // serve 1: {"body":"old"}
	mid := e.fresh1321()   // resolve M begins after mark 1, before mark 2
	e.advance(200 * time.Millisecond)
	// E stores "mid": it began before mark 1, so it replaces nothing, but it
	// changed the bytes and publishes; the client refetches.
	if put := e.refreshPut1321(t, early, `{"body":"mid"}`); put != PutChanged {
		t.Fatalf("PRE: %v, want changed", put)
	}
	written(st, cycleRecv(t, st)) // closes nothing: serve 1 is not replaced yet
	e.mark()                      // mark 2: the data moves again
	e.hit(t)                      // serve 2 (the newest): {"body":"mid"}, dirty after mark 2
	e.advance(300 * time.Millisecond)

	// M stores "mid" again: identical to the cell and to serve 2's bytes. It
	// replaces serve 1 (began after mark 1) but NOT serve 2 (began before mark
	// 2), so it proves nothing about what the client now shows.
	if put := e.refreshPut1321(t, mid, `{"body":"mid"}`); put != PutIdentical {
		t.Fatalf("PRE: %v, want identical", put)
	}
	if !noFrameWithin(t, st, 50*time.Millisecond) {
		t.Fatal("PRE: an identical refresh Put published a frame")
	}
	s := k1062Layer(t, StaleLayerL1)
	if s.Closes[stalePushCloseIdentical] != 0 || s.Open != 2 {
		t.Fatalf("M3 RED: identical=%d open=%d, want 0/2 — the Put does not replace the client's newest serve (it began "+
			"before that serve's mark), so it cannot prove the client holds current bytes; closing the older serve "+
			"'identical' would record a stale display as confirmed (#1321 review M3)", s.Closes[stalePushCloseIdentical], s.Open)
	}
	if s.ToPush.Count != 0 {
		t.Fatalf("M3 RED: stale_to_push recorded %d sample(s) for serves no frame or confirming Put closed", s.ToPush.Count)
	}

	// Scope: a Put that DOES replace the newest serve with its bytes closes both.
	e.advance(100 * time.Millisecond)
	if put := e.refreshPut1321(t, e.fresh1321(), `{"body":"mid"}`); put != PutIdentical {
		t.Fatalf("PRE: %v, want identical", put)
	}
	if s := k1062Layer(t, StaleLayerL1); s.Closes[stalePushCloseIdentical] != 2 || s.Open != 0 {
		t.Fatalf("M3 scope: identical=%d open=%d, want 2/0 once a Put replaces the newest serve with its bytes",
			s.Closes[stalePushCloseIdentical], s.Open)
	}
}
