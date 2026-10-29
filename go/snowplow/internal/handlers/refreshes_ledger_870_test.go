package handlers

// refreshes_ledger_870_test.go — #870 K16e, the handler side of v5.1 C2: the
// /refreshes handler tells the store-swap push ledger about a frame ONLY after
// its writeFrame returned true. A failed write — refresh or resync — tells it
// nothing (K16e (4)). RED under X44 (a resync closes on a failed write) and
// X43's handler-side twin (a refresh frame reported before or without its
// write).

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func ledgerStream() (cache.RefreshStream, *[]string, *int) {
	var wrote []string
	resyncs := 0
	return cache.RefreshStream{
		Wrote:       func(k string) { wrote = append(wrote, k) },
		WroteResync: func() { resyncs++ },
	}, &wrote, &resyncs
}

func TestRefreshes870_LedgerHearsOnlyWrittenFrames(t *testing.T) {
	t.Run("refresh_written", func(t *testing.T) {
		st, wrote, _ := ledgerStream()
		var frames []string
		if !writeRefreshFrame(func(f string) bool { frames = append(frames, f); return true }, st, "k1") {
			t.Fatal("a successful write reported failure")
		}
		if len(frames) != 1 || len(*wrote) != 1 || (*wrote)[0] != "k1" {
			t.Fatalf("frames=%v ledger=%v, want one frame and one ledger note for k1", frames, *wrote)
		}
	})
	t.Run("refresh_failed", func(t *testing.T) {
		st, wrote, _ := ledgerStream()
		if writeRefreshFrame(func(string) bool { return false }, st, "k1") {
			t.Fatal("a failed write reported success")
		}
		if len(*wrote) != 0 {
			t.Fatalf("K16e RED (X43): the ledger heard of a frame whose write failed: %v", *wrote)
		}
	})
	t.Run("resync_written", func(t *testing.T) {
		st, _, resyncs := ledgerStream()
		writeResyncFrame(func(f string) bool { return f == refreshResyncFrame }, st)
		if *resyncs != 1 {
			t.Fatalf("a written resync frame told the ledger %d time(s), want 1", *resyncs)
		}
	})
	t.Run("resync_failed", func(t *testing.T) {
		st, _, resyncs := ledgerStream()
		writeResyncFrame(func(string) bool { return false }, st)
		if *resyncs != 0 {
			t.Fatalf("K16e (4) RED (X44): a resync whose write FAILED closed ledger entries (%d)", *resyncs)
		}
	})
}
