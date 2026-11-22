package api

// issue1569_failure_replay_test.go — #1569: a resolve served by replay (a
// SeedResolveMemo hit) notes the failures its body carries in its cell's round,
// so the round never ends as a success that wipes the cell's notes. The rule
// per cell: the first occurrence of a failure in its residency at its level,
// every unchanged repeat at DEBUG, a changed class at its level again, and a
// recovery the producer saw clears the note (a recurrence logs again).
//
// The rounds are driven directly (BeginFailureRound, FailureLogLevel,
// WithFailureCapture, ReplayFailures): the real refresher path is
// dispatchers/issue1569_refresher_failure_log_test.go.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	t1569Target = "stage\x00sa\x00/api/v1/namespaces/x/configmaps/y"
	t1569Raw    = "/api/v1/namespaces/x/configmaps/y"
)

// produce1569 is one PRODUCING resolve of cell key: it logs a failure of
// class (none when class is "") and returns the line's level and what a memo
// entry would store for it.
func produce1569(key, class string, own slog.Level) (slog.Level, string) {
	ctx, end := BeginFailureRound(cache.WithL1KeyContext(context.Background(), key))
	ctx, cap := WithFailureCapture(ctx)
	lv := slog.Level(-100)
	if class != "" {
		lv = FailureLogLevel(ctx, own, t1569Target, class)
	}
	end()
	return lv, cap.Stored()
}

// hit1569 is one round of cell key served from the memo entry stored.
func hit1569(key, stored string) {
	ctx, end := BeginFailureRound(cache.WithL1KeyContext(context.Background(), key))
	ReplayFailures(ctx, stored)
	end()
}

func setup1569(t *testing.T) {
	t.Helper()
	t.Cleanup(SetFailureLogResidentForTest(func(string) bool { return true }))
}

// U1 — the 057 shape: two cells on one RESTAction, the producer alternating.
func TestIssue1569_MemoSiblings_UnchangedRepeatIsDebug_FirstPerCellAtItsLevel(t *testing.T) {
	setup1569(t)
	var got []slog.Level
	for i, producer := range []string{"w1", "w2", "w1", "w2", "w1"} {
		hitter := map[string]string{"w1": "w2", "w2": "w1"}[producer]
		lv, stored := produce1569(producer, classPerIdentityDenied, slog.LevelWarn)
		if stored == "" {
			t.Fatalf("burst %d: the producing resolve's failure was not captured", i+1)
		}
		hit1569(hitter, stored)
		got = append(got, lv)
	}
	want := []slog.Level{slog.LevelWarn, slog.LevelWarn, slog.LevelDebug, slog.LevelDebug, slog.LevelDebug}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("#1569: producer levels %v, want %v — each cell's first deny at WARN, every unchanged "+
				"repeat DEBUG, though its cell was served from the memo in between", got, want)
		}
	}
}

// U2 — a CHANGED failure is never hidden: a new class on the same target logs
// at its level in the cell that produced it, and again at its level in the
// sibling's own first resolve of it (the sibling had only been served it).
func TestIssue1569_ChangedClass_LogsAtItsLevel_InEveryCell(t *testing.T) {
	setup1569(t)
	lv, s := produce1569("w1", classPerIdentityDenied, slog.LevelWarn)
	if lv != slog.LevelWarn {
		t.Fatalf("SETUP: first deny logged at %v", lv)
	}
	hit1569("w2", s)
	if lv, s = produce1569("w2", classPerIdentityDenied, slog.LevelWarn); lv != slog.LevelWarn {
		t.Fatalf("SETUP: w2's first own deny logged at %v, want WARN", lv)
	}
	hit1569("w1", s)
	// The failure changes: the same read now fails with a 500.
	if lv, s = produce1569("w1", "500", slog.LevelError); lv != slog.LevelError {
		t.Fatalf("#1569 RED: w1's CHANGED failure (deny -> 500) logged at %v, want ERROR", lv)
	}
	hit1569("w2", s)
	if lv, s = produce1569("w2", "500", slog.LevelError); lv != slog.LevelError {
		t.Fatalf("#1569 RED: w2's first own resolve of the changed failure logged at %v, want ERROR — "+
			"the replay must not count as the cell having logged a class it never logged", lv)
	}
	hit1569("w1", s)
	if lv, _ = produce1569("w1", "500", slog.LevelError); lv != slog.LevelDebug {
		t.Fatalf("w1's unchanged 500 logged at %v, want DEBUG", lv)
	}
}

// U3 — a recovery the producer saw clears the sibling's note: its recurrence
// logs at its level.
func TestIssue1569_RecoveryThroughTheMemo_ThenRecurrence_LogsAgain(t *testing.T) {
	setup1569(t)
	_, s := produce1569("w1", classPerIdentityDenied, slog.LevelWarn)
	hit1569("w2", s)
	if lv, _ := produce1569("w2", classPerIdentityDenied, slog.LevelWarn); lv != slog.LevelWarn {
		t.Fatalf("SETUP: w2's first own deny logged at %v", lv)
	}
	// w1 recovers (its round has no failure); w2 is served that success.
	_, s = produce1569("w1", "", slog.LevelWarn)
	if s != "" {
		t.Fatalf("SETUP: a round with no failure stored %q", s)
	}
	hit1569("w2", s)
	if lv, _ := produce1569("w2", classPerIdentityDenied, slog.LevelWarn); lv != slog.LevelWarn {
		t.Fatalf("#1569 RED: w2's deny after a recovery it was served logged at %v, want WARN — a recurrence is new", lv)
	}
}

// U3b — a served round that overlapped a change never makes a failure DEBUG on
// stale notes: w1 has logged its deny; a served round of w1 reads that, a
// concurrent resolve of w1 recovers and ends first, and the served round's end
// writes the replayed deny as NOT logged, so w1's next deny logs at its level.
func TestIssue1569_ServedRoundOverlappingARecovery_NeverHidesTheRecurrence(t *testing.T) {
	setup1569(t)
	_, s := produce1569("w1", classPerIdentityDenied, slog.LevelWarn)
	served, endServed := BeginFailureRound(cache.WithL1KeyContext(context.Background(), "w1"))
	ReplayFailures(served, s) // reads w1's notes: logged
	if _, s2 := produce1569("w1", "", slog.LevelWarn); s2 != "" {
		t.Fatalf("SETUP: the recovering round stored %q", s2)
	}
	endServed()
	if lv, _ := produce1569("w1", classPerIdentityDenied, slog.LevelWarn); lv != slog.LevelWarn {
		t.Fatalf("#1569 RED: after a served round overlapped a recovery, w1's deny logged at %v, want WARN", lv)
	}
}

// U4 — a capture collects only its own cell's failures: one noted in a nested
// resolve under another key (the RAFullList cell) is that cell's.
func TestIssue1569_Capture_CollectsOnlyItsCell(t *testing.T) {
	setup1569(t)
	ctx, end := BeginFailureRound(cache.WithL1KeyContext(context.Background(), "w1"))
	ctx, cap := WithFailureCapture(ctx)
	nested, endNested := BeginFailureRound(cache.WithL1KeyContext(ctx, "ra-full-list"))
	_ = FailureLogLevel(nested, slog.LevelWarn, t1569Target, classPerIdentityDenied)
	endNested()
	end()
	if s := cap.Stored(); s != "" {
		t.Fatalf("#1569: the widget cell's capture collected another cell's failure: %q", s)
	}
}

// U5 — what a memo entry carries is digests and closed classes, never a target
// in clear (a path can name a namespace or an object).
func TestIssue1569_Stored_HoldsNoTargetInClear(t *testing.T) {
	setup1569(t)
	_, s := produce1569("w1", classPerIdentityDenied, slog.LevelWarn)
	if s == "" || strings.Contains(s, t1569Raw) || strings.Contains(s, "configmaps") {
		t.Fatalf("#1569: stored failures %q must be non-empty and hold no target text", s)
	}
}

// U6 — an inert (dry-run, #443) resolve neither captures nor replays.
func TestIssue1569_InertResolve_NeitherCapturesNorReplays(t *testing.T) {
	setup1569(t)
	_, s := produce1569("w1", classPerIdentityDenied, slog.LevelWarn)
	inert := cache.WithInert(cache.WithL1KeyContext(context.Background(), "w2"))
	ctx, end := BeginFailureRound(inert)
	ctx, cap := WithFailureCapture(ctx)
	ReplayFailures(ctx, s)
	end()
	if cap.Stored() != "" {
		t.Fatal("#1569: an inert resolve captured a failure")
	}
	if lv, _ := produce1569("w2", classPerIdentityDenied, slog.LevelWarn); lv != slog.LevelWarn {
		t.Fatalf("#1569: an inert replay spent w2's first line (logged at %v)", lv)
	}
}
