package api

// issue1319_replay_outcomes_test.go — #1319: a body served WITHOUT running its
// resolve (a stored cell, a seed-memo entry) reports the outcomes the
// producing resolve stored, through the same collector a resolve would claim.
//
//   R1 a replay claims the collector: HeaderValue reads the stored value, a
//      resolve that runs later on the ctx keeps its own outcomes privately;
//   R2 a replay of "" (complete) reads [] and StorableStageOutcomes "";
//   R3 a collector a resolve already claimed ignores a replay;
//   R4 SealStageOutcomes ends the claim window of a collector nothing claimed;
//   R5 StorableStageOutcomes is "" exactly when the header would be [].

import (
	"context"
	"testing"
)

const r1319Stored = `[{"name":"sa","ok":false,"reason":"Forbidden"}]`

func TestIssue1319_R1_ReplayClaimsTheCollector(t *testing.T) {
	ctx, o := WithStageOutcomes(context.Background())
	ReplayStageOutcomes(ctx, r1319Stored)
	if got := o.HeaderValue(); got != r1319Stored {
		t.Fatalf("R1 RED: a replayed collector reads %q, want the stored %q", got, r1319Stored)
	}
	if got := StorableStageOutcomes(o); got != r1319Stored {
		t.Fatalf("R1 RED: StorableStageOutcomes = %q, want %q", got, r1319Stored)
	}
	// A resolve that runs on ctx after the replay must not report into it.
	if late := claimStageOutcomes(ctx); late == o {
		t.Fatal("R1 RED: a resolve after the replay claimed the replayed collector")
	}
}

func TestIssue1319_R2_ReplayOfCompleteReadsComplete(t *testing.T) {
	ctx, o := WithStageOutcomes(context.Background())
	ReplayStageOutcomes(ctx, "")
	if got := o.HeaderValue(); got != stageOutcomesAllSucceeded {
		t.Fatalf("R2 RED: a replay of a complete cell reads %q, want []", got)
	}
	if got := StorableStageOutcomes(o); got != "" {
		t.Fatalf("R2 RED: StorableStageOutcomes = %q, want \"\"", got)
	}
}

func TestIssue1319_R3_AClaimedCollectorIgnoresAReplay(t *testing.T) {
	ctx, o := WithStageOutcomes(context.Background())
	run := claimStageOutcomes(ctx)
	if run != o {
		t.Fatal("SETUP: the first resolve must claim the installed collector")
	}
	run.register([]string{"sa"})
	run.fail("sa", StageReasonTimeout)
	ReplayStageOutcomes(ctx, r1319Stored)
	want := `[{"name":"sa","ok":false,"reason":"Timeout"}]`
	if got := o.HeaderValue(); got != want {
		t.Fatalf("R3 RED: a replay overwrote the resolve's own outcomes: %q, want %q", got, want)
	}
}

func TestIssue1319_R4_SealEndsTheClaimWindow(t *testing.T) {
	ctx, o := WithStageOutcomes(context.Background())
	SealStageOutcomes(ctx)
	late := claimStageOutcomes(ctx)
	if late == o {
		t.Fatal("R4 RED: a resolve after the seal claimed the collector")
	}
	late.register([]string{"child"})
	late.fail("child", StageReasonError)
	if got := StorableStageOutcomes(o); got != "" {
		t.Fatalf("R4 RED: a sealed collector reads %q, want complete", got)
	}
	// No collector on ctx: every call is a no-op.
	SealStageOutcomes(context.Background())
	ReplayStageOutcomes(context.Background(), r1319Stored)
	if StageOutcomesFromContext(context.Background()) != nil {
		t.Fatal("R4: a bare ctx carries no collector")
	}
}

func TestIssue1319_R5_StorableIsEmptyExactlyWhenComplete(t *testing.T) {
	if StorableStageOutcomes(nil) != "" {
		t.Fatal("R5: a nil collector must store \"\"")
	}
	ctx, o := WithStageOutcomes(context.Background())
	run := claimStageOutcomes(ctx)
	run.register([]string{"a", "b"})
	run.ran("a")
	run.ran("b")
	if got := StorableStageOutcomes(o); got != "" {
		t.Fatalf("R5 RED: every stage ran and succeeded, stored %q, want \"\"", got)
	}
	_, o2 := WithStageOutcomes(context.Background())
	o2.register([]string{"a"}) // registered, never ran: NotRun
	if got := StorableStageOutcomes(o2); got == "" {
		t.Fatal("R5 RED: a stage that never ran was stored as complete")
	}
}
