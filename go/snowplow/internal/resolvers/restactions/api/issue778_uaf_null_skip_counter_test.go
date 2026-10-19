//go:build unit || integration

package api

// issue778_uaf_null_skip_counter_test.go — #778: a userAccessFilter stage whose
// resourcesFrom skipped a null (#767) is counted, and the stages skipping NOW
// are visible, so a silently cached deny-all (a typo'd path, or an input not
// yet reconciled) shows at the shipped LOG_LEVEL=warn.
//
// ARMS, through blueprint-formdef's exact api and the real Resolve (#767's
// n767Resolve harness) unless noted:
//   C1  `[.compdef.status.resource]` with no extras yields [null]: the total and
//       the "<ns>/<name>|<stage>" row move by EXACTLY 1 per resolve (two
//       resolves → 2), on the accessors AND on /debug/vars; skipping_now.
//   C2  the same expression resolved (`["fireworksapps"]`) moves nothing and
//       creates no row.
//   C3  an unresolved set (`[null, 1]`) is a stage error with its own ERROR, not
//       a skip: it moves nothing.
//   C4  metadata only: the by-stage keys are exactly "<ns>/<name>|<stage>" —
//       no expression text, no resolved value.
//   C5  two nulls in one stage evaluation count once (per evaluation, not per
//       element).
//   C6  CURRENT STATE: a stage skips (gauge 1), then the same stage resolves
//       (its compdef reconciled) → skipping_now false, gauge 0, count unchanged.
//   C7  a skipping stage whose next evaluation is unresolved is no longer
//       skipping now.
//   C8  the by-stage map is bounded: a stage beyond the cap folds into the
//       __other__ row (recorder-level).

import (
	"encoding/json"
	"expvar"
	"fmt"
	"strings"
	"testing"
)

const n778Key = "krateo-system/blueprint-formdef|namespaces"

func n778Expvar(t *testing.T) (uint64, map[string]UAFNullSkipRow) {
	t.Helper()
	RegisterUAFNullSkippedExpvarForTest()
	var total uint64
	byStage := map[string]UAFNullSkipRow{}
	for name, dst := range map[string]any{
		"snowplow_uaf_resources_null_skipped_total":    &total,
		"snowplow_uaf_resources_null_skipped_by_stage": &byStage,
	} {
		v := expvar.Get(name)
		if v == nil {
			t.Fatalf("#778: %s is not published on /debug/vars", name)
		}
		if err := json.Unmarshal([]byte(v.String()), dst); err != nil {
			t.Fatalf("#778: %s: %v", name, err)
		}
	}
	return total, byStage
}

func n778Reset(t *testing.T) {
	t.Helper()
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)
}

func TestIssue778_C1_NullSkipCountsExactlyOncePerResolve(t *testing.T) {
	n778Reset(t)
	for i := 1; i <= 2; i++ {
		r := n767Resolve(t, "[.compdef.status.resource]", nil, n767Ready)
		if ns, _ := json.Marshal(r.namespaces); string(ns) != "[]" || r.stageErrs != 0 {
			t.Fatalf("C1 PRE: resolve %d: namespaces=%s stage-errs=%d, want the #767 quiet deny-all", i, ns, r.stageErrs)
		}
		if got := UAFNullSkippedTotal(); got != uint64(i) {
			t.Fatalf("C1 RED: after %d resolve(s) the total is %d, want %d — exactly one per stage evaluation that skipped", i, got, i)
		}
		if got := UAFNullSkippedByStage()[n778Key]; got.Count != uint64(i) || !got.SkippingNow {
			t.Fatalf("C1 RED: after %d resolve(s) %q is %+v, want count %d skipping_now (rows %v)", i, n778Key, got, i, UAFNullSkippedByStage())
		}
	}
	if n := UAFNullSkippingStages(); n != 1 {
		t.Fatalf("C1: %d stages skipping now, want 1", n)
	}
	total, byStage := n778Expvar(t)
	if total != 2 || len(byStage) != 1 || byStage[n778Key] != (UAFNullSkipRow{Count: 2, SkippingNow: true}) {
		t.Fatalf("C1: /debug/vars total=%d by_stage=%v, want 2 and {%q: {2 true}}", total, byStage, n778Key)
	}
}

func TestIssue778_C2_ResolvedSetMovesNothing(t *testing.T) {
	n778Reset(t)
	r := n767Resolve(t, "[.compdef.status.resource]", n767Named, n767Ready)
	if ns, _ := json.Marshal(r.namespaces); string(ns) != `["ns-a"]` {
		t.Fatalf("C2 PRE: namespaces=%s, want [\"ns-a\"]", ns)
	}
	if got := UAFNullSkippedTotal(); got != 0 {
		t.Fatalf("C2 RED: a resolved resource set moved the null-skip total to %d", got)
	}
	if rows := UAFNullSkippedByStage(); len(rows) != 0 {
		t.Fatalf("C2: a stage that never skipped got a row: %v", rows)
	}
}

func TestIssue778_C3_UnresolvedSetIsNotASkip(t *testing.T) {
	n778Reset(t)
	r := n767Resolve(t, "[.compdef.status.resource, 1]", nil, n767Ready)
	if r.stageErrs == 0 {
		t.Fatal("C3 PRE: [null, 1] must stay a stage error")
	}
	if got := UAFNullSkippedTotal(); got != 0 {
		t.Fatalf("C3 RED: an unresolved set was counted as a null skip (%d) — it already has its stage error and ERROR", got)
	}
}

func TestIssue778_C4_ByStageKeysAreMetadataOnly(t *testing.T) {
	n778Reset(t)
	n767Resolve(t, "[.compdef.status.resource]", nil, n767Ready)
	_, byStage := n778Expvar(t)
	for k := range byStage {
		if k != n778Key {
			t.Fatalf("C4: unexpected by-stage key %q, want exactly %q", k, n778Key)
		}
		for _, leak := range []string{"compdef", "status", "resource", "[", "u-767"} {
			if strings.Contains(k, leak) {
				t.Fatalf("C4: by-stage key %q carries %q — the expression or an identity leaked into the metric", k, leak)
			}
		}
	}
}

func TestIssue778_C5_TwoNullsCountOnce(t *testing.T) {
	n778Reset(t)
	n767Resolve(t, "[.compdef.status.resource, .compdef.status.kind]", nil, n767Ready)
	if got := UAFNullSkippedTotal(); got != 1 {
		t.Fatalf("C5 RED: two nulls in one stage evaluation moved the total to %d, want 1", got)
	}
}

func TestIssue778_C6_ReconciledStageStopsSkippingNow(t *testing.T) {
	n778Reset(t)
	n767Resolve(t, "[.compdef.status.resource]", n767Named, n767NotReady)
	if n := UAFNullSkippingStages(); n != 1 {
		t.Fatalf("C6 PRE: the not-reconciled stage must be skipping now; gauge %d", n)
	}
	n767Resolve(t, "[.compdef.status.resource]", n767Named, n767Ready)
	if n := UAFNullSkippingStages(); n != 0 {
		t.Fatalf("C6 RED: after the compdef reconciled the gauge still reads %d — an \"ever\" gauge cannot tell a typo from a transient", n)
	}
	row := UAFNullSkippedByStage()[n778Key]
	if row.Count != 1 || row.SkippingNow {
		t.Fatalf("C6: row %+v, want count 1 (unchanged) and skipping_now false", row)
	}
	if got := UAFNullSkippedTotal(); got != 1 {
		t.Fatalf("C6: total %d, want 1 (the resolved evaluation is not a skip)", got)
	}
}

func TestIssue778_C7_UnresolvedAfterSkipIsNotSkippingNow(t *testing.T) {
	n778Reset(t)
	n767Resolve(t, "[.compdef.status.resource]", nil, n767Ready)
	n767Resolve(t, "[.compdef.status.resource, 1]", nil, n767Ready)
	if row := UAFNullSkippedByStage()[n778Key]; row.Count != 1 || row.SkippingNow {
		t.Fatalf("C7 RED: row %+v after an unresolved evaluation, want count 1 and skipping_now false", row)
	}
}

func TestIssue778_C8_ByStageMapIsCapped(t *testing.T) {
	n778Reset(t)
	limit := UAFNullSkipStageCap()
	for i := 0; i < limit+3; i++ {
		RecordUAFResourcesEvaluatedForTest("ns", fmt.Sprintf("ra-%d", i), "s", true)
	}
	rows := UAFNullSkippedByStage()
	if len(rows) != limit+1 {
		t.Fatalf("C8 RED: %d rows after %d distinct stages, want the cap %d plus the overflow row", len(rows), limit+3, limit)
	}
	if other := rows[uafNullSkipOtherKey]; other.Count != 3 || !other.SkippingNow {
		t.Fatalf("C8: overflow row %+v, want the 3 stages past the cap folded in", other)
	}
	if got := UAFNullSkippedTotal(); got != uint64(limit+3) {
		t.Fatalf("C8: total %d, want %d — folding must not lose counts", got, limit+3)
	}
}
