//go:build unit || integration

package api

// issue778_uaf_null_skip_counter_test.go — #778: a userAccessFilter stage whose
// resourcesFrom skipped a null (#767) is counted, so a silently cached deny-all
// (a typo'd path, or an input not yet reconciled) is visible at the shipped
// LOG_LEVEL=warn.
//
// ARMS, through blueprint-formdef's exact api and the real Resolve (#767's
// n767Resolve harness):
//   C1  `[.compdef.status.resource]` with no extras yields [null]: the total and
//       the "<ns>/<name>|<stage>" row move by EXACTLY 1 per resolve (two
//       resolves → 2), on the accessors AND on /debug/vars.
//   C2  the same expression resolved (`["fireworksapps"]`) moves nothing.
//   C3  an unresolved set (`[null, 1]`) is a stage error with its own ERROR, not
//       a skip: it moves nothing.
//   C4  metadata only: the by-stage keys are exactly "<ns>/<name>|<stage>" —
//       no expression text, no resolved value.
//   C5  two nulls in one stage evaluation count once.

import (
	"encoding/json"
	"expvar"
	"strings"
	"testing"
)

const n778Key = "krateo-system/blueprint-formdef|namespaces"

func n778Expvar(t *testing.T) (uint64, map[string]uint64) {
	t.Helper()
	RegisterUAFNullSkippedExpvarForTest()
	var total uint64
	byStage := map[string]uint64{}
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

func TestIssue778_C1_NullSkipCountsExactlyOncePerResolve(t *testing.T) {
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)

	for i := 1; i <= 2; i++ {
		r := n767Resolve(t, "[.compdef.status.resource]", nil, n767Ready)
		if ns, _ := json.Marshal(r.namespaces); string(ns) != "[]" || r.stageErrs != 0 {
			t.Fatalf("C1 PRE: resolve %d: namespaces=%s stage-errs=%d, want the #767 quiet deny-all", i, ns, r.stageErrs)
		}
		if got := UAFNullSkippedTotal(); got != uint64(i) {
			t.Fatalf("C1 RED: after %d resolve(s) the total is %d, want %d — exactly one per stage evaluation that skipped", i, got, i)
		}
		if got := UAFNullSkippedByStage()[n778Key]; got != uint64(i) {
			t.Fatalf("C1 RED: after %d resolve(s) %q is %d, want %d (rows %v)", i, n778Key, got, i, UAFNullSkippedByStage())
		}
	}
	if n := UAFNullSkippedStages(); n != 1 {
		t.Fatalf("C1: %d distinct stages, want 1", n)
	}
	total, byStage := n778Expvar(t)
	if total != 2 || byStage[n778Key] != 2 || len(byStage) != 1 {
		t.Fatalf("C1: /debug/vars total=%d by_stage=%v, want 2 and {%q: 2}", total, byStage, n778Key)
	}
}

func TestIssue778_C2_ResolvedSetMovesNothing(t *testing.T) {
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)
	r := n767Resolve(t, "[.compdef.status.resource]", n767Named, n767Ready)
	if ns, _ := json.Marshal(r.namespaces); string(ns) != `["ns-a"]` {
		t.Fatalf("C2 PRE: namespaces=%s, want [\"ns-a\"]", ns)
	}
	if got := UAFNullSkippedTotal(); got != 0 {
		t.Fatalf("C2 RED: a resolved resource set moved the null-skip total to %d", got)
	}
}

func TestIssue778_C3_UnresolvedSetIsNotASkip(t *testing.T) {
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)
	r := n767Resolve(t, "[.compdef.status.resource, 1]", nil, n767Ready)
	if r.stageErrs == 0 {
		t.Fatal("C3 PRE: [null, 1] must stay a stage error")
	}
	if got := UAFNullSkippedTotal(); got != 0 {
		t.Fatalf("C3 RED: an unresolved set was counted as a null skip (%d) — it already has its stage error and ERROR", got)
	}
}

func TestIssue778_C5_TwoNullsCountOnce(t *testing.T) {
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)
	n767Resolve(t, "[.compdef.status.resource, .compdef.status.kind]", nil, n767Ready)
	if got := UAFNullSkippedTotal(); got != 1 {
		t.Fatalf("C5 RED: two nulls in one stage evaluation moved the total to %d, want 1", got)
	}
}

func TestIssue778_C4_ByStageKeysAreMetadataOnly(t *testing.T) {
	ResetUAFNullSkippedForTest()
	t.Cleanup(ResetUAFNullSkippedForTest)
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
