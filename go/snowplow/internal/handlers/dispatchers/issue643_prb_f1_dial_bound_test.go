package dispatchers

// issue643_prb_f1_dial_bound_test.go — #643 PR-B F1: the dial bound.
//
// Across all background passes in one process, each (external key × RA version)
// is dialled AT MOST ONCE: boot, or the first background pass to meet the key,
// dials it; every later pass skips it. Driven on the shared #643 harness (real
// engine, real seed, real apiref/restactions resolve, a counting httptest
// endpoint behind a named endpointRef Secret).
//
// Arms:
//   F1a fast   — boot met every key: background passes dial 0, internal cells
//                are still re-Put K×N per gvr-discovered pass (I-3), and the
//                positive control reads K×M per background pass.
//   F1b cut    — boot was cut right before its external tail: the FIRST
//                background pass dials each key once (K×M×2 in total), later
//                passes 0.
//   F1c slow   — every external fetch stalls past the boot-readiness bound
//                (abandoned; the stage-error sink is bumped too): a boot resume
//                and every background pass dial 0, and a gvr-discovered pass on
//                a budget its re-dials used to overrun now completes.
//
// RED on PR-A's base (no engine-wide skip): F1a/F1b dial K×M×2 on EVERY
// background pass; F1c's boot resume re-dials every abandoned key (the stage
// error branch declined before the mark).
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"testing"
	"time"
)

const h643K, h643M, h643N = 3, 2, 4

func h643SkipDelta(before map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range PrewarmSkipDeclinedExternalSnapshot() {
		out[c.Kind] = c.Count - before[c.Kind]
	}
	return out
}

func h643SkipSnap() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range PrewarmSkipDeclinedExternalSnapshot() {
		out[c.Kind] = c.Count
	}
	return out
}

func TestIssue643_F1a_BackgroundPassesDialZeroAfterBoot(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	if got := h643Cohorts(); got != h643K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, h643K)
	}
	h643Budget(t, nil)
	wantBoot := h643K * h643M * 2

	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != wantBoot {
		t.Fatalf("boot dialled %d, want %d (every external key once)", d, wantBoot)
	}
	for i, s := range []prewarmScope{
		{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR},
		{kind: scopeKindGVRDiscovered, gvr: h643RAGVR},
		{kind: scopeKindGVRDiscovered, gvr: h643ThingsGVR},
		{kind: scopeKindKeepwarm},
	} {
		skip0 := h643SkipSnap()
		puts0 := pipBindingSetSeedResolvesTotal.Load()
		if d := env.pass(s); d != 0 {
			t.Fatalf("background pass %d (%s) dialled the external endpoint %d times, want 0 — boot already met every external key (dials=%v)", i, s.kind, d, env.ep.byPath())
		}
		if got := h643SkipDelta(skip0)[string(s.kind)]; got != uint64(h643K*h643M) {
			t.Fatalf("background pass %d (%s): skip_declined_external_total{%s} moved by %d, want %d (the positive control: one skip per external (widget, cohort))", i, s.kind, s.kind, got, h643K*h643M)
		}
		if s.kind == scopeKindGVRDiscovered {
			// I-3: every cacheable cell is still re-resolved and re-Put by a
			// gvr-discovered pass — the skip removes only the external ones.
			if got := pipBindingSetSeedResolvesTotal.Load() - puts0; got != uint64(h643K*h643N) {
				t.Fatalf("gvr-discovered pass %d re-Put %d internal cells, want %d (K×N) — the skip must not remove a cacheable cell (I-3)", i, got, h643K*h643N)
			}
		}
	}
}

func TestIssue643_F1b_BootCutBeforeExternalTail_FirstBackgroundPassDialsOnce(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	if got := h643Cohorts(); got != h643K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, h643K)
	}
	h643Budget(t, map[prewarmScopeKind]time.Duration{scopeKindBoot: 2 * time.Second})

	// Cut boot exactly at its external tail (#594 orders external units last):
	// the first external unit holds until the boot budget expires.
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	cutBoot := true
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if cutBoot && mode == seedModeBoot && env.external[e.W.GetNamespace()+"/"+e.W.GetName()] {
			<-ctx.Done()
			return ctx.Err()
		}
		return prevW(ctx, e, authnNS, mode)
	}
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != 0 {
		t.Fatalf("precondition: the cut boot dialled %d, want 0 (cut before the external tail)", d)
	}
	cutBoot = false

	total := 0
	for i, s := range []prewarmScope{
		{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR},
		{kind: scopeKindGVRDiscovered, gvr: h643RAGVR},
		{kind: scopeKindKeepwarm},
		{kind: scopeKindGVRDiscovered, gvr: h643ThingsGVR},
	} {
		d := env.pass(s)
		total += d
		if i > 0 && d != 0 {
			t.Fatalf("background pass %d (%s) dialled %d, want 0 — the first background pass already met every external key", i, s.kind, d)
		}
	}
	if want := h643K * h643M * 2; total != want {
		t.Fatalf("background passes dialled %d in total, want %d (each external key exactly once)", total, want)
	}
}

func TestIssue643_F1c_SlowEndpoint_AbandonedFetchesAreDialledOnce(t *testing.T) {
	t.Setenv("RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS", "200")
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, slow: h643M, internal: h643N})
	env.ep.stall = 5 * time.Second
	if got := h643Cohorts(); got != h643K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, h643K)
	}
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)
	want := h643K * h643M * 2

	_, _, ab0 := h643ExternalCounts()
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != want {
		t.Fatalf("boot dialled %d, want %d", d, want)
	}
	if _, _, ab1 := h643ExternalCounts(); ab1-ab0 != uint64(want) {
		t.Fatalf("precondition: %d fetches abandoned at the bound, want %d (the slow arm must exercise the abandoned class)", ab1-ab0, want)
	}
	first := h643Last(t, string(scopeKindBoot)) // the full first pass, abandoned tail included

	// A boot RESUME (a second boot pass) must not re-dial an abandoned key.
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != 0 {
		t.Fatalf("boot resume re-dialled %d abandoned external fetches, want 0 — an abandoned fetch is an observed external touch and must mark (brief §1(c) as amended)", d)
	}

	// A gvr-discovered pass on two thirds of boot's measured wall time: on the
	// base its K×M×2 abandoned re-dials alone overran this; with the skip it
	// finishes.
	budgets[scopeKindGVRDiscovered] = time.Duration(first.ElapsedMS*2/3+1) * time.Millisecond
	cut0 := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut)
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR}); d != 0 {
		t.Fatalf("gvr-discovered dialled %d abandoned external fetches, want 0", d)
	}
	if got := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut) - cut0; got != 0 {
		t.Fatalf("gvr-discovered pass was cut (%d) on a budget its work fits once the external tail is skipped", got)
	}
	if l := h643Last(t, string(scopeKindGVRDiscovered)); l.Done != l.Planned {
		t.Fatalf("gvr-discovered planned=%d done=%d, want done == planned", l.Planned, l.Done)
	}
}
