package dispatchers

// issue643_f9_detectors_test.go — #643 F9: the PR-A detectors can SEE the
// defect. Driven on the shared #643 harness (real engine, real seed, real
// apiref/restactions resolve, an httptest external endpoint that counts dials).
//
// On main (and with PR-A, which changes no behaviour) a background pass
// re-dials every external cell for every cohort, a slow external fetch is
// abandoned at the boot-readiness bound, and a gvr-discovered pass whose budget
// is shorter than its work is cut with planned > done. F9 asserts that each
// detector reads NON-ZERO under exactly that behaviour — the RED baseline — and
// that the positive controls for the #643 skips read 0 on every background kind
// (the skips do not exist yet). When PR-B lands, the background-dial and
// positive-control assertions flip to their targets in the PR-B arms.
//
// Not t.Parallel(): the harness reassigns package vars.

import (
	"testing"
	"time"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func h643ExternalCounts() (bg, req, bgAbandoned uint64) {
	for _, c := range restactionsapi.ExternalFetchSnapshot() {
		switch c.Origin {
		case restactionsapi.ExternalFetchOriginBackground:
			bg = c.Count
		case restactionsapi.ExternalFetchOriginRequest:
			req = c.Count
		}
	}
	for _, c := range restactionsapi.ExternalFetchAbandonedSnapshot() {
		if c.Origin == restactionsapi.ExternalFetchOriginBackground {
			bgAbandoned = c.Count
		}
	}
	return
}

func h643PassCount(kind, outcome string) uint64 {
	for _, c := range PrewarmPassTotalSnapshot() {
		if c.Kind == kind && c.Outcome == outcome {
			return c.Count
		}
	}
	return 0
}

func h643Last(t *testing.T, kind string) PrewarmPassLast {
	t.Helper()
	for _, l := range PrewarmPassLastSnapshot() {
		if l.Kind == kind {
			return l
		}
	}
	t.Fatalf("no last-pass reading for kind %q", kind)
	return PrewarmPassLast{}
}

func h643SkipDeclined(kind string) uint64 {
	for _, c := range PrewarmSkipDeclinedExternalSnapshot() {
		if c.Kind == kind {
			return c.Count
		}
	}
	return 0
}

func TestIssue643_F9_DetectorsReadTheDefect(t *testing.T) {
	const K, M, N = 3, 2, 4
	t.Setenv("RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS", "300")
	env := h643Setup(t, h643Opts{cohorts: K, external: M, internal: N})
	if got := h643Cohorts(); got != K {
		t.Fatalf("fixture precondition: %d cohorts enumerated for the widget GVR, want %d", got, K)
	}
	wantDials := K * M * 2 // every external stage, once per cohort
	h643Budget(t, nil)

	bg0, req0, _ := h643ExternalCounts()

	// Boot: the first meeting of every external key dials it once.
	bootDials := env.pass(prewarmScope{kind: scopeKindBoot})
	if bootDials != wantDials {
		t.Fatalf("boot dialled %d times, want %d (K=%d × M=%d × 2 stages). dials=%v", bootDials, wantDials, K, M, env.ep.byPath())
	}
	if l := h643Last(t, string(scopeKindBoot)); l.Planned != int64(K*(M+N)) || l.Done != l.Planned {
		t.Fatalf("boot last pass planned=%d done=%d, want planned=done=%d", l.Planned, l.Done, K*(M+N))
	}
	internalWarm, externalWarm := env.warmWidgetCells()
	if internalWarm != K*N || externalWarm != 0 {
		t.Fatalf("after boot: %d internal + %d external widget cells warm, want %d + 0", internalWarm, externalWarm, K*N)
	}

	// THE DEFECT, read by the detectors: every background pass dials every
	// external key again, for every cohort.
	for i, s := range []prewarmScope{
		{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR},
		{kind: scopeKindGVRDiscovered, gvr: h643RAGVR},
		{kind: scopeKindKeepwarm},
	} {
		if d := env.pass(s); d != wantDials {
			t.Fatalf("background pass %d (%s) dialled %d times, want %d — the RED baseline is every pass re-dials every external key",
				i, s.kind, d, wantDials)
		}
	}
	bg1, req1, _ := h643ExternalCounts()
	if got := bg1 - bg0; got != uint64(4*wantDials) {
		t.Fatalf("snowplow_external_fetch_total{origin=background} moved by %d over boot + 3 background passes, want %d (it must count every dial the endpoint saw)", got, 4*wantDials)
	}
	if req1 != req0 {
		t.Fatalf("snowplow_external_fetch_total{origin=request} moved by %d with no /call — the origin split is wrong", req1-req0)
	}
	if got := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeComplete); got != 2 {
		t.Fatalf("pass_total{gvr-discovered,complete} = %d, want 2", got)
	}
	// Positive controls: no background kind has the skip yet.
	for _, k := range []prewarmScopeKind{scopeKindGVRDiscovered, scopeKindKeepwarm} {
		if n := h643SkipDeclined(string(k)); n != 0 {
			t.Fatalf("skip_declined_external_total{%s} = %d on main; the skip does not exist for this kind yet", k, n)
		}
	}
	for _, c := range PrewarmSkipResumeDoneSnapshot() {
		if c.Count != 0 {
			t.Fatalf("skip_resume_done_total{%s} = %d; the resume skip does not exist yet", c.Kind, c.Count)
		}
	}
}

// TestIssue643_F9_CutAndAbandonDetectors — a slow external endpoint and a
// gvr-discovered budget shorter than the pass's work: the pass is cut (pass_total
// {gvr-discovered,cut} > 0, last planned > done) and the stalled fetches are
// abandoned at the boot-readiness bound (external_fetch_abandoned_total
// {background} > 0) while the server is still holding them.
func TestIssue643_F9_CutAndAbandonDetectors(t *testing.T) {
	const K, M, N = 3, 2, 4
	t.Setenv("RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS", "200")
	env := h643Setup(t, h643Opts{cohorts: K, external: M, slow: M, internal: N})
	env.ep.stall = 5 * time.Second
	if got := h643Cohorts(); got != K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, K)
	}
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)

	_, _, ab0 := h643ExternalCounts()
	cut0 := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut)

	env.pass(prewarmScope{kind: scopeKindBoot}) // an hour budget: completes, every slow fetch abandoned at 200ms
	_, _, ab1 := h643ExternalCounts()
	if got := ab1 - ab0; got != uint64(K*M*2) {
		t.Fatalf("external_fetch_abandoned_total{background} moved by %d, want %d — every external fetch stalled past the 200ms bound (dials=%v)", got, K*M*2, env.ep.byPath())
	}

	// The gvr-discovered budget is derived from the measured boot pass (same
	// work: the pre-seed settle + the K×(M+N) units, ~K×M×2×200ms of it the
	// abandoned external tail): two thirds of it covers the pre-seed work and
	// the internal units but not the external tail, on any machine speed.
	boot := h643Last(t, string(scopeKindBoot))
	budget := time.Duration(boot.ElapsedMS*2/3) * time.Millisecond
	budgets[scopeKindGVRDiscovered] = budget
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})
	if got := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut) - cut0; got != 1 {
		t.Fatalf("pass_total{gvr-discovered,cut} moved by %d, want 1 (the pass's external tail outlasts its %v budget)", got, budget)
	}
	l := h643Last(t, string(scopeKindGVRDiscovered))
	if l.Planned <= l.Done {
		t.Fatalf("cut gvr-discovered pass: planned=%d done=%d, want planned > done", l.Planned, l.Done)
	}
	if l.Planned != int64(K*(M+N)) {
		t.Fatalf("gvr-discovered planned=%d, want %d (K×(M+N), declared before the first unit)", l.Planned, K*(M+N))
	}
	if l.BudgetMS != budget.Milliseconds() || l.ElapsedMS < budget.Milliseconds() {
		t.Fatalf("gvr-discovered budget_ms=%d elapsed_ms=%d, want %d and >= it", l.BudgetMS, l.ElapsedMS, budget.Milliseconds())
	}
	if l.ResolvedWidgetUnits == 0 || l.ResolvedWidgetUS == 0 {
		t.Fatalf("gvr-discovered resolved_widget_units=%d resolved_widget_us=%d; the per-unit cost inputs must be non-zero", l.ResolvedWidgetUnits, l.ResolvedWidgetUS)
	}
}
