package api

// issue1288_stage_not_executed_test.go — #1288: a stage snowplow deliberately
// did not execute is not a failure. SummarizeStageOutcomes counts it in
// NotExecuted, never in Failed, and names no reason for it; the export /
// background write refusal records the closed-set WriteRefused (never the
// generic Error); a write that depends on a refused write is refused too,
// never Skipped (Skipped only follows a failure).
//
// THE ORACLE IS NOT THE PARSER. Collector arms build the outcomes by hand (the
// orchestrator's register / ran / fail / skip) and state the counts they put
// there. Resolver arms run ResolveWithFailure against the #904 harness's
// counting upstream: "nothing was sent" is the upstream's count, and the
// expected rows are the stages the arm declared.
//
//   - N1 a dry run's refused write only: failed 0, not_executed 1, no reasons;
//   - N2 a mix (NotFound, Skipped after it, a dry-run refusal, a background
//     refusal, NotRun): each count is what the arm put there;
//   - N3 the truncated summary: failed is its N, flagged Truncated (the span
//     then sets no not_executed: the summary cannot tell);
//   - N4 an export resolve of read → write → dependent write: nothing is sent,
//     both writes read WriteRefused (not Error, not Skipped), and the summary
//     of the header it rendered is failed 0, not_executed 2;
//   - N5 the same for a background resolve (the refresher origin);
//   - N6 the classifier is a property of the reason: every declared code has
//     a class, and only StageNotExecuted and WriteRefused are deliberate (the
//     S5 arm reads the declared constants and fails on an unclassified one);
//     every code fail() can record is ranked (else fail() folds it to Error),
//     the orchestrator-only codes are not, and WriteRefused ranks below every
//     failure code;
//   - N7 (review F1) a write stage with one item the plan refused
//     (InvalidPath) and one the export / refresher refused reads InvalidPath,
//     counts in failed, and its dependent is Skipped;
//   - N8 (review F2) a write iterating a refused stage's output has zero items:
//     it succeeds, unlisted, and nothing is sent.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func n1288Want(t *testing.T, what string, got StageOutcomeSummary, failed, notExecuted int, reasons ...string) {
	t.Helper()
	s1264Want(t, what, got, failed, reasons...)
	if got.NotExecuted != notExecuted || got.Truncated {
		t.Fatalf("%s: summary NotExecuted=%d Truncated=%v, want NotExecuted=%d Truncated=false",
			what, got.NotExecuted, got.Truncated, notExecuted)
	}
}

func TestIssue1288_N1_ADryRunRefusalOnlyIsNoFailure(t *testing.T) {
	o := newStageOutcomes()
	o.register([]string{"read", "create"})
	o.ran("read")
	o.fail("create", StageReasonNotExecuted) // the #443 k refusal, as dispatchOneCall records it
	v := o.HeaderValue()
	got := s1264Summary(t, v)
	n1288Want(t, "header "+v, got, 0, 1)
	if got.Reasons != nil {
		t.Fatalf("a dry run's refusal only: Reasons = %#v, want nil (no failure_reasons attribute)", got.Reasons)
	}
}

func TestIssue1288_N2_AMixCountsEachClass(t *testing.T) {
	o := newStageOutcomes()
	o.register([]string{"ok", "missing", "after-missing", "draft-write", "export-write", "never"})
	o.ran("ok")
	o.fail("missing", StageReasonNotFound)
	o.skip("after-missing", "missing")
	o.fail("draft-write", StageReasonNotExecuted)
	o.fail("export-write", StageReasonWriteRefused)
	// "never" is registered and never ran: NotRun.
	v := o.HeaderValue()
	// failed: missing, after-missing, never. not executed: draft-write, export-write.
	n1288Want(t, "header "+v, s1264Summary(t, v), 3, 2,
		StageReasonNotFound, StageReasonNotRun, StageReasonSkipped)
}

func TestIssue1288_N3_TruncatedSummaryCannotTellDeliberateRows(t *testing.T) {
	got := s1264Summary(t, TruncatedStageOutcomes(7))
	if got.Failed != 7 || got.NotExecuted != 0 || !got.Truncated || got.Reasons != nil {
		t.Fatalf("truncated summary = %+v, want Failed=7 NotExecuted=0 Truncated=true Reasons=nil", got)
	}
}

// n1288Resolve is u904Resolve under a non-customer ctx (mark), which is what
// the export handler's re-dispatch (cache.WithExportRead) or a background
// driver installs.
func n1288Resolve(t *testing.T, u *u904Upstream, stages []*templates.API, mark func(context.Context) context.Context) u904Result {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	ctx = mark(ctx)
	ctx, outcomes := WithStageOutcomes(ctx)
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC:                  &rest.Config{Host: u.srv.URL},
		AuthnNS:             u904AuthnNS,
		Items:               stages,
		RESTActionNamespace: "ns-1288",
		RESTActionName:      "ra-1288",
	})
	raw := outcomes.HeaderValue()
	var rows []u904Row
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("X-Snowplow-Stage-Outcomes is not a JSON array: %v (%s)", err, raw)
	}
	byName := map[string]u904Row{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	return u904Result{dict: dict, rows: byName, raw: raw}
}

func n1288RefusedLifecycle(t *testing.T, what string, mark func(context.Context) context.Context) {
	t.Helper()
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1"}`},
	})
	res := n1288Resolve(t, u, u904Lifecycle(), mark)
	if got := u.n(http.MethodGet, "/cloudservers/s1"); got != 1 {
		t.Fatalf("SETUP: %s: the read stage was sent %d time(s), want 1 (counts=%v)", what, got, u.count)
	}
	if got := u.writes(); got != 0 {
		t.Fatalf("%s sent %d write(s) (counts=%v)", what, got, u.count)
	}
	res.wantSucceeded(t, "server")
	// The refused write, and the write that depends on it: refused for the
	// same reason, never Skipped (nothing failed).
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonWriteRefused})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonWriteRefused})
	n1288Want(t, what+" header "+res.raw, s1264Summary(t, res.raw), 0, 2)
}

func TestIssue1288_N4_AnExportsRefusedWritesAreNotExecutedNotErrorNorSkipped(t *testing.T) {
	n1288RefusedLifecycle(t, "an export", cache.WithExportRead)
}

func TestIssue1288_N5_ABackgroundResolvesRefusedWritesAreNotExecuted(t *testing.T) {
	n1288RefusedLifecycle(t, "a refresher resolve", func(ctx context.Context) context.Context {
		return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
	})
}

// N6 — every declared code is classified in the package's closed set, with the
// class the test's own table (s1264ReasonTable, which fails on a declared code
// it does not classify) gives it; the deliberate class is exactly the two
// refusals. The same classifier decides the #904 rule: a deliberate
// non-execution never blocks a dependent write, a failure always does.
func TestIssue1288_N6_TheClassIsAPropertyOfTheReason(t *testing.T) {
	var deliberate []string
	for n, c := range s1264ReasonTable(t) {
		if _, ok := stageReasonClasses[c.code]; !ok {
			t.Errorf("%s (%q) has no class in stageReasonClasses", n, c.code)
		}
		if got := stageNotExecutedByDesign(c.code); got != c.notExecuted {
			t.Errorf("stageNotExecutedByDesign(%s) = %v, want %v", n, got, c.notExecuted)
		}
		o := newStageOutcomes()
		o.register([]string{"dep"})
		if c.code == StageReasonSkipped {
			o.skip("dep", "before")
		} else {
			o.fail("dep", c.code)
		}
		if got := o.blocksDependentWrite("dep"); got == c.notExecuted {
			t.Errorf("a stage that reads %s: blocksDependentWrite = %v, want %v", n, got, !c.notExecuted)
		}
		if c.notExecuted {
			deliberate = append(deliberate, c.code)
		}
	}
	if len(deliberate) != 2 {
		t.Fatalf("deliberate codes %q, want exactly StageNotExecuted and WriteRefused", deliberate)
	}
	if stageNotExecutedByDesign("") || stageNotExecutedByDesign(StageReasonOther) {
		t.Fatal("an empty or unknown reason must never read as a deliberate non-execution")
	}

	// Review F1 — the rank map. Every classified code that fail() records must
	// be ranked (a classified code missing there is folded to Error by fail(),
	// so it could never reach the header), with a positive rank (0 is a stage
	// that recorded nothing). The orchestrator's own codes (recorded by skip(),
	// HeaderValue, abortedResolve: never by fail()) must NOT be ranked, so no
	// item can record them. And WriteRefused ranks below every failure code.
	orchestrator := map[string]bool{StageReasonSkipped: true, StageReasonNotRun: true, StageReasonAborted: true}
	for n, c := range s1264ReasonTable(t) {
		rank, ranked := reasonRank[c.code]
		if orchestrator[c.code] {
			if ranked {
				t.Errorf("%s is recorded only by the orchestrator, yet ranked %d: fail() could record it", n, rank)
			}
			continue
		}
		if !ranked || rank <= 0 {
			t.Errorf("%s (%q) is classified but has rank %d (ranked=%v): fail() would fold it to Error", n, c.code, rank, ranked)
			continue
		}
		o := newStageOutcomes()
		o.register([]string{"s"})
		o.fail("s", c.code)
		if got := o.reasonOf("s"); got != c.code {
			t.Errorf("fail(%s) recorded %q, want %q", n, got, c.code)
		}
		if !c.notExecuted && rank <= reasonRank[StageReasonWriteRefused] {
			t.Errorf("failure %s ranks %d, not above WriteRefused's %d: a mixed stage would hide it", n, rank, reasonRank[StageReasonWriteRefused])
		}
	}
	for code := range reasonRank {
		if _, ok := stageReasonClasses[code]; !ok {
			t.Errorf("ranked code %q has no class", code)
		}
	}
}

// n1288Marks is the export and the refresher mode, each a non-customer resolve.
var n1288Marks = []struct {
	name string
	mark func(context.Context) context.Context
}{
	{"export", cache.WithExportRead},
	{"refresher", func(ctx context.Context) context.Context {
		return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
	}},
}

// N7 (review F1) — a write stage whose iterator yields one item the plan
// refuses (an empty id renders an empty path segment: InvalidPath) and one
// valid item (refused by the export / background rule) FAILED: it reads
// InvalidPath, counts in failed, and blocks its dependent write (Skipped).
func TestIssue1288_N7_AMixedStageReadsItsFailureNotWriteRefused(t *testing.T) {
	for _, m := range n1288Marks {
		t.Run(m.name, func(t *testing.T) {
			u := u904NewUpstream(t, map[string]u904Route{
				"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1","volumes":[{"id":"v1"},{"id":""}]}`},
			})
			res := n1288Resolve(t, u, u904IteratorLifecycle(), m.mark)
			if got := u.writes(); got != 0 {
				t.Fatalf("%s sent %d write(s) (counts=%v)", m.name, got, u.count)
			}
			res.wantSucceeded(t, "server")
			res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonInvalidPath})
			res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "detach"})
			n1288Want(t, m.name+" header "+res.raw, s1264Summary(t, res.raw), 2, 0,
				StageReasonInvalidPath, StageReasonSkipped)
		})
	}
}

// N8 (review F2) — a write that ITERATES the refused stage's output has zero
// items: nothing to send, so it succeeds and is not listed (the #904 zero-item
// rule). It reads neither WriteRefused nor Skipped, and nothing is sent.
func TestIssue1288_N8_AnIteratorOverARefusedStageHasNothingToSend(t *testing.T) {
	for _, m := range n1288Marks {
		t.Run(m.name, func(t *testing.T) {
			u := u904NewUpstream(t, map[string]u904Route{
				"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1"}`},
			})
			remove := u904Stage("remove", http.MethodDelete, `${ "/cloudservers/s1/items/" + .id }`)
			remove.DependsOn = &templates.Dependency{Name: "detach", Iterator: ptr.To(".detach.items")}
			stages := []*templates.API{
				u904Stage("server", http.MethodGet, "/cloudservers/s1"),
				u904After(u904Stage("detach", http.MethodPost, "/cloudservers/s1/detach"), "server"),
				remove,
			}
			res := n1288Resolve(t, u, stages, m.mark)
			if got := u.writes(); got != 0 {
				t.Fatalf("%s sent %d write(s) (counts=%v)", m.name, got, u.count)
			}
			res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonWriteRefused})
			res.wantSucceeded(t, "server", "remove")
			n1288Want(t, m.name+" header "+res.raw, s1264Summary(t, res.raw), 0, 1)
		})
	}
}
