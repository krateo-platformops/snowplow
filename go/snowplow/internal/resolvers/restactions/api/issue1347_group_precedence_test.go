package api

// issue1347_group_precedence_test.go — #1347 review blocker, lead's ruling:
// when one stage records reasons of more than one client group, the stage
// reports a reason of the highest group: cut / failed > denied > by design.
// reasonRank orders reasons only inside a group. Before, the rank alone
// decided, so a stage with one 403 item and one item cut by the page budget
// read Forbidden ("denied": the portal showed no notice although data was
// cut).
//
// THE ORACLE IS NOT THE INSTRUMENT: the groups and their order are this
// file's own table (p1347Groups, p1347Order), held equal to the package's
// map so a new code fails here until it is placed; the expected winner of
// every pair is read from that table, never from reasonOutranks or
// reasonRank.
//
//   P1 every (denied × cut / failed) pair, in both orders: the stage reads
//      the cut / failed code, in its group, on reasonOf and on the header
//      row, and it blocks a dependent write;
//   P2 every (by design × denied or cut / failed) pair, in both orders: the
//      stage reads the other code, in its group, and blocks a dependent write;
//   P3 a new code takes its group's place: a denied code ranked above every
//      code still loses to a cut / failed one;
//   P4 every group a code is placed in has a place in the order;
//   P5 the real resolve: an iterator read with a 403 item and an item the
//      page budget cuts reads Timeout on the header (and Error beside a 500).

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

const (
	p1347Cut    = "cut / failed"
	p1347Denied = "denied"
	p1347Design = "by design"
)

// p1347Order is the ruling's group order, highest first.
var p1347Order = []string{p1347Cut, p1347Denied, p1347Design}

// p1347Groups is the frontend doc's table (§3), written out here.
var p1347Groups = map[string]string{
	"Timeout": p1347Cut, "Error": p1347Cut, "NotFound": p1347Cut, "Unauthorized": p1347Cut,
	"NotRun": p1347Cut, "Skipped": p1347Cut, "Aborted": p1347Cut, "InvalidPath": p1347Cut,
	"TemplateError": p1347Cut, "NotSent": p1347Cut, "DestinationRefused": p1347Cut,
	"UserAccessFilterOnNonReadVerb": p1347Cut,
	"Forbidden":                     p1347Denied, "NotFoundForRequester": p1347Denied,
	"StageNotExecuted": p1347Design, "WriteRefused": p1347Design,
}

// p1347Orchestrator is the codes only the orchestrator writes (skip(),
// HeaderValue, abortedResolve), never an item through fail(): a stage that
// holds one of them holds no other reason, so it is in no pair.
var p1347Orchestrator = map[string]bool{"Skipped": true, "NotRun": true, "Aborted": true}

// p1347Recordable returns the codes of group g that fail() records, after
// holding the oracle to the package's maps.
func p1347Recordable(t *testing.T, g string) []string {
	t.Helper()
	if len(p1347Groups) != len(StageReasonGroups) {
		t.Fatalf("SETUP: the oracle places %d codes, StageReasonGroups %d: add the new code to p1347Groups", len(p1347Groups), len(StageReasonGroups))
	}
	for code, want := range p1347Groups {
		if got := StageReasonGroups[code]; got != want {
			t.Fatalf("SETUP: StageReasonGroups puts %s in %q, the oracle in %q", code, got, want)
		}
		if _, ranked := reasonRank[code]; ranked == p1347Orchestrator[code] {
			t.Fatalf("SETUP: %s ranked=%v, orchestrator=%v: a code fail() records is ranked, an orchestrator code is not", code, ranked, p1347Orchestrator[code])
		}
	}
	var out []string
	for code, cg := range p1347Groups {
		if cg == g && !p1347Orchestrator[code] {
			out = append(out, code)
		}
	}
	if len(out) == 0 {
		t.Fatalf("SETUP: group %q has no recordable code", g)
	}
	return out
}

func p1347Higher(a, b string) bool {
	for _, g := range p1347Order {
		if g == a {
			return true
		}
		if g == b {
			return false
		}
	}
	return false
}

// p1347Arm records first then second on one stage and asserts the stage reads
// want, in the oracle's group, on reasonOf and on the header row.
func p1347Arm(t *testing.T, first, second, want string) {
	t.Helper()
	o := newStageOutcomes()
	o.register([]string{"s"})
	o.fail("s", first)
	o.fail("s", second)
	if got := o.reasonOf("s"); got != want {
		t.Errorf("%s then %s: the stage reads %s (group %q), want %s (group %q)",
			first, second, got, StageReasonGroup(got), want, p1347Groups[want])
		return
	}
	var rows []stageOutcomeJSON
	if err := json.Unmarshal([]byte(o.HeaderValue()), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("%s then %s: header %q, want one row (%v)", first, second, o.HeaderValue(), err)
	}
	if rows[0].Reason != want || StageReasonGroup(rows[0].Reason) != p1347Groups[want] {
		t.Errorf("%s then %s: header row %+v reads group %q, want %s in %q",
			first, second, rows[0], StageReasonGroup(rows[0].Reason), want, p1347Groups[want])
	}
	if !o.blocksDependentWrite("s") {
		t.Errorf("%s then %s: the stage reads %s, yet does not block a dependent write", first, second, want)
	}
}

func TestIssue1347_P1_ACutOrFailedItemOutranksADenialInOneStage(t *testing.T) {
	n := 0
	for _, d := range p1347Recordable(t, p1347Denied) {
		for _, c := range p1347Recordable(t, p1347Cut) {
			t.Run(d+"+"+c, func(t *testing.T) {
				p1347Arm(t, d, c, c)
				p1347Arm(t, c, d, c)
			})
			n++
		}
	}
	if n != 2*9 {
		t.Fatalf("SETUP: %d (denied × cut / failed) pairs, want 18 (2 denied × 9 recordable cut / failed codes)", n)
	}
}

func TestIssue1347_P2_AByDesignReasonLosesToEveryOtherGroup(t *testing.T) {
	n := 0
	for _, b := range p1347Recordable(t, p1347Design) {
		for _, g := range []string{p1347Denied, p1347Cut} {
			if !p1347Higher(g, p1347Design) {
				t.Fatalf("SETUP: the oracle's order does not put %q above %q", g, p1347Design)
			}
			for _, other := range p1347Recordable(t, g) {
				t.Run(b+"+"+other, func(t *testing.T) {
					p1347Arm(t, b, other, other)
					p1347Arm(t, other, b, other)
				})
				n++
			}
		}
	}
	if n != 2*(2+9) {
		t.Fatalf("SETUP: %d (by design × other) pairs, want 22", n)
	}
}

// P3 — the precedence comes from the group a code is placed in: a code placed
// in "denied" with a rank above every other code still loses to Timeout.
func TestIssue1347_P3_ANewCodeTakesItsGroupsPlace(t *testing.T) {
	const future = "FutureDenial1347"
	top := 0
	for _, r := range reasonRank {
		top = max(top, r)
	}
	StageReasonGroups[future] = StageGroupDenied
	reasonRank[future] = top + 100
	p1347Groups[future] = p1347Denied
	t.Cleanup(func() { delete(StageReasonGroups, future); delete(reasonRank, future); delete(p1347Groups, future) })
	p1347Arm(t, future, StageReasonTimeout, StageReasonTimeout)
	p1347Arm(t, StageReasonTimeout, future, StageReasonTimeout)
	// And it outranks a by-design code, as its group does.
	p1347Arm(t, future, StageReasonNotExecuted, future)
}

func TestIssue1347_P4_EveryGroupHasItsPlaceInTheOrder(t *testing.T) {
	for code, g := range StageReasonGroups {
		if _, ok := stageGroupPrecedence[g]; !ok {
			t.Fatalf("P4 RED: %s is in group %q, which has no place in stageGroupPrecedence", code, g)
		}
	}
	if len(stageGroupPrecedence) != len(p1347Order) {
		t.Fatalf("P4 RED: stageGroupPrecedence orders %d groups, the ruling 3", len(stageGroupPrecedence))
	}
	for i := 1; i < len(p1347Order); i++ {
		if !(stageGroupPrecedence[p1347Order[i-1]] > stageGroupPrecedence[p1347Order[i]]) {
			t.Fatalf("P4 RED: %q must outrank %q (%v)", p1347Order[i-1], p1347Order[i], stageGroupPrecedence)
		}
	}
}

// P5 — through the real resolve: one iterator read (continueOnError) whose
// first item the upstream answers 403 and whose second item is
//   - "cut": connected and never answered, so the request's page budget ends
//     it (504 Timeout, #1115);
//   - "fail": answered 500 (Error).
//
// The header row reads the cut / failed code, whatever order the items are
// in and finish in; before the ruling it read Forbidden.
func TestIssue1347_P5_AResolvedStageWithADenialAndACutReadsTheCut(t *testing.T) {
	release := make(chan struct{})
	srv := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/deny"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","status":"Failure","code":403,"reason":"Forbidden"}`)
		case strings.HasSuffix(r.URL.Path, "/cut"):
			select { // connected, never answers: the budget ends it
			case <-release:
			case <-r.Context().Done():
			}
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"kind":"Status","status":"Failure","code":500,"reason":"InternalError"}`)
		}
	})
	t.Cleanup(func() { close(release) })
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(srv.srv.URL)})
	for _, tc := range []struct {
		items []any
		want  string
	}{
		{[]any{"deny", "cut"}, StageReasonTimeout},
		{[]any{"cut", "deny"}, StageReasonTimeout},
		{[]any{"deny", "fail"}, StageReasonError},
		{[]any{"fail", "deny"}, StageReasonError},
	} {
		before := len(srv.all())
		st := x1106Stage("each", http.MethodGet, `${ "/x/" + .p }`)
		st.ContinueOnError = ptr.To(true)
		st.DependsOn = &templates.Dependency{Name: "", Iterator: ptr.To(`[ .ps[] | {p: .} ]`)}
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: x1106User, Groups: []string{"tenants"}}))
		// Room for the 403 on a loaded CI host; the hung item is cut.
		ctx = pagebudget.WithCustomer(ctx, time.Now().Add(2*time.Second-pagebudget.PageMaxGoal))
		ctx, oc := WithStageOutcomes(ctx)
		dict, _ := ResolveWithFailure(ctx, ResolveOptions{
			RC: &rest.Config{Host: "http://127.0.0.1:1"}, AuthnNS: x1106NS, Items: []*templates.API{st},
			Extras: map[string]any{"ps": tc.items}, RESTActionNamespace: x1106NS, RESTActionName: "ra-1347",
		})
		// SETUP: both items were sent (a 500 may be retried within the budget).
		sent := map[string]bool{}
		for _, h := range srv.all()[before:] {
			sent[h.Path] = true
		}
		for _, p := range tc.items {
			if !sent["/x/"+p.(string)] {
				t.Fatalf("SETUP (%v): item %v was not sent: %+v", tc.items, p, srv.all()[before:])
			}
		}
		// PRE: the errorKey holds both items' answers, the 403 among them.
		b, _ := json.Marshal(dict["eachErr"])
		if errs, _ := dict["eachErr"].([]any); len(errs) != 2 || !strings.Contains(string(b), `"code":403`) {
			t.Fatalf("PRE (%v): eachErr = %s, want two errors, one of them the 403", tc.items, b)
		}
		raw := oc.HeaderValue()
		rows := x1106Rows(t, raw)
		if len(rows) != 1 || rows[0].Name != "each" || rows[0].Reason != tc.want || StageReasonGroup(rows[0].Reason) != p1347Cut {
			t.Fatalf("P5 RED (%v): header %s, want the one stage reading %s (%q)", tc.items, raw, tc.want, p1347Cut)
		}
	}
}
