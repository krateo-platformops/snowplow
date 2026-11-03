package api

// issue1112_d2_undialled_write_test.go — #1112 D2: a declared write that
// snowplow refuses to send is a stage failure with a reason, never a success.
//
//   A  the #288 shape guard no longer judges an external endpoint's path: the
//      Zendesk ids that parse as invalid apiserver names are SENT.
//   B  on an own-cluster dial the guard still refuses, and the write FAILS
//      with InvalidPath (400), its errorKey and the resolve's StageFailure.
//   C  each template cause FAILS with TemplateError: a jq error in the path,
//      the payload, a header; an unrendered ${...}. Nothing is sent.
//   D  a partially refused iterator write sends the good items and FAILS.
//   E  continueOnError keeps the failure in the outcomes, no StageFailure.
//   F  a READ's skip stays silent (unchanged), and an empty-iterator write
//      still succeeds (#904 ruling).
//   G  the catch-all: a write that sent fewer items than it planned and
//      recorded nothing FAILS with NotSent.
//   H  the header row carries the name and the code only.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// x1106ErrorKeyJSON is the JSON of the value under errorKey (the dict also
// holds the request extras, which carry the caller's input by design).
func x1106ErrorKeyJSON(t *testing.T, o x1106Out, key string) string {
	t.Helper()
	v, ok := o.dict[key]
	if !ok {
		t.Fatalf("no %s in the dict: %s", key, o.dictJSON)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func TestIssue1112_D2_A_ExternalPathIsNotJudgedByTheApiserverShapeGuard(t *testing.T) {
	ep := newX1106Srv(t, false, x1106OK)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	for _, id := range []any{float64(42), "Ab_12", "user@example.com", "UPPER"} {
		before := len(ep.all())
		o := x1106Resolve(t, map[string]any{"ticketId": id, "text": x1106Body},
			x1106Write("put", http.MethodPut, `${ "/api/v2/tickets/" + (.ticketId|tostring) }`))
		if sent := len(ep.all()) - before; sent != 1 {
			t.Fatalf("RED: ticketId=%v: %d request(s) sent to the external endpoint, want 1 (outcomes %s)", id, sent, o.outcomes)
		}
		if o.outcomes != "[]" || o.failure != nil {
			t.Fatalf("ticketId=%v: the sent write succeeded: header %s failure %+v", id, o.outcomes, o.failure)
		}
	}
}

func TestIssue1112_D2_B_OwnClusterGuardRefusalFailsTheWrite(t *testing.T) {
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	o := x1106Resolve(t, map[string]any{"name": "Ab_12", "text": x1106Body},
		x1106Own("put", http.MethodPut, `${ "/api/v1/namespaces/team-a/configmaps/" + .name }`))
	if n := len(api.all()); n != 0 {
		t.Fatalf("PRE: the #288 guard must still refuse the invalid name on the apiserver: %d sent", n)
	}
	x1106WantReason(t, o.outcomes, "put", StageReasonInvalidPath)
	if o.failure == nil || o.failure.Stage != "put" || o.failure.Code != http.StatusBadRequest ||
		!strings.Contains(o.failure.Message, `write stage "put" verb PUT was not sent`) {
		t.Fatalf("the refused write must be the resolve's 400 StageFailure (#709), got %+v", o.failure)
	}
	errs, _ := o.dict["putErr"].([]any)
	if len(errs) != 1 {
		t.Fatalf("the refusal must be under the stage's errorKey, got %s", o.dictJSON)
	}
	m, _ := errs[0].(map[string]any)
	if m["reason"] != StageReasonInvalidPath || statusCodeOf(m["code"]) != http.StatusBadRequest {
		t.Fatalf("errorKey value: %v", m)
	}
	if ek := x1106ErrorKeyJSON(t, o, "putErr"); strings.Contains(ek, "Ab_12") || strings.Contains(o.outcomes, "Ab_12") ||
		strings.Contains(o.failure.Message, "Ab_12") {
		t.Fatalf("the refusal must not echo the rendered path: %s", ek)
	}
}

func TestIssue1112_D2_C_EveryTemplateCauseFailsTheWrite(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func() *templates.API
	}{
		{"path_jq_error", func() *templates.API {
			return x1106Write("w", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tonumber|tostring) }`)
		}},
		{"payload_jq_error", func() *templates.API {
			s := x1106Write("w", http.MethodPost, "/zd/tickets")
			s.Payload = ptr.To(`${ {id: (.ticketId|tonumber)} | tojson }`)
			return s
		}},
		{"header_jq_error", func() *templates.API {
			s := x1106Write("w", http.MethodPatch, "/zd/tickets/1")
			s.Headers = []string{`X-Ticket: ${ .ticketId | tonumber | tostring }`}
			return s
		}},
		{"unrendered_template", func() *templates.API {
			return x1106Write("w", http.MethodDelete, `/zd/tickets/${ .ticketId `)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := newX1106Srv(t, false, x1106OK)
			x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
			o := x1106Resolve(t, map[string]any{"ticketId": "not-a-number", "text": x1106Body}, tc.stage())
			if n := len(ep.all()); n != 0 {
				t.Fatalf("PRE: a write whose template failed is never sent: %d sent", n)
			}
			x1106WantReason(t, o.outcomes, "w", StageReasonTemplateError)
			if o.failure == nil || o.failure.Code != http.StatusBadRequest {
				t.Fatalf("want the 400 StageFailure, got %+v", o.failure)
			}
			if ek := x1106ErrorKeyJSON(t, o, "wErr"); strings.Contains(ek, "not-a-number") {
				t.Fatalf("the refusal must not echo caller input: %s", ek)
			}
		})
	}
}

func TestIssue1112_D2_D_PartiallyRefusedIteratorWriteFails(t *testing.T) {
	s := x1106Write("w", http.MethodPut, `${ "/zd/tickets/" + (.id|tonumber|tostring) }`)
	s.DependsOn = &templates.Dependency{Name: "ids", Iterator: ptr.To(`.ids`)}
	ids := x1106Stage("ids", http.MethodGet, "/zd/ids")
	ep2 := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/zd/ids" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"1"},{"id":"x y"},{"id":"3"}]`))
			return
		}
		x1106OK(w, r)
	})
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep2.srv.URL)})
	o := x1106Resolve(t, x1106Extras, ids, s)
	puts := 0
	for _, h := range ep2.all() {
		if h.Method == http.MethodPut {
			puts++
		}
	}
	if puts != 2 {
		t.Fatalf("PRE: the two good items are sent, got %d PUT(s) (outcomes %s)", puts, o.outcomes)
	}
	x1106WantReason(t, o.outcomes, "w", StageReasonTemplateError)
	if o.failure == nil || o.failure.Stage != "w" {
		t.Fatalf("a partially refused write is the resolve's StageFailure, got %+v", o.failure)
	}
}

func TestIssue1112_D2_E_ContinueOnErrorKeepsTheFailureInTheOutcomes(t *testing.T) {
	ep := newX1106Srv(t, false, x1106OK)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	s := x1106Write("w", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tonumber|tostring) }`)
	s.ContinueOnError = ptr.To(true)
	o := x1106Resolve(t, map[string]any{"ticketId": "abc", "text": x1106Body}, s)
	x1106WantReason(t, o.outcomes, "w", StageReasonTemplateError)
	if o.failure != nil {
		t.Fatalf("continueOnError: no StageFailure, got %+v", o.failure)
	}
}

func TestIssue1112_D2_F_ReadSkipSilent_EmptyIteratorWriteSucceeds(t *testing.T) {
	ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	})
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	read := x1106Stage("r", http.MethodGet, `${ "/zd/tickets/" + (.ticketId|tonumber|tostring) }`)
	o := x1106Resolve(t, map[string]any{"ticketId": "abc"}, read)
	if o.outcomes != "[]" || len(ep.all()) != 0 {
		t.Fatalf("a read's template skip is unchanged (silent, nothing sent): header %s sent %d", o.outcomes, len(ep.all()))
	}
	list := x1106Stage("list", http.MethodGet, "/zd/items")
	del := x1106Write("del", http.MethodDelete, `${ "/zd/items/" + .id }`)
	del.DependsOn = &templates.Dependency{Name: "list", Iterator: ptr.To(`.list`)}
	o = x1106Resolve(t, map[string]any{}, list, del)
	if o.outcomes != "[]" {
		t.Fatalf("an empty-iterator write has nothing to send and succeeds (#904 ruling): header %s", o.outcomes)
	}
}

// G — the catch-all, on the stage-end decision itself: no production path
// leaves a write item unsent without recording why (each refusal above records
// its own), so the arm drives failUnsentWrites with the counts it reads.
func TestIssue1112_D2_G_CatchAllFailsAnUnsentWrite(t *testing.T) {
	newRun := func() *resolveRun {
		_, sink := cache.WithStageErrorSink(context.Background())
		r := &resolveRun{log: scalarFalsifierLogger(), dict: map[string]any{}, ctx: context.Background(),
			outcomes: newStageOutcomes(), stageErrSink: sink}
		r.outcomes.register([]string{"w"})
		r.outcomes.ran("w")
		return r
	}
	sc := func(verb string, planned, sent int) *stageCtx {
		s := &stageCtx{id: "w", apiCall: &templates.API{Name: "w", Verb: ptr.To(verb), ErrorKey: ptr.To("wErr")},
			dictMu: &sync.Mutex{}, calls: make([]request.RequestOptions, planned)}
		s.sent.Store(int64(sent))
		return s
	}
	r := newRun()
	r.failUnsentWrites(sc(http.MethodPost, 3, 2))
	x1106WantReason(t, r.outcomes.HeaderValue(), "w", StageReasonNotSent)
	if r.failure == nil || r.failure.Code != http.StatusInternalServerError {
		t.Fatalf("want the 500 StageFailure, got %+v", r.failure)
	}
	if r.stageErrSink.Count() == 0 {
		t.Fatal("the stage-error sink must be bumped: no Put accepts the result")
	}
	for _, tc := range []struct {
		name          string
		verb          string
		planned, sent int
	}{{"all_sent", http.MethodPost, 3, 3}, {"read", http.MethodGet, 3, 0}} {
		r := newRun()
		r.failUnsentWrites(sc(tc.verb, tc.planned, tc.sent))
		if h := r.outcomes.HeaderValue(); h != "[]" {
			t.Fatalf("%s: no failure, got %s", tc.name, h)
		}
	}
	r = newRun()
	r.outcomes.fail("w", StageReasonForbidden)
	r.failUnsentWrites(sc(http.MethodPost, 3, 0))
	x1106WantReason(t, r.outcomes.HeaderValue(), "w", StageReasonForbidden)
}

func TestIssue1112_D2_H_HeaderRowCarriesNameAndCodeOnly(t *testing.T) {
	ep := newX1106Srv(t, false, x1106OK)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	o := x1106Resolve(t, map[string]any{"ticketId": "secret-ish-input", "text": x1106Body},
		x1106Write("w", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tonumber|tostring) }`))
	if o.outcomes != `[{"name":"w","ok":false,"reason":"TemplateError"}]` {
		t.Fatalf("header: %s", o.outcomes)
	}
	x1106NoSecretIn(t, "D2 H", o, x1106Token)
	if ek := x1106ErrorKeyJSON(t, o, "wErr"); o.failure == nil || strings.Contains(o.failure.Message, "secret-ish-input") || strings.Contains(ek, "secret-ish-input") {
		t.Fatalf("the failure and the errorKey must not echo caller input: %+v %s", o.failure, ek)
	}
}
