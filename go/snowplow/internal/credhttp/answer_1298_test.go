package credhttp

// answer_1298_test.go — #1298 review: what Send returned becomes the record's
// outcome, code and body (Answer.Outcome), never the envelope's code.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func TestAnswer1298_OutcomeOfWhatSendReturned(t *testing.T) {
	denied := &url1298Err{err: &dialguard.ErrDenied{}}
	for _, c := range []struct {
		name        string
		resp        *http.Response
		err         error
		note        string
		wantOutcome string
		wantCode    int
		wantBody    string
	}{
		{"201", &http.Response{StatusCode: 201}, nil, "", "success", 201, ""},
		{"201 body not used", &http.Response{StatusCode: 201}, nil, audit.NoteBodyNotJSON, "success", 201, audit.NoteBodyNotJSON},
		{"302", &http.Response{StatusCode: 302}, nil, "", "failure", 302, "snowplow refused to follow a 302 Found redirect"},
		{"403", &http.Response{StatusCode: 403}, nil, "", "failure", 403, "upstream answered 403 Forbidden"},
		{"transport error", nil, errors.New("connect: connection refused"), "", "failure", 0, audit.MessageNoResponse},
		{"destination refused", nil, denied, "", "failure", 0, audit.MessageDestinationRefused},
		{"budget not sent", nil, &pagebudget.ExhaustedError{Result: pagebudget.NotSent}, "", "failure", 0, audit.MessageBudgetNotSent},
		{"budget while connecting", nil, &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedConnect}, "", "failure", 0, audit.MessageBudgetNotSent},
		{"budget awaiting the answer", nil, &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedAnswer}, "", "failure", 0, audit.MessageBudgetNoAnswer},
		{"budget after a written write", nil, &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedAnswer, MayHaveBeenApplied: true}, "", "failure", 0, audit.MessageBudgetMayBeApplied},
		{"budget, wrapped", nil, fmt.Errorf("post: %w", &pagebudget.ExhaustedError{Result: pagebudget.NotSent}), "", "failure", 0, audit.MessageBudgetNotSent},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, a := WithAnswer(context.Background())
			a.observe(c.resp, c.err)
			a.Note(c.note)
			o, code, body := a.Outcome()
			if o != c.wantOutcome || code != c.wantCode || body != c.wantBody {
				t.Errorf("Outcome() = (%q, %d, %q), want (%q, %d, %q)", o, code, body, c.wantOutcome, c.wantCode, c.wantBody)
			}
		})
	}
	t.Run("no observer", func(t *testing.T) {
		var a *Answer
		a.observe(&http.Response{StatusCode: 201}, nil) // nil-safe
		a.Note(audit.NoteBodyUnread)
		if o, code, body := a.Outcome(); o != "failure" || code != 0 || body != audit.MessageNoResponse {
			t.Errorf("nil Answer Outcome() = (%q, %d, %q)", o, code, body)
		}
	})
}

// Send records on the request's Answer: the status line it returned, through
// the real send policy.
func TestAnswer1298_SendRecordsTheStatusLine(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	ctx, a := WithAnswer(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	resp, err := Send(SendSiteCall, Client(srv.Client()), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if o, code, _ := a.Outcome(); o != "failure" || code != http.StatusConflict {
		t.Errorf("RED #1298: Send did not record the answer: (%q, %d)", o, code)
	}
	if AnswerFrom(context.Background()) != nil {
		t.Error("a ctx without an observer returned one")
	}
}

// url1298Err wraps an error as net/http does a transport failure.
type url1298Err struct{ err error }

func (e *url1298Err) Error() string { return "Post: " + e.err.Error() }
func (e *url1298Err) Unwrap() error { return e.err }
