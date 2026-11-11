package credhttp

// answer.go — what the authority of one endpoint dial answered, observed at the
// dial, for the audit record only (AUDIT-4 installer#410, #1298).
//
// THE ENVELOPE IS NOT THE ANSWER. Both dials (the RESTAction stage fetch and the
// /call passthrough, Do) hand their caller a response.Status whose code is often
// one snowplow made up: 500 for a connection refused or a transport error, 502
// for a refused destination (nothing was sent) and for a refused 3xx, 504 when
// the page budget ended the exchange, 406 (Do) or 422 (the stage) for a 2xx
// whose body snowplow will not use, 200 for every 2xx. An audit record built
// from that code says "upstream answered 500" when nothing answered, or "502"
// when the upstream answered 302. The record is kept 365 days for forensics, so
// it is built from this observation instead.
//
// WHERE: Send, the one place an endpoint request leaves, records the status
// line it returned or why it returned none (the destination policy, the page
// budget, anything else = no response). The dial's own code adds a note on a
// 2xx body it did not use. A ctx without an Answer records nothing.
//
// One dial, one goroutine: written before the dial returns, read after.

import (
	"context"
	"errors"
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// Answer is what one dial's authority answered: the final status code (0 = no
// response), why snowplow got none when it was snowplow's own doing, and why an
// accepted body was not used.
type Answer struct {
	code int
	// refusal is the closed-set message (audit.Message*) of a dial snowplow
	// itself ended with no answer; "" when it was not snowplow's doing.
	refusal  string
	bodyNote string
}

type answerCtxKey struct{}

// WithAnswer returns ctx carrying a fresh observer for one dial.
func WithAnswer(ctx context.Context) (context.Context, *Answer) {
	a := &Answer{}
	return context.WithValue(ctx, answerCtxKey{}, a), a
}

// AnswerFrom returns the dial's observer on ctx, or nil when none.
func AnswerFrom(ctx context.Context) *Answer {
	a, _ := ctx.Value(answerCtxKey{}).(*Answer)
	return a
}

// Answered records the final HTTP status code. Nil-safe.
func (a *Answer) Answered(code int) {
	if a != nil {
		a.code = code
	}
}

// Note records why an accepted body was not used (audit.Note*). Nil-safe.
func (a *Answer) Note(n string) {
	if a != nil {
		a.bodyNote = n
	}
}

// observe records what a Send returned: the status line, or why there is none.
func (a *Answer) observe(resp *http.Response, err error) {
	if a == nil {
		return
	}
	if err == nil && resp != nil {
		a.code, a.refusal = resp.StatusCode, ""
		return
	}
	a.code, a.refusal = 0, sendRefusal(err)
}

// sendRefusal is the closed-set message of a Send error that was snowplow's own
// doing, or "" for any other error (nothing answered).
func sendRefusal(err error) string {
	if DestinationRefused(err) {
		return audit.MessageDestinationRefused
	}
	var e *pagebudget.ExhaustedError
	if errors.As(err, &e) {
		switch {
		case e.MayHaveBeenApplied:
			return audit.MessageBudgetMayBeApplied
		case e.Result == pagebudget.ExhaustedAnswer:
			return audit.MessageBudgetNoAnswer
		}
		return audit.MessageBudgetNotSent
	}
	return ""
}

// Outcome is the record's outcome, http.response.status_code (0 = omitted: no
// authority answered) and body, from the closed set (audit/outcome.go):
//
//   - a 2xx is a success with its real code: the write was accepted, even when
//     snowplow did not use the body (the note says so);
//   - a dial snowplow's destination policy refused was never sent: snowplow
//     itself refused, which is what the schema's denied means (#1291), so it is
//     denied, with no code (no authority answered; audit#13) and the refusal's
//     own message. The request writer's closed set of denial reasons has no
//     value for it, and a denied record without one conforms;
//   - a dial the page budget ended is a failure with no code and the budget's
//     own message: it ran out of time, nothing refused it;
//   - anything else is a failure with audit.FailureMessage of the observed code
//     (0 = no response; a 3xx = the redirect snowplow refused, which an
//     authority did answer, so it keeps its code).
//
// A nil Answer (no observer) reads as no response.
func (a *Answer) Outcome() (outcome string, code int, message string) {
	if a == nil {
		return audit.OutcomeFailure, 0, audit.FailureMessage(0)
	}
	if a.code >= 200 && a.code < 300 {
		return audit.OutcomeSuccess, a.code, a.bodyNote
	}
	if a.code == 0 && a.refusal == audit.MessageDestinationRefused {
		return audit.OutcomeDenied, 0, a.refusal
	}
	if a.code == 0 && a.refusal != "" {
		return audit.OutcomeFailure, 0, a.refusal
	}
	return audit.OutcomeFailure, a.code, audit.FailureMessage(a.code)
}
