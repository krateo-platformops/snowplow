package audit

// outcome_1298_test.go — the closed set of a dialled action's record body,
// pinned where it is defined (#1298 review): every branch of FailureMessage,
// and the rule that what snowplow did itself never reads as an answer.

import (
	"strings"
	"testing"
)

func TestFailureMessage1298_EveryBranch(t *testing.T) {
	for _, c := range []struct {
		code int
		want string
	}{
		{0, "no response from the endpoint"},
		{301, "snowplow refused to follow a 301 Moved Permanently redirect"},
		{302, "snowplow refused to follow a 302 Found redirect"},
		{307, "snowplow refused to follow a 307 Temporary Redirect redirect"},
		{399, "snowplow refused to follow a 399 redirect"}, // a 3xx with no standard text
		{400, "upstream answered 400 Bad Request"},
		{403, "upstream answered 403 Forbidden"},
		{404, "upstream answered 404 Not Found"},
		{409, "upstream answered 409 Conflict"},
		{500, "upstream answered 500 Internal Server Error"},
		{502, "upstream answered 502 Bad Gateway"},
		{599, "upstream answered 599"}, // no standard text: the bare code
		{499, "upstream answered 499"},
	} {
		if got := FailureMessage(c.code); got != c.want {
			t.Errorf("FailureMessage(%d) = %q, want %q", c.code, got, c.want)
		}
	}
}

// What snowplow did itself (no authority answered) never reads as an upstream
// answer, and a note on an accepted write says it was accepted.
func TestFailureMessage1298_SnowplowsOwnDoingNeverReadsAsAnAnswer(t *testing.T) {
	for _, m := range []string{
		MessageNoResponse, MessageDestinationRefused,
		MessageBudgetNotSent, MessageBudgetNoAnswer, MessageBudgetMayBeApplied,
		FailureMessage(0), FailureMessage(302),
	} {
		if strings.Contains(m, "upstream answered") {
			t.Errorf("%q reads as an upstream answer", m)
		}
	}
	for _, n := range []string{NoteBodyUnread, NoteBodyUnconvertible, NoteBodyNotJSON} {
		if !strings.HasPrefix(n, "accepted; ") {
			t.Errorf("note %q does not say the write was accepted", n)
		}
	}
}
