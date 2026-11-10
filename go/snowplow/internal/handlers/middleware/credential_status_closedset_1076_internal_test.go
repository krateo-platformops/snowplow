// credential_status_closedset_1076_internal_test.go — #1076: the pre-dispatch
// cause set is CLOSED. Its members are pinned as literals, and a cause outside
// it is never written as a 401.
package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

func TestPreDispatch401_1076_TheCauseSetIsClosed(t *testing.T) {
	want := map[string]string{
		"TokenMissing":             `Bearer`,
		"TokenInvalid":             `Bearer error="invalid_token"`,
		"TokenExpired":             `Bearer error="invalid_token"`,
		"KeyNotPublished":          `Bearer error="invalid_token"`,
		"SessionCredentialMissing": `Bearer error="invalid_token"`,
		"StepUpRequired":           `Bearer error="insufficient_user_authentication"`,
	}
	var got []string
	for k, c := range preDispatchCauses {
		got = append(got, k)
		if c.challenge != want[k] {
			t.Errorf("%s challenge = %q, want %q", k, c.challenge, want[k])
		}
		if c.message == "" {
			t.Errorf("%s has no human sentence", k)
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "KeyNotPublished,SessionCredentialMissing,StepUpRequired,TokenExpired,TokenInvalid,TokenMissing" {
		t.Fatalf("the closed set is %v", got)
	}
	for k := range want {
		rec := httptest.NewRecorder()
		_ = preDispatchUnauthorized(rec, k, errors.New("m"))
		if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"details":{"causes":[{"reason":"`+k+`",`) ||
			rec.Result().Header.Get("WWW-Authenticate") != want[k] {
			t.Fatalf("%s: code=%d hdr=%q body=%s", k, rec.Code, rec.Result().Header.Get("WWW-Authenticate"), rec.Body.String())
		}
	}
}

// A cause outside the set is a programming error: plumbing's 500, with no
// cause and no challenge, never a 401 a client would read as "nothing was
// dispatched".
func TestPreDispatch401_1076_UnknownCauseIsNever401(t *testing.T) {
	for _, c := range []string{"", "Unauthorized", "tokenexpired", "TokenExpired "} {
		rec := httptest.NewRecorder()
		_ = preDispatchUnauthorized(rec, c, errors.New("m"))
		if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "causes") ||
			rec.Header().Get("WWW-Authenticate") != "" {
			t.Fatalf("cause %q: code=%d hdr=%q body=%s", c, rec.Code, rec.Header().Get("WWW-Authenticate"), rec.Body.String())
		}
	}
}
