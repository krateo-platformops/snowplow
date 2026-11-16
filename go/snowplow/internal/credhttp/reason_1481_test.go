package credhttp

// reason_1481_test.go — #1481 (plumbing#195): the reason snowplow records for a
// failure's code, at credhttp's edge.
//
//   R  ReasonForCode per code: a 502 (and every other 5xx but 500, 503 and
//      504) has none, never plumbing's InternalError; 504, 429, 503 and 500
//      keep the reason of their own code.
//   D  credhttp.Do (the /call passthrough) answers 502, 503, 504 and 429 with
//      that reason, for every body that is not a Status, and a refused
//      redirect with 502 and no reason.
//
// The expected reasons are literals: none is computed from the function under
// test.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIssue1481_R_ReasonForCode(t *testing.T) {
	for _, tc := range []struct {
		code int
		want string
	}{
		{http.StatusBadGateway, ""},
		{http.StatusServiceUnavailable, "ServiceUnavailable"},
		{http.StatusGatewayTimeout, "Timeout"},
		{http.StatusTooManyRequests, "TooManyRequests"},
		{http.StatusInternalServerError, "InternalError"},
		{http.StatusNotImplemented, ""}, // plumbing says Invalid, 422's reason
		{http.StatusHTTPVersionNotSupported, ""},
		{http.StatusInsufficientStorage, ""},
		{599, ""},
		{http.StatusNotFound, "NotFound"},
		{http.StatusForbidden, "Forbidden"},
		{http.StatusUnprocessableEntity, "UnprocessableEntity"}, // not a Kubernetes reason: kept
		{http.StatusFound, ""},
		{http.StatusTeapot, ""},
	} {
		if got := string(ReasonForCode(tc.code)); got != tc.want {
			t.Errorf("RED: ReasonForCode(%d) = %q, want %q", tc.code, got, tc.want)
		}
		st := FailureStatus(tc.code, nil)
		if st.Status != response.StatusFailure || st.Code != tc.code || string(st.Reason) != tc.want {
			t.Errorf("RED: FailureStatus(%d) = status %q code %d reason %q, want Failure %d %q",
				tc.code, st.Status, st.Code, st.Reason, tc.code, tc.want)
		}
	}
	// snowplow's own reason, which documents no Kubernetes code, survives.
	if got := ReasonAgreeing(DestinationRefusedReason, http.StatusBadGateway); got != DestinationRefusedReason {
		t.Errorf("RED: ReasonAgreeing dropped %q at 502: %q", DestinationRefusedReason, got)
	}
}

// x1481Classes is what apierrors' predicates say of an envelope, as a
// classifier reading it would see it.
func x1481Classes(st *response.Status) (internal, unavailable, timeout, throttled bool) {
	err := &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure,
		Code: int32(st.Code), Reason: metav1.StatusReason(st.Reason), Message: st.Message}}
	return apierrors.IsInternalError(err), apierrors.IsServiceUnavailable(err), apierrors.IsTimeout(err), apierrors.IsTooManyRequests(err)
}

func TestIssue1481_D_DoAnswersEachCodeWithItsOwnReason(t *testing.T) {
	type want struct {
		reason                                    string
		internal, unavailable, timeout, throttled bool
	}
	wants := map[int]want{
		http.StatusBadGateway:         {"", false, false, false, false},
		http.StatusServiceUnavailable: {"ServiceUnavailable", false, true, false, false},
		http.StatusGatewayTimeout:     {"Timeout", false, false, true, false},
		http.StatusTooManyRequests:    {"TooManyRequests", false, false, false, true},
	}
	bodies := map[string]struct{ ct, body string }{
		"empty":           {"", ""},
		"text":            {"text/plain", "upstream failed"},
		"html":            {"text/html", "<html><body>Bad Gateway</body></html>"},
		"json not Status": {"application/json", `{"error":"upstream failed"}`},
	}
	for code, w := range wants {
		for name, b := range bodies {
			t.Run(http.StatusText(code)+"/"+name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
					if b.ct != "" {
						rw.Header().Set("Content-Type", b.ct)
					}
					rw.WriteHeader(code)
					_, _ = rw.Write([]byte(b.body))
				}))
				t.Cleanup(srv.Close)
				// A write: sent once, so the status line is the answer (a read's
				// retry client would back off on a 5xx and a 429).
				st := Do(context.Background(), request.RequestOptions{
					RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/default/configmaps/cm-x", Verb: ptr.To(http.MethodPut)},
					Endpoint:    &endpoints.Endpoint{ServerURL: srv.URL, Token: "t"},
				})
				if st.Status != response.StatusFailure || st.Code != code {
					t.Fatalf("PRE: Do answered status %q code %d, want a Failure %d", st.Status, st.Code, code)
				}
				if string(st.Reason) != w.reason {
					t.Errorf("RED: Do answered %d with reason %q, want %q", code, st.Reason, w.reason)
				}
				in, un, to, th := x1481Classes(st)
				if in != w.internal || un != w.unavailable || to != w.timeout || th != w.throttled {
					t.Errorf("RED: %d reads IsInternalError=%v IsServiceUnavailable=%v IsTimeout=%v IsTooManyRequests=%v, want %v %v %v %v",
						code, in, un, to, th, w.internal, w.unavailable, w.timeout, w.throttled)
				}
			})
		}
	}

	// A refused redirect is 502 and, like any 502, carries no reason: it fails
	// the same way on every retry, never a transient InternalError.
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Location", "/elsewhere")
		rw.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	st := Do(context.Background(), request.RequestOptions{
		RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/default/configmaps/cm-x", Verb: ptr.To(http.MethodGet)},
		Endpoint:    &endpoints.Endpoint{ServerURL: srv.URL, Token: "t"},
	})
	if st.Status != response.StatusFailure || st.Code != http.StatusBadGateway {
		t.Fatalf("PRE: the refused redirect answered status %q code %d", st.Status, st.Code)
	}
	if st.Reason != "" {
		t.Errorf("RED: the refused redirect carries reason %q, want none", st.Reason)
	}
	if in, _, _, _ := x1481Classes(st); in {
		t.Error("RED: the refused redirect reads IsInternalError")
	}
}
