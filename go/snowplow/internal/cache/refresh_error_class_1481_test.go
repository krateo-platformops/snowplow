package cache

// refresh_error_class_1481_test.go — #1481: no 5xx reads as "we don't know"
// (#608), and none is read as a 500's InternalError when its code says
// otherwise. A 502 is bad_gateway, snowplow's own refused redirect
// redirect_refused, another 5xx without a class of its own server_error; 500,
// 503, 504 and 429 keep theirs. Retry behaviour is unchanged: only forbidden
// and destination_refused are non-retryable.

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func x1481Status(code int, reason string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure,
		Code: int32(code), Reason: metav1.StatusReason(reason), Message: `restaction ns/ra stage "s" failed: x`}}
}

func TestIssue1481_RefreshClassPerCode(t *testing.T) {
	generic := func(code int) error {
		return apierrors.NewGenericServerResponse(code, http.MethodGet, schema.GroupResource{Resource: "configmaps"}, "cm", "upstream failed", 0, false)
	}
	for _, tc := range []struct {
		name         string
		err          error
		want         string
		nonRetryable bool
	}{
		{"502, no reason", x1481Status(502, ""), RefreshErrBadGateway, false},
		{"502, client-go's InternalError (the internal-rest-config branch)", generic(502), RefreshErrBadGateway, false},
		{"502, a reason that is not Kubernetes'", x1481Status(502, "BadGateway"), RefreshErrBadGateway, false},
		{"502, snowplow's refused redirect", x1481Status(502, credhttp.RedirectRefusedReason), RefreshErrRedirectRefused, false},
		{"502, snowplow's refused destination", x1481Status(502, credhttp.DestinationRefusedReason), RefreshErrDestinationRefused, true},
		{"501, no reason", x1481Status(501, ""), RefreshErrServerError, false},
		{"505, client-go's InternalError", generic(505), RefreshErrServerError, false},
		{"508, no reason", x1481Status(508, ""), RefreshErrServerError, false},
		{"599, no reason", x1481Status(599, ""), RefreshErrServerError, false},
		{"500 InternalError", x1481Status(500, "InternalError"), RefreshErrUnavailable, false},
		{"500, client-go", generic(500), RefreshErrUnavailable, false},
		{"503 ServiceUnavailable", x1481Status(503, "ServiceUnavailable"), RefreshErrUnavailable, false},
		{"503, client-go", generic(503), RefreshErrUnavailable, false},
		{"504 Timeout", x1481Status(504, "Timeout"), RefreshErrTimeout, false},
		{"504, no reason", x1481Status(504, ""), RefreshErrTimeout, false},
		{"429 TooManyRequests", x1481Status(429, "TooManyRequests"), RefreshErrUnavailable, false},
		{"429, no reason", x1481Status(429, ""), RefreshErrUnavailable, false},
		// The code is read through every wrap, as the reasons are.
		{"502 wrapped twice", fmt.Errorf("resolveAndPopulateL1 widgets/x: %w",
			WrapResolveStage(ResolveStageApiRef, "apiref x/y", x1481Status(502, ""))), RefreshErrBadGateway, false},
		// An error with no status has no code: still not a server class.
		{"plain error", errors.New("boom"), RefreshErrUnclassified, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := ClassifyRefreshError(tc.err); got != tc.want {
				t.Errorf("RED: ClassifyRefreshError = %q, want %q", got, tc.want)
			}
			if got := IsNonRetryableRefreshError(tc.err); got != tc.nonRetryable {
				t.Errorf("RED: IsNonRetryableRefreshError = %v, want %v (retry behaviour must not change)", got, tc.nonRetryable)
			}
		})
	}
}

// The new classes are in the closed set: each is its own series, never folded
// into unclassified.
func TestIssue1481_NewClassesAreInTheClosedSet(t *testing.T) {
	ResetRefreshFailedByClassForTest()
	t.Cleanup(ResetRefreshFailedByClassForTest)
	for _, c := range []string{RefreshErrBadGateway, RefreshErrRedirectRefused, RefreshErrServerError} {
		recordRefreshFailedClass(c, ResolveStageApiRef)
	}
	got := map[string]uint64{}
	for _, c := range RefreshFailedByClassSnapshot() {
		got[c.Class+"/"+c.Stage] = c.N
	}
	want := map[string]uint64{"bad_gateway/apiref": 1, "redirect_refused/apiref": 1, "server_error/apiref": 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("RED: by-class snapshot = %v, want %v", got, want)
	}
}
