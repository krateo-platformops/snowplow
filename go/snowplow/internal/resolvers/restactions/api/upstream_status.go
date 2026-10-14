// upstream_status.go — #714: an upstream's Status body may not steer snowplow's
// own error classification.
//
// THE DEFECT. A non-2xx answer whose body is Status-shaped JSON was taken at its
// word: its `code` and `reason` became the recorded Status. An endpoint that
// answers HTTP 500 with {"reason":"Forbidden"} (or {"code":403}) then read as a
// forbidden everywhere snowplow classifies a failure:
//   - the refresher (ClassifyRefreshError / IsNonRetryableRefreshError): a 500
//     became a non-retryable forbidden — dropped on the first attempt;
//   - the seed (classifySeedErr): an RBAC deny — never re-enqueued;
//   - the inline stage-outcomes header (stageReasonOf): reported "Forbidden".
// apierrors.Is* match the reason FIRST, whatever the code, and #709's
// StageFailure carries the recorded reason into a typed StatusError.
//
// THE RULE, applied where an upstream body becomes a Status (nonOKStatus), for
// every endpoint alike:
//   - the code is the HTTP status line's, always — a body cannot claim another;
//   - a reason is kept only when it is a Kubernetes StatusReason whose
//     documented code IS that code (metav1 types.go); any other non-empty
//     reason is dropped, and every reader falls back to the code;
//   - an empty reason stays empty: a reason is never invented. A 403 without
//     reason=Forbidden is still not the apiserver's per-identity denial
//     (#681 F2).
// For the Kubernetes apiserver the rule is a no-op: its body's code and reason
// always agree with the status line.

package api

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reasonCodes is each Kubernetes StatusReason's documented HTTP code
// (k8s.io/apimachinery/pkg/apis/meta/v1/types.go).
var reasonCodes = map[metav1.StatusReason]int{
	metav1.StatusReasonUnauthorized:          401,
	metav1.StatusReasonForbidden:             403,
	metav1.StatusReasonNotFound:              404,
	metav1.StatusReasonAlreadyExists:         409,
	metav1.StatusReasonConflict:              409,
	metav1.StatusReasonGone:                  410,
	metav1.StatusReasonExpired:               410,
	metav1.StatusReasonInvalid:               422,
	metav1.StatusReasonServerTimeout:         500,
	metav1.StatusReasonStoreReadError:        500,
	metav1.StatusReasonInternalError:         500,
	metav1.StatusReasonTimeout:               504,
	metav1.StatusReasonTooManyRequests:       429,
	metav1.StatusReasonBadRequest:            400,
	metav1.StatusReasonMethodNotAllowed:      405,
	metav1.StatusReasonNotAcceptable:         406,
	metav1.StatusReasonRequestEntityTooLarge: 413,
	metav1.StatusReasonUnsupportedMediaType:  415,
	metav1.StatusReasonServiceUnavailable:    503,
}

// trustedReason returns the upstream-claimed reason if it agrees with the HTTP
// code, else "" (the reader falls back to the code).
func trustedReason(claimed string, code int) string {
	if claimed == "" {
		return ""
	}
	if c, ok := reasonCodes[metav1.StatusReason(claimed)]; ok && c == code {
		return claimed
	}
	return ""
}
