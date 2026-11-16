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
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// trustedReason returns the upstream-claimed reason if it agrees with the HTTP
// code, else "" (the reader falls back to the code). Each Kubernetes
// StatusReason's documented code is credhttp.DocumentedCode (#1481 moved the
// table there, beside the rule for a reason derived from a code,
// credhttp.ReasonForCode); a reason that is not a Kubernetes one is dropped.
func trustedReason(claimed string, code int) string {
	if claimed == "" {
		return ""
	}
	if c, ok := credhttp.DocumentedCode(claimed); ok && c == code {
		return claimed
	}
	return ""
}
