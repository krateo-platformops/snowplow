package credhttp

// reason.go — #1481: the reason snowplow records for a failure's HTTP code.
//
// THE CHANGE IT ANSWERS. Since plumbing v1.84.0 (plumbing#195) response.New
// gives a failure the reason apimachinery's NewGenericServerResponse gives the
// same code: 504 Timeout, 429 TooManyRequests, and every other 5xx, 502 among
// them, InternalError. apierrors.IsInternalError matches that reason whatever
// the code, and snowplow's classifiers read InternalError as a transient
// failure of the server: the refresher's class "unavailable"
// (cache.ClassifyRefreshError). Taken as is, every 502 snowplow reports would
// have become "unavailable", including the ones snowplow makes itself for a
// refused redirect (#726, #729), which fail the same way on every retry.
//
// THE DECISION. A reason snowplow records never names another code than the
// one it is recorded with. Each Kubernetes StatusReason documents its HTTP code
// (k8s.io/apimachinery/pkg/apis/meta/v1 types.go, reasonCodes below); a reason
// whose documented code is a different one is dropped, and every reader falls
// back to the code, as apierrors' predicates do for a reason they do not know.
// This is the rule #714 already applies to an upstream's claimed reason
// (restactions/api upstream_status.go), now applied to a reason derived from
// the code too. What it gives, per code:
//   - 502 Bad Gateway: no reason. metav1 has none for 502, and InternalError
//     documents 500. A 502 is an upstream gateway that failed, or snowplow's
//     own refusal of a redirect; neither is the server failing internally.
//     The refresher's class is "unclassified", the seed's verdict operational
//     (retried), the stage outcome "Error".
//   - 504 Timeout, 429 TooManyRequests, 503 ServiceUnavailable, 500
//     InternalError: kept. Each names its own code, so it says nothing the
//     code does not; apierrors.IsTimeout and IsTooManyRequests matched those
//     codes without a reason before.
//   - every other 5xx (505, 507, 599, ...) carries none, like 502; and 501 Not
//     Implemented no longer carries Invalid, which documents 422.
//   - a reason that is not a Kubernetes StatusReason has no documented code
//     and is kept: snowplow's own DestinationRefused (502), plumbing's
//     UnprocessableEntity (422).

import (
	"github.com/krateo-platformops/plumbing/http/response"
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

// DocumentedCode is the HTTP code Kubernetes documents for reason; ok is false
// for a reason that is not a Kubernetes StatusReason (or is empty).
func DocumentedCode(reason string) (code int, ok bool) {
	code, ok = reasonCodes[metav1.StatusReason(reason)]
	return code, ok
}

// ReasonAgreeing is reason unless it is a Kubernetes StatusReason documented
// for another code than code; then it is "" and readers fall back to the code.
func ReasonAgreeing(reason string, code int) string {
	if c, ok := DocumentedCode(reason); ok && c != code {
		return ""
	}
	return reason
}

// ReasonForCode is the reason snowplow records for a failure it knows only by
// its code: plumbing's (response.New) when that agrees with the code
// (ReasonAgreeing), else none. ReasonForCode(502) is "".
func ReasonForCode(code int) response.StatusReason {
	return response.StatusReason(ReasonAgreeing(string(response.New(code, nil).Reason), code))
}

// FailureStatus is response.New for a failure snowplow reports with code:
// always a Failure, whatever the code (a 3xx included), and its reason is
// ReasonForCode(code).
func FailureStatus(code int, err error) *response.Status {
	res := response.New(code, err)
	res.Status = response.StatusFailure
	res.Reason = ReasonForCode(code)
	return res
}
