package dispatchers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// reasonFailedDependency is the Status reason of a 424 answer. Neither
// plumbing nor apimachinery names one for 424.
const reasonFailedDependency response.StatusReason = "FailedDependency"

// encodeWidgetResolveError answers a widget /call whose CR was read but whose
// resolve failed — the apiRef RESTAction, one of its stages, a nested resolve,
// or the status validation.
//
// #853 — a 404 means the WIDGET is not there, and only fetchObject (the CR
// read, before any resolve) answers it. A 404 or 410 from a dependency is
// answered 424 Failed Dependency, with the dependency's message: the widget
// exists, something it reads does not. Passed through, the dependency's 404
// was indistinguishable from a missing widget, and the SPA retries a 404 as a
// cold informer (useWidgetQuery shouldRetryWidgetFetch: 3 retries, ~4.9 s per
// load) before rendering it as "Not found". The #709 /agents/new widget on an
// install without kagent took ~6 s on every load that way. 424 is a 4xx other
// than 404, which the SPA does not retry and renders as the widget's error
// state with this message and a manual Retry.
//
// Every other code keeps its meaning and is passed through unchanged: 401
// (the session-resume flow), 403 (the calm "not available to you" state), 429
// and 5xx (transient; the SPA retries them, and 503/504 show the timeout
// state), and 400 (a status that fails its schema). A non-Status error stays
// a 500.
//
// One rule for every widget kind and every dependency; it changes the answer
// only. Nothing on this path is cached: the handler returns before any Put.
func encodeWidgetResolveError(wri http.ResponseWriter, err error) {
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		response.InternalError(wri, err)
		return
	}
	code := int(statusErr.Status().Code)
	msg := fmt.Errorf("%s", statusErr.Status().Message)
	if code == http.StatusNotFound || code == http.StatusGone {
		st := response.New(http.StatusFailedDependency, msg)
		st.Status = response.StatusFailure
		st.Reason = reasonFailedDependency
		response.Encode(wri, st)
		return
	}
	response.Encode(wri, response.New(code, msg))
}
