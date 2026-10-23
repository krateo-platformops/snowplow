package dispatchers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/redact"
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
// state with this message.
//
// The CR read's own 404 never reaches here, and must not: the SPA reads a 404
// on a refresh-frame refetch as a confirmed delete (#256) and stops retrying.
//
// Every other code keeps its meaning and is passed through unchanged: 401
// (the session-resume flow), 403 (the calm "not available to you" state), 429
// and 5xx (transient; the SPA retries them, and 503/504 show the timeout
// state), and 400 (a status that fails its schema). A non-Status error stays
// a 500.
//
// The 424 message is shown to the user verbatim as the SPA's subtitle under
// "Data source unavailable", so it is product copy: dependencyFailureMessage.
//
// One rule for every widget kind and every dependency; it changes the answer
// only. Nothing on this path is cached: the handler returns before any Put.
func encodeWidgetResolveError(ctx context.Context, wri http.ResponseWriter, err error) {
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		response.InternalError(wri, err)
		return
	}
	code := int(statusErr.Status().Code)
	if code == http.StatusNotFound || code == http.StatusGone {
		st := response.New(http.StatusFailedDependency,
			errors.New(dependencyFailureMessage(ctx, statusErr.Status().Message)))
		st.Status = response.StatusFailure
		st.Reason = reasonFailedDependency
		response.Encode(wri, st)
		return
	}
	response.Encode(wri, response.New(code, fmt.Errorf("%s", statusErr.Status().Message)))
}

// dependencyFailureFallback is the 424 message when the dependency's own
// message cannot be shown: it is empty, or it names an identity.
const dependencyFailureFallback = "A data source this widget reads is not available."

// dependencyFailureMessage is the dependency's own message when it is safe to
// show the user, else dependencyFailureFallback.
//
// The messages that reach here are identity-free by construction: a stage
// failure's message is built from the request's coordinates, never from the
// upstream (stage_failure.go), and an apiserver NotFound names only the
// resource and the object. This check keeps that true for any other source.
// A message is never shown when it names a requester (the apiserver's
// `User "…"`), a `<user>-clientconfig` Secret, or this caller's username or
// groups (the same token set the learned-class log redaction uses).
func dependencyFailureMessage(ctx context.Context, msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" || redact.ErrorText(msg) != msg {
		return dependencyFailureFallback
	}
	if ui, err := xcontext.UserInfo(ctx); err == nil {
		for _, tok := range learnedRedactionTokens(ui.Username, ui.Groups) {
			if strings.Contains(msg, tok) {
				return dependencyFailureFallback
			}
		}
	}
	return msg
}
