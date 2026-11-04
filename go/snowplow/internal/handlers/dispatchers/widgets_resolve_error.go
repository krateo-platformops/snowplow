package dispatchers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/kubeutil"
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
// message cannot be shown: it is empty, or it names an identity. The SPA
// renders it as the subtitle under the title "Data source unavailable"
// (frontend#500), so it adds what the title does not: the detail exists and
// was withheld. It names nothing.
const dependencyFailureFallback = "The details of this data source can't be shown here."

// dependencyFailureMessage is the dependency's own message when it is safe to
// show the user, else dependencyFailureFallback.
//
// A stage failure's message is built from the request's coordinates, never
// from the upstream's text (stage_failure.go). But those coordinates include
// the object name from the stage path, and a RESTAction path can be templated
// from the caller's identity — lower-cased into a Kubernetes name or
// URL-escaped (#856 gate: `configmaps "carol.smith%40example.org" not found`).
// So the message is checked in every form a reader could decode it to — as
// sent, path-unescaped and query-unescaped (repeatedly, for double escaping),
// all case-folded — and is never shown when any form carries:
//   - one of this caller's tokens: the username, its DNS-1123 form, its local
//     part (before "@"), or any of its groups;
//   - an identity shape, whoever it belongs to (identityShapes).
func dependencyFailureMessage(ctx context.Context, msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return dependencyFailureFallback
	}
	var tokens []string
	if ui, err := xcontext.UserInfo(ctx); err == nil {
		tokens = callerIdentityTokens(ui.Username, ui.Groups)
	}
	for _, form := range decodedForms(msg) {
		for _, shape := range identityShapes {
			if shape.MatchString(form) {
				return dependencyFailureFallback
			}
		}
		for _, tok := range tokens {
			if strings.Contains(form, tok) {
				return dependencyFailureFallback
			}
		}
	}
	return msg
}

// identityShapes match, in a case-folded form, text that names or carries an
// identity or a credential no matter whose it is. A dependency message never
// needs any of them, so a false positive costs only the fixed sentence.
var identityShapes = []*regexp.Regexp{
	regexp.MustCompile(`\buser\s*"[^"]*"`),                           // the apiserver's requester: User "…"
	regexp.MustCompile(`-clientconfig\b`),                            // an authn <user>-clientconfig Secret, quoted or not, in a path or not
	regexp.MustCompile(`\bcn\s*=`),                                   // a certificate subject
	regexp.MustCompile(`\buser(name)?\s*[:=]`),                       // username: … / user=…
	regexp.MustCompile(`\b(access_|id_|refresh_)?token\s*[:=]`),      // token=… / ?token=…
	regexp.MustCompile(`\bbearer\s+\S`),                              // an Authorization value
	regexp.MustCompile(`\beyj[a-z0-9_-]+\.[a-z0-9_-]+\.[a-z0-9_-]*`), // a JWT (base64url of `{"` is eyJ)
	regexp.MustCompile(`\bsystem:[a-z]`),                             // a system: group or user
}

// decodedForms is msg case-folded, plus every distinct form path- or
// query-unescaping reaches from it (up to three rounds).
func decodedForms(msg string) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) bool {
		s = strings.ToLower(s)
		if _, dup := seen[s]; dup {
			return false
		}
		seen[s] = struct{}{}
		out = append(out, s)
		return true
	}
	add(msg)
	frontier := []string{msg}
	for round := 0; round < 3 && len(frontier) > 0; round++ {
		var next []string
		for _, f := range frontier {
			for _, unescape := range []func(string) (string, error){url.PathUnescape, url.QueryUnescape} {
				if u, err := unescape(f); err == nil && add(u) {
					next = append(next, u)
				}
			}
		}
		frontier = next
	}
	return out
}

// callerIdentityTokens is the caller's identity, case-folded: the username,
// its DNS-1123 form (how authn names the caller's objects), its local part
// before "@", and every group, system: groups included.
func callerIdentityTokens(username string, groups []string) []string {
	var out []string
	add := func(s string) {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			out = append(out, s)
		}
	}
	add(username)
	if username != "" {
		add(kubeutil.MakeDNS1123Compatible(username))
		if at := strings.IndexByte(username, '@'); at > 0 {
			add(username[:at])
		}
	}
	for _, g := range groups {
		add(g)
	}
	return out
}
