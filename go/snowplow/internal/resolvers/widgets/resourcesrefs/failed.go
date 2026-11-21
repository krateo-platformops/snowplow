// failed.go — #1564: a resourcesRef that cannot be resolved is a FAILURE
// ENTRY, never an absence.
//
// THE DEFECT. Resolve joined a failed ref's error into a value it then
// discarded (`return results, nil`): the ref vanished from
// status.resourcesRefs, the widget answered 200, and nothing was logged. The
// portal looks a child up by id, so a vanished ref read exactly as "this
// widget has no such child": in agentiko-sandbox run 14 an app-shell sider
// plausibly disappeared that way, and with it the Menu.
//
// A REF FAILS in one of three classes (FailedRef*):
//   - api_version_invalid: its apiVersion does not parse;
//   - type_not_installed: discovery says the apiserver does not serve its type
//     (cache.KindNotServedError: the group/version's document is a 404, or lists
//     no such resource). Only that ref fails;
//   - discovery_failed: discovery could not answer at all (unreachable,
//     refused, the caller's credential). Every ref whose kind is not already
//     known in-process fails the same way on that resolve.
//
// WHAT A FAILURE PRODUCES, for every class:
//   - in status, one entry per verb the ref declares, as a success would
//     have: its id and verb, allowed:false (no action is possible: there is
//     nothing to act on) and NO path, so no reader can fetch it as if it were
//     a working child; and `error`, the same k8s-style Status a widget /call
//     answers for a failed dependency (docs/howto-frontend-widget-dependency-
//     status.md). type_not_installed is the TypeNotInstalled 424 itself
//     (statusdetails); discovery_failed keeps discovery's own HTTP code when
//     it had one (401, 403, 429, 5xx), else 500; api_version_invalid is 500,
//     as a resolve failure with no HTTP status is. The message is built from
//     the reference's coordinates alone: no apiserver text, no identity;
//   - one log line with the class and the redacted error: WARN when a customer
//     is resolving (the serve origin), Debug when a background driver is (the
//     #563 rule, cache.RefDenialOrigin), so a broken ref cannot flood the log
//     from every refresh;
//   - one count in snowplow_resourcesrefs_failed_total{reason}, every origin;
//   - one bump of the resolve's stage-error sink, so the partial widget is
//     SERVED but never PERSISTED (#313's Put-gate): a transient discovery
//     failure is not pinned in L1 for that identity, and the next resolve
//     retries it. cache.KindForGVR keeps only successes, so nothing else
//     remembers the failure either.
package resourcesrefs

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/statusdetails"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The closed set of failure classes: the reason label of
// snowplow_resourcesrefs_failed_total and of the log line.
const (
	FailedRefAPIVersionInvalid = "api_version_invalid"
	FailedRefTypeNotInstalled  = "type_not_installed"
	FailedRefDiscoveryFailed   = "discovery_failed"
)

// FailedRefClasses is the closed set, in emit order.
var FailedRefClasses = []string{FailedRefAPIVersionInvalid, FailedRefTypeNotInstalled, FailedRefDiscoveryFailed}

// refError is resolveOne's failure: its class, the GVR when the apiVersion
// parsed, and the underlying error (logged redacted, never put in status).
type refError struct {
	class string
	gvr   schema.GroupVersionResource
	err   error
}

func (e *refError) Error() string { return e.err.Error() }
func (e *refError) Unwrap() error { return e.err }

// failedRef turns one ref's failure into its failure entries, and logs,
// counts and bumps the stage-error sink once for it.
func failedRef(ctx context.Context, in *templatesv1.ResourceRef, err error) []templatesv1.ResourceRefResult {
	var re *refError
	if !errors.As(err, &re) {
		// Every resolveOne error is a *refError; an unclassified one is a
		// discovery-side failure by construction (it came after the parse).
		re = &refError{class: FailedRefDiscoveryFailed, err: err}
	}
	recordFailedRef(re.class)
	cache.StageErrorSinkFromContext(ctx).Bump("resourcesRefs", "resource ref failed: "+re.class)

	log := xcontext.Logger(ctx)
	if origin := cache.RefDenialOrigin(ctx); origin == cache.RefDenialOriginServe {
		log.WarnContext(ctx, "resource ref failed; it is a failure entry in status.resourcesRefs",
			slog.String("id", in.ID),
			slog.String("reason", re.class),
			slog.String("apiVersion", in.APIVersion),
			slog.String("resource", in.Resource),
			slog.String("namespace", in.Namespace),
			slog.String("err", redact.Err(re.err)))
	} else {
		log.DebugContext(ctx, "resource ref failed; it is a failure entry in status.resourcesRefs",
			slog.String("id", in.ID),
			slog.String("reason", re.class),
			slog.String("apiVersion", in.APIVersion),
			slog.String("resource", in.Resource),
			slog.String("namespace", in.Namespace),
			slog.String("origin", origin),
			slog.String("err", redact.Err(re.err)))
	}

	st := failureStatus(re)
	out := []templatesv1.ResourceRefResult{}
	for _, verb := range mapVerbs(in.Verb) {
		out = append(out, templatesv1.ResourceRefResult{
			ID:      in.ID,
			Verb:    kubeToREST[verb],
			Allowed: false,
			Error:   st,
		})
	}
	return out
}

// failureStatus is the entry's `error`: the dependency-status contract's
// Status for the class, with a message built from the ref's coordinates only.
func failureStatus(re *refError) *response.Status {
	switch re.class {
	case FailedRefTypeNotInstalled:
		return statusdetails.TypeNotInstalledStatus(re.gvr, statusdetails.TypeNotInstalledMessage(re.gvr))
	case FailedRefAPIVersionInvalid:
		return failure(http.StatusInternalServerError, response.StatusReasonInternalError,
			"The reference's apiVersion is not a valid group/version.")
	}
	code, reason := http.StatusInternalServerError, response.StatusReasonInternalError
	msg := fmt.Sprintf("The type %s (%s) could not be looked up.", re.gvr.Resource, re.gvr.GroupVersion())
	var apiStatus apierrors.APIStatus
	if errors.As(re.err, &apiStatus) {
		if c := int(apiStatus.Status().Code); c >= 400 {
			code = c
			reason = response.StatusReason(apiStatus.Status().Reason)
			msg = fmt.Sprintf("The type %s (%s) could not be looked up: discovery answered %d.", re.gvr.Resource, re.gvr.GroupVersion(), c)
		}
	}
	return failure(code, reason, msg)
}

func failure(code int, reason response.StatusReason, msg string) *response.Status {
	st := response.New(code, nil)
	st.Status = response.StatusFailure
	st.Reason = reason
	st.Message = msg
	return st
}

var failedRefCount struct {
	apiVersionInvalid atomic.Uint64
	typeNotInstalled  atomic.Uint64
	discoveryFailed   atomic.Uint64
}

func recordFailedRef(class string) {
	switch class {
	case FailedRefAPIVersionInvalid:
		failedRefCount.apiVersionInvalid.Add(1)
	case FailedRefTypeNotInstalled:
		failedRefCount.typeNotInstalled.Add(1)
	default:
		failedRefCount.discoveryFailed.Add(1)
	}
}

// FailedRefSnapshot returns the {reason} counts.
func FailedRefSnapshot() map[string]uint64 {
	return map[string]uint64{
		FailedRefAPIVersionInvalid: failedRefCount.apiVersionInvalid.Load(),
		FailedRefTypeNotInstalled:  failedRefCount.typeNotInstalled.Load(),
		FailedRefDiscoveryFailed:   failedRefCount.discoveryFailed.Load(),
	}
}

var failedRefExpvarOnce sync.Once

func init() { registerFailedRefExpvar() }

// registerFailedRefExpvar publishes snowplow_resourcesrefs_failed_total, every
// cell present (a 0 is a reading). Ungated on the cache mode (listed in the
// CFG-1 nonCacheInitPublishers exception): every widget resolve resolves its
// refs, cache on or off.
func registerFailedRefExpvar() {
	failedRefExpvarOnce.Do(func() {
		expvar.Publish("snowplow_resourcesrefs_failed_total", expvar.Func(func() any {
			return FailedRefSnapshot()
		}))
	})
}
