// stage_audit.go — AUDIT-4 (krateo-platformops/installer#410): the audit record
// for a RESTAction api[] stage that writes.
//
// THE GAP. A RESTAction stage may carry any HTTP verb, and a stage whose verb
// is not a read is dialled at branch E (dispatchOneCall: every other branch
// serves GET only) on the caller's credential — the caller's own clientconfig
// for a stage with no endpointRef, the endpoint's credential otherwise. Before
// AUDIT-4 that lane emitted nothing, so "a user triggered a RESTAction that
// changed something as them" had no record, while the same change made through
// a write /call did (handlers/call.go).
//
// WHAT IS RECORDED. One record per write-verb stage DIAL — an iterator stage
// that fans out to N calls is N mutations of N targets, so it is N records. A
// write is any method that is not safe in the RFC 9110 §9.2.1 sense (GET, HEAD,
// OPTIONS, TRACE): the verb decides, never the path or the endpoint, so a POST
// that only evaluates (an access review, a render) is recorded like any other
// POST. The outcome is the answering authority's: the upstream's status code
// and message on a refusal, the same shape as the /call write record so the two
// are queryable together.
//
// WHAT IS NOT RECORDED, ON PURPOSE.
//   - A resolve with no requester. The refresher, the cohort seed and the
//     prewarm drivers re-resolve RESTActions in the background. A write stage
//     they dial was asked for by no one: recording it under the identity on the
//     ctx would attribute to a cohort's representative an action that user did
//     not take. It is also the "write that originates from a reconcile" of the
//     controller-write boundary (installer#413): that boundary records such a
//     write only when it targets *.krateo.io, changes .spec and differs from
//     the previous reconcile, and snowplow holds no previous state of a stage's
//     target to establish the last two. So the background arm emits nothing.
//   - A dry run's write stage: it is refused before any egress (#443 k) and
//     reaches no authority, so nothing was done to anything.
package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// stageAuditAction is krateo.action on a RESTAction stage record.
const stageAuditAction = "restaction"

// isSafeMethod reports whether verb is a safe (read-only) HTTP method, RFC 9110
// §9.2.1. Every other method is a write for the audit trail.
func isSafeMethod(verb string) bool {
	switch strings.ToUpper(verb) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// stageAuditEvent builds the record for one write-verb stage dial whose answer
// is res, or reports false when the dial is not one the trail records: a safe
// method, or a resolve that is not a customer request.
func stageAuditEvent(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, res *response.Status) (audit.Event, bool) {
	verb := strings.ToUpper(ptr.Deref(call.Verb, http.MethodGet))
	if isSafeMethod(verb) {
		return audit.Event{}, false
	}
	if cache.RefDenialOrigin(ctx) != cache.RefDenialOriginServe {
		return audit.Event{}, false
	}

	path, _, _ := strings.Cut(call.Path, "?")
	ev := audit.Event{
		Action:              stageAuditAction,
		Verb:                verb,
		URLPath:             path,
		RESTActionNamespace: raNamespace,
		RESTActionName:      raName,
		Stage:               apiCall.Name,
	}
	if call.Endpoint != nil {
		if u, err := url.Parse(call.Endpoint.ServerURL); err == nil {
			ev.ServerAddress = u.Host // never userinfo, never the query
		}
	}
	// A stage with no endpointRef dials the caller's own clientconfig, which is
	// the Kubernetes apiserver: its path names the object acted on. A named
	// endpoint may be any HTTP API, so its path is recorded as a path only.
	if apiCall.EndpointRef == nil && !cache.ServiceAccountDialFromContext(ctx) {
		if gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path); ok {
			ev.Group, ev.Version, ev.Resource = gvr.Group, gvr.Version, gvr.Resource
			ev.Namespace, ev.Name = ns, name
			ev.Subresource = apiServerSubresource(path)
		}
	}

	ev.Outcome, ev.Code = "success", http.StatusOK
	if res == nil || res.Status == response.StatusFailure {
		ev.Outcome, ev.Code = "failure", http.StatusInternalServerError
		if res != nil {
			if res.Code != 0 {
				ev.Code = res.Code
			}
			ev.Message = res.Message
		}
	} else if res.Code != 0 {
		ev.Code = res.Code
	}
	return ev, true
}

// auditStageWrite emits the record for one dialled stage (see stageAuditEvent).
func auditStageWrite(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, res *response.Status) {
	if ev, ok := stageAuditEvent(ctx, raNamespace, raName, apiCall, call, res); ok {
		audit.Emit(ctx, ev)
	}
}

// apiServerSubresource returns the subresource an apiserver object path
// addresses (".../<name>/status" -> "status"), or "" for a whole object or a
// collection. ParseAPIServerPathToDep attributes a subresource to its parent
// and drops it; the record keeps it, as the /call write record does. The
// segment positions are the parser's shapes (apicall.ParsePath):
//
//	/api/<v>/[namespaces/<ns>/]<resource>/<name>/<subresource...>
//	/apis/<g>/<v>/[namespaces/<ns>/]<resource>/<name>/<subresource...>
func apiServerSubresource(path string) string {
	path = strings.TrimRight(path, "/")
	var parts []string
	nsAt := 0 // index of the optional "namespaces" segment = index of <resource>
	switch {
	case strings.HasPrefix(path, "/api/"):
		parts = strings.Split(strings.TrimPrefix(path, "/api/"), "/")
		nsAt = 1 // after <v>
	case strings.HasPrefix(path, "/apis/"):
		parts = strings.Split(strings.TrimPrefix(path, "/apis/"), "/")
		nsAt = 2 // after <g>/<v>
	default:
		return ""
	}
	nameAt := nsAt + 1 // <resource>/<name>
	// The parser's namespaced condition: "namespaces" followed by <ns> and <resource>.
	if len(parts) > nsAt && parts[nsAt] == "namespaces" && len(parts) >= nsAt+3 {
		nameAt = nsAt + 3 // namespaces/<ns>/<resource>/<name>
	}
	if len(parts) <= nameAt+1 {
		return ""
	}
	return strings.Join(parts[nameAt+1:], "/")
}
