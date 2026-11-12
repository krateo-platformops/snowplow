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
// POST.
//
// A record names exactly one target (schema v1, #1291): the Kubernetes object
// a stage with no endpointRef wrote (with the apiserver's verb for its path),
// or else the host the write was sent to (server.address, with the lowercase
// method as its verb). A successful write to the apiserver also records the
// written object's uid, generation and resourceVersion, read from the
// apiserver's answer (#957, audit.RecordWrittenObject): three object
// identifiers, never the body itself. A write to a named endpoint records none
// of them. A stage whose apiserver path asks for a dry run (?dryRun=All) is
// recorded as krateo.action=restaction.dryRun, krateo.audit.dry_run=true, and
// names no written object: nothing was persisted.
//
// WHAT A RECORD NEVER HOLDS. This trail is kept for 365 days, so nothing that
// can carry a credential goes into it:
//   - the request PATH: a webhook path (Slack /services/T/B/<secret>, Discord
//     /api/webhooks/<id>/<token>, Telegram /bot<token>/…) or one built by jq from
//     an earlier stage's token is a secret in its own right. The RESTAction, its
//     stage and its target identify the action and the authority.
//   - the upstream's response BODY: an error body can echo a token. The outcome
//     is the real status code plus a short reason class from a closed set
//     (audit.FailureMessage, shared with the /call record), never upstream
//     text.
//
// THE OUTCOME is what the authority answered, observed at the dial
// (credhttp.Answer, shared with the /call record): the real status code (201
// and 204 are not reported as 200); a 2xx whose body snowplow cannot read or
// convert is still a SUCCESS — the write was accepted — with a note; a dial
// that got no response is a failure with no code, and so is one snowplow ended
// itself (its destination policy, the page budget), with that refusal's own
// message; a 3xx is the redirect snowplow refused to follow.
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
//   - A write abandoned before the dial: the stage's endpoint Secret cannot be
//     resolved, its iterator or path template fails, or the #288 malformed-dial
//     guard refuses the path. Nothing was sent; the stage error is in the
//     response and the log, not in the trail.
package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// stageAuditAction is krateo.action on a RESTAction stage record.
const stageAuditAction = "restaction"

// isSafeMethod reports whether verb is a safe (read-only) HTTP method, RFC 9110
// §9.2.1. Every other method is a write. It is THE write classifier of the
// resolver: the audit trail (#902), the background-write refusal (#903), the
// dependent-write rule (#904) and the stale-pass hold (#905) all read it
// (isWriteVerb, write_verb.go). The empty method is classified as it is sent:
// net/http sends "" as GET. #1088: it is credhttp.SafeMethod, which the
// endpoint send policy reads too (a write is sent once), so "is this a write"
// has one answer at the resolver and at the transport.
func isSafeMethod(verb string) bool {
	return credhttp.SafeMethod(verb)
}

// stageAuditEvent builds the record for one write-verb stage dial whose
// authority answered a, or reports false when the dial is not one the trail
// records: a safe method, or a resolve that is not a customer request. body is
// the 2xx response body the dial returned (nil when there is none): a write to
// the apiserver records the identity of the object it names (#957).
func stageAuditEvent(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, a *credhttp.Answer, body []byte) (audit.Event, bool) {
	method := strings.ToUpper(ptr.Deref(call.Verb, http.MethodGet))
	if isSafeMethod(method) {
		return audit.Event{}, false
	}
	if cache.RefDenialOrigin(ctx) != cache.RefDenialOriginServe {
		return audit.Event{}, false
	}

	ev := audit.Event{
		Action:              stageAuditAction,
		HTTPMethod:          method,
		RESTActionNamespace: raNamespace,
		RESTActionName:      raName,
		RESTActionStage:     apiCall.Name,
	}
	// A stage with no endpointRef dials the caller's own clientconfig, which is
	// the Kubernetes apiserver: its path names the object acted on, and the
	// object's coordinates are recorded as /call records them, with the verb
	// the apiserver derives for that path. A named endpoint may be any HTTP
	// API: nothing of its path is recorded, and the record names the host it
	// went to and the lowercase method.
	apiServerObject := false
	if apiCall.EndpointRef == nil && !cache.ServiceAccountDialFromContext(ctx) {
		path, query, hasQuery := splitPathQuery(call.Path) // as the apiserver receives it (#1073 F1)
		// #957 — a stage may ask the apiserver for a dry run in its path
		// (".../demo?dryRun=All", e.g. a validation stage). Nothing is
		// persisted: the record says so and names no written object.
		ev.DryRun = apiServerDryRun(query)
		if gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path); ok {
			ev.Group, ev.Version, ev.Resource = gvr.Group, gvr.Version, gvr.Resource
			ev.Namespace, ev.ObjectName = ns, name
			ev.Subresource = apiServerSubresource(path)
			dialled := path
			if hasQuery {
				dialled += "?" + query
			}
			ev.Verb = audit.KubernetesVerb(method, dialled)
			apiServerObject = true
		}
	}
	// #1291 — a record names exactly one target (schema v1): the Kubernetes
	// object above, or else the host the write was sent to. The host of the
	// caller's own clientconfig is the apiserver's, which the object already
	// names.
	if !apiServerObject {
		if call.Endpoint != nil {
			if u, err := url.Parse(call.Endpoint.ServerURL); err == nil {
				ev.ServerAddress = u.Host // never userinfo, a path or a query
			}
		}
		ev.Verb = audit.MethodVerb(method)
	}

	ev.Outcome, ev.StatusCode, ev.Message = a.Outcome()
	// #957 — the written object's identity accompanies its coordinates: only a
	// record that names a Kubernetes object can say which object it was. It is
	// read from the response, never from the stage's request payload.
	if apiServerObject {
		audit.RecordWrittenObject(&ev, body)
	}
	return ev, true
}

// apiServerDryRun reports whether an apiserver request with this raw query is
// a dry run, decided as the apiserver decides it (k8s.io/apimachinery
// meta/v1 zz_generated.conversion.go, k8s.io/apiserver util/dryrun.IsDryRun):
// the case-sensitive key "dryRun" is present with at least one value. Only
// "All" is accepted; any other value is refused with a 400, so such a write
// fails and is recorded as a failure anyway. Pairs that do not parse are
// skipped, as the apiserver's url.Values decoding skips them.
func apiServerDryRun(rawQuery string) bool {
	q, _ := url.ParseQuery(rawQuery)
	return len(q["dryRun"]) > 0
}

// auditStageWrite emits the record for one dialled stage (see stageAuditEvent).
func auditStageWrite(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, a *credhttp.Answer, body []byte) {
	if ev, ok := stageAuditEvent(ctx, raNamespace, raName, apiCall, call, a, body); ok {
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
