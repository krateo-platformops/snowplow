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

	"github.com/krateo-platformops/plumbing/endpoints"
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
// records: a safe method, or a resolve with no requester (stageRecorded). body
// is the 2xx response body the dial returned (nil when there is none): a write
// to the apiserver records the identity of the object it names (#957).
func stageAuditEvent(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, a *credhttp.Answer, body []byte) (audit.Event, bool) {
	method := strings.ToUpper(ptr.Deref(call.Verb, http.MethodGet))
	if !stageRecorded(ctx, method) {
		return audit.Event{}, false
	}
	ev := stageEvent(raNamespace, raName, apiCall, method)
	apiServerObject := stageTarget(ctx, &ev, apiCall, call.Path, call.Endpoint, stageEndpointRef(ctx), true)

	// destination_refused (audit v0.12.0) when the destination policy refused
	// the dial: snowplow's own refusal.
	ev.Outcome, ev.StatusCode, ev.Message, ev.DenialReason = a.OutcomeWithReason()
	// #957 — the written object's identity accompanies its coordinates: only a
	// record that names a Kubernetes object can say which object it was. It is
	// read from the response, never from the stage's request payload.
	if apiServerObject {
		audit.RecordWrittenObject(&ev, body)
	}
	return ev, true
}

// stageRecorded reports whether a stage action with method is one the trail
// records: a write (not a safe method) on a resolve somebody asked for. Every
// non-customer origin but an export has no requester (the refresher, the
// cohort seed, the prewarm drivers, a demand revalidation, a background
// self-loopback hop): recording one under the identity on the ctx would
// attribute to a cohort's representative an action that user did not take. An
// export is the customer's own request (its writes are refused, and that
// refusal is recorded).
func stageRecorded(ctx context.Context, method string) bool {
	if isSafeMethod(method) {
		return false
	}
	requested := true
	cache.EachNonCustomerOrigin(ctx, func(o string) bool {
		if o != cache.NonCustomerExport {
			requested = false
			return false
		}
		return true
	})
	return requested
}

// stageEvent is the part of a stage record every record shares: the action,
// the method and the RESTAction stage.
func stageEvent(raNamespace, raName string, apiCall *templates.API, method string) audit.Event {
	return audit.Event{
		Action:              stageAuditAction,
		HTTPMethod:          method,
		RESTActionNamespace: raNamespace,
		RESTActionName:      raName,
		RESTActionStage:     apiCall.Name,
	}
}

// stageTarget sets ev's target and verb for a stage action of ev.HTTPMethod to
// path on ep, and reports whether it named a Kubernetes object.
//
//   - A stage with no endpointRef dials the caller's own clientconfig, which is
//     the Kubernetes apiserver: its path names the object acted on, and the
//     object's coordinates are recorded as /call records them, with the verb
//     the apiserver derives for that path.
//   - A named endpoint may be any HTTP API: nothing of its path is recorded,
//     and the record names the host it went to (server.address) and the
//     lowercase method. #1283 — and which endpointRef the stage dialled it
//     through (krateo.audit.endpoint_ref.*, the reference as resolved: a
//     templated name rendered), names only: the shared writer never writes a
//     per-user credential Secret's name.
//   - #1291 — a record names exactly one target (schema v1). A dial whose
//     path names no object records the host it went to (the caller's own
//     apiserver included: that is where the write went). An action that was
//     not dialled (dialled false) and names no object or no resolved endpoint
//     names none: the shared writer marks it target.
func stageTarget(ctx context.Context, ev *audit.Event, apiCall *templates.API, path string, ep *endpoints.Endpoint, ref *templates.Reference, dialled bool) (apiServerObject bool) {
	method := ev.HTTPMethod
	if apiCall.EndpointRef == nil && !cache.ServiceAccountDialFromContext(ctx) && path != "" {
		p, query, hasQuery := splitPathQuery(path) // as the apiserver receives it (#1073 F1)
		// #957 — a stage may ask the apiserver for a dry run in its path
		// (".../demo?dryRun=All", e.g. a validation stage). Nothing is
		// persisted: the record says so and names no written object.
		ev.DryRun = apiServerDryRun(query)
		if gvr, ns, name, ok := cache.ParseAPIServerPathToDep(p); ok {
			ev.Group, ev.Version, ev.Resource = gvr.Group, gvr.Version, gvr.Resource
			ev.Namespace, ev.ObjectName = ns, name
			ev.Subresource = apiServerSubresource(p)
			uri := p
			if hasQuery {
				uri += "?" + query
			}
			ev.Verb = audit.KubernetesVerb(method, uri)
			return true
		}
	}
	ev.Verb = audit.MethodVerb(method)
	if !dialled && apiCall.EndpointRef == nil {
		return false // the caller's apiserver, object unknown: marked target
	}
	if ep != nil {
		if u, err := url.Parse(ep.ServerURL); err == nil {
			ev.ServerAddress = u.Host // never userinfo, a path or a query
		}
	}
	if ev.ServerAddress != "" && apiCall.EndpointRef != nil && ref != nil {
		ev.EndpointRefNamespace, ev.EndpointRefName = ref.Namespace, ref.Name
	}
	return false
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

// stageRefusal is one write stage, or one item of it, that was not sent
// (audit#22 (a)): what is known of it when it was refused.
type stageRefusal struct {
	apiCall *templates.API
	// method is the item's method; "" takes the stage's verb.
	method string
	// path and ep are the item's rendered path and resolved endpoint, when
	// it got that far ("" and nil otherwise); ref the endpointRef resolved.
	path string
	ep   *endpoints.Endpoint
	ref  *templates.Reference
	// outcome is denied (snowplow refused, with reason) or failure (a read
	// snowplow needed to send it failed: code is what that authority
	// answered, 0 when nothing did).
	outcome, reason string
	code            int
}

// auditStageRefusal emits the record of a write stage snowplow did not send,
// on a resolve somebody asked for (stageRecorded): denied with its reason, or a
// failure of what it needed first. A refused READ stage gets none, as a sent
// read does not. The body is a fixed phrase, never a value of the stage (its
// path or payload can carry a credential).
func (r *resolveRun) auditStageRefusal(ctx context.Context, sr stageRefusal) {
	method := strings.ToUpper(sr.method)
	if method == "" {
		method = strings.ToUpper(ptr.Deref(sr.apiCall.Verb, http.MethodGet))
	}
	if !stageRecorded(ctx, method) {
		return
	}
	ev := stageEvent(r.opts.RESTActionNamespace, r.opts.RESTActionName, sr.apiCall, method)
	stageTarget(ctx, &ev, sr.apiCall, sr.path, sr.ep, sr.ref, false)
	ev.Outcome, ev.DenialReason, ev.StatusCode = sr.outcome, sr.reason, sr.code
	if sr.outcome == audit.OutcomeDenied {
		ev.StatusCode, ev.Message = 0, audit.MessageRefused
	} else {
		ev.Message = audit.FailureMessage(sr.code)
	}
	audit.Emit(ctx, ev)
}

type stageEndpointRefKey struct{}

// withStageEndpointRef carries the endpointRef a stage resolved (templated
// name rendered, #113) to its dial, for the record (#1283). nil: none.
func withStageEndpointRef(ctx context.Context, ref *templates.Reference) context.Context {
	return context.WithValue(ctx, stageEndpointRefKey{}, ref)
}

func stageEndpointRef(ctx context.Context) *templates.Reference {
	ref, _ := ctx.Value(stageEndpointRefKey{}).(*templates.Reference)
	return ref
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
