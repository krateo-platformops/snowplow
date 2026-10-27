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
// WHAT A RECORD NEVER HOLDS. This trail is kept for 365 days, so nothing that
// can carry a credential goes into it:
//   - the request PATH: a webhook path (Slack /services/T/B/<secret>, Discord
//     /api/webhooks/<id>/<token>, Telegram /bot<token>/…) or one built by jq from
//     an earlier stage's token is a secret in its own right. The RESTAction, its
//     stage and server.address identify the action and the authority.
//   - the upstream's response BODY: an error body can echo a token. The outcome
//     is the real status code plus a short reason class from a closed set
//     (outcomeMessage), never upstream text.
//
// THE OUTCOME is what the authority answered, observed at the dial (dialAnswer):
// the real status code (201 and 204 are not reported as 200); a 2xx whose body
// snowplow cannot read or convert is still a SUCCESS — the write was accepted —
// with a note; a dial that got no response is a failure with no code.
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
	"fmt"
	"net/http"
	"net/url"
	"strings"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// stageAuditAction is krateo.action on a RESTAction stage record.
const stageAuditAction = "restaction"

// isSafeMethod reports whether verb is a safe (read-only) HTTP method, RFC 9110
// §9.2.1. Every other method is a write. It is THE write classifier of the
// resolver: the audit trail (#902), the background-write refusal (#903), the
// dependent-write rule (#904) and the stale-pass hold (#905) all read it
// (isWriteVerb, write_verb.go). The empty method is classified as it is sent:
// net/http sends "" as GET.
func isSafeMethod(verb string) bool {
	switch strings.ToUpper(verb) {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// stageAuditEvent builds the record for one write-verb stage dial whose
// authority answered a, or reports false when the dial is not one the trail
// records: a safe method, or a resolve that is not a customer request.
func stageAuditEvent(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, a *dialAnswer) (audit.Event, bool) {
	verb := strings.ToUpper(ptr.Deref(call.Verb, http.MethodGet))
	if isSafeMethod(verb) {
		return audit.Event{}, false
	}
	if cache.RefDenialOrigin(ctx) != cache.RefDenialOriginServe {
		return audit.Event{}, false
	}

	ev := audit.Event{
		Action:              stageAuditAction,
		Verb:                verb,
		RESTActionNamespace: raNamespace,
		RESTActionName:      raName,
		Stage:               apiCall.Name,
	}
	if call.Endpoint != nil {
		if u, err := url.Parse(call.Endpoint.ServerURL); err == nil {
			ev.ServerAddress = u.Host // never userinfo, a path or a query
		}
	}
	// A stage with no endpointRef dials the caller's own clientconfig, which is
	// the Kubernetes apiserver: its path names the object acted on, and the
	// object's coordinates are recorded as /call records them. A named endpoint
	// may be any HTTP API: nothing of its path is recorded.
	if apiCall.EndpointRef == nil && !cache.ServiceAccountDialFromContext(ctx) {
		path, _, _ := strings.Cut(call.Path, "?")
		if gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path); ok {
			ev.Group, ev.Version, ev.Resource = gvr.Group, gvr.Version, gvr.Resource
			ev.Namespace, ev.Name = ns, name
			ev.Subresource = apiServerSubresource(path)
		}
	}

	ev.Outcome, ev.Code, ev.Message = outcomeOf(a)
	return ev, true
}

// outcomeOf turns what the authority answered into (outcome, code, message).
// The message is from a closed set and never carries upstream text.
func outcomeOf(a *dialAnswer) (outcome string, code int, message string) {
	if a == nil || a.code == 0 {
		return "failure", 0, "no response from the endpoint"
	}
	if a.code >= 200 && a.code < 300 {
		return "success", a.code, a.bodyNote
	}
	return "failure", a.code, outcomeMessage(a.code)
}

// outcomeMessage is the reason class of a non-2xx answer: the status text of
// the code, or the bare code for one with no standard text.
func outcomeMessage(code int) string {
	if t := http.StatusText(code); t != "" {
		return fmt.Sprintf("upstream answered %d %s", code, t)
	}
	return fmt.Sprintf("upstream answered %d", code)
}

// auditStageWrite emits the record for one dialled stage (see stageAuditEvent).
func auditStageWrite(ctx context.Context, raNamespace, raName string, apiCall *templates.API, call httpcall.RequestOptions, a *dialAnswer) {
	if ev, ok := stageAuditEvent(ctx, raNamespace, raName, apiCall, call, a); ok {
		audit.Emit(ctx, ev)
	}
}

// dialAnswer is what the authority of one branch-E dial answered, observed by
// httpFetchConverting for the audit record only: the real status code (the
// fetch's returned envelope reports every 2xx as 200 and a 2xx it cannot
// convert as 422) and whether a 2xx body could not be used. Zero code means no
// response was received. One dial, one goroutine: it is written by the fetch
// before it returns and read after.
type dialAnswer struct {
	code     int
	bodyNote string
}

// The closed set of notes on an accepted (2xx) write.
const (
	dialNoteBodyUnread        = "accepted; the response body could not be read"
	dialNoteBodyUnconvertible = "accepted; the response body is not JSON or YAML"
)

// answered records the final HTTP status code. Nil-safe: a fetch with no
// observer on its ctx records nothing.
func (a *dialAnswer) answered(code int) {
	if a != nil {
		a.code = code
	}
}

// note records why an accepted body could not be used. Nil-safe.
func (a *dialAnswer) note(n string) {
	if a != nil {
		a.bodyNote = n
	}
}

type dialAnswerCtxKey struct{}

// withDialAnswer returns ctx carrying a fresh observer for one dial.
func withDialAnswer(ctx context.Context) (context.Context, *dialAnswer) {
	a := &dialAnswer{}
	return context.WithValue(ctx, dialAnswerCtxKey{}, a), a
}

// dialAnswerFromContext returns the dial's observer, or nil when none.
func dialAnswerFromContext(ctx context.Context) *dialAnswer {
	a, _ := ctx.Value(dialAnswerCtxKey{}).(*dialAnswer)
	return a
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
