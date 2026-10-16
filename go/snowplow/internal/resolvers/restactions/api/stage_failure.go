// stage_failure.go — #709: keep the upstream status of a stage that failed
// WITHOUT continueOnError, so the RESTAction resolver can report it.
//
// THE DEFECT. Since #313 C-A a per-item stage failure never truncates the
// resolve: the error is recorded under the stage's errorKey and the remaining
// items and stages run. The Go error the worker builds for a stage without
// continueOnError (itemErrs[i]) was only ever logged at Debug and dropped, so
// api.Resolve returned a dict and nothing else. When the RESTAction's own
// filter then could not produce a value from that dict (it reads the failed
// stage, finds nothing, and yields no output — jq `empty`), the resolver
// handed "" to the JSON decoder and the request failed as
// `500 InternalError: unexpected end of JSON input`. The apiserver had said
// 404 NotFound, naming the missing object; that answer was gone.
//
// WHAT THIS KEEPS. recordItemError wraps a non-continueOnError item error into
// a *stageItemFailure that carries the status the stage recorded (code,
// reason, message). After each stage, the first such failure in item order —
// and across stages, the first failing stage in topological order — becomes
// the resolve's StageFailure. Deterministic whatever order concurrent items
// finish in.
//
// WHERE THE CODE AND REASON COME FROM, in order:
//  1. the value recorded under errorKey, when it is a Status map with a code —
//     the external fetch's upstream Status, the #681 canonical forbidden body;
//  2. a typed *apierrors.StatusError in the item error's chain (the
//     internal-rest-config dial, which records a plain string);
//  3. otherwise 500 InternalError.
//
// THE MESSAGE NEVER COMES FROM THE UPSTREAM (identity, #681 S1 generalised).
// An upstream message can name the requester for ANY status, not only a 403:
// the apiserver's Forbidden text ("User \"alice\" cannot …"), an endpointRef
// host echoing the caller in a 404 or a 429, a dial error naming the
// requester's <user>-clientconfig Secret or a certificate CN. So the message
// is composed only from the status code and the request's own coordinates —
// the stage's path as the RESTAction wrote it, after templating — exactly as
// the canonical forbidden body is (per_identity_forbidden.go):
//   - an apiserver path: the apiserver's own phrasing rebuilt from (resource,
//     namespace, name): `<resource> "<name>" not found` for a 404, the
//     canonical forbidden text for a 403, otherwise the coordinates plus the
//     code and its status text;
//   - any other path: the code and its status text only — never the URL,
//     which can carry credentials (#523).
// The upstream's full message is still recorded under the stage's errorKey
// and logged at the dispatch site, exactly as before.
//
// WHAT IT DOES NOT CHANGE. The dict, the errorKey values, the stage outcomes,
// the stage-error sink bumps and every Put gate are exactly as before: the
// failure is an additional return, read only by restactions.Resolve, and only
// when the RESTAction's filter cannot produce a value (see restactions.go).

package api

import (
	"errors"
	"fmt"
	"net/http"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StageFailure is the first stage of a resolve that failed without
// continueOnError: the stage name and the upstream status it recorded.
type StageFailure struct {
	Stage   string
	Code    int
	Reason  string
	Message string

	// redirect is the upstream's 3xx when Code was mapped to 502 (#729).
	redirect int
}

// StatusError renders the failure as a typed apiserver status, so every
// caller that maps errors.As(*apierrors.StatusError) to an HTTP answer (the
// widget and RESTAction /call handlers) and every classifier that reads
// apierrors.Is* (the refresher, the seed) sees the upstream code and reason.
// The message names the RESTAction and the stage, then the canonical message
// (canonicalStageMessage) — never the upstream's own text.
func (f *StageFailure) StatusError(raNamespace, raName string) *apierrors.StatusError {
	if f == nil {
		return nil
	}
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    int32(f.Code),
		Reason:  metav1.StatusReason(f.Reason),
		Message: fmt.Sprintf("restaction %s/%s stage %q failed: %s", raNamespace, raName, f.Stage, f.Message),
	}}
}

// stageItemFailure is the item error of a stage without continueOnError. It
// keeps the text the worker built (Error) and its cause chain (Unwrap) so the
// Debug join line is unchanged, and adds the recorded status.
type stageItemFailure struct {
	err     error
	failure StageFailure
}

func (e *stageItemFailure) Error() string { return e.err.Error() }
func (e *stageItemFailure) Unwrap() error { return e.err }

// newStageItemFailure derives the stage's code and reason from what
// recordItemError recorded (accumVal) and the worker's item error.
func newStageItemFailure(stage string, accumVal any, itemErr error) *stageItemFailure {
	f := StageFailure{Stage: stage}
	if m, ok := accumVal.(map[string]any); ok {
		if code := statusCodeOf(m["code"]); code > 0 {
			f.Code = code
			f.Reason, _ = m["reason"].(string)
		}
	}
	if f.Code == 0 {
		var se *apierrors.StatusError
		if errors.As(itemErr, &se) {
			f.Code = int(se.ErrStatus.Code)
			f.Reason = string(se.ErrStatus.Reason)
		}
	}
	if f.Code == 0 {
		f.Code = http.StatusInternalServerError
	}
	// #729 — a 3xx is not an answer a client can act on. A credentialed dial
	// never follows a redirect (#723/#724, internal/credhttp), so a stage that
	// got one failed because the upstream misbehaved (or is an attack): report
	// 502 Bad Gateway, never a bare 302 with no Location. The 3xx itself stays
	// in the canonical message and under the stage's errorKey.
	if f.Code >= 300 && f.Code < 400 {
		f.redirect = f.Code
		f.Code = http.StatusBadGateway
		f.Reason = ""
	}
	if f.Reason == "" {
		f.Reason = string(response.New(f.Code, nil).Reason)
	}
	return &stageItemFailure{err: itemErr, failure: f}
}

// canonicalStageMessage is the identity-free message for a stage that failed
// with code on call: built from the code and the call's own coordinates only.
func canonicalStageMessage(code int, call httpcall.RequestOptions) string {
	return stageMessage(code, fmt.Sprintf("upstream answered %d %s", code, http.StatusText(code)), call)
}

// canonicalRedirectMessage is canonicalStageMessage for a stage whose
// upstream answered a redirect (#729): reported as 502, it names the 3xx.
func canonicalRedirectMessage(redirect int, call httpcall.RequestOptions) string {
	return stageMessage(http.StatusBadGateway,
		fmt.Sprintf("upstream answered %d %s, a redirect snowplow does not follow", redirect, http.StatusText(redirect)), call)
}

// stageMessage builds the identity-free message from code, the "answered"
// clause and the call's own coordinates.
func stageMessage(code int, answered string, call httpcall.RequestOptions) string {
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(call.Path)
	if !ok {
		return answered
	}
	gr := gvr.GroupResource()
	verb := ptr.Deref(call.Verb, http.MethodGet)
	switch {
	case code == http.StatusNotFound && name != "":
		return apierrors.NewNotFound(gr, name).Error()
	case code == http.StatusForbidden:
		return canonicalForbiddenMessage(gvr, ns, name, rbacVerbOf(verb, name))
	}
	target := gr.String()
	if name != "" {
		target = fmt.Sprintf("%s %q", target, name)
	}
	if ns != "" {
		target += fmt.Sprintf(" in namespace %q", ns)
	}
	return target + ": " + answered
}

// rbacVerbOf is the RBAC verb of an HTTP verb on the parsed coordinate.
func rbacVerbOf(verb, name string) string {
	switch verb {
	case http.MethodPost:
		return "create"
	case http.MethodPut:
		return "update"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	}
	return readVerbFor(name)
}

// statusCodeOf reads a Status map's code: a json.Unmarshal'd float64 or the
// int response.AsMap produces.
func statusCodeOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// firstStageFailure returns the failure of the lowest-index item that failed
// without continueOnError, with its identity-free message built from that
// item's call, or nil. itemErrs[i] is calls[i]'s slot.
func firstStageFailure(itemErrs []error, calls []httpcall.RequestOptions) *StageFailure {
	for i, err := range itemErrs {
		var sf *stageItemFailure
		if errors.As(err, &sf) {
			f := sf.failure
			var call httpcall.RequestOptions
			if i < len(calls) {
				call = calls[i]
			}
			if f.redirect != 0 {
				f.Message = canonicalRedirectMessage(f.redirect, call)
			} else {
				f.Message = canonicalStageMessage(f.Code, call)
			}
			return &f
		}
	}
	return nil
}
