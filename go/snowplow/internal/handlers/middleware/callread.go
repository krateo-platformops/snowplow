// callread.go — #186. The POST /call/read body-decode middleware and its wire
// envelope. It lifts a request's `extras` off the URL query (where a large
// value is charged against the gateway's ~16 KiB request-header budget and
// 431s before Snowplow is ever invoked) and onto the request BODY, stashing
// the decoded map on the context so the context-first util.ParseExtras returns
// it. Mounted ONLY on POST /call/read; every other route is untouched.
package middleware

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// inlineGroup / inlineResource / inlineKind name the only resource an inline
// object may be (#443 part 2): a RESTAction, templates.krateo.io/v1.
const (
	inlineGroup      = "templates.krateo.io"
	inlineAPIVersion = "templates.krateo.io/v1"
	inlineResource   = "restactions"
	inlineKind       = "RESTAction"
)

// validateInlineObject checks an inline object against the request's query
// addressing. Every failure is a 400 at the caller:
//   - the query addresses restactions in group templates.krateo.io (an object
//     on resource=widgets or on any other GVR is refused here, before the
//     dispatcher, so it can never reach the widgets handler or the read-only
//     /call fallthrough);
//   - the object is apiVersion templates.krateo.io/v1, kind RESTAction;
//   - it converts into a typed RESTAction;
//   - its metadata.name and metadata.namespace equal the query's.
//
// The 1 MiB body bound is enforced by the LimitReader above (a truncated body
// fails to decode).
func validateInlineObject(q url.Values, raw map[string]any) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(q.Get("apiVersion"))
	if err != nil {
		return nil, fmt.Errorf("query apiVersion: %w", err)
	}
	if gv.Group != inlineGroup || q.Get("resource") != inlineResource {
		return nil, fmt.Errorf("an inline object is accepted only for resource %q in group %q, not %q in %q",
			inlineResource, inlineGroup, q.Get("resource"), gv.Group)
	}
	obj := &unstructured.Unstructured{Object: raw}
	if obj.GetAPIVersion() != inlineAPIVersion || obj.GetKind() != inlineKind {
		return nil, fmt.Errorf("the object must be apiVersion %q kind %q, got %q %q",
			inlineAPIVersion, inlineKind, obj.GetAPIVersion(), obj.GetKind())
	}
	var typed templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &typed); err != nil {
		return nil, fmt.Errorf("the object does not convert to a RESTAction: %w", err)
	}
	if obj.GetName() != q.Get("name") || obj.GetNamespace() != q.Get("namespace") {
		return nil, fmt.Errorf("the object's metadata name/namespace %q/%q must equal the query's %q/%q",
			obj.GetNamespace(), obj.GetName(), q.Get("namespace"), q.Get("name"))
	}
	return obj, nil
}

// CallReadMaxBodyBytes bounds the POST /call/read request body. It is a fixed
// DoS ceiling (like http.Server.MaxHeaderBytes), NOT a tunable knob — matching
// the existing hardcoded 1 MiB caps at handlers/jq.go (MaxBodySize) and the
// /call body read (call.go). 1 MiB is ~65× the ~15 KiB URL budget the body
// channel replaces, so a legitimate extras body is far under it.
const CallReadMaxBodyBytes = 1 * 1024 * 1024

// callReadBody is the POST /call/read wire envelope: {"extras": { ... }}. The
// wrapper (vs a bare extras object) is forward-compatible — a future read-body
// field can be added without ambiguity — and keeps the decoded `extras` value
// byte-for-byte what `?extras=<same json>` would yield, so the two channels
// derive the identical cache key (key parity by construction).
type callReadBody struct {
	Extras map[string]any `json:"extras"`
	// Object (#443 part 2) is an inline RESTAction to resolve INSTEAD of the
	// stored one: a dry-run resolve of a draft. When present it is validated
	// against the query's addressing (validateInlineObject), stashed on the
	// context (util.WithInlineObject) and the request is marked inert
	// (cache.WithInert): it persists nothing.
	Object map[string]any `json:"object,omitempty"`
}

// BodyExtrasDecode decodes the {"extras":{...}} body of a POST /call/read
// request and stashes the extras map on the context via util.WithExtras, so
// the context-first util.ParseExtras (extras.go) returns it at all three call
// sites unchanged. A body larger than CallReadMaxBodyBytes is truncated by the
// LimitReader and then fails JSON decode → 400 (never silently forwarded); a
// malformed body is likewise a 400. The body is consumed here; the downstream
// resolve handlers read extras from the CONTEXT, not the body, and the
// read-only Call fallthrough (handlers.CallRead) never reads a body — so
// consuming it here is safe.
//
// The stash happens even when the decoded extras is nil (an empty or
// extras-less body): that pins the read route to the body channel and stops a
// stray `?extras=` in the URL from being read on POST /call/read.
func BodyExtrasDecode(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body callReadBody
		if r.Body != nil {
			dec := json.NewDecoder(io.LimitReader(r.Body, CallReadMaxBodyBytes))
			if err := dec.Decode(&body); err != nil {
				response.BadRequest(w, fmt.Errorf("invalid POST /call/read body (expected {\"extras\":{...}}): %w", err))
				return
			}
		}
		ctx := util.WithExtras(r.Context(), body.Extras)
		if body.Object != nil {
			obj, err := validateInlineObject(r.URL.Query(), body.Object)
			if err != nil {
				response.BadRequest(w, fmt.Errorf("invalid POST /call/read inline object: %w", err))
				return
			}
			// One of the two places allowed to mint the inert flag (the other
			// is the trusted self-loopback ingest): an inline object is a
			// caller-vouched draft, so its resolve leaves no trace.
			ctx = cache.WithInert(util.WithInlineObject(ctx, obj))
		}
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}
