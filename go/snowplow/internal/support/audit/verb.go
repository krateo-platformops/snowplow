// verb.go — krateo.audit.verb (schema v1, #1291): the verb of what snowplow
// dialled, as the authority it dialled derives it.
//
//   - A record whose target is a Kubernetes object names the verb the
//     apiserver derives for the request snowplow sent (KubernetesVerb): the
//     apiserver's own RequestInfoFactory, on the method and the URI that was
//     dialled. POST is create, PUT update, PATCH patch, DELETE delete on a
//     named object and deletecollection on a collection, GET get, list or
//     watch.
//   - Any other record (a RESTAction stage that wrote to a named endpoint)
//     names the lowercase method (MethodVerb), the apiserver's non-resource
//     rule.
//
// A verb the schema does not list (a method no Kubernetes or non-resource rule
// names) is not written: the shared writer then marks the record verb, and the
// method itself stays in http.request.method.
package audit

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	krateoaudit "github.com/krateo-platformops/audit"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/util/sets"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// requestInfo is the apiserver's request classifier, with the apiserver's
// default prefixes (k8s.io/apiserver pkg/server config.go
// NewRequestInfoResolver).
var requestInfo = &apirequest.RequestInfoFactory{
	APIPrefixes:          sets.NewString("api", "apis"),
	GrouplessAPIPrefixes: sets.NewString("api"),
}

// verbs is the schema's closed set of krateo.audit.verb.
var verbs = func() []string {
	k, _ := krateoaudit.LookupKey(krateoaudit.KeyVerb)
	return k.Values
}()

// KubernetesVerb is the krateo.audit.verb of a request with method sent to the
// Kubernetes apiserver at uri (path and query, as dialled), or "" when the
// apiserver would derive no verb the schema lists.
func KubernetesVerb(method, uri string) string {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return ""
	}
	info, err := requestInfo.NewRequestInfo(&http.Request{Method: strings.ToUpper(method), URL: u})
	if err != nil {
		return ""
	}
	return listed(info.Verb)
}

// KubernetesObject is the object the Kubernetes apiserver resolves a request
// with method to uri (path and query, as dialled) to: the apiserver's own
// RequestInfoFactory, so a record built from it names what the apiserver acts
// on, by construction. ok is false when the apiserver would read uri as no
// resource request.
func KubernetesObject(method, uri string) (info apirequest.RequestInfo, ok bool) {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return info, false
	}
	ri, err := requestInfo.NewRequestInfo(&http.Request{Method: strings.ToUpper(method), URL: u})
	if err != nil || !ri.IsResourceRequest {
		return info, false
	}
	return *ri, true
}

// MethodOfVerb is the HTTP method a request with krateo.audit.verb verb was
// sent with: the inverse of KubernetesVerb and MethodVerb, so a record's
// http.request.method can be checked against its verb (schema v1: the method
// is derived from the verb of the authority request the writer sent, never the
// method of the request it served). "" for a verb with no method.
func MethodOfVerb(verb string) string {
	switch verb {
	case "create", "post":
		return http.MethodPost
	case "update", "put":
		return http.MethodPut
	case "patch":
		return http.MethodPatch
	case "delete", "deletecollection":
		return http.MethodDelete
	case "get", "list", "watch":
		return http.MethodGet
	case "head":
		return http.MethodHead
	case "options":
		return http.MethodOptions
	}
	return ""
}

// DialMethodMismatch is the falsifier of F1 (#1379 review): it reports a
// record whose http.request.method is not the method of its
// krateo.audit.verb (MethodOfVerb), i.e. a record that names the method of the
// request snowplow SERVED instead of the one it SENT. The library's Check
// cannot see it: both values are in their closed sets either way. A record
// with no verb, or a method recorded as _OTHER, is not judged.
func DialMethodMismatch(attrs []attribute.KeyValue) (method, verb string, mismatch bool) {
	for _, kv := range attrs {
		switch string(kv.Key) {
		case krateoaudit.KeyHTTPMethod:
			method = kv.Value.AsString()
		case krateoaudit.KeyVerb:
			verb = kv.Value.AsString()
		}
	}
	if verb == "" || method == "" || method == "_OTHER" {
		return method, verb, false
	}
	return method, verb, MethodOfVerb(verb) != method
}

// MethodVerb is the krateo.audit.verb of a request with method to anything
// that is not a Kubernetes object: the lowercase method, or "" when the schema
// does not list it.
func MethodVerb(method string) string {
	return listed(strings.ToLower(method))
}

func listed(verb string) string {
	if slices.Contains(verbs, verb) {
		return verb
	}
	return ""
}
