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
