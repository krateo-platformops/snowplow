package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

// auditTarget is the Kubernetes object a request addressed, as its audit
// record names it.
type auditTarget struct {
	gvr         schema.GroupVersionResource
	subresource string
	nsn         types.NamespacedName
}

// requestAuditEvent is the audit record of one request a handler served: the
// action, the verb the apiserver derives for what was dialled (verb,
// audit.KubernetesVerb), the method the request was SENT with, the target it
// addressed, and what it answered. Every audited request — a /call write and
// an /export — builds its record here and adds only what is its own on top (a
// /call write: the dry-run mark, the failure message and the written object,
// #957; an export: its format and row counts). So every audited request names
// its target by the same attributes (krateo.audit.object_ref.* with their v0.3
// twins k8s.resource.*, and k8s.namespace.name), and none carries a method it
// was not sent with (#1055). Who sent it, what the request was received with
// (source IPs, User-Agent, request URI, #1251) and the target's preview flag
// are not built here: audit.Emit reads them from the request context for every
// record, this one and a RESTAction stage's alike.
func requestAuditEvent(action, method, verb string, t auditTarget, outcome string, code int) audit.Event {
	return audit.Event{
		Action:      action,
		Verb:        verb,
		HTTPMethod:  strings.ToUpper(method),
		Group:       t.gvr.Group,
		Version:     t.gvr.Version,
		Resource:    t.gvr.Resource,
		Subresource: t.subresource,
		ObjectName:  t.nsn.Name,
		Namespace:   t.nsn.Namespace,
		Outcome:     outcome,
		StatusCode:  code,
	}
}

// dialledTarget is the object the apiserver resolves the /call dial for opts
// to (F3, #1379 review): buildURIPath's URI, classified by the apiserver's own
// RequestInfoFactory (audit.KubernetesObject). The record's target is this
// classification, so it describes the dial by construction. asked is false
// when the classification differs from what the request named: a group,
// version, resource or subresource, a namespace the request gave, or a name
// the path carries (resource=namespaces&namespace=foo&name=bar dials namespace
// bar, so it names namespace bar, not foo; resource=finalize under a namespace
// dials that namespace's finalize subresource). A /call write is refused then,
// before the dial (callHandler.ServeHTTP).
func dialledTarget(opts callOptions) (uri string, t auditTarget, asked bool, err error) {
	uri, err = buildURIPath(opts)
	if err != nil {
		return "", auditTarget{}, false, err
	}
	info, ok := audit.KubernetesObject(opts.verb, uri)
	if !ok {
		return uri, auditTarget{}, false, errNotAnObject
	}
	t = auditTarget{
		gvr:         schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource},
		subresource: info.Subresource,
		nsn:         types.NamespacedName{Namespace: info.Namespace, Name: info.Name},
	}
	asked = t.gvr == opts.gvr && t.subresource == opts.subresource &&
		(opts.nsn.Namespace == "" || opts.nsn.Namespace == t.nsn.Namespace) &&
		(!nameInPath(opts.verb) || opts.nsn.Name == "" || opts.nsn.Name == t.nsn.Name)
	return uri, t, asked, nil
}

// errNotAnObject is a /call URI the apiserver would read as no resource
// request.
var errNotAnObject = errors.New("the request does not name a Kubernetes resource")

// dialledObjectName is the name of the object a /call write acted on, as its
// record names it (#1378): the object the apiserver wrote, never a value only
// the record saw. t is the dial's classification (dialledTarget).
//
//   - A by-name verb (PUT, PATCH, DELETE) names the object in the dialled path:
//     the name the apiserver resolved from it.
//   - A create (POST) is sent to the collection, so the query's name never
//     reaches the apiserver: its create handler takes the name from the body
//     (k8s.io/apiserver v0.36.5 pkg/endpoints/handlers/create.go createHandler,
//     installed with includeName=false for a collection POST: the path names
//     no object, and the name is the decoded object's), generating it from
//     metadata.generateName when the body has none (pkg/registry/rest/
//     create.go BeforeCreate). So the record names the object the apiserver
//     answered with: its metadata.name, which covers a generated name.
//   - A create the apiserver answered with no object (a failure, a 403
//     included) names what the apiserver was asked to create: the body's
//     metadata.name, when it is one valid path segment; otherwise none (the
//     record names the resource without a name, as the apiserver's audit
//     does for a create whose object it could not decode).
//
// A disagreeing query name is not refused: it is never sent, and the create
// succeeds under the body's name, so the request can create something; it is
// only never recorded (krateo.request.uri keeps the query as received).
func dialledObjectName(opts callOptions, t auditTarget, outcome string, answer []byte) string {
	if nameInPath(opts.verb) {
		return t.nsn.Name
	}
	if outcome == audit.OutcomeSuccess {
		if n := metadataName(answer); n != "" {
			return n
		}
	}
	if n := metadataName(opts.dat); n != "" && len(content.IsPathSegmentName(n)) == 0 {
		return n
	}
	return ""
}

// metadataName is the metadata.name of a JSON or YAML object, or "" when doc
// is not one or names none. The keys are matched exactly, as apimachinery's
// decoder matches them (F5, #1379 review): {"metadata":{"Name":"decoy"}} has no
// name. YAML is converted to JSON first (sigs.k8s.io/yaml.YAMLToJSON, which
// leaves JSON as it is), then decoded into maps, whose keys are case-exact.
func metadataName(doc []byte) string {
	if len(doc) == 0 {
		return ""
	}
	js, err := yaml.YAMLToJSON(doc)
	if err != nil {
		return ""
	}
	var o map[string]any
	if json.Unmarshal(js, &o) != nil {
		return ""
	}
	meta, _ := o["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	return name
}

// exportAuditTarget is the target an export resolves: the apiVersion,
// resource, subresource, name and namespace it forwards unchanged to every
// /call page it dispatches (dispatchCall strips only exportParams), read as the
// /call lane reads them. An export is audited only after the /call lane
// answered, which it does only for a target it could parse, so this parse
// cannot disagree with the lane's; an apiVersion that does not parse still
// names its resource, and the record's missing group and version are then
// visible in it rather than hidden.
func exportAuditTarget(q url.Values) auditTarget {
	gv, _ := schema.ParseGroupVersion(q.Get("apiVersion"))
	return auditTarget{
		gvr:         gv.WithResource(q.Get("resource")),
		subresource: q.Get("subresource"),
		nsn:         types.NamespacedName{Namespace: q.Get("namespace"), Name: q.Get("name")},
	}
}

// exportAuditEvent is an export's record: the target it addressed, with the
// verb of the read its /call lane dials for it (dialledVerb). #1257 — a
// target the /call lane cannot dial (a value that is not one apiserver path
// segment: buildURIPath refuses it, and so does the lane) names no object,
// since none was or could be read: the record carries no Kubernetes target,
// so the shared writer marks it target.
//
// F1 (#1379 review) — http.request.method is the method of the authority
// request the writer SENT (schema v1, audit v0.9.0+: "derived from the verb of
// the authority request the writer sent ... never the method of the request
// the writer served"): an export only ever dials GET, so the record says GET
// for a GET and a POST /export alike. krateo.request.uri records what it
// received.
//
// The target is what the GET dial resolves to (dialledTarget's classification
// of the lane's URI), so it describes the read by construction.
func exportAuditEvent(req *http.Request, outcome string, code int) audit.Event {
	t, uri, ok := exportAuditTarget(req.URL.Query()).dialled()
	if !ok {
		return requestAuditEvent("export", http.MethodGet, audit.MethodVerb(http.MethodGet), auditTarget{}, outcome, code)
	}
	return requestAuditEvent("export", http.MethodGet, audit.KubernetesVerb(http.MethodGet, uri), t, outcome, code)
}

// ExportRoute is the audit descriptor of GET and POST /export: the record of
// what an export asked for is the one its handler builds (exportAuditEvent).
func ExportRoute() audit.Route {
	return audit.Route{Asked: func(req *http.Request) audit.Event { return exportAuditEvent(req, "", 0) }}
}

// dialled is what the GET /call lane's dial for t resolves to (dialledTarget)
// and its URI, and false when the lane cannot dial it.
func (t auditTarget) dialled() (auditTarget, string, bool) {
	uri, d, _, err := dialledTarget(callOptions{
		gvr: t.gvr, nsn: t.nsn, subresource: t.subresource,
		verb: http.MethodGet, namespaced: t.nsn.Namespace != "",
	})
	return d, uri, err == nil
}

// dialledVerb is the krateo.audit.verb of the read an export dials for t: the
// GET /call lane's apiserver URI for that target, classified as the apiserver
// classifies it. A target with no URI to dial is no Kubernetes object, and
// its verb is the lowercase method of the read, the apiserver's non-resource
// rule.
func (t auditTarget) dialledVerb() string {
	_, uri, ok := t.dialled()
	if !ok {
		return audit.MethodVerb(http.MethodGet)
	}
	return audit.KubernetesVerb(http.MethodGet, uri)
}
