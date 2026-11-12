package handlers

import (
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

// dialledObjectName is the name of the object a /call write acted on, as its
// record names it (#1378): the object the apiserver wrote, never a value only
// the record saw.
//
//   - A by-name verb (PUT, PATCH, DELETE) names the object in the dialled path
//     (buildURIPath, nameInPath): that name, which the apiserver resolves.
//   - A create (POST) is sent to the collection, so the query's name never
//     reaches the apiserver: its create handler takes the name from the body
//     (k8s.io/apiserver pkg/endpoints/handlers/create.go createHandler, with
//     includeName=false for a collection POST: the path names no object, and
//     the name is the decoded object's), generating it from
//     metadata.generateName when the body has none (pkg/registry/rest/
//     create.go BeforeCreate). So the record names the object the apiserver
//     answered with: its metadata.name, which covers a generated name.
//   - A create the apiserver answered with no object (a failure) names the
//     body's metadata.name, what the apiserver was asked to create, when it is
//     one valid path segment; otherwise none (the record names the resource
//     without a name, as the apiserver's audit does for a create whose object
//     it could not decode).
//
// A disagreeing query name is not refused: it is never sent, and the create
// succeeds under the body's name, so the request can create something; it
// is only never recorded.
func dialledObjectName(opts callOptions, outcome string, answer []byte) string {
	if nameInPath(opts.verb) {
		return opts.nsn.Name
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
// is not one or names none.
func metadataName(doc []byte) string {
	if len(doc) == 0 {
		return ""
	}
	var o struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if yaml.Unmarshal(doc, &o) != nil {
		return ""
	}
	return o.Metadata.Name
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
func exportAuditEvent(req *http.Request, outcome string, code int) audit.Event {
	t := exportAuditTarget(req.URL.Query())
	if _, ok := t.dialledURI(); !ok {
		t = auditTarget{}
	}
	return requestAuditEvent("export", req.Method, t.dialledVerb(), t, outcome, code)
}

// ExportRoute is the audit descriptor of GET and POST /export: every export
// is recorded, on every exit (#986), so a refused one always is too, built by
// the record's own builder (exportAuditEvent).
func ExportRoute() audit.Route {
	return audit.Route{Asked: func(req *http.Request) (audit.Event, bool) {
		return exportAuditEvent(req, audit.OutcomeDenied, 0), true
	}}
}

// dialledURI is the apiserver URI the GET /call lane dials for t
// (buildURIPath), and false when the lane cannot dial it.
func (t auditTarget) dialledURI() (string, bool) {
	uri, err := buildURIPath(callOptions{
		gvr: t.gvr, nsn: t.nsn, subresource: t.subresource,
		verb: http.MethodGet, namespaced: t.nsn.Namespace != "",
	})
	return uri, err == nil
}

// dialledVerb is the krateo.audit.verb of the read an export dials for t: the
// GET /call lane's apiserver URI for that target, classified as the apiserver
// classifies it. A target with no URI to dial is no Kubernetes object, and
// its verb is the lowercase method of the read, the apiserver's non-resource
// rule.
func (t auditTarget) dialledVerb() string {
	uri, ok := t.dialledURI()
	if !ok {
		return audit.MethodVerb(http.MethodGet)
	}
	return audit.KubernetesVerb(http.MethodGet, uri)
}
