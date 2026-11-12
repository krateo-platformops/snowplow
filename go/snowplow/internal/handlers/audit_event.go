package handlers

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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
