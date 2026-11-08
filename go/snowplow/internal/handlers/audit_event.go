package handlers

import (
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
// action, the method the request was SENT with, the target it addressed, and
// what it answered. Every audited request — a /call write and an /export —
// builds its record here and adds only what is its own on top (a /call write:
// the dry-run mark, the failure message and the written object, #957; an
// export: its format and row counts). So every audited request names its
// target by the same attributes (k8s.resource.group, version, resource,
// subresource, name and k8s.namespace.name), and none carries a method it was
// not sent with (#1055). What the request was received with (source IPs,
// User-Agent, request URI, #1251) is not built here: audit.Middleware reads it
// at the edge and the emitter stamps it on every record, this one and a
// RESTAction stage's alike (internal/support/audit/request.go).
func requestAuditEvent(action, method string, t auditTarget, outcome string, code int) audit.Event {
	return audit.Event{
		Action:      action,
		Verb:        strings.ToUpper(method),
		Group:       t.gvr.Group,
		Version:     t.gvr.Version,
		Resource:    t.gvr.Resource,
		Subresource: t.subresource,
		Name:        t.nsn.Name,
		Namespace:   t.nsn.Namespace,
		Outcome:     outcome,
		Code:        code,
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
