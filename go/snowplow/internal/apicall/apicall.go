// Package apicall is the single definition of two facts about a resolved
// apiserver call path that more than one package must agree on:
//
//   - its shape (ParsePath): the (gvr, namespace, name) it addresses, or
//     ok=false for a path that is not an apiserver resource path;
//   - its query (PlanQuery): whether the informer can reproduce the query
//     string exactly and apply it in process (#618 / #628), or the whole call
//     must fall through to the apiserver.
//
// The resolver (internal/cache ParseAPIServerPathToDep, and
// internal/resolvers/restactions/api planListQuery) serves and keys calls by
// these answers. The jqinterest analysis (#603) reads them to know which
// list a literal stage path is served: an applied labelSelector makes the
// list depend on labels (#683), and a refused query means the call falls
// through to the apiserver, so the edge reads everything. Defining them once
// is what keeps the analysis and the resolver from drifting apart.
//
// The package is pure: no state, no I/O.
package apicall

import (
	"net/url"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
)

// ParsePath extracts (gvr, namespace, name) from a fully-resolved apiserver
// path. It returns name="" for a LIST form and ok=false for a non-apiserver
// path, an unresolved JQ fragment or a malformed shape.
//
// Path shapes recognised (after query-string + trailing-slash stripping):
//
//	/api/v1/<resource>                              -> {core/v1, ns="",  name=""}
//	/api/v1/<resource>/<name>                       -> {core/v1, ns="",  name=<name>}
//	/api/v1/namespaces/<ns>/<resource>              -> {core/v1, ns=<ns>,name=""}
//	/api/v1/namespaces/<ns>/<resource>/<name>       -> {core/v1, ns=<ns>,name=<name>}
//	/apis/<g>/<v>/<resource>                        -> {g/v,     ns="",  name=""}
//	/apis/<g>/<v>/<resource>/<name>                 -> {g/v,     ns="",  name=<name>}
//	/apis/<g>/<v>/namespaces/<ns>/<resource>        -> {g/v,     ns=<ns>,name=""}
//	/apis/<g>/<v>/namespaces/<ns>/<resource>/<name> -> {g/v,     ns=<ns>,name=<name>}
//
// Subresource suffixes (e.g. ".../status", ".../scale") are accepted and
// attributed to the parent resource.
func ParsePath(path string) (gvr schema.GroupVersionResource, namespace, name string, ok bool) {
	// Strip query string.
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	// Strip trailing slash.
	path = strings.TrimRight(path, "/")

	// Reject paths with unresolved JQ template fragments.
	if strings.Contains(path, "${") {
		return schema.GroupVersionResource{}, "", "", false
	}

	switch {
	case strings.HasPrefix(path, "/api/"):
		// /api/<version>/<resource>[/...]
		rest := strings.TrimPrefix(path, "/api/")
		parts := strings.Split(rest, "/")
		if len(parts) < 2 {
			return schema.GroupVersionResource{}, "", "", false
		}
		version := parts[0]
		// Namespaced form: /api/v1/namespaces/<ns>/<resource>[/<name>[/<subresource>...]]
		if parts[1] == "namespaces" && len(parts) >= 4 {
			ns := parts[2]
			resource := parts[3]
			objName := ""
			if len(parts) >= 5 {
				objName = parts[4]
			}
			return schema.GroupVersionResource{
				Version:  version,
				Resource: resource,
			}, ns, objName, true
		}
		// Cluster-scoped form: /api/v1/<resource>[/<name>[/<subresource>...]]
		resource := parts[1]
		objName := ""
		if len(parts) >= 3 {
			objName = parts[2]
		}
		return schema.GroupVersionResource{
			Version:  version,
			Resource: resource,
		}, "", objName, true

	case strings.HasPrefix(path, "/apis/"):
		// /apis/<group>/<version>/<resource>[/...]
		// or:  /apis/<group>/<version>/namespaces/<ns>/<resource>[/...]
		rest := strings.TrimPrefix(path, "/apis/")
		parts := strings.Split(rest, "/")
		if len(parts) < 3 {
			return schema.GroupVersionResource{}, "", "", false
		}
		group := parts[0]
		version := parts[1]
		if parts[2] == "namespaces" && len(parts) >= 5 {
			ns := parts[3]
			resource := parts[4]
			objName := ""
			if len(parts) >= 6 {
				objName = parts[5]
			}
			return schema.GroupVersionResource{
				Group:    group,
				Version:  version,
				Resource: resource,
			}, ns, objName, true
		}
		resource := parts[2]
		objName := ""
		if len(parts) >= 4 {
			objName = parts[3]
		}
		return schema.GroupVersionResource{
			Group:    group,
			Version:  version,
			Resource: resource,
		}, "", objName, true
	}

	return schema.GroupVersionResource{}, "", "", false
}

// Query parameter names and the field labels the informer can reproduce.
const (
	ParamLabelSelector = "labelSelector"
	ParamFieldSelector = "fieldSelector"

	FieldMetadataName      = "metadata.name"
	FieldMetadataNamespace = "metadata.namespace"
)

// Query plan reasons: a bounded set (the resolver's counter labels).
const (
	ReasonSelector                  = "selector"
	ReasonMalformed                 = "malformed_query"
	ReasonQueryOnGet                = "query_on_get"
	ReasonRepeatedParam             = "repeated_param"
	ReasonPagination                = "pagination"
	ReasonResourceVersion           = "resource_version"
	ReasonWatch                     = "watch"
	ReasonUnknownParam              = "unknown_param"
	ReasonInvalidSelector           = "invalid_selector"
	ReasonUnsupportedField          = "unsupported_field"
	ReasonNamespaceFieldClusterPath = "namespace_field_cluster_path"
)

// FallthroughReasons is the closed set of reasons PlanQuery gives a call
// that must fall through to the apiserver.
func FallthroughReasons() []string {
	return []string{
		ReasonMalformed,
		ReasonQueryOnGet,
		ReasonRepeatedParam,
		ReasonPagination,
		ReasonResourceVersion,
		ReasonWatch,
		ReasonUnknownParam,
		ReasonInvalidSelector,
		ReasonUnsupportedField,
		ReasonNamespaceFieldClusterPath,
	}
}

// Query is a call's query string, classified. The zero value is "no query":
// applicable, empty canonical form, matches every object.
type Query struct {
	// Canon is the canonical query string ("" when there is nothing to
	// apply). It is what the content key folds and what a rebuilt call path
	// carries after `?`.
	Canon string
	// Applicable is false when the call must fall through to the apiserver.
	Applicable bool
	// Reason is the bounded reason (Reason*), set for both outcomes; "" for
	// an empty query.
	Reason string

	Label labels.Selector // nil when no label selector applies
	Field fields.Selector // nil when no field selector applies

	// LabelStr / FieldStr are the canonical selector strings, for a caller
	// that hands the selectors to a live apiserver LIST (ListOptions).
	LabelStr string
	FieldStr string
}

// PlanQuery classifies the query string of call path p.
//
// isList is true for a collection read (ParsePath returned name==""), and
// namespaced is true when the path is namespace-scoped (it returned ns!="").
// It is a pure function of its inputs, so every site that keys, serves or
// analyses a call derives the same answer.
//
// Exactly two selector shapes are applied in process, because the informer
// reproduces their semantics EXACTLY:
//   - labelSelector, parsed with labels.Parse (the apiserver's own parser),
//     so the full set-based syntax matches by construction;
//   - fieldSelector whose every requirement is on metadata.name, or on
//     metadata.namespace when the call is NAMESPACE-SCOPED. On a
//     cluster-wide path metadata.namespace is refused: the Node and
//     Namespace field-label conversions reject it with a 400, and the path
//     does not say whether the resource is cluster-scoped.
//
// Everything else falls through to the apiserver with the query intact: any
// other field selector, limit/continue, resourceVersion(Match), watch, a
// repeated param, an unparseable selector, any query on a GET-by-name, and
// any param not listed here.
func PlanQuery(p string, isList, namespaced bool) Query {
	raw := ""
	if i := strings.IndexByte(p, '?'); i >= 0 {
		raw = p[i+1:]
	}
	if raw == "" {
		return Query{Applicable: true}
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return Query{Reason: ReasonMalformed}
	}
	if len(vals) == 0 {
		// e.g. "?&": no param at all, nothing to apply or drop.
		return Query{Applicable: true}
	}
	if !isList {
		// A GET-by-name has no selector semantics to reproduce; whatever it
		// carries is the apiserver's to interpret.
		return Query{Reason: ReasonQueryOnGet}
	}

	// Classify every param BEFORE applying any, so the fall-through reason is
	// deterministic whatever the param order. A param the informer cannot
	// reproduce wins over a supported one: the whole call falls through.
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(vals[k]) != 1 {
			return Query{Reason: ReasonRepeatedParam}
		}
		switch k {
		case ParamLabelSelector, ParamFieldSelector:
		case "limit", "continue":
			return Query{Reason: ReasonPagination}
		case "resourceVersion", "resourceVersionMatch":
			return Query{Reason: ReasonResourceVersion}
		case "watch", "allowWatchBookmarks", "sendInitialEvents":
			return Query{Reason: ReasonWatch}
		default:
			return Query{Reason: ReasonUnknownParam}
		}
	}

	q := Query{Applicable: true, Reason: ReasonSelector}
	canon := url.Values{}
	if v, ok := vals[ParamLabelSelector]; ok {
		sel, err := labels.Parse(v[0])
		if err != nil {
			return Query{Reason: ReasonInvalidSelector}
		}
		if !sel.Empty() {
			q.Label = sel
			q.LabelStr = sel.String()
			canon.Set(ParamLabelSelector, q.LabelStr)
		}
	}
	if v, ok := vals[ParamFieldSelector]; ok {
		sel, err := fields.ParseSelector(v[0])
		if err != nil {
			return Query{Reason: ReasonInvalidSelector}
		}
		reqs := sel.Requirements()
		terms := make([]string, 0, len(reqs))
		for _, r := range reqs {
			switch r.Field {
			case FieldMetadataName:
			case FieldMetadataNamespace:
				if !namespaced {
					return Query{Reason: ReasonNamespaceFieldClusterPath}
				}
			default:
				return Query{Reason: ReasonUnsupportedField}
			}
			op := "="
			switch r.Operator {
			case selection.Equals, selection.DoubleEquals:
			case selection.NotEquals:
				op = "!="
			default:
				// fields.ParseSelector only produces =, == and !=; anything
				// else is not a shape we can vouch for.
				return Query{Reason: ReasonInvalidSelector}
			}
			terms = append(terms, r.Field+op+fields.EscapeValue(r.Value))
		}
		if len(terms) > 0 {
			sort.Strings(terms)
			q.Field = sel
			q.FieldStr = strings.Join(terms, ",")
			canon.Set(ParamFieldSelector, q.FieldStr)
		}
	}
	q.Canon = canon.Encode() // url.Values.Encode sorts by key
	if q.Canon == "" {
		// Only empty selectors: semantically the unselected list.
		return Query{Applicable: true}
	}
	return q
}

// Matches reports whether an object with these coordinates and labels is in
// the selected set, using the field set the apiserver builds for an object
// (generic.ObjectMetaFieldsSet: metadata.name, plus metadata.namespace).
func (q Query) Matches(name, namespace string, lbls map[string]string) bool {
	if q.Label != nil && !q.Label.Matches(labels.Set(lbls)) {
		return false
	}
	if q.Field != nil && !q.Field.Matches(fields.Set{
		FieldMetadataName:      name,
		FieldMetadataNamespace: namespace,
	}) {
		return false
	}
	return true
}
