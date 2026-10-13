// list_query.go — #618 / #602: the query string of an informer-served call.
//
// THE DEFECT. cache.ParseAPIServerPathToDep strips everything after `?`, and
// every informer-served path keyed and served on the parsed (gvr, ns, name)
// alone. A RESTAction stage carrying `?labelSelector=`, `?fieldSelector=` or
// `?limit=` was therefore served the UNSELECTED list, silently, and the
// identity-free apistage content cell for a selector stage was the same cell as
// the unselected stage's (#618). The cluster-list collapse rebuilt its path from
// the GVR and discarded the query the same way (#602).
//
// THE RULE (team decision on #618, hybrid of options B and A). Exactly two
// selector shapes have semantics the informer can reproduce EXACTLY, and only
// those are applied in process:
//
//   - labelSelector, parsed with k8s.io/apimachinery labels.Parse — the same
//     function the apiserver's query conversion calls — so the full set-based
//     syntax (in, notin, exists, !key) matches by construction;
//   - fieldSelector whose every requirement is on metadata.name, or on
//     metadata.namespace when the call is NAMESPACE-SCOPED. Parsed with
//     fields.ParseSelector, as the apiserver does.
//
// EVERYTHING ELSE FALLS THROUGH to the apiserver with the query intact, which
// is correct by construction: any other field selector, limit/continue,
// resourceVersion(Match), watch, a repeated param, an unparseable selector (so
// the apiserver returns its own 400), any query on a GET-by-name, and any param
// this file does not know. Nothing is ever silently dropped: every param is
// either applied or the whole call goes to the apiserver.
//
// WHY metadata.namespace IS NOT APPLIED ON A CLUSTER-WIDE PATH. metadata.name is
// a supported field label for every built-in kind and every CRD (TRACED,
// k8s.io/kubernetes v1.36.2 pkg/apis/*/conversion.go: every
// AddFieldLabelConversionFunc admits "metadata.name"). metadata.namespace is NOT:
// the Node and Namespace conversion funcs (pkg/apis/core/v1/conversion.go) reject
// it with a 400, and a cluster-wide path does not say whether the resource is
// cluster-scoped. On a namespace-scoped path the resource is provably namespaced,
// so there the label is always supported and its value is the path namespace.
//
// THE KEY. The canonical query (selectors re-rendered, field requirements
// sorted, params sorted by name; an empty selector is dropped because it
// selects everything) is folded into every cache key that stores these results:
// the apistage content cell (ResolvedKeyInputs.Query) and therefore the
// cluster-list collapse cell, which uses the same key.
package api

import (
	"encoding/json"
	"net/url"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

const (
	listQueryParamLabelSelector = "labelSelector"
	listQueryParamFieldSelector = "fieldSelector"

	fieldMetadataName      = "metadata.name"
	fieldMetadataNamespace = "metadata.namespace"
)

// listQuery is a call's query string, classified. The zero value is "no
// query": applicable, empty canonical form, matches every object.
type listQuery struct {
	// canon is the canonical query string ("" when there is nothing to
	// apply). It is what the content key folds and what a rebuilt call path
	// carries after `?`.
	canon string
	// applicable is false when the call must fall through to the apiserver.
	applicable bool
	// reason is the bounded counter reason (listQueryReason*), set for both
	// outcomes; "" for an empty query.
	reason string

	label labels.Selector // nil when no label selector applies
	field fields.Selector // nil when no field selector applies

	// labelStr / fieldStr are the canonical selector strings, for a caller
	// that hands the selectors to a live apiserver LIST (ListOptions).
	labelStr string
	fieldStr string
}

// empty reports whether there is nothing to apply in process.
func (q listQuery) empty() bool { return q.applicable && q.canon == "" }

// splitPathQuery returns the path and the raw query of an apiserver call path.
// hasQuery is true when a `?` is present, even with an empty query after it.
func splitPathQuery(p string) (path, rawQuery string, hasQuery bool) {
	if i := strings.IndexByte(p, '?'); i >= 0 {
		return p[:i], p[i+1:], true
	}
	return p, "", false
}

// planListQuery classifies the query string of call path p.
//
// isList is true for a collection read (ParseAPIServerPathToDep returned
// name==""); namespaced is true when the path is namespace-scoped (it returned
// ns!=""). It is a pure function of its inputs, so every site that keys or
// serves a call derives the same answer.
func planListQuery(p string, isList, namespaced bool) listQuery {
	_, raw, _ := splitPathQuery(p)
	if raw == "" {
		return listQuery{applicable: true}
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return listQuery{reason: listQueryReasonMalformed}
	}
	if len(vals) == 0 {
		// e.g. "?&" — no param at all, nothing to apply or drop.
		return listQuery{applicable: true}
	}
	if !isList {
		// A GET-by-name has no selector semantics to reproduce; whatever it
		// carries is the apiserver's to interpret.
		return listQuery{reason: listQueryReasonQueryOnGet}
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
			return listQuery{reason: listQueryReasonRepeatedParam}
		}
		switch k {
		case listQueryParamLabelSelector, listQueryParamFieldSelector:
		case "limit", "continue":
			return listQuery{reason: listQueryReasonPagination}
		case "resourceVersion", "resourceVersionMatch":
			return listQuery{reason: listQueryReasonResourceVersion}
		case "watch", "allowWatchBookmarks", "sendInitialEvents":
			return listQuery{reason: listQueryReasonWatch}
		default:
			return listQuery{reason: listQueryReasonUnknownParam}
		}
	}

	q := listQuery{applicable: true, reason: listQueryReasonSelector}
	canon := url.Values{}
	if v, ok := vals[listQueryParamLabelSelector]; ok {
		sel, err := labels.Parse(v[0])
		if err != nil {
			return listQuery{reason: listQueryReasonInvalidSelector}
		}
		if !sel.Empty() {
			q.label = sel
			q.labelStr = sel.String()
			canon.Set(listQueryParamLabelSelector, q.labelStr)
		}
	}
	if v, ok := vals[listQueryParamFieldSelector]; ok {
		sel, err := fields.ParseSelector(v[0])
		if err != nil {
			return listQuery{reason: listQueryReasonInvalidSelector}
		}
		reqs := sel.Requirements()
		terms := make([]string, 0, len(reqs))
		for _, r := range reqs {
			switch r.Field {
			case fieldMetadataName:
			case fieldMetadataNamespace:
				if !namespaced {
					return listQuery{reason: listQueryReasonNamespaceFieldClusterPath}
				}
			default:
				return listQuery{reason: listQueryReasonUnsupportedField}
			}
			op := "="
			switch r.Operator {
			case selection.Equals, selection.DoubleEquals:
			case selection.NotEquals:
				op = "!="
			default:
				// fields.ParseSelector only produces =, == and !=; anything
				// else is not a shape we can vouch for.
				return listQuery{reason: listQueryReasonInvalidSelector}
			}
			terms = append(terms, r.Field+op+fields.EscapeValue(r.Value))
		}
		if len(terms) > 0 {
			sort.Strings(terms)
			q.field = sel
			q.fieldStr = strings.Join(terms, ",")
			canon.Set(listQueryParamFieldSelector, q.fieldStr)
		}
	}
	q.canon = canon.Encode() // url.Values.Encode sorts by key
	if q.canon == "" {
		// Only empty selectors: semantically the unselected list.
		return listQuery{applicable: true}
	}
	return q
}

// matches reports whether an object with these coordinates and labels is in
// the selected set, using the field set the apiserver builds for an object
// (generic.ObjectMetaFieldsSet: metadata.name, plus metadata.namespace).
func (q listQuery) matches(name, namespace string, lbls map[string]string) bool {
	if q.label != nil && !q.label.Matches(labels.Set(lbls)) {
		return false
	}
	if q.field != nil && !q.field.Matches(fields.Set{
		fieldMetadataName:      name,
		fieldMetadataNamespace: namespace,
	}) {
		return false
	}
	return true
}

// itemMeta is the slice of an object the selectors read. Decoding only this
// from an item's stored JSON keeps the #578 zero-decode envelope path cheap: the
// rest of the object is never materialised.
type itemMeta struct {
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
}

// filterRaw keeps the per-item JSON documents the query selects. ok=false when
// an item cannot be decoded, in which case the caller falls through rather than
// guess.
func (q listQuery) filterRaw(raws [][]byte) ([][]byte, bool) {
	if q.empty() {
		return raws, true
	}
	out := make([][]byte, 0, len(raws))
	for _, r := range raws {
		var m itemMeta
		if err := json.Unmarshal(r, &m); err != nil {
			return nil, false
		}
		if q.matches(m.Metadata.Name, m.Metadata.Namespace, m.Metadata.Labels) {
			out = append(out, r)
		}
	}
	return out, true
}

// withQuery appends the canonical query to a rebuilt apiserver path.
func withQuery(path, canon string) string {
	if canon == "" {
		return path
	}
	return path + "?" + canon
}
