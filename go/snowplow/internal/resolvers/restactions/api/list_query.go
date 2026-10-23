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
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/krateo-platformops/snowplow/internal/apicall"
)

const (
	listQueryParamLabelSelector = apicall.ParamLabelSelector
	listQueryParamFieldSelector = apicall.ParamFieldSelector

	fieldMetadataName      = apicall.FieldMetadataName
	fieldMetadataNamespace = apicall.FieldMetadataNamespace
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
// serves a call derives the same answer. The classification itself is
// internal/apicall PlanQuery, shared with the jqinterest analysis (#683
// follow-up), so the analysis and the resolver cannot disagree on which
// queries are applied in process.
func planListQuery(p string, isList, namespaced bool) listQuery {
	q := apicall.PlanQuery(p, isList, namespaced)
	return listQuery{
		canon:      q.Canon,
		applicable: q.Applicable,
		reason:     q.Reason,
		label:      q.Label,
		field:      q.Field,
		labelStr:   q.LabelStr,
		fieldStr:   q.FieldStr,
	}
}

// matches reports whether an object with these coordinates and labels is in
// the selected set, using the field set the apiserver builds for an object
// (generic.ObjectMetaFieldsSet: metadata.name, plus metadata.namespace).
func (q listQuery) matches(name, namespace string, lbls map[string]string) bool {
	return apicall.Query{Label: q.label, Field: q.field}.Matches(name, namespace, lbls)
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
		m, ok := q.matchRaw(r)
		if !ok {
			return nil, false
		}
		if m {
			out = append(out, r)
		}
	}
	return out, true
}

// matchRaw is filterRaw's predicate for ONE item (#854: the incremental list
// cell re-matches a changed object with exactly this function). ok=false when the
// item cannot be decoded.
func (q listQuery) matchRaw(r []byte) (match, ok bool) {
	if q.empty() {
		return true, true
	}
	var m itemMeta
	if err := json.Unmarshal(r, &m); err != nil {
		return false, false
	}
	return q.matches(m.Metadata.Name, m.Metadata.Namespace, m.Metadata.Labels), true
}

// withQuery appends the canonical query to a rebuilt apiserver path.
func withQuery(path, canon string) string {
	if canon == "" {
		return path
	}
	return path + "?" + canon
}
