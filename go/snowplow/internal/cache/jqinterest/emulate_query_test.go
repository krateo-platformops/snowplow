package jqinterest

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/krateo-platformops/snowplow/internal/apicall"
)

// emuApplyListQuery serves the tracked call's list the way the resolver does
// (#683), using the SAME classifier: internal/apicall ParsePath and PlanQuery,
// which cache.ParseAPIServerPathToDep and restactions/api planListQuery
// delegate to.
//
//   - Not an apiserver resource path (an endpoint's own /?query=, or a
//     template that does not render on this dict): served unselected, as
//     before #683.
//   - A query PlanQuery APPLIES: filtered in process by Query.Matches over
//     each item's name, namespace and labels (the resolver's itemMeta decode).
//   - A query PlanQuery REFUSES falls through to the apiserver:
//   - on a LITERAL path the analysis decides statically (All,
//     query_unapplied), so the emulator answers as the apiserver does
//     (emuApiserverList) and the answer is comparable: a mutation that
//     ignores the refused query must be caught by it;
//   - on a TEMPLATED path the call is only known at run time, and the
//     runtime §2.4 fall-through rule (query_unapplied) owns it: a fallback
//     outcome, never compared.
func emuApplyListQuery(tracked *Stage, items []any, dict map[string]any) ([]any, string) {
	_, templated := MaybeQuery(tracked.Path)
	p, err := renderTemplate(tracked.Path, dict)
	if err != nil {
		return items, ""
	}
	_, ns, name, ok := apicall.ParsePath(p)
	if !ok {
		return items, ""
	}
	q := apicall.PlanQuery(p, name == "", ns != "")
	if !q.Applicable {
		if templated {
			return nil, "FALLBACK query_unapplied " + q.Reason
		}
		return emuApiserverList(p, items)
	}
	if q.Label == nil && q.Field == nil {
		return items, ""
	}
	kept := []any{}
	for _, it := range items {
		md, lbls := emuItemMeta(it)
		n, _ := md["name"].(string)
		x, _ := md["namespace"].(string)
		if q.Matches(n, x, lbls) {
			kept = append(kept, it)
		}
	}
	return kept, ""
}

func emuItemMeta(it any) (map[string]any, map[string]string) {
	m, _ := it.(map[string]any)
	md, _ := m["metadata"].(map[string]any)
	lbls := map[string]string{}
	if ls, ok := md["labels"].(map[string]any); ok {
		for k, v := range ls {
			if sv, ok := v.(string); ok {
				lbls[k] = sv
			}
		}
	}
	return md, lbls
}

// emuApiserverList is the apiserver's answer to a LIST whose query the
// informer does not reproduce. It applies every param with the apiserver's
// own semantics, as far as they act on the items:
//   - labelSelector: labels.Parse; an invalid one is a 400 (an error outcome);
//   - fieldSelector: fields.ParseSelector; each requirement compares the
//     string at the field's JSON path. This is a SELECTABLE field: a built-in
//     field label (pods status.phase) or a CRD selectableFields entry, both of
//     which can be mutable. An invalid one is a 400;
//   - limit: the first N items (the list is sorted by (namespace, name), E5);
//   - continue, resourceVersion(Match), watch flags and any other param: no
//     effect on the first page's items;
//   - a repeated param: the first value, as the apiserver's query decoding
//     takes it.
func emuApiserverList(p string, items []any) ([]any, string) {
	raw := p[strings.IndexByte(p, '?')+1:]
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, "apiserver 400: malformed query"
	}
	out := append([]any{}, items...)
	if v := vals.Get(apicall.ParamLabelSelector); v != "" {
		sel, err := labels.Parse(v)
		if err != nil {
			return nil, "apiserver 400: " + err.Error()
		}
		var kept []any
		for _, it := range out {
			_, lbls := emuItemMeta(it)
			if sel.Matches(labels.Set(lbls)) {
				kept = append(kept, it)
			}
		}
		out = kept
	}
	if v := vals.Get(apicall.ParamFieldSelector); v != "" {
		sel, err := fields.ParseSelector(v)
		if err != nil {
			return nil, "apiserver 400: " + err.Error()
		}
		var kept []any
		for _, it := range out {
			set := fields.Set{}
			for _, r := range sel.Requirements() {
				set[r.Field] = emuFieldValue(it, r.Field)
			}
			if sel.Matches(set) {
				kept = append(kept, it)
			}
		}
		out = kept
	}
	if v := vals.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 && n < len(out) {
			out = out[:n]
		}
	}
	if out == nil {
		out = []any{}
	}
	return out, ""
}

// emuFieldValue is the string the apiserver's field set holds for a dotted
// field path: the value's string form, "" when absent.
func emuFieldValue(it any, field string) string {
	var cur any = it
	for _, k := range strings.Split(field, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[k]
	}
	switch v := cur.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}
