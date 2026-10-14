package jqinterest

import (
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

// emuApplyListQuery applies the tracked call's query string the way the
// resolver does since #628 (restactions/api list_query.go planListQuery),
// so the differential fuzz sees the list the cell really sees (#683):
//   - labelSelector: labels.Parse, matched against each item's labels;
//   - fieldSelector on metadata.name / metadata.namespace: applied;
//   - anything else, a repeated param or an unparseable selector: the call
//     falls through to the apiserver. That is the #603 §2.4 runtime fallback
//     (query_unapplied), never narrowed, so it is a fallback outcome here.
//
// A templated path is rendered against the dict, as the resolver renders it.
func emuApplyListQuery(tracked *Stage, items []any, dict map[string]any) ([]any, string) {
	p, err := renderTemplate(tracked.Path, dict)
	if err != nil {
		return nil, "FALLBACK tracked path template error"
	}
	i := strings.IndexByte(p, '?')
	if i < 0 {
		return items, ""
	}
	vals, err := url.ParseQuery(p[i+1:])
	if err != nil {
		return nil, "FALLBACK query_unapplied (malformed)"
	}
	var lsel labels.Selector
	var fsel fields.Selector
	for k, v := range vals {
		if len(v) != 1 {
			return nil, "FALLBACK query_unapplied (repeated)"
		}
		switch k {
		case "labelSelector":
			s, err := labels.Parse(v[0])
			if err != nil {
				return nil, "FALLBACK query_unapplied (label selector)"
			}
			lsel = s
		case "fieldSelector":
			s, err := fields.ParseSelector(v[0])
			if err != nil {
				return nil, "FALLBACK query_unapplied (field selector)"
			}
			for _, r := range s.Requirements() {
				if r.Field != "metadata.name" && r.Field != "metadata.namespace" {
					return nil, "FALLBACK query_unapplied (field)"
				}
			}
			fsel = s
		default:
			return nil, "FALLBACK query_unapplied (" + k + ")"
		}
	}
	var kept []any
	for _, it := range items {
		m, _ := it.(map[string]any)
		md, _ := m["metadata"].(map[string]any)
		if lsel != nil {
			set := labels.Set{}
			if ls, ok := md["labels"].(map[string]any); ok {
				for k, v := range ls {
					if sv, ok := v.(string); ok {
						set[k] = sv
					}
				}
			}
			if !lsel.Matches(set) {
				continue
			}
		}
		if fsel != nil {
			name, _ := md["name"].(string)
			ns, _ := md["namespace"].(string)
			if !fsel.Matches(fields.Set{"metadata.name": name, "metadata.namespace": ns}) {
				continue
			}
		}
		kept = append(kept, it)
	}
	if kept == nil {
		kept = []any{}
	}
	return kept, ""
}
