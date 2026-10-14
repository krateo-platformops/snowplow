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
//   - next to a selector, any other param, a repeated param or an
//     unparseable selector: the call falls through to the apiserver. That is
//     the #603 §2.4 runtime fallback (query_unapplied), never narrowed, so it
//     is a fallback outcome here;
//   - a query with no selector param selects nothing: served unselected.
//
// A templated path is rendered against the dict, as the resolver renders a
// stage path. When the render fails, no query is known and the list is served
// unselected: the emulator then models the tracked call exactly as it did
// before #683, so a chain whose path template does not render on a fuzzed
// dict keeps its trials comparable instead of turning every one into a
// fallback (the analysis reads the whole labels map for every templated
// path, so any query a render could produce is covered either way).
func emuApplyListQuery(tracked *Stage, items []any, dict map[string]any) ([]any, string) {
	p, err := renderTemplate(tracked.Path, dict)
	if err != nil {
		return items, ""
	}
	i := strings.IndexByte(p, '?')
	if i < 0 {
		return items, ""
	}
	vals, err := url.ParseQuery(p[i+1:])
	if err != nil {
		return nil, "FALLBACK query_unapplied (malformed)"
	}
	if _, ok := vals["labelSelector"]; !ok {
		if _, ok := vals["fieldSelector"]; !ok {
			// No selector: a non-Kubernetes endpoint's own query (the
			// corpus's ClickHouse /?query= stages) or a selector-free param
			// that falls through. Nothing selects items, so the list is
			// served as before #683 and the trial stays comparable.
			return items, ""
		}
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
