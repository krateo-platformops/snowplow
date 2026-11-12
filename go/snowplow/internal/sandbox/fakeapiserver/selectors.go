//go:build js && wasm

package fakeapiserver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

// Label and field selectors, parsed by apimachinery as the apiserver parses them. Every resource
// supports metadata.name and metadata.namespace (the generic registry's DefaultClusterScopedAttr and
// DefaultNamespaceScopedAttr); any other field is the apiserver's 400 "field label not supported", as
// for a CRD that declares no selectableFields.

func parseLabelSelector(raw string) (labels.Selector, error) { return labels.Parse(raw) }

func parseFieldSelector(raw string) (fields.Selector, error) {
	sel, err := fields.ParseSelector(raw)
	if err != nil {
		return nil, err
	}
	for _, r := range sel.Requirements() {
		if r.Field != "metadata.name" && r.Field != "metadata.namespace" {
			return nil, fmt.Errorf("field label not supported: %s", r.Field)
		}
		if r.Operator != selection.Equals && r.Operator != selection.DoubleEquals && r.Operator != selection.NotEquals {
			return nil, fmt.Errorf("invalid selector: %q", raw)
		}
	}
	return sel, nil
}

// selectorName is the name a list or watch pins with metadata.name=<x>: the apiserver's RequestInfo
// authorizes such a request as that name, so a resourceNames grant applies.
func selectorName(sel fields.Selector) string {
	if sel == nil {
		return ""
	}
	name, _ := sel.RequiresExactMatch("metadata.name")
	return name
}

func objectLabels(obj map[string]any) labels.Set {
	md, _ := obj["metadata"].(map[string]any)
	ls, _ := md["labels"].(map[string]any)
	set := labels.Set{}
	for k, v := range ls {
		if s, ok := v.(string); ok {
			set[k] = s
		}
	}
	return set
}

func objectFields(obj map[string]any) fields.Set {
	md, _ := obj["metadata"].(map[string]any)
	name, _ := md["name"].(string)
	ns, _ := md["namespace"].(string)
	return fields.Set{"metadata.name": name, "metadata.namespace": ns}
}

func selected(obj map[string]any, ls labels.Selector, fs fields.Selector) bool {
	return (ls == nil || ls.Matches(objectLabels(obj))) && (fs == nil || fs.Matches(objectFields(obj)))
}
