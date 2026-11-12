//go:build js && wasm

package fakeapiserver

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

// Label selectors are parsed by apimachinery as the apiserver parses them. Field selectors are
// accepted per kind exactly as kube-apiserver v1.36 accepts them (fieldlabels.json, taken from
// k8s.io/kubernetes v1.36.2 and compared with its source by scripts/checkfieldlabels): a built-in
// kind's registered field labels with their values as its strategy computes them; any other kind,
// apimachinery's default, metadata.name and metadata.namespace (pkg/runtime/conversion.go
// DefaultMetaV1FieldSelectorConversion); a custom resource, those plus its CRD's selectableFields.
// Anything else is the apiserver's 400 "field label not supported".

//go:embed fieldlabels.json
var fieldLabelsJSON []byte

type kindFields struct {
	Group       string            `json:"group"`
	Version     string            `json:"version"`
	Kind        string            `json:"kind"`
	Resource    string            `json:"resource"`
	Source      string            `json:"source"`
	Labels      map[string]string `json:"labels"`  // label -> value path (with an optional "bool:", "int:", "first:", "either:", "absent:" form)
	Aliases     map[string]string `json:"aliases"` // a label converted to another (spec.host -> spec.nodeName)
	Unsupported string            `json:"unsupported"`
}

var builtinFields = func() map[string]kindFields {
	var f struct {
		Kinds []kindFields `json:"kinds"`
	}
	if err := json.Unmarshal(fieldLabelsJSON, &f); err != nil {
		panic("fakeapiserver: fieldlabels.json: " + err.Error())
	}
	m := map[string]kindFields{}
	for _, k := range f.Kinds {
		m[k.Group+"/"+k.Resource] = k
	}
	return m
}()

var defaultFields = kindFields{Labels: map[string]string{"metadata.name": "metadata.name", "metadata.namespace": "metadata.namespace"}}

func parseLabelSelector(raw string) (labels.Selector, error) { return labels.Parse(raw) }

// fieldsFor is the field labels a resource accepts; for a custom resource, its CRD's selectable
// fields are added to the defaults.
func (s *Server) fieldsFor(gr string) kindFields {
	if k, ok := builtinFields[gr]; ok && !s.custom[gr] {
		return k
	}
	k := kindFields{Labels: map[string]string{}}
	for l, p := range defaultFields.Labels {
		k.Labels[l] = p
	}
	for _, l := range s.selectable[gr] {
		k.Labels[l] = l
	}
	return k
}

// selector is a parsed field selector with its labels converted for one kind.
type selector struct {
	sel  fields.Selector
	kind kindFields
}

// parseFieldSelector parses raw and converts each label as the kind's conversion func does.
func parseFieldSelector(raw string, k kindFields) (*selector, error) {
	sel, err := fields.ParseSelector(raw)
	if err != nil {
		return nil, err
	}
	sel, err = sel.Transform(func(label, value string) (string, string, error) {
		if to, ok := k.Aliases[label]; ok {
			label = to
		}
		if _, ok := k.Labels[label]; !ok {
			if k.Unsupported != "" {
				return "", "", fmt.Errorf(k.Unsupported, label)
			}
			return "", "", fmt.Errorf("field label not supported: %s", label)
		}
		return label, value, nil
	})
	if err != nil {
		return nil, err
	}
	for _, r := range sel.Requirements() {
		if r.Operator != selection.Equals && r.Operator != selection.DoubleEquals && r.Operator != selection.NotEquals {
			return nil, fmt.Errorf("invalid selector: %q", raw)
		}
	}
	return &selector{sel: sel, kind: k}, nil
}

// selectorName is the name a list or watch pins with metadata.name=<x>, from the raw query as the
// apiserver's RequestInfoFactory reads it, before any handler parses the selector: a
// resourceNames grant then applies. A selector that does not parse pins nothing.
func selectorName(raw string) string {
	sel, err := fields.ParseSelector(raw)
	if err != nil {
		return ""
	}
	name, _ := sel.RequiresExactMatch("metadata.name")
	return name
}

func lookup(obj any, path string) (any, bool) {
	cur := obj
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func format(v any, ok bool) string {
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case json.Number:
		return t.String()
	case float64:
		return fmt.Sprint(int64(t))
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// value is one field label's value in obj, as the kind's GetAttrs computes it.
func value(obj map[string]any, spec string) string {
	form, path, found := strings.Cut(spec, ":")
	if !found {
		form, path = "", spec
	}
	switch form {
	case "bool":
		v, ok := lookup(obj, path)
		if !ok {
			return "false"
		}
		return format(v, ok)
	case "int":
		v, ok := lookup(obj, path)
		if !ok {
			return "0"
		}
		return format(v, ok)
	case "first": // status.podIPs.ip: the first entry's ip
		i := strings.LastIndex(path, ".")
		v, _ := lookup(obj, path[:i])
		arr, _ := v.([]any)
		if len(arr) == 0 {
			return ""
		}
		return format(lookup(arr[0], path[i+1:]))
	case "either": // the first non-empty of two paths
		for _, p := range strings.Split(path, ",") {
			if v := format(lookup(obj, p)); v != "" {
				return v
			}
		}
		return ""
	case "absent": // accepted by the conversion func, absent from the field set
		return ""
	}
	return format(lookup(obj, path))
}

func (sel *selector) matches(obj map[string]any) bool {
	if sel == nil {
		return true
	}
	set := fields.Set{}
	for _, r := range sel.sel.Requirements() {
		set[r.Field] = value(obj, sel.kind.Labels[r.Field])
	}
	return sel.sel.Matches(set)
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

func selected(obj map[string]any, ls labels.Selector, fs *selector) bool {
	return (ls == nil || ls.Matches(objectLabels(obj))) && fs.matches(obj)
}
