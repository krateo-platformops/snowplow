//go:build js && wasm

package fakeapiserver

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// isBuiltinGroup is SNAPSHOT.md §3's one snapshot rule, in the words agentiko-sandbox keeps it in
// (snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs, both isBuiltinGroup):
//
// Built-in (needs no CRD): the core group, groups without a dot, and *.k8s.io. Every other group's
// resource needs its CRD, named <plural>.<group>, and that CRD must serve the listed version
// (spec.versions[].name with served: true), and its spec.group and spec.names.plural must be that
// group and plural.
func isBuiltinGroup(group string) bool {
	return group == "" || !strings.Contains(group, ".") || strings.HasSuffix(group, ".k8s.io")
}

// listedResources is the snapshot's discovery.resources as the rule reads it: each group version's
// resource names, in the list's order. An entry that is null or not an object, and one whose name
// is absent or not a string, names nothing and is skipped; a group version whose list is null or
// has no resources lists nothing. A `resources` that is not a list is refused, naming the group
// version.
func listedResources(snapshot []byte) (map[string][]string, error) {
	var snap struct {
		Discovery struct {
			Resources map[string]json.RawMessage `json:"resources"`
		} `json:"discovery"`
	}
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for gv, raw := range snap.Discovery.Resources {
		var list map[string]json.RawMessage
		if json.Unmarshal(raw, &list) != nil || list["resources"] == nil || string(list["resources"]) == "null" {
			continue
		}
		var entries []json.RawMessage
		if err := json.Unmarshal(list["resources"], &entries); err != nil {
			return nil, fmt.Errorf("discovery.resources[%q]: resources is not a list", gv)
		}
		for _, e := range entries {
			var entry map[string]json.RawMessage
			var name string
			if json.Unmarshal(e, &entry) != nil || entry["name"] == nil || string(entry["name"]) == "null" || json.Unmarshal(entry["name"], &name) != nil {
				continue
			}
			out[gv] = append(out[gv], name)
		}
	}
	return out, nil
}

// crdlessTypes is the rule applied to a snapshot: the resources its discovery lists (listedResources)
// in a group that is not built in, whose CRD is not among its objects as the rule asks. A
// subresource entry counts as its resource: the name up to its first "/". The CRD is the
// CustomResourceDefinition of apiextensions.k8s.io (any version of that group) whose metadata.name
// is exactly <plural>.<group>; of two with one name, the last among the objects. Each offender is
// named once, as one of
//
//	<group/version> <plural> (no CustomResourceDefinition <plural>.<group>)
//	<group/version> <plural> (CustomResourceDefinition <plural>.<group>: spec.group is "x", expected "<group>")
//	<group/version> <plural> (CustomResourceDefinition <plural>.<group>: spec.names.plural is "x", expected "<plural>")
//	<group/version> <plural> (CustomResourceDefinition <plural>.<group> does not serve <version>)
//
// sorted; a CRD whose spec.group and spec.names.plural both disagree says both, joined by " and ",
// and a field that is absent or not a string "is not a string".
//
// The version is the group version after its first "/". It is served by a spec.versions entry with
// that name, exactly, and served the boolean true, any such entry if several have the name: a
// spec.versions that is missing, empty or not a list serves nothing, and so do an entry that is not
// an object or has no name, and a served that is absent or anything but true ("true", 1).
//
// A real apiserver derives a custom type's discovery entry from its CRD (spec.group,
// spec.names.plural and each served version), so it never lists one without the other; the fake
// would serve such a type as an empty list it knows nothing about (no schema, no selectable
// fields), and nothing would say so (#1464). The fake keys what it reads of a CRD by its spec.group
// and spec.names.plural, so a CRD of the right name and another spec is no CRD for the type.
func crdlessTypes(listed map[string][]string, objects []map[string]any) []string {
	type crdSpec struct {
		group, plural any
		served        map[string]bool
	}
	crds := map[string]crdSpec{} // by metadata.name
	for _, obj := range objects {
		av, _ := obj["apiVersion"].(string)
		if g, _ := splitGV(av); obj["kind"] != "CustomResourceDefinition" || g != "apiextensions.k8s.io" {
			continue
		}
		md, _ := obj["metadata"].(map[string]any)
		name, ok := md["name"].(string)
		if !ok {
			continue
		}
		spec, _ := obj["spec"].(map[string]any)
		names, _ := spec["names"].(map[string]any)
		crd := crdSpec{group: spec["group"], plural: names["plural"], served: map[string]bool{}}
		versions, _ := spec["versions"].([]any)
		for _, v := range versions {
			vm, _ := v.(map[string]any)
			if n, ok := vm["name"].(string); ok && vm["served"] == true {
				crd.served[n] = true
			}
		}
		crds[name] = crd
	}
	disagrees := func(field string, found any, want string) string {
		switch s, ok := found.(string); {
		case !ok:
			return fmt.Sprintf("%s is not a string, expected %s", field, strconv.Quote(want))
		case s != want:
			return fmt.Sprintf("%s is %s, expected %s", field, strconv.Quote(s), strconv.Quote(want))
		}
		return ""
	}
	var out []string
	for gv, names := range listed {
		group, version := splitGV(gv)
		if isBuiltinGroup(group) {
			continue
		}
		seen := map[string]bool{}
		for _, name := range names {
			plural, _, _ := strings.Cut(name, "/")
			if seen[plural] {
				continue
			}
			seen[plural] = true
			crdName := plural + "." + group
			crd, ok := crds[crdName]
			if !ok {
				out = append(out, fmt.Sprintf("%s %s (no CustomResourceDefinition %s)", gv, plural, crdName))
				continue
			}
			var wrong []string
			for _, d := range []string{disagrees("spec.group", crd.group, group), disagrees("spec.names.plural", crd.plural, plural)} {
				if d != "" {
					wrong = append(wrong, d)
				}
			}
			switch {
			case len(wrong) > 0:
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s: %s)", gv, plural, crdName, strings.Join(wrong, " and ")))
			case !crd.served[version]:
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s does not serve %s)", gv, plural, crdName, version))
			}
		}
	}
	sort.Strings(out)
	return out
}
