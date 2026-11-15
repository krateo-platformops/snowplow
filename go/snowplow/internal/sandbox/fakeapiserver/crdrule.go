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
// resource names, in the list's order. A group version whose list is null or has no resources lists
// nothing. A `resources` that is not a list is refused, naming the group version, and so is an entry
// whose name is not a non-empty string (a null entry, one that is not an object, a name that is
// absent, null, not a string or ""), naming the group version and the entry's index; both for every
// group version, the built-in ones too.
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
		for i, e := range entries {
			var entry map[string]json.RawMessage
			var name string
			if json.Unmarshal(e, &entry) != nil || entry["name"] == nil || json.Unmarshal(entry["name"], &name) != nil || name == "" {
				return nil, fmt.Errorf("discovery.resources[%q]: resources[%d]: name is not a non-empty string", gv, i)
			}
			out[gv] = append(out[gv], name)
		}
	}
	return out, nil
}

// crdlessTypes is the rule applied to a snapshot: the resources its discovery lists (listedResources)
// in a group that is not built in, whose CRD is not among its objects as the rule asks. A
// subresource entry counts as its resource: the name up to its first "/". The CRD is the
// CustomResourceDefinition of apiextensions.k8s.io/v1, exactly, whose metadata.name is exactly
// <plural>.<group>; of two with one name, the last among the objects. Each offender is named once,
// sorted, as
//
//	<group/version> <plural> (no CustomResourceDefinition <plural>.<group>)
//
// or, for a CRD that is there, with everything that is wrong with it, joined by " and ":
//
//	<group/version> <plural> (CustomResourceDefinition <plural>.<group>: <reason> and <reason>)
//	    spec.group is "x", expected "<group>"
//	    spec.names.plural is "x", expected "<plural>"
//	    spec.versions names "<a version>" more than once
//	    does not serve <version>
//
// A spec.group or spec.names.plural that is absent or not a string "is not a string".
//
// The version is the group version after its first "/". It is served by a spec.versions entry with
// that name, exactly, and served the boolean true: a spec.versions that is missing, empty or not a
// list serves nothing, and so do an entry that is not an object or has no name, and a served that
// is absent or anything but true ("true", 1). A CRD that names any version in more than one entry
// is malformed (a real apiserver rejects it: spec.versions[i].name: Duplicate value) and no entry is
// chosen: the type that needs it is refused. Only a CRD a listed custom type needs is read, so a
// malformed CRD no listed type needs is not this rule's to refuse.
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
		twice         []string // the version names more than one spec.versions entry has, sorted
	}
	crds := map[string]crdSpec{} // by metadata.name
	for _, obj := range objects {
		av, _ := obj["apiVersion"].(string)
		if obj["kind"] != "CustomResourceDefinition" || av != "apiextensions.k8s.io/v1" {
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
		entries := map[string]int{}
		for _, v := range versions {
			vm, _ := v.(map[string]any)
			n, ok := vm["name"].(string)
			if !ok {
				continue
			}
			if entries[n]++; entries[n] == 2 {
				crd.twice = append(crd.twice, n)
			}
			if vm["served"] == true {
				crd.served[n] = true
			}
		}
		sort.Strings(crd.twice)
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
			for _, v := range crd.twice {
				wrong = append(wrong, "spec.versions names "+strconv.Quote(v)+" more than once")
			}
			if !crd.served[version] {
				wrong = append(wrong, "does not serve "+version)
			}
			if len(wrong) > 0 {
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s: %s)", gv, plural, crdName, strings.Join(wrong, " and ")))
			}
		}
	}
	sort.Strings(out)
	return out
}
