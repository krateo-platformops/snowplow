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

// The rule is kept in three steps, each over one reading of the snapshot (strictJSON's values for
// snapshot.json, the loaded objects for the pages), and each refusing with every breach it finds,
// sorted:
//
//	listedResources  what discovery lists, and the lists that are not lists;
//	snapshotCRDs     the CRDs among the objects, and the ones that are not well formed;
//	crdlessTypes     the listed custom types without their CRD.
//
// A real apiserver derives a custom type's discovery entry from its CRD (spec.group,
// spec.names.plural and each served version), so it never lists one without the other; the fake
// would serve such a type as an empty list it knows nothing about (no schema, no selectable
// fields), and nothing would say so (#1464).

// wellFormed says a name read from JSON is the name that was written. encoding/json decodes an
// unpaired surrogate escape ("\ud800") and a byte that is not UTF-8 as U+FFFD, where JavaScript
// keeps the surrogate, so two different names would compare equal here and differ there; a name
// that holds U+FFFD after decoding, written so or not, is refused.
func wellFormed(name string) bool { return !strings.ContainsRune(name, '\uFFFD') }

// listedResources is what the snapshot's discovery lists: each group version's resource names, in
// the list's order, from strictJSON's reading of snapshot.json. Every group version is read, the
// built-in ones too, and each of these is a problem, named by its group version:
//
//   - a group version whose value is not an object with a `resources` array (null, a string, an
//     object without resources or with resources null or anything but an array);
//   - an entry whose name is not a non-empty string (a null entry, one that is not an object, a
//     name that is absent, null, not a string or "");
//   - a group version or a resource name that is not well-formed Unicode.
//
// A snapshot with no discovery.resources (absent or null) lists nothing.
func listedResources(snapshot any) (listed map[string][]string, problems []string) {
	snap, _ := snapshot.(map[string]any)
	discovery, _ := snap["discovery"].(map[string]any)
	if discovery["resources"] == nil {
		return nil, nil
	}
	resources, ok := discovery["resources"].(map[string]any)
	if !ok {
		return nil, []string{"discovery.resources is not an object"}
	}
	listed = map[string][]string{}
	for gv, v := range resources {
		if !wellFormed(gv) {
			problems = append(problems, fmt.Sprintf("discovery.resources[%s] is not well-formed Unicode", strconv.Quote(gv)))
			continue
		}
		list, _ := v.(map[string]any)
		entries, ok := list["resources"].([]any)
		if !ok {
			problems = append(problems, fmt.Sprintf("discovery.resources[%s] is not an APIResourceList with a resources array", strconv.Quote(gv)))
			continue
		}
		listed[gv] = []string{}
		for i, e := range entries {
			entry, _ := e.(map[string]any)
			switch name, ok := entry["name"].(string); {
			case !ok || name == "":
				problems = append(problems, fmt.Sprintf("discovery.resources[%s]: resources[%d]: name is not a non-empty string", strconv.Quote(gv), i))
			case !wellFormed(name):
				problems = append(problems, fmt.Sprintf("discovery.resources[%s]: resources[%d]: name %s is not well-formed Unicode", strconv.Quote(gv), i, strconv.Quote(name)))
			default:
				listed[gv] = append(listed[gv], name)
			}
		}
	}
	sort.Strings(problems)
	return listed, problems
}

// isCRD says an object is a CustomResourceDefinition: that kind, of apiextensions.k8s.io/v1,
// exactly. The rule and the loader's registration of CRD schemas both ask it, so no object is a CRD
// to one and not to the other.
func isCRD(obj map[string]any) bool {
	return obj["kind"] == "CustomResourceDefinition" && obj["apiVersion"] == "apiextensions.k8s.io/v1"
}

// snapshotCRDs is the CRDs among the snapshot's objects, in their order and by metadata.name. Each
// of these is a problem, whether or not discovery lists the CRD's type:
//
//   - a CRD whose metadata.name is not exactly <spec.names.plural>.<spec.group>, or with one of the
//     three absent or not a string: the fake keys a CRD's schema by spec.group and
//     spec.names.plural, so such a CRD would give its schema to a type that is not the one it is
//     named for;
//   - two CRDs with one name;
//   - a metadata.name, spec.group, spec.names.plural or spec.versions[].name that is not
//     well-formed Unicode.
//
// Only CRDs with none of these reach the loader's registration.
func snapshotCRDs(objects []map[string]any) (crds []map[string]any, byName map[string]map[string]any, problems []string) {
	byName = map[string]map[string]any{}
	for _, obj := range objects {
		if !isCRD(obj) {
			continue
		}
		md, _ := obj["metadata"].(map[string]any)
		spec, _ := obj["spec"].(map[string]any)
		names, _ := spec["names"].(map[string]any)
		name, okName := md["name"].(string)
		group, okGroup := spec["group"].(string)
		plural, okPlural := names["plural"].(string)
		who := "CustomResourceDefinition " + strconv.Quote(name)
		var wrong []string
		switch {
		case !okName:
			who = "a CustomResourceDefinition"
			wrong = append(wrong, "metadata.name is not a string")
		case !okGroup || !okPlural:
			for field, ok := range map[string]bool{"spec.group": okGroup, "spec.names.plural": okPlural} {
				if !ok {
					wrong = append(wrong, field+" is not a string")
				}
			}
		case name != plural+"."+group:
			wrong = append(wrong, "metadata.name is not <spec.names.plural>.<spec.group>, "+strconv.Quote(plural+"."+group))
		}
		ill := !wellFormed(name) || !wellFormed(group) || !wellFormed(plural)
		versions, _ := spec["versions"].([]any)
		for _, v := range versions {
			vm, _ := v.(map[string]any)
			if n, ok := vm["name"].(string); ok && !wellFormed(n) {
				ill = true
			}
		}
		if ill {
			wrong = append(wrong, "a name in it is not well-formed Unicode")
		}
		if _, twice := byName[name]; twice && okName {
			wrong = append(wrong, "two objects have this name")
		}
		if len(wrong) > 0 {
			sort.Strings(wrong)
			problems = append(problems, who+": "+strings.Join(wrong, " and "))
		}
		if okName {
			byName[name] = obj
		}
		crds = append(crds, obj)
	}
	sort.Strings(problems)
	return crds, byName, problems
}

// crdlessTypes is the rule itself: the resources discovery lists (listedResources) in a group that
// is not built in, whose CRD (snapshotCRDs, by metadata.name) is not among the objects as the rule
// asks. A subresource entry counts as its resource: the name up to its first "/". The version is
// the group version after its first "/". Each offender is named once, sorted, as
//
//	<group/version> <plural> (no CustomResourceDefinition <plural>.<group>)
//
// or, for a CRD that is there:
//
//	<group/version> <plural> (CustomResourceDefinition <plural>.<group>: <reason> and <reason>)
//
// A CRD that names a version in more than one spec.versions entry is malformed (a real apiserver
// rejects it: spec.versions[i].name: Duplicate value) and no entry is chosen: that alone is said,
// `spec.versions names <the name> more than once`, for any name two entries share, the listed
// version's or not, a string or not; entries with no name share none. Only the CRD a listed custom
// type needs is asked this.
//
// Otherwise the reasons are everything else that is wrong with it:
//
//	spec.group is "x", expected "<group>"
//	spec.names.plural is "x", expected "<plural>"
//	does not serve <version>
//
// The version is served by a spec.versions entry with that name, exactly, and served the boolean
// true: a spec.versions that is missing, empty or not a list serves nothing, and so do an entry
// that is not an object or has no name, and a served that is absent or anything but true ("true",
// 1). (snapshotCRDs has already refused a CRD whose name is not its spec's; the spec is still
// compared here because one name can be cut into a plural and a group in more than one place.)
func crdlessTypes(listed map[string][]string, crds map[string]map[string]any) []string {
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
			spec, _ := crd["spec"].(map[string]any)
			specNames, _ := spec["names"].(map[string]any)
			versions, _ := spec["versions"].([]any)
			var wrong []string
			if twice, ok := sharedVersionName(versions); ok {
				wrong = []string{"spec.versions names " + twice + " more than once"}
			} else {
				for _, d := range []string{disagrees("spec.group", spec["group"], group), disagrees("spec.names.plural", specNames["plural"], plural)} {
					if d != "" {
						wrong = append(wrong, d)
					}
				}
				served := false
				for _, v := range versions {
					vm, _ := v.(map[string]any)
					served = served || (vm["name"] == version && vm["served"] == true)
				}
				if !served {
					wrong = append(wrong, "does not serve "+version)
				}
			}
			if len(wrong) > 0 {
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s: %s)", gv, plural, crdName, strings.Join(wrong, " and ")))
			}
		}
	}
	sort.Strings(out)
	return out
}

// sharedVersionName is the first spec.versions name two entries share, as JSON. Names are compared
// as JavaScript's === compares them, which is how agentiko-sandbox's validator finds one: two
// strings, two numbers of one value (1 and 1.0), two booleans or two nulls are the same name; an
// entry that is not an object or has no name has none; a name that is an object or a list equals
// no other.
func sharedVersionName(versions []any) (string, bool) {
	seen := map[string]bool{}
	for _, v := range versions {
		vm, _ := v.(map[string]any)
		name, has := vm["name"]
		if !has {
			continue
		}
		var key string
		switch n := name.(type) {
		case string:
			key = strconv.Quote(n)
		case json.Number:
			f, _ := n.Float64()                         // out of range is an infinity, as in JavaScript
			key = strconv.FormatFloat(f+0, 'g', -1, 64) // +0: -0 and 0 are one number
		case float64:
			key = strconv.FormatFloat(n+0, 'g', -1, 64)
		case bool:
			key = strconv.FormatBool(n)
		case nil:
			key = "null"
		default:
			continue
		}
		if seen[key] {
			return key, true
		}
		seen[key] = true
	}
	return "", false
}
