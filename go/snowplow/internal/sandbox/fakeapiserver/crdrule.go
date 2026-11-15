//go:build js && wasm

package fakeapiserver

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// isBuiltinGroup is SNAPSHOT.md §3's one snapshot rule, in the words agentiko-sandbox keeps it in
// (snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs, both isBuiltinGroup):
//
// Built-in (needs no CRD): the core group, groups without a dot, and *.k8s.io. Every other group's
// resource needs its CRD, named <plural>.<group>, and that CRD must serve the listed version
// (spec.versions[].name with served: true).
func isBuiltinGroup(group string) bool {
	return group == "" || !strings.Contains(group, ".") || strings.HasSuffix(group, ".k8s.io")
}

// crdlessTypes is the rule applied to a snapshot: the resources its discovery lists in a group that
// is not built in, with no CustomResourceDefinition of apiextensions.k8s.io named <plural>.<group>,
// by exact name, among its objects, or with one that does not serve the group version's version. A
// subresource entry (<resource>/<sub>) counts as its resource. Each is, once and sorted,
// "<group/version> <plural> (no CustomResourceDefinition <plural>.<group>)",
// "<group/version> <plural> (CustomResourceDefinition <plural>.<group> does not serve <version>)" or
// "<group/version> <plural> (CustomResourceDefinition <plural>.<group> names <version> more than once)".
//
// A version is served by a spec.versions entry with that name, exactly, and served the boolean true:
// a spec.versions that is missing, empty or not a list, an entry that is not an object or has no
// name, and a served that is absent or anything but true ("true", 1) serve nothing. A CRD that names
// the listed version in more than one entry is one a real apiserver rejects (spec.versions[i].name:
// Duplicate value), so which entry would count is not decided here: the type is refused.
//
// A real apiserver derives a custom type's discovery entry from its CRD, so it never lists one
// without the other; the fake would serve such a type as an empty list it knows nothing about
// (no schema, no selectable fields), and nothing would say so (#1464).
func crdlessTypes(resources map[string]metav1.APIResourceList, objects []map[string]any) []string {
	type versions struct{ served, twice map[string]bool }
	crds := map[string]versions{} // by CRD name
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
		vs := versions{served: map[string]bool{}, twice: map[string]bool{}}
		named := map[string]bool{}
		spec, _ := obj["spec"].(map[string]any)
		list, _ := spec["versions"].([]any)
		for _, v := range list {
			vm, _ := v.(map[string]any)
			n, ok := vm["name"].(string)
			if !ok {
				continue
			}
			if named[n] {
				vs.twice[n] = true
			}
			named[n] = true
			if vm["served"] == true {
				vs.served[n] = true
			}
		}
		crds[name] = vs
	}
	var out []string
	for gv, list := range resources {
		group, version := splitGV(gv)
		if isBuiltinGroup(group) {
			continue
		}
		seen := map[string]bool{}
		for _, r := range list.APIResources {
			plural, _, _ := strings.Cut(r.Name, "/")
			if seen[plural] {
				continue
			}
			seen[plural] = true
			crd := plural + "." + group
			if vs, ok := crds[crd]; !ok {
				out = append(out, fmt.Sprintf("%s %s (no CustomResourceDefinition %s)", gv, plural, crd))
			} else if vs.twice[version] {
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s names %s more than once)", gv, plural, crd, version))
			} else if !vs.served[version] {
				out = append(out, fmt.Sprintf("%s %s (CustomResourceDefinition %s does not serve %s)", gv, plural, crd, version))
			}
		}
	}
	sort.Strings(out)
	return out
}
