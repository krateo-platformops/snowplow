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
// resource needs its CRD, named <plural>.<group>.
func isBuiltinGroup(group string) bool {
	return group == "" || !strings.Contains(group, ".") || strings.HasSuffix(group, ".k8s.io")
}

// crdlessTypes is the rule applied to a snapshot: the resources its discovery lists in a group that
// is not built in, with no CustomResourceDefinition of apiextensions.k8s.io named <plural>.<group>,
// by exact name, among its objects. A subresource entry (<resource>/<sub>) counts as its resource.
// Each is "<group/version> <plural> (no CustomResourceDefinition <plural>.<group>)", once, sorted.
//
// A real apiserver derives a custom type's discovery entry from its CRD, so it never lists one
// without the other; the fake would serve such a type as an empty list it knows nothing about
// (no schema, no selectable fields), and nothing would say so (#1464).
func crdlessTypes(resources map[string]metav1.APIResourceList, objects []map[string]any) []string {
	crds := map[string]bool{}
	for _, obj := range objects {
		av, _ := obj["apiVersion"].(string)
		if g, _ := splitGV(av); obj["kind"] != "CustomResourceDefinition" || g != "apiextensions.k8s.io" {
			continue
		}
		md, _ := obj["metadata"].(map[string]any)
		if name, ok := md["name"].(string); ok {
			crds[name] = true
		}
	}
	var out []string
	for gv, list := range resources {
		group, _ := splitGV(gv)
		if isBuiltinGroup(group) {
			continue
		}
		seen := map[string]bool{}
		for _, r := range list.APIResources {
			plural, _, _ := strings.Cut(r.Name, "/")
			if crd := plural + "." + group; !crds[crd] && !seen[plural] {
				seen[plural] = true
				out = append(out, fmt.Sprintf("%s %s (no CustomResourceDefinition %s)", gv, plural, crd))
			}
		}
	}
	sort.Strings(out)
	return out
}
