package cache

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// #1617 — ReadSetFromCollapsed keeps the namespace the render produced and
// falls back to the skeleton's verdict when that segment did not render.
func TestReadSetFromCollapsed_1617(t *testing.T) {
	core := `${ "/api/v1/namespaces/" + .ns + "/pods/" + .n }`
	grp := `${ "/apis/apps/v1/namespaces/" + .ns + "/deployments/" + .n }`
	gp := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	gd := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	for _, tc := range []struct {
		name, tmpl, rendered string
		want                 ReadSetCoord
	}{
		{"core, name collapsed", core, "/api/v1/namespaces/ns1/pods/", ReadSetCoord{GVR: gp, Namespace: "ns1", Name: nameTemplated, OK: true}},
		{"apis, name collapsed", grp, "/apis/apps/v1/namespaces/ns1/deployments/", ReadSetCoord{GVR: gd, Namespace: "ns1", Name: nameTemplated, OK: true}},
		{"core, namespace collapsed", core, "/api/v1/namespaces//pods/x", ReadSetCoord{GVR: gp, NamespaceUnknown: true, Name: nameTemplated, OK: true}},
		{"core, both collapsed", core, "/api/v1/namespaces//pods/", ReadSetCoord{GVR: gp, NamespaceUnknown: true, Name: nameTemplated, OK: true}},
		{"apis, namespace collapsed", grp, "/apis/apps/v1/namespaces//deployments/x", ReadSetCoord{GVR: gd, NamespaceUnknown: true, Name: nameTemplated, OK: true}},
		{"no render available", core, "", ReadSetCoord{GVR: gp, NamespaceUnknown: true, Name: nameTemplated, OK: true}},
		{"invalid namespace", core, "/api/v1/namespaces/Bad_NS/pods/", ReadSetCoord{GVR: gp, NamespaceUnknown: true, Name: nameTemplated, OK: true}},
		{"query ignored", core, "/api/v1/namespaces/ns1/pods/?x=1", ReadSetCoord{GVR: gp, Namespace: "ns1", Name: nameTemplated, OK: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReadSetFromCollapsed(tc.tmpl, RenderedNamespace(tc.rendered)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("want %+v, got %+v", tc.want, got)
			}
		})
	}
}
