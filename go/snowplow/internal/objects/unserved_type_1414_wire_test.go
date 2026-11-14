package objects

// unserved_type_1414_wire_test.go (#1414): what the read puts on the wire, and what it answers
// when the credential may not discover.

import (
	"fmt"
	"net/http"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
)

// A credential that may not discover (401 or 403 on /api and /apis) leaves the mapper without a
// copy at all. That is not a mapping miss: the apiserver is not asked about the resource, and the
// answer is the same whether the type is served or not: the discovery's own refusal (403), or a
// 500 (401). Never a 424, a 404 or an object.
func TestUnservedType_ARefusedDiscoveryIsNotAMappingMiss(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, legacy := range []bool{false, true} {
			for name, ref := range map[string]templatesv1.ObjectReference{
				"unserved_type":  ref1414(widgetsGV, "flexes", "header-context"),
				"served_object":  ref1414(widgetsGV, "buttons", "x"),
				"missing_object": ref1414(widgetsGV, "buttons", "gone"),
			} {
				t.Run(fmt.Sprintf("%d_legacy=%v_%s", code, legacy, name), func(t *testing.T) {
					a := &unservedapi.Server{DiscoveryRefused: code, LegacyDiscovery: legacy}
					res := get1414(t, a, false, ref)
					if res.Err == nil {
						t.Fatalf("the read succeeded though discovery was refused; requests: %v", a.Paths())
					}
					t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, a.Paths())
					// The answers as they were before #1414: a 403 on discovery is the read's
					// 403, a 401 is a 500.
					want := http.StatusInternalServerError
					if code == http.StatusForbidden {
						want = http.StatusForbidden
					}
					if res.Err.Code != want {
						t.Errorf("code = %d (%s), want %d", res.Err.Code, res.Err.Message, want)
					}
					for _, p := range a.Paths() {
						if p != "/api" && p != "/apis" {
							t.Errorf("after a refused discovery nothing else may be requested, saw %s", p)
						}
					}
				})
			}
		}
	}
}

// The object path is the reference's, namespace included, on the mapped route and on the
// mapper-miss route alike. The apiserver serves configmaps x in two namespaces, so a read that
// lost or changed its namespace would still find an object.
func TestUnservedType_TheDialledPathIsTheReferences(t *testing.T) {
	inNS := func(ref templatesv1.ObjectReference, ns string) templatesv1.ObjectReference {
		ref.Namespace = ns
		return ref
	}
	for _, c := range []struct {
		name string
		a    *unservedapi.Server
		ref  templatesv1.ObjectReference
		want string
		code int // 0: the object is read
	}{
		{"mapped_core_ns", &unservedapi.Server{}, ref1414("v1", "configmaps", "x"), "/api/v1/namespaces/ns/configmaps/x", 0},
		{"mapped_core_other", &unservedapi.Server{}, inNS(ref1414("v1", "configmaps", "x"), "other"), "/api/v1/namespaces/other/configmaps/x", 0},
		{"mapped_group", &unservedapi.Server{}, ref1414(widgetsGV, "buttons", "x"), "/apis/" + widgetsGV + "/namespaces/ns/buttons/x", 0},
		{"mapped_group_missing", &unservedapi.Server{}, ref1414(widgetsGV, "buttons", "gone"), "/apis/" + widgetsGV + "/namespaces/ns/buttons/gone", 404},
		{"miss_group", &unservedapi.Server{WidgetsDiscoveryLags: true}, ref1414(widgetsGV, "buttons", "x"), "/apis/" + widgetsGV + "/namespaces/ns/buttons/x", 0},
		{"miss_group_missing", &unservedapi.Server{WidgetsDiscoveryLags: true}, ref1414(widgetsGV, "buttons", "gone"), "/apis/" + widgetsGV + "/namespaces/ns/buttons/gone", 404},
		{"miss_group_other_namespace", &unservedapi.Server{WidgetsDiscoveryLags: true}, inNS(ref1414(widgetsGV, "buttons", "x"), "other"), "/apis/" + widgetsGV + "/namespaces/other/buttons/x", 404},
		{"miss_legacy_group", &unservedapi.Server{LegacyDiscovery: true, WidgetsDiscoveryLags: true, WidgetsDocFailures: 1}, ref1414(widgetsGV, "buttons", "x"), "/apis/" + widgetsGV + "/namespaces/ns/buttons/x", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := get1414(t, c.a, false, c.ref)
			all, _ := c.a.ObjectPaths()
			if len(all) != 1 || all[0] != c.want {
				t.Fatalf("the apiserver saw object paths %v, want exactly [%s]", all, c.want)
			}
			switch {
			case c.code == 0 && (res.Err != nil || res.Unstructured == nil || res.Unstructured.GetNamespace() != c.ref.Namespace):
				t.Fatalf("want the object in namespace %s, got err=%+v", c.ref.Namespace, res.Err)
			case c.code != 0 && (res.Err == nil || res.Err.Code != c.code):
				t.Fatalf("want %d, got %+v", c.code, res.Err)
			}
		})
	}
}
