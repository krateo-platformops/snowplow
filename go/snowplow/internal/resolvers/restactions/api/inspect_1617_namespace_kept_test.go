package api

import (
	"context"
	"reflect"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// #1617 — a name that comes from a prior stage must not make a KNOWN namespace
// unknown. The empty name collapses the render (#1185) and inspect used to
// re-read the TEMPLATE, where the namespace segment is an expression, so the
// namespace the render had produced was thrown away.

// inspect1617 runs a two-stage RESTAction: a literal `prior` stage and a
// `target` stage depending on it, with the given extras.
func inspect1617(t *testing.T, targetPath string, extras map[string]any) []Resource {
	t.Helper()
	withInspectSARESTConfig(t, discovery504(t))
	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{API: []*templates.API{
		{Name: "prior", Path: "/api/v1/namespaces/ns1/configmaps/cd"},
		{Name: "target", Path: targetPath, DependsOn: &templates.Dependency{Name: "prior"}},
	}}}
	rows, unresolved, err := InspectReadSet(context.Background(), ra, extras)
	if err != nil {
		t.Fatalf("InspectReadSet: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved: %+v", unresolved)
	}
	return rows
}

// targetRow is the row of the target stage's resource (the prior stage reads
// configmaps).
func targetRow(t *testing.T, rows []Resource, resource string) Resource {
	t.Helper()
	for _, r := range rows {
		if r.Resource == resource {
			return r
		}
	}
	t.Fatalf("no %s row in %+v", resource, rows)
	return Resource{}
}

func TestIssue1617_NameCollapseKeepsNamespace(t *testing.T) {
	ns := map[string]any{"compositionNamespace": "ns1"}
	for _, tc := range []struct {
		name     string
		path     string
		resource string
		want     Resource
	}{
		{"core /api", `${ "/api/v1/namespaces/" + .compositionNamespace + "/services/" + .prior.name }`, "services",
			Resource{Version: "v1", Resource: "services", Namespace: "ns1", Verb: "get"}},
		{"group /apis", `${ "/apis/apps/v1/namespaces/" + .compositionNamespace + "/deployments/" + .prior.name }`, "deployments",
			Resource{Group: "apps", Version: "v1", Resource: "deployments", Namespace: "ns1", Verb: "get"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := targetRow(t, inspect1617(t, tc.path, ns), tc.resource)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("want %+v, got %+v", tc.want, got)
			}
		})
	}
}

// A collapsed NAMESPACE is still unknown, with or without a collapsed name.
func TestIssue1617_NamespaceCollapseStaysUnknown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		path     string
		resource string
		verb     string
	}{
		{"ns from prior, literal name", `${ "/api/v1/namespaces/" + .prior.ns + "/services/web" }`, "services", "get"},
		{"ns from prior, collection", `${ "/api/v1/namespaces/" + .prior.ns + "/services" }`, "services", "list"},
		{"ns and name from prior", `${ "/api/v1/namespaces/" + .prior.ns + "/services/" + .prior.name }`, "services", "get"},
		{"apis, ns from prior", `${ "/apis/apps/v1/namespaces/" + .prior.ns + "/deployments/x" }`, "deployments", "get"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := inspect1617(t, tc.path, map[string]any{"compositionNamespace": "ns1"})
			got := targetRow(t, rows, tc.resource)
			if !got.NamespaceUnknown || got.Namespace != "" || got.Verb != tc.verb {
				t.Fatalf("want unknown namespace with verb %s, got %+v", tc.verb, got)
			}
		})
	}
}

// A query value from a prior stage leaves the path whole: a concrete list row,
// correct before #1617, pinned here.
func TestIssue1617_QueryValueFromPriorStageStaysConcreteList(t *testing.T) {
	rows := inspect1617(t, `${ "/api/v1/namespaces/" + .compositionNamespace + "/services?labelSelector=" + (.prior.sel // "x") }`,
		map[string]any{"compositionNamespace": "ns1"})
	got := targetRow(t, rows, "services")
	want := Resource{Version: "v1", Resource: "services", Namespace: "ns1", Verb: "list"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}

// A known namespace with an INVALID name (refused by the #288 DNS-1123 check, not
// by the empty-segment one) keeps its namespace: a bad name says nothing about
// the namespace. On main this was namespaceUnknown; ns1 is the intended answer.
// Pins the second source of the namespace (setup.go, the validInterpolatedPath skip).
func TestIssue1617_InvalidNameKeepsKnownNamespace(t *testing.T) {
	rows := inspect1617(t, `${ "/api/v1/namespaces/" + .compositionNamespace + "/services/" + .badName }`,
		map[string]any{"compositionNamespace": "ns1", "badName": "Bad_Name"})
	got := targetRow(t, rows, "services")
	want := Resource{Version: "v1", Resource: "services", Namespace: "ns1", Verb: "get"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}

// An invalid rendered namespace is not kept: it stays unknown.
func TestIssue1617_InvalidRenderedNamespaceStaysUnknown(t *testing.T) {
	rows := inspect1617(t, `${ "/api/v1/namespaces/" + .compositionNamespace + "/services/" + .prior.name }`,
		map[string]any{"compositionNamespace": "Bad_NS"})
	got := targetRow(t, rows, "services")
	if !got.NamespaceUnknown || got.Namespace != "" {
		t.Fatalf("want unknown namespace, got %+v", got)
	}
}
