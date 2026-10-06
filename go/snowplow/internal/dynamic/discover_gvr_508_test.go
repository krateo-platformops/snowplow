package dynamic

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
)

// disco508 serves two CRD groups that share the plural `widgets`, both carrying
// the same category. That is the shape #508 is about, and it is not exotic: it
// becomes MORE likely as a cluster accumulates CRDs, which is the direction this
// product moves.
//
// Both resources deliberately leave APIResource.Group and .Version EMPTY, which
// is what a conformant apiserver sends: apimachinery documents them as "the
// preferred group of the resource. Empty implies the group of the containing
// resource list" (types.go:1180-1184). They are populated only for a resource
// that differs from its list, e.g. a subresource — which `scale` below exercises.
type disco508 struct {
	discovery.DiscoveryInterface // embed nil — unreached methods panic
}

func (disco508) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return []*metav1.APIResourceList{
		{
			GroupVersion: "widgets.krateo.io/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "widgets", Kind: "Widget", Namespaced: true, Categories: []string{"krateo"}},
			},
		},
		{
			GroupVersion: "legacy.example.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "widgets", Kind: "LegacyWidget", Namespaced: true, Categories: []string{"krateo"}},
				// A subresource whose group/version legitimately DIFFER from the
				// containing list: these per-resource fields must still WIN.
				{Name: "widgets/scale", Kind: "Scale", Group: "autoscaling", Version: "v1", Categories: []string{"krateo"}},
			},
		},
	}, nil
}

// TestIssue508_DiscoverCarriesGroupAndVersion is the falsifier.
//
// RED before the fix: Discover read APIResource.Group/.Version directly, which
// are empty for every ordinary resource, so BOTH widgets rows came back as
// GroupVersionResource{"", "", "widgets"} — indistinguishable from each other
// and carrying no group at all. The one consumer (handlers/list.go, GET
// /list?category=…) hands that to ListObjects → resourceInterfaceFor →
// mapper.KindFor, which then resolves by PLURAL ALONE across every group: fine
// while a plural is unique, wrong or ambiguous the moment it is not. Nothing
// reports it — no error, no counter, no log.
func TestIssue508_DiscoverCarriesGroupAndVersion(t *testing.T) {
	cli := newPerCallClient(t, disco508{})

	got, err := cli.Discover(context.Background(), "krateo")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	type gvr struct{ g, v, r string }
	seen := map[gvr]bool{}
	for _, x := range got {
		seen[gvr{x.Group, x.Version, x.Resource}] = true
		if x.Resource != "" && x.Version == "" {
			t.Errorf("#508 RED: %+v has NO VERSION — the group/version live on the containing list, "+
				"not on the APIResource, so KindFor would resolve this by plural alone", x)
		}
	}

	for _, want := range []gvr{
		{"widgets.krateo.io", "v1beta1", "widgets"},
		{"legacy.example.io", "v1", "widgets"},
	} {
		if !seen[want] {
			t.Errorf("#508 RED: missing %+v — two CRDs sharing a plural must stay DISTINGUISHABLE; got %+v", want, got)
		}
	}

	// The per-resource override is what those fields are actually for, so it must
	// still win: the scale subresource keeps autoscaling/v1, not its list's
	// legacy.example.io/v1.
	if !seen[gvr{"autoscaling", "v1", "widgets/scale"}] {
		t.Errorf("#508: a subresource whose APIResource.Group/.Version are SET must keep them "+
			"(that is the only case those fields are populated); got %+v", got)
	}

	if len(got) != 3 {
		t.Errorf("#508: expected exactly 3 discovered resources, got %d: %+v", len(got), got)
	}
}
