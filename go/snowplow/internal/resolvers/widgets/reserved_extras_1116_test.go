package widgets

// #1116 — the widget path hands the request extras to its apiRef RESTAction;
// restactions.Resolve refuses reserved keys there with an
// *api.ReservedExtrasError. widgets.Resolve must return it findable through
// its stage wrapping, so the dispatcher answers the 400 (not a 500).

import (
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestReservedExtras1116_WidgetResolveKeepsTheRefusal(t *testing.T) {
	refusal := api.CheckReservedExtras([]*templatesv1.API{{Name: "crds"}}, map[string]any{"crds": []any{}})
	if refusal == nil {
		t.Fatal("setup: no refusal")
	}
	stubApiRef(t, nil, refusal)
	w := baseWidget()
	_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w, Extras: map[string]any{"crds": []any{}}})
	re, ok := api.AsReservedExtrasError(err)
	if !ok || len(re.Keys) != 1 || re.Keys[0].Key != "crds" {
		t.Fatalf("widgets.Resolve: want the #1116 refusal naming crds through its wrapping, got %v", err)
	}
}
