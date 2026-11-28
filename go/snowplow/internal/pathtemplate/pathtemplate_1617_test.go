package pathtemplate

import (
	"fmt"
	"strings"
	"testing"
)

// #1617 — an empty-segment refusal carries the path as rendered, for the
// read-set enumeration, and never prints it.
func TestEmptySegmentRefusalCarriesTheRender(t *testing.T) {
	_, err := Render(t.Context(), `${ "/api/v1/namespaces/" + .ns + "/pods/" + .n }`, map[string]any{"ns": "ns1"})
	got, ok := RenderedOfEmptySegment(err)
	if !ok || got.Value() != "/api/v1/namespaces/ns1/pods/" {
		t.Fatalf("want the render with its empty name, got %q ok=%v err=%v", got, ok, err)
	}
	if strings.Contains(err.Error(), "ns1") {
		t.Fatalf("the refusal text must not carry the render: %v", err)
	}
	if s := fmt.Sprintf("%v %+v %#v", got, *err.(*RefusedError), got); strings.Contains(s, "ns1") {
		t.Fatalf("a formatted refusal must not carry the render: %s", s)
	}
	if _, ok := RenderedOfEmptySegment(&RefusedError{Reason: ReasonDotSegment, Rendered: "x"}); ok {
		t.Fatal("only an empty-segment refusal exposes a render")
	}
}
