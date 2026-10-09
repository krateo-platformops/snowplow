//go:build unit || integration

package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestIssue578_MarshalAsListRaw_ByteIdenticalToMarshalAsList is the falsifier
// for the whole #578 envelope change.
//
// marshalAsListRaw replaces a decode -> marshal round trip with a byte
// concatenation. That is only safe if the bytes it produces are EXACTLY what
// marshalAsList produced, because the envelope feeds JQ and the existing
// goldens. This test asserts byte equality, not semantic equality.
//
// MUTATION-CHECKED: this test goes RED under each of the realistic ways to get
// the concatenation wrong —
//   - emitting apiVersion/kind/items instead of the sorted apiVersion/items/kind
//   - omitting the comma between items
//   - emitting "items":null instead of "items":[] for the empty set
//   - hand-quoting apiVersion/kind instead of json.Marshal-ing them
//
// TestIssue578_MarshalAsListRaw_RejectsNaturalKeyOrder pins the first of those
// explicitly, since sorted-key-order is the single least obvious requirement.
func TestIssue578_MarshalAsListRaw_ByteIdenticalToMarshalAsList(t *testing.T) {
	cases := []struct {
		name       string
		apiVersion string
		listKind   string
		items      []map[string]any
	}{
		{
			name:       "empty set still emits items:[]",
			apiVersion: "v1",
			listKind:   "PodsList",
			items:      nil,
		},
		{
			name:       "single item",
			apiVersion: "observability.krateo.io/v1alpha1",
			listKind:   "IncidentsList",
			items: []map[string]any{
				{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident",
					"metadata": map[string]any{"name": "a", "namespace": "krateo-system"}},
			},
		},
		{
			name:       "several items exercise the separator",
			apiVersion: "v1",
			listKind:   "EventsList",
			items: []map[string]any{
				{"kind": "Event", "metadata": map[string]any{"name": "e1"}},
				{"kind": "Event", "metadata": map[string]any{"name": "e2"}},
				{"kind": "Event", "metadata": map[string]any{"name": "e3"}},
			},
		},
		{
			name:       "nested structures, arrays and mixed scalar types",
			apiVersion: "composition.krateo.io/v1-8-67",
			listKind:   "BuilderpublishesList",
			items: []map[string]any{
				{
					"kind": "Builderpublish",
					"metadata": map[string]any{
						"name":        "nested",
						"annotations": map[string]any{"z": "last", "a": "first"},
					},
					"spec": map[string]any{
						"replicas": int64(3),
						"ratio":    1.5,
						"enabled":  true,
						"disabled": false,
						"absent":   nil,
						"list":     []any{"x", int64(1), true, nil},
					},
				},
			},
		},
		{
			name:       "characters that require JSON escaping",
			apiVersion: `weird/v1"\\`,
			listKind:   "Quote\"List",
			items: []map[string]any{
				{"kind": "Thing", "metadata": map[string]any{
					"name":    "tab\there-newline\nhere-quote\"here-backslash\\here",
					"unicode": "héllo ☃ é",
				}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The decoded form marshalAsList consumes.
			items := make([]*unstructured.Unstructured, 0, len(tc.items))
			// The stored form marshalAsListRaw consumes. Produced the SAME way
			// newBytesObject produces `raw`: json.Marshal of the item map.
			raws := make([][]byte, 0, len(tc.items))
			for _, m := range tc.items {
				items = append(items, &unstructured.Unstructured{Object: m})
				b, err := json.Marshal(m)
				if err != nil {
					t.Fatalf("fixture marshal: %v", err)
				}
				raws = append(raws, b)
			}

			want, err := marshalAsList(tc.apiVersion, tc.listKind, items)
			if err != nil {
				t.Fatalf("marshalAsList: %v", err)
			}
			got, err := marshalAsListRaw(tc.apiVersion, tc.listKind, raws)
			if err != nil {
				t.Fatalf("marshalAsListRaw: %v", err)
			}

			if string(got) != string(want) {
				t.Fatalf("NOT byte-identical\n want %s\n  got %s", want, got)
			}

			// Non-vacuity: the fixture must actually have produced an envelope
			// with the item count we intended, so a bug that drops every item
			// cannot pass by making both sides equally empty.
			var probe struct {
				APIVersion string            `json:"apiVersion"`
				Kind       string            `json:"kind"`
				Items      []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(got, &probe); err != nil {
				t.Fatalf("result is not valid JSON: %v", err)
			}
			if len(probe.Items) != len(tc.items) {
				t.Fatalf("item count: want %d got %d", len(tc.items), len(probe.Items))
			}
			if probe.APIVersion != tc.apiVersion || probe.Kind != tc.listKind {
				t.Fatalf("envelope identity drifted: apiVersion=%q kind=%q", probe.APIVersion, probe.Kind)
			}
			// `items` must be an empty ARRAY, never null, so JQ `.items[]`
			// yields an empty stream (the marshalAsList contract).
			if len(tc.items) == 0 && !strings.Contains(string(got), `"items":[]`) {
				t.Fatalf("empty set must emit items:[] — got %s", got)
			}
		})
	}
}

// TestIssue578_MarshalAsListRaw_RejectsNaturalKeyOrder pins the sorted-key
// requirement directly.
//
// json.Marshal emits a map's keys sorted, so marshalAsList yields
// apiVersion, items, kind. Writing the envelope in the "natural" reading order
// apiVersion, kind, items produces semantically identical JSON that is NOT
// byte-identical. This test states that fact as an executable claim so a future
// edit that "tidies" the key order is caught here rather than by a golden
// diff somewhere downstream.
func TestIssue578_MarshalAsListRaw_RejectsNaturalKeyOrder(t *testing.T) {
	item := map[string]any{"kind": "Thing", "metadata": map[string]any{"name": "x"}}
	b, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}

	got, err := marshalAsListRaw("v1", "ThingsList", [][]byte{b})
	if err != nil {
		t.Fatalf("marshalAsListRaw: %v", err)
	}

	iAPI := strings.Index(string(got), `"apiVersion"`)
	iItems := strings.Index(string(got), `"items"`)
	iKind := strings.Index(string(got), `"kind":"ThingsList"`)
	if iAPI < 0 || iItems < 0 || iKind < 0 {
		t.Fatalf("envelope missing a top-level key: %s", got)
	}
	if !(iAPI < iItems && iItems < iKind) {
		t.Fatalf("top-level keys must appear in json.Marshal's SORTED order "+
			"(apiVersion, items, kind) to stay byte-identical to marshalAsList; got %s", got)
	}

	// And cross-check against the real thing rather than trusting the index
	// arithmetic alone.
	want, err := marshalAsList("v1", "ThingsList", []*unstructured.Unstructured{{Object: item}})
	if err != nil {
		t.Fatalf("marshalAsList: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("drift\n want %s\n  got %s", want, got)
	}
}

// TestIssue578_MarshalAsListRaw_SizeIsExact asserts the envelope is built in a
// single allocation's worth of capacity — the preallocation must be exact, not
// merely sufficient, because a short estimate silently reintroduces the
// reallocation-and-copy this change exists to remove.
func TestIssue578_MarshalAsListRaw_SizeIsExact(t *testing.T) {
	for _, n := range []int{0, 1, 2, 17} {
		t.Run(fmt.Sprintf("items=%d", n), func(t *testing.T) {
			raws := make([][]byte, 0, n)
			for i := 0; i < n; i++ {
				raws = append(raws, []byte(fmt.Sprintf(`{"i":%d}`, i)))
			}
			got, err := marshalAsListRaw("v1", "XsList", raws)
			if err != nil {
				t.Fatalf("marshalAsListRaw: %v", err)
			}
			if cap(got) != len(got) {
				t.Fatalf("preallocation not exact for %d items: len=%d cap=%d "+
					"(a mismatch means the size arithmetic drifted from the writes)",
					n, len(got), cap(got))
			}
		})
	}
}

// TestIssue578_MarshalAsListRaw_CeilingIsChecked proves the overflow guard on
// the size accumulation FIRES, rather than being a comment that asserts safety.
//
// CodeQL (go/allocation-size-overflow) flagged the first revision of
// marshalAsListRaw: the accumulated size feeds a make() capacity, and
// "each len(r) is already resident in memory so it cannot overflow" is a
// plausible argument, not a checked bound. The function now returns an error
// above a ceiling; the caller handles that exactly as it handles any marshal
// failure — fall through to the live apiserver — so the degenerate case is a
// correct serve, never a truncated envelope.
//
// The ceiling is lowered here instead of allocating 2 GiB. Restored via
// t.Cleanup so the production value cannot leak into a sibling test.
func TestIssue578_MarshalAsListRaw_CeilingIsChecked(t *testing.T) {
	orig := maxListEnvelopeBytes
	t.Cleanup(func() { maxListEnvelopeBytes = orig })

	item := []byte(`{"kind":"Thing","metadata":{"name":"x"}}`)

	// A ceiling comfortably above the fixed envelope scaffolding but below the
	// scaffolding plus this item: the per-item arm must reject.
	maxListEnvelopeBytes = 40
	if _, err := marshalAsListRaw("v1", "ThingsList", [][]byte{item}); err == nil {
		t.Fatalf("per-item arm did not reject an envelope over the ceiling")
	}

	// The separator arm has its own check, so it needs its own assertion: many
	// EMPTY items push the comma count over the ceiling without any single item
	// exceeding it.
	maxListEnvelopeBytes = 40
	many := make([][]byte, 200)
	for i := range many {
		many[i] = []byte(`1`)
	}
	if _, err := marshalAsListRaw("v1", "ThingsList", many); err == nil {
		t.Fatalf("separator arm did not reject an envelope over the ceiling")
	}

	// And at the production ceiling a normal envelope is unaffected — the guard
	// must not be a functional change for real lists.
	maxListEnvelopeBytes = orig
	got, err := marshalAsListRaw("v1", "ThingsList", [][]byte{item})
	if err != nil {
		t.Fatalf("production ceiling rejected a normal envelope: %v", err)
	}
	if cap(got) != len(got) {
		t.Fatalf("guard broke the exact preallocation: len=%d cap=%d", len(got), cap(got))
	}
}
