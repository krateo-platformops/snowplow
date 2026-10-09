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

// TestIssue578_MarshalAsListRaw_CeilingIsChecked proves the size ceiling is
// ENFORCED at every input shape that can reach it.
//
// WHAT THIS TEST CANNOT PROVE, stated plainly because an earlier version of it
// claimed otherwise. marshalAsListRaw guards the size in two layers: a
// per-addend check before each accumulation, and a final check on `n`
// immediately before the make(). Those layers are MUTUALLY REDUNDANT for
// observable behaviour — measured by mutation:
//
//	remove the per-addend checks only -> this test still PASSES (final catches it)
//	remove the final check only       -> this test still PASSES (per-addend catches it)
//	remove BOTH                       -> 4 of 5 sub-cases FAIL
//
// So no black-box test can attribute a refusal to a particular arm, and sub-case
// names here describe the INPUT SHAPE, not the arm. The first version of this
// test asserted only `err != nil` at one ceiling of 40 — where a zero-item
// envelope already fails on the listKind header — so the two arms it named were
// never reached and both sub-cases passed for the wrong reason.
//
// WHY BOTH LAYERS STAY ANYWAY, since redundant code is normally a smell:
//   - the per-addend checks are what keep `n` from WRAPPING int before the final
//     check reads it. If `n` wrapped negative, `n > maxListEnvelopeBytes` would
//     be false and the make() would receive a bogus capacity. Demonstrating that
//     needs ~2^63 bytes of input, so it is unreachable by test and is the one
//     property here that rests on the argument rather than on a measurement.
//   - they are also what makes the bound LEGIBLE to CodeQL: each puts `len(x)`
//     on the smaller side of the comparison, the form the Go query
//     go/allocation-size-overflow recognises as a length check, which is what
//     sanitizes `av` and `lk` (json.Marshal output of a non-constant string is a
//     "potentially large value" to that query).
//
// The byte arithmetic the boundaries below are derived from:
//
//	pfxAPIVersion `{"apiVersion":`  14
//	pfxItems      `,"items":[`      10
//	pfxKind       `],"kind":`        9
//	suffix        `}`                1   => scaffolding 34
//	av  json.Marshal("v1")           4   => 38
//	lk  json.Marshal("ThingsList")  12   => 50  (an empty envelope is exactly 50)
//	one fixture item                40   => 90  (a one-item envelope)
func TestIssue578_MarshalAsListRaw_CeilingIsChecked(t *testing.T) {
	orig := maxListEnvelopeBytes
	t.Cleanup(func() { maxListEnvelopeBytes = orig })

	const emptyEnvelope = 50 // scaffolding + av + lk
	item := []byte(`{"kind":"Thing","metadata":{"name":"x"}}`)
	if len(item) != 40 {
		t.Fatalf("fixture item is %d bytes; the boundaries below assume 40", len(item))
	}

	t.Run("refuses a ceiling below the fixed scaffolding", func(t *testing.T) {
		// Below the fixed scaffolding nothing can be assembled at all.
		maxListEnvelopeBytes = 10
		if _, err := marshalAsListRaw("v1", "ThingsList", nil); err == nil {
			t.Fatalf("a ceiling below the fixed scaffolding must be refused")
		}
	})

	t.Run("refuses one byte under an empty envelope, accepts exactly one", func(t *testing.T) {
		// One byte under an empty envelope: the listKind header is what pushes it
		// over, and there are no items to blame.
		maxListEnvelopeBytes = emptyEnvelope - 1
		if _, err := marshalAsListRaw("v1", "ThingsList", nil); err == nil {
			t.Fatalf("ceiling %d must refuse an empty envelope needing %d",
				emptyEnvelope-1, emptyEnvelope)
		}
		// Exactly an empty envelope: accepted, so the refusal above was the
		// header arm and not an off-by-default rejection of everything.
		maxListEnvelopeBytes = emptyEnvelope
		got, err := marshalAsListRaw("v1", "ThingsList", nil)
		if err != nil {
			t.Fatalf("ceiling %d must accept an empty envelope: %v", emptyEnvelope, err)
		}
		if len(got) != emptyEnvelope {
			t.Fatalf("empty envelope is %d bytes; the arithmetic in this test is stale", len(got))
		}
	})

	t.Run("accepts the headers but refuses an item that does not fit", func(t *testing.T) {
		// Room for the headers but not for one 40-byte item: empty succeeds,
		// one item fails. That is the per-item arm and nothing else.
		maxListEnvelopeBytes = emptyEnvelope + 10
		if _, err := marshalAsListRaw("v1", "ThingsList", nil); err != nil {
			t.Fatalf("empty envelope must still fit under ceiling %d: %v",
				emptyEnvelope+10, err)
		}
		if _, err := marshalAsListRaw("v1", "ThingsList", [][]byte{item}); err == nil {
			t.Fatalf("one %d-byte item must not fit in %d bytes of headroom",
				len(item), 10)
		}
	})

	t.Run("counts the separators toward the ceiling", func(t *testing.T) {
		// To isolate the SEPARATOR arm the items must contribute nothing, so
		// this uses zero-length elements. That input is UNREACHABLE in
		// production — rawItemJSON drops a bytesObject with empty raw, and both
		// json.Marshal arms return at least "{}" — so this exercises the arm's
		// arithmetic, not a reachable serve. With 1-byte items the item total
		// always exceeds the comma count, so no realistic input can isolate it.
		maxListEnvelopeBytes = emptyEnvelope + 5
		few := [][]byte{{}, {}} // 1 comma
		if _, err := marshalAsListRaw("v1", "ThingsList", few); err != nil {
			t.Fatalf("1 separator must fit in 5 bytes of headroom: %v", err)
		}
		many := make([][]byte, 10) // 9 commas
		for i := range many {
			many[i] = []byte{}
		}
		if _, err := marshalAsListRaw("v1", "ThingsList", many); err == nil {
			t.Fatalf("9 separators must not fit in 5 bytes of headroom")
		}
	})

	t.Run("production ceiling leaves a normal envelope untouched", func(t *testing.T) {
		maxListEnvelopeBytes = orig
		got, err := marshalAsListRaw("v1", "ThingsList", [][]byte{item})
		if err != nil {
			t.Fatalf("production ceiling rejected a normal envelope: %v", err)
		}
		if cap(got) != len(got) {
			t.Fatalf("the guard broke exact preallocation: len=%d cap=%d", len(got), cap(got))
		}
		// And the ceiling must be positive on this target — a negative ceiling
		// would make the remaining budget negative and refuse every list.
		if maxListEnvelopeBytes <= 0 {
			t.Fatalf("maxListEnvelopeBytes is %d; a ceiling must be positive on every "+
				"target the module builds for (1<<31 overflows a 32-bit int)",
				maxListEnvelopeBytes)
		}
	})
}
