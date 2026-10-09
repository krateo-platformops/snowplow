//go:build unit || integration

package cache

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestIssue578_StoredRawIsByteIdenticalToReMarshalledDecode is THE correctness
// claim underneath #578.
//
// The envelope change replaces, per item:
//
//	json.Marshal(bytesObject.Decode().Object)      (the pre-#578 path)
//
// with:
//
//	bytesObject.raw                                 (the #578 path)
//
// Those are only interchangeable if a stored `raw` equals the re-marshalling of
// its own decoded form, BYTE FOR BYTE. The subtle way that fails is number
// fidelity: decoding JSON into interface{} with the stdlib lands every number
// on float64, so an int64 replica count of 3 would come back as 3 but a large
// integer would re-marshal in exponent form and silently differ. apimachinery
// decodes with sigs.k8s.io/json's PreserveInts behaviour precisely to avoid
// that — this test is the executable proof that the property holds for the
// shapes real Kubernetes objects carry, rather than an assumption in a comment.
//
// If this test ever goes red, the raw envelope path is NOT safe and must be
// reverted — not patched around.
func TestIssue578_StoredRawIsByteIdenticalToReMarshalledDecode(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]any
	}{
		{
			name: "plain object",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "a", "namespace": "krateo-system"},
			},
		},
		{
			name: "integers of several magnitudes",
			obj: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "d", "namespace": "ns"},
				"spec": map[string]any{
					"replicas":   int64(3),
					"generation": int64(1791534491122175009), // a real resourceVersion magnitude
					"zero":       int64(0),
					"negative":   int64(-42),
					"largeInt":   int64(1 << 53),
					"biggerInt":  int64(1<<62 - 1),
				},
			},
		},
		{
			name: "floats, bools, nulls and empty containers",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Thing",
				"metadata":   map[string]any{"name": "t"},
				"spec": map[string]any{
					"ratio":      1.5,
					"wholeFloat": float64(2),
					"enabled":    true,
					"disabled":   false,
					"absent":     nil,
					"emptyMap":   map[string]any{},
					"emptyList":  []any{},
				},
			},
		},
		{
			name: "deep nesting and arrays of objects",
			obj: map[string]any{
				"apiVersion": "observability.krateo.io/v1alpha1",
				"kind":       "Incident",
				"metadata":   map[string]any{"name": "inc", "namespace": "krateo-system"},
				"status": map[string]any{
					"checks": []any{
						map[string]any{"at": "2026-10-09T08:10:33Z", "exit": int64(1), "script": "precondition"},
						map[string]any{"at": "2026-10-09T08:11:35Z", "exit": int64(1), "script": "precondition"},
					},
					"firings": int64(18),
					"conditions": []any{
						map[string]any{"type": "Synced", "status": "True"},
					},
				},
			},
		},
		{
			name: "strings needing escapes and non-ASCII",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Thing",
				"metadata": map[string]any{
					"name": "esc",
					"annotations": map[string]any{
						"quote":     `he said "hi"`,
						"backslash": `C:\path\to`,
						"newline":   "line1\nline2",
						"tab":       "a\tb",
						"unicode":   "héllo ☃",
						"html":      "<b>&</b>",
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uns := &unstructured.Unstructured{Object: tc.obj}

			bo, err := newBytesObject(uns)
			if err != nil {
				t.Fatalf("newBytesObject: %v", err)
			}
			if len(bo.raw) == 0 {
				t.Fatalf("stored raw is empty — nothing is being tested")
			}

			decoded, err := bo.Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			reMarshalled, err := json.Marshal(decoded.Object)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}

			if string(bo.raw) != string(reMarshalled) {
				t.Fatalf("stored raw is NOT byte-identical to re-marshalled decode.\n"+
					"The #578 raw envelope path would serve different bytes than the decoded path.\n"+
					"  raw: %s\n  re-: %s", bo.raw, reMarshalled)
			}

			// Non-vacuity: the fixture must have survived into the stored bytes.
			// Without this a bug that stores "{}" would pass trivially.
			var probe map[string]any
			if err := json.Unmarshal(bo.raw, &probe); err != nil {
				t.Fatalf("stored raw is not valid JSON: %v", err)
			}
			if probe["kind"] != tc.obj["kind"] {
				t.Fatalf("fixture did not survive storage: kind=%v", probe["kind"])
			}
		})
	}
}

// TestIssue578_RawItemJSON_HandlesBothIndexerShapes — an indexer slot holds a
// *bytesObject for virtually every GVR (Ship H5: streaming unless excepted),
// but can hold a plain *unstructured.Unstructured for the typed-RBAC exception
// and for a newBytesObject marshal failure. Both must render, and both must
// render to the SAME bytes the decoded path would have produced, so the
// fallback is a cost difference and never a correctness difference.
func TestIssue578_RawItemJSON_HandlesBothIndexerShapes(t *testing.T) {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "cm", "namespace": "ns"},
		"data":       map[string]any{"k": "v"},
	}
	want, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}

	bo, err := newBytesObject(&unstructured.Unstructured{Object: obj})
	if err != nil {
		t.Fatalf("newBytesObject: %v", err)
	}

	gotBytes, ok := rawItemJSON(bo)
	if !ok {
		t.Fatalf("bytesObject arm must render")
	}
	if string(gotBytes) != string(want) {
		t.Fatalf("bytesObject arm drifted\n want %s\n  got %s", want, gotBytes)
	}

	gotUns, ok := rawItemJSON(&unstructured.Unstructured{Object: obj})
	if !ok {
		t.Fatalf("Unstructured fallback arm must render")
	}
	if string(gotUns) != string(want) {
		t.Fatalf("Unstructured fallback drifted\n want %s\n  got %s", want, gotUns)
	}

	// Degenerate slots are DROPPED, matching listFromIndexer's behaviour of
	// dropping an item it cannot convert — a bad object must never fail the
	// whole list.
	for name, in := range map[string]interface{}{
		"nil bytesObject":  (*bytesObject)(nil),
		"nil Unstructured": (*unstructured.Unstructured)(nil),
		"unrelated type":   struct{ X int }{1},
		"empty raw":        &bytesObject{},
	} {
		if _, ok := rawItemJSON(in); ok {
			t.Fatalf("%s must be dropped, not rendered", name)
		}
	}
}
