package widgets

import "testing"

// #1135 — plumbing v1.26 fixed maps.NestedValue: a missing last key used to read
// as (nil, true), which NestedString turned into an error; it now reads as not
// found. These accessors map both to "", so the fix changes nothing here, and
// a value of the wrong type still reads as "" too.
func TestIssue1135_MetadataAccessorsReadAMissingKeyAsEmpty(t *testing.T) {
	for name, obj := range map[string]map[string]any{
		"missing_leaf":   {"metadata": map[string]any{}},
		"missing_parent": {},
		"wrong_type": {
			"apiVersion": 1, "kind": 1,
			"metadata": map[string]any{"namespace": 1, "name": 1, "uid": 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			for field, got := range map[string]string{
				"apiVersion": GetAPIVersion(obj),
				"kind":       GetKind(obj),
				"namespace":  GetNamespace(obj),
				"name":       GetName(obj),
				"uid":        GetUID(obj),
			} {
				if got != "" {
					t.Errorf("%s = %q, want empty", field, got)
				}
			}
		})
	}
	full := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"namespace": "ns", "name": "n", "uid": "u"},
	}
	if GetAPIVersion(full) != "v1" || GetKind(full) != "Pod" || GetNamespace(full) != "ns" || GetName(full) != "n" || GetUID(full) != "u" {
		t.Fatal("control: a present value reads back")
	}
}
