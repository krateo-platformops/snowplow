package jqinterest

import "testing"

// Witnesses for the reviewer's surviving mutations R2 (key field not read) and
// R5 (has() observes only the kind). Green on pristine source; must go RED
// under the mutation if the mutation is a real unsoundness.
func TestReviewerWitness_R2R5(t *testing.T) {
	env := emuEnv{identity: 1}
	r2 := oneStage(`[.s.items[] | select(.metadata.name == "a") | .spec.v]`, "")
	for _, b := range advCheck(t, "R2-witness", r2, []advPair{{"name a->b",
		[]any{advItem("a", "ns1", map[string]any{"v": "1"})}, []any{advItem("b", "ns1", map[string]any{"v": "1"})}, env}}) {
		t.Errorf("COUNTEREXAMPLE %s", b)
	}
	r5 := oneStage(`[.s.items[] | .spec | has("x")]`, "")
	for _, b := range advCheck(t, "R5-witness", r5, []advPair{{"add key x",
		[]any{advItem("a", "ns1", map[string]any{"v": "1"})}, []any{advItem("a", "ns1", map[string]any{"v": "1", "x": 1.0})}, env}}) {
		t.Errorf("COUNTEREXAMPLE %s", b)
	}
}
