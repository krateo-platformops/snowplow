//go:build unit || integration

package api

import (
	"encoding/json"
	"expvar"
	"testing"
)

// TestIssue578_DispatchShapeIsPublishedAndReadableAsARatio pins the instrument
// that accepts or refutes #578's envelope half.
//
// The metric this REPLACES (allocation rate / pod CPU) moved 149x on 057 in one
// afternoon because another component's retry loop was fixed. A share-of-serves
// ratio cannot move for that reason, which is the whole point.
func TestIssue578_DispatchShapeIsPublishedAndReadableAsARatio(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	RegisterInformerDispatchShapeExpvarForTest()

	v := expvar.Get("snowplow_informer_dispatch_shape")
	if v == nil {
		t.Fatalf("snowplow_informer_dispatch_shape is not published; #578's envelope half has no " +
			"instrument, and CPU/allocation were already shown to be confounded")
	}
	var got map[string]int64
	if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
		t.Fatalf("published value is not a flat int map: %v (%s)", err, v.String())
	}
	// BOTH halves of the ratio must be present, or the reading is impossible.
	for _, k := range []string{"list_served", "list_served_raw"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("%q missing — the ratio needs both terms; got %v", k, got)
		}
	}
	// And the denominators that let a zero ratio be told from an idle pod.
	for _, k := range []string{"get_served", "fallthrough", "rbac_dropped", "sync_wait_served"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("%q missing — without it a zero ratio cannot be distinguished from "+
				"an idle pivot; got %v", k, got)
		}
	}

	// The raw counter must TRACK the dedicated path, not be decorative: bumping it
	// must move the published value.
	before := got["list_served_raw"]
	dispatchInformerListServedRaw.Add(1)
	var after map[string]int64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_informer_dispatch_shape").String()), &after); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if after["list_served_raw"] != before+1 {
		t.Fatalf("the published value does not track the counter (%d -> %d); it is decorative",
			before, after["list_served_raw"])
	}
}
