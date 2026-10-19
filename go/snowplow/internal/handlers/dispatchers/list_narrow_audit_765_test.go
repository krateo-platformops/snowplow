// list_narrow_audit_765_test.go — #603 PR-2 / #765 SHADOW: the WIRING arm of
// the would-change audit. The audit classifier can be right and never called
// (the #574 "shipped and inert" failure): this drives the REAL refresher
// terminal (resolveAndPopulateL1) under an avoid-only dequeue token and
// asserts the audit moved. RED without the NoteNarrowAuditWrite call in
// resolve_populate.go.
package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestListNarrow765_AuditIsWiredAtTheRefresherTerminal(t *testing.T) {
	c := instrumentCacheOn(t)
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)

	run := func(name string, prior, produces []byte) map[string]uint64 {
		t.Helper()
		inputs := instrumentInputs(name)
		key := cache.ComputeKey(inputs)
		c.Put(key, &cache.ResolvedEntry{RawJSON: prior, Inputs: &inputs})
		prev := resolveOnceFn
		resolveOnceFn = func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) { return produces, nil }
		defer func() { resolveOnceFn = prev }()
		before := cache.ListNarrowAuditSnapshot()
		if err := resolveAndPopulateL1(cache.WithNarrowAuditAvoidOnlyForTest(context.Background(), key), inputs, nil, nil); err != nil {
			t.Fatalf("resolveAndPopulateL1: %v", err)
		}
		after := cache.ListNarrowAuditSnapshot()
		delta := map[string]uint64{}
		for k, v := range after {
			delta[k] = v - before[k]
		}
		return delta
	}
	if d := run("n765-changed", []byte(`{"items":[{"a":1}]}`), []byte(`{"items":[{"a":2}]}`)); d[cache.AuditVerdictChanged] != 1 {
		t.Fatalf("#765 audit NOT WIRED: an avoid-only refresh that changed the bytes did not count a "+
			"would_change at the real refresher terminal (delta %v)", d)
	}
	if d := run("n765-identical", []byte(`{"items":[{"a":1}]}`), []byte(`{"items":[{"a":1}]}`)); d[cache.AuditVerdictIdentical] != 1 || d[cache.AuditVerdictChanged] != 0 {
		t.Fatalf("#765 audit: an avoid-only refresh reproducing the bytes must count identical (delta %v)", d)
	}
}
