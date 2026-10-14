package jqinterest

import (
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

func hasWholeRead(in Interest, path string) bool {
	for _, r := range in.Reads {
		if r.Path.String() == path && r.Mode == ModeWhole {
			return true
		}
	}
	return false
}

// TestIssue683_SelectorKeysAreInTheReadSet: what each query shape adds to
// the read set, and the fixed witness, RED by construction under
// selector-paths-dropped and silent with the rule. The witness is
// reviewer-603's ?labelSelector=tier=web with a tier web -> db UPDATE. It
// covers a plain list cell and a membership-keyed one, whose select runs
// only on the objects the selector already admitted.
func TestIssue683_SelectorKeysAreInTheReadSet(t *testing.T) {
	const tier, labelsMap = `."metadata"."labels"."tier"`, `."metadata"."labels"`
	for _, tc := range []struct {
		name, path    string
		want, notRead []string
	}{
		{"labelSelector equality", "/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb", []string{tier}, []string{labelsMap}},
		{"set-based and existence", "/apis/g/v1/namespaces/ns/s?labelSelector=tier+in+%28web%2Capi%29%2C%21legacy%2Cteam",
			[]string{tier, `."metadata"."labels"."legacy"`, `."metadata"."labels"."team"`}, []string{labelsMap}},
		{"labelSelector next to a param that makes the call fall through", "/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb&limit=5", []string{tier}, nil},
		{"templated path: the whole labels map", `${ "/apis/g/v1/namespaces/ns/s?labelSelector=tier%3D" + "web" }`, []string{labelsMap}, nil},
		{"name/namespace fieldSelector: immutable, nothing added", "/apis/g/v1/namespaces/ns/s?fieldSelector=metadata.name%3Da", nil, []string{tier, labelsMap}},
		{"no query: nothing added", "/apis/g/v1/namespaces/ns/s", nil, []string{tier, labelsMap}},
	} {
		in := Analyze(selectorStage(tc.path, `[.s.items[] | .spec.v]`), jqsupport.ModuleLoader())
		if in.All {
			t.Errorf("%s: unexpectedly All: %s", tc.name, in)
			continue
		}
		for _, w := range tc.want {
			if !hasWholeRead(in, w) {
				t.Errorf("%s: read set lacks a whole read of %s: %s", tc.name, w, in)
			}
		}
		for _, n := range tc.notRead {
			if hasWholeRead(in, n) {
				t.Errorf("%s: read set has %s, which the query does not need: %s", tc.name, n, in)
			}
		}
	}

	for _, a := range []arm{
		{"plain list cell", mutSelectorPathsDropped,
			selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb", `[.s.items[] | .metadata.name]`)},
		{"membership-keyed cell", mutSelectorPathsDropped,
			selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb", `[.s.items[] | select(.metadata.name == "a") | .spec.v]`)},
	} {
		if witnessCounterexample(a) {
			t.Errorf("%s: the tier web->db witness fires with the rule in place", a.name)
		}
		testMutations = map[string]bool{a.mutation: true}
		red := witnessCounterexample(a)
		mut := Analyze(a.chain, jqsupport.ModuleLoader())
		testMutations = nil
		if !red {
			t.Errorf("%s: the witness does not fire under %s (interest %s)", a.name, a.mutation, mut)
			continue
		}
		t.Logf("RED by witness under %s: %s (mutated interest %s)", a.mutation, a.name, mut)
	}
}
