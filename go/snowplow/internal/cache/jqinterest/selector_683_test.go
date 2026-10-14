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

// labelled is an item named a in ns whose labels are exactly lbls.
func labelled(lbls map[string]any) map[string]any {
	it := advItem("a", "ns", map[string]any{"v": "1"})
	if lbls != nil {
		it["metadata"].(map[string]any)["labels"] = lbls
	}
	return it
}

// selectorWitnessFires reports whether the UPDATE before -> after keeps the
// item's projection on the analysed read set while changing the emulated
// output: an under-invalidation.
func selectorWitnessFires(c Chain, before, after map[string]any) (bool, Interest, emuOutcome, emuOutcome) {
	in := Analyze(c, jqsupport.ModuleLoader())
	x, y := emulate(c, []any{before}, emuEnv{identity: 1}), emulate(c, []any{after}, emuEnv{identity: 1})
	if in.All {
		return false, in, x, y
	}
	if string(Project(before, in.Reads)) != string(Project(after, in.Reads)) {
		return false, in, x, y
	}
	return (okOutcome(x) || okOutcome(y)) && !sameOutcome(x, y), in, x, y
}

// TestIssue683_EverySelectorShapeMarks: one fixed witness per selector shape.
// Each UPDATE changes ONLY labels and moves the object in or out of the
// selected list, so the output changes. With the rule in place the read set
// sees it (green); under selector-paths-dropped it does not (RED). Main before
// #683 had no selector rule at all, which is exactly the mutated analysis.
func TestIssue683_EverySelectorShapeMarks(t *testing.T) {
	const name = `[.s.items[] | .metadata.name]`
	reviewer := Chain{RA: RESTAction{Stages: []Stage{{Name: "things",
		Path:   "/apis/g/v1/namespaces/ns/things?labelSelector=tier%3Dweb",
		Filter: strp(`[.things.items[] | .metadata.name]`)}}}, Tracked: "things"}
	for _, tc := range []struct {
		name          string
		chain         Chain
		before, after map[string]any
	}{
		{"reviewer-603: tier=web, tier web->db", reviewer,
			labelled(map[string]any{"tier": "web"}), labelled(map[string]any{"tier": "db"})},
		{"reviewer-603: tier=web, tier db->web (moves IN)", reviewer,
			labelled(map[string]any{"tier": "db"}), labelled(map[string]any{"tier": "web"})},
		{"reviewer-603: tier=web, labels removed", reviewer,
			labelled(map[string]any{"tier": "web"}), labelled(nil)},
		{"!= : tier!=web, db->web", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier%21%3Dweb", name),
			labelled(map[string]any{"tier": "db"}), labelled(map[string]any{"tier": "web"})},
		{"in : tier in (web,api), api->db", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier+in+%28web%2Capi%29", name),
			labelled(map[string]any{"tier": "api"}), labelled(map[string]any{"tier": "db"})},
		{"notin : tier notin (web), db->web", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier+notin+%28web%29", name),
			labelled(map[string]any{"tier": "db"}), labelled(map[string]any{"tier": "web"})},
		{"exists : team, label added", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=team", name),
			labelled(map[string]any{"tier": "web"}), labelled(map[string]any{"tier": "web", "team": "x"})},
		{"!key : !legacy, label added", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=%21legacy", name),
			labelled(map[string]any{"tier": "web"}), labelled(map[string]any{"tier": "web", "legacy": "true"})},
		{"multiple keys: tier=web,env=prod, env prod->dev", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb%2Cenv%3Dprod", name),
			labelled(map[string]any{"tier": "web", "env": "prod"}), labelled(map[string]any{"tier": "web", "env": "dev"})},
		{"multiple keys, set-based mix: env in (prod),!legacy, legacy added", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=env+in+%28prod%29%2C%21legacy", name),
			labelled(map[string]any{"env": "prod"}), labelled(map[string]any{"env": "prod", "legacy": "y"})},
		{"membership-keyed: select(.metadata.name == \"a\"), tier web->db", selectorStage("/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb",
			`[.s.items[] | select(.metadata.name == "a") | .spec.v]`),
			labelled(map[string]any{"tier": "web"}), labelled(map[string]any{"tier": "db"})},
		{"templated path: whole labels map, tier web->db", selectorStage(`${ "/apis/g/v1/namespaces/ns/s?labelSelector=" + "tier%3Dweb" }`, name),
			labelled(map[string]any{"tier": "web"}), labelled(map[string]any{"tier": "db"})},
	} {
		if fires, in, x, y := selectorWitnessFires(tc.chain, tc.before, tc.after); fires {
			t.Errorf("%s: COUNTEREXAMPLE with the rule in place: interest %s, output %s -> %s", tc.name, in, x, y)
		}
		testMutations = map[string]bool{mutSelectorPathsDropped: true}
		fires, in, x, y := selectorWitnessFires(tc.chain, tc.before, tc.after)
		testMutations = nil
		if !fires {
			t.Errorf("%s: NOT RED under %s: interest %s, output %s -> %s", tc.name, mutSelectorPathsDropped, in, x, y)
			continue
		}
		t.Logf("RED under %s: %s (mutated interest %s; output %s -> %s)", mutSelectorPathsDropped, tc.name, in, x, y)
	}
}
