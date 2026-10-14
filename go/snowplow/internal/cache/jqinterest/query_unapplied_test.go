package jqinterest

import (
	"strings"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// #683 follow-up (Refs #603): a LITERAL stage path whose query
// internal/apicall PlanQuery refuses falls through to the apiserver, so the
// edge is All (why=query_unapplied <reason>), decided statically. PR-2 then
// does not depend on the runtime §2.4 fall-through rule for a literal path.

// fallWitness is an UPDATE that a fall-through query's apiserver semantics
// see and a name-only read set does not: the selectable field status.phase
// Running -> Failed and the label tier web -> db, together.
func fallWitness() (before, after map[string]any) {
	mk := func(phase, tier string) map[string]any {
		it := labelled(map[string]any{"tier": tier})
		it["status"] = map[string]any{"phase": phase}
		return it
	}
	return mk("Running", "web"), mk("Failed", "db")
}

func TestQueryUnapplied_LiteralPathIsAllStatically(t *testing.T) {
	const name = `[.s.items[] | .metadata.name]`
	const np, cp = "/apis/g/v1/namespaces/ns/s", "/apis/g/v1/s"
	for _, tc := range []struct {
		name, path, reason string
		// detectable: the witness UPDATE changes the apiserver's answer, so
		// the mutation must be caught (a limit or an immutable field cannot
		// be moved by an UPDATE: those rows pin the verdict only).
		detectable bool
	}{
		{"mutable fieldSelector (selectable field)", np + "?fieldSelector=status.phase%3DRunning", "unsupported_field", true},
		{"labelSelector + limit", np + "?labelSelector=tier%3Dweb&limit=5", "pagination", true},
		{"labelSelector + unknown param", np + "?labelSelector=tier%3Dweb&pretty=true", "unknown_param", true},
		{"labelSelector + resourceVersion", np + "?labelSelector=tier%3Dweb&resourceVersion=0", "resource_version", true},
		{"repeated labelSelector", np + "?labelSelector=tier%3Dweb&labelSelector=env%3Dprod", "repeated_param", true},
		{"selector-free limit", np + "?limit=5", "pagination", false},
		{"selector-free continue", np + "?continue=abc", "pagination", false},
		{"watch", np + "?watch=1", "watch", false},
		{"metadata.namespace on a cluster path", cp + "?fieldSelector=metadata.namespace%3Dns", "namespace_field_cluster_path", false},
		{"unparseable labelSelector", np + "?labelSelector=" + "tier+in+%28", "invalid_selector", false},
		{"malformed query", np + "?labelSelector=%zz", "malformed_query", false},
	} {
		c := selectorStage(tc.path, name)
		in := Analyze(c, jqsupport.ModuleLoader())
		if !in.All || in.Why != "query_unapplied "+tc.reason {
			t.Errorf("%s: want ALL (query_unapplied %s), got %s", tc.name, tc.reason, in)
		}
		before, after := fallWitness()
		if fires, in, x, y := selectorWitnessFires(c, before, after); fires {
			t.Errorf("%s: COUNTEREXAMPLE with the rule in place: %s, %s -> %s", tc.name, in, x, y)
		}
		testMutations = map[string]bool{mutQueryUnappliedNarrowed: true}
		mut := Analyze(c, jqsupport.ModuleLoader())
		fires, _, x, y := selectorWitnessFires(c, before, after)
		testMutations = nil
		if mut.All {
			t.Errorf("%s: still ALL under %s (%s): the mutation does not reach this path", tc.name, mutQueryUnappliedNarrowed, mut)
			continue
		}
		if tc.detectable && !fires {
			t.Errorf("%s: NOT RED under %s: interest %s, output %s -> %s", tc.name, mutQueryUnappliedNarrowed, mut, x, y)
			continue
		}
		if tc.detectable {
			t.Logf("RED under %s: %s (mutated interest %s; apiserver answer %s -> %s)", mutQueryUnappliedNarrowed, tc.name, mut, x, y)
		}
	}
}

// The verdict is not widened where nothing falls through: an applied query,
// a query-free path, a non-apiserver path's own query, a templated path.
func TestQueryUnapplied_AppliedOrForeignQueriesStayNarrowed(t *testing.T) {
	const name = `[.s.items[] | .metadata.name]`
	for _, p := range []string{
		"/apis/g/v1/namespaces/ns/s?labelSelector=tier%3Dweb",
		"/apis/g/v1/namespaces/ns/s?fieldSelector=metadata.namespace%3Dns%2Cmetadata.name%3Da",
		"/apis/g/v1/s?fieldSelector=metadata.name%3Da",
		"/apis/g/v1/namespaces/ns/s",
		"/apis/g/v1/namespaces/ns/s?",
		"/?query=SELECT%201%20FORMAT%20JSON",
		`${ "/apis/g/v1/namespaces/ns/s?limit=" + "5" }`,
	} {
		in := Analyze(selectorStage(p, name), jqsupport.ModuleLoader())
		if in.All {
			t.Errorf("%s: unexpectedly %s", p, in)
		}
	}
}

// The random search, too: under the mutation the fuzz finds the
// counterexample on the fall-through chains by itself or by the witness.
func TestQueryUnapplied_FuzzRedUnderMutation(t *testing.T) {
	c := selectorStage("/apis/g/v1/namespaces/ns/s?fieldSelector=status.phase%3DRunning", `[.s.items[] | .metadata.name]`)
	st := &fuzzStats{}
	if fuzzChain(namedChain{name: "Q-pristine", chain: c}, 9100, armTrials, st) {
		t.Fatalf("counterexample with the rule in place:\n%s", strings.Join(st.failures, "\n"))
	}
	testMutations = map[string]bool{mutQueryUnappliedNarrowed: true}
	defer func() { testMutations = nil }()
	st = &fuzzStats{}
	found := fuzzChain(namedChain{name: "Q-mutated", chain: c}, 9100, armTrials, st)
	// As for every S-arm (armWitness), the fixed witness joins the search:
	// the random search reaches a phase flip on a selected object only at
	// the exhaustive budget.
	before, after := fallWitness()
	fires, _, _, _ := selectorWitnessFires(c, before, after)
	if !found && !fires {
		t.Fatalf("mutation %s went UNDETECTED in %d trials and by the witness (narrowed=%d)", mutQueryUnappliedNarrowed, st.trials, st.narrowed)
	}
	t.Logf("RED under %s: random search found=%v (%d trials), witness fires=%v", mutQueryUnappliedNarrowed, found, st.trials, fires)
}
