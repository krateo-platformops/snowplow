package jqinterest

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// The stratified corpus subset (#663). Under -race the whole-corpus tests no
// longer fit the per-package CI timeout. The default path runs a committed,
// deterministic subset that keeps one representative of every stratum the
// analysis distinguishes (corpusStrata). The exhaustive build
// (-tags jqinterest_exhaustive) runs the whole corpus, and also recomputes
// the subset and fails when the committed one no longer covers every stratum
// (TestCorpusSubset_IsCurrent, corpus_subset_exhaustive_test.go).

const subsetFile = "testdata/corpus_subset.json"

type corpusSubset struct {
	Note string `json:"note"`
	// Pinned: chains kept whatever their strata. Each is the chain on which
	// the WHOLE-corpus F14 found its counterexample for a named mutation
	// (-tags jqinterest_exhaustive, JQINTEREST_MUTATION=<name>), so the
	// subset F14 keeps detecting it. Regeneration keeps them; edit by hand.
	Pinned []pinnedChain `json:"pinned"`
	Chains []string      `json:"chains"`
}

type pinnedChain struct {
	Chain    string `json:"chain"`
	Mutation string `json:"mutation"`
}

// corpusForRun is the corpus a corpus-wide test runs on: the whole corpus
// under -tags jqinterest_exhaustive, the committed subset otherwise.
func corpusForRun(t testing.TB) (*corpusFile, []namedChain) {
	t.Helper()
	c, chains := loadCorpus(t)
	if exhaustive {
		return c, chains
	}
	b, err := os.ReadFile(subsetFile)
	if err != nil {
		t.Fatalf("read %s: %v", subsetFile, err)
	}
	var s corpusSubset
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatalf("parse %s: %v", subsetFile, err)
	}
	want := map[string]bool{}
	for _, n := range s.Chains {
		want[n] = true
	}
	for _, p := range s.Pinned {
		if !want[p.Chain] {
			t.Fatalf("%s: pinned chain %q (mutation %s) is not in the subset", subsetFile, p.Chain, p.Mutation)
		}
	}
	var out []namedChain
	for _, nc := range chains {
		if want[nc.name] {
			out = append(out, nc)
			delete(want, nc.name)
		}
	}
	if len(want) > 0 {
		var missing []string
		for n := range want {
			missing = append(missing, n)
		}
		sort.Strings(missing)
		t.Fatalf("%s names %d chains the corpus no longer has (regenerate it, see corpus_subset_exhaustive_test.go): %v", subsetFile, len(missing), missing)
	}
	t.Logf("corpus: stratified subset, %d of %d chains (%s); -tags jqinterest_exhaustive runs all", len(out), len(chains), subsetFile)
	return c, out
}

// corpusStrata are the classes a chain belongs to. A subset that has one
// member of every class exercises every distinct:
//   - RESTAction program set and tracked stage (stage filter, spec.filter,
//     other stages' templates, userAccessFilter): "ra";
//   - widget kind, and whether it has data templates and resourcesRefs:
//     "kind", "widget";
//   - analysis verdict, read set and membership claim: "interest";
//   - the same for its mutable-UAF variants: "variant";
//   - membership blocker reason (first word class): "blocker";
//   - extras source shape (root, path, stream, default): "source".
func corpusStrata(nc namedChain) []string {
	rb, _ := json.Marshal(struct {
		R RESTAction
		T string
	}{nc.chain.RA, nc.chain.Tracked})
	ra := string(rb)
	kind := "cell"
	if strings.HasPrefix(nc.name, "w:") {
		kind = strings.SplitN(strings.TrimPrefix(nc.name, "w:"), "/", 2)[0]
	}
	keys := []string{"ra:" + ra, "kind:" + kind}
	if w := nc.chain.Widget; w != nil {
		keys = append(keys, fmt.Sprintf("widget:%s templates=%v resourcesRefs=%v", kind, len(w.DataTemplates) > 0, len(w.ResourcesRefs) > 0))
	}
	in := Analyze(nc.chain, jqsupport.ModuleLoader())
	keys = append(keys, "interest:"+interestClass(in))
	for _, b := range in.MembershipBlockers {
		keys = append(keys, "blocker:"+blockerClass(b))
	}
	if in.Membership != nil {
		for _, d := range in.Membership.Disjuncts {
			for _, k := range d {
				if k.Extras != nil {
					keys = append(keys, "source:"+sourceShape(*k.Extras))
				}
			}
		}
	}
	for _, v := range uafVariants([]namedChain{nc}) {
		vi := Analyze(v.chain, jqsupport.ModuleLoader())
		keys = append(keys, "variant:"+strings.SplitN(v.name, "+", 2)[1]+"|"+interestClass(vi))
	}
	return keys
}

// interestClass is the verdict's shape, not its paths (the paths are per
// widget and would make every chain its own class): why it is All, or which
// read modes it has, whether any read excludes a subtree, whether it reads
// the whole item, and whether it claims membership.
func interestClass(in Interest) string {
	if in.All {
		// the why carries program text; keep its kind.
		return "ALL " + strings.SplitN(in.Why, " ", 2)[0]
	}
	modes := map[string]bool{}
	for _, r := range in.Reads {
		m := r.Mode.String()
		if len(r.Excl) > 0 {
			m += "-excl"
		}
		if len(r.Path) == 0 {
			m += "@root"
		}
		modes[m] = true
	}
	var ms []string
	for m := range modes {
		ms = append(ms, m)
	}
	sort.Strings(ms)
	s := "reads{" + strings.Join(ms, ",") + "}"
	if in.Membership != nil {
		s += " member"
	}
	return s
}

func blockerClass(b string) string {
	f := strings.Fields(b)
	if len(f) > 3 {
		f = f[:3]
	}
	return strings.Join(f, " ")
}

// sourceShape abstracts an extras source to its shape: root, step kinds,
// stream, default kind.
func sourceShape(x ExtrasSource) string {
	var b strings.Builder
	b.WriteString(string(x.Root))
	for _, st := range x.Path {
		if st.Any {
			b.WriteString("[]")
		} else {
			b.WriteString(".k")
		}
	}
	if x.HasDefault {
		fmt.Fprintf(&b, " // %T", x.Default)
	}
	return b.String()
}

// stratifiedSubset keeps, in corpus order, every chain that is the first of
// one of its strata, every chain that claims membership (the high-stakes
// claim, a few dozen), every pinned chain, and every member the shared-list
// pair fuzz pairs (sharedListPrefixes), so the subset has the same pairs.
func stratifiedSubset(chains []namedChain, pinned []pinnedChain) (subset []namedChain, strata int) {
	pin := map[string]bool{}
	for _, p := range pinned {
		pin[p.Chain] = true
	}
	// The shared-list pair fuzz must see exactly the whole corpus's pairs.
	for _, n := range sharedListPrefixes(chains) {
		pin[n] = true
	}
	seen := map[string]bool{}
	for _, nc := range chains {
		keep := pin[nc.name]
		for _, k := range corpusStrata(nc) {
			if !seen[k] {
				seen[k] = true
				keep = true
			}
		}
		if !keep && Analyze(nc.chain, jqsupport.ModuleLoader()).Membership != nil {
			keep = true
		}
		if keep {
			subset = append(subset, nc)
		}
	}
	return subset, len(seen)
}
