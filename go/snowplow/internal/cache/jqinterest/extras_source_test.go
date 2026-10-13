package jqinterest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/itchyny/gojq"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// #633: a Decidable extras guard must carry a structured source {root, path,
// stream} that PR-2 can evaluate, and evaluating it on the cell's REAL root
// must yield exactly the values the program compares the key with.
//
// The source arm checks that against the program itself. It rewrites the
// constant operand C of every guard that carries a source into
// (C | _jqi633_capture(id)), where _jqi633_capture records each value C
// yields and passes it through. The emulator (emulate_test.go) hands every
// jq program the resolve runs, with its data, to a probe, which runs the
// rewritten program on that data. It also hands over the three roots
// (stage input, RESTAction dict, widget data source). After each emulated
// resolve, for each guard:
//   - every captured value is in source.Eval(root). A value the program
//     compares with that the source misses is an object PR-2 would call a
//     non-member while it is one (unsound);
//   - when the program compared at all, every value of source.Eval(root) was
//     captured (the source is exact, not just a superset).
// When Eval raises, PR-2 reads the edge as everything for that cell, so the
// trial is counted and not compared.

const captureFn = "_jqi633_capture"

// sourceProbe is the instrumented view of one chain.
type sourceProbe struct {
	in        Interest
	rewritten map[string][]string    // program text -> instrumented texts
	sources   map[int][]ExtrasSource // capture id -> the sources of that operand
	unprobed  []string               // guards whose operand is in no parsed program
	missing   []string               // Decidable extras guards with no source
}

func newSourceProbe(c Chain) *sourceProbe {
	type parsed struct {
		src string
		q   *gojq.Query
	}
	var roots []parsed
	testParseHook = func(src string, q *gojq.Query) { roots = append(roots, parsed{src, q}) }
	in := Analyze(c, jqsupport.ModuleLoader())
	testParseHook = nil

	p := &sourceProbe{in: in, rewritten: map[string][]string{}, sources: map[int][]ExtrasSource{}}
	if in.Membership != nil {
		for _, d := range in.Membership.Disjuncts {
			for _, k := range d {
				if k.Decidable() && k.Values == nil && k.Extras == nil {
					p.missing = append(p.missing, k.String())
				}
			}
		}
	}
	before := make([]string, len(roots))
	for i, r := range roots {
		before[i] = r.q.String()
	}
	ids := map[*gojq.Query]int{}
	for _, s := range in.sites {
		id, ok := ids[s.q]
		if !ok {
			id = len(ids) + 1
			ids[s.q] = id
			orig := *s.q
			call := &gojq.Query{Term: &gojq.Term{Type: gojq.TermTypeFunc, Func: &gojq.Func{
				Name: captureFn, Args: []*gojq.Query{{Term: &gojq.Term{Type: gojq.TermTypeNumber, Number: strconv.Itoa(id)}}},
			}}}
			*s.q = gojq.Query{Term: &gojq.Term{Type: gojq.TermTypeQuery, Query: &gojq.Query{
				Left:  &gojq.Query{Term: &gojq.Term{Type: gojq.TermTypeQuery, Query: &orig}},
				Op:    gojq.OpPipe,
				Right: call,
			}}}
		}
		p.sources[id] = append(p.sources[id], s.src)
	}
	all := ""
	for i, r := range roots {
		if s := r.q.String(); s != before[i] {
			p.rewritten[r.src] = append(p.rewritten[r.src], s)
			all += s + "\n"
		}
	}
	for _, s := range in.sites {
		if !strings.Contains(all, captureFn+"("+strconv.Itoa(ids[s.q])+")") {
			p.unprobed = append(p.unprobed, s.src.String())
		}
	}
	return p
}

var (
	captureCodeMu sync.Mutex
	captureCode   = map[string]*gojq.Code{}
)

// capture runs an instrumented program on data and adds every value each
// operand yields to into[id].
func capture(prog string, data any, into map[int][]any) error {
	captureCodeMu.Lock()
	code, ok := captureCode[prog]
	captureCodeMu.Unlock()
	if !ok {
		q, err := gojq.Parse(prog)
		if err != nil {
			return err
		}
		// Compiled once per program; the function records into the sink
		// of the run in progress.
		code, err = gojq.Compile(q, gojq.WithModuleLoader(jqsupport.ModuleLoader()),
			gojq.WithFunction(captureFn, 1, 1, func(v any, args []any) any {
				captureSinkMu.Lock()
				if captureSink != nil {
					id, _ := strconv.Atoi(fmt.Sprint(args[0]))
					captureSink[id] = append(captureSink[id], v)
				}
				captureSinkMu.Unlock()
				return v
			}))
		if err != nil {
			return err
		}
		captureCodeMu.Lock()
		captureCode[prog] = code
		captureCodeMu.Unlock()
	}
	captureSinkMu.Lock()
	captureSink = into
	captureSinkMu.Unlock()
	defer func() {
		captureSinkMu.Lock()
		captureSink = nil
		captureSinkMu.Unlock()
	}()
	iter := code.RunWithContext(context.Background(), data)
	for {
		v, ok := iter.Next()
		if !ok {
			return nil
		}
		if _, isErr := v.(error); isErr {
			return nil // the real run stops here too
		}
	}
}

var (
	captureSinkMu sync.Mutex
	captureSink   map[int][]any
)

type sourceStats struct {
	chains, sites, trials, compared, evalErrors, streamCompared int
	failures                                                    []string
}

func (st *sourceStats) fail(format string, args ...any) {
	if len(st.failures) < 20 {
		st.failures = append(st.failures, fmt.Sprintf(format, args...))
	}
}

func canon(v any) string {
	b, _ := json.Marshal(normalizeNumbersDeep(v))
	return string(b)
}

func canonSet(vs []any) map[string]bool {
	out := map[string]bool{}
	for _, v := range vs {
		out[canon(v)] = true
	}
	return out
}

func setString(m map[string]bool) string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return "{" + strings.Join(ks, ", ") + "}"
}

// sourceEnv is a fuzz environment whose extras also carry arrays and objects
// under the chain's vocabulary keys, so stream and nested sources have
// something to iterate.
func sourceEnv(g *gen, c Chain) emuEnv {
	env := chainEnvelope(g, c, nil)
	for _, k := range g.v.keys {
		switch g.r.Intn(4) {
		case 0:
			var a []any
			for i := g.r.Intn(4); i > 0; i-- {
				a = append(a, g.pick(poolNames))
			}
			if a == nil {
				a = []any{}
			}
			env.extras[k] = a
		case 1:
			env.extras[k] = g.value(1)
		}
	}
	return env
}

// checkSources runs the source arm on one chain.
func checkSources(nc namedChain, seed int64, trials int, st *sourceStats) {
	p := newSourceProbe(nc.chain)
	st.chains++
	for _, m := range p.missing {
		st.fail("%s: Decidable extras guard without a structured source: %s", nc.name, m)
	}
	for _, u := range p.unprobed {
		st.fail("%s: guard source %s: its operand is in no parsed program, so it cannot be checked", nc.name, u)
	}
	if len(p.sources) == 0 {
		return
	}
	st.sites += len(p.sources)
	r := rand.New(rand.NewSource(seed))
	g := &gen{r: r, v: chainVocab(nc.chain)}
	for t := 0; t < trials; t++ {
		env := sourceEnv(g, nc.chain)
		var items []any
		for i := r.Intn(3); i >= 0; i-- {
			it := g.item()
			if r.Intn(2) == 0 {
				md := it["metadata"].(map[string]any)
				md["name"], md["namespace"] = env.extras["name"], env.extras["namespace"]
			}
			items = append(items, it)
		}
		roots := map[ExtrasRoot]any{}
		captured := map[int][]any{}
		env.probe = func(kind, query string, data any) {
			if query == "" {
				if _, ok := roots[ExtrasRoot(kind)]; !ok {
					roots[ExtrasRoot(kind)] = deepCopy(data)
				}
				return
			}
			for _, prog := range p.rewritten[query] {
				if err := capture(prog, data, captured); err != nil {
					st.fail("%s: instrumented program does not compile: %v\n%s", nc.name, err, prog)
				}
			}
		}
		emulate(nc.chain, items, env)
		st.trials++
		for id, srcs := range p.sources {
			got := canonSet(captured[id])
			want := map[string]bool{}
			evalErr, rootMissing := false, false
			stream := false
			for _, s := range srcs {
				stream = stream || s.Stream
				root, ok := roots[s.Root]
				if !ok {
					rootMissing = true
					continue
				}
				vs, err := s.Eval(root)
				if err != nil {
					evalErr = true
					continue
				}
				for k := range canonSet(vs) {
					want[k] = true
				}
			}
			switch {
			case evalErr:
				st.evalErrors++
				continue
			case rootMissing:
				if len(got) > 0 {
					st.fail("%s: the program compared with %s, but root of %v was never seen", nc.name, setString(got), srcs)
				}
				continue
			}
			for k := range got {
				if !want[k] {
					st.fail("%s: source %v misses a value the program compares with: captured %s, source evaluates to %s (extras=%v)",
						nc.name, srcs, setString(got), setString(want), env.extras)
					break
				}
			}
			if len(got) > 0 {
				st.compared++
				if stream {
					st.streamCompared++
				}
				for k := range want {
					if !got[k] {
						st.fail("%s: source %v yields %s, which the program never compared with: captured %s (extras=%v)",
							nc.name, srcs, k, setString(got), env.extras)
						break
					}
				}
			}
		}
	}
}

// sourcePositiveArms: chains whose guard IS a single extras navigation
// chain, one per root and shape, so the source arm is never vacuous.
func sourcePositiveArms() []namedChain {
	ds := oneStage("", `{list: [.s.items[]]}`)
	ds.Widget = &Widget{DataTemplates: []string{`${ .name as $n | [.list[] | select(.metadata.name == $n)] }`}}
	dsConst := oneStage("", `{x: 1}`)
	dsConst.Widget = &Widget{DataTemplates: []string{`${ .x }`}}
	return []namedChain{
		{"src: stage input .extras.name via $n", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == $n)]`, "")},
		{"src: stage input .extras via $e, then .name", oneStage(`.extras as $e | [.s.items[] | select(.metadata.name == $e.name)]`, "")},
		{"src: stage input stream .extras.names[]", oneStage(`.extras.names as $ns | [.s.items[] | select(.metadata.name == $ns[])]`, "")},
		{"src: stage input nested stream .extras.sel[].name", oneStage(`.extras.sel as $s | [.s.items[] | select(.metadata.name == $s[].name)]`, "")},
		{"src: stage input .slice.page", oneStage(`.slice.page as $p | [.s.items[] | select(.metadata.generation == $p)]`, "")},
		{"src: stage input through a user function", oneStage(`def n: .extras.name; n as $n | [.s.items[] | select(.metadata.name == $n)]`, "")},
		{"src: stage input under a try (?)", oneStage(`.extras.names as $ns | [.s.items[] | select(.metadata.name == ($ns[]?))]`, "")},
		{"src: RESTAction dict .name", oneStage("", `.name as $n | [.s.items[] | select(.metadata.name == $n)]`)},
		{"src: RESTAction dict stream .names[]", oneStage("", `.names as $ns | {l: [.s.items[] | select(.metadata.namespace == $ns[])]}`)},
		{"src: widget data source .name", ds},
	}
}

// sourceNegativeArms: constants that involve the extras but are NOT one
// navigation chain. None may be Decidable (#633).
func sourceNegativeArms() []namedChain {
	return []namedChain{
		{"neg: union (\"zzz\", $n)", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ("zzz", $n))]`, "")},
		{"neg: object {app: $n}", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.labels == {app: $n})]`, "")},
		{"neg: two extras paths ($e.a, $e.b)", oneStage(`.extras as $e | [.s.items[] | select(.metadata.name == ($e.a, $e.b))]`, "")},
		{"neg: default .extras.name // \"\"", oneStage(`(.extras.name // "") as $n | [.s.items[] | select(.metadata.name == $n)]`, "")},
		{"neg: first($ns[])", oneStage(`.extras.names as $ns | [.s.items[] | select(.metadata.name == first($ns[]))]`, "")},
		{"neg: $n | select(. != \"\")", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ($n | select(. != "")))]`, "")},
		{"neg: $ns[0]", oneStage(`.extras.names as $ns | [.s.items[] | select(.metadata.name == $ns[0])]`, "")},
		{"neg: $n + \"-x\"", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ($n + "-x"))]`, "")},
		{"neg: $n | ascii_downcase", oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ($n | ascii_downcase))]`, "")},
		{"neg: dict key equal to the tracked stage", oneStage("", `.s as $x | [.s.items[]? | select(.metadata.name == $x)]`)},
	}
}

func allSourceChains(t *testing.T) []namedChain {
	_, chains := loadCorpus(t)
	chains = append(chains, uafVariants(chains)...)
	for _, a := range soundnessArms() {
		chains = append(chains, namedChain{name: a.name, chain: a.chain})
	}
	chains = append(chains, sourcePositiveArms()...)
	chains = append(chains, sourceNegativeArms()...)
	return chains
}

// TestExtrasSource_EvaluatesToTheComparedValue is the #633 source arm, over
// the corpus, the mutable-UAF variants, every soundness arm and the #633
// arms.
func TestExtrasSource_EvaluatesToTheComparedValue(t *testing.T) {
	chains := allSourceChains(t)
	st := &sourceStats{}
	for i, nc := range chains {
		checkSources(nc, int64(6330+i), 60, st)
	}
	t.Logf("source arm: chains=%d, guard operands with a source=%d, trials=%d, operand comparisons checked=%d (stream %d), Eval raised (edge reads everything)=%d",
		st.chains, st.sites, st.trials, st.compared, st.streamCompared, st.evalErrors)
	for _, f := range st.failures {
		t.Errorf("%s", f)
	}
	if st.compared == 0 || st.streamCompared == 0 {
		t.Errorf("vacuous source arm: comparisons checked=%d, stream comparisons=%d", st.compared, st.streamCompared)
	}
	// Every positive arm is Decidable through its source and is compared.
	for i, nc := range sourcePositiveArms() {
		one := &sourceStats{}
		checkSources(nc, int64(6400+i), 60, one)
		in := Analyze(nc.chain, jqsupport.ModuleLoader())
		if in.Membership == nil || one.compared == 0 {
			t.Errorf("%s: want a membership decided by its extras source and checked against the program, got %s (blockers %v, compared %d)",
				nc.name, in, in.MembershipBlockers, one.compared)
		}
	}
}

// TestExtrasSource_NotASingleChainIsNotDecidable: every #633 negative shape
// is refused, and the refusal is recorded as a blocker.
func TestExtrasSource_NotASingleChainIsNotDecidable(t *testing.T) {
	for _, nc := range sourceNegativeArms() {
		in := Analyze(nc.chain, jqsupport.ModuleLoader())
		t.Logf("%-40s %s blockers=%v", nc.name, in, in.MembershipBlockers)
		if in.Membership != nil {
			for _, d := range in.Membership.Disjuncts {
				for _, k := range d {
					if k.Values == nil {
						t.Errorf("%s: claimed an extras guard that is not one navigation chain: %s (source %v)", nc.name, k, k.Extras)
					}
				}
			}
		}
	}
}

// The reviewer's three cases (#633), RED-first: on a605736d each one was a
// Decidable guard with no evaluable source.
func TestIssue633_ReviewerUnionIsNotDecidable(t *testing.T) {
	assertBlocked(t, `.extras.name as $n | [.s.items[] | select(.metadata.name == ("zzz", $n))]`)
}

func TestIssue633_ReviewerObjectIsNotDecidable(t *testing.T) {
	assertBlocked(t, `.extras.name as $n | [.s.items[] | select(.metadata.labels == {app: $n})]`)
}

func TestIssue633_ReviewerStreamCarriesRootPathStream(t *testing.T) {
	c := oneStage(`.extras.names as $ns | [.s.items[] | select(.metadata.name == $ns[])]`, "")
	in := Analyze(c, jqsupport.ModuleLoader())
	if in.Membership == nil || len(in.Membership.Disjuncts) != 1 || len(in.Membership.Disjuncts[0]) != 1 {
		t.Fatalf("want one guard: %s (blockers %v)", in, in.MembershipBlockers)
	}
	k := in.Membership.Disjuncts[0][0]
	want := ExtrasSource{Root: RootStageInput, Path: Path{{Key: "extras"}, {Key: "names"}, {Any: true}}, Stream: true}
	if k.Extras == nil || k.Extras.String() != want.String() || k.Extras.Root != want.Root || !k.Extras.Stream {
		t.Fatalf("$ns[] must carry {root %s, path %s, stream true}, got %v (%s)", want.Root, want.Path, k.Extras, k)
	}
	pig := map[string]any{"extras": map[string]any{"names": []any{"a", "b"}}, "s": map[string]any{}}
	vs, err := k.Extras.Eval(pig)
	if err != nil || setString(canonSet(vs)) != `{"a", "b"}` {
		t.Fatalf("Eval on the stage input: %v, %v", vs, err)
	}
	st := &sourceStats{}
	checkSources(namedChain{"reviewer $ns[]", c}, 6331, 200, st)
	for _, f := range st.failures {
		t.Errorf("%s", f)
	}
	if st.streamCompared == 0 {
		t.Errorf("vacuous: the stream source was never compared with the program")
	}
}

func assertBlocked(t *testing.T, prog string) {
	t.Helper()
	in := Analyze(oneStage(prog, ""), jqsupport.ModuleLoader())
	if in.Membership != nil {
		t.Errorf("%s: claimed %s; the constant is not one extras navigation chain", prog, in)
	}
	blocked := false
	for _, b := range in.MembershipBlockers {
		blocked = blocked || strings.HasPrefix(b, "undecidable key constant")
	}
	if !blocked {
		t.Errorf("%s: want an undecidable-constant blocker, got %v", prog, in.MembershipBlockers)
	}
}

// TestIssue633_UnionDecidableMutationIsRed: the named mutation marks the
// union ("zzz", $n) Decidable through its extras alternative. The source arm
// must catch it: the program also compares with "zzz", which the source
// .extras.name does not yield.
func TestIssue633_UnionDecidableMutationIsRed(t *testing.T) {
	c := namedChain{"union under " + mutUnionDecidable, oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ("zzz", $n))]`, "")}
	green := &sourceStats{}
	checkSources(c, 6332, 200, green)
	if len(green.failures) > 0 {
		t.Fatalf("without the mutation: %v", green.failures)
	}
	testMutations = map[string]bool{mutUnionDecidable: true}
	in := Analyze(c.chain, jqsupport.ModuleLoader())
	red := &sourceStats{}
	checkSources(c, 6332, 200, red)
	testMutations = nil
	if in.Membership == nil {
		t.Fatalf("the mutation must make the union Decidable (a claimed membership): %s %v", in, in.MembershipBlockers)
	}
	if len(red.failures) == 0 {
		t.Fatalf("mutation %q went UNDETECTED by the source arm (%d comparisons)", mutUnionDecidable, red.compared)
	}
	t.Logf("RED under %s: %s", mutUnionDecidable, red.failures[0])
}
