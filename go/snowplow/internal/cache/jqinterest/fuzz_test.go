package jqinterest

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// F14 — the differential fuzz. For every chain the analyzer narrows, and for
// random lists and random UPDATEs of one item:
//
//   - if the item's projection on the read set is UNCHANGED, the cell's real
//     output (real gojq, real plumbing jqutil, the resolver glue emulated in
//     emulate_test.go) must be byte-identical, under every identity's RBAC
//     view; a run that is fine before must be fine after (an unchanged
//     projection cannot introduce a stage-filter error, which would leak the
//     raw list);
//   - if the chain is membership-keyed, ADDing, DELETEing or arbitrarily
//     UPDATEing an object whose key is not the cell's (non-member) must leave
//     the output byte-identical;
//   - non-vacuity: across the corpus, UPDATEs that change the projection do
//     change outputs (counted and required > 0);
//   - mutable userAccessFilter namespaceFrom/nameFrom under identity-narrowed
//     bodies, and two cells sharing one list with different read sets: see
//     fuzz_variants_test.go.
//
// Knobs: JQINTEREST_FUZZ_TRIALS (per chain and mode, default 6) and
// JQINTEREST_FUZZ_SEED (default 603).

type fuzzStats struct {
	mu                                   sync.Mutex
	chains, narrowed, member             int
	trials, eqTrials, neTrials, neDiff   int
	memberTrials, emulations             int
	absentMemberTrials, extrasCollisions int
	nearMissTrials                       int
	okRuns, fallbackRuns                 int
	failures                             []string
}

func (st *fuzzStats) fail(s string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.failures) < 20 {
		st.failures = append(st.failures, s)
	}
}

func okOutcome(o emuOutcome) bool { return o.fallback == "" && o.err == "" }

func sameOutcome(a, b emuOutcome) bool {
	return okOutcome(a) && okOutcome(b) && bytes.Equal(a.body, b.body)
}

func fuzzEnvelope(g *gen) emuEnv {
	env := emuEnv{
		extras:      map[string]any{"name": g.pick(poolNames), "namespace": g.pick(poolNS)},
		identity:    uint32(1 + g.r.Intn(3)),
		fanOutFirst: g.r.Intn(2) == 0,
	}
	for i := g.r.Intn(3); i > 0; i-- {
		if len(g.v.keys) > 0 {
			env.extras[g.pick(g.v.keys)] = g.str()
		}
	}
	if g.r.Intn(2) == 0 {
		env.perPage, env.page = 1+g.r.Intn(5), 1+g.r.Intn(2)
	}
	return env
}

// chainEnvelope is fuzzEnvelope plus, one trial in eight, a request extras
// key equal to the tracked stage's name. resolve.go newResolveRun seeds the
// dict with the extras at top level, so jsonHandlerCore ACCUMULATES the
// stage's response after the seed: [seed, response] (review finding 3).
func chainEnvelope(g *gen, c Chain, st *fuzzStats) emuEnv {
	env := fuzzEnvelope(g)
	if g.r.Intn(8) == 0 {
		env.extras[c.Tracked] = g.value(1)
		if st != nil {
			st.mu.Lock()
			st.extrasCollisions++
			st.mu.Unlock()
		}
	}
	return env
}

func describe(nc namedChain, in Interest, items []any, before, after any, a, b emuOutcome, env emuEnv) string {
	trunc := func(s string) string {
		if len(s) > 600 {
			return s[:600] + "…"
		}
		return s
	}
	var bb, ab bytes.Buffer
	writeCanonical(&bb, before)
	writeCanonical(&ab, after)
	return fmt.Sprintf("chain %s\n  interest: %s\n  identity=%d extras=%v\n  before: %s\n  after:  %s\n  A: %s\n  B: %s",
		nc.name, trunc(in.String()), env.identity, env.extras, trunc(bb.String()), trunc(ab.String()), trunc(a.String()), trunc(b.String()))
}

// fuzzChain runs trials on one chain and returns true when a counterexample was found.
func fuzzChain(nc namedChain, seed int64, trials int, st *fuzzStats) bool {
	in := Analyze(nc.chain, jqsupport.ModuleLoader())
	st.mu.Lock()
	st.chains++
	st.mu.Unlock()
	if in.All {
		return false
	}
	st.mu.Lock()
	st.narrowed++
	if in.Membership != nil {
		st.member++
	}
	st.mu.Unlock()
	r := rand.New(rand.NewSource(seed))
	g := &gen{r: r, v: chainVocab(nc.chain)}
	found := false
	// Read-set trials: UPDATE one item.
	for t := 0; t < trials; t++ {
		env := chainEnvelope(g, nc.chain, st)
		n := 1 + r.Intn(4)
		items := make([]any, n)
		for i := range items {
			it := g.item()
			if r.Intn(2) == 0 {
				md := it["metadata"].(map[string]any)
				md["name"], md["namespace"] = env.extras["name"], env.extras["namespace"]
			}
			items[i] = it
		}
		i := r.Intn(n)
		before := items[i].(map[string]any)
		after := g.mutate(before, in.Reads)
		items2 := append([]any{}, items...)
		items2[i] = after
		a, b := emulate(nc.chain, items, env), emulate(nc.chain, items2, env)
		eq := bytes.Equal(Project(before, in.Reads), Project(after, in.Reads))
		st.mu.Lock()
		st.trials++
		st.emulations += 2
		for _, o := range []emuOutcome{a, b} {
			if okOutcome(o) {
				st.okRuns++
			} else if o.fallback != "" {
				st.fallbackRuns++
			}
		}
		if eq {
			st.eqTrials++
		} else {
			st.neTrials++
			if okOutcome(a) && okOutcome(b) && !bytes.Equal(a.body, b.body) {
				st.neDiff++
			}
		}
		st.mu.Unlock()
		if eq && (okOutcome(a) || okOutcome(b)) && !sameOutcome(a, b) {
			st.fail("READ-SET counterexample (projection unchanged, output changed)\n" + describe(nc, in, items, before, after, a, b, env))
			found = true
			break
		}
	}
	if in.Membership == nil {
		return found
	}
	// Membership trials: a non-member ADD / DELETE / UPDATE.
	// A non-member must fail EVERY disjunct, so every key path of every
	// disjunct gets a value no constant can equal. That value is either a
	// fresh string or, where A1 allows it, an ABSENT key: no label at all,
	// or a cluster-scoped object with no namespace. An absent key reads as
	// null, so it is used only when no guard can compare equal to null:
	// - no guard's literal value set contains null;
	// - in that trial every vocabulary key missing from the extras gets a
	//   fresh string, so an extras-derived constant is not null either.
	nullable := false
	for _, d := range in.Membership.Disjuncts {
		for _, k := range d {
			for _, v := range k.Values {
				if v == nil {
					nullable = true
				}
			}
		}
	}
	absentAllowed := func(keys []string) bool {
		if len(keys) == 0 || keys[0] == "metadata" && len(keys) == 1 {
			return false // metadata itself is always an object (A1)
		}
		return !(len(keys) == 2 && keys[0] == "metadata" && keys[1] == "name") // every object has a name
	}
	absentTrial := false
	setKeys := func(it map[string]any) {
		for _, p := range in.Membership.Paths() {
			var keys []string
			for _, s := range p {
				keys = append(keys, s.Key)
			}
			if absentTrial && !nullable && absentAllowed(keys) && r.Intn(2) == 0 {
				if len(keys) == 3 && keys[0] == "metadata" && (keys[1] == "labels" || keys[1] == "annotations") && r.Intn(2) == 0 {
					deletePath(it, keys[:2]) // no labels/annotations map at all
				} else {
					deletePath(it, keys)
				}
				continue
			}
			if r.Intn(5) == 0 && len(keys) > 1 && keys[0] != "metadata" {
				// a non-member whose key path does not even resolve. Never
				// under metadata: axiom A1 (metadata is an object with string
				// name/namespace) holds for every informer-served object, so
				// such a non-member is outside the population. It surfaced
				// only once the mutable-UAF variants let it past the
				// userAccessFilter: the corpus's own namespaceFrom
				// (.metadata.namespace) errors on it and denies the item.
				// The parent must NOT be an object: a random object can
				// hold the key with a matching value, which made "non-members"
				// that were really members (found by the opt-key arm).
				v := g.value(2)
				if _, isObj := v.(map[string]any); isObj || v == nil {
					v = []any{g.value(2)}
				}
				setPath(it, keys[:len(keys)-1], v)
			} else {
				setPath(it, keys, g.freshString())
			}
		}
	}
	// nonMember sets an item's keys so that it fails every disjunct. When
	// every guard has a literal value set, the oracle (claimedMember) can
	// decide membership. Half the time the generator then builds a CLAIMED
	// non-member from adversarial key values, so that the claim itself, and
	// not only fresh strings, is tested. Each key path takes one of:
	//  - a guard's literal value. This yields an object matching only some
	//    guards, or only one disjunct, the object a flat AND of two selects
	//    would wrongly skip (review finding 4);
	//  - a value of the request extras. If the analysis dropped an extras
	//    alternative from Values, this is a real member that the claim calls
	//    a non-member (re-gate N4);
	//  - a string from the programs that the guard does not list.
	// The candidate is kept only when the claimed DNF calls it a non-member.
	_, literalDNF := claimedMember(in.Membership, map[string]any{})
	nonMember := func(it map[string]any, env emuEnv) {
		setKeys(it)
		if !literalDNF || r.Intn(2) == 0 {
			return
		}
		var extrasVals []any
		for _, v := range env.extras {
			extrasVals = append(extrasVals, v)
		}
		sort.Slice(extrasVals, func(i, j int) bool { return fmt.Sprint(extrasVals[i]) < fmt.Sprint(extrasVals[j]) })
		near := deepCopy(it).(map[string]any)
		for _, d := range in.Membership.Disjuncts {
			for _, k := range d {
				var keys []string
				for _, s := range k.Path {
					keys = append(keys, s.Key)
				}
				switch r.Intn(4) {
				case 0:
					setPath(near, keys, deepCopy(k.Values[r.Intn(len(k.Values))]))
				case 1:
					if len(extrasVals) > 0 {
						setPath(near, keys, deepCopy(extrasVals[r.Intn(len(extrasVals))]))
					}
				case 2:
					if len(g.v.strs) > 0 {
						setPath(near, keys, g.pick(g.v.strs))
					}
				}
			}
		}
		if m, _ := claimedMember(in.Membership, near); !m {
			for kk := range it {
				delete(it, kk)
			}
			for kk, v := range near {
				it[kk] = v
			}
			st.mu.Lock()
			st.nearMissTrials++
			st.mu.Unlock()
		}
	}
	// Membership is the rarer, higher-stakes claim: five times the trials.
	for t := 0; t < 5*trials; t++ {
		env := chainEnvelope(g, nc.chain, st)
		absentTrial = r.Intn(3) == 0
		if absentTrial {
			for _, k := range g.v.keys {
				if _, ok := env.extras[k]; !ok {
					env.extras[k] = g.freshString()
				}
			}
		}
		var items []any
		for i := r.Intn(4); i >= 0; i-- {
			it := g.item()
			if r.Intn(2) == 0 {
				md := it["metadata"].(map[string]any)
				md["name"], md["namespace"] = env.extras["name"], env.extras["namespace"]
			}
			items = append(items, it)
		}
		nm := g.item()
		nonMember(nm, env)
		pos := r.Intn(len(items) + 1)
		withNM := append(append(append([]any{}, items[:pos]...), nm), items[pos:]...)
		var before, after []any
		var bDesc, aDesc any
		switch r.Intn(3) {
		case 0: // ADD
			before, after, bDesc, aDesc = items, withNM, "absent", nm
		case 1: // DELETE
			before, after, bDesc, aDesc = withNM, items, nm, "absent"
		default: // UPDATE of a non-member, anything but its key
			nm2 := g.mutate(nm, in.Reads)
			nonMember(nm2, env)
			after = append(append(append([]any{}, items[:pos]...), nm2), items[pos:]...)
			before, bDesc, aDesc = withNM, nm, nm2
		}
		a, b := emulate(nc.chain, before, env), emulate(nc.chain, after, env)
		st.mu.Lock()
		st.memberTrials++
		if absentTrial && !nullable {
			st.absentMemberTrials++
		}
		st.emulations += 2
		st.mu.Unlock()
		if (okOutcome(a) || okOutcome(b)) && !sameOutcome(a, b) {
			st.fail("MEMBERSHIP counterexample (a non-member changed the output)\n" + describe(nc, in, before, bDesc, aDesc, a, b, env))
			found = true
			break
		}
	}
	return found
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func runFuzz(chains []namedChain, seed int64, trials int) *fuzzStats {
	st := &fuzzStats{}
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				fuzzChain(chains[i], seed*1_000_003+int64(i), trials, st)
			}
		}()
	}
	for i := range chains {
		work <- i
	}
	close(work)
	wg.Wait()
	return st
}

// TestF14_DifferentialFuzz_PortalCorpus is F14 over the portal corpus.
func TestF14_DifferentialFuzz_PortalCorpus(t *testing.T) {
	_, chains := loadCorpus(t)
	trials := envInt("JQINTEREST_FUZZ_TRIALS", 6)
	seed := int64(envInt("JQINTEREST_FUZZ_SEED", 603))
	if m := os.Getenv("JQINTEREST_MUTATION"); m != "" {
		// RED-first evidence on the WHOLE corpus: run with one analysis rule
		// switched off (see mutation.go); this test must then fail.
		testMutations = map[string]bool{m: true}
		defer func() { testMutations = nil }()
		t.Logf("MUTATION %q is ON: this run must find a counterexample", m)
	}
	corpusChains := len(chains)
	variants := uafVariants(chains)
	pairs, lists := sharedListPairs(chains)
	chains = append(chains, variants...)
	st := runFuzz(chains, seed, trials)
	// Pairs are few (one per shared list and widget pair): five times the trials.
	ps := runPairFuzz(pairs, seed, 5*trials)
	t.Logf("F14: corpus chains=%d + mutable-UAF variants=%d (namespaceFrom .spec.targetNamespace; + nameFrom .spec.owner.name, name-specific verb)", corpusChains, len(variants))
	t.Logf("F14: shared-list pairs=%d over %d lists, trials=%d, split (exactly one cell's projection changed)=%d, split proved (that cell's bytes changed, the other's identical)=%d, emulations=%d",
		ps.pairs, lists, ps.trials, ps.split, ps.splitProved, ps.emulations)
	for _, f := range ps.failures {
		t.Errorf("%s", f)
	}
	if len(variants) == 0 || ps.pairs == 0 || ps.splitProved == 0 {
		t.Errorf("vacuous fuzz: mutable-UAF variants=%d, shared-list pairs=%d, proved splits=%d", len(variants), ps.pairs, ps.splitProved)
	}
	t.Logf("F14: seed=%d trials/chain/mode=%d chains=%d narrowed=%d membership-keyed=%d", seed, trials, st.chains, st.narrowed, st.member)
	t.Logf("F14: read-set trials=%d (projection-equal %d, projection-changed %d of which output-changed %d), membership trials=%d, emulations=%d",
		st.trials, st.eqTrials, st.neTrials, st.neDiff, st.memberTrials, st.emulations)
	t.Logf("F14: read-set emulations that resolved OK=%d, that hit the stage-filter-error fallback=%d", st.okRuns, st.fallbackRuns)
	t.Logf("F14: membership trials whose non-member has an ABSENT key (no label / no namespace)=%d, oracle-built claimed non-members (guard / extras / program values)=%d; trials with an extras key equal to the tracked stage=%d",
		st.absentMemberTrials, st.nearMissTrials, st.extrasCollisions)
	if st.absentMemberTrials == 0 || st.extrasCollisions == 0 {
		t.Errorf("vacuous fuzz: absent-key non-member trials=%d, extras/stage collisions=%d", st.absentMemberTrials, st.extrasCollisions)
	}
	for _, f := range st.failures {
		t.Errorf("%s", f)
	}
	if st.eqTrials == 0 || st.neDiff == 0 {
		t.Errorf("vacuous fuzz: projection-equal trials=%d, output-changing projection-changed trials=%d", st.eqTrials, st.neDiff)
	}
}
