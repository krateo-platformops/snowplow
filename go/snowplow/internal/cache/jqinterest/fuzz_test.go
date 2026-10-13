package jqinterest

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
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
	mu                                 sync.Mutex
	chains, narrowed, member           int
	trials, eqTrials, neTrials, neDiff int
	memberTrials, emulations           int
	okRuns, fallbackRuns               int
	failures                           []string
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
		env := fuzzEnvelope(g)
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
	setKeys := func(it map[string]any) {
		for _, k := range in.Membership.Keys {
			var keys []string
			for _, s := range k.Path {
				keys = append(keys, s.Key)
			}
			if r.Intn(5) == 0 && len(keys) > 1 && keys[0] != "metadata" {
				// a non-member whose key path does not even resolve. Never
				// under metadata: axiom A1 (metadata is an object with string
				// name/namespace) holds for every informer-served object, so
				// such a non-member is outside the population. It surfaced
				// only once the mutable-UAF variants let it past the
				// userAccessFilter: the corpus's own namespaceFrom
				// (.metadata.namespace) errors on it and denies the item.
				setPath(it, keys[:len(keys)-1], g.value(2))
			} else {
				setPath(it, keys, g.freshString())
			}
		}
	}
	// Membership is the rarer, higher-stakes claim: five times the trials.
	for t := 0; t < 5*trials; t++ {
		env := fuzzEnvelope(g)
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
		setKeys(nm)
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
			setKeys(nm2)
			after = append(append(append([]any{}, items[:pos]...), nm2), items[pos:]...)
			before, bDesc, aDesc = withNM, nm, nm2
		}
		a, b := emulate(nc.chain, before, env), emulate(nc.chain, after, env)
		st.mu.Lock()
		st.memberTrials++
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
	for _, f := range st.failures {
		t.Errorf("%s", f)
	}
	if st.eqTrials == 0 || st.neDiff == 0 {
		t.Errorf("vacuous fuzz: projection-equal trials=%d, output-changing projection-changed trials=%d", st.eqTrials, st.neDiff)
	}
}
