package jqinterest

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// Two F14 populations the portal corpus alone does not reach.
//
//  1. MUTABLE userAccessFilter reads under identity-narrowed bodies. Every
//     userAccessFilter in the corpus reads the immutable .metadata.namespace
//     (and none sets nameFrom), so the corpus never exercises a namespaceFrom
//     or nameFrom over a field an UPDATE can change. uafVariants re-runs every
//     narrowed LIST-edge chain with two such filters on its tracked stage.
//     The emulator applies each identity's RBAC oracle to the result
//     (emulate_test.go rbacAllows), so every trial compares identity-narrowed
//     bodies.
//  2. TWO CELLS SHARING ONE LIST WITH DIFFERENT READ SETS. The RESTAction cell
//     and each widget cell over the same (RESTAction, tracked stage) record
//     the same list edge but read different fields. One UPDATE is applied to
//     the shared list, and each cell is held to its OWN projection. The fuzz
//     requires trials where the projections disagree, with exactly the cell
//     whose projection changed seeing changed bytes.

// The mutable UAF shapes. The paths are not in any corpus program, so the
// generator learns them only through chainVocab's UAF branch. They are
// therefore populated and mutated only when the chain model carries the UAF.
var (
	uafNSOnly = UAF{NamespaceFrom: ".spec.targetNamespace"}
	uafNSName = UAF{NamespaceFrom: ".spec.targetNamespace", NameFrom: ".spec.owner.name", NameSpecificVerb: true}
)

// withUAF returns c with the tracked stage's userAccessFilter replaced by u
// (keeping any resourcesFrom). Stages are copied so the corpus chain is
// untouched.
func withUAF(c Chain, u UAF) Chain {
	st := append([]Stage(nil), c.RA.Stages...)
	for i := range st {
		if st[i].Name != c.Tracked {
			continue
		}
		nu := u
		if st[i].UAF != nil {
			nu.ResourcesFrom = st[i].UAF.ResourcesFrom
		}
		st[i].UAF = &nu
	}
	c.RA.Stages = st
	return c
}

func trackedStage(c Chain) *Stage {
	for i := range c.RA.Stages {
		if c.RA.Stages[i].Name == c.Tracked {
			return &c.RA.Stages[i]
		}
	}
	return nil
}

// narrowedListChain: a LIST-edge chain that the analyzer narrows.
func narrowedListChain(c Chain) bool {
	st := trackedStage(c)
	return st != nil && isListCall(st.Path) && !Analyze(c, jqsupport.ModuleLoader()).All
}

// uafVariants: two identity-narrowed variants of every narrowed LIST-edge chain.
func uafVariants(chains []namedChain) []namedChain {
	var out []namedChain
	for _, nc := range chains {
		if !narrowedListChain(nc.chain) {
			continue
		}
		out = append(out,
			namedChain{name: nc.name + "+uaf(ns)", chain: withUAF(nc.chain, uafNSOnly)},
			namedChain{name: nc.name + "+uaf(ns,name)", chain: withUAF(nc.chain, uafNSName)})
	}
	return out
}

type cellPair struct {
	name string
	a, b namedChain
}

// maxPairsPerList bounds the pairs taken from one shared list (a RESTAction
// with many widgets would otherwise contribute quadratically many).
const maxPairsPerList = 6

// sharedListPairs: pairs of narrowed cells over the SAME list (same
// RESTAction, same tracked stage) whose read sets differ. Each pair is also
// emitted under both mutable UAF shapes, applied to both cells (the edge is
// the same list call).
func sharedListPairs(chains []namedChain) (pairs []cellPair, lists int) {
	groups := map[string][]namedChain{}
	var order []string
	for _, nc := range chains {
		if len(nc.chain.RA.Stages) == 0 || !narrowedListChain(nc.chain) {
			continue
		}
		// The corpus loader shares one Stages backing array per RESTAction,
		// so its address identifies the RESTAction.
		k := fmt.Sprintf("%p#%s", &nc.chain.RA.Stages[0], nc.chain.Tracked)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], nc)
	}
	for _, k := range order {
		g := groups[k]
		sort.Slice(g, func(i, j int) bool { return g[i].name < g[j].name })
		n := 0
		for i := 0; i < len(g) && n < maxPairsPerList; i++ {
			ri := Analyze(g[i].chain, jqsupport.ModuleLoader()).String()
			for j := i + 1; j < len(g) && n < maxPairsPerList; j++ {
				if Analyze(g[j].chain, jqsupport.ModuleLoader()).String() == ri {
					continue
				}
				if n == 0 {
					lists++
				}
				n++
				pairs = append(pairs, cellPair{name: g[i].name + " ⟷ " + g[j].name, a: g[i], b: g[j]})
				for _, u := range []struct {
					tag string
					u   UAF
				}{{"+uaf(ns)", uafNSOnly}, {"+uaf(ns,name)", uafNSName}} {
					a := namedChain{name: g[i].name + u.tag, chain: withUAF(g[i].chain, u.u)}
					b := namedChain{name: g[j].name + u.tag, chain: withUAF(g[j].chain, u.u)}
					if Analyze(a.chain, jqsupport.ModuleLoader()).All || Analyze(b.chain, jqsupport.ModuleLoader()).All {
						continue
					}
					pairs = append(pairs, cellPair{name: a.name + " ⟷ " + b.name, a: a, b: b})
				}
			}
		}
	}
	return pairs, lists
}

// sharedListPrefixes: for every shared list, its members up to the last
// one sharedListPairs pairs (in its name order). A subset that keeps them
// yields exactly the same pairs as the whole corpus: every member it adds
// sorts after them and, as the whole corpus already shows, pairs with none
// of the earlier members inside the per-list budget (#663).
func sharedListPrefixes(chains []namedChain) []string {
	groups := map[string][]namedChain{}
	var order []string
	for _, nc := range chains {
		if len(nc.chain.RA.Stages) == 0 || !narrowedListChain(nc.chain) {
			continue
		}
		k := fmt.Sprintf("%p#%s", &nc.chain.RA.Stages[0], nc.chain.Tracked)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], nc)
	}
	var out []string
	for _, k := range order {
		g := groups[k]
		sort.Slice(g, func(i, j int) bool { return g[i].name < g[j].name })
		n, last := 0, -1
		for i := 0; i < len(g) && n < maxPairsPerList; i++ {
			ri := Analyze(g[i].chain, jqsupport.ModuleLoader()).String()
			for j := i + 1; j < len(g) && n < maxPairsPerList; j++ {
				if Analyze(g[j].chain, jqsupport.ModuleLoader()).String() == ri {
					continue
				}
				n++
				if j > last {
					last = j
				}
			}
		}
		for i := 0; i <= last; i++ {
			out = append(out, g[i].name)
		}
	}
	return out
}

func mergeVocab(a, b vocab) vocab {
	var v vocab
	seenC, seenK, seenS := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, x := range []vocab{a, b} {
		for _, c := range x.chains {
			if k := strings.Join(c, "."); !seenC[k] {
				seenC[k] = true
				v.chains = append(v.chains, c)
			}
		}
		for _, k := range x.keys {
			if !seenK[k] {
				seenK[k] = true
				v.keys = append(v.keys, k)
			}
		}
		for _, s := range x.strs {
			if !seenS[s] {
				seenS[s] = true
				v.strs = append(v.strs, s)
			}
		}
	}
	sort.Strings(v.keys)
	sort.Strings(v.strs)
	return v
}

type pairStats struct {
	mu sync.Mutex
	// trials: UPDATEs applied to a shared list; split: trials where exactly one
	// cell's projection changed; splitProved: split trials where that cell's
	// bytes changed AND the other cell's bytes stayed identical (the read sets
	// are not only different, they separate real outputs).
	pairs, trials, split, splitProved, emulations int
	failures                                      []string
}

func (st *pairStats) fail(s string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.failures) < 20 {
		st.failures = append(st.failures, s)
	}
}

// fuzzPair applies random UPDATEs to one shared list and holds each cell to
// its own projection. Returns true on a counterexample.
func fuzzPair(p cellPair, seed int64, trials int, st *pairStats) bool {
	ia := Analyze(p.a.chain, jqsupport.ModuleLoader())
	ib := Analyze(p.b.chain, jqsupport.ModuleLoader())
	st.mu.Lock()
	st.pairs++
	st.mu.Unlock()
	if ia.All || ib.All {
		return false
	}
	r := rand.New(rand.NewSource(seed))
	g := &gen{r: r, v: mergeVocab(chainVocab(p.a.chain), chainVocab(p.b.chain))}
	for t := 0; t < trials; t++ {
		env := chainEnvelope(g, p.a.chain, nil)
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
		reads := ia.Reads
		if r.Intn(2) == 0 {
			reads = ib.Reads
		}
		after := g.mutate(before, reads)
		items2 := append([]any{}, items...)
		items2[i] = after
		eqA := bytes.Equal(Project(before, ia.Reads), Project(after, ia.Reads))
		eqB := bytes.Equal(Project(before, ib.Reads), Project(after, ib.Reads))
		a1, a2 := emulate(p.a.chain, items, env), emulate(p.a.chain, items2, env)
		b1, b2 := emulate(p.b.chain, items, env), emulate(p.b.chain, items2, env)
		st.mu.Lock()
		st.trials++
		st.emulations += 4
		if eqA != eqB {
			st.split++
			// the cell whose projection changed must show it; the other must not
			chX, chY, unX, unY := a1, a2, b1, b2
			if eqA {
				chX, chY, unX, unY = b1, b2, a1, a2
			}
			if okOutcome(chX) && okOutcome(chY) && !bytes.Equal(chX.body, chY.body) && sameOutcome(unX, unY) {
				st.splitProved++
			}
		}
		st.mu.Unlock()
		for _, c := range []struct {
			nc   namedChain
			in   Interest
			eq   bool
			x, y emuOutcome
		}{{p.a, ia, eqA, a1, a2}, {p.b, ib, eqB, b1, b2}} {
			if c.eq && (okOutcome(c.x) || okOutcome(c.y)) && !sameOutcome(c.x, c.y) {
				st.fail("SHARED-LIST counterexample (this cell's projection unchanged, its output changed)\n  pair " + p.name + "\n" +
					describe(c.nc, c.in, items, before, after, c.x, c.y, env))
				return true
			}
		}
	}
	return false
}

func runPairFuzz(pairs []cellPair, seed int64, trials int) *pairStats {
	st := &pairStats{}
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				fuzzPair(pairs[i], chainSeed(seed*2+1, pairs[i].name), trials, st)
			}
		}()
	}
	for i := range pairs {
		work <- i
	}
	close(work)
	wg.Wait()
	return st
}
