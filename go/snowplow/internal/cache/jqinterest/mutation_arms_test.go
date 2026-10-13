package jqinterest

import (
	"bytes"
	"strings"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// The soundness arms (S-arms), RED-first. Each arm is a chain built so that
// one analysis rule is load-bearing. With the rule in place the differential
// fuzz finds NO counterexample (green); with the rule switched off through its
// named mutation (mutation.go) the SAME fuzz, same seeds, finds one (RED). An
// S-arm that no mutation turns RED is not a detector, so every rule the arms
// claim to pin has its mutation here.

func strp(s string) *string { return &s }

// oneStage is a RESTAction whose only stage "s" is the tracked LIST.
func oneStage(stageFilter string, raFilter string) Chain {
	st := Stage{Name: "s"}
	if stageFilter != "" {
		st.Filter = strp(stageFilter)
	}
	c := Chain{RA: RESTAction{Stages: []Stage{st}}, Tracked: "s"}
	if raFilter != "" {
		c.RA.Filter = strp(raFilter)
	}
	return c
}

type arm struct {
	name     string
	mutation string
	chain    Chain
}

func soundnessArms() []arm {
	uaf := oneStage(`[.s.items[] | .metadata.name]`, "")
	uaf.RA.Stages[0].UAF = &UAF{NamespaceFrom: ".spec.target"}

	uafName := oneStage(`[.s.items[] | .metadata.name]`, "")
	uafName.RA.Stages[0].UAF = &UAF{NameFrom: ".spec.owner", NameSpecificVerb: true}

	rrt := oneStage("", `{list: [.s.items[] | {x: .spec.x}]}`)
	rrt.Widget = &Widget{ResourcesRefs: []ResourcesRef{{Iterator: "${ .list }", Fields: []string{"${ .x.y }"}}}}

	other := oneStage(`[.s.items[] | .metadata.name]`, `{t: .t}`)
	other.RA.Stages = append(other.RA.Stages, Stage{Name: "t", DependsOn: "s", Iterator: ".s", Path: `${ "/t/" + . }`})

	return []arm{
		{"F-nav: navigation observes the kind along the path (an error on a dropped value)", mutNavNoKind,
			oneStage(`[.s.items[] | (.spec.x | .y | 1)]`, "")},
		{"F-iter: iteration observes the length", mutIterNoShape,
			oneStage(`[.s.items[] | [.spec.list[]? | 1] | length]`, "")},
		{"F-length: length observes the length, not only the kind", mutLengthAsKind,
			oneStage(`[.s.items[] | .spec.list | length]`, "")},
		{"F1: a compared field is observed whole", mutEqAsKind,
			oneStage(`[.s.items[] | .spec.x == "a"]`, "")},
		{"F-alt: // observes the truthiness of its left side", mutAltNoTruth,
			oneStage(`[.s.items[] | [.spec.list[]? // empty] | length]`, "")},
		{"F-select: select observes its predicate", mutSelectNoTruth,
			oneStage(`[.s.items[] | [.spec.list[]? | select(.)] | length]`, "")},
		{"F-if: if observes its condition", mutIfNoTruth,
			oneStage(`[.s.items[] | [.spec.list[]? | if . then 1 else empty end] | length]`, "")},
		{"F4c: sort_by(f) observes f whole (an order change crosses a page boundary)", mutSortByAsNav,
			oneStage(`[.s.items[] | .spec.list | arrays | map(objects) | sort_by(.k) | map(.n)]`, "")},
		{"F-generic: a builtin observes its input whole", mutGenericNoInput,
			oneStage(`[.s.items[] | .spec.x | tostring]`, "")},
		{"F-catch: try/catch observes whole what its body touched (messages embed values)", mutCatchNotStrict,
			oneStage(`[.s.items[] | try (.spec.x | .y) catch .]`, "")},
		{"F12: UAF namespaceFrom reads a mutable field", mutUAFExcluded, uaf},
		{"F12b: UAF nameFrom (name-specific verb) reads a mutable field", mutUAFNameFromExcluded, uafName},
		{"F-rrt: resourcesRefs fields are strict (an error message is written into the ref)", mutRRTNotStrict, rrt},
		{"F-strict-receiver: a strict navigation observes its receiver (the message previews it)", mutStrictNoReceiver, rrt},
		{"F-dependent: a later stage's request built from the list", mutOtherStagesSkip, other},
		{"F-del: del(.a.b) excludes .a.b only", mutDelExclParent,
			oneStage(`[.s.items[] | del(.metadata.annotations) | .metadata.name]`, "")},
		{"F3b-total: a key path that can error on a non-member blocks membership", mutKeyNoTotality,
			oneStage(`[.s.items[] | select(.spec.k.v == "x")]`, "")},
		{"F3b-agg: an aggregate over non-members blocks membership", mutKeyNoAggregation,
			oneStage(`{a: [.s.items[] | select(.metadata.name == "x")], n: ([.s.items[] | 1] | length)}`, "")},
		// #603 PR-1 review (reviewer-603) findings 1-5.
		{"A1: a user binding of $ENV shadows the built-in", mutBuiltinVarsFirst,
			oneStage(`[.s.items[] | .spec.secret] as $ENV | $ENV`, "")},
		{"A2: a $ENV function parameter shadows the built-in", mutBuiltinVarsFirst,
			oneStage(`def f($ENV): $ENV; [.s.items[] | f(.spec.secret)]`, "")},
		{"A4: a stage named extras keeps its own response under .extras", mutPigConstWins,
			Chain{RA: RESTAction{Stages: []Stage{{Name: "extras", Filter: strp(`[.extras.items[] | .spec.secret]`)}}}, Tracked: "extras"}},
		{"A5: a stage named slice sees its response when the request is not paginated", mutPigConstWins,
			Chain{RA: RESTAction{Stages: []Stage{{Name: "slice", Filter: strp(`[.slice.items[] | .spec.secret]`)}}}, Tracked: "slice"}},
		{"A6: an extras key equal to the stage name pre-seeds dict[stage]: [seed, response]", mutNoExtrasSeed,
			oneStage("", `.s | if type == "array" then .[1].items[0].spec.secret else "n" end`)},
		{"M4: two selects are two disjuncts, not one AND", mutKeyFlatAND,
			oneStage(`[.s.items[] | select(.metadata.name == "a")] + [.s.items[] | select(.metadata.namespace == "ns2")]`, "")},
		{"R2: the key field of a select guard is read", mutKeyNotRead,
			oneStage(`[.s.items[] | select(.metadata.name == "a") | .spec.v]`, "")},
		{"R5: has(k) observes the key set, not only the kind", mutHasAsKind,
			oneStage(`[.s.items[] | .spec | has("x")]`, "")},
		// reviewer-603 re-gate (C1).
		{"N4: a key constant with a non-literal alternative has no literal value set", mutLiteralValuesSkip,
			oneStage(`.extras.name as $n | [.s.items[] | select(.metadata.name == ("zzz", $n))]`, "")},
		// #666: every rule of toItemReads / itemExcl that maps a read of the
		// list envelope onto item reads, each with a fixed witness (an
		// UPDATE of a field the output carries) so it is RED by construction,
		// not by which chains a seed reaches.
		{"R4: a whole .items read (stage filter) observes every item whole", mutItemsWholeDropped,
			oneStage(`.s.items | tojson`, "")},
		{"R4b: a whole .items read (spec.filter) observes every item whole", mutItemsWholeDropped,
			oneStage("", `.s.items`)},
		{"E-whole: a whole read of the envelope observes every item whole", mutEnvelopeWholeDropped,
			oneStage(`.s | tojson`, "")},
		{"E-iter: .[] of the envelope reaches .items", mutEnvelopeIterDropped,
			oneStage(`[.s[]] | tojson`, "")},
		{"E-elem: a read under .items[] is an item read", mutItemsElemDropped,
			oneStage(`[.s.items[] | .spec.secret]`, "")},
		{"E-excl: an envelope exclusion (del(.kind)) is not an item exclusion", mutItemExclMisapplied,
			oneStage(`.s | del(.kind) | tojson`, "")},
	}
}

// armWitness: per mutation, one UPDATE the random search rarely builds; it
// joins the search as a fixed trial.
func armWitness() map[string]*[2]map[string]any {
	return map[string]*[2]map[string]any{
		mutSortByAsNav: sortWitness(),
		// #663: the random search needs ~750 trials to flip one element's
		// truthiness with its kind unchanged; the default budget is 500.
		// false -> true keeps every kind and the length, and changes
		// `// empty` and select(.).
		mutAltNoTruth:    truthWitness(),
		mutSelectNoTruth: truthWitness(),
		// reviewer-603's R2 witness: the name a -> b moves the object out of
		// select(.metadata.name == "a").
		mutKeyNotRead: {advItem("a", "ns1", map[string]any{"v": "1"}), advItem("b", "ns1", map[string]any{"v": "1"})},
		// reviewer-603's R5 witness: adding key x flips has("x").
		mutHasAsKind: {advItem("a", "ns1", map[string]any{"v": "1"}), advItem("a", "ns1", map[string]any{"v": "1", "x": 1.0})},
		// #666 (reviewer-603's R4 witness): spec.secret a -> b changes an
		// output that carries the whole item.
		mutItemsWholeDropped:    secretWitness(),
		mutEnvelopeWholeDropped: secretWitness(),
		mutEnvelopeIterDropped:  secretWitness(),
		mutItemsElemDropped:     secretWitness(),
		// the item's own kind changes; the envelope's kind is what del() drops.
		mutItemExclMisapplied: {advItem("a", "ns1", map[string]any{"v": "1"}), withKind(advItem("a", "ns1", map[string]any{"v": "1"}), "Other")},
	}
}

// secretWitness: spec.secret a -> b.
func secretWitness() *[2]map[string]any {
	return &[2]map[string]any{advItem("a", "ns1", map[string]any{"secret": "a"}), advItem("a", "ns1", map[string]any{"secret": "b"})}
}

func withKind(it map[string]any, kind string) map[string]any {
	it["kind"] = kind
	return it
}

// truthWitness: one list element turns from false to true.
func truthWitness() *[2]map[string]any {
	mk := func(v bool) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "o", "namespace": "ns1"},
			"spec": map[string]any{"list": []any{v}}}
	}
	return &[2]map[string]any{mk(false), mk(true)}
}

// sortWitness: two elements; raising the first one's sort key past the
// second's swaps their order, with every kind unchanged.
func sortWitness() *[2]map[string]any {
	mk := func(k1 float64) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "o", "namespace": "ns1"},
			"spec": map[string]any{"list": []any{
				map[string]any{"k": k1, "n": "first"},
				map[string]any{"k": float64(2), "n": "second"},
			}}}
	}
	return &[2]map[string]any{mk(1), mk(3)}
}

// witnessCounterexample reports whether the arm's witness UPDATE keeps the
// projection while changing the output.
type memberWitnessCase struct {
	env           emuEnv
	before, after map[string]any
}

// memberWitness: per mutation, an object that the claimed membership must
// not call a non-member, the extras that make it a member, and an UPDATE of
// it. The witness fires when the oracle claims a non-member and the UPDATE
// changes the real output.
func memberWitness() map[string]memberWitnessCase {
	return map[string]memberWitnessCase{
		// re-gate N4: name == ("zzz", $n) with extras.name = "a" admits "a".
		mutLiteralValuesSkip: {emuEnv{identity: 1, extras: map[string]any{"name": "a"}},
			advItem("a", "ns1", map[string]any{"v": "1"}), advItem("a", "ns1", map[string]any{"v": "2"})},
	}
}

func witnessCounterexample(a arm) bool {
	if mw, ok := memberWitness()[a.mutation]; ok {
		in := Analyze(a.chain, jqsupport.ModuleLoader())
		if in.Membership != nil {
			if m, known := claimedMember(in.Membership, mw.before); known && !m {
				x, y := emulate(a.chain, []any{mw.before}, mw.env), emulate(a.chain, []any{mw.after}, mw.env)
				if (okOutcome(x) || okOutcome(y)) && !sameOutcome(x, y) {
					return true
				}
			}
		}
	}
	w := armWitness()[a.mutation]
	if w == nil {
		return false
	}
	in := Analyze(a.chain, jqsupport.ModuleLoader())
	if in.All {
		return false
	}
	before, after := w[0], w[1]
	if !bytes.Equal(Project(before, in.Reads), Project(after, in.Reads)) {
		return false
	}
	x, y := emulate(a.chain, []any{before}, emuEnv{identity: 1}), emulate(a.chain, []any{after}, emuEnv{identity: 1})
	return (okOutcome(x) || okOutcome(y)) && !sameOutcome(x, y)
}

func TestSoundnessArms_GreenWithEveryRule(t *testing.T) {
	for i, a := range soundnessArms() {
		st := &fuzzStats{}
		if fuzzChain(namedChain{name: a.name, chain: a.chain}, int64(7000+i), armTrials, st) || witnessCounterexample(a) {
			t.Errorf("%s: counterexample with the rule in place:\n%s", a.name, strings.Join(st.failures, "\n"))
		}
		if st.narrowed == 0 {
			t.Errorf("%s: the chain is not narrowed at all — the arm tests nothing", a.name)
		}
	}
}

func TestSoundnessArms_RedUnderTheirMutation(t *testing.T) {
	for i, a := range soundnessArms() {
		testMutations = map[string]bool{a.mutation: true}
		st := &fuzzStats{}
		found := fuzzChain(namedChain{name: a.name, chain: a.chain}, int64(7000+i), armTrials, st) || witnessCounterexample(a)
		testMutations = nil
		if !found {
			t.Errorf("%s: mutation %q went UNDETECTED in %d trials — the arm is not a detector", a.name, a.mutation, armTrials)
			continue
		}
		t.Logf("RED under %-20s %s", a.mutation, a.name)
	}
}

// F5-style: two cells over the SAME list with DIFFERENT read sets (the
// coordinator's ask). Widget A shows .status.state, widget B shows
// .status.lastSyncedAt. A heartbeat changes only B's field: A's projection is
// unchanged and A's bytes are identical; B's projection changes and B's bytes
// change. The state change is the mirror image.
func TestTwoCellsShareAListWithDifferentReadSets(t *testing.T) {
	base := oneStage("", `{list: [.s.items[] | {name: .metadata.name, state: .status.state, synced: .status.lastSyncedAt}]}`)
	a, b := base, base
	a.Widget = &Widget{DataTemplates: []string{"${ [.list[] | .state] }"}}
	b.Widget = &Widget{DataTemplates: []string{"${ [.list[] | .synced] }"}}
	ia, ib := Analyze(a, jqsupport.ModuleLoader()), Analyze(b, jqsupport.ModuleLoader())
	if ia.All || ib.All {
		t.Fatalf("not narrowed: A=%s B=%s", ia, ib)
	}
	has := func(in Interest, p string) bool {
		for _, r := range in.Reads {
			if r.Path.String() == p && r.Mode == ModeWhole {
				return true
			}
		}
		return false
	}
	state, synced := `."status"."state"`, `."status"."lastSyncedAt"`
	if !has(ia, state) || has(ia, synced) || !has(ib, synced) || has(ib, state) {
		t.Fatalf("read sets: A=%s B=%s", ia, ib)
	}
	item := func(state, synced string) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "i1", "namespace": "ns1"},
			"status": map[string]any{"state": state, "lastSyncedAt": synced}}
	}
	env := emuEnv{identity: 1}
	for _, tc := range []struct {
		name          string
		before, after map[string]any
		aSame, bSame  bool
	}{
		{"heartbeat", item("open", "t1"), item("open", "t2"), true, false},
		{"state change", item("open", "t1"), item("resolved", "t1"), false, true},
	} {
		for _, cell := range []struct {
			chain Chain
			in    Interest
			same  bool
			tag   string
		}{{a, ia, tc.aSame, "A"}, {b, ib, tc.bSame, "B"}} {
			projSame := bytes.Equal(Project(tc.before, cell.in.Reads), Project(tc.after, cell.in.Reads))
			x := emulate(cell.chain, []any{tc.before}, env)
			y := emulate(cell.chain, []any{tc.after}, env)
			if !okOutcome(x) || !okOutcome(y) {
				t.Fatalf("%s/%s: emulation failed: %s / %s", tc.name, cell.tag, x, y)
			}
			if projSame != cell.same || bytes.Equal(x.body, y.body) != cell.same {
				t.Errorf("%s/%s: projection unchanged=%v output unchanged=%v, want both %v (%s → %s)",
					tc.name, cell.tag, projSame, bytes.Equal(x.body, y.body), cell.same, x, y)
			}
		}
	}
}

// F12: UAF namespaceFrom on a mutable field, under identity-narrowed bodies.
// The read set must carry the field, and for some identity a change of it
// must change the narrowed body (so the read is load-bearing).
func TestF12_UAFNamespaceFromIsInTheReadSet(t *testing.T) {
	c := oneStage(`[.s.items[] | .metadata.name]`, "")
	c.RA.Stages[0].UAF = &UAF{NamespaceFrom: ".spec.targetNs"}
	in := Analyze(c, jqsupport.ModuleLoader())
	found := false
	for _, r := range in.Reads {
		if r.Path.String() == `."spec"."targetNs"` && r.Mode == ModeWhole {
			found = true
		}
	}
	if !found {
		t.Fatalf("UAF namespaceFrom read missing: %s", in)
	}
	it := func(ns string) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "o", "namespace": "x"}, "spec": map[string]any{"targetNs": ns}}
	}
	changed := false
	for id := uint32(1); id <= 8 && !changed; id++ {
		for _, ns := range []string{"ns1", "ns2", "ns3", "ns4", "ns5"} {
			x := emulate(c, []any{it("ns0")}, emuEnv{identity: id})
			y := emulate(c, []any{it(ns)}, emuEnv{identity: id})
			if okOutcome(x) && okOutcome(y) && !bytes.Equal(x.body, y.body) {
				changed = true
				break
			}
		}
	}
	if !changed {
		t.Fatal("no identity's narrowed body depends on spec.targetNs — the arm is vacuous")
	}
}

// F12b: UAF nameFrom on a mutable field, under identity-narrowed bodies.
// nameFrom is evaluated only for name-specific verbs (refilter.go
// evalSingleMemo): for a list verb it must NOT be in the read set; for a
// name-specific verb it must be, and changing it must change some identity's
// narrowed body (so the read is load-bearing).
func TestF12b_UAFNameFromIsInTheReadSetForNameSpecificVerbs(t *testing.T) {
	mk := func(nameSpecific bool) Chain {
		c := oneStage(`[.s.items[] | .metadata.name]`, "")
		c.RA.Stages[0].UAF = &UAF{NameFrom: ".spec.owner", NameSpecificVerb: nameSpecific}
		return c
	}
	has := func(in Interest) bool {
		for _, r := range in.Reads {
			if r.Path.String() == `."spec"."owner"` && r.Mode == ModeWhole {
				return true
			}
		}
		return false
	}
	if in := Analyze(mk(false), jqsupport.ModuleLoader()); in.All || has(in) {
		t.Fatalf("list verb: nameFrom is not evaluated, so it must not be read: %s", in)
	}
	c := mk(true)
	in := Analyze(c, jqsupport.ModuleLoader())
	if in.All || !has(in) {
		t.Fatalf("name-specific verb: UAF nameFrom read missing: %s", in)
	}
	it := func(owner string) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "o", "namespace": "ns1"}, "spec": map[string]any{"owner": owner}}
	}
	changed := false
	for id := uint32(1); id <= 8 && !changed; id++ {
		for _, o := range []string{"u1", "u2", "u3", "u4", "u5"} {
			x := emulate(c, []any{it("u0")}, emuEnv{identity: id})
			y := emulate(c, []any{it(o)}, emuEnv{identity: id})
			if okOutcome(x) && okOutcome(y) && !bytes.Equal(x.body, y.body) {
				changed = true
				break
			}
		}
	}
	if !changed {
		t.Fatal("no identity's narrowed body depends on spec.owner — the arm is vacuous")
	}
}
