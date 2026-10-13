package jqinterest

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// A concrete membership oracle for Membership claims whose constants are
// literal (KeyGuard.Values set). PR-2's event side has to decide the same
// question. A claimed NON-member must never change the cell's output.

// pathValue returns the value at a key-only path, and whether it is present.
func pathValue(item any, p Path) (any, bool) {
	cur := item
	for _, s := range p {
		m, ok := cur.(map[string]any)
		if s.Any || !ok {
			return nil, false
		}
		if cur, ok = m[s.Key]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

// claimedMember evaluates the DNF on item. known=false when a guard's
// constant is not a literal set (it is decided by the extras at run time).
func claimedMember(m *Membership, item any) (member, known bool) {
	for _, d := range m.Disjuncts {
		all := true
		for _, k := range d {
			if k.Values == nil {
				return false, false
			}
			v, _ := pathValue(item, k.Path) // absent reads as null
			hit := false
			for _, c := range k.Values {
				if jsonEqual(v, c) {
					hit = true
				}
			}
			if !hit {
				all = false
			}
		}
		if all {
			return true, true
		}
	}
	return false, true
}

// TestMembership_DNF pins review finding 4. Two separate selects are two
// DISJUNCTS: an object that passes either one is a member. A flat AND list
// would call (ns1, a) a non-member of
// `select(.name=="a") + select(.namespace=="ns2")` and skip its real change.
func TestMembership_DNF(t *testing.T) {
	two := oneStage(`[.s.items[] | select(.metadata.name == "a")] + [.s.items[] | select(.metadata.namespace == "ns2")]`, "")
	one := oneStage(`[.s.items[] | select(.metadata.name == "a" and .metadata.namespace == "ns2")]`, "")
	stream := oneStage(`[.s.items[] | select(.metadata.name == ("a", "b"))]`, "")
	it := func(name, ns, v string) map[string]any { return advItem(name, ns, map[string]any{"v": v}) }

	ti, oi, si := Analyze(two, jqsupport.ModuleLoader()), Analyze(one, jqsupport.ModuleLoader()), Analyze(stream, jqsupport.ModuleLoader())
	t.Logf("two selects: %s", ti)
	t.Logf("one select:  %s", oi)
	t.Logf("stream:      %s", si)
	if ti.Membership != nil {
		if len(ti.Membership.Disjuncts) != 2 {
			t.Errorf("two selects must be two disjuncts: %s", ti)
		}
		if m, known := claimedMember(ti.Membership, it("a", "ns1", "1")); !known || !m {
			t.Errorf("(ns1, a) passes the first select and must be a member: %s", ti)
		}
	}
	// The real output confirms (ns1, a) is a member: its UPDATE changes it.
	env := emuEnv{identity: 1}
	x, y := emulate(two, []any{it("a", "ns1", "1")}, env), emulate(two, []any{it("a", "ns1", "2")}, env)
	if sameOutcome(x, y) {
		t.Fatalf("witness is vacuous: %s / %s", x, y)
	}
	if oi.Membership == nil || len(oi.Membership.Disjuncts) != 1 || len(oi.Membership.Disjuncts[0]) != 2 {
		t.Errorf("one select with an AND is one disjunct of two guards: %s", oi)
	} else if m, _ := claimedMember(oi.Membership, it("a", "ns1", "1")); m {
		t.Errorf("(ns1, a) fails namespace==ns2 and is not a member of the conjunctive select: %s", oi)
	}
	if si.Membership == nil {
		t.Fatalf("stream constant: no membership: %s", si)
	}
	k := si.Membership.Disjuncts[0][0]
	if len(k.Values) != 2 || valuesString(k.Values) != `{"a", "b"}` {
		t.Errorf(`stream constant ("a","b") must carry its value set, got %s`, k)
	}
	for _, n := range []string{"a", "b"} {
		if m, known := claimedMember(si.Membership, it(n, "ns1", "1")); !known || !m {
			t.Errorf("%s must be a member of the stream guard: %s", n, si)
		}
	}
}

// TestMembership_ReviewerShapes keeps reviewer-603's membership shapes as
// arms. Whatever each one is analysed to, the differential fuzz (read-set
// UPDATEs, and non-member ADD/DELETE/UPDATE when membership is claimed) must
// find no counterexample.
func TestMembership_ReviewerShapes(t *testing.T) {
	for i, tc := range []struct{ name, prog string }{
		{"two-selects-disjunctive", `[.s.items[] | select(.metadata.name == "a")] + [.s.items[] | select(.metadata.namespace == "ns2")]`},
		{"one-select-conjunctive", `[.s.items[] | select(.metadata.name == "a" and .metadata.namespace == "ns2")]`},
		{"stream-const", `[.s.items[] | select(.metadata.name == ("a", "b"))]`},
		{"label-key", `[.s.items[] | select(.metadata.labels.app == "x")] | length`},
		{"after-sort", `.s.items | sort_by(.metadata.name) | .[] | select(.metadata.name == "a")`},
		{"collected-then-select", `[.s.items[] | .metadata.name] | map(select(. == "a"))`},
		{"if-launder", `[.s.items[] | select(.metadata.name == "a") | (.spec.x == 1) as $b | (if $b then "b" else "c" end)] as $k | [.s.items[] | select(.metadata.name == $k[0])]`},
		{"isempty-guard", `if isempty(.s.items[] | select(.metadata.labels.app == "x")) then "none" else "some" end`},
		{"opt-key", `[.s.items[] | select(.spec.k? == "x")]`},
		{"loc-unbound", `[.s.items[] | select(.metadata.name == $__loc__.file)]`},
	} {
		st := &fuzzStats{}
		c := oneStage(tc.prog, "")
		t.Logf("%-24s %s", tc.name, Analyze(c, jqsupport.ModuleLoader()))
		if fuzzChain(namedChain{name: tc.name, chain: c}, int64(9100+i), armTrials, st) {
			for _, f := range st.failures {
				t.Errorf("%s", f)
			}
		}
	}
}

// TestKeyGuard_EveryClaimedGuardIsDecidable pins the KeyGuard contract
// (re-gate C2): every guard of a claimed Membership is Decidable. Its
// literal value set is known, or its constant is extras-derived. A constant
// object folds into Values. A guard that is neither (an array literal) drops
// the whole claim into MembershipBlockers.
func TestKeyGuard_EveryClaimedGuardIsDecidable(t *testing.T) {
	obj := Analyze(oneStage(`[.s.items[] | select(.metadata.labels == {"app": "x"})]`, ""), jqsupport.ModuleLoader())
	if obj.Membership == nil {
		t.Fatalf("constant-object guard: no membership: %s", obj)
	}
	k := obj.Membership.Disjuncts[0][0]
	if k.ConstClass != "literal" || len(k.Values) != 1 || !jsonEqual(k.Values[0], map[string]any{"app": "x"}) {
		t.Errorf(`labels == {"app":"x"} must fold into Values: %s (class %s)`, k, k.ConstClass)
	}
	for _, it := range []struct {
		labels map[string]any
		member bool
	}{{map[string]any{"app": "x"}, true}, {map[string]any{"app": "y"}, false}, {map[string]any{"app": "x", "b": "c"}, false}} {
		item := map[string]any{"metadata": map[string]any{"name": "o", "namespace": "ns1", "labels": it.labels}}
		if m, known := claimedMember(obj.Membership, item); !known || m != it.member {
			t.Errorf("labels %v: claimed member=%v known=%v, want %v", it.labels, m, known, it.member)
		}
	}
	// Constants that are literal-class but do not fold: an array literal,
	// and a literal computed by a builtin. Both are bound outside the select,
	// so the guard is recognised, and then refused as undecidable.
	for _, prog := range []string{
		`["a"] as $c | [.s.items[] | select(.metadata.name == $c)]`,
		`("A" | ascii_downcase) as $c | [.s.items[] | select(.metadata.name == $c)]`,
	} {
		in := Analyze(oneStage(prog, ""), jqsupport.ModuleLoader())
		if in.Membership != nil {
			t.Errorf("%s: an undecidable guard must not claim membership: %s", prog, in)
		}
		blocked := false
		for _, b := range in.MembershipBlockers {
			blocked = blocked || strings.HasPrefix(b, "undecidable key constant")
		}
		if !blocked {
			t.Errorf("%s: the refusal must be recorded as an undecidable-constant blocker: %v (%s)", prog, in.MembershipBlockers, in)
		}
	}
	// The contract over everything the package analyses: corpus chains,
	// mutable-UAF variants, and every arm.
	_, chains := loadCorpus(t)
	chains = append(chains, uafVariants(chains)...)
	for _, a := range soundnessArms() {
		chains = append(chains, namedChain{name: a.name, chain: a.chain})
	}
	claims := 0
	for _, nc := range chains {
		in := Analyze(nc.chain, jqsupport.ModuleLoader())
		if in.Membership == nil {
			continue
		}
		claims++
		for _, d := range in.Membership.Disjuncts {
			for _, k := range d {
				if !k.Decidable() {
					t.Errorf("%s: claimed guard is not decidable: %s (class %s)", nc.name, k, k.ConstClass)
				}
			}
		}
	}
	if claims == 0 {
		t.Fatal("no membership claims checked")
	}
	t.Logf("membership claims checked: %d, every guard decidable", claims)
}
