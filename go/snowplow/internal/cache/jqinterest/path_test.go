package jqinterest

import (
	"bytes"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/jqutil"
)

func TestProject_ShapeCarrying(t *testing.T) {
	r := []Read{{Path: Path{{Key: "status"}, {Key: "phase"}}, Mode: ModeKind}}
	base := map[string]any{"status": map[string]any{"phase": "Ready", "x": 1.0}}
	same := map[string]any{"status": map[string]any{"phase": "Failed", "y": 2.0}} // kind unchanged
	gone := map[string]any{"status": map[string]any{}}                            // key absent
	null := map[string]any{"status": map[string]any{"phase": nil}}                // present, null
	str := map[string]any{"status": "oops"}                                       // parent not an object
	eq := func(a, b map[string]any) bool { return bytes.Equal(Project(a, r), Project(b, r)) }
	if !eq(base, same) {
		t.Error("a kind read must ignore a same-kind value change and sibling keys")
	}
	for name, other := range map[string]map[string]any{"absent": gone, "null": null, "parent-string": str} {
		if eq(base, other) {
			t.Errorf("a kind read must see %s", name)
		}
	}
	if eq(gone, null) {
		t.Error("absent and null must project differently")
	}
}

func TestProject_ShapeAndWhole(t *testing.T) {
	list := Path{{Key: "spec"}, {Key: "list"}}
	shape := []Read{{Path: list, Mode: ModeShape}}
	whole := []Read{{Path: list, Mode: ModeWhole}}
	a := map[string]any{"spec": map[string]any{"list": []any{1.0, 2.0}}}
	b := map[string]any{"spec": map[string]any{"list": []any{1.0, 3.0}}}
	c := map[string]any{"spec": map[string]any{"list": []any{1.0}}}
	if !bytes.Equal(Project(a, shape), Project(b, shape)) || bytes.Equal(Project(a, shape), Project(c, shape)) {
		t.Error("shape must see the length and not the elements")
	}
	if bytes.Equal(Project(a, whole), Project(b, whole)) {
		t.Error("whole must see the elements")
	}
}

func TestProject_ExclusionsAndAnySteps(t *testing.T) {
	r := []Read{{Path: Path{}, Mode: ModeWhole, Excl: []Path{{{Key: "metadata"}, {Key: "managedFields"}}}}}
	a := map[string]any{"metadata": map[string]any{"name": "x", "managedFields": []any{"t1"}}}
	b := map[string]any{"metadata": map[string]any{"name": "x", "managedFields": []any{"t2"}}}
	c := map[string]any{"metadata": map[string]any{"name": "y", "managedFields": []any{"t1"}}}
	if !bytes.Equal(Project(a, r), Project(b, r)) {
		t.Error("an excluded subtree must not be observed")
	}
	if bytes.Equal(Project(a, r), Project(c, r)) {
		t.Error("the rest must be observed")
	}
	anyR := []Read{{Path: Path{{Key: "c"}, {Any: true}, {Key: "v"}}, Mode: ModeWhole}}
	x := map[string]any{"c": []any{map[string]any{"v": 1.0, "w": 1.0}}}
	y := map[string]any{"c": []any{map[string]any{"v": 1.0, "w": 2.0}}}
	z := map[string]any{"c": []any{map[string]any{"v": 2.0}}}
	if !bytes.Equal(Project(x, anyR), Project(y, anyR)) || bytes.Equal(Project(x, anyR), Project(z, anyR)) {
		t.Error("an Any step must observe every element's sub-path and nothing else")
	}
}

func TestNormalizeReads_DropsOnlySubsumed(t *testing.T) {
	in := []Read{
		{Path: Path{{Key: "a"}}, Mode: ModeWhole},
		{Path: Path{{Key: "a"}, {Key: "b"}}, Mode: ModeKind},
		{Path: Path{{Key: "a"}}, Mode: ModeWhole},
		{Path: Path{{Key: "c"}}, Mode: ModeShape},
	}
	out := normalizeReads(in)
	if len(out) != 2 {
		t.Fatalf("normalizeReads = %v", out)
	}
}

// MaybeQuery must agree with plumbing jqutil.MaybeQuery: the chain model
// extracts the same jq the resolver evaluates.
func TestMaybeQuery_ParityWithPlumbing(t *testing.T) {
	for _, s := range []string{
		"${ .a }", "plain", "x ${ {a: 1} } y", "${ .a } ${ .b }", "${ unbalanced", "",
		"${ [.list[] | {x: .y}] }", "${}",
	} {
		q1, ok1 := MaybeQuery(s)
		q2, ok2 := jqutil.MaybeQuery(s)
		if q1 != q2 || ok1 != ok2 {
			t.Errorf("MaybeQuery(%q) = (%q,%v), plumbing = (%q,%v)", s, q1, ok1, q2, ok2)
		}
	}
}

func TestAnalyzeProgram_Verdicts(t *testing.T) {
	cases := []struct {
		prog     string
		all      bool
		has, not []string
		member   bool
	}{
		{prog: `[.items[] | {name: .metadata.name, state: .status.state}]`,
			has: []string{`whole ."metadata"."name"`, `whole ."status"."state"`}, not: []string{`whole .`}},
		{prog: `.items | length`, not: []string{`whole .`}},
		{prog: `[.items[]? | del(.metadata.managedFields)]`, has: []string{`whole . minus{."metadata"."managedFields"}`}},
		{prog: `[.items[] | select(.metadata.name == "x" and .metadata.namespace == "y")]`, member: true},
		{prog: `[.items[] | select(.status.phase == "x")]`, has: []string{`whole .`}},
		{prog: `[.items[] | select(.metadata.name == "x")] | length`, member: true},
		{prog: `([.items[] | 1] | length | tostring) as $c | [.items[] | select(.metadata.name == $c)]`, member: false},
		{prog: `input`, all: true},
		{prog: `now`, all: true},
		{prog: `label $f | 1`, all: true},
		{prog: `def r: if type == "array" then map(r) else . end; [.items[] | .spec | r]`, has: []string{`whole ."spec"`}},
	}
	for _, c := range cases {
		in := AnalyzeProgram(c.prog, nil)
		s := in.String()
		if in.All != c.all {
			t.Errorf("%s: All=%v want %v (%s)", c.prog, in.All, c.all, s)
			continue
		}
		for _, h := range c.has {
			if !strings.Contains(s, h) {
				t.Errorf("%s: missing %q in %s", c.prog, h, s)
			}
		}
		for _, n := range c.not {
			if strings.Contains(s, n+";") || strings.Contains(s, n+"}") {
				t.Errorf("%s: unexpected %q in %s", c.prog, n, s)
			}
		}
		if (in.Membership != nil) != c.member {
			t.Errorf("%s: membership=%v want %v (%s)", c.prog, in.Membership != nil, c.member, s)
		}
	}
}
