package jqinterest

import (
	"fmt"
	"sort"
	"strings"
)

// absKind classifies an abstract value. The abstract value of a jq expression
// over-approximates the STREAM of values it can produce, and records — as a
// side effect into the analysis state — every part of the tracked list that
// can influence that stream.
type absKind uint8

const (
	// aConst: independent of the tracked list (literals, extras, other stages'
	// data). Reading it records nothing.
	aConst absKind = iota + 1
	// aRef: a value reached by navigation from the tracked root at path. Its
	// content is NOT yet recorded; an operation that observes it records the
	// matching Read.
	aRef
	// aOpaque: derived from the tracked list; everything that influenced it is
	// ALREADY recorded. Observing it records nothing further.
	aOpaque
	// aObj: an object built by the program. Known fields map to their values;
	// rest (when non-nil) is the value of any other key (open object).
	aObj
	// aArr: an array built by the program; every element is described by elem.
	// Its length and order are functions of reads already recorded.
	aArr
	// aUnion: any of alts (a stream may yield values of different provenance).
	// A union with no alts is the empty stream.
	aUnion
)

// constClass is where a constant comes from, for membership-key provenance.
type constClass uint8

const (
	ccLiteral constClass = iota + 1 // a literal in the program
	ccExtras                        // request extras / slice: already folded into the cell key
	ccOther                         // another stage's data: a different dependency edge
)

func (c constClass) String() string {
	switch c {
	case ccLiteral:
		return "literal"
	case ccExtras:
		return "extras"
	case ccOther:
		return "other_edge"
	}
	return "unknown"
}

type abs struct {
	k absKind

	// aRef
	path    Path
	excl    []Path // relative to path
	guarded bool   // a membership-guarded element (only the cell's own objects reach here)
	inst    int    // iteration instance: which `.[]` produced this element (0 = not an element)

	// aConst
	cc     constClass
	lit    any
	hasLit bool
	src    string // printable source of the constant (membership provenance)

	// aObj
	fields map[string]*abs
	rest   *abs

	// aArr
	elem *abs

	// aUnion
	alts []*abs

	// cu: whether this value is produced at all (how many times) depends on
	// elements of the tracked list that have not passed a membership guard.
	cu bool
	// elemCU (aArr): the element count depends on unguarded elements.
	elemCU bool
}

var (
	absOpaque = &abs{k: aOpaque}
	absEmpty  = &abs{k: aUnion}
)

func constOf(cc constClass, src string) *abs { return &abs{k: aConst, cc: cc, src: src} }

func literal(v any, src string) *abs {
	return &abs{k: aConst, cc: ccLiteral, lit: v, hasLit: true, src: src}
}

func joinClass(a, b constClass) constClass {
	if a > b {
		return a
	}
	return b
}

// union joins alternatives, flattening nested unions and dropping duplicates.
func union(xs ...*abs) *abs {
	var alts []*abs
	seen := map[string]bool{}
	var add func(x *abs)
	add = func(x *abs) {
		if x == nil {
			return
		}
		if x.k == aUnion {
			for _, a := range x.alts {
				add(a)
			}
			return
		}
		key := x.sig()
		if seen[key] {
			return
		}
		seen[key] = true
		alts = append(alts, x)
	}
	for _, x := range xs {
		add(x)
	}
	if len(alts) == 1 {
		return alts[0]
	}
	return &abs{k: aUnion, alts: alts}
}

// sig is a structural signature used to de-duplicate union alternatives.
func (a *abs) sig() string {
	var b strings.Builder
	a.writeSig(&b, 0)
	return b.String()
}

func (a *abs) writeSig(b *strings.Builder, depth int) {
	if depth > 12 {
		// Too deep to compare structurally: never treat as a duplicate.
		fmt.Fprintf(b, "&%p", a)
		return
	}
	if a.cu {
		b.WriteString("~")
	}
	if a.elemCU {
		b.WriteString("^")
	}
	switch a.k {
	case aConst:
		b.WriteString("C")
		b.WriteString(a.cc.String())
		if a.hasLit {
			b.WriteString("=")
			b.WriteString(a.src)
		}
	case aRef:
		b.WriteString("R")
		b.WriteString(a.path.String())
		for _, e := range a.excl {
			b.WriteString("-")
			b.WriteString(e.String())
		}
		if a.guarded {
			b.WriteString("!g")
		}
		b.WriteString("#")
		b.WriteString(itoa(a.inst))
	case aOpaque:
		b.WriteString("O")
	case aObj:
		b.WriteString("{")
		keys := make([]string, 0, len(a.fields))
		for k := range a.fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(k)
			b.WriteString(":")
			a.fields[k].writeSig(b, depth+1)
			b.WriteString(",")
		}
		if a.rest != nil {
			b.WriteString("*:")
			a.rest.writeSig(b, depth+1)
		}
		b.WriteString("}")
	case aArr:
		b.WriteString("[")
		a.elem.writeSig(b, depth+1)
		b.WriteString("]")
	case aUnion:
		b.WriteString("U(")
		for _, x := range a.alts {
			x.writeSig(b, depth+1)
			b.WriteString("|")
		}
		b.WriteString(")")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var d []byte
	for i > 0 {
		d = append([]byte{byte('0' + i%10)}, d...)
		i /= 10
	}
	if neg {
		d = append([]byte{'-'}, d...)
	}
	return string(d)
}

// isConst reports whether a (and everything in it) is independent of the
// tracked list. A union of constants is constant; an empty stream is too.
func (a *abs) isConst() bool {
	switch a.k {
	case aConst:
		return true
	case aObj:
		for _, f := range a.fields {
			if !f.isConst() {
				return false
			}
		}
		return a.rest == nil || a.rest.isConst()
	case aArr:
		return a.elem.isConst()
	case aUnion:
		for _, x := range a.alts {
			if !x.isConst() {
				return false
			}
		}
		return true
	}
	return false
}

// constClassOf returns the joined class of a constant value.
func (a *abs) constClassOf() constClass {
	switch a.k {
	case aConst:
		return a.cc
	case aObj:
		c := ccLiteral
		for _, f := range a.fields {
			c = joinClass(c, f.constClassOf())
		}
		if a.rest != nil {
			c = joinClass(c, a.rest.constClassOf())
		}
		return c
	case aArr:
		return a.elem.constClassOf()
	case aUnion:
		c := ccLiteral
		for _, x := range a.alts {
			c = joinClass(c, x.constClassOf())
		}
		return c
	}
	return ccOther
}

// derived returns the value of an operation whose result depends on xs:
// constant when they all are (joined class), opaque otherwise. The caller has
// already recorded the reads the operation makes.
func derived(src string, xs ...*abs) *abs {
	c := ccLiteral
	for _, x := range xs {
		if !x.isConst() {
			return absOpaque
		}
		c = joinClass(c, x.constClassOf())
	}
	return constOf(c, src)
}

// mapAbs applies f to every alternative of a and joins the results.
func mapAbs(a *abs, f func(*abs) *abs) *abs {
	if a.k == aUnion {
		out := make([]*abs, 0, len(a.alts))
		for _, x := range a.alts {
			out = append(out, f(x))
		}
		return union(out...)
	}
	return f(a)
}

// hasUnguardedElem reports whether a contains a tracked list element that has
// not passed a membership guard.
func (a *abs) hasUnguardedElem() bool {
	switch a.k {
	case aRef:
		return a.inst != 0 && !a.guarded
	case aObj:
		for _, f := range a.fields {
			if f.hasUnguardedElem() {
				return true
			}
		}
		return a.rest != nil && a.rest.hasUnguardedElem()
	case aArr:
		return a.elemCU || a.elem.hasUnguardedElem()
	case aUnion:
		for _, x := range a.alts {
			if x.hasUnguardedElem() {
				return true
			}
		}
	}
	return false
}

// guardCopy marks every element Ref of instance inst as guarded.
func (a *abs) guardCopy(inst int) *abs {
	switch a.k {
	case aRef:
		if a.inst == inst && !a.guarded {
			c := *a
			c.guarded = true
			return &c
		}
		return a
	case aObj:
		c := &abs{k: aObj, fields: make(map[string]*abs, len(a.fields))}
		for k, f := range a.fields {
			c.fields[k] = f.guardCopy(inst)
		}
		if a.rest != nil {
			c.rest = a.rest.guardCopy(inst)
		}
		return c
	case aArr:
		return &abs{k: aArr, elem: a.elem.guardCopy(inst), elemCU: a.elemCU, cu: a.cu}
	case aUnion:
		out := make([]*abs, len(a.alts))
		for i, x := range a.alts {
			out[i] = x.guardCopy(inst)
		}
		return union(out...)
	}
	return a
}

// instances collects the element instances a carries (0 excluded).
func (a *abs) instances(into map[int]bool) {
	switch a.k {
	case aRef:
		if a.inst != 0 {
			into[a.inst] = true
		}
	case aObj:
		for _, f := range a.fields {
			f.instances(into)
		}
		if a.rest != nil {
			a.rest.instances(into)
		}
	case aArr:
		a.elem.instances(into)
	case aUnion:
		for _, x := range a.alts {
			x.instances(into)
		}
	}
}
