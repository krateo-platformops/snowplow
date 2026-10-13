package jqinterest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/itchyny/gojq"
)

// topErr aborts the analysis: the program contains something whose read set
// cannot be proved. The verdict is "reads everything" (interest 0).
type topErr struct{ why string }

func (t topErr) Error() string { return "jqinterest: unprovable: " + t.why }

func top(format string, args ...any) { panic(topErr{why: fmt.Sprintf(format, args...)}) }

// opClass says what an observation is for. It decides whether reading an
// element of the tracked list BEFORE its membership guard is allowed: only
// operations that can never raise an error and never aggregate across
// elements are (an error on a non-member would abort the whole program, so the
// cell would depend on that non-member).
type opClass uint8

const (
	opNav   opClass = iota + 1 // navigation step (totality checked by the caller)
	opIter                     // `.[]` (total only when protected by `?`)
	opEq                       // == / != (total)
	opTruth                    // truthiness: if / and / or / not / select / // (total)
	opType                     // type tests (total)
	opOther                    // anything else: may error or aggregate
)

func (o opClass) total() bool { return o == opEq || o == opTruth || o == opType }

// state is one analysis run.
type state struct {
	reads []Read
	// strict > 0 inside a program whose error MESSAGES reach the output
	// (`try … catch`, resourcesRefsTemplate fields): a message can embed any
	// value it touched, so every kind/shape observation is upgraded to whole.
	strict int
	// uaf > 0 while evaluating userAccessFilter namespaceFrom/nameFrom: a
	// per-item filter that denies on error, so its reads never abort the
	// program and are allowed before the membership guard.
	uaf int

	listPath   Path // the tracked list, relative to the tracked root
	violations []string
	guards     [][]KeyGuard // one conjunction per select (DNF)
	sites      []extrasSite // every recognised guard with an extras source
	nextInst   int
	loader     gojq.ModuleLoader
	stack      []*gojq.FuncDef
	readCount  int
}

func (s *state) violate(format string, args ...any) {
	if len(s.violations) < 32 {
		s.violations = append(s.violations, fmt.Sprintf(format, args...))
	}
}

// isElementPath reports whether p is inside an element of the tracked list.
func (s *state) isElementPath(p Path) bool {
	return len(p) > len(s.listPath) && p.hasPrefix(s.listPath) && p[len(s.listPath)].Any
}

// elemRel returns p relative to its list element.
func (s *state) elemRel(p Path) Path { return p[len(s.listPath)+1:] }

// rec records an observation of the Ref a at mode.
func (s *state) rec(a *abs, mode Mode, op opClass) {
	if a.k != aRef {
		return
	}
	if s.strict > 0 {
		mode = ModeWhole
	}
	excl := a.excl
	if mode == ModeKind {
		excl = nil
	}
	s.reads = append(s.reads, Read{Path: a.path, Mode: mode, Excl: excl})
	s.readCount++
	if s.uaf > 0 {
		return
	}
	switch {
	case s.isElementPath(a.path):
		if !a.guarded && !(op.total() || (op == opNav)) {
			s.violate("%s read of %s before its membership guard", mode, a.path)
		}
	case len(a.path) <= len(s.listPath) && s.listPath.hasPrefix(a.path):
		// The list itself (or the envelope): only iteration and kind are
		// per-element; anything else observes the whole membership.
		if !(op == opIter && len(a.path) == len(s.listPath)) && mode != ModeKind {
			s.violate("%s read of the list itself (%s)", mode, a.path)
		}
	}
}

// tainted: a value whose existence or content depends on elements that have
// not passed a membership guard.
func tainted(a *abs) bool {
	if a == nil {
		return false
	}
	if a.cu {
		return true
	}
	if a.k == aUnion {
		for _, x := range a.alts {
			if tainted(x) {
				return true
			}
		}
		return false
	}
	return a.hasUnguardedElem()
}

// nonTotal is called by every operation that may raise an error or aggregate:
// if any of its inputs is tainted, the cell is not membership-keyable.
func (s *state) nonTotal(what string, xs ...*abs) {
	if s.uaf > 0 || mutated(mutKeyNoAggregation) {
		return
	}
	for _, x := range xs {
		if tainted(x) {
			s.violate("%s over elements before their membership guard", what)
			return
		}
	}
}

func (s *state) consumeWhole(a *abs, op opClass) {
	switch a.k {
	case aRef:
		s.rec(a, ModeWhole, op)
	case aObj:
		for _, f := range a.fields {
			s.consumeWhole(f, op)
		}
		if a.rest != nil {
			s.consumeWhole(a.rest, op)
		}
	case aArr:
		if !mutated(mutKeyNoAggregation) && (a.elemCU) {
			s.violate("an array collected over unguarded elements is observed whole")
		}
		s.consumeWhole(a.elem, op)
	case aUnion:
		for _, x := range a.alts {
			s.consumeWhole(x, op)
		}
	}
}

func (s *state) consumeShape(a *abs, op opClass) {
	switch a.k {
	case aRef:
		s.rec(a, ModeShape, op)
	case aArr:
		if !mutated(mutKeyNoAggregation) && (a.elemCU || a.elem.hasUnguardedElem()) {
			s.violate("length/keys of an array collected over unguarded elements")
		}
	case aUnion:
		for _, x := range a.alts {
			s.consumeShape(x, op)
		}
	}
}

func (s *state) consumeKind(a *abs, op opClass) {
	switch a.k {
	case aRef:
		s.rec(a, ModeKind, op)
	case aUnion:
		for _, x := range a.alts {
			s.consumeKind(x, op)
		}
	}
}

// a1Object reports whether the element-relative path is an object (or absent)
// on every Kubernetes object (axiom A1: an informer list item is a JSON object
// whose metadata is an object and whose labels/annotations are string maps).
func a1Object(rel Path) bool {
	switch len(rel) {
	case 0:
		return true
	case 1:
		return !rel[0].Any && rel[0].Key == "metadata"
	case 2:
		return !rel[0].Any && rel[0].Key == "metadata" && !rel[1].Any &&
			(rel[1].Key == "labels" || rel[1].Key == "annotations")
	}
	return false
}

// navKey navigates `.key` (protected: inside a `?`).
func (s *state) navKey(a *abs, key string, protected bool) *abs {
	return mapAbs(a, func(x *abs) *abs {
		s.strictReceiver(x)
		switch x.k {
		case aRef:
			if s.isElementPath(x.path) && !x.guarded && !protected && s.uaf == 0 && !a1Object(s.elemRel(x.path)) {
				if !mutated(mutKeyNoTotality) {
					s.violate("navigation .%s on %s can raise an error before the membership guard", key, x.path)
				}
			}
			var excl []Path
			for _, e := range x.excl {
				if !e[0].Any && e[0].Key == key {
					if len(e) == 1 {
						return withCU(literal(nil, "null"), x.cu)
					}
					excl = append(excl, e[1:])
				}
			}
			r := &abs{k: aRef, path: x.path.with(Step{Key: key}), excl: excl, guarded: x.guarded, inst: x.inst, cu: x.cu}
			if !mutated(mutNavNoKind) {
				s.rec(r, ModeKind, opNav)
			}
			return r
		case aObj:
			if f, ok := x.fields[key]; ok {
				if x.rest != nil {
					return withCU(union(f, x.rest), x.cu)
				}
				return withCU(f, x.cu)
			}
			if x.rest != nil {
				if x.nav != nil && x.rest.isConst() && x.rest.constClassOf() == ccExtras {
					// x IS an extras root (the RESTAction dict, the widget
					// data source) and key is none of its known fields: the
					// value is the root's own value at key, null if absent.
					c := constOf(ccExtras, string(x.nav.Root)+"."+key)
					c.nav = navStep(x.nav, Step{Key: key})
					return withCU(c, x.cu)
				}
				return withCU(union(x.rest, literal(nil, "null")), x.cu)
			}
			return withCU(literal(nil, "null"), x.cu)
		case aArr:
			return absEmpty // `.key` on an array is an error: no value
		case aConst:
			if x.hasLit {
				if m, ok := x.lit.(map[string]any); ok {
					c := literal(m[key], x.src+"."+key)
					c.cc = x.cc
					return withCU(c, x.cu)
				}
			}
			c := constOf(x.cc, x.src+"."+key)
			c.nav = navStep(x.nav, Step{Key: key})
			return withCU(c, x.cu)
		}
		return withCU(absOpaque, x.cu)
	})
}

// navIndex navigates `.[n]` with a number (or a dynamic key): any element.
func (s *state) navIndex(a *abs, protected bool) *abs {
	return mapAbs(a, func(x *abs) *abs {
		s.strictReceiver(x)
		switch x.k {
		case aRef:
			if s.isElementPath(x.path) && !x.guarded && !protected && s.uaf == 0 {
				s.violate("positional access on %s before the membership guard", x.path)
			}
			s.rec(x, ModeShape, opOther)
			r := &abs{k: aRef, path: x.path.with(Step{Any: true}), excl: anyExcl(x.excl), guarded: x.guarded, inst: x.inst, cu: x.cu}
			if !mutated(mutNavNoKind) {
				s.rec(r, ModeKind, opNav)
			}
			return r
		case aArr:
			if !mutated(mutKeyNoAggregation) && (x.elemCU || x.elem.hasUnguardedElem()) {
				s.violate("positional access on an array collected over unguarded elements")
			}
			return withCU(union(x.elem, literal(nil, "null")), x.cu)
		case aObj:
			vals := []*abs{literal(nil, "null")}
			for _, f := range x.fields {
				vals = append(vals, f)
			}
			if x.rest != nil {
				vals = append(vals, x.rest)
			}
			return withCU(union(vals...), x.cu)
		case aConst:
			c := constOf(x.cc, x.src+"[]")
			return withCU(c, x.cu)
		}
		return withCU(absOpaque, x.cu)
	})
}

func anyExcl(excl []Path) []Path {
	var out []Path
	for _, e := range excl {
		if e[0].Any && len(e) > 1 {
			out = append(out, e[1:])
		}
	}
	return out
}

// iterate is `.[]`.
func (s *state) iterate(a *abs, protected bool) *abs {
	return mapAbs(a, func(x *abs) *abs {
		s.strictReceiver(x)
		switch x.k {
		case aRef:
			isList := x.path.String() == s.listPath.String()
			if s.isElementPath(x.path) && !x.guarded && !protected && s.uaf == 0 {
				s.violate("iteration of %s can raise an error before the membership guard", x.path)
			}
			if !mutated(mutIterNoShape) {
				s.rec(x, ModeShape, opIter)
			} else {
				s.rec(x, ModeKind, opIter)
			}
			r := &abs{k: aRef, path: x.path.with(Step{Any: true}), excl: anyExcl(x.excl), guarded: x.guarded, inst: x.inst, cu: x.cu}
			if isList {
				s.nextInst++
				r.inst = s.nextInst
				r.guarded = false
				r.cu = true // which elements exist is membership
			}
			return r
		case aArr:
			e := x.elem
			if x.elemCU {
				e = withCU(e, true)
			}
			return withCU(e, x.cu)
		case aObj:
			vals := []*abs{}
			for _, f := range x.fields {
				vals = append(vals, f)
			}
			if x.rest != nil {
				vals = append(vals, x.rest)
			}
			return withCU(union(vals...), x.cu)
		case aConst:
			c := constOf(x.cc, x.src+"[]")
			c.nav = navStep(x.nav, Step{Any: true})
			return withCU(c, x.cu)
		}
		return withCU(absOpaque, x.cu)
	})
}

// withCU returns a with its "count depends on unguarded elements" flag ORed.
func withCU(a *abs, cu bool) *abs {
	if !cu || a == nil {
		return a
	}
	if a.k == aUnion {
		out := make([]*abs, len(a.alts))
		for i, x := range a.alts {
			out[i] = withCU(x, true)
		}
		u := union(out...)
		if u.k == aUnion && len(u.alts) == 0 {
			return u
		}
		return u
	}
	if a.cu {
		return a
	}
	c := *a
	c.cu = true
	return &c
}

func anyCU(a *abs) bool {
	if a.k == aUnion {
		for _, x := range a.alts {
			if x.cu {
				return true
			}
		}
		return false
	}
	return a.cu
}

// ---------------------------------------------------------------------------
// environments

type closure struct {
	q   *gojq.Query
	env *env
}

type userFunc struct {
	def *gojq.FuncDef
	env *env // the definition environment (the def itself is visible: recursion)
}

type env struct {
	parent *env
	vars   map[string]*abs
	funcs  map[string]any // "name/arity" -> *userFunc | *closure
}

func newEnv(parent *env) *env {
	return &env{parent: parent, vars: map[string]*abs{}, funcs: map[string]any{}}
}

func (e *env) lookupVar(name string) (*abs, bool) {
	for x := e; x != nil; x = x.parent {
		if v, ok := x.vars[name]; ok {
			return v, true
		}
	}
	return nil, false
}

func (e *env) lookupFunc(name string, arity int) (any, bool) {
	k := name + "/" + strconv.Itoa(arity)
	for x := e; x != nil; x = x.parent {
		if f, ok := x.funcs[k]; ok {
			return f, true
		}
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// evaluation

// eval evaluates q on in. A constant produced while the evaluation observed
// the tracked list (a branch chosen by a read, an array whose length a read
// decided) is NOT independent of the list: it is demoted to opaque, so it can
// never qualify as a membership-key constant.
func (s *state) eval(q *gojq.Query, in *abs, e *env) *abs {
	rc := s.readCount
	v := s.evalQ(q, in, e)
	if s.readCount != rc {
		v = deconst(v)
	}
	return v
}

func deconst(a *abs) *abs {
	switch a.k {
	case aConst:
		return withCU(absOpaque, a.cu)
	case aObj:
		c := &abs{k: aObj, fields: make(map[string]*abs, len(a.fields)), cu: a.cu}
		for k, f := range a.fields {
			c.fields[k] = deconst(f)
		}
		if a.rest != nil {
			c.rest = deconst(a.rest)
		}
		return c
	case aArr:
		return &abs{k: aArr, elem: deconst(a.elem), elemCU: a.elemCU, cu: a.cu}
	case aUnion:
		out := make([]*abs, len(a.alts))
		for i, x := range a.alts {
			out[i] = deconst(x)
		}
		return union(out...)
	}
	return a
}

func (s *state) evalQ(q *gojq.Query, in *abs, e *env) *abs {
	if q == nil {
		return in
	}
	if len(q.Imports) > 0 || len(q.FuncDefs) > 0 {
		e = newEnv(e)
		for _, im := range q.Imports {
			s.importModule(im, e)
		}
		for _, fd := range q.FuncDefs {
			ne := newEnv(e)
			uf := &userFunc{def: fd, env: ne}
			ne.funcs[fd.Name+"/"+strconv.Itoa(len(fd.Args))] = uf
			e = ne
		}
		rest := *q
		rest.Imports, rest.FuncDefs, rest.Meta = nil, nil, nil
		return s.eval(&rest, in, e)
	}
	if q.Term != nil {
		return s.evalTerm(q.Term, in, e)
	}
	if len(q.Patterns) > 0 {
		if len(q.Patterns) > 1 {
			top("?// destructuring alternatives")
		}
		v := s.eval(q.Left, in, e)
		ne := newEnv(e)
		s.bindPattern(q.Patterns[0], v, in, ne)
		r := s.eval(q.Right, in, ne)
		return withCU(r, anyCU(v))
	}
	switch q.Op {
	case gojq.OpPipe:
		l := s.eval(q.Left, in, e)
		return s.eval(q.Right, l, e)
	case gojq.OpComma:
		return union(s.eval(q.Left, in, e), s.eval(q.Right, in, e))
	case gojq.OpAlt:
		l := s.eval(q.Left, in, e)
		if !mutated(mutAltNoTruth) {
			s.consumeKind(l, opTruth)
		}
		r := s.eval(q.Right, in, e)
		if c := altDefault(l, r, anyCU(in)); c != nil {
			return c
		}
		return union(l, withCU(r, anyCU(in)))
	case gojq.OpAnd, gojq.OpOr:
		l := s.eval(q.Left, in, e)
		s.consumeKind(l, opTruth)
		r := s.eval(q.Right, in, e)
		s.consumeKind(r, opTruth)
		return withCU(derived(q.Op.String(), l, r), anyCU(in) || tainted(l) || tainted(r))
	case gojq.OpEq, gojq.OpNe:
		l := s.eval(q.Left, in, e)
		r := s.eval(q.Right, in, e)
		if mutated(mutEqAsKind) {
			s.consumeKind(l, opEq)
		} else {
			s.consumeWhole(l, opEq)
		}
		if mutated(mutEqAsKind) {
			s.consumeKind(r, opEq)
		} else {
			s.consumeWhole(r, opEq)
		}
		return withCU(derived(q.Op.String(), l, r), anyCU(in) || tainted(l) || tainted(r))
	case gojq.OpAdd, gojq.OpSub, gojq.OpMul, gojq.OpDiv, gojq.OpMod,
		gojq.OpGt, gojq.OpLt, gojq.OpGe, gojq.OpLe:
		l := s.eval(q.Left, in, e)
		r := s.eval(q.Right, in, e)
		s.nonTotal(q.Op.String(), in, l, r)
		s.consumeWhole(l, opOther)
		s.consumeWhole(r, opOther)
		return withCU(derived(q.Op.String(), l, r), anyCU(in))
	case gojq.OpAssign, gojq.OpModify, gojq.OpUpdateAdd, gojq.OpUpdateSub,
		gojq.OpUpdateMul, gojq.OpUpdateDiv, gojq.OpUpdateMod, gojq.OpUpdateAlt:
		// Update-assignment: the result is the input with paths rewritten.
		// Observe the input whole; the operands only add what they read.
		s.nonTotal("assignment", in)
		s.consumeWhole(in, opOther)
		l := s.eval(q.Left, in, e)
		r := s.eval(q.Right, in, e)
		s.consumeWhole(l, opOther)
		s.consumeWhole(r, opOther)
		return withCU(derived("assign", in, l, r), anyCU(in))
	}
	top("query operator %v", q.Op)
	return nil
}

func (s *state) evalTerm(t *gojq.Term, in *abs, e *env) *abs {
	// `x?` wraps everything before it: navigation there cannot raise.
	lastOpt := -1
	for i, sf := range t.SuffixList {
		if sf.Optional {
			lastOpt = i
		}
	}
	protectedBase := lastOpt >= 0
	cur := s.evalTermBase(t, in, e, protectedBase)
	for i, sf := range t.SuffixList {
		protected := i < lastOpt
		switch {
		case sf.Index != nil:
			cur = s.evalIndex(cur, sf.Index, in, e, protected)
		case sf.Iter:
			cur = s.iterate(cur, protected)
		case sf.Optional:
			// errors suppressed; value unchanged
		default:
			top("term suffix")
		}
	}
	return cur
}

func (s *state) evalTermBase(t *gojq.Term, in *abs, e *env, protected bool) *abs {
	switch t.Type {
	case gojq.TermTypeIdentity:
		return in
	case gojq.TermTypeRecurse:
		s.nonTotal("..", in)
		s.consumeWhole(in, opOther)
		return union(in, withCU(derived("..", in), anyCU(in)))
	case gojq.TermTypeNull:
		return withCU(literal(nil, "null"), anyCU(in))
	case gojq.TermTypeTrue:
		return withCU(literal(true, "true"), anyCU(in))
	case gojq.TermTypeFalse:
		return withCU(literal(false, "false"), anyCU(in))
	case gojq.TermTypeNumber:
		return withCU(literal(numberLit(t.Number), t.Number), anyCU(in))
	case gojq.TermTypeString:
		return s.evalString(t.Str, "", in, e)
	case gojq.TermTypeFormat:
		if t.Str == nil {
			s.nonTotal("format "+t.Format, in)
			s.consumeWhole(in, opOther)
			return withCU(derived(t.Format, in), anyCU(in))
		}
		return s.evalString(t.Str, t.Format, in, e)
	case gojq.TermTypeIndex:
		return s.evalIndex(in, t.Index, in, e, protected)
	case gojq.TermTypeFunc:
		return s.evalFunc(t.Func, in, e, protected)
	case gojq.TermTypeObject:
		return s.evalObject(t.Object, in, e)
	case gojq.TermTypeArray:
		if t.Array.Query == nil {
			return withCU(literal([]any{}, "[]"), anyCU(in))
		}
		el := s.eval(t.Array.Query, in, e)
		a := &abs{k: aArr, elem: stripCU(el), elemCU: anyCU(el) && !anyCU(in)}
		if anyCU(el) && anyCU(in) {
			// the element count depends on the outer element too: still per outer element
			a.elemCU = elemCountTainted(el, in)
		}
		return withCU(a, anyCU(in))
	case gojq.TermTypeUnary:
		v := s.evalTerm(t.Unary.Term, in, e)
		s.nonTotal("unary", in, v)
		s.consumeWhole(v, opOther)
		return withCU(derived("unary", v), anyCU(in))
	case gojq.TermTypeIf:
		return s.evalIf(t.If, in, e)
	case gojq.TermTypeTry:
		if t.Try.Catch != nil {
			inc := 1
			if mutated(mutCatchNotStrict) {
				inc = 0
			}
			s.strict += inc
			b := s.eval(t.Try.Body, in, e)
			s.strict -= inc
			s.consumeWhole(b, opOther)
			h := s.eval(t.Try.Catch, withCU(derived("error", in), anyCU(in)), e)
			return union(b, h)
		}
		return s.eval(t.Try.Body, in, e)
	case gojq.TermTypeReduce:
		r := t.Reduce
		src := s.eval(r.Query, in, e)
		s.nonTotal("reduce", in, src)
		s.consumeWhole(src, opOther)
		ne := newEnv(e)
		s.bindPattern(r.Pattern, derived("reduce-item", src), in, ne)
		start := s.eval(r.Start, in, e)
		s.consumeWhole(start, opOther)
		acc := derived("reduce-acc", start, src)
		upd := s.eval(r.Update, acc, ne)
		s.consumeWhole(upd, opOther)
		return withCU(derived("reduce", src, start, upd), anyCU(in))
	case gojq.TermTypeForeach:
		f := t.Foreach
		src := s.eval(f.Query, in, e)
		s.nonTotal("foreach", in, src)
		s.consumeWhole(src, opOther)
		ne := newEnv(e)
		s.bindPattern(f.Pattern, derived("foreach-item", src), in, ne)
		start := s.eval(f.Start, in, e)
		s.consumeWhole(start, opOther)
		acc := derived("foreach-acc", start, src)
		upd := s.eval(f.Update, acc, ne)
		s.consumeWhole(upd, opOther)
		out := upd
		if f.Extract != nil {
			out = s.eval(f.Extract, derived("foreach-state", upd), ne)
			s.consumeWhole(out, opOther)
		}
		return withCU(derived("foreach", src, start, upd, out), anyCU(in))
	case gojq.TermTypeLabel:
		top("label/break")
	case gojq.TermTypeBreak:
		top("label/break")
	case gojq.TermTypeQuery:
		return s.eval(t.Query, in, e)
	}
	top("term type %v", t.Type)
	return nil
}

// strictReceiver: under strict analysis, the value a navigation is applied to
// is observed whole — a type error's message previews the receiver's content
// (gojq typeErrorPreview), and in a strict program that message is output.
func (s *state) strictReceiver(x *abs) {
	if s.strict > 0 && !mutated(mutStrictNoReceiver) {
		s.consumeWhole(x, opOther)
	}
}

// numberLit is a number literal's value: a number, never a string (a string
// literal index is a key lookup; a number is a position).
func numberLit(s string) any {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s + "#number"
}

// elemCountTainted: inside a per-outer-element context, an array collected
// from a stream is per-element unless the stream itself iterated the tracked
// list again. Conservative: any unguarded element in the collected values.
func elemCountTainted(el, in *abs) bool { return el.hasUnguardedElem() && !in.hasUnguardedElem() }

func stripCU(a *abs) *abs {
	if a == nil {
		return nil
	}
	if a.k == aUnion {
		out := make([]*abs, len(a.alts))
		for i, x := range a.alts {
			out[i] = stripCU(x)
		}
		return union(out...)
	}
	if !a.cu {
		return a
	}
	c := *a
	c.cu = false
	return &c
}

func (s *state) evalString(str *gojq.String, format string, in *abs, e *env) *abs {
	if len(str.Queries) == 0 {
		return withCU(literal(str.Str, strconv.Quote(str.Str)), anyCU(in))
	}
	vals := []*abs{}
	for _, q := range str.Queries {
		v := s.eval(q, in, e)
		// String interpolation stringifies the value: total (tojson never
		// fails) — but it observes the whole value.
		if format != "" {
			s.nonTotal("format "+format, in, v)
		}
		s.consumeWhole(v, opEq)
		vals = append(vals, v)
	}
	return withCU(derived("string", vals...), anyCU(in) || anyTainted(vals))
}

func anyTainted(xs []*abs) bool {
	for _, x := range xs {
		if anyCU(x) {
			return true
		}
	}
	return false
}

// indexKey resolves a literal index key, if the index is one.
func (s *state) evalIndex(cur *abs, idx *gojq.Index, termIn *abs, e *env, protected bool) *abs {
	if idx.Name != "" {
		return s.navKey(cur, idx.Name, protected)
	}
	if idx.Str != nil {
		if len(idx.Str.Queries) == 0 {
			return s.navKey(cur, idx.Str.Str, protected)
		}
		k := s.evalString(idx.Str, "", termIn, e)
		return s.dynIndex(cur, k, protected)
	}
	if idx.IsSlice {
		var bounds []*abs
		if idx.Start != nil {
			bounds = append(bounds, s.eval(idx.Start, termIn, e))
		}
		if idx.End != nil {
			bounds = append(bounds, s.eval(idx.End, termIn, e))
		}
		for _, b := range bounds {
			s.consumeWhole(b, opOther)
		}
		return mapAbs(cur, func(x *abs) *abs {
			switch x.k {
			case aArr:
				if !mutated(mutKeyNoAggregation) && (x.elemCU || x.elem.hasUnguardedElem()) {
					s.violate("slice of an array collected over unguarded elements")
				}
				return withCU(&abs{k: aArr, elem: x.elem}, x.cu)
			case aRef:
				if !protected {
					s.nonTotal("slice", x)
				}
				s.consumeWhole(x, opOther)
				return withCU(absOpaque, x.cu)
			case aConst:
				return withCU(derived("slice", append(bounds, x)...), x.cu)
			}
			return withCU(absOpaque, x.cu)
		})
	}
	k := s.eval(idx.Start, termIn, e)
	if k.k == aConst && k.hasLit {
		if str, ok := k.lit.(string); ok && k.cc == ccLiteral {
			return s.navKey(cur, str, protected)
		}
	}
	return s.dynIndex(cur, k, protected)
}

// dynIndex is `.[k]` with a computed key or a number.
func (s *state) dynIndex(cur, k *abs, protected bool) *abs {
	s.consumeWhole(k, opEq)
	if tainted(k) {
		s.violate("index computed from unguarded elements")
	}
	if k.k == aConst && k.hasLit {
		if str, ok := k.lit.(string); ok {
			return s.navKey(cur, str, protected)
		}
	}
	return s.navIndex(cur, protected)
}

func (s *state) evalIf(f *gojq.If, in *abs, e *env) *abs {
	c := s.eval(f.Cond, in, e)
	if !mutated(mutIfNoTruth) {
		s.consumeKind(c, opTruth)
	}
	cu := anyCU(in) || tainted(c)
	branches := []*abs{s.eval(f.Then, in, e)}
	for _, el := range f.Elif {
		ec := s.eval(el.Cond, in, e)
		if !mutated(mutIfNoTruth) {
			s.consumeKind(ec, opTruth)
		}
		cu = cu || tainted(ec)
		branches = append(branches, s.eval(el.Then, in, e))
	}
	if f.Else != nil {
		branches = append(branches, s.eval(f.Else, in, e))
	} else {
		branches = append(branches, in)
	}
	return withCU(union(branches...), cu)
}

func (s *state) evalObject(o *gojq.Object, in *abs, e *env) *abs {
	obj := &abs{k: aObj, fields: map[string]*abs{}}
	cu := anyCU(in)
	for _, kv := range o.KeyVals {
		var name string
		static := true
		var val *abs
		switch {
		case kv.Key != "":
			if strings.HasPrefix(kv.Key, "$") {
				name = kv.Key[1:]
				if kv.Val == nil {
					val = s.lookupVar(kv.Key, in, e)
				}
			} else {
				name = kv.Key
				if kv.Val == nil {
					val = s.navKey(in, kv.Key, false)
				}
			}
		case kv.KeyString != nil:
			if len(kv.KeyString.Queries) == 0 {
				name = kv.KeyString.Str
				if kv.Val == nil {
					val = s.navKey(in, name, false)
				}
			} else {
				k := s.evalString(kv.KeyString, "", in, e)
				s.consumeWhole(k, opEq)
				cu = cu || tainted(k)
				static = false
				if kv.Val == nil {
					val = s.dynIndex(in, k, false)
				}
			}
		case kv.KeyQuery != nil:
			k := s.eval(kv.KeyQuery, in, e)
			if k.k == aConst && k.hasLit {
				if str, ok := k.lit.(string); ok {
					name = str
					break
				}
			}
			s.nonTotal("computed object key", in, k)
			s.consumeWhole(k, opOther)
			cu = cu || tainted(k)
			static = false
		default:
			top("object key form")
		}
		if kv.Val != nil {
			val = s.eval(kv.Val, in, e)
		}
		if val == nil {
			top("object value")
		}
		cu = cu || anyCU(val)
		val = stripCU(val)
		if static {
			obj.fields[name] = val
		} else if obj.rest == nil {
			obj.rest = val
		} else {
			obj.rest = union(obj.rest, val)
		}
	}
	return withCU(obj, cu)
}

// lookupVar resolves $name in gojq's order (compiler.go compileFunc): a
// user binding (`as $x`, a `$x` function parameter, a destructured key)
// first, then the built-in. A user may rebind a built-in name, and the
// binding wins. The krateo-platformops/gojq fork has no $__loc__, so an
// unbound $__loc__ is a compile error, and it gets no special case here.
func (s *state) lookupVar(name string, in *abs, e *env) *abs {
	if name == "$ENV" && mutated(mutBuiltinVarsFirst) {
		return withCU(literal(map[string]any{}, "$ENV"), anyCU(in))
	}
	if v, ok := e.lookupVar(name); ok {
		return withCU(v, anyCU(in))
	}
	if name == "$ENV" {
		// No environ loader is installed anywhere in snowplow
		// (jqvalue.go / plumbing jqutil): $ENV is the constant {}.
		return withCU(literal(map[string]any{}, "$ENV"), anyCU(in))
	}
	top("unbound variable %s", name)
	return nil
}

func (s *state) bindPattern(p *gojq.Pattern, v, in *abs, ne *env) {
	switch {
	case p.Name != "":
		ne.vars[p.Name] = stripCU(v)
	case len(p.Array) > 0:
		for _, sub := range p.Array {
			s.bindPattern(sub, s.navIndex(v, false), in, ne)
		}
	case len(p.Object) > 0:
		for _, po := range p.Object {
			var val *abs
			switch {
			case po.Key != "":
				key := strings.TrimPrefix(po.Key, "$")
				val = s.navKey(v, key, false)
				if strings.HasPrefix(po.Key, "$") {
					ne.vars[po.Key] = stripCU(val)
				}
			case po.KeyString != nil && len(po.KeyString.Queries) == 0:
				val = s.navKey(v, po.KeyString.Str, false)
			case po.KeyString != nil:
				k := s.evalString(po.KeyString, "", in, ne)
				val = s.dynIndex(v, k, false)
			case po.KeyQuery != nil:
				k := s.eval(po.KeyQuery, in, ne)
				val = s.dynIndex(v, k, false)
			default:
				top("object pattern")
			}
			if po.Val != nil {
				s.bindPattern(po.Val, val, in, ne)
			}
		}
	default:
		top("pattern")
	}
}

// ---------------------------------------------------------------------------
// function calls

func (s *state) evalFunc(f *gojq.Func, in *abs, e *env, protected bool) *abs {
	if strings.HasPrefix(f.Name, "$") {
		if f.Name == "$__prog_args" {
			top("$__prog_args")
		}
		return s.lookupVar(f.Name, in, e)
	}
	if fn, ok := e.lookupFunc(f.Name, len(f.Args)); ok {
		switch x := fn.(type) {
		case *closure:
			return s.eval(x.q, in, x.env)
		case *userFunc:
			return s.callUser(x, f.Args, in, e)
		}
	}
	if strings.Contains(f.Name, "::") {
		top("unresolved module function %s/%d", f.Name, len(f.Args))
	}
	// A builtin's result is never the navigated extras value itself: first,
	// limit and select keep only some of it, del changes it (#633).
	return stripNav(s.builtin(f, in, e, protected))
}

func (s *state) callUser(uf *userFunc, args []*gojq.Query, in *abs, caller *env) *abs {
	for _, d := range s.stack {
		if d == uf.def {
			// A recursive call. The body is already being analysed one level
			// up (its reads of enclosing variables are recorded there). The
			// recursive call is the same body on a different input and
			// arguments: observe both whole, and the result is opaque.
			s.nonTotal("recursive call", in)
			s.consumeWhole(in, opOther)
			vals := []*abs{in}
			for _, a := range args {
				v := s.eval(a, in, caller)
				s.nonTotal("recursive call", v)
				s.consumeWhole(v, opOther)
				vals = append(vals, v)
			}
			return withCU(derived("recursion", vals...), anyCU(in))
		}
	}
	if len(s.stack) > 64 {
		top("call depth")
	}
	s.stack = append(s.stack, uf.def)
	defer func() { s.stack = s.stack[:len(s.stack)-1] }()
	ne := newEnv(uf.env)
	cu := false
	for i, name := range uf.def.Args {
		if strings.HasPrefix(name, "$") {
			v := s.eval(args[i], in, caller)
			cu = cu || anyCU(v)
			ne.vars[name] = stripCU(v)
			ne.funcs[name[1:]+"/0"] = &closure{q: args[i], env: caller}
		} else {
			ne.funcs[name+"/0"] = &closure{q: args[i], env: caller}
		}
	}
	return withCU(s.eval(uf.def.Body, in, ne), cu)
}

func (s *state) importModule(im *gojq.Import, e *env) {
	path := im.ImportPath
	prefix := ""
	switch {
	case im.ImportPath != "" && strings.HasPrefix(im.ImportAlias, "$"):
		top("data import %s", im.ImportPath)
	case im.ImportPath != "":
		prefix = im.ImportAlias + "::"
	case im.IncludePath != "":
		path = im.IncludePath
	default:
		top("import form")
	}
	if s.loader == nil {
		top("import %q without a module loader", path)
	}
	ml, ok := s.loader.(interface {
		LoadModule(string) (*gojq.Query, error)
	})
	if !ok {
		top("module loader cannot load %q", path)
	}
	mq, err := ml.LoadModule(path)
	if err != nil {
		top("module %q: %v", path, err)
	}
	me := newEnv(nil)
	for _, sub := range mq.Imports {
		s.importModule(sub, me)
	}
	cur := me
	for _, fd := range mq.FuncDefs {
		ne := newEnv(cur)
		ne.funcs[fd.Name+"/"+strconv.Itoa(len(fd.Args))] = &userFunc{def: fd, env: ne}
		cur = ne
		e.funcs[prefix+fd.Name+"/"+strconv.Itoa(len(fd.Args))] = &userFunc{def: fd, env: ne}
	}
}
