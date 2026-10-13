package jqinterest

import (
	"strconv"

	"github.com/itchyny/gojq"
)

// genericBuiltins are gojq builtins that are DETERMINISTIC functions of their
// input and arguments. The generic rule observes the input whole and every
// argument's values whole, and yields an opaque value: sound for any such
// function, whatever it does. Functions with a more precise rule are handled
// in builtin() before this table is consulted. Anything not listed (and not
// user-defined) is unprovable.
var genericBuiltins = map[string]bool{
	"abs": true, "utf8bytelength": true, "add": true, "toboolean": true, "tonumber": true,
	"tostring": true, "reverse": true, "contains": true, "inside": true, "indices": true,
	"index": true, "rindex": true, "startswith": true, "endswith": true, "ltrimstr": true,
	"rtrimstr": true, "trimstr": true, "ltrim": true, "rtrim": true, "trim": true,
	"explode": true, "implode": true, "split": true, "join": true, "ascii_downcase": true,
	"ascii_upcase": true, "tojson": true, "fromjson": true, "format": true, "flatten": true,
	"min": true, "max": true, "sort": true, "unique": true, "floor": true, "round": true,
	"nearbyint": true, "rint": true, "ceil": true, "trunc": true, "significand": true,
	"fabs": true, "sqrt": true, "cbrt": true, "exp": true, "exp10": true, "exp2": true,
	"expm1": true, "frexp": true, "modf": true, "log": true, "log10": true, "log1p": true,
	"log2": true, "logb": true, "gamma": true, "tgamma": true, "lgamma": true, "pow": true,
	"atan2": true, "fmin": true, "fmax": true, "fmod": true, "ldexp": true, "infinite": true,
	"isfinite": true, "isinfinite": true, "nan": true, "isnan": true, "isnormal": true,
	"setpath": true, "delpaths": true, "getpath": true, "transpose": true, "bsearch": true,
	"gmtime": true, "localtime": true, "mktime": true, "strftime": true, "strflocaltime": true,
	"strptime": true, "todate": true, "fromdate": true, "todateiso8601": true,
	"fromdateiso8601": true, "dateadd": true, "datesub": true, "date": true,
	"test": true, "match": true, "capture": true, "scan": true, "splits": true, "sub": true,
	"gsub": true, "ascii": true, "to_entries": true, "from_entries": true,
	"with_entries": true, "map_values": true, "paths": true, "leaf_paths": true,
	"path": true, "pick": true, "recurse": true, "tostream": true, "fromstream": true,
	"truncate_stream": true, "combinations": true, "walk": true, "range": true,
	"until": true, "while": true, "repeat": true, "in": true, "IN": true, "INDEX": true,
	"JOIN": true, "toarray": true, "have_literal_numbers": true, "have_decnum": true,
	"getpath/1": true, "splits/2": true, "ascii/0": true, "@text": true, "tojson/0": true,
	"input_filename": true, "builtins": true, "min_by_raw": true, "sin": true, "cos": true,
	"tan": true, "asin": true, "acos": true, "atan": true, "sinh": true, "cosh": true,
	"tanh": true, "asinh": true, "acosh": true, "atanh": true, "erf": true, "erfc": true,
	"j0": true, "j1": true, "y0": true, "y1": true, "copysign": true, "drem": true,
	"fdim": true, "hypot": true, "jn": true, "nextafter": true, "nexttoward": true,
	"remainder": true, "scalb": true, "scalbln": true, "yn": true, "fma": true,
	"finites": true, "normals": true,
}

// unprovable are builtins whose output is not a function of the program's
// input (other inputs, the clock, process control).
var unprovable = map[string]bool{
	"input": true, "inputs": true, "now": true, "halt": true, "halt_error": true,
	"input_line_number": true, "modulemeta": true, "get_search_list": true,
	"get_prog_origin": true, "get_jq_origin": true, "$__prog_args": true,
}

func (s *state) builtin(f *gojq.Func, in *abs, e *env, protected bool) *abs {
	name, n := f.Name, len(f.Args)
	arg := func(i int) *abs { return s.eval(f.Args[i], in, e) }
	cu := anyCU(in)
	switch {
	case unprovable[name]:
		top("builtin %s/%d is not a function of the input", name, n)
	case name == "empty" && n == 0:
		return absEmpty
	case name == "not" && n == 0:
		s.consumeKind(in, opTruth)
		return withCU(derived("not", in), cu)
	case name == "type" && n == 0:
		s.consumeKind(in, opType)
		return withCU(derived("type", in), cu)
	case (name == "length" || name == "keys" || name == "keys_unsorted") && n == 0:
		s.nonTotal(name, in)
		if mutated(mutLengthAsKind) {
			s.consumeKind(in, opOther)
		} else {
			s.consumeShape(in, opOther)
		}
		if name == "length" {
			return withCU(derived(name, in), cu)
		}
		return withCU(&abs{k: aArr, elem: derived(name, in)}, cu)
	case name == "has" && n == 1:
		k := arg(0)
		s.nonTotal("has", in, k)
		if mutated(mutHasAsKind) {
			s.consumeKind(in, opOther)
		} else {
			s.consumeShape(in, opOther)
		}
		s.consumeWhole(k, opOther)
		return withCU(derived("has", in, k), cu)
	case name == "select" && n == 1:
		return s.selectFn(f.Args[0], in, e)
	case name == "map" && n == 1:
		el := s.iterate(in, protected)
		out := s.eval(f.Args[0], el, e)
		return withCU(&abs{k: aArr, elem: stripCU(out), elemCU: tainted(out) && !in.hasUnguardedElem()}, cu)
	case (name == "first" || name == "last") && n == 0:
		return s.navIndex(in, protected)
	case name == "nth" && n == 1:
		k := arg(0)
		s.consumeWhole(k, opOther)
		return s.navIndex(in, protected)
	case (name == "first" || name == "last") && n == 1:
		out := s.eval(f.Args[0], in, e)
		if tainted(out) && !mutated(mutKeyNoAggregation) {
			s.violate("%s over a stream of unguarded elements", name)
		}
		return out
	case (name == "limit" || name == "skip" || name == "nth") && n == 2:
		k := arg(0)
		s.nonTotal(name, in, k)
		s.consumeWhole(k, opOther)
		out := s.eval(f.Args[1], in, e)
		if tainted(out) && !mutated(mutKeyNoAggregation) {
			s.violate("%s over a stream of unguarded elements", name)
		}
		return out
	case name == "isempty" && n == 1:
		g := s.eval(f.Args[0], in, e)
		return withCU(derived("isempty", g), cu || tainted(g))
	case (name == "any" || name == "all") && n == 0:
		el := s.iterate(in, protected)
		s.consumeKind(el, opTruth)
		s.nonTotal(name, in)
		return withCU(derived(name, el), cu)
	case (name == "any" || name == "all") && n == 1:
		el := s.iterate(in, protected)
		c := s.eval(f.Args[0], el, e)
		s.consumeKind(c, opTruth)
		s.nonTotal(name, in, c)
		return withCU(derived(name, c), cu)
	case (name == "any" || name == "all") && n == 2:
		g := s.eval(f.Args[0], in, e)
		c := s.eval(f.Args[1], g, e)
		s.consumeKind(c, opTruth)
		s.nonTotal(name, in, g, c)
		return withCU(derived(name, c), cu)
	case (name == "sort_by" || name == "group_by" || name == "unique_by" ||
		name == "min_by" || name == "max_by") && n == 1:
		s.nonTotal(name, in)
		el := s.iterate(in, protected)
		k := s.eval(f.Args[0], el, e)
		if !mutated(mutSortByAsNav) {
			s.consumeWhole(k, opOther)
		}
		el = stripCU(el)
		switch name {
		case "group_by":
			return withCU(&abs{k: aArr, elem: &abs{k: aArr, elem: el}}, cu)
		case "min_by", "max_by":
			return withCU(union(el, literal(nil, "null")), cu)
		}
		return withCU(&abs{k: aArr, elem: el}, cu)
	case (name == "values" || name == "nulls" || name == "booleans" || name == "numbers" ||
		name == "strings" || name == "arrays" || name == "objects" || name == "iterables" ||
		name == "scalars") && n == 0:
		s.consumeKind(in, opType)
		return withCU(in, cu || in.hasUnguardedElem())
	case name == "error" && n == 0:
		s.nonTotal("error", in)
		s.consumeWhole(in, opOther)
		return absEmpty
	case name == "error" && n == 1:
		m := arg(0)
		s.nonTotal("error", in, m)
		s.consumeWhole(m, opOther)
		return absEmpty
	case (name == "debug" || name == "stderr") && n == 0:
		return in
	case name == "debug" && n == 1:
		s.eval(f.Args[0], in, e)
		return in
	case (name == "env") && n == 0:
		return withCU(literal(map[string]any{}, "env"), cu)
	case name == "del" && n == 1:
		if paths, ok := simplePaths(f.Args[0]); ok {
			return s.del(in, paths)
		}
	case name == "getpath" && n == 1, name == "splits" && n == 1:
		// fall through to generic
	}
	if genericBuiltins[name] || genericBuiltins[name+"/"+strconv.Itoa(n)] || name == "del" {
		s.nonTotal(name, in)
		if !mutated(mutGenericNoInput) {
			s.consumeWhole(in, opOther)
		}
		vals := []*abs{in}
		for i := range f.Args {
			v := arg(i)
			s.nonTotal(name, v)
			s.consumeWhole(v, opOther)
			vals = append(vals, v)
		}
		return withCU(derived(name, vals...), cu)
	}
	top("unknown function %s/%d", name, n)
	return nil
}

// selectFn is select(p). When p's leading `and` conjuncts compare a field of
// the input element with a value independent of the tracked list, the
// element passes only if it is one of the cell's own objects: the result is a
// GUARDED element, and the comparison is recorded as a membership key.
func (s *state) selectFn(p *gojq.Query, in *abs, e *env) *abs {
	insts := map[int]bool{}
	in.instances(insts)
	if len(insts) == 1 && in.hasUnguardedElem() {
		var inst int
		for k := range insts {
			inst = k
		}
		conj := splitAnd(p)
		var keys []KeyGuard
		i := 0
		for ; i < len(conj); i++ {
			kg, ok := s.keyConjunct(conj[i], in, inst, e)
			if !ok {
				break
			}
			keys = append(keys, kg)
		}
		if len(keys) > 0 {
			g := in.guardCopy(inst)
			g = stripCU(g)
			for ; i < len(conj); i++ {
				c := s.eval(conj[i], g, e)
				s.consumeKind(c, opTruth)
			}
			s.guards = append(s.guards, keys)
			return g
		}
	}
	c := s.eval(p, in, e)
	if !mutated(mutSelectNoTruth) {
		s.consumeKind(c, opTruth)
	}
	return withCU(in, anyCU(in) || tainted(c) || in.hasUnguardedElem())
}

// keyConjunct recognises `L == R` where one side is a field of element inst
// reached by total navigation and the other is a constant computed WITHOUT
// observing the tracked list.
func (s *state) keyConjunct(q *gojq.Query, in *abs, inst int, e *env) (KeyGuard, bool) {
	q = unparen(q)
	if q.Term != nil || q.Op != gojq.OpEq || len(q.Patterns) > 0 {
		return KeyGuard{}, false
	}
	// Evaluate speculatively: a conjunct that turns out not to be a key is
	// rolled back, and the caller evaluates it again on the GUARDED element
	// (the `and` short-circuit means only the cell's own objects reach it).
	nReads, nViol, rc0 := len(s.reads), len(s.violations), s.readCount
	rollback := func() (KeyGuard, bool) {
		s.reads, s.violations, s.readCount = s.reads[:nReads], s.violations[:nViol], rc0
		return KeyGuard{}, false
	}
	rcL := s.readCount
	l := s.eval(q.Left, in, e)
	lPure := s.readCount == rcL
	rcR := s.readCount
	r := s.eval(q.Right, in, e)
	rPure := s.readCount == rcR
	isKeyRef := func(a *abs) bool { return a.k == aRef && a.inst == inst && !a.guarded && s.isElementPath(a.path) }
	var ref, cst *abs
	switch {
	case isKeyRef(l) && rPure && r.isConst():
		ref, cst = l, r
	case isKeyRef(r) && lPure && l.isConst():
		ref, cst = r, l
	default:
		return rollback()
	}
	if len(s.violations) != nViol {
		// the key navigation itself could raise an error on a non-member
		return rollback()
	}
	if !mutated(mutKeyNotRead) {
		s.consumeWhole(ref, opEq)
	}
	return KeyGuard{
		Path:        s.elemRel(ref.path),
		ConstClass:  cst.constClassOf().String(),
		ConstSource: constSource(cst),
		Values:      literalValues(cst),
	}, true
}

// literalValues returns every value a constant can take when each of its
// alternatives is a literal (a stream such as ("a", "b") is a union), and nil
// otherwise.
func literalValues(a *abs) []any {
	var out []any
	var walk func(x *abs) bool
	walk = func(x *abs) bool {
		switch x.k {
		case aConst:
			if !x.hasLit || x.cc != ccLiteral {
				return false
			}
			out = append(out, x.lit)
			return true
		case aUnion:
			for _, y := range x.alts {
				if !walk(y) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !walk(a) || len(out) == 0 {
		return nil
	}
	return out
}

func constSource(a *abs) string {
	if a.k == aConst {
		return a.src
	}
	return a.sig()
}

func unparen(q *gojq.Query) *gojq.Query {
	for q != nil && q.Term != nil && q.Term.Type == gojq.TermTypeQuery && len(q.Term.SuffixList) == 0 {
		q = q.Term.Query
	}
	return q
}

// splitAnd flattens a left-nested `and` chain into its conjuncts, in
// evaluation order.
func splitAnd(q *gojq.Query) []*gojq.Query {
	q = unparen(q)
	if q.Term == nil && q.Op == gojq.OpAnd && len(q.Patterns) == 0 {
		return append(splitAnd(q.Left), splitAnd(q.Right)...)
	}
	return []*gojq.Query{q}
}

// simplePaths extracts literal key paths from a del() argument such as
// `.a.b, .c["d"]`. Anything else (numbers, slices, computed keys, iteration)
// is not simple.
func simplePaths(q *gojq.Query) ([]Path, bool) {
	q = unparen(q)
	if q.Term == nil {
		if q.Op == gojq.OpComma && len(q.Patterns) == 0 {
			l, ok1 := simplePaths(q.Left)
			r, ok2 := simplePaths(q.Right)
			return append(l, r...), ok1 && ok2
		}
		return nil, false
	}
	t := q.Term
	var p Path
	key := func(idx *gojq.Index) (string, bool) {
		switch {
		case idx.Name != "":
			return idx.Name, true
		case idx.Str != nil && len(idx.Str.Queries) == 0:
			return idx.Str.Str, true
		case !idx.IsSlice && idx.Start != nil:
			st := unparen(idx.Start)
			if st.Term != nil && st.Term.Type == gojq.TermTypeString && st.Term.Str != nil && len(st.Term.Str.Queries) == 0 && len(st.Term.SuffixList) == 0 {
				return st.Term.Str.Str, true
			}
		}
		return "", false
	}
	switch t.Type {
	case gojq.TermTypeIndex:
		k, ok := key(t.Index)
		if !ok {
			return nil, false
		}
		p = append(p, Step{Key: k})
	default:
		return nil, false
	}
	for _, sf := range t.SuffixList {
		if sf.Index == nil {
			return nil, false
		}
		k, ok := key(sf.Index)
		if !ok {
			return nil, false
		}
		p = append(p, Step{Key: k})
	}
	return []Path{p}, true
}

// del removes literal key paths. On a Ref the removed subtrees become
// exclusions: a later whole read does not observe them.
func (s *state) del(in *abs, paths []Path) *abs {
	return mapAbs(in, func(x *abs) *abs {
		switch x.k {
		case aRef:
			for _, p := range paths {
				// del raises an error if a parent on the path is neither an
				// object nor null; before the guard it must be total (A1).
				if s.isElementPath(x.path) && !x.guarded && s.uaf == 0 {
					xrel := s.elemRel(x.path)
					full := append(append(Path{}, xrel...), p[:len(p)-1]...)
					for j := len(xrel); j <= len(full); j++ {
						if !a1Object(full[:j]) {
							s.violate("del(%s) on %s can raise an error before the membership guard", p, x.path)
							break
						}
					}
				}
				// the parents' kinds decide whether del errors
				s.rec(x, ModeKind, opNav)
				cur := x
				for _, st := range p[:len(p)-1] {
					cur = &abs{k: aRef, path: cur.path.with(st), inst: x.inst, guarded: x.guarded}
					s.rec(cur, ModeKind, opNav)
				}
			}
			c := *x
			excl := paths
			if mutated(mutDelExclParent) {
				excl = nil
				for _, p := range paths {
					if len(p) > 1 {
						p = p[:len(p)-1]
					}
					excl = append(excl, p)
				}
			}
			c.excl = append(append([]Path{}, x.excl...), excl...)
			return &c
		case aObj:
			c := &abs{k: aObj, fields: map[string]*abs{}, rest: x.rest, cu: x.cu}
			for k, v := range x.fields {
				c.fields[k] = v
			}
			for _, p := range paths {
				if len(p) == 1 {
					delete(c.fields, p[0].Key)
				} else if v, ok := c.fields[p[0].Key]; ok {
					c.fields[p[0].Key] = s.del(v, []Path{p[1:]})
				}
			}
			if x.rest != nil {
				c.rest = s.del(x.rest, paths)
			}
			return c
		case aConst:
			return x
		case aArr:
			return absEmpty // del(.key) on an array is an error
		}
		return x
	})
}
