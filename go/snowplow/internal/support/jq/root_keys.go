package jq

// root_keys.go — #1477: which top-level keys of its input a jq query reads.
//
// A RESTAction template is evaluated against the resolve dict: the request
// extras, plus what the resolver writes itself (each stage's output under its
// name, errorKeys, the pagination block). To tell whether a template can be
// rendered by a resolve that carries a given set of extras, the caller needs
// the dict keys the template reads. RootKeys returns them, or ok=false when
// the query reads its input in a way this analysis does not follow.
//
// SOUNDNESS. ok=true promises: every top-level key of the input that can
// influence the query's output is in keys. The analysis tracks, for every
// sub-expression, whether its input is the query's own input (the root). It
// returns ok=false whenever the root could flow somewhere it cannot follow:
//   - the root as a whole value (`.`, `..`, `. as $x`, `(.)`, a bare
//     `@format`), outside the left side of a pipe whose left side is exactly
//     `.`;
//   - an index into the root that is not a constant string (`.[$k]`,
//     `.[.a]`, `.[]`, a slice);
//   - a function call, a function definition, an import or an assignment
//     operator whose input is the root: a builtin or a module function can
//     read any key of its input (`keys`, `to_entries`, `getpath`, `tojson`),
//     and an assignment outputs the whole root;
//   - a destructuring pattern with a computed key.
// Under these rules no sub-expression whose analysis is ok can output the
// root itself, so the right side of a pipe never sees the root unless its
// left side is `.`. ok=false is the caller's "cannot tell": it must take the
// conservative answer for its question.
//
// A READ IS A READ, ALTERNATIVES INCLUDED. A key read on the left of `//` (`.k
// // d`) is reported like any other. That a template supplies a default for a
// missing key shows only that its author handled the absence in jq, not that
// the default renders anything a request would use: the live portal's spoke
// endpointRef is `((.name // "") | gsub(...)) + "-endpoint"`, whose default
// renders the Secret name "endpoint", the miss #1477 removes. So the caller
// (restactionsapi.NeedsRequestExtras) treats a key read under an alternative
// as needed. The consequence: a hub-default template (`(.name // "hub") +
// "-endpoint"`) is not prewarmed with no extras either; its no-extras cell is
// filled by the first request that renders it.

import (
	"sort"
	"strings"

	"github.com/itchyny/gojq"
)

// RootKeys parses query (the jq inside a `${ ... }` template, as
// jqutil.MaybeQuery returns it) and returns the top-level input keys it reads,
// sorted, or ok=false when it does not parse or reads its input in a way the
// analysis does not follow (see the file comment).
func RootKeys(query string) (keys []string, ok bool) {
	keys, _, _, ok = RootKeysUnder(query, "")
	return keys, ok
}

// RootKeysUnder is RootKeys plus, for one top-level key `under`, the keys read
// directly beneath it (`.under.k`, `.under["k"]`), sorted. subOK is false when
// the query reads `under` other than through such a constant index (as a whole
// value, through a pipe, an iteration or a computed index): then any key
// beneath it may be read. With under == "" (or a query that never reads it) sub
// is empty and subOK true. #1477: a stage filter's input holds the request
// extras under `extras` (handler.go jsonHandlerCore).
func RootKeysUnder(query, under string) (keys, sub []string, subOK, ok bool) {
	q, err := gojq.Parse(query)
	if err != nil {
		return nil, nil, false, false
	}
	w := rootKeyWalker{keys: map[string]struct{}{}, under: under, sub: map[string]struct{}{}}
	w.query(q, true)
	if w.unknown {
		return nil, nil, false, false
	}
	return sortedKeys(w.keys), sortedKeys(w.sub), !w.subUnknown, true
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type rootKeyWalker struct {
	keys    map[string]struct{}
	unknown bool
	// under, sub, subUnknown: RootKeysUnder's nested read set.
	under      string
	sub        map[string]struct{}
	subUnknown bool
}

// readBeneath records what a root read of key does with the key's value:
// next is the suffix applied to it (nil when none). Only a read of w.under is
// tracked: a constant index beneath it is a sub key, anything else makes every
// sub key possible.
func (w *rootKeyWalker) readBeneath(key string, next *gojq.Suffix) {
	if w.under == "" || key != w.under {
		return
	}
	if next != nil && next.Index != nil {
		if k, ok := constIndexKey(next.Index); ok {
			w.sub[k] = struct{}{}
			return
		}
	}
	w.subUnknown = true
}

// constIndexKey is the key of a constant-string index (`.k`, `."k"`,
// `.["k"]`).
func constIndexKey(idx *gojq.Index) (string, bool) {
	switch {
	case idx == nil || idx.IsSlice || idx.End != nil:
		return "", false
	case idx.Start != nil:
		return constString(idx.Start)
	case idx.Name != "":
		return idx.Name, true
	case idx.Str != nil && idx.Str.Queries == nil:
		return idx.Str.Str, true
	}
	return "", false
}

// query walks q; root reports whether q's input is the root.
func (w *rootKeyWalker) query(q *gojq.Query, root bool) {
	if q == nil || w.unknown {
		return
	}
	if root && (len(q.Imports) > 0 || len(q.FuncDefs) > 0) {
		w.unknown = true
		return
	}
	if q.Term != nil {
		w.term(q.Term, root)
		return
	}
	switch q.Op {
	case gojq.OpPipe:
		if len(q.Patterns) > 0 {
			// `L as $p | R`: R runs on the same input as L.
			for _, p := range q.Patterns {
				w.pattern(p)
			}
			w.query(q.Left, root)
			w.query(q.Right, root)
			return
		}
		if isBareIdentity(q.Left) {
			w.query(q.Right, root)
			return
		}
		w.query(q.Left, root)
		w.query(q.Right, false)
	case gojq.OpAssign, gojq.OpModify, gojq.OpUpdateAdd, gojq.OpUpdateSub,
		gojq.OpUpdateMul, gojq.OpUpdateDiv, gojq.OpUpdateMod, gojq.OpUpdateAlt:
		if root {
			w.unknown = true
			return
		}
		w.query(q.Left, false)
		w.query(q.Right, false)
	default:
		// Comma, arithmetic, comparison, and/or, alternative: both sides
		// receive the same input.
		w.query(q.Left, root)
		w.query(q.Right, root)
	}
}

// pattern refuses a destructuring pattern with a computed key: its key query
// runs on the bound value, which this analysis does not track.
func (w *rootKeyWalker) pattern(p *gojq.Pattern) {
	if p == nil {
		return
	}
	for _, a := range p.Array {
		w.pattern(a)
	}
	for _, o := range p.Object {
		if o.KeyQuery != nil || (o.KeyString != nil && o.KeyString.Queries != nil) {
			w.unknown = true
			return
		}
		w.pattern(o.Val)
	}
}

func isBareIdentity(q *gojq.Query) bool {
	return q != nil && q.Term != nil && q.Term.Type == gojq.TermTypeIdentity &&
		len(q.Term.SuffixList) == 0 && len(q.Imports) == 0 && len(q.FuncDefs) == 0
}

// term walks t; root reports whether t's input is the root.
func (w *rootKeyWalker) term(t *gojq.Term, root bool) {
	if t == nil || w.unknown {
		return
	}
	suffixes := t.SuffixList
	switch t.Type {
	case gojq.TermTypeIdentity:
		if root {
			// `.` with suffixes: the first suffix indexes the root.
			if len(suffixes) == 0 || suffixes[0].Index == nil {
				w.unknown = true
				return
			}
			k := w.rootIndex(suffixes[0].Index)
			suffixes = suffixes[1:]
			w.readBeneath(k, firstSuffix(suffixes))
		}
	case gojq.TermTypeRecurse:
		if root {
			w.unknown = true
			return
		}
	case gojq.TermTypeIndex:
		if root {
			w.readBeneath(w.rootIndex(t.Index), firstSuffix(suffixes))
		} else {
			w.innerIndex(t.Index, root)
		}
	case gojq.TermTypeFunc:
		if strings.HasPrefix(t.Func.Name, "$") {
			// A variable reads no input; one bound to the root as a whole
			// was refused where it was bound (`. as $x`).
			break
		}
		if root {
			// A builtin or module function can read any key of its input.
			w.unknown = true
			return
		}
		for _, a := range t.Func.Args {
			w.query(a, false)
		}
	case gojq.TermTypeObject:
		for _, kv := range t.Object.KeyVals {
			w.objectKeyVal(kv, root)
		}
	case gojq.TermTypeArray:
		w.query(t.Array.Query, root)
	case gojq.TermTypeUnary:
		w.term(t.Unary.Term, root)
	case gojq.TermTypeFormat:
		if t.Str == nil {
			if root {
				w.unknown = true // `@base64` alone formats the whole input
				return
			}
		} else {
			w.str(t.Str, root)
		}
	case gojq.TermTypeString:
		w.str(t.Str, root)
	case gojq.TermTypeIf:
		w.query(t.If.Cond, root)
		w.query(t.If.Then, root)
		for _, e := range t.If.Elif {
			w.query(e.Cond, root)
			w.query(e.Then, root)
		}
		w.query(t.If.Else, root)
	case gojq.TermTypeTry:
		w.query(t.Try.Body, root)
		w.query(t.Try.Catch, false) // the catch body's input is the error
	case gojq.TermTypeReduce:
		w.pattern(t.Reduce.Pattern)
		w.query(t.Reduce.Query, root)
		w.query(t.Reduce.Start, root)
		w.query(t.Reduce.Update, false) // input: the accumulator
	case gojq.TermTypeForeach:
		w.pattern(t.Foreach.Pattern)
		w.query(t.Foreach.Query, root)
		w.query(t.Foreach.Start, root)
		w.query(t.Foreach.Update, false)
		w.query(t.Foreach.Extract, false)
	case gojq.TermTypeLabel:
		w.query(t.Label.Body, root)
	case gojq.TermTypeQuery:
		w.query(t.Query, root)
	}
	if w.unknown {
		return
	}
	// The remaining suffixes index the term's VALUE, not the root; an index
	// expression inside them (`.a[.b]`) still runs on the term's input.
	for _, s := range suffixes {
		if s.Index != nil {
			w.innerIndex(s.Index, root)
		}
	}
}

// rootIndex records a constant-string index into the root and returns its
// key, or gives up ("").
func (w *rootKeyWalker) rootIndex(idx *gojq.Index) string {
	k, ok := constIndexKey(idx)
	if !ok {
		w.unknown = true
		return ""
	}
	w.keys[k] = struct{}{}
	return k
}

func firstSuffix(ss []*gojq.Suffix) *gojq.Suffix {
	if len(ss) == 0 {
		return nil
	}
	return ss[0]
}

// constString reports the value of a query that is exactly one string
// literal with no interpolation.
func constString(q *gojq.Query) (string, bool) {
	if q == nil || q.Term == nil || len(q.Imports) > 0 || len(q.FuncDefs) > 0 {
		return "", false
	}
	t := q.Term
	if t.Type != gojq.TermTypeString || t.Str == nil || t.Str.Queries != nil || len(t.SuffixList) > 0 {
		return "", false
	}
	return t.Str.Str, true
}

// innerIndex walks the index expressions of an index applied to a non-root
// value; those expressions run on the enclosing term's input.
func (w *rootKeyWalker) innerIndex(idx *gojq.Index, root bool) {
	if idx == nil {
		return
	}
	if idx.Str != nil {
		w.str(idx.Str, root)
	}
	w.query(idx.Start, root)
	w.query(idx.End, root)
}

func (w *rootKeyWalker) objectKeyVal(kv *gojq.ObjectKeyVal, root bool) {
	if kv.KeyString != nil {
		w.str(kv.KeyString, root)
	}
	w.query(kv.KeyQuery, root)
	if kv.Val != nil {
		w.query(kv.Val, root)
		return
	}
	// Shorthand `{name}` / `{"name"}` is `{name: .name}`; `{$x}` reads a
	// variable; `{(q)}` with no value is not jq.
	if !root {
		return
	}
	switch {
	case kv.Key != "" && kv.Key[0] == '$':
	case kv.Key != "":
		w.keys[kv.Key] = struct{}{}
		w.readBeneath(kv.Key, nil)
	case kv.KeyString != nil && kv.KeyString.Queries == nil:
		w.keys[kv.KeyString.Str] = struct{}{}
		w.readBeneath(kv.KeyString.Str, nil)
	default:
		w.unknown = true
	}
}

func (w *rootKeyWalker) str(s *gojq.String, root bool) {
	if s == nil {
		return
	}
	for _, q := range s.Queries {
		w.query(q, root)
	}
}
