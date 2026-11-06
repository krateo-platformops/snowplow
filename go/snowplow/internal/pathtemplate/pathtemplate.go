// Package pathtemplate renders a RESTAction stage `path` (#1123).
//
// A stage path is a jq program (`${ ... }`) that builds the request URI. The
// program is written by the RESTAction author; the values it reads come from
// the call's input: the /call extras, an earlier stage's output, an iterator
// element. Before #1123 the program's result was dialed as-is, so a value that
// contained `/`, `..`, `?` or `#` changed the request itself: `42/../../users/me`
// walked out of the resource the author named, and `x?role=admin` appended a
// query. The request goes out with the stage's credential (the endpoint's, or
// the ServiceAccount's), so the caller chose what that credential was used for.
//
// # The rule
//
// The program's STRING LITERALS are the template: what the author wrote. Every
// other value the program splices into the path is a VALUE, and a value is
// exactly one thing:
//
//   - in the path part (before the template's first `?`), ONE PATH SEGMENT. It
//     is escaped with url.PathEscape, so `?`, `#`, `%` and the rest cannot end
//     the path or smuggle an escape. A value that contains `/` cannot be one
//     segment at all — the apiserver, and most HTTP servers, decode `%2F` back
//     into a separator — so it is refused, not escaped;
//   - in the query part (after the template's `?`), ONE QUERY VALUE: `&`, `=`,
//     `+`, `#`, `%`, `;` and everything outside RFC 3986's query characters are
//     escaped, so it cannot add a parameter or end the query.
//
// A rendered path with a `.` or `..` segment (or a percent-encoded one) is
// refused: the value or the template would walk the request out of the
// resource the template names.
//
// A value that leaves a path segment empty is refused too (#1185), on every
// verb and whatever the stage dials: `"/v2/tickets/" + .id` with .id "" or null
// would name the collection, not one ticket. The rule reads the pieces, not the
// rendered string. A segment is refused when a value was spliced into it and it
// holds nothing, so:
//
//   - a value empty inside a segment that has other text is not an empty
//     segment (`"/configmaps/" + .plural + "-" + .version + "-jsonschema"`);
//   - the query and the fragment hold no segments (`"/x?q=" + .v` is fine);
//   - no value, no refusal: `if .pl == "" then "" else "/" + .pl end` splices no
//     empty value, and a `/` or `//` the author wrote stays the author's text.
//     This rule neither refuses nor excuses it: the stage's dial guard judges it
//     as it judges any literal;
//   - a value is empty when it renders nothing. Whitespace renders `%20`: like
//     the dot check, this rule compares the segment exactly and trims nothing.
//     A dial that takes no escape refuses it there (validInterpolatedPath).
//
// "Spliced" is syntactic, and it is what an author reads in the program:
//
//   - an operand of `+` next to a template (`"/tickets/" + .id`);
//   - a string interpolation (`"/tickets/\(.id)"`);
//   - the whole program, when it is not a template at all (`${ .path }`).
//
// A template is a string literal, an interpolated string, `+` with a template
// operand, a parenthesized template, an `if` with a template branch, the right
// side of a pipe or of `E as $x | ...` that is a template, or a variable bound
// to a template (`(if .r == "1h" then "1%20HOUR" else "6%20HOUR" end) as $iv`
// keeps `$iv` the author's text) — and an object of templates indexed by a
// literal key (`$w.bk`). Anything else (`.x`, `.x // "d"`, `(.x | f)`, a
// function call, a value-bound variable) is a value.
//
// # A value the author already encoded
//
// `E | @uri` (and `@uri "...\(E)..."`) is a value the program escaped itself:
// jq's @uri leaves nothing but unreserved characters and %XX, so it is one
// query value as it stands, and escaping it again would send a different value
// (`%2F` as `%252F`). It renders as written. In the path part it is still one
// segment: an encoded '/' is refused like a '/', because a server decodes it
// back into a separator. Any other format (@base64, @sh, ...) is a value.
//
// # Declared multi-segment values
//
// A value that is meant to span segments says so: `segments(E)`. E is split on
// `/` and each piece is escaped as one segment; a `.` or `..` piece is still
// refused, so a declared value can never walk up the tree, and it cannot add a
// query. segments() is valid only where it is spliced into the path part.
//
// The edges of segments(E), which are the contract an author writes against:
//
//   - E null or "": one empty piece, so an empty segment, refused like any other
//     value. segments() does not make a value optional: the template's own `/`
//     stays, and `"/a/" + segments(.p)` would still name `/a/`. An optional part
//     is written so that no empty value is spliced (`if .p == "" then "" else
//     "/" + .p end`), or gated by the stage's iterator, which then yields no
//     element and has nothing to call;
//   - each piece is one segment, so an empty piece is refused wherever it is:
//     inside (`a//b`), at the end (`a/`), or a value that is only `/`. The one
//     `/` that is not a boundary between pieces is the leading `/` of a value
//     that opens the path (`${ segments(.path) }` with `/apis/apps/v1`): the root;
//   - E not a string (a number, a boolean, an array, an object): a render
//     error, never a tostring;
//   - segments() in the query is refused whatever E is, null included: the
//     refusal reads the template, so it does not wait for a value to arrive.
//
// A declared value can choose HOW MANY segments it is. Never declare one built
// from caller input on a stage whose credential the caller does not hold; split
// a known shape into single values instead, e.g. an apiVersion:
//
//	(.apiVersion | split("/")) as [$g, $v] | "/apis/" + $g + "/" + $v + "/" + .resource
//
// A query parameter is declared by writing it in the template:
// `"/events?limit=200&composition_id=" + .uid` encodes .uid as one query value.
//
// The package is pure: no state, no I/O.
package pathtemplate

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/jqutil"

	"github.com/krateo-platformops/snowplow/internal/apicall"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// SegmentsFunc is the declared multi-segment value: `segments(E)`. The jq
// interest analysis (internal/cache/jqinterest) reads it as a function of its
// argument.
const SegmentsFunc = "segments"

// reservedPrefix names the variables and the function the rewrite introduces.
// A program that already uses it is refused, so the rewrite can never capture
// one of the author's names.
const reservedPrefix = "__snowplow_path_"

const rawFunc = reservedPrefix + "raw"

// The four part kinds a rewritten program emits, besides a literal (a plain
// JSON string): a `+` operand ("v": string or null, jq's `+` identity), an
// interpolation or a non-template program ("i": already tostring'd), a declared
// multi-segment value ("s"), and a value the program encoded with @uri ("u").
const (
	partValue    = "v"
	partInterp   = "i"
	partSegments = "s"
	partEncoded  = "u"
)

// uriFormat is the jq format that makes a value one encoded value.
const uriFormat = "@uri"

// Refusal reasons: a bounded set, so a caller can count them by reason.
const (
	ReasonValueSpansSegments = "value_spans_segments"
	ReasonDotSegment         = "dot_segment"
	ReasonSegmentsInQuery    = "segments_in_query"
	ReasonEmptySegment       = "empty_segment"
)

// RefusedError is a path the template rendered but that must not be dialed:
// a value tried to be more than one segment or one query value. It never
// carries the value (the value is caller input; it is not logged).
type RefusedError struct {
	Reason string
}

func (e *RefusedError) Error() string {
	switch e.Reason {
	case ReasonValueSpansSegments:
		return "path template: a value spliced into the path contains '/', so it is not one segment (declare a multi-segment value with segments())"
	case ReasonDotSegment:
		return "path template: the rendered path has a '.' or '..' segment"
	case ReasonSegmentsInQuery:
		return "path template: segments() is spliced into the query; it declares path segments"
	case ReasonEmptySegment:
		return "path template: a value spliced into the path left a segment empty (write an optional part so that no empty value is spliced, or gate it with an iterator)"
	default:
		return "path template: refused (" + e.Reason + ")"
	}
}

// IsRefused reports whether err is a RefusedError.
func IsRefused(err error) bool {
	var r *RefusedError
	return errors.As(err, &r)
}

// IsEmptySegment reports whether err is the refusal of a value that left a path
// segment empty (#1185).
func IsEmptySegment(err error) bool {
	var r *RefusedError
	return errors.As(err, &r) && r.Reason == ReasonEmptySegment
}

var (
	errMultipleValues = errors.New("path template: the path expression produced more than one value")
	errNotString      = errors.New("path template: a value spliced into the path is not a string")
	errReserved       = errors.New("path template: the path expression uses the reserved prefix " + reservedPrefix)
)

// Template is a compiled stage path. It is immutable and safe to render
// concurrently: an iterator stage compiles its path once and renders it per
// element.
type Template struct {
	literal string     // the path, when it is not a ${...} template
	code    *gojq.Code // the rewritten program, when it is
	err     error      // the parse or compile error, returned by every Render
}

// Compile compiles a stage path. It never fails: a template that does not
// parse or compile yields a Template whose every Render returns that error,
// sanitized exactly as the plain jq render returns it.
func Compile(tmpl string) *Template {
	q, ok := jqutil.MaybeQuery(tmpl)
	if !ok {
		return &Template{literal: q}
	}
	if strings.Contains(q, reservedPrefix) {
		return &Template{err: errReserved}
	}
	parsed, err := gojq.Parse(q)
	if err != nil {
		return &Template{err: jqsupport.SanitizeError(fmt.Errorf("invalid jq query %q: %w", q, err))}
	}
	code, err := gojq.Compile(rewrite(parsed),
		gojq.WithModuleLoader(jqsupport.ModuleLoader()),
		gojq.WithFunction(rawFunc, 0, 0, func(v any, _ []any) any { return rawOf(v) }),
	)
	if err != nil {
		return &Template{err: jqsupport.SanitizeError(fmt.Errorf("unable to compile jq query %q: %w", q, err))}
	}
	return &Template{code: code}
}

// Render renders a stage path for one input (the dict, or one iterator
// element): Compile(tmpl).Render(ctx, data).
func Render(ctx context.Context, tmpl string, data any) (string, error) {
	return Compile(tmpl).Render(ctx, data)
}

// Render renders the path for one input. A string that is not a `${...}`
// template is returned unchanged: it holds no values. jq parse, compile and
// runtime errors are returned sanitized (jqsupport.SanitizeError), exactly as
// the plain jq render returns them; a path the rule refuses returns a
// *RefusedError.
func (t *Template) Render(ctx context.Context, data any) (string, error) {
	if t.err != nil {
		return "", t.err
	}
	if t.code == nil {
		return t.literal, nil
	}
	iter := t.code.RunWithContext(ctx, data)
	var out any
	n := 0
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return "", jqsupport.SanitizeError(err)
		}
		out = v
		n++
	}
	switch n {
	case 0:
		// No output renders "", exactly as the plain jq render did. The dial
		// plan refuses a template that rendered no path (createRequestOption).
		return "", nil
	case 1:
		return assemble(out)
	default:
		return "", errMultipleValues
	}
}

// ---------------------------------------------------------------------------
// The rewrite: a template expression becomes an expression that yields an
// ARRAY OF PARTS (literals and marked values) with the same number of outputs,
// in the same order, as the original expression. Go then assembles the parts,
// escaping each value for the position it lands in.

type kind int

const (
	kValue kind = iota
	kTemplate
	kObject // an object whose fields are all templates (bound to a variable)
)

type shape struct {
	k      kind
	fields map[string]bool // kObject: the field names
}

type scope struct {
	parent *scope
	name   string // "$x"
	sh     shape
	tvar   string // the parts variable, when sh is a template or an object
}

func (s *scope) lookup(name string) *scope {
	for c := s; c != nil; c = c.parent {
		if c.name == name {
			return c
		}
	}
	return nil
}

type rewriter struct{ n int }

func (r *rewriter) fresh() string {
	r.n++
	return fmt.Sprintf("$%st%d", reservedPrefix, r.n)
}

// rewrite turns the parsed path program into the parts-yielding program.
func rewrite(root *gojq.Query) *gojq.Query {
	r := &rewriter{}
	body := bodyOf(root)
	tq, sh := r.conv(body, nil)
	if sh.k != kTemplate {
		// Not a template: the whole program is one value, rendered with jq's
		// tostring like the plain render's JSON output (a string as itself).
		tq = pipe(paren(body), arrayOf(objOne(partInterp, call("tostring"))))
	}
	return withBody(root, tq)
}

// conv returns the parts form of q and its shape. For a value it returns
// (nil, value): the caller wraps the ORIGINAL expression.
func (r *rewriter) conv(q *gojq.Query, sc *scope) (*gojq.Query, shape) {
	if q == nil {
		return nil, shape{}
	}
	if len(q.FuncDefs) > 0 {
		bq, sh := r.conv(bodyOf(q), sc)
		if sh.k == kValue {
			return nil, sh
		}
		return withBody(q, bq), sh
	}
	if q.Term != nil {
		return r.convTerm(q.Term, sc)
	}
	switch {
	case q.Op == gojq.OpPipe && len(q.Patterns) > 0:
		return r.convBind(q, sc)
	case q.Op == gojq.OpPipe:
		rq, sh := r.conv(q.Right, sc)
		if sh.k == kValue {
			return nil, sh
		}
		return pipe(q.Left, rq), sh
	case q.Op == gojq.OpAdd:
		lq, lsh := r.conv(q.Left, sc)
		rq, rsh := r.conv(q.Right, sc)
		if lsh.k != kTemplate && rsh.k != kTemplate {
			return nil, shape{}
		}
		if lsh.k != kTemplate {
			lq = valuePart(q.Left)
		}
		if rsh.k != kTemplate {
			rq = valuePart(q.Right)
		}
		return &gojq.Query{Left: lq, Op: gojq.OpAdd, Right: rq}, shape{k: kTemplate}
	}
	return nil, shape{}
}

// convBind is `L as $x | R` (and the destructuring forms).
func (r *rewriter) convBind(q *gojq.Query, sc *scope) (*gojq.Query, shape) {
	if len(q.Patterns) == 1 && q.Patterns[0].Name != "" {
		name := q.Patterns[0].Name
		lq, lsh := r.conv(q.Left, sc)
		if lsh.k == kValue {
			rq, rsh := r.conv(q.Right, &scope{parent: sc, name: name, sh: lsh})
			if rsh.k == kValue {
				return nil, rsh
			}
			return &gojq.Query{Left: q.Left, Op: gojq.OpPipe, Right: rq, Patterns: q.Patterns}, rsh
		}
		tv := r.fresh()
		rq, rsh := r.conv(q.Right, &scope{parent: sc, name: name, sh: lsh, tvar: tv})
		if rsh.k == kValue {
			return nil, rsh
		}
		// parts(L) as $t | ($t | raw) as $x | R'
		// $x keeps L's own value for every non-template use; the template
		// positions of R' read $t instead.
		raw := pipe(variable(tv), call(rawFunc))
		inner := &gojq.Query{Left: raw, Op: gojq.OpPipe, Right: rq, Patterns: []*gojq.Pattern{{Name: name}}}
		return &gojq.Query{Left: lq, Op: gojq.OpPipe, Right: inner, Patterns: []*gojq.Pattern{{Name: tv}}}, rsh
	}
	// Destructuring (`as [$a, $b]`, `?//`): every bound name is a value.
	inner := sc
	for _, p := range q.Patterns {
		for _, name := range patternNames(p) {
			inner = &scope{parent: inner, name: name}
		}
	}
	rq, rsh := r.conv(q.Right, inner)
	if rsh.k == kValue {
		return nil, rsh
	}
	return &gojq.Query{Left: q.Left, Op: gojq.OpPipe, Right: rq, Patterns: q.Patterns}, rsh
}

func (r *rewriter) convTerm(t *gojq.Term, sc *scope) (*gojq.Query, shape) {
	if len(t.SuffixList) > 0 {
		return r.convFieldOfBound(t, sc)
	}
	switch t.Type {
	case gojq.TermTypeString:
		return stringParts(t.Str, objOne(partInterp, call("tostring"))), shape{k: kTemplate}
	case gojq.TermTypeFormat:
		if t.Format != uriFormat {
			return nil, shape{}
		}
		encoded := objOne(partEncoded, termQ(&gojq.Term{Type: gojq.TermTypeFormat, Format: uriFormat}))
		if t.Str == nil {
			return arrayOf(encoded), shape{k: kTemplate} // `E | @uri`
		}
		return stringParts(t.Str, encoded), shape{k: kTemplate} // `@uri "...\(E)..."`
	case gojq.TermTypeQuery:
		iq, sh := r.conv(t.Query, sc)
		if sh.k == kValue {
			return nil, sh
		}
		return paren(iq), sh
	case gojq.TermTypeIf:
		return r.convIf(t.If, sc)
	case gojq.TermTypeObject:
		return r.convObject(t.Object, sc)
	case gojq.TermTypeFunc:
		f := t.Func
		if strings.HasPrefix(f.Name, "$") && len(f.Args) == 0 {
			if b := sc.lookup(f.Name); b != nil && b.sh.k == kTemplate {
				return variable(b.tvar), shape{k: kTemplate}
			}
			return nil, shape{}
		}
		if f.Name == SegmentsFunc && len(f.Args) == 1 {
			return pipe(paren(f.Args[0]), arrayOf(objOne(partSegments, identity()))), shape{k: kTemplate}
		}
	}
	return nil, shape{}
}

// stringParts is a string's parts: its literal pieces as they are, and each
// interpolation piped into interp, the one-part object that marks it.
func stringParts(str *gojq.String, interp *gojq.Query) *gojq.Query {
	var out *gojq.Query
	for _, e := range str.Queries {
		var p *gojq.Query
		if e.Term != nil && e.Term.Str != nil {
			p = arrayOf(e) // a literal piece
		} else {
			p = pipe(paren(e), arrayOf(interp))
		}
		if out == nil {
			out = p
		} else {
			out = &gojq.Query{Left: out, Op: gojq.OpAdd, Right: p}
		}
	}
	if out == nil {
		return arrayOf(termQ(&gojq.Term{Type: gojq.TermTypeString, Str: &gojq.String{Str: str.Str}}))
	}
	return out
}

// convFieldOfBound is `$w.key` where $w is bound to an object of templates.
func (r *rewriter) convFieldOfBound(t *gojq.Term, sc *scope) (*gojq.Query, shape) {
	if t.Type != gojq.TermTypeFunc || !strings.HasPrefix(t.Func.Name, "$") || len(t.Func.Args) != 0 || len(t.SuffixList) != 1 {
		return nil, shape{}
	}
	s := t.SuffixList[0]
	if s.Index == nil || s.Iter || s.Optional {
		return nil, shape{}
	}
	key, ok := literalIndexKey(s.Index)
	if !ok {
		return nil, shape{}
	}
	b := sc.lookup(t.Func.Name)
	if b == nil || b.sh.k != kObject || !b.sh.fields[key] {
		return nil, shape{}
	}
	return termQ(&gojq.Term{
		Type:       gojq.TermTypeFunc,
		Func:       &gojq.Func{Name: b.tvar},
		SuffixList: []*gojq.Suffix{{Index: &gojq.Index{Name: key}}},
	}), shape{k: kTemplate}
}

func (r *rewriter) convIf(x *gojq.If, sc *scope) (*gojq.Query, shape) {
	branches := []*gojq.Query{x.Then}
	for _, el := range x.Elif {
		branches = append(branches, el.Then)
	}
	branches = append(branches, x.Else) // nil: `else .`
	conv := make([]*gojq.Query, len(branches))
	shapes := make([]shape, len(branches))
	anyT, allO := false, true
	for i, b := range branches {
		if b == nil {
			shapes[i] = shape{}
		} else {
			conv[i], shapes[i] = r.conv(b, sc)
		}
		anyT = anyT || shapes[i].k == kTemplate
		allO = allO && shapes[i].k == kObject
	}
	var out shape
	switch {
	case allO:
		// Every branch is an object of templates: the same field names, or it
		// is not one shape.
		for i := 1; i < len(shapes); i++ {
			if !sameFields(shapes[0].fields, shapes[i].fields) {
				return nil, shape{}
			}
		}
		out = shapes[0]
	case anyT:
		for i, sh := range shapes {
			if sh.k == kObject {
				return nil, shape{} // a template in one branch, an object in another
			}
			if sh.k == kValue {
				b := branches[i]
				if b == nil {
					b = identity()
				}
				conv[i] = valuePart(b)
			}
		}
		out = shape{k: kTemplate}
	default:
		return nil, shape{}
	}
	nx := &gojq.If{Cond: x.Cond, Then: conv[0]}
	for i, el := range x.Elif {
		nx.Elif = append(nx.Elif, &gojq.IfElif{Cond: el.Cond, Then: conv[i+1]})
	}
	nx.Else = conv[len(conv)-1]
	return termQ(&gojq.Term{Type: gojq.TermTypeIf, If: nx}), out
}

func (r *rewriter) convObject(o *gojq.Object, sc *scope) (*gojq.Query, shape) {
	if len(o.KeyVals) == 0 {
		return nil, shape{}
	}
	fields := map[string]bool{}
	kvs := make([]*gojq.ObjectKeyVal, 0, len(o.KeyVals))
	for _, kv := range o.KeyVals {
		key := kv.Key
		if key == "" && kv.KeyString != nil && kv.KeyString.Queries == nil {
			key = kv.KeyString.Str
		}
		if key == "" || strings.HasPrefix(key, "$") || kv.KeyQuery != nil || kv.Val == nil {
			return nil, shape{}
		}
		vq, vsh := r.conv(kv.Val, sc)
		if vsh.k != kTemplate {
			return nil, shape{}
		}
		fields[key] = true
		kvs = append(kvs, &gojq.ObjectKeyVal{Key: key, Val: paren(vq)})
	}
	return termQ(&gojq.Term{Type: gojq.TermTypeObject, Object: &gojq.Object{KeyVals: kvs}}), shape{k: kObject, fields: fields}
}

func literalIndexKey(ix *gojq.Index) (string, bool) {
	if ix.Start != nil || ix.End != nil || ix.IsSlice {
		return "", false
	}
	if ix.Name != "" {
		return ix.Name, true
	}
	if ix.Str != nil && ix.Str.Queries == nil {
		return ix.Str.Str, true
	}
	return "", false
}

func sameFields(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func patternNames(p *gojq.Pattern) []string {
	if p == nil {
		return nil
	}
	var out []string
	if p.Name != "" {
		out = append(out, p.Name)
	}
	for _, e := range p.Array {
		out = append(out, patternNames(e)...)
	}
	for _, e := range p.Object {
		if strings.HasPrefix(e.Key, "$") {
			out = append(out, e.Key)
		}
		out = append(out, patternNames(e.Val)...)
	}
	return out
}

// --- AST helpers -----------------------------------------------------------

func termQ(t *gojq.Term) *gojq.Query { return &gojq.Query{Term: t} }

func paren(q *gojq.Query) *gojq.Query {
	return termQ(&gojq.Term{Type: gojq.TermTypeQuery, Query: q})
}

func pipe(l, r *gojq.Query) *gojq.Query {
	return &gojq.Query{Left: l, Op: gojq.OpPipe, Right: r}
}

func identity() *gojq.Query { return termQ(&gojq.Term{Type: gojq.TermTypeIdentity}) }

func call(name string) *gojq.Query {
	return termQ(&gojq.Term{Type: gojq.TermTypeFunc, Func: &gojq.Func{Name: name}})
}

func variable(name string) *gojq.Query { return call(name) }

func arrayOf(q *gojq.Query) *gojq.Query {
	return termQ(&gojq.Term{Type: gojq.TermTypeArray, Array: &gojq.Array{Query: q}})
}

func objOne(key string, val *gojq.Query) *gojq.Query {
	return termQ(&gojq.Term{Type: gojq.TermTypeObject, Object: &gojq.Object{
		KeyVals: []*gojq.ObjectKeyVal{{Key: key, Val: val}},
	}})
}

// valuePart is `(E | [{v: .}])`: one parts array per output of E.
func valuePart(e *gojq.Query) *gojq.Query {
	return pipe(paren(e), arrayOf(objOne(partValue, identity())))
}

// bodyOf is q without its module header and function definitions.
func bodyOf(q *gojq.Query) *gojq.Query {
	return &gojq.Query{Term: q.Term, Left: q.Left, Right: q.Right, Patterns: q.Patterns, Op: q.Op}
}

// withBody is q's module header and function definitions over body.
func withBody(q, body *gojq.Query) *gojq.Query {
	if q.Meta == nil && len(q.Imports) == 0 && len(q.FuncDefs) == 0 {
		return body
	}
	return &gojq.Query{Meta: q.Meta, Imports: q.Imports, FuncDefs: q.FuncDefs, Term: paren(body).Term}
}

// --- parts -----------------------------------------------------------------

// partOf returns a part's kind and payload; a literal is kind "".
func partOf(p any) (string, any, bool) {
	switch p := p.(type) {
	case string:
		return "", p, true
	case map[string]any:
		if len(p) != 1 {
			return "", nil, false
		}
		for k, v := range p {
			switch k {
			case partValue, partInterp, partSegments, partEncoded:
				return k, v, true
			}
		}
	}
	return "", nil, false
}

// rawOf is a template's own value, unescaped: what the original expression
// yielded. A single part is its payload, whatever its type (`if c then "x"
// else .n end` is .n itself); several parts are concatenated as jq's `+` would,
// null contributing nothing, and a non-string is jq's "cannot add" error.
// An object of templates is each field's raw value.
func rawOf(v any) any {
	switch v := v.(type) {
	case []any:
		if len(v) == 1 {
			_, p, ok := partOf(v[0])
			if !ok {
				return errNotString
			}
			return p
		}
		var b strings.Builder
		for _, e := range v {
			_, p, ok := partOf(e)
			if !ok {
				return errNotString
			}
			switch p := p.(type) {
			case nil:
			case string:
				b.WriteString(p)
			default:
				return errNotString
			}
		}
		return b.String()
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, f := range v {
			rv := rawOf(f)
			if err, ok := rv.(error); ok {
				return err
			}
			out[k] = rv
		}
		return out
	}
	return errNotString
}

type position int

const (
	inPath position = iota
	inQuery
	inFragment
)

// assemble joins the parts, escaping each value for where it lands.
func assemble(v any) (string, error) {
	parts, ok := v.([]any)
	if !ok {
		return "", errNotString
	}
	var b strings.Builder
	var segs segmentWatch
	pos := inPath
	for _, e := range parts {
		k, p, ok := partOf(e)
		if !ok {
			return "", errNotString
		}
		// Before the null drop: segments() in the query is the template's fault,
		// whatever value this call happens to carry.
		if k == partSegments && pos != inPath {
			return "", &RefusedError{Reason: ReasonSegmentsInQuery}
		}
		if p == nil && k != "" {
			if pos == inPath {
				segs.value("") // jq's `+ null`: a value that rendered nothing
			}
			continue
		}
		s, ok := p.(string)
		if !ok {
			return "", errNotString
		}
		switch k {
		case "":
			if pos == inPath {
				segs.literal(s)
			}
			b.WriteString(s)
			pos = advance(pos, s)
		case partValue, partInterp:
			if pos == inPath {
				if strings.Contains(s, "/") {
					return "", &RefusedError{Reason: ReasonValueSpansSegments}
				}
				segs.value(s)
				b.WriteString(url.PathEscape(s))
			} else {
				b.WriteString(EscapeQueryValue(s))
			}
		case partEncoded:
			if pos == inPath {
				if dec, err := url.PathUnescape(s); err != nil || strings.Contains(dec, "/") {
					return "", &RefusedError{Reason: ReasonValueSpansSegments}
				}
				segs.value(s)
			}
			b.WriteString(s)
		case partSegments:
			if b.Len() == 0 && strings.HasPrefix(s, "/") {
				// The root of a value that opens the path, not a boundary
				// between two pieces.
				segs.literal("/")
				b.WriteByte('/')
				s = s[1:]
			}
			for i, piece := range strings.Split(s, "/") {
				if i > 0 {
					segs.end()
					b.WriteByte('/')
				}
				segs.value(piece)
				b.WriteString(url.PathEscape(piece))
			}
		}
	}
	if pos == inPath {
		segs.end()
	}
	out := b.String()
	if hasDotSegment(out) {
		return "", &RefusedError{Reason: ReasonDotSegment}
	}
	if segs.emptied {
		return "", &RefusedError{Reason: ReasonEmptySegment}
	}
	return out, nil
}

// segmentWatch follows the path part as assemble writes it, piece by piece, and
// records a segment that a value left empty (#1185). It sees what the rendered
// string cannot: `/a/` from `"/a/" + .id` and `/a/` as the author wrote it are
// the same text, and only the first lost a value.
type segmentWatch struct {
	text    bool // the current segment holds text
	spliced bool // a value was spliced into the current segment
	emptied bool // a finished segment had a value spliced into it and holds nothing
}

// literal takes author text that lands in the path part. Its first '?' or '#'
// ends the path (advance's rule), and with it the current segment.
func (w *segmentWatch) literal(s string) {
	cut := strings.IndexAny(s, "?#")
	if cut >= 0 {
		s = s[:cut]
	}
	for {
		seg, rest, more := strings.Cut(s, "/")
		w.text = w.text || seg != ""
		if !more {
			break
		}
		w.end()
		s = rest
	}
	if cut >= 0 {
		w.end()
	}
}

// value takes one value spliced into the current segment; s is what it rendered.
func (w *segmentWatch) value(s string) {
	w.spliced = true
	w.text = w.text || s != ""
}

// end finishes the current segment.
func (w *segmentWatch) end() {
	w.emptied = w.emptied || (w.spliced && !w.text)
	w.text, w.spliced = false, false
}

// advance moves the position past a literal: the first '?' in the path ends
// it, and a '#' starts the fragment (SplitURI's order: the fragment is cut
// first, so a '#' before any '?' means there is no query).
func advance(pos position, lit string) position {
	for i := 0; i < len(lit) && pos != inFragment; i++ {
		switch lit[i] {
		case '#':
			pos = inFragment
		case '?':
			if pos == inPath {
				pos = inQuery
			}
		}
	}
	return pos
}

// hasDotSegment reports a '.' or '..' segment in the path part, percent-encoded
// or not.
func hasDotSegment(p string) bool {
	path, _, _ := apicall.SplitURI(p)
	for _, seg := range strings.Split(path, "/") {
		if dec, err := url.PathUnescape(seg); err == nil {
			seg = dec
		}
		if seg == "." || seg == ".." {
			return true
		}
	}
	return false
}

// EscapeQueryValue escapes s as one query value (a key or a value of one
// parameter). It keeps RFC 3986's query characters that carry no structure in
// a query string — unreserved, and ! $ ' ( ) * , : @ / ? — and escapes the
// rest: & = + ; # % (the separators and the escape), space, and everything
// else. So a value renders as itself whenever it can, and never adds a
// parameter. url.QueryEscape would also escape '@', ':' and '/', changing
// values that are already safe.
func EscapeQueryValue(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if keepInQuery(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&15])
	}
	return b.String()
}

func keepInQuery(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	switch c {
	case '-', '.', '_', '~', '!', '$', '\'', '(', ')', '*', ',', ':', '@', '/', '?':
		return true
	}
	return false
}
