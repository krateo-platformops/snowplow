package redact

// endpoint_url_guard_test.go — #503. No log site in this module may emit a
// URL-bearing endpoint field without routing it through redact.URL.
//
// THE DEFECT THIS PINS. 14 sites handed Endpoint.ServerURL straight to slog
// (internal/resolvers/restactions/api/resolve.go — 3 at Error, 1 at Warn, 9 at
// Debug — and endpoints_tls_233.go at Warn). Error and Warn are always on in
// production, so a ServerURL of the ordinary "https://user:pass@host/base"
// shape reached otel_logs in clear. #502 hardened endpointLogAttr until it
// refuses anything it cannot clean; none of these sites went through it.
//
// WHY THIS GUARD IS NOT A SPELLING MATCH. The #453 scan in log_guard_test.go is
// SYNTACTIC: it matches identity field names and identifier conventions in the
// source text. That is the #485/#500 bypass class — hoist the field into a
// local and the spelling is gone:
//
//	u := ep.ServerURL
//	slog.String("host", u)        // a spelling match sees only "u"
//
// So this guard is TYPE-driven on the ORIGIN of the value, not on how it is
// written:
//
//   - A leak is SEEDED from go/types: a selector whose *types.Selection is a
//     FieldVal naming a string field whose name ends in URL/URI, on any struct
//     type. The field is identified by its type object, so renaming the local,
//     the receiver or the import changes nothing. It is deliberately not pinned
//     to endpoints.Endpoint: a new URL field on any logged struct is covered
//     without anyone editing a list.
//
//   - Taint PROPAGATES to a fixed point over a single module-wide object map,
//     so the hoist above is caught, and so are the variants: a re-assignment, a
//     string concatenation, fmt.Sprintf, strings.TrimSuffix, url.Parse followed
//     by u.String() or u.Host, a struct field written in one function and logged
//     in another, and a helper function that RETURNS the field (the callee's
//     *types.Func object is itself tainted, so `slog.String("h", host(ep))`
//     fails too).
//
//   - A sanitiser is recognised by its *types.Func OBJECT — package path plus
//     name — never by the source spelling. An import alias, a dot-import or a
//     rename cannot launder a value past it. (#502's own structural guard was
//     rewritten twice for exactly these evasions.)
//
//   - Only a value whose static type can CARRY text is flagged
//     (canRenderURLText): a bool or an int derived from a URL cannot leak it,
//     so `slog.Bool("self", parsedHostEqualsSelf(ep.ServerURL))` is clean.
//
// WHAT IT STILL DOES NOT SEE — stated rather than implied, because claiming
// more than was built is the failure #503 warns about:
//
//   - Taint is flow-INSENSITIVE. An object assigned a URL anywhere in the
//     module is tainted at every read of it, so a variable deliberately
//     overwritten with a clean value before being logged would be a false
//     positive. None exists today; the direction is the safe one.
//   - It is INTRA-module. A method on a dependency's type that returns a URL
//     field (none exists on endpoints.Endpoint today) is not followed.
//   - A func VALUE (a closure assigned to a variable, a method value passed as
//     a callback) is not followed — only a direct call of a named function.
//   - The SINK SET is an enumeration: logMethods, printSinks and urlAttrCtors.
//     Its package is resolved by object (so an aliased or dot-imported slog is
//     still seen), but a logging shape not in those tables is not a sink here.
//     It also scans PRODUCTION files only, in parity with the #453 typed half:
//     a _test.go leak is not a production leak.
//   - It proves nothing about what redact.URL itself returns. That is
//     internal/redact's own corpus arm (TestS499_URLNeverLeaksOverAShapeCorpus)
//     plus the behavioural arms in the api package
//     (TestS503_ResolveErrorLogNeverCarriesAServerURLPassword and
//     TestS503_UnparseableCAWarnNeverCarriesAServerURLPassword), which drive a
//     credential-bearing ServerURL through the real resolver and the real TLS
//     client builder.

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// urlSanitizers are the functions that make a URL-derived value fit to log,
// keyed "<package path>.<Func>" so the key is an identity, not a spelling.
//
// redact.URL is the one that renders: it drops userinfo, replaces every query
// value, and returns URLUnparseable for anything it cannot clean. The rest are
// one-way keyed digests (Digest and the labels built on it) — they cannot
// return their input, so a URL passed through one cannot leak either.
//
// redact.ErrorText is deliberately ABSENT. It rewrites the apiserver's
// `User "x"` and `"x-clientconfig"` patterns and nothing else (redact.go:129),
// so it does not remove a credential from a URL and must not be treated as if
// it did.
var urlSanitizers = map[string]bool{
	"github.com/krateo-platformops/snowplow/internal/redact.URL":         true,
	"github.com/krateo-platformops/snowplow/internal/redact.Digest":      true,
	"github.com/krateo-platformops/snowplow/internal/redact.User":        true,
	"github.com/krateo-platformops/snowplow/internal/redact.Group":       true,
	"github.com/krateo-platformops/snowplow/internal/redact.Groups":      true,
	"github.com/krateo-platformops/snowplow/internal/redact.Identity":    true,
	"github.com/krateo-platformops/snowplow/internal/redact.Prefixed":    true,
	"github.com/krateo-platformops/snowplow/internal/redact.KeyLabel":    true,
	"github.com/krateo-platformops/snowplow/internal/redact.ValueDigest": true,

	// A module helper that only ever returns redact.URL output.
	// TestS503_RegisteredHelpersReturnOnlySanitisedURLs checks every return
	// statement of each one, so registering a helper cannot smuggle a raw URL
	// through this set.
	"github.com/krateo-platformops/snowplow/internal/handlers.logServerURL": true,
}

// urlHelperSanitizers is the subset of urlSanitizers that is a MODULE function
// rather than one of this package's own primitives. Each is verified by
// TestS503_RegisteredHelpersReturnOnlySanitisedURLs.
var urlHelperSanitizers = map[string]bool{
	"github.com/krateo-platformops/snowplow/internal/handlers.logServerURL": true,
}

// urlFieldSuffixes name a struct field that holds a URL. A URL is the carrier
// #503 is about: credentials ride in its userinfo and its query.
var urlFieldSuffixes = []string{"URL", "Url", "URI", "Uri"}

// WHY THIS GUARD IS NOT SEEDED FROM "ANY Endpoint FIELD", which #503 suggests.
// It was built that way and then backed out, on evidence.
//
// Pinning plumbing's Endpoint by type identity (package path
// "github.com/krateo-platformops/plumbing/endpoints", name "Endpoint") and
// seeding from EVERY string field of it also drags in Token, Username,
// Password and the Aws* credentials. Run against the module, that produced
// five findings, all in internal/handlers/dispatchers/resolve_populate.go
// (:233 and :401/437/473/582), and NONE of them is a URL leak:
//
//	refreshUser := inputs.RepresentativeUsername
//	if saUser, ok := phase1SAUsername(saEP.Token); ok { refreshUser = saUser }
//
// refreshUser is a USERNAME extracted from a service-account JWT, not the
// token, and it reaches its log sites through refreshLogUser — a helper
// already registered in the #453 redactingHelpers set, i.e. already redacted.
// The findings were artefacts of this guard not sharing that registry.
//
// So the credential fields of an Endpoint are #453's territory: the syntactic
// scan's identitySelectors, the typed half's sensitiveFieldExact, and
// redactingHelpers. Duplicating that registry here would rebuild #453 inside
// #503, and until it was duplicated this guard would tell a reader to "wrap it
// in redact.URL" for a bearer token — advice that is simply wrong.
//
// Checked while backing it out: NO site in the module logs a raw Endpoint
// credential field. The five findings above were the complete result, and all
// five were redacted or descriptive values.
//
// isURLBearingField reports whether f is a string struct field whose name says
// it holds a URL — on ANY struct, not only an Endpoint, so a URL field on a
// type this guard has never heard of is covered the day it is written.
//
// Driven off the *types.Var, so it is the field's identity that matches, not
// the text at the call site. The NAME is a heuristic and is admitted as one —
// see the note above on why the wider "any Endpoint field" seed was built,
// measured and then removed rather than kept.
func isURLBearingField(f *types.Var) bool {
	if f == nil || !f.IsField() {
		return false
	}
	b, ok := f.Type().Underlying().(*types.Basic)
	if !ok || b.Info()&types.IsString == 0 {
		return false
	}
	for _, s := range urlFieldSuffixes {
		if strings.HasSuffix(f.Name(), s) {
			return true
		}
	}
	return false
}

// canRenderURLText reports whether a value of type t can carry URL text into a
// record. A bool or a number computed FROM a URL cannot leak it, so those are
// not flagged; everything else (string, interface, pointer, struct, slice) is.
func canRenderURLText(t types.Type) bool {
	if t == nil {
		return true
	}
	if b, ok := t.Underlying().(*types.Basic); ok {
		return b.Info()&types.IsString != 0
	}
	return true
}

// isNetURL reports whether t is net/url.URL.
func isNetURL(t types.Type) bool {
	n, ok := t.(*types.Named)
	if !ok {
		return false
	}
	return n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "net/url" && n.Obj().Name() == "URL"
}

// propagatesURL draws THE BOUNDARY OF WHAT THIS GUARD CLAIMS, and it is the one
// decision in this file worth arguing with.
//
// Taint follows a URL through a value that still IS the URL: a string (or a
// named string type), a []byte or []string of them, and a net/url.URL — the
// shapes a transform like fmt.Sprintf, strings.TrimSuffix or url.Parse hands
// back. Those are the #503 class: the field, or the field restated.
//
// It deliberately STOPS at an error and at an arbitrary struct, because a URL
// inside one of those is inside free TEXT that some other package composed.
// That is a real leak and a separate one. Traced while building this guard:
// httpFetchAllowingNonJSON (external_fetch.go:74-82) builds `uri` from
// Endpoint.ServerURL and, when url.Parse rejects it, returns
// response.New(500, err) — and url.Error.Error() renders its URL VERBATIM,
// credentials and all (probed: `parse "https://u:pa ss@host/...": net/url:
// invalid control character in URL`). resolve.go then logs that envelope's
// Message at Error level. Scrubbing a URL out of arbitrary error text is a
// different design question from redacting a field — it is the #453 error-text
// residual and the #500 taint-pass gap — so it gets its OWN issue, #523, rather
// than being quietly folded in here or parked in an allow-list.
//
// TestS503_ErrorTextEmbeddingIsTheDOCUMENTEDGap pins this boundary in the
// failing direction, so the gap is a tested statement rather than a sentence in
// a comment that nobody can check.
func propagatesURL(t types.Type) bool {
	if t == nil {
		return false
	}
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Info()&types.IsString != 0
	case *types.Slice:
		b, ok := u.Elem().Underlying().(*types.Basic)
		return ok && (b.Info()&types.IsString != 0 || b.Kind() == types.Uint8)
	case *types.Pointer:
		return isNetURL(u.Elem())
	case *types.Struct:
		return isNetURL(t)
	}
	return false
}

// urlAttrCtors are the slog constructors whose VALUE argument this guard reads.
// slog.String is the one the #503 sites used and the one loggedValues (the
// #453 typed half) deliberately ignores, because that half only looks for a
// struct logged whole and slog.String forces a string.
var urlAttrCtors = map[string]int{ // ctor -> number of args when the value is the last one
	"String": 2, "Any": 2, "StringValue": 1, "AnyValue": 1,
}

// loggedURLValues returns the argument expressions of c that a log or print
// sink renders as text (nil when c is not a sink).
//
// The sink's PACKAGE is resolved through go/types where it can be, so an alias
// or a dot-import of log/slog, fmt or log cannot hide a sink behind a different
// spelling — the same reasoning as isSanitizerCall. The name tables are then
// still consulted as a fallback, which is a UNION rather than a replacement:
// resolving strictly to log/slog would lose a module logger WRAPPER whose
// method is also called Debug/Warn/Error, and losing a sink is a false
// negative. So this accepts a sink if either route recognises it.
func (g *urlGuard) loggedURLValues(c *ast.CallExpr) []ast.Expr {
	pkg, name := calleeName(c.Fun)
	resolved := false
	if fn := g.funcObject(c); fn != nil && fn.Pkg() != nil {
		switch p := fn.Pkg().Path(); p {
		case "log/slog":
			pkg, name, resolved = "slog", fn.Name(), true
		case "fmt", "log":
			pkg, name, resolved = p, fn.Name(), true
		}
	}
	if pkg == "slog" {
		if want, ok := urlAttrCtors[name]; ok && len(c.Args) == want {
			return c.Args[want-1:]
		}
		if name == "Group" && len(c.Args) > 1 {
			return c.Args[1:]
		}
	}
	if first, ok := printSinks[pkg+"."+name]; ok {
		return c.Args[min(first, len(c.Args)):]
	}
	msgIdx, isLogMethod := logMethods[name]
	if !isLogMethod {
		return nil
	}
	// A resolved log/slog top-level function (a dot-import makes it a bare
	// Ident) is a sink whatever its syntactic shape; for the unresolved
	// fallback, keep the existing guard's requirement that it be a method
	// call, so a local function that merely happens to be named Log is not
	// mistaken for one.
	if _, isSel := c.Fun.(*ast.SelectorExpr); !isSel && !(resolved && pkg == "slog") {
		return nil
	}
	return c.Args[min(msgIdx+1, len(c.Args)):]
}

// urlGuard is the module-wide taint state. One map of *types.Object for the
// whole module: object identity is unique per declaration, so locals of
// different functions never collide, while a struct FIELD or a named FUNCTION
// is shared — which is what makes cross-function taint work.
type urlGuard struct {
	sanitizers map[string]bool
	taint      map[types.Object]string
	// fnTaint is per RESULT INDEX, not per function. Precision matters here:
	// httpFetchAllowingNonJSON returns (*response.Status, []byte, string,
	// error) and only results 0 and 3 carry the URL — both through error text,
	// which propagatesURL stops. Tainting the whole function instead would
	// taint `res` and report an error-text leak as if this guard covered it.
	fnTaint map[*types.Func]map[int]string
	pkg     *packages.Package
}

// funcObject returns the *types.Func a call resolves to, by OBJECT. An import
// alias, a dot-import or a local rename all land on the same object, so none of
// them can launder a value past the sanitiser set.
func (g *urlGuard) funcObject(c *ast.CallExpr) *types.Func {
	var id *ast.Ident
	switch f := c.Fun.(type) {
	case *ast.Ident:
		id = f
	case *ast.SelectorExpr:
		id = f.Sel
	case *ast.IndexExpr: // a generic instantiation
		switch inner := f.X.(type) {
		case *ast.Ident:
			id = inner
		case *ast.SelectorExpr:
			id = inner.Sel
		}
	}
	if id == nil {
		return nil
	}
	fn, _ := g.pkg.TypesInfo.Uses[id].(*types.Func)
	return fn
}

func (g *urlGuard) isSanitizerCall(c *ast.CallExpr) bool {
	fn := g.funcObject(c)
	if fn == nil || fn.Pkg() == nil {
		return false
	}
	return g.sanitizers[fn.Pkg().Path()+"."+fn.Name()]
}

// objectOf resolves an identifier to its declared object (a use or a def).
func (g *urlGuard) objectOf(id *ast.Ident) types.Object {
	if o := g.pkg.TypesInfo.Uses[id]; o != nil {
		return o
	}
	return g.pkg.TypesInfo.Defs[id]
}

// fieldOf returns the struct field a selector reads, when it is one.
func (g *urlGuard) fieldOf(sel *ast.SelectorExpr) (*types.Var, types.Type) {
	s, ok := g.pkg.TypesInfo.Selections[sel]
	if !ok || s.Kind() != types.FieldVal {
		return nil, nil
	}
	v, _ := s.Obj().(*types.Var)
	return v, s.Recv()
}

// taintedExpr returns the originating field label when e carries a URL, "" when
// it does not. A sanitiser call stops the walk — that is the only way a tainted
// value becomes clean.
func (g *urlGuard) taintedExpr(e ast.Expr) string {
	switch v := e.(type) {
	case nil:
		return ""
	case *ast.ParenExpr:
		return g.taintedExpr(v.X)
	case *ast.Ident:
		if o := g.objectOf(v); o != nil {
			return g.taint[o]
		}
	case *ast.SelectorExpr:
		if f, recv := g.fieldOf(v); f != nil {
			if isURLBearingField(f) {
				name := "a struct"
				if recv != nil {
					name = recv.String()
				}
				return name + "." + f.Name()
			}
			if l := g.taint[f]; l != "" {
				return l
			}
		}
		return g.taintedExpr(v.X)
	case *ast.StarExpr:
		return g.taintedExpr(v.X)
	case *ast.UnaryExpr:
		return g.taintedExpr(v.X)
	case *ast.BinaryExpr: // string concatenation
		if s := g.taintedExpr(v.X); s != "" {
			return s
		}
		return g.taintedExpr(v.Y)
	case *ast.IndexExpr:
		return g.taintedExpr(v.X)
	case *ast.SliceExpr:
		return g.taintedExpr(v.X)
	case *ast.TypeAssertExpr:
		return g.taintedExpr(v.X)
	case *ast.CallExpr:
		return g.callTaint(v)[0]
	case *ast.CompositeLit:
		for _, el := range v.Elts {
			if kv, ok := el.(*ast.KeyValueExpr); ok {
				if s := g.taintedExpr(kv.Value); s != "" {
					return s
				}
				continue
			}
			if s := g.taintedExpr(el); s != "" {
				return s
			}
		}
	case *ast.KeyValueExpr:
		return g.taintedExpr(v.Value)
	}
	return ""
}

// callTaint returns the taint of a call PER RESULT INDEX. A result is tainted
// when either:
//
//   - the callee is a module function whose body was traced returning a URL at
//     that index (this is what catches a hoist behind a function boundary,
//     `slog.String("host", hostOf(ep))`); or
//   - a tainted value was handed in — as an argument or as the receiver — and
//     the result's TYPE can still be the URL (propagatesURL). This covers
//     fmt.Sprintf, strings.TrimSuffix, url.Parse and anything else that
//     restates a URL, without having to enumerate them.
//
// A sanitiser call is clean at every index: that is the one cleansing step.
func (g *urlGuard) callTaint(v *ast.CallExpr) map[int]string {
	if g.isSanitizerCall(v) {
		return nil
	}
	rt := g.pkg.TypesInfo.Types[v].Type
	tup, isTuple := rt.(*types.Tuple)
	n := 1
	if isTuple {
		n = tup.Len()
	}
	resultAt := func(i int) types.Type {
		if isTuple {
			return tup.At(i).Type()
		}
		return rt
	}

	out := map[int]string{}
	if fn := g.funcObject(v); fn != nil {
		for i, l := range g.fnTaint[fn] {
			if i < n && l != "" && propagatesURL(resultAt(i)) {
				out[i] = l
			}
		}
	}

	in := ""
	if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
		in = g.taintedExpr(sel.X) // a method on a tainted receiver: u.String()
	}
	if in == "" {
		for _, a := range v.Args {
			if s := g.taintedExpr(a); s != "" {
				in = s
				break
			}
		}
	}
	if in != "" {
		for i := 0; i < n; i++ {
			if out[i] == "" && propagatesURL(resultAt(i)) {
				out[i] = in
			}
		}
	}
	return out
}

// multiTaint returns the per-index taint of the single right-hand side of an
// n-target assignment (`u, err := url.Parse(...)`). A non-call right-hand side
// (a type assertion, a map index, a channel receive) taints every target.
func (g *urlGuard) multiTaint(rhs ast.Expr, n int) map[int]string {
	if call, ok := rhs.(*ast.CallExpr); ok {
		return g.callTaint(call)
	}
	out := map[int]string{}
	if l := g.taintedExpr(rhs); l != "" {
		for i := 0; i < n; i++ {
			out[i] = l
		}
	}
	return out
}

// mark taints the object an assignment target denotes: a local (an Ident) or a
// struct field (a Selector). Marking the FIELD object is what carries taint
// across functions.
func (g *urlGuard) mark(lhs ast.Expr, label string) bool {
	if label == "" {
		return false
	}
	var o types.Object
	switch v := lhs.(type) {
	case *ast.Ident:
		if v.Name == "_" {
			return false
		}
		o = g.objectOf(v)
	case *ast.SelectorExpr:
		if f, _ := g.fieldOf(v); f != nil {
			o = f
		}
	case *ast.StarExpr:
		return g.mark(v.X, label)
	case *ast.IndexExpr:
		return g.mark(v.X, label)
	}
	if o == nil || g.taint[o] != "" {
		return false
	}
	g.taint[o] = label
	return true
}

// propagate runs one pass of the taint relation over a declaration and reports
// whether anything changed. The caller iterates to a fixed point.
func (g *urlGuard) propagate(decl ast.Decl) bool {
	changed := false
	// The function object a return statement belongs to, innermost first.
	var fnStack []*types.Func
	if fd, ok := decl.(*ast.FuncDecl); ok {
		if fn, _ := g.pkg.TypesInfo.Defs[fd.Name].(*types.Func); fn != nil {
			fnStack = append(fnStack, fn)
		}
	}
	ast.Inspect(decl, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			if len(s.Rhs) == len(s.Lhs) {
				for i := range s.Rhs {
					changed = g.mark(s.Lhs[i], g.taintedExpr(s.Rhs[i])) || changed
				}
			} else if len(s.Rhs) == 1 {
				// u, err := url.Parse(ep.ServerURL) — per result index, so the
				// *url.URL is tainted and the error is not.
				for i, l := range g.multiTaint(s.Rhs[0], len(s.Lhs)) {
					changed = g.mark(s.Lhs[i], l) || changed
				}
			}
		case *ast.ValueSpec:
			if len(s.Values) == len(s.Names) {
				for i := range s.Values {
					changed = g.mark(s.Names[i], g.taintedExpr(s.Values[i])) || changed
				}
			} else if len(s.Values) == 1 {
				for i, l := range g.multiTaint(s.Values[0], len(s.Names)) {
					changed = g.mark(s.Names[i], l) || changed
				}
			}
		case *ast.RangeStmt:
			if l := g.taintedExpr(s.X); l != "" {
				changed = g.mark(s.Key, l) || changed
				changed = g.mark(s.Value, l) || changed
			}
		case *ast.SendStmt:
			if l := g.taintedExpr(s.Value); l != "" {
				changed = g.mark(s.Chan, l) || changed
			}
		case *ast.ReturnStmt:
			if len(fnStack) == 0 {
				return true
			}
			fn := fnStack[len(fnStack)-1]
			for i, res := range s.Results {
				l := g.taintedExpr(res)
				if l == "" {
					continue
				}
				if g.fnTaint[fn] == nil {
					g.fnTaint[fn] = map[int]string{}
				}
				if g.fnTaint[fn][i] == "" {
					g.fnTaint[fn][i] = l
					changed = true
				}
			}
		}
		return true
	})
	return changed
}

// endpointURLScan reports every production log site in pkgs that emits a
// URL-derived value without a sanitiser, and counts the sites that DO go
// through one (the non-vacuity signal: a scan that stopped recognising the
// field would report zero of both).
func endpointURLScan(pkgs []*packages.Package, sanitizers map[string]bool) (hits []string, sanitised int) {
	type decl struct {
		p *packages.Package
		f *ast.File
		d ast.Decl
	}
	var decls []decl
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			for _, d := range f.Decls {
				decls = append(decls, decl{p, f, d})
			}
		}
	}

	g := &urlGuard{sanitizers: sanitizers, taint: map[types.Object]string{}, fnTaint: map[*types.Func]map[int]string{}}
	// Fixed point over the WHOLE module, so a field or a helper tainted in one
	// package is tainted at a log site in another. Bounded: each pass can only
	// add to the map, and the bound is generous rather than tuned.
	for pass := 0; pass < 12; pass++ {
		changed := false
		for _, d := range decls {
			g.pkg = d.p
			changed = g.propagate(d.d) || changed
		}
		if !changed {
			break
		}
	}

	for _, d := range decls {
		g.pkg = d.p
		ast.Inspect(d.d, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, v := range g.loggedURLValues(c) {
				tv := d.p.TypesInfo.Types[v]
				if isSlogType(tv.Type) {
					// An Attr/Value handed to a log method: its own constructor
					// call is a sink in its own right and is visited separately,
					// so counting it here would report (and count) every site
					// twice.
					continue
				}
				if !canRenderURLText(tv.Type) {
					continue
				}
				pos := d.p.Fset.Position(v.Pos())
				where := filepath.Base(filepath.Dir(pos.Filename)) + "/" + filepath.Base(pos.Filename)
				if l := g.taintedExpr(v); l != "" {
					hits = append(hits, fmt.Sprintf("%s:%d: logged value %s carries %s without redact.URL",
						where, pos.Line, types.ExprString(v), l))
					continue
				}
				// A sanitised URL: the value is a sanitiser call wrapping
				// something tainted. Counted so the arm cannot pass vacuously.
				if call, ok := v.(*ast.CallExpr); ok && g.isSanitizerCall(call) {
					for _, a := range call.Args {
						if g.taintedExprIgnoringSanitizers(a) != "" {
							sanitised++
							break
						}
					}
				}
			}
			return true
		})
	}
	sort.Strings(hits)
	return hits, sanitised
}

// taintedExprIgnoringSanitizers is taintedExpr with the cleansing step removed,
// used only to count how many sanitised sites exist. Without it a redact.URL
// call would report "" for its own argument and the non-vacuity count would
// always be zero.
func (g *urlGuard) taintedExprIgnoringSanitizers(e ast.Expr) string {
	saved := g.sanitizers
	g.sanitizers = map[string]bool{}
	defer func() { g.sanitizers = saved }()
	return g.taintedExpr(e)
}

// ---------------------------------------------------------------------------
// The module arm
// ---------------------------------------------------------------------------

// TestS503_NoLogSiteEmitsAnEndpointURLWithoutRedactURL is the #503 invariant
// over the real module: no production log site renders a URL-bearing endpoint
// field, or anything derived from one, without redact.URL.
//
// Mutation evidence (PR #503): dropping redact.URL from ANY of the 14 fixed
// sites makes this RED, and so does the hoisted form
// `u := call.Endpoint.ServerURL; slog.String("host", u)`, which the syntactic
// #453 scan passes.
func TestS503_NoLogSiteEmitsAnEndpointURLWithoutRedactURL(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	pkgs := loadPackages(t, filepath.Join("..", ".."))
	if len(pkgs) < 20 {
		t.Fatalf("NON-VACUITY: only %d packages loaded", len(pkgs))
	}
	hits, sanitised := endpointURLScan(pkgs, urlSanitizers)
	for _, h := range hits {
		t.Errorf("#503 endpoint URL logged unredacted: %s — wrap it in redact.URL", h)
	}
	// A scan that stopped recognising the field — a renamed field, a changed
	// selection kind, a broken package load — would report ZERO hits and read
	// as perfect health. So the arm also requires it to still SEE the sites
	// that ARE correctly redacted, and the floor is the exact count rather
	// than a token non-zero: deleting a redaction is then caught here even if
	// the hit list were somehow empty.
	//
	// 15 = the 14 #503 sites (resolve.go x13 + endpoints_tls_233.go x1) plus
	// the pre-existing endpointLogAttr site (internal/handlers/endpoint_log.go,
	// via the registered logServerURL helper — #487/#502).
	//
	// If a log site is legitimately REMOVED, this number must come down with
	// it, deliberately and in the same commit. That is the intended cost.
	const wantSanitised = 15
	if sanitised < wantSanitised {
		t.Errorf("NON-VACUITY: the scan recognised only %d sanitised endpoint-URL log sites, want at least %d "+
			"(14 #503 sites + endpointLogAttr). A lower number means either a redaction was deleted or this "+
			"guard stopped seeing the field at all; both are failures, and neither may pass as an empty hit list",
			sanitised, wantSanitised)
	}
}

// TestS503_RegisteredHelpersReturnOnlySanitisedURLs verifies every module
// helper in urlSanitizers actually returns only sanitiser output. Registering a
// helper is how a raw URL could otherwise be smuggled through the set, so the
// registration is checked rather than trusted — the same discipline
// checkHelpers applies to the #453 identity helpers.
func TestS503_RegisteredHelpersReturnOnlySanitisedURLs(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	if len(urlHelperSanitizers) == 0 {
		t.Skip("no module helpers registered")
	}
	pkgs := loadPackages(t, filepath.Join("..", ".."))
	g := &urlGuard{sanitizers: urlSanitizers, taint: map[types.Object]string{}, fnTaint: map[*types.Func]map[int]string{}}

	seen := map[string]bool{}
	for _, p := range pkgs {
		g.pkg = p
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Recv != nil || fd.Body == nil {
					continue
				}
				key := p.PkgPath + "." + fd.Name.Name
				if !urlHelperSanitizers[key] {
					continue
				}
				seen[key] = true
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					ret, ok := n.(*ast.ReturnStmt)
					if !ok {
						return true
					}
					for _, res := range ret.Results {
						call, isCall := res.(*ast.CallExpr)
						if !isCall || !g.isSanitizerCall(call) {
							pos := p.Fset.Position(res.Pos())
							t.Errorf("#503: registered helper %s returns %s at %s:%d, which is not a sanitiser call — "+
								"either make it one or drop the helper from urlSanitizers",
								key, types.ExprString(res), filepath.Base(pos.Filename), pos.Line)
						}
					}
					return true
				})
			}
		}
	}
	for key := range urlHelperSanitizers {
		if !seen[key] {
			t.Errorf("NON-VACUITY: registered helper %s was not found in the module — a stale entry in "+
				"urlSanitizers silently widens the clean set", key)
		}
	}
}

// ---------------------------------------------------------------------------
// The guard's own non-vacuity arm, over a throwaway module
// ---------------------------------------------------------------------------

// TestS503_GuardDetectsEveryBypassShape is the arm on the arm. Each bypass the
// issue names — above all the HOIST that defeats a spelling match — must be
// reported, and each clean shape must NOT be, so the guard is coverage in both
// directions rather than only the strict one.
func TestS503_GuardDetectsEveryBypassShape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	for _, sub := range []string{"", "redact", "endpoints", "hoist"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(rel, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("go.mod", "module fixture\n\ngo 1.22\n")
	write("endpoints/endpoints.go", `package endpoints

type Endpoint struct {
	ServerURL string
	ProxyURL  string
	Token     string
}
`)
	// A helper in ANOTHER package that returns the field. This is the arm for
	// cross-package fnTaint, which depends on *types.Func pointer identity
	// holding across packages in one packages.Load — an assumption worth
	// testing rather than believing.
	write("hoist/hoist.go", `package hoist

import "fixture/endpoints"

// Host hoists the field across a PACKAGE boundary.
func Host(ep *endpoints.Endpoint) string { return ep.ServerURL }
`)
	// The fixture's sanitiser is a DIFFERENT package from the real one, which
	// is why the sanitiser set is a parameter: the guard matches the function
	// OBJECT, so the fixture proves the mechanism without touching the module.
	write("redact/redact.go", `package redact

// URL is the fixture stand-in for the real redact.URL.
func URL(raw string) string { return "clean" }

// ErrorText is NOT a URL sanitiser — it stands in for the real one, which
// rewrites identity patterns only.
func ErrorText(s string) string { return s }
`)
	src := `package fixture

import (
	"fmt"
	"log/slog"
	lg "log/slog"
	"net/url"
	"os"
	"strings"

	"fixture/endpoints"
	"fixture/hoist"
	ralias "fixture/redact"
)

type holder struct{ host string }

// hostOf hoists the field behind a FUNCTION boundary.
func hostOf(ep *endpoints.Endpoint) string { return ep.ServerURL }

// cleanHostOf is the same shape, sanitised.
func cleanHostOf(ep *endpoints.Endpoint) string { return ralias.URL(ep.ServerURL) }

func isSelf(ep *endpoints.Endpoint) bool { return ep.ServerURL == "self" }

func F(ep *endpoints.Endpoint, h *holder, l *slog.Logger) {
	slog.Error("direct", slog.String("host", ep.ServerURL)) // LEAK

	u := ep.ServerURL
	slog.Error("hoisted local", slog.String("host", u)) // LEAK

	v := u
	slog.Warn("re-assigned", slog.String("host", v)) // LEAK

	slog.Warn("concatenated", slog.String("host", "at "+u)) // LEAK

	slog.Info("sprintf", slog.String("host", fmt.Sprintf("%s/base", ep.ServerURL))) // LEAK

	slog.Info("trimmed", slog.String("host", strings.TrimSuffix(ep.ServerURL, "/"))) // LEAK

	pu, _ := url.Parse(ep.ServerURL)
	slog.Error("reparsed string", slog.String("host", pu.String())) // LEAK
	slog.Error("reparsed host", slog.String("host", pu.Host))       // LEAK

	h.host = ep.ServerURL
	slog.Error("via struct field", slog.String("host", h.host)) // LEAK

	slog.Error("via helper", slog.String("host", hostOf(ep))) // LEAK

	slog.Error("proxy field", slog.String("proxy", ep.ProxyURL)) // LEAK

	l.With("host", u).Error("kv pair") // LEAK

	slog.Error("any", slog.Any("host", u)) // LEAK

	fmt.Fprintf(os.Stderr, "host=%s\n", u) // LEAK

	slog.Error("not a sanitiser", slog.String("host", ralias.ErrorText(ep.ServerURL))) // LEAK

	// An ALIASED slog import: the sink's package is resolved by object, so the
	// spelling "lg" does not hide it.
	lg.Error("aliased slog import", lg.String("host", u)) // LEAK

	slog.Error("helper in another package", slog.String("host", hoist.Host(ep))) // LEAK

	// ---- clean shapes: these must NOT be flagged ----
	slog.Error("redacted direct", slog.String("host", ralias.URL(ep.ServerURL)))        // CLEAN
	slog.Error("redacted hoisted", slog.String("host", ralias.URL(u)))                  // CLEAN
	slog.Error("redacted helper", slog.String("host", cleanHostOf(ep)))                 // CLEAN
	slog.Error("a bool cannot carry it", slog.Bool("self", isSelf(ep)))                 // CLEAN
	slog.Error("a length cannot carry it", slog.Int("n", len(ep.ServerURL)))            // CLEAN
	slog.Error("a non-URL field", slog.String("tok", ep.Token))                         // CLEAN (#453's job)
	slog.Error("a literal", slog.String("host", "https://kubernetes.default.svc"))      // CLEAN
}
`
	write("f.go", src)

	sanitizers := map[string]bool{"fixture/redact.URL": true}
	hits, sanitised := endpointURLScan(loadPackages(t, dir), sanitizers)

	lines := strings.Split(src, "\n")
	want := map[int]bool{}
	for i, l := range lines {
		if strings.Contains(l, "// LEAK") {
			want[i+1] = true
		}
	}
	if len(want) != 17 {
		t.Fatalf("fixture has %d LEAK markers, want 17", len(want))
	}

	got := map[int]bool{}
	for _, h := range hits {
		var ln int
		if _, err := fmt.Sscanf(h[strings.Index(h, ".go:")+4:], "%d", &ln); err == nil {
			got[ln] = true
		}
		if !want[ln] {
			t.Errorf("CLEAN shape flagged — the guard is too strict: %s", h)
		}
	}
	for ln := range want {
		if !got[ln] {
			t.Errorf("BYPASS not detected at fixture line %d: %s", ln, strings.TrimSpace(lines[ln-1]))
		}
	}
	// The counter measures DIRECT sanitiser wraps — a sanitiser call whose own
	// argument is tainted. The fixture has two (`URL(ep.ServerURL)` and
	// `URL(u)`); `cleanHostOf(ep)` is clean because its BODY sanitises, which
	// makes it clean without being a wrap, so it is deliberately not counted.
	if sanitised != 2 {
		t.Errorf("NON-VACUITY: the fixture has 2 directly-wrapped sanitised sites, the scan counted %d", sanitised)
	}

	// The sanitiser set is what makes the clean shapes clean: with it emptied,
	// redact.URL("...") must itself become a hit. Without this, a guard whose
	// sanitiser matching had silently stopped working would still pass the
	// assertions above.
	emptied, _ := endpointURLScan(loadPackages(t, dir), nil)
	if len(emptied) <= len(hits) {
		t.Errorf("NON-VACUITY: emptying the sanitiser set must turn the redacted sites into hits; "+
			"got %d hits with it and %d without", len(hits), len(emptied))
	}
}

// TestS503_ErrorTextEmbeddingIsTheDOCUMENTEDGap pins the boundary propagatesURL
// draws, in the direction that FAILS: a URL that reaches a log site inside an
// error's TEXT is NOT reported by this guard.
//
// This is not a wish — it is the shape found in production while building the
// guard (external_fetch.go:80 → resolve.go:1333, see propagatesURL), and it is
// asserted here so nobody reads this file as covering it. If a later change
// teaches the guard to follow error text, this arm goes RED and must be
// deleted ON PURPOSE, with the issue it closes named. That is the point: the
// gap cannot be forgotten and cannot be silently claimed as closed. The issue
// that owns it is #523.
func TestS503_ErrorTextEmbeddingIsTheDOCUMENTEDGap(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	for _, sub := range []string{"", "redact", "endpoints"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(rel, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.22\n")
	write("endpoints/endpoints.go", "package endpoints\n\ntype Endpoint struct{ ServerURL string }\n")
	write("redact/redact.go", "package redact\n\nfunc URL(raw string) string { return \"clean\" }\n")
	src := `package fixture

import (
	"fmt"
	"log/slog"

	"fixture/endpoints"
)

// parseIt mimics external_fetch.go: the URL rides out inside an error.
func parseIt(ep *endpoints.Endpoint) error { return fmt.Errorf("parse %q: bad", ep.ServerURL) }

func F(ep *endpoints.Endpoint) {
	err := parseIt(ep)
	slog.Error("not reported by this guard", slog.Any("err", err))
	slog.Error("nor this one", slog.String("error", err.Error()))

	// The SAME field as a string IS reported — so this fixture also proves the
	// gap is specific to error text, not a hole in the whole mechanism.
	slog.Error("reported", slog.String("host", ep.ServerURL)) // LEAK
}
`
	write("f.go", src)

	hits, _ := endpointURLScan(loadPackages(t, dir), map[string]bool{"fixture/redact.URL": true})
	joined := strings.Join(hits, "\n")
	if len(hits) != 1 {
		t.Fatalf("the error-text shapes must NOT be reported and the plain field MUST be: want exactly 1 hit, got %d:\n%s",
			len(hits), joined)
	}
	if !strings.Contains(joined, "ep.ServerURL") {
		t.Errorf("the one hit must be the plain field read, not an error-text shape; got %s", joined)
	}
	if strings.Contains(joined, "err") {
		t.Errorf("DOCUMENTED GAP CLOSED UNINTENTIONALLY: this guard now follows a URL through error text (%s). "+
			"That is a real improvement — but update propagatesURL's comment, name the issue it closes, and "+
			"delete this arm deliberately rather than leaving the claim ambiguous", joined)
	}
}

// TestS503_SanitizerIsMatchedByObjectNotBySpelling pins the evasion #502's own
// structural guard was rewritten twice for: a dot-import and an alias must not
// change whether a call counts as the sanitiser, and a DIFFERENT function that
// merely happens to be named URL must not count as one.
func TestS503_SanitizerIsMatchedByObjectNotBySpelling(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	for _, sub := range []string{"", "redact", "endpoints", "impostor"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(rel, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.22\n")
	write("endpoints/endpoints.go", "package endpoints\n\ntype Endpoint struct{ ServerURL string }\n")
	write("redact/redact.go", "package redact\n\nfunc URL(raw string) string { return \"clean\" }\n")
	// A package of the same FUNCTION NAME that does nothing. A spelling match
	// on "redact.URL" or on "URL(" would accept it.
	write("impostor/impostor.go", "package impostor\n\nfunc URL(raw string) string { return raw }\n")
	src := `package fixture

import (
	"log/slog"

	"fixture/endpoints"
	. "fixture/redact"
	impostor "fixture/impostor"
)

func F(ep *endpoints.Endpoint) {
	slog.Error("dot-imported real sanitiser", slog.String("host", URL(ep.ServerURL)))    // CLEAN
	slog.Error("aliased impostor", slog.String("host", impostor.URL(ep.ServerURL)))      // LEAK
}
`
	write("f.go", src)

	hits, sanitised := endpointURLScan(loadPackages(t, dir), map[string]bool{"fixture/redact.URL": true})
	joined := strings.Join(hits, "\n")
	if len(hits) != 1 {
		t.Fatalf("want exactly 1 hit (the impostor), got %d:\n%s", len(hits), joined)
	}
	if !strings.Contains(joined, "impostor.URL") {
		t.Errorf("the impostor URL() must be the hit; got %s", joined)
	}
	if sanitised != 1 {
		t.Errorf("the DOT-IMPORTED real sanitiser must be recognised as one (sanitised=1); got %d", sanitised)
	}
}
