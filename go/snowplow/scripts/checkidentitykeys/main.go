// Command checkidentitykeys is the #449 identity-key completeness gate.
//
// THE INVARIANT: a cache or reuse key must fold EVERY identity input that
// determines the cached content — or the store must be declared identity-free
// and re-gated per requester at serve.
//
// THE DEFECT CLASS. It broke three times in two days on three code paths, each
// time because one mint site assembled the identity dimensions by hand and
// missed one:
//
//	#423 resolved cells keyed by the first-match binding only;
//	#432 the SeedResolveMemo keyed by (username, groups), not the RBAC class;
//	#435 the raFullList key without the RBAC sub-generation.
//
// #449 made the dimensions single-sourced (rbac.IdentityClassOf → the one
// writer ResolvedKeyInputs.SetIdentity; one identity-free class list,
// cache.identityFreeClasses). This gate keeps it that way. It parses every
// non-test .go file under <root>/internal directly (go/parser — build
// constraints are ignored, so every build tag is covered) and enforces:
//
//	R0 the sources of truth are intact: ComputeKey's identity branch tests
//	   IsIdentityFreeClass and folds the dimensions it reads; SetIdentity writes
//	   every one of them; IdentityClassOf derives the binding set via
//	   SubjectBindingSetDigest and the sub-gen via
//	   RBACSubGenForSubject(_, WithAuthenticatedGroup(_)).
//	R1 every ComputeKey call on identity-BOUND inputs goes through SetIdentity
//	   (in the hashing function or a builder it calls). Inputs carried in as a
//	   parameter are a re-hash, not a mint. Identity-FREE inputs (a class in
//	   identityFreeClasses) must not call SetIdentity.
//	R2 nothing but SetIdentity writes an identity dimension of a
//	   ResolvedKeyInputs — no keyed literal element, no field assignment or
//	   ++/--; no IdentityClass field is edited outside packages cache and rbac
//	   (a class reaches SetIdentity exactly as IdentityClassOf returned it);
//	   and nothing takes the address of an identity field of either type.
//	R3 one derivation: only rbac.IdentityClassOf reads the class sources
//	   (SubjectBindingSetDigest, RBACSubGenForSubject), and every SetIdentity
//	   call takes its class from it (directly, through a local, or through a
//	   parameter whose every caller passes one). Test seams (*ForTest) are
//	   exempt and must have no production caller.
//	R4 every registered identity-bound reuse key (the SeedResolveMemo key) folds
//	   an rbac.IdentityClassOf-derived argument.
//	R5 every map / sync.Map store whose name marks it as a memo, cache, store,
//	   index, shard, registry or set is declared in storeRegistry with how it is
//	   isolated. A new store fails until its author states that.
//
// The dimension set is READ from ComputeKey, so a new dimension added there is
// required everywhere at once. What no AST rule can prove — that the class
// itself is complete (#423 folded every dimension the class had) and rotates
// on every verdict change — is pinned by the dispatchers property test
// (identity_class_property_test.go).
//
// USAGE:
//
//	checkidentitykeys <module root>   (the directory holding internal/)
//
// Exits 0 when clean; exits 1 and prints rule file:line: reason otherwise.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	keyInputsType = "ResolvedKeyInputs"
	classType     = "IdentityClass"
	computeKey    = "ComputeKey"
	setIdentity   = "SetIdentity"
	classOf       = "IdentityClassOf"

	// #180 Phase B — the SECOND dimension category. An identity dimension is a
	// function of the REQUESTER ALONE, which is why rbac.IdentityClassOf takes
	// only a jwtutil.UserInfo and why R3 can demand one derivation. A UAF-scope
	// digest is not: it is a function of (requester, access domain), and the
	// domain comes from the CR/apiRef chain. It therefore cannot be produced by
	// IdentityClassOf, and folding it in the identity branch would auto-register
	// it as an identity dimension (r0 discovers dims from whatever that branch
	// reads) and then fail R0 because SetIdentity cannot write it.
	//
	// So it gets its own branch, its own single writer and its own single
	// derivation, and S0/S2/S3 below are the exact analogues of R0/R2/R3. The
	// guard's property is unchanged — every key ingredient has exactly one
	// source — it is now enforced PER CATEGORY instead of assuming one category
	// exists.
	scopeFreePredicate = "IsScopeKeyedClass"
	scopeType          = "ScopeClass"
	setScope           = "SetScope"
	scopeOf            = "ScopeClassOf"
)

// scopeSources are the scope dimensions' sources of truth. Only ScopeClassOf may
// call them — the same single-derivation rule classSources gets, for the same
// reason: a second reader is a second derivation that can drift.
var scopeSources = []string{"ComputeProjectionDigest"}

// classSources are the class dimensions' sources of truth. Only
// IdentityClassOf may call them.
var classSources = []string{"SubjectBindingSetDigest", "RBACSubGenForSubject"}

// identityBoundReuseKeys are reuse-key methods whose stored content is resolved
// under one identity: the key must fold that identity's class. recvFrom is the
// call that yields the store on a context.
var identityBoundReuseKeys = []struct{ method, recvFrom string }{
	{method: "Key", recvFrom: "SeedResolveMemoFromContext"},
}

// storeRegistry declares every content-reuse / identity-relevant store and how
// it is isolated. Categories:
//
//	class-keyed      the key folds the full identity class (R1-R4 enforce it)
//	free+regated     identity-free content, re-gated per requester at serve
//	snapshot-scoped  memo of an RBAC derivation keyed by exact identity, swapped per snapshot
//	no-content       keys, counters, versions, membership — never a served body
//	invocation-local lives for one call and is keyed by exact identity
//	credential-keyed caches a client keyed by the exact credential it carries
var storeRegistry = map[string]string{
	"cache.ResolvedCacheStore.index":                  "class-keyed (identity-free classes re-gated: widgetContent→gateWidgetEnvelope, apistage→gateContentEnvelope)",
	"cache.ResolvedCacheStore.tombstones":             "no-content",
	"cache.SeedResolveMemo.m":                         "class-keyed (R4)",
	"cache.seedMemoValue.Body":                        "class-keyed (the value of SeedResolveMemo, R4)",
	"cache.sliceabilityMemo.verdicts":                 "no-content (verdict per raKey × shape; raKey class-keyed)",
	"cache.sliceabilityMemo.shapeNegative":            "no-content (identity-free structural verdict)",
	"cache.raLayerIndex.byRA":                         "no-content (content versions)",
	"cache.raLayerIndex.byWidget":                     "no-content (content versions)",
	"cache.BootSeededSet.m":                           "no-content",
	"cache.SeedDeclinedExternalSet.m":                 "no-content",
	"cache.bindingsByGVRIndex.byGVR":                  "no-content (identity-free binding index)",
	"cache.bindingsByGVRIndex.wildcard":               "no-content (identity-free binding index)",
	"cache.bindingsByGVRIndex.byRole":                 "no-content (identity-free binding index)",
	"cache.bindingsByGVRIndex.entries":                "no-content (identity-free binding index)",
	"cache.bindingsByGVRIndex.roleRefUnresolved":      "no-content (identity-free binding index)",
	"cache.bindingsByGVRIndex.navigated":              "no-content (identity-free binding index)",
	"cache.keySet.keys":                               "no-content (dep edges)",
	"cache.depSet.deps":                               "no-content (dep edges)",
	"cache.RBACShiftAccumulator.set":                  "no-content (subjects to reseed)",
	"cache.RotatedSubjectSet.set":                     "no-content (subjects to reseed)",
	"cache.refreshBroadcaster.subs":                   "no-content (signal-only SSE)",
	"cache.refreshBroadcaster.keySubs":                "no-content (signal-only SSE; keys derived under each connection's identity)",
	"cache.refreshBroadcaster.windows":                "no-content (per-key coalesce timers; signal-only, #484)",
	"cache.servedGroupsSet":                           "no-content (discovery groups)",
	"cache.ResourceWatcher.eagerSet":                  "no-content",
	"cache.pluralsStore":                              "free+regated (discovery, identity-free)",
	"cache.pluralsKindReverseStore":                   "free+regated (discovery, identity-free)",
	"cache.identityFreeClasses":                       "no-content (the identity-free class list)",
	"cache.secretsCacheRetainedDataKeys":              "no-content (#510: Secret data-key NAMES only — the AUTHN informer transform's allow-list; no value, no identity)",
	"rbac.bindingSetShard.m":                          "snapshot-scoped (exact username + groups)",
	"rbac.snapshotAuthzShard.m":                       "snapshot-scoped (username + groups FNV64, same username only)",
	"rbac.requesterProfileShard.m":                    "snapshot-scoped (username + groups FNV64, same username only)",
	"schema.crdSchemaMemo":                            "free+regated (CRD schema, identity-free)",
	"dispatchers.bootConvergenceState.priorFailedSet": "no-content",
	"cache.routeScopeRegistry":                        "no-content (route → scope name)",
	"cache.storeDivergentLostUpdate":                  "no-content (counters)",
	"cache.storeDivergentLostDelete":                  "no-content (counters)",
	"cache.storeDivergentLostAdd":                     "no-content (counters)",
	"cache.storeDivergentUIDMismatch":                 "no-content (counters)",
	"cache.storeVerifySkipByReason":                   "no-content (counters)",
	"api.internalClientCache":                         "credential-keyed (a client per exact *rest.Config pointer; its credential IS the key)",
	"api.discoveryClientCache":                        "credential-keyed (a client per exact *rest.Config pointer; its credential IS the key)",
}

var storeName = regexp.MustCompile(`(?i)(memo|cache|store|index|shard|registry|broadcaster)|(Set|^set|^subs)$`)

type finding struct {
	pos  token.Position
	rule string
	msg  string
}

type fnInfo struct {
	pkg       string
	name      string
	decl      *ast.FuncDecl
	recvType  string
	returnsKI bool
	callsSet  bool            // calls .SetIdentity(
	calls     map[string]bool // simple callee names
	kiVars    map[string]bool // locals / params typed ResolvedKeyInputs
	icVars    map[string]bool // locals / params typed IdentityClass
}

type gate struct {
	fset     *token.FileSet
	root     string
	files    []*ast.File
	funcs    []*fnInfo
	byName   map[string][]*fnInfo // free functions and methods by simple name
	dims     map[string]bool
	scope    map[string]bool // #180: the SECOND category — (requester, domain) dimensions
	free     map[string]bool
	out      []finding
	builders map[string]bool // functions returning ResolvedKeyInputs that (transitively) call SetIdentity
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: checkidentitykeys <module root>")
		os.Exit(2)
	}
	g, err := load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "checkidentitykeys:", err)
		os.Exit(2)
	}
	g.run()
	for _, f := range g.out {
		fmt.Printf("%s %s: %s\n", f.rule, g.rel(f.pos), f.msg)
	}
	if len(g.out) > 0 {
		fmt.Printf("checkidentitykeys: %d violation(s)\n", len(g.out))
		os.Exit(1)
	}
	fmt.Printf("checkidentitykeys: OK (identity dimensions %v; identity-free classes %v)\n", sorted(g.dims), sorted(g.free))
}

func load(root string) (*gate, error) {
	g := &gate{fset: token.NewFileSet(), root: root, byName: map[string][]*fnInfo{}, dims: map[string]bool{},
		scope: map[string]bool{}, free: map[string]bool{}, builders: map[string]bool{}}
	err := filepath.Walk(filepath.Join(root, "internal"), func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			if fi.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(g.fset, p, nil, 0)
		if perr != nil {
			return perr
		}
		g.files = append(g.files, f)
		return nil
	})
	return g, err
}

func (g *gate) add(pos token.Pos, rule, format string, args ...any) {
	g.out = append(g.out, finding{g.fset.Position(pos), rule, fmt.Sprintf(format, args...)})
}

func (g *gate) rel(p token.Position) string {
	if !p.IsValid() {
		return "<tree>"
	}
	if r, err := filepath.Rel(g.root, p.Filename); err == nil {
		return fmt.Sprintf("%s:%d", r, p.Line)
	}
	return p.String()
}

func (g *gate) run() {
	for _, f := range g.files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				fi := g.collect(fd)
				fi.pkg = f.Name.Name
				g.funcs = append(g.funcs, fi)
				g.byName[fi.name] = append(g.byName[fi.name], fi)
			}
		}
	}
	g.r0()
	if len(g.dims) == 0 {
		return // R0 already reported; the rest would be noise
	}
	g.buildersFixedPoint()
	g.r1()
	g.r2()
	g.r3()
	g.r4()
	g.r5()
	// #180: the scope category is OPTIONAL. Before Phase B lands there is no
	// scope branch in ComputeKey, g.scope is empty, and these are no-ops — so
	// this change is inert on today's tree and cannot produce a finding until
	// the dimension actually exists.
	if len(g.scope) > 0 {
		g.s0()
		g.s2()
		g.s3()
	}
	sort.Slice(g.out, func(i, j int) bool {
		if g.out[i].rule != g.out[j].rule {
			return g.out[i].rule < g.out[j].rule
		}
		return g.rel(g.out[i].pos) < g.rel(g.out[j].pos)
	})
}

// ── R0: the sources of truth ────────────────────────────────────────────────

func (g *gate) r0() {
	var ck, si, ico *fnInfo
	for _, fi := range g.funcs {
		switch {
		case fi.name == computeKey && fi.decl.Recv == nil:
			ck = fi
		case fi.name == setIdentity && fi.recvType == keyInputsType:
			si = fi
		case fi.name == classOf && fi.decl.Recv == nil:
			ico = fi
		}
	}
	for _, f := range g.files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range gd.Specs {
				vs, ok := sp.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, n := range vs.Names {
					if n.Name != "identityFreeClasses" || i >= len(vs.Values) {
						continue
					}
					if cl, ok := vs.Values[i].(*ast.CompositeLit); ok {
						for _, e := range cl.Elts {
							if kv, ok := e.(*ast.KeyValueExpr); ok {
								g.free[identName(kv.Key)] = true
							}
						}
					}
				}
			}
		}
	}
	if len(g.free) == 0 {
		g.add(token.NoPos, "R0", "identityFreeClasses (the single identity-free class list) not found")
	}
	if ck == nil {
		g.add(token.NoPos, "R0", "ComputeKey not found")
		return
	}
	param := ck.decl.Type.Params.List[0].Names[0].Name
	ast.Inspect(ck.decl.Body, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || !mentions(is.Cond, "CacheEntryClass") {
			return true
		}
		// #180: the SCOPE branch also switches on CacheEntryClass, so it would
		// otherwise be read as a malformed identity branch — flagged for not
		// testing IsIdentityFreeClass, and its dimension collected into the
		// IDENTITY set, which is precisely the conflation this category exists to
		// avoid. The two branches must be disjoint, and the scope predicate is
		// what tells them apart.
		if findCall(is.Cond, scopeFreePredicate) != nil {
			return true
		}
		if findCall(is.Cond, "IsIdentityFreeClass") == nil {
			g.add(is.Pos(), "R0", "ComputeKey's identity branch must test IsIdentityFreeClass (the single identity-free class list), not a literal class")
		}
		ast.Inspect(is.Body, func(m ast.Node) bool {
			if se, ok := m.(*ast.SelectorExpr); ok && identName(se.X) == param {
				g.dims[se.Sel.Name] = true
			}
			return true
		})
		return false
	})
	// #180 — the SECOND branch, discovered the same way: whatever ComputeKey
	// reads inside a branch testing the scope predicate is a scope dimension.
	// Auto-discovery is deliberate and is the whole reason this works: a
	// dimension cannot be added to the key without the auditor noticing it.
	ast.Inspect(ck.decl.Body, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || findCall(is.Cond, scopeFreePredicate) == nil {
			return true
		}
		ast.Inspect(is.Body, func(m ast.Node) bool {
			if se, ok := m.(*ast.SelectorExpr); ok && identName(se.X) == param {
				g.scope[se.Sel.Name] = true
			}
			return true
		})
		return false
	})
	if len(g.dims) == 0 {
		g.add(ck.decl.Pos(), "R0", "could not read ComputeKey's identity dimensions (no identity branch over CacheEntryClass)")
		return
	}
	if si == nil {
		g.add(token.NoPos, "R0", "ResolvedKeyInputs.SetIdentity (the single identity writer) not found")
	} else {
		wrote := map[string]bool{}
		ast.Inspect(si.decl.Body, func(n ast.Node) bool {
			if as, ok := n.(*ast.AssignStmt); ok {
				for _, l := range as.Lhs {
					if se, ok := l.(*ast.SelectorExpr); ok {
						wrote[se.Sel.Name] = true
					}
				}
			}
			return true
		})
		for d := range g.dims {
			if !wrote[d] {
				g.add(si.decl.Pos(), "R0", "SetIdentity does not write identity dimension %s that ComputeKey folds", d)
			}
		}
	}
	if ico == nil {
		g.add(token.NoPos, "R0", "rbac.IdentityClassOf (the single class derivation) not found")
	} else {
		if findCall(ico.decl.Body, "SubjectBindingSetDigest") == nil {
			g.add(ico.decl.Pos(), "R0", "IdentityClassOf must derive the binding set via SubjectBindingSetDigest")
		}
		c := findCall(ico.decl.Body, "RBACSubGenForSubject")
		if c == nil || len(c.Args) < 2 || findCall(c.Args[1], "WithAuthenticatedGroup") == nil {
			g.add(ico.decl.Pos(), "R0", "IdentityClassOf must derive the sub-gen via RBACSubGenForSubject(_, WithAuthenticatedGroup(_)) (#424)")
		}
	}
}

// ── collection ──────────────────────────────────────────────────────────────

func (g *gate) collect(fd *ast.FuncDecl) *fnInfo {
	fi := &fnInfo{name: fd.Name.Name, decl: fd, calls: map[string]bool{}, kiVars: map[string]bool{}, icVars: map[string]bool{}}
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		fi.recvType = typeName(fd.Recv.List[0].Type)
		for _, n := range fd.Recv.List[0].Names {
			if fi.recvType == keyInputsType {
				fi.kiVars[n.Name] = true
			}
		}
	}
	for _, p := range fd.Type.Params.List {
		for _, n := range p.Names {
			switch typeName(p.Type) {
			case keyInputsType:
				fi.kiVars[n.Name] = true
			case classType:
				fi.icVars[n.Name] = true
			}
		}
	}
	if fd.Type.Results != nil {
		for _, r := range fd.Type.Results.List {
			if typeName(r.Type) == keyInputsType {
				fi.returnsKI = true
				for _, n := range r.Names {
					fi.kiVars[n.Name] = true
				}
			}
		}
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			name := calleeName(x)
			fi.calls[name] = true
			if name == setIdentity {
				fi.callsSet = true
			}
		case *ast.ValueSpec:
			if x.Type == nil {
				break
			}
			for _, n := range x.Names {
				switch typeName(x.Type) {
				case keyInputsType:
					fi.kiVars[n.Name] = true
				case classType:
					fi.icVars[n.Name] = true
				}
			}
		}
		return true
	})
	return fi
}

// buildersFixedPoint marks functions returning ResolvedKeyInputs that call
// SetIdentity themselves or through another such builder.
func (g *gate) buildersFixedPoint() {
	for changed := true; changed; {
		changed = false
		for _, fi := range g.funcs {
			if !fi.returnsKI || g.builders[fi.name] {
				continue
			}
			ok := fi.callsSet
			for c := range fi.calls {
				ok = ok || g.builders[c]
			}
			if ok {
				g.builders[fi.name] = true
				changed = true
			}
		}
	}
}

// ── R1: identity-bound hash sites go through SetIdentity ───────────────────

func (g *gate) r1() {
	for _, fi := range g.funcs {
		if fi.name == computeKey && fi.decl.Recv == nil {
			continue
		}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || calleeName(call) != computeKey || len(call.Args) != 1 {
				return true
			}
			origin, class := g.originOf(fi, call.Args[0])
			switch {
			case origin == "param":
				// re-hash of carried inputs: minted (and checked) where they were built
			case g.free[class]:
				if fi.callsSet || g.builders[origin] {
					g.add(call.Pos(), "R1", "%s hashes identity-free class %s but writes an identity (SetIdentity); identity-free content must never be keyed by a requester", fi.name, class)
				}
			default:
				if !(fi.callsSet || g.builders[origin] || anyCalled(fi, g.builders)) {
					g.add(call.Pos(), "R1", "%s hashes identity-bound inputs (class %s) that never went through SetIdentity — the key folds no identity class (the #423/#435 shape)", fi.name, orDash(class))
				}
			}
			return true
		})
	}
}

// originOf classifies ComputeKey's argument: "param" (carried inputs), the
// builder / function it came from, plus the class expression of its literal.
func (g *gate) originOf(fi *fnInfo, arg ast.Expr) (string, string) {
	if cl, ok := arg.(*ast.CompositeLit); ok {
		return fi.name, classOfLit(cl)
	}
	if c, ok := arg.(*ast.CallExpr); ok {
		return calleeName(c), g.literalClassOf(calleeName(c))
	}
	id, ok := arg.(*ast.Ident)
	if !ok {
		return fi.name, ""
	}
	for _, p := range fi.decl.Type.Params.List {
		for _, n := range p.Names {
			if n.Name == id.Name {
				return "param", ""
			}
		}
	}
	origin, class := fi.name, ""
	ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, l := range as.Lhs {
			if identName(l) != id.Name {
				continue
			}
			r := as.Rhs[0]
			if len(as.Rhs) == len(as.Lhs) {
				r = as.Rhs[i]
			}
			switch v := r.(type) {
			case *ast.CompositeLit:
				origin, class = fi.name, classOfLit(v)
			case *ast.CallExpr:
				origin, class = calleeName(v), g.literalClassOf(calleeName(v))
			}
		}
		return true
	})
	return origin, class
}

func (g *gate) literalClassOf(fn string) string {
	for _, fi := range g.byName[fn] {
		var class string
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			if cl, ok := n.(*ast.CompositeLit); ok && typeName(cl.Type) == keyInputsType {
				if c := classOfLit(cl); c != "" {
					class = c
				}
			}
			return true
		})
		if class != "" {
			return class
		}
	}
	return ""
}

// ── R2: SetIdentity is the only writer ──────────────────────────────────────

func (g *gate) r2() {
	for _, fi := range g.funcs {
		if fi.name == setIdentity && fi.recvType == keyInputsType {
			continue
		}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CompositeLit:
				if typeName(x.Type) != keyInputsType {
					return true
				}
				for _, e := range x.Elts {
					if kv, ok := e.(*ast.KeyValueExpr); ok && g.dims[identName(kv.Key)] {
						g.add(kv.Pos(), "R2", "%s sets identity dimension %s in a ResolvedKeyInputs literal; only SetIdentity may write it", fi.name, identName(kv.Key))
					}
				}
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					g.r2Write(fi, l, x.Pos())
				}
			case *ast.IncDecStmt:
				g.r2Write(fi, x.X, x.Pos())
			case *ast.UnaryExpr:
				// &x.RBACSubGen hands out a pointer that writes past every rule
				// above (`p := &in.RBACSubGen; *p = 0`). No production code needs
				// the address of an identity field, so taking it is the violation.
				se, ok := x.X.(*ast.SelectorExpr)
				if x.Op != token.AND || !ok || !g.dims[se.Sel.Name] {
					return true
				}
				if g.isKeyInputs(fi, se.X) || g.isIdentityClass(fi, se.X) {
					g.add(x.Pos(), "R2", "%s takes the address of identity dimension %s; a pointer to it is an unchecked writer", fi.name, se.Sel.Name)
				}
			}
			return true
		})
	}
}

// r2Write flags a write to an identity dimension through lhs: of a
// ResolvedKeyInputs anywhere (only SetIdentity may write those), and of an
// IdentityClass outside the packages that own it (cache defines it,
// rbac.IdentityClassOf produces it). Editing a class between IdentityClassOf
// and SetIdentity — `c := rbac.IdentityClassOf(ui); c.RBACSubGen = 0` — is
// the #435 shape one step removed.
func (g *gate) r2Write(fi *fnInfo, lhs ast.Expr, pos token.Pos) {
	se, ok := lhs.(*ast.SelectorExpr)
	if !ok || !g.dims[se.Sel.Name] {
		return
	}
	switch {
	case g.isKeyInputs(fi, se.X):
		g.add(pos, "R2", "%s assigns identity dimension %s directly; only SetIdentity may write it", fi.name, se.Sel.Name)
	case fi.pkg != "cache" && fi.pkg != "rbac" && g.isIdentityClass(fi, se.X):
		g.add(pos, "R2", "%s edits field %s of an IdentityClass; a class must reach SetIdentity exactly as rbac.IdentityClassOf returned it", fi.name, se.Sel.Name)
	}
}

// isIdentityClass reports whether e is (syntactically known to be) an
// IdentityClass: a typed local / param, or a local assigned from
// IdentityClassOf, from a ResolvedKeyInputs Class() read, or from an
// IdentityClass literal.
func (g *gate) isIdentityClass(fi *fnInfo, e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.StarExpr:
		return g.isIdentityClass(fi, x.X)
	case *ast.ParenExpr:
		return g.isIdentityClass(fi, x.X)
	case *ast.Ident:
		if fi.icVars[x.Name] {
			return true
		}
		found := false
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || found {
				return !found
			}
			for i, l := range as.Lhs {
				if identName(l) != x.Name {
					continue
				}
				r := as.Rhs[0]
				if len(as.Rhs) == len(as.Lhs) {
					r = as.Rhs[i]
				}
				if u, ok := r.(*ast.UnaryExpr); ok {
					r = u.X
				}
				switch v := r.(type) {
				case *ast.CompositeLit:
					found = typeName(v.Type) == classType
				case *ast.CallExpr:
					found = calleeName(v) == classOf || calleeName(v) == "Class"
				}
			}
			return !found
		})
		return found
	}
	return false
}

// isKeyInputs reports whether e is (syntactically known to be) a
// ResolvedKeyInputs: a typed local / param / receiver, a local assigned from a
// ResolvedKeyInputs literal or from a function returning one, or the Inputs
// field of a ResolvedEntry.
func (g *gate) isKeyInputs(fi *fnInfo, e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.StarExpr:
		return g.isKeyInputs(fi, x.X)
	case *ast.ParenExpr:
		return g.isKeyInputs(fi, x.X)
	case *ast.SelectorExpr:
		return x.Sel.Name == "Inputs"
	case *ast.Ident:
		if fi.kiVars[x.Name] {
			return true
		}
		found := false
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || found {
				return !found
			}
			for i, l := range as.Lhs {
				if identName(l) != x.Name {
					continue
				}
				r := as.Rhs[0]
				if len(as.Rhs) == len(as.Lhs) {
					r = as.Rhs[i]
				}
				if u, ok := r.(*ast.UnaryExpr); ok {
					r = u.X
				}
				switch v := r.(type) {
				case *ast.CompositeLit:
					found = typeName(v.Type) == keyInputsType
				case *ast.CallExpr:
					for _, f := range g.byName[calleeName(v)] {
						found = found || f.returnsKI
					}
				}
			}
			return !found
		})
		return found
	}
	return false
}

// ── R3: SetIdentity takes its class from IdentityClassOf ────────────────────

func (g *gate) r3() {
	// The class sources are read ONLY by IdentityClassOf: a second reader is a
	// second derivation that can drift (#435 was one; #262's learned-class
	// signature was another).
	for _, fi := range g.funcs {
		if fi.name == classOf || strings.HasSuffix(fi.name, "ForTest") {
			continue
		}
		for _, src := range classSources {
			if c := findCall(fi.decl.Body, src); c != nil {
				g.add(c.Pos(), "R3", "%s derives an identity class itself (%s); only rbac.IdentityClassOf may — call it instead", fi.name, src)
			}
		}
	}
	forwarded := map[string]int{} // function → index of the class parameter it forwards
	for _, fi := range g.funcs {
		if strings.HasSuffix(fi.name, "ForTest") {
			continue
		}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || calleeName(call) != setIdentity || len(call.Args) != 2 {
				return true
			}
			arg := call.Args[1]
			if g.fromClassOf(fi, arg) {
				return true
			}
			if idx := paramIndex(fi.decl, identName(arg)); idx >= 0 {
				forwarded[fi.name] = idx
				return true
			}
			g.add(call.Pos(), "R3", "%s calls SetIdentity with a class not derived from rbac.IdentityClassOf", fi.name)
			return true
		})
	}
	for _, fi := range g.funcs {
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := calleeName(call)
			if idx, ok := forwarded[name]; ok && !strings.HasSuffix(fi.name, "ForTest") {
				if idx >= len(call.Args) || !g.fromClassOf(fi, call.Args[idx]) {
					g.add(call.Pos(), "R3", "%s passes %s a class not derived from rbac.IdentityClassOf", fi.name, name)
				}
			}
			if strings.HasSuffix(name, "ForTest") && !strings.HasSuffix(fi.name, "ForTest") {
				for _, callee := range g.byName[name] {
					if callee.callsSet || g.builders[name] {
						g.add(call.Pos(), "R3", "%s (production) calls the identity test seam %s", fi.name, name)
					}
				}
			}
			return true
		})
	}
}

// fromClassOf: e is an IdentityClassOf call, or a local assigned from one.
func (g *gate) fromClassOf(fi *fnInfo, e ast.Expr) bool {
	if c, ok := e.(*ast.CallExpr); ok {
		return calleeName(c) == classOf
	}
	name := identName(e)
	if name == "" {
		return false
	}
	found := false
	ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return !found
		}
		for i, l := range as.Lhs {
			if identName(l) == name && i < len(as.Rhs) {
				if c, ok := as.Rhs[i].(*ast.CallExpr); ok && calleeName(c) == classOf {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// ── R4: reuse keys fold the class ───────────────────────────────────────────

func (g *gate) r4() {
	for _, fi := range g.funcs {
		recvs := map[string]bool{}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, r := range as.Rhs {
				for _, rk := range identityBoundReuseKeys {
					if findCall(r, rk.recvFrom) != nil && i < len(as.Lhs) {
						recvs[identName(as.Lhs[i])] = true
					}
				}
			}
			return true
		})
		if len(recvs) == 0 {
			continue
		}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !recvs[identName(sel.X)] {
				return true
			}
			for _, rk := range identityBoundReuseKeys {
				if sel.Sel.Name != rk.method {
					continue
				}
				folds := false
				for _, a := range call.Args {
					folds = folds || g.derives(a, classOf, 2)
				}
				if !folds {
					g.add(call.Pos(), "R4", "%s: reuse key %s.%s does not fold an rbac.IdentityClassOf class — it would serve a body across RBAC classes (the #432 shape)", fi.name, identName(sel.X), rk.method)
				}
			}
			return true
		})
	}
}

// derives: e calls name, or calls a function of the tree that does (bounded).
func (g *gate) derives(e ast.Node, name string, depth int) bool {
	if findCall(e, name) != nil {
		return true
	}
	if depth == 0 {
		return false
	}
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			for _, fi := range g.byName[calleeName(c)] {
				found = found || g.derives(fi.decl.Body, name, depth-1)
			}
		}
		return !found
	})
	return found
}

// ── R5: store registry ──────────────────────────────────────────────────────

func (g *gate) r5() {
	seen := map[string]bool{}
	check := func(name string, pos token.Pos) {
		if seen[name] {
			return
		}
		seen[name] = true
		if _, ok := storeRegistry[name]; !ok {
			g.add(pos, "R5", "store %s is not in the identity registry (scripts/checkidentitykeys storeRegistry) — declare how it is isolated: class-keyed | free+regated | snapshot-scoped | no-content | invocation-local | credential-keyed", name)
		}
	}
	for _, f := range g.files {
		pkg := f.Name.Name
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range gd.Specs {
				switch s := sp.(type) {
				case *ast.ValueSpec:
					isMap := s.Type != nil && isMapType(s.Type)
					for i, n := range s.Names {
						if !isMap && i < len(s.Values) {
							if cl, ok := s.Values[i].(*ast.CompositeLit); ok && cl.Type != nil && isMapType(cl.Type) {
								isMap = true
							}
						}
						if isMap && storeName.MatchString(n.Name) {
							check(pkg+"."+n.Name, n.Pos())
						}
					}
				case *ast.TypeSpec:
					st, ok := s.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, fld := range st.Fields.List {
						if !isMapType(fld.Type) {
							continue
						}
						for _, n := range fld.Names {
							if storeName.MatchString(s.Name.Name) || storeName.MatchString(n.Name) {
								check(pkg+"."+s.Name.Name+"."+n.Name, n.Pos())
							}
						}
					}
				}
			}
		}
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func anyCalled(fi *fnInfo, set map[string]bool) bool {
	for c := range fi.calls {
		if set[c] {
			return true
		}
	}
	return false
}

func paramIndex(fd *ast.FuncDecl, name string) int {
	if name == "" {
		return -1
	}
	i := 0
	for _, p := range fd.Type.Params.List {
		if len(p.Names) == 0 {
			i++
			continue
		}
		for _, n := range p.Names {
			if n.Name == name {
				return i
			}
			i++
		}
	}
	return -1
}

func isMapType(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.MapType:
		return true
	case *ast.SelectorExpr:
		return t.Sel.Name == "Map" // sync.Map
	case *ast.StarExpr:
		return isMapType(t.X)
	}
	return false
}

func classOfLit(cl *ast.CompositeLit) string {
	for _, e := range cl.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok && identName(kv.Key) == "CacheEntryClass" {
			switch v := kv.Value.(type) {
			case *ast.BasicLit:
				return v.Value
			default:
				return identName(v)
			}
		}
	}
	return ""
}

func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.StarExpr:
		return typeName(t.X)
	}
	return ""
}

// identName is the trailing name of an identifier or selector.
func identName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	}
	return ""
}

func calleeName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

func mentions(n ast.Node, s string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if id, ok := m.(*ast.Ident); ok && strings.Contains(id.Name, s) {
			found = true
		}
		return !found
	})
	return found
}

func findCall(n ast.Node, name string) *ast.CallExpr {
	var hit *ast.CallExpr
	ast.Inspect(n, func(m ast.Node) bool {
		if c, ok := m.(*ast.CallExpr); ok && hit == nil && calleeName(c) == name {
			hit = c
		}
		return hit == nil
	})
	return hit
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// ─────────────────────────────────────────────────────────────────────────
// #180 Phase B — the SCOPE dimension category.
//
// S0/S2/S3 are the exact analogues of R0/R2/R3 for a dimension that is a
// function of (requester, access domain) rather than of the requester alone.
// There is deliberately no S1 or S4: R1 already proves every identity-bound
// ComputeKey call goes through SetIdentity, and a scope dimension rides the same
// ResolvedKeyInputs through the same mint sites, so R1's coverage is unchanged.
// R4's reuse keys do not carry a scope.
//
// WHY A SECOND CATEGORY RATHER THAN A WIDER FIRST ONE. rbac.IdentityClassOf
// takes a jwtutil.UserInfo and nothing else, and three of its five production
// callers construct that UserInfo from a bare username + groups
// (learned_identity_seed, apiref/resolve, identity_class_guard) — they have no
// access domain in hand at all. Widening its signature would force those callers
// to supply something they structurally do not have, which is where the next
// instance of the #423/#432/#435 bug would come from: in the plumbing of the
// guard meant to prevent it.
// ─────────────────────────────────────────────────────────────────────────

// s0 asserts the scope category's sources of truth are intact, mirroring r0:
// SetScope writes every scope dimension ComputeKey folds, and ScopeClassOf
// derives them from the declared sources.
func (g *gate) s0() {
	var ss, sco *fnInfo
	for _, fi := range g.funcs {
		switch {
		case fi.name == setScope && fi.recvType == keyInputsType:
			ss = fi
		case fi.name == scopeOf:
			sco = fi
		}
	}

	if ss == nil {
		g.add(token.NoPos, "S0", "ComputeKey folds scope dimensions %v but ResolvedKeyInputs.%s (the single scope writer) does not exist", sorted(g.scope), setScope)
		return
	}
	wrote := map[string]bool{}
	ast.Inspect(ss.decl.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok {
			for _, l := range as.Lhs {
				if se, ok := l.(*ast.SelectorExpr); ok {
					wrote[se.Sel.Name] = true
				}
			}
		}
		return true
	})
	for _, d := range sorted(g.scope) {
		if !wrote[d] {
			g.add(ss.decl.Pos(), "S0", "%s does not write scope dimension %s that ComputeKey folds", setScope, d)
		}
	}

	if sco == nil {
		g.add(token.NoPos, "S0", "%s (the single scope derivation) not found", scopeOf)
		return
	}
	// The derivation must take BOTH inputs. A one-argument ScopeClassOf is the
	// shape that cannot see the access domain, and it is exactly the mistake
	// this category exists to make impossible — so it is checked, not assumed.
	if n := countParams(sco.decl); n < 2 {
		g.add(sco.decl.Pos(), "S0", "%s takes %d parameter(s); a scope is a function of (requester, access domain) and a derivation that cannot see the domain can only fabricate one", scopeOf, n)
	}
	for _, src := range scopeSources {
		if findCall(sco.decl.Body, src) == nil {
			g.add(sco.decl.Pos(), "S0", "%s must derive the scope via %s", scopeOf, src)
		}
	}
}

// s2 is r2 for the scope category: nothing but SetScope writes a scope
// dimension, and nothing outside the owning packages edits a ScopeClass between
// its derivation and that write.
func (g *gate) s2() {
	for _, fi := range g.funcs {
		if fi.name == setScope || strings.HasSuffix(fi.name, "ForTest") {
			continue
		}
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					g.s2Write(fi, l, x.Pos())
				}
			case *ast.CompositeLit:
				// A scope dimension set in a ResolvedKeyInputs literal bypasses
				// the writer exactly as an identity dimension would.
				if typeName(x.Type) == keyInputsType {
					for _, e := range x.Elts {
						if kv, ok := e.(*ast.KeyValueExpr); ok && g.scope[identName(kv.Key)] {
							g.add(kv.Pos(), "S2", "%s sets scope dimension %s in a %s literal; only %s may write it", fi.name, identName(kv.Key), keyInputsType, setScope)
						}
					}
				}
			case *ast.UnaryExpr:
				// &in.UAFScopeDigest is an unchecked writer, same as for identity.
				if x.Op == token.AND {
					if se, ok := x.X.(*ast.SelectorExpr); ok && g.scope[se.Sel.Name] && g.isKeyInputs(fi, se.X) {
						g.add(x.Pos(), "S2", "%s takes the address of scope dimension %s; a pointer to it is an unchecked writer", fi.name, se.Sel.Name)
					}
				}
			}
			return true
		})
	}
}

func (g *gate) s2Write(fi *fnInfo, lhs ast.Expr, pos token.Pos) {
	se, ok := lhs.(*ast.SelectorExpr)
	if !ok || !g.scope[se.Sel.Name] {
		return
	}
	if g.isKeyInputs(fi, se.X) {
		g.add(pos, "S2", "%s assigns scope dimension %s directly; only %s may write it", fi.name, se.Sel.Name, setScope)
	}
}

// s3 is r3 for the scope category: the scope sources are read ONLY by
// ScopeClassOf. A second reader is a second derivation that can drift from the
// first — the failure mode that produced #435 on the identity side.
func (g *gate) s3() {
	for _, fi := range g.funcs {
		if fi.name == scopeOf || strings.HasSuffix(fi.name, "ForTest") {
			continue
		}
		for _, src := range scopeSources {
			if c := findCall(fi.decl.Body, src); c != nil {
				g.add(c.Pos(), "S3", "%s derives a scope itself (%s); only %s may — call it instead", fi.name, src, scopeOf)
			}
		}
	}
}

// countParams counts a declaration's parameters, flattening grouped names
// (`func f(a, b string)` is two).
func countParams(fd *ast.FuncDecl) int {
	if fd.Type.Params == nil {
		return 0
	}
	n := 0
	for _, f := range fd.Type.Params.List {
		if len(f.Names) == 0 {
			n++ // unnamed parameter
			continue
		}
		n += len(f.Names)
	}
	return n
}
