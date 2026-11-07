package redact

// log_guard_test.go is the structural guard of #453 and #1192. No slog site in
// the module may emit an identity, in clear or as a label.
//
// The guard parses every non-test .go file of the module directly, so build
// constraints are not applied and every tag (unit, integration, the debug
// builds) is covered. For each slog call it checks:
//
//   - an attribute (slog.String/Any/... or a key/value pair of a log method or
//     a With) or a slog.Group whose KEY is an identity key (user, username,
//     groups, identity, cohort, rep, subject, cn, ...), whatever its value;
//   - any attribute VALUE, and the message, whose expression calls an identity
//     labeller (redact.User, cache.LearnedClassLabel, ...): a label is the
//     identity under another name once a traceId joins its line to the audit
//     record;
//   - any attribute VALUE, and the message, whose expression reads an identity
//     source (x.Username, x.Groups, x.CommonName, x.Subjects, or a local named
//     username, groups, cohort, ui, ...).
//
// It also refuses a call to plumbing's use.Logger, which attaches the caller's
// name and groups to the request logger from a file this scan cannot see.
//
// The API-group "group" attribute (gvr.Group, opts.Group, a discovery group)
// is not an identity, so "group" is deliberately not an identity key and .Group
// is not an identity selector. ".Groups" (plural) is the identity group set.
//
// TYPED HALF (TestS453_NoLoggedValueTypeCarriesIdentityOrCredential, adopted
// from reviewer-424's #481 probe). A syntactic scan cannot see a struct logged
// whole: slog.Any("endpoint", ep) renders every exported field of an
// endpoints.Endpoint (Username, Token, Password, AwsSecretKey) through the JSON
// handler, and the text handler's %+v renders the json:"-" ClientKeyData too.
// The typed scan loads the module with go/types and flags every logged value
// whose static type reaches an identity or credential field through fields,
// pointers, slices, arrays or maps. A type that renders itself (slog.LogValuer,
// error) is trusted to its own method.
//
// An err VALUE whose text came from the apiserver or plumbing ('User "x" cannot
// list ...') is not this guard's: ScrubLogAttr takes it out at the handler.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// identityKeys are attribute keys that name an identity. Nothing may be logged
// under them, redacted or not. Any key starting with "user" counts too
// (isIdentityKey).
var identityKeys = map[string]bool{
	"groups": true, "identity": true, "cohort": true, "rep": true,
	"representative": true, "requester": true, "subject": true,
	"subjects": true, "cn": true, "who": true, "principal": true,
	"member": true, "members": true, "secret_name": true,
}

func isIdentityKey(key string) bool {
	return identityKeys[key] || strings.HasPrefix(strings.ToLower(key), "user")
}

// identityLabellers render one identity as a stable label.
var identityLabellers = map[string]bool{
	"User": true, "StaleSubject": true, "LearnedClassLabel": true,
}

// identitySelectors are field names whose value is an identity fact.
var identitySelectors = map[string]bool{
	"Username": true, "Groups": true, "CommonName": true, "Subjects": true,
}

// identityIdents are local names that, by this module's convention, hold an
// identity (a username, a group set, a UserInfo, a cohort label input).
var identityIdents = map[string]bool{
	"username": true, "userName": true, "groups": true, "cn": true,
	"ui": true, "userInfo": true, "user": true, "cohort": true,
	"refreshUser": true, "refreshGroups": true, "rankKey": true,
	"subject": true, "subjects": true, "secretName": true,
}

var logMethods = map[string]int{ // method -> index of the message argument (-1: none)
	"Debug": 0, "Info": 0, "Warn": 0, "Error": 0,
	"DebugContext": 1, "InfoContext": 1, "WarnContext": 1, "ErrorContext": 1,
	"Log": 2, "LogAttrs": 2, "With": -1,
}

// printSinks are non-slog writers of diagnostic text -> index of the first
// argument that is content (after the writer).
var printSinks = map[string]int{
	"fmt.Printf": 0, "fmt.Println": 0, "fmt.Print": 0,
	"fmt.Fprintf": 1, "fmt.Fprintln": 1, "fmt.Fprint": 1,
	"log.Printf": 0, "log.Println": 0, "log.Print": 0,
	"log.Fatalf": 0, "log.Panicf": 0,
}

var slogAttrCtors = map[string]bool{
	"String": true, "Any": true, "Attr": true, "Int": true, "Int64": true,
	"Uint64": true, "Bool": true, "Float64": true, "Duration": true,
	"Time": true, "Group": true,
}

type finding struct {
	pos  token.Position
	what string
}

func (f finding) String() string { return fmt.Sprintf("%s: %s", f.pos, f.what) }

type scanner struct {
	fset     *token.FileSet
	findings []finding
}

func (s *scanner) add(n ast.Node, format string, a ...any) {
	s.findings = append(s.findings, finding{pos: s.fset.Position(n.Pos()), what: fmt.Sprintf(format, a...)})
}

func calleeName(fun ast.Expr) (pkg, name string) {
	switch f := fun.(type) {
	case *ast.Ident:
		return "", f.Name
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			return id.Name, f.Sel.Name
		}
		return "", f.Sel.Name
	case *ast.IndexExpr:
		return calleeName(f.X)
	}
	return "", ""
}

// clean reports whether e is a value that cannot carry an identity: a literal
// or a call into this package other than an identity labeller.
func (s *scanner) clean(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return true
	case *ast.ParenExpr:
		return s.clean(v.X)
	case *ast.SelectorExpr:
		id, ok := v.X.(*ast.Ident)
		return ok && id.Name == "redact"
	case *ast.CallExpr:
		pkg, name := calleeName(v.Fun)
		if pkg == "redact" {
			return !identityLabellers[name]
		}
		// slog.StringValue / AnyValue of a clean value (an Attr literal's Value).
		if pkg == "slog" && slogValueCtors[name] && len(v.Args) == 1 {
			return s.clean(v.Args[0])
		}
	case *ast.BinaryExpr: // "x" + redact.URL(u)
		return v.Op == token.ADD && s.clean(v.X) && s.clean(v.Y)
	}
	return false
}

// labelledIdentity returns the identity labeller e calls ("" when none).
func labelledIdentity(e ast.Expr) string {
	hit := ""
	ast.Inspect(e, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok && hit == "" {
			if _, name := calleeName(c.Fun); identityLabellers[name] {
				hit = name
			}
		}
		return hit == ""
	})
	return hit
}

// identityExpr returns the first identity source e reads, outside any
// redacted sub-expression ("" when none).
func (s *scanner) identityExpr(e ast.Expr) string {
	hit := ""
	ast.Inspect(e, func(n ast.Node) bool {
		if hit != "" {
			return false
		}
		if x, ok := n.(ast.Expr); ok && s.clean(x) {
			return false
		}
		switch v := n.(type) {
		case *ast.SelectorExpr:
			if identitySelectors[v.Sel.Name] {
				hit = "." + v.Sel.Name
				return false
			}
		case *ast.Ident:
			if identityIdents[v.Name] {
				hit = v.Name
			}
		case *ast.FuncLit:
			return false
		}
		return true
	})
	return hit
}

func (s *scanner) checkAttr(n ast.Node, key string, val ast.Expr) {
	if isIdentityKey(key) {
		s.add(n, "identity key %q: no identity reaches a log line, redacted or not", key)
		return
	}
	if fn := labelledIdentity(val); fn != "" {
		s.add(n, "attribute %q is an identity label (%s)", key, fn)
		return
	}
	if s.clean(val) {
		return
	}
	if src := s.identityExpr(val); src != "" {
		s.add(n, "attribute %q reads identity source %s", key, src)
	}
}

func strLit(e ast.Expr) (string, bool) {
	bl, ok := e.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	return strings.Trim(bl.Value, "`\""), true
}

// checkKV walks a slog key/value argument list.
func (s *scanner) checkKV(args []ast.Expr) {
	for i := 0; i < len(args); i++ {
		if k, ok := strLit(args[i]); ok && i+1 < len(args) {
			s.checkAttr(args[i], k, args[i+1])
			i++
			continue
		}
		if c, ok := args[i].(*ast.CallExpr); ok {
			if pkg, name := calleeName(c.Fun); pkg == "slog" && slogAttrCtors[name] {
				continue // visited as its own call
			}
		}
		if fn := labelledIdentity(args[i]); fn != "" {
			s.add(args[i], "log argument is an identity label (%s)", fn)
		} else if src := s.identityExpr(args[i]); src != "" {
			s.add(args[i], "log argument reads identity source %s", src)
		}
	}
}

// slogValueCtors build a slog.Value from one argument (#490 review: the Value
// half of a hand-written slog.Attr{Key, Value} literal).
var slogValueCtors = map[string]bool{"StringValue": true, "AnyValue": true}

// checkAttrLiteral checks a hand-written slog.Attr{Key: k, Value: v} literal
// exactly like slog.Any(k, v).
func (s *scanner) checkAttrLiteral(cl *ast.CompositeLit) {
	if pkg, name := calleeName(cl.Type); pkg != "slog" || name != "Attr" {
		return
	}
	var keyE, valE ast.Expr
	for i, e := range cl.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if id, ok := kv.Key.(*ast.Ident); ok {
				switch id.Name {
				case "Key":
					keyE = kv.Value
				case "Value":
					valE = kv.Value
				}
			}
			continue
		}
		switch i { // positional
		case 0:
			keyE = e
		case 1:
			valE = e
		}
	}
	if valE == nil {
		return
	}
	key := ""
	if keyE != nil {
		key, _ = strLit(keyE)
	}
	s.checkAttr(cl, key, valE)
}

func (s *scanner) visitCall(c *ast.CallExpr) {
	pkg, name := calleeName(c.Fun)
	if pkg == "slog" && slogValueCtors[name] && len(c.Args) == 1 {
		if fn := labelledIdentity(c.Args[0]); fn != "" {
			s.add(c, "slog.%s is an identity label (%s)", name, fn)
		} else if src := s.identityExpr(c.Args[0]); src != "" {
			s.add(c, "slog.%s reads identity source %s", name, src)
		}
		return
	}
	if pkg == "use" && name == "Logger" {
		s.add(c, "use.Logger attaches the caller's name and groups to the request logger; mount middleware.RequestLogger")
		return
	}
	if pkg == "slog" && slogAttrCtors[name] {
		if len(c.Args) < 2 {
			return
		}
		key, ok := strLit(c.Args[0])
		if !ok {
			key = ""
		}
		if name == "Group" {
			if isIdentityKey(key) {
				s.add(c, "identity group %q: no identity reaches a log line, redacted or not", key)
			}
			s.checkKV(c.Args[1:])
			return
		}
		s.checkAttr(c, key, c.Args[1])
		return
	}
	// Print sinks: fmt.Print*/Fprint* (a stderr diagnostic lane) and the std
	// log package. Every argument after the writer must be identity-free.
	if first, ok := printSinks[pkg+"."+name]; ok {
		for _, a := range c.Args[min(first, len(c.Args)):] {
			if fn := labelledIdentity(a); fn != "" {
				s.add(a, "%s.%s argument is an identity label (%s)", pkg, name, fn)
			} else if src := s.identityExpr(a); src != "" {
				s.add(a, "%s.%s argument reads identity source %s", pkg, name, src)
			}
		}
		return
	}
	msgIdx, isLog := logMethods[name]
	if !isLog {
		return
	}
	if _, isSel := c.Fun.(*ast.SelectorExpr); !isSel {
		return
	}
	if msgIdx >= 0 {
		if len(c.Args) <= msgIdx {
			return
		}
		if fn := labelledIdentity(c.Args[msgIdx]); fn != "" {
			s.add(c, "log message is an identity label (%s)", fn)
		} else if src := s.identityExpr(c.Args[msgIdx]); src != "" {
			s.add(c, "log message reads identity source %s", src)
		}
		s.checkKV(c.Args[msgIdx+1:])
		return
	}
	s.checkKV(c.Args)
}

func (s *scanner) scanFile(f *ast.File) {
	for _, d := range f.Decls {
		ast.Inspect(d, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				s.visitCall(c)
			}
			if cl, ok := n.(*ast.CompositeLit); ok {
				s.checkAttrLiteral(cl)
			}
			return true
		})
	}
}

func scanSource(t *testing.T, src string) []finding {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	s := &scanner{fset: fset}
	s.scanFile(f)
	return s.findings
}

var skipDirs = map[string]bool{"testdata": true, "vendor": true, ".git": true, "scripts": true, "e2e": true, "hack": true}

func TestS453_NoLogSiteEmitsAnIdentityInClear(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	s := &scanner{fset: fset}
	files := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && (skipDirs[d.Name()] || strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return perr
		}
		files++
		s.scanFile(f)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 200 {
		t.Fatalf("NON-VACUITY: only %d files scanned; the walk root is wrong", files)
	}
	sort.Slice(s.findings, func(i, j int) bool { return s.findings[i].String() < s.findings[j].String() })
	for _, f := range s.findings {
		t.Errorf("#1192 identity on a log line: %s", f)
	}
	if len(s.findings) > 0 {
		t.Logf("drop each (%d): a log line reaches identity through its traceId and the audit record", len(s.findings))
	}
}

// TestS453_GuardDetectsEveryShape is the non-vacuity arm: each leaking shape
// must be reported, and each redacted shape must pass.
func TestS453_GuardDetectsEveryShape(t *testing.T) {
	leaks := map[string]string{
		"attr user":          `log.Warn("m", slog.String("user", u))`,
		"attr username kv":   `log.Info("m", "username", name)`,
		"selector any key":   `log.Debug("m", slog.String("rep", x.Username))`,
		"selector other key": `log.Debug("m", slog.String("who_else", x.Username))`,
		"groups any":         `log.Info("m", slog.Any("groups", gs))`,
		"groups selector":    `log.Info("m", slog.Any("g", ui.Groups))`,
		"identity key":       `log.Info("m", slog.String("identity", ranked[r].key))`,
		"cohort key":         `log.Info("m", slog.String("cohort", lbl))`,
		"message":            `log.Info("denied for " + ui.Username)`,
		"sprintf message":    `log.Info(fmt.Sprintf("u=%s", username))`,
		"context method":     `slog.InfoContext(ctx, "m", slog.String("user", u))`,
		"with":               `l := log.With("user", u); _ = l`,
		"group nested":       `log.Info("m", slog.Group("g", "user", u))`,
		"attrs slice":        `attrs := []any{slog.String("user", user)}; _ = attrs`,
		"cn ident":           `log.Info("m", slog.String("subjectcn", cn))`,
		"secret name":        `log.Info("m", slog.String("s", secretName))`,
		"stderr lane":        `fmt.Fprintf(os.Stderr, "user=%s groups=%v\n", username, groups)`,
		"std log":            `log.Printf("user=%s", ui.Username)`,
		"attr literal key":   `a := slog.Attr{Key: "user", Value: slog.StringValue(u)}; _ = a`,
		"attr literal pos":   `a := slog.Attr{"who_else", slog.StringValue(ui.Username)}; _ = a`,
		"string value":       `v := slog.StringValue(ui.Username); _ = v`,
		"any value groups":   `v := slog.AnyValue(ui.Groups); _ = v`,
		// #1192: a label, a literal or a constant under an identity key is
		// still an identity attribute.
		"redacted user":     `log.Warn("m", slog.String("user", redact.User(u)))`,
		"literal user":      `log.Info("m", slog.String("user", "<none>"))`,
		"user_hash":         `log.Info("m", slog.String("user_hash", redact.User(u)))`,
		"label under other": `log.Info("m", slog.String("target", "rep "+redact.User(u)))`,
		"label in message":  `log.Info("rep " + redact.User(u))`,
		"label kv":          `log.Info("m", "x", redact.User(u))`,
		"learned label":     `log.Info("m", slog.String("x", cache.LearnedClassLabel(u, g)))`,
		"stale subject":     `log.Info("m", slog.String("x", cache.StaleSubject(u)))`,
		"label to stderr":   `fmt.Fprintf(os.Stderr, "u=%s\n", redact.User(username))`,
		"label value":       `v := slog.StringValue(redact.User(u)); _ = v`,
		"attr literal":      `a := slog.Attr{Key: "user", Value: slog.StringValue(redact.User(u))}; _ = a`,
		"group of identity": `log.With(slog.Group("user", slog.String("name", n), slog.String("groups", g)))`,
		"plumbing logger":   `c := use.NewChain(use.Logger(log)); _ = c`,
		"identity kv with":  `l := log.With("traceId", id, "user", u); _ = l`,
	}
	for name, stmt := range leaks {
		src := "package p\nfunc f() {\n" + stmt + "\n}\n"
		if got := scanSource(t, src); len(got) == 0 {
			t.Errorf("RED shape %q not detected: %s", name, stmt)
		}
	}
	cleanShapes := map[string]string{
		"api group":     `log.Info("m", slog.String("group", gvr.Group))`,
		"count":         `log.Info("m", slog.Int("n", len(x)))`,
		"prefix concat": `log.Info("m", slog.String("target", "url "+redact.URL(u)))`,
		"opaque label":  `log.Info("m", slog.String("key", redact.Prefixed("key", k)))`,
		"scrubbed err":  `log.Info("m", slog.String("err", redact.Err(err)))`,
		"non-log Info":  `x.Info()`,
		"other logger":  `c := use.NewChain(use.TraceId(), middleware.RequestLogger(log)); _ = c`,
		"group of attr": `log.Info("m", slog.Group("page", slog.Int("size", n)))`,
	}
	for name, stmt := range cleanShapes {
		src := "package p\nfunc f() {\n" + stmt + "\n}\n"
		if got := scanSource(t, src); len(got) != 0 {
			t.Errorf("clean shape %q flagged: %v", name, got)
		}
	}
}

// ---- typed half -------------------------------------------------------------

// sensitiveFieldExact are struct fields whose value is an identity or a
// credential. Matched on the field name with its first letter upper-cased, so
// an unexported field (rendered by the text handler's %+v) counts too.
var sensitiveFieldExact = map[string]bool{
	// identity
	"Username": true, "UserName": true, "User": true, "Groups": true,
	"CommonName": true, "Subjects": true,
	// credential
	"Token": true, "Password": true, "ClientKeyData": true,
	"ClientCertificateData": true, "AwsSecretKey": true, "AwsAccessKey": true,
	"AccessToken": true, "BearerToken": true, "RefreshToken": true,
	"PrivateKey": true, "SecretKey": true, "ClientKey": true, "ClientCert": true,
	"KeyData": true, "CertData": true, "Passphrase": true, "Secret": true,
}

// sensitiveFieldSuffix catches the families (FooToken, AdminPassword, ...).
var sensitiveFieldSuffix = []string{"Token", "Password", "SecretKey", "PrivateKey", "KeyData", "CertificateData"}

func sensitiveField(name string) bool {
	if name == "" || name == "_" {
		return false
	}
	n := strings.ToUpper(name[:1]) + name[1:]
	if sensitiveFieldExact[n] || strings.HasPrefix(n, "Representative") {
		return true
	}
	for _, s := range sensitiveFieldSuffix {
		if strings.HasSuffix(n, s) {
			return true
		}
	}
	return false
}

// rendersItself reports whether slog renders t through its own method
// (slog.LogValuer, error) rather than field by field.
func rendersItself(t types.Type) bool {
	for _, tt := range []types.Type{t, types.NewPointer(t)} {
		ms := types.NewMethodSet(tt)
		for i := 0; i < ms.Len(); i++ {
			switch ms.At(i).Obj().Name() {
			case "LogValue", "Error":
				return true
			}
		}
	}
	return false
}

// sensitivePath returns the field path through which t reaches a sensitive
// field ("" when none).
func sensitivePath(t types.Type, depth int, seen map[types.Type]bool) string {
	if depth > 6 || t == nil || seen[t] {
		return ""
	}
	seen[t] = true
	// #487 (reviewer-424): a LogValuer/error renders itself only when it IS
	// the logged value. Nested in a struct, json / %+v render its fields and
	// ignore LogValue, so below depth 0 it is walked like any other type.
	if _, isNamed := t.(*types.Named); isNamed && depth == 0 && rendersItself(t) {
		return ""
	}
	switch u := t.Underlying().(type) {
	case *types.Pointer:
		return sensitivePath(u.Elem(), depth+1, seen)
	case *types.Slice:
		return sensitivePath(u.Elem(), depth+1, seen)
	case *types.Array:
		return sensitivePath(u.Elem(), depth+1, seen)
	case *types.Map:
		if s := sensitivePath(u.Key(), depth+1, seen); s != "" {
			return s
		}
		return sensitivePath(u.Elem(), depth+1, seen)
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			f := u.Field(i)
			if sensitiveField(f.Name()) {
				return f.Name()
			}
			if s := sensitivePath(f.Type(), depth+1, seen); s != "" {
				return f.Name() + "." + s
			}
		}
	}
	return ""
}

// loggedValues returns the argument expressions of c that a log or print sink
// renders (nil when c is not a sink).
func loggedValues(c *ast.CallExpr) []ast.Expr {
	pkg, name := calleeName(c.Fun)
	if pkg == "slog" {
		switch name {
		case "Any", "Attr":
			if len(c.Args) == 2 {
				return c.Args[1:]
			}
		case "AnyValue": // #490 review: also inside hand-written slog.Attr{Key, Value} literals
			if len(c.Args) == 1 {
				return c.Args
			}
		case "Group":
			if len(c.Args) > 1 {
				return c.Args[1:]
			}
		}
	}
	if first, ok := printSinks[pkg+"."+name]; ok {
		return c.Args[min(first, len(c.Args)):]
	}
	if _, isSel := c.Fun.(*ast.SelectorExpr); !isSel {
		return nil
	}
	if msgIdx, ok := logMethods[name]; ok {
		return c.Args[min(msgIdx+1, len(c.Args)):]
	}
	return nil
}

// typedScan reports every logged value of a production (non-test) file whose
// static type reaches a sensitive field.
// opaqueKind reports whether t is, or reaches through fields, pointers,
// slices, arrays or maps, an opaque data container whose CONTENT a handler
// renders wholesale: an interface other than error (any, map[string]any,
// []any, ...). Such a value is a resolved body, a JQ result or a template
// dict. It can hold Secret data or per-user rows, and its static type says
// nothing about what it holds (#487). A LogValuer/error is trusted only as
// the logged value itself (depth 0).
func opaqueKind(t types.Type, depth int, seen map[types.Type]bool) string {
	if depth > 6 || t == nil || seen[t] {
		return ""
	}
	seen[t] = true
	if isErrorType(t) {
		return ""
	}
	if _, isNamed := t.(*types.Named); isNamed && depth == 0 && rendersItself(t) {
		return ""
	}
	switch u := t.Underlying().(type) {
	case *types.Interface:
		return "interface " + t.String()
	case *types.Pointer:
		return opaqueKind(u.Elem(), depth+1, seen)
	case *types.Slice:
		if s := opaqueKind(u.Elem(), depth+1, seen); s != "" {
			return "[]" + s
		}
	case *types.Array:
		if s := opaqueKind(u.Elem(), depth+1, seen); s != "" {
			return "[n]" + s
		}
	case *types.Map:
		if s := opaqueKind(u.Elem(), depth+1, seen); s != "" {
			return "map value " + s
		}
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if s := opaqueKind(u.Field(i).Type(), depth+1, seen); s != "" {
				return "field " + u.Field(i).Name() + ": " + s
			}
		}
	}
	return ""
}

// isSlogType reports whether t is a log/slog type (Attr, Value, ...) or a
// pointer/slice/array of one.
func isSlogType(t types.Type) bool {
	for {
		switch u := t.(type) {
		case *types.Pointer:
			t = u.Elem()
			continue
		case *types.Slice:
			t = u.Elem()
			continue
		case *types.Array:
			t = u.Elem()
			continue
		case *types.Named:
			return u.Obj().Pkg() != nil && u.Obj().Pkg().Path() == "log/slog"
		}
		return false
	}
}

func isErrorType(t types.Type) bool {
	errIface, _ := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	return types.Implements(t, errIface)
}

// opaqueAllow is the REVIEWED allow-list of logged opaque values, keyed
// "<pkgdir>/<file>:<func>:<expr>". Each entry says why its content can carry
// no body, Secret or identity. Adding an entry is a review decision.
//
// A recover() value (#487: the reviewed exception class) needs no entry. An
// identifier assigned from recover() in the same function is recognised
// structurally (recoverObjects).
//
// It is empty: #490 turned per_call_log's []any into a []slog.Attr emitted
// with LogAttrs, so its old entry is gone.
var opaqueAllow = map[string]string{}

// recoverObjects returns the objects a function assigns from recover().
func recoverObjects(info *types.Info, root ast.Node) map[types.Object]bool {
	out := map[types.Object]bool{}
	ast.Inspect(root, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		c, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := c.Fun.(*ast.Ident); !ok || id.Name != "recover" {
			return true
		}
		for _, l := range as.Lhs {
			if id, ok := l.(*ast.Ident); ok {
				if o := info.ObjectOf(id); o != nil {
					out[o] = true
				}
			}
		}
		return true
	})
	return out
}

// typedScan reports every logged value of a production (non-test) file whose
// static type reaches a sensitive field, or is an opaque data container not in
// the reviewed allow-list.
func typedScan(pkgs []*packages.Package) []string {
	return typedScanWith(pkgs, opaqueAllow)
}

func typedScanWith(pkgs []*packages.Package, allow map[string]string) []string {
	var hits []string
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			for _, d := range f.Decls {
				fn := ""
				if fd, ok := d.(*ast.FuncDecl); ok {
					fn = fd.Name.Name
				}
				recovered := recoverObjects(p.TypesInfo, d)
				ast.Inspect(d, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					for _, v := range loggedValues(c) {
						tv, ok := p.TypesInfo.Types[v]
						if !ok || tv.Type == nil {
							continue
						}
						if isSlogType(tv.Type) {
							continue // an Attr/Value (or a slice of them): its own constructor call is checked
						}
						pos := p.Fset.Position(v.Pos())
						where := filepath.Base(filepath.Dir(pos.Filename)) + "/" + filepath.Base(pos.Filename)
						if s := sensitivePath(tv.Type, 0, map[types.Type]bool{}); s != "" {
							hits = append(hits, fmt.Sprintf("%s:%d: logged value of type %s reaches %s", where, pos.Line, tv.Type, s))
							continue
						}
						if id, ok := v.(*ast.Ident); ok && recovered[p.TypesInfo.ObjectOf(id)] {
							continue // a recover() value: the reviewed exception class
						}
						if k := opaqueKind(tv.Type, 0, map[types.Type]bool{}); k != "" {
							key := where + ":" + fn + ":" + types.ExprString(v)
							if _, ok := allow[key]; !ok {
								hits = append(hits, fmt.Sprintf("%s:%d: logged opaque value %s (%s) — log ids, sizes and digests, "+
									"or allow-list %q with a reason", where, pos.Line, types.ExprString(v), k, key))
							}
						}
					}
					return true
				})
			}
		}
	}
	sort.Strings(hits)
	return hits
}

func loadPackages(t *testing.T, dir string) []*packages.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  dir,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("load %s: %v", p.PkgPath, e)
		}
	}
	return pkgs
}

func TestS453_NoLoggedValueTypeCarriesIdentityOrCredential(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	pkgs := loadPackages(t, filepath.Join("..", ".."))
	if len(pkgs) < 20 {
		t.Fatalf("NON-VACUITY: only %d packages loaded", len(pkgs))
	}
	for _, h := range typedScan(pkgs) {
		t.Errorf("#453 struct logged whole: %s — log a redacted projection instead", h)
	}
}

// TestS453_TypedScanDetectsEveryShape is the typed half's non-vacuity arm, over
// a throwaway module: each leaking shape is reported, each clean one is not.
// #487 added the opaque-value shapes (a body dict, an any, a struct holding an
// any, a LogValuer nested below depth 0) and the reviewed exceptions (a
// recover() value, an error, an allow-listed expression).
func TestS453_TypedScanDetectsEveryShape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	src := `package fixture

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
)

type Endpoint struct {
	ServerURL     string
	ClientKeyData string ` + "`json:\"-\"`" + `
}
type Wrap struct{ Inner *Endpoint }
type Rep struct{ RepresentativeUsername string }
type cfg struct{ username string }
type Safe struct{ N int; Name string }
type Valuer struct{ Password string }
type Eval struct{ Path string; Value any }
type Holder struct{ V Valuer }

func (Valuer) LogValue() slog.Value { return slog.StringValue("redacted") }

func F(ep Endpoint, w []Wrap, m map[string]Rep, u cfg, s Safe, v Valuer, l *slog.Logger,
	dict map[string]any, val any, evals []Eval, h Holder, allowed []any) {
	slog.Debug("a", slog.Any("endpoint", ep)) // LEAK
	slog.Info("b", "wrapped", w) // LEAK
	l.With("m", m).Info("c") // LEAK
	fmt.Fprintf(os.Stderr, "%+v\n", &ep) // LEAK
	slog.Info("d", slog.Group("g", "u", u)) // LEAK
	slog.Debug("resolved api", slog.Any("dict", dict)) // LEAK
	slog.Debug("v", slog.Any("value", val)) // LEAK
	slog.Debug("evals", slog.Any("evals", evals)) // LEAK
	slog.Debug("nested valuer", slog.Any("h", h)) // LEAK
	slog.Debug("anyvalue kv", "v", slog.AnyValue(dict)) // LEAK
	l.LogAttrs(nil, slog.LevelDebug, "attr literal", slog.Attr{Key: "dict", Value: slog.AnyValue(val)}) // LEAK
	l.LogAttrs(nil, slog.LevelDebug, "attrs", []slog.Attr{slog.Int("n", 1)}...) // CLEAN
	slog.Info("e", slog.Any("safe", s), slog.Any("v", v), "n", s.N) // CLEAN
	slog.Info("f", slog.Any("err", errors.New("x")), slog.Any("allowed", allowed)) // CLEAN
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic", slog.Any("panic", r)) // CLEAN
		}
	}()
}
`
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module fixture\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	hits := typedScanWith(loadPackages(t, dir), map[string]string{"fixture/f.go:F:allowed": "fixture allow-list entry"})
	lines := strings.Split(src, "\n")
	want := map[int]bool{}
	for i, l := range lines {
		if strings.Contains(l, "// LEAK") {
			want[i+1] = true
		}
	}
	got := map[int]bool{}
	for _, h := range hits {
		var ln int
		if _, err := fmt.Sscanf(h[strings.Index(h, ".go:")+4:], "%d", &ln); err == nil {
			got[ln] = true
		}
		if !want[ln] {
			t.Errorf("clean shape flagged: %s", h)
		}
	}
	for ln := range want {
		if !got[ln] {
			t.Errorf("RED shape not detected at fixture line %d: %s", ln, strings.TrimSpace(lines[ln-1]))
		}
	}
	if len(want) != 11 {
		t.Fatalf("fixture has %d LEAK markers, want 11", len(want))
	}
	// The allow-list is what keeps "allowed" clean: without it, it is a hit.
	if !strings.Contains(strings.Join(typedScanWith(loadPackages(t, dir), nil), "\n"), "allowed") {
		t.Error("NON-VACUITY: the allow-listed []any must be a hit when the allow-list is empty")
	}
}
