package jq

// modules_reserve_639_test.go — #639 RED-first arms.
//
// THE DEFECTS (origin/main 1205faad, modules.go):
//   - :64-67 a custom module's read/parse error was DISCARDED and the loader fell
//     through to the embedded built-in of the same name — a broken operator
//     module was silent (different output, no error), and for a non-built-in
//     name the parse error surfaced as a misleading "not found";
//   - :33-37 an operator module beat a built-in of the same name, so any writer
//     of the custom-modules source could replace `health` (or #623's
//     `krateo/architecture/v1`) for every resolve on the cluster.
//
// Each arm drives the loader through gojq exactly as a resolver filter does
// (an `include` in a compiled query), against a real temp directory standing in
// for the /jq-modules ConfigMap mount.

import (
	"bytes"
	"context"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/itchyny/gojq"
)

const s639Sentinel = "OPERATOR-SHADOW-SENTINEL-639"

// s639Eval compiles and runs query with loader ml; it returns the single value
// or the compile/run error.
func s639Eval(t *testing.T, ml gojq.ModuleLoader, query string, input any) (any, error) {
	t.Helper()
	q, err := gojq.Parse(query)
	if err != nil {
		t.Fatalf("SETUP: parse %q: %v", query, err)
	}
	code, err := gojq.Compile(q, gojq.WithModuleLoader(ml))
	if err != nil {
		return nil, err
	}
	iter := code.RunWithContext(context.Background(), input)
	v, ok := iter.Next()
	if !ok {
		return nil, nil
	}
	if e, isErr := v.(error); isErr {
		return nil, e
	}
	return v, nil
}

func s639WriteModule(t *testing.T, dir, name, body string) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name)+".jq")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestIssue639_BuiltinNamesEnumeratedFromEmbeddedSet pins that the reserved set
// IS the embedded set (not a hand list): every modules/**/*.jq file is a name,
// and nothing else is.
func TestIssue639_BuiltinNamesEnumeratedFromEmbeddedSet(t *testing.T) {
	var want []string
	if err := fsWalkJQ(builtinModulesFS, builtinRoot, &want); err != nil {
		t.Fatal(err)
	}
	got := BuiltinModuleNames()
	if len(got) == 0 {
		t.Fatal("no built-in module names enumerated — the reservation would protect nothing")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuiltinModuleNames() = %v, want every embedded module %v", got, want)
	}
	for _, must := range []string{"health", "quantity"} {
		if !contains(got, must) {
			t.Errorf("built-in %q missing from the reserved set %v", must, got)
		}
	}
}

// TestIssue639_NestedBuiltinNameIsReserved — the enumeration recurses, so a
// nested built-in (#623's `krateo/architecture/v1`, embedded under a
// subdirectory) is reserved by being embedded, and a same-named operator file
// does not win. Uses a MapFS so it does not wait on #623 landing.
func TestIssue639_NestedBuiltinNameIsReserved(t *testing.T) {
	builtin := fstest.MapFS{
		"modules/health.jq":                 {Data: []byte(`def who: "builtin-health";`)},
		"modules/krateo/architecture/v1.jq": {Data: []byte(`def who: "builtin-arch";`)},
	}
	names := builtinModuleNames(builtin, "modules")
	if want := []string{"health", "krateo/architecture/v1"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("names = %v, want %v (enumeration must recurse)", names, want)
	}

	dir := t.TempDir()
	s639WriteModule(t, dir, "krateo/architecture/v1", `def who: "`+s639Sentinel+`";`)
	l := newLayeredModuleLoader(builtin, "modules", dir)
	got, err := s639Eval(t, l, `include "krateo/architecture/v1"; who`, nil)
	if err != nil {
		t.Fatalf("import of the nested built-in failed: %v", err)
	}
	if got != "builtin-arch" {
		t.Fatalf("nested built-in resolved to %v — an operator file shadowed it", got)
	}
}

// TestIssue639_CustomModuleCannotShadowAnyBuiltin — for EVERY embedded built-in,
// an operator module of the same name defining a sentinel must not be what the
// import resolves to. RED on origin/main: the custom layer won every collision.
func TestIssue639_CustomModuleCannotShadowAnyBuiltin(t *testing.T) {
	for _, name := range BuiltinModuleNames() {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s639WriteModule(t, dir, name, `def s639_shadow: "`+s639Sentinel+`";`)
			l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

			_, err := s639Eval(t, l, `include "`+name+`"; s639_shadow`, nil)
			if err == nil {
				t.Fatalf("operator module %q SHADOWED the built-in: its sentinel def resolved", name)
			}
			// And the built-in itself still resolves under that name.
			q, perr := l.LoadModule(name)
			if perr != nil || q == nil {
				t.Fatalf("built-in %q no longer loads: %v", name, perr)
			}
			if strings.Contains(q.String(), s639Sentinel) {
				t.Fatalf("LoadModule(%q) returned the operator module", name)
			}
		})
	}
}

// TestIssue639_CustomModuleParseError_FailsImportingFilter — a broken operator
// module (non-reserved name) fails the importing filter WITH its parse error.
// RED on origin/main: the parse error was discarded and the filter failed with
// a misleading "module not found" from the built-in fallthrough.
func TestIssue639_CustomModuleParseError_FailsImportingFilter(t *testing.T) {
	dir := t.TempDir()
	s639WriteModule(t, dir, "s639broken", `def f: (;`)
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

	_, err := s639Eval(t, l, `include "s639broken"; f`, nil)
	if err == nil {
		t.Fatal("a filter importing a module that does not parse SUCCEEDED")
	}
	if !strings.Contains(err.Error(), "parse error") || strings.Contains(err.Error(), "not found") {
		t.Fatalf("error does not report the custom module's parse failure (it was swallowed): %v", err)
	}
}

// TestIssue639_BrokenCustomShadowOfBuiltin_IsRefusedNotParsed — a broken
// operator file under a RESERVED name is refused outright: the import resolves
// to the built-in, the operator file is never parsed, and the refusal is
// reported. (A reserved name has exactly one module — the built-in — so there
// is nothing to "fall back" from.)
func TestIssue639_BrokenCustomShadowOfBuiltin_IsRefusedNotParsed(t *testing.T) {
	dir := t.TempDir()
	s639WriteModule(t, dir, "health", `def normalize_health: (; # `+s639Sentinel)
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

	got, err := s639Eval(t, l, `include "health"; normalize_health`, "Healthy")
	if err != nil || got != "OK" {
		t.Fatalf("include \"health\" = (%v, %v), want the built-in's \"OK\"", got, err)
	}
	if refused := l.refusedCustomModules(); !reflect.DeepEqual(refused, []string{"health"}) {
		t.Fatalf("refusedCustomModules() = %v, want [health]", refused)
	}
}

// TestIssue639_RefusalLog_NamesOnlyNeverContent — the boot report carries the
// module NAME and never the module content.
func TestIssue639_RefusalLog_NamesOnlyNeverContent(t *testing.T) {
	dir := t.TempDir()
	s639WriteModule(t, dir, "health", `def leak: "`+s639Sentinel+`";`)
	s639WriteModule(t, dir, "s639ok", `def ok: 1;`)
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

	var buf bytes.Buffer
	logRefusedCustomModules(slog.New(slog.NewJSONHandler(&buf, nil)), dir, l.refusedCustomModules())
	out := buf.String()
	if !strings.Contains(out, `"module":"health"`) || !strings.Contains(out, `"level":"ERROR"`) {
		t.Fatalf("refusal not reported at ERROR by name: %s", out)
	}
	if strings.Contains(out, s639Sentinel) || strings.Contains(out, "def leak") {
		t.Fatalf("refusal log carries module CONTENT: %s", out)
	}
	if strings.Contains(out, "s639ok") {
		t.Fatalf("a non-colliding custom module was reported as refused: %s", out)
	}
}

// TestIssue639_CustomModule_NonReservedStillLoads — the control: an operator
// module under a free name still loads, and a missing one is "not found".
func TestIssue639_CustomModule_NonReservedStillLoads(t *testing.T) {
	dir := t.TempDir()
	s639WriteModule(t, dir, "s639ok", `def ok: "custom-ok";`)
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)
	if got, err := s639Eval(t, l, `include "s639ok"; ok`, nil); err != nil || got != "custom-ok" {
		t.Fatalf("operator module under a free name = (%v, %v), want custom-ok", got, err)
	}
	if _, err := s639Eval(t, l, `include "s639-missing"; .`, nil); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing module error = %v, want not found", err)
	}
	if _, err := s639Eval(t, l, `include "../escape"; .`, nil); err == nil {
		t.Fatal("a module name escaping the modules directory loaded")
	}
}

// fsWalkJQ is an INDEPENDENT oracle for the enumeration (recursive ReadDir,
// not the production WalkDir): every *.jq file under dir, as a module name.
func fsWalkJQ(fsys fs.FS, dir string, out *[]string) error {
	var rec func(d, prefix string) error
	rec = func(d, prefix string) error {
		ents, err := fs.ReadDir(fsys, d)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.IsDir() {
				if err := rec(d+"/"+e.Name(), prefix+e.Name()+"/"); err != nil {
					return err
				}
				continue
			}
			if strings.HasSuffix(e.Name(), ".jq") {
				*out = append(*out, prefix+strings.TrimSuffix(e.Name(), ".jq"))
			}
		}
		return nil
	}
	if err := rec(dir, ""); err != nil {
		return err
	}
	sort.Strings(*out)
	return nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// TestIssue639_EscapingModuleName_RefusedEvenWhenTargetExists — the path guard
// (filepath.IsLocal) must refuse a name that leaves the modules directory even
// when the escape TARGET EXISTS; "not found" alone cannot see the guard. Covers
// `../<existing>` and an absolute name. Ported from reviewer-619's arm RA.
func TestIssue639_EscapingModuleName_RefusedEvenWhenTargetExists(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "mods")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "escape.jq"), []byte(`def pwn: "ESCAPED";`), 0o644); err != nil {
		t.Fatal(err)
	}
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

	got, err := s639Eval(t, l, `include "../escape"; pwn`, nil)
	if err == nil || got == "ESCAPED" {
		t.Fatalf("../escape resolved to a file outside the modules dir (got %v)", got)
	}
	if !strings.Contains(err.Error(), "invalid module name") {
		t.Errorf("../escape: want 'invalid module name', got %v", err)
	}

	abs := filepath.Join(parent, "escape")
	if _, err := l.LoadModule(abs); err == nil || !strings.Contains(err.Error(), "invalid module name") {
		t.Errorf("absolute module name %q not refused: %v", abs, err)
	}
}

// TestIssue639_UnreadableCustomModule_FailsWithReadError — a custom module
// that exists but cannot be read (a directory named like a module; no read
// permission) fails the importing filter with "unable to read", never "not
// found" and never a fallback. Ported from reviewer-619's arm RB.
func TestIssue639_UnreadableCustomModule_FailsWithReadError(t *testing.T) {
	dir := t.TempDir()
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)

	t.Run("directory named like a module", func(t *testing.T) {
		if err := os.MkdirAll(filepath.Join(dir, "s639dir.jq"), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := s639Eval(t, l, `include "s639dir"; .`, nil)
		if err == nil || strings.Contains(err.Error(), "not found") || !strings.Contains(err.Error(), "unable to read") {
			t.Fatalf("a directory module did not fail closed with its read error: %v", err)
		}
	})

	t.Run("no read permission", func(t *testing.T) {
		p := filepath.Join(dir, "s639locked.jq")
		if err := os.WriteFile(p, []byte(`def f: 1;`), 0o000); err != nil {
			t.Fatal(err)
		}
		if _, rerr := os.ReadFile(p); rerr == nil {
			t.Skip("this user can read a 0000 file (root); the directory case above still covers the read-error branch")
		}
		_, err := s639Eval(t, l, `include "s639locked"; f`, nil)
		if err == nil || strings.Contains(err.Error(), "not found") || !strings.Contains(err.Error(), "unable to read") {
			t.Fatalf("unreadable module did not fail closed with its read error: %v", err)
		}
	})
}

// TestIssue639_EveryModuleSourceIsEmbedded — `//go:embed modules/*.jq` does
// NOT descend into subdirectories, so a nested built-in directory added under
// modules/ without its own embed pattern would silently be neither embedded nor
// RESERVED. Walk the SOURCE tree on disk and require every .jq file to be in
// the embedded FS and in the reserved set: a missing embed line fails here.
func TestIssue639_EveryModuleSourceIsEmbedded(t *testing.T) {
	var onDisk []string
	err := filepath.WalkDir(builtinRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".jq") {
			onDisk = append(onDisk, filepath.ToSlash(p))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk source %s/: %v", builtinRoot, err)
	}
	if len(onDisk) == 0 {
		t.Fatalf("no .jq sources under %s/ — the arm walked the wrong directory", builtinRoot)
	}
	reserved := map[string]bool{}
	for _, n := range BuiltinModuleNames() {
		reserved[n] = true
	}
	for _, p := range onDisk {
		if _, err := fs.Stat(builtinModulesFS, p); err != nil {
			t.Errorf("source %s is NOT embedded — add its directory to the //go:embed line in modules.go: %v", p, err)
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(p, builtinRoot+"/"), ".jq")
		if !reserved[name] {
			t.Errorf("embedded module %q is not in the reserved set", name)
		}
	}
}
