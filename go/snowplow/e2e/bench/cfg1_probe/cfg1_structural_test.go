// cfg1_structural_test.go — #192 (1.12.6 commit C0): the CFG-1 cache-off
// falsifier made STRUCTURAL.
//
// THE GAP IT CLOSES. cfg1_falsifier.sh (HG-321, Ship 0.30.163) asserted a
// hand-maintained list of FIVE expvar keys against the 4-value CACHE_ENABLED
// matrix. At the time of #192 there were 18 files whose init() is gated on
// cache.Disabled() publishing 65 keys; a publisher that forgot the gate
// shipped unnoticed unless it happened to be one of the five. This test
// derives the gated key set FROM SOURCE and asserts all of it.
//
// WHAT IS STRUCTURAL HERE (four checks, none hand-listed):
//
//  1. The GATED KEY SET — go/parser over the module: a file is CFG-1-gated
//     when a function body has `if Disabled() { return }` (bare, or
//     `cache.Disabled()`) and the file publishes expvar keys with string
//     literal names via expvar.Publish/NewMap/NewInt/NewFloat/NewString. The
//     gate in `init()` means registered-at-process-start (asserted both
//     ways); the gate in any other function means registered-when-a-
//     subsystem-starts (e.g. the prewarm engine — asserted absent under
//     cache-off only, since a bare probe starts nothing). Keys published in
//     helpers called from the gated function live in the same file (the
//     codebase idiom), so per-file collection is exact for every publisher
//     present at #192 and is pinned by the floors below.
//  2. The UNGATED KEY SET — same walk, files whose init() publishes without
//     the gate. These MUST be present under cache-off too (the inverse
//     arm: the gate must not leak onto non-cache surfaces).
//  3. The PROBE'S IMPORT GRAPH — `go list -deps` of this binary must contain
//     every package that owns a gated publisher. A gated publisher added in
//     a package the probe does not import would otherwise be "absent" under
//     every env value and pass vacuously.
//  4. THE COMPLEMENT — under cache-off, the ONLY snowplow_* keys allowed at
//     /debug/vars are the ones the DECLARED non-cache exception list
//     (nonCacheInitPublishers) names. This is the check that catches a
//     publisher that never had a gate, or lost it: the leaked key is named in
//     the failure, and the derivation arm independently names the file.
//
// NON-EXERCISE GUARD. The derived set must have at least cfg1MinGatedFiles
// files and cfg1MinGatedKeys keys (the counts at #192). An empty or
// shrunken derivation FAILS the run — the falsifier cannot pass by finding
// nothing (feedback_falsifier_must_actually_run_under_gate_tag_env).
//
// LEGACY SUBSET. The five HG-321 names must be a subset of the derived
// gated set, so the original arm's meaning is preserved, not replaced.
//
// WHY A PROCESS PER ENV VALUE (unchanged from HG-321). expvar registration
// happens at package init(), which reads CACHE_ENABLED once per process, and
// expvar has no Unpublish. The matrix is therefore N spawned processes of
// this very binary (built into a temp dir), each answering /debug/vars
// anonymously — no JWT gate here, this is not the server.
//
// SCOPE NOTE. Publishers that live in the SERVER's package main (e.g.
// build_info_expvar.go) are registered from main(), not from an init(), and
// cannot be exercised by a probe binary; they are out of this test's scope by
// construction (the walk skips the module root's package main) and are
// covered by the server's own tests.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Floors = the derivation at #192 (2026-09-14). Raise them when publishers
// are added; never lower them without a design note — a lower floor is how
// this falsifier would go silent again.
const (
	// On main 63fdf99 (1.12.5): 18 init-time files / 61 keys (deps_expvar.go
	// `snowplow_deps` joined in 1.12.5) + 1 runtime-gated file / 5 keys
	// (prewarm engine). Raised from 18/65 (pre-1.12.5 main 98a64cc).
	cfg1MinGatedFiles = 19
	cfg1MinGatedKeys  = 66
)

// nonCacheInitPublishers is the EXPLICIT exception list: files whose init()
// publishes snowplow_* keys WITHOUT the CFG-1 gate because they are not cache
// surfaces. This is the one hand-maintained list left, and it is a list of
// exceptions, not of the governed set: an init publisher that is not here
// and has no gate FAILS the derivation arm by name. Removing a gate from a
// cache publisher therefore cannot reclassify it as "non-cache" silently —
// the first falsifier draft had exactly that hole (the mutation probe passed
// the matrix because the ungated set was derived, not declared).
var nonCacheInitPublishers = map[string][]string{
	// /readyz backstop: a readiness surface, meaningful with the cache off.
	"internal/handlers/dispatchers/readiness_backstop_metrics.go": {"snowplow_readyz_backstop_fired"},
}

// legacyHG321Keys are the five names the shell falsifier asserted since
// 0.30.163. They must remain a subset of the derived gated set.
var legacyHG321Keys = []string{
	"snowplow_apiserver_fallthrough_total",
	"snowplow_assertion_violations_total",
	"snowplow_apiserver_fallthrough_cells",
	"snowplow_upstream_controller_health",
	"snowplow_upstream_webhook_failurepolicy",
}

// publisher is one expvar-publishing Go file and what the walk found in it.
type publisher struct {
	file         string   // module-relative path
	pkgDir       string   // module-relative directory (= import path suffix)
	hasInit      bool     // declares func init()
	gated        bool     // init() has `if Disabled() { return }` → registered at process start, gated
	gatedRuntime bool     // no gated init, but a non-init function publishes behind `if Disabled() { return }`
	keys         []string // string-literal expvar names published in this file
}

// cfg1Derivation is the structural result the arms assert against.
//
// Four classes fall out of the walk:
//   - gated (init-time)      → matrix asserts PRESENT under true, ABSENT otherwise
//   - gatedRuntime           → registered when a subsystem starts (e.g. the prewarm
//     engine at prewarm_engine.go, registerPrewarmEngineMetrics) behind the same
//     gate; a bare probe never starts it, so the matrix asserts only ABSENT under
//     the three off values (presence under true is the subsystem's own test)
//   - ungated (init-time)    → non-cache surfaces; must be PRESENT under all four
//   - ungatedRuntime         → registered from somewhere else with no gate in the
//     file (e.g. the RBAC publish seq); logged, not asserted — the gate is not
//     required of non-cache surfaces, and a cache surface in this class is what
//     a reviewer should look at (the log line names it)
type cfg1Derivation struct {
	gated          []publisher
	gatedRuntime   []publisher
	ungated        []publisher
	ungatedRuntime []publisher
}

func keysOf(ps []publisher) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.keys...)
	}
	sort.Strings(out)
	return out
}

func (d cfg1Derivation) gatedKeys() []string        { return keysOf(d.gated) }
func (d cfg1Derivation) gatedRuntimeKeys() []string { return keysOf(d.gatedRuntime) }
func (d cfg1Derivation) allGated() []publisher {
	return append(append([]publisher{}, d.gated...), d.gatedRuntime...)
}

func (d cfg1Derivation) ungatedKeys() []string { return keysOf(d.ungated) }

func (d cfg1Derivation) gatedPkgDirs() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range d.allGated() {
		if !seen[p.pkgDir] {
			seen[p.pkgDir] = true
			out = append(out, p.pkgDir)
		}
	}
	sort.Strings(out)
	return out
}

// moduleRoot finds go.mod upward from this test's directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// deriveCFG1 walks the module and classifies every expvar publisher.
// Skipped: *_test.go, e2e/, testdata/, vendor/, .git, and the module root's
// own package main (server publishers registered from main(), see SCOPE
// NOTE).
func deriveCFG1(t *testing.T, root string) cfg1Derivation {
	t.Helper()
	var d cfg1Derivation
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(root, path)
		if entry.IsDir() {
			switch {
			case rel == ".":
				return nil
			case strings.HasPrefix(rel, "e2e"), strings.HasPrefix(rel, "vendor"),
				strings.HasPrefix(rel, ".git"), strings.Contains(rel, "testdata"):
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if !strings.Contains(rel, string(filepath.Separator)) {
			return nil // module root package main — out of scope by construction
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", rel, perr)
		}
		p := classifyFile(f)
		if len(p.keys) == 0 {
			return nil
		}
		p.file = rel
		p.pkgDir = filepath.ToSlash(filepath.Dir(rel))
		switch {
		case p.gated:
			d.gated = append(d.gated, p)
		case p.gatedRuntime:
			d.gatedRuntime = append(d.gatedRuntime, p)
		case p.hasInit:
			d.ungated = append(d.ungated, p)
		default:
			d.ungatedRuntime = append(d.ungatedRuntime, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	for _, ps := range [][]publisher{d.gated, d.gatedRuntime, d.ungated, d.ungatedRuntime} {
		sort.Slice(ps, func(i, j int) bool { return ps[i].file < ps[j].file })
	}
	return d
}

// classifyFile: collect literal expvar names; detect init(); detect the gate
// (in init → gated at process start; in any other function → gated at runtime).
func classifyFile(f *ast.File) publisher {
	var p publisher
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || fn.Body == nil {
			continue
		}
		isInit := fn.Name.Name == "init" && fn.Recv == nil
		if isInit {
			p.hasInit = true
		}
		if initHasDisabledGate(fn.Body) {
			if isInit {
				p.gated = true
			} else {
				p.gatedRuntime = true
			}
		}
	}
	if p.gated {
		p.gatedRuntime = false // init-time gate is the stronger class
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "expvar" {
			return true
		}
		switch sel.Sel.Name {
		case "Publish", "NewMap", "NewInt", "NewFloat", "NewString":
		default:
			return true
		}
		if len(call.Args) == 0 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		name, err := strconv.Unquote(lit.Value)
		if err == nil && strings.HasPrefix(name, "snowplow_") {
			p.keys = append(p.keys, name)
		}
		return true
	})
	sort.Strings(p.keys)
	return p
}

// initHasDisabledGate reports whether the init body contains
// `if Disabled() { ... return ... }` / `if cache.Disabled() { ... return ... }`.
func initHasDisabledGate(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || found {
			return !found
		}
		call, ok := ifs.Cond.(*ast.CallExpr)
		if !ok {
			return true
		}
		var fname string
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			fname = fun.Name
		case *ast.SelectorExpr:
			fname = fun.Sel.Name
		}
		if fname != "Disabled" {
			return true
		}
		for _, st := range ifs.Body.List {
			if _, isRet := st.(*ast.ReturnStmt); isRet {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// ─── the arms ───────────────────────────────────────────────────────────────

// TestCFG1_Structural_DerivationHasFloor is the non-exercise guard plus the
// legacy-subset arm. It runs first and alone so a shrunken derivation is
// reported as ITS OWN failure, not as a confusing matrix result.
func TestCFG1_Structural_DerivationHasFloor(t *testing.T) {
	d := deriveCFG1(t, moduleRoot(t))
	gk := d.gatedKeys()
	grk := d.gatedRuntimeKeys()
	allGatedFiles := len(d.gated) + len(d.gatedRuntime)
	allGatedKeys := len(gk) + len(grk)
	t.Logf("gated publishers: %d files / %d keys (init-time %d/%d, runtime %d/%d); ungated init publishers: %d files / %d keys; ungated runtime publishers: %d files",
		allGatedFiles, allGatedKeys, len(d.gated), len(gk), len(d.gatedRuntime), len(grk), len(d.ungated), len(d.ungatedKeys()), len(d.ungatedRuntime))
	for _, p := range d.gated {
		t.Logf("  gated(init)    %-66s %d keys", p.file, len(p.keys))
	}
	for _, p := range d.gatedRuntime {
		t.Logf("  gated(runtime) %-66s %d keys", p.file, len(p.keys))
	}
	for _, p := range d.ungated {
		allowed, ok := nonCacheInitPublishers[p.file]
		if !ok {
			t.Errorf("init() in %s publishes %v WITHOUT the CFG-1 gate and is not in nonCacheInitPublishers — add `if Disabled() { return }` to its init, or add it to the exception list with a reason", p.file, p.keys)
			continue
		}
		if strings.Join(allowed, ",") != strings.Join(p.keys, ",") {
			t.Errorf("non-cache exception %s publishes %v but the exception list says %v — update the list deliberately", p.file, p.keys, allowed)
		}
		t.Logf("  ungated(init)  %-66s %v (declared non-cache)", p.file, p.keys)
	}
	for file := range nonCacheInitPublishers {
		found := false
		for _, p := range d.ungated {
			if p.file == file {
				found = true
			}
		}
		if !found {
			t.Errorf("nonCacheInitPublishers names %s but the walk found no ungated init publisher there — stale exception", file)
		}
	}
	for _, p := range d.ungatedRuntime {
		t.Logf("  NOT GOVERNED   %-66s %v — registered from elsewhere with no Disabled() gate in the file; fine for a non-cache surface, a review item for a cache one", p.file, p.keys)
	}
	if allGatedFiles < cfg1MinGatedFiles {
		t.Fatalf("NON-EXERCISE: derived %d gated publisher files, floor is %d — the walk found too little to falsify anything", allGatedFiles, cfg1MinGatedFiles)
	}
	if allGatedKeys < cfg1MinGatedKeys {
		t.Fatalf("NON-EXERCISE: derived %d gated keys, floor is %d", allGatedKeys, cfg1MinGatedKeys)
	}
	set := map[string]bool{}
	for _, k := range append(append([]string{}, gk...), grk...) {
		if set[k] {
			t.Errorf("duplicate gated key across files: %s", k)
		}
		set[k] = true
	}
	for _, legacy := range legacyHG321Keys {
		if !set[legacy] {
			t.Errorf("legacy HG-321 key %q is not in the derived gated set — the original arm's meaning would be lost", legacy)
		}
	}
}

// TestCFG1_Structural_ProbeImportsEveryGatedPackage: derivation 3.
func TestCFG1_Structural_ProbeImportsEveryGatedPackage(t *testing.T) {
	root := moduleRoot(t)
	d := deriveCFG1(t, root)
	if len(d.allGated()) == 0 {
		t.Fatal("NON-EXERCISE: no gated publishers derived")
	}
	list := exec.Command("go", "list", "-deps", "./e2e/bench/cfg1_probe/")
	list.Dir = root // the test's cwd is the package dir; the pattern is module-relative
	out, err := list.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	deps := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		deps[strings.TrimSpace(line)] = true
	}
	modPath := modulePath(t, root)
	for _, dir := range d.gatedPkgDirs() {
		imp := modPath + "/" + dir
		if !deps[imp] {
			t.Errorf("probe binary does not import gated publisher package %s — its keys would be vacuously absent under every env value; add a side-effect import to cfg1_probe/main.go", imp)
		}
	}
}

// TestCFG1_Structural_Matrix is HG-321 rebuilt on the derived set: four
// processes, one per CACHE_ENABLED value, asserting every derived gated key
// ABSENT under false/unset/invalid and PRESENT under true, and every ungated
// init-published key PRESENT under all four.
func TestCFG1_Structural_Matrix(t *testing.T) {
	root := moduleRoot(t)
	d := deriveCFG1(t, root)
	gated := d.gatedKeys()
	gatedRuntime := d.gatedRuntimeKeys()
	// The keys allowed under cache-off come from the DECLARED exception list,
	// never from the derivation — see nonCacheInitPublishers.
	var ungated []string
	for _, ks := range nonCacheInitPublishers {
		ungated = append(ungated, ks...)
	}
	sort.Strings(ungated)
	if len(gated)+len(gatedRuntime) < cfg1MinGatedKeys {
		// Report, but still run the matrix: the complement arm below is what
		// NAMES the publisher that lost its gate, which the floor alone cannot.
		t.Errorf("NON-EXERCISE: %d gated keys derived (floor %d)", len(gated)+len(gatedRuntime), cfg1MinGatedKeys)
	}

	bin := filepath.Join(t.TempDir(), "cfg1_probe")
	build := exec.Command("go", "build", "-o", bin, "./e2e/bench/cfg1_probe/")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build probe: %v\n%s", err, out)
	}

	cases := []struct {
		label  string
		env    *string // nil = unset
		expect bool    // gated keys present?
	}{
		{"CACHE_ENABLED=true", strPtr("true"), true},
		{"CACHE_ENABLED=false", strPtr("false"), false},
		{"CACHE_ENABLED (unset)", nil, false},
		{"CACHE_ENABLED=invalid", strPtr("invalid"), false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			keys := spawnAndReadVars(t, bin, tc.env)
			var wrong []string
			for _, k := range gated {
				if keys[k] != tc.expect {
					wrong = append(wrong, k)
				}
			}
			if len(wrong) > 0 {
				verb := "ABSENT"
				if tc.expect {
					verb = "PRESENT"
				}
				t.Errorf("%s: %d/%d gated keys not %s as the CFG-1 contract requires: %v", tc.label, len(wrong), len(gated), verb, wrong)
			}
			// THE COMPLEMENT ARM — the one that catches a NEW publisher that never
			// had a gate (which the derivation, by construction, cannot list):
			// under cache-off, the ONLY snowplow_* keys allowed at /debug/vars are
			// the ungated init-time ones. Anything else is a cache publisher
			// missing its CFG-1 gate, named here so it cannot ship unnoticed.
			if !tc.expect {
				allowed := map[string]bool{}
				for _, k := range ungated {
					allowed[k] = true
				}
				var unexpected []string
				for k := range keys {
					if strings.HasPrefix(k, "snowplow_") && !allowed[k] {
						unexpected = append(unexpected, k)
					}
				}
				sort.Strings(unexpected)
				if len(unexpected) > 0 {
					t.Errorf("%s: snowplow_* keys present under cache-off that no ungated init publishes — a cache publisher is missing its Disabled() gate: %v", tc.label, unexpected)
				}
			}
			// Runtime-gated keys: a bare probe starts no subsystem, so under the
			// off values they MUST be absent (the gate); under true they are
			// absent too unless the subsystem ran — that side is not asserted.
			if !tc.expect {
				var leaked []string
				for _, k := range gatedRuntime {
					if keys[k] {
						leaked = append(leaked, k)
					}
				}
				if len(leaked) > 0 {
					t.Errorf("%s: runtime-gated keys present under cache-off: %v", tc.label, leaked)
				}
			}
			var missing []string
			for _, k := range ungated {
				if !keys[k] {
					missing = append(missing, k)
				}
			}
			if len(missing) > 0 {
				t.Errorf("%s: ungated (non-cache) keys missing — the gate leaked onto a non-cache surface: %v", tc.label, missing)
			}
			t.Logf("%s: %d init-gated keys checked (%s), %d runtime-gated keys checked absent-under-off, %d ungated keys present, %d keys total at /debug/vars",
				tc.label, len(gated), map[bool]string{true: "present", false: "absent"}[tc.expect], len(gatedRuntime), len(ungated), len(keys))
		})
	}
}

func strPtr(s string) *string { return &s }

func modulePath(t *testing.T, root string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	t.Fatal("module path not found in go.mod")
	return ""
}

// spawnAndReadVars starts the probe with the given CACHE_ENABLED arrangement
// on a free port, polls /debug/vars until it answers, and returns the set of
// top-level keys. The process is killed on return.
func spawnAndReadVars(t *testing.T, bin string, cacheEnabled *string) map[string]bool {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cmd := exec.Command(bin, "-addr="+addr)
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "CACHE_ENABLED=") {
			env = append(env, kv)
		}
	}
	if cacheEnabled != nil {
		env = append(env, "CACHE_ENABLED="+*cacheEnabled)
	}
	cmd.Env = env
	var logbuf strings.Builder
	cmd.Stdout = &logbuf
	cmd.Stderr = &logbuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start probe: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(20 * time.Second)
	var body []byte
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/debug/vars")
		if err == nil {
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if body == nil {
		t.Fatalf("probe never answered /debug/vars at %s; log:\n%s", addr, logbuf.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode /debug/vars: %v", err)
	}
	keys := make(map[string]bool, len(raw))
	for k := range raw {
		keys[k] = true
	}
	return keys
}
