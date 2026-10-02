// seed_terminal_put_guard_394_enum_test.go — #394 arm 4: the STRUCTURAL
// enumeration guard for the seed terminal Put, the gen-guard sibling of
// TestA1_EveryResolvedEntryPutSiteIsUAFGatedOrWaived.
//
// It pins two things a future edit could silently undo:
//
//  1. SITES. Every prod function in this package that builds a seed cell — a
//     `cache.ResolvedEntry{...}` literal carrying the seed-attribution field
//     `SeededAtBoot` — writes it through seedTerminalPut and contains NO direct
//     `.Put(` / `.ReplaceIfGen(` call. Enumerated by AST walk, not by a name
//     list, so a new seed Put site (or a reverted one) fails on the day it is
//     written. The two seed primitives must also capture the guard at entry
//     (seedTerminalGuardFor).
//  2. MODES. Every declared seedScopeMode constant (enumerated from the const
//     block by AST) must be classified here, and every mode except seedModeBoot
//     must yield a GUARDED terminal Put (PutIfGen, never a plain Put). A new
//     post-readyz mode therefore cannot reach a plain Put without failing this
//     test.
//
// RED on origin/main: the walk finds seedOneWidget and
// seedRestactionResolveAndPutProd calling handle.Put directly.
package dispatchers

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// s394SeedCellMarker is the ResolvedEntry field that identifies a SEED cell
// literal (#130 F3 seed attribution — only the seed primitives set it).
const s394SeedCellMarker = "SeededAtBoot"

type s394SeedSite struct {
	file, fn     string
	line         int
	directWrites []string // forbidden direct store writes in the function
	viaHelper    bool     // calls seedTerminalPut(
	captures     bool     // calls seedTerminalGuardFor( / seedTerminalGuardFromContext(
}

func s394CollectSeedSites(t *testing.T) []s394SeedSite {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	var out []s394SeedSite
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		src, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range src.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var site *s394SeedSite
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || !isResolvedEntryLit(lit.Type) {
					return true
				}
				for _, el := range lit.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if id, ok := kv.Key.(*ast.Ident); ok && id.Name == s394SeedCellMarker && site == nil {
						site = &s394SeedSite{file: path, fn: fn.Name.Name, line: fset.Position(lit.Pos()).Line}
					}
				}
				return true
			})
			if site == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.SelectorExpr:
					switch f.Sel.Name {
					case "Put", "ReplaceIfGen", "PutIfGen":
						site.directWrites = append(site.directWrites, f.Sel.Name+"@"+itoaLine(fset.Position(call.Pos()).Line))
					}
				case *ast.Ident:
					switch f.Name {
					case "seedTerminalPut":
						site.viaHelper = true
					case "seedTerminalGuardFor", "seedTerminalGuardFromContext":
						site.captures = true
					}
				}
				return true
			})
			out = append(out, *site)
		}
	}
	return out
}

func TestS394_SeedTerminalPutSitesAreGenGuarded(t *testing.T) {
	sites := s394CollectSeedSites(t)
	found := map[string]bool{}
	for _, s := range sites {
		found[s.fn] = true
	}
	// Non-vacuity: the two known seed Put sites must be found by the walk.
	for _, want := range []string{"seedOneWidget", "seedRestactionResolveAndPutProd"} {
		if !found[want] {
			t.Fatalf("VACUOUS GUARD: the AST walk did not find the seed Put site in %s (sites=%v) — the "+
				"%s marker or the literal shape changed; fix the walk, do not delete the expectation", want, sites, s394SeedCellMarker)
		}
	}
	for _, s := range sites {
		if len(s.directWrites) > 0 {
			t.Errorf("#394: %s:%d (%s) builds a seed cell and writes the store DIRECTLY (%v). A post-readyz seed "+
				"(keepwarm / gvr-discovered) reaching a plain Put resurrects a cell removed during its resolve. "+
				"Route the write through seedTerminalPut with the guard captured at seed entry.",
				s.file, s.line, s.fn, s.directWrites)
		}
		if !s.viaHelper {
			t.Errorf("#394: %s:%d (%s) builds a seed cell but never calls seedTerminalPut", s.file, s.line, s.fn)
		}
		if !s.captures {
			t.Errorf("#394: %s:%d (%s) writes a seed cell without reading a terminal guard "+
				"(seedTerminalGuardFor / seedTerminalGuardFromContext)", s.file, s.line, s.fn)
		}
	}

	// The restaction tail reads its guard off ctx, so the PRIMITIVE that drives
	// it must capture at entry; seedOneWidget captures inline.
	if !s394FuncCalls(t, "phase1_pip_seed.go", "seedOneRestaction", "seedTerminalGuardFor") ||
		!s394FuncCalls(t, "phase1_pip_seed.go", "seedOneRestaction", "withSeedTerminalGuard") {
		t.Errorf("#394: seedOneRestaction must capture the guard at seed entry (seedTerminalGuardFor) and carry it " +
			"to the tail on resCtx (withSeedTerminalGuard)")
	}
	if !s394FuncCalls(t, "phase1_pip_seed.go", "seedOneWidget", "seedTerminalGuardFor") {
		t.Errorf("#394: seedOneWidget must capture the guard at seed entry (seedTerminalGuardFor)")
	}
}

func s394FuncCalls(t *testing.T, file, fnName, callee string) bool {
	t.Helper()
	fset := token.NewFileSet()
	src, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	hit := false
	for _, decl := range src.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != fnName || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == callee {
					hit = true
				}
			}
			return true
		})
	}
	return hit
}

// s394DeclaredSeedModes enumerates the seedScopeMode constants from the const
// block that declares them (iota block: the first spec carries the type).
func s394DeclaredSeedModes(t *testing.T) []string {
	t.Helper()
	files, _ := filepath.Glob("*.go")
	var names []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(raw), "seedScopeMode") {
			continue
		}
		fset := token.NewFileSet()
		src, err := parser.ParseFile(fset, path, raw, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range src.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			inBlock := false
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				if id, ok := vs.Type.(*ast.Ident); ok {
					inBlock = id.Name == "seedScopeMode"
				} else if vs.Type != nil || len(vs.Values) > 0 {
					inBlock = false
				}
				if inBlock {
					for _, n := range vs.Names {
						names = append(names, n.Name)
					}
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

// s394RecordingHandle is a cacheHandle that records which write the helper used.
type s394RecordingHandle struct {
	gen                  uint64
	puts, putIfGens      int
	putThenRemarks       int
	lastCapturedGenInPut uint64
}

func (h *s394RecordingHandle) Get(string) (*cache.ResolvedEntry, bool) { return nil, false }
func (h *s394RecordingHandle) GetNoTouch(string) (*cache.ResolvedEntry, bool) {
	return nil, false
}
func (h *s394RecordingHandle) Put(string, *cache.ResolvedEntry) { h.puts++ }
func (h *s394RecordingHandle) CaptureGen(string) uint64         { return h.gen }
func (h *s394RecordingHandle) PutThenRemark(context.Context, string, *cache.ResolvedEntry) {
	h.putThenRemarks++
}
func (h *s394RecordingHandle) PutIfGen(_ context.Context, _ string, _ *cache.ResolvedEntry, g uint64) bool {
	h.putIfGens++
	h.lastCapturedGenInPut = g
	return g == h.gen
}

func TestS394_EverySeedModeIsClassified_PostReadyzModesAreGuarded(t *testing.T) {
	// guardedByMode is the classification table. A new seedScopeMode constant
	// fails below until it is added here — and only seedModeBoot may be false.
	guardedByMode := map[string]struct {
		mode    seedScopeMode
		guarded bool
	}{
		"seedModeBoot":          {seedModeBoot, false}, // boot = plain+remark PRE-readyz (#323, #408); guarded post-readyz (below)
		"seedModeKeepwarm":      {seedModeKeepwarm, true},
		"seedModeGVRDiscovered": {seedModeGVRDiscovered, true},
	}
	// This table classifies the PRE-readyz guard; the post-readyz boot row is the
	// #408 block after the loop.
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	declared := s394DeclaredSeedModes(t)
	if len(declared) < 3 {
		t.Fatalf("VACUOUS GUARD: found only %v seedScopeMode constants", declared)
	}
	for _, name := range declared {
		c, ok := guardedByMode[name]
		if !ok {
			t.Errorf("#394: seedScopeMode %s is not classified. Every mode except boot runs post-readyz and must "+
				"gen-guard the seed terminal Put — add it to guardedByMode (and to seedTerminalGuardFor if it is "+
				"genuinely pre-readyz)", name)
			continue
		}
		h := &s394RecordingHandle{gen: 7}
		g := seedTerminalGuardFor(c.mode, h, "k")
		if g.guarded != c.guarded {
			t.Errorf("#394: seedTerminalGuardFor(%s).guarded = %v, want %v", name, g.guarded, c.guarded)
		}
		if !seedTerminalPut(context.Background(), h, "k", &cache.ResolvedEntry{}, g) {
			t.Errorf("#394: %s: an unmoved generation must be accepted", name)
		}
		if c.guarded {
			if h.puts != 0 || h.putIfGens != 1 || h.lastCapturedGenInPut != 7 {
				t.Errorf("#394: post-readyz mode %s must write via PutIfGen(capturedGen=7) and never plain Put; "+
					"puts=%d putIfGens=%d gen=%d", name, h.puts, h.putIfGens, h.lastCapturedGenInPut)
			}
			h.gen = 8 // a removal moved the generation
			if seedTerminalPut(context.Background(), h, "k", &cache.ResolvedEntry{}, g) {
				t.Errorf("#394: %s: a moved generation must be REFUSED", name)
			}
		} else if h.puts != 0 || h.putIfGens != 0 || h.putThenRemarks != 1 {
			t.Errorf("#394/#408: pre-readyz boot must stay a plain Put (#323) carrying the #375 remark "+
				"(PutThenRemark); puts=%d putIfGens=%d putThenRemarks=%d", h.puts, h.putIfGens, h.putThenRemarks)
		}
	}

	// #408 — boot after /readyz. (a) A boot unit CAPTURED post-readyz is guarded.
	// (b) A boot unit captured pre-readyz whose Put lands post-readyz (the RA
	// content tail keeps seeding after the first-nav latch) is gen-guarded with
	// the generation captured BEFORE the flip.
	cache.MarkPhase1Done()
	h := &s394RecordingHandle{gen: 7}
	if g := seedTerminalGuardFor(seedModeBoot, h, "k"); !g.guarded || g.gen != 7 {
		t.Errorf("#408: seedTerminalGuardFor(seedModeBoot) post-readyz = %+v, want guarded with gen 7", g)
	}
	cache.ResetPhase1DoneForTest()
	h = &s394RecordingHandle{gen: 7}
	pre := seedTerminalGuardFor(seedModeBoot, h, "k")
	cache.MarkPhase1Done()
	h.gen = 8 // a removal during the resolve, after the capture
	if seedTerminalPut(context.Background(), h, "k", &cache.ResolvedEntry{}, pre) {
		t.Errorf("#408: a boot Put that crossed the /readyz flip must be gen-guarded and REFUSE a moved generation")
	}
	if h.putIfGens != 1 || h.lastCapturedGenInPut != 7 || h.puts != 0 || h.putThenRemarks != 0 {
		t.Errorf("#408: crossed boot Put must be PutIfGen(pre-flip gen 7); puts=%d putIfGens=%d gen=%d putThenRemarks=%d",
			h.puts, h.putIfGens, h.lastCapturedGenInPut, h.putThenRemarks)
	}
	cache.ResetPhase1DoneForTest()

	// The ctx carrier round-trips, and a ctx without it yields the zero guard.
	want := seedTerminalGuard{guarded: true, gen: 3}
	if got := seedTerminalGuardFromContext(withSeedTerminalGuard(context.Background(), want)); got != want {
		t.Errorf("ctx carrier round-trip: got %+v want %+v", got, want)
	}
	if got := seedTerminalGuardFromContext(context.Background()); got.guarded {
		t.Errorf("a ctx without the guard must yield the zero (unguarded) guard, got %+v", got)
	}
}
