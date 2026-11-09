package jq

// compiled_test.go — the compiled-program cache (compiled.go, run 23): a
// reused program answers exactly what a fresh gojq.Parse + gojq.Compile
// answers, on the deployed portal corpus; errors are never cached; an operator
// module edit is seen at once; a program unused for a GC cycle is dropped;
// concurrent reuse is safe (run under -race).
//
//	go test ./internal/support/jq/ -run '^$' -bench CompileCache -benchmem -count=6

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/itchyny/gojq"
)

// compileFresh is the pre-cache compile: what restactions.evalFilter did
// inline before run 23's fix, with its error wrapping.
func compileFresh(query string, ml gojq.ModuleLoader) (*gojq.Code, error) {
	pq, err := gojq.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("invalid jq query %q: %w", query, err)
	}
	code, err := gojq.Compile(pq, gojq.WithModuleLoader(ml))
	if err != nil {
		return nil, fmt.Errorf("unable to compile jq query %q: %w", query, err)
	}
	return code, nil
}

// runAll drains a program, keeping every output (an error value included, as
// the last element) — the full observable result of one run.
func runAll(code *gojq.Code, data any) []any {
	var out []any
	iter := code.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			return out
		}
		if err, isErr := v.(error); isErr {
			return append(out, "error: "+err.Error())
		}
		out = append(out, v)
	}
}

// portalCorpusFilters is every RESTAction filter and stage filter of the portal
// suite deployed on krateo-057 (portal 1.8.84, the snowplow corpus testdata).
func portalCorpusFilters(tb testing.TB) []string {
	tb.Helper()
	raw, err := os.ReadFile("../../resolvers/restactions/api/testdata/portal_deployed_1.8.84_corpus.json")
	if err != nil {
		tb.Fatal(err)
	}
	var doc struct {
		RestActions []struct {
			Filter *string `json:"filter"`
			Stages []struct {
				Filter *string `json:"filter"`
			} `json:"stages"`
		} `json:"restactions"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		tb.Fatal(err)
	}
	var out []string
	for _, ra := range doc.RestActions {
		if ra.Filter != nil {
			out = append(out, *ra.Filter)
		}
		for _, s := range ra.Stages {
			if s.Filter != nil {
				out = append(out, *s.Filter)
			}
		}
	}
	if len(out) < 150 {
		tb.Fatalf("corpus: want the deployed portal's ~190 filters, got %d", len(out))
	}
	return out
}

// corpusInputs are inputs that drive the corpus filters down different
// branches: empty, nulls, extras, lists of objects, scalars of the wrong type.
func corpusInputs() []any {
	obj := func(i int) map[string]any {
		return map[string]any{
			"apiVersion": "composition.krateo.io/v1-2-0", "kind": "FireworksApp",
			"metadata": map[string]any{
				"name": fmt.Sprintf("app-%d", i), "namespace": "demo-system",
				"creationTimestamp": "2026-10-01T10:00:00Z",
				"labels":            map[string]any{"krateo.io/composition-definition-name": "fireworksapps"},
			},
			"status": map[string]any{"conditions": []any{
				map[string]any{"type": "Ready", "status": "True", "reason": "Available", "lastTransitionTime": "2026-10-01T10:00:00Z"},
			}},
		}
	}
	list := map[string]any{"items": []any{obj(1), obj(2), obj(3)}}
	return []any{
		nil,
		map[string]any{},
		map[string]any{"namespace": "demo-system", "name": "app-1", "status": "failed", "q": "app", "range": "7d", "username": "alice"},
		map[string]any{"items": []any{obj(1), obj(2)}, "list": list, "compositions": list, "allCompositions": list, "crds": list, "bindings": list, "roles": list},
		[]any{obj(1), "x", 3.0, nil},
		"a string",
		42,
	}
}

// TestCompileCache_CorpusIdenticalToFreshCompile — every corpus filter, on every
// input, run twice through the cached program (the second run reuses it) and
// once through a fresh compile: identical outputs and identical errors.
func TestCompileCache_CorpusIdenticalToFreshCompile(t *testing.T) {
	inputs := corpusInputs()
	for i, q := range portalCorpusFilters(t) {
		fresh, ferr := compileFresh(q, ModuleLoader())
		for round := 0; round < 2; round++ {
			cached, cerr := Compile(q)
			if fmt.Sprint(cerr) != fmt.Sprint(ferr) {
				t.Fatalf("filter %d: cached compile error %v, fresh %v", i, cerr, ferr)
			}
			if ferr != nil {
				continue
			}
			for j, in := range inputs {
				got, want := runAll(cached, in), runAll(fresh, in)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("filter %d input %d round %d: cached %#v, fresh %#v", i, j, round, got, want)
				}
			}
		}
	}
}

// entryOf is the cache's entry for q, or nil.
func (c *programCache) entryOf(q string) *programEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.entries[q]
}

func (c *programCache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// TestCompileCache_ReusesProgramInUse — the same source compiles once while it
// is in use, across GC cycles.
func TestCompileCache_ReusesProgramInUse(t *testing.T) {
	c := newProgramCache(processPrograms.loader)
	q := `[.[] | select(.n > 1)] | length`
	a, err := c.compile(q)
	if err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 5; cycle++ {
		b, err := c.compile(q)
		if err != nil {
			t.Fatal(err)
		}
		if a != b || c.compiles.Load() != 1 {
			t.Fatalf("cycle %d: program in use not reused: same=%v compiles=%d", cycle, a == b, c.compiles.Load())
		}
		c.sweep()
	}
}

// TestCompileCache_SweepDropsUnusedPrograms — the bound: a program not used for
// a whole GC cycle is dropped and its source compiles again; a program used in
// every cycle stays. The cache holds what is in use, not what it has seen.
func TestCompileCache_SweepDropsUnusedPrograms(t *testing.T) {
	c := newProgramCache(processPrograms.loader)
	const n = 64
	for i := 0; i < n; i++ {
		if _, err := c.compile(fmt.Sprintf(`.x + %d`, i)); err != nil {
			t.Fatal(err)
		}
	}
	const hot = `.x + 0`
	c.sweep() // end of the cycle they were compiled in: all kept
	if c.size() != n {
		t.Fatalf("after one cycle: %d cached, want all %d", c.size(), n)
	}
	if _, err := c.compile(hot); err != nil { // used in the next cycle
		t.Fatal(err)
	}
	c.sweep() // end of a cycle in which only hot was used
	if c.size() != 1 || c.entryOf(hot) == nil {
		t.Fatalf("after an unused cycle: %d cached, want only the one in use", c.size())
	}
	before := c.compiles.Load()
	if _, err := c.compile(`.x + 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.compile(hot); err != nil {
		t.Fatal(err)
	}
	if c.compiles.Load() != before+1 {
		t.Fatalf("compiles %d → %d: want only the dropped source to compile again", before, c.compiles.Load())
	}
}

// TestCompileCache_GCDrivesTheSweep — the process cache is swept by the GC: a
// program nothing uses leaves it within a few cycles.
func TestCompileCache_GCDrivesTheSweep(t *testing.T) {
	q := fmt.Sprintf(`.gc_driven_sweep_%d`, os.Getpid())
	if _, err := Compile(q); err != nil {
		t.Fatal(err)
	}
	if processPrograms.entryOf(q) == nil {
		t.Fatal("not cached")
	}
	for i := 0; i < 100 && processPrograms.entryOf(q) != nil; i++ {
		runtime.GC() // cleanups run on their own goroutine after each cycle
		runtime.Gosched()
	}
	if processPrograms.entryOf(q) != nil {
		t.Fatal("an unused program survived 100 GC cycles: the GC does not drive the sweep")
	}
}

// TestCompileCache_ErrorsAreFreshEveryTime — a parse error and a compile error
// are a fresh compile's, word for word, and are never cached.
func TestCompileCache_ErrorsAreFreshEveryTime(t *testing.T) {
	c := newProgramCache(processPrograms.loader)
	for _, q := range []string{`.[`, `nosuchfunction(1)`, `include "nosuchmodule"; .`, `import "x" as $data; $data`, `$undefined`} {
		_, want := compileFresh(q, ModuleLoader())
		if want == nil {
			t.Fatalf("%q: want an error", q)
		}
		for round := 0; round < 3; round++ {
			before := c.compiles.Load()
			_, got := c.compile(q)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("%q: error %v, fresh %v", q, got, want)
			}
			if c.compiles.Load() != before+1 {
				t.Fatalf("%q: an error was served from the cache", q)
			}
		}
	}
}

// TestCompileCache_BuiltinModules — a program that includes or imports a
// built-in module is cached (the embedded source cannot change) and records no
// operator file.
func TestCompileCache_BuiltinModules(t *testing.T) {
	c := newProgramCache(processPrograms.loader)
	for _, q := range []string{
		`include "health"; [.[] | normalize_health]`,
		`import "health" as h; [.[] | h::normalize_health]`,
		`include "quantity"; .`,
	} {
		code, err := c.compile(q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		e := c.entryOf(q)
		if len(e.custom) != 0 {
			t.Fatalf("%q: recorded operator files %v for a built-in module", q, e.custom)
		}
		fresh, _ := compileFresh(q, ModuleLoader())
		in := []any{map[string]any{"health": "Healthy"}, map[string]any{"status": map[string]any{}}}
		if got, want := runAll(code, in), runAll(fresh, in); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q: cached %#v, fresh %#v", q, got, want)
		}
		again, _ := c.compile(q)
		if again != code {
			t.Fatalf("%q: a built-in-module program was not reused", q)
		}
	}
}

// TestCompileCache_OperatorModuleEditIsSeen — a program that includes an
// operator module (JQ_MODULES_PATH, a ConfigMap the kubelet updates in place)
// is reused while the file is byte-identical, compiled again the moment it
// changes, and fails exactly as a fresh compile once it is gone.
func TestCompileCache_OperatorModuleEditIsSeen(t *testing.T) {
	dir := t.TempDir()
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)
	c := newProgramCache(func() *layeredModuleLoader { return l })
	file := filepath.Join(dir, "site.jq")
	write := func(src string) {
		if err := os.WriteFile(file, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const q = `include "site"; [.[] | label_of]`
	in := []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}}
	check := func(stage string, wantCompiles int64) *gojq.Code {
		t.Helper()
		code, err := c.compile(q)
		fresh, ferr := compileFresh(q, l)
		if fmt.Sprint(err) != fmt.Sprint(ferr) {
			t.Fatalf("%s: error %v, fresh %v", stage, err, ferr)
		}
		if c.compiles.Load() != wantCompiles {
			t.Fatalf("%s: compiles=%d, want %d", stage, c.compiles.Load(), wantCompiles)
		}
		if err != nil {
			return nil
		}
		if got, want := runAll(code, in), runAll(fresh, in); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: cached %#v, fresh %#v", stage, got, want)
		}
		return code
	}

	write(`def label_of: "v1-" + .name;`)
	v1 := check("first compile", 1)
	if again := check("unchanged file", 1); again != v1 {
		t.Fatal("unchanged operator module: program not reused")
	}
	write(`def label_of: "v2-" + .name;`)
	v2 := check("edited file", 2)
	if got := runAll(v2, in); !reflect.DeepEqual(got, []any{[]any{"v2-a", "v2-b"}}) {
		t.Fatalf("edited module not seen: %#v", got)
	}
	write(`def label_of: "v2-" + .name;`) // rewritten, same bytes
	check("rewritten with the same bytes", 2)
	write(`def label_of: "v2-" + ;`)
	check("unparseable module", 3)
	check("unparseable module, again", 4)
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	check("module removed", 5)
	write(`def label_of: "v3-" + .name;`)
	check("module restored", 6)
}

// TestCompileCache_RuntimeModulemetaNotRecorded — the program keeps its loader
// for gojq's runtime `modulemeta`; a load at run time answers like the
// layered loader and records nothing into the finished compile.
func TestCompileCache_RuntimeModulemetaNotRecorded(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "site.jq"), []byte(`def f: 1;`), 0o600); err != nil {
		t.Fatal(err)
	}
	l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, dir)
	c := newProgramCache(func() *layeredModuleLoader { return l })
	const q = `"site" | modulemeta | .defs`
	code, err := c.compile(q)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := compileFresh(q, l)
	if got, want := runAll(code, nil), runAll(fresh, nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("modulemeta: cached %#v, fresh %#v", got, want)
	}
	e := c.entryOf(q)
	if len(e.custom) != 0 {
		t.Fatalf("a run-time module load was recorded: %v", e.custom)
	}

	// The loader the program keeps stops recording at finish, so a long-lived
	// program's run-time loads do not accumulate.
	rec := &recordingLoader{base: l}
	pq, err := gojq.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	prog, err := gojq.Compile(pq, gojq.WithModuleLoader(rec))
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.finish(); len(got) != 0 {
		t.Fatalf("the compile recorded %v; it reads no module", got)
	}
	for i := 0; i < 3; i++ {
		runAll(prog, nil)
	}
	rec.mu.Lock()
	n := len(rec.custom)
	rec.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d run-time module loads recorded after finish", n)
	}
}

// TestCompileCache_ConcurrentReuse — goroutines compile and run the same corpus
// programs at once (under -race: no data race on the cache or on a shared
// program), each run equal to a fresh compile's.
func TestCompileCache_ConcurrentReuse(t *testing.T) {
	filters := portalCorpusFilters(t)
	inputs := corpusInputs()
	want := make([][][]any, len(filters))
	for i, q := range filters {
		fresh, err := compileFresh(q, ModuleLoader())
		if err != nil {
			continue
		}
		want[i] = make([][]any, len(inputs))
		for j, in := range inputs {
			want[i][j] = runAll(fresh, in)
		}
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for r := 0; r < 3; r++ {
				for i, q := range filters {
					if want[i] == nil {
						continue
					}
					code, err := Compile(q)
					if err != nil {
						t.Errorf("filter %d: %v", i, err)
						return
					}
					j := (g + r + i) % len(inputs)
					if got := runAll(code, inputs[j]); !reflect.DeepEqual(got, want[i][j]) {
						t.Errorf("filter %d input %d: concurrent run %#v, fresh %#v", i, j, got, want[i][j])
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

var compileCacheSink *gojq.Code

// BenchmarkCompileCache_PortalCorpus — obtaining the program for every
// deployed portal filter once: path=before parses and compiles each (the
// pre-fix evalFilter), path=after asks the cache (steady state: every program
// is used once per op, so it stays cached across GC cycles).
func BenchmarkCompileCache_PortalCorpus(b *testing.B) {
	filters := portalCorpusFilters(b)
	b.Run("path=before", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, q := range filters {
				compileCacheSink, _ = compileFresh(q, ModuleLoader())
			}
		}
	})
	b.Run("path=after", func(b *testing.B) {
		// Start from the steady state: every program cached (a sweep during
		// the warm-up compiles can drop some; compile until none is missing).
		for pass := 0; pass < 20; pass++ {
			for _, q := range filters {
				compileCacheSink, _ = Compile(q)
			}
			missing := 0
			for _, q := range filters {
				if !IsCompiledForTest(q) {
					missing++
				}
			}
			if missing == 0 {
				break
			}
		}
		b.ReportAllocs()
		for b.Loop() {
			for _, q := range filters {
				compileCacheSink, _ = Compile(q)
			}
		}
	})
}
