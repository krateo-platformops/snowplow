package jq

// compiled.go — run 23 CPU profile: the RESTAction filter re-parsed and
// re-compiled its jq program on every resolve (gojq.Parse + gojq.Compile
// ≈0.56% of all CPU from restactions.evalFilter alone). Compile hands back the
// program an earlier call compiled from the same source.
//
// WHAT A PROGRAM DEPENDS ON. gojq.Compile's output is a function of the query
// text, the compiler options, and the source of every module the query
// imports or includes (read through the loader at compile time). Compile always
// uses the same option — WithModuleLoader(ModuleLoader()), the process loader —
// so an entry is keyed by the query text and carries the module sources the
// compile read that can change:
//   - built-in modules are embedded in the binary: they cannot change;
//   - operator modules are files under JQ_MODULES_PATH (a ConfigMap mount the
//     kubelet updates in place): every one the compile read is recorded with
//     its exact bytes, and a lookup re-reads it and reuses the program only if
//     every byte is unchanged. Otherwise it compiles again, which reads the
//     new file, or fails exactly as a fresh compile does.
//
// Errors are never cached: a source that fails to parse or compile is parsed
// and compiled again on every call, so its error is a fresh compile's.
//
// THE BOUND. A program stays cached while it is used at least once per GC
// cycle: at the end of every cycle the programs not used since the previous
// one are dropped (the primary/victim scheme sync.Pool uses for the same
// reason). So the cache holds the programs of the filters in use — the
// RESTActions being resolved now — and nothing else for longer than two GC
// cycles: a filter that changed or was deleted, and an inline RESTAction's
// one-off filter, fall out by themselves. The GC paces the sweep, so there is
// nothing to tune: no size, no TTL.
//
// CONCURRENT REUSE. A *gojq.Code is immutable once compiled: Run builds a
// fresh env per call (gojq: "It is safe to call this method in goroutines, to
// reuse a compiled *Code"; the krateo fork keeps that — execute only reads
// bc.codes, and the compile-time regexp cache it carries is a sync.Map).
//
// SHARED CONSTANTS. A cached program's constant values (a literal object or
// array the compiler folded) are shared by every run of it, so a program's
// outputs, which can be those constants, must be treated as read-only by its
// caller. Every caller of Compile documents why its outputs are.

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/itchyny/gojq"
)

// moduleSource is one operator module file a compile read, as it read it.
type moduleSource struct {
	path string
	src  []byte
}

// programEntry is one cached program and the operator module files its
// compile read. used is set by every lookup that returns it and cleared by
// every sweep.
type programEntry struct {
	code   *gojq.Code
	custom []moduleSource
	used   atomic.Bool
}

// programCache maps a query's source to its compiled program (see the file
// comment for what an entry depends on and what bounds it).
type programCache struct {
	loader   func() *layeredModuleLoader
	mu       sync.RWMutex
	entries  map[string]*programEntry
	compiles atomic.Int64 // compiles performed (misses); a test seam
}

func newProgramCache(loader func() *layeredModuleLoader) *programCache {
	return &programCache{loader: loader, entries: map[string]*programEntry{}}
}

var processPrograms = newProgramCache(func() *layeredModuleLoader {
	return ModuleLoader().(*layeredModuleLoader)
})

func init() {
	afterEveryGC(processPrograms.sweep)
}

// Compile parses and compiles query with WithModuleLoader(ModuleLoader()) —
// what gojq.Parse + gojq.Compile with that option return — reusing the program
// an earlier call compiled from the same source while it is in use and every
// operator module it read is unchanged.
//
// The errors are the ones the RESTAction filter has always returned: a parse
// error as `invalid jq query %q: %w`, a compile error as
// `unable to compile jq query %q: %w`.
//
// The program's outputs can be its constants, shared with every other run of
// it: the caller must not modify them.
func Compile(query string) (*gojq.Code, error) {
	return processPrograms.compile(query)
}

// IsCompiledForTest reports whether the process cache holds a program for
// query. Test seam only: benchmarks use it to start from the steady state.
func IsCompiledForTest(query string) bool {
	processPrograms.mu.RLock()
	defer processPrograms.mu.RUnlock()
	_, ok := processPrograms.entries[query]
	return ok
}

func (c *programCache) compile(query string) (*gojq.Code, error) {
	if code := c.lookup(query); code != nil {
		return code, nil
	}
	code, custom, err := c.compileRecording(query)
	if err != nil {
		return nil, err
	}
	e := &programEntry{code: code, custom: custom}
	e.used.Store(true)
	c.mu.Lock()
	c.entries[query] = e
	c.mu.Unlock()
	return code, nil
}

// lookup returns the cached program for query whose operator modules are
// unchanged, or nil.
func (c *programCache) lookup(query string) *gojq.Code {
	c.mu.RLock()
	e := c.entries[query]
	c.mu.RUnlock()
	if e == nil {
		return nil
	}
	for _, m := range e.custom {
		src, err := os.ReadFile(m.path)
		if err != nil || !bytes.Equal(src, m.src) {
			return nil
		}
	}
	if !e.used.Load() {
		e.used.Store(true)
	}
	return e.code
}

// sweep drops every program not used since the previous sweep and starts a
// new cycle for the rest.
func (c *programCache) sweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for q, e := range c.entries {
		if !e.used.Swap(false) {
			delete(c.entries, q)
		}
	}
}

// afterEveryGC runs f once after every GC cycle, on the runtime's cleanup
// goroutine: a sentinel is allocated unreachable, and its cleanup — run after
// the cycle that frees it — calls f and allocates the next one. The sentinel
// holds a pointer so it is never tiny-allocated (a tiny block can outlive its
// object and delay the cleanup).
func afterEveryGC(f func()) {
	type sentinel struct{ _ *byte }
	var arm func()
	arm = func() {
		runtime.AddCleanup(new(sentinel), func(struct{}) {
			f()
			arm()
		}, struct{}{})
	}
	arm()
}

// compileRecording is gojq.Parse + gojq.Compile(WithModuleLoader(loader)) that
// also returns every operator module file the compile read.
func (c *programCache) compileRecording(query string) (*gojq.Code, []moduleSource, error) {
	c.compiles.Add(1)
	pq, err := gojq.Parse(query)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid jq query %q: %w", query, err)
	}
	rec := &recordingLoader{base: c.loader()}
	code, err := gojq.Compile(pq, gojq.WithModuleLoader(rec))
	custom := rec.finish()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to compile jq query %q: %w", query, err)
	}
	return code, custom, nil
}

// recordingLoader is the layered loader with the same answers (it implements
// the same single method, LoadModule, so gojq takes the same paths through
// it), recording the operator module files read until finish. The compiled
// program keeps its loader (gojq's runtime `modulemeta` loads through it), so
// a load after finish delegates without recording: a long-lived program's
// run-time loads do not accumulate.
type recordingLoader struct {
	base *layeredModuleLoader

	mu       sync.Mutex
	finished bool
	custom   []moduleSource
}

func (r *recordingLoader) LoadModule(name string) (*gojq.Query, error) {
	if _, ok := r.base.reserved[name]; ok {
		return r.base.loadBuiltin(name)
	}
	q, read, err := r.base.loadCustomSource(name)
	if read != nil {
		r.mu.Lock()
		if !r.finished {
			r.custom = append(r.custom, *read)
		}
		r.mu.Unlock()
	}
	return q, err
}

// finish stops recording and returns what the compile read.
func (r *recordingLoader) finish() []moduleSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finished = true
	return r.custom
}
