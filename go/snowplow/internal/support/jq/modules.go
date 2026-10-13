// Package jq provides the shared gojq module loader used across snowplow's
// resolvers. ModuleLoader builds (once) a layered loader over two sources: the
// built-in modules embedded in this package (the files the //go:embed line
// below names — today modules/*.jq, e.g. `health`, the normalized health/usage
// aggregation semantics; that pattern does NOT descend into subdirectories, so
// a nested built-in directory needs its own pattern on that line, which
// TestIssue639_EveryModuleSourceIsEmbedded enforces) and, optionally, operator
// modules from the directory named by the JQ_MODULES_PATH env var. jq
// expressions in RESTActions and widgets can import common helper modules with
// zero deployment configuration.
//
// #639 — the two sources do not shadow each other:
//   - every built-in module name is RESERVED. A name in the embedded set always
//     resolves to the embedded module; an operator file of the same name is
//     refused (never read, never parsed) and reported once at load, by name.
//     Whoever can write the custom-modules source therefore cannot replace a
//     built-in for every resolve on the cluster.
//   - a custom module that fails to read or parse FAILS the importing filter.
//     It never falls back to anything, and the error names the parse failure.
//
// The reserved set is ENUMERATED from the embedded filesystem (recursively),
// never hand-written, so a built-in added later is reserved by being embedded.
package jq

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/itchyny/gojq"
)

const (
	EnvModulesPath = "JQ_MODULES_PATH"

	// builtinRoot is the embedded directory the built-in modules live under; a
	// module named N is the file builtinRoot + "/" + N + ".jq".
	builtinRoot = "modules"
	moduleExt   = ".jq"
)

var (
	// A module name may contain '/': `include "krateo/architecture/v1"`
	// loads modules/krateo/architecture/v1.jq. A `*.jq` pattern does not
	// descend into subdirectories, so each nested directory is listed.
	//go:embed modules/*.jq modules/krateo/architecture/*.jq
	builtinModulesFS embed.FS

	once         sync.Once
	cachedLoader gojq.ModuleLoader
)

// ModuleLoader returns the process-wide layered module loader. It never
// returns nil: with JQ_MODULES_PATH unset only the built-in embedded modules
// resolve; with it set, operator modules resolve too, under every name the
// built-in set does not reserve.
func ModuleLoader() gojq.ModuleLoader {
	once.Do(func() {
		customDir := ""
		if basePath, ok := os.LookupEnv(EnvModulesPath); ok {
			customDir = strings.TrimSpace(basePath)
		}
		l := newLayeredModuleLoader(builtinModulesFS, builtinRoot, customDir)
		logRefusedCustomModules(slog.Default(), customDir, l.refusedCustomModules())
		cachedLoader = l
	})
	return cachedLoader
}

// BuiltinModuleNames returns the reserved module names: every *.jq file in the
// embedded set, at any depth, as the name an `import`/`include` uses (the path
// below modules/, without the extension — e.g. "health",
// "krateo/architecture/v1"). Sorted.
func BuiltinModuleNames() []string {
	return builtinModuleNames(builtinModulesFS, builtinRoot)
}

// builtinModuleNames walks root in fsys recursively and returns the module
// name of every *.jq file. A walk error yields the names found so far; the
// embedded FS cannot fail, and a test FS that does is a test bug the caller's
// assertions catch.
func builtinModuleNames(fsys fs.FS, root string) []string {
	var names []string
	_ = fs.WalkDir(fsys, root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, moduleExt) {
			return nil
		}
		rel := strings.TrimPrefix(p, root+"/")
		names = append(names, strings.TrimSuffix(rel, moduleExt))
		return nil
	})
	sort.Strings(names)
	return names
}

// layeredModuleLoader resolves a reserved (built-in) name from the embedded
// set and any other name from the operator directory.
type layeredModuleLoader struct {
	builtin   fs.FS
	root      string
	reserved  map[string]struct{}
	customDir string // "" = no operator modules
}

func newLayeredModuleLoader(builtin fs.FS, root, customDir string) *layeredModuleLoader {
	l := &layeredModuleLoader{
		builtin:   builtin,
		root:      root,
		reserved:  map[string]struct{}{},
		customDir: customDir,
	}
	for _, n := range builtinModuleNames(builtin, root) {
		l.reserved[n] = struct{}{}
	}
	return l
}

func (l *layeredModuleLoader) LoadModule(name string) (*gojq.Query, error) {
	if _, ok := l.reserved[name]; ok {
		return l.loadBuiltin(name)
	}
	return l.loadCustom(name)
}

func (l *layeredModuleLoader) loadBuiltin(name string) (*gojq.Query, error) {
	dat, err := fs.ReadFile(l.builtin, path.Join(l.root, name+moduleExt))
	if err != nil {
		return nil, fmt.Errorf("built-in module %q: %w", name, err)
	}
	parsed, err := gojq.Parse(string(dat))
	if err != nil {
		return nil, fmt.Errorf("error parsing built-in module %q: %w", name, err)
	}
	return parsed, nil
}

// loadCustom reads an operator module. Only a missing file is "not found";
// every other failure — an unreadable file, a parse error, a name that would
// escape the directory — is returned as-is and fails the importing filter.
func (l *layeredModuleLoader) loadCustom(name string) (*gojq.Query, error) {
	if l.customDir == "" {
		return nil, fmt.Errorf("module %q not found", name)
	}
	p, ok := l.customPath(name)
	if !ok {
		return nil, fmt.Errorf("module %q: invalid module name", name)
	}
	dat, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("module %q not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("custom module %q: unable to read: %w", name, err)
	}
	parsed, err := gojq.Parse(string(dat))
	if err != nil {
		return nil, fmt.Errorf("custom module %q: parse error: %w", name, err)
	}
	return parsed, nil
}

// customPath maps a module name to its file in the operator directory. A name
// that is not a local path (absolute, or climbing out with "..") is refused.
func (l *layeredModuleLoader) customPath(name string) (string, bool) {
	rel := filepath.FromSlash(name) + moduleExt
	if !filepath.IsLocal(rel) {
		return "", false
	}
	return filepath.Join(l.customDir, rel), true
}

// refusedCustomModules returns the reserved names an operator file also uses:
// the custom modules this loader refuses. Names only; nothing is read.
func (l *layeredModuleLoader) refusedCustomModules() []string {
	if l.customDir == "" {
		return nil
	}
	var refused []string
	for n := range l.reserved {
		p, ok := l.customPath(n)
		if !ok {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			refused = append(refused, n)
		}
	}
	sort.Strings(refused)
	return refused
}

// logRefusedCustomModules reports each refused custom module once, by NAME.
// The module's content is never logged.
func logRefusedCustomModules(log *slog.Logger, customDir string, refused []string) {
	for _, n := range refused {
		log.Error("jq custom module refused: its name is reserved by a built-in module; the built-in is used",
			slog.String("module", n),
			slog.String("modules_dir", customDir))
	}
}
