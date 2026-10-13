// Package jq provides the shared gojq module loader used across snowplow's
// resolvers. ModuleLoader builds (once) a layered loader: modules from the
// directory named by the JQ_MODULES_PATH env var (operator-supplied,
// wins on name collision) over the built-in modules embedded in this
// package (modules/*.jq — e.g. `health`, the normalized health/usage
// aggregation semantics), so jq expressions in RESTActions and widgets
// can import common helper modules with zero deployment configuration.
package jq

import (
	"embed"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/jqutil"
)

const (
	EnvModulesPath = "JQ_MODULES_PATH"
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
// returns nil: with JQ_MODULES_PATH unset only the built-in embedded
// modules resolve; with it set, filesystem modules take precedence (an
// operator can override a built-in by shipping a module with the same
// name — built-ins must never shadow operator content).
func ModuleLoader() gojq.ModuleLoader {
	once.Do(func() {
		var fallback gojq.ModuleLoader
		if basePath, ok := os.LookupEnv(EnvModulesPath); ok {
			basePath = strings.TrimSpace(basePath)
			if basePath != "" {
				fallback = jqutil.DirModuleLoader(basePath)
			}
		}

		cachedLoader = &layeredModuleLoader{primary: fallback}
	})
	return cachedLoader
}

// layeredModuleLoader resolves a module name through the primary
// (filesystem) loader first, then falls back to the embedded built-ins.
type layeredModuleLoader struct {
	primary gojq.ModuleLoader
}

func (l *layeredModuleLoader) LoadModule(name string) (*gojq.Query, error) {
	if l.primary != nil {
		if ml, ok := l.primary.(interface {
			LoadModule(string) (*gojq.Query, error)
		}); ok {
			if q, err := ml.LoadModule(name); err == nil {
				return q, nil
			}
		}
	}

	dat, err := builtinModulesFS.ReadFile("modules/" + name + ".jq")
	if err != nil {
		return nil, fmt.Errorf("module %q not found: %w", name, err)
	}

	parsed, err := gojq.Parse(string(dat))
	if err != nil {
		return nil, fmt.Errorf("error parsing built-in module %q: %w", name, err)
	}

	return parsed, nil
}
