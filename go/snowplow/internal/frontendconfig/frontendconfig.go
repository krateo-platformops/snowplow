// Package frontendconfig is the one definition of WHERE the frontend's configuration lives: the
// ConfigMap the frontend reads its config.json from, in snowplow's authn namespace.
//
// Phase 1's navigation-root source (internal/handlers/dispatchers phase1_roots.go) reads it to find
// the frontend's entry points, and the Agentiko sandbox's fake apiserver
// (internal/sandbox/fakeapiserver) synthesises it under the same name, so both name it through this
// package and never restate the default. It imports nothing of snowplow's, so any package can use it
// without an import cycle.
package frontendconfig

import (
	"strings"

	"github.com/krateo-platformops/plumbing/env"
)

// Env names the env var (chart value) that points at the frontend ConfigMap. Per the
// no-special-cases contract even the ConfigMap's NAME is config: an operator who renames the
// ConfigMap re-points its readers without a code change.
const Env = "FRONTEND_CONFIG_CONFIGMAP"

// Default is the ConfigMap's name when Env is unset or empty. It is a default, not a policy.
const Default = "frontend-config-vars"

// DataKey is the one key inside the ConfigMap whose value is the frontend's config.json text.
const DataKey = "config.json"

// ConfigMapName returns the configured frontend ConfigMap name (Env, default Default). An unset or
// empty env var falls back to the default: an empty chart value means "use the default", not "no
// ConfigMap".
func ConfigMapName() string {
	if v := strings.TrimSpace(env.String(Env, "")); v != "" {
		return v
	}
	return Default
}
