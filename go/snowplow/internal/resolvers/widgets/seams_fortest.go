package widgets

import (
	"context"

	"k8s.io/client-go/rest"
)

// SetResolveApiRefForTest swaps the apiRef data-source hop (resolveApiRefFn)
// and returns the restore. TEST-ONLY: it lets a package that drives the REAL
// Resolve — the prewarm pagination drain (#874) — stand a data source in for
// the apiRef RESTAction without an apiserver, so the emitted
// status.resourcesRefs.slice is the production computation. Production never
// calls it.
func SetResolveApiRefForTest(fn func(ctx context.Context, opts ResolveOptions) (map[string]any, error)) (restore func()) {
	prev := resolveApiRefFn
	resolveApiRefFn = fn
	return func() { resolveApiRefFn = prev }
}

// SetValidateObjectStatusForTest swaps the CRD-status validation hop and
// returns the restore. TEST-ONLY, the companion of SetResolveApiRefForTest.
func SetValidateObjectStatusForTest(fn func(ctx context.Context, rc *rest.Config, obj map[string]any) error) (restore func()) {
	prev := validateObjectStatusFn
	validateObjectStatusFn = fn
	return func() { validateObjectStatusFn = prev }
}
