// internal_dispatch_predial.go — #677 (1): branch C's authorize-before-dial
// step for a per-identity GET-by-name, and the typed re-gate Forbidden that both
// branch C deny points return.
//
// See the call site in dispatchViaInternalRESTConfig for the why. This file
// holds the pieces that have to be shared or seamed:
//
//   - preDialAuthzNamespace decides WHICH namespace the pre-dial evaluation
//     runs against, and refuses to decide when it cannot be sure: a namespaced
//     GVR whose ref carries no namespace is never authorized cluster-wide, and a
//     scope lookup that fails falls back to the post-fetch path.
//   - newRegateForbidden / isRegateDenied: a per-identity re-gate deny is a
//     *apierrors.StatusError Forbidden (byte-identical message to the pre-#677
//     post-fetch deny, the #681 canonical identity-free text) wrapped in a
//     marker type, so resolve.go classifies it as a per-identity forbidden (F1)
//     without parsing text.
//   - snowplow_internal_dispatch_predial_total{outcome}: a closed set.

package api

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// preDialScopeFn resolves whether a GVR is namespaced, through the process-wide
// SA discovery singleton (shape metadata only). A seam ONLY so the hermetic arms
// can answer without a cluster; production never reassigns it.
var preDialScopeFn = dynamic.SharedSAScopeForGVR

// postFetchRegateFn is the post-fetch re-gate. A seam ONLY so the P3-m mutation
// arm can remove it and prove it is load-bearing; production never reassigns it.
var postFetchRegateFn = filterGetByRBAC

// preDialAuthzNamespace returns the namespace the pre-dial `get` evaluation runs
// against, and ok=false when the pre-dial step must NOT decide (fall back to the
// post-fetch path, which judges the object the apiserver returns):
//
//   - a non-empty ref namespace is used as is (a namespaced GET);
//   - an empty ref namespace is a cluster-wide evaluation ONLY when discovery
//     says the GVR is cluster-scoped. A namespaced GVR with an empty namespace,
//     or a scope that cannot be resolved, falls back.
func preDialAuthzNamespace(gvr schema.GroupVersionResource, namespace string) (string, bool) {
	if namespace != "" {
		return namespace, true
	}
	namespaced, err := preDialScopeFn(gvr)
	if err != nil || namespaced {
		return "", false
	}
	return "", true
}

// regateDeniedError marks a branch C per-identity re-gate deny. It unwraps to the
// Forbidden *StatusError, so apierrors.IsForbidden and the error text are
// exactly the pre-#677 ones.
type regateDeniedError struct{ status *apierrors.StatusError }

func (e *regateDeniedError) Error() string { return e.status.Error() }
func (e *regateDeniedError) Unwrap() error { return e.status }

// newRegateForbidden builds the deny both branch C deny points return. Its text
// IS the #681 canonical identity-free message (canonicalForbiddenMessage).
func newRegateForbidden(gvr schema.GroupVersionResource, namespace, name string) error {
	return &regateDeniedError{status: apierrors.NewForbidden(gvr.GroupResource(), name,
		fmt.Errorf("user not authorized to get %s/%s", namespace, name))}
}

// isRegateDenied reports whether err is a branch C per-identity re-gate deny.
func isRegateDenied(err error) bool {
	var e *regateDeniedError
	return errors.As(err, &e)
}

// --- snowplow_internal_dispatch_predial_total{outcome} -----------------------

const (
	preDialAllowed       = "allowed"
	preDialDenied        = "denied"
	preDialScopeFallback = "scope_fallback"
)

// PreDialOutcomes is the closed outcome set, in emit order.
var PreDialOutcomes = []string{preDialAllowed, preDialDenied, preDialScopeFallback}

var preDialCounts [3]atomic.Uint64

func recordPreDial(outcome string) {
	for i, o := range PreDialOutcomes {
		if o == outcome {
			preDialCounts[i].Add(1)
			return
		}
	}
}

// PreDialCount is one {outcome} cell.
type PreDialCount struct {
	Outcome string
	Count   uint64
}

// PreDialSnapshot returns every outcome cell, zeros included.
func PreDialSnapshot() []PreDialCount {
	out := make([]PreDialCount, len(PreDialOutcomes))
	for i, o := range PreDialOutcomes {
		out[i] = PreDialCount{Outcome: o, Count: preDialCounts[i].Load()}
	}
	return out
}

// RecordPreDialForTest moves one outcome cell through the production counter.
// Tests only.
func RecordPreDialForTest(outcome string) { recordPreDial(outcome) }

var preDialExpvarOnce sync.Once

// CFG-1: the same Disabled() gate as every other cache-side publisher in this
// package.
func init() {
	if cache.Disabled() {
		return
	}
	preDialExpvarOnce.Do(func() {
		expvar.Publish("snowplow_internal_dispatch_predial_total", expvar.Func(func() any {
			m := map[string]uint64{}
			for _, c := range PreDialSnapshot() {
				m[c.Outcome] = c.Count
			}
			return m
		}))
	})
}

// compile-time: the seam keeps filterGetByRBAC's signature.
var _ func(context.Context, schema.GroupVersionResource, *unstructured.Unstructured) bool = postFetchRegateFn
