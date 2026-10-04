// inert.go — #443 part 2. The single "persist nothing" flag carried by an
// inline dry-run resolve (POST /call/read with a RESTAction "object" body).
//
// An inline RESTAction is vouched for only by the caller, and the request is a
// dry run, so the resolve must leave no trace in the process: no L1 Put or
// touch, no dep edge, no informer registration, no cluster-list populate, no
// SSE, no learned-identity observation, no shadow parity. Rather than thread a
// boolean through every frame, ONE ctx flag is checked inside the cache
// package's own mutators and at the few registration and egress points that
// are not cache methods (design #443 §2, choke points (a)–(n)).
//
// Minting is restricted to exactly two places:
//   - middleware.BodyExtrasDecode, when the body carries a valid "object";
//   - the trusted self-loopback ingest (dispatchers.reseedInertFromHeaders),
//     which re-installs it on a nested HTTP /call hop that carried InertHeader.
//
// The default action under the flag is SKIP: a silent no-op that returns the
// "nothing stored" value. The one REFUSE is a write-verb API stage, which is
// not executed at all (resolve.go dispatchOneCall).
package cache

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// InertHeader is set on an HTTP self-loopback /call made by an inert resolve,
// so the next hop is inert too. It is honoured only on a trusted self-loopback
// (a snowplow-validated JWT arriving on the URL_SELF host), exactly like
// NestedDepthHeader and ResolveAncestorsHeader, and is never sent to an
// external host. A caller forging it affects only their own request.
const InertHeader = "X-Snowplow-Inert"

type inertCtxKey struct{}

// WithInert marks ctx as an inert (persist-nothing) resolve.
func WithInert(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, inertCtxKey{}, true)
}

// Inert reports whether ctx belongs to an inert (persist-nothing) resolve.
func Inert(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(inertCtxKey{}).(bool)
	return v
}

// closedInertCh is the already-closed channel EnsureResourceTypeFor returns
// under the inert flag, so a caller that waits for sync never blocks.
var closedInertCh = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// EnsureResourceTypeFor is the ctx-aware form of EnsureResourceType, used on
// every resolve-path registration site (#443 choke point (b)). Under the inert
// flag it registers nothing and returns (false, an already-closed channel), so
// the read falls through to the apiserver as the caller, which is what an
// unregistered GVR does today. Otherwise it delegates unchanged.
func (rw *ResourceWatcher) EnsureResourceTypeFor(ctx context.Context, gvr schema.GroupVersionResource) (added bool, sync <-chan struct{}) {
	if Inert(ctx) {
		return false, closedInertCh
	}
	return rw.EnsureResourceType(gvr)
}
