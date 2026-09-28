// shadow_hook.go — v7 Step 2D: the DARK shadow-parity hook seam in package rbac.
//
// WHAT THIS IS. EvaluateRBAC's defer (evaluate.go) calls a single, read-only
// hook after the live verdict is decided, so a second, independent RBAC reading
// (the requester profile R + projection P + digest, owned by the dispatchers
// package) can be compared against the served verdict. The hook is DARK: it
// changes NO verdict, NO served byte, NO cache key. It only bumps counters.
//
// WHY THE SEAM LIVES HERE. The hook BODY lives in package dispatchers (it needs
// the AccessDomain / projection / coverage types). package rbac cannot import
// dispatchers (that is the existing import direction: dispatchers → rbac). So the
// dispatchers package REGISTERS its hook function into this atomic pointer at
// init; package rbac calls it through the pointer. nil until registered.
//
// DARK-SAFETY (design §4.1, PM condition 1/2/3):
//   - shadowParityEnabled is the observability TOGGLE. DEFAULT-OFF. It is a
//     process-local runtime flag with NO env/config backing, so a fresh pod
//     always starts with it off — it is structurally unable to persist into a
//     latency-acceptance window (which runs on fresh pods). It is NOT a behavior
//     knob: it gates ONLY the dark measurement, never a verdict / byte / key.
//   - The hook fires only when snap != nil && err == nil (see the EvaluateRBAC
//     defer), i.e. on memo-hit permits and walk permit/deny — never on
//     cache-off / nil-snap / error.
//   - The hook is read-only and recover-isolated on BOTH sides (the defer here
//     is an ultimate backstop; the hook body owns dark_panic_total and its own
//     recover) — a dark panic must NEVER crash a live request.

package rbac

import (
	"context"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// ShadowHookFunc is the dark shadow-parity hook signature. It receives the
// CHECK's own snapshot (so R is built at the check's generation — no generation
// skew), the check's opts, and the live verdict `allowed`. It is READ-ONLY and
// MUST NOT mutate any EvaluateRBAC return value.
type ShadowHookFunc func(ctx context.Context, snap *cache.RBACSnapshot, opts EvaluateOptions, allowed bool)

// shadowHook is the registered dark hook, nil until the dispatchers package
// registers one at init. One atomic load per EvaluateRBAC call when the toggle
// is on.
var shadowHook atomic.Pointer[ShadowHookFunc]

// shadowParityEnabled is the observability toggle for the dark shadow-parity
// subsystem. DEFAULT-OFF (atomic.Bool zero value). See the file header for why
// it cannot persist into a latency-acceptance window and why it is not a
// behavior knob.
var shadowParityEnabled atomic.Bool

// SetShadowHook registers (or, with nil, clears) the dark shadow-parity hook.
// Called by the dispatchers package at init; production never calls it with a
// non-nil hook from anywhere else.
func SetShadowHook(fn ShadowHookFunc) {
	if fn == nil {
		shadowHook.Store(nil)
		return
	}
	shadowHook.Store(&fn)
}

// SetShadowParityEnabled turns the dark shadow-parity measurement on/off. An
// observability control calls this; it gates nothing that changes behavior.
func SetShadowParityEnabled(on bool) { shadowParityEnabled.Store(on) }

// ShadowParityEnabled reports whether the dark shadow-parity toggle is on. The
// dispatcher entry reads it to decide whether to install the (otherwise wasted)
// shadow context.
func ShadowParityEnabled() bool { return shadowParityEnabled.Load() }
