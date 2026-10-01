// dep_gen_guard_fortest.go — #375 test seams for the dependency-generation guard.
//
// These live outside _test.go so the cross-package falsifier arms (restactions/api,
// dispatchers, apiref) can drive them, the same convention as ResetDepsForTest /
// SetDepsTestMode. Production code MUST NOT call any of them: every seam they install is
// nil in production, and none changes guard behaviour — they observe (remark observer,
// unguarded hook) or open a deterministic window (bump hook) for the arms.

package cache

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SetUnguardedPutHookForTest installs fn as the #375 (B) test-build enforcement for a
// NIL-sink accepted gen-guarded Put (a resolve entry outside WithL1KeyContext /
// WithDepGenSink). The documented test-build mechanism: an arm installs a hook that
// fails (or panics) so a drifted entry goes RED in CI; production leaves it nil and the
// drift is fail-fresh remarked + counted on unguarded_put_total. Returns a restore func.
func SetUnguardedPutHookForTest(fn func(l1Key string)) (restore func()) {
	var p *func(string)
	if fn != nil {
		p = &fn
	}
	prev := unguardedPutHook.Swap(p)
	return func() { unguardedPutHook.Store(prev) }
}

// SetDepGenRemarkObserverForTest installs fn to observe every PUT-THEN-REMARK the guard
// fires (reason "moved" | "nil_sink"). Returns a restore func.
func SetDepGenRemarkObserverForTest(fn func(l1Key, reason string)) (restore func()) {
	var p *func(string, string)
	if fn != nil {
		p = &fn
	}
	prev := depGenRemarkObserver.Swap(p)
	return func() { depGenRemarkObserver.Store(prev) }
}

// SetDepBumpHookForTest installs fn at bumpCoordinateGen's two (D) windows ("after_add",
// "after_store"). Returns a restore func.
func SetDepBumpHookForTest(fn func(stage string)) (restore func()) {
	var p *func(string)
	if fn != nil {
		p = &fn
	}
	prev := depBumpHook.Swap(p)
	return func() { depBumpHook.Store(prev) }
}

// DepEventSeqForTest returns the process-global dep-event sequence.
func DepEventSeqForTest() uint64 { return depEventSeq.Load() }

// DepBucketLastBumpSeqForTest returns the lastBumpSeq of the forward bucket for
// (gvr, namespace, name) — name "" means the LIST wildcard — and whether it exists.
func DepBucketLastBumpSeqForTest(gvr schema.GroupVersionResource, namespace, name string) (uint64, bool) {
	if name == "" {
		name = listWildcard
	}
	ksI, ok := Deps().forward.Load(DepKey{GVR: gvr, Namespace: namespace, Name: name})
	if !ok {
		return 0, false
	}
	return ksI.(*keySet).lastBumpSeq.Load(), true
}

// DepGenSinkForTest reports ctx's dep-gen sink: its startSeq, a copy of the recorded
// deps, and whether a sink is installed at all (nil sink ⇒ ok=false).
func DepGenSinkForTest(ctx context.Context) (startSeq uint64, deps []DepKey, ok bool) {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return 0, nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startSeq, append([]DepKey(nil), s.deps...), true
}

// ResetUnguardedPutTotalForTest zeroes the #375 detector on the Deps() singleton.
func ResetUnguardedPutTotalForTest() { Deps().unguardedPutTotal.Store(0) }
