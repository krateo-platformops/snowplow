// issue216_f6b_gone_verdict_test.go — 1.12.7 F6b, cache side.
//
// The dispatchers-side arm proves the END-TO-END outcome: after a gone verdict
// the per-binding seed no longer reaches the widget. These two arms pin the
// two properties of the FIRE SITE that the cross-package arm cannot see,
// because both turn on states that are unexported here:
//
//  1. ONLY objAbsent fires. objExists is a dirty-mark, and objUnknown /
//     objUnknownDegraded mean the informer is not authoritative. Remove on
//     positive evidence, never on absence — an informer that is not recovering
//     must not be able to empty the warm set one coordinate at a time.
//
//  2. It fires for a coordinate with NO dependent L1 entries. This is the
//     placement assertion, and it is the whole point: the population F6b
//     exists for is a harvested copy whose cell was already evicted, so
//     `matched` is empty and OnObjectEvent returns early. A fire site placed
//     after that return would miss every entry that matters.
package cache

import (
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type f6bFired struct {
	mu   sync.Mutex
	seen []string
}

func (f *f6bFired) record(gvr schema.GroupVersionResource, ns, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, gvr.String()+"|"+ns+"|"+name)
}

func (f *f6bFired) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
}

func f6bTestGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// TestIssue216_F6b_OnlyTheAbsentVerdictFires — the verdict discrimination.
func TestIssue216_F6b_OnlyTheAbsentVerdictFires(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetGoneForgetHooksForTest()
	t.Cleanup(ResetGoneForgetHooksForTest)

	var fired f6bFired
	RegisterGoneForgetHook(fired.record)

	gvr := f6bTestGVR()
	const ns, name = "demo-system", "f6b-flex"

	// A resident entry with an edge, so `matched` is non-empty and every
	// verdict below takes the full body rather than the early return.
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	const key = "L1_f6b-flex"
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &ResolvedKeyInputs{
		CacheEntryClass: "widgets", Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: ns, Name: name,
	}})
	Deps().Record(key, gvr, ns, name)

	for _, tc := range []struct {
		label string
		state objectState
	}{
		{"EXISTS", objExists},
		{"UNKNOWN", objUnknown},
		{"UNKNOWN_DEGRADED", objUnknownDegraded},
	} {
		Deps().OnObjectEvent(gvr, ns, name, tc.state)
		if n := fired.count(); n != 0 {
			t.Fatalf("RED (F6b): the %s verdict fired the gone-forget hook %d time(s). Only an "+
				"authoritative ABSENT verdict — a servable informer whose indexer does not hold "+
				"the object — may cause a harvested copy to be dropped; removing on a "+
				"non-authoritative informer empties the warm set", tc.label, n)
		}
	}

	Deps().OnObjectEvent(gvr, ns, name, objAbsent)
	if n := fired.count(); n != 1 {
		t.Fatalf("RED (F6b): the ABSENT verdict fired the gone-forget hook %d time(s), want 1", n)
	}
	if got, want := fired.seen[0], gvr.String()+"|"+ns+"|"+name; got != want {
		t.Fatalf("F6b: the hook was fired with %q, want %q", got, want)
	}
}

// TestIssue216_F6b_FiresWhenNoL1EntryDependsOnTheCoordinate — the placement
// assertion. This is the immortal shape: the harvested copy is still held, but
// nothing in L1 depends on the coordinate any more (the cell was evicted, or
// its edges were stripped). OnObjectEvent returns early on an empty match set,
// so a fire site below that return reaches nothing.
//
// RED: move notifyObjectGone below the `len(matched) == 0` return.
func TestIssue216_F6b_FiresWhenNoL1EntryDependsOnTheCoordinate(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetGoneForgetHooksForTest()
	t.Cleanup(ResetGoneForgetHooksForTest)

	var fired f6bFired
	RegisterGoneForgetHook(fired.record)

	gvr := f6bTestGVR()
	const ns, name = "demo-system", "orphaned-flex"

	// Deliberately NO Put and NO Record: nothing in L1 depends on this
	// coordinate, exactly like a widget whose cell the audit already repaired.
	if n := len(Deps().collectMatchesWithDep(gvr, ns, name)); n != 0 {
		t.Fatalf("premise: expected an empty match set, got %d", n)
	}

	Deps().OnObjectEvent(gvr, ns, name, objAbsent)

	if n := fired.count(); n != 1 {
		t.Fatalf("RED (F6b): an ABSENT verdict for a coordinate with NO dependent L1 entry fired "+
			"the hook %d time(s), want 1. This is precisely the population the hook exists for — "+
			"a harvested copy whose cell is already gone — and OnObjectEvent returns early on an "+
			"empty match set, so a fire site placed after that return never reaches it", n)
	}
}

// TestIssue216_F6b_RegistrationIsIdempotent — the same fn pointer registered
// twice must fire once, mirroring RegisterGVRDiscoveredHook. Guards against a
// double-wire firing the harvester forget twice per verdict.
func TestIssue216_F6b_RegistrationIsIdempotent(t *testing.T) {
	ResetGoneForgetHooksForTest()
	t.Cleanup(ResetGoneForgetHooksForTest)
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	var fired f6bFired
	RegisterGoneForgetHook(fired.record)
	RegisterGoneForgetHook(fired.record) // duplicate — must be a no-op
	RegisterGoneForgetHook(nil)          // nil — must be ignored

	Deps().OnObjectEvent(f6bTestGVR(), "demo-system", "dup-flex", objAbsent)
	if n := fired.count(); n != 1 {
		t.Fatalf("F6b: a duplicate registration fired the hook %d time(s), want 1", n)
	}
}
