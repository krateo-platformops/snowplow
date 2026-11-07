// put_admission.go — the store's admission of a write (#1233).
//
// THE RULE (Diego, 2026-11-07): while the RBAC snapshot is incomplete, no writer
// stores a shared cache entry. A key folds the requester's binding set as the
// snapshot reports it (SubjectBindingSet, #423), and a snapshot published during
// the RBAC informers' initial LIST lacks bindings. Two identities whose real
// bindings differ can then derive one key, and whatever one of them resolved
// would be served to the other. #1221 and #1229 were two routes to that; #1233
// is the impersonated refresher read (#574, #615), which no re-gate covers.
//
// So the rule is not a writer's: it is the store's. admitWrite is the ONE check
// every write runs before it touches a cell, whoever is writing: a user's /call,
// the refresher, the prewarm walk and the seeds, the content prewarm, the
// apistage and cluster-list content cells. There is no per-writer exception, and
// no per-class one either: an identity-free cell (widgetContent, apistage) is
// the most shared cell of all, and refusing it costs the same as refusing the
// rest.
//
// WHERE. Every write of the store reaches putCoreLocked, the only code that
// inserts into the index. Each function that calls putCoreLocked admits first
// (putPlain, putIfGen, replaceIfGen, replaceIfGenRefresh), and every exported
// write reaches one of those. scripts/checkresolvedentrysites (site-class 6)
// fails the build on a putCoreLocked call that does not return on !admitWrite
// first, and on an index insertion anywhere but putCoreLocked. So a new Put site
// cannot skip it.
//
// THE RESOLVE'S START, NOT ONLY THE PUT. A resolve that began on an incomplete
// snapshot is refused even when its Put lands after the snapshot completed. Its
// body is what the incomplete snapshot let it read, and a missing Role or
// ClusterRole moves no key dimension (binding set and sub-generation are equal
// on both snapshots, #682), so the #424 identity-class guard cannot see it. The
// ExternalTouchedSink every Put-capable resolve installs at its start records
// whether the snapshot was complete then; a memo hit replays its producer's
// (put_gate_effects.go). The key is minted before the resolve starts, so a key
// minted on an incomplete snapshot belongs to a resolve the store refuses.
//
// THE PREWARM WAITS. The prewarm and the seeds start their writing work only
// once the complete snapshot is published (WaitRBACSnapshotComplete), so no
// boot unit is refused and left cold (#506).
//
// THE COST. Every write in the window is refused, so the window serves uncached.
// Once #1231 lands (Ready waits for a complete snapshot) that window is boot
// before Ready, where only the prewarm runs. Until then a pod whose RBAC lists
// take longer than 60 s can go Ready inside it. snowplow_l1_put_declined_rbac_incomplete
// counts every refused write by writer and by cell class, so 057 and run 22 can
// read it.

package cache

import (
	"context"
	"expvar"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// rbacSnapshotComplete reports whether the published RBAC snapshot is complete:
// one exists, and it was built after every RBAC informer finished its initial
// LIST (#947 Synced). No snapshot at all is not complete.
func rbacSnapshotComplete() bool {
	s := LiveRBACSnapshot()
	return s != nil && s.Synced
}

// RBACSnapshotComplete reports whether the published RBAC snapshot is complete:
// one exists, built after every RBAC informer finished its initial LIST.
func RBACSnapshotComplete() bool { return rbacSnapshotComplete() }

// admitWrite is the store's admission of one write. It returns false, and the
// caller stores nothing, when:
//   - the resolve is inert (#443 (a): a dry run persists nothing; not counted as
//     an RBAC decline, whatever the snapshot is), or
//   - the RBAC snapshot is not complete now, or was not when the resolve began
//     (#1233; the ExternalTouchedSink every Put-capable resolve installs at its
//     start carries that, and a memo hit replays its producer's). A body read
//     on an incomplete snapshot is filtered by it even when the Put lands after
//     the snapshot completed: a Role the initial LIST had not delivered moves no
//     key dimension, so no identity-class guard can see it. That refusal is
//     counted.
func admitWrite(ctx context.Context, entry *ResolvedEntry) bool {
	if Inert(ctx) {
		return false
	}
	if !rbacSnapshotComplete() || ExternalTouchedSinkFromContext(ctx).StartedIncomplete() {
		notePutDeclinedRBACIncomplete(ctx, entry)
		return false
	}
	return true
}

// putClassUnknown labels an entry that carries no Inputs.
const putClassUnknown = "unknown"

// putDeclinedRBACIncomplete counts the writes admitWrite refused because the
// snapshot was not complete, keyed "<writer>/<class>". The writer is the driver
// RefDenialOrigin names (refresher, cohort-seed, prewarm-engine-boot, the content
// prewarm's prewarm-path, background-unattributed, or serve for a user's /call);
// the class is the entry's cache entry class. Both sets are closed and small.
var putDeclinedRBACIncomplete sync.Map // string → *atomic.Uint64

func notePutDeclinedRBACIncomplete(ctx context.Context, entry *ResolvedEntry) {
	writer := RefDenialOrigin(ctx)
	class := putClassUnknown
	if entry != nil && entry.Inputs != nil && entry.Inputs.CacheEntryClass != "" {
		class = entry.Inputs.CacheEntryClass
	}
	key := writer + "/" + class
	v, _ := putDeclinedRBACIncomplete.LoadOrStore(key, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

// PutDeclinedRBACIncompleteSnapshot returns the refused writes by
// "<writer>/<class>", plus "total".
func PutDeclinedRBACIncompleteSnapshot() map[string]uint64 {
	out := map[string]uint64{}
	var total uint64
	putDeclinedRBACIncomplete.Range(func(k, v any) bool {
		n := v.(*atomic.Uint64).Load()
		out[k.(string)] = n
		total += n
		return true
	})
	out["total"] = total
	return out
}

// PutDeclinedRBACIncompleteTotal is the number of writes refused because the
// RBAC snapshot was not complete.
func PutDeclinedRBACIncompleteTotal() uint64 {
	return PutDeclinedRBACIncompleteSnapshot()["total"]
}

// PutDeclinedRBACIncompleteKeys returns the "<writer>/<class>" keys counted so
// far, sorted.
func PutDeclinedRBACIncompleteKeys() []string {
	var out []string
	putDeclinedRBACIncomplete.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	sort.Strings(out)
	return out
}

// CompleteRBACSnapshotForTest publishes a complete RBAC snapshot for one test
// and restores the previous one at its cleanup. A test that writes the store
// states that the snapshot it writes under is complete (#1233): the published
// one, marked complete, or an empty complete one when none is published.
// Production code MUST NOT call this: production publishes go through
// rebuildRBACSnapshot only.
func CompleteRBACSnapshotForTest(t interface{ Cleanup(func()) }) {
	prev := rbacSnap.Load()
	next := &RBACSnapshot{Synced: true}
	if prev != nil {
		cp := *prev
		cp.Synced = true
		next = &cp
	}
	PublishRBACSnapshotForTest(next)
	t.Cleanup(func() { PublishRBACSnapshotForTest(prev) })
}

// WaitCompleteRBACSnapshotForTest waits until the published RBAC snapshot is
// complete, as a test's fixture must before it writes the store: a watcher's
// RBAC informers publish a partial snapshot first, asynchronously. It fails the
// test after timeout. Production code MUST NOT call this.
func WaitCompleteRBACSnapshotForTest(t interface {
	Helper()
	Fatalf(string, ...any)
}, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !rbacSnapshotComplete() {
		if time.Now().After(deadline) {
			t.Fatalf("the RBAC snapshot was not complete within %s", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// PartialRBACSnapshotForTest publishes the current RBAC snapshot marked not
// complete (or an empty incomplete one when none is published) for one test,
// and restores the previous one at its cleanup: the state the RBAC informers'
// initial LIST publishes. Production code MUST NOT call this.
func PartialRBACSnapshotForTest(t interface{ Cleanup(func()) }) {
	prev := rbacSnap.Load()
	next := &RBACSnapshot{}
	if prev != nil {
		cp := *prev
		cp.Synced = false
		next = &cp
	}
	PublishRBACSnapshotForTest(next)
	t.Cleanup(func() { PublishRBACSnapshotForTest(prev) })
}

func init() {
	// CFG-1: under CACHE_ENABLED=false there is no store write to admit.
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_l1_put_declined_rbac_incomplete", expvar.Func(func() any {
		return PutDeclinedRBACIncompleteSnapshot()
	}))
}
