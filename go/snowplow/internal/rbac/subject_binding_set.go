// subject_binding_set.go — #423: the requester's full matching-binding set,
// as the identity dimension of the resolved-output L1 key.
//
// THE DEFECT (#423): the restactions / widgets / raFullList cells were keyed by
// the FIRST binding granting `get` on the dispatched CR (BindingUID) plus the
// RBACSubGen sum. Two users co-bound by that one binding derived the same key
// even when one of them held an EXTRA binding that changed what the
// RESTAction's own steps could read — and the hit path serves entry.RawJSON
// verbatim. A co-bound user lacking the per-step grant was served the other
// user's resolved data (TestFalsifier_L1PerStepRBAC_CrossUser).
//
// THE FIX: fold a digest of EVERY binding (ClusterRoleBinding + RoleBinding, in
// every namespace) whose subjects match the requester. Every RBAC verdict
// EvaluateRBAC can return for a requester is a function of exactly that set
// (plus the role rules it references, which are the same objects for every
// requester holding the same set). So two requesters with the same set get the
// same verdict for every (verb, group, resource, namespace, name) — the
// resolved output of any step is byte-identical for them — and a requester
// with a different set gets a different key. Sharing is preserved for the
// common shape (group-only members of the same groups hold the same set).
//
// ROLE CONTENTS ARE NOT FOLDED. A role edit changes the permissions of every
// holder of the referencing bindings identically, so it cannot make two
// same-set requesters diverge; it is a staleness event, not an isolation
// event, and it is already handled by the per-subject RBACSubGen the key also
// folds (#257 onRoleUpdated → onRoleRulesChanged bumps every subject of every
// referencing binding). TestSubjectBindingSet_RoleEditRotatesKey proves the
// rotation end to end.
//
// SAME SUBJECT SEMANTICS AS THE EVALUATOR. The set is built only from the
// evaluator's own unexported functions — selectCRBCandidates /
// selectRBCandidatesAllNS (index pre-filters) gated by anySubjectMatches (the
// correctness barrier), with effectiveGroups' ServiceAccount synthetic groups —
// exactly as BuildRequesterProfile does. The one deliberate difference:
// system:authenticated is ALWAYS added. Every real /call carries a non-empty
// username (it is JWT-authenticated), so for customers this is identical to the
// evaluator. It matters for the prewarm seed, whose group representative is
// {Username:"", Groups:[g]} (pickRepresentativeFromSubjects): without it the
// seed would omit the system:authenticated bindings every real member holds and
// mint a key no customer derives. Treating the representative as
// authenticated makes seed and customer keys agree (key parity).
//
// IDENTITY IN THE KEY IS BINDING IDs, NEVER NAMES. The ids are the SOT
// cache.BindingUIDFromCRB / BindingUIDFromRB forms ("C:<uid>",
// "R:<ns>/<uid>"); the digest is a SHA-256 over the sorted, de-duplicated ids.
// Nothing here is logged.
//
// COST. Memoised per (snapshot, Username, sorted Groups) with the same
// swapped-shard discipline as the requester profile memo (profile.go). A hit is
// one RLock'd map lookup plus building the groups key; a miss walks only the
// requester's own index buckets (O(the requester's bindings), never O(all
// bindings)). The shard is bound to the snapshot POINTER, not to PublishSeq: a
// republish swaps the shard, so a digest never outlives the snapshot it was
// computed from, and a snapshot published with a reused or zero PublishSeq
// (test publishes) can never be served another snapshot's digest. The shard
// holds the pointer, so it cannot be recycled while the shard is live.

package rbac

import (
	"crypto/sha256"
	"encoding/hex"
	"expvar"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// systemAuthenticatedGroup is the implicit group of every authenticated
// request (Kubernetes auth chain; mirrors anySubjectMatches).
const systemAuthenticatedGroup = "system:authenticated"

// SubjectBindingSetDigest returns the digest of every binding in the published
// RBAC snapshot whose subjects match (username, groups), or "" when no snapshot
// is published (cache-off / pre-readiness). A "" digest only ever accompanies a
// "" BindingUID on the dispatch path (EvaluateRBAC denies without a snapshot),
// and a "" BindingUID is never served or populated (serveFromCacheEligible).
func SubjectBindingSetDigest(username string, groups []string) string {
	if cache.Disabled() {
		return ""
	}
	rw := cache.Global()
	if rw == nil {
		return ""
	}
	return subjectBindingSetDigestFor(rw.Snapshot(), username, groups)
}

// subjectBindingSetDigestFor is the memoised accessor over an explicit snapshot.
func subjectBindingSetDigestFor(snap *cache.RBACSnapshot, username string, groups []string) string {
	if snap == nil {
		return ""
	}
	key := bindingSetMemoKey{username: username, groups: canonicalGroupsKey(groups)}

	shard := currentBindingSetShard(snap)
	shard.mu.RLock()
	if d, ok := shard.m[key]; ok {
		shard.mu.RUnlock()
		bindingSetMemoHits.Add(1)
		return d
	}
	shard.mu.RUnlock()
	bindingSetMemoMisses.Add(1)

	d := buildSubjectBindingSetDigest(snap, username, groups)

	shard = currentBindingSetShard(snap)
	shard.mu.Lock()
	if existing, ok := shard.m[key]; ok {
		shard.mu.Unlock()
		return existing
	}
	if len(shard.m) < bindingSetMemoCap {
		shard.m[key] = d
	} else {
		bindingSetMemoRefused.Add(1)
	}
	shard.mu.Unlock()
	return d
}

// SubjectBindingIDs returns the sorted, de-duplicated binding ids matching
// (username, groups) in snap — the pre-digest form. Exported for the key-parity
// goldens and the debug surfaces' tests; production code uses the digest.
func SubjectBindingIDs(snap *cache.RBACSnapshot, username string, groups []string) []string {
	if snap == nil {
		return nil
	}
	id := EvaluateOptions{Username: username, Groups: WithAuthenticatedGroup(groups)}

	seen := map[string]struct{}{}
	var ids []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		ids = append(ids, s)
	}
	for _, crb := range selectCRBCandidates(snap, id) {
		if anySubjectMatches(crb.Subjects, id) {
			add(cache.BindingUIDFromCRB(crb))
		}
	}
	for _, rb := range selectRBCandidatesAllNS(snap, id) {
		if anySubjectMatches(rb.Subjects, id) {
			add(cache.BindingUIDFromRB(rb))
		}
	}
	sort.Strings(ids)
	return ids
}

func buildSubjectBindingSetDigest(snap *cache.RBACSnapshot, username string, groups []string) string {
	ids := SubjectBindingIDs(snap, username, groups)
	h := sha256.New()
	h.Write([]byte("sbs1\x00")) // schema tag for the digest itself
	for _, id := range ids {
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// WithAuthenticatedGroup returns groups plus system:authenticated (once,
// de-duplicated, order otherwise preserved). Every real request is
// authenticated, so this is the requester's EFFECTIVE group set. #424 uses it
// at three sites that must agree for one identity class: the binding-set digest
// (above), the RBACSubGen fold (dispatchCacheLookupKey) and the prewarm seed's
// resolve identity (withCohortSeedContext) — a seed representative with
// Username=="" would otherwise not match system:authenticated subjects in
// EvaluateRBAC (that match is gated on a non-empty username) and its seeded body
// would be NARROWER than a real member's under the same key.
func WithAuthenticatedGroup(groups []string) []string {
	for _, g := range groups {
		if g == systemAuthenticatedGroup {
			return groups
		}
	}
	out := make([]string, 0, len(groups)+1)
	out = append(out, groups...)
	return append(out, systemAuthenticatedGroup)
}

// canonicalGroupsKey is an EXACT (collision-free) order-independent encoding of
// a group set — the memo key must never alias two different group sets, since
// the memoised value is a security-relevant key dimension.
func canonicalGroupsKey(groups []string) string {
	if len(groups) == 0 {
		return ""
	}
	sorted := make([]string, len(groups))
	copy(sorted, groups)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

// bindingSetMemoCap bounds one snapshot's entries: the cardinality is the
// live identity population (~1000 users at production scale), the same bound
// rationale as requesterProfileMemoCap. On breach the digest is computed but
// not cached (correct, just uncached) — never evict-to-OOM.
const bindingSetMemoCap = 4096

type bindingSetMemoKey struct {
	username string
	groups   string
}

type bindingSetShard struct {
	snap *cache.RBACSnapshot
	mu   sync.RWMutex
	m    map[bindingSetMemoKey]string
}

var (
	bindingSetMemo       atomic.Pointer[bindingSetShard]
	bindingSetMemoHits   atomic.Uint64
	bindingSetMemoMisses atomic.Uint64
	// bindingSetMemoRefused counts cap-breach inserts (digest computed, not cached).
	bindingSetMemoRefused atomic.Uint64
	bindingSetExpvarOnce  sync.Once
)

// init publishes the binding-set memo keys only when the cache subsystem is on
// (CFG-1: the memo backs the resolved-output L1 key, which does not exist under
// cache-off, so its keys must be absent there — e2e/bench/cfg1_probe).
func init() {
	if cache.Disabled() {
		return
	}
	RegisterSubjectBindingSetExpvar()
}

// RegisterSubjectBindingSetExpvar publishes the binding-set memo counters on
// /debug/vars. Every RBAC event republishes the snapshot and swaps the memo
// shard, so the hit ratio is the operator's read on how often /call pays the
// cold build (hits / (hits + misses)). Idempotent (sync.Once); called from the
// Disabled()-gated init above.
//
//	snowplow_binding_set_memo_hits    — cumulative memo hits
//	snowplow_binding_set_memo_misses  — cumulative cold builds
//	snowplow_binding_set_memo_refused — cap-breach (computed, not cached)
//	snowplow_binding_set_memo_entries — live entries in the current shard
func RegisterSubjectBindingSetExpvar() {
	bindingSetExpvarOnce.Do(func() {
		expvar.Publish("snowplow_binding_set_memo_hits", expvar.Func(func() any { return bindingSetMemoHits.Load() }))
		expvar.Publish("snowplow_binding_set_memo_misses", expvar.Func(func() any { return bindingSetMemoMisses.Load() }))
		expvar.Publish("snowplow_binding_set_memo_refused", expvar.Func(func() any { return bindingSetMemoRefused.Load() }))
		expvar.Publish("snowplow_binding_set_memo_entries", expvar.Func(func() any {
			if cur := bindingSetMemo.Load(); cur != nil {
				cur.mu.RLock()
				defer cur.mu.RUnlock()
				return len(cur.m)
			}
			return 0
		}))
	})
}

func currentBindingSetShard(snap *cache.RBACSnapshot) *bindingSetShard {
	for {
		cur := bindingSetMemo.Load()
		if cur != nil && cur.snap == snap {
			return cur
		}
		fresh := &bindingSetShard{snap: snap, m: make(map[bindingSetMemoKey]string, 64)}
		if bindingSetMemo.CompareAndSwap(cur, fresh) {
			return fresh
		}
	}
}

// SubjectBindingSetDigestForSnapshotForTest runs the memoised digest against an
// explicit snapshot (benchmarks and arms that build a snapshot without a
// watcher). Test-only pass-through; no production caller.
func SubjectBindingSetDigestForSnapshotForTest(snap *cache.RBACSnapshot, username string, groups []string) string {
	return subjectBindingSetDigestFor(snap, username, groups)
}

// ResetSubjectBindingSetMemoForTest clears the memo and its counters.
func ResetSubjectBindingSetMemoForTest() {
	bindingSetMemo.Store(nil)
	bindingSetMemoHits.Store(0)
	bindingSetMemoMisses.Store(0)
	bindingSetMemoRefused.Store(0)
}

// SubjectBindingSetMemoStatsForTest returns (hits, misses).
func SubjectBindingSetMemoStatsForTest() (uint64, uint64) {
	return bindingSetMemoHits.Load(), bindingSetMemoMisses.Load()
}
