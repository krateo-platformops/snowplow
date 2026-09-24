// rbac_subgen.go — #118 (c) DURABLE fix: per-subject RBAC sub-generation.
//
// THE DEFECT (docs/118-uaf-rbac-stale-read-design-2026-07-22.md §root-cause):
// the resolved-cache key folds one dispatch-authorizing BindingUID and NO RBAC
// generation, while a userAccessFilter stage re-evaluates RBAC per object per
// per-object-namespace — a dependency the key never sees. An out-of-band RBAC
// grant/revoke bumps RBACGen + rebuilds the snapshot but evicts ZERO resolved
// cells, so a user's own now-stale UAF view is served until the cell leaves
// cache. (d) capped the WINDOW; (c) fixes the KEY.
//
// THE FIX: a PER-SUBJECT sub-generation counter that bumps ONLY when THAT
// subject's effective bindings change, folded into the cache key. Blast radius =
// exactly the users whose OWN bindings changed — a composition-install binding
// for tenant-X bumps only tenant-X's subjects, leaving every other user's cells
// hot (this is what makes (c) survive the 50K install storm that global RBACGen
// (option a) dies in).
//
// WHY per-SUBJECT, not global (option a) — the 50K install storm creates RBAC
// bindings continuously; a global gen would rotate the WHOLE identity-bound key
// space on every one, keeping L1 permanently cold. Per-subject bounds the herd
// to the change (design §fix-options (c) vs (a)).
//
// GROUP-GRANT CRUX (C-118-2): a binding can grant via a GROUP the requesting
// user is in. RBACSubGenForSubject therefore folds over the user counter AND
// every presented group's counter (and the SA counter when the identity is a
// ServiceAccount) — so a group-scoped grant/revoke moves the user's effective
// sub-gen. Folding only the user counter is BLIND to group grants (the RED arm).
//
// CONCURRENCY: a sync.Map[subjectKey]*atomic.Uint64. Bumps come from the RBAC
// informer hooks (onBindingAdd/Update/Delete, single informer goroutine but the
// atomic makes it race-safe regardless); reads are lock-free on the hot /call
// path (LoadOrStore is amortized-O(1), the atomic Load is wait-free) — mirrors
// RBACGen's single-atomic-load discipline, just sharded per subject. No new
// informer, no per-request binding-set walk (the design's feasibility guardrail):
// the bump reuses the subjects the onBinding* hooks ALREADY have in hand
// (subjectsFromRBAC(o.Subjects)); the reader does O(user's group count) map
// reads.

package cache

import (
	"strings"
	"sync"
	"sync/atomic"
)

// rbacSubGen holds the per-subject sub-generation counters. Keyed by subjectKey
// (the SAME comparable {Kind,Name,Namespace} the binding indexes use), value is
// a *atomic.Uint64 so a bump and a read never lock. A subject absent from the
// map reads as 0 (never granted/revoked yet) — the zero value is correct.
var rbacSubGen sync.Map // subjectKey -> *atomic.Uint64

// #247 KEY-ROTATION VANTAGE: rbacSubGen is unexported and per-subject, so the
// only published RBAC counter (snowplow_rbac_publish_seq / RBACGen) is GLOBAL —
// one bump per snapshot publish, saying nothing about WHOSE key rotated. The two
// counters below close that hole from the surface the rotation happens on.
//
// WHAT NEITHER OF THEM CLOSES — the bound is a CHAIN, and each link can hold
// while the next one fails:
//
//	publish_seq → bumps_total → minting events → resident excess
//
// publish_seq does not say whose key rotated. bumps_total does not say whether a
// rotation ever MINTED anything: a bump on an idle subject rotates that
// subject's key and mints nothing until traffic next arrives — the cost lands as
// a cold miss on that user's next request, which no counter here observes.
// Reading any link as if it were the last one is the error these counters exist
// to stop, and it is the error the original 40.7% analysis made by reading
// publish_seq as a rotation rate.
//
// The arrows are NECESSITY, NOT an ordering by size — do not rewrite them as
// `>=`. Each link is required for the next and sufficient for none, so the ZERO
// propagates forward (no publishes ⇒ no bumps ⇒ no RBAC-caused mints) while a
// large number propagates forward not at all. The necessity is structural:
// flushPendingSubGenBumps has exactly one production caller, rebuildRBACSnapshot
// immediately after rbacSnap.Store (rbac_snapshot.go), and it is the only path
// to BumpSubjectSubGens. Magnitudes order NEITHER way: one publish flushes the
// whole accumulated subject set, so bumps_total routinely exceeds publish_seq,
// while a rebuild whose pending set is empty advances publish_seq alone.
//
// NOT an identity-scoped limitation. /debug/apistage is a GLOBAL residency dump
// — DebugApistage calls RangeMetadata, which walks the whole store with no
// requester predicate (the body is hash-only because CELLS are per-identity, but
// the enumeration is not). So a capture cannot hide rotation happening on some
// other tenant's subjects; it can only fail to show rotation that has not minted
// yet. An earlier draft of this comment claimed the opposite and was wrong.
//
// Both are process-lifetime monotonic and read only by the metrics surfaces;
// nothing on the /call path reads them, and subGenValue (the hot read side)
// is untouched.
var (
	// rbacSubGenBumps counts CUMULATIVE per-subject sub-generation bumps —
	// one per (subject, bump) pair, so a publish flushing 40 subjects adds 40.
	// Answers "is the key rotating at all, and how fast".
	rbacSubGenBumps atomic.Uint64

	// rbacSubGenSubjects counts DISTINCT subjects that have ever had a
	// counter created, i.e. the size of rbacSubGen. Answers "over how wide a
	// blast radius". rbacSubGenBumps alone cannot tell 12,000 bumps on 3
	// subjects (a hot loop on one tenant) from 12,000 bumps on 12,000
	// subjects (a fleet-wide rotation) — and that shape is exactly what the
	// per-subject design exists to bound.
	//
	// HIGH-WATER MARK, NOT A RATE. subGenCounterFor LoadOrStores an entry that
	// is NEVER removed, so this only ever ratchets up and then SATURATES: once
	// the first broad storm has touched the subject population, the delta over
	// any window is ~0 forever — in a healthy cluster and a sick one alike. Do
	// not chart it as a rate and do not read a flat line as "quiet"; a flat
	// line is the expected steady state. Its meaning is the one-time
	// denominator for rbacSubGenBumps, nothing more.
	rbacSubGenSubjects atomic.Uint64
)

// RBACSubGenBumpsTotal returns the cumulative count of per-subject
// sub-generation bumps since process start. Monotonic.
//
// READING THE ZERO: 0 means no subject's sub-generation has ever moved — no
// identity-bound L1 key has rotated for an RBAC reason. That reading is only
// admissible because a LIVENESS arm has shown the counter can move through the
// production path: TestBindingNoopCounters_BumpsStillFireOnEveryNoop drives the
// real onBindingUpdate hook, flushes the pending set, and fails if this stays
// 0. Without such an arm a zero would mean "not looking", not "not rotating" —
// and the other two counters in this workstream do NOT yet have one (see
// rbacSubGenSubjects here, and bindingNoopUpdates in
// rbac_binding_noop_counters.go).
func RBACSubGenBumpsTotal() uint64 { return rbacSubGenBumps.Load() }

// RBACSubGenSubjectsTracked returns the number of DISTINCT subjects that have a
// sub-generation counter — the blast radius denominator for
// RBACSubGenBumpsTotal.
//
// READING THE ZERO — AND THE FLAT LINE: monotone and SATURATING, because
// counters are never removed (see rbacSubGenSubjects). 0 means no subject has
// ever been touched; a value that stops moving means the subject population has
// been covered, NOT that RBAC went quiet. Healthy and broken read the same here
// over any window after the first storm, so never derive a rate from it —
// rbacSubGenBumps is the rate, this is its denominator.
func RBACSubGenSubjectsTracked() uint64 { return rbacSubGenSubjects.Load() }

// subGenCounterFor returns the (creating if absent) atomic counter for subj.
// LoadOrStore is the single amortized-O(1) map op; the returned pointer is
// stable for the process lifetime so a bump and a concurrent read share it.
//
// The distinct-subject counter is incremented ONLY on the branch that genuinely
// stored: the Load fast path above never creates an entry, and LoadOrStore's
// loaded=true means a concurrent racer won the store for this subject — counting
// either would over-report the blast radius.
func subGenCounterFor(subj subjectKey) *atomic.Uint64 {
	if v, ok := rbacSubGen.Load(subj); ok {
		return v.(*atomic.Uint64)
	}
	v, loaded := rbacSubGen.LoadOrStore(subj, new(atomic.Uint64))
	if !loaded {
		rbacSubGenSubjects.Add(1)
	}
	return v.(*atomic.Uint64)
}

// BumpSubjectSubGens increments the sub-generation of each subject whose
// effective bindings just changed. Called from the RBAC informer hooks
// (onBindingAdd/Update/Delete) with the binding's subjects already in hand
// (subjectsFromRBAC(o.Subjects)) — so NO new informer and NO binding-set walk.
// An Update deletes-old-then-adds-new; the hook calls this for BOTH the old
// subject set (on the delete leg) and the new (on the add leg), so a
// subject-list edit that drops user U and adds user V bumps both U (its grant
// changed: removed) and V (added). Idempotent-safe: every call is a monotonic
// +1, and the key only needs to CHANGE on any relevant RBAC event, not carry a
// meaningful absolute value.
func BumpSubjectSubGens(subjects []subjectKey) {
	for _, s := range subjects {
		subGenCounterFor(s).Add(1)
	}
	// One atomic add for the whole batch, not one per element — the counter is
	// a cumulative total, so len(subjects) is the exact delta. Sequenced AFTER
	// the loop so the published total never claims work that has not landed.
	rbacSubGenBumps.Add(uint64(len(subjects)))
}

// RBACSubGenForSubject returns the requesting identity's EFFECTIVE RBAC
// sub-generation: the sum over the identity's own subject counter(s) — the User
// counter, every presented Group's counter, and (when the username is a
// ServiceAccount) the SA counter. A sum means ANY relevant subject's bump
// changes the total, so the folded key rotates on a grant/revoke via the user
// OR any of its groups OR (for an SA) the SA identity — the C-118-2 group-grant
// crux. Lock-free (a handful of atomic Loads). O(len(groups)) map reads on the
// hot path; a user has O(10) groups (design INFERRED bound, pinned by the herd
// arm). Returns 0 for an identity none of whose subjects has ever been touched
// (correct: nothing changed → no key rotation).
//
// The subjectKey mapping mirrors subjectsFromRBAC exactly: a User is
// {Kind:"User",Name:username}; a Group is {Kind:"Group",Name:g}; a
// ServiceAccount username "system:serviceaccount:<ns>:<name>" folds the SA
// subject {Kind:"ServiceAccount",Name:<name>,Namespace:<ns>} so a binding whose
// subject is that SA (which onBinding* recorded as an SA subjectKey) bumps it.
func RBACSubGenForSubject(username string, groups []string) uint64 {
	var sum uint64
	if username != "" {
		if ns, name, ok := parseServiceAccountUsername(username); ok {
			// The identity is a ServiceAccount: fold the SA subject counter (the
			// shape onBinding* recorded), NOT a User counter — a binding granting
			// this SA is indexed under the SA subjectKey.
			sum += subGenValue(subjectKey{Kind: subjectKindServiceAccount, Name: name, Namespace: ns})
		} else {
			// A human/user identity: fold the User subject counter.
			sum += subGenValue(subjectKey{Kind: subjectKindUser, Name: username})
		}
	}
	for _, g := range groups {
		if g == "" {
			continue
		}
		sum += subGenValue(subjectKey{Kind: subjectKindGroup, Name: g})
	}
	return sum
}

// subGenValue reads a subject's counter WITHOUT creating it — an untouched
// subject reads 0 (no map entry, no allocation). Keeping the read side
// allocation-free means a /call for a never-granted-since-boot identity does not
// grow the map.
func subGenValue(subj subjectKey) uint64 {
	if v, ok := rbacSubGen.Load(subj); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}

// subjectKind constants — the rbacv1.Subject.Kind string values, named here so
// the reader's subjectKey construction cannot drift from subjectsFromRBAC's.
const (
	subjectKindUser           = "User"
	subjectKindGroup          = "Group"
	subjectKindServiceAccount = "ServiceAccount"
)

// serviceAccountUsernamePrefix is the canonical prefix a ServiceAccount presents
// as its username on the request (system:serviceaccount:<ns>:<name>).
const serviceAccountUsernamePrefix = "system:serviceaccount:"

// parseServiceAccountUsername splits a canonical SA username into (namespace,
// name). Returns ok=false for a non-SA username (a human user), which the caller
// then folds as a User subject. Mirrors f3SubjectFor / the k8s canonical form.
func parseServiceAccountUsername(username string) (namespace, name string, ok bool) {
	rest, found := strings.CutPrefix(username, serviceAccountUsernamePrefix)
	if !found {
		return "", "", false
	}
	ns, nm, hasColon := strings.Cut(rest, ":")
	if !hasColon || ns == "" || nm == "" {
		return "", "", false
	}
	return ns, nm, true
}

// ResetRBACSubGenForTest clears all per-subject counters AND the two published
// observability counters. TEST-ONLY — production never resets (the counters are
// monotonic for the process lifetime).
//
// The observability counters are reset HERE rather than behind a separate
// helper: rbacSubGenSubjects is defined as "distinct subjects that have a
// counter", so emptying the map without zeroing it would leave the published
// gauge permanently over-reporting for every later test in the package.
func ResetRBACSubGenForTest() {
	rbacSubGen.Range(func(k, _ any) bool {
		rbacSubGen.Delete(k)
		return true
	})
	rbacSubGenBumps.Store(0)
	rbacSubGenSubjects.Store(0)
}
