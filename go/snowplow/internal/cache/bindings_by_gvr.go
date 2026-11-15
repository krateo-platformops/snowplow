// bindings_by_gvr.go — Ship 1 of the unified dynamic cohort-prewarm
// engine: the incremental BindingsByGVR reverse index.
//
// PURPOSE. The boot prewarm seed (phase1_pip_seed.go) must scope the
// cohort set PER NAVIGATED GVR — the ~3-6 subjects that can actually
// read each GVR — instead of the global GVR-agnostic powerset (~34
// cohorts dominated by control-plane SAs that never navigate). This
// index is that scoping substrate: for a navigated GVR it returns the
// subjects whose RBAC roleRef rules grant get/list on it (plus the
// cluster-admin `*/*` wildcard subjects).
//
// CRITICAL — LIST not KEY (project_resource_driven_cohort_design_2026_05_28).
// This index changes only WHICH SUBJECTS the seed enumerates per GVR. It
// does NOT change the L1 cell KEY: the per-(layer, BindingUID) L1 cell
// stays keyed by the FIRST-MATCH binding's UID, derived per-layer at
// dispatch time by rbac.EvaluateRBAC. The index is SEED-TARGETING only
// — it picks WHICH BindingUIDs to prewarm per GVR via the
// EnumeratePrewarmTargetsForGVR enumerator (prewarm_enumeration.go).
//
// Ship 0.30.242 H.c-layered Phase 2b: the prior EnumerateResourceCohorts
// returned `[]Cohort` for a per-cohort dispatch fan-out (cohort dedupe
// happened at the BindingSetHash layer). H.c-layered replaces that with
// per-binding targets (each binding → one prewarm dispatch keyed by that
// binding's UID — cell sharing is finer-grained per design §1.2).
//
// AUTHZ BOUNDARY UNTOUCHED. This index is SEED-TARGETING only. The
// per-request authz boundary remains EvaluateRBAC (evaluate.go:210) over
// the wholesale snapshot (rebuildRBACSnapshot / rebuildSubjectIndexes).
// List over-inclusion = wasted seed (benign); under-inclusion = per-user
// fallback resolve (benign). Eventual consistency is fine.
//
// A FOLD OF EVENTS, NEVER REBUILT (#1458). The index holds two things:
//
//   - MEMBERSHIP: which binding names which role, with which subjects
//     (entries, byRole). It is what a role's rules edit reaches an identity's
//     cache keys through (onRoleRulesChanged → the sub-generation of the
//     subjects of the bindings that name the role). It is written by the RBAC
//     informer event hooks ALONE (bindings_by_gvr_delta.go), from the
//     informers' first event on, the initial list included. NOTHING refills it
//     from the published RBAC snapshot: a refill reads a snapshot that is
//     published later than the events are handled, so it drops (or resurrects)
//     what the event path already applied (#1458).
//   - BUCKETS: which bindings grant get/list on which navigated resource
//     (byGVR, wildcard), over the navigated set. Derived from membership, the
//     role rules and the navigated set; they decide prewarm targeting only.
//     SetNavigatedGVRs (the readiness settle and every prewarm pass) and
//     AddNavigatedGVR (CRD discovery) re-derive them; they never touch
//     membership.
//
// READY. The index is ready once the four RBAC handlers have been delivered
// their informers' initial lists (their registrations' HasSynced): membership
// then holds every binding. The initial-publish goroutine, which readiness
// waits for (#1231), sets it once (markBindingsIndexReady). Until then the
// enumerators answer nothing and the hooks record no sub-generation bump (the
// #682 note covers that window with one rotation at ready).
//
// The wholesale rebuild of this index per ~4.6/s snapshot republish was
// measured at ~265 ms/build → ~122% CPU and is REJECTED (Gate-2). The
// per-binding-event delta was measured at ~5.8 µs → 0.0027% CPU at 4.6/s
// (set-backed buckets make the DELETE side O(navigatedGVRs), not
// O(bucket-size)).
//
// SET-BACKED BUCKETS (Gate-2 lesson). Buckets are map[subjectKey]struct{}
// — NOT slices. A slice bucket made an admin-wildcard binding delete an
// O(390K) linear scan (774 µs/event in the first cut). Set buckets make
// the delete O(1) per touched bucket.
//
// CONCURRENCY. The index is guarded by its own RWMutex (bindingsIndexMu)
// — it is mutated by the RBAC informer processor goroutine(s) (via the
// delta hooks, which run on the same processor goroutine as
// scheduleRBACRebuild — handler bodies must stay non-blocking, and the
// delta is a few map ops) and read by EnumerateResourceCohorts on the
// boot/engine path. We take the write lock for deltas and the read lock
// for enumeration. The matcher (roleRefGrantsGetList) reads the published
// immutable snapshot's roles, so role-rule lookups are lock-free against
// the snapshot.
//
// FEEDBACK_NO_SPECIAL_CASES. No GVR / resource / user literals. The
// navigated-GVR set is supplied by the caller (RegisteredGVRs() from the
// walk); subject matching flows from the rule wildcards; the wildcard
// bucket is the generic `*/*` rule, not a hardcoded cluster-admin name.
//
// FEEDBACK_CHECK_K8S_CLIENTGO_PRIOR_ART. client-go has no per-GVR
// subject reverse index — the authorizer evaluates one request at a
// time. This is a snowplow seed-targeting concern.

package cache

import (
	"sync"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// groupResource is the {apiGroup, resource} index key — a navigated GVR
// reduced to the pair RBAC rules match on (RBAC rules carry no version).
type groupResource struct {
	Group    string
	Resource string
}

// grFromGVR reduces a GVR to its {group, resource} index key.
func grFromGVR(gvr schema.GroupVersionResource) groupResource {
	return groupResource{Group: gvr.Group, Resource: gvr.Resource}
}

// subjectKey is one RBAC subject, canonicalised into the form the cohort
// machinery consumes. Kind is the rbac/v1 subject kind ("User", "Group",
// "ServiceAccount"); Name is the subject name; Namespace is set only for
// ServiceAccount subjects (the SA's namespace). subjectKey is comparable
// so it is a valid map key for the set-backed buckets.
type subjectKey struct {
	Kind      string
	Name      string
	Namespace string
}

// bindingID is the stable per-binding identity the buckets are keyed on
// for incremental delete. A binding's subjects all share its bindingID;
// the bucket value is the set of bindingIDs whose roleRef grants the
// GVR, each carrying the binding's subjects.
//
// Ship 0.30.242 H.c-layered Phase 2b: the value the bindingID newtype
// wraps is now produced by cache.BindingUIDFromCRB / FromRB
// (match_subject.go — the SOT for binding-identity derivation). The
// seed-time index landing AND the dispatcher-time L1 cell key (the
// BindingUID string field on ResolvedKeyInputs) both consume the SAME
// string from these constructors — so seed-vs-dispatch agreement is
// structural rather than a coincidence of separate identity helpers.
//
// The bindingID type stays internal (unexported) so this file's index
// buckets remain typed independently of the exported ResolvedKeyInputs.BindingUID
// field; the string value is byte-identical at both sites.
type bindingID string

// bindingEntry is the per-binding value stored in a GVR bucket: the
// binding's identity + the subjects it grants. The subjects are copied by
// value (small Subject list) so the bucket is self-contained and does not
// pin the snapshot's typed objects.
type bindingEntry struct {
	id       bindingID
	subjects []subjectKey
	// role is the byRole key of the role the binding names (roleRefKey), ""
	// for a roleRef of an unknown kind.
	role string
}

// bindingsByGVRIndex is the incremental reverse index.
//
//   - byGVR[gr] = set of bindingEntries whose roleRef grants get/list on
//     {group,resource} gr (exact-match, non-resourceNames-scoped).
//   - wildcard  = set of bindingEntries whose roleRef carries a `*/*`
//     get/list rule (cluster-admin). NOT expanded across every GVR — the
//     per-GVR cohort = byGVR[gr] ∪ wildcard.
//   - byRole[roleKey] = set of bindingIDs referencing that role — used by
//     the role-rule-change re-route path to find every binding to
//     re-resolve when a ClusterRole/Role's rules change.
//   - entries[id] = the bindingEntry for an id — lets the delta hooks
//     unrol a binding without re-deriving its subjects, and the role
//     re-route look up an id's entry.
type bindingsByGVRIndex struct {
	mu       sync.RWMutex
	byGVR    map[groupResource]map[bindingID]struct{}
	wildcard map[bindingID]struct{}
	byRole   map[string]map[bindingID]struct{}
	entries  map[bindingID]bindingEntry

	// navigated is the set of GVRs the index buckets are maintained for.
	// Set by SetNavigatedGVRs; the delta hooks enrol only into these
	// buckets. A GVR discovered later (a CRD) widens this via
	// AddNavigatedGVR.
	navigated map[groupResource]struct{}

	// ready: the four RBAC handlers have been delivered their initial lists,
	// so membership holds every binding (#1458). Set once
	// (markBindingsIndexReady); never returns to false in production.
	ready bool
}

// bindingsIndexSingleton is the process-wide index. Lazily constructed.
var (
	bindingsIndexInstance *bindingsByGVRIndex
	bindingsIndexOnce     sync.Once
)

func bindingsByGVRSingleton() *bindingsByGVRIndex {
	bindingsIndexOnce.Do(func() {
		bindingsIndexInstance = &bindingsByGVRIndex{
			byGVR:     map[groupResource]map[bindingID]struct{}{},
			wildcard:  map[bindingID]struct{}{},
			byRole:    map[string]map[bindingID]struct{}{},
			entries:   map[bindingID]bindingEntry{},
			navigated: map[groupResource]struct{}{},
		}
	})
	return bindingsIndexInstance
}

// roleRefKey renders the byRole map key for a binding's roleRef in the
// binding's namespace ("" for a CRB). Mirrors roleRefPermits' resolution
// domain: ClusterRole is cluster-scoped; Role is ns-scoped.
func roleRefKey(namespace string, ref rbacv1.RoleRef) string {
	switch ref.Kind {
	case "ClusterRole":
		return "C/" + ref.Name
	case "Role":
		return "R/" + namespace + "/" + ref.Name
	default:
		return ""
	}
}

// crbBindingID / rbBindingID compute the stable binding identity by
// delegating to the SOT helpers (match_subject.go). The 1-line wrappers
// give the index a typed bindingID while keeping the identity STRING
// byte-identical to the dispatcher-time BindingUID consumed by
// ResolvedKeyInputs.BindingUID — seed-vs-dispatch agreement is built
// into the shared SOT.
func crbBindingID(p *rbacv1.ClusterRoleBinding) bindingID {
	return bindingID(BindingUIDFromCRB(p))
}

func rbBindingID(p *rbacv1.RoleBinding) bindingID {
	return bindingID(BindingUIDFromRB(p))
}

// subjectsFromRBAC projects a binding's rbac/v1 Subjects into subjectKeys.
// Only User / Group / ServiceAccount kinds are projected (the cohort
// machinery consumes User+Groups; SA subjects are carried so a future SA
// cohort path can use them — collectCohortBindingIDs already ignores SA).
// Each subject's key is bindingSubjectKey's (match_subject.go): the one form
// a binding event bumps and the evaluator's subject matcher compares (#1443).
func subjectsFromRBAC(subjects []rbacv1.Subject) []subjectKey {
	if len(subjects) == 0 {
		return nil
	}
	out := make([]subjectKey, 0, len(subjects))
	for _, s := range subjects {
		if k, ok := bindingSubjectKey(s); ok {
			out = append(out, k)
		}
	}
	return out
}

// rbacWildcardMatches is the RBAC wildcard rule: "*" matches every
// value; otherwise exact match. Mirrors rbac/evaluate.go's
// stringSliceMatches semantics — duplicated here as a package-private
// helper because cache/ cannot import rbac/ (rbac/ already imports
// cache/, so an inverse import would create a cycle).
//
// Ship 0.30.242 H.c-layered Phase 2b: this helper used to live in
// cohort_ns_acl.go (deleted in commit 1d93d02) under the name
// rbacStringSliceMatches. Moved here because bindings_by_gvr.go is the
// only surviving caller; the move keeps the predicate co-located with
// the 2 functions that use it.
func rbacWildcardMatches(allowed []string, want string) bool {
	for _, a := range allowed {
		if a == "*" || a == want {
			return true
		}
	}
	return false
}

// rulesGrantGetList reports whether any rule in `rules` grants get OR list
// on the given {group,resource} — mirroring rulesPermit's wildcard
// semantics (evaluate.go:446-463). A resourceNames-scoped rule grants a
// `list` only when the request names one of its objects (a
// `fieldSelector=metadata.name=` list, #1049) and a `get` only of a named
// object; a navigation LIST names none, so such a rule does NOT enrol a
// binding for nav LIST (faithful to resourceNameMatches for an unnamed
// request). This index decides warmth (prewarm / navigation enrolment),
// never an authorization. The `*/*` case is handled by rulesGrantWildcard.
func rulesGrantGetList(rules []rbacv1.PolicyRule, gr groupResource) bool {
	for _, rule := range rules {
		if len(rule.ResourceNames) > 0 {
			continue
		}
		if !rbacWildcardMatches(rule.Verbs, "get") && !rbacWildcardMatches(rule.Verbs, "list") {
			continue
		}
		if !rbacWildcardMatches(rule.APIGroups, gr.Group) {
			continue
		}
		if !rbacWildcardMatches(rule.Resources, gr.Resource) {
			continue
		}
		return true
	}
	return false
}

// rulesGrantWildcard reports whether the rule set contains a `*/*`
// get/list rule (independent of any specific GVR) — populates the
// wildcard bucket ONCE per binding instead of per GVR.
func rulesGrantWildcard(rules []rbacv1.PolicyRule) bool {
	for _, rule := range rules {
		if len(rule.ResourceNames) > 0 {
			continue
		}
		if (rbacWildcardMatches(rule.Verbs, "get") || rbacWildcardMatches(rule.Verbs, "list")) &&
			sliceHasStar(rule.APIGroups) && sliceHasStar(rule.Resources) {
			return true
		}
	}
	return false
}

func sliceHasStar(s []string) bool {
	for _, v := range s {
		if v == "*" {
			return true
		}
	}
	return false
}

// rulesForRoleRef resolves a binding's roleRef to its rule set against the
// published snapshot — mirrors roleRefPermits (evaluate.go:407-434). The
// snapshot is immutable post-publish so this read is lock-free. namespace
// is "" for a CRB; for a RoleBinding it is the binding's namespace.
func rulesForRoleRef(snap *RBACSnapshot, namespace string, ref rbacv1.RoleRef) ([]rbacv1.PolicyRule, bool) {
	if snap == nil {
		return nil, false
	}
	switch ref.Kind {
	case "ClusterRole":
		cr, ok := snap.ClusterRolesByName[ref.Name]
		if !ok {
			return nil, false
		}
		return cr.Rules, true
	case "Role":
		if namespace == "" {
			return nil, false
		}
		r, ok := snap.RolesByNSName[namespace+"/"+ref.Name]
		if !ok {
			return nil, false
		}
		return r.Rules, true
	default:
		return nil, false
	}
}

// ─────────────────────────────────────────────────────────────────────
// Delta primitives (WRITE-locked). enrol/unrol take a fully-resolved
// bindingEntry + rules so the hooks resolve the snapshot ONCE per event.
// ─────────────────────────────────────────────────────────────────────

// enrolLocked records a binding's membership (entries + byRole) and enrols it
// into the per-GVR buckets / wildcard bucket. Caller holds idx.mu (write).
// rules is the binding's resolved roleRef rules; rk is its roleRefKey ("" if
// unresolvable).
func (idx *bindingsByGVRIndex) enrolLocked(entry bindingEntry, rules []rbacv1.PolicyRule, rk string) {
	entry.role = rk
	idx.entries[entry.id] = entry
	if rk != "" {
		set := idx.byRole[rk]
		if set == nil {
			set = map[bindingID]struct{}{}
			idx.byRole[rk] = set
		}
		set[entry.id] = struct{}{}
	}
	idx.bucketLocked(entry.id, rules)
}

// bucketLocked enrols a binding id into the wildcard bucket or the per-GVR
// buckets its rules grant. It writes buckets only, never membership. Caller
// holds idx.mu (write).
func (idx *bindingsByGVRIndex) bucketLocked(id bindingID, rules []rbacv1.PolicyRule) {
	if rulesGrantWildcard(rules) {
		idx.wildcard[id] = struct{}{}
		return
	}
	for gr := range idx.navigated {
		if rulesGrantGetList(rules, gr) {
			set := idx.byGVR[gr]
			if set == nil {
				set = map[bindingID]struct{}{}
				idx.byGVR[gr] = set
			}
			set[id] = struct{}{}
		}
	}
}

// recordBindingLocked is the ONE way a binding enters the index (#1449,
// #1458): the binding ADD / UPDATE event hooks call it. The binding is
// recorded under the role it names (entries + byRole) whether or not that role
// is in snap. A role that is absent has no rules, so THIS function puts the
// binding in no GVR or wildcard bucket (it grants nothing, and the evaluator,
// which reads the snapshot and never this index, denies it); it stays reachable
// through byRole, so the role's create, every rules edit and its delete reach
// the binding's subjects (onRoleRulesChanged). That holds for the record path
// only: when a role is DELETED, onRoleRulesChanged re-buckets its bindings
// under the role's last-known rules, where they stay until the next event on
// the role or the binding, or the next build (#1457; prewarm targeting only,
// never keys or authorization). Caller holds idx.mu (write).
func (idx *bindingsByGVRIndex) recordBindingLocked(snap *RBACSnapshot, namespace string, ref rbacv1.RoleRef, id bindingID, subjects []subjectKey) {
	rules, _ := rulesForRoleRef(snap, namespace, ref)
	idx.enrolLocked(bindingEntry{id: id, subjects: subjects}, rules, roleRefKey(namespace, ref))
}

// unrolLocked removes a binding id from every bucket it could be in. Caller
// holds idx.mu (write). With set-backed buckets each delete is O(1); we
// touch at most |navigated| buckets + wildcard + byRole. rk is the
// binding's roleRefKey (for the targeted byRole delete); when "" we skip
// the byRole delete (the binding never enrolled there).
func (idx *bindingsByGVRIndex) unrolLocked(id bindingID, rk string) {
	delete(idx.wildcard, id)
	for _, set := range idx.byGVR {
		delete(set, id)
	}
	if rk != "" {
		if set := idx.byRole[rk]; set != nil {
			delete(set, id)
		}
	}
	delete(idx.entries, id)
}

// ─────────────────────────────────────────────────────────────────────
// The navigated set and the ready signal. Neither writes membership.
// ─────────────────────────────────────────────────────────────────────

// SetNavigatedGVRs sets the navigated set and re-derives the per-GVR and
// wildcard buckets from the membership the index holds and the published
// snapshot's role rules. Called by the readiness settle and by every prewarm
// pass, over the registered GVRs. It NEVER writes membership (entries, byRole):
// it can neither drop a binding the event path recorded nor bring back one it
// removed (#1458). Safe with a nil or empty set (no bucket until
// AddNavigatedGVR or a later call widens it). Returns the number of bindings
// the index holds.
func SetNavigatedGVRs(navigatedGVRs []schema.GroupVersionResource) int {
	idx := bindingsByGVRSingleton()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.navigated = make(map[groupResource]struct{}, len(navigatedGVRs))
	for _, gvr := range navigatedGVRs {
		idx.navigated[grFromGVR(gvr)] = struct{}{}
	}
	idx.rebucketLocked(rbacSnap.Load())
	return len(idx.entries)
}

// rebucketLocked empties the buckets and enrols every binding the index holds
// under the rules snap gives the role it names. Buckets only. Caller holds
// idx.mu (write).
func (idx *bindingsByGVRIndex) rebucketLocked(snap *RBACSnapshot) {
	idx.byGVR = map[groupResource]map[bindingID]struct{}{}
	idx.wildcard = map[bindingID]struct{}{}
	for id, entry := range idx.entries {
		rules, _ := rulesForRoleKey(snap, entry.role)
		idx.bucketLocked(id, rules)
	}
}

// markBindingsIndexReady marks the index ready: the four RBAC handlers have
// been delivered their informers' initial lists, so membership holds every
// binding. Called once per watcher by the initial-publish goroutine, after its
// complete publish (waitAndPublishInitialRBACSnapshot); idempotent. The first
// call derives the buckets from that snapshot and settles the #682 note: one
// rotation of every per-identity key if an RBAC event the keys can only see
// through the sub-generation arrived before ready (rbac_subgen_epoch.go).
func markBindingsIndexReady() {
	idx := bindingsByGVRSingleton()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.ready {
		return
	}
	idx.rebucketLocked(rbacSnap.Load())
	idx.ready = true
	rotateIfRBACEventSeenPreBuild()
}

// BindingsByGVRIndexBuilt reports whether the index is ready: the RBAC
// handlers have been delivered their initial lists (markBindingsIndexReady).
// The enumerators answer nothing before it.
func BindingsByGVRIndexBuilt() bool {
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.ready
}

// AddNavigatedGVR widens the index's navigated set by one GVR and enrols
// every already-known binding that grants get/list on it (a CRD discovery
// widens the navigated surface). Cheap: walks idx.entries once, resolving each
// binding's rules against the snapshot. Buckets only, never membership.
// Idempotent — a GVR already in navigated is a no-op.
func AddNavigatedGVR(gvr schema.GroupVersionResource) {
	idx := bindingsByGVRSingleton()
	gr := grFromGVR(gvr)

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, ok := idx.navigated[gr]; ok {
		return
	}
	idx.navigated[gr] = struct{}{}

	snap := rbacSnap.Load()
	if snap == nil {
		return
	}
	// Enrol every known non-wildcard binding that grants this GVR. Wildcard
	// bindings already cover every GVR (the per-GVR cohort unions wildcard),
	// so they need no per-GVR enrolment.
	for id, entry := range idx.entries {
		if _, isWild := idx.wildcard[id]; isWild {
			continue
		}
		rules, ok := rulesForRoleKey(snap, entry.role)
		if !ok {
			continue
		}
		if rulesGrantGetList(rules, gr) {
			set := idx.byGVR[gr]
			if set == nil {
				set = map[bindingID]struct{}{}
				idx.byGVR[gr] = set
			}
			set[entry.id] = struct{}{}
		}
	}
}

// rulesForRoleKey resolves a byRole key (roleRefKey) to the role's rules in
// snap. (nil,false) for "" (a roleRef of an unknown kind) or an absent role.
func rulesForRoleKey(snap *RBACSnapshot, rk string) ([]rbacv1.PolicyRule, bool) {
	if rk == "" {
		return nil, false
	}
	ns, ref := parseRoleRefKey(rk)
	return rulesForRoleRef(snap, ns, ref)
}

// parseRoleRefKey is the inverse of roleRefKey: "C/<name>" → (ns="",
// ClusterRole ref); "R/<ns>/<name>" → (ns, Role ref). Used by
// rulesForRoleKey to re-resolve a known binding's rules.
func parseRoleRefKey(rk string) (string, rbacv1.RoleRef) {
	if len(rk) > 2 && rk[:2] == "C/" {
		return "", rbacv1.RoleRef{Kind: "ClusterRole", Name: rk[2:]}
	}
	if len(rk) > 2 && rk[:2] == "R/" {
		rest := rk[2:]
		// rest = "<ns>/<name>" — split on the first "/".
		for i := 0; i < len(rest); i++ {
			if rest[i] == '/' {
				return rest[:i], rbacv1.RoleRef{Kind: "Role", Name: rest[i+1:]}
			}
		}
	}
	return "", rbacv1.RoleRef{}
}

// EnumerateResourceCohorts was the pre-Ship-0.30.242 seed cohort source
// per GVR. It returned []Cohort whose subjects were dispatched in a
// per-cohort fan-out. DELETED in Phase 2b (this commit) — the function
// is replaced by EnumeratePrewarmTargetsForGVR in prewarm_enumeration.go,
// which reshapes the per-GVR enumeration around per-binding targets
// (each binding → one prewarm dispatch keyed by that binding's UID).
// Design §7.2.
//
// The old function's seed-vs-dispatch agreement relied on
// BindingSetHash(sentinel, [group]) matching the dispatcher-time hash
// for a group-only user. Under H.c-layered the seed dispatches under a
// representative SubjectIdentity for each binding (pickRepresentativeFromSubjects
// in match_subject.go), and the L1 cell key folds BindingUID rather
// than the cohort hash — so seed-vs-dispatch agreement is now built
// into the BindingUID itself (both sides call cache.BindingUIDFromCRB/FromRB
// on the same binding object pointer-equal in the snapshot).

// ResetBindingsByGVRIndexForTest clears the singleton's state. Production
// MUST NOT call this. Tests (including cross-package dispatcher tests) use
// it to observe a clean index.
func ResetBindingsByGVRIndexForTest() {
	idx := bindingsByGVRSingleton()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.resetLocked()
}

// beginBindingsIndexForWatcher empties the index's membership and buckets and
// clears ready, keeping the navigated set: a watcher is about to start the RBAC
// informers whose events the index folds. Called by NewResourceWatcher before
// its informers run.
func beginBindingsIndexForWatcher() {
	idx := bindingsByGVRSingleton()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	navigated := idx.navigated
	idx.resetLocked()
	idx.navigated = navigated
}

func (idx *bindingsByGVRIndex) resetLocked() {
	idx.byGVR = map[groupResource]map[bindingID]struct{}{}
	idx.wildcard = map[bindingID]struct{}{}
	idx.byRole = map[string]map[bindingID]struct{}{}
	idx.entries = map[bindingID]bindingEntry{}
	idx.navigated = map[groupResource]struct{}{}
	idx.ready = false
}

// SeedBindingsIndexFromSnapshotForTest is the TEST-ONLY way to stand an index
// up without an informer's event stream: it empties the index, records every
// binding of the PUBLISHED snapshot as its ADD event would (recordBindingLocked,
// the function the hooks use), sets the navigated set and marks the index
// ready (settling the #682 note, as the first ready does). Returns the number
// of bindings recorded.
//
// Production MUST NOT call this, and no production path does anything like it:
// membership is written by the event hooks alone (#1458; the source guard
// TestIssue1458_NoProductionPathRefillsMembershipFromTheSnapshot pins it). A
// test that drives a live watcher and seeds from the snapshot must first let
// the snapshot catch up with the events, or it reproduces #1458 on itself.
func SeedBindingsIndexFromSnapshotForTest(navigatedGVRs []schema.GroupVersionResource) int {
	idx := bindingsByGVRSingleton()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	wasReady := idx.ready
	idx.resetLocked()
	for _, gvr := range navigatedGVRs {
		idx.navigated[grFromGVR(gvr)] = struct{}{}
	}
	snap := rbacSnap.Load()
	recorded := 0
	if snap != nil {
		for _, crb := range snap.ClusterRoleBindings {
			if crb == nil {
				continue
			}
			idx.recordBindingLocked(snap, "", crb.RoleRef, crbBindingID(crb), subjectsFromRBAC(crb.Subjects))
			recorded++
		}
		for ns, rbs := range snap.RoleBindingsByNS {
			for _, rb := range rbs {
				if rb == nil {
					continue
				}
				idx.recordBindingLocked(snap, ns, rb.RoleRef, rbBindingID(rb), subjectsFromRBAC(rb.Subjects))
				recorded++
			}
		}
	}
	idx.ready = true
	if !wasReady {
		rotateIfRBACEventSeenPreBuild()
	}
	return recorded
}

// BindingsIndexNamesSubjectForTest reports whether a binding the index holds
// names the subject: the index's MEMBERSHIP, read whether the index is ready
// or not (the enumerators answer nothing before ready). TEST-ONLY.
func BindingsIndexNamesSubjectForTest(kind, namespace, name string) bool {
	want := subjectKey{Kind: kind, Name: name, Namespace: namespace}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, e := range idx.entries {
		for _, s := range e.subjects {
			if s == want {
				return true
			}
		}
	}
	return false
}
