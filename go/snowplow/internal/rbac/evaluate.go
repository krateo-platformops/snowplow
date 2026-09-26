// Package rbac — EvaluateRBAC: in-process Role-Based Access Control
// evaluator (Tag 0.30.4, Revision 1 binding).
//
// In cache=on mode snowplow MUST satisfy every Role-Based Access Control
// check against the informer-cached RBAC types (Role, RoleBinding,
// ClusterRole, ClusterRoleBinding). ZERO SubjectAccessReview calls to
// apiserver in cache=on mode — that rule is hard-tested in
// evaluate_test.go and is the rollback trigger for this tag.
//
// In cache=off mode the helper falls through to SubjectAccessReview
// (correctness baseline) — preserves the CACHE_ENABLED toggle's
// removability contract per project_redis_removal.md.
package rbac

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// evaluateRBACCallCount is a process-scoped counter of EvaluateRBAC
// invocations. It exists so tests can assert call-count properties (the
// 0.30.111 namespace-keyed memo falsifier asserts an N-namespace LIST
// makes exactly N EvaluateRBAC calls). One atomic add per call — the
// production cost is negligible and the counter is never read on a hot
// path. Mirrors the established api-package metrics-counter pattern
// (dispatchInformerRBACDropped).
var evaluateRBACCallCount atomic.Uint64

// EvaluateRBACCallCount returns the number of EvaluateRBAC calls since
// process start (or since the last ResetEvaluateRBACCallCount). Exported
// for test instrumentation; production code has no reason to read it.
func EvaluateRBACCallCount() uint64 {
	return evaluateRBACCallCount.Load()
}

// ResetEvaluateRBACCallCount zeroes the EvaluateRBAC call counter.
// TEST-ONLY — production code MUST NOT call it.
func ResetEvaluateRBACCallCount() {
	evaluateRBACCallCount.Store(0)
}

// EvaluateOptions captures every input the evaluator needs to make a
// permit/deny decision. Mirrors authorizationv1.ResourceAttributes so
// the cache=off fallback (SubjectAccessReview) is a one-to-one mapping.
type EvaluateOptions struct {
	// Username is the authenticated user (e.g. "cyberjoker").
	Username string
	// Groups are the user's group memberships (e.g. {"devs"}).
	Groups []string
	// Verb is the Kubernetes Role-Based Access Control verb (lowercase,
	// e.g. "get", "list", "watch", "create", "update", "patch",
	// "delete"). Wildcard "*" matches every verb.
	Verb string
	// Group is the API group ("" for the core group).
	Group string
	// Resource is the plural resource name (e.g. "secrets",
	// "restactions").
	Resource string
	// Namespace is the request namespace. Empty string = cluster-wide.
	Namespace string
	// Name is the request object's name. For name-specific verbs
	// ("get"/"update"/"patch"/"delete" and similar) it is the name of
	// the single object the request targets — e.g. for a per-item check
	// on a served LIST it is that item's metadata.name. Empty string
	// means "no single named object" (the request is a collection-verb
	// such as "list"/"watch"/"create"/"deletecollection", or the caller
	// did not thread a name).
	//
	// Name is consumed by Kubernetes `resourceNames` semantics
	// (ResourceNameMatches): a PolicyRule with a non-empty
	// rule.ResourceNames only matches a request whose Name is in that
	// list — and only for name-specific verbs. A resourceNames-scoped
	// rule NEVER grants a collection verb. See rulesPermit /
	// resourceNameMatches below.
	Name string

	// SkipBindingUID, when true, lets the evaluator SKIP the
	// (Name, UID) stable-sort of the CRB/RB candidate sets
	// (sortCRBsStable / sortRBsStable). That sort exists ONLY to make
	// the returned matchedBindingUID deterministic across snapshot
	// republishes (design §6). The permit/deny VERDICT is
	// order-independent — EvaluateRBAC early-returns on the FIRST
	// matching binding and any matching binding permits (RBAC v1 has no
	// deny rules), so the yes/no answer is identical with or without the
	// sort. SkipBindingUID=true is therefore safe for callers that
	// consume `allowed` only and discard matchedBindingUID via `_`.
	//
	// DEFAULT (zero value) is false = DO NOT skip = sort runs and the
	// returned matchedBindingUID is the deterministic first-match UID.
	// This is the SAFE default: any caller that does not explicitly opt
	// in keeps the stable UID. Inverted field name (Skip…) is chosen so
	// the zero-value struct preserves the pre-Ship-L1 behaviour — the
	// cache-key callers (dispatchCacheLookupKey, ra_full_list, the
	// helpers.go diagnostic) set NOTHING and keep the deterministic UID.
	//
	// Ship L1 (0.30.252): set true from the six per-item call sites
	// enumerated below that discard matchedBindingUID. On the dominant
	// 17,929-CRB SA-refilter path this removes the per-item stable-sort
	// (~43% of pod CPU at 50K scale — task-288 §levers L1).
	SkipBindingUID bool
}

// Ship B (0.30.138): the rbac-package GVR vars are dead — the snapshot
// reads come from `*cache.RBACSnapshot` fields, and the writer side
// owns the GVR set via `rbacTypedGVRs` (strip.go:101-106). Removing
// the duplicate set here aligns with the single-source-of-truth rule
// recorded in the design's `feedback_no_special_cases` discussion.

// EvaluateRBAC returns (allowed, matchedBindingUID, err).
//
// allowed: true iff opts describes an action permitted by the cluster
// Role-Based Access Control rules, evaluated against the in-process
// informer cache. Semantics match Kubernetes apiserver:
//   - any matching rule permits (no deny rules in RBAC v1)
//   - "*" wildcards match every verb / resource / API group
//   - empty Username / Groups is treated as "no subject matches" → deny
//
// matchedBindingUID: the per-binding identity of the FIRST-MATCH CRB or
// RB that authorised the access (cache.BindingUIDFromCRB / FromRB
// applied to the matching binding). Ship 0.30.242 H.c-layered (Phase 2
// step 2a) addition:
//   - allowed=true  => matchedBindingUID is the first-permitting binding's
//     UID-stable identity ("C:<uid>" for a CRB; "R:<ns>/<uid>" for an RB).
//   - allowed=false => matchedBindingUID == "" (no permit, no match).
//   - cache=off     => matchedBindingUID == "" (no snapshot; SAR fallback).
//
// FIRST-MATCH STABLE ORDERING (design §6): within each phase (CRB then
// RB), candidates are sorted by (Name, UID) AFTER the index pre-filter.
// CRB phase precedes RB phase (k8s upstream convention; preserves v3's
// two-phase walk semantics).
//
// In cache=off mode (cache.Disabled() == true) the function falls
// through to SubjectAccessReview-via-UserCan with a synthesised
// UserCanOptions. The fallback exists so CACHE_ENABLED=false retains
// the upstream correctness baseline (project_redis_removal.md). No
// BindingUID is returned under cache=off — there is no in-process
// snapshot to identify the matching binding from.
//
// Returns (true, uid, nil) on permit, (false, "", nil) on deny,
// (false, "", err) on internal evaluator error.
//
// PER-ITEM CALLERS: 6 sites use the ALLOWED return only and ignore the
// matchedBindingUID via `_` (helpers.go:checkDispatchRBAC, refilter.go
// evalSingle, informer_dispatch_rbac.go list-filter + get-filter,
// cluster_list.go gate, informer_serve.go get-filter). The signature
// change is the SINGLE source of truth: cache-key callers consume
// matchedBindingUID; per-item callers consume allowed. No split API.
//
// Ship L1 (0.30.252): those 6 per-item sites pass
// EvaluateOptions.SkipBindingUID=true so the evaluator skips the
// CRB/RB stable-sort (the sort exists only to make matchedBindingUID
// deterministic — see EvaluateOptions.SkipBindingUID). The cache-key
// callers (dispatchCacheLookupKey, ra_full_list, the helpers.go
// diagnostic) leave SkipBindingUID at its safe zero-value (false) and
// keep the deterministic UID.
func EvaluateRBAC(ctx context.Context, opts EvaluateOptions) (allowed bool, matchedBindingUID string, err error) {
	log := xcontext.Logger(ctx)
	evaluateRBACCallCount.Add(1)

	// v7 Step 2D — DARK shadow-parity hook. NAMED RETURNS + hoisted `snap`
	// exist ONLY so this defer can read the final (allowed, err) and the
	// snapshot the verdict was decided against. The logic below is unchanged —
	// F-D6(b) proves the verdict+UID table is byte-identical before/after this
	// signature refactor.
	//
	// The hook fires on returns where snap != nil && err == nil: the memo-hit
	// permit (:249-ish) and the walk permit/deny (:301-ish). It NO-OPS on
	// cache-off (snap stays nil), nil-snap / not-wired (err != nil), and
	// evaluator error (err != nil). It is read-only and recover-isolated (the
	// hook body owns dark_panic_total and its own recover; this deferred
	// recover is the ultimate backstop — a dark panic must never crash a live
	// request). Toggle DEFAULT-OFF: one atomic load then return on the hot path.
	var snap *cache.RBACSnapshot
	defer func() {
		if !shadowParityEnabled.Load() {
			return
		}
		if snap == nil || err != nil {
			return
		}
		h := shadowHook.Load()
		if h == nil {
			return
		}
		defer func() { _ = recover() }()
		(*h)(ctx, snap, opts, allowed)
	}()

	if cache.Disabled() {
		// Cache=off correctness baseline. UserCan reads the user's
		// endpoint from ctx and issues a SelfSubjectAccessReview. No
		// BindingUID is available (no snapshot).
		ok := UserCan(ctx, UserCanOptions{
			Verb: opts.Verb,
			GroupResource: schema.GroupResource{
				Group: opts.Group, Resource: opts.Resource,
			},
			Namespace: opts.Namespace,
		})
		return ok, "", nil
	}

	rw := cache.Global()
	if rw == nil {
		// Cache=on flagged but watcher not wired — defensive
		// degrade-to-deny. Without the informer we cannot honour the
		// "zero SubjectAccessReview in cache=on" rule, and we MUST NOT
		// silently fall back to apiserver (would violate Revision 1).
		log.Warn("rbac.evaluate: cache enabled but Global() is nil — denying",
			slog.String("user", opts.Username),
			slog.String("verb", opts.Verb),
			slog.String("group", opts.Group),
			slog.String("resource", opts.Resource),
			slog.String("namespace", opts.Namespace),
		)
		return false, "", fmt.Errorf("rbac: cache=on but ResourceWatcher not wired")
	}

	// Ship B (0.30.138, AC-B.3) — SINGLE rbacSnap.Load() at the top of
	// EvaluateRBAC. The resulting *cache.RBACSnapshot is threaded as an
	// explicit parameter into evaluateAgainstInformerFirstMatch AND
	// roleRefPermits, so every read inside one EvaluateRBAC call observes
	// the SAME snapshot version (AC-B.3).
	snap = rw.Snapshot()
	if snap == nil {
		// AC-B.8 — degrade-to-deny pre-readiness gate.
		log.Warn("rbac.evaluate: typed-RBAC snapshot not yet published — denying",
			slog.String("user", opts.Username),
			slog.String("verb", opts.Verb),
			slog.String("group", opts.Group),
			slog.String("resource", opts.Resource),
			slog.String("namespace", opts.Namespace),
		)
		return false, "", fmt.Errorf("rbac: snapshot not yet built")
	}

	// Ship L2 (0.30.253) — snapshot-generation authz memo. Sits AFTER the
	// cache=off / nil-snap guards (cache=off is the SAR baseline and is
	// NEVER memoised) and BEFORE the candidate walk. The generation we
	// validate against is snap.PublishSeq — a plain field read on the
	// snapshot pointer we ALREADY hold (no second snapshot load, no TTL;
	// PM B1). A hit replaces the O(candidate-CRB) walk with one keyed map
	// read; the whole-shard swap on a PublishSeq change is the only
	// invalidation, so no stale verdict survives a republish (PM B1,
	// design §3.3). SkipBindingUID is part of the key so a UID-consumer
	// (SkipBindingUID=false) and a verdict-only consumer never share an
	// entry — the UID-consumer's cached UID is the deterministic sorted
	// first-match, identical to the cold walk (PM B6, design §3.1).
	memoKey := snapshotAuthzKey{
		Gen:        snap.PublishSeq,
		Username:   opts.Username,
		GroupsHash: canonicalGroupsHash(opts.Groups),
		Verb:       opts.Verb,
		Group:      opts.Group,
		Resource:   opts.Resource,
		Namespace:  opts.Namespace,
		Name:       opts.Name,
		SkipUID:    opts.SkipBindingUID,
	}
	if v, ok := authzMemoLookup(snap.PublishSeq, memoKey); ok {
		log.Debug("rbac.evaluate",
			slog.String("path", "in-process-memo-hit"),
			slog.String("user", opts.Username),
			slog.Bool("allowed", v.Allowed),
			slog.String("matched_binding_uid", v.MatchedBindingUID),
		)
		return v.Allowed, v.MatchedBindingUID, nil
	}

	allowed, matchedBindingUID, err = evaluateAgainstInformerFirstMatch(ctx, snap, opts)
	if err != nil {
		log.Error("rbac.evaluate: informer evaluation failed",
			slog.String("user", opts.Username), slog.Any("err", err))
		return false, "", err
	}

	// Store the freshly-walked verdict for this generation — but ONLY
	// PERMITS (allowed == true). Ship 0.30.254 / Task #301 corrective:
	// NEVER cache a deny.
	//
	// WHY denies must not be cached: a deny can be TRANSIENTLY WRONG under
	// snapshot churn. EvaluateRBAC is fail-closed per-call — a walk that
	// runs while the candidate index / ClusterRolesByName resolve is
	// momentarily not yet coherent during a rebuild can return a wrong
	// `false` (RecordRBACSnapshotMiss path). Pre-L2 that wrong deny
	// self-healed on the very next call (the snapshot is coherent a moment
	// later). Caching it pinned the wrong deny for the whole RBAC
	// generation on a HOT key (the snowplow SA holds a static wildcard CRB
	// that can NEVER be correctly denied), starving the RAFullList refresh
	// and leaving admin compositions-list stuck empty (#301 §G).
	//
	// A deny therefore falls back to the walk every call — the same cost
	// L2 already pays on a miss — and self-heals exactly as pre-L2. This
	// is safe AND keeps the CPU win: permits dominate the hot population
	// (the wildcard-SA refresher + admin are permit-dominated; #301 §H:
	// 160M hits were overwhelmingly permits), so the walk-collapse
	// survives. A permit is monotone-correct within a generation (a
	// binding removal bumps PublishSeq and CAS-swaps the shard), so it is
	// safe to cache; only the deny verdict can be transiently wrong.
	if allowed {
		authzMemoStore(snap.PublishSeq, memoKey, snapshotAuthzVerdict{
			Allowed:           true,
			MatchedBindingUID: matchedBindingUID,
		})
	} else {
		authzMemoDenyUncached.Add(1)
	}

	log.Debug("rbac.evaluate",
		slog.String("path", "in-process"),
		slog.String("user", opts.Username),
		slog.String("verb", opts.Verb),
		slog.String("group", opts.Group),
		slog.String("resource", opts.Resource),
		slog.String("namespace", opts.Namespace),
		slog.Bool("allowed", allowed),
		slog.String("matched_binding_uid", matchedBindingUID),
	)
	return allowed, matchedBindingUID, nil
}

// evaluateAgainstInformerFirstMatch walks every ClusterRoleBinding and
// (when namespace is non-empty) RoleBinding in opts.Namespace looking
// for a Subject that matches opts.Username / opts.Groups /
// "system:authenticated". For every match the bound Role / ClusterRole
// is resolved and its rules walked. First permitting rule wins (RBAC
// semantics).
//
// Ship 0.30.242 H.c-layered (Phase 2 step 2a) — RENAMED from
// evaluateAgainstInformer. Two ADDITIVE changes vs the pre-rename body:
//
//	(a) selectCRBCandidates / selectRBCandidates results are sorted into
//	    stable lexicographic order (Name, UID) BEFORE iteration. This
//	    guarantees the FIRST-MATCH BindingUID is deterministic across
//	    snapshot republishes (design §6).
//	(b) on first permit the function returns the binding's UID alongside
//	    the verdict. CRB matches produce "C:<uid>"; RB matches produce
//	    "R:<ns>/<uid>" (cache.BindingUIDFromCRB / FromRB).
//
// Ship B (0.30.138) — reads typed *rbacv1.{ClusterRole,Role}Binding
// from a pre-built `*cache.RBACSnapshot` passed in by EvaluateRBAC. No
// per-call ListTypedObjects / GetTypedObject. AC-B.3: the snap pointer
// is captured ONCE at the top of EvaluateRBAC and threaded through
// every sub-read so one eval observes one coherent snapshot version.
//
// 0.30.6's subject-prefilter ordering is preserved (subject match
// BEFORE the rule walk) so a no-subject CRB still costs only the
// subject scan, not the expensive PolicyRule walk.
//
// COST: the sort is O(K log K) where K = candidate count. K is bounded:
// admin matches ~50 CRBs at production scale; cyberjoker matches ~12.
// The sort is negligible vs the rule-walk inside roleRefPermits.
func evaluateAgainstInformerFirstMatch(ctx context.Context, snap *cache.RBACSnapshot, opts EvaluateOptions) (bool, string, error) {
	log := xcontext.Logger(ctx)

	// 1) ClusterRoleBindings — apply cluster-wide. Cluster-wide
	//    permits override namespace scope. Sort in stable order
	//    (design §6) so the first-match BindingUID is deterministic.
	crbCandidates := selectCRBCandidates(snap, opts)
	// The stable sort exists ONLY to make the returned matchedBindingUID
	// deterministic; the verdict is order-independent (first match wins,
	// any match permits). Skip it when the caller discards the UID.
	if !opts.SkipBindingUID {
		sortCRBsStable(crbCandidates)
	}
	for _, crb := range crbCandidates {
		// Subject prefilter FIRST — skip the entire roleRefPermits
		// walk when no subject matches. The index already narrowed
		// the candidate set; this matcher is the post-lookup
		// correctness gate.
		if !anySubjectMatches(crb.Subjects, opts) {
			continue
		}
		permits, err := roleRefPermits(snap, "", crb.RoleRef, opts, log)
		if err != nil {
			return false, "", err
		}
		if permits {
			return true, cache.BindingUIDFromCRB(crb), nil
		}
	}

	// 2) RoleBindings in opts.Namespace — only when namespace is set.
	//    A RoleBinding's permit is scoped to its own namespace; the
	//    RoleRef can point at a Role (same namespace) or a ClusterRole
	//    (cluster-wide) but the binding's effect is the namespace.
	if opts.Namespace != "" {
		rbCandidates := selectRBCandidates(snap, opts.Namespace, opts)
		if !opts.SkipBindingUID {
			sortRBsStable(rbCandidates)
		}
		for _, rb := range rbCandidates {
			if !anySubjectMatches(rb.Subjects, opts) {
				continue
			}
			permits, err := roleRefPermits(snap, opts.Namespace, rb.RoleRef, opts, log)
			if err != nil {
				return false, "", err
			}
			if permits {
				return true, cache.BindingUIDFromRB(rb), nil
			}
		}
	}

	return false, "", nil
}

// sortCRBsStable sorts candidate CRBs into stable lexicographic order
// on (Name, UID). All candidates share Kind="ClusterRoleBinding" and
// Namespace="" so those tuple dimensions are implicit. Used by
// evaluateAgainstInformerFirstMatch to make first-match deterministic.
//
// sort.SliceStable is used so equal-key candidates (only possible under
// apiserver bugs / mocks) preserve their index-pre-filter order.
func sortCRBsStable(crbs []*rbacv1.ClusterRoleBinding) {
	sort.SliceStable(crbs, func(i, j int) bool {
		if crbs[i].Name != crbs[j].Name {
			return crbs[i].Name < crbs[j].Name
		}
		return string(crbs[i].UID) < string(crbs[j].UID)
	})
}

// sortRBsStable sorts candidate RBs into stable lexicographic order on
// (Namespace, Name, UID). Namespace is defensively included even though
// selectRBCandidates returns RBs from a single ns by construction.
func sortRBsStable(rbs []*rbacv1.RoleBinding) {
	sort.SliceStable(rbs, func(i, j int) bool {
		if rbs[i].Namespace != rbs[j].Namespace {
			return rbs[i].Namespace < rbs[j].Namespace
		}
		if rbs[i].Name != rbs[j].Name {
			return rbs[i].Name < rbs[j].Name
		}
		return string(rbs[i].UID) < string(rbs[j].UID)
	})
}

// selectCRBCandidates returns the candidate set of ClusterRoleBindings
// reachable by opts — a strict SUPERSET of the linear-scan match set.
// The caller (evaluateAgainstInformerFirstMatch) then post-filters each candidate
// with anySubjectMatches (the canonical correctness barrier, evaluate.go:402-431).
//
// Routing — mirrors the index population in cache.rebuildSubjectIndexes
// and the architect's §3 case-walk one-to-one:
//
//  1. CRBsByUser[opts.Username]                          (User subjects)
//  2. CRBsByGroup[g] for every g in effectiveGroups(opts) (Group subjects;
//     effectiveGroups adds the synthetic system:serviceaccounts groups
//     for SA identities, evaluate.go:451-462 — REUSED here so the index
//     and the matcher agree on group expansion)
//  3. CRBsByGroup["system:authenticated"]                (implicit group;
//     mirrors anySubjectMatches' s.Name == "system:authenticated" branch.
//     The matcher requires opts.Username != "", so we only include this
//     index landing when the request is authenticated.)
//  4. CRBsByServiceAccount["<ns>/<name>"]                (SA subjects;
//     only when opts.Username is a canonical SA)
//  5. CRBsCatchAll                                        (unrecognised
//     Subject.Kind safety net — under-inclusion would silently drop a
//     future-Kind binding, which the matcher would otherwise still match)
//
// Pointer-dedup — a CRB whose Subjects match multiple routes (e.g. a
// User + Group subject set, or a multi-subject CRB hitting both the
// per-user and the per-group index) appears in the union slice once.
// Dedup wastes a few extra anySubjectMatches calls but never produces
// a wrong answer.
//
// Returns nil-or-empty when no route hits — the caller iterates over
// zero elements, identical to the pre-Ship-169 linear scan's behaviour
// when no CRB matches.
func selectCRBCandidates(snap *cache.RBACSnapshot, opts EvaluateOptions) []*rbacv1.ClusterRoleBinding {
	if snap == nil {
		return nil
	}

	// effectiveGroups handles the SA-synthetic-groups expansion. We
	// derive isSA + saNS here so both selectCRBCandidates and
	// effectiveGroups agree on the SA identity.
	saNS, saName, isSA := parseServiceAccountUsername(opts.Username)
	groups := effectiveGroups(opts, isSA, saNS)

	// Pointer-keyed dedup. A binding appearing under multiple index
	// landings is added once.
	seen := make(map[*rbacv1.ClusterRoleBinding]struct{})
	var out []*rbacv1.ClusterRoleBinding
	add := func(crbs []*rbacv1.ClusterRoleBinding) {
		for _, c := range crbs {
			if _, ok := seen[c]; ok {
				continue
			}
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}

	if opts.Username != "" {
		add(snap.CRBsByUser[opts.Username])
	}
	for _, g := range groups {
		add(snap.CRBsByGroup[g])
	}
	// system:authenticated is implicit for every authenticated request.
	// anySubjectMatches gates this on opts.Username != "" — mirror that
	// here so we don't include the implicit-group landing for an empty
	// (unauthenticated) identity.
	if opts.Username != "" {
		add(snap.CRBsByGroup["system:authenticated"])
	}
	if isSA {
		add(snap.CRBsByServiceAccount[saNS+"/"+saName])
	}
	// CRBsCatchAll — unrecognised Kind safety net. Always included;
	// post-lookup anySubjectMatches rejects unrecognised kinds via its
	// default switch arm (no case → no match).
	add(snap.CRBsCatchAll)

	return out
}

// routeRBSubjects is the shared subject-routing body extracted from
// selectRBCandidates (v7 Step 2A — design §2.1; PM condition 6, the
// evaluate.go:512-543 span: parseServiceAccountUsername + effectiveGroups +
// the seen/add pointer-dedup + the User/Group/system:authenticated-gated/SA/
// catch-all routing). It routes an identity onto a set of ALREADY
// namespace-scoped RoleBinding subject-index maps and returns the ORDERED,
// pointer-deduped candidate slice.
//
// The four map arguments differ only in how the caller scoped them:
//   - selectRBCandidates passes the per-namespace inner maps
//     (snap.RBs*ByNS[ns]) → candidates for that one namespace.
//   - selectRBCandidatesAllNS passes the flat all-namespace maps
//     (snap.RBs*AllNS) → the union of the subject's candidates across every
//     namespace.
//
// Both selectors share this ONE routing function, so the per-ns path and the
// all-namespace path can never drift (resolves R2-M2; guarded by F-C0).
//
// Behaviour is byte-identical to the pre-extraction inline body: indexing a
// nil map yields a nil slice and add() ranges over it as zero iterations, so
// the old inline nil-map guards (`if inner := m[ns]; inner != nil`) only ever
// skipped a nil-slice add — dropping them changes nothing. The candidate
// ORDER is preserved exactly: User, then each effective group in order, then
// system:authenticated (gated on a non-empty username), then the SA key, then
// the catch-all — first-occurrence position preserved under dedup.
func routeRBSubjects(
	opts EvaluateOptions,
	byUser, byGroup, byServiceAccount map[string][]*rbacv1.RoleBinding,
	catchAll []*rbacv1.RoleBinding,
) []*rbacv1.RoleBinding {
	saNS, saName, isSA := parseServiceAccountUsername(opts.Username)
	groups := effectiveGroups(opts, isSA, saNS)

	seen := make(map[*rbacv1.RoleBinding]struct{})
	var out []*rbacv1.RoleBinding
	add := func(rbs []*rbacv1.RoleBinding) {
		for _, r := range rbs {
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			out = append(out, r)
		}
	}

	if opts.Username != "" {
		add(byUser[opts.Username])
	}
	for _, g := range groups {
		add(byGroup[g])
	}
	// system:authenticated is implicit for every authenticated request;
	// mirror anySubjectMatches by gating it on a non-empty username.
	if opts.Username != "" {
		add(byGroup["system:authenticated"])
	}
	if isSA {
		add(byServiceAccount[saNS+"/"+saName])
	}
	add(catchAll)

	return out
}

// selectRBCandidates is the RoleBinding analogue of selectCRBCandidates,
// scoped to a single namespace. Same routing rules; inner-map lookup on a
// missing namespace returns nil and the routing falls through to
// RBsCatchAllByNS[ns] (also nil for an absent namespace), so the empty
// case yields an empty candidate set with no allocations.
//
// v7 Step 2A: the routing body was extracted verbatim into routeRBSubjects
// (behaviour-preserving — F-C0). This function now only namespace-scopes the
// index maps and delegates.
func selectRBCandidates(snap *cache.RBACSnapshot, ns string, opts EvaluateOptions) []*rbacv1.RoleBinding {
	if snap == nil || ns == "" {
		return nil
	}
	return routeRBSubjects(
		opts,
		snap.RBsByUserByNS[ns],
		snap.RBsByGroupByNS[ns],
		snap.RBsByServiceAccountByNS[ns],
		snap.RBsCatchAllByNS[ns],
	)
}

// selectRBCandidatesAllNS is the all-namespace analogue of
// selectRBCandidates (v7 Step 2A — design §3). It returns the union of a
// subject's RoleBinding candidates across EVERY namespace, using the flat
// all-namespace reverse indexes on the snapshot and the SAME routeRBSubjects
// routing selectRBCandidates uses. The result is pointer-deduped; each
// returned RB carries its own .Namespace, so a caller (the Step B
// requester-profile builder) can bucket the candidates by namespace after an
// anySubjectMatches gate.
//
// DARK in Step 2A: no serving path calls this. Its first consumer is the
// Step B requester profile; it is exercised now only by F-C1 (via
// SelectRBCandidatesAllNSForTest), which proves it equals the union over
// every namespace of selectRBCandidates.
func selectRBCandidatesAllNS(snap *cache.RBACSnapshot, opts EvaluateOptions) []*rbacv1.RoleBinding {
	if snap == nil {
		return nil
	}
	return routeRBSubjects(
		opts,
		snap.RBsByUserAllNS,
		snap.RBsByGroupAllNS,
		snap.RBsByServiceAccountAllNS,
		snap.RBsCatchAllAllNS,
	)
}

// lookupRoleRefRules is the PURE, SIDE-EFFECT-FREE resolve half split out of
// roleRefPermits (v7 Step 2B — design §2.1, Part B). It performs ONLY the
// snapshot map read + kind routing and returns the resolved PolicyRules:
//
//   - ClusterRole: snap.ClusterRolesByName[ref.Name]; miss → (nil, false).
//   - Role: namespace=="" (kind=Role in a ClusterRoleBinding — invalid per
//     Kubernetes) → (nil, false); else snap.RolesByNSName[namespace+"/"+
//     ref.Name]; miss → (nil, false).
//   - any other kind → (nil, false).
//
// The second return ("resolved") is true iff a rule set was found. A
// (nil, true) never happens: a hit always carries the target's .Rules slice
// (which may be empty for a rule-less Role/ClusterRole — still a resolve hit,
// just a deny once rulesPermit runs).
//
// DARK-SAFETY — LOAD-BEARING: lookupRoleRefRules calls NO counter and NO log;
// in particular it MUST NOT call cache.RecordRBACSnapshotMiss. The dark
// requester profile (profile.go) resolves a requester's every roleRef through
// this function, once per (identity, generation). Recording a miss here would
// inflate the production AC-B.10 miss ratio (cache.RecordRBACSnapshotMiss /
// rbac_snapshot.go:221-234) and break the Step-2 dark invariant. All miss
// accounting stays at roleRefPermits' OWN (served-path) call site below, so the
// served path's miss-count is byte-identical to pre-split (F-D1).
func lookupRoleRefRules(snap *cache.RBACSnapshot, namespace string, ref rbacv1.RoleRef) ([]rbacv1.PolicyRule, bool) {
	switch ref.Kind {
	case "ClusterRole":
		cr, ok := snap.ClusterRolesByName[ref.Name]
		if !ok {
			return nil, false
		}
		return cr.Rules, true

	case "Role":
		if namespace == "" {
			// kind=Role in a ClusterRoleBinding is invalid per
			// Kubernetes — treat as deny (no resolve).
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

// roleRefPermits resolves ref (Role or ClusterRole) against the
// passed-in snapshot and walks its rules. namespace is the
// RoleBinding's namespace (used to resolve kind=Role); empty when ref
// came from a ClusterRoleBinding.
//
// Ship B (0.30.138) — map lookups on the snapshot replace the
// per-call GetTypedObject reads (pre-Ship-B: evaluate.go:254/:270). A
// missed lookup (`!ok`) is recorded via cache.RecordRBACSnapshotMiss
// (AC-B.10) and treated as a deny — same fail-closed posture as
// today's GetTypedObject !ok.
//
// v7 Step 2B — the resolve half is now lookupRoleRefRules (above); this
// function is resolve + rulesPermit + miss-accounting. The miss bump stays
// HERE, at the served-path call site, with the SAME (kind, namespace, name)
// labels and the SAME selectivity as the pre-split switch: ONLY a ClusterRole
// absent from the snapshot, and a namespaced Role (namespace != "") absent from
// the snapshot, were ever recorded. The kind=Role-in-CRB (namespace=="") and
// unknown-kind arms denied WITHOUT recording, so they stay silent here too.
// This keeps roleRefPermits byte-identical in verdict AND miss-count to
// pre-split (F-D1); lookupRoleRefRules records nothing, so the dark profile
// builder never reaches this bump.
func roleRefPermits(snap *cache.RBACSnapshot, namespace string, ref rbacv1.RoleRef, opts EvaluateOptions, log *slog.Logger) (bool, error) {
	_ = log // reserved for future per-ref debug logging; kept in signature for parity
	rules, ok := lookupRoleRefRules(snap, namespace, ref)
	if ok {
		return rulesPermit(rules, opts), nil
	}
	// Resolve miss — record it with the exact pre-split labels/selectivity.
	switch ref.Kind {
	case "ClusterRole":
		cache.RecordRBACSnapshotMiss("ClusterRole", "", ref.Name)
	case "Role":
		if namespace != "" {
			cache.RecordRBACSnapshotMiss("Role", namespace, ref.Name)
		}
	}
	return false, nil
}

// rulesPermit returns true iff any PolicyRule in rules permits opts.
// Wildcard semantics match Kubernetes: "*" in Verbs, Resources or
// APIGroups matches everything.
//
// 0.30.109 (G1) — rule.ResourceNames is now honoured. A rule scoped to
// specific named objects (resourceNames: ["foo"]) must NOT be treated
// as granting every object of that GVR. resourceNameMatches implements
// the Kubernetes `ResourceNameMatches` semantics — see its doc comment.
// Before 0.30.109 this check was absent: a resourceNames-scoped rule
// over-exposed every object (cross-user leak in filterListByRBAC).
func rulesPermit(rules []rbacv1.PolicyRule, opts EvaluateOptions) bool {
	for _, rule := range rules {
		if !stringSliceMatches(rule.Verbs, opts.Verb) {
			continue
		}
		if !stringSliceMatches(rule.APIGroups, opts.Group) {
			continue
		}
		if !stringSliceMatches(rule.Resources, opts.Resource) {
			continue
		}
		if !resourceNameMatches(rule, opts) {
			continue
		}
		return true
	}
	return false
}

// nameSpecificVerbs is the set of RBAC verbs that act on a single named
// object — the only verbs for which a resourceNames-scoped rule can
// grant access. Mirrors the Kubernetes RBAC authorizer: the collection
// verbs ("list", "watch", "create", "deletecollection") have no single
// named object, so a rule with a non-empty ResourceNames must never
// match them. (rbac/v1's authorizer scopes resourceNames to exactly
// these verbs; "get"/"update"/"patch"/"delete" — and the "*" wildcard,
// handled separately — are name-specific.)
var nameSpecificVerbs = map[string]struct{}{
	"get":    {},
	"update": {},
	"patch":  {},
	"delete": {},
}

// IsNameSpecificVerb reports whether verb acts on a single named object —
// i.e. whether a resourceNames-scoped rule (and therefore the request's
// object Name) can EVER matter for this verb. It is the exact predicate
// resourceNameMatches uses internally, exported so callers that derive a
// per-object Name (e.g. the userAccessFilter refilter's NameFrom, #123) can
// skip that derivation for collection verbs — where the Name is never
// consulted — rather than fail-closed on a Name-derivation error that would
// be irrelevant to the grant. The "*" verb wildcard on a request is not a
// real request verb (requests carry a concrete verb), so it is not listed;
// resourceNameMatches handles the rule-side wildcard separately.
func IsNameSpecificVerb(verb string) bool {
	_, ok := nameSpecificVerbs[verb]
	return ok
}

// resourceNameMatches implements Kubernetes `ResourceNameMatches`
// semantics for a single PolicyRule (0.30.109, G1):
//
//   - rule.ResourceNames empty  → matches all objects (unchanged
//     behaviour for unscoped rules).
//   - rule.ResourceNames non-empty → the rule is scoped to specific
//     named objects:
//   - It can only ever match a name-specific verb
//     ("get"/"update"/"patch"/"delete", or the verb wildcard "*").
//     For a collection verb ("list"/"watch"/"create"/
//     "deletecollection") it does NOT match — a resourceNames-scoped
//     rule never grants `list`.
//   - The request's object name (opts.Name) must appear in
//     rule.ResourceNames.
//
// opts.Verb is already known to satisfy the rule's Verbs list by the
// time this is called (rulesPermit checks Verbs first). We re-derive
// the name-specific predicate from opts.Verb itself rather than the
// rule's Verbs so that a wildcard-verb rule with resourceNames is still
// correctly denied for a collection-verb request.
func resourceNameMatches(rule rbacv1.PolicyRule, opts EvaluateOptions) bool {
	if len(rule.ResourceNames) == 0 {
		return true
	}
	// Non-empty ResourceNames: only name-specific verbs can match.
	if _, nameSpecific := nameSpecificVerbs[opts.Verb]; !nameSpecific {
		return false
	}
	// The targeted object's name must be in the list.
	for _, n := range rule.ResourceNames {
		if n == opts.Name {
			return true
		}
	}
	return false
}

// stringSliceMatches implements the RBAC wildcard rule: "*" matches
// every value; otherwise an exact match is required.
func stringSliceMatches(allowed []string, want string) bool {
	for _, a := range allowed {
		if a == "*" || a == want {
			return true
		}
	}
	return false
}

// anySubjectMatches returns true iff opts.Username, any of opts.Groups
// (as Kind="Group") or the system-authenticated group appears in subjects.
// ServiceAccount subjects are matched when opts.Username has the
// canonical "system:serviceaccount:<ns>:<name>" form.
//
// 0.30.109 (G3/G6) — ServiceAccount synthetic groups. Kubernetes
// implicitly places every ServiceAccount in two synthetic groups:
//   - "system:serviceaccounts"            (all ServiceAccounts)
//   - "system:serviceaccounts:<ns>"       (all SAs in the SA's namespace)
//
// A binding granting a Group subject of either name must therefore
// match a request whose Username is a canonical ServiceAccount in the
// matching namespace. effectiveGroups (below) computes the augmented
// group set; anySubjectMatches consults it for every Group subject.
// This mirrors k8s.io/apiserver's serviceaccount.UserInfo.
func anySubjectMatches(subjects []rbacv1.Subject, opts EvaluateOptions) bool {
	saNS, saName, isSA := parseServiceAccountUsername(opts.Username)
	groups := effectiveGroups(opts, isSA, saNS)

	for _, s := range subjects {
		switch s.Kind {
		case rbacv1.UserKind:
			if s.Name == opts.Username {
				return true
			}
		case rbacv1.GroupKind:
			for _, g := range groups {
				if s.Name == g {
					return true
				}
			}
			// Every authenticated request gains the
			// system:authenticated group implicitly (Kubernetes
			// auth chain).
			if s.Name == "system:authenticated" && opts.Username != "" {
				return true
			}
		case rbacv1.ServiceAccountKind:
			if isSA && s.Namespace == saNS && s.Name == saName {
				return true
			}
		}
	}
	return false
}

// well-known ServiceAccount synthetic group names (0.30.109, G3/G6).
// Mirrors k8s.io/apiserver/pkg/authentication/serviceaccount.
const (
	allServiceAccountsGroup     = "system:serviceaccounts"
	serviceAccountsNamespacePfx = "system:serviceaccounts:"
)

// effectiveGroups returns the group set used for Group-subject matching.
// It is opts.Groups plus, when the request identity is a canonical
// ServiceAccount, the two synthetic ServiceAccount groups Kubernetes
// adds implicitly:
//
//	system:serviceaccounts
//	system:serviceaccounts:<the-SA's-namespace>
//
// For a non-ServiceAccount identity the synthetic groups are not added
// and the function returns opts.Groups unchanged (no allocation when
// there is nothing to add).
func effectiveGroups(opts EvaluateOptions, isSA bool, saNS string) []string {
	if !isSA {
		return opts.Groups
	}
	groups := make([]string, 0, len(opts.Groups)+2)
	groups = append(groups, opts.Groups...)
	groups = append(groups,
		allServiceAccountsGroup,
		serviceAccountsNamespacePfx+saNS,
	)
	return groups
}

// parseServiceAccountUsername decodes the
// "system:serviceaccount:<ns>:<name>" form. Returns (ns, name, true) on
// success; ("", "", false) for non-ServiceAccount usernames.
func parseServiceAccountUsername(u string) (string, string, bool) {
	const prefix = "system:serviceaccount:"
	if !strings.HasPrefix(u, prefix) {
		return "", "", false
	}
	rest := u[len(prefix):]
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// IsServiceAccountUsername reports whether u is a canonical Kubernetes
// ServiceAccount username of the form "system:serviceaccount:<ns>:<name>".
// It is the boolean projection of parseServiceAccountUsername — the single
// source of truth for the SA-identity prefix test — so callers outside this
// package (e.g. the internal-REST-config dispatcher's serve-un-narrowed
// predicate) never hand-roll the "system:serviceaccount:" prefix check.
func IsServiceAccountUsername(u string) bool {
	_, _, ok := parseServiceAccountUsername(u)
	return ok
}

// asClusterRoleBinding extracts a *rbacv1.ClusterRoleBinding from an
// indexer object. Happy path: the indexer already holds a typed
// pointer (cache/strip.go stripAndTypeClusterRoleBinding ran at the
// Add/Update event), the type assertion succeeds, and per-call
// FromUnstructured cost is zero. This is the 0.30.6 headline win.
//
// Defensive fallback path: the indexer entry is still
// *unstructured.Unstructured (e.g. transform missed it, test seeded
// without the transform pipeline, or a future code regression). In
// that case we convert once with the existing toClusterRoleBinding
// helper and log WARN with fallback=true so the regression is loud
// (plan §"Code-path falsifier"). Allow/deny result is bit-exact equal
// either way — the test suite asserts equivalence.
//
// Returns error only when the indexer object is neither typed nor
// convertible (the fallback FromUnstructured failed).
func asClusterRoleBinding(obj interface{}, log *slog.Logger) (*rbacv1.ClusterRoleBinding, error) {
	if crb, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
		return crb, nil
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("rbac.asClusterRoleBinding: indexer object is neither *rbacv1.ClusterRoleBinding nor *unstructured.Unstructured (%T)", obj)
	}
	log.Warn("rbac.indexer.read fallback=true",
		slog.String("subsystem", "rbac"),
		slog.String("kind", "ClusterRoleBinding"),
		slog.String("name", uns.GetName()),
		slog.String("hint", "indexer entry was Unstructured — typed transform did not fire on this object"),
	)
	return toClusterRoleBinding(uns)
}

// asRoleBinding is the RoleBinding analogue of asClusterRoleBinding.
func asRoleBinding(obj interface{}, log *slog.Logger) (*rbacv1.RoleBinding, error) {
	if rb, ok := obj.(*rbacv1.RoleBinding); ok {
		return rb, nil
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("rbac.asRoleBinding: indexer object is neither *rbacv1.RoleBinding nor *unstructured.Unstructured (%T)", obj)
	}
	log.Warn("rbac.indexer.read fallback=true",
		slog.String("subsystem", "rbac"),
		slog.String("kind", "RoleBinding"),
		slog.String("name", uns.GetName()),
		slog.String("namespace", uns.GetNamespace()),
		slog.String("hint", "indexer entry was Unstructured — typed transform did not fire on this object"),
	)
	return toRoleBinding(uns)
}

// asClusterRole is the ClusterRole analogue of asClusterRoleBinding.
func asClusterRole(obj interface{}, log *slog.Logger) (*rbacv1.ClusterRole, error) {
	if cr, ok := obj.(*rbacv1.ClusterRole); ok {
		return cr, nil
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("rbac.asClusterRole: indexer object is neither *rbacv1.ClusterRole nor *unstructured.Unstructured (%T)", obj)
	}
	log.Warn("rbac.indexer.read fallback=true",
		slog.String("subsystem", "rbac"),
		slog.String("kind", "ClusterRole"),
		slog.String("name", uns.GetName()),
		slog.String("hint", "indexer entry was Unstructured — typed transform did not fire on this object"),
	)
	return toClusterRole(uns)
}

// asRole is the Role analogue of asClusterRoleBinding.
func asRole(obj interface{}, log *slog.Logger) (*rbacv1.Role, error) {
	if r, ok := obj.(*rbacv1.Role); ok {
		return r, nil
	}
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("rbac.asRole: indexer object is neither *rbacv1.Role nor *unstructured.Unstructured (%T)", obj)
	}
	log.Warn("rbac.indexer.read fallback=true",
		slog.String("subsystem", "rbac"),
		slog.String("kind", "Role"),
		slog.String("name", uns.GetName()),
		slog.String("namespace", uns.GetNamespace()),
		slog.String("hint", "indexer entry was Unstructured — typed transform did not fire on this object"),
	)
	return toRole(uns)
}

// to{Kind} helpers (below) remain for the defensive Unstructured
// fallback path only. The cache=on happy path uses the as{Kind}
// helpers above with zero per-call conversion.

func toRoleBinding(uns *unstructured.Unstructured) (*rbacv1.RoleBinding, error) {
	out := &rbacv1.RoleBinding{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		return nil, fmt.Errorf("rbac: convert RoleBinding %s/%s: %w", uns.GetNamespace(), uns.GetName(), err)
	}
	return out, nil
}

func toClusterRoleBinding(uns *unstructured.Unstructured) (*rbacv1.ClusterRoleBinding, error) {
	out := &rbacv1.ClusterRoleBinding{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		return nil, fmt.Errorf("rbac: convert ClusterRoleBinding %s: %w", uns.GetName(), err)
	}
	return out, nil
}

func toRole(uns *unstructured.Unstructured) (*rbacv1.Role, error) {
	out := &rbacv1.Role{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		return nil, fmt.Errorf("rbac: convert Role %s/%s: %w", uns.GetNamespace(), uns.GetName(), err)
	}
	return out, nil
}

func toClusterRole(uns *unstructured.Unstructured) (*rbacv1.ClusterRole, error) {
	out := &rbacv1.ClusterRole{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		return nil, fmt.Errorf("rbac: convert ClusterRole %s: %w", uns.GetName(), err)
	}
	return out, nil
}
