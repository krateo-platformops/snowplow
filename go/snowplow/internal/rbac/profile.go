// profile.go — v7 Step 2B: the DARK requester profile R.
//
// R is a SECOND RBAC reading of a single requester's cluster-wide grants,
// assembled ONLY from the evaluator's own unexported functions
// (selectCRBCandidates / anySubjectMatches / selectRBCandidatesAllNS /
// lookupRoleRefRules / rulesPermit). It lives in package rbac precisely so it
// can call those unexported functions verbatim — that verbatim reuse is the
// anti-drift guarantee behind the R-correctness theorem: R.Permits(opts) ==
// EvaluateRBAC(opts).allowed over the WHOLE domain, not just observed rows
// (design §2.1; proven exhaustively by F-D2).
//
// R is REQUEST-INDEPENDENT: it depends only on the requester's identity
// (Username + Groups). It ignores Verb/Group/Resource/Namespace/Name — those
// are supplied later, per check, to Permits. So R is built ~once per
// (identity, generation) and answers many checks (design §2.2).
//
// DARK (Step 2B invariant): NOTHING on any serving path calls R. It is folded
// into no cache key, stored on no cache entry, changes no verdict and no served
// byte, and emits NO observable counter or log. Its only callers in Step 2B are
// this package's tests. lookupRoleRefRules (evaluate.go) is side-effect-free, so
// building R bumps NO production counter — in particular NOT
// cache.RecordRBACSnapshotMiss (F-D5).

package rbac

import (
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
)

// RequesterProfile is the pre-resolved grant set of one requester against one
// RBAC snapshot generation.
//
//   - Gen is the snapshot generation (snap.PublishSeq) R was built against; it
//     scopes R's validity — a republish bumps PublishSeq and R must be rebuilt.
//   - ClusterRules is the union of every ClusterRole rule reachable through a
//     matching ClusterRoleBinding (cluster-wide — applies in every namespace).
//   - NamespacedRules[ns] is the union of every rule reachable through a
//     matching RoleBinding IN namespace ns (applies only in ns).
//
// The union-of-rules shape is what makes Permits equivalent to EvaluateRBAC:
// rulesPermit is a pure OR over its rule slice, so rulesPermit(A‖B) ==
// rulesPermit(A) || rulesPermit(B). Concatenating the matching bindings' rules
// therefore preserves "any matching binding permits" exactly (RBAC v1 has no
// deny rules; first-match is order-independent for the verdict). R stores the
// raw PolicyRules and re-runs the evaluator's rulesPermit, so resourceNames /
// name-specific / wildcard matching is bit-for-bit the served evaluator's.
//
// SEAM (design §2.1): R yields the boolean verdict only; it cannot reproduce
// EvaluateRBAC's matchedBindingUID (that is first-match over an ORDERED
// candidate set — order-dependent). No consumer may derive a BindingUID from R.
type RequesterProfile struct {
	Gen             uint64
	ClusterRules    []rbacv1.PolicyRule
	NamespacedRules map[string][]rbacv1.PolicyRule
}

// BuildRequesterProfile assembles R for identity id against snap. It is a PURE
// cold build (no memo — see RequesterProfileFor for the memoised accessor).
//
// It uses ONLY reused evaluator functions:
//   - cluster scope: selectCRBCandidates + anySubjectMatches gate each CRB,
//     then lookupRoleRefRules(snap,"",crb.RoleRef) appends into ClusterRules.
//   - namespaced scope: selectRBCandidatesAllNS (v7 Step 2A) + anySubjectMatches
//     gate each RB, then lookupRoleRefRules(snap,rb.Namespace,rb.RoleRef) appends
//     into NamespacedRules[rb.Namespace].
//
// A binding whose role is missing (lookupRoleRefRules → false) contributes
// NOTHING — mirroring the evaluator's fail-closed deny — and, because
// lookupRoleRefRules is side-effect-free, records NO snapshot miss (F-D5).
//
// Only id.Username and id.Groups are read; every other EvaluateOptions field is
// ignored (request-independence). We route through an identity-only options
// value to make that contract explicit and drift-proof.
func BuildRequesterProfile(snap *cache.RBACSnapshot, id EvaluateOptions) *RequesterProfile {
	p := &RequesterProfile{
		NamespacedRules: map[string][]rbacv1.PolicyRule{},
	}
	if snap == nil {
		return p
	}
	p.Gen = snap.PublishSeq

	// Identity-only view: only Username + Groups steer candidate selection and
	// subject matching. This is what makes R request-independent.
	idOnly := EvaluateOptions{Username: id.Username, Groups: id.Groups}

	// Cluster scope — matching ClusterRoleBindings → ClusterRole rules.
	for _, crb := range selectCRBCandidates(snap, idOnly) {
		if !anySubjectMatches(crb.Subjects, idOnly) {
			continue
		}
		if rules, ok := lookupRoleRefRules(snap, "", crb.RoleRef); ok {
			p.ClusterRules = append(p.ClusterRules, rules...)
		}
	}

	// Namespaced scope — matching RoleBindings across EVERY namespace, bucketed
	// by each RB's own namespace.
	for _, rb := range selectRBCandidatesAllNS(snap, idOnly) {
		if !anySubjectMatches(rb.Subjects, idOnly) {
			continue
		}
		if rules, ok := lookupRoleRefRules(snap, rb.Namespace, rb.RoleRef); ok {
			p.NamespacedRules[rb.Namespace] = append(p.NamespacedRules[rb.Namespace], rules...)
		}
	}

	return p
}

// Permits answers one authorization check from R. It permits iff any
// ClusterRule permits it OR (for a namespaced request) any rule in the
// request's namespace bucket permits it — reusing the evaluator's rulesPermit
// verbatim (so resourceNames / name-specific / wildcard semantics match
// EvaluateRBAC exactly).
//
// The opts.Namespace != "" guard mirrors evaluateAgainstInformerFirstMatch,
// which only walks RoleBindings when the request carries a namespace. It is
// also defensive: RoleBindings always carry a non-empty namespace, so
// NamespacedRules[""] is always empty anyway.
func (p *RequesterProfile) Permits(opts EvaluateOptions) bool {
	if p == nil {
		return false
	}
	if rulesPermit(p.ClusterRules, opts) {
		return true
	}
	return opts.Namespace != "" && rulesPermit(p.NamespacedRules[opts.Namespace], opts)
}

// ─────────────────────────────────────────────────────────────────────────
// R memo — generation-scoped, bounded, identity-keyed.
//
// Mirrors the snapshot authz memo's shard-swap discipline
// (snapshot_authz_memo.go): a single atomic.Pointer to a shard that carries the
// generation it is valid for, its map, and an RWMutex. A PublishSeq change
// CAS-swaps a fresh empty shard, so an R never outlives its snapshot generation
// — a revoke bumps PublishSeq and drops every stale-gen R atomically (design
// §2.2). Because R is keyed only on (Gen, Username, GroupsHash) it is built at
// most once per identity per generation and reused across every per-item check.
//
// DARK: the memo emits NO expvar and NO log — its only observable surface is
// the test-only stats reader (export_test_hooks.go). Nothing here is identity
// in any exported or logged form: the key stores the raw Username in an
// in-process map (never serialised), and groups are folded to a uint64 via the
// evaluator's own canonicalGroupsHash (order-independent — groups_hash.go).
// ─────────────────────────────────────────────────────────────────────────

// requesterProfileMemoCap bounds one generation's R entries. R is keyed per
// distinct (Username, GroupsHash) identity, NOT per check, so its cardinality
// is the live identity population (~1000 users at production scale;
// project_production_scale). 4096 (2^12) sits comfortably above that with
// headroom for SA identities and group-set variety, and far below the
// per-check authz memo's 16384. On cap breach the memo REFUSES the insert (the
// caller still gets a freshly-built R — it just is not cached); never
// evict-to-OOM. This is a DARK Step-2 bound; the empirical (Username,
// GroupsHash) cardinality from the 057 corpus (design §5.1 Q6) validates/retunes
// it before any Step-3 promotion. Not an env knob (no magic env vars).
const requesterProfileMemoCap = 4096

// requesterProfileKey is the identity-only memo key. Gen scopes it to one
// snapshot generation (redundant with the shard's gen, but keeps a
// transiently-backwards shard from serving a wrong-generation R — same
// load-bearing role Gen-in-key plays in snapshotAuthzKey).
type requesterProfileKey struct {
	Gen        uint64
	Username   string
	GroupsHash uint64 // canonicalGroupsHash(Groups) — groups_hash.go
}

type requesterProfileShard struct {
	gen uint64
	mu  sync.RWMutex
	m   map[requesterProfileKey]*RequesterProfile
}

var requesterProfileMemo atomic.Pointer[requesterProfileShard]

// requesterProfileMemo test-only counters. NOT registered in expvar and NEVER
// logged — the memo is dark. Read only through the *ForTest surface.
var (
	requesterProfileMemoHits    atomic.Uint64
	requesterProfileMemoMisses  atomic.Uint64
	requesterProfileMemoStores  atomic.Uint64
	requesterProfileMemoRefused atomic.Uint64
)

// currentRequesterProfileShard returns the shard valid for gen, CAS-swapping a
// fresh empty shard in if the live one is absent or stale. Guarantees the
// returned shard has shard.gen == gen. Same lost-CAS handling as the authz
// memo's currentAuthzShard.
func currentRequesterProfileShard(gen uint64) *requesterProfileShard {
	for {
		cur := requesterProfileMemo.Load()
		if cur != nil && cur.gen == gen {
			return cur
		}
		fresh := &requesterProfileShard{
			gen: gen,
			m:   make(map[requesterProfileKey]*RequesterProfile, 64),
		}
		if requesterProfileMemo.CompareAndSwap(cur, fresh) {
			return fresh
		}
		// Lost the race — re-evaluate the loaded pointer next iteration.
	}
}

// RequesterProfileFor returns R for identity id against snap, memoised per
// (snap.PublishSeq, Username, canonicalGroupsHash(Groups)). On a hit it returns
// the cached *RequesterProfile (pointer-reused across every check in the
// generation); on a miss it cold-builds via BuildRequesterProfile, stores, and
// returns. A nil snap is not memoised (returns an empty profile).
//
// DARK: no serving path calls this in Step 2B — its callers are this package's
// tests. Its first real (still-dark) consumer is the Step D shadow hook.
func RequesterProfileFor(snap *cache.RBACSnapshot, id EvaluateOptions) *RequesterProfile {
	if snap == nil {
		return BuildRequesterProfile(nil, id)
	}
	gen := snap.PublishSeq
	key := requesterProfileKey{
		Gen:        gen,
		Username:   id.Username,
		GroupsHash: canonicalGroupsHash(id.Groups),
	}

	shard := currentRequesterProfileShard(gen)
	shard.mu.RLock()
	if p, ok := shard.m[key]; ok {
		shard.mu.RUnlock()
		requesterProfileMemoHits.Add(1)
		return p
	}
	shard.mu.RUnlock()
	requesterProfileMemoMisses.Add(1)

	p := BuildRequesterProfile(snap, id)

	// Re-resolve the shard: a republish may have swapped it out between the
	// read-unlock and now. Only store into a shard still stamped for gen.
	shard = currentRequesterProfileShard(gen)
	shard.mu.Lock()
	if existing, ok := shard.m[key]; ok {
		// Another goroutine built + stored first — reuse its pointer so all
		// callers of this generation converge on one R.
		shard.mu.Unlock()
		return existing
	}
	if len(shard.m) >= requesterProfileMemoCap {
		shard.mu.Unlock()
		requesterProfileMemoRefused.Add(1)
		return p // uncached, but correct
	}
	shard.m[key] = p
	shard.mu.Unlock()
	requesterProfileMemoStores.Add(1)
	return p
}

// resetRequesterProfileMemo clears the memo singleton and all counters.
// Exposed to the evaltest package via ResetRequesterProfileMemoForTest.
func resetRequesterProfileMemo() {
	requesterProfileMemo.Store(nil)
	requesterProfileMemoHits.Store(0)
	requesterProfileMemoMisses.Store(0)
	requesterProfileMemoStores.Store(0)
	requesterProfileMemoRefused.Store(0)
}

// requesterProfileMemoStats returns (hits, misses, stores, refused, entries)
// for the current shard. Exposed via RequesterProfileMemoStatsForTest.
func requesterProfileMemoStats() (hits, misses, stores, refused uint64, entries int) {
	hits = requesterProfileMemoHits.Load()
	misses = requesterProfileMemoMisses.Load()
	stores = requesterProfileMemoStores.Load()
	refused = requesterProfileMemoRefused.Load()
	if cur := requesterProfileMemo.Load(); cur != nil {
		cur.mu.RLock()
		entries = len(cur.m)
		cur.mu.RUnlock()
	}
	return
}
