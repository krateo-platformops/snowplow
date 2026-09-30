// snapshot_authz_memo_test.go — Ship L2 (0.30.253), Task #291.
//
// Falsifier set for the snapshot-generation authz memo
// (internal/rbac/snapshot_authz_memo.go). All tests run in the evaltest
// package against an in-process fake-watcher RBACSnapshot — NEVER against
// the remote kubeconfig (feedback_no_go_test_against_remote_kubeconfig).
//
// Coverage:
//   F2     — concurrent -race republish hammer on the SHARED shard.
//   F3+B4  — bidirectional republish invalidation, including the
//            per-Name (resourceNames) dimension (allow `get foo` /
//            deny `get bar` never collapses to one entry).
//   B6     — UID-determinism-through-memo: a HIT on the SkipBindingUID=
//            false path returns the SAME sorted first-match UID as the
//            cold walk.
//   B3     — canonicalGroupsHash collision guard (["a","bc"] vs
//            ["ab","c"]) + group-set order independence.
//   F5-ish — hit-rate + cap counters move as expected on a repeated tuple.

package evaltest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// ──────────────────────────────────────────────────────────────────────
// F3 + B4 — bidirectional republish invalidation incl. per-Name
// ──────────────────────────────────────────────────────────────────────

// rebuildCurrentSnapshot bumps PublishSeq by re-running the synchronous
// rebuild against the live watcher's informer state. Used to force a
// generation change (the memo MUST swap its shard).
func rebuildCurrentSnapshot(t *testing.T) {
	t.Helper()
	rw := cache.Global()
	if rw == nil {
		t.Fatalf("rebuildCurrentSnapshot: cache.Global() is nil")
	}
	cache.RebuildRBACSnapshotForTest(rw)
}

// TestL2_F3_RepublishInvalidation_DenyAllowDeny seeds gen G1 where alice
// is ALLOWED, caches the verdict, then republishes a gen G2 in which the
// binding is GONE (alice DENIED). The memo MUST return false after the
// republish (shard swapped + re-walked), and the swap counter MUST move.
// Then it re-adds and republishes G3 (alice ALLOWED again) — proving the
// invalidation is bidirectional. This is the RBAC-correctness gate
// (PM B1): a stale `true` across a republish is the forbidden defect.
func TestL2_F3_RepublishInvalidation_DenyAllowDeny(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	// We need to mutate the binding set between generations. Hand-build
	// snapshots via the cache test seams: seed an initial allowing
	// snapshot through newTestWatcher, then publish synthetic snapshots
	// for the deny / re-allow generations.
	crb := clusterRoleBindingWithUID("alice-bind", "admin", "alice-crb-uid",
		userSubject("alice"),
	)
	newTestWatcher(t,
		clusterRole("admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		crb,
	)

	opts := rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Resource: "secrets", Namespace: "default",
	}

	// G1 — allowed, then cached.
	a1, _, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil || !a1 {
		t.Fatalf("G1: expected allowed=true err=nil; got allowed=%v err=%v", a1, err)
	}
	// Second call in G1 must be a HIT (counter check below proves it).
	a1b, _, _ := rbac.EvaluateRBAC(context.Background(), opts)
	if !a1b {
		t.Fatalf("G1 second call: expected cached allowed=true; got %v", a1b)
	}
	hits1, _, _, _, _ := rbac.AuthzMemoStatsForTest()
	if hits1 == 0 {
		t.Fatalf("G1: expected at least one memo hit on the repeated tuple; got 0")
	}

	// G2 — publish a synthetic snapshot with NO bindings (alice denied).
	// Build a fresh snapshot literal, populate its subject indexes via the
	// cache seam, stamp a bumped PublishSeq, and publish it. The memo MUST
	// swap its shard on the generation change and re-walk -> deny.
	swapsBefore := func() uint64 { _, _, s, _, _ := rbac.AuthzMemoStatsForTest(); return s }()
	denySnap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"admin": clusterRole("admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		},
		ClusterRoleBindings: nil, // NO bindings -> alice denied
	}
	cache.RebuildSubjectIndexesForTest(denySnap)
	denySnap.PublishSeq = currentPublishSeq(t) + 1
	cache.PublishRBACSnapshotForTest(denySnap)

	a2, uid2, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil {
		t.Fatalf("G2: err=%v", err)
	}
	if a2 {
		t.Fatalf("F3 INVARIANT BROKEN (PM B1): stale allowed=true served across republish; G2 has no binding, MUST deny. uid=%q", uid2)
	}
	swapsAfter := func() uint64 { _, _, s, _, _ := rbac.AuthzMemoStatsForTest(); return s }()
	if swapsAfter <= swapsBefore {
		t.Fatalf("F3: expected the shard to swap on the G2 generation change; swaps %d -> %d", swapsBefore, swapsAfter)
	}

	// G3 — re-allow: publish a snapshot WITH the allowing binding again.
	allowSnap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"admin": clusterRole("admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		},
		ClusterRoleBindings: []*rbacv1.ClusterRoleBinding{crb},
	}
	cache.RebuildSubjectIndexesForTest(allowSnap)
	allowSnap.PublishSeq = currentPublishSeq(t) + 1
	cache.PublishRBACSnapshotForTest(allowSnap)

	a3, _, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil {
		t.Fatalf("G3: err=%v", err)
	}
	if !a3 {
		t.Fatalf("F3 bidirectional: G3 re-added the allowing binding; MUST allow again; got allowed=false")
	}
}

// TestL2_B4_PerNameVerdictsNeverCollapse seeds a resourceNames-scoped
// rule: alice may `get foo` but NOT `get bar` (same gvr/ns). The memo
// key includes Name, so the two verdicts MUST be cached independently —
// never collapsed to one entry. A collapse would leak `get bar` as
// allowed (or deny `get foo`).
func TestL2_B4_PerNameVerdictsNeverCollapse(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	// ClusterRole granting get ONLY on resourceNames: ["foo"].
	cr := clusterRole("foo-only-getter")
	cr.Rules = []rbacv1.PolicyRule{{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"foo"},
	}}
	newTestWatcher(t,
		cr,
		clusterRoleBindingWithUID("alice-foo-bind", "foo-only-getter", "foo-crb-uid",
			userSubject("alice"),
		),
	)

	base := rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "secrets", Namespace: "default",
	}

	// get foo -> ALLOWED.
	optsFoo := base
	optsFoo.Name = "foo"
	aFoo, _, err := rbac.EvaluateRBAC(context.Background(), optsFoo)
	if err != nil || !aFoo {
		t.Fatalf("B4: get foo expected allowed=true; got allowed=%v err=%v", aFoo, err)
	}

	// get bar -> DENIED. If the memo collapsed Name, this would wrongly
	// return the cached `foo` verdict (true).
	optsBar := base
	optsBar.Name = "bar"
	aBar, _, err := rbac.EvaluateRBAC(context.Background(), optsBar)
	if err != nil {
		t.Fatalf("B4: get bar err=%v", err)
	}
	if aBar {
		t.Fatalf("B4 INVARIANT BROKEN: memo collapsed the Name dimension — `get bar` returned allowed=true (must be false; resourceNames is [foo])")
	}

	// Re-ask both repeatedly — verdicts must stay per-Name AND become hits.
	for i := 0; i < 5; i++ {
		if a, _, _ := rbac.EvaluateRBAC(context.Background(), optsFoo); !a {
			t.Fatalf("B4: repeated get foo flipped to deny at i=%d", i)
		}
		if a, _, _ := rbac.EvaluateRBAC(context.Background(), optsBar); a {
			t.Fatalf("B4: repeated get bar flipped to allow at i=%d", i)
		}
	}
	hits, _, _, _, _ := rbac.AuthzMemoStatsForTest()
	if hits == 0 {
		t.Fatalf("B4: repeated per-Name tuples produced no memo hits")
	}
}

// ──────────────────────────────────────────────────────────────────────
// B6 — UID-determinism through the memo
// ──────────────────────────────────────────────────────────────────────

// TestL2_B6_UIDDeterministicThroughMemo asserts that for
// SkipBindingUID=false a memo HIT returns the SAME sorted first-match UID
// as the cold walk. Two CRBs both grant alice; the (Name,UID) stable
// sort makes "aaa-bind" (uid-aaa) the deterministic first match. The
// first call (cold walk) and every subsequent call (memo hit) MUST all
// return "C:uid-aaa".
func TestL2_B6_UIDDeterministicThroughMemo(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	newTestWatcher(t,
		clusterRole("admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		clusterRoleBindingWithUID("zzz-bind", "admin", "uid-zzz", userSubject("alice")),
		clusterRoleBindingWithUID("aaa-bind", "admin", "uid-aaa", userSubject("alice")),
	)

	opts := rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Resource: "secrets", Namespace: "default",
		SkipBindingUID: false, // UID-consuming path — sort runs, UID deterministic
	}
	const wantUID = "C:uid-aaa"

	// Cold walk.
	a0, uid0, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil || !a0 {
		t.Fatalf("B6 cold: allowed=%v err=%v", a0, err)
	}
	if uid0 != wantUID {
		t.Fatalf("B6 cold-walk first-match UID = %q, want %q", uid0, wantUID)
	}

	// Repeated calls — now memo hits. UID MUST be identical.
	for i := 0; i < 10; i++ {
		a, uid, err := rbac.EvaluateRBAC(context.Background(), opts)
		if err != nil || !a {
			t.Fatalf("B6 hit i=%d: allowed=%v err=%v", i, a, err)
		}
		if uid != wantUID {
			t.Fatalf("B6 INVARIANT BROKEN: memo HIT i=%d returned UID %q, want the deterministic sorted first-match %q", i, uid, wantUID)
		}
	}
	hits, _, _, _, _ := rbac.AuthzMemoStatsForTest()
	if hits < 10 {
		t.Fatalf("B6: expected >=10 memo hits; got %d", hits)
	}

	// Cross-check: a SkipBindingUID=true entry for the SAME tuple is a
	// SEPARATE key (verdict-only) and must not pollute the UID-consumer's
	// entry. Ask the verdict-only variant; the UID-consumer must still
	// return the deterministic UID afterwards.
	optsSkip := opts
	optsSkip.SkipBindingUID = true
	if a, _, _ := rbac.EvaluateRBAC(context.Background(), optsSkip); !a {
		t.Fatalf("B6: SkipBindingUID=true variant should still allow")
	}
	a2, uid2, _ := rbac.EvaluateRBAC(context.Background(), opts)
	if !a2 || uid2 != wantUID {
		t.Fatalf("B6: UID-consumer entry polluted by verdict-only variant — got allowed=%v uid=%q, want %q", a2, uid2, wantUID)
	}
}

// ──────────────────────────────────────────────────────────────────────
// F2 — concurrent -race republish hammer on the shared shard
// ──────────────────────────────────────────────────────────────────────

// TestL2_F2_ConcurrentRepublishHammer spawns N reader goroutines that
// hammer EvaluateRBAC over overlapping tuples while a writer goroutine
// republishes the snapshot (bumping PublishSeq) mid-flight. `go test
// -race` is the load-bearing falsifier: the shard is SHARED mutable
// state (unlike a per-request memo) and the RWMutex + atomic shard-swap is
// the thing under test (feedback_shared_vs_copy_is_a_concurrency_change).
// The verdict for each tuple is generation-invariant in this fixture
// (the bindings never change), so every read MUST return the expected
// verdict regardless of interleaving.
func TestL2_F2_ConcurrentRepublishHammer(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	// Seed several identities/resources so reader tuples overlap and
	// collide on the shard.
	newTestWatcher(t,
		clusterRole("reader", rule([]string{""}, []string{"configmaps", "secrets"}, []string{"get", "list"})),
		clusterRoleBindingWithUID("alice-bind", "reader", "uid-alice", userSubject("alice")),
		clusterRoleBindingWithUID("bob-bind", "reader", "uid-bob", userSubject("bob")),
	)

	type tuple struct {
		user, verb, resource string
		want                 bool
	}
	tuples := []tuple{
		{"alice", "get", "configmaps", true},
		{"alice", "list", "secrets", true},
		{"bob", "get", "secrets", true},
		{"bob", "get", "pods", false},         // not granted
		{"carol", "get", "configmaps", false}, // unbound identity
	}

	const readers = 32
	const itersPerReader = 200
	const republishes = 80

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan string, readers*4)

	// Writer: republish (bump PublishSeq) repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < republishes; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rebuildCurrentSnapshot(t)
		}
	}()

	// Readers.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < itersPerReader; i++ {
				tp := tuples[(r+i)%len(tuples)]
				allowed, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: tp.user, Verb: tp.verb, Group: "", Resource: tp.resource,
					Namespace: "default", SkipBindingUID: true,
				})
				if err != nil {
					errCh <- fmt.Sprintf("reader %d iter %d: err=%v", r, i, err)
					close(stop)
					return
				}
				if allowed != tp.want {
					errCh <- fmt.Sprintf("reader %d iter %d: %s/%s/%s verdict=%v want=%v",
						r, i, tp.user, tp.verb, tp.resource, allowed, tp.want)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatalf("F2 concurrent hammer FAIL: %s", msg)
	default:
	}
}

// ──────────────────────────────────────────────────────────────────────
// F5-ish — hit-rate + cap counter sanity
// ──────────────────────────────────────────────────────────────────────

// TestL2_F5_HitRateAndCounters drives one repeated tuple and asserts the
// hit rate approaches 1 within a stable generation (no republish), and
// that entries stays small (well under the cap). This is the in-process
// analogue of the on-cluster F5 expvar read.
func TestL2_F5_HitRateAndCounters(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	newTestWatcher(t,
		clusterRole("admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		clusterRoleBindingWithUID("alice-bind", "admin", "uid-alice", userSubject("alice")),
	)
	opts := rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Resource: "secrets", Namespace: "default",
		SkipBindingUID: true,
	}
	const n = 1000
	for i := 0; i < n; i++ {
		if a, _, err := rbac.EvaluateRBAC(context.Background(), opts); err != nil || !a {
			t.Fatalf("F5 iter %d: allowed=%v err=%v", i, a, err)
		}
	}
	hits, misses, _, refused, entries := rbac.AuthzMemoStatsForTest()
	total := hits + misses
	if total == 0 {
		t.Fatalf("F5: no lookups recorded")
	}
	hitRate := float64(hits) / float64(total)
	if hitRate < 0.85 {
		t.Fatalf("F5: hit rate %.4f < 0.85 (hits=%d misses=%d) — unexpected high-cardinality dimension?", hitRate, hits, misses)
	}
	if refused != 0 {
		t.Fatalf("F5: %d refused inserts on a single-tuple workload (cap breach should be impossible here)", refused)
	}
	if entries != 1 {
		t.Fatalf("F5: expected exactly 1 cached entry for one repeated tuple; got %d", entries)
	}
}

// ──────────────────────────────────────────────────────────────────────
// B3 — canonicalGroupsHash collision + order independence
// ──────────────────────────────────────────────────────────────────────

// TestL2_B3_GroupsHashCollisionAndOrder exercises the shared canonical
// groups-set hasher THROUGH EvaluateRBAC's observable behaviour: two
// identities whose groups differ only by partition (["a","bc"] vs
// ["ab","c"]) MUST NOT share a memo entry (the 0.30.239 collision), and
// reordering the same group set MUST hit the SAME entry (order
// independence). We assert this via the memo: seed a binding that grants
// group "bc" but NOT "ab"/"a"/"c"; the ["a","bc"] identity is ALLOWED,
// the ["ab","c"] identity is DENIED — a hash collision would cross-serve.
func TestL2_B3_GroupsHashCollisionAndOrder(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	// Bind group "bc" to a reader role.
	newTestWatcher(t,
		clusterRole("bc-reader", rule([]string{""}, []string{"configmaps"}, []string{"get"})),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "bc-bind", UID: types.UID("uid-bc")},
			Subjects:   []rbacv1.Subject{groupSubject("bc")},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "bc-reader"},
		},
	)

	get := func(user string, groups []string) bool {
		a, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: user, Groups: groups, Verb: "get", Resource: "configmaps", Namespace: "default",
			SkipBindingUID: true,
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC(%s,%v): err=%v", user, groups, err)
		}
		return a
	}

	// Identity with group set {a, bc} — member of "bc" -> ALLOWED.
	// (distinct Username so the only shared key dimension that could
	// collide is GroupsHash.)
	if !get("u1", []string{"a", "bc"}) {
		t.Fatalf("B3 setup: identity with group 'bc' must be allowed")
	}
	// Identity with group set {ab, c} — NOT a member of "bc" -> DENIED.
	// A naive concat hasher would alias {a,bc} and {ab,c} to "abc" and,
	// combined with an unlucky key reuse, risk a cross-serve. The
	// length-prefix makes the hashes distinct; the verdict MUST be deny.
	if get("u2", []string{"ab", "c"}) {
		t.Fatalf("B3 COLLISION: identity {ab,c} (no 'bc' group) returned allowed=true — groups-set hash aliased {a,bc} vs {ab,c}")
	}

	// Order independence: {bc, a} reordered must behave EXACTLY like
	// {a, bc} (allowed) — and hit the same memo entry.
	if !get("u1", []string{"bc", "a"}) {
		t.Fatalf("B3 order: {bc,a} must match {a,bc} (allowed)")
	}
}

// ──────────────────────────────────────────────────────────────────────
// H3 [SEC] — role-RULE republish invalidation (same binding/role NAME,
// rules changed). The memo MUST NOT serve a stale allow when a
// ClusterRole's RULES are narrowed to no longer grant the resource, and
// MUST re-allow when the rules are widened again. This is the role-rule
// edit dimension of the memory note
// feedback_hermetic_keyrotation_is_not_endtoend_fix — a memo keyed only
// on the BINDING set (ignoring that the bound Role's rules changed) would
// serve a stale allow across a rule Update that failed to bump PublishSeq
// OR whose bump the memo ignored.
// ──────────────────────────────────────────────────────────────────────

// TestL2_H3_RoleRuleRepublish_NarrowWiden seeds G1 where alice is allowed
// `get configmaps` via a ClusterRole "cm-getter" whose rule grants exactly
// that. The verdict is cached (repeat = hit). G2 republishes (PublishSeq+1)
// the SAME binding NAME and SAME ClusterRole NAME "cm-getter" but with its
// rule NARROWED to grant `get secrets` only — no longer configmaps. The
// next `get configmaps` call MUST DENY (shard swapped on the generation
// change, re-walk sees the narrowed rule) and the swap counter MUST move.
// G3 WIDENS the rule back to grant configmaps and MUST re-allow.
//
// RED arm: a memo that keys on the binding set alone (or otherwise fails
// to invalidate when only the bound Role's rules change) serves the cached
// G1 allow at G2 — a stale allow, the forbidden SEC defect.
func TestL2_H3_RoleRuleRepublish_NarrowWiden(t *testing.T) {
	rbac.ResetAuthzMemoForTest()

	// The binding is STABLE across all three generations — only the bound
	// ClusterRole's RULES change. Same NAME + UID throughout.
	crb := clusterRoleBindingWithUID("alice-cm-bind", "cm-getter", "cm-crb-uid",
		userSubject("alice"),
	)

	// G1 — cm-getter grants `get configmaps`.
	newTestWatcher(t,
		clusterRole("cm-getter", rule([]string{""}, []string{"configmaps"}, []string{"get"})),
		crb,
	)

	opts := rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "configmaps", Namespace: "default",
		SkipBindingUID: true,
	}

	// G1 allow + cache it (repeat = hit).
	if a, _, err := rbac.EvaluateRBAC(context.Background(), opts); err != nil || !a {
		t.Fatalf("H3 G1: expected allowed=true err=nil; got allowed=%v err=%v", a, err)
	}
	if a, _, _ := rbac.EvaluateRBAC(context.Background(), opts); !a {
		t.Fatalf("H3 G1 repeat: expected cached allowed=true")
	}
	hits1, _, _, _, _ := rbac.AuthzMemoStatsForTest()
	if hits1 == 0 {
		t.Fatalf("H3 G1: expected a memo hit on the repeated allow tuple; got 0")
	}

	// G2 — republish the SAME binding + SAME ClusterRole NAME but with the
	// rule NARROWED to `get secrets` (no longer configmaps). PublishSeq+1.
	swapsBefore := func() uint64 { _, _, s, _, _ := rbac.AuthzMemoStatsForTest(); return s }()
	narrowSnap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"cm-getter": clusterRole("cm-getter", rule([]string{""}, []string{"secrets"}, []string{"get"})),
		},
		ClusterRoleBindings: []*rbacv1.ClusterRoleBinding{crb},
	}
	cache.RebuildSubjectIndexesForTest(narrowSnap)
	narrowSnap.PublishSeq = currentPublishSeq(t) + 1
	cache.PublishRBACSnapshotForTest(narrowSnap)

	a2, _, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil {
		t.Fatalf("H3 G2: err=%v", err)
	}
	if a2 {
		t.Fatalf("H3 INVARIANT BROKEN (SEC): stale allow served across a role-RULE narrow — cm-getter no longer grants `get configmaps` at G2 (binding NAME unchanged), MUST deny. A memo keyed only on the binding set would return the cached G1 allow.")
	}
	swapsAfter := func() uint64 { _, _, s, _, _ := rbac.AuthzMemoStatsForTest(); return s }()
	if swapsAfter <= swapsBefore {
		t.Fatalf("H3 G2: expected the shard to swap on the generation change; swaps %d -> %d", swapsBefore, swapsAfter)
	}

	// G3 — WIDEN the rule back to grant configmaps. Same binding + role
	// NAME. PublishSeq+1. MUST re-allow (bidirectional).
	widenSnap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"cm-getter": clusterRole("cm-getter", rule([]string{""}, []string{"configmaps"}, []string{"get"})),
		},
		ClusterRoleBindings: []*rbacv1.ClusterRoleBinding{crb},
	}
	cache.RebuildSubjectIndexesForTest(widenSnap)
	widenSnap.PublishSeq = currentPublishSeq(t) + 1
	cache.PublishRBACSnapshotForTest(widenSnap)

	a3, _, err := rbac.EvaluateRBAC(context.Background(), opts)
	if err != nil {
		t.Fatalf("H3 G3: err=%v", err)
	}
	if !a3 {
		t.Fatalf("H3 bidirectional: G3 re-widened cm-getter to grant `get configmaps`; MUST allow again; got allowed=false")
	}
}

// ──────────────────────────────────────────────────────────────────────
// M8 — capacity: on cap breach the memo REFUSES to insert (never evicts),
// and a refused tuple still returns the correct walk-fallback verdict.
// ──────────────────────────────────────────────────────────────────────

// TestL2_M8_CapRefusesNeverEvicts drives >16384 DISTINCT permit tuples in
// ONE generation against a wildcard-admin CRB (every tuple permits). The
// tuples vary only in the Name dimension so each produces a distinct memo
// key. Once the shard fills to the empirical cap (snapshotAuthzMemoCap =
// 16384) further inserts MUST be REFUSED (authzMemoRefused > 0), the entry
// count MUST settle at EXACTLY the cap (never above — no unbounded growth;
// never evicted below — refuse, don't LRU-evict), and a tuple whose insert
// was refused MUST still return the correct allow (the caller always gets
// the freshly-walked verdict; only the cache insert is skipped).
//
// RED arms this discriminates:
//   - unbounded growth: entries would exceed the cap.
//   - LRU-evict instead of refuse: authzMemoRefused would stay 0.
//   - wrong verdict on a refused key: the refused tuple would deny.
func TestL2_M8_CapRefusesNeverEvicts(t *testing.T) {
	const cap = 16384 // snapshotAuthzMemoCap (snapshot_authz_memo.go)
	const overfill = cap + 500

	// #327: the cap-refuse-never-evict invariant is exercised on a PINNED shard
	// via rbac.AuthzShardCapInvariantForTest — immune to the background snapshot
	// republish that swaps the global memo pointer (correct per-gen
	// invalidation) and made the old end-to-end EvaluateRBAC fill flake 2/3
	// under -count (#241).
	//
	// DROPPED here on purpose (diff-for-deleted-tests): the old end-to-end
	// cap-fill (EvaluateRBAC over `overfill` distinct tuples + AuthzMemoStats
	// assertions) is the flake, now relocated to the pinned-shard seam; and the
	// old "a refused key re-walks to the correct verdict" arm — a refused key is
	// a plain memo MISS, whose walk-fallback is already covered by the F2/F3/F5
	// arms in this package, so keeping it would duplicate them with no new
	// coverage AND re-introduce the flaky cap-fill.
	entries, refusedDelta, overwriteGrew := rbac.AuthzShardCapInvariantForTest()

	// Cap breach must have REFUSED inserts (not evicted to make room).
	if refusedDelta == 0 {
		t.Fatalf("M8 CAP BROKEN: filled %d distinct permit tuples (cap=%d) but refused==0 — the memo EVICTED instead of refusing (or the cap is not enforced)", overfill, cap)
	}
	// Entries must settle EXACTLY at the cap: never above (unbounded), and
	// never below (refuse, don't evict).
	if entries != cap {
		t.Fatalf("M8 CAP BROKEN: entries=%d, want exactly cap=%d (>cap = unbounded growth; <cap = eviction). refusedDelta=%d", entries, cap, refusedDelta)
	}
	// The number of refused inserts must equal the overflow beyond the cap
	// (each distinct key; the first `cap` fill the map, the rest are refused).
	// This pins "refuse, don't evict" precisely.
	if want := uint64(overfill - cap); refusedDelta != want {
		t.Fatalf("M8 CAP: refusedDelta=%d, want exactly overflow=%d (overfill %d - cap %d) — off means evict-then-reinsert churn", refusedDelta, want, overfill, cap)
	}
	// Idempotent overwrite of an EXISTING key must be allowed and must NOT grow
	// the map beyond cap (the refuse guard exempts keys already present).
	if overwriteGrew {
		t.Fatalf("M8 CAP: idempotent overwrite of an existing key grew the map beyond cap=%d — the already-present exemption is broken", cap)
	}
}

// currentPublishSeq reads the live snapshot's PublishSeq via the cache
// test seam. Helper for the F3 synthetic-republish phases.
func currentPublishSeq(t *testing.T) uint64 {
	t.Helper()
	snap := cache.LiveRBACSnapshot()
	if snap == nil {
		return 0
	}
	return snap.PublishSeq
}
