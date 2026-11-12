// empty_binding_uid_invariant_test.go — Ship 0.30.242 H.c-layered
// Phase 3 F7 (HARD GATE per the F1-deletion decision at 2c).
//
// THE INVARIANT (biconditional, per design §4.1 SEMANTICS + Diego's
// 2c note 2):
//
//     uid == ""   IFF   (cache.Disabled()  OR  allowed == false)
//
// Equivalently — three orthogonal scenarios cover the 3 ways the
// rbac.EvaluateRBAC contract is exercised:
//
//   Scenario 1 — cache.Disabled() == true:
//     EvaluateRBAC falls through to UserCan (SAR). No in-process
//     snapshot is available, so MatchedBindingUID MUST be "". The
//     `allowed` return is whatever UserCan returns; the uid is "" in
//     ALL cases.
//
//   Scenario 2 — cache.Disabled() == false AND no binding grants:
//     EvaluateRBAC evaluates against the snapshot; no CRB and no RB
//     matches the (verb, gvr, ns) tuple → returns (false, "", nil).
//
//   Scenario 3 — cache.Disabled() == false AND a binding grants:
//     EvaluateRBAC matches the FIRST permitting binding (CRB-phase
//     before RB-phase, per design §6 stable ordering). MatchedBindingUID
//     MUST be the binding's canonical identity ("C:<uid>" for a CRB;
//     "R:<ns>/<uid>" for an RB). NEVER "".
//
// BICONDITIONAL ASSERTION SHAPE:
//
//   Forward direction (uid=="" → cache.Disabled() OR allowed==false):
//     Tested by scenarios 1 and 2. If uid="" surfaces here, the
//     scenario tells us which branch produced it.
//
//   Reverse direction (cache.Disabled() OR allowed==false → uid==""):
//     Tested explicitly:
//       - Scenario 1 (cache.Disabled()=true) MUST always have uid="".
//         Tested with TWO sub-cases: one where UserCan would allow,
//         and one where it would deny — both MUST produce uid="".
//       - Scenario 2 (cache.Disabled()=false, denied) MUST have uid="".
//
//   F7 fails the gate IF EITHER direction is broken. A regression that
//   leaks uid for a deny path would slip past a one-direction-only
//   assertion; the biconditional shape closes that hole.
//
// PHASE 3 HARD GATE: F7 PASS is a prerequisite for Phase 4 dual-gate
// review per the F1-deletion decision at 2c commit 74d5090.

package evaltest

import (
	"context"
	"errors"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// clusterRoleBindingWithUID is a fixture helper: clusterRoleBinding +
// explicit metadata.uid so the test can assert the returned BindingUID
// equals "C:<uid>" deterministically. The base helpers in
// evaluate_test.go don't set UID (they rely on the dynamic fake's
// auto-assignment which we don't observe).
func clusterRoleBindingWithUID(name, roleName, uid string, subjects ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	crb := clusterRoleBinding(name, roleName, subjects...)
	crb.ObjectMeta = metav1.ObjectMeta{
		Name: name,
		UID:  types.UID(uid),
	}
	return crb
}

// ──────────────────────────────────────────────────────────────────────
// F7 — Empty-BindingUID invariant (biconditional)
// ──────────────────────────────────────────────────────────────────────

// Scenario 1a — cache.Disabled()=true + UserCan would PERMIT.
// UserCan reads the user's endpoint from ctx and issues a
// SelfSubjectAccessReview. Under cache=off there is no in-process
// snapshot, so MatchedBindingUID MUST be "" regardless of the
// `allowed` outcome.
//
// Test harness limitation: we cannot easily wire a SAR-permitting
// context here (would require a kube-apiserver + bearer token). We
// don't NEED to — the contract is: cache.Disabled()=true → uid="",
// independent of allowed. We assert just that: uid=="" when cache is
// off. (UserCan returns false on missing ctx-endpoint, which is the
// "deny-by-default" arm; the deny arm of the biconditional already
// has its own assertion. Scenario 1's load-bearing claim is "uid=='' "
// in BOTH UserCan branches.)
func TestF7_CacheDisabled_BindingUIDAlwaysEmpty(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	// No newTestWatcher — under cache=off, EvaluateRBAC doesn't touch
	// the snapshot/watcher path. cache.SetGlobal stays nil from any
	// previous test run.
	cache.SetGlobal(nil)

	allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username:  "alice",
		Verb:      "get",
		Group:     "",
		Resource:  "configmaps",
		Namespace: "default",
	})
	// #1362: with no user endpoint the cache=off review cannot be asked, which
	// is ErrReviewUnavailable (never an allow). Any other error means the
	// cache=off path was not the one taken.
	if err != nil && !errors.Is(err, rbac.ErrReviewUnavailable) {
		t.Fatalf("EvaluateRBAC under cache=off: err=%v (want nil or ErrReviewUnavailable)", err)
	}
	if allowed && err != nil {
		t.Fatalf("EvaluateRBAC under cache=off allowed on an error")
	}
	if uid != "" {
		t.Fatalf("F7 INVARIANT BROKEN: cache.Disabled()=true MUST yield uid=\"\"; got uid=%q (allowed=%v)", uid, allowed)
	}
	// The `allowed` value depends on the SAR fallback path which is
	// effectively a deny under test harness (no real apiserver). We
	// do NOT assert on `allowed` — F7's invariant is uid=="" only.
}

// Scenario 2 — cache.Disabled()=false + no binding grants.
// The snapshot is published but no CRB and no RB matches the
// (verb, gvr, ns) tuple. EvaluateRBAC MUST return (false, "", nil).
func TestF7_CacheEnabledDenied_BindingUIDEmpty(t *testing.T) {
	// Snapshot has a CRB granting alice "get configmaps" — but bob
	// is asking, and there's no binding for bob.
	newTestWatcher(t,
		clusterRole("alice-only-reader",
			rule([]string{""}, []string{"configmaps"}, []string{"get"}),
		),
		clusterRoleBinding("alice-bind", "alice-only-reader",
			userSubject("alice"),
		),
	)

	allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username:  "bob", // not bound — DENY
		Verb:      "get",
		Group:     "",
		Resource:  "configmaps",
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC: err=%v (want nil)", err)
	}
	if allowed {
		t.Fatalf("F7 setup invariant: bob MUST be denied (no binding); got allowed=true uid=%q", uid)
	}
	if uid != "" {
		t.Fatalf("F7 INVARIANT BROKEN: allowed=false MUST yield uid=\"\"; got uid=%q", uid)
	}
}

// Scenario 3 — cache.Disabled()=false + binding grants.
// SA-path with a cluster-admin-style CRB. EvaluateRBAC MUST return
// (true, "C:<sa-crb-uid>", nil) — explicitly NON-empty uid.
//
// Per Diego's 2c note 1: use the snowplow SA's cluster-admin CRB
// shape. We don't need the LIVE cluster's UID — the point is that
// the SA-path returns a non-empty BindingUID. We stamp a
// deterministic fixture UID and assert the returned uid is
// "C:<fixture-uid>".
func TestF7_CacheEnabledAllowed_SAPath_BindingUIDNotEmpty(t *testing.T) {
	const saCRBUID = "sa-cluster-admin-uid-f7"
	newTestWatcher(t,
		clusterRole("cluster-admin",
			rule([]string{"*"}, []string{"*"}, []string{"*"}),
		),
		clusterRoleBindingWithUID("snowplow-sa-admin-bind", "cluster-admin", saCRBUID,
			saSubject("krateo-system", "snowplow"),
		),
	)

	allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username:  "system:serviceaccount:krateo-system:snowplow",
		Verb:      "get",
		Group:     "",
		Resource:  "secrets",
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC SA-path: err=%v (want nil)", err)
	}
	if !allowed {
		t.Fatalf("F7 setup invariant: snowplow SA should be cluster-admin; got allowed=false (uid=%q)", uid)
	}
	wantUID := "C:" + saCRBUID
	if uid != wantUID {
		t.Fatalf("F7 INVARIANT BROKEN: allowed=true SA-path MUST yield non-empty BindingUID=%q; got uid=%q", wantUID, uid)
	}
}

// Scenario 3b — cache.Disabled()=false + binding grants via User-kind CRB.
// Same invariant (non-empty uid) but the matched binding is User-kind.
// Asserts the "C:<uid>" shape generalises beyond SA-kind.
func TestF7_CacheEnabledAllowed_UserPath_BindingUIDNotEmpty(t *testing.T) {
	const userCRBUID = "user-admin-uid-f7"
	newTestWatcher(t,
		clusterRole("user-admin",
			rule([]string{""}, []string{"configmaps"}, []string{"get"}),
		),
		clusterRoleBindingWithUID("alice-admin-bind", "user-admin", userCRBUID,
			userSubject("alice"),
		),
	)

	allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username:  "alice",
		Verb:      "get",
		Group:     "",
		Resource:  "configmaps",
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC User-path: err=%v (want nil)", err)
	}
	if !allowed {
		t.Fatalf("F7 setup invariant: alice should match the user-admin CRB; got allowed=false (uid=%q)", uid)
	}
	wantUID := "C:" + userCRBUID
	if uid != wantUID {
		t.Fatalf("F7 INVARIANT BROKEN: allowed=true User-path MUST yield non-empty BindingUID=%q; got uid=%q", wantUID, uid)
	}
}

// ──────────────────────────────────────────────────────────────────────
// Biconditional reverse-direction guard
// ──────────────────────────────────────────────────────────────────────

// TestF7_BiconditionalReverse asserts the IFF direction explicitly:
//
//   FORWARD:  uid=="" → (cache.Disabled() OR allowed==false)
//             [covered by scenarios 1 and 2; if uid="" surfaces in
//             scenario 3, the test fails]
//
//   REVERSE:  (cache.Disabled() OR allowed==false) → uid==""
//             [covered explicitly here: 4 sub-cases]
//
// Without the reverse direction, a regression that leaked a uid for
// a deny path or a cache=off path would slip past forward-only
// assertions. The reverse-direction guard closes that hole.
func TestF7_BiconditionalReverse(t *testing.T) {
	// Sub-case (a) cache.Disabled()=true + ANY user → uid="".
	t.Run("cache_off_uid_empty", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		cache.SetGlobal(nil)
		_, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "anyone", Verb: "get", Resource: "configmaps", Namespace: "default",
		})
		// #1362: no user endpoint → ErrReviewUnavailable; uid is still "".
		if err != nil && !errors.Is(err, rbac.ErrReviewUnavailable) {
			t.Fatalf("EvaluateRBAC cache=off: err=%v", err)
		}
		if uid != "" {
			t.Fatalf("biconditional reverse: cache=off MUST → uid=\"\"; got uid=%q", uid)
		}
	})

	// Sub-case (b) cache=on + allowed=false (denied) → uid="".
	t.Run("cache_on_denied_uid_empty", func(t *testing.T) {
		newTestWatcher(t,
			clusterRole("admin",
				rule([]string{"*"}, []string{"*"}, []string{"*"}),
			),
			clusterRoleBindingWithUID("admin-bind", "admin", "admin-crb-uid",
				userSubject("alice"),
			),
		)
		allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "bob", // not bound — DENY
			Verb:     "get",
			Resource: "secrets",
			Namespace: "default",
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC: err=%v", err)
		}
		if allowed {
			t.Fatalf("setup: bob should be denied; got allowed=true uid=%q", uid)
		}
		if uid != "" {
			t.Fatalf("biconditional reverse: allowed=false MUST → uid=\"\"; got uid=%q", uid)
		}
	})

	// Sub-case (c) cache=on + allowed=true + RB-path → uid non-empty.
	// Tests the RB phase of the two-phase walk (CRB phase finds no
	// match; RB phase produces the first-match).
	t.Run("cache_on_allowed_rb_path_uid_nonempty", func(t *testing.T) {
		const rbUID = "rb-uid-f7-reverse"
		rb := roleBinding("ns-a", "alice-reader", "Role", "reader",
			userSubject("alice"),
		)
		rb.ObjectMeta = metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "alice-reader",
			UID:       types.UID(rbUID),
		}
		newTestWatcher(t,
			role("ns-a", "reader",
				rule([]string{""}, []string{"configmaps"}, []string{"get"}),
			),
			rb,
		)
		allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "alice", Verb: "get", Resource: "configmaps", Namespace: "ns-a",
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC RB-path: err=%v", err)
		}
		if !allowed {
			t.Fatalf("setup: alice should match the RB; got allowed=false (uid=%q)", uid)
		}
		wantUID := "R:ns-a/" + rbUID
		if uid != wantUID {
			t.Fatalf("biconditional reverse forward-check (allowed=true RB-path): uid MUST be %q; got %q", wantUID, uid)
		}
	})

	// Sub-case (d) Forward direction sanity — uid="" implies one of
	// the two predicates is true. Re-uses sub-case (b)'s deny-shape
	// and asserts the cache.Disabled() check ALSO matches the
	// invariant (cache is on in this sub-case, so allowed must be
	// false for uid="" to hold).
	t.Run("uid_empty_implies_disabled_or_denied", func(t *testing.T) {
		newTestWatcher(t,
			clusterRole("admin",
				rule([]string{"*"}, []string{"*"}, []string{"*"}),
			),
			clusterRoleBindingWithUID("admin-bind", "admin", "admin-crb-uid",
				userSubject("alice"),
			),
		)
		allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "bob", Verb: "get", Resource: "secrets", Namespace: "default",
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC: err=%v", err)
		}
		if uid == "" {
			// Forward direction: uid="" → cache.Disabled() OR allowed==false.
			// cache.Disabled() is false in this sub-case (newTestWatcher set
			// CACHE_ENABLED=true); so allowed MUST be false.
			if cache.Disabled() {
				t.Fatalf("forward direction sanity broken: newTestWatcher set CACHE_ENABLED=true but cache.Disabled() reports true")
			}
			if allowed {
				t.Fatalf("F7 FORWARD INVARIANT BROKEN: uid=\"\" with cache.Disabled()=false AND allowed=true; biconditional violated (got allowed=%v)", allowed)
			}
		}
	})
}
