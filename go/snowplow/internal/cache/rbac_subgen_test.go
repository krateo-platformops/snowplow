// rbac_subgen_test.go — #118 (c) per-user RBAC sub-generation falsifiers
// (cache-package arms: the counter mechanics + the key fold).
//
// The durable fix folds RBACSubGenForSubject into ComputeKey so an out-of-band
// RBAC change that touches THIS user's own bindings rotates the key (cold miss →
// fresh UAF refilter), while leaving other users' cells hot (herd-proportional,
// survives the 50K install storm). These arms pin: the group-grant crux
// (C-118-2), per-user isolation (C-118-3), the v5 fold (C-118-7), and the 50K
// herd bound proxy (C-118-5). C-118-1 (revoke→drop CONTENT) is on-cluster; the
// hermetic proxy here is "a bump changes the key" (cold miss forces refilter).

package cache

import "testing"

// bump helpers mirror the onBinding* subjectKey shapes.
func userSubj(name string) subjectKey {
	return subjectKey{Kind: subjectKindUser, Name: name}
}
func groupSubj(name string) subjectKey {
	return subjectKey{Kind: subjectKindGroup, Name: name}
}

// C-118-2 (GRANT-VIA-GROUP crux) — a bump of a GROUP the user presents must move
// the user's effective sub-gen. The RED is a fold over {user} ONLY (blind to
// groups); this arm proves the prod fold includes groups by bumping ONLY a group
// counter and asserting the user's effective sub-gen changes.
func TestRBACSubGen_GroupGrantBumpsUserEffective(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	const user = "alice"
	groups := []string{"devs", "on-call"}

	before := RBACSubGenForSubject(user, groups)

	// A grant/revoke via the "devs" GROUP (not the user directly) — the hook
	// bumps the group's subject counter.
	BumpSubjectSubGens([]subjectKey{groupSubj("devs")})

	after := RBACSubGenForSubject(user, groups)
	if after == before {
		t.Fatalf("C-118-2 VIOLATED: a grant via GROUP 'devs' did NOT move alice's effective sub-gen (before=%d after=%d) — the fold is blind to groups (folds {user} only). A group-scoped grant/revoke would then serve stale until TTL.", before, after)
	}

	// RED-control (the {user}-only mis-fold): a bump of a group the user is NOT
	// in must NOT move her sub-gen (isolation the other way — proves the fold is
	// scoped to HER groups, not global).
	mid := RBACSubGenForSubject(user, groups)
	BumpSubjectSubGens([]subjectKey{groupSubj("some-other-team")})
	if RBACSubGenForSubject(user, groups) != mid {
		t.Fatal("C-118-2: a bump of a group alice is NOT in must not move her effective sub-gen")
	}
}

// C-118-3 (per-user isolation) — U1's grant change bumps U1's effective sub-gen
// but NOT U2's (they share no subject). Preserves feedback_l1_per_user_keyed_
// never_cohort: per-subject blast radius, not a global rotation.
func TestRBACSubGen_PerUserIsolation(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	u1before := RBACSubGenForSubject("u1", []string{"team-a"})
	u2before := RBACSubGenForSubject("u2", []string{"team-b"})

	// A binding change touching ONLY u1 (its own User subject).
	BumpSubjectSubGens([]subjectKey{userSubj("u1")})

	if RBACSubGenForSubject("u1", []string{"team-a"}) == u1before {
		t.Fatal("C-118-3: u1's own grant change must move u1's effective sub-gen")
	}
	if got := RBACSubGenForSubject("u2", []string{"team-b"}); got != u2before {
		t.Fatalf("C-118-3 VIOLATED: u1's grant change moved u2's effective sub-gen (before=%d after=%d) — per-user isolation broken; this is the global-rotation (option a) herd, not per-subject (c)", u2before, got)
	}
}

// C-118-5 (50K HERD, ≥2 tenants) — a stream of binding changes for tenant-X
// subjects must NOT move tenant-Y's effective sub-gen. This is the arm that
// discriminates (c) from (a): under global RBACGen every bump rotates ALL keys
// (Y goes cold); under per-subject (c), Y's sub-gen is untouched (Y stays HIT).
// ≥2 tenants REQUIRED (a single-tenant arm can't tell per-subject from global).
func TestRBACSubGen_HerdBound_TenantYStaysStableUnderTenantXStorm(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	// Tenant-Y user, its sub-gen captured before the storm.
	yBefore := RBACSubGenForSubject("y-user", []string{"tenant-y"})

	// Simulate a tenant-X composition-install RBAC-binding stream: many bindings,
	// each granting tenant-X subjects (users + the tenant-x group). This is the
	// 50K install storm shape.
	for i := 0; i < 1000; i++ {
		BumpSubjectSubGens([]subjectKey{
			userSubj("x-user"),
			groupSubj("tenant-x"),
		})
	}

	// Tenant-Y's effective sub-gen is UNCHANGED — Y's cells stay hot through the
	// entire tenant-X storm. Under global RBACGen this would have rotated 1000×.
	if got := RBACSubGenForSubject("y-user", []string{"tenant-y"}); got != yBefore {
		t.Fatalf("C-118-5 VIOLATED: a 1000-binding tenant-X install storm moved tenant-Y's sub-gen (before=%d after=%d) — (c) collapsed to (a)'s global herd; Y's cells would go cold under every unrelated install", yBefore, got)
	}
	// Sanity: tenant-X's OWN sub-gen DID move (the storm is real, not a no-op).
	if RBACSubGenForSubject("x-user", []string{"tenant-x"}) == 0 {
		t.Fatal("C-118-5 setup: tenant-X's sub-gen must have moved (the storm must actually bump)")
	}
}

// C-118-7 + the fold — ComputeKey folds RBACSubGen for identity-bound classes
// (different sub-gen → different key) and NOT for widgetContent (identity-free);
// and the version is v7 (#118 (c)-v2 bumped v5→v6 for the deferred-bump timeline
// change; #423 bumped v6→v7 for the SubjectBindingSet fold — see
// resolvedKeyVersion history).
func TestRBACSubGen_FoldedIntoKey_NotForWidgetContent(t *testing.T) {
	if resolvedKeyVersion != "v7" {
		t.Fatalf("C-118-7: resolvedKeyVersion must be v7 (v6: the RBACSubGen fold + (c)-v2 deferred-bump timeline; v7: #423 SubjectBindingSet); got %q", resolvedKeyVersion)
	}

	base := ResolvedKeyInputs{
		CacheEntryClass: "restactions",
		Group:           "templates.krateo.io", Version: "v1", Resource: "restactions",
		Namespace: "ns", Name: "ra", BindingUID: "C:b1",
	}
	k0 := ComputeKey(base)
	bumped := base
	bumped.RBACSubGen = 7
	k1 := ComputeKey(bumped)
	if k0 == k1 {
		t.Fatal("C-118-7 VIOLATED: an identity-bound (restactions) key did NOT change when RBACSubGen changed — the sub-gen is not folded, so an RBAC change would serve the stale cell")
	}

	// widgetContent is identity-free — RBACSubGen must NOT affect its key (folding
	// identity there breaks the shared-content invariant; design §key-parity).
	wc := ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets",
		Namespace: "ns", Name: "w",
	}
	wc0 := ComputeKey(wc)
	wcBumped := wc
	wcBumped.RBACSubGen = 42
	if ComputeKey(wcBumped) != wc0 {
		t.Fatal("C-118-7: widgetContent is identity-free — RBACSubGen must NOT change its key (shared-content invariant)")
	}
}

// SA-identity fold — a ServiceAccount username folds the SA subject counter, so a
// binding granting that SA (recorded by onBinding* as an SA subjectKey) moves the
// SA identity's effective sub-gen.
func TestRBACSubGen_ServiceAccountIdentity(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	const saUser = "system:serviceaccount:ns-x:runner"
	before := RBACSubGenForSubject(saUser, nil)
	// A binding granting the SA — the hook records the SA subjectKey.
	BumpSubjectSubGens([]subjectKey{{Kind: subjectKindServiceAccount, Name: "runner", Namespace: "ns-x"}})
	if RBACSubGenForSubject(saUser, nil) == before {
		t.Fatal("SA identity: a grant to the SA must move the SA username's effective sub-gen (parseServiceAccountUsername → SA subjectKey)")
	}
	// A human user with the same trailing name must NOT be affected (different Kind).
	if RBACSubGenForSubject("runner", nil) != 0 {
		t.Fatal("SA identity: a User named 'runner' must not inherit the SA 'runner' counter (Kind-scoped)")
	}
}
