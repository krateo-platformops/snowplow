// compute_key_identity_invariants_test.go — coverage-audit M1 + M2.
//
// These lock the two SECURITY-critical ComputeKey identity invariants that the
// H.c-layered / #118 key-shape carries, and which a plausible-wrong refactor
// could silently break:
//
//   M1 [SEC/mem]: RepresentativeUsername + RepresentativeGroups are BOOKKEEPING
//     carried on ResolvedEntry.Inputs, NOT key material (resolved.go §Ship A.3
//     "EXCLUDED FROM COMPUTEKEY"). Two members of the same binding equivalence
//     class writing the same cell must NOT shift the cell's identity by name —
//     else the per-binding cell fragments per-representative (memory blow-up)
//     and, worse, an attacker-controlled representative name could partition
//     cells. The RED arm is an impl that folds Representative* into the hash.
//
//   M2 [SEC]: BindingUID is folded for every identity-bound class
//     (restactions / widgets / raFullList) but SKIPPED for the identity-free
//     classes (IdentityFreeClasses — widgetContent, and apistage since #449;
//     resolved.go ComputeKey `if !IsIdentityFreeClass(...)`). If widgetContent
//     folded BindingUID it would fragment the shared envelope per binding
//     (breaking the shared-content invariant); if an identity-bound class
//     SKIPPED it, two distinct bindings would COLLIDE on one cell — a
//     cross-binding RBAC leak. Both directions are asserted; both RED impls are
//     proven below via a shadow wrong-impl.
package cache

import "testing"

// baseIdentityInputs is a fully-populated identity-bound key-input bundle. The
// Representative* fields carry a concrete "first writer" identity; the invariant
// under test is that mutating ONLY those fields never moves the key.
func baseIdentityInputs() ResolvedKeyInputs {
	return ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassWidgets,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "compositionsgrids",
		Namespace:       "demo",
		Name:            "main",
		BindingUID:      "C:uid-0123456789abcdef",
		PerPage:         20,
		Page:            1,
		RBACSubGen:      3,
		Extras:          map[string]any{"foo": "bar"},

		// Representative bookkeeping — must be key-INVISIBLE.
		RepresentativeUsername: "alice",
		RepresentativeGroups:   []string{"tenant-a", "viewers"},
	}
}

// CacheEntryClassWidgets / CacheEntryClassRestactions are not exported consts in
// resolved.go (only apistage/widgetContent/raFullList are). The string values
// are load-bearing and used verbatim across the package; alias them here so the
// table reads intent-first and a rename would fail to compile.
const (
	CacheEntryClassWidgets     = "widgets"
	CacheEntryClassRestactions = "restactions"
)

// TestComputeKey_RepresentativeIdentityExcluded is M1: mutating ONLY
// RepresentativeUsername and RepresentativeGroups leaves the key BYTE-IDENTICAL.
//
// RED PROOF: a shadow impl that folds Representative* into the hash
// (computeKeyFoldingRepresentative below) produces DIVERGENT keys for the two
// clones — the sub-test at the bottom proves the shadow reds, so this GREEN arm
// genuinely discriminates against the "folded Representative*" wrong impl.
func TestComputeKey_RepresentativeIdentityExcluded(t *testing.T) {
	base := baseIdentityInputs()

	// Clone, then mutate ONLY the representative identity fields.
	mutated := base
	mutated.RepresentativeUsername = "mallory" // different (attacker-controlled) name
	mutated.RepresentativeGroups = []string{"tenant-z", "admins", "extra"}

	if got, want := ComputeKey(mutated), ComputeKey(base); got != want {
		t.Fatalf("M1: mutating RepresentativeUsername/Groups shifted the key — "+
			"Representative* MUST be excluded from ComputeKey (per-binding cell would "+
			"fragment per representative)\n base=%s\n mut =%s", want, got)
	}

	// Also assert a genuine key field (BindingUID) STILL moves the key — proves
	// the equality above is not a degenerate "everything hashes the same".
	moved := base
	moved.BindingUID = "C:uid-different"
	if ComputeKey(moved) == ComputeKey(base) {
		t.Fatalf("M1 control: changing BindingUID must change the key (coalesced inputs)")
	}
}

// TestComputeKey_WidgetContentIsIdentityFree is M2 part (1): two widgetContent
// inputs that differ ONLY in BindingUID hash IDENTICALLY — the shared envelope
// is identity-free.
//
// RED PROOF: a shadow impl that folds BindingUID for widgetContent
// (computeKeyFoldingBindingUIDForWidgetContent) diverges the two — proven in the
// dedicated RED sub-test.
//
// #449: the same holds for EVERY identity-free class (IdentityFreeClasses —
// widgetContent and apistage), for all three identity dimensions.
func TestComputeKey_WidgetContentIsIdentityFree(t *testing.T) {
	for _, class := range IdentityFreeClasses() {
		in1 := ResolvedKeyInputs{
			CacheEntryClass: class,
			Group:           "g", Version: "v", Resource: "r",
			Namespace: "ns", Name: "n",
			BindingUID:        "C:uid-alpha",
			SubjectBindingSet: "digest-alpha",
			RBACSubGen:        11, // also identity-bound-only; must be excluded
		}
		in2 := in1
		in2.BindingUID = "R:ns/uid-beta"
		in2.SubjectBindingSet = "digest-beta"
		in2.RBACSubGen = 999
		zero := in1
		zero.SetIdentity("", IdentityClass{})

		if ComputeKey(in1) != ComputeKey(in2) || ComputeKey(in1) != ComputeKey(zero) {
			t.Fatalf("M2(1): %s keys diverged on identity — the identity-free shared "+
				"content must NOT fold identity\n a=%s\n b=%s\n zero=%s",
				class, ComputeKey(in1), ComputeKey(in2), ComputeKey(zero))
		}
	}
}

// TestComputeKey_IdentityBoundClassesFoldBindingUID is M2 part (2): for every
// identity-bound class, two inputs differing ONLY in BindingUID produce DISTINCT
// keys — else two bindings collide on one cell (cross-binding RBAC leak).
//
// RED PROOF: a shadow impl that SKIPS the BindingUID fold for all classes
// (computeKeyNeverFoldingBindingUID) collapses the two — proven in the RED
// sub-test.
func TestComputeKey_IdentityBoundClassesFoldBindingUID(t *testing.T) {
	for _, class := range []string{
		CacheEntryClassRestactions,
		CacheEntryClassWidgets,
		CacheEntryClassRAFullList,
	} {
		t.Run(class, func(t *testing.T) {
			in1 := ResolvedKeyInputs{
				CacheEntryClass: class,
				Group:           "g", Version: "v", Resource: "r",
				Namespace: "ns", Name: "n",
				BindingUID: "C:uid-alpha",
			}
			in2 := in1
			in2.BindingUID = "R:ns/uid-beta"
			if ComputeKey(in1) == ComputeKey(in2) {
				t.Fatalf("M2(2): class %q did NOT fold BindingUID — two distinct bindings "+
					"collide on one cell (cross-binding RBAC leak)", class)
			}
		})
	}
}

// --- RED-arm proofs: shadow wrong-impls + assertions that they RED ----------

// computeKeyFoldingRepresentative is the M1 WRONG impl: it distinguishes inputs
// by their representative identity (the defect this test guards against).
func computeKeyFoldingRepresentative(in ResolvedKeyInputs) string {
	// Cheap, deterministic surrogate: fold the representative identity into the
	// canonical key by stuffing it into Extras before delegating to the REAL
	// ComputeKey. If the real ComputeKey ever folded Representative* this is
	// exactly the divergence we'd see.
	clone := in
	clone.Extras = map[string]any{}
	for k, v := range in.Extras {
		clone.Extras[k] = v
	}
	clone.Extras["__rep_user"] = in.RepresentativeUsername
	reps := make([]any, 0, len(in.RepresentativeGroups))
	for _, g := range in.RepresentativeGroups {
		reps = append(reps, g)
	}
	clone.Extras["__rep_groups"] = reps
	return ComputeKey(clone)
}

// TestComputeKey_RepresentativeFold_RedArm proves the M1 GREEN arm discriminates:
// the wrong impl (folds Representative*) DIVERGES on the same clone pair the
// GREEN arm asserts EQUAL.
func TestComputeKey_RepresentativeFold_RedArm(t *testing.T) {
	base := baseIdentityInputs()
	mutated := base
	mutated.RepresentativeUsername = "mallory"
	mutated.RepresentativeGroups = []string{"tenant-z"}

	if computeKeyFoldingRepresentative(base) == computeKeyFoldingRepresentative(mutated) {
		t.Fatalf("RED-arm sanity: the Representative-folding wrong impl SHOULD diverge on a " +
			"representative-only mutation — if it does not, the M1 test is not discriminating")
	}
}

// computeKeyFoldingBindingUIDForWidgetContent is the M2(1) WRONG impl: it folds
// BindingUID even for the identity-free widgetContent class.
func computeKeyFoldingBindingUIDForWidgetContent(in ResolvedKeyInputs) string {
	clone := in
	clone.Extras = map[string]any{}
	for k, v := range in.Extras {
		clone.Extras[k] = v
	}
	clone.Extras["__binding_uid"] = in.BindingUID
	return ComputeKey(clone)
}

// TestComputeKey_WidgetContentFold_RedArm proves the M2(1) GREEN arm
// discriminates: the wrong impl (folds BindingUID for widgetContent) diverges on
// the BindingUID-only pair the GREEN arm asserts EQUAL.
func TestComputeKey_WidgetContentFold_RedArm(t *testing.T) {
	in1 := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgetContent, BindingUID: "C:a"}
	in2 := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgetContent, BindingUID: "C:b"}
	if computeKeyFoldingBindingUIDForWidgetContent(in1) == computeKeyFoldingBindingUIDForWidgetContent(in2) {
		t.Fatalf("RED-arm sanity: the widgetContent-BindingUID-folding wrong impl SHOULD diverge; " +
			"M2(1) is not discriminating otherwise")
	}
}

// computeKeyNeverFoldingBindingUID is the M2(2) WRONG impl: it never folds the
// BindingUID (identity-free for EVERY class). Two distinct bindings collapse.
func computeKeyNeverFoldingBindingUID(in ResolvedKeyInputs) string {
	clone := in
	clone.BindingUID = "" // erase the identity fold
	return ComputeKey(clone)
}

// TestComputeKey_NeverFoldBindingUID_RedArm proves the M2(2) GREEN arm
// discriminates: the wrong impl (never folds BindingUID) COLLIDES the two
// distinct bindings the GREEN arm asserts DISTINCT.
func TestComputeKey_NeverFoldBindingUID_RedArm(t *testing.T) {
	for _, class := range []string{
		CacheEntryClassRestactions,
		CacheEntryClassWidgets,
		CacheEntryClassRAFullList,
	} {
		in1 := ResolvedKeyInputs{CacheEntryClass: class, BindingUID: "C:a"}
		in2 := ResolvedKeyInputs{CacheEntryClass: class, BindingUID: "R:ns/b"}
		if computeKeyNeverFoldingBindingUID(in1) != computeKeyNeverFoldingBindingUID(in2) {
			t.Fatalf("RED-arm sanity for class %q: the never-fold-BindingUID wrong impl SHOULD "+
				"collide two bindings; M2(2) is not discriminating otherwise", class)
		}
	}
}
