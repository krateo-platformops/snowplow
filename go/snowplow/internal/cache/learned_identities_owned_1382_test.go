package cache

// learned_identities_owned_1382_test.go — #1382: learned identities read the
// username from a session Secret's certificate CN, and learn a Secret under
// that CN only when endpoints.OwnsSecret proves it that user's.
//
//   - one user's two Secrets (the exact-named one authn#140 writes, annotated,
//     and the legacy one an earlier login left, certificate only) collapse
//     into ONE class: the class key is (CN, groups), never the Secret name,
//     so the exact-vs-legacy name cannot change it (#1382 requirement 6);
//   - two different users whose Secrets claim one CN never merge: a Secret
//     annotated for another user than its certificate names is not that CN's
//     and is skipped, so it neither joins nor keeps alive that CN's class.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
)

func liAnnotated(sec *corev1.Secret, user string) *corev1.Secret {
	sec.Annotations = map[string]string{endpoints.UsernameAnnotation: user}
	return sec
}

func TestIssue1382_Learned_OneUsersExactAndLegacySecretsAreOneClass(t *testing.T) {
	fired := liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)
	exact := liAnnotated(liSecret(t, "john.doe-clientconfig", "john.doe", []string{"devs", "ops"}, nb.Add(time.Minute), "1"), "john.doe")
	legacy := liSecret(t, "johndoe-clientconfig", "john.doe", []string{"ops", "devs"}, nb, "2")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{exact, legacy})
	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "john.doe" {
		t.Fatalf("want ONE class for john.doe, got %+v", got)
	}
	if !got[0].LastSeen.Equal(nb.Add(time.Minute)) {
		t.Errorf("LastSeen %v, want the newer login %v", got[0].LastSeen, nb.Add(time.Minute))
	}
	if *fired != 1 {
		t.Errorf("hook fired %d times, want 1 (one new class)", *fired)
	}
	// The class key is the same whichever Secret is learned alone.
	ResetLearnedIdentitiesForTest()
	SyncLearnedFromSecretsForTest([]*corev1.Secret{exact})
	k1 := LearnedIdentitiesSnapshot()[0].Key()
	ResetLearnedIdentitiesForTest()
	SyncLearnedFromSecretsForTest([]*corev1.Secret{legacy})
	k2 := LearnedIdentitiesSnapshot()[0].Key()
	if k1 != k2 || k1 != got[0].Key() {
		t.Fatal("the learned class key depends on which of the user's Secrets it was learned from")
	}
	// One Secret going keeps the class; both going drop it.
	SyncLearnedFromSecretsForTest([]*corev1.Secret{exact, legacy})
	SyncLearnedFromSecretsForTest([]*corev1.Secret{legacy})
	if n, from, _ := LearnedClassCounts(); n != 1 || from != 1 {
		t.Fatalf("after the exact Secret went: classes=%d fromSecrets=%d, want 1/1", n, from)
	}
	SyncLearnedFromSecretsForTest(nil)
	if n, _, _ := LearnedClassCounts(); n != 0 {
		t.Fatalf("after both went: %d classes, want 0", n)
	}
}

func TestIssue1382_Learned_TwoUsersClaimingOneCNNeverMerge(t *testing.T) {
	liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)
	// alice's own Secret: annotated alice, certificate CN alice.
	alice := liAnnotated(liSecret(t, "alice-clientconfig", "alice", []string{"devs"}, nb, "1"), "alice")
	// bob's Secret carries a certificate that claims alice's CN, with bob's
	// groups: it is not alice's (the annotation says bob) and not bob's (the
	// certificate says alice).
	bob := liAnnotated(liSecret(t, "bob-clientconfig", "alice", []string{"admins"}, nb.Add(time.Minute), "2"), "bob")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{alice, bob})
	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "alice" || len(got[0].Groups) != 1 || got[0].Groups[0] != "devs" {
		t.Fatalf("want only alice's own class {alice [devs]}, got %+v", got)
	}
	if _, _, unparseable := LearnedClassCounts(); unparseable != 0 {
		t.Errorf("a refused Secret counted as unparseable (%d): its certificate parsed", unparseable)
	}
	// bob's Secret alone mints nothing, and does not keep alice's class alive.
	SyncLearnedFromSecretsForTest([]*corev1.Secret{bob})
	if n, _, _ := LearnedClassCounts(); n != 0 {
		t.Fatalf("the refused Secret kept or minted %d class(es), want 0", n)
	}
	if WasLearnedUsername("bob") {
		t.Error("bob was learned from a certificate that is not his")
	}
}

// A Secret that fails OwnsSecret for the CN it would be learned under is
// skipped: annotated for someone else, same name shape, same groups.
func TestIssue1382_Learned_NotOwnedIsSkipped(t *testing.T) {
	liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)
	sec := liAnnotated(liSecret(t, "carol-clientconfig", "carol", []string{"devs"}, nb, "1"), "mallory")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{sec})
	if n, _, _ := LearnedClassCounts(); n != 0 {
		t.Fatalf("a Secret not owned by its CN was learned (%d classes)", n)
	}
	// The same Secret re-annotated for its CN is learned (control).
	sec2 := liAnnotated(liSecret(t, "carol-clientconfig", "carol", []string{"devs"}, nb, "2"), "carol")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{sec2})
	if n, _, _ := LearnedClassCounts(); n != 1 {
		t.Fatalf("control: the owned Secret was not learned (%d classes)", n)
	}
}
