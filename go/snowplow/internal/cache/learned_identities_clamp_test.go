package cache

// learned_identities_clamp_test.go — #262 hardening (reviewer-415).
//
// LastSeen orders admission (newest first). A certificate whose NotBefore lies
// in the future (forged, or a skewed signer clock) must not sort newest forever
// and crowd every real login out of the bound: LastSeen is clamped to the
// instant the registry observed it.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func TestLearned262_FutureNotBeforeIsClamped(t *testing.T) {
	liHooked(t)
	ingest := time.Now()
	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, "skew-clientconfig", "skew", []string{"g"}, ingest.Add(10*365*24*time.Hour), "1"),
	})
	time.Sleep(20 * time.Millisecond)
	later := time.Now()
	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, "skew-clientconfig", "skew", []string{"g"}, ingest.Add(10*365*24*time.Hour), "1"),
		// x509 NotBefore has second precision: a fresh login a second ahead is
		// clamped to THIS observation, which is after the skewed one.
		liSecret(t, "real-clientconfig", "real", []string{"g"}, later.Add(time.Second), "1"),
	})
	got := LearnedIdentitiesSnapshot()
	now := time.Now()
	for _, c := range got {
		if c.LastSeen.After(now) {
			t.Fatalf("CLAMP RED: %s has LastSeen %v in the future (now %v)", c.Username, c.LastSeen, now)
		}
	}
	if len(got) != 2 || got[0].Username != "real" {
		t.Fatalf("CLAMP RED: a real login AFTER the skewed one must sort first; got order %v, %v", got[0].Username, got[len(got)-1].Username)
	}
}
