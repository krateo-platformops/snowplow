package cache

// learned_identities_alarm_test.go — #262 MUST 1 (architect-262).
//
// "_from_secrets == 0 while clientconfig Secrets exist" is the authn
// format-drift alarm, so it must be READABLE. If authn ever wrote a clientconfig
// without client-certificate-data (a bearer-credential format), a filter on the
// cert key would skip it silently: from_secrets=0 AND unparseable=0, a zero that
// reads as health. Every `*-clientconfig` Secret is counted
// (snowplow_learned_clientconfig_secrets), and one no class can be read from is
// unparseable.

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLearned262_CertlessClientconfigTripsTheAlarm(t *testing.T) {
	liHooked(t)
	SyncLearnedFromSecretsForTest([]*corev1.Secret{{
		ObjectMeta: metav1.ObjectMeta{Name: "dora-clientconfig", ResourceVersion: "1"},
		Data:       map[string][]byte{"bearer": []byte("opaque"), "server-url": []byte("https://kubernetes.default.svc")},
	}, {
		ObjectMeta: metav1.ObjectMeta{Name: "authn-jwt-signing-key", ResourceVersion: "1"},
		Data:       map[string][]byte{"private.pem": []byte("x")},
	}})
	r, s, u := LearnedClassCounts()
	if total := LearnedClientconfigSecrets(); r != 0 || s != 0 || u != 1 || total != 1 {
		t.Fatalf("MUST1 RED: a clientconfig Secret without a certificate must show in the alarm counters: "+
			"registered=%d from_secrets=%d unparseable=%d clientconfig_secrets=%d, want 0/0/1/1", r, s, u, total)
	}
}
