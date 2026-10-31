package cache

// learned_identities_stepup_994_test.go — #994: prewarm learning reads only the
// SESSION credential Secrets. A step-up Secret `<user>-clientconfig-stepup`
// (authn#49) lives about 15 minutes and its certificate carries the session's
// groups plus krateo:loa:2; it is excluded on purpose (isClientconfigSecret),
// never learned as a class and never counted in the format-drift alarm.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLearned994_StepUpSecretsAreNeverLearned(t *testing.T) {
	fired := liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)
	stepUpGroups := []string{"devs", credsecret.StepUpGroup}

	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		// dave: a session Secret and a live step-up Secret side by side.
		liSecret(t, credsecret.SessionName("dave"), "dave", []string{"devs"}, nb, "1"),
		liSecret(t, credsecret.StepUpName("dave"), "dave", stepUpGroups, nb.Add(time.Minute), "2"),
		// erin: only a step-up Secret (her session Secret is gone).
		liSecret(t, credsecret.StepUpName("erin"), "erin", stepUpGroups, nb, "3"),
		// frank: a step-up Secret whose certificate does not parse.
		{ObjectMeta: metav1.ObjectMeta{Name: credsecret.StepUpName("frank"), ResourceVersion: "4"},
			Data: map[string][]byte{clientCertificateDataKey: []byte("not a certificate")}},
	})

	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "dave" || len(got[0].Groups) != 1 || got[0].Groups[0] != "devs" {
		t.Fatalf("only dave's SESSION class may be learned; a step-up Secret is never a class. got %+v", got)
	}
	for _, c := range got {
		for _, g := range c.Groups {
			if g == credsecret.StepUpGroup {
				t.Fatalf("a learned class carries %s: a step-up certificate was learned (%+v)", credsecret.StepUpGroup, c)
			}
		}
	}
	if WasLearnedUsername("erin") {
		t.Fatal("erin has only a step-up Secret; she must not have been learned")
	}
	if r, s, u := LearnedClassCounts(); r != 1 || s != 1 || u != 0 {
		t.Fatalf("counts registered/from_secrets/unparseable = %d/%d/%d, want 1/1/0 "+
			"(a step-up Secret is neither a class nor an unparseable login)", r, s, u)
	}
	if n := LearnedClientconfigSecrets(); n != 1 {
		t.Fatalf("the format-drift alarm's denominator counts %d Secrets, want 1 (the session Secret only)", n)
	}
	if *fired != 1 {
		t.Fatalf("the class hook fired %d times, want 1", *fired)
	}
}
