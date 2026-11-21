package cache

// learned_identities_delegated_1560_test.go — #1560: a delegated credential
// Secret (authn#167, one per person and actor, about 600 s, NO labels) is
// never a learned identity. S1 selects login artifacts by name
// (credsecret.IsSessionName) and a delegated name never ends in a session
// suffix, so the exclusion is automatic; this pins it. And the Secrets
// informer's transform keeps what the delegated check reads (both
// annotations and the certificate), so a label-less delegated Secret in the
// informer is held, not skipped as malformed.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/testsupport/delegatedcred"
)

func TestLearned1560_DelegatedSecretsAreNeverLearned(t *testing.T) {
	fired := liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)

	// dave has a session Secret and a live delegated one; erin has only a
	// delegated one; frank's delegated Secret holds no readable certificate.
	daveDelegated := delegatedcred.Secret(t, delegatedcred.Spec{Username: "dave", Actor: "agentiko", Groups: []string{"devs"}})
	daveDelegated.ResourceVersion = "2"
	erinDelegated := delegatedcred.Secret(t, delegatedcred.Spec{Username: "erin", Actor: "agentiko", Groups: []string{"ops"}})
	erinDelegated.ResourceVersion = "3"
	frankDelegated := delegatedcred.Secret(t, delegatedcred.Spec{Username: "frank", Actor: "agentiko"})
	frankDelegated.ResourceVersion = "4"
	frankDelegated.Data[clientCertificateDataKey] = []byte("not a certificate")
	for _, s := range []*corev1.Secret{daveDelegated, erinDelegated, frankDelegated} {
		if len(s.Labels) != 0 || !credsecret.IsDelegatedName(s.Name) {
			t.Fatalf("PRE: %q must be a label-less delegated Secret", s.Name)
		}
	}

	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, credsecret.SessionNames("dave")[0], "dave", []string{"devs"}, nb, "1"),
		daveDelegated, erinDelegated, frankDelegated,
	})

	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "dave" {
		t.Fatalf("only dave's SESSION class may be learned; a delegated Secret is never a class. got %+v", got)
	}
	if WasLearnedUsername("erin") {
		t.Fatal("erin has only a delegated Secret; she must not have been learned")
	}
	if r, s, u := LearnedClassCounts(); r != 1 || s != 1 || u != 0 {
		t.Fatalf("counts registered/from_secrets/unparseable = %d/%d/%d, want 1/1/0 "+
			"(a delegated Secret is neither a class nor an unparseable login)", r, s, u)
	}
	if n := LearnedClientconfigSecrets(); n != 1 {
		t.Fatalf("the format-drift alarm's denominator counts %d Secrets, want 1 (the session Secret only)", n)
	}
	if *fired != 1 {
		t.Fatalf("the class hook fired %d times, want 1", *fired)
	}
}

// The informer's transform (#510) keeps a delegated Secret's annotations and
// certificate: after it, the Secret is still held for its token. A transform
// that dropped annotations would make every delegated informer hit a miss.
func TestSecretsTransform1560_KeepsWhatTheDelegatedCheckReads(t *testing.T) {
	sec := delegatedcred.Secret(t, delegatedcred.Spec{Namespace: "krateo-system", Username: "dave", Actor: "agentiko",
		Groups: []string{"devs"}, ServerURL: "https://k8s.example", Token: "tok"})
	out, err := SecretsCacheTransformForTest(sec)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*corev1.Secret)
	if !ok {
		t.Fatalf("the transform returned %T", out)
	}
	if got.Annotations[endpoints.UsernameAnnotation] != "dave" || got.Annotations[endpoints.ActorAnnotation] != "agentiko" {
		t.Fatalf("the transform dropped an annotation the delegated check reads: %v", got.Annotations)
	}
	cred := credsecret.Select(jwtutil.UserInfo{Username: "dave", Groups: []string{"devs"}, Act: &jwtutil.Actor{Sub: "agentiko"}})
	if o := cred.Check(got, time.Now()); o != credsecret.Held {
		t.Fatalf("after the transform the delegated Secret is %v, want held", o)
	}
}
