package cache

// learned_identities_test.go — #262 S1/S2 registry unit arms.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func liCert(t *testing.T, cn string, orgs []string, nb time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(nb.UnixNano()), Subject: pkix.Name{CommonName: cn, Organization: orgs},
		NotBefore: nb, NotAfter: nb.Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func liSecret(t *testing.T, name, cn string, orgs []string, nb time.Time, rv string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: rv},
		Data: map[string][]byte{
			clientCertificateDataKey: []byte(base64.StdEncoding.EncodeToString(liCert(t, cn, orgs, nb))),
		},
	}
}

func liHooked(t *testing.T) *int {
	t.Helper()
	ResetLearnedIdentitiesForTest()
	t.Cleanup(ResetLearnedIdentitiesForTest)
	n := 0
	RegisterLearnedClassHook(func() { n++ })
	return &n
}

func TestLearned262_SecretsAreTheSource_CNNotTheName(t *testing.T) {
	fired := liHooked(t)
	nb := time.Now().Add(-time.Hour).Truncate(time.Second)
	// The Secret name is DNS-1123-lossy; the username comes from the CN.
	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, "alice-example-com-clientconfig", "alice@example.com", []string{"devs", "ops"}, nb, "1"),
		{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Data: map[string][]byte{"x": []byte("y")}},
	})
	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "alice@example.com" || len(got[0].Groups) != 2 || !got[0].LastSeen.Equal(nb) {
		t.Fatalf("want one class {alice@example.com [devs ops]} seen at the login instant, got %+v", got)
	}
	if *fired != 1 {
		t.Fatalf("a new class fires the hook once, fired=%d", *fired)
	}
	if p := DrainPendingLearnedClasses(); len(p) != 1 {
		t.Fatalf("pending=%d, want 1", len(p))
	}
	if r, s, u := LearnedClassCounts(); r != 1 || s != 1 || u != 0 {
		t.Fatalf("counts %d/%d/%d", r, s, u)
	}
}

func TestLearned262_UnchangedRVIsNotReparsed(t *testing.T) {
	liHooked(t)
	nb := time.Now().Add(-time.Hour)
	s := liSecret(t, "bob-clientconfig", "bob", []string{"devs"}, nb, "7")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{s})
	// Same RV, different bytes: a real Secret never does this, so it proves the
	// certificate is not parsed again.
	s2 := s.DeepCopy()
	s2.Data[clientCertificateDataKey] = []byte("not a certificate")
	SyncLearnedFromSecretsForTest([]*corev1.Secret{s2})
	if r, _, u := LearnedClassCounts(); r != 1 || u != 0 {
		t.Fatalf("an unchanged ResourceVersion must not re-parse: registered=%d unparseable=%d", r, u)
	}
	s2.ResourceVersion = "8"
	SyncLearnedFromSecretsForTest([]*corev1.Secret{s2})
	if r, s, u := LearnedClassCounts(); r != 0 || s != 0 || u != 1 {
		t.Fatalf("a moved RV with an unreadable certificate: registered=%d from_secrets=%d unparseable=%d, want 0/0/1", r, s, u)
	}
}

func TestLearned262_ReloginReplacesClass_DeleteRemoves_SAExcluded(t *testing.T) {
	fired := liHooked(t)
	nb := time.Now().Add(-time.Hour)
	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, "carol-clientconfig", "carol", []string{"devs"}, nb, "1"),
		liSecret(t, "sa-clientconfig", "system:serviceaccount:ns:robot", nil, nb, "1"),
	})
	if r, _, u := LearnedClassCounts(); r != 1 || u != 0 {
		t.Fatalf("a ServiceAccount login is not a class and not unparseable: registered=%d unparseable=%d", r, u)
	}
	SyncLearnedFromSecretsForTest([]*corev1.Secret{
		liSecret(t, "carol-clientconfig", "carol", []string{"devs", "ops"}, nb.Add(time.Minute), "2"),
	})
	got := LearnedIdentitiesSnapshot()
	if len(got) != 1 || len(got[0].Groups) != 2 {
		t.Fatalf("a re-login with a new group set REPLACES the class, got %+v", got)
	}
	if *fired != 2 {
		t.Fatalf("the membership change is a new class (hook fired %d, want 2)", *fired)
	}
	if !WasLearnedUsername("carol") {
		t.Fatal("the privacy predicate remembers a learned username")
	}
	SyncLearnedFromSecretsForTest(nil)
	if r, _, _ := LearnedClassCounts(); r != 0 {
		t.Fatalf("a deleted Secret removes its class, registered=%d", r)
	}
	if !WasLearnedUsername("carol") {
		t.Fatal("the privacy predicate outlives the class (its cells keep the representative)")
	}
}

func TestLearned262_LiveOnlyExpiresWithJWT_NewestFirst(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	liHooked(t)
	nb := time.Now().Add(-time.Hour)
	SyncLearnedFromSecretsForTest([]*corev1.Secret{liSecret(t, "old-clientconfig", "old", []string{"g"}, nb, "1")})
	ObserveLiveIdentity("live", []string{"g", "h"}, time.Now().Add(150*time.Millisecond))
	ObserveLiveIdentity("noexp", []string{"g"}, time.Time{}) // never registered from traffic alone
	got := LearnedIdentitiesSnapshot()
	if len(got) != 2 || got[0].Username != "live" || got[1].Username != "old" {
		t.Fatalf("newest LastSeen first: got %+v", got)
	}
	time.Sleep(200 * time.Millisecond)
	got = LearnedIdentitiesSnapshot()
	if len(got) != 1 || got[0].Username != "old" {
		t.Fatalf("a live-only class leaves with its JWT: got %+v", got)
	}
	// The class key is over the EFFECTIVE groups (system:authenticated folds in).
	if LearnedClassKey("u", []string{"g"}) != LearnedClassKey("u", []string{"system:authenticated", "g"}) {
		t.Fatal("class key must fold system:authenticated")
	}
}
