// Package delegatedcred builds, for a test, the delegated credential Secret
// authn's /delegate writes for a token that carries `act` (authn#167,
// snowplow#1560).
//
// The Secret is shaped as authn's contract describes it: named
// endpoints.DelegatedSecretName(username, actor, groups); annotated with the username
// (endpoints.UsernameAnnotation) and the actor (endpoints.ActorAnnotation);
// carrying NO labels (an actor's exchange is not a sign-in, so none of the
// tenant-onboarding labels a session Secret has); holding a client certificate
// whose CommonName is the username and whose Organizations are the groups, as
// base64 of a PEM block, with its key and a CA in the same form. The material
// is real (an ECDSA key pair and a self-signed certificate), so the endpoint
// extracted from the Secret builds a transport and a stage can dial with it.
//
// Production code never imports it.
package delegatedcred

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

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Spec is one delegated credential Secret.
type Spec struct {
	Namespace string
	// Username and Actor name the Secret and fill its two annotations.
	Username string
	Actor    string
	// Groups are the certificate's Organizations, and part of the name.
	Groups []string
	// NotBefore and NotAfter bound the certificate. Zero values give a
	// certificate valid from a minute ago for ten minutes (authn's are
	// about 600 s).
	NotBefore time.Time
	NotAfter  time.Time
	// NameGroups, when non-nil, are the groups the NAME is derived from
	// instead of Groups: a Secret named for one set of groups whose
	// certificate carries another, which CheckDelegatedCredential refuses.
	NameGroups []string
	// ServerURL and Token are the endpoint's server-url and token.
	ServerURL string
	Token     string
}

// Secret returns the delegated credential Secret for s.
func Secret(t testing.TB, s Spec) *corev1.Secret {
	t.Helper()
	nb, na := s.NotBefore, s.NotAfter
	if nb.IsZero() {
		nb = time.Now().Add(-time.Minute)
	}
	if na.IsZero() {
		na = nb.Add(10 * time.Minute)
	}
	nameGroups := s.Groups
	if s.NameGroups != nil {
		nameGroups = s.NameGroups
	}
	cert, key := Certificate(t, s.Username, s.Groups, nb, na)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.Namespace,
			Name:      endpoints.DelegatedSecretName(s.Username, s.Actor, nameGroups),
			Annotations: map[string]string{
				endpoints.UsernameAnnotation: s.Username,
				endpoints.ActorAnnotation:    s.Actor,
			},
		},
		Data: map[string][]byte{
			"server-url":                 []byte(s.ServerURL),
			"token":                      []byte(s.Token),
			"client-certificate-data":    cert,
			"client-key-data":            key,
			"certificate-authority-data": cert,
		},
	}
}

// Certificate returns a self-signed client certificate for cn with
// Organizations groups, valid from notBefore to notAfter, and its key, each as
// base64 of a PEM block (authn's storage form).
func Certificate(t testing.TB, cn string, groups []string, notBefore, notAfter time.Time) (certB64, keyB64 []byte) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("delegatedcred: key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn, Organization: append([]string(nil), groups...)},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatalf("delegatedcred: certificate: %v", err)
	}
	kd, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatalf("delegatedcred: marshal key: %v", err)
	}
	enc := func(typ string, b []byte) []byte {
		return []byte(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b})))
	}
	return enc("CERTIFICATE", der), enc("EC PRIVATE KEY", kd)
}
