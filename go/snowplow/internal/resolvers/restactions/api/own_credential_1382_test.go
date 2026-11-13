package api

// own_credential_1382_test.go — #1382 at the per-stage dial site
// (resolveOne(nil) → resolveOwn): the caller's own credential is read
// exact-then-legacy, from the informer and then the apiserver, and a Secret
// is used only when endpoints.OwnsSecret proves it the caller's.
//
// The X/Y arm: X = john.doe, whose exact Secret john.doe-clientconfig is
// missing; Y = johndoe, whose johndoe-clientconfig (X's legacy name, Y's
// exact name) exists and is Y's. X must never get Y's credential: the stage
// fails with a NotFound (the miss behaviour), and nothing is dialled with
// Y's token.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

const (
	oc1382NS = "krateo-system"
	oc1382X  = "john.doe"
	oc1382Y  = "johndoe"
)

// oc1382Cert is a base64 PEM client certificate issued to cn, as authn stores
// one in client-certificate-data.
func oc1382Cert(t testing.TB, cn string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"devs"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// oc1382Secret is a credential Secret called name carrying token, annotated
// for annotated (when not "") and holding a certificate issued to certCN
// (when not "").
func oc1382Secret(t testing.TB, name, token, annotated, certCN string) *corev1.Secret {
	t.Helper()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: oc1382NS, Name: name},
		Data:       map[string][]byte{"server-url": []byte("https://k8s.example"), "token": []byte(token)},
	}
	if annotated != "" {
		sec.Annotations = map[string]string{endpoints.UsernameAnnotation: annotated}
	}
	if certCN != "" {
		sec.Data["client-certificate-data"] = []byte(oc1382Cert(t, certCN))
	}
	return sec
}

// oc1382Informer publishes secs as the servable Secrets snapshot, or leaves
// the informer unservable when secs is nil.
func oc1382Informer(t testing.TB, secs []*corev1.Secret) {
	t.Helper()
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	if secs == nil {
		return
	}
	by := map[string]*corev1.Secret{}
	for _, s := range secs {
		by[s.Name] = s
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: by})
	cache.ForceSecretsCacheReadyForTest(oc1382NS)
}

// oc1382APIServer serves have by name and answers NotFound for anything else
// (or code for a name in fail), recording every Secret name asked for.
func oc1382APIServer(t testing.TB, fail map[string]int, have ...*corev1.Secret) (*rest.Config, func() []string) {
	t.Helper()
	by := map[string]*corev1.Secret{}
	for _, s := range have {
		by[s.Name] = s
	}
	var mu sync.Mutex
	var asked []string
	prefix := "/api/v1/namespaces/" + oc1382NS + "/secrets/"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name, ok := strings.CutPrefix(r.URL.Path, prefix)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		asked = append(asked, name)
		mu.Unlock()
		if code, failing := fail[name]; failing {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
				"reason": "InternalError", "code": code, "message": "boom"})
			return
		}
		s, ok := by[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
				"reason": "NotFound", "code": 404, "message": `secrets "` + name + `" not found`,
				"details": map[string]any{"name": name, "kind": "secrets"}})
			return
		}
		out := s.DeepCopy()
		out.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), asked...)
	}
}

func oc1382Resolve(t *testing.T, rc *rest.Config, user string, groups ...string) (endpoints.Endpoint, error) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	prev := env.TestMode()
	env.SetTestMode(true) // keep the fixture's server-url
	t.Cleanup(func() { env.SetTestMode(prev) })
	m := &endpointReferenceMapper{authnNS: oc1382NS, username: user, groups: groups, rc: rc}
	ep, isSA, err := m.resolveOne(context.Background(), nil)
	if isSA {
		t.Fatal("the caller's own credential was reported as the ServiceAccount endpoint")
	}
	return ep, err
}

// The premise of the X/Y arm: X's legacy name is Y's exact name.
func TestIssue1382_XYPremise(t *testing.T) {
	x := credsecret.SessionNames(oc1382X)
	y := credsecret.SessionNames(oc1382Y)
	if !slices.Equal(x, []string{"john.doe-clientconfig", "johndoe-clientconfig"}) || !slices.Equal(y, []string{"johndoe-clientconfig"}) {
		t.Fatalf("premise: X's names %v, Y's names %v", x, y)
	}
}

// X/Y at the dial site, every source: Y's Secret (owned by Y through its
// annotation, or through its certificate's CN) under X's legacy name is never
// X's credential.
func TestIssue1382_DialSite_XNeverGetsYsSecret(t *testing.T) {
	for _, proof := range []struct{ name, annotated, cn string }{
		{"annotated", oc1382Y, ""},
		{"annotated+cert", oc1382Y, oc1382Y},
		{"legacy cert only", "", oc1382Y},
	} {
		ySecret := oc1382Secret(t, "johndoe-clientconfig", "Y-TOKEN", proof.annotated, proof.cn)
		for _, src := range []string{"informer", "apiserver", "both"} {
			t.Run(proof.name+"/"+src, func(t *testing.T) {
				var inf []*corev1.Secret
				var srv []*corev1.Secret
				switch src {
				case "informer":
					inf = []*corev1.Secret{ySecret}
				case "apiserver":
					srv = []*corev1.Secret{ySecret}
				case "both":
					inf, srv = []*corev1.Secret{ySecret}, []*corev1.Secret{ySecret}
				}
				oc1382Informer(t, inf)
				rc, asked := oc1382APIServer(t, nil, srv...)
				ep, err := oc1382Resolve(t, rc, oc1382X)
				if ep.Token == "Y-TOKEN" {
					t.Fatalf("X resolved Y's credential")
				}
				if !apierrors.IsNotFound(err) {
					t.Fatalf("X's resolve: err=%v, want the NotFound of a missing credential", err)
				}
				if strings.Contains(err.Error(), "johndoe-clientconfig") {
					t.Errorf("the miss names Y's Secret: %v", err)
				}
				if got := asked(); !slices.Equal(got, []string{"john.doe-clientconfig", "johndoe-clientconfig"}) {
					t.Errorf("apiserver asked %v, want exact then legacy", got)
				}
				// Control: Y itself resolves the same Secret.
				ep, err = oc1382Resolve(t, rc, oc1382Y)
				if err != nil || ep.Token != "Y-TOKEN" {
					t.Fatalf("control: Y's own resolve = %q, %v; want Y's credential", ep.Token, err)
				}
			})
		}
	}
}

// The step-up names behave alike: Y's step-up Secret under X's legacy step-up
// name is never X's.
func TestIssue1382_DialSite_StepUp_XNeverGetsYsSecret(t *testing.T) {
	ySecret := oc1382Secret(t, "johndoe-clientconfig-stepup", "Y-STEPUP", oc1382Y, "")
	oc1382Informer(t, []*corev1.Secret{ySecret})
	rc, asked := oc1382APIServer(t, nil, ySecret)
	ep, err := oc1382Resolve(t, rc, oc1382X, credsecret.StepUpGroup)
	if ep.Token == "Y-STEPUP" || !apierrors.IsNotFound(err) {
		t.Fatalf("X's step-up resolve = %q, %v; want the NotFound miss", ep.Token, err)
	}
	if got := asked(); !slices.Equal(got, []string{"john.doe-clientconfig-stepup", "johndoe-clientconfig-stepup"}) {
		t.Errorf("apiserver asked %v", got)
	}
}

// plumbing#152 (v1.61.0): an annotation that names the caller does not prove
// ownership when the Secret's certificate was issued to someone else.
func TestIssue1382_DialSite_AnnotationXCertificateY_Refused(t *testing.T) {
	mixed := oc1382Secret(t, "alice-clientconfig", "MIXED", "alice", "mallory")
	for _, src := range []string{"informer", "apiserver"} {
		t.Run(src, func(t *testing.T) {
			if src == "informer" {
				oc1382Informer(t, []*corev1.Secret{mixed})
			} else {
				oc1382Informer(t, nil)
			}
			rc, _ := oc1382APIServer(t, nil, mixed)
			ep, err := oc1382Resolve(t, rc, "alice")
			if ep.Token == "MIXED" || !apierrors.IsNotFound(err) {
				t.Fatalf("resolve = %q, %v; want the NotFound miss", ep.Token, err)
			}
		})
	}
}

// Exact first: with both of X's Secrets present and X's, the exact one wins,
// from the informer and from the apiserver.
func TestIssue1382_DialSite_ExactFirst(t *testing.T) {
	exact := oc1382Secret(t, "john.doe-clientconfig", "EXACT", oc1382X, "")
	legacy := oc1382Secret(t, "johndoe-clientconfig", "LEGACY", "", oc1382X)
	t.Run("informer", func(t *testing.T) {
		oc1382Informer(t, []*corev1.Secret{exact, legacy})
		rc, asked := oc1382APIServer(t, nil)
		ep, err := oc1382Resolve(t, rc, oc1382X)
		if err != nil || ep.Token != "EXACT" || len(asked()) != 0 {
			t.Fatalf("got %q, %v, asked %v; want EXACT from memory", ep.Token, err, asked())
		}
	})
	t.Run("apiserver", func(t *testing.T) {
		oc1382Informer(t, nil)
		rc, asked := oc1382APIServer(t, nil, exact, legacy)
		ep, err := oc1382Resolve(t, rc, oc1382X)
		if err != nil || ep.Token != "EXACT" || !slices.Equal(asked(), []string{"john.doe-clientconfig"}) {
			t.Fatalf("got %q, %v, asked %v; want EXACT in one GET", ep.Token, err, asked())
		}
	})
}

// Legacy where exact is missing: from the informer with NO apiserver GET (the
// migration's steady state costs nothing), and from the apiserver with the
// one extra GET.
func TestIssue1382_DialSite_LegacyWhenExactMissing(t *testing.T) {
	legacy := oc1382Secret(t, "johndoe-clientconfig", "LEGACY", "", oc1382X)
	t.Run("informer", func(t *testing.T) {
		oc1382Informer(t, []*corev1.Secret{legacy})
		rc, asked := oc1382APIServer(t, nil, legacy)
		ep, err := oc1382Resolve(t, rc, oc1382X)
		if err != nil || ep.Token != "LEGACY" || len(asked()) != 0 {
			t.Fatalf("got %q, %v, asked %v; want LEGACY from memory, no GET", ep.Token, err, asked())
		}
	})
	t.Run("apiserver", func(t *testing.T) {
		oc1382Informer(t, nil)
		rc, asked := oc1382APIServer(t, nil, legacy)
		ep, err := oc1382Resolve(t, rc, oc1382X)
		if err != nil || ep.Token != "LEGACY" || !slices.Equal(asked(), []string{"john.doe-clientconfig", "johndoe-clientconfig"}) {
			t.Fatalf("got %q, %v, asked %v; want LEGACY after the exact name's 404", ep.Token, err, asked())
		}
	})
}

// A failure that is not a NotFound is not a miss: it is returned, and the
// legacy name is not tried in its place.
func TestIssue1382_DialSite_NonNotFoundIsNotAMiss(t *testing.T) {
	oc1382Informer(t, nil)
	legacy := oc1382Secret(t, "johndoe-clientconfig", "LEGACY", "", oc1382X)
	rc, asked := oc1382APIServer(t, map[string]int{"john.doe-clientconfig": http.StatusInternalServerError}, legacy)
	ep, err := oc1382Resolve(t, rc, oc1382X)
	if err == nil || apierrors.IsNotFound(err) || ep.Token == "LEGACY" {
		t.Fatalf("got %q, %v; want the 500, not a fall-back", ep.Token, err)
	}
	if got := asked(); !slices.Equal(got, []string{"john.doe-clientconfig"}) {
		t.Errorf("asked %v, want only the exact name", got)
	}
}

// #1382 requirement 6: the exact-vs-legacy choice cannot change a key for the
// same user. The credential a stage dials keys its HTTP client
// (credhttp.EndpointKeyOf over the Endpoint); the same credential stored under
// the exact or the legacy name resolves to the same Endpoint and the same key.
// (The resolved-output identity is the token's UserInfo, never the Secret:
// TestIssue1382_CacheKey_IdentityIsTheTokens in the middleware package; the
// learned class key: TestIssue1382_Learned_* in internal/cache.)
func TestIssue1382_CacheKey_ExactOrLegacyNameSameEndpointKey(t *testing.T) {
	cert := oc1382Cert(t, oc1382X)
	mk := func(name string) *corev1.Secret {
		s := oc1382Secret(t, name, "SAME", "", "")
		s.Data["client-certificate-data"] = []byte(cert)
		return s
	}
	resolveFrom := func(sec *corev1.Secret) endpoints.Endpoint {
		oc1382Informer(t, []*corev1.Secret{sec})
		rc, _ := oc1382APIServer(t, nil)
		ep, err := oc1382Resolve(t, rc, oc1382X)
		if err != nil {
			t.Fatalf("%s: %v", sec.Name, err)
		}
		return ep
	}
	exact := resolveFrom(mk("john.doe-clientconfig"))
	legacy := resolveFrom(mk("johndoe-clientconfig"))
	if exact != legacy {
		t.Fatalf("the same credential resolved differently under its two names")
	}
	if credhttp.EndpointKeyOf(&exact, "held") != credhttp.EndpointKeyOf(&legacy, "held") {
		t.Fatal("the endpoint client key differs between the exact and the legacy name")
	}
}

// The readers directly: an empty informer never serves, and the apiserver
// miss with every name absent is the first name's NotFound.
func TestIssue1382_Readers_MissIsFirstNamesNotFound(t *testing.T) {
	oc1382Informer(t, nil)
	t.Setenv("CACHE_ENABLED", "true")
	if _, served, err := FromInformerOwnSecret(context.Background(), oc1382NS, oc1382X, credsecret.SessionNames(oc1382X)); served || err != nil {
		t.Fatalf("an unservable informer served: %v %v", served, err)
	}
	rc, _ := oc1382APIServer(t, nil)
	_, err := FromAPIServerOwnSecret(context.Background(), rc, oc1382NS, oc1382X, credsecret.SessionNames(oc1382X))
	if !apierrors.IsNotFound(err) || !strings.Contains(err.Error(), `"john.doe-clientconfig"`) {
		t.Fatalf("err = %v, want the exact name's NotFound", err)
	}
}
