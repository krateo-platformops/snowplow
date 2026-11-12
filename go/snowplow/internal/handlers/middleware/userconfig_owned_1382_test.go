// userconfig_owned_1382_test.go — #1382 at the auth middleware (every
// authenticated route): the requester's credential Secret is read
// exact-then-legacy and used only when endpoints.OwnsSecret proves it theirs.
//
// The X/Y arm: X = john.doe, whose exact Secret john.doe-clientconfig is
// missing; Y = johndoe, whose johndoe-clientconfig (X's legacy name, Y's
// exact name) exists and is Y's. X gets the 401 of a missing credential, from
// the informer path and from the apiserver re-read, and never reaches the
// handler with Y's credential.
package middleware_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const (
	o1382X = "john.doe"
	o1382Y = "johndoe"
)

func o1382Token(t *testing.T, user string, groups ...string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: user, Groups: groups, Duration: time.Hour, KeyID: testKeyID, PrivateKey: testPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func o1382Cert(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
}

// o1382Secret is a credential Secret called name pointing at serverURL,
// annotated for annotated (when not "") and holding a certificate issued to
// certCN (when not "").
func o1382Secret(t *testing.T, name, serverURL, annotated, certCN string) *corev1.Secret {
	t.Helper()
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAuthnNS, Name: name},
		Data:       map[string][]byte{"server-url": []byte(serverURL), "token": []byte("tok-" + name)},
	}
	if annotated != "" {
		s.Annotations = map[string]string{endpoints.UsernameAnnotation: annotated}
	}
	if certCN != "" {
		s.Data["client-certificate-data"] = o1382Cert(t, certCN)
	}
	return s
}

type o1382Result struct {
	code    int
	body    string
	reached bool
	ep      endpoints.Endpoint
	user    jwtutil.UserInfo
}

func o1382Serve(t *testing.T, tok string) o1382Result {
	t.Helper()
	var res o1382Result
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res.reached = true
		res.ep, _ = xcontext.UserConfig(r.Context())
		res.user, _ = xcontext.UserInfo(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	wrapped := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS)).Then(terminal)
	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, r)
	res.code, res.body = rec.Code, rec.Body.String()
	return res
}

const o1382YURL = "https://y.o1382.example/k8s"

// X never gets Y's Secret, whether Y's ownership is proven by the annotation
// or by the certificate, from the informer or from the apiserver re-read.
func TestIssue1382_Middleware_XGets401NeverYsSecret(t *testing.T) {
	for _, proof := range []struct{ name, annotated, cn string }{
		{"annotated", o1382Y, ""},
		{"legacy cert only", "", o1382Y},
	} {
		for _, src := range []string{"informer", "apiserver"} {
			t.Run(proof.name+"/"+src, func(t *testing.T) {
				y := o1382Secret(t, "johndoe-clientconfig", o1382YURL, proof.annotated, proof.cn)
				if src == "informer" {
					su994Informer(t, y)
				} else {
					su994Informer(t)
				}
				asked := su994APIServer(t, y)
				res := o1382Serve(t, o1382Token(t, o1382X, "devs"))
				if res.reached || res.ep.ServerURL == o1382YURL {
					t.Fatalf("X reached the handler with Y's credential: %+v", res.ep.ServerURL)
				}
				if res.code != http.StatusUnauthorized || !strings.Contains(res.body, middleware.CauseSessionCredentialMissing) {
					t.Fatalf("X: code=%d body=%s; want the missing-credential 401", res.code, res.body)
				}
				if strings.Contains(res.body, "johndoe-clientconfig") {
					t.Errorf("the 401 names Y's Secret: %s", res.body)
				}
				if got := asked(); !slices.Equal(got, []string{"john.doe-clientconfig", "johndoe-clientconfig"}) {
					t.Errorf("apiserver asked %v, want exact then legacy", got)
				}
				// Control: Y itself is served Y's Secret.
				res = o1382Serve(t, o1382Token(t, o1382Y, "devs"))
				if !res.reached || res.ep.ServerURL != o1382YURL {
					t.Fatalf("control: Y code=%d body=%s; want Y's credential", res.code, res.body)
				}
			})
		}
	}
}

// The step-up names alike: a krateo:loa:2 token for X never gets Y's step-up
// Secret, and answers StepUpRequired.
func TestIssue1382_Middleware_StepUp_XGets401NeverYsSecret(t *testing.T) {
	y := o1382Secret(t, "johndoe-clientconfig-stepup", o1382YURL, o1382Y, "")
	su994Informer(t, y)
	asked := su994APIServer(t, y)
	res := o1382Serve(t, o1382Token(t, o1382X, "devs", credsecret.StepUpGroup))
	if res.reached || res.code != http.StatusUnauthorized || !strings.Contains(res.body, middleware.CauseStepUpRequired) {
		t.Fatalf("X step-up: reached=%v code=%d body=%s", res.reached, res.code, res.body)
	}
	if got := asked(); !slices.Equal(got, []string{"john.doe-clientconfig-stepup", "johndoe-clientconfig-stepup"}) {
		t.Errorf("apiserver asked %v", got)
	}
}

// plumbing#152 (v1.61.0): annotation says the requester, certificate says
// someone else: refused, on both paths.
func TestIssue1382_Middleware_AnnotationXCertificateY_Refused(t *testing.T) {
	mixed := o1382Secret(t, "alice-clientconfig", o1382YURL, "alice", "mallory")
	for _, src := range []string{"informer", "apiserver"} {
		t.Run(src, func(t *testing.T) {
			if src == "informer" {
				su994Informer(t, mixed)
			} else {
				su994Informer(t)
			}
			su994APIServer(t, mixed)
			res := o1382Serve(t, o1382Token(t, "alice", "devs"))
			if res.reached || res.code != http.StatusUnauthorized {
				t.Fatalf("code=%d reached=%v body=%s; want 401", res.code, res.reached, res.body)
			}
		})
	}
}

// Exact first, and legacy from memory with no apiserver GET when exact is
// missing.
func TestIssue1382_Middleware_ExactFirstLegacyFromMemory(t *testing.T) {
	const exactURL, legacyURL = "https://exact.o1382.example", "https://legacy.o1382.example"
	exact := o1382Secret(t, "john.doe-clientconfig", exactURL, o1382X, "")
	legacy := o1382Secret(t, "johndoe-clientconfig", legacyURL, "", o1382X)
	t.Run("both", func(t *testing.T) {
		su994Informer(t, exact, legacy)
		asked := su994APIServer(t)
		res := o1382Serve(t, o1382Token(t, o1382X, "devs"))
		if !res.reached || res.ep.ServerURL != exactURL || len(asked()) != 0 {
			t.Fatalf("got %q asked %v; want the exact Secret from memory", res.ep.ServerURL, asked())
		}
	})
	t.Run("legacy only", func(t *testing.T) {
		su994Informer(t, legacy)
		asked := su994APIServer(t)
		res := o1382Serve(t, o1382Token(t, o1382X, "devs"))
		if !res.reached || res.ep.ServerURL != legacyURL || len(asked()) != 0 {
			t.Fatalf("got %q asked %v; want the legacy Secret from memory, no GET", res.ep.ServerURL, asked())
		}
	})
	t.Run("apiserver legacy after exact 404", func(t *testing.T) {
		su994Informer(t)
		asked := su994APIServer(t, legacy)
		res := o1382Serve(t, o1382Token(t, o1382X, "devs"))
		if !res.reached || res.ep.ServerURL != legacyURL || !slices.Equal(asked(), []string{"john.doe-clientconfig", "johndoe-clientconfig"}) {
			t.Fatalf("got %q asked %v", res.ep.ServerURL, asked())
		}
	})
}

// #1382 requirement 6: the identity every cache key downstream folds is the
// validated token's UserInfo, put on the context by this middleware; it does
// not depend on which of the user's Secrets served the credential.
func TestIssue1382_CacheKey_IdentityIsTheTokens(t *testing.T) {
	exact := o1382Secret(t, "john.doe-clientconfig", "https://same.example", o1382X, "")
	legacy := o1382Secret(t, "johndoe-clientconfig", "https://same.example", "", o1382X)
	var users []jwtutil.UserInfo
	var eps []endpoints.Endpoint
	for _, sec := range []*corev1.Secret{exact, legacy} {
		su994Informer(t, sec)
		su994APIServer(t)
		res := o1382Serve(t, o1382Token(t, o1382X, "devs", "ops"))
		if !res.reached {
			t.Fatalf("%s: code=%d body=%s", sec.Name, res.code, res.body)
		}
		users = append(users, res.user)
		eps = append(eps, res.ep)
	}
	if users[0].Username != users[1].Username || !slices.Equal(users[0].Groups, users[1].Groups) || users[0].Username != o1382X {
		t.Fatalf("the identity differs with the Secret's name: %+v vs %+v", users[0], users[1])
	}
	if eps[0].ServerURL != eps[1].ServerURL {
		t.Fatalf("the same credential content served differently")
	}
}
