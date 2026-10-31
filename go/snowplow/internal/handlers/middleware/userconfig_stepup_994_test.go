// userconfig_stepup_994_test.go — #994: the step-up credential (authn#49).
//
// Driven through the REAL middleware.UserConfig with real RS256 JWTs, the real
// Secrets informer (fake clientset) and — for the miss branch — the real
// apiserver re-read (credhttp.FromSecret) against a recording fake apiserver.
//
//   - a token whose groups carry krateo:loa:2 reads `<user>-clientconfig-stepup`;
//   - a token without the group never touches a step-up Secret, even when one
//     exists (in the informer, or on the apiserver);
//   - a missing step-up Secret (not in the informer, NotFound on the re-read)
//     answers 401 and never falls back to the session Secret, although the
//     session Secret is right there in both places.
package middleware_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const (
	su994User      = "su994-dora"
	su994Group     = "su994-devs"
	su994Session   = "https://session.su994.example/k8s"
	su994StepUp    = "https://stepup.su994.example/k8s"
	su994SecretsNS = testAuthnNS
)

func su994Token(t *testing.T, groups ...string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   su994User,
		Groups:     groups,
		Duration:   time.Hour,
		KeyID:      testKeyID,
		PrivateKey: testPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

func su994Secret(name, serverURL string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: su994SecretsNS, Name: name},
		Data:       map[string][]byte{"server-url": []byte(serverURL), "token": []byte("tok-" + name)},
	}
}

// su994Informer starts the real Secrets informer over exactly secrets.
func su994Informer(t *testing.T, secrets ...*corev1.Secret) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetFallthroughCountersForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	objs := make([]*corev1.Secret, len(secrets))
	copy(objs, secrets)
	cli := fake.NewSimpleClientset()
	for _, s := range objs {
		if _, err := cli.CoreV1().Secrets(s.Namespace).Create(context.Background(), s, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed %s: %v", s.Name, err)
		}
	}
	restore := cache.SetSecretsClientForTest(cli)
	t.Cleanup(restore)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, su994SecretsNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	if !cache.SecretsCacheServable() {
		t.Fatal("PRE: the Secrets informer must be servable")
	}
}

// su994APIServer is the apiserver the miss branch re-reads from: it serves the
// Secrets in have and answers NotFound for any other name, recording every
// Secret name it was asked for.
func su994APIServer(t *testing.T, have ...*corev1.Secret) func() []string {
	t.Helper()
	byName := map[string]*corev1.Secret{}
	for _, s := range have {
		byName[s.Name] = s
	}
	var mu sync.Mutex
	var asked []string
	prefix := "/api/v1/namespaces/" + su994SecretsNS + "/secrets/"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasPrefix(r.URL.Path, prefix) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, prefix)
		mu.Lock()
		asked = append(asked, name)
		mu.Unlock()
		s, ok := byName[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404,
				"message": `secrets "` + name + `" not found`, "details": map[string]any{"name": name, "kind": "secrets"},
			})
			return
		}
		out := s.DeepCopy()
		out.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	restore := credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: srv.URL, BearerToken: "tok-sa"}, nil
	})
	t.Cleanup(restore)
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), asked...)
	}
}

type su994Result struct {
	code    int
	body    string
	reached bool
	ep      endpoints.Endpoint
	user    jwtutil.UserInfo
}

func su994Serve(t *testing.T, tok string) su994Result {
	t.Helper()
	var res su994Result
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

// The group selects the step-up Secret; the same user without it reads the
// session Secret — both present in the informer.
func TestUserConfig994_StepUpGroupSelectsStepUpSecret(t *testing.T) {
	su994Informer(t,
		su994Secret(credsecret.SessionName(su994User), su994Session),
		su994Secret(credsecret.StepUpName(su994User), su994StepUp))
	asked := su994APIServer(t)

	up := su994Serve(t, su994Token(t, su994Group, credsecret.StepUpGroup))
	if up.code != http.StatusOK || !up.reached {
		t.Fatalf("step-up token: code=%d body=%s; want 200 from the informer hit", up.code, up.body)
	}
	if up.ep.ServerURL != su994StepUp {
		t.Fatalf("a token carrying %s must read the step-up Secret: endpoint=%q, want %q",
			credsecret.StepUpGroup, up.ep.ServerURL, su994StepUp)
	}
	if !credsecret.IsStepUp(up.user.Groups) {
		t.Fatalf("the step-up group must reach the request ctx (it folds into RBAC and every key): groups=%v", up.user.Groups)
	}

	plain := su994Serve(t, su994Token(t, su994Group))
	if plain.code != http.StatusOK || plain.ep.ServerURL != su994Session {
		t.Fatalf("a token without the group must read the session Secret: code=%d endpoint=%q, want 200 %q",
			plain.code, plain.ep.ServerURL, su994Session)
	}
	if got := asked(); len(got) != 0 {
		t.Fatalf("both reads were informer hits; the apiserver must not be asked, got %v", got)
	}
}

// A token without the group never touches a step-up Secret: with only the
// step-up Secret in the informer AND on the apiserver, it re-reads its session
// name, gets NotFound and answers 401.
func TestUserConfig994_NoGroupNeverReadsStepUpSecret(t *testing.T) {
	stepUp := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
	su994Informer(t, stepUp)
	asked := su994APIServer(t, stepUp)

	res := su994Serve(t, su994Token(t, su994Group))
	if res.reached || res.code != http.StatusUnauthorized {
		t.Fatalf("a token without %s whose session Secret is gone must answer 401 without reaching the handler; "+
			"code=%d reached=%v endpoint=%q", credsecret.StepUpGroup, res.code, res.reached, res.ep.ServerURL)
	}
	got := asked()
	if len(got) != 1 || got[0] != credsecret.SessionName(su994User) {
		t.Fatalf("the apiserver re-read must ask for the session Secret only, got %v", got)
	}
	if strings.Contains(res.body, credsecret.StepUpSuffix) {
		t.Fatalf("the 401 names a step-up Secret: %s", res.body)
	}
}

// A missing step-up Secret answers 401: not in the informer, NotFound on the
// apiserver re-read — and the session Secret, present in both, is never read.
func TestUserConfig994_MissingStepUpSecretIs401NoFallback(t *testing.T) {
	session := su994Secret(credsecret.SessionName(su994User), su994Session)
	su994Informer(t, session)
	asked := su994APIServer(t, session)

	res := su994Serve(t, su994Token(t, su994Group, credsecret.StepUpGroup))
	if res.reached || res.code != http.StatusUnauthorized {
		t.Fatalf("NO-FALLBACK VIOLATED: a step-up token whose step-up Secret is missing must answer 401; "+
			"code=%d reached=%v endpoint=%q (session endpoint %q)", res.code, res.reached, res.ep.ServerURL, su994Session)
	}
	got := asked()
	if len(got) != 1 || got[0] != credsecret.StepUpName(su994User) {
		t.Fatalf("the apiserver re-read must ask for the step-up Secret only (no session fallback), got %v", got)
	}
}
