package middleware_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// Investigation arm: a token whose user's credential Secret existed for an
// earlier request and was then deleted. Control = request before deletion;
// then the same token on a write verb and a read verb after deletion.
func TestInv_CredentialDeletedAfterSuccess_WriteAndRead(t *testing.T) {
	const user = "invx"
	const xURL = "https://x.inv.example/k8s"
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAuthnNS, Name: user + "-clientconfig",
			Annotations: map[string]string{endpoints.UsernameAnnotation: user}},
		Data: map[string][]byte{"server-url": []byte(xURL), "token": []byte("tok-x")},
	}
	other := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAuthnNS, Name: "other-clientconfig",
			Annotations: map[string]string{endpoints.UsernameAnnotation: "other"}},
		Data: map[string][]byte{"server-url": []byte("https://other.inv.example"), "token": []byte("tok-other")},
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetFallthroughCountersForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	cli := fake.NewSimpleClientset(sec, other)
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, testAuthnNS); err != nil {
		t.Fatal(err)
	}
	// The apiserver stand-in holds only another user's Secret, ever.
	asked := su994APIServer(t, other)

	tok := o1382Token(t, user, "devs")
	serve := func(method string) (code int, reached bool, ep endpoints.Endpoint) {
		terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached = true
			ep, _ = xcontext.UserConfig(r.Context())
			_, _ = io.WriteString(w, `{}`)
		})
		w := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS)).Then(terminal)
		r := httptest.NewRequest(method, "/call?x=1", strings.NewReader(""))
		r.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		w.ServeHTTP(rec, r)
		return rec.Code, reached, ep
	}

	code, reached, ep := serve(http.MethodPost)
	t.Logf("CONTROL POST before delete: code=%d reached=%v server=%s token=%s", code, reached, ep.ServerURL, ep.Token)
	if code != 200 || ep.ServerURL != xURL || ep.Token != "tok-x" {
		t.Fatalf("control failed")
	}

	if err := cli.CoreV1().Secrets(testAuthnNS).Delete(ctx, sec.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cache.SecretsSnapshotLoad().ByName[sec.Name]; !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, m := range []string{http.MethodPost, http.MethodGet} {
		code, reached, ep = serve(m)
		t.Logf("AFTER delete %s: code=%d reached=%v server=%q token=%q apiserverAsked=%v", m, code, reached, ep.ServerURL, ep.Token, asked())
		if reached || code != http.StatusUnauthorized || ep.Token != "" {
			t.Errorf("%s after delete: code=%d reached=%v (want 401, handler not reached)", m, code, reached)
		}
	}
}
