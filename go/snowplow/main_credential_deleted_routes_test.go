// main_credential_deleted_routes_test.go — through the REAL route table
// (mountCallerIdentityRoutes, the one main() mounts) and the real
// middleware.UserConfig: a caller whose credential Secret is gone gets 401
// SessionCredentialMissing on the read routes that serve from L1 (GET /call,
// POST /call/read, GET /list) and on the write route (POST /call), whatever
// the dispatcher behind them could serve.
//
// CONTROL first: the same token with its Secret present gets past the auth
// middleware (anything but the credential-missing 401).
//
// The route-order mutants (the dispatcher ahead of the lookup, a hit served
// before it) are exercised against this file by editing public_identity.go;
// see the PR's RUN / NOT RUN table. The dispatcher-level cell-in-L1 form of
// the arm is internal/handlers/dispatchers/credential_deleted_l1_hit_test.go.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func TestCredentialDeleted_RouteTable_ReadAndWriteGet401(t *testing.T) {
	const authnNS = "krateo-system"
	const user = "cdroute"
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetRouteScopeRegistryForTest()
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: authnNS, Name: user + "-clientconfig",
			Annotations: map[string]string{endpoints.UsernameAnnotation: user}},
		Data: map[string][]byte{"server-url": []byte("https://cd.example/k8s"), "token": []byte("tok")},
	}
	cli := fake.NewSimpleClientset(sec)
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, authnNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	apisrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
			"reason": "NotFound", "code": 404, "message": "secrets not found"})
	}))
	t.Cleanup(apisrv.Close)
	t.Cleanup(credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: apisrv.URL, BearerToken: "sa"}, nil
	}))

	mux := http.NewServeMux()
	mountCallerIdentityRoutes(newRouteTable(mux), use.NewChain(), debugAuthTestKeys, authnNS, nil)
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: user, Groups: []string{"devs"}, Duration: time.Hour,
		KeyID: debugAuthTestKeyID, PrivateKey: debugAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	const q = "?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=krateo-system&name=ra"
	routes := []struct{ method, path string }{
		{http.MethodGet, "/call" + q},
		{http.MethodPost, "/call/read" + q},
		{http.MethodGet, "/list" + q},
		{http.MethodPost, "/call" + q},
	}
	drive := func(method, path string) (int, string) {
		var body *strings.Reader
		if method == http.MethodPost {
			body = strings.NewReader(`{}`)
		} else {
			body = strings.NewReader("")
		}
		r := httptest.NewRequest(method, path, body)
		r.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }() // a mutant that runs the dispatcher with no identity may panic
			mux.ServeHTTP(rec, r)
		}()
		return rec.Code, rec.Body.String()
	}
	missing := func(code int, body string) bool {
		return code == http.StatusUnauthorized && strings.Contains(body, "SessionCredentialMissing")
	}

	// CONTROL: Secret present -> past the auth middleware on every route.
	for _, rt := range routes {
		if code, body := drive(rt.method, rt.path); missing(code, body) || code == http.StatusUnauthorized {
			t.Fatalf("CONTROL %s %s: with the credential present the auth middleware answered %d %s", rt.method, rt.path, code, body)
		}
	}

	if err := cli.CoreV1().Secrets(authnNS).Delete(ctx, sec.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cache.SecretsSnapshotLoad().ByName[sec.Name]; !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, rt := range routes {
		if code, body := drive(rt.method, rt.path); !missing(code, body) {
			t.Errorf("%s %s with the credential deleted: got %d %s; want 401 SessionCredentialMissing", rt.method, rt.path, code, body)
		}
	}
}
