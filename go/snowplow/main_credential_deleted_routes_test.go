// main_credential_deleted_routes_test.go — through the REAL route table
// (mountCallerIdentityRoutes, the one main() mounts) and the real
// middleware.UserConfig: a caller whose credential Secret is gone is answered
// 401 SessionCredentialMissing on EVERY identity route, read or write, and
// nothing is dialled for them.
//
// Routes covered: GET /call, POST /call/read, GET /list, GET and POST /export,
// POST /jq, POST/PUT/PATCH/DELETE /call, POST/PUT/PATCH /call/dry-run.
//
// CONTROL first: the same token with its Secret present gets past the auth
// middleware on each route (anything but a 401).
//
// Why no L1 cell is seeded here: a /call read reaches the L1 Get only after the
// dispatcher's fetchObject and RBAC gate, whose seams (setFetchObjectForTest and
// friends) are unexported test shims inside package dispatchers, and a hit needs
// a key derived from a live RBAC snapshot. Exporting a seam from production code
// only for this test is not the clean way, so the cell-in-L1 form of the arm is
// internal/handlers/dispatchers/credential_deleted_l1_hit_test.go (real
// UserConfig in front of the real RESTAction handler, a warm cell, control =
// served as a hit). This file pins the part that one cannot: the ROUTE WIRING
// (UserConfig ahead of the dispatcher and of every write handler) and that no
// request is dialled for a caller with no credential.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

	// Build the routes first: the handlers capture the process-wide SA rest.Config
	// singleton (no exported reset), so it must not see the override below.
	mux := http.NewServeMux()
	mountCallerIdentityRoutes(newRouteTable(mux), use.NewChain(), debugAuthTestKeys, authnNS, nil)

	// One stand-in server: the apiserver for the Secret re-read (this user's
	// Secret is never there) and the user's own endpoint. It records every
	// request that is not a Secret read: those are DIALS made for the caller.
	var mu sync.Mutex
	var dials []string
	apisrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/"+authnNS+"/secrets/") {
			mu.Lock()
			dials = append(dials, r.Method+" "+r.URL.Path)
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
			"reason": "NotFound", "code": 404, "message": "not found"})
	}))
	t.Cleanup(apisrv.Close)
	t.Cleanup(credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: apisrv.URL, BearerToken: "sa"}, nil
	}))

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: authnNS, Name: user + "-clientconfig",
			Annotations: map[string]string{endpoints.UsernameAnnotation: user}},
		Data: map[string][]byte{"server-url": []byte(apisrv.URL), "token": []byte("tok")},
	}
	cli := fake.NewSimpleClientset(sec)
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, authnNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}

	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: user, Groups: []string{"devs"}, Duration: time.Hour,
		KeyID: debugAuthTestKeyID, PrivateKey: debugAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	const q = "?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=krateo-system&name=ra"
	const w = "?apiVersion=v1&resource=configmaps&namespace=krateo-system&name=cm"
	routes := []struct{ method, path string }{
		{http.MethodGet, "/call" + q},
		{http.MethodPost, "/call/read" + q},
		{http.MethodGet, "/list" + q},
		{http.MethodGet, "/export" + q},
		{http.MethodPost, "/export" + q},
		{http.MethodPost, "/jq"},
		{http.MethodPost, "/call" + w},
		{http.MethodPut, "/call" + w},
		{http.MethodPatch, "/call" + w},
		{http.MethodDelete, "/call" + w},
		{http.MethodPost, "/call/dry-run" + w},
		{http.MethodPut, "/call/dry-run" + w},
		{http.MethodPatch, "/call/dry-run" + w},
	}
	drive := func(method, path string) (int, string) {
		r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }() // a route that runs a handler with no identity may panic
			mux.ServeHTTP(rec, r)
		}()
		return rec.Code, rec.Body.String()
	}
	missing := func(code int, body string) bool {
		return code == http.StatusUnauthorized && strings.Contains(body, "SessionCredentialMissing")
	}

	// CONTROL: Secret present -> past the auth middleware on every route.
	for _, rt := range routes {
		if code, body := drive(rt.method, rt.path); code == http.StatusUnauthorized {
			t.Fatalf("CONTROL %s %s: the caller's credential is present, yet the route answered 401 %s "+
				"(a handler ran ahead of the credential lookup, or the lookup is broken)", rt.method, rt.path, body)
		}
	}

	if err := cli.CoreV1().Secrets(authnNS).Delete(ctx, sec.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := cache.SecretsSnapshotLoad().ByName[sec.Name]; !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the informer snapshot still holds the deleted Secret after 3s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	dials = nil // anything dialled before this point was the control
	mu.Unlock()

	for _, rt := range routes {
		if code, body := drive(rt.method, rt.path); !missing(code, body) {
			t.Errorf("%s %s: the caller's credential is gone, so the route must answer 401 SessionCredentialMissing "+
				"before any handler runs; it answered %d %s (route wiring: a handler or cache hit is ahead of the credential lookup)",
				rt.method, rt.path, code, body)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dials) != 0 {
		t.Errorf("a caller with no credential was dialled for: %v (nothing may be sent)", dials)
	}
}
