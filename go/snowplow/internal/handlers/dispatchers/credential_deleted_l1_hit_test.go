// credential_deleted_l1_hit_test.go — a caller whose credential Secret is gone
// gets 401, never a cached cell.
//
// A cell is already in L1 for the caller's cohort and the caller's token is
// valid. Control: with the credential Secret present the request is served the
// cell (L1 hit, no resolve). Then the Secret is deleted and the SAME request
// must be answered 401 SessionCredentialMissing with the L1 Get never reached
// and the resolver never run.
//
// The chain under test is the production order: middleware.UserConfig, then the
// RESTAction dispatcher (public_identity.go mounts GET /call that way; the
// route-table twin of this arm is TestCredentialDeleted_RouteTable... in
// package main). The arm also runs the two broken orders (the lookup after the
// dispatcher, and the dispatcher serving the hit before the lookup) and asserts
// each is DETECTED, so a reordering cannot pass unnoticed.
package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crypto/rand"
	"crypto/rsa"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

const cdAuthnNS = "krateo-system"

type cdOrder int

const (
	cdAuthThenDispatch cdOrder = iota // production
	cdDispatchThenAuth                // mutant: lookup after the dispatcher
	cdHitBeforeAuth                   // mutant: a hit is served before the lookup
)

type cdRun struct {
	code     int
	body     string
	resolves int
}

func cdSetup(t *testing.T) (serve func(order cdOrder) cdRun, deleteSecret func(), cellKey string, handle cacheHandle, warm []byte) {
	t.Helper()
	h1BuildWatcher(t) // CACHE_ENABLED=true, RBAC snapshot granting h1User the RA GVR

	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: cdAuthnNS, Name: h1User + "-clientconfig",
			Annotations: map[string]string{endpoints.UsernameAnnotation: h1User}},
		Data: map[string][]byte{"server-url": []byte("https://alice.cd.example/k8s"), "token": []byte("tok-alice")},
	}
	cli := fake.NewSimpleClientset(sec)
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	ictx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ictx, nil, cdAuthnNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	// The apiserver re-read: this user's Secret is never there.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
			"reason": "NotFound", "code": 404, "message": "secrets not found"})
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: srv.URL, BearerToken: "sa"}, nil
	}))

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keys := jwtutil.NewStaticKeySource(&key.PublicKey)
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: h1User, Groups: []string{"devs"}, Duration: time.Hour, KeyID: "cd-kid", PrivateKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The cell already in L1 for this caller's cohort.
	reqCtx := h1ReqCtx(h1User)
	k, h, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if h == nil || k == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("setup: no live key for the cohort")
	}
	w := []byte(`{"warm":"cd-cell"}` + "\n")
	putFixture(h, k, &cache.ResolvedEntry{RawJSON: w, Inputs: inputs})

	var resolves int
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			resolves++
			return &templatesv1.RESTAction{}, nil
		})
	t.Cleanup(restore)

	auth := middleware.UserConfig(keys, cdAuthnNS)
	serve = func(order cdOrder) cdRun {
		resolves = 0
		dispatch := RESTAction()
		var hdl http.Handler
		switch order {
		case cdAuthThenDispatch:
			hdl = auth(dispatch)
		case cdDispatchThenAuth:
			// UserConfig downstream of the dispatcher: the dispatcher sees the
			// request with only the identity a bare context carries.
			hdl = http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
				dispatch.ServeHTTP(wr, r.WithContext(h1ReqCtx(h1User)))
			})
		case cdHitBeforeAuth:
			// A hit is answered from L1 on the token's identity alone, before
			// the credential lookup runs.
			hdl = http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
				if e, ok := h.Get(k); ok {
					_, _ = wr.Write(e.RawJSON)
					return
				}
				auth(dispatch).ServeHTTP(wr, r)
			})
		}
		r := httptest.NewRequest(http.MethodGet, "/call", nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		hdl.ServeHTTP(rec, r)
		b, _ := io.ReadAll(rec.Result().Body)
		return cdRun{code: rec.Code, body: string(b), resolves: resolves}
	}
	deleteSecret = func() {
		if err := cli.CoreV1().Secrets(cdAuthnNS).Delete(ictx, sec.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := cache.SecretsSnapshotLoad().ByName[sec.Name]; !ok {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("the informer snapshot still holds the deleted Secret")
	}
	return serve, deleteSecret, k, h, w
}

func TestCredentialDeleted_AfterSuccess_ReadAndWriteGet401_NeverCache(t *testing.T) {
	serve, deleteSecret, key, handle, warm := cdSetup(t)

	// CONTROL: credential present -> the production chain serves the cell.
	ctl := serve(cdAuthThenDispatch)
	if ctl.code != 200 || !bytes.Equal([]byte(ctl.body), warm) || ctl.resolves != 0 {
		t.Fatalf("CONTROL: with the credential present want 200, the warm cell, no resolve; got code=%d resolves=%d body=%q",
			ctl.code, ctl.resolves, ctl.body)
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("CONTROL: the cell must be in L1")
	}

	deleteSecret()

	// The production order: 401, no cell, no resolve.
	got := serve(cdAuthThenDispatch)
	if got.code != http.StatusUnauthorized || got.resolves != 0 ||
		strings.Contains(got.body, "cd-cell") || !strings.Contains(got.body, middleware.CauseSessionCredentialMissing) {
		t.Fatalf("credential gone: want 401 %s with no cell and no resolve; got code=%d resolves=%d body=%q",
			middleware.CauseSessionCredentialMissing, got.code, got.resolves, got.body)
	}

	// MUTANTS: each broken order must be visible to the assertion above.
	detect := func(r cdRun) bool {
		return !(r.code == http.StatusUnauthorized && r.resolves == 0 &&
			!strings.Contains(r.body, "cd-cell") && strings.Contains(r.body, middleware.CauseSessionCredentialMissing))
	}
	for name, order := range map[string]cdOrder{"lookup-after-dispatcher": cdDispatchThenAuth, "hit-before-lookup": cdHitBeforeAuth} {
		m := serve(order)
		if !detect(m) {
			t.Errorf("mutant %s NOT detected: code=%d body=%q", name, m.code, m.body)
		} else {
			t.Logf("mutant %s detected: code=%d body=%q", name, m.code, strings.TrimSpace(m.body))
		}
	}
}
