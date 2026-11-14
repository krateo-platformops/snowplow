package main

// main_audit_e2e_test.go — #1379 re-review, ruling 1: the audited routes'
// records end to end, through the REAL route table (mountCallWriteRoutes,
// mountExportRoutes, routeTable.HandleAudited with audit.OpenRecord), the REAL
// request chain (newRequestChain) and the REAL login middleware
// (middleware.UserConfig, a test signing key, the Secrets informer holding
// the caller's clientconfig).
//
//   R  a body the decoder refuses (POST /export) and a malformed /call query
//      are each exactly ONE denied invalid_request row;
//   S  a successful export (GET and POST) and a successful /call write are
//      each exactly ONE row: the handler's, never a second from the fallback;
//   U  an unverified request (no token) is ZERO rows;
//   P  a handler that panics after its dial is exactly one row saying panic;
//   D  the falsifier: the design-error counter (an audited exit with no
//      handler record that is not a 4xx) never moves on these routes, except
//      in P, where it must.
//
// These arms fail if the route table mounts without OpenRecord, if UserConfig
// does not call SetCaller, or if the export emits through the plain emit.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

const (
	e2eUser    = "e2e-dora"
	e2eNS      = "krateo-system"
	e2eKID     = "kid-e2e-1379"
	e2eCMQuery = "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"
	e2eCM      = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default"},"data":{"k":"v"}}`
	e2eCreated = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default","uid":"u1","generation":1,"resourceVersion":"1"}}`
	e2eList    = `{"apiVersion":"v1","kind":"ConfigMapList","metadata":{},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}}]}`
)

type e2eNamespaces struct{}

func (e2eNamespaces) NamespaceLabels(context.Context, string) (map[string]string, bool, error) {
	return map[string]string{}, true, nil
}

// e2eEnv is the pod's audited routes in front of a fake apiserver, with the
// caller's clientconfig in the Secrets informer, and a recorder on the audit
// pipeline. It returns the server's handler, a token for the caller, and the
// recorder.
func e2eEnv(t *testing.T, extra func(rt *routeTable, chain use.Chain, login func(http.Handler) http.Handler)) (http.Handler, string, *audittest.Recorder) {
	t.Helper()
	return e2eEnvWith(t, e2eLogin{groups: []string{"devs"}, seed: []string{credsecret.SessionNames(e2eUser)[0]}}, extra)
}

// e2eLogin is the caller's login state: the token's groups, the credential
// Secrets the informer holds, and how the apiserver answers a re-read of a
// Secret the informer does not hold (404 by default).
type e2eLogin struct {
	groups []string
	seed   []string
	// owner is the user the seeded Secrets belong to, stamped as authn
	// writes it (endpoints.UsernameAnnotation, #1382); "" is the caller.
	owner        string
	secretStatus int
	// spentBudget gives the request an already-spent page budget, so the
	// credential re-read is never sent (504).
	spentBudget bool
}

func e2eEnvWith(t *testing.T, login e2eLogin, extra func(rt *routeTable, chain use.Chain, login func(http.Handler) http.Handler)) (http.Handler, string, *audittest.Recorder) {
	t.Helper()
	if login.secretStatus == 0 {
		login.secretStatus = http.StatusNotFound
	}
	if login.owner == "" {
		login.owner = e2eUser
	}
	if login.spentBudget {
		t.Cleanup(middleware.SetPageBudgetArrivalForTest(func() time.Time { return time.Now().Add(-time.Hour) }))
	}
	var apiURL string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		if i := strings.Index(r.URL.Path, "/secrets/"); i >= 0 {
			// The apiserver's re-read of a credential Secret: the seeded
			// one, as the informer holds it, else the configured failure.
			name := r.URL.Path[i+len("/secrets/"):]
			if login.secretStatus == http.StatusNotFound && slices.Contains(login.seed, name) {
				out := e2eSecret(name, apiURL, login.owner)
				out.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
				_ = json.NewEncoder(w).Encode(out)
				return
			}
			w.WriteHeader(login.secretStatus)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":`+strconv.Itoa(login.secretStatus)+`}`)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if strings.HasSuffix(r.URL.Path, "/cm-x") {
				_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x"},"items":[{"id":"a"},{"id":"b"}]}`)
				return
			}
			_, _ = io.WriteString(w, e2eList)
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, e2eCreated)
		}
	}))
	t.Cleanup(api.Close)
	apiURL = api.URL

	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	cli := fake.NewSimpleClientset()
	for _, name := range login.seed {
		sec := e2eSecret(name, api.URL, login.owner)
		if _, err := cli.CoreV1().Secrets(e2eNS).Create(context.Background(), sec, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	// The UserConfig re-read of a Secret the informer does not hold goes to
	// the fake apiserver.
	t.Cleanup(credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: api.URL, BearerToken: "tok-sa"}, nil
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, e2eNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{Username: e2eUser, Groups: login.groups,
		Duration: time.Hour, KeyID: e2eKID, PrivateKey: key, Principal: jwtutil.PrincipalHuman})
	if err != nil {
		t.Fatal(err)
	}

	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	audit.SetNamespaceReader(e2eNamespaces{})

	mux := http.NewServeMux()
	rt := newRouteTable(mux)
	chain := newRequestChain(slog.New(slog.DiscardHandler))
	userConfig := middleware.UserConfig(jwtutil.NewStaticKeySource(&key.PublicKey), e2eNS)
	mountCallWriteRoutes(rt, chain, userConfig)
	mountExportRoutes(rt, chain, userConfig)
	if extra != nil {
		extra(rt, chain, userConfig)
	}
	return mux, tok, rec
}

// e2eSecret is a credential Secret as authn writes it: its owner stamped in
// endpoints.UsernameAnnotation (#1382), which every reader checks.
func e2eSecret(name, serverURL, owner string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e2eNS, Name: name,
		Annotations: map[string]string{endpoints.UsernameAnnotation: owner}},
		Data: map[string][]byte{"server-url": []byte(serverURL), "token": []byte("tok")}}
}

func e2eDo(h http.Handler, tok, method, target, body string) int {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, target, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

func e2eRow(t *testing.T, recs []sdklog.Record) (map[string]string, string) {
	t.Helper()
	if len(recs) != 1 {
		t.Fatalf("%d rows, want exactly 1", len(recs))
	}
	a := map[string]string{}
	for _, kv := range audittest.Attributes(recs[0]) {
		a[string(kv.Key)] = kv.Value.Emit()
	}
	return a, recs[0].Body().AsString()
}

func TestAudit_E2E_R_S_U_D_EveryAuditedRouteClassThroughTheRealTableAndLogin(t *testing.T) {
	for _, c := range []struct {
		name           string
		verified       bool
		method, target string
		body           string
		code           int
		rows           int
		outcome        string
		reason         string
		action         string
	}{
		{"R: POST /export, a body the decoder refuses", true, http.MethodPost, "/export?format=json&" + e2eCMQuery, `{"extras": "not an object"}`,
			http.StatusBadRequest, 1, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, "export"},
		{"R: PUT /call, a malformed query", true, http.MethodPut, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm-x/status"), e2eCM,
			http.StatusBadRequest, 1, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, "call"},
		{"S: GET /export", true, http.MethodGet, "/export?format=json&" + e2eCMQuery, "",
			http.StatusOK, 1, krateoaudit.OutcomeSuccess, "", "export"},
		{"S: POST /export", true, http.MethodPost, "/export?format=json&" + e2eCMQuery, `{"extras":{}}`,
			http.StatusOK, 1, krateoaudit.OutcomeSuccess, "", "export"},
		{"S: POST /call", true, http.MethodPost, "/call?" + e2eCMQuery, e2eCM,
			http.StatusOK, 1, krateoaudit.OutcomeSuccess, "", "call"},
		{"S: POST /call/dry-run", true, http.MethodPost, "/call/dry-run?" + e2eCMQuery, e2eCM,
			http.StatusOK, 1, krateoaudit.OutcomeSuccess, "", "call.dryRun"},
		{"U: POST /call, no token", false, http.MethodPost, "/call?" + e2eCMQuery, e2eCM,
			http.StatusUnauthorized, 0, "", "", ""},
		{"U: GET /export, no token", false, http.MethodGet, "/export?format=json&" + e2eCMQuery, "",
			http.StatusUnauthorized, 0, "", "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, tok, rec := e2eEnv(t, nil)
			if !c.verified {
				tok = ""
			}
			before := audit.DesignErrorCount("answered")
			if code := e2eDo(h, tok, c.method, c.target, c.body); code != c.code {
				t.Fatalf("answered %d, want %d", code, c.code)
			}
			// D — the fallback never fires on a non-4xx exit of a real route.
			if d := audit.DesignErrorCount("answered") - before; d != 0 {
				t.Errorf("an exit was answered without its handler recording the outcome (%d): a design error", d)
			}
			recs := rec.Records()
			if c.rows == 0 {
				if len(recs) != 0 {
					t.Fatalf("%d rows for an unverified request, want 0", len(recs))
				}
				return
			}
			a, _ := e2eRow(t, recs)
			if a[krateoaudit.KeyOutcome] != c.outcome || a[krateoaudit.KeyDenialReason] != c.reason || a[krateoaudit.KeyAction] != c.action {
				t.Errorf("action %q outcome %q reason %q, want %s %s %q", a[krateoaudit.KeyAction], a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason], c.action, c.outcome, c.reason)
			}
			if a[krateoaudit.KeyUserName] != e2eUser {
				t.Errorf("user.name %q: the row does not name the caller the login verified", a[krateoaudit.KeyUserName])
			}
			opts := []audittest.Option{}
			if _, named := a[krateoaudit.KeyObjectResource]; !named {
				opts = append(opts, audittest.AllowIncomplete(krateoaudit.RuleTarget))
			}
			audittest.AssertConformant(t, audit.Writer, recs[0], opts...)
			audittest.AssertRequiredKeysDetected(t, audit.Writer, recs[0], opts...)
			audittest.AssertEnumsDetected(t, audit.Writer, recs[0], opts...)
		})
	}
}

func TestAudit_E2E_P_APanicAfterTheDialIsOneRowSayingPanic(t *testing.T) {
	var apiURL string
	h, tok, rec := e2eEnv(t, func(rt *routeTable, chain use.Chain, login func(http.Handler) http.Handler) {
		rt.HandleAudited("POST /e2e-panic", handlers.CallRoute(), chain.Append(login).Then(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The dial happens; the handler then panics before
				// recording its outcome.
				if resp, err := http.Get(apiURL); err == nil {
					_ = resp.Body.Close()
				}
				panic("after the dial")
			})))
	})
	apiURL = "http://127.0.0.1:1" // nothing listens: the dial is attempted, its answer irrelevant
	before := audit.DesignErrorCount("panic")
	func() {
		defer func() {
			if p := recover(); p != "after the dial" {
				t.Errorf("the panic was not re-raised as it was (got %v)", p)
			}
		}()
		e2eDo(h, tok, http.MethodPost, "/e2e-panic?"+e2eCMQuery, e2eCM)
		t.Errorf("the handler's panic did not propagate")
	}()
	a, body := e2eRow(t, rec.Records())
	if a[krateoaudit.KeyOutcome] != krateoaudit.OutcomeFailure || body != audit.MessagePanic {
		t.Errorf("outcome %q body %q, want failure %q", a[krateoaudit.KeyOutcome], body, audit.MessagePanic)
	}
	if strings.Contains(body, "nothing was sent") {
		t.Errorf("the panic row says nothing was sent")
	}
	if d := audit.DesignErrorCount("panic") - before; d != 1 {
		t.Errorf("snowplow_audit_unrecorded_exit_total{panic} moved by %d, want 1", d)
	}
}

// #1379 re-review — the login middleware's own refusals of a VERIFIED caller,
// through the real route table and the real UserConfig: a token whose
// session Secret is missing, and a krateo:loa:2 token whose step-up Secret is
// missing, are each exactly one denied row with the credential reason, no
// status code; a credential read the apiserver refuses (403, 500, 429), or
// one the page budget never sends (504), is one failure row with NO status
// code (neither snowplow's own code nor its ServiceAccount's read is an
// answer of the authority the record names) and the credential body. None
// moves the design-error counter.
func TestAudit_E2E_C_TheLoginsOwnRefusalsAreOneRowEach(t *testing.T) {
	stepUp := []string{"devs", credsecret.StepUpGroup}
	for _, login := range []struct {
		name    string
		login   e2eLogin
		code    int
		outcome string
		reason  string
		body    string
	}{
		{"no session Secret", e2eLogin{groups: []string{"devs"}},
			http.StatusUnauthorized, krateoaudit.OutcomeDenied, audit.DenialSessionCredentialMissing, audit.MessageRefused},
		{"a krateo:loa:2 token with no step-up Secret", e2eLogin{groups: stepUp, seed: []string{credsecret.SessionNames(e2eUser)[0]}},
			http.StatusUnauthorized, krateoaudit.OutcomeDenied, audit.DenialStepUpRequired, audit.MessageRefused},
		// #1385: a Secret under the caller's name that belongs to another
		// user is a miss at every read site, so the caller's credential is
		// missing.
		{"the Secret under the caller's name is another user's", e2eLogin{groups: []string{"devs"}, seed: []string{credsecret.SessionNames(e2eUser)[0]}, owner: "e2e-mallory"},
			http.StatusUnauthorized, krateoaudit.OutcomeDenied, audit.DenialSessionCredentialMissing, audit.MessageRefused},
		{"the credential read refused (403)", e2eLogin{groups: []string{"devs"}, secretStatus: http.StatusForbidden},
			http.StatusInternalServerError, krateoaudit.OutcomeFailure, "", audit.MessageCredentialUnreadable},
		{"the credential read failed (500)", e2eLogin{groups: []string{"devs"}, secretStatus: http.StatusInternalServerError},
			http.StatusInternalServerError, krateoaudit.OutcomeFailure, "", audit.MessageCredentialUnreadable},
		{"the credential read throttled (429)", e2eLogin{groups: []string{"devs"}, secretStatus: http.StatusTooManyRequests},
			http.StatusInternalServerError, krateoaudit.OutcomeFailure, "", audit.MessageCredentialUnreadable},
		{"the credential re-read never sent (page budget, 504)", e2eLogin{groups: []string{"devs"}, spentBudget: true},
			http.StatusGatewayTimeout, krateoaudit.OutcomeFailure, "", audit.MessageCredentialUnreadable},
	} {
		for _, route := range []struct {
			name, method, target, body, action string
		}{
			{"POST /call", http.MethodPost, "/call?" + e2eCMQuery, e2eCM, "call"},
			{"GET /export", http.MethodGet, "/export?format=json&" + e2eCMQuery, "", "export"},
		} {
			t.Run(login.name+", "+route.name, func(t *testing.T) {
				h, tok, rec := e2eEnvWith(t, login.login, nil)
				before := audit.DesignErrorCount("answered")
				if code := e2eDo(h, tok, route.method, route.target, route.body); code != login.code {
					t.Fatalf("answered %d, want %d", code, login.code)
				}
				if d := audit.DesignErrorCount("answered") - before; d != 0 {
					t.Errorf("the design-error counter moved by %d on a legitimate refusal or failure", d)
				}
				a, body := e2eRow(t, rec.Records())
				if a[krateoaudit.KeyOutcome] != login.outcome || a[krateoaudit.KeyDenialReason] != login.reason || a[krateoaudit.KeyAction] != route.action {
					t.Errorf("action %q outcome %q reason %q, want %s %s %q", a[krateoaudit.KeyAction], a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason], route.action, login.outcome, login.reason)
				}
				if got, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
					t.Errorf("http.response.status_code %q: no authority the record names answered", got)
				}
				if body != login.body {
					t.Errorf("body %q, want %q", body, login.body)
				}
				if a[krateoaudit.KeyUserName] != e2eUser {
					t.Errorf("user.name %q: the row does not name the verified caller", a[krateoaudit.KeyUserName])
				}
				for k, v := range a {
					if strings.Contains(v, "-clientconfig") {
						t.Errorf("%s names the credential Secret", k)
					}
				}
				opts := []audittest.Option{}
				if _, named := a[krateoaudit.KeyObjectResource]; !named {
					opts = append(opts, audittest.AllowIncomplete(krateoaudit.RuleTarget))
				}
				audittest.AssertConformant(t, audit.Writer, rec.Records()[0], opts...)
				audittest.AssertRequiredKeysDetected(t, audit.Writer, rec.Records()[0], opts...)
				audittest.AssertEnumsDetected(t, audit.Writer, rec.Records()[0], opts...)
			})
		}
	}
}
