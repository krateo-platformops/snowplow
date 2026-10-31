package api

// stepup_credential_994_test.go — #994: the step-up credential Secret
// `<user>-clientconfig-stepup` (authn#49) in the RESTAction resolver.
//
//   N  the nil-ref stage (no endpointRef: the caller's OWN credential) dials the
//      step-up Secret for an identity whose groups carry krateo:loa:2, and the
//      session Secret otherwise — through the REAL api.Resolve, writes included
//      (the step-up credential exists for writes acmp's admission policies
//      guard). A missing step-up Secret fails the stage; the session credential
//      is never dialed in its place.
//   G  a request-TEMPLATED endpointRef resolving to `x-clientconfig-stepup` is
//      refused in resolveOne, so a caller can never dial another user's
//      ELEVATED credential. Since #1025 the same rule refuses a LITERAL one in
//      a stored RESTAction too (issue1025_per_user_ref_test.go).
//   C  #443: a caller-supplied (inline draft) literal `x-clientconfig-stepup`
//      ref is refused before any Secret read.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

const (
	su994User     = "su994-erin"
	su994Group    = "su994-team"
	su994NS       = "krateo-system"
	su994TokSess  = "TOK-SESSION-994"
	su994TokUp    = "TOK-STEPUP-994"
	su994Forged   = "https://STEPUP-CREDS-FORGED.invalid"
	su994WritePth = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"
)

// su994Dial is one request the fake apiserver received.
type su994Dial struct{ method, path, bearer string }

// su994Upstream records every request with the bearer it carried. Secret GETs
// (the resolver's apiserver re-read of a credential Secret on an informer miss)
// answer NotFound; everything else succeeds.
func su994Upstream(t *testing.T) (*httptest.Server, func() []su994Dial) {
	t.Helper()
	var mu sync.Mutex
	var got []su994Dial
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		got = append(got, su994Dial{r.Method, r.URL.Path, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/secrets/") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []su994Dial {
		mu.Lock()
		defer mu.Unlock()
		return append([]su994Dial(nil), got...)
	}
}

// su994Secrets publishes the given credential Secrets (name → bearer), all
// pointing at apiURL, as the informer snapshot a /call resolve reads.
func su994Secrets(t *testing.T, apiURL string, byName map[string]string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true) // keep the seeded server-url
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	snap := map[string]*corev1.Secret{}
	for name, tok := range byName {
		snap[name] = mkSecret(su994NS, name, map[string]string{"server-url": apiURL, "token": tok})
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: snap})
	cache.ForceSecretsCacheReadyForTest(su994NS)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
}

func su994Stage(id, verb, path string) *templates.API {
	return &templates.API{Name: id, Path: path, Verb: ptr.To(verb), ContinueOnError: ptr.To(true), ErrorKey: ptr.To(id + "Err")}
}

func su994Resolve(ctx context.Context, apiURL string) map[string]any {
	return Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: apiURL}, AuthnNS: su994NS,
		RESTActionNamespace: su994NS, RESTActionName: "ra994",
		Items: []*templates.API{
			su994Stage("read", http.MethodGet, su994WritePth+"/demo"),
			su994Stage("create", http.MethodPost, su994WritePth),
		}})
}

func su994Bearers(dials []su994Dial) map[string]int {
	out := map[string]int{}
	for _, d := range dials {
		if !strings.Contains(d.path, "/secrets/") {
			out[d.bearer]++
		}
	}
	return out
}

// N — the nil-ref stage dials the caller's own credential, step-up or session
// by the identity's groups. Both Secrets exist.
func TestStepUp994_N_NilRefDialsTheIdentitysOwnCredential(t *testing.T) {
	api, dials := su994Upstream(t)
	su994Secrets(t, api.URL, map[string]string{
		credsecret.SessionName(su994User): su994TokSess,
		credsecret.StepUpName(su994User):  su994TokUp,
	})

	su994Resolve(ctxWithUser(su994User, su994Group, credsecret.StepUpGroup), api.URL)
	up := su994Bearers(dials())
	if up[su994TokUp] != 2 || up[su994TokSess] != 0 || len(up) != 1 {
		t.Fatalf("a step-up identity's read AND write must both dial the step-up credential only; bearers=%v", up)
	}

	n := len(dials())
	su994Resolve(ctxWithUser(su994User, su994Group), api.URL)
	plain := su994Bearers(dials()[n:])
	if plain[su994TokSess] != 2 || plain[su994TokUp] != 0 || len(plain) != 1 {
		t.Fatalf("an identity without %s must dial the session credential only; bearers=%v", credsecret.StepUpGroup, plain)
	}
}

// N — a step-up identity whose step-up Secret is gone (absent from the informer,
// NotFound on the apiserver re-read) fails its stages. The session Secret is
// right there; it is never dialed in its place.
func TestStepUp994_N_MissingStepUpSecretFailsNoSessionFallback(t *testing.T) {
	api, dials := su994Upstream(t)
	su994Secrets(t, api.URL, map[string]string{credsecret.SessionName(su994User): su994TokSess})

	su994Resolve(ctxWithUser(su994User, su994Group, credsecret.StepUpGroup), api.URL)
	got := dials()
	if b := su994Bearers(got); len(b) != 0 {
		t.Fatalf("NO-FALLBACK VIOLATED: a step-up identity without its step-up Secret dialed %v (session bearer %q)", b, su994TokSess)
	}
	var reread []string
	for _, d := range got {
		if strings.Contains(d.path, "/secrets/") {
			reread = append(reread, d.path[strings.LastIndex(d.path, "/")+1:])
		}
	}
	if len(reread) == 0 {
		t.Fatal("PRE: the informer miss must re-read the Secret from the apiserver")
	}
	for _, name := range reread {
		if name != credsecret.StepUpName(su994User) {
			t.Fatalf("the apiserver re-read asked for %q; only the step-up Secret may be read (reads=%v)", name, reread)
		}
	}
}

// su994SeedForgeable publishes `victim-clientconfig-stepup` carrying a SENTINEL
// server-url: if a lookup for it ever HITS, the sentinel comes back.
func su994SeedForgeable(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	name := credsecret.StepUpName("victim")
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		name: mkSecret(su994NS, name, map[string]string{"server-url": su994Forged}),
	}})
	cache.ForceSecretsCacheReadyForTest(su994NS)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
}

// G — the eval site renders and normalises a templated name; it does not
// judge it. resolveOne refuses the per-user name it produces (#1025: one rule,
// at the choke point).
func TestStepUp994_G_TemplatedStepUpRefRefusedAtResolveOne(t *testing.T) {
	su994SeedForgeable(t)
	r := &resolveRun{dict: map[string]any{"want": "victim"}}
	ref, templated := r.evalEndpointRef(&templates.Reference{Name: `${ .want + "-clientconfig-stepup" }`, Namespace: su994NS})
	if !templated || ref == nil || ref.Name != credsecret.StepUpName("victim") {
		t.Fatalf("the eval site must render the templated name; got ref=%+v templated=%v", ref, templated)
	}
	m := &endpointReferenceMapper{authnNS: su994NS, username: "attacker"}
	ep, _, err := m.resolveOne(context.Background(), ref)
	if !errors.Is(err, ErrPerUserCredentialRef) || ep.ServerURL == su994Forged {
		t.Fatalf("a templated ref to another user's step-up Secret was resolved (ep=%q err=%v) — "+
			"the caller dials that user's ELEVATED credential", ep.ServerURL, err)
	}
	// Control: the same template to a non-credential name renders and is not refused.
	ok, _ := r.evalEndpointRef(&templates.Reference{Name: `${ .want + "-stepup-endpoint" }`, Namespace: su994NS})
	if ok == nil || ok.Name != "victim-stepup-endpoint" {
		t.Fatalf("control: a non-credential templated name must render; got %+v", ok)
	}
}

// C — a caller-supplied (inline draft) literal `x-clientconfig-stepup` ref is
// refused before any Secret read, like a literal `x-clientconfig` one.
func TestStepUp994_C_CallerSuppliedLiteralStepUpRefRefused(t *testing.T) {
	var mu sync.Mutex
	var read []string
	prev := fromSecretAsCallerFn
	fromSecretAsCallerFn = func(_ context.Context, _ *rest.Config, name, _ string) (endpoints.Endpoint, error) {
		mu.Lock()
		read = append(read, name)
		mu.Unlock()
		return endpoints.Endpoint{ServerURL: su994Forged}, nil
	}
	t.Cleanup(func() { fromSecretAsCallerFn = prev })

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://caller.su994.example", Token: "caller"}))
	m := &endpointReferenceMapper{authnNS: su994NS, username: "attacker", provenance: ProvenanceCallerSupplied}

	for _, name := range []string{"x-clientconfig-stepup", credsecret.StepUpName("victim"), "x-clientconfig"} {
		ep, _, err := m.resolveOne(ctx, &templates.Reference{Name: name, Namespace: su994NS})
		if err == nil || ep.ServerURL == su994Forged {
			t.Fatalf("#443 VIOLATED: a caller-supplied literal ref %q was resolved (ep=%q err=%v)", name, ep.ServerURL, err)
		}
		if !errors.Is(err, ErrPerUserCredentialRef) {
			t.Fatalf("want the per-user credential refusal for %q; got %v", name, err)
		}
	}
	mu.Lock()
	n := len(read)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("a refused caller-supplied ref must never be read; reads=%v", read)
	}
	// Control: a non-credential literal ref IS read as the caller (the seam is live).
	if _, _, err := m.resolveOne(ctx, &templates.Reference{Name: "render-endpoint", Namespace: su994NS}); err != nil {
		t.Fatalf("control: a non-credential caller-supplied ref must be read; got %v", err)
	}
	if len(read) != 1 || read[0] != "render-endpoint" {
		t.Fatalf("control: want one read of render-endpoint, got %v", read)
	}
}
