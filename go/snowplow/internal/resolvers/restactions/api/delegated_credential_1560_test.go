package api

// delegated_credential_1560_test.go — #1560: a RESTAction stage that dials the
// caller's OWN credential (no endpointRef) under a token that carries `act`
// dials the DELEGATED credential (authn#167), exactly as the auth middleware
// chose it: credsecret.Select is the one selector. Every arm drives the REAL
// api.Resolve, so the credential is observed where it matters, on the wire.
//
//   S1  a delegated token's read and write stages dial the delegated
//       credential; the person's session Secret is in the snapshot and is
//       never dialed. (The mutant "the mapper drops Act", resolve.go, goes RED
//       here: the stage would dial the session credential.)
//   S2  the same person without `act` dials the session credential, as
//       before.
//   S3  an informer miss reads the delegated Secret from the apiserver: one
//       GET, the delegated name only.
//   S4  a stale informer copy (a re-exchange replaced it) is re-read from the
//       apiserver once and the fresh credential is dialed; when the apiserver
//       copy is stale too, nothing is dialed.
//   R   #1025: a named endpointRef to a delegated Secret is refused, stored or
//       caller-supplied, before anything is read or dialed.

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
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/testsupport/delegatedcred"
)

const (
	d1560User     = "d1560-frida"
	d1560Group    = "d1560-team"
	d1560Actor    = "d1560-agent"
	d1560TokSess  = "TOK-SESSION-1560"
	d1560TokDeleg = "TOK-DELEGATED-1560"
	d1560TokFresh = "TOK-DELEGATED-FRESH-1560"
)

// d1560Upstream is a fake apiserver. It serves the Secrets in its map on a
// Secret GET (NotFound for any other name) and records the name asked for;
// every other request succeeds and is recorded with its bearer.
type d1560Upstream struct {
	srv     *httptest.Server
	mu      sync.Mutex
	secrets map[string]*corev1.Secret
	gets    []string
	dials   []su994Dial
}

func newD1560Upstream(t *testing.T) *d1560Upstream {
	t.Helper()
	u := &d1560Upstream{secrets: map[string]*corev1.Secret{}}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		u.mu.Lock()
		defer u.mu.Unlock()
		if i := strings.Index(r.URL.Path, "/secrets/"); i >= 0 {
			name := r.URL.Path[i+len("/secrets/"):]
			u.gets = append(u.gets, name)
			s, ok := u.secrets[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
				return
			}
			out := s.DeepCopy()
			out.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		u.dials = append(u.dials, su994Dial{r.Method, r.URL.Path, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *d1560Upstream) serve(secs ...*corev1.Secret) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, s := range secs {
		u.secrets[s.Name] = s
	}
}

func (u *d1560Upstream) record() (gets []string, bearers map[string]int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.gets...), su994Bearers(u.dials)
}

// d1560Snapshot publishes secs, each through the REAL informer transform
// (#510), as the Secrets snapshot a resolve reads.
func d1560Snapshot(t *testing.T, secs ...*corev1.Secret) {
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
	for _, s := range secs {
		out, err := cache.SecretsCacheTransformForTest(s.DeepCopy())
		if err != nil {
			t.Fatal(err)
		}
		snap[s.Name] = out.(*corev1.Secret)
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: snap})
	cache.ForceSecretsCacheReadyForTest(su994NS)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
}

// d1560Session is the person's session Secret, served at url with bearer tok.
func d1560Session(url string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: authnMeta(su994NS, credsecret.SessionNames(d1560User)[0], d1560User),
		Data:       map[string][]byte{"server-url": []byte(url), "token": []byte(d1560TokSess)},
	}
}

// d1560Delegated is the person's delegated credential for d1560Actor (no
// labels, as authn writes it), its certificate carrying groups and valid from
// nb to na (zero: valid now).
func d1560Delegated(t *testing.T, url, tok string, groups []string, nb, na time.Time) *corev1.Secret {
	t.Helper()
	return delegatedcred.Secret(t, delegatedcred.Spec{Namespace: su994NS, Username: d1560User, Actor: d1560Actor,
		Groups: groups, NotBefore: nb, NotAfter: na, ServerURL: url, Token: tok})
}

// d1560Ctx is a verified request context for the person, delegated through
// d1560Actor when delegated.
func d1560Ctx(delegated bool) context.Context {
	ui := jwtutil.UserInfo{Username: d1560User, Groups: []string{d1560Group}}
	if delegated {
		ui.Act = &jwtutil.Actor{Sub: d1560Actor, Principal: "agent"}
	}
	return xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(ui))
}

func d1560Resolve(ctx context.Context, apiURL string, stages ...*templates.API) string {
	ctx, outcomes := WithStageOutcomes(ctx)
	Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: apiURL}, AuthnNS: su994NS,
		RESTActionNamespace: su994NS, RESTActionName: "ra1560", Items: stages})
	return outcomes.HeaderValue()
}

func d1560Read() *templates.API {
	return &templates.API{Name: "read", Path: su994WritePth + "/demo", Verb: ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("readErr")}
}

// S1, S2 — the stage dials the credential the token selects.
func TestStage1560_S1S2_TheStageDialsTheCredentialTheTokenSelects(t *testing.T) {
	up := newD1560Upstream(t)
	d1560Snapshot(t, d1560Session(up.srv.URL), d1560Delegated(t, up.srv.URL, d1560TokDeleg, []string{d1560Group}, time.Time{}, time.Time{}))

	d1560Resolve(d1560Ctx(true), up.srv.URL, d1560Read(), su994Stage("create", http.MethodPost, su994WritePth))
	gets, b := up.record()
	if b[d1560TokDeleg] != 2 || len(b) != 1 {
		t.Fatalf("S1: a delegated token's read AND write stages must dial the delegated credential only; bearers=%v", b)
	}
	if len(gets) != 0 {
		t.Fatalf("S1: an informer hit must not GET a Secret; gets=%v", gets)
	}

	up2 := newD1560Upstream(t)
	d1560Snapshot(t, d1560Session(up2.srv.URL), d1560Delegated(t, up2.srv.URL, d1560TokDeleg, []string{d1560Group}, time.Time{}, time.Time{}))
	d1560Resolve(d1560Ctx(false), up2.srv.URL, d1560Read(), su994Stage("create", http.MethodPost, su994WritePth))
	if _, b := up2.record(); b[d1560TokSess] != 2 || len(b) != 1 {
		t.Fatalf("S2: a token without act must dial the session credential only, as before; bearers=%v", b)
	}
}

// S3 — an informer miss reads the delegated Secret from the apiserver: exactly
// one GET, of the delegated name. The session Secret is in the snapshot and is
// not used in its place.
func TestStage1560_S3_InformerMissReadsTheDelegatedSecretOnce(t *testing.T) {
	up := newD1560Upstream(t)
	d1560Snapshot(t, d1560Session(up.srv.URL))
	up.serve(d1560Delegated(t, up.srv.URL, d1560TokDeleg, []string{d1560Group}, time.Time{}, time.Time{}))

	d1560Resolve(d1560Ctx(true), up.srv.URL, d1560Read())
	gets, b := up.record()
	if b[d1560TokDeleg] != 1 || len(b) != 1 {
		t.Fatalf("the stage must dial the delegated credential read from the apiserver; bearers=%v", b)
	}
	if want := endpoints.DelegatedSecretName(d1560User, d1560Actor); len(gets) != 1 || gets[0] != want {
		t.Fatalf("want exactly one GET of the delegated name; gets=%v", gets)
	}
}

// S4 — a stale informer copy is re-read once. Fresh on the apiserver: the
// fresh credential is dialed. Stale there too, or gone: nothing is dialed, and
// the session credential is never dialed in its place.
func TestStage1560_S4_StaleInformerCopyIsReReadOnce(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name      string
		stale     func(url string) *corev1.Secret
		apiserver func(url string) *corev1.Secret // nil: absent there
		want      string                          // "" : nothing dialed
	}{
		{"stale groups, fresh on the apiserver",
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokDeleg, []string{"d1560-other"}, time.Time{}, time.Time{})
			},
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokFresh, []string{d1560Group}, time.Time{}, time.Time{})
			},
			d1560TokFresh},
		{"expired, fresh on the apiserver",
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokDeleg, []string{d1560Group}, now.Add(-20*time.Minute), now.Add(-10*time.Minute))
			},
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokFresh, []string{d1560Group}, time.Time{}, time.Time{})
			},
			d1560TokFresh},
		{"stale in both",
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokDeleg, []string{"d1560-other"}, time.Time{}, time.Time{})
			},
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokFresh, []string{"d1560-other"}, time.Time{}, time.Time{})
			},
			""},
		{"stale in the informer, gone from the apiserver",
			func(url string) *corev1.Secret {
				return d1560Delegated(t, url, d1560TokDeleg, []string{"d1560-other"}, time.Time{}, time.Time{})
			},
			nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newD1560Upstream(t)
			d1560Snapshot(t, d1560Session(up.srv.URL), tc.stale(up.srv.URL))
			if tc.apiserver != nil {
				up.serve(tc.apiserver(up.srv.URL))
			}
			d1560Resolve(d1560Ctx(true), up.srv.URL, d1560Read())
			gets, b := up.record()
			if tc.want != "" && (b[tc.want] != 1 || len(b) != 1) {
				t.Fatalf("want the apiserver's fresh credential dialed once; bearers=%v", b)
			}
			if tc.want == "" && len(b) != 0 {
				t.Fatalf("a stale delegated credential must dial nothing (never the session's %s); bearers=%v", d1560TokSess, b)
			}
			if want := endpoints.DelegatedSecretName(d1560User, d1560Actor); len(gets) != 1 || gets[0] != want {
				t.Fatalf("the stale hit must be re-read exactly once, for the delegated name only; gets=%v", gets)
			}
		})
	}
}

// R — #1025: a named endpointRef to a delegated Secret is refused for every
// provenance, through the REAL resolve: nothing is read or dialed, the
// refusal is logged and counted, and the name never reaches the log.
func TestStage1560_R_NamedRefToADelegatedSecretIsRefused(t *testing.T) {
	victimDelegated := delegatedcred.Secret(t, delegatedcred.Spec{Namespace: su994NS, Username: i1025Victim, Actor: d1560Actor,
		Groups: []string{d1560Group}, Token: i1025TokVictim})
	name := victimDelegated.Name
	for _, tc := range []struct {
		prov Provenance
		want string
	}{
		{ProvenanceStored, perUserRefProvenanceStored},
		{ProvenanceCallerSupplied, perUserRefProvenanceCallerSupplied},
	} {
		t.Run(tc.want, func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			victimDelegated.Data["server-url"] = []byte(apiURL)
			snap := cache.SecretsSnapshotLoad()
			snap.ByName[name] = victimDelegated
			var read []string
			var mu sync.Mutex
			prev := fromSecretAsCallerFn
			fromSecretAsCallerFn = func(_ context.Context, _ *rest.Config, n, _ string) (endpoints.Endpoint, error) {
				mu.Lock()
				read = append(read, n)
				mu.Unlock()
				return endpoints.Endpoint{ServerURL: apiURL, Token: i1025TokVictim}, nil
			}
			t.Cleanup(func() { fromSecretAsCallerFn = prev })

			before := PerUserRefRefusedSnapshot()
			header, logs := i1025Run(t, apiURL, &templates.Reference{Name: name, Namespace: su994NS}, tc.prov, nil)
			i1025AssertRefused(t, dials(), header, logs, name, tc.want, false, before, PerUserRefRefusedSnapshot())
			mu.Lock()
			defer mu.Unlock()
			if len(read) != 0 {
				t.Fatalf("a refused delegated ref must never be read; reads=%v", read)
			}
		})
	}
}
