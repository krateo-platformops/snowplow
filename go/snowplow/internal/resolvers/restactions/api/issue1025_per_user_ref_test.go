package api

// issue1025_per_user_ref_test.go — #1025: a stage's endpointRef never resolves
// to a per-user credential Secret, the session `<user>-clientconfig` or the
// step-up `<user>-clientconfig-stepup`, whoever vouches for the spec.
//
// Every arm drives the REAL api.Resolve, so the refusal is observed where it
// matters: the victim's credential is never presented to the apiserver. The
// informer snapshot holds the victim's Secrets, each carrying its own bearer;
// a resolve that selected one would dial with that bearer.
//
//   A1  stored RA, literal `victim-clientconfig`          → refused
//   A2  stored RA, literal `victim-clientconfig-stepup`   → refused
//   A3  stored RA, request-templated name (incl. one that only becomes a
//       per-user name after DNS-1123 normalisation)       → refused
//   A4  inline draft (caller-supplied), literal session and step-up refs
//                                                          → refused, never read
//   A5  a normal endpoint Secret, literal or templated     → dials it
//   A6  nil ref → the caller's OWN credential: step-up for a krateo:loa:2
//       identity, session otherwise (#994, #1024)
//
// A1–A4 assert the SAME refusal: the one error, the one log line and the one
// counter, labelled by provenance. There is one rule, not one per provenance.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/redact"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

const (
	i1025Caller      = "i1025-carol"
	i1025Victim      = "i1025-victim"
	i1025TokCaller   = "TOK-CALLER-SESSION-1025"
	i1025TokCallerUp = "TOK-CALLER-STEPUP-1025"
	i1025TokVictim   = "TOK-VICTIM-SESSION-1025"
	i1025TokVictimUp = "TOK-VICTIM-STEPUP-1025"
	i1025TokEndpoint = "TOK-ENDPOINT-1025"
	i1025Endpoint    = "i1025-spoke-endpoint"
	i1025Path        = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps/demo"
)

// i1025Log is a goroutine-safe sink for the resolve's slog output.
type i1025Log struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *i1025Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *i1025Log) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// i1025Setup seeds the informer snapshot with the caller's and the victim's
// credential Secrets plus one ordinary endpoint Secret, every one pointing at
// the fake apiserver with its own bearer.
func i1025Setup(t *testing.T) (string, func() []su994Dial) {
	t.Helper()
	api, dials := su994Upstream(t)
	su994Secrets(t, api.URL, map[string]string{
		credsecret.SessionName(i1025Caller): i1025TokCaller,
		credsecret.StepUpName(i1025Caller):  i1025TokCallerUp,
		credsecret.SessionName(i1025Victim): i1025TokVictim,
		credsecret.StepUpName(i1025Victim):  i1025TokVictimUp,
		i1025Endpoint:                       i1025TokEndpoint,
	})
	return api.URL, dials
}

// i1025Run resolves one GET stage with the given endpointRef as the caller and
// returns the stage-outcomes header and the log the resolve wrote.
func i1025Run(t *testing.T, apiURL string, ref *templates.Reference, prov Provenance, extras map[string]any, groups ...string) (string, string) {
	t.Helper()
	logs := &i1025Log{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(logger),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: i1025Caller, Groups: groups}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: apiURL, Token: i1025TokCaller}),
	)
	ctx, outcomes := WithStageOutcomes(ctx)
	Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: apiURL}, AuthnNS: su994NS,
		RESTActionNamespace: su994NS, RESTActionName: "ra1025", Provenance: prov, Extras: extras,
		Items: []*templates.API{{
			Name: "read", Path: i1025Path, Verb: ptr.To(http.MethodGet),
			EndpointRef: ref, ContinueOnError: ptr.To(true), ErrorKey: ptr.To("readErr"),
		}}})
	return outcomes.HeaderValue(), logs.String()
}

// i1025AssertRefused is the ONE refusal every provenance must show.
func i1025AssertRefused(t *testing.T, dials []su994Dial, header, logs, secretName, wantProv string, templated bool, before, after map[string]uint64) {
	t.Helper()
	for _, d := range dials {
		if d.bearer == i1025TokVictim || d.bearer == i1025TokVictimUp {
			t.Fatalf("#1025 VIOLATED: the stage dialed %s %s with the VICTIM's credential (%s)", d.method, d.path, d.bearer)
		}
	}
	if b := su994Bearers(dials); len(b) != 0 {
		t.Fatalf("#1025: a refused stage must dial nothing; bearers=%v", b)
	}
	if want := `{"name":"read","ok":false,"reason":"` + StageReasonError + `"}`; header != "["+want+"]" {
		t.Fatalf("#1025: want stage outcome %s, got %s", want, header)
	}
	if !strings.Contains(logs, `"msg":"endpointRef refused: it names a per-user credential Secret"`) {
		t.Fatalf("#1025: the refusal log line is missing; logs=%s", logs)
	}
	for _, kv := range []string{
		`"provenance":"` + wantProv + `"`,
		`"secret_name":"` + redact.Prefixed("secret", secretName) + `"`,
	} {
		if !strings.Contains(logs, kv) {
			t.Fatalf("#1025: the refusal log line lacks %s; logs=%s", kv, logs)
		}
	}
	if wantT := `"templated":` + map[bool]string{true: "true", false: "false"}[templated]; !strings.Contains(logs, wantT) {
		t.Fatalf("#1025: the refusal log line lacks %s; logs=%s", wantT, logs)
	}
	// Never the Secret name nor the victim's username in clear.
	for _, clear := range []string{secretName, i1025Victim} {
		if strings.Contains(logs, clear) {
			t.Fatalf("#1025: the log carries %q in clear; logs=%s", clear, logs)
		}
	}
	for _, p := range []string{perUserRefProvenanceStored, perUserRefProvenanceCallerSupplied} {
		want := before[p]
		if p == wantProv {
			want++
		}
		if after[p] != want {
			t.Fatalf("#1025: snowplow_endpointref_per_user_refused_total{%s} went %d → %d, want %d", p, before[p], after[p], want)
		}
	}
}

// A1, A2 — a stored RESTAction's LITERAL ref to another user's session or
// step-up credential is refused.
func TestIssue1025_A1A2_StoredLiteralPerUserRefRefused(t *testing.T) {
	for _, name := range []string{credsecret.SessionName(i1025Victim), credsecret.StepUpName(i1025Victim)} {
		t.Run(name[len(i1025Victim):], func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			before := PerUserRefRefusedSnapshot()
			header, logs := i1025Run(t, apiURL, &templates.Reference{Name: name, Namespace: su994NS}, ProvenanceStored, nil)
			i1025AssertRefused(t, dials(), header, logs, name, perUserRefProvenanceStored, false, before, PerUserRefRefusedSnapshot())
		})
	}
}

// A3 — a stored RESTAction's REQUEST-TEMPLATED ref folding an extra into a
// per-user name is refused by the same rule. The mixed-case template only
// becomes a per-user name after DNS-1123 normalisation: the rule checks the
// name that is looked up, never the rendered text.
func TestIssue1025_A3_StoredTemplatedPerUserRefRefused(t *testing.T) {
	for _, tc := range []struct{ tmpl, resolved string }{
		{`${ .want + "-clientconfig" }`, credsecret.SessionName(i1025Victim)},
		{`${ .want + "-clientconfig-stepup" }`, credsecret.StepUpName(i1025Victim)},
		{`${ .want + "-ClientConfig-StepUp" }`, credsecret.StepUpName(i1025Victim)},
	} {
		t.Run(tc.tmpl, func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			before := PerUserRefRefusedSnapshot()
			header, logs := i1025Run(t, apiURL, &templates.Reference{Name: tc.tmpl, Namespace: su994NS}, ProvenanceStored,
				map[string]any{"want": i1025Victim})
			i1025AssertRefused(t, dials(), header, logs, tc.resolved, perUserRefProvenanceStored, true, before, PerUserRefRefusedSnapshot())
		})
	}
}

// A4 — an inline (caller-supplied) draft's literal session or step-up ref is
// refused by the same rule, before the Secret is read as the caller.
func TestIssue1025_A4_CallerSuppliedPerUserRefRefused(t *testing.T) {
	for _, name := range []string{credsecret.SessionName(i1025Victim), credsecret.StepUpName(i1025Victim)} {
		t.Run(name[len(i1025Victim):], func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			var read []string
			var mu sync.Mutex
			prev := fromSecretAsCallerFn
			fromSecretAsCallerFn = func(_ context.Context, _ *rest.Config, n, _ string) (endpoints.Endpoint, error) {
				mu.Lock()
				read = append(read, n)
				mu.Unlock()
				return endpoints.Endpoint{ServerURL: apiURL, Token: i1025TokVictimUp}, nil
			}
			t.Cleanup(func() { fromSecretAsCallerFn = prev })

			before := PerUserRefRefusedSnapshot()
			header, logs := i1025Run(t, apiURL, &templates.Reference{Name: name, Namespace: su994NS}, ProvenanceCallerSupplied, nil)
			i1025AssertRefused(t, dials(), header, logs, name, perUserRefProvenanceCallerSupplied, false, before, PerUserRefRefusedSnapshot())
			mu.Lock()
			defer mu.Unlock()
			if len(read) != 0 {
				t.Fatalf("#1025: a refused caller-supplied ref must never be read; reads=%d", len(read))
			}
		})
	}
}

// A5 — a normal endpoint Secret, literal or templated, still resolves and is
// dialed with its own credential; nothing is refused.
func TestIssue1025_A5_NormalEndpointSecretWorks(t *testing.T) {
	for _, name := range []string{i1025Endpoint, `${ .want + "-spoke-endpoint" }`} {
		t.Run(name, func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			before := PerUserRefRefusedSnapshot()
			header, logs := i1025Run(t, apiURL, &templates.Reference{Name: name, Namespace: su994NS}, ProvenanceStored,
				map[string]any{"want": "i1025"})
			if b := su994Bearers(dials()); b[i1025TokEndpoint] != 1 || len(b) != 1 {
				t.Fatalf("a normal endpointRef must dial its own credential once; bearers=%v logs=%s", b, logs)
			}
			if header != "[]" {
				t.Fatalf("a normal endpointRef stage must succeed; outcomes=%s", header)
			}
			if after := PerUserRefRefusedSnapshot(); after[perUserRefProvenanceStored] != before[perUserRefProvenanceStored] {
				t.Fatalf("a normal endpointRef must not count a refusal: %v → %v", before, after)
			}
			if strings.Contains(logs, "endpointRef refused") {
				t.Fatalf("a normal endpointRef must not log a refusal; logs=%s", logs)
			}
		})
	}
}

// A6 — a nil ref still dials the caller's OWN credential, chosen from the
// caller's identity: step-up for a krateo:loa:2 identity, session otherwise
// (#994/#1024). The rule never touches the nil-ref synthesis, which builds a
// per-user name itself.
func TestIssue1025_A6_NilRefUsesTheCallersOwnCredential(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups []string
		want   string
	}{
		{"session", []string{"team"}, i1025TokCaller},
		{"stepup", []string{"team", credsecret.StepUpGroup}, i1025TokCallerUp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apiURL, dials := i1025Setup(t)
			before := PerUserRefRefusedSnapshot()
			header, _ := i1025Run(t, apiURL, nil, ProvenanceStored, nil, tc.groups...)
			if b := su994Bearers(dials()); b[tc.want] != 1 || len(b) != 1 {
				t.Fatalf("a nil ref must dial the caller's own %s credential only; bearers=%v", tc.name, b)
			}
			if header != "[]" {
				t.Fatalf("a nil-ref stage must succeed; outcomes=%s", header)
			}
			if after := PerUserRefRefusedSnapshot(); after[perUserRefProvenanceStored] != before[perUserRefProvenanceStored] {
				t.Fatalf("a nil ref must not count a refusal: %v → %v", before, after)
			}
		})
	}
}

// The choke point itself: resolveOne returns the one sentinel for every
// provenance, and its text names no Secret.
func TestIssue1025_ResolveOneRefusesEveryProvenance(t *testing.T) {
	// Both of the victim's Secrets are in the snapshot with a SENTINEL
	// server-url: a lookup that is not refused returns it.
	names := []string{credsecret.SessionName("victim"), credsecret.StepUpName("victim")}
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	snap := map[string]*corev1.Secret{}
	for _, n := range names {
		snap[n] = mkSecret(su994NS, n, map[string]string{"server-url": su994Forged})
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: snap})
	cache.ForceSecretsCacheReadyForTest(su994NS)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })

	for _, prov := range []Provenance{ProvenanceStored, ProvenanceCallerSupplied} {
		m := &endpointReferenceMapper{authnNS: su994NS, username: "attacker", provenance: prov}
		for _, name := range names {
			ep, _, err := m.resolveOne(context.Background(), &templates.Reference{Name: name, Namespace: su994NS})
			if !errors.Is(err, ErrPerUserCredentialRef) || ep.ServerURL == su994Forged {
				t.Fatalf("provenance %s, ref %q: want ErrPerUserCredentialRef and no endpoint; got ep=%q err=%v",
					provenanceLabel(prov), name, ep.ServerURL, err)
			}
			if strings.Contains(err.Error(), "victim") {
				t.Fatalf("the refusal text must not name the Secret: %v", err)
			}
		}
	}
}
