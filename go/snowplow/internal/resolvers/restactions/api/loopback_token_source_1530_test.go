package api

// loopback_token_source_1530_test.go — #1530: a prewarm context carries the SOURCE of the authn-issued token
// (loopbacktoken), never the token, and a stage asks it only once bearerAppendForStage has decided the stage carries
// a bearer, under the stage's own context. The leak guard is the heart of it: the token reaches exactly the stages the
// decision allows (the self-loopback, an exportJwt opt-in), never a named external endpoint, and the source is never
// even asked for one. Driven through the real Resolve, dialling httptest servers through named endpointRefs resolved
// from the Secrets snapshot (the #1106 harness), the wire's Authorization recorded.

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/loopbacktoken"
	"k8s.io/client-go/rest"
)

const t1530Token = "SEED-JWT-1530-authn-issued"

// t1530Source records every ask: the endpointRef of the stage whose context it was asked under (stageEndpointRef:
// the stage's own context carries it; the resolve's root context does not), and whether that context was live.
type t1530Source struct {
	mu   sync.Mutex
	refs []string
	err  error
}

func (s *t1530Source) ask(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := "<no stage context>"
	if r := stageEndpointRef(ctx); r != nil {
		ref = r.Name
	}
	if ctx.Err() != nil {
		ref += " (done)"
	}
	s.refs = append(s.refs, ref)
	if s.err != nil {
		return "", s.err
	}
	return t1530Token, nil
}

func (s *t1530Source) asked() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.refs...)
	sort.Strings(out)
	return out
}

type t1530Env struct {
	self, ext *x1106Srv
}

// t1530Setup: snowplow's own host (selfSrv, URL_SELF) and an external host (extSrv, another host and port), and three
// named endpointRef Secrets: self-ep -> snowplow itself, ext-ep and exp-ep -> the external host. None carries a token
// of its own, so any Authorization on the wire is the appended bearer.
func t1530Setup(t *testing.T) t1530Env {
	t.Helper()
	e := t1530Env{self: newX1106Srv(t, false, x1106OK), ext: newX1106Srv(t, false, x1106OK)}
	if !SetSelfHost(e.self.srv.URL) {
		t.Fatalf("SetSelfHost(%q) did not take", e.self.srv.URL)
	}
	t.Cleanup(func() { SetSelfHost("") })
	ext := x1106OtherURL(e.ext.srv)
	x1106Env(t, "", map[string]map[string]string{
		"self-ep": {"server-url": e.self.srv.URL},
		"ext-ep":  {"server-url": ext},
		"exp-ep":  {"server-url": ext},
	})
	return e
}

func t1530Stage(name, ref, path string, exportJWT bool) *templates.API {
	s := &templates.API{Name: name, Verb: ptr.To("GET"), Path: path,
		EndpointRef: &templates.Reference{Name: ref, Namespace: x1106NS}, ContinueOnError: ptr.To(true)}
	if exportJWT {
		s.ExportJWT = ptr.To(true)
	}
	return s
}

// t1530Resolve runs the stages under a context carrying `src` (a prewarm context) and, when set, the requester's
// own `token` (a customer /call).
func t1530Resolve(t *testing.T, src loopbacktoken.Source, token string, stages ...*templates.API) {
	t.Helper()
	opts := []xcontext.WithContextFunc{
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: x1106User, Groups: []string{"tenants"}}),
		xcontext.WithLogger(slog.Default()),
	}
	if token != "" {
		opts = append(opts, xcontext.WithAccessToken(token))
	}
	ctx := loopbacktoken.WithSource(xcontext.BuildContext(context.Background(), opts...), src)
	_, _ = ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: "http://127.0.0.1:1"}, AuthnNS: x1106NS, Items: stages,
		RESTActionNamespace: x1106NS, RESTActionName: "t1530-actions",
	})
}

func t1530Auth(t *testing.T, z *x1106Srv, path string) []string {
	t.Helper()
	var out []string
	for _, h := range z.all() {
		if h.Path == path {
			out = append(out, h.Auth)
		}
	}
	return out
}

// TestLoopbackTokenSource_ReachesOnlyTheStagesTheDecisionAllows is the leak guard. One resolve, three stages on a
// prewarm context (a source, no token): the self-loopback and the exportJwt opt-in present the source's token; the
// plain external stage presents none, and the source is never asked for it. Every ask ran under the asking stage's
// own, live context (mutations: asking the source before bearerAppendForStage decides asks it for ext-ep too; asking
// it under the resolve's root context loses the stage's endpointRef).
func TestLoopbackTokenSource_ReachesOnlyTheStagesTheDecisionAllows(t *testing.T) {
	e := t1530Setup(t)
	src := &t1530Source{}
	t1530Resolve(t, src.ask, "",
		t1530Stage("self", "self-ep", "/call/self", false),
		t1530Stage("ext", "ext-ep", "/ext", false),
		t1530Stage("exp", "exp-ep", "/exp", true))

	if got := t1530Auth(t, e.self, "/call/self"); len(got) != 1 || got[0] != "Bearer "+t1530Token {
		t.Fatalf("the self-loopback stage's Authorization: %q, want one request with the source's token", got)
	}
	if got := t1530Auth(t, e.ext, "/exp"); len(got) != 1 || got[0] != "Bearer "+t1530Token {
		t.Fatalf("the exportJwt stage's Authorization: %q, want one request with the source's token", got)
	}
	if got := t1530Auth(t, e.ext, "/ext"); len(got) != 1 || got[0] != "" {
		t.Fatalf("THE LEAK: the plain external stage's Authorization: %q, want one request with none", got)
	}
	if got := src.asked(); len(got) != 2 || got[0] != "exp-ep" || got[1] != "self-ep" {
		t.Fatalf("the source was asked under %q, want exactly once for each of exp-ep and self-ep, under their own "+
			"live stage context, and never for ext-ep", got)
	}
}

// TestLoopbackTokenSource_ACustomersOwnTokenWins: a customer /call carries its requester's own token; the source, were
// one present, is never asked, and the self-loopback presents the requester's token.
func TestLoopbackTokenSource_ACustomersOwnTokenWins(t *testing.T) {
	e := t1530Setup(t)
	src := &t1530Source{}
	t1530Resolve(t, src.ask, "USER-JWT-1530", t1530Stage("self", "self-ep", "/call/self", false))
	if got := t1530Auth(t, e.self, "/call/self"); len(got) != 1 || got[0] != "Bearer USER-JWT-1530" {
		t.Fatalf("the self-loopback stage's Authorization: %q, want the requester's own token", got)
	}
	if got := src.asked(); len(got) != 0 {
		t.Fatalf("the source was asked %v with the requester's own token on the context", got)
	}
}

// TestLoopbackTokenSource_UnavailableTokenRunsTheStageTokenLess: the source cannot answer (authn down): the stage is
// still sent, token-less, exactly as a prewarm stage without a token always was (the degrade), after one ask.
func TestLoopbackTokenSource_UnavailableTokenRunsTheStageTokenLess(t *testing.T) {
	e := t1530Setup(t)
	src := &t1530Source{err: errors.New("dial tcp: connection refused")}
	t1530Resolve(t, src.ask, "", t1530Stage("self", "self-ep", "/call/self", false))
	if got := t1530Auth(t, e.self, "/call/self"); len(got) != 1 || got[0] != "" {
		t.Fatalf("the self-loopback stage with no token to be had: Authorization %q, want one token-less request", got)
	}
	if got := src.asked(); len(got) != 1 || got[0] != "self-ep" {
		t.Fatalf("the source was asked %v, want once, for the self-loopback stage", got)
	}
}
