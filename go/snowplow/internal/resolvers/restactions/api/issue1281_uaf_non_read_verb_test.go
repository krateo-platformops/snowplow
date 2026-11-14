//go:build unit || integration

package api

// issue1281_uaf_non_read_verb_test.go — #1281 (SECURITY) arms.
//
// THE DEFECT. userAccessFilter switches a stage's credential to snowplow's
// ServiceAccount. Only the CRD rule (GET/HEAD only) kept it off a write, and
// admission checks it only when a RESTAction is stored, so a RESTAction stored
// before the rule (or with it relaxed) carrying userAccessFilter on a POST sent
// that POST as the ServiceAccount: the user's RBAC never decided the write.
//
// THE HARNESS. The real resolver (ResolveWithFailure), cache off, against one
// fake apiserver that records every request it receives with its bearer
// token. The tokens tell the credentials apart:
//   - u81SAToken / u81ICToken — snowplow's ServiceAccount (the UAF endpoint
//     seam, the in-cluster config seam);
//   - u81UserToken — the requester's own clientconfig (its Secret is served
//     by the fake, read with u81RCToken).
//
// ARMS.
//   A  a stage with userAccessFilter on POST, PUT, PATCH, DELETE is refused:
//      the fake receives 0 requests, the outcome is
//      UserAccessFilterOnNonReadVerb, the errorKey and the StageFailure say so,
//      the counter counts it, the WARN line holds metadata only;
//   B  a dependent write of a refused stage is not sent (#904 Skipped);
//   C  a GET stage with userAccessFilter is unchanged: dialled as the
//      ServiceAccount and refiltered to what the user may list;
//   D  a write stage without userAccessFilter is unchanged: dialled once with
//      the user's own clientconfig;
//   E  dry run: a userAccessFilter write reports UserAccessFilterOnNonReadVerb,
//      a plain write keeps StageNotExecuted; neither is sent;
//   F  no resolve kind is exempt: an inline draft (caller-supplied) and a
//      background resolve refuse it with the same reason;
//   G  the report does not depend on the data: an iterator stage whose
//      iterator yields zero items is refused, listed and counted all the same;
//   H  nor on a failed dependency: the stage reports its own reason, not the
//      #904 Skipped.
//
// RED WITHOUT THE FIX (run 2026-11-09, refuseUAFOnNonReadVerb's call disabled
// in ResolveWithFailure's stage loop): A fails on every verb, the fake
// receiving the write with the ServiceAccount's token (e.g. POST
// .../namespaces/ns-a/widgets Token:SA-EP-TOKEN-1281); A2 the same; B sends
// both writes, the first as the ServiceAccount; E reports StageNotExecuted for
// the UAF write; F fails on both kinds; G reads header [] (the stage is not
// listed); H reads Skipped. G and H are also what a per-item refusal at
// dispatch (the first cut of this fix) reports: with zero items, or a skipped
// stage, no item reaches dispatch. C and D pass with and without the fix.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

const (
	u81AuthnNS   = "krateo-system"
	u81User      = "u-1281-alice"
	u81SAToken   = "SA-EP-TOKEN-1281"
	u81ICToken   = "SA-IC-TOKEN-1281"
	u81RCToken   = "SA-RC-TOKEN-1281"
	u81UserToken = "USER-TOKEN-1281"
	u81Group     = "example.test"
	u81Resource  = "widgets"
	u81ListPath  = "/apis/example.test/v1/widgets"
	// u81EmptyListPath answers a list with no items; u81ConflictPath answers
	// any write with 409.
	u81EmptyListPath = "/apis/example.test/v1/namespaces/ns-empty/widgets"
	u81ConflictPath  = "/apis/example.test/v1/namespaces/ns-a/widgets/busy"
	u81RAName        = "ra-1281"
	u81RANS          = "ns-1281"
)

type u81Hit struct{ Method, Path, Token string }

type u81Server struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits []u81Hit
}

func (s *u81Server) clientconfigPath() string {
	return "/api/v1/namespaces/" + u81AuthnNS + "/secrets/" + credsecret.For(u81User, []string{"devs"})[0]
}

// all returns every request received, the clientconfig Secret read excluded
// (it is the resolver reading the user's credential, not a stage's request).
func (s *u81Server) all() []u81Hit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []u81Hit{}
	for _, h := range s.hits {
		if h.Path != s.clientconfigPath() {
			out = append(out, h)
		}
	}
	return out
}

func newU81Server(t *testing.T) *u81Server {
	t.Helper()
	s := &u81Server{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.hits = append(s.hits, u81Hit{Method: r.Method, Path: r.URL.Path,
			Token: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == s.clientconfigPath():
			b, _ := json.Marshal(corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: authnMeta(u81AuthnNS, credsecret.For(u81User, []string{"devs"})[0], u81User),
				Data:       map[string][]byte{"server-url": []byte(s.srv.URL), "token": []byte(u81UserToken)},
			})
			_, _ = w.Write(b)
		case r.Method == http.MethodGet && r.URL.Path == u81ListPath:
			_, _ = io.WriteString(w, `{"apiVersion":"example.test/v1","kind":"WidgetList","metadata":{"resourceVersion":"1"},"items":[`+
				`{"apiVersion":"example.test/v1","kind":"Widget","metadata":{"name":"w-a","namespace":"ns-a"}},`+
				`{"apiVersion":"example.test/v1","kind":"Widget","metadata":{"name":"w-b","namespace":"ns-b"}}]}`)
		case r.Method == http.MethodGet && r.URL.Path == u81EmptyListPath:
			_, _ = io.WriteString(w, `{"apiVersion":"example.test/v1","kind":"WidgetList","metadata":{"resourceVersion":"1"},"items":[]}`)
		case r.Method != http.MethodGet && r.URL.Path == u81ConflictPath:
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","code":409}`)
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"apiVersion":"example.test/v1","kind":"Widget","metadata":{"name":"w-new","namespace":"ns-a"}}`)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// u81Env wires the seams: the ServiceAccount endpoint and in-cluster config
// point at the fake with the SA tokens; the cache-off refilter's
// SelfSubjectAccessReviews answer "the user may list widgets in ns-a only".
func u81Env(t *testing.T, s *u81Server) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	t.Cleanup(SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: s.srv.URL, Token: u81SAToken}, nil
	}))
	t.Cleanup(SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: s.srv.URL, BearerToken: u81ICToken}, nil
	}))
	t.Cleanup(rbac.SetSARClientsetForTest(func(_ context.Context, _ endpoints.Endpoint) (kubernetes.Interface, error) {
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
			ssar, ok := a.(k8stesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview)
			if !ok || ssar.Spec.ResourceAttributes == nil {
				return true, nil, errors.New("u81: not a resource SelfSubjectAccessReview")
			}
			ra := ssar.Spec.ResourceAttributes
			out := ssar.DeepCopy()
			out.Status.Allowed = ra.Group == u81Group && ra.Resource == u81Resource && ra.Namespace == "ns-a" &&
				(ra.Verb == "list" || ra.Verb == "get")
			return true, out, nil
		})
		return cs, nil
	}))
}

type u81Out struct {
	dict     map[string]any
	failure  *StageFailure
	outcomes string
	logs     string
}

type u81Kind int

const (
	u81Customer u81Kind = iota
	u81DryRun
	u81Inline
	u81Background
)

func u81Resolve(t *testing.T, s *u81Server, kind u81Kind, stages ...*templates.API) u81Out {
	t.Helper()
	var logs bytes.Buffer
	var logMu sync.Mutex
	lg := slog.New(slog.NewJSONHandler(writerFunc(func(p []byte) (int, error) {
		logMu.Lock()
		defer logMu.Unlock()
		return logs.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: u81User, Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: s.srv.URL, Token: u81UserToken}),
		xcontext.WithLogger(lg))
	prov := ProvenanceStored
	switch kind {
	case u81DryRun:
		ctx = cache.WithInert(ctx)
	case u81Inline:
		prov = ProvenanceCallerSupplied
	case u81Background:
		ctx = cache.WithBackgroundHop(ctx)
	}
	ctx, oc := WithStageOutcomes(ctx)
	dict, f := ResolveWithFailure(ctx, ResolveOptions{
		RC:      &rest.Config{Host: s.srv.URL, BearerToken: u81RCToken},
		AuthnNS: u81AuthnNS, Items: stages,
		RESTActionNamespace: u81RANS, RESTActionName: u81RAName,
		Provenance: prov,
	})
	logMu.Lock()
	defer logMu.Unlock()
	return u81Out{dict: dict, failure: f, outcomes: oc.HeaderValue(), logs: logs.String()}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func u81UAF() *templates.UserAccessFilterSpec {
	return &templates.UserAccessFilterSpec{Verb: "list", Group: u81Group, Resource: u81Resource, NamespaceFrom: ".metadata.namespace"}
}

// u81WriteStage is a write of one widget in ns-a, with a userAccessFilter when
// uaf is true.
func u81WriteStage(name, verb string, uaf bool) *templates.API {
	path := "/apis/example.test/v1/namespaces/ns-a/widgets"
	if verb != http.MethodPost {
		path += "/w-a"
	}
	s := &templates.API{Name: name, Verb: ptr.To(verb), Path: path,
		Payload: ptr.To(`{"metadata":{"name":"w-a"}}`), Headers: []string{"Content-Type: application/json"},
		ErrorKey: ptr.To(name + "Err")}
	if uaf {
		s.UserAccessFilter = u81UAF()
	}
	return s
}

type u81Row struct {
	Name, Reason, DependsOn string
	OK                      bool
}

func u81Rows(t *testing.T, header string) map[string]u81Row {
	t.Helper()
	var rows []u81Row
	if err := json.Unmarshal([]byte(header), &rows); err != nil {
		t.Fatalf("X-Snowplow-Stage-Outcomes is not a JSON array: %q (%v)", header, err)
	}
	m := map[string]u81Row{}
	for _, r := range rows {
		m[r.Name] = r
	}
	return m
}

func u81WantReason(t *testing.T, o u81Out, name, reason string) {
	t.Helper()
	r, ok := u81Rows(t, o.outcomes)[name]
	if !ok || r.OK || r.Reason != reason {
		t.Errorf("stage %q outcome = %+v (listed=%v), want reason %s; header %s", name, r, ok, reason, o.outcomes)
	}
}

// u81WantRefused asserts the refusal's other surfaces: the errorKey record,
// the metadata-only WARN line, and no identity anywhere.
func u81WantRefused(t *testing.T, o u81Out, name, verb string) {
	t.Helper()
	u81WantReason(t, o, name, StageReasonUAFOnNonReadVerb)
	b, _ := json.Marshal(o.dict[name+"Err"])
	if !strings.Contains(string(b), StageReasonUAFOnNonReadVerb) || !strings.Contains(string(b), "verb "+verb) {
		t.Errorf("errorKey %sErr = %s, want the %s record naming verb %s", name, b, StageReasonUAFOnNonReadVerb, verb)
	}
	var warn map[string]any
	for _, line := range strings.Split(o.logs, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["level"] == "WARN" &&
			m["msg"] == "stage refused: userAccessFilter on a verb other than GET or HEAD" && m["name"] == name {
			warn = m
		}
	}
	if warn == nil {
		t.Fatalf("no WARN refusal line for stage %q; logs:\n%s", name, o.logs)
	}
	for k, want := range map[string]string{"verb": verb, "reason": StageReasonUAFOnNonReadVerb,
		"restaction_namespace": u81RANS, "restaction_name": u81RAName} {
		if warn[k] != want {
			t.Errorf("WARN line %s = %v, want %s (line %v)", k, warn[k], want, warn)
		}
	}
	lb, _ := json.Marshal(warn)
	for _, secret := range []string{u81User, u81SAToken, u81UserToken, "/apis/"} {
		if strings.Contains(string(lb), secret) {
			t.Errorf("the WARN line carries %q: %s", secret, lb)
		}
	}
	if strings.Contains(string(b), u81User) {
		t.Errorf("the errorKey record carries the user: %s", b)
	}
}

// A — a userAccessFilter write stage is never sent, on every write verb.
func TestIssue1281_A_UAFWriteStageIsRefused(t *testing.T) {
	for _, verb := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(verb, func(t *testing.T) {
			s := newU81Server(t)
			u81Env(t, s)
			before := UAFNonReadVerbRefusedSnapshot()
			o := u81Resolve(t, s, u81Customer, u81WriteStage("w", verb, true))
			if hits := s.all(); len(hits) != 0 {
				t.Fatalf("#1281 RED: a %s stage carrying userAccessFilter reached the apiserver: %+v", verb, hits)
			}
			u81WantRefused(t, o, "w", verb)
			if o.failure == nil || o.failure.Stage != "w" || o.failure.Code != http.StatusInternalServerError {
				t.Errorf("StageFailure = %+v, want stage w, code 500", o.failure)
			}
			after := UAFNonReadVerbRefusedSnapshot()
			if d := after[perUserRefProvenanceStored] - before[perUserRefProvenanceStored]; d != 1 {
				t.Errorf("snowplow_uaf_non_read_verb_refused_total{stored} moved by %d, want 1", d)
			}
		})
	}
}

// A2 — continueOnError keeps the resolve going but the stage still fails.
func TestIssue1281_A2_ContinueOnErrorStillRefuses(t *testing.T) {
	s := newU81Server(t)
	u81Env(t, s)
	st := u81WriteStage("w", http.MethodPost, true)
	st.ContinueOnError = ptr.To(true)
	o := u81Resolve(t, s, u81Customer, st)
	if hits := s.all(); len(hits) != 0 {
		t.Fatalf("#1281 RED: the write reached the apiserver: %+v", hits)
	}
	u81WantRefused(t, o, "w", http.MethodPost)
	if o.failure != nil {
		t.Errorf("continueOnError: StageFailure = %+v, want nil", o.failure)
	}
}

// B — a write that depends on a refused stage is not sent (#904).
func TestIssue1281_B_DependentWriteOfARefusedStageIsSkipped(t *testing.T) {
	s := newU81Server(t)
	u81Env(t, s)
	dep := u81WriteStage("after", http.MethodPost, false)
	dep.DependsOn = &templates.Dependency{Name: "w"}
	o := u81Resolve(t, s, u81Customer, u81WriteStage("w", http.MethodPost, true), dep)
	if hits := s.all(); len(hits) != 0 {
		t.Fatalf("a write was sent: %+v", hits)
	}
	u81WantReason(t, o, "w", StageReasonUAFOnNonReadVerb)
	if r := u81Rows(t, o.outcomes)["after"]; r.Reason != StageReasonSkipped || r.DependsOn != "w" {
		t.Errorf("dependent outcome = %+v, want Skipped by w; header %s", r, o.outcomes)
	}
}

// C — a GET stage with userAccessFilter is unchanged: one dial as the
// ServiceAccount, refiltered to the namespace the user may list.
func TestIssue1281_C_UAFReadStageUnchanged(t *testing.T) {
	s := newU81Server(t)
	u81Env(t, s)
	before := UAFNonReadVerbRefusedSnapshot()
	o := u81Resolve(t, s, u81Customer, &templates.API{Name: "r", Verb: ptr.To(http.MethodGet), Path: u81ListPath, UserAccessFilter: u81UAF()})
	hits := s.all()
	if len(hits) != 1 || hits[0].Method != http.MethodGet || hits[0].Path != u81ListPath {
		t.Fatalf("requests = %+v, want exactly one GET %s", hits, u81ListPath)
	}
	if tok := hits[0].Token; tok != u81SAToken && tok != u81ICToken {
		t.Errorf("the UAF read was dialled with token %q, want the ServiceAccount's", tok)
	}
	if o.outcomes != "[]" {
		t.Errorf("outcomes = %s, want []", o.outcomes)
	}
	b, _ := json.Marshal(o.dict["r"])
	if !strings.Contains(string(b), `"w-a"`) || strings.Contains(string(b), `"w-b"`) {
		t.Errorf("dict[r] = %s, want w-a only (the user may list ns-a only)", b)
	}
	if after := UAFNonReadVerbRefusedSnapshot(); after[perUserRefProvenanceStored] != before[perUserRefProvenanceStored] {
		t.Errorf("a read moved the refusal counter")
	}
}

// D — a write stage without userAccessFilter is unchanged: sent once, with the
// user's own clientconfig.
func TestIssue1281_D_PlainWriteStageUnchanged(t *testing.T) {
	for _, verb := range []string{http.MethodPost, http.MethodDelete} {
		t.Run(verb, func(t *testing.T) {
			s := newU81Server(t)
			u81Env(t, s)
			st := u81WriteStage("w", verb, false)
			o := u81Resolve(t, s, u81Customer, st)
			hits := s.all()
			if len(hits) != 1 || hits[0].Method != verb || hits[0].Path != st.Path {
				t.Fatalf("requests = %+v, want exactly one %s %s", hits, verb, st.Path)
			}
			if hits[0].Token != u81UserToken {
				t.Errorf("the write was dialled with token %q, want the user's own clientconfig", hits[0].Token)
			}
			if o.outcomes != "[]" {
				t.Errorf("outcomes = %s, want []", o.outcomes)
			}
		})
	}
}

// E — dry run: neither write is sent; the UAF one names its own reason, the
// plain one keeps StageNotExecuted.
func TestIssue1281_E_DryRun(t *testing.T) {
	for _, verb := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(verb, func(t *testing.T) {
			s := newU81Server(t)
			u81Env(t, s)
			o := u81Resolve(t, s, u81DryRun, u81WriteStage("uaf", verb, true), u81WriteStage("plain", verb, false))
			if hits := s.all(); len(hits) != 0 {
				t.Fatalf("a dry run sent: %+v", hits)
			}
			u81WantRefused(t, o, "uaf", verb)
			u81WantReason(t, o, "plain", StageReasonNotExecuted)
		})
	}
}

// F — no resolve kind is exempt.
func TestIssue1281_F_InlineAndBackgroundRefuseToo(t *testing.T) {
	for _, c := range []struct {
		name string
		kind u81Kind
		cell string
	}{
		{"inline", u81Inline, perUserRefProvenanceCallerSupplied},
		{"background", u81Background, perUserRefProvenanceStored},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newU81Server(t)
			u81Env(t, s)
			before := UAFNonReadVerbRefusedSnapshot()
			o := u81Resolve(t, s, c.kind, u81WriteStage("w", http.MethodPost, true))
			if hits := s.all(); len(hits) != 0 {
				t.Fatalf("sent: %+v", hits)
			}
			u81WantRefused(t, o, "w", http.MethodPost)
			if d := UAFNonReadVerbRefusedSnapshot()[c.cell] - before[c.cell]; d != 1 {
				t.Errorf("counter {%s} moved by %d, want 1", c.cell, d)
			}
		})
	}
}

// G — the refusal does not depend on the data: an iterator write whose
// iterator yields zero items (nothing would be sent anyway) is still refused,
// listed and counted.
func TestIssue1281_G_ZeroItemIteratorIsRefusedToo(t *testing.T) {
	s := newU81Server(t)
	u81Env(t, s)
	before := UAFNonReadVerbRefusedSnapshot()
	src := &templates.API{Name: "src", Verb: ptr.To(http.MethodGet), Path: u81EmptyListPath}
	w := u81WriteStage("w", http.MethodDelete, true)
	w.Path = `${ "/apis/example.test/v1/namespaces/ns-a/widgets/" + .metadata.name }`
	w.Payload = nil
	w.DependsOn = &templates.Dependency{Name: "src", Iterator: ptr.To(".src.items")}
	o := u81Resolve(t, s, u81Customer, src, w)
	hits := s.all()
	if len(hits) != 1 || hits[0].Method != http.MethodGet || hits[0].Path != u81EmptyListPath {
		t.Fatalf("requests = %+v, want only the source GET %s", hits, u81EmptyListPath)
	}
	u81WantRefused(t, o, "w", http.MethodDelete)
	if _, listed := u81Rows(t, o.outcomes)["src"]; listed {
		t.Errorf("the source read failed: header %s", o.outcomes)
	}
	if d := UAFNonReadVerbRefusedSnapshot()[perUserRefProvenanceStored] - before[perUserRefProvenanceStored]; d != 1 {
		t.Errorf("counter {stored} moved by %d, want 1", d)
	}
}

// H — a dependency that failed does not hide it either: the stage reports its
// own misconfiguration, not the #904 Skipped.
func TestIssue1281_H_FailedDependencyDoesNotHideIt(t *testing.T) {
	s := newU81Server(t)
	u81Env(t, s)
	first := u81WriteStage("first", http.MethodPut, false)
	first.Path = u81ConflictPath
	w := u81WriteStage("w", http.MethodPost, true)
	w.DependsOn = &templates.Dependency{Name: "first"}
	o := u81Resolve(t, s, u81Customer, first, w)
	hits := s.all()
	if len(hits) != 1 || hits[0].Method != http.MethodPut || hits[0].Path != u81ConflictPath {
		t.Fatalf("requests = %+v, want only the PUT %s", hits, u81ConflictPath)
	}
	u81WantReason(t, o, "first", StageReasonError)
	u81WantRefused(t, o, "w", http.MethodPost)
}
