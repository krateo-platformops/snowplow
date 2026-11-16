package api

// issue904_dependent_write_test.go — #904: a write stage is not sent when the
// stage it depends on failed or was skipped, unless the write stage itself sets
// continueOnError. The upstream is an httptest server reached through an
// endpointRef Secret (the provider shape: read → detach volumes → delete), and
// the test owns the request counter: "not sent" is a count of 0 at the
// upstream, never an inference from the dict.
//
// ARMS
//   A  read → failing write (409) → dependent DELETE: the DELETE is not sent;
//      the header lists the failure and the skip (with the stage depended on).
//   B  the same with continueOnError on the DELETE: it is sent once, and the
//      header still lists the failure.
//   C  an empty-iterator dependency SUCCEEDED: the dependent DELETE is sent.
//   D  one of N iterator items fails: the dependent DELETE is not sent.
//   E  a skip propagates over 3 levels (POST → PUT → DELETE after a failed GET).
//   F  a read-only RESTAction with a failing stage is unchanged: the later read
//      stage runs and the error stays under its errorKey.
//   G  an iterator write over a failed stage sends none of its items.
//   H  a dry run (#443 k) still reports StageNotExecuted for a dependent write:
//      a dry-run refusal is not a failure that skips (the preview stays what a
//      real run would do).
//   I  a userAccessFilter stage that could not acquire its ServiceAccount
//      endpoint failed: its dependent write is not sent.
//
// Mutation (logic-only, owned): make dependentWriteBlocked return false → A, D,
// E, G go RED on the upstream counts.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	u904AuthnNS = "krateo-system"
	u904Secret  = "aruba-ep"
)

type u904Route struct {
	status int
	body   string
}

// u904Upstream is both the fake apiserver (it serves the endpointRef Secret)
// and the provider upstream the Secret points at. It counts every request by
// "METHOD path".
type u904Upstream struct {
	srv    *httptest.Server
	mu     sync.Mutex
	routes map[string]u904Route
	count  map[string]int
}

func u904NewUpstream(t *testing.T, routes map[string]u904Route) *u904Upstream {
	t.Helper()
	u := &u904Upstream{routes: routes, count: map[string]int{}}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+u904AuthnNS+"/secrets/"+u904Secret {
			b, _ := json.Marshal(corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: u904Secret, Namespace: u904AuthnNS},
				Data:       map[string][]byte{"server-url": []byte(u.srv.URL)},
			})
			_, _ = w.Write(b)
			return
		}
		k := r.Method + " " + r.URL.Path
		u.mu.Lock()
		u.count[k]++
		rt, ok := u.routes[k]
		u.mu.Unlock()
		if !ok {
			rt = u904Route{status: http.StatusOK, body: `{}`}
		}
		w.WriteHeader(rt.status)
		_, _ = w.Write([]byte(rt.body))
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *u904Upstream) n(method, path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.count[method+" "+path]
}

func (u *u904Upstream) writes() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for k, v := range u.count {
		if !strings.HasPrefix(k, "GET ") {
			n += v
		}
	}
	return n
}

func u904Stage(name, verb, path string) *templates.API {
	return &templates.API{
		Name:        name,
		Verb:        ptr.To(verb),
		Path:        path,
		EndpointRef: &templates.Reference{Name: u904Secret, Namespace: u904AuthnNS},
		ErrorKey:    ptr.To(name + "Err"),
	}
}

func u904After(s *templates.API, dep string) *templates.API {
	s.DependsOn = &templates.Dependency{Name: dep}
	return s
}

func u904Continue(s *templates.API) *templates.API {
	s.ContinueOnError = ptr.To(true)
	return s
}

type u904Row struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Reason    string `json:"reason"`
	DependsOn string `json:"dependsOn"`
}

type u904Result struct {
	dict map[string]any
	rows map[string]u904Row
	raw  string
}

func u904Resolve(t *testing.T, u *u904Upstream, stages []*templates.API, inert bool) u904Result {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	if inert {
		ctx = cache.WithInert(ctx)
	}
	ctx, outcomes := WithStageOutcomes(ctx)
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC:                  &rest.Config{Host: u.srv.URL},
		AuthnNS:             u904AuthnNS,
		Items:               stages,
		RESTActionNamespace: "ns-904",
		RESTActionName:      "ra-904",
	})
	raw := outcomes.HeaderValue()
	var rows []u904Row
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("X-Snowplow-Stage-Outcomes is not a JSON array: %v (%s)", err, raw)
	}
	byName := map[string]u904Row{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	return u904Result{dict: dict, rows: byName, raw: raw}
}

func (r u904Result) wantRow(t *testing.T, w u904Row) {
	t.Helper()
	if got, ok := r.rows[w.Name]; !ok || got != w {
		t.Errorf("stage %q outcome = %+v (listed=%v), want %+v; header=%s", w.Name, got, ok, w, r.raw)
	}
}

func (r u904Result) wantSucceeded(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if got, ok := r.rows[n]; ok {
			t.Errorf("stage %q succeeded and must not be listed; got %+v; header=%s", n, got, r.raw)
		}
	}
}

// The provider's delete lifecycle, explicit: read → detach (409) → remove.
func u904Lifecycle() []*templates.API {
	return []*templates.API{
		u904Stage("server", http.MethodGet, "/cloudservers/s1"),
		u904After(u904Stage("detach", http.MethodPost, "/cloudservers/s1/detach"), "server"),
		u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "detach"),
	}
}

func u904ConflictRoutes() map[string]u904Route {
	return map[string]u904Route{
		"GET /cloudservers/s1":         {http.StatusOK, `{"id":"s1"}`},
		"POST /cloudservers/s1/detach": {http.StatusConflict, `{"kind":"Status","status":"Failure","code":409,"message":"volumes busy"}`},
	}
}

func TestIssue904_A_FailedWriteStopsTheDependentWrite(t *testing.T) {
	u := u904NewUpstream(t, u904ConflictRoutes())
	res := u904Resolve(t, u, u904Lifecycle(), false)
	if got := u.n(http.MethodPost, "/cloudservers/s1/detach"); got != 1 {
		t.Fatalf("PRE: the detach POST must be sent once, got %d", got)
	}
	if got := u.n(http.MethodDelete, "/cloudservers/s1"); got != 0 {
		t.Fatalf("#904 RED: the DELETE was sent %d time(s) after the detach it depends on failed", got)
	}
	if _, present := res.dict["remove"]; present {
		t.Errorf("the skipped stage must leave no value in the dict: %v", res.dict["remove"])
	}
	res.wantSucceeded(t, "server")
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonError})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "detach"})
}

func TestIssue904_B_ContinueOnErrorOnTheDependentSendsIt(t *testing.T) {
	u := u904NewUpstream(t, u904ConflictRoutes())
	stages := u904Lifecycle()
	u904Continue(stages[2])
	res := u904Resolve(t, u, stages, false)
	if got := u.n(http.MethodDelete, "/cloudservers/s1"); got != 1 {
		t.Fatalf("continueOnError on the dependent: the DELETE must be sent exactly once, got %d", got)
	}
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonError})
	res.wantSucceeded(t, "server", "remove")
}

// continueOnError on the DEPENDENCY does not let its dependent write through:
// the flag is read on the dependent write stage only.
func TestIssue904_B2_ContinueOnErrorOnTheDependencyDoesNotSendTheDependent(t *testing.T) {
	u := u904NewUpstream(t, u904ConflictRoutes())
	stages := u904Lifecycle()
	u904Continue(stages[1])
	res := u904Resolve(t, u, stages, false)
	if got := u.n(http.MethodDelete, "/cloudservers/s1"); got != 0 {
		t.Fatalf("#904 RED: continueOnError on the detach sent the dependent DELETE %d time(s)", got)
	}
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "detach"})
}

func u904IteratorLifecycle() []*templates.API {
	detach := u904Stage("detach", http.MethodPost, `${ "/cloudservers/s1/volumes/" + .id + "/detach" }`)
	detach.DependsOn = &templates.Dependency{Name: "server", Iterator: ptr.To(".server.volumes")}
	return []*templates.API{
		u904Stage("server", http.MethodGet, "/cloudservers/s1"),
		detach,
		u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "detach"),
	}
}

func TestIssue904_C_EmptyIteratorDependencySucceeded(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1","volumes":[]}`},
	})
	res := u904Resolve(t, u, u904IteratorLifecycle(), false)
	if got := u.writes() - u.n(http.MethodDelete, "/cloudservers/s1"); got != 0 {
		t.Fatalf("PRE: no volume, no detach POST; got %d other write(s)", got)
	}
	if got := u.n(http.MethodDelete, "/cloudservers/s1"); got != 1 {
		t.Fatalf("#904 RED: an empty-iterator detach SUCCEEDED, so the DELETE must be sent exactly once; got %d (header=%s)", got, res.raw)
	}
	res.wantSucceeded(t, "server", "detach", "remove")
	if res.raw != "[]" {
		t.Errorf("every stage succeeded: the header must be [], got %s", res.raw)
	}
}

func TestIssue904_D_OneOfNItemsFailingStopsTheDependent(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1":                    {http.StatusOK, `{"id":"s1","volumes":[{"id":"v1"},{"id":"v2"},{"id":"v3"}]}`},
		"POST /cloudservers/s1/volumes/v2/detach": {http.StatusInternalServerError, `{"kind":"Status","status":"Failure","code":500}`},
	})
	res := u904Resolve(t, u, u904IteratorLifecycle(), false)
	for _, v := range []string{"v1", "v2", "v3"} {
		if got := u.n(http.MethodPost, "/cloudservers/s1/volumes/"+v+"/detach"); got != 1 {
			t.Fatalf("PRE: the detach of %s must be sent once (#313 C-A: items do not cancel each other), got %d", v, got)
		}
	}
	if got := u.n(http.MethodDelete, "/cloudservers/s1"); got != 0 {
		t.Fatalf("#904 RED: one of 3 detaches failed, yet the DELETE was sent %d time(s)", got)
	}
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonError})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "detach"})
}

func TestIssue904_E_SkipPropagatesThreeLevels(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusBadRequest, `{"kind":"Status","status":"Failure","code":400}`},
	})
	stages := []*templates.API{
		u904Stage("server", http.MethodGet, "/cloudservers/s1"),
		u904After(u904Stage("stop", http.MethodPost, "/cloudservers/s1/stop"), "server"),
		u904After(u904Stage("resize", http.MethodPut, "/cloudservers/s1/size"), "stop"),
		u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "resize"),
	}
	res := u904Resolve(t, u, stages, false)
	if got := u.writes(); got != 0 {
		t.Fatalf("#904 RED: %d write(s) sent down a chain whose first read failed (counts=%v)", got, u.count)
	}
	res.wantRow(t, u904Row{Name: "server", Reason: StageReasonError})
	res.wantRow(t, u904Row{Name: "stop", Reason: StageReasonSkipped, DependsOn: "server"})
	res.wantRow(t, u904Row{Name: "resize", Reason: StageReasonSkipped, DependsOn: "stop"})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "resize"})
}

func TestIssue904_F_ReadOnlyRESTActionUnchanged(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /a": {http.StatusBadRequest, `{"kind":"Status","status":"Failure","code":400}`},
		"GET /b": {http.StatusOK, `{"ok":true}`},
	})
	stages := []*templates.API{
		u904Stage("a", http.MethodGet, "/a"),
		u904After(u904Stage("b", http.MethodGet, "/b"), "a"),
	}
	res := u904Resolve(t, u, stages, false)
	if got := u.n(http.MethodGet, "/b"); got != 1 {
		t.Fatalf("a read stage over a failed stage must still run (progressive rendering), got %d", got)
	}
	if b, _ := res.dict["b"].(map[string]any); b["ok"] != true {
		t.Errorf("the later read's value must be in the dict: %v", res.dict["b"])
	}
	if _, ok := res.dict["aErr"]; !ok {
		t.Errorf("the failed read's error must stay under its errorKey: %v", res.dict)
	}
	res.wantRow(t, u904Row{Name: "a", Reason: StageReasonError})
	res.wantSucceeded(t, "b")
}

func TestIssue904_G_IteratorWriteOverAFailedStageSendsNone(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusBadRequest, `{"kind":"Status","status":"Failure","code":400,"volumes":[{"id":"v1"},{"id":"v2"}]}`},
	})
	res := u904Resolve(t, u, u904IteratorLifecycle(), false)
	if got := u.writes(); got != 0 {
		t.Fatalf("#904 RED: %d write(s) sent by an iterator stage over a failed stage (counts=%v)", got, u.count)
	}
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonSkipped, DependsOn: "server"})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "detach"})
}

func TestIssue904_H_DryRunStillReportsNotExecuted(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1"}`},
	})
	res := u904Resolve(t, u, u904Lifecycle(), true)
	if got := u.writes(); got != 0 {
		t.Fatalf("a dry run must send no write, got %d", got)
	}
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonNotExecuted})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonNotExecuted})
}

// I — a userAccessFilter stage whose ServiceAccount endpoint cannot be acquired
// answers an empty result, but it FAILED (#760): its dependent write is not sent.
func TestIssue904_I_UAFStageWithoutSAEndpointFailed(t *testing.T) {
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) { return nil, errors.New("token file unreadable") }
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	u := u904NewUpstream(t, nil)
	stages := []*templates.API{
		{Name: "compositions", Path: "/apis/composition.krateo.io/v1/compositions",
			UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "list", Group: "composition.krateo.io", Resource: "compositions"}},
		u904After(u904Stage("cleanup", http.MethodDelete, "/cleanup"), "compositions"),
	}
	res := u904Resolve(t, u, stages, false)
	if got := u.writes(); got != 0 {
		t.Fatalf("#904 RED: %d write(s) sent after a UAF stage that could not acquire its endpoint", got)
	}
	res.wantRow(t, u904Row{Name: "compositions", Reason: StageReasonError})
	res.wantRow(t, u904Row{Name: "cleanup", Reason: StageReasonSkipped, DependsOn: "compositions"})
}

// A nested resolve never reports into the top-level collector: the first
// resolve on the ctx claims it, a second one records privately.
func TestIssue904_OutcomesClaimedByTheTopLevelResolveOnly(t *testing.T) {
	ctx, top := WithStageOutcomes(context.Background())
	if got := claimStageOutcomes(ctx); got != top {
		t.Fatal("the first resolve on the ctx must claim the installed collector")
	}
	if got := claimStageOutcomes(ctx); got == top || got == nil {
		t.Fatal("a later (nested) resolve must record into a private collector")
	}
}

func TestIssue904_IsWriteVerb(t *testing.T) {
	for v, want := range map[string]bool{
		"GET": false, "get": false, "HEAD": false, "OPTIONS": false, "TRACE": false,
		"POST": true, "PUT": true, "PATCH": true, "DELETE": true, "post": true, "CONNECT": true,
	} {
		if got := isWriteVerb(ptr.To(v)); got != want {
			t.Errorf("isWriteVerb(%q)=%v, want %v", v, got, want)
		}
	}
	if isWriteVerb(nil) {
		t.Error("a nil verb is GET")
	}
}
