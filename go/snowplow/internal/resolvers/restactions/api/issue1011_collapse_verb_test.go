package api

// issue1011_collapse_verb_test.go — #1011: the cluster-list collapse replaces
// an iterator stage's per-item calls with ONE cluster-scope GET LIST. It is a
// read optimisation, so it may only stand in for calls that are themselves a
// plain GET. Before the fix it had no verb gate: once the cluster-list cell
// was warm, a write iterator (POST per item to a namespaced collection) was
// rewritten into that GET, and its writes were silently never sent (200, an
// empty X-Snowplow-Stage-Outcomes).
//
// The upstream is an httptest server that counts every request by
// "METHOD path"; "sent" is a count at the upstream, never an inference from
// the dict.
//
// ARMS
//   W  a write iterator sends every write on every call: cold (first call),
//      and again once the cell is warm. RED on main 85658423: the second call
//      sends 0.
//   WE the same write iterator through a named endpointRef: a named endpoint
//      is not excluded from the collapse, so it is gated by the same rule.
//   R  a read iterator still collapses once the cell is warm: the second call
//      sends no per-item GET to the upstream (no read regression).
//   H  a HEAD iterator is not rewritten into a GET either: every HEAD is sent
//      on every call.

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const (
	ns1011     = "krateo-system"
	extRef1011 = "ext-1011"
	path1011   = `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`
	iter1011   = `[{"ns":"team-a"},{"ns":"team-b"},{"ns":"team-c"}]`
)

// up1011 is the upstream every stage dials: it counts requests by
// "METHOD path" and answers 200 with an empty list.
type up1011 struct {
	srv   *httptest.Server
	mu    sync.Mutex
	count map[string]int
}

func newUp1011(t *testing.T) *up1011 {
	t.Helper()
	u := &up1011{count: map[string]int{}}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.count[r.Method+" "+r.URL.Path]++
		u.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","metadata":{},"items":[]}`))
	}))
	t.Cleanup(u.srv.Close)
	return u
}

// sent is the number of requests with method to the namespaced widgets path
// of each iterator namespace.
func (u *up1011) sent(method string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, ns := range []string{"team-a", "team-b", "team-c"} {
		n += u.count[method+" /apis/widgets.krateo.io/v1/namespaces/"+ns+"/widgets"]
	}
	return n
}

// setup1011 builds a cache-on watcher whose user may list widgets
// cluster-wide (collapse gate 5), turns the collapse on, and publishes the
// user's clientconfig and a named endpoint Secret, both pointing at u.
func setup1011(t *testing.T, u *up1011) context.Context {
	t.Helper()
	newClusterListWatcher(t)
	withClusterListCollapseEnabledForTest(t)
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	ResetClusterListAsyncStateForTest()
	t.Cleanup(ResetClusterListAsyncStateForTest)
	cache.ResetClusterListCellCountersForTest()
	populated1011 = make(chan struct{}, 4)
	prevPopulate := populateClusterListCellSyncFn
	populateClusterListCellSyncFn = func(ctx context.Context, log *slog.Logger, a *templates.API, gvr schema.GroupVersionResource,
		key string, call httpcall.RequestOptions, store *cache.ResolvedCacheStore) bool {
		defer func() { populated1011 <- struct{}{} }()
		return prevPopulate(ctx, log, a, gvr, key, call, store)
	}
	t.Cleanup(func() { populateClusterListCellSyncFn = prevPopulate })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	own := kubeutil.MakeDNS1123Compatible(clusterListBroadUser) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		own: {ObjectMeta: metav1.ObjectMeta{Namespace: ns1011, Name: own},
			Data: map[string][]byte{"server-url": []byte(u.srv.URL), "token": []byte("tok")}},
		extRef1011: {ObjectMeta: metav1.ObjectMeta{Namespace: ns1011, Name: extRef1011},
			Data: map[string][]byte{"server-url": []byte(u.srv.URL), "token": []byte("ext-tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest(ns1011)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	return ctxWithUser(clusterListBroadUser)
}

func stage1011(verb string) *templates.API {
	s := &templates.API{
		Name:            "notify",
		Verb:            ptr.To(verb),
		Path:            path1011,
		DependsOn:       &templates.Dependency{Iterator: ptr.To(iter1011)},
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("notifyErr"),
	}
	if verb == http.MethodPost {
		s.Payload = ptr.To(`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"name":"exported"}}`)
	}
	return s
}

// resolve1011 runs one /call-shaped resolve and returns the stage-outcomes
// header.
func resolve1011(t *testing.T, ctx context.Context, u *up1011, stage *templates.API) string {
	t.Helper()
	ctx, outcomes := WithStageOutcomes(ctx)
	_, _ = ResolveWithFailure(ctx, ResolveOptions{
		RC:                  &rest.Config{Host: u.srv.URL},
		AuthnNS:             ns1011,
		Items:               []*templates.API{stage},
		RESTActionNamespace: ns1011,
		RESTActionName:      "ra-1011",
	})
	raw := outcomes.HeaderValue()
	var rows []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("X-Snowplow-Stage-Outcomes is not a JSON array: %v (%s)", err, raw)
	}
	return raw
}

// populated1011 receives once per async cluster-list populate that a cold call
// spawned (setup1011 wraps the populate seam).
var populated1011 chan struct{}

// warmCell1011 populates the stage's cluster-list cell the way the boot
// prewarm does, so the next call finds it warm; and when an earlier cold call
// spawned the async populate (the cold-fallback counter, reset by setup1011, moved),
// it waits for that populate to end, so no populate outlives the arm.
func warmCell1011(t *testing.T, ctx context.Context) {
	t.Helper()
	read := stage1011(http.MethodGet)
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	call := buildClusterListCall(read, endpoints.Endpoint{}, f1WidgetsGVR)
	if !populateClusterListCellSync(ctx, clusterListLogger(t), read, f1WidgetsGVR, contentKey, call, cache.ResolvedCache()) {
		t.Fatal("setup: the cluster-list cell could not be populated")
	}
	if _, cold := cache.ClusterListCellCounters(); cold > 0 {
		select {
		case <-populated1011:
		case <-time.After(10 * time.Second):
			t.Fatal("setup: the cold call's async populate never ended")
		}
	}
}

func writesOnEveryCall1011(t *testing.T, named bool) {
	u := newUp1011(t)
	ctx := setup1011(t, u)
	stage := stage1011(http.MethodPost)
	if named {
		stage.EndpointRef = &templates.Reference{Name: extRef1011, Namespace: ns1011}
	}

	resolve1011(t, ctx, u, stage)
	if n := u.sent(http.MethodPost); n != 3 {
		t.Fatalf("cold call: %d POSTs reached the upstream, want 3 (one per item)", n)
	}
	// The verb gate comes first (#1012 review M12): a write's cold call does
	// not take the cold-cell path at all — no cold fallback, no async
	// cluster-scope populate scheduled for it — and the deny gate is 9.
	if _, cold := cache.ClusterListCellCounters(); cold != 0 {
		<-populated1011 // let the populate it scheduled end before failing
		t.Fatalf("#1011 W RED (M12): a write's cold call took the cold-cell path (cold fallbacks %d) and scheduled a cluster-scope populate", cold)
	}
	if n := len(populated1011); n != 0 {
		t.Fatalf("#1011 W RED (M12): a write's cold call scheduled %d cluster-scope populate(s)", n)
	}
	if _, used, gate := attemptClusterListCollapse(ctx, clusterListLogger(t), stage, map[string]any{},
		endpoints.Endpoint{}, cache.ResolvedCache(), true); used || gate != 9 {
		t.Fatalf("#1011 W RED (M12): a write stage's collapse decision is used=%v deny gate %d, want false and 9", used, gate)
	}
	warmCell1011(t, ctx)
	raw := resolve1011(t, ctx, u, stage)
	if n := u.sent(http.MethodPost) - 3; n != 3 {
		t.Fatalf("#1011 RED: the call after the cluster-list cell warmed sent %d POSTs, want 3; outcomes %s", n, raw)
	}
	if get := u.sent(http.MethodGet); get != 0 {
		t.Fatalf("a write iterator sent %d per-item GETs; it must send only its writes", get)
	}
}

func TestIssue1011_W_AWriteIteratorSendsEveryWriteOnEveryCall(t *testing.T) {
	writesOnEveryCall1011(t, false)
}

func TestIssue1011_WE_ANamedEndpointWriteIteratorSendsEveryWriteOnEveryCall(t *testing.T) {
	writesOnEveryCall1011(t, true)
}

func TestIssue1011_R_AReadIteratorStillCollapsesWhenWarm(t *testing.T) {
	u := newUp1011(t)
	ctx := setup1011(t, u)
	stage := stage1011(http.MethodGet)
	warmCell1011(t, ctx)
	warm0, _ := cache.ClusterListCellCounters()
	resolve1011(t, ctx, u, stage)
	if warm, _ := cache.ClusterListCellCounters(); warm != warm0+1 {
		t.Fatalf("a GET iterator did not take the cluster-list collapse on a warm cell (cell-warm counter %d → %d)", warm0, warm)
	}
	if n := u.sent(http.MethodGet); n != 0 {
		t.Fatalf("a collapsed GET iterator sent %d per-item GETs to the upstream, want 0", n)
	}
}

func TestIssue1011_H_AHeadIteratorIsNotRewrittenIntoAGet(t *testing.T) {
	u := newUp1011(t)
	ctx := setup1011(t, u)
	stage := stage1011(http.MethodHead)
	warmCell1011(t, ctx)
	raw := resolve1011(t, ctx, u, stage)
	if n := u.sent(http.MethodHead); n != 3 {
		t.Fatalf("a HEAD iterator on a warm cell sent %d HEADs, want 3 (one per item); outcomes %s", n, raw)
	}
	if strings.Contains(raw, "notify") {
		t.Fatalf("the HEAD stage reported a failure: %s", raw)
	}
}

// TestIssue1011_P_ThePrewarmRosterAdmitsOnlyPlainGetStages: the boot prewarm
// seeds a cluster-list cell only for a stage the collapse admits. A seeded
// cell is what makes a later customer call collapse, so the roster and the
// collapse's verb gate must never disagree.
func TestIssue1011_P_ThePrewarmRosterAdmitsOnlyPlainGetStages(t *testing.T) {
	const path = "/apis/widgets.krateo.io/v1/namespaces/team-a/widgets"
	for _, c := range []struct {
		verb *string
		want bool
	}{
		{nil, true},
		{ptr.To(http.MethodGet), true},
		{ptr.To(http.MethodHead), false},
		{ptr.To(""), false},
		{ptr.To("get"), false},
		{ptr.To(http.MethodPost), false},
		{ptr.To(http.MethodDelete), false},
	} {
		stage := &templates.API{Name: "s", Path: path, Verb: c.verb,
			DependsOn: &templates.Dependency{Iterator: ptr.To(iter1011)}}
		roster := EnumerateClusterListCells(context.Background(), nil,
			[]*templates.RESTAction{{Spec: templates.RESTActionSpec{API: []*templates.API{stage}}}})
		if got := len(roster.Cells) == 1; got != c.want {
			t.Errorf("verb %v: seeded=%v, want %v", ptr.Deref(c.verb, "<nil>"), got, c.want)
		}
	}
}

// TestIssue1011_B_OnlyThePlainGetGatedCallersBuildTheCollapsedCall: the
// collapsed call is built only where a plain-GET gate has run, read from the
// production source of the module (no _test.go file):
//   - buildClusterListCallQ is called only by attemptClusterListCollapse (its
//     verb gate, deny gate 9) and PrewarmClusterListCells, plus the
//     buildClusterListCall wrapper, which no production code calls;
//   - a prewarm roster entry is constructed only by EnumerateClusterListCells,
//     whose stage filter is the same predicate (arm P).
//
// RED when a new caller builds the collapsed call, or a roster entry is made
// anywhere else.
func TestIssue1011_B_OnlyThePlainGetGatedCallersBuildTheCollapsedCall(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..")) // the module root (go/snowplow)
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string][]string{} // name -> enclosing functions
	names := map[string]bool{"buildClusterListCallQ": true, "buildClusterListCall": true,
		"ClusterListCellEntry": true, "ClusterListCellRoster": true}
	fset := token.NewFileSet()
	files := 0
	err = filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			switch de.Name() {
			case "testdata", "vendor", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		files++
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				var name string
				switch x := n.(type) {
				case *ast.CallExpr:
					switch fn := x.Fun.(type) {
					case *ast.Ident:
						name = fn.Name
					case *ast.SelectorExpr:
						name = fn.Sel.Name
					}
				case *ast.CompositeLit:
					switch ty := x.Type.(type) {
					case *ast.Ident:
						name = ty.Name
					case *ast.SelectorExpr:
						name = ty.Sel.Name
					}
				}
				if names[name] {
					refs[name] = append(refs[name], fd.Name.Name)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("the scan read %d files: it is not looking at the production tree", files)
	}
	want := map[string][]string{
		"buildClusterListCallQ": {"PrewarmClusterListCells", "attemptClusterListCollapse", "buildClusterListCall"},
		"buildClusterListCall":  nil,
		"ClusterListCellEntry":  {"EnumerateClusterListCells"},
		"ClusterListCellRoster": {"EnumerateClusterListCells"},
	}
	for name, w := range want {
		got := append([]string(nil), refs[name]...)
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(w, ",") {
			t.Errorf("#1011 B RED: %s is built/called from %v in production code, want exactly %v", name, got, w)
		}
	}
}
