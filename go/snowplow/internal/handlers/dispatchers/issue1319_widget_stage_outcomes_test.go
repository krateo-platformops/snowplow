package dispatchers

// issue1319_widget_stage_outcomes_test.go — #1319: a widget /call answers
// X-Snowplow-Stage-Outcomes for its apiRef RESTAction's stages, PRESENT ONLY
// when a stage did not succeed, on a cold resolve and on an L1 hit.
//
// Through the REAL widgetsHandler.ServeHTTP → widgets.Resolve → apiref.Resolve
// → restactions resolve, and the REAL L1: the #681 harness (a per-user
// httptest apiserver that authorizes every dial with the real EvaluateRBAC).
// The widget's apiRef RESTAction (continueOnError stages) reads x/y, which A
// may not read (an apiserver 403 on A's own dial), and pub/p (served
// in-process).
//
// THE ORACLE IS NOT THE INSTRUMENT: expected rows come from the stages the
// arm declares and from what each upstream answers, decoded by this file's own
// decoder; the closed code set is this file's own list.
//
//   W1 every stage succeeds: no header, cold or hit;
//   W2 a stage cut by the page budget (Timeout): the header names it, cold;
//      the cell is NOT stored (the #1319 storage decision: the shared Put gate
//      declines a budget cut like every other non-denial stage error);
//   W3 a per-identity denial (Forbidden, #681 DenialOnly) is stored WITH its
//      outcome; the hit (no dial) answers the same header;
//   W4 the codes are a closed set and carry no identity or upstream text;
//   W5 a refetch that completes has no header: after the cut, the next /call
//      resolves complete; and a refresher re-resolve that completes replaces a
//      cell stored with a denial, whose next hit has no header;
//   W6 the refresher keeps a widget cell's outcomes when its re-resolve is
//      incomplete (and stored): the next hit has the header.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// w1319Codes is the closed reason-code set the frontend doc publishes
// (docs/howto-frontend-widget-stage-outcomes.md §3), written out here.
var w1319Codes = map[string]bool{
	"StageNotExecuted": true, "WriteRefused": true, "Forbidden": true, "NotFound": true, "NotFoundForRequester": true,
	"Unauthorized": true, "NotRun": true, "Error": true, "Skipped": true, "Aborted": true,
	"InvalidPath": true, "TemplateError": true, "NotSent": true, "Timeout": true,
	"DestinationRefused": true, "UserAccessFilterOnNonReadVerb": true,
}

type w1319Env struct {
	*c681Env
	widget *unstructured.Unstructured
	// granting answers every dial 200 (an upstream that grants what
	// snowplow's RBAC copy would deny): A's x/y read then completes.
	granting *httptest.Server
	grants   atomic.Int64
	granted  atomic.Bool
}

// w1319Setup is the #681 harness plus: the apiRef RESTAction in the informer
// (so apiref.Resolve reads it as production does), a widget pointing at it,
// and a binding that lets all681 read widgets (so the widget key carries a
// binding set and is cacheable).
func w1319Setup(t *testing.T, raPaths ...string) *w1319Env {
	t.Helper()
	return w1319SetupRA(t, c681RAObj(raPaths...))
}

// w1319AbortedRA is a RESTAction whose stage depends on a stage that does not
// exist: its stages cannot be ordered, so the resolve runs none (an Aborted
// resolve, api.abortedResolve) and records no stage error.
func w1319AbortedRA() *unstructured.Unstructured {
	ra := c681RAObj("/api/v1/namespaces/pub/configmaps/p")
	ra.Object["spec"].(map[string]any)["api"].([]any)[0].(map[string]any)["dependsOn"] = map[string]any{"name": "ghost"}
	return ra
}

func w1319SetupRA(t *testing.T, ra *unstructured.Unstructured) *w1319Env {
	t.Helper()
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "false")
	e := &w1319Env{c681Env: c681Setup(t)}
	e.create(t, h1RAGVR, ra)
	e.create(t, c681CRGVR, c681CR("widget-reader1319",
		rbacv1.PolicyRule{APIGroups: []string{h1WidgetGVR.Group}, Resources: []string{h1WidgetGVR.Resource}, Verbs: []string{"get", "list"}}))
	e.create(t, c681CRBGVR, &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "widget-reader1319", UID: types.UID("uid-widget-reader1319")},
		Subjects:   []rbacv1.Subject{c681Group(c681All)},
		RoleRef:    rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "ClusterRole", Name: "widget-reader1319"}})
	if _, ch := e.rw.EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("SETUP: restactions informer did not sync")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !e.rw.IsServable(h1RAGVR) || !w1319Cacheable(c681A.ctx(), e) {
		if time.Now().After(deadline) {
			t.Fatalf("SETUP: restactions servable=%v, widget key cacheable=%v", e.rw.IsServable(h1RAGVR), w1319Cacheable(c681A.ctx(), e))
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.widget = h1WidgetUnstructured(map[string]any{
		"apiRef": map[string]any{"name": c681RA, "namespace": h1NS, "resource": h1RAGVR.Resource,
			"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version},
		// The widget renders its RESTAction's stages, so its body shows what
		// completed (the cache must not constrain the widget's JQ).
		"widgetDataTemplate": []any{
			map[string]any{"forPath": "x", "expression": "${ .sa }"},
			map[string]any{"forPath": "pub", "expression": "${ .sb }"},
		}})
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: e.widget.DeepCopy()}
	})
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	r3 := widgets.SetValidateObjectStatusForTest(func(context.Context, *rest.Config, map[string]any) error { return nil })
	t.Cleanup(func() { r3(); r2(); r1() })

	e.granting = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.grants.Add(1)
		seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":%q,"name":%q},"data":{"v":"GRANTED-1319"}}`, seg[3], seg[len(seg)-1])
	}))
	t.Cleanup(e.granting.Close)
	return e
}

func w1319Cacheable(ctx context.Context, e *w1319Env) bool {
	_, h, in := e.widgetKey(ctx)
	return h != nil && in != nil && in.BindingUID != ""
}

func (e *w1319Env) widgetKey(ctx context.Context) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	w := h1WidgetUnstructured(nil)
	if e.widget != nil {
		w = e.widget
	}
	return dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, w.Object, nil))
}

// pointAtGranting points A's clientconfig at the granting upstream (on) or
// back at the #681 apiserver (off).
func (e *w1319Env) pointAtGranting(t *testing.T, on bool) {
	t.Helper()
	secrets := map[string]*corev1.Secret{}
	for _, i := range []ident681{c681A, c681B, c681C} {
		url := e.user.URL
		if on && i.user == c681A.user {
			url = e.granting.URL
		}
		n := kubeutil.MakeDNS1123Compatible(i.user) + "-clientconfig"
		secrets[n] = &corev1.Secret{ObjectMeta: authnMeta(c681AuthnNS, n, i.user),
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte(i.token())}}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: secrets})
	e.granted.Store(on)
}

// call serves one widget /call under ctx; it returns the recorder and the
// dials A's #681 apiserver received.
func (e *w1319Env) call(t *testing.T, ctx context.Context) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	d0 := e.dials.Load()
	h := &widgetsHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Fatalf("widget /call: status %d body=%s", rec.Code, rec.Body.String())
	}
	return rec, e.dials.Load() - d0
}

// ctx is A's request ctx as the userconfig middleware builds it: identity
// plus A's own endpoint (the one its clientconfig names).
func (e *w1319Env) ctx() context.Context {
	url := e.user.URL
	if e.granted.Load() {
		url = e.granting.URL
	}
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: c681A.user, Groups: c681A.groups}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: url, Token: c681A.token()}))
}

// cutCtx is A's request whose page budget is already spent: every exchange it
// makes is cut (#1115) before it is sent.
func (e *w1319Env) cutCtx() context.Context {
	return pagebudget.WithCustomer(e.ctx(), time.Now().Add(-2*pagebudget.PageMaxGoal))
}

func (e *w1319Env) cell(t *testing.T, ctx context.Context) (*cache.ResolvedEntry, bool) {
	t.Helper()
	k, _, _ := e.widgetKey(ctx)
	return cache.ResolvedCache().GetNoTouch(k)
}

type w1319Row struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Reason    string `json:"reason"`
	DependsOn string `json:"dependsOn"`
}

// w1319Rows decodes a PRESENT header with this file's own decoder, and holds
// every row to the closed set (W4) and to no identity text.
func w1319Rows(t *testing.T, what string, rec *httptest.ResponseRecorder) map[string]w1319Row {
	t.Helper()
	vs := rec.Header().Values(util.HeaderStageOutcomes)
	if len(vs) != 1 {
		t.Fatalf("%s: %d %s values, want exactly one (status %d)", what, len(vs), util.HeaderStageOutcomes, rec.Code)
	}
	var rows []w1319Row
	if err := json.Unmarshal([]byte(vs[0]), &rows); err != nil {
		t.Fatalf("%s: %s=%q is not a JSON array: %v", what, util.HeaderStageOutcomes, vs[0], err)
	}
	if len(rows) == 0 {
		t.Fatalf("%s: a PRESENT widget header listed no stage (%q): absent must mean complete, present must mean incomplete", what, vs[0])
	}
	by := map[string]w1319Row{}
	for _, r := range rows {
		if r.OK {
			t.Fatalf("%s: row %+v reads ok:true; the header lists only stages that did not succeed", what, r)
		}
		if !w1319Codes[r.Reason] {
			t.Fatalf("W4 RED (%s): reason %q is outside the closed set", what, r.Reason)
		}
		by[r.Name] = r
	}
	c681NoIdentity(t, what+"'s header", []byte(vs[0]))
	if strings.Contains(strings.ToLower(vs[0]), "forbidden:") || strings.Contains(vs[0], "cannot get") {
		t.Fatalf("W4 RED (%s): the header carries upstream text: %s", what, vs[0])
	}
	return by
}

func w1319Absent(t *testing.T, what string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if vs := rec.Header().Values(util.HeaderStageOutcomes); len(vs) != 0 {
		t.Fatalf("%s: %s = %q, want it ABSENT (every stage completed)", what, util.HeaderStageOutcomes, vs)
	}
}

func w1319Served(t *testing.T, what string, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := rec.Header().Get(ServeOutcomeHeader); got != want {
		t.Fatalf("SETUP (%s): %s=%q, want %q", what, ServeOutcomeHeader, got, want)
	}
}

func TestIssue1319_W1_AllStagesCompleteHasNoHeaderColdOrHit(t *testing.T) {
	e := w1319Setup(t, "/api/v1/namespaces/pub/configmaps/p")
	cold, _ := e.call(t, e.ctx())
	w1319Served(t, "cold", cold, "cold")
	if !strings.Contains(cold.Body.String(), "PUBDATA-681") {
		t.Fatalf("SETUP: the widget body must carry its RESTAction's data: %s", s425Trunc(cold.Body.Bytes()))
	}
	w1319Absent(t, "W1 cold", cold)
	cell, ok := e.cell(t, e.ctx())
	if !ok {
		t.Fatal("SETUP: a complete widget result must be stored")
	}
	if cell.StageOutcomes != "" {
		t.Fatalf("W1 RED: a complete cell stored outcomes %q", cell.StageOutcomes)
	}
	hit, _ := e.call(t, e.ctx())
	w1319Served(t, "hit", hit, "fresh")
	w1319Absent(t, "W1 hit", hit)
}

func TestIssue1319_W2_ABudgetCutStageIsPresentColdAndNotStored(t *testing.T) {
	e := w1319Setup(t, "/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	rec, dials := e.call(t, e.cutCtx())
	if dials != 0 {
		t.Fatalf("SETUP: a spent budget must send nothing; A's apiserver got %d dials", dials)
	}
	rows := w1319Rows(t, "W2 cold", rec)
	if r, ok := rows["sa"]; !ok || r.Reason != "Timeout" {
		t.Fatalf("W2 RED: the cut stage sa reads %+v (present=%v), want reason Timeout; rows=%v", r, ok, rows)
	}
	if _, ok := rows["sb"]; ok || len(rows) != 1 {
		t.Fatalf("W2 RED: the header lists a stage that succeeded: %v", rows)
	}
	if !strings.Contains(rec.Body.String(), "PUBDATA-681") {
		t.Fatalf("W2: the partial body must still carry the stage that completed: %s", s425Trunc(rec.Body.Bytes()))
	}
	if _, ok := e.cell(t, e.ctx()); ok {
		t.Fatal("W2 storage decision RED: a budget-cut result was stored; the next user would be served the partial body as a cell nothing marks for replacement")
	}
}

func TestIssue1319_W3_ADenialIsStoredWithItsOutcomeAndReplayedOnHit(t *testing.T) {
	e := w1319Setup(t, "/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	cold, dials := e.call(t, e.ctx())
	if dials != 1 {
		t.Fatalf("SETUP: A's cold /call made %d dials, want 1 (the 403)", dials)
	}
	rows := w1319Rows(t, "W3 cold", cold)
	if r := rows["sa"]; r.Reason != "Forbidden" || len(rows) != 1 {
		t.Fatalf("W3: cold rows %v, want exactly sa Forbidden", rows)
	}
	cell, ok := e.cell(t, e.ctx())
	if !ok {
		t.Fatal("SETUP: a denial-only widget result must be stored (#681)")
	}
	if cell.StageOutcomes != cold.Header().Get(util.HeaderStageOutcomes) {
		t.Fatalf("W3 RED: the cell stores %q, the cold answer said %q", cell.StageOutcomes, cold.Header().Get(util.HeaderStageOutcomes))
	}
	hit, dials := e.call(t, e.ctx())
	w1319Served(t, "hit", hit, "fresh")
	if dials != 0 {
		t.Fatalf("SETUP: the hit dialled %d times", dials)
	}
	if got, want := hit.Header().Get(util.HeaderStageOutcomes), cold.Header().Get(util.HeaderStageOutcomes); got != want {
		t.Fatalf("W3 RED: the hit answers %s=%q, want the cold %q", util.HeaderStageOutcomes, got, want)
	}
	w1319Rows(t, "W3 hit", hit)
}

func TestIssue1319_W5_ARefetchThatCompletesHasNoHeader(t *testing.T) {
	t.Run("after a budget cut", func(t *testing.T) {
		e := w1319Setup(t, "/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
		e.pointAtGranting(t, true)
		cut, _ := e.call(t, e.cutCtx())
		if r := w1319Rows(t, "W5 cut", cut)["sa"]; r.Reason != "Timeout" {
			t.Fatalf("SETUP: the cut call's sa reads %+v, want Timeout", r)
		}
		// The refetch: the upstream answers this time, every stage completes.
		// (A granted own-credential answer is an external touch, so it is
		// served live and not stored: each refetch resolves, and each is
		// complete.)
		for i := 0; i < 2; i++ {
			g0 := e.grants.Load()
			re, _ := e.call(t, e.ctx())
			if e.grants.Load() == g0 {
				t.Fatal("SETUP: the refetch did not reach the upstream")
			}
			if !strings.Contains(re.Body.String(), "GRANTED-1319") {
				t.Fatalf("SETUP: the refetch body must carry the completed stage: %s", s425Trunc(re.Body.Bytes()))
			}
			w1319Absent(t, fmt.Sprintf("W5 refetch %d", i+1), re)
		}
	})
	t.Run("after a refresher re-resolve", func(t *testing.T) {
		// A definite NotFound (#1080, a watched named object that does not
		// exist) is a per-identity answer: stored WITH its outcome, which reads
		// its own closed code NotFoundForRequester (#1319 ruling). The object
		// is then created; the refresher's re-resolve completes and replaces
		// the cell (a /refreshes frame), and the refetch has no header.
		e := w1319Setup(t, "/api/v1/namespaces/pub/configmaps/q")
		cold, _ := e.call(t, e.ctx())
		if r := w1319Rows(t, "W5 stored definite NotFound", cold)["sa"]; r.Reason != "NotFoundForRequester" {
			t.Fatalf("NotFoundForRequester RED: the definite NotFound reads %+v, want NotFoundForRequester", r)
		}
		if cell, ok := e.cell(t, e.ctx()); !ok || cell.StageOutcomes == "" {
			t.Fatalf("SETUP: the NotFound result must be stored with its outcome (present=%v)", ok)
		}
		e.create(t, c681CMs, c681CMObj("pub", "q", "Q-1319"))
		e.waitInformerHas(t, "pub", "q")
		e.refresh(t, e.ctx())
		hit, _ := e.call(t, e.ctx())
		w1319Served(t, "hit", hit, "fresh")
		if !strings.Contains(hit.Body.String(), "Q-1319") {
			t.Fatalf("SETUP: the refreshed cell must carry the completed stage: %s", s425Trunc(hit.Body.Bytes()))
		}
		w1319Absent(t, "W5 refetch of the refreshed cell", hit)
	})
}

func TestIssue1319_W6_TheRefresherKeepsAnIncompleteWidgetCellsOutcomes(t *testing.T) {
	e := w1319Setup(t, "/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	cold, _ := e.call(t, e.ctx())
	want := cold.Header().Get(util.HeaderStageOutcomes)
	w1319Rows(t, "W6 stored denial", cold)
	e.refresh(t, e.ctx())
	hit, dials := e.call(t, e.ctx())
	w1319Served(t, "hit", hit, "fresh")
	if dials != 0 {
		t.Fatalf("SETUP: the hit dialled %d times", dials)
	}
	if got := hit.Header().Get(util.HeaderStageOutcomes); got != want {
		t.Fatalf("W6 RED: the hit of the refreshed cell answers %q, want the refresher's own outcomes %q", got, want)
	}
}

// waitInformerHas polls until the configmaps informer serves ns/name.
func (e *w1319Env) waitInformerHas(t *testing.T, ns, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := e.rw.GetObject(context.Background(), c681CMs, ns, name); ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("SETUP: the informer never saw configmap %s/%s", ns, name)
}

// refresh runs the refresher's re-resolve of A's widget cell
// (resolveAndPopulateL1), its resolve the production widget re-resolve
// (resolveWidgetForRefresh) over the widget CR, and requires that it stored a
// new entry.
func (e *w1319Env) refresh(t *testing.T, ctx context.Context) {
	t.Helper()
	cell, ok := e.cell(t, ctx)
	if !ok || cell.Inputs == nil {
		t.Fatal("SETUP: no widget cell to refresh")
	}
	var resolved atomic.Int64
	restore := setResolveOnceForTest(func(rctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		resolved.Add(1)
		return resolveWidgetForRefresh(rctx, objects.Result{GVR: h1WidgetGVR, Unstructured: e.widget.DeepCopy()}, in, c681AuthnNS)
	})
	defer restore()
	ep := &endpoints.Endpoint{ServerURL: e.user.URL, Token: c681A.token()}
	if err := resolveAndPopulateL1(context.Background(), *cell.Inputs, ep, &rest.Config{Host: e.user.URL, BearerToken: c681A.token()}); err != nil {
		t.Fatalf("resolveAndPopulateL1: %v", err)
	}
	if resolved.Load() != 1 {
		t.Fatalf("SETUP: the refresher resolved %d times, want 1", resolved.Load())
	}
	if after, ok := e.cell(t, ctx); !ok || after == cell {
		t.Fatalf("SETUP: the refresher did not store a new entry (present=%v)", ok)
	}
}

// W7 — the external-TTL Put (a widget that opts into
// krateo.io/external-cache-ttl-seconds, #1221), on the #1221 harness (a
// third-party named endpoint, a per-user apiserver). It stores only a resolve
// with no stage error: a stage error beside a genuine external touch is never
// DenialOnly (cache.DenialOnly requires GenuineExternal == 0), so the verdict
// declines it. Its stored outcomes are therefore always complete:
//   - clean: the cell is stored under the external TTL with no outcomes, and
//     neither the cold answer nor the hit carries a header;
//   - a denial beside the third-party touch (bob's own 403): answered cold
//     with its header, and not stored.
func TestIssue1319_W7_TheExternalTTLPutStoresOnlyCompleteOutcomes(t *testing.T) {
	a := k1221Shapes[2] // informer_not_registered: bob's read is his own dial
	t.Run("clean", func(t *testing.T) {
		e := k1221Setup(t, a, k1221RA(k1221FeedStage()), k1221Widget("${ .feed.value }", true))
		cold := e.serve(e.aliceCtx)
		if cold.Code != http.StatusOK || e.wire.named.Load() != 1 {
			t.Fatalf("SETUP: alice's cold /call: code=%d named=%d", cold.Code, e.wire.named.Load())
		}
		w1319Absent(t, "W7 clean cold", cold)
		cell, ok := e.store.GetNoTouch(e.key(e.aliceCtx))
		if !ok || !cell.ExternalTTL {
			t.Fatalf("SETUP: the external-TTL Put must store the clean cell (present=%v)", ok)
		}
		if cell.StageOutcomes != "" {
			t.Fatalf("W7 RED: the clean external-TTL cell stores outcomes %q", cell.StageOutcomes)
		}
		hit := e.serve(e.aliceCtx)
		w1319Served(t, "hit", hit, "fresh")
		w1319Absent(t, "W7 clean hit", hit)
	})
	t.Run("a denial beside the third-party touch", func(t *testing.T) {
		e := k1221Setup(t, a, k1221RA(k1221FeedStage(), k1221TargetStage(a)), k1221Widget("${ .feed.value }", true))
		cold := e.serve(e.bobCtx)
		if cold.Code != http.StatusOK || e.wire.bob403.Load() != 1 || e.wire.named.Load() != 1 {
			t.Fatalf("SETUP: bob's cold /call: code=%d bob403=%d named=%d", cold.Code, e.wire.bob403.Load(), e.wire.named.Load())
		}
		if r := w1319Rows(t, "W7 denial cold", cold); r["target"].Reason != "Forbidden" || len(r) != 1 {
			t.Fatalf("W7 RED: rows %v, want exactly target Forbidden", r)
		}
		if _, ok := e.store.GetNoTouch(e.key(e.bobCtx)); ok {
			t.Fatal("W7 RED: a denial beside a genuine external touch was stored under the external TTL")
		}
	})
}

// C — the identity-free content shell keeps and replays the outcomes like
// every other cell. Reachable state: the walker's own sequence
// (withWidgetContentPutSinks → widgets.Resolve → populateWidgetContentL1,
// phase1_walk.go) over a widget with no templates (the class the content
// layer serves) whose RESTAction aborts; then the widgets handler's content
// hit. A complete RESTAction's shell answers no header.
func TestIssue1319_C_ContentShellKeepsAndReplaysOutcomes(t *testing.T) {
	for _, c := range []struct {
		name     string
		ra       *unstructured.Unstructured
		complete bool
	}{
		{"aborted", w1319AbortedRA(), false},
		{"complete", c681RAObj("/api/v1/namespaces/pub/configmaps/p"), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := w1319SetupRA(t, c.ra)
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			e.widget = h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": c681RA, "namespace": h1NS,
				"resource": h1RAGVR.Resource, "apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version}})
			if isRBACSensitiveApiRefWidget(e.widget.Object) {
				t.Fatal("SETUP: a template-less apiRef widget must be served by the content layer")
			}
			// The walker's resolve and populate.
			wctx := withWidgetContentPutSinks(e.ctx())
			in := e.widget.DeepCopy()
			res, err := widgets.Resolve(wctx, widgets.ResolveOptions{In: in.DeepCopy(), RC: &rest.Config{Host: e.user.URL},
				AuthnNS: c681AuthnNS, PerPage: -1, Page: -1})
			if err != nil {
				t.Fatalf("SETUP: the walker's widgets.Resolve: %v", err)
			}
			populateWidgetContentL1(wctx, h1WidgetGVR, in, -1, -1, res, 0)
			key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
			shell, ok := cache.ResolvedCache().GetNoTouch(key)
			if !ok {
				t.Fatal("SETUP: the walker's populate stored no shell")
			}
			if c.complete != (shell.StageOutcomes == "") {
				t.Fatalf("C RED: the %s shell stores outcomes %q", c.name, shell.StageOutcomes)
			}
			hit, _ := e.call(t, e.ctx())
			if got := hit.Header().Get(util.HeaderStageOutcomes); got != shell.StageOutcomes {
				t.Fatalf("C RED: the content hit answers %q, the shell stores %q (serve=%s)", got, shell.StageOutcomes, hit.Header().Get(ServeOutcomeHeader))
			}
			if c.complete {
				w1319Absent(t, "C complete content hit", hit)
			} else if r := w1319Rows(t, "C aborted content hit", hit); r[""].Reason != "Aborted" {
				t.Fatalf("SETUP: rows %v, want the Aborted row", r)
			}
		})
	}
}
