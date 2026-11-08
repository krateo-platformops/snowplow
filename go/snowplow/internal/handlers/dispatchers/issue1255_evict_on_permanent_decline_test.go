//go:build unit

package dispatchers

// issue1255_evict_on_permanent_decline_test.go — #1255, on the REAL widget
// /call (widgetsHandler.ServeHTTP → widgets.Resolve → apiRef → the RESTAction
// read from its informer) and the REAL refresher body (resolveAndPopulateL1 with
// the production resolveOnceFn), over the #1221 fakes: a per-credential apiserver
// and a named endpoint.
//
// The 057 shape (2026-11-08): a widget's cached body came from a RESTAction with
// no named-endpoint stage; portal#384 edited the RESTAction to add one. The
// refresher's re-resolve of the widget then touches an external endpoint, is
// declined (an external result is never stored), and — before #1255 — kept the
// pre-edit body "until the TTL".
//
//   W1  once the edit is observed and the refresh declines, the pre-edit body is
//       never served again: the cell is gone and every later /call (several,
//       the repeated stale serves of the report) answers with the post-edit
//       output. RED before the fix (the cell is kept and the next /call is an
//       L1 hit on the pre-edit body).
//   W2  a TRANSIENT decline — a stage error with no external touch, and a failed
//       resolve — keeps the prior entry, unsuppressed, as before.
//   W3  the eviction is announced to an armed /refreshes subscriber (the refresh
//       frame of a deletion), so an armed tab refetches.
//
// The rule, the epoch race guard, the external-TTL contract and the layered
// (raFullList) consumer remark are pinned on the store in
// internal/cache/issue1255_evict_unstorable_test.go.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	n1255PreEdit = "pre-edit-v0.2.0"
	// n1255Expr renders the named endpoint's answer once the RESTAction has the
	// stage, and the pre-edit marker while it has none.
	n1255Expr = `${ .feed.value // "` + n1255PreEdit + `" }`
)

type n1255Env struct {
	*k1221Env
	dyn k8sdynamic.Interface
	a   psArm
}

// n1255Setup is k1221Setup keeping the fake client, so the arm can edit the
// RESTAction, with the widget's own informer registered too (the refresher
// re-fetches the widget CR through it).
func n1255Setup(t *testing.T) *n1255Env {
	t.Helper()
	a := k1221Shapes[0] // informer_servable: the RESTAction's K8s stage is served in-process
	ra := k1221RA(k1221TargetStage(a))
	cr := k1221Widget(n1255Expr, false)

	k423Env(t)
	// The refresher's process-held resolve memo (#598) outlives a test's dep
	// tracker: without this, one arm's refresh could replay another arm's resolve.
	resetRefreshMemoForTest()
	t.Cleanup(resetRefreshMemoForTest)
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	crdschema.InvalidateCRDSchemaMemo()
	t.Cleanup(crdschema.InvalidateCRDSchemaMemo)
	dyn := psBuildWatcher(t, a, append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(cr))...)
	for _, g := range []schema.GroupVersionResource{h1RAGVR, h1WidgetGVR} {
		if _, ch := cache.Global().EnsureResourceType(g); ch != nil {
			select {
			case <-ch:
			case <-time.After(30 * time.Second):
				t.Fatalf("PRECONDITION: the %s informer did not sync", g.Resource)
			}
		}
	}
	w := &k1221Wire{}
	apiserver, endpoint := k1221Apiserver(t, a, w), k1221Endpoint(t, w)
	psSeedClientconfigs(t, apiserver.URL)
	byName := map[string]*corev1.Secret{
		k1221EP: {
			ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: k1221EP},
			Data:       map[string][]byte{"server-url": []byte(endpoint.URL), "token": []byte(k1221Token)},
		},
	}
	for _, u := range psUsers {
		byName[u+"-clientconfig"] = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: u + "-clientconfig"},
			Data:       map[string][]byte{"server-url": []byte(apiserver.URL), "token": []byte("tok-" + u)},
		}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: byName})
	alice := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: psAlice, Groups: psGroups(a)}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: apiserver.URL, Token: "tok-" + psAlice}),
		xcontext.WithAccessToken("jwt-"+psAlice))
	e := &k1221Env{t: t, cr: cr, saURL: apiserver.URL, endpointURL: endpoint.URL, wire: w,
		aliceCtx: alice, bobCtx: alice, store: cache.ResolvedCache()}
	return &n1255Env{k1221Env: e, dyn: dyn, a: a}
}

// edit replaces the RESTAction's stages and waits until its informer serves them.
func (e *n1255Env) edit(t *testing.T, stages ...map[string]any) {
	t.Helper()
	ra := k1221RA(stages...)
	ri := e.dyn.Resource(h1RAGVR).Namespace(h1NS)
	cur, err := ri.Get(context.Background(), psRAName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PRECONDITION: get the RESTAction: %v", err)
	}
	ra.SetResourceVersion(cur.GetResourceVersion())
	if _, err := ri.Update(context.Background(), ra, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("PRECONDITION: edit the RESTAction: %v", err)
	}
	want := len(stages)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := cache.Global().GetObject(context.Background(), h1RAGVR, h1NS, psRAName); ok {
			if api, _, _ := unstructured.NestedSlice(got.Object, "spec", "api"); len(api) == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("PRECONDITION: the RESTAction informer never observed the edit")
}

// refresh runs the refresher's body on the widget cell, exactly as a dequeue of
// its dirty mark does (the production resolveOnceFn, the SA transport).
func (e *n1255Env) refresh(t *testing.T, key string) {
	t.Helper()
	entry, ok := e.store.GetNoTouch(key)
	if !ok || entry == nil || entry.Inputs == nil {
		t.Fatal("PRECONDITION: the widget cell must be resident before its refresh")
	}
	saEP := &endpoints.Endpoint{ServerURL: e.saURL, Token: "tok-" + psAlice}
	if err := resolveAndPopulateL1(context.Background(), *entry.Inputs, saEP, &rest.Config{Host: e.saURL}); err != nil {
		t.Fatalf("PRECONDITION: the refresh must run to its Put-gate, got %v", err)
	}
}

// n1255Title is the widget's rendered output (status.widgetData.title). The spec
// carries the expression, marker included, so only the status tells the pre-edit
// body from the post-edit one.
func n1255Title(t *testing.T, body []byte) string {
	t.Helper()
	var w struct {
		Status struct {
			WidgetData struct {
				Title string `json:"title"`
			} `json:"widgetData"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		t.Fatalf("PRECONDITION: the widget body must be JSON: %v (%s)", err, psTrunc(string(body), 200))
	}
	return w.Status.WidgetData.Title
}

// warm serves alice once and returns her widget key, which must now hold the
// pre-edit body.
func (e *n1255Env) warm(t *testing.T) string {
	t.Helper()
	rec := e.serve(e.aliceCtx)
	if rec.Code != http.StatusOK || n1255Title(t, rec.Body.Bytes()) != n1255PreEdit {
		t.Fatalf("PRECONDITION: the pre-edit /call must render the pre-edit output (code=%d body=%s)",
			rec.Code, psTrunc(rec.Body.String(), 400))
	}
	key := e.key(e.aliceCtx)
	if c, ok := e.store.GetNoTouch(key); !ok || n1255Title(t, c.Raw()) != n1255PreEdit {
		t.Fatal("PRECONDITION: the pre-edit widget body must be cached")
	}
	return key
}

func TestIssue1255_W1_AnEditThatMakesTheWidgetExternalNeverServesThePreEditBody(t *testing.T) {
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	e := n1255Setup(t)
	key := e.warm(t)
	evicted0 := e.store.Stats().EvictUnstorableTotal

	// portal#384's shape: the RESTAction gains a stage on a named endpoint.
	e.edit(t, k1221TargetStage(e.a), k1221FeedStage())
	e.refresh(t, key)
	if e.wire.named.Load() == 0 {
		t.Fatal("PRECONDITION (non-vacuity): the refresh must have resolved the edited RESTAction and dialled the endpoint")
	}

	if c, ok := e.store.GetNoTouch(key); ok && n1255Title(t, c.Raw()) == n1255PreEdit {
		t.Errorf("#1255 RED: the refresh declined the external re-resolve and KEPT the pre-edit body; it would be served until its TTL")
	}
	if d := e.store.Stats().EvictUnstorableTotal - evicted0; d != 1 {
		t.Errorf("#1255 RED: want exactly 1 unstorable eviction, got %d", d)
	}
	// The repeated serves of the report: none may answer from the pre-edit body,
	// and none may store it again (an external result is never cached), so no
	// serve can restart a TTL on it either.
	for i := 1; i <= 3; i++ {
		rec := e.serve(e.aliceCtx)
		if rec.Code != http.StatusOK {
			t.Fatalf("/call %d after the edit: status %d", i, rec.Code)
		}
		if got := n1255Title(t, rec.Body.Bytes()); got != k1221Value {
			t.Fatalf("#1255 RED: /call %d after the edit rendered %q, want the post-edit %q (the pre-edit body is %q)",
				i, got, k1221Value, n1255PreEdit)
		}
		if _, ok := e.store.GetNoTouch(key); ok {
			t.Fatalf("#1255 RED: /call %d after the edit stored the widget cell again (an external result is never cached)", i)
		}
	}
}

func TestIssue1255_W2_ATransientDeclineKeepsThePriorEntry(t *testing.T) {
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	e := n1255Setup(t)
	key := e.warm(t)
	prior, _ := e.store.GetNoTouch(key)
	evicted0 := e.store.Stats().EvictUnstorableTotal
	declined0 := cache.RefresherSkippedStageError()
	keeps := func(what string) {
		t.Helper()
		got, ok := e.store.GetNoTouch(key)
		if !ok || got != prior {
			t.Fatalf("#1255 over-reach (%s): a transient decline must keep the prior entry (resident=%v, same=%v)",
				what, ok, got == prior)
		}
		if d := e.store.Stats().EvictUnstorableTotal - evicted0; d != 0 {
			t.Fatalf("#1255 over-reach (%s): a transient decline evicted %d cell(s)", what, d)
		}
		if _, suppressed := cache.RefreshSuppressedReason(key); suppressed {
			t.Fatalf("#1255 over-reach (%s): a transient decline suppressed the key", what)
		}
	}

	// A stage that fails under the background identity only (the refresher-only
	// 401 of an exportJwt loopback, an upstream 500): the REAL re-resolve of the
	// unedited RESTAction, plus one recorded stage error and no external touch.
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		b, err := resolveOnceProd(ctx, in)
		cache.StageErrorSinkFromContext(ctx).Bump("broken", "upstream 500")
		return b, err
	})
	e.refresh(t, key)
	restore()
	if cache.RefresherSkippedStageError() == declined0 {
		t.Fatal("PRECONDITION (non-vacuity): the refresh must have been declined for a stage error")
	}
	keeps("stage error")

	// A failed resolve (the refresher retries it): no verdict at all.
	restore = setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return nil, errors.New("upstream timeout")
	})
	defer restore()
	entry, _ := e.store.GetNoTouch(key)
	if err := resolveAndPopulateL1(context.Background(), *entry.Inputs, nil, nil); err == nil {
		t.Fatal("PRECONDITION: a failed resolve must return its error to the refresher")
	}
	keeps("failed resolve")
}

func TestIssue1255_W3_TheEvictionIsPushedToAnArmedSubscriber(t *testing.T) {
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	e := n1255Setup(t)
	key := e.warm(t)
	sub, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()

	e.edit(t, k1221TargetStage(e.a), k1221FeedStage())
	e.refresh(t, key)

	select {
	case got := <-sub:
		if got != key {
			t.Fatalf("#1255 RED: the armed subscriber was told about %q, want the evicted key", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("#1255 RED: the eviction was silent — an armed tab keeps showing the pre-edit body (cache.stale_push_unreplaced)")
	}
}
