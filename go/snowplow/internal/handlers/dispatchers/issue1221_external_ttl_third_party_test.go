package dispatchers

// issue1221_external_ttl_third_party_test.go — #1221: the external-TTL opt-in caches
// only a touch that carried nothing of the requester. It runs the REAL
// widgets.Resolve, status validation included, against a fake apiserver and a fake
// named endpoint that both answer by credential and never read the snapshot.
//
// On a partial RBAC snapshot two identities derive one widget key, so anything the
// opt-in stores there is served to both. Four arms, each on real dispatch:
//
//   - a stage dialled as the requester is declined, and bob is not served alice's body;
//   - a named-endpoint widget keeps its cache: the positive arm, against over-tightening;
//   - one requester-dialled stage beside a named one declines the whole Put, in
//     either stage order;
//   - a named endpoint that carries the requester's JWT (exportJwt, the self-loopback)
//     is declined: a named endpoint alone is not enough.
//
// The sink's own rules, child sinks and the memo replay are in cache/put_verdict_1221_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const (
	k1221EP    = "k1221-named-endpoint"
	k1221Token = "tok-k1221-named"
	k1221Value = "third-party-value"
	// k1221Mark follows the sentinel on a requester's answer only, so a cell can be
	// told from the object the informer holds: the content cell is filled from the
	// informer by design and gated per user at serve. This mark in a cell is a defect.
	k1221Mark = "-from-the-dial"
	k1221TTL  = 60 * time.Second
)

var k1221Shapes = []psArm{
	{name: "informer_servable", target: psConfigmapsGVR, watchShape: true},
	{name: "list_shape", target: psConfigmapsGVR, watchShape: false},
	{name: "informer_not_registered", target: psConfigmapsGVR, watchShape: true, noPrereg: true},
	{name: "user_bound", target: psConfigmapsGVR, watchShape: true, userBound: true},
}

// k1221Wire counts who reached the two fakes.
type k1221Wire struct {
	aliceOK, bob403, other atomic.Int64 // the apiserver
	named                  atomic.Int64 // the endpoint, on its own credential
	aliceJWT, bobJWT       atomic.Int64 // the endpoint, on a requester's JWT
}

// k1221Apiserver judges real rights by credential, and answers discovery and the CRD.
func k1221Apiserver(t *testing.T, a psArm, w *k1221Wire) *httptest.Server {
	t.Helper()
	want := "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if h643ServeDiscovery(rw, r.URL.Path) {
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		if r.URL.Path != want {
			rw.WriteHeader(http.StatusNotFound)
			_, _ = rw.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
			return
		}
		switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
		case "tok-" + psAlice:
			w.aliceOK.Add(1)
			body, _ := json.Marshal(map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
				"data":     map[string]any{"password": psWireValue(a) + k1221Mark},
			})
			_, _ = rw.Write(body)
		case "tok-" + psBob:
			w.bob403.Add(1)
			rw.WriteHeader(http.StatusForbidden)
			_, _ = rw.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
		default:
			w.other.Add(1)
			rw.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// k1221Endpoint is a named endpoint: on its own credential one answer for everyone;
// on a requester's JWT that requester's own answer.
func k1221Endpoint(t *testing.T, w *k1221Wire) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		value := ""
		switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
		case k1221Token:
			w.named.Add(1)
			value = k1221Value
		case "jwt-" + psAlice:
			w.aliceJWT.Add(1)
			value = psSentinel + k1221Mark
		case "jwt-" + psBob:
			w.bobJWT.Add(1)
			value = "for-bob"
		default:
			w.other.Add(1)
			rw.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := json.Marshal(map[string]any{"value": value})
		_, _ = rw.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func k1221FeedStage() map[string]any {
	return map[string]any{
		"name": "feed", "path": "/feed", "continueOnError": true,
		"endpointRef": map[string]any{"name": k1221EP, "namespace": psAuthnNS},
	}
}

func k1221TargetStage(a psArm) map[string]any {
	return map[string]any{
		"name": "target", "continueOnError": true,
		"path": "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj,
	}
}

func k1221RA(stages ...map[string]any) *unstructured.Unstructured {
	api := make([]any, 0, len(stages))
	for _, s := range stages {
		api = append(api, s)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": psRAName, "namespace": h1NS},
		"spec":       map[string]any{"api": api},
	}}
}

func k1221Widget(expression string, optedIn bool) *unstructured.Unstructured {
	w := h1WidgetUnstructured(map[string]any{
		"apiRef": map[string]any{"name": psRAName, "namespace": h1NS},
		"widgetDataTemplate": []any{
			map[string]any{"forPath": "title", "expression": expression},
		},
	})
	if optedIn {
		w.SetAnnotations(map[string]string{externalCacheTTLAnnotation: "60"})
	}
	return w
}

type k1221Env struct {
	t                *testing.T
	cr               *unstructured.Unstructured
	saURL            string
	endpointURL      string
	wire             *k1221Wire
	aliceCtx, bobCtx context.Context
	store            *cache.ResolvedCacheStore
}

func k1221Setup(t *testing.T, a psArm, ra, cr *unstructured.Unstructured) *k1221Env {
	t.Helper()
	k423Env(t)
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	crdschema.InvalidateCRDSchemaMemo()
	t.Cleanup(crdschema.InvalidateCRDSchemaMemo)

	psBuildWatcher(t, a, append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(cr))...)
	if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			t.Fatal("PRECONDITION: the RESTAction informer did not sync")
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

	ctxFor := func(user string) context.Context {
		return xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: psGroups(a)}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: apiserver.URL, Token: "tok-" + user}),
			xcontext.WithAccessToken("jwt-"+user))
	}
	return &k1221Env{t: t, cr: cr, saURL: apiserver.URL, endpointURL: endpoint.URL, wire: w,
		aliceCtx: ctxFor(psAlice), bobCtx: ctxFor(psBob), store: cache.ResolvedCache()}
}

func (e *k1221Env) key(ctx context.Context) string {
	e.t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
		h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, e.cr.Object, nil))
	if h == nil || in == nil || in.BindingUID == "" {
		e.t.Fatalf("PRECONDITION: need a live handle and a non-empty BindingUID; handle=%v in=%v", h != nil, in)
	}
	return k
}

// sharePartial publishes the partial snapshot and returns the key both users now
// derive, checked cold.
func (e *k1221Env) sharePartial() string {
	e.t.Helper()
	if e.key(e.aliceCtx) == e.key(e.bobCtx) {
		e.t.Fatalf("PRECONDITION: alice and bob share a widget key on the complete snapshot")
	}
	k1214PublishPartial(e.t)
	key := e.key(e.aliceCtx)
	if key != e.key(e.bobCtx) {
		e.t.Fatalf("PRECONDITION: alice and bob must derive one widget key on the partial snapshot")
	}
	if _, ok := e.store.GetNoTouch(key); ok {
		e.t.Fatalf("PRECONDITION: the shared key must be cold before alice's request")
	}
	return key
}

func (e *k1221Env) serve(ctx context.Context) *httptest.ResponseRecorder {
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: e.cr.DeepCopy()}
	})
	defer restore()
	h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: e.saURL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
	return rec
}

// cellsHoldingTheDial lists the class of every resident cell holding a requester's answer.
func (e *k1221Env) cellsHoldingTheDial() []string {
	var out []string
	for _, k := range e.store.KeysForTest() {
		c, ok := e.store.GetNoTouch(k)
		if !ok || c == nil || !bytes.Contains(c.Raw(), []byte(k1221Mark)) {
			continue
		}
		class := "?"
		if c.Inputs != nil {
			class = c.Inputs.CacheEntryClass
		}
		out = append(out, class)
	}
	return out
}

// aliceNotStored serves alice and reports, without stopping, anything she left in a
// shared cell, so that with the rule gone the arm goes on to show bob's body.
func (e *k1221Env) aliceNotStored(key string) {
	e.t.Helper()
	alice := e.serve(e.aliceCtx)
	if alice.Code != http.StatusOK || !psHasSentinel(alice.Body.Bytes()) || !bytes.Contains(alice.Body.Bytes(), []byte(k1221Mark)) {
		e.t.Fatalf("PRECONDITION: alice's own dial must bring her answer, or nothing here can leak (code=%d body=%s)",
			alice.Code, psTrunc(alice.Body.String(), 400))
	}
	if _, ok := e.store.GetNoTouch(key); ok {
		e.t.Errorf("RED (#1221): the shared widget key is resident: a touch that carried the requester must not be stored")
	}
	if held := e.cellsHoldingTheDial(); len(held) != 0 {
		e.t.Errorf("RED (#1221): alice's answer is in shared cell(s) %v", held)
	}
}

func (e *k1221Env) bobNotServedAlice() *httptest.ResponseRecorder {
	e.t.Helper()
	bob := e.serve(e.bobCtx)
	if psHasSentinel(bob.Body.Bytes()) {
		e.t.Fatalf("CROSS-IDENTITY SERVE (#1221): bob was served alice's answer from a shared cell (code=%d, his 403s=%d, his JWT dials=%d)",
			bob.Code, e.wire.bob403.Load(), e.wire.bobJWT.Load())
	}
	if e.wire.other.Load() != 0 {
		e.t.Fatalf("PRECONDITION: %d dial(s) under a credential neither fake knows", e.wire.other.Load())
	}
	return bob
}

func TestIssue1221_AStageDialledAsTheRequesterIsNeverStoredUnderTheOptIn(t *testing.T) {
	for _, a := range k1221Shapes {
		t.Run(a.name, func(t *testing.T) {
			cr := k1221Widget("${ .target.data.password }", true)
			e := k1221Setup(t, a, k1221RA(k1221TargetStage(a)), cr)
			key := e.sharePartial()
			e.aliceNotStored(key)
			bob := e.bobNotServedAlice()
			if e.wire.aliceOK.Load() == 0 || e.wire.bob403.Load() == 0 {
				t.Fatalf("PRECONDITION: both must have dialled as themselves (alice=%d, bob's 403s=%d, code=%d body=%s)",
					e.wire.aliceOK.Load(), e.wire.bob403.Load(), bob.Code, psTrunc(bob.Body.String(), 300))
			}
		})
	}
}

func TestIssue1221_ANamedEndpointWidgetKeepsItsCache(t *testing.T) {
	a := k1221Shapes[0]
	expr := "${ .feed.value }"
	serveTwice := func(t *testing.T, e *k1221Env) {
		t.Helper()
		for i := 1; i <= 2; i++ {
			rec := e.serve(e.aliceCtx)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), k1221Value) {
				t.Fatalf("PRECONDITION: call %d must bring the endpoint's answer (code=%d body=%s)", i, rec.Code, psTrunc(rec.Body.String(), 400))
			}
		}
	}
	mustCache := func(t *testing.T, e *k1221Env) {
		t.Helper()
		if got := e.wire.named.Load(); got != 1 {
			t.Fatalf("RED (#1221): a genuine opt-in widget must be served from its cache: %d dials over 2 calls, want 1", got)
		}
		c, ok := e.store.GetNoTouch(e.key(e.aliceCtx))
		if !ok || c == nil || !c.ExternalTTL || c.TTLOverride != k1221TTL {
			t.Fatalf("RED (#1221): the opt-in cell must be resident with its bounded TTL: ok=%v entry=%+v", ok, c)
		}
	}

	t.Run("complete_snapshot", func(t *testing.T) {
		e := k1221Setup(t, a, k1221RA(k1221FeedStage()), k1221Widget(expr, true))
		serveTwice(t, e)
		mustCache(t, e)
	})
	// The rule is on the dial, never on the snapshot: a body that does not depend on
	// who asked may be shared.
	t.Run("partial_snapshot", func(t *testing.T) {
		e := k1221Setup(t, a, k1221RA(k1221FeedStage()), k1221Widget(expr, true))
		e.sharePartial()
		serveTwice(t, e)
		mustCache(t, e)
		if rec := e.serve(e.bobCtx); !strings.Contains(rec.Body.String(), k1221Value) || e.wire.named.Load() != 1 {
			t.Fatalf("RED (#1221): bob must read the same third-party answer from the cell (dials=%d body=%s)",
				e.wire.named.Load(), psTrunc(rec.Body.String(), 300))
		}
	})
	// The control: without the annotation the same widget dials every time, so the
	// single dial above is the opt-in's doing, and the count can fail.
	t.Run("not_opted_in", func(t *testing.T) {
		e := k1221Setup(t, a, k1221RA(k1221FeedStage()), k1221Widget(expr, false))
		serveTwice(t, e)
		if got := e.wire.named.Load(); got != 2 {
			t.Fatalf("PRECONDITION: without the opt-in each call must dial: %d dials over 2 calls, want 2", got)
		}
	})
}

func TestIssue1221_ARequesterDialledStageBesideANamedOneDeclinesThePut(t *testing.T) {
	expr := `${ (.feed.value|tostring) + "|" + (.target.data.password|tostring) }`
	for _, a := range k1221Shapes {
		for _, order := range []string{"named_first", "requester_first"} {
			t.Run(a.name+"/"+order, func(t *testing.T) {
				stages := []map[string]any{k1221FeedStage(), k1221TargetStage(a)}
				if order == "requester_first" {
					stages[0], stages[1] = stages[1], stages[0]
				}
				e := k1221Setup(t, a, k1221RA(stages...), k1221Widget(expr, true))
				key := e.sharePartial()
				e.aliceNotStored(key)
				bob := e.bobNotServedAlice()
				if e.wire.named.Load() == 0 || e.wire.bob403.Load() == 0 {
					t.Fatalf("PRECONDITION: both stages must have dialled (named=%d, bob's 403s=%d, code=%d body=%s)",
						e.wire.named.Load(), e.wire.bob403.Load(), bob.Code, psTrunc(bob.Body.String(), 300))
				}
			})
		}
	}
}

func TestIssue1221_ANamedEndpointThatCarriesTheRequesterIsNotStored(t *testing.T) {
	routes := map[string]func(t *testing.T, stage map[string]any) func(*k1221Env){
		"exportJwt": func(_ *testing.T, stage map[string]any) func(*k1221Env) {
			stage["exportJwt"] = true
			return func(*k1221Env) {}
		},
		"self_loopback": func(t *testing.T, _ map[string]any) func(*k1221Env) {
			return func(e *k1221Env) {
				if !api.SetSelfHost(e.endpointURL) {
					t.Fatalf("PRECONDITION: the self host must parse: %s", e.endpointURL)
				}
				t.Cleanup(func() { api.SetSelfHost("") })
			}
		},
	}
	for route, arrange := range routes {
		for _, a := range k1221Shapes {
			t.Run(route+"/"+a.name, func(t *testing.T) {
				stage := k1221FeedStage()
				after := arrange(t, stage)
				e := k1221Setup(t, a, k1221RA(stage), k1221Widget("${ .feed.value }", true))
				after(e)
				key := e.sharePartial()
				e.aliceNotStored(key)
				bob := e.bobNotServedAlice()
				if e.wire.aliceJWT.Load() == 0 || e.wire.bobJWT.Load() == 0 || e.wire.named.Load() != 0 {
					t.Fatalf("PRECONDITION: each requester's JWT must have reached the endpoint (alice=%d bob=%d, own credential=%d, code=%d body=%s)",
						e.wire.aliceJWT.Load(), e.wire.bobJWT.Load(), e.wire.named.Load(), bob.Code, psTrunc(bob.Body.String(), 300))
				}
			})
		}
	}
}
