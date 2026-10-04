// inline443_test.go — #443 part 2, the inline dry-run resolve: hermetic arms
// driven through the REAL chain (middleware.BodyExtrasDecode → the real
// restActionHandler.ServeHTTP → the real resolver), cache ON, a real
// ResourceWatcher and RBAC snapshot (the #423 ps harness), and a fake
// apiserver that authorises each caller's token with the real evaluator and
// records every request it receives.
//
// Arms (design #443 §6, TestS443_Inline):
//
//	b  SameNameNoL1Shadow — a draft named like a warm stored RESTAction is
//	   resolved from the body, never served the stored cell. RED with (f) off.
//	c  Inertness — one sub-arm per carrier, each RED with its own suppression
//	   mutated off (the PR body lists each mutation and its RED line).
//	d  WriteVerbStageNeverDispatched — RED with (k) off.
//	e  Provenance — RED with Provenance forced to Stored.
//	g  Validation400 (handler side; the body-shape cases are in middleware).
//	h  ClientCancel.
//	   Echo headers on success, a stage-error 200 and a filter 500.
package dispatchers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

var in443PodsGVR = schema.GroupVersionResource{Version: "v1", Resource: "pods"}

// in443Arm is the ps harness arm every inline arm runs on: configmaps x/y,
// alice bound by a User CRB and allowed get/list on configmaps in x, bob not.
// watchShape so the informer pivot (and so the apistage content cell) serves.
var in443Arm = psArm{name: "inline443", target: psConfigmapsGVR, userBound: true, watchShape: true}

// in443Req is one request the fake apiserver received.
type in443Req struct {
	method, path, user string
	inertHeader        string
	cancelled          bool
}

// in443API wraps the ps fake apiserver: it records every request (method,
// path, the caller its bearer token names), serves extra routes, and can block
// a path until the client cancels.
type in443API struct {
	mu     sync.Mutex
	reqs   []in443Req
	extra  map[string]http.HandlerFunc
	block  string
	srv    *httptest.Server
	inner  *httptest.Server
	waitCh chan struct{}
}

func newIn443API(t *testing.T) *in443API {
	t.Helper()
	perUser := map[string]*atomic.Int64{psAlice: {}, psBob: {}}
	f := &in443API{extra: map[string]http.HandlerFunc{}, waitCh: make(chan struct{}, 8)}
	f.inner = psFakeAPIServer(t, in443Arm, perUser)
	u, _ := url.Parse(f.inner.URL)
	proxy := httputil.NewSingleHostReverseProxy(u)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := strings.TrimPrefix(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "tok-")
		rec := in443Req{method: r.Method, path: r.URL.Path, user: user, inertHeader: r.Header.Get(cache.InertHeader)}
		f.mu.Lock()
		h := f.extra[r.Method+" "+r.URL.Path]
		block := f.block != "" && r.URL.Path == f.block
		f.mu.Unlock()
		if block {
			f.waitCh <- struct{}{}
			select {
			case <-r.Context().Done():
				rec.cancelled = true
			case <-time.After(10 * time.Second):
			}
			f.record(rec)
			return
		}
		f.record(rec)
		if h != nil {
			h(w, r)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *in443API) record(r in443Req) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r)
	f.mu.Unlock()
}

func (f *in443API) requests() []in443Req {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]in443Req(nil), f.reqs...)
}

func (f *in443API) reset() {
	f.mu.Lock()
	f.reqs = nil
	f.mu.Unlock()
}

// in443Setup builds the cache-on harness and returns the fake apiserver.
func in443Setup(t *testing.T, extra ...runtime.Object) *in443API {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	psBuildWatcher(t, in443Arm, extra...)
	f := newIn443API(t)
	psSeedClientconfigs(t, f.srv.URL)
	return f
}

// in443Ctx is the caller's request ctx as the UserConfig middleware leaves it:
// identity plus the caller's own clientconfig endpoint.
func in443Ctx(f *in443API, user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: psGroups(in443Arm)}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-" + user}),
	)
}

func in443Handler(f *in443API) http.Handler {
	return middleware.BodyExtrasDecode(&restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}})
}

// in443RA builds a RESTAction named name in h1NS with the given stages and an
// optional top-level filter.
func in443RA(name string, filter string, stages ...map[string]any) map[string]any {
	api := make([]any, 0, len(stages))
	for _, s := range stages {
		api = append(api, s)
	}
	spec := map[string]any{"api": api}
	if filter != "" {
		spec["filter"] = filter
	}
	return map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec":       spec,
	}
}

// targetStage is the ps arm's informer-served GET of configmaps x/y.
func targetStage() map[string]any {
	return map[string]any{"name": "target", "continueOnError": true,
		"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj}
}

// serveInline POSTs /call/read with {"extras":{},"object":obj} as ctx's caller.
func serveInline(t *testing.T, f *in443API, ctx context.Context, obj map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	name, _, _ := unstructured.NestedString(obj, "metadata", "name")
	body, err := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
	if err != nil {
		t.Fatal(err)
	}
	target := "/call/read?apiVersion=" + h1RAGVR.Group + "/" + h1RAGVR.Version +
		"&resource=restactions&namespace=" + h1NS + "&name=" + name
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	rec := httptest.NewRecorder()
	in443Handler(f).ServeHTTP(rec, req)
	return rec
}

// in443JWT is a bearer whose only meaningful claim is a future exp: the
// learned-identity observation (observeLiveCaller) registers a live caller
// only when its JWT carries an expiry. The signature is never checked there.
func in443JWT() string {
	enc := base64.RawURLEncoding.EncodeToString
	exp := time.Now().Add(time.Hour).Unix()
	return enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(`{"exp":`+strconv.FormatInt(exp, 10)+`}`)) + ".sig"
}

// serveStored resolves the STORED RESTAction obj (via the fetchObject seam,
// as GET /call would after reading the CR) as ctx's caller.
func serveStored(t *testing.T, f *in443API, ctx context.Context, obj map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	cr := &unstructured.Unstructured{Object: obj}
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	defer restore()
	h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	h.ServeHTTP(rec, req)
	return rec
}

func assertInlineEcho(t *testing.T, rec *httptest.ResponseRecorder, what string) {
	t.Helper()
	if rec.Header().Get(util.HeaderDryRun) != "All" || rec.Header().Get(util.HeaderResolveSource) != util.ResolveSourceRequestBody {
		t.Errorf("%s: echo %s=%q %s=%q, want All / request-body", what,
			util.HeaderDryRun, rec.Header().Get(util.HeaderDryRun),
			util.HeaderResolveSource, rec.Header().Get(util.HeaderResolveSource))
	}
}

// in443State is everything an inert resolve must leave untouched.
type in443State struct {
	l1Keys       []string
	lastRead     map[string]int64
	deps         map[string]int64
	gvrs         []string
	clusterList  int
	navGroups    []string
	refresher    [12]uint64
	sse          [4]uint64
	learned      int
	extSkipped   uint64
	uafDeclined  uint64
	shadowChecks uint64
}

func takeIn443State(t *testing.T) in443State {
	t.Helper()
	var s in443State
	store := cache.ResolvedCache()
	s.l1Keys = store.KeysForTest()
	sort.Strings(s.l1Keys)
	s.lastRead = map[string]int64{}
	for _, k := range s.l1Keys {
		if m, ok := store.MetadataForKey(k); ok {
			s.lastRead[k] = m.LastReadSeconds
		}
	}
	s.deps = cache.DepsStatsByStat()
	if rw := cache.Global(); rw != nil {
		for _, g := range rw.RegisteredGVRs() {
			s.gvrs = append(s.gvrs, g.String())
		}
		sort.Strings(s.gvrs)
	}
	s.clusterList = cache.ClusterListKeyCountForTest()
	s.navGroups = cache.NavigationDiscoveredGroupsSnapshot()
	sort.Strings(s.navGroups)
	e, c, fl, rt, d, sne, snh, sse, y, cp, fo, qd := cache.RefresherSnapshot()
	s.refresher = [12]uint64{e, c, fl, rt, d, sne, snh, sse, y, cp, fo, uint64(qd)}
	p, dl, dr, co := cache.RefreshBroadcasterCounters()
	s.sse = [4]uint64{p, dl, dr, co}
	s.learned = len(cache.LearnedIdentitiesSnapshot())
	s.extSkipped = cache.ExternalSkippedPut()
	s.uafDeclined = cache.RestactionsUAFPutDeclined()
	s.shadowChecks = shadowChecksTotal.Load()
	return s
}

func diffIn443State(t *testing.T, carrier string, before, after in443State) {
	t.Helper()
	check := func(field string, b, a any) {
		if !reflect.DeepEqual(b, a) {
			t.Errorf("%s: an inert (dry-run) resolve changed %s: before=%v after=%v", carrier, field, b, a)
		}
	}
	check("L1 keys", before.l1Keys, after.l1Keys)
	for k, b := range before.lastRead {
		if a, ok := after.lastRead[k]; ok && b >= 0 && a < b {
			t.Errorf("%s: an inert (dry-run) resolve touched L1 cell %s… (LastReadSeconds %d → %d)", carrier, k[:12], b, a)
		}
	}
	check("dep stats", before.deps, after.deps)
	check("watcher GVR set", before.gvrs, after.gvrs)
	check("cluster-list registry", before.clusterList, after.clusterList)
	check("navigation groups", before.navGroups, after.navGroups)
	check("refresher counters", before.refresher, after.refresher)
	check("SSE counters", before.sse, after.sse)
	check("learned identity classes", before.learned, after.learned)
	check("external-skipped-Put counter (Put-chain decline)", before.extSkipped, after.extSkipped)
	check("UAF Put-decline counter (Put-chain decline)", before.uafDeclined, after.uafDeclined)
	check("shadow-parity checks", before.shadowChecks, after.shadowChecks)
}

func TestS443_Inline(t *testing.T) {
	// a — ParityGolden (hermetic half; the kind half is TestS443_Kind_Inline in
	// internal/handlers). The SAME RESTAction body resolved STORED (L1 cold,
	// then L1 warm) and INLINE yields byte-identical replies for each caller,
	// and the two callers' replies differ (the fixture discriminates RBAC).
	// Fixture: K=3 stages × M=2 iterator items, dependsOn, path templating,
	// per-stage filters, a top-level filter; F2 adds an empty-result error
	// stage (never cached, so it is compared cold vs inline only).
	t.Run("a_ParityGolden", func(t *testing.T) {
		y2 := &corev1.ConfigMap{
			TypeMeta:   psTypeMeta(in443Arm, "ConfigMap"),
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "y2"},
			Data:       map[string]string{"password": psSentinel + "-2"},
		}
		f := in443Setup(t, y2)
		f.extra["GET /api/v1/namespaces/"+psTargetNS+"/configmaps/y2"] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != "tok-"+psAlice {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
				return
			}
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"y2","namespace":"` + psTargetNS + `"},"data":{"password":"` + psSentinel + `-2"}}`))
		}
		stages := func(withErr bool) []map[string]any {
			st := []map[string]any{
				{"name": "cms", "continueOnError": true, "errorKey": "cmsErr",
					// A stage filter sees the stage output under its own name.
					"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "filter": "[.cms.items[] | {name: .metadata.name}] | sort_by(.name)"},
				{"name": "each", "continueOnError": true, "errorKey": "eachErr",
					"dependsOn": map[string]any{"name": "cms", "iterator": ".cms"},
					"path":      `${ "/api/v1/namespaces/` + psTargetNS + `/configmaps/" + .name }`,
					"filter":    ".each.data.password"},
				{"name": "count", "continueOnError": true,
					"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj, "filter": ".count.metadata.name"},
			}
			if withErr {
				st = append(st, map[string]any{"name": "missing", "continueOnError": true, "errorKey": "missingErr",
					"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist"})
			}
			return st
		}
		top := `{cms: [.cms[]?.name], each: (.each | if type == "array" then sort else . end), count: .count, errs: [.cmsErr, .eachErr, .missingErr]}`
		for _, fx := range []struct {
			name    string
			withErr bool
		}{{"F1_cacheable", false}, {"F2_with_error_stage", true}} {
			replies := map[string]string{}
			for _, user := range []string{psAlice, psBob} {
				cache.ResetResolvedCacheForTest()
				ctx := in443Ctx(f, user)
				body := in443RA(psRAName, top, stages(fx.withErr)...)
				cold := serveStored(t, f, ctx, body)
				warmKeys := len(cache.ResolvedCache().KeysForTest())
				warm := serveStored(t, f, ctx, body)
				if !fx.withErr && user == psAlice && warmKeys == 0 {
					t.Errorf("%s/%s: the stored cold resolve left no L1 cell, so the 'warm' reply was not served from L1", fx.name, user)
				}
				t.Logf("%s/%s: L1 keys after cold=%d reply=%s", fx.name, user, warmKeys, psTrunc(cold.Body.String(), 260))
				inline := serveInline(t, f, ctx, body)
				if cold.Code != http.StatusOK || inline.Code != http.StatusOK {
					t.Fatalf("%s/%s: codes stored=%d inline=%d", fx.name, user, cold.Code, inline.Code)
				}
				if inline.Body.String() != cold.Body.String() {
					t.Errorf("%s/%s: inline reply differs from the STORED (L1-cold) reply\n stored=%s\n inline=%s",
						fx.name, user, psTrunc(cold.Body.String(), 700), psTrunc(inline.Body.String(), 700))
				}
				if inline.Body.String() != warm.Body.String() {
					t.Errorf("%s/%s: inline reply differs from the STORED (L1-warm) reply", fx.name, user)
				}
				replies[user] = inline.Body.String()
			}
			if replies[psAlice] == replies[psBob] {
				t.Errorf("%s: alice and bob got identical replies — the fixture does not discriminate the callers' RBAC: %s",
					fx.name, psTrunc(replies[psAlice], 400))
			}
			// The iterator stage read BOTH items: its projected values are the
			// two passwords, side by side in alice's "each".
			if !strings.Contains(replies[psAlice], `"each":["`+psSentinel+`","`+psSentinel+`-2"]`) {
				t.Errorf("%s: alice's reply does not show the iterator stage over both items (M=2 not exercised): %s", fx.name, psTrunc(replies[psAlice], 700))
			}
		}
	})

	// b — SameNameNoL1Shadow.
	t.Run("b_SameNameNoL1Shadow", func(t *testing.T) {
		f := in443Setup(t)
		alice := in443Ctx(f, psAlice)
		s1 := in443RA(psRAName, "", targetStage())
		for i := 0; i < 2; i++ { // cold fill, then a warm hit
			if rec := serveStored(t, f, alice, s1); rec.Code != http.StatusOK {
				t.Fatalf("stored warm-up %d: code=%d body=%s", i, rec.Code, rec.Body.String())
			}
		}
		if cache.ResolvedCache().Len() == 0 {
			t.Fatal("PRE: the stored RESTAction cell must be warm (no L1 entry after two stored resolves)")
		}
		s2 := in443RA(psRAName, `{"draft":"S2-inline-443"}`, targetStage())
		rec := serveInline(t, f, alice, s2)
		if rec.Code != http.StatusOK {
			t.Fatalf("inline: code=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "S2-inline-443") {
			t.Fatalf("inline S2 was served the stored S1 cell: body=%s", psTrunc(rec.Body.String(), 400))
		}
		assertInlineEcho(t, rec, "inline S2")
	})

	// c — Inertness, one sub-arm per carrier.
	t.Run("c1_NeverInformedGVR", func(t *testing.T) {
		f := in443Setup(t)
		alice := in443Ctx(f, psAlice)
		before := takeIn443State(t)
		obj := in443RA("draft-c1", "", map[string]any{"name": "pods", "continueOnError": true,
			"path": "/api/v1/namespaces/" + psTargetNS + "/pods"})
		rec := serveInline(t, f, alice, obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
		time.Sleep(50 * time.Millisecond) // a registration would be synchronous; settle any async edge
		diffIn443State(t, "c1 (b) never-informed GVR", before, takeIn443State(t))
	})

	t.Run("c3_ApistageContentPut", func(t *testing.T) {
		// A cold inline resolve of an informer-served stage: the apistage
		// content cell (an identity-free key, independent of the handler key)
		// would be PutIfGen'd on the miss. (a) refuses it.
		f := in443Setup(t)
		alice := in443Ctx(f, psAlice)
		before := takeIn443State(t)
		rec := serveInline(t, f, alice, in443RA("draft-c3", "", targetStage()))
		if rec.Code != http.StatusOK || !psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("inline resolve did not serve the stage: code=%d body=%s", rec.Code, psTrunc(rec.Body.String(), 300))
		}
		diffIn443State(t, "c3 (a) apistage content Put", before, takeIn443State(t))
	})

	t.Run("c6_TouchOnWarmApistageCell", func(t *testing.T) {
		f := in443Setup(t)
		alice := in443Ctx(f, psAlice)
		// Warm the apistage content cell with a STORED resolve. A freshly Put
		// cell reads LastReadSeconds == -1 (never read).
		if rec := serveStored(t, f, alice, in443RA(psRAName, "", targetStage())); rec.Code != http.StatusOK {
			t.Fatalf("stored warm-up: code=%d", rec.Code)
		}
		// LastReadSeconds has one-second resolution: wait until every warm
		// cell reads >= 1, so a touch (which resets it to 0) is visible.
		time.Sleep(1200 * time.Millisecond)
		before := takeIn443State(t)
		if len(before.lastRead) == 0 {
			t.Fatal("PRE: the stored fill left no L1 cell")
		}
		for k, v := range before.lastRead {
			if v < 1 {
				t.Fatalf("PRE: cell %s… LastReadSeconds=%d, want >= 1 after the wait", k[:12], v)
			}
		}
		rec := serveInline(t, f, alice, in443RA("draft-c6", "", targetStage()))
		if rec.Code != http.StatusOK || !psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("inline resolve did not serve the stage: code=%d", rec.Code)
		}
		diffIn443State(t, "c6 (e) touch on a warm apistage cell", before, takeIn443State(t))
	})

	t.Run("c7_HandlerPutChainAndObserveLiveCaller", func(t *testing.T) {
		// A userAccessFilter stage: on a STORED resolve the refilter marks the
		// resolve UAF-touched and the handler Put chain declines it (the UAF
		// Put-decline counter), and observeLiveCaller registers the caller's
		// learned identity class (the request carries a JWT with an expiry).
		// Neither may happen inert.
		f := in443Setup(t)
		cache.ResetLearnedIdentitiesForTest()
		t.Cleanup(cache.ResetLearnedIdentitiesForTest)
		alice := in443Ctx(f, psAlice)
		obj := in443RA("draft-c7", "", map[string]any{"name": "uaf", "continueOnError": true,
			"path":             "/api/v1/namespaces/" + psTargetNS + "/configmaps",
			"userAccessFilter": map[string]any{"verb": "get", "resource": "configmaps", "group": ""}})
		before := takeIn443State(t)
		if rec := serveInline(t, f, alice, obj); rec.Code != http.StatusOK {
			t.Fatalf("code=%d", rec.Code)
		}
		diffIn443State(t, "c7 (g)+(h) handler Put chain and observeLiveCaller", before, takeIn443State(t))

		// Control (the arm can fail): the SAME spec resolved STORED moves both.
		ctl := takeIn443State(t)
		if rec := serveStored(t, f, alice, obj); rec.Code != http.StatusOK {
			t.Fatalf("control: code=%d", rec.Code)
		}
		after := takeIn443State(t)
		if after.uafDeclined == ctl.uafDeclined {
			t.Errorf("CONTROL: a stored resolve of the UAF stage did not move the UAF Put-decline counter — (g) is not observable")
		}
		if after.learned == ctl.learned {
			t.Errorf("CONTROL: a stored resolve did not register the caller's learned identity — (h) is not observable")
		}
	})

	// c5 — the HTTP self-loopback carrier (j). Egress: a stage whose
	// endpoint is snowplow's own host carries X-Snowplow-Inert: 1 when the
	// resolve is inert, and only then. Ingest: a TRUSTED self-loopback hop
	// carrying the header is resolved inert (no L1 fill); an untrusted one
	// (arriving on another host) is not.
	t.Run("c5_SelfLoopbackEgress", func(t *testing.T) {
		f := in443Setup(t)
		if !api.SetSelfHost(f.srv.URL) {
			t.Fatal("SetSelfHost refused the fake's URL")
		}
		t.Cleanup(func() { api.SetSelfHost("") })
		ctx := xcontext.BuildContext(in443Ctx(f, psAlice), xcontext.WithAccessToken("tok-"+psAlice))
		// A non-apiserver path on snowplow's own host: never informer-served,
		// so both the inline and the stored resolve always dial it.
		stagePath := "/selfloop-443"
		obj := in443RA("draft-c5", "", map[string]any{"name": "self", "continueOnError": true, "path": stagePath})
		header := func() (string, bool) {
			for _, r := range f.requests() {
				if r.path == stagePath {
					return r.inertHeader, true
				}
			}
			return "", false
		}
		f.reset()
		serveInline(t, f, ctx, obj)
		got, dialed := header()
		if !dialed {
			t.Fatalf("the self-loopback stage never dialed: %+v", f.requests())
		}
		if got != "1" {
			t.Errorf("inert self-loopback hop carried %s=%q, want \"1\"", cache.InertHeader, got)
		}
		// Control: the same stage on a STORED resolve carries no header.
		f.reset()
		serveStored(t, f, ctx, obj)
		if got, dialed := header(); !dialed || got != "" {
			t.Errorf("stored self-loopback hop: dialed=%v %s=%q, want dialed and no header", dialed, cache.InertHeader, got)
		}
	})

	t.Run("c5_SelfLoopbackIngest", func(t *testing.T) {
		f := in443Setup(t)
		SetSelfLoopbackHost("http://snowplow.self.example:8081")
		t.Cleanup(func() { SetSelfLoopbackHost("") })
		stored := in443RA(psRAName, "", targetStage())
		hop := func(host string, withHeader bool) *httptest.ResponseRecorder {
			cr := &unstructured.Unstructured{Object: stored}
			restore := setFetchObjectForTest(func(*http.Request) objects.Result {
				return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
			})
			defer restore()
			req := httptest.NewRequest(http.MethodGet, "http://"+host+"/call", nil).WithContext(in443Ctx(f, psAlice))
			if withHeader {
				req.Header.Set(cache.InertHeader, "1")
			}
			rec := httptest.NewRecorder()
			(&restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}).ServeHTTP(rec, req)
			return rec
		}
		store := cache.ResolvedCache()
		before := len(store.KeysForTest())
		if rec := hop("snowplow.self.example:8081", true); rec.Code != http.StatusOK {
			t.Fatalf("trusted hop: code=%d", rec.Code)
		}
		if got := len(store.KeysForTest()); got != before {
			t.Errorf("a TRUSTED self-loopback hop carrying %s filled L1 (%d → %d keys); it must be inert", cache.InertHeader, before, got)
		}
		// Untrusted: the header arrives on another host → ignored → a normal fill.
		if rec := hop("evil.example:8081", true); rec.Code != http.StatusOK {
			t.Fatalf("untrusted hop: code=%d", rec.Code)
		}
		if got := len(store.KeysForTest()); got == before {
			t.Errorf("an UNTRUSTED request carrying %s was treated as inert (no L1 fill); the header must be ignored", cache.InertHeader)
		}
	})

	// d — WriteVerbStageNeverDispatched: a POST stage aimed at the fake
	// apiserver and one at an external server; both receive zero requests,
	// and the envelope carries the dry-run stage error.
	t.Run("d_WriteVerbStageNeverDispatched", func(t *testing.T) {
		f := in443Setup(t)
		var extHits atomic.Int64
		ext := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			extHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}))
		defer ext.Close()
		f.extra["POST /api/v1/namespaces/"+psTargetNS+"/configmaps"] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"ConfigMap"}`))
		}
		obj := in443RA("draft-d", "",
			map[string]any{"name": "create", "verb": "POST", "continueOnError": true, "errorKey": "createErr",
				"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "payload": `{"metadata":{"name":"evil"}}`},
			map[string]any{"name": "webhook", "verb": "POST", "continueOnError": true, "errorKey": "webhookErr",
				"path": ext.URL + "/hook", "payload": `{}`},
		)
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psAlice), obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
		for _, r := range f.requests() {
			if r.method != http.MethodGet {
				t.Errorf("the apiserver received a %s %s from a dry-run resolve", r.method, r.path)
			}
		}
		if n := extHits.Load(); n != 0 {
			t.Errorf("the external server received %d requests from a dry-run resolve", n)
		}
		body := rec.Body.String()
		for _, want := range []string{`dry-run: stage \"create\" verb POST is not executed`, `dry-run: stage \"webhook\" verb POST is not executed`} {
			if !strings.Contains(body, want) {
				t.Errorf("envelope lacks the stage error %s: %s", want, psTrunc(body, 600))
			}
		}
		assertInlineEcho(t, rec, "write-verb dry run")
	})

	// e — Provenance.
	t.Run("e1_LiteralClientconfigRefRefused", func(t *testing.T) {
		f := in443Setup(t)
		obj := in443RA("draft-e1", "", map[string]any{"name": "asAlice", "continueOnError": true, "errorKey": "asAliceErr",
			"endpointRef": map[string]any{"name": psAlice + "-clientconfig", "namespace": psAuthnNS},
			"path":        "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj})
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psBob), obj)
		for _, r := range f.requests() {
			if r.user == psAlice {
				t.Errorf("bob's draft dialed with ALICE's credentials: %s %s", r.method, r.path)
			}
		}
		if psHasSentinel(rec.Body.Bytes()) {
			t.Errorf("bob's draft read alice's data through her clientconfig: %s", psTrunc(rec.Body.String(), 300))
		}
		assertInlineEcho(t, rec, "e1")
	})

	t.Run("e2_EndpointRefSecretReadAsCaller", func(t *testing.T) {
		// A named endpointRef Secret is read with a rest config built from the
		// CALLER's clientconfig endpoint, never the ServiceAccount's. The
		// per-user rest config is certificate-based in production (no bearer
		// in a kubeconfig), so the arm tells the two identities apart by the
		// HOST each one dials: the caller's endpoint is "localhost:<port>",
		// the ServiceAccount rest config is "127.0.0.1:<port>" (same fake).
		// Only the ServiceAccount may read this Secret (the caller gets 403).
		f := in443Setup(t)
		secretPath := "/api/v1/namespaces/" + psAuthnNS + "/secrets/shared-ep"
		var hosts []string
		var mu sync.Mutex
		f.extra["GET "+secretPath] = func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			hosts = append(hosts, r.Host)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if strings.HasPrefix(r.Host, "localhost") { // the caller's identity: denied
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
				return
			}
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"shared-ep","namespace":"` + psAuthnNS +
				`"},"data":{"server-url":"` + b64(f.srv.URL) + `","token":"` + b64("tok-"+psAlice) + `"}}`))
		}
		obj := in443RA("draft-e2", "", map[string]any{"name": "viaRef", "continueOnError": true, "errorKey": "viaRefErr",
			"endpointRef": map[string]any{"name": "shared-ep", "namespace": psAuthnNS},
			"path":        "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj})
		callerURL := strings.Replace(f.srv.URL, "127.0.0.1", "localhost", 1)
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: psBob, Groups: psGroups(in443Arm)}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: callerURL, Token: "tok-" + psBob}),
		)
		rec := serveInline(t, f, ctx, obj)
		mu.Lock()
		got := append([]string(nil), hosts...)
		mu.Unlock()
		if len(got) == 0 {
			t.Fatalf("the endpointRef Secret was never read through the apiserver (it must be read live, as the caller); body=%s",
				psTrunc(rec.Body.String(), 400))
		}
		for _, h := range got {
			if !strings.HasPrefix(h, "localhost") {
				t.Errorf("the endpointRef Secret was read via %q (the ServiceAccount rest config), want the caller's endpoint", h)
			}
		}
		if psHasSentinel(rec.Body.Bytes()) {
			t.Errorf("bob's draft used a Secret only the ServiceAccount can read: %s", psTrunc(rec.Body.String(), 300))
		}
		if !strings.Contains(rec.Body.String(), "viaRefErr") {
			t.Errorf("expected a stage error under viaRefErr: %s", psTrunc(rec.Body.String(), 400))
		}
		assertInlineEcho(t, rec, "e2")
	})

	t.Run("e3_UAFStageDialsCaller", func(t *testing.T) {
		// A UAF stage over a path the caller cannot list (pods in x: bob's only
		// pod grant is get in z), declaring uaf resources bob CAN get. Stored
		// semantics dial the ServiceAccount; caller-supplied semantics dial the
		// caller, so the apiserver's 403 is the stage error and no row is read.
		f := in443Setup(t)
		listPath := "/api/v1/namespaces/" + psTargetNS + "/pods"
		f.extra["GET "+listPath] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != "tok-"+psAlice {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"pods is forbidden","reason":"Forbidden","code":403}`))
				return
			}
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"p","namespace":"z"},"spec":{"x":"` + psSentinel + `"}}]}`))
		}
		obj := in443RA("draft-e3", "", map[string]any{"name": "uaf", "continueOnError": true, "errorKey": "uafErr",
			"path":             listPath,
			"userAccessFilter": map[string]any{"verb": "get", "resource": "pods", "group": ""}})
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psBob), obj)
		var dialedAsCaller bool
		for _, r := range f.requests() {
			if r.path == listPath {
				if r.user != psBob {
					t.Errorf("the UAF stage dialed as %q, want the caller (bob)", r.user)
				}
				dialedAsCaller = true
			}
		}
		if !dialedAsCaller {
			t.Errorf("the caller-supplied UAF stage never dialed the apiserver as the caller (Stored semantics: ServiceAccount dial); reqs=%+v", f.requests())
		}
		if psHasSentinel(rec.Body.Bytes()) {
			t.Errorf("bob's UAF draft read rows he cannot list: %s", psTrunc(rec.Body.String(), 300))
		}
		if !strings.Contains(rec.Body.String(), "forbidden") {
			t.Errorf("expected the apiserver's 403 as the UAF stage error: %s", psTrunc(rec.Body.String(), 400))
		}
	})

	t.Run("e_NoInternalRESTConfigOnInlineCtx", func(t *testing.T) {
		// An inline request ctx never carries an internal (ServiceAccount) rest
		// config; resolveOneAsCaller refuses one as defense in depth.
		f := in443Setup(t)
		var sawInternal atomic.Bool
		restore := setRestactionsResolveForTest(func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			if _, ok := cache.InternalRESTConfigFromContext(ctx); ok {
				sawInternal.Store(true)
			}
			if opts.Provenance != api.ProvenanceCallerSupplied {
				t.Errorf("inline resolve ran with provenance %v, want CallerSupplied", opts.Provenance)
			}
			return restactions.Resolve(ctx, opts)
		})
		defer restore()
		if rec := serveInline(t, f, in443Ctx(f, psAlice), in443RA("draft-e0", "", targetStage())); rec.Code != http.StatusOK {
			t.Fatalf("code=%d", rec.Code)
		}
		if sawInternal.Load() {
			t.Fatal("the inline resolve ctx carried an internal (ServiceAccount) rest config")
		}
	})

	// g (handler side) — an inline object can only reach this handler for a
	// RESTAction; a body for the wrong name is refused before the resolver.
	t.Run("g_NameMismatch400", func(t *testing.T) {
		f := in443Setup(t)
		obj := in443RA("other-name", "", targetStage())
		body, _ := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
		req := httptest.NewRequest(http.MethodPost, "/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&namespace="+h1NS+"&name=draft-g",
			bytes.NewReader(body)).WithContext(in443Ctx(f, psAlice))
		f.reset()
		rec := httptest.NewRecorder()
		in443Handler(f).ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || len(f.requests()) != 0 {
			t.Errorf("name mismatch: code=%d outbound=%d, want 400 and 0", rec.Code, len(f.requests()))
		}
	})

	// Echo on a stage-error 200 and on a filter 500.
	t.Run("EchoOnStageErrorAndFilterError", func(t *testing.T) {
		f := in443Setup(t)
		alice := in443Ctx(f, psAlice)
		rec := serveInline(t, f, alice, in443RA("draft-echo1", "", map[string]any{"name": "missing", "continueOnError": true,
			"errorKey": "missingErr", "path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist"}))
		if rec.Code != http.StatusOK {
			t.Errorf("stage error: code=%d, want 200", rec.Code)
		}
		assertInlineEcho(t, rec, "stage-error 200")
		rec = serveInline(t, f, alice, in443RA("draft-echo2", ".nope | error(\"boom\")", targetStage()))
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("filter error: code=%d, want 500", rec.Code)
		}
		assertInlineEcho(t, rec, "filter 500")
	})

	// h — ClientCancel: a client disconnect aborts the in-flight stage
	// request (the fake server sees it cancelled).
	t.Run("h_ClientCancel", func(t *testing.T) {
		f := in443Setup(t)
		blockPath := "/api/v1/namespaces/" + psTargetNS + "/pods"
		f.mu.Lock()
		f.block = blockPath
		f.mu.Unlock()
		ctx, cancel := context.WithCancel(in443Ctx(f, psAlice))
		done := make(chan struct{})
		go func() {
			defer close(done)
			serveInline(t, f, ctx, in443RA("draft-h", "", map[string]any{"name": "slow", "continueOnError": true, "path": blockPath}))
		}()
		select {
		case <-f.waitCh:
		case <-time.After(5 * time.Second):
			t.Fatal("the stage request never reached the fake apiserver")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the handler did not return after the client cancelled")
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			for _, r := range f.requests() {
				if r.path == blockPath && r.cancelled {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("the in-flight stage request was not cancelled by the client disconnect: %+v", f.requests())
	})
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
