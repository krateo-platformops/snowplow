package dispatchers

// learned_identity_262_harness_test.go — the shared #262 harness.
//
// Everything below the seams is REAL: the RBAC informer → snapshot → binding
// index → sub-gen flush; the AUTHN_NAMESPACE Secrets informer (a fake
// clientset behind the production StartSecretsInformer) → snapshot rebuild →
// the learned registry; the learned enumeration wrapper; withCohortSeedContext;
// seedOneWidget / seedOneRestaction with their key derivation, admission gate
// and #394/#424 terminal Put; and the customer key derived by
// dispatchCacheLookupKey from a /call identity. The seams are the resolver
// edges only (the widget resolve and the RESTAction fetch), exactly as the
// #258/#394 arms use them.
//
// The clientconfig Secrets carry what authn writes (authn 0.27.3,
// internal/helpers/kube/config/storage/storage.go + gen.go): an x509
// certificate with CN = username and O = groups, stored base64(PEM) under
// client-certificate-data.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

const (
	l262AuthnNS = "l262-authn"
	l262Group   = "l262-devs"
	l262XNS     = "l262-x"
)

// l262TargetGVR is the RESTActions' target GVR (restActionTargetGVRFn seam).
var l262TargetGVR = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1", Resource: "l262targets"}

var l262Serial atomic.Int64

// l262CertPEM is an x509 certificate shaped like authn's: CN=username,
// O=groups, NotBefore = the login instant.
func l262CertPEM(t testing.TB, cn string, orgs []string, notBefore time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(l262Serial.Add(1)),
		Subject:      pkix.Name{CommonName: cn, Organization: orgs},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(24 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// l262ClientconfigSecret is a `<user>-clientconfig` Secret as authn writes it.
func l262ClientconfigSecret(t testing.TB, secretName, cn string, orgs []string, notBefore time.Time, rv string) *corev1.Secret {
	t.Helper()
	crt := base64.StdEncoding.EncodeToString(l262CertPEM(t, cn, orgs, notBefore))
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: l262AuthnNS, ResourceVersion: rv},
		Data: map[string][]byte{
			"client-certificate-data":    []byte(crt),
			"client-key-data":            []byte("dW51c2Vk"),
			"certificate-authority-data": []byte("dW51c2Vk"),
			"server-url":                 []byte("https://kubernetes.default.svc"),
			"proxy-url":                  []byte(""),
		},
	}
}

var (
	l262CRBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	l262CRGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	l262RBGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	l262RGVR   = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

// l262Env is one arm's cluster: the RBAC watcher, the Secrets informer and
// the resident units.
type l262Env struct {
	dyn     *dynamicfake.FakeDynamicClient
	rw      *cache.ResourceWatcher
	kube    *kubefake.Clientset
	widgets []navWidgetEntry
	ras     []templatesv1.ObjectReference
	deps    rePrewarmDeps
}

// l262Opts shapes an arm's cluster.
type l262Opts struct {
	// extraUsers each get a User-subject RoleBinding in l262XNS (a binding no
	// group representative holds — the #262 shape).
	extraUsers []string
	// groupCohorts are additional group subjects bound to the reader role.
	groupCohorts []string
	secrets      []*corev1.Secret
	widgets      int // nav widgets (default 3)
	ras          int // RESTActions (default 2)
}

func l262Binding(name, kind, subject string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
		Subjects:   []rbacv1.Subject{{Kind: kind, APIGroup: "rbac.authorization.k8s.io", Name: subject}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "l262-reader"},
	}
}

func l262ExtraRB(user string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: l262XNS, Name: "l262-extra-" + user, UID: types.UID("uid-l262-extra-" + user)},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "l262-extra"},
	}
}

// l262Setup builds the arm's cluster and resets every process-wide surface the
// learned path reads (registry, admission, t_unit, L1, deps, sub-gens).
func l262Setup(t *testing.T, o l262Opts) *l262Env {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	engineLatchTestMu.Lock()
	t.Cleanup(engineLatchTestMu.Unlock)
	zeroCustomerInFlight()
	resetFirstNavLatchForTest()
	t.Cleanup(resetFirstNavLatchForTest)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	resetLearnedAdmissionForTest()
	t.Cleanup(resetLearnedAdmissionForTest)
	resetSeedUnitCostForTest()
	t.Cleanup(resetSeedUnitCostForTest)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:       "RESTActionList",
		h1WidgetGVR:   "PanelList",
		l262TargetGVR: "L262TargetList",
		l262CRBGVR:    "ClusterRoleBindingList",
		l262CRGVR:     "ClusterRoleList",
		l262RBGVR:     "RoleBindingList",
		l262RGVR:      "RoleList",
	}
	objs := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "l262-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "list"},
				APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group, l262TargetGVR.Group},
				Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource, l262TargetGVR.Resource},
			}},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: l262XNS, Name: "l262-extra"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		l262Binding("l262-devs", "Group", l262Group),
	}
	for _, g := range o.groupCohorts {
		objs = append(objs, l262Binding("l262-cohort-"+g, "Group", g))
	}
	for _, u := range o.extraUsers {
		objs = append(objs, l262ExtraRB(u))
	}
	env := &l262Env{}
	env.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, env.dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1WidgetGVR, h1RAGVR, l262TargetGVR})
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
	env.rw = rw

	// The AUTHN_NAMESPACE Secrets informer — the production StartSecretsInformer
	// over a fake clientset (S1 runs on its real rebuild path).
	objsK := make([]runtime.Object, 0, len(o.secrets))
	for _, s := range o.secrets {
		objsK = append(objsK, s)
	}
	env.kube = kubefake.NewSimpleClientset(objsK...)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	restoreCli := cache.SetSecretsClientForTest(env.kube)
	ictx, icancel := context.WithCancel(context.Background())
	if err := cache.StartSecretsInformer(ictx, nil, l262AuthnNS); err != nil {
		icancel()
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	t.Cleanup(func() {
		icancel()
		restoreCli()
		cache.ResetSecretsInformerForTest()
		cache.ResetSecretsSnapshotForTest()
	})

	// Resident units: nav widgets (NavOrder = harvest order) whose apiRefs name
	// the RESTActions.
	nw, nr := o.widgets, o.ras
	if nw == 0 {
		nw = 3
	}
	if nr == 0 {
		nr = 2
	}
	navHarv := newNavWidgetHarvester()
	raHarv := newContentPrewarmHarvester()
	for i := 0; i < nw; i++ {
		raName := "l262-ra-" + itoaLine(i%nr)
		w := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
			"kind":       "Panel",
			"metadata":   map[string]any{"name": "l262-w" + itoaLine(i), "namespace": h1NS},
			"spec":       map[string]any{"apiRef": map[string]any{"name": raName, "namespace": h1NS}},
		}}
		navHarv.harvestNavWidget(w, h1WidgetGVR, -1, -1, -1, -1)
		raHarv.harvestApiRef(w)
	}
	env.widgets = navHarv.snapshot()
	env.ras = raHarv.snapshot()
	env.deps = rePrewarmDeps{harvester: raHarv, navHarv: navHarv, authnNS: l262AuthnNS}
	return env
}

// l262WaitLearned waits until the registry holds n classes (the informer's
// rebuild is asynchronous after an event).
func l262WaitLearned(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if r, _, _ := cache.LearnedClassCounts(); r == n {
			return
		}
		if time.Now().After(deadline) {
			r, s, u := cache.LearnedClassCounts()
			t.Fatalf("learned registry never reached %d classes (registered=%d from_secrets=%d unparseable=%d)", n, r, s, u)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// l262Stubs installs the resolver-edge seams: a widget resolve (optionally
// sleeping, optionally logging its requester through the ctx logger the way
// the resolve path's own sites do), the RESTAction fetch (a hermetic RA, no api
// steps) and the RA target GVR.
type l262StubOpts struct {
	sleep time.Duration // both kinds, unless overridden below
	// widgetSleep / raSleep — fixed, DISTINCT per-kind costs (deterministic
	// bound arms); widgetSleepFor overrides the widget cost per requester.
	widgetSleep, raSleep time.Duration
	widgetSleepFor       map[string]time.Duration
	probe                bool
	// clock, when set, makes the costs VIRTUAL: a resolve advances the
	// injected seedClock by its cost instead of sleeping — the bound's cost
	// measurement and the backstop arithmetic become exact and load-proof.
	clock *l262Clock
}

func (o l262StubOpts) spend(d time.Duration) {
	if d <= 0 {
		return
	}
	if o.clock != nil {
		o.clock.advance(d)
		return
	}
	time.Sleep(d)
}

// l262Clock is a deterministic seedClock: time moves only when a stubbed
// resolve spends its injected cost.
type l262Clock struct {
	base   time.Time
	offset atomic.Int64
}

func (c *l262Clock) now() time.Time          { return c.base.Add(time.Duration(c.offset.Load())) }
func (c *l262Clock) advance(d time.Duration) { c.offset.Add(int64(d)) }

// l262VirtualClock installs a virtual seedClock for the arm.
func l262VirtualClock(t *testing.T) *l262Clock {
	t.Helper()
	c := &l262Clock{base: time.Now()}
	prev := seedClock
	seedClock = c.now
	t.Cleanup(func() { seedClock = prev })
	return c
}

func (o l262StubOpts) widgetCost(user string) time.Duration {
	if d, ok := o.widgetSleepFor[user]; ok {
		return d
	}
	if o.widgetSleep > 0 {
		return o.widgetSleep
	}
	return o.sleep
}

func (o l262StubOpts) raCost() time.Duration {
	if o.raSleep > 0 {
		return o.raSleep
	}
	return o.sleep
}

func l262Stubs(t *testing.T, o l262StubOpts) {
	t.Helper()
	origW, origGet, origTGVR, origTail := widgetsResolveFn, seedObjectsGetFn, restActionTargetGVRFn, seedRestactionResolveAndPutFn
	t.Cleanup(func() {
		widgetsResolveFn, seedObjectsGetFn, restActionTargetGVRFn, seedRestactionResolveAndPutFn = origW, origGet, origTGVR, origTail
	})
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		user := ""
		if ui, err := xcontext.UserInfo(ctx); err == nil {
			user = ui.Username
		}
		o.spend(o.widgetCost(user))
		if o.probe {
			if ui, err := xcontext.UserInfo(ctx); err == nil {
				xcontext.Logger(ctx).Info("l262.resolve_path.requester",
					"user", ui.Username, "groups", ui.Groups)
			}
		}
		return h1WidgetUnstructured(map[string]any{}), nil
	}
	seedObjectsGetFn = func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
		return objects.Result{
			GVR: restActionGVR,
			Unstructured: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
				"kind":       "RESTAction",
				"metadata":   map[string]any{"name": ref.Name, "namespace": ref.Namespace},
				"spec":       map[string]any{"api": []any{}, "filter": `"` + ref.Name + `"`},
			}},
		}
	}
	restActionTargetGVRFn = func(_ context.Context, _ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return l262TargetGVR, true
	}
	if o.raCost() > 0 {
		seedRestactionResolveAndPutFn = func(
			ctx, resCtx context.Context, cr *templatesv1.RESTAction, ref templatesv1.ObjectReference,
			authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
			stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
		) error {
			o.spend(o.raCost())
			return seedRestactionResolveAndPutProd(ctx, resCtx, cr, ref, authnNS, key, handle, inputs, got, stageErrSink, extTouchedSink)
		}
	}
}

// l262CustomerCtx is a /call identity exactly as the dispatcher sees it (the
// JWT's username + groups; no system:authenticated — the key adds it).
func l262CustomerCtx(user string, groups []string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
}

// l262Keys returns the customer's production keys for every resident unit
// (widgets then RESTActions).
func l262Keys(t *testing.T, env *l262Env, ctx context.Context) (keys []string, handle cacheHandle) {
	t.Helper()
	for _, e := range env.widgets {
		k, h, in := dispatchCacheLookupKey(ctx, "widgets", e.GVR.Group, e.GVR.Version, e.GVR.Resource,
			e.W.GetNamespace(), e.W.GetName(), -1, -1, effectiveKeyExtras(ctx, e.W.Object, nil))
		if h == nil || k == "" || in == nil || in.BindingUID == "" {
			t.Fatalf("precondition: a live widgets key with a non-empty BindingUID for %s", e.W.GetName())
		}
		keys, handle = append(keys, k), h
	}
	for _, ref := range env.ras {
		k, h, in := dispatchCacheLookupKey(ctx, "restactions", restActionGVR.Group, restActionGVR.Version,
			restActionGVR.Resource, ref.Namespace, ref.Name, -1, -1, nil)
		if h == nil || k == "" || in == nil || in.BindingUID == "" {
			t.Fatalf("precondition: a live restactions key with a non-empty BindingUID for %s", ref.Name)
		}
		keys, handle = append(keys, k), h
	}
	return keys, handle
}

// l262Warm counts the customer's warm units.
func l262Warm(t *testing.T, env *l262Env, ctx context.Context) (warm, total int) {
	t.Helper()
	keys, handle := l262Keys(t, env, ctx)
	for _, k := range keys {
		if _, ok := handle.GetNoTouch(k); ok {
			warm++
		}
	}
	return warm, len(keys)
}

// l262Boot runs the REAL engine seed pass (seedScopeYielding) over the
// resident units in the given mode.
func l262Boot(t *testing.T, env *l262Env, mode seedScopeMode) {
	t.Helper()
	if err := seedScopeYielding(context.Background(), env.ras, env.widgets,
		endpoints.Endpoint{}, nil, l262AuthnNS, mode); err != nil {
		t.Fatalf("seedScopeYielding(%v): %v", mode, err)
	}
}

// l262LearnedTargets counts the wrapper's learned targets for gvr, per class.
func l262LearnedTargets(gvr schema.GroupVersionResource) map[string]int {
	out := map[string]int{}
	for _, tg := range enumeratePrewarmTargetsForGVRFn(gvr, "list") {
		if tg.Learned {
			out[tg.Subject.Username]++
		}
	}
	return out
}

// l262StartWorker runs the engine worker for an arm and JOINS it on cleanup:
// the cleanup cancels the worker ctx and waits for runWorker to return BEFORE
// the earlier-registered cleanups (LIFO) restore the resolver seams the worker
// reads — an un-joined worker raced those restores (reviewer-415, 1/100 -race).
func l262StartWorker(t *testing.T, e *prewarmEngine) {
	t.Helper()
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.runWorker(wctx)
	}()
	t.Cleanup(func() {
		wcancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the engine worker did not exit within 30s of its ctx cancel")
		}
	})
}

// l262WarmByKind splits a customer's warm units into widgets and RESTActions.
func l262WarmByKind(t *testing.T, env *l262Env, ctx context.Context) (wWarm, wTotal, rWarm, rTotal int) {
	t.Helper()
	keys, handle := l262Keys(t, env, ctx)
	for i, k := range keys {
		_, ok := handle.GetNoTouch(k)
		if i < len(env.widgets) {
			wTotal++
			if ok {
				wWarm++
			}
		} else {
			rTotal++
			if ok {
				rWarm++
			}
		}
	}
	return wWarm, wTotal, rWarm, rTotal
}

// l262Phases records the classes each learned phase seeded (learnedPhaseObserver).
type l262Phases struct {
	mu sync.Mutex
	m  map[string][]string
}

func (p *l262Phases) get(phase string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.m[phase]...)
}

func l262ObservePhases(t *testing.T) *l262Phases {
	t.Helper()
	p := &l262Phases{m: map[string][]string{}}
	prev := learnedPhaseObserver
	learnedPhaseObserver = func(phase string, classes []string) {
		p.mu.Lock()
		p.m[phase] = classes
		p.mu.Unlock()
	}
	t.Cleanup(func() { learnedPhaseObserver = prev })
	return p
}

func l262Contains(xs []string, k string) bool {
	for _, x := range xs {
		if x == k {
			return true
		}
	}
	return false
}

func l262SameKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
