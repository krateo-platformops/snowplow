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
	sleep time.Duration
	probe bool
}

func l262Stubs(t *testing.T, o l262StubOpts) {
	t.Helper()
	origW, origGet, origTGVR, origTail := widgetsResolveFn, seedObjectsGetFn, restActionTargetGVRFn, seedRestactionResolveAndPutFn
	t.Cleanup(func() {
		widgetsResolveFn, seedObjectsGetFn, restActionTargetGVRFn, seedRestactionResolveAndPutFn = origW, origGet, origTGVR, origTail
	})
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		if o.sleep > 0 {
			time.Sleep(o.sleep)
		}
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
	if o.sleep > 0 {
		seedRestactionResolveAndPutFn = func(
			ctx, resCtx context.Context, cr *templatesv1.RESTAction, ref templatesv1.ObjectReference,
			authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
			stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
		) error {
			time.Sleep(o.sleep)
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
