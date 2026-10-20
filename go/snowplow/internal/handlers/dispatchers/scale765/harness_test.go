//go:build scale765 || unit || integration

// harness_test.go: the #765 in-process scale harness (spec: architect-623,
// #765 issuecomment-6266045747; plan: issuecomment-6266221429).
//
// WHERE. Its own package (internal/handlers/dispatchers/scale765), not the
// spec's dispatchers/scale765_harness_test.go: the dispatchers package already
// spends ~540 s of the 10 min per-package `go test` budget under -race, and the
// smoke variant (~90 s under -race on a loaded host) would push it over. Here it
// has its own budget. The only production touch is
// dispatchers.RegisterRefreshHandlersWithSATransportForTest.
//
// WHAT IT IS. snowplow's real refresh machinery at run 8's shape, in one test
// binary, so the decision between fix A, fix B and fix C is measured here before
// any paid GKE run:
//
//   - a cache.ResourceWatcher over a dynamic fake holding run 8's population:
//     50 namespaces x 1,000 objects of the bench composition GVR, plus every other
//     composition GVR run 8 filled (content_fill_by_gvr), with their CRDs and
//     CompositionDefinitions;
//   - run 8's RBAC (e2e/bench/bench/rbac_model.py: 50 team Roles, 10% cross-team
//     users, 10 admins, so 153 identity classes at 1,000 users) plus the portal's
//     devs grants;
//   - the RESTActions dashboard-data and compositions-list and their 27 widgets,
//     rendered from the portal chart at the jqinterest corpus commit, and the
//     bench's own probe RESTAction;
//   - the production GET /call chain: middleware.UserConfig (a real RS256 JWT
//     per user, the <user>-clientconfig Secret from the Secrets informer) ->
//     FallthroughScopeMiddleware -> handlers.Dispatcher(dispatchers.All()) ->
//     the real RESTAction / Widgets handlers. No resolver seam;
//   - the production refresher (default parallelism 4) behind the production
//     refresh body (resolveAndPopulateL1), the dep-event watcher that marks
//     cells, the real api-stage content cells, and the background workers
//     main.go starts (deps reconcile, store verification, sliceability
//     reverifier, the customer-inflight hook).
//
// The ONE out-of-process stand-in is a fake apiserver that answers what an
// in-cluster pod gets from the real one and the informers cannot serve:
// discovery and the widget CRDs widgets.Resolve validates its status against.
// Every object read is informer-served; an object read that reaches the fake
// apiserver is COUNTED and reported (it means the harness is not measuring the
// informer path).
//
// WHAT IT MEASURES. Everything comes from snowplow's own instruments, read in
// process: the dirty_to_fresh samples snowplow emits (SetDirtyToFreshRecorder),
// the dirty-mark attribution counters, the refresher's stat family, the content
// fill counters by reason / GVR / RESTAction, the resolved cache's counters and
// the fill admission gate's per-class queue. The harness computes no figure the
// code under test also computes; its own probe (write a marker, poll the probe
// RESTAction through the real /call every 250 ms) and its customer loop are
// clients, exactly like the bench's fresh606 and load606.
//
// BOUNDS. No bound in an arm is derived from the code under test. The run-8
// calibration targets are constants copied from the run-8 capture; the
// acceptance bars are the owner's rulings.
package scale765_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakewatch"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

//go:embed testdata/portal.yaml
var s765PortalYAML []byte

//go:embed testdata/widget_crds.yaml
var s765WidgetCRDsYAML []byte

//go:embed testdata/probe.yaml
var s765ProbeYAML []byte

// ---------------------------------------------------------------------------
// Population constants (run 8, RUN unless marked)
// ---------------------------------------------------------------------------

const (
	// s765RelNS is the portal's release namespace and the authn namespace, as
	// on run 8 (content_fill_by_ra keys are "krateo-system/...").
	s765RelNS = "krateo-system"
	// s765FreshAnnotation is the key run 8's ChurnDriver AND its probe wrote
	// (load606.py FRESH_ANNOTATION; _cmd_fresh churns with the same patch).
	s765FreshAnnotation = "snowplow-bench/fresh-marker"
	s765ProbeNS         = "bench-ns-01"
	s765ProbeName       = "bench-app-01-01"
	s765ProbeRA         = "bench-fresh-listdep"
	s765ProbeRAStatus   = "bench-fresh-listdep-status"
	s765JWTKeyID        = "s765"
)

// s765SABearerFixture is a JWT-SHAPED (unsigned, never verified) bearer whose
// sub is snowplow's own ServiceAccount: identity-free cells refresh under that
// subject. Assembled at runtime from its JSON parts, so the repository holds no
// credential-shaped literal.
func s765SABearerFixture() string {
	seg := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return seg(`{"alg":"RS256","kid":"test"}`) + "." +
		seg(`{"sub":"system:serviceaccount:`+s765RelNS+`:snowplow"}`) + "." +
		seg("signature-bytes")
}

var (
	s765BenchGVR = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1-2-2", Resource: "githubscaffoldingwithcompositionpages"}
	s765RAGVR    = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	s765CRDGVR   = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	s765CDGVR    = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1alpha1", Resource: "compositiondefinitions"}
)

// s765OtherComp is one of the other composition GVRs run 8 filled: the exact set
// in content_fill_by_gvr over 10:10-10:39Z (max_items 1, builderpublishes 0), with
// run 8's max_bytes for the one-object envelope.
type s765OtherComp struct {
	version, plural string
	objects         int
	envelopeBytes   int
}

// s765OtherComps: 23 GVRs with one object plus builderpublishes with none (RUN,
// run8-mem606.jsonl). The spec said "~36"; the capture says 24.
var s765OtherComps = []s765OtherComp{
	{"v0-1-1", "clickhouseoperators", 1, 4858},
	{"v0-1-10", "otelcollectordaemonsets", 1, 3539},
	{"v0-1-15", "krateosseproxies", 1, 2747},
	{"v0-1-2", "krateomarketplacecatalogs", 1, 2282},
	{"v0-1-22", "krateoobservabilities", 1, 8754},
	{"v0-15-6", "gitprovidercrds", 1, 2441},
	{"v0-15-6", "gitproviders", 1, 2854},
	{"v0-2-0", "mongodboperators", 1, 4505},
	{"v0-2-49", "alertprovidercrds", 1, 2251},
	{"v0-27-3", "authncrds", 1, 3160},
	{"v0-27-3", "authns", 1, 3878},
	{"v0-28-1", "controllerrenderservices", 1, 3103},
	{"v0-28-1", "oasgenprovidercrds", 1, 2252},
	{"v0-28-1", "oasgenproviders", 1, 3643},
	{"v0-3-430", "installers", 1, 26859},
	{"v0-3-8", "otelcollectordeployments", 1, 3531},
	{"v0-5-0", "blueprintrenderservices", 1, 3083},
	{"v1-12-50", "snowplowcrds", 1, 2218},
	{"v1-12-50", "snowplows", 1, 4325},
	{"v1-8-4", "frontendcrds", 1, 12894},
	{"v1-8-4", "frontends", 1, 4859},
	{"v1-8-72", "builderpublishes", 0, 87},
	{"v1-8-72", "portalagents", 1, 30481},
	{"v1-8-72", "portals", 1, 143017},
}

// s765Run8BytesPerItem: run 8's 50K envelope, max_bytes 70,745,429 / 50,000 (RUN).
const s765Run8BytesPerItem = 70745429.0 / 50000.0

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// s765Config is the population shape. Full = run 8; smoke = 1/50 of it.
type s765Config struct {
	Name           string
	Namespaces     int
	PerNS          int
	Users          int
	Admins         int
	CrossTeamEvery int
	ChurnPerNS     int
	// PopulateWorkers is the number of concurrent customer calls while the
	// resident set is built (run 8's driver ran --call-workers 512 over 1,000
	// users; 64 is enough to keep 4 cores busy).
	PopulateWorkers int
}

func s765Full() s765Config {
	return s765Config{Name: "full", Namespaces: 50, PerNS: 1000, Users: 1000, Admins: 10,
		CrossTeamEvery: 10, ChurnPerNS: 20, PopulateWorkers: 64}
}

func s765Smoke() s765Config {
	return s765Config{Name: "smoke", Namespaces: 50, PerNS: 20, Users: 20, Admins: 10,
		CrossTeamEvery: 10, ChurnPerNS: 20, PopulateWorkers: 16}
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

type s765User struct {
	name   string
	groups []string
	token  string
}

type s765Widget struct {
	kind, plural, name, apiRef string
}

type s765H struct {
	t   *testing.T
	cfg s765Config

	dyn     *dynamicfake.FakeDynamicClient
	rw      *cache.ResourceWatcher
	call    http.Handler
	sse     *httptest.Server // the production /refreshes SSE route (RefreshAuth -> handlers.Refreshes), streaming
	users   []s765User
	widgets []s765Widget
	api     *s765APIServer

	rvSeq atomic.Int64

	// dirty_to_fresh samples snowplow emits (its own histogram's input).
	d2fMu      sync.Mutex
	d2fOn      bool
	d2fSamples []int64

	cancel context.CancelFunc
}

// s765Setup builds the harness. Every global it touches is restored by
// t.Cleanup, so the smoke variant can run inside the package suite.
func s765Setup(t *testing.T, cfg s765Config) *s765H {
	t.Helper()
	tSetup := time.Now()
	h := &s765H{t: t, cfg: cfg}

	// The chart's cache env (helm/snowplow/values.yaml), so the harness runs
	// the production posture: nothing here changes a refresher knob.
	for k, v := range map[string]string{
		"CACHE_ENABLED":                            "true",
		"RESOLVED_CACHE_ENABLED":                   "true",
		"AUTHN_NAMESPACE":                          s765RelNS,
		"UAF_RESOLVED_TTL_SECONDS":                 "300",
		"CATALOG_UNSERVABLE_TTL_SECONDS":           "300",
		"DEPS_RELIST_BRIDGE_TIMEOUT_SECONDS":       "120",
		"DEPS_RECONCILE_SAMPLE":                    "512",
		"DEPS_RECONCILE_PERIOD_SECONDS":            "30",
		"REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND": "5",
		"REFRESH_EVICTION_PUBLISH_BURST":           "10",
		"RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS":     "86400",
	} {
		t.Setenv(k, v)
	}
	// GOGC 50 / GOMEMLIMIT 7GiB, as the chart sets them.
	prevGC := debug.SetGCPercent(50)
	prevLimit := debug.SetMemoryLimit(7 << 30)
	t.Cleanup(func() { debug.SetGCPercent(prevGC); debug.SetMemoryLimit(prevLimit) })
	// LOG_LEVEL=warn, as the chart sets it; the bytes go nowhere.
	// main.go routes package-level slog AND every request/background ctx
	// logger through one handler at LOG_LEVEL; so does the harness.
	s765Log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if os.Getenv("SCALE765_DEBUG_LOG") != "" { // a harness debugging aid, never on in a measured run
		s765Log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	prevLog := slog.Default()
	slog.SetDefault(s765Log)
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	t.Setenv("DEBUG", "false")
	// Out of cluster, plumbing's UserConfig would rewrite every endpoint to
	// kubernetes.default.svc; test mode keeps the fake apiserver's URL, so a
	// stray apiserver read is answered (and counted) instead of timing out.
	prevTM := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prevTM) })

	if err := cache.ResetCacheProcessStateForTest(); err != nil {
		t.Fatalf("ResetCacheProcessStateForTest: %v", err)
	}

	// --- fixtures ---
	portal := s765Decode(t, s765PortalYAML)
	widgetCRDs := s765Decode(t, s765WidgetCRDsYAML)
	probes := s765Decode(t, s765ProbeYAML)
	pluralOf := map[string]string{}
	for _, crd := range widgetCRDs {
		kind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")
		plural, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "plural")
		pluralOf[kind] = plural
	}
	var portalUnstructured []*unstructured.Unstructured
	var rbacObjs []k8sruntime.Object
	for _, o := range portal {
		switch {
		case o.GetAPIVersion() == "rbac.authorization.k8s.io/v1":
			rbacObjs = append(rbacObjs, s765TypedRBAC(t, o))
		case strings.HasPrefix(o.GetAPIVersion(), "widgets.templates.krateo.io/"):
			ref, _, _ := unstructured.NestedString(o.Object, "spec", "apiRef", "name")
			plural := pluralOf[o.GetKind()]
			if plural == "" {
				t.Fatalf("no CRD for widget kind %s", o.GetKind())
			}
			h.widgets = append(h.widgets, s765Widget{kind: o.GetKind(), plural: plural, name: o.GetName(), apiRef: ref})
			portalUnstructured = append(portalUnstructured, o)
		default:
			portalUnstructured = append(portalUnstructured, o)
		}
	}
	if len(h.widgets) != 27 {
		t.Fatalf("portal fixture: %d widgets, want 27", len(h.widgets))
	}
	// Widget GVRs the 27 widgets only REFERENCE (resourcesRefs): on a cluster
	// they exist, so their informers register and they are discoverable.
	refOnly := map[schema.GroupVersionResource]bool{}
	for _, o := range portal {
		if !strings.HasPrefix(o.GetAPIVersion(), "widgets.templates.krateo.io/") {
			continue
		}
		items, _, _ := unstructured.NestedSlice(o.Object, "spec", "resourcesRefs", "items")
		tmpls, _, _ := unstructured.NestedSlice(o.Object, "spec", "resourcesRefsTemplate")
		for _, tp := range tmpls {
			if m, ok := tp.(map[string]any); ok {
				if inner, ok := m["template"]; ok {
					items = append(items, inner)
				}
			}
		}
		for _, it := range items {
			m, _ := it.(map[string]any)
			av, _ := m["apiVersion"].(string)
			res, _ := m["resource"].(string)
			gv, err := schema.ParseGroupVersion(av)
			if err != nil || res == "" || strings.Contains(av+res, "$") {
				continue
			}
			known := false
			for _, p := range pluralOf {
				known = known || p == res
			}
			if !known {
				refOnly[gv.WithResource(res)] = true
			}
		}
	}

	// --- the fake apiserver (discovery + widget CRDs only) ---
	h.api = newS765APIServer(t)
	saRC := &rest.Config{Host: h.api.srv.URL}
	saEP := &endpoints.Endpoint{ServerURL: h.api.srv.URL, Token: s765SABearerFixture()}

	// --- objects ---
	scheme := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		s765RAGVR:    "RESTActionList",
		s765CRDGVR:   "CustomResourceDefinitionList",
		s765CDGVR:    "CompositionDefinitionList",
		s765BenchGVR: "GithubScaffoldingWithCompositionPageList",
	}
	for _, r := range []string{"clusterrolebindings", "clusterroles", "rolebindings", "roles"} {
		listKinds[schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}] =
			map[string]string{"clusterrolebindings": "ClusterRoleBindingList", "clusterroles": "ClusterRoleList",
				"rolebindings": "RoleBindingList", "roles": "RoleList"}[r]
	}
	compGVRs := []schema.GroupVersionResource{s765BenchGVR}
	h.api.add(s765BenchGVR, "GithubScaffoldingWithCompositionPage", true)
	for _, oc := range s765OtherComps {
		gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: oc.version, Resource: oc.plural}
		kind := s765KindFor(oc.plural)
		listKinds[gvr] = kind + "List"
		compGVRs = append(compGVRs, gvr)
		h.api.add(gvr, kind, true)
	}
	for _, w := range h.widgets {
		gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: w.plural}
		listKinds[gvr] = w.kind + "List"
		h.api.add(gvr, w.kind, true)
	}
	for gvr := range refOnly {
		kind := s765KindFor(gvr.Resource)
		listKinds[gvr] = kind + "List"
		h.api.add(gvr, kind, true)
	}
	h.api.add(s765RAGVR, "RESTAction", true)
	h.api.add(s765CRDGVR, "CustomResourceDefinition", false)
	h.api.add(s765CDGVR, "CompositionDefinition", true)

	var seed []k8sruntime.Object
	seed = append(seed, rbacObjs...)
	seed = append(seed, s765TeamRBAC(cfg)...)
	for _, o := range portalUnstructured {
		seed = append(seed, o)
	}
	for _, o := range probes {
		seed = append(seed, o)
	}
	// CRDs: every composition GVR + the widget CRDs (served whole, schemas and
	// all, as the cluster's CRD list is).
	crdByName := map[string][]byte{}
	for _, crd := range widgetCRDs {
		raw, _ := json.Marshal(crd.Object)
		crdByName[crd.GetName()] = raw
		seed = append(seed, crd)
	}
	for _, gvr := range compGVRs {
		crd := s765CompositionCRD(gvr)
		raw, _ := json.Marshal(crd.Object)
		crdByName[crd.GetName()] = raw
		seed = append(seed, crd)
		seed = append(seed, s765CompositionDefinition(gvr))
	}
	h.api.setCRDs(crdByName)
	// The bench composition GVR: cfg.Namespaces x cfg.PerNS objects.
	for n := 1; n <= cfg.Namespaces; n++ {
		for k := 1; k <= cfg.PerNS; k++ {
			seed = append(seed, s765Composition(n, k))
		}
	}
	for _, oc := range s765OtherComps {
		for i := 0; i < oc.objects; i++ {
			seed = append(seed, s765OtherObject(oc))
		}
	}
	h.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	barrier := fakewatch.Install(h.dyn)

	// --- users, their JWTs and their <user>-clientconfig Secrets ---
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	var secrets []k8sruntime.Object
	for i := 1; i <= cfg.Users; i++ {
		u := s765User{name: fmt.Sprintf("scaleuser-%04d", i), groups: s765UserGroups(cfg, i)}
		u.token, err = jwtutil.CreateToken(jwtutil.CreateTokenOptions{
			Username: u.name, Groups: u.groups, Duration: 48 * time.Hour, KeyID: s765JWTKeyID, PrivateKey: key,
		})
		if err != nil {
			t.Fatalf("CreateToken: %v", err)
		}
		h.users = append(h.users, u)
		secrets = append(secrets, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: u.name + "-clientconfig", Namespace: s765RelNS,
				UID: types.UID("uid-cc-" + u.name), ResourceVersion: "1"},
			Data: map[string][]byte{"server-url": []byte(h.api.srv.URL)},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(s765Log)) // main.go's cacheCtx
	h.cancel = cancel

	// --- the Secrets informer (the per-user clientconfig lookup, F-3) ---
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	restoreCli := cache.SetSecretsClientForTest(kubefake.NewSimpleClientset(secrets...))
	if err := cache.StartSecretsInformer(ctx, nil, s765RelNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	t.Cleanup(func() {
		restoreCli()
		cache.ResetSecretsInformerForTest()
		cache.ResetSecretsSnapshotForTest()
	})
	s765Await(t, 60*time.Second, "secrets cache servable", cache.SecretsCacheServable)

	// --- the watcher ---
	rw, err := cache.NewResourceWatcher(ctx, h.dyn)
	if err != nil {
		cancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	h.rw = rw
	prevGlobal := cache.Global()
	cache.SetGlobal(rw)
	cache.SetProcessSARestConfig(saRC)
	watched := append([]schema.GroupVersionResource{s765RAGVR, s765CRDGVR, s765CDGVR}, compGVRs...)
	for _, w := range h.widgets {
		watched = append(watched, schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: w.plural})
	}
	for gvr := range refOnly {
		watched = append(watched, gvr)
	}
	resources := map[string]bool{}
	for _, gvr := range watched {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Minute):
			t.Fatalf("informer %s did not sync", gvr)
		}
		resources[gvr.Resource] = true
	}
	var resList []string
	for r := range resources {
		resList = append(resList, r)
	}
	if err := barrier.WaitFor(2*time.Minute, resList...); err != nil {
		t.Fatalf("fakewatch: %v", err)
	}
	if err := rw.WaitForCacheSync(context.Background(), 2*time.Minute); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(2 * time.Minute); err != nil {
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	for _, gvr := range watched {
		gvr := gvr
		s765Await(t, 2*time.Minute, "servable "+gvr.String(), func() bool { return rw.IsServable(gvr) })
	}

	// --- the UAF stages' SA endpoint (in cluster: the projected SA token) ---
	restoreSA := api.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) { return saEP, nil })
	// ... and rest.InClusterConfig, which api.Resolve falls back to when the
	// dispatcher has no SA *rest.Config (out of cluster it errors, and every
	// resolve would come back empty).
	restoreIC := api.SetInClusterConfigForTest(func() (*rest.Config, error) { return saRC, nil })

	// --- the refresher, exactly as main.go wires it (RegisterRefreshHandlers,
	// with the SA transport pointed at the fake apiserver: out of cluster the
	// projected SA files RegisterRefreshHandlers reads do not exist) ---
	dispatchers.RegisterRefreshHandlersWithSATransportForTest(saEP, saRC)
	cache.SetCustomerInflightHook(dispatchers.CustomerInFlight)
	dispatchers.RegisterL1LookupMetricsForTest() // main.go: registered at init when the cache is on
	cache.StartRefresher(ctx)
	cache.StartDepsReconcile(ctx)
	cache.StartStoreVerification(ctx)
	cache.StartSliceabilityReverifier(ctx)
	cache.SetDirtyToFreshRecorder(func(ms int64) {
		h.d2fMu.Lock()
		if h.d2fOn {
			h.d2fSamples = append(h.d2fSamples, ms)
		}
		h.d2fMu.Unlock()
	})

	// --- the GET /call chain (main.go) ---
	h.call = use.NewChain(use.TraceId(), use.Logger(s765Log)).Append(
		middleware.UserConfig(jwtutil.NewStaticKeySource(&key.PublicKey), s765RelNS),
		cache.FallthroughScopeMiddleware(cache.ScopeCallGeneric),
		handlers.Dispatcher(dispatchers.All())).
		Then(handlers.Call())
	// --- the /refreshes SSE route (main.go): a real streaming server, since an
	// SSE response must flush frame by frame ---
	sseMux := http.NewServeMux()
	sseMux.Handle("POST /refreshes", use.NewChain(use.TraceId(), use.Logger(s765Log)).Append(
		middleware.RefreshAuth(jwtutil.NewStaticKeySource(&key.PublicKey))).
		Then(handlers.Refreshes()))
	h.sse = httptest.NewServer(sseMux)
	t.Cleanup(h.sse.Close)

	t.Cleanup(func() {
		cache.SetDirtyToFreshRecorder(nil)
		cancel()
		restoreSA()
		restoreIC()
		cache.SetCustomerInflightHook(nil)
		if err := cache.ResetCacheProcessStateForTest(); err != nil {
			t.Errorf("teardown ResetCacheProcessStateForTest: %v", err)
		}
		rw.Stop()
		cache.SetGlobal(prevGlobal)
		cache.ResetProcessSARestConfigForTest()
		cache.PublishRBACSnapshotForTest(nil)
	})
	t.Logf("scale765 %s: setup %s (objects=%d users=%d widgets=%d)", cfg.Name,
		time.Since(tSetup).Round(time.Millisecond), cfg.Namespaces*cfg.PerNS, len(h.users), len(h.widgets))
	return h
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

func s765Decode(t *testing.T, raw []byte) []*unstructured.Unstructured {
	t.Helper()
	dec := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	var out []*unstructured.Unstructured
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatalf("decode fixture: %v", err)
		}
		if len(m) == 0 {
			continue
		}
		out = append(out, &unstructured.Unstructured{Object: m})
	}
}

func s765TypedRBAC(t *testing.T, o *unstructured.Unstructured) k8sruntime.Object {
	t.Helper()
	var obj k8sruntime.Object
	switch o.GetKind() {
	case "Role":
		obj = &rbacv1.Role{}
	case "RoleBinding":
		obj = &rbacv1.RoleBinding{}
	case "ClusterRole":
		obj = &rbacv1.ClusterRole{}
	case "ClusterRoleBinding":
		obj = &rbacv1.ClusterRoleBinding{}
	default:
		t.Fatalf("unexpected RBAC kind %s", o.GetKind())
	}
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(o.Object, obj); err != nil {
		t.Fatalf("convert %s/%s: %v", o.GetKind(), o.GetName(), err)
	}
	m := obj.(metav1.Object)
	m.SetUID(types.UID("uid-" + strings.ToLower(o.GetKind()) + "-" + o.GetNamespace() + "-" + o.GetName()))
	m.SetResourceVersion("1")
	return obj
}

// s765ReaderRules is rbac_model.READER_RULES.
var s765ReaderRules = []rbacv1.PolicyRule{
	{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"*"}, Verbs: []string{"get", "list"}},
	{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}},
}

// s765TeamRBAC is rbac_model.namespace_rbac_yaml for every namespace plus
// admin_rbac_yaml, plus snowplow's own ServiceAccount read grant (the chart's
// ClusterRole: the identity-free cells refresh under it).
func s765TeamRBAC(cfg s765Config) []k8sruntime.Object {
	var out []k8sruntime.Object
	for n := 1; n <= cfg.Namespaces; n++ {
		ns := fmt.Sprintf("bench-ns-%02d", n)
		out = append(out,
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "bench-team-reader", Namespace: ns, UID: types.UID("uid-role-" + ns), ResourceVersion: "1"},
				Rules: s765ReaderRules},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "bench-team-reader", Namespace: ns, UID: types.UID("uid-rb-" + ns), ResourceVersion: "1"},
				RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "bench-team-reader"},
				Subjects: []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "Group", Name: fmt.Sprintf("bench-team-%02d", n)}}},
		)
	}
	out = append(out,
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "bench-platform-admin-reader", UID: "uid-cr-admin", ResourceVersion: "1"}, Rules: s765ReaderRules},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "bench-platform-admin-reader", UID: "uid-crb-admin", ResourceVersion: "1"},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "bench-platform-admin-reader"},
			Subjects: []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "Group", Name: "bench-platform-admins"}}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "snowplow-s765-reader", UID: "uid-cr-snowplow", ResourceVersion: "1"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "snowplow-s765-reader", UID: "uid-crb-snowplow", ResourceVersion: "1"},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "snowplow-s765-reader"},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: s765RelNS}}},
	)
	return out
}

// s765UserGroups is rbac_model.user_groups (1-based i), verbatim.
func s765UserGroups(cfg s765Config, i int) []string {
	home := ((i - 1) % cfg.Namespaces) + 1
	groups := []string{"devs", fmt.Sprintf("bench-team-%02d", home)}
	if cfg.CrossTeamEvery > 0 && i%cfg.CrossTeamEvery == 0 && cfg.Namespaces >= 2 {
		other := ((home - 1 + 1 + (i/cfg.CrossTeamEvery)%(cfg.Namespaces-1)) % cfg.Namespaces) + 1
		if other != home {
			groups = append(groups, fmt.Sprintf("bench-team-%02d", other))
		}
	}
	if i <= cfg.Admins {
		groups = append(groups, "bench-platform-admins")
	}
	return groups
}

// s765IdentityClasses counts distinct group sets (rbac_model.identity_classes).
func s765IdentityClasses(cfg s765Config) int {
	seen := map[string]bool{}
	for i := 1; i <= cfg.Users; i++ {
		g := append([]string(nil), s765UserGroups(cfg, i)...)
		sort.Strings(g)
		seen[strings.Join(g, ",")] = true
	}
	return len(seen)
}

func s765KindFor(plural string) string {
	s := strings.TrimSuffix(plural, "s")
	if strings.HasSuffix(plural, "ies") {
		s = strings.TrimSuffix(plural, "ies") + "y"
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// s765Composition is cluster.composition_yaml(ns, name) verbatim, as the API
// server returns it: run 8's compositions had no status (controller held).
func s765Composition(n, k int) *unstructured.Unstructured {
	ns := fmt.Sprintf("bench-ns-%02d", n)
	name := fmt.Sprintf("bench-app-%02d-%02d", n, k)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1-2-2",
		"kind":       "GithubScaffoldingWithCompositionPage",
		"metadata": map[string]any{
			"name": name, "namespace": ns,
			"uid":               fmt.Sprintf("0b6f3c2e-%04d-4d1a-9c0e-%012d", n, k),
			"resourceVersion":   "1",
			"generation":        int64(1),
			"creationTimestamp": "2026-10-19T09:00:00Z",
		},
		"spec": map[string]any{
			"argocd": map[string]any{
				"namespace": "krateo-system",
				"application": map[string]any{
					"project":     "default",
					"source":      map[string]any{"path": "chart/"},
					"destination": map[string]any{"server": "https://kubernetes.default.svc", "namespace": name},
					"syncEnabled": false,
					"syncPolicy":  map[string]any{"automated": map[string]any{"prune": true, "selfHeal": true}},
				},
			},
			"app": map[string]any{"service": map[string]any{"type": "NodePort", "port": int64(31180)}},
			"git": map[string]any{
				"unsupportedCapabilities": true,
				"insecure":                true,
				"fromRepo": map[string]any{
					"scmUrl": "https://github.com", "org": "krateoplatformops-blueprints",
					"name": "github-scaffolding-with-composition-page", "branch": "main", "path": "skeleton/",
					"credentials": map[string]any{"authMethod": "generic",
						"secretRef": map[string]any{"namespace": "krateo-system", "name": "github-repo-creds", "key": "token"}},
				},
				"toRepo": map[string]any{
					"scmUrl": "https://github.com", "org": "krateoplatformops-test", "name": name,
					"branch": "main", "path": "/",
					"credentials": map[string]any{"authMethod": "generic",
						"secretRef": map[string]any{"namespace": "krateo-system", "name": "github-repo-creds", "key": "token"}},
					"private": false, "initialize": true, "deletionPolicy": "Delete", "verbose": false,
					"configurationRef": map[string]any{"name": "repo-config", "namespace": "demo-system"},
				},
			},
		},
	}}
}

// s765OtherObject is the one object of another composition GVR, padded to run
// 8's envelope size for that GVR.
func s765OtherObject(oc s765OtherComp) *unstructured.Unstructured {
	pad := oc.envelopeBytes - 600
	if pad < 0 {
		pad = 0
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/" + oc.version,
		"kind":       s765KindFor(oc.plural),
		"metadata": map[string]any{
			"name": "krateo-" + strings.TrimSuffix(oc.plural, "s"), "namespace": s765RelNS,
			"uid": "uid-" + oc.plural, "resourceVersion": "1", "creationTimestamp": "2026-10-19T09:00:00Z",
		},
		"spec": map[string]any{"values": strings.Repeat("x", pad)},
		// The status shape 057's platform compositions carry (read 2026-10-19):
		// Ready, and Synced, whose lastTransitionTime their controller rewrites.
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "reason": "Available",
				"message": "Composition is up-to-date", "lastTransitionTime": "2026-10-19T07:44:12Z"},
			map[string]any{"type": "Synced", "status": "True", "reason": "ReconcileSuccess",
				"message": "", "lastTransitionTime": "2026-10-19T09:00:00Z"}}},
	}}
}

// PLATFORM CHURN (#765 ruling b: measured, never picked). Run 8's 24 other
// composition GVRs are the platform's own compositions, and their live
// controllers write them. Measured READ-ONLY on krateo-057 (snowplow 1.12.50),
// 2026-10-19 12:23:59Z-12:39:07Z (908 s), one `kubectl get <plural> -A
// --watch-only --output-watch-events` per GVR: MODIFIED events per object.
// Every event observed was a status write: generation unchanged, and a full-
// object diff of the snowplows and installers objects shows exactly
// status.conditions[Synced].lastTransitionTime changing, about once a minute.
// builderpublishes (17 objects on 057, 14.9 events per object) has no object in
// run 8's population, so it writes nothing here.
const s765PlatformWindowSec = 908.0

var s765PlatformEvents = map[string]float64{ // per object, in the window
	"clickhouseoperators": 15, "otelcollectordaemonsets": 15, "krateosseproxies": 15,
	"krateomarketplacecatalogs": 15, "krateoobservabilities": 15, "gitprovidercrds": 15,
	"gitproviders": 15, "mongodboperators": 15, "alertprovidercrds": 15, "authncrds": 15,
	"authns": 15, "controllerrenderservices": 15, "oasgenprovidercrds": 15, "oasgenproviders": 15,
	"installers": 15, "otelcollectordeployments": 15, "blueprintrenderservices": 15,
	"snowplowcrds": 15, "snowplows": 15, "frontendcrds": 15, "frontends": 15,
	"builderpublishes": 254.0 / 17, "portalagents": 15, "portals": 4,
}

// writePlatform is one controller status write on a platform composition:
// conditions[Synced].lastTransitionTime = now, as measured on 057.
func (h *s765H) writePlatform(oc s765OtherComp) error {
	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: oc.version, Resource: oc.plural}
	ri := h.dyn.Resource(gvr).Namespace(s765RelNS)
	obj, err := ri.Get(context.Background(), "krateo-"+strings.TrimSuffix(oc.plural, "s"), metav1.GetOptions{})
	if err != nil {
		return err
	}
	conds, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for i, c := range conds {
		if m, ok := c.(map[string]any); ok && m["type"] == "Synced" {
			m["lastTransitionTime"] = time.Now().UTC().Format(time.RFC3339Nano)
			conds[i] = m
		}
	}
	_ = unstructured.SetNestedSlice(obj.Object, conds, "status", "conditions")
	obj.SetResourceVersion(strconv.FormatInt(1000+h.rvSeq.Add(1), 10))
	_, err = ri.Update(context.Background(), obj, metav1.UpdateOptions{})
	return err
}

func s765CompositionCRD(gvr schema.GroupVersionResource) *unstructured.Unstructured {
	kind := s765KindFor(gvr.Resource)
	if gvr == s765BenchGVR {
		kind = "GithubScaffoldingWithCompositionPage"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": gvr.Resource + "." + gvr.Group, "uid": "uid-crd-" + gvr.Resource, "resourceVersion": "1"},
		"spec": map[string]any{
			"group": gvr.Group,
			"names": map[string]any{"kind": kind, "listKind": kind + "List", "plural": gvr.Resource,
				"singular": strings.ToLower(kind)},
			"scope": "Namespaced",
			"versions": []any{map[string]any{"name": gvr.Version, "served": true, "storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object",
					"x-kubernetes-preserve-unknown-fields": true}}}},
		},
		"status": map[string]any{"acceptedNames": map[string]any{"kind": kind, "plural": gvr.Resource}},
	}}
}

func s765CompositionDefinition(gvr schema.GroupVersionResource) *unstructured.Unstructured {
	ns, name := s765RelNS, gvr.Resource
	if gvr == s765BenchGVR {
		ns, name = s765ProbeNS, "github-scaffolding-with-composition-page"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "core.krateo.io/v1alpha1",
		"kind":       "CompositionDefinition",
		"metadata":   map[string]any{"name": name, "namespace": ns, "uid": "uid-cd-" + gvr.Resource, "resourceVersion": "1"},
		"spec":       map[string]any{"chart": map[string]any{"repo": name, "url": "https://marketplace.krateo.io", "version": "1.2.2"}},
		"status": map[string]any{
			"resource":   gvr.Resource,
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
		},
	}}
}

// ---------------------------------------------------------------------------
// The fake apiserver: discovery and CRDs, nothing else
// ---------------------------------------------------------------------------

type s765APIServer struct {
	srv *httptest.Server

	mu       sync.Mutex
	gvrs     map[schema.GroupVersion][]metav1.APIResource
	crds     map[string][]byte
	served   map[string]int // discovery + CRD GETs
	notFound map[string]int // every other path: an apiserver read the informers did not serve
}

func newS765APIServer(t *testing.T) *s765APIServer {
	s := &s765APIServer{gvrs: map[schema.GroupVersion][]metav1.APIResource{}, served: map[string]int{}, notFound: map[string]int{}}
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case p == "/api":
			s.served[p]++
			writeJSON(w, &metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
			return
		case p == "/api/v1":
			s.served[p]++
			writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1"})
			return
		case p == "/apis":
			s.served[p]++
			byGroup := map[string][]string{}
			for gv := range s.gvrs {
				byGroup[gv.Group] = append(byGroup[gv.Group], gv.Version)
			}
			var groups []metav1.APIGroup
			for g, vs := range byGroup {
				sort.Strings(vs)
				var gvd []metav1.GroupVersionForDiscovery
				for _, v := range vs {
					gvd = append(gvd, metav1.GroupVersionForDiscovery{GroupVersion: g + "/" + v, Version: v})
				}
				groups = append(groups, metav1.APIGroup{Name: g, Versions: gvd, PreferredVersion: gvd[0]})
			}
			sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
			writeJSON(w, &metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}, Groups: groups})
			return
		case strings.HasPrefix(p, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/"):
			if raw, ok := s.crds[strings.TrimPrefix(p, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/")]; ok {
				s.served["crd"]++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(raw)
				return
			}
		default:
			parts := strings.Split(strings.Trim(p, "/"), "/")
			if len(parts) == 3 && parts[0] == "apis" {
				gv := schema.GroupVersion{Group: parts[1], Version: parts[2]}
				if res, ok := s.gvrs[gv]; ok {
					s.served["discovery"]++
					writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
						GroupVersion: gv.String(), APIResources: res})
					return
				}
			}
		}
		s.notFound[s765PathClass(p)]++
		http.NotFound(w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// s765PathClass drops object names so the not-found table stays small.
func s765PathClass(p string) string {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) > 6 {
		parts = parts[:6]
	}
	return "/" + strings.Join(parts, "/")
}

func (s *s765APIServer) add(gvr schema.GroupVersionResource, kind string, namespaced bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gv := gvr.GroupVersion()
	s.gvrs[gv] = append(s.gvrs[gv], metav1.APIResource{Name: gvr.Resource, SingularName: strings.ToLower(kind),
		Namespaced: namespaced, Kind: kind, Verbs: metav1.Verbs{"get", "list", "watch"}})
}

func (s *s765APIServer) setCRDs(m map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crds = m
}

func (s *s765APIServer) unservedReads() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for k, v := range s.notFound {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// Customer calls through the real chain
// ---------------------------------------------------------------------------

func s765WidgetPath(w s765Widget) string {
	return "/call?apiVersion=widgets.templates.krateo.io%2Fv1beta1&resource=" + w.plural +
		"&name=" + w.name + "&namespace=" + s765RelNS
}

func s765RAPath(name, ns string) string {
	return "/call?apiVersion=templates.krateo.io%2Fv1&resource=restactions&name=" + url.QueryEscape(name) + "&namespace=" + ns
}

// get issues one customer GET /call as u through the production chain.
func (h *s765H) get(u s765User, path string) (int, []byte, time.Duration) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+u.token)
	rec := httptest.NewRecorder()
	t0 := time.Now()
	h.call.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes(), time.Since(t0)
}

// s765PopulateReport is the resident set the customer calls built.
type s765PopulateReport struct {
	Wall              string         `json:"wall"`
	Calls             int            `json:"calls"`
	StatusByWidget    map[string]any `json:"status_by_widget"`
	ProbeStatus       map[string]int `json:"probe_status"`
	ResidentByClass   map[string]int `json:"resident_by_class"`
	ResidentTotal     int            `json:"resident_total"`
	IdentityClasses   int            `json:"identity_classes"`
	QuiesceWall       string         `json:"quiesce_wall"`
	QuiesceReached    bool           `json:"quiesce_reached"`
	BenchBytesPerItem float64        `json:"bench_bytes_per_item"`
	UnservedReads     map[string]int `json:"apiserver_unserved_reads"`
}

// populate has every user load the 27 widgets (the dashboard/compositions views,
// default extras), and the probe user load both probe RESTActions; then waits
// for the refresher to go quiet.
func (h *s765H) populate() s765PopulateReport {
	h.t.Helper()
	t0 := time.Now()
	type task struct {
		u s765User
		w s765Widget
	}
	tasks := make(chan task)
	var mu sync.Mutex
	status := map[string]map[int]int{}
	var calls atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < h.cfg.PopulateWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tk := range tasks {
				code, _, _ := h.get(tk.u, s765WidgetPath(tk.w))
				calls.Add(1)
				mu.Lock()
				if status[tk.w.name] == nil {
					status[tk.w.name] = map[int]int{}
				}
				status[tk.w.name][code]++
				mu.Unlock()
			}
		}()
	}
	for _, u := range h.users {
		for _, w := range h.widgets {
			tasks <- task{u, w}
		}
	}
	close(tasks)
	wg.Wait()
	probe := map[string]int{}
	for _, ra := range []string{s765ProbeRA, s765ProbeRAStatus} {
		code, body, _ := h.get(h.probeUser(), s765RAPath(ra, s765ProbeNS))
		probe[ra] = code
		if code != http.StatusOK {
			h.t.Fatalf("probe RESTAction %s: HTTP %d: %.300s", ra, code, body)
		}
	}
	wall := time.Since(t0)

	q0 := time.Now()
	reached := s765Wait(10*time.Minute, func() bool {
		st := cache.RefresherStatsByStat()
		f := api.ContentFillStatsSnapshot()
		return st["queue_depth"] == 0 && f.Inflight == 0
	}, 5*time.Second)

	rep := s765PopulateReport{Wall: wall.Round(time.Millisecond).String(), Calls: int(calls.Load()),
		StatusByWidget: map[string]any{}, ProbeStatus: probe, QuiesceWall: time.Since(q0).Round(time.Millisecond).String(),
		QuiesceReached: reached, IdentityClasses: s765IdentityClasses(h.cfg), UnservedReads: h.api.unservedReads()}
	for w, m := range status {
		rep.StatusByWidget[w] = m
	}
	rep.ResidentByClass, rep.ResidentTotal = s765Resident()
	if g, ok := api.ContentFillStatsSnapshot().ByGVR[s765BenchGVR.String()]; ok && g.MaxItems > 0 {
		rep.BenchBytesPerItem = float64(g.MaxBytes) / float64(g.MaxItems)
	}
	return rep
}

func (h *s765H) probeUser() s765User { return h.users[0] } // scaleuser-0001: admin, team 01

func s765Resident() (map[string]int, int) {
	by := map[string]int{}
	total := 0
	if c := cache.ResolvedCache(); c != nil {
		c.RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
			by[m.CacheEntryClass]++
			total++
			return true
		})
	}
	return by, total
}

// ---------------------------------------------------------------------------
// A measurement point: churn mode x rate
// ---------------------------------------------------------------------------

type s765Mode string

const (
	s765Annotation s765Mode = "annotation"
	s765Status     s765Mode = "status"
)

type s765Point struct {
	Mode       s765Mode
	Rate       float64
	WarmUp     time.Duration
	Measure    time.Duration
	ProbeEvery time.Duration
	PollEvery  time.Duration
	CustEvery  time.Duration
	Drain      time.Duration // after churn stops: wait for the queue to empty, bounded
	// Platform turns on the measured platform-composition churn (ruling b).
	Platform bool
	// Viewers > 0 turns on the VIEWED arm (#765, Diego's ruling: refresh viewed
	// cells first): that many users, each of a distinct identity class, hold the
	// dashboard page open (a live /refreshes subscription on its 5
	// dashboard-data widgets, re-fetching a widget on each push, as the SPA
	// does); as many other classes form the unviewed control group.
	Viewers int
}

// s765Snap is every in-process instrument at one instant.
type s765Snap struct {
	at         time.Time
	refresher  map[string]int64
	marks      map[string]uint64
	markEvents uint64
	fill       api.ContentFillStats
	store      cache.ResolvedCacheStats
	rusageSec  float64
	rtUsedSec  float64
	marksByGVR map[string]uint64
	narrow     cache.ListNarrowGate  // #765 B shadow: the owner's enforcement gate readout (#784)
	decisions  map[string]uint64     // B shadow: list_dep marks by "decision/reason"
	lookups    map[string]s765Lookup // snowplow_dispatch_l1_lookups: customer GET /call only
}

// s765Lookup is one handler kind's customer L1 lookups. snowplow counts them
// at the dispatcher's cache lookup, which only a customer GET /call reaches
// (the refresher and demand revalidations never run that lookup).
type s765Lookup struct{ Hit, Miss uint64 }

func s765Lookups() map[string]s765Lookup {
	out := map[string]s765Lookup{}
	v := expvar.Get("snowplow_dispatch_l1_lookups")
	if v == nil {
		return out
	}
	var m map[string]map[string]uint64
	if json.Unmarshal([]byte(v.String()), &m) != nil {
		return out
	}
	for k, c := range m {
		kind, _, _ := strings.Cut(k, "|")
		l := out[kind]
		l.Hit += c["hit_total"]
		l.Miss += c["miss_total"]
		out[kind] = l
	}
	return out
}

func s765Take() s765Snap {
	s := s765Snap{at: time.Now(), refresher: cache.RefresherStatsByStat(), marks: cache.DirtyMarkByEntryClassSnapshot(),
		markEvents: cache.DirtyMarkEventsTotal(), fill: api.ContentFillStatsSnapshot(), lookups: s765Lookups(),
		marksByGVR: cache.DirtyMarkGVRSnapshot(), narrow: cache.ListNarrowGateSnapshot(), decisions: map[string]uint64{}}
	for _, d := range cache.ListNarrowDecisionsSnapshot() {
		s.decisions[d.Decision+"/"+d.Reason] += d.Count
	}
	for _, v := range cache.ListNarrowCommitVerdictsSnapshot() {
		s.decisions["commit:"+v.Verdict+"/"+v.Why] += v.Count
	}
	if c := cache.ResolvedCache(); c != nil {
		s.store = c.Stats()
	}
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		s.rusageSec = float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
	}
	samples := []metrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}, {Name: "/cpu/classes/idle:cpu-seconds"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindFloat64 && samples[1].Value.Kind() == metrics.KindFloat64 {
		s.rtUsedSec = samples[0].Value.Float64() - samples[1].Value.Float64()
	}
	return s
}

// s765Dist is a distribution summary.
type s765Dist struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	Max float64 `json:"max"`
}

func s765Summarize(xs []float64) s765Dist {
	if len(xs) == 0 {
		return s765Dist{}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	pct := func(p float64) float64 { // nearest rank, as load606.pct
		k := int(math.Ceil(p/100*float64(len(s)))) - 1
		if k < 0 {
			k = 0
		}
		return s[k]
	}
	return s765Dist{N: len(s), P50: pct(50), P90: pct(90), P95: pct(95), Max: s[len(s)-1]}
}

// s765ProbeSample is one probe write.
type s765ProbeSample struct {
	Seq       int     `json:"seq"`
	Converged bool    `json:"converged"`
	LatencyMS float64 `json:"latency_ms"` // converged: write -> visible; else age at the end
}

// s765Report is one point's result. Every number in it is RUN.
type s765Report struct {
	Harness    string    `json:"harness"`
	Mode       s765Mode  `json:"mode"`
	Rate       float64   `json:"rate"`
	WarmUp     string    `json:"warm_up"`
	Measure    string    `json:"measure"`
	Host       s765Host  `json:"host"`
	GoMaxProcs int       `json:"gomaxprocs"`
	Started    time.Time `json:"started"`

	EventsWritten         int64             `json:"events_written"` // bench churn + probe writes
	Platform              bool              `json:"platform_churn"`
	PlatformEvents        int64             `json:"platform_events_written"`
	MarksByGVR            map[string]uint64 `json:"marks_by_gvr"`
	MarksPerBenchEvent    float64           `json:"marks_per_bench_event"`
	MarksPerPlatformEvent float64           `json:"marks_per_platform_event"`
	EventsPerSec          float64           `json:"events_per_sec"`
	WriteErrors           int64             `json:"write_errors"`
	MarksTotal            uint64            `json:"marks_total"`
	MarksPerEvent         float64           `json:"marks_per_event"`
	MarkEventsDelta       uint64            `json:"dirty_mark_events_total_delta"`
	MarksByClass          map[string]uint64 `json:"marks_by_entry_class"`

	RefresherDelta      map[string]int64 `json:"refresher_delta"`
	RefreshesPerSec     float64          `json:"refreshes_per_sec"` // non-content L1 stores/s (the design comment's figure)
	RefresherDonePerSec float64          `json:"refresher_completed_per_sec"`
	QueueDepth          s765Dist         `json:"queue_depth"`

	FillsByReason        map[string]uint64  `json:"fills_by_reason"`
	FillsPerSecByReason  map[string]float64 `json:"fills_per_sec_by_reason"`
	BenchFillsPerSec     float64            `json:"bench_list_fills_per_sec"`
	BenchFillsPerEvent   float64            `json:"bench_list_fills_per_event"`
	BenchFillEWMAms      float64            `json:"bench_list_fill_ewma_ms"`
	BenchFillMaxMS       int64              `json:"bench_list_fill_max_ms"`
	BenchFillConcurrency s765Dist           `json:"bench_list_forced_fills_in_flight"`
	// Refresher worker utilisation (architect-623 on option A: workers parked on
	// an in-flight or queued fill cannot use the CPU A frees). The share of
	// worker-samples per state, from the goroutine profile once a second.
	RefresherWorkerShare       map[string]float64 `json:"refresher_worker_state_share"`
	RefresherWorkersMean       float64            `json:"refresher_workers_seen_mean"`
	FillWaitersMean            float64            `json:"stage_goroutines_waiting_on_fill_mean"`
	FillsByRA                  map[string]uint64  `json:"fills_by_ra"`
	DashboardFillsPerBenchFill float64            `json:"dashboard_data_fills_per_bench_fill"`
	FillsByGVRPerSec           map[string]float64 `json:"fills_by_gvr_per_sec"`

	DirtyToFresh      s765Dist          `json:"dirty_to_fresh_ms"`
	OpenWindowsAtEnd  int               `json:"dirty_windows_open_at_end"`
	Probe             s765Dist          `json:"probe_converged_ms"`
	ProbeSamples      int               `json:"probe_samples"`
	ProbeUnconverged  int               `json:"probe_unconverged"`
	ProbeOldestOpenMS float64           `json:"probe_oldest_unconverged_ms"`
	ProbeList         []s765ProbeSample `json:"probe_list"`

	CPUCoresRusage  float64 `json:"cpu_cores_rusage"`
	CPUCoresRuntime float64 `json:"cpu_cores_runtime_metrics"`

	Customer          s765Dist       `json:"customer_call_ms"`
	CustomerNon200    map[string]int `json:"customer_non_200"`
	CustomerQueuedMax int64          `json:"customer_fill_admission_queued_max"`
	DebtQueuedMax     int64          `json:"debt_fill_admission_queued_max"`
	MissDelta         uint64         `json:"miss_total_delta"`
	// Customer-origin L1 misses (ruling c): the customer loop's widget GETs and
	// the probe's RESTAction GETs, counted by snowplow at the dispatcher lookup.
	CustomerWidgetMiss uint64 `json:"customer_widget_l1_miss"`
	CustomerWidgetHit  uint64 `json:"customer_widget_l1_hit"`
	ProbeRAMiss        uint64 `json:"probe_restaction_l1_miss"`
	// Evictions in the window, by cause, beside dirty_ended_unfresh_evicted
	// (every eviction of a cell whose dirty window was open). TTL expiries of
	// dirty cells >= unfresh_evicted - (lru + delete + max_age + no_representative).
	EvictLRUDelta      uint64 `json:"evict_lru_total_delta"`
	EvictDeleteDelta   uint64 `json:"evict_delete_total_delta"`
	EvictMaxAgeDelta   uint64 `json:"evict_max_age_total_delta"`
	EvictNoRepDelta    uint64 `json:"evict_no_representative_total_delta"`
	DirtyEvictedDelta  int64  `json:"dirty_ended_unfresh_evicted_delta"`
	DirtyTTLExpiriesLB int64  `json:"dirty_ttl_expiries_lower_bound"`
	// B SHADOW (#773/#784), deltas over the window: the owner's gate readout,
	// and the share of ALL marks enforcement would skip. would_change > 0 is
	// a STOP (coordinator ruling).
	NarrowGate             cache.ListNarrowGate `json:"list_narrow_gate_delta"`
	WouldAvoidShareOfMarks float64              `json:"would_avoid_share_of_marks"`
	NarrowDecisions        map[string]uint64    `json:"list_narrow_decisions_delta"` // decision/reason -> list_dep marks; "commit:<verdict>/<why>" -> interest commit verdicts (snowplow_deps_interest_verdict_total)
	// Per RESTAction at the end of the window: resident cells built on it, and
	// the interest state of their 50K-GVR list edge (absent / all / pending /
	// narrowed). Widget cells are attributed to their spec.apiRef.
	ResidentByRA     map[string]int            `json:"resident_cells_by_ra"`
	EdgeStateByRA    map[string]map[string]int `json:"bench_list_edge_interest_state_by_ra"`
	NowRACells       int                       `json:"now_ra_cells_resident"`     // cells on RAs whose JQ uses now (dashboard-data, compositions-list)
	NowRAMarkShare   float64                   `json:"now_ra_mark_share"`         // widget-cell marks / all marks: every harness widget sits on a now-RA
	RAEdgeAvoidShare float64                   `json:"ra_edge_would_avoid_share"` // would-avoid marks / marks on RESTAction-class cells
	RAEdgeMarks      uint64                    `json:"ra_edge_marks"`
	HitDelta         uint64                    `json:"hit_total_delta"`
	EvictTTLDelta    uint64                    `json:"evict_ttl_total_delta"`
	StaleServedDelta uint64                    `json:"stale_served_total_delta"`
	ResidentAtEnd    int                       `json:"resident_entries_at_end"`
	UnservedReads    map[string]int            `json:"apiserver_unserved_reads"`

	ChurnRateNote string            `json:"churn_rate_note"`
	Viewed        *s765ViewedReport `json:"viewed,omitempty"`

	DrainWall    string `json:"drain_wall,omitempty"`
	DrainReached bool   `json:"drain_reached"`
}

// run drives one point and returns its report.
func (h *s765H) run(p s765Point) s765Report {
	h.t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	targets := h.churnTargets()
	var wg sync.WaitGroup
	var written, writeErrs atomic.Int64
	var measuring atomic.Bool

	// The churn generator: run 8's ChurnDriver, round-robin at p.Rate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t0 := time.Now()
		for i := 0; ; i++ {
			due := t0.Add(time.Duration(float64(i) / p.Rate * float64(time.Second)))
			if d := time.Until(due); d > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				}
			} else if ctx.Err() != nil {
				return
			}
			tg := targets[i%len(targets)]
			if err := h.write(p.Mode, tg[0], tg[1], fmt.Sprintf("churn-%d", i), i); err != nil {
				writeErrs.Add(1)
				continue
			}
			if measuring.Load() {
				written.Add(1)
			}
		}
	}()

	// The platform churn: each platform composition's controller status write,
	// at its measured per-object rate, phases spread so they do not align.
	var platformWritten atomic.Int64
	if p.Platform {
		k := 0
		for _, oc := range s765OtherComps {
			ev := s765PlatformEvents[oc.plural]
			if oc.objects == 0 || ev == 0 {
				continue
			}
			oc, period := oc, time.Duration(s765PlatformWindowSec/ev*float64(time.Second))
			offset := time.Duration(k) * period / time.Duration(len(s765OtherComps))
			k++
			wg.Add(1)
			go func() {
				defer wg.Done()
				next := time.Now().Add(offset)
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Until(next)):
					}
					next = next.Add(period)
					if err := h.writePlatform(oc); err != nil {
						writeErrs.Add(1)
					} else if measuring.Load() {
						platformWritten.Add(1)
					}
				}
			}()
		}
	}

	// The probe: a marker every ProbeEvery into the churned field of the probe
	// object; the poller reads the probe RESTAction through the real /call.
	var pmu sync.Mutex
	writtenAt := map[int]time.Time{}
	countIt := map[int]bool{}
	converged := map[int]time.Duration{}
	probeRA := s765ProbeRA
	if p.Mode == s765Status {
		probeRA = s765ProbeRAStatus
	}
	markerRE := regexp.MustCompile(`fresh-` + string(p.Mode) + `-(\d+)`)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(p.ProbeEvery)
		defer tick.Stop()
		for seq := 0; ; seq++ {
			if err := h.write(p.Mode, s765ProbeNS, s765ProbeName, fmt.Sprintf("fresh-%s-%d", p.Mode, seq), -1); err == nil {
				pmu.Lock()
				writtenAt[seq] = time.Now()
				countIt[seq] = measuring.Load()
				pmu.Unlock()
				if measuring.Load() {
					written.Add(1)
				}
			} else {
				writeErrs.Add(1)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(p.PollEvery)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			code, body, _ := h.get(h.probeUser(), s765RAPath(probeRA, s765ProbeNS))
			if code != http.StatusOK {
				continue
			}
			best := -1
			for _, m := range markerRE.FindAllSubmatch(body, -1) {
				if n, err := strconv.Atoi(string(m[1])); err == nil && n > best {
					best = n
				}
			}
			now := time.Now()
			pmu.Lock()
			for seq, at := range writtenAt {
				if _, done := converged[seq]; !done && seq <= best {
					converged[seq] = now.Sub(at)
				}
			}
			pmu.Unlock()
		}
	}()

	// The customer: one real widget GET per CustEvery, rotating users and widgets.
	var cmu sync.Mutex
	var custMS []float64
	custNon200 := map[string]int{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(p.CustEvery)
		defer tick.Stop()
		for k := 0; ; k++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			u := h.users[k%len(h.users)]
			w := h.widgets[k%len(h.widgets)]
			code, _, d := h.get(u, s765WidgetPath(w))
			if !measuring.Load() {
				continue
			}
			cmu.Lock()
			custMS = append(custMS, float64(d.Microseconds())/1000)
			if code != http.StatusOK {
				custNon200[fmt.Sprintf("%s:%d", w.name, code)]++
			}
			cmu.Unlock()
		}
	}()

	// The sampler: queue depth and the per-class admission queue, every second.
	var smu sync.Mutex
	var depth []float64
	var custQ, debtQ int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if !measuring.Load() {
				continue
			}
			st := cache.RefresherStatsByStat()
			adm := api.ContentFillAdmissionStatsSnapshot()
			smu.Lock()
			depth = append(depth, float64(st["queue_depth"]))
			if q := adm.Queued["customer"]; q > custQ {
				custQ = q
			}
			if q := adm.Queued["debt"]; q > debtQ {
				debtQ = q
			}
			smu.Unlock()
		}
	}()

	// The fill-concurrency sampler: how many goroutines are inside a
	// refresh_forced fill of the shared CLUSTER-WIDE bench list right now, read
	// from the runtime's goroutine profile (the fill's dispatch and parse run
	// under pprof labels), every 100 ms. A runtime fact, not a snowplow counter:
	// option A's claim (≤ 1 fill in flight per edge, not ≈ N) is read here.
	var conc []float64
	workerStates := map[string]int{}
	var workerSamples, tickN, fillWaiters int
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if !measuring.Load() {
				continue
			}
			n := s765BenchListFillsInFlight()
			smu.Lock()
			conc = append(conc, float64(n))
			smu.Unlock()
			if tickN++; tickN%10 == 0 { // once a second: the worker states
				ws := s765RefresherWorkerStates()
				smu.Lock()
				for k, v := range ws {
					if k == "fill_waiters" {
						fillWaiters += v
						continue
					}
					workerStates[k] += v
				}
				workerSamples++
				smu.Unlock()
			}
		}
	}()

	var viewed *s765Viewed
	if p.Viewers > 0 {
		viewed = h.startViewed(ctx, &wg, p.Viewers, &measuring)
	}

	started := time.Now()
	h.t.Logf("scale765 %s@%g: warm-up %s", p.Mode, p.Rate, p.WarmUp)
	time.Sleep(p.WarmUp)
	h.d2fMu.Lock()
	h.d2fSamples, h.d2fOn = nil, true
	h.d2fMu.Unlock()
	measuring.Store(true)
	a := s765Take()
	h.t.Logf("scale765 %s@%g: measuring %s", p.Mode, p.Rate, p.Measure)
	time.Sleep(p.Measure)
	measuring.Store(false)
	b := s765Take()
	h.d2fMu.Lock()
	d2f := append([]int64(nil), h.d2fSamples...)
	h.d2fOn = false
	h.d2fMu.Unlock()
	stop()
	wg.Wait()

	rep := s765Report{Harness: h.cfg.Name, Mode: p.Mode, Rate: p.Rate, WarmUp: p.WarmUp.String(), Measure: p.Measure.String(),
		Host: s765HostInfo(), GoMaxProcs: runtime.GOMAXPROCS(0), Started: started.UTC()}
	secs := b.at.Sub(a.at).Seconds()
	rep.EventsWritten, rep.WriteErrors = written.Load(), writeErrs.Load()
	rep.EventsPerSec = float64(rep.EventsWritten) / secs
	rep.MarksByClass = map[string]uint64{}
	for k, v := range b.marks {
		if d := v - a.marks[k]; d > 0 {
			rep.MarksByClass[k] = d
			rep.MarksTotal += d
		}
	}
	rep.MarkEventsDelta = b.markEvents - a.markEvents
	rep.Platform, rep.PlatformEvents = p.Platform, platformWritten.Load()
	if all := rep.EventsWritten + rep.PlatformEvents; all > 0 {
		rep.MarksPerEvent = float64(rep.MarksTotal) / float64(all)
	}
	rep.MarksByGVR = map[string]uint64{}
	var benchMarks, platformMarks uint64
	for k, v := range b.marksByGVR {
		d := v - a.marksByGVR[k]
		if d == 0 {
			continue
		}
		rep.MarksByGVR[k] = d
		if k == s765BenchGVR.String() {
			benchMarks += d
		} else if strings.HasPrefix(k, "composition.krateo.io/") {
			platformMarks += d
		}
	}
	if rep.EventsWritten > 0 {
		rep.MarksPerBenchEvent = float64(benchMarks) / float64(rep.EventsWritten)
	}
	if rep.PlatformEvents > 0 {
		rep.MarksPerPlatformEvent = float64(platformMarks) / float64(rep.PlatformEvents)
	}
	rep.RefresherDelta = map[string]int64{}
	for k, v := range b.refresher {
		rep.RefresherDelta[k] = v - a.refresher[k]
	}
	rep.RefresherDelta["queue_depth"] = b.refresher["queue_depth"] // a gauge: its end value
	rep.RefresherDonePerSec = float64(rep.RefresherDelta["completed"]) / secs
	nonContent := (b.store.StoreTotal - b.store.ApistageStoreTotal) - (a.store.StoreTotal - a.store.ApistageStoreTotal)
	rep.RefreshesPerSec = float64(nonContent) / secs
	smu.Lock()
	rep.QueueDepth = s765Summarize(depth)
	rep.BenchFillConcurrency = s765Summarize(conc)
	rep.RefresherWorkerShare = map[string]float64{}
	total := 0
	for _, v := range workerStates {
		total += v
	}
	for k, v := range workerStates {
		rep.RefresherWorkerShare[k] = float64(v) / float64(total)
	}
	if workerSamples > 0 {
		rep.RefresherWorkersMean = float64(total) / float64(workerSamples)
		rep.FillWaitersMean = float64(fillWaiters) / float64(workerSamples)
	}
	rep.CustomerQueuedMax, rep.DebtQueuedMax = custQ, debtQ
	smu.Unlock()

	rep.FillsByReason, rep.FillsPerSecByReason = map[string]uint64{}, map[string]float64{}
	for k, v := range b.fill.Total {
		rep.FillsByReason[k] = v - a.fill.Total[k]
		rep.FillsPerSecByReason[k] = float64(v-a.fill.Total[k]) / secs
	}
	bg := s765BenchGVR.String()
	benchFills := b.fill.ByGVR[bg].Fills - a.fill.ByGVR[bg].Fills
	rep.BenchFillsPerSec = float64(benchFills) / secs
	if rep.EventsWritten > 0 {
		rep.BenchFillsPerEvent = float64(benchFills) / float64(rep.EventsWritten)
	}
	rep.BenchFillEWMAms, rep.BenchFillMaxMS = b.fill.ByGVR[bg].DurationMSEWMA, b.fill.ByGVR[bg].DurationMSMax
	rep.FillsByRA = map[string]uint64{}
	for k, v := range b.fill.ByRA {
		if d := v.Fills - a.fill.ByRA[k].Fills; d > 0 {
			rep.FillsByRA[k] = d
		}
	}
	if benchFills > 0 {
		rep.DashboardFillsPerBenchFill = float64(rep.FillsByRA[s765RelNS+"/dashboard-data"]) / float64(benchFills)
	}
	rep.FillsByGVRPerSec = map[string]float64{}
	for k, v := range b.fill.ByGVR {
		if d := v.Fills - a.fill.ByGVR[k].Fills; d > 0 {
			rep.FillsByGVRPerSec[k] = float64(d) / secs
		}
	}

	d2fF := make([]float64, len(d2f))
	for i, v := range d2f {
		d2fF[i] = float64(v)
	}
	rep.DirtyToFresh = s765Summarize(d2fF)
	rep.OpenWindowsAtEnd = cache.DirtyWindowsOpenForTest()

	pmu.Lock()
	var conv []float64
	for seq, at := range writtenAt {
		if !countIt[seq] {
			continue
		}
		rep.ProbeSamples++
		if d, ok := converged[seq]; ok {
			conv = append(conv, float64(d.Microseconds())/1000)
			rep.ProbeList = append(rep.ProbeList, s765ProbeSample{Seq: seq, Converged: true, LatencyMS: float64(d.Microseconds()) / 1000})
		} else {
			age := float64(b.at.Sub(at).Microseconds()) / 1000
			rep.ProbeUnconverged++
			rep.ProbeOldestOpenMS = math.Max(rep.ProbeOldestOpenMS, age)
			rep.ProbeList = append(rep.ProbeList, s765ProbeSample{Seq: seq, LatencyMS: age})
		}
	}
	pmu.Unlock()
	sort.Slice(rep.ProbeList, func(i, j int) bool { return rep.ProbeList[i].Seq < rep.ProbeList[j].Seq })
	rep.Probe = s765Summarize(conv)

	rep.CPUCoresRusage = (b.rusageSec - a.rusageSec) / secs
	rep.CPUCoresRuntime = (b.rtUsedSec - a.rtUsedSec) / secs
	cmu.Lock()
	rep.Customer = s765Summarize(custMS)
	rep.CustomerNon200 = custNon200
	cmu.Unlock()
	rep.MissDelta = b.store.MissTotal - a.store.MissTotal
	rep.CustomerWidgetMiss = b.lookups["widgets"].Miss - a.lookups["widgets"].Miss
	rep.CustomerWidgetHit = b.lookups["widgets"].Hit - a.lookups["widgets"].Hit
	rep.ProbeRAMiss = b.lookups["restactions"].Miss - a.lookups["restactions"].Miss
	rep.EvictLRUDelta = b.store.EvictLRUTotal - a.store.EvictLRUTotal
	rep.EvictDeleteDelta = b.store.EvictDeleteTotal - a.store.EvictDeleteTotal
	rep.EvictMaxAgeDelta = b.store.EvictMaxAgeTotal - a.store.EvictMaxAgeTotal
	rep.EvictNoRepDelta = b.store.EvictNoRepresentativeTotal - a.store.EvictNoRepresentativeTotal
	rep.DirtyEvictedDelta = rep.RefresherDelta["dirty_ended_unfresh_evicted"]
	rep.NarrowGate = cache.ListNarrowGate{
		WouldAvoidMarks:    b.narrow.WouldAvoidMarks - a.narrow.WouldAvoidMarks,
		AvoidOnlyRefreshes: b.narrow.AvoidOnlyRefreshes - a.narrow.AvoidOnlyRefreshes,
		IdenticalAudits:    b.narrow.IdenticalAudits - a.narrow.IdenticalAudits,
		WouldChange:        b.narrow.WouldChange - a.narrow.WouldChange,
		NoPrior:            b.narrow.NoPrior - a.narrow.NoPrior,
		Unaudited:          b.narrow.Unaudited - a.narrow.Unaudited,
	}
	if g := &rep.NarrowGate; g.AvoidOnlyRefreshes > 0 {
		g.UnauditedShare = float64(g.Unaudited) / float64(g.AvoidOnlyRefreshes)
	}
	rep.ResidentByRA, rep.EdgeStateByRA = h.narrowStateByRA()
	rep.NowRACells = rep.ResidentByRA[s765RelNS+"/dashboard-data"] + rep.ResidentByRA[s765RelNS+"/compositions-list"]
	if rep.MarksTotal > 0 {
		rep.NowRAMarkShare = float64(rep.MarksByClass["widgets"]) / float64(rep.MarksTotal)
	}
	rep.RAEdgeMarks = rep.MarksByClass["restactions"] + rep.MarksByClass["raFullList"]
	rep.NarrowDecisions = map[string]uint64{}
	for k, v := range b.decisions {
		if d := v - a.decisions[k]; d > 0 {
			rep.NarrowDecisions[k] = d
		}
	}
	if rep.MarksTotal > 0 {
		rep.WouldAvoidShareOfMarks = float64(rep.NarrowGate.WouldAvoidMarks) / float64(rep.MarksTotal)
	}
	// Every avoided decision lands on a RESTAction-class cell in this
	// population (the widget cells are interest_all), so this is the avoided
	// share among RA-edge marks; the per-RA edge states say which RA.
	if rep.RAEdgeMarks > 0 {
		rep.RAEdgeAvoidShare = float64(rep.NarrowGate.WouldAvoidMarks) / float64(rep.RAEdgeMarks)
	}
	rep.DirtyTTLExpiriesLB = rep.DirtyEvictedDelta - int64(rep.EvictLRUDelta+rep.EvictDeleteDelta+rep.EvictMaxAgeDelta+rep.EvictNoRepDelta)
	if rep.DirtyTTLExpiriesLB < 0 {
		rep.DirtyTTLExpiriesLB = 0
	}
	rep.HitDelta = b.store.HitTotal - a.store.HitTotal
	rep.EvictTTLDelta = b.store.EvictTTLTotal - a.store.EvictTTLTotal
	rep.StaleServedDelta = b.store.StaleServedTotal - a.store.StaleServedTotal
	_, rep.ResidentAtEnd = s765Resident()
	rep.UnservedReads = h.api.unservedReads()
	rep.ChurnRateNote = "the churn rate is a harness ASSUMPTION (run 8's bench rate), not a measured production rate"
	if viewed != nil {
		rep.Viewed = viewed.report()
	}

	if p.Drain > 0 {
		d0 := time.Now()
		rep.DrainReached = s765Wait(p.Drain, func() bool {
			return cache.RefresherStatsByStat()["queue_depth"] == 0 && api.ContentFillStatsSnapshot().Inflight == 0
		}, 5*time.Second)
		rep.DrainWall = time.Since(d0).Round(time.Second).String()
	}
	return rep
}

// s765DiffReport is the differential check: every served body equals a cold
// resolve of the same request against the same (quiesced) informer state.
type s765DiffReport struct {
	Compared   int            `json:"compared"`
	Mismatched int            `json:"mismatched"`
	ByWidget   map[string]int `json:"mismatched_by_widget,omitempty"`
	Errors     int            `json:"errors"`
}

// s765Canon re-encodes a /call body without status.traceId (a per-request id
// the refresher's rebuild does not carry), so two bodies compare on content.
func s765Canon(body []byte) []byte {
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return body
	}
	if st, ok := m["status"].(map[string]any); ok {
		delete(st, "traceId")
	}
	out, _ := json.Marshal(m)
	return out
}

// differential is the harness's own oracle for "outputs byte-identical to
// forced-fill outputs": with churn stopped and the refresher drained, it reads
// each identity class's 27 widgets (served from the refreshed cells), then
// drops EVERY resident cell, content cells included, and reads them again:
// each second read is a cold resolve whose content is filled fresh from the
// informer. Any difference is a refreshed body built from older content than
// the state it claims. Call it only after the drain; it rebuilds the cells it
// touches and leaves the rest evicted, so it is an arm's last step.
func (h *s765H) differential() s765DiffReport {
	rep := s765DiffReport{ByWidget: map[string]int{}}
	reps := map[string]s765User{} // one user per identity class
	for _, u := range h.users {
		g := append([]string(nil), u.groups...)
		sort.Strings(g)
		k := strings.Join(g, ",")
		if _, ok := reps[k]; !ok {
			reps[k] = u
		}
	}
	type key struct{ user, path string }
	before := map[key][]byte{}
	for _, u := range reps {
		for _, w := range h.widgets {
			code, body, _ := h.get(u, s765WidgetPath(w))
			if code != http.StatusOK {
				rep.Errors++
				continue
			}
			before[key{u.name, s765WidgetPath(w)}] = s765Canon(body)
		}
	}
	if c := cache.ResolvedCache(); c != nil {
		for _, k := range c.KeysForTest() {
			c.DeleteForTest(k)
		}
	}
	for k, b := range before {
		var u s765User
		for _, cand := range reps {
			if cand.name == k.user {
				u = cand
			}
		}
		code, body, _ := h.get(u, k.path)
		if code != http.StatusOK {
			rep.Errors++
			continue
		}
		rep.Compared++
		if !bytes.Equal(b, s765Canon(body)) {
			rep.Mismatched++
			q, _ := url.ParseQuery(strings.SplitN(k.path, "?", 2)[1])
			rep.ByWidget[q.Get("name")]++
			if dir := os.Getenv("SCALE765_OUT"); dir != "" && rep.ByWidget[q.Get("name")] == 1 {
				// The first mismatch per widget, both bodies, for the reader.
				_ = os.WriteFile(filepath.Join(dir, "diff-"+q.Get("name")+"-served.json"), b, 0o644)
				_ = os.WriteFile(filepath.Join(dir, "diff-"+q.Get("name")+"-cold.json"), s765Canon(body), 0o644)
			}
		}
	}
	return rep
}

// s765RefresherWorkerStates classifies every refresher worker goroutine (its
// stack holds cache.(*refresher).processNext) from a debug=2 goroutine dump:
//   - idle: parked in the workqueue's Get (no work queued);
//   - waiting_fill: blocked inside the api-stage content fill machinery (a
//     flight it joined, a pending flight, the fill admission gate);
//   - working: running or runnable (on CPU, or ready for it);
//   - blocked_other: any other wait (locks, channels, sleeps).
//
// A runtime fact, read the same way on every build.
func s765RefresherWorkerStates() map[string]int {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		return nil
	}
	out := map[string]int{}
	for _, g := range strings.Split(buf.String(), "\n\n") {
		if !strings.Contains(g, "cache.(*refresher).processNext") {
			// A resolve's stages run on child goroutines (the worker itself
			// then shows as blocked in runStage): count, separately, every
			// stage goroutine parked on a content fill it did not lead
			// (a joined or pending flight, or the admission gate). In the
			// harness's steady state these are almost all refresher work
			// (customers hit their cells); reported as fill_waiters.
			hdr := g
			if i := strings.IndexByte(g, '\n'); i >= 0 {
				hdr = g[:i]
			}
			if !strings.Contains(hdr, "[running]") && !strings.Contains(hdr, "[runnable]") &&
				strings.Contains(g, "dispatchOneCall") &&
				(strings.Contains(g, "contentFlight") || strings.Contains(g, "contentFillGate") ||
					strings.Contains(g, "apistage_fill_flight.go") || strings.Contains(g, "apistage_fill_admission.go")) {
				out["fill_waiters"]++
			}
			continue
		}
		hdr := g
		if i := strings.IndexByte(g, '\n'); i >= 0 {
			hdr = g[:i]
		}
		switch {
		case strings.Contains(g, "workqueue") && strings.Contains(g, ".Get("):
			out["idle"]++
		case strings.Contains(hdr, "[running]") || strings.Contains(hdr, "[runnable]"):
			out["working"]++
		case strings.Contains(g, "contentFlight") || strings.Contains(g, "contentFillGate") ||
			strings.Contains(g, "apistage_fill_flight.go") || strings.Contains(g, "apistage_fill_admission.go"):
			out["waiting_fill"]++
		default:
			// Name the innermost snowplow frame it is blocked under, so a
			// reader can tell a rate-floor or yield park from a lock.
			where := "unknown"
			for _, l := range strings.Split(g, "\n")[1:] {
				if strings.HasPrefix(l, "github.com/krateo-platformops/snowplow/internal/") {
					where = strings.TrimPrefix(l, "github.com/krateo-platformops/snowplow/internal/")
					if i := strings.IndexByte(where, '('); i > 0 && strings.HasSuffix(where, ")") {
						where = where[:strings.LastIndexByte(where, '(')]
					}
					break
				}
			}
			out["blocked_other:"+where]++
		}
	}
	return out
}

// narrowStateByRA attributes every resident widget / RESTAction cell to its
// RESTAction (a widget by its spec.apiRef) and reads B's interest state on the
// cell's list edge of the 50K GVR: cluster-wide for the portal RAs, bench-ns-01
// for the probes. A read of the dep tracker, not a counter.
func (h *s765H) narrowStateByRA() (map[string]int, map[string]map[string]int) {
	apiRef := map[string]string{}
	for _, w := range h.widgets {
		apiRef[w.name] = s765RelNS + "/" + w.apiRef
	}
	resident := map[string]int{}
	states := map[string]map[string]int{}
	c := cache.ResolvedCache()
	if c == nil {
		return resident, states
	}
	c.RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		var ra string
		dk := cache.DepKey{GVR: s765BenchGVR, Name: "*"}
		switch m.CacheEntryClass {
		case "widgets":
			ra = apiRef[m.Name]
		case "restactions":
			ra = m.Namespace + "/" + m.Name
			if m.Namespace == s765ProbeNS {
				dk.Namespace = s765ProbeNS
			}
		default:
			return true
		}
		if ra == "" {
			return true
		}
		resident[ra]++
		st := cache.ListInterestStateForTest(m.KeyHash, dk)
		if i := strings.IndexByte(st, ':'); i > 0 {
			st = st[:i] // "narrowed:<read set>" -> "narrowed"
		}
		if states[ra] == nil {
			states[ra] = map[string]int{}
		}
		states[ra][st]++
		return true
	})
	return resident, states
}

// s765BenchListFillsInFlight counts the goroutines inside a refresh_forced fill
// of the cluster-wide bench list (pprof labels apistage_fill_gvr = the bench
// GVR, apistage_fill_reason = refresh_forced, apistage_fill_ra not one of the
// namespaced probe RESTActions) from the goroutine profile.
func s765BenchListFillsInFlight() int {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		return -1
	}
	gvrLabel := `"apistage_fill_gvr":"` + s765BenchGVR.String() + `"`
	n := 0
	for _, rec := range strings.Split(buf.String(), "\n\n") {
		i := strings.Index(rec, "# labels: ")
		if i < 0 {
			continue
		}
		labels := rec[i:]
		if j := strings.IndexByte(labels, '\n'); j >= 0 {
			labels = labels[:j]
		}
		if !strings.Contains(labels, gvrLabel) || !strings.Contains(labels, `"apistage_fill_reason":"refresh_forced"`) ||
			strings.Contains(labels, `"apistage_fill_ra":"`+s765ProbeNS+`/`) {
			continue
		}
		var count int
		if _, err := fmt.Sscanf(rec, "%d @", &count); err == nil {
			n += count
		}
	}
	return n
}

// churnTargets is load606 _cmd_fresh's target list: bench-ns-NN/bench-app-NN-KK
// for KK in 1..ChurnPerNS, minus the probe object.
func (h *s765H) churnTargets() [][2]string {
	var out [][2]string
	for n := 1; n <= h.cfg.Namespaces; n++ {
		for k := 1; k <= h.cfg.ChurnPerNS && k <= h.cfg.PerNS; k++ {
			ns, name := fmt.Sprintf("bench-ns-%02d", n), fmt.Sprintf("bench-app-%02d-%02d", n, k)
			if ns == s765ProbeNS && name == s765ProbeName {
				continue
			}
			out = append(out, [2]string{ns, name})
		}
	}
	return out
}

// write mutates ns/name in the churned field and UPDATEs it through the fake,
// so the informer sees a real UPDATE (its watch is registered: fakewatch).
func (h *s765H) write(mode s765Mode, ns, name, marker string, i int) error {
	ri := h.dyn.Resource(s765BenchGVR).Namespace(ns)
	obj, err := ri.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	switch mode {
	case s765Annotation:
		ann := obj.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann[s765FreshAnnotation] = marker
		obj.SetAnnotations(ann)
	case s765Status:
		st := "True"
		if i >= 0 && i%2 == 1 {
			st = "False"
		}
		_ = unstructured.SetNestedSlice(obj.Object, []any{map[string]any{
			"type": "Ready", "status": st, "reason": "Churn", "message": marker,
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}}, "status", "conditions")
	}
	obj.SetResourceVersion(strconv.FormatInt(1000+h.rvSeq.Add(1), 10))
	_, err = ri.Update(context.Background(), obj, metav1.UpdateOptions{})
	return err
}

// ---------------------------------------------------------------------------
// Calibration: run 8's shape at annotation churn 10/s, within a factor of 2
// ---------------------------------------------------------------------------

// The run-8 values (RUN: architect-623's design comment 6265999826 and the
// run-8 capture, window 10:10-10:39Z, no customer load). Constants, never read
// from the code under test.
const (
	s765Run8RefreshesPerSec   = 4.53   // non-content L1 stores/s
	s765Run8BenchFillsPerSec  = 0.685  // the 50K list
	s765Run8BenchFillEWMAms   = 3384.0 // its fill EWMA
	s765Run8DashFillsPerBench = 28.48  // dashboard-data fills (34,035) / 50K-list fills (1,195)
	s765Run8QueueP50          = 383.0
	s765MinutesMS             = 60000.0 // "dirty_to_fresh p95 in minutes"
)

type s765Check struct {
	Name   string  `json:"name"`
	Got    float64 `json:"got"`
	Lo     float64 `json:"lo"`
	Hi     float64 `json:"hi"`
	Result string  `json:"result"`
}

func s765Within2x(name string, got, run8 float64) s765Check {
	c := s765Check{Name: name, Got: got, Lo: run8 / 2, Hi: run8 * 2, Result: "PASS"}
	if got < c.Lo || got > c.Hi {
		c.Result = "FAIL"
	}
	return c
}

func s765Calibrate(r s765Report) []s765Check {
	// Validity first: GOMAXPROCS bounds the Ps, not the CPU the host grants
	// them. runtime/metrics counts a P as busy while the OS has descheduled its
	// thread; rusage counts what actually ran. When the two diverge the host
	// starved the harness and every throughput figure below is depressed by
	// the host, not by snowplow: the run is INVALID, never a calibration FAIL.
	v := s765Check{Name: "host_cpu_granted_share", Got: 0, Lo: 0.8, Hi: 1.2, Result: "PASS"}
	if r.CPUCoresRuntime > 0 {
		v.Got = r.CPUCoresRusage / r.CPUCoresRuntime
	}
	if v.Got < v.Lo {
		v.Result = "INVALID"
	}
	cs := []s765Check{v,
		s765Within2x("refreshes_per_sec", r.RefreshesPerSec, s765Run8RefreshesPerSec),
		s765Within2x("bench_list_fills_per_sec", r.BenchFillsPerSec, s765Run8BenchFillsPerSec),
		s765Within2x("bench_list_fill_ewma_ms", r.BenchFillEWMAms, s765Run8BenchFillEWMAms),
		s765Within2x("dashboard_data_fills_per_bench_fill", r.DashboardFillsPerBenchFill, s765Run8DashFillsPerBench),
		s765Within2x("queue_depth_p50", r.QueueDepth.P50, s765Run8QueueP50),
	}
	m := s765Check{Name: "dirty_to_fresh_p95_ms_in_minutes", Got: r.DirtyToFresh.P95, Lo: s765MinutesMS, Hi: math.MaxFloat64, Result: "PASS"}
	// A window that never closed is longer than any that did: an empty sample
	// set with windows still open, or open probe samples older than a minute,
	// is "minutes" too, and is reported as such rather than as a pass on zero.
	if r.DirtyToFresh.P95 < s765MinutesMS && !(r.ProbeUnconverged > 0 && r.ProbeOldestOpenMS >= s765MinutesMS) {
		m.Result = "FAIL"
	}
	return append(cs, m)
}

// ---------------------------------------------------------------------------
// Host and output
// ---------------------------------------------------------------------------

type s765Host struct {
	OS      string `json:"os"`
	Arch    string `json:"arch"`
	NumCPU  int    `json:"num_cpu"`
	CPU     string `json:"cpu"`
	LoadAvg string `json:"loadavg"`
	Go      string `json:"go"`
}

func s765HostInfo() s765Host {
	h := s765Host{OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU(), Go: runtime.Version()}
	run := func(name string, args ...string) string {
		out, err := exec.Command(name, args...).Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	if runtime.GOOS == "darwin" {
		h.CPU = run("sysctl", "-n", "machdep.cpu.brand_string")
		h.LoadAvg = run("sysctl", "-n", "vm.loadavg")
	} else {
		if b, err := os.ReadFile("/proc/loadavg"); err == nil {
			h.LoadAvg = strings.TrimSpace(string(b))
		}
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(l, "model name") {
					h.CPU = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
					break
				}
			}
		}
	}
	return h
}

// s765Emit logs v as JSON and, when SCALE765_OUT names a directory, writes it
// there as <name>.json.
func s765Emit(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	t.Logf("scale765 %s:\n%s", name, raw)
	if dir := os.Getenv("SCALE765_OUT"); dir != "" {
		if err := os.WriteFile(filepath.Join(dir, name+".json"), raw, 0o644); err != nil {
			t.Errorf("write %s: %v", name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func s765Await(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	if !s765Wait(d, ok, 0) {
		t.Fatalf("timed out after %s waiting for %s", d, what)
	}
}

// s765Wait polls ok every 50 ms until it has held for `hold` (0 = once) or d
// passes.
func s765Wait(d time.Duration, ok func() bool, hold time.Duration) bool {
	end := time.Now().Add(d)
	var since time.Time
	for time.Now().Before(end) {
		if ok() {
			if since.IsZero() {
				since = time.Now()
			}
			if time.Since(since) >= hold {
				return true
			}
		} else {
			since = time.Time{}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
