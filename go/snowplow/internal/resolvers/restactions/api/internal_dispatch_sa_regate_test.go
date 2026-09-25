// internal_dispatch_sa_regate_test.go — falsifier for
// fix/sa-config-fallthrough-rbac-regate.
//
// THE LEAK (defensive-authorization gap closed by this fix):
//
//	dispatchViaInternalRESTConfig (branch C) serves an apiserver GET/LIST
//	through a client-go dynamic client built from the context-carried
//	SA *rest.Config. dispatchers/restactions.go:262 and
//	dispatchers/widgets.go:273 attach that SA *rest.Config to EVERY
//	in-cluster per-user request (for the TLS-CA reason in
//	internal_dispatch.go's header). So when the per-user informer / F1
//	gate (branches A/B) does not serve, branch C fetches the apiserver
//	bytes with the SA client and — pre-fix — returned the RAW bytes with
//	NO per-user RBAC re-gate. Verified live: user cyberjoker (groups
//	[devs]), denied `get configmaps`, received all 379 blueprints-catalog
//	entries.
//
// THE FIX: at BOTH serve points, when the request is NOT a genuine SA /
// identity-free operation (internalDispatchServesUnnarrowed), the fetched
// bytes are re-gated with the SAME per-user helpers branch B uses
// (filterGetByRBAC / filterListByRBAC). A denied GET → apierrors.Forbidden;
// a LIST → the ctx identity's authorized subset (served-empty on full deny).
//
// RIGGING: each arm drives the REAL dispatchViaInternalRESTConfig against a
// hermetic in-process fake apiserver (httptest) reachable through a
// *rest.Config, with the RBAC verdict sourced from an in-process
// ResourceWatcher snapshot (cache=on arms) or an injected fake
// SelfSubjectAccessReview clientset (cache=off arm). NEVER the kind cluster /
// a remote kubeconfig.
//
// ── ARMS ──────────────────────────────────────────────────────────────
//   ARM-1  denied per-user GET (cyberjoker repro) → served==false, Forbidden.
//   ARM-2  allowed per-user GET control          → served==true, sha256 parity.
//   ARM-3  denied per-user LIST narrowing         → namespaces=={authorized}.
//   ARM-4  Phase-1 SA-walk (ServeWatcher present) → full un-narrowed set.
//   ARM-5  cache-off parity (SAR deny)            → identical Forbidden to ARM-1.
//   ARM-6  group-only user (Username=="")         → narrowed (denied) / group grant served.
//   ARM-7  refresher identity-free (canonical SA) → full un-narrowed set (clause d).
//   ARM-8  RC1 guard: non-exempt deny + UNPUBLISHED snapshot → fall through
//          (served==false, err==nil — NOT a Forbidden), so a mid-rebuild
//          refresher does not overwrite a warm cell empty.
//
// ── TWO-MUTANT CONTROL NOTE ────────────────────────────────────────────
//
//	(i)  INVERT the predicate (internalDispatchServesUnnarrowed returns the
//	     negation): ARM-4 and ARM-7 go RED. ARM-4 → the SA walk is narrowed;
//	     under its UNPUBLISHED snapshot the RC1 guard falls through
//	     (served==false, not the full set). ARM-7 → the refresher is narrowed;
//	     its PUBLISHED snapshot grants the SA nothing, so the LIST serves
//	     empty (len 0, not the full set).
//	(ii) DROP the group-only handling (exempt on Username==""): ARM-6's
//	     denied sub-case goes RED — the group-only end-user is served the raw
//	     object un-narrowed (served==true).
//
// Neither mutant leaves ARM-2 (byte-parity) affected, so the control arms
// stay green: each defect is caught by exactly the arm named above.

package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

// saRegateIdentity is a canonical Kubernetes ServiceAccount username — the
// refresher's identity-free path (clause d of internalDispatchServesUnnarrowed).
const saRegateIdentity = "system:serviceaccount:krateo-system:snowplow"

// fakeRAItem is one restaction the fake apiserver holds.
type fakeRAItem struct{ ns, name string }

// fakeRAObjectJSON renders a single RestAction object body (the GET shape and
// the per-item LIST shape) — a minimal apiserver-faithful envelope.
func fakeRAObjectJSON(ns, name string) string {
	return `{"apiVersion":"templates.krateo.io/v1","kind":"RestAction",` +
		`"metadata":{"namespace":"` + ns + `","name":"` + name + `"},` +
		`"spec":{"marker":"m"}}`
}

// newFakeRestActionAPIServer starts an in-process HTTP server that answers the
// dispatch test GVR's GET-by-name and LIST (cluster-wide + namespace-scoped)
// from `items`. client-go's dynamic client (built from the returned
// *rest.Config) reaches it exactly as it would a real apiserver — so branch C
// runs its REAL Get/List path (feedback_falsifier_must_drive_real_boundary).
func newFakeRestActionAPIServer(t *testing.T, items []fakeRAItem) *rest.Config {
	t.Helper()
	const base = "/apis/templates.krateo.io/v1/"
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		if !strings.HasPrefix(p, base) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
			return
		}
		seg := strings.Split(strings.TrimPrefix(p, base), "/")
		var ns, name string
		isList := false
		switch {
		case len(seg) == 1 && seg[0] == "restactions":
			isList = true // cluster-wide LIST
		case len(seg) == 3 && seg[0] == "namespaces" && seg[2] == "restactions":
			ns, isList = seg[1], true // namespace-scoped LIST
		case len(seg) == 4 && seg[0] == "namespaces" && seg[2] == "restactions":
			ns, name = seg[1], seg[3] // GET-by-name
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
			return
		}
		if isList {
			var b strings.Builder
			b.WriteString(`{"apiVersion":"templates.krateo.io/v1","kind":"RestActionList",` +
				`"metadata":{"resourceVersion":"1"},"items":[`)
			first := true
			for _, it := range items {
				if ns != "" && it.ns != ns {
					continue
				}
				if !first {
					b.WriteByte(',')
				}
				first = false
				b.WriteString(fakeRAObjectJSON(it.ns, it.name))
			}
			b.WriteString(`]}`)
			_, _ = io.WriteString(w, b.String())
			return
		}
		for _, it := range items {
			if it.ns == ns && it.name == name {
				_, _ = io.WriteString(w, fakeRAObjectJSON(ns, name))
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

// sha256Hex is the returned-bytes fingerprint each arm reports.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// servedItemNamespaces decodes a LIST envelope and returns the set of
// metadata.namespace values across its items.
func servedItemNamespaces(t *testing.T, raw []byte) map[string]bool {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("served LIST is not valid JSON: %v\nbytes: %s", err, string(raw))
	}
	items, _ := env["items"].([]any)
	out := map[string]bool{}
	for _, it := range items {
		m, _ := it.(map[string]any)
		meta, _ := m["metadata"].(map[string]any)
		if ns, ok := meta["namespace"].(string); ok {
			out[ns] = true
		}
	}
	return out
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ── ARM-1 ──────────────────────────────────────────────────────────────
// A denied per-user GET (the cyberjoker repro) must NOT be served under the
// SA identity: served==false && the error is a Forbidden.
//
// RED pre-fix: branch C returns (bytes,true,nil). GREEN post-fix.
func TestSARegate_ARM1_DeniedPerUserGet_Forbidden(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	newRBACNarrowGetWatcher(t) // grants narrowUser `get` in team-a/team-b only

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"bench-ns-1", "bench-ns-1-x"}})
	ctx := cache.WithInternalRESTConfig(ctxWithUser(narrowUser), rc)

	raw, served, err := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, getByNamePath("bench-ns-1", "bench-ns-1-x")))

	t.Logf("ARM-1 served=%v err=%v sha256=%s", served, err, sha256Hex(raw))
	if served {
		t.Fatalf("LEAK: denied per-user GET was served under the SA identity "+
			"(served=true, %d bytes) — a narrow-RBAC user received an object in a "+
			"namespace they have no `get` grant for", len(raw))
	}
	if raw != nil {
		t.Fatalf("denied GET must return nil bytes; got %d", len(raw))
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("denied per-user GET must return a Forbidden error; got %v", err)
	}
}

// ── ARM-2 ──────────────────────────────────────────────────────────────
// An ALLOWED per-user GET is served, byte-identical to the un-narrowed
// (SA-exempt) serve of the same object. Control: GREEN both sides.
func TestSARegate_ARM2_AllowedPerUserGet_ByteParity(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	newRBACNarrowGetWatcher(t) // narrowUser IS granted `get` in team-a

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"team-a", "team-a-x"}})

	// Authorized per-user serve.
	ctxUser := cache.WithInternalRESTConfig(ctxWithUser(narrowUser), rc)
	rawUser, servedUser, errUser := dispatchViaInternalRESTConfig(ctxUser,
		buildCall(http.MethodGet, getByNamePath("team-a", "team-a-x")))
	if errUser != nil || !servedUser {
		t.Fatalf("authorized GET must serve; got served=%v err=%v", servedUser, errUser)
	}

	// Un-narrowed reference serve (canonical SA identity — clause d exempts).
	resetInternalClientCacheForTest()
	ctxSA := cache.WithInternalRESTConfig(ctxWithUser(saRegateIdentity), rc)
	rawSA, servedSA, errSA := dispatchViaInternalRESTConfig(ctxSA,
		buildCall(http.MethodGet, getByNamePath("team-a", "team-a-x")))
	if errSA != nil || !servedSA {
		t.Fatalf("SA-exempt reference GET must serve; got served=%v err=%v", servedSA, errSA)
	}

	t.Logf("ARM-2 user_sha256=%s sa_sha256=%s", sha256Hex(rawUser), sha256Hex(rawSA))
	if sha256Hex(rawUser) != sha256Hex(rawSA) {
		t.Fatalf("allowed per-user GET bytes must equal the un-narrowed serve:\n user=%s\n   sa=%s",
			string(rawUser), string(rawSA))
	}
	if !strings.Contains(string(rawUser), `"name":"team-a-x"`) {
		t.Fatalf("served object is not team-a-x: %s", string(rawUser))
	}
}

// ── ARM-3 ──────────────────────────────────────────────────────────────
// A denied per-user LIST is narrowed to the user's authorized namespaces.
// The SA LIST spans an authorized ns + two denied ns; post-fix the served
// items are exactly the authorized-namespace subset (len>0).
func TestSARegate_ARM3_DeniedPerUserList_Narrowed(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	newRBACNarrowWatcher(t) // grants narrowUser `list` in team-a/team-b

	// SA LIST spans team-a (authorized) + bench-ns-1 + bench-ns-2 (denied).
	rc := newFakeRestActionAPIServer(t, []fakeRAItem{
		{"team-a", "team-a-x"}, {"team-a", "team-a-y"},
		{"bench-ns-1", "bench-ns-1-x"},
		{"bench-ns-2", "bench-ns-2-x"},
	})
	ctx := cache.WithInternalRESTConfig(ctxWithUser(narrowUser), rc)

	raw, served, err := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/restactions"))
	if err != nil || !served {
		t.Fatalf("cluster-wide LIST must serve; got served=%v err=%v", served, err)
	}

	nss := servedItemNamespaces(t, raw)
	names := servedItemNames(t, raw)
	t.Logf("ARM-3 served namespaces=%v names=%v sha256=%s", sortedSet(nss), sortedSet(names), sha256Hex(raw))

	if nss["bench-ns-1"] || nss["bench-ns-2"] {
		t.Fatalf("LEAK: denied namespaces leaked into the served LIST: %v", sortedSet(nss))
	}
	if len(nss) != 1 || !nss["team-a"] {
		t.Fatalf("served LIST must be narrowed to the authorized namespace {team-a}; got %v", sortedSet(nss))
	}
	if len(names) == 0 {
		t.Fatalf("narrowed LIST unexpectedly empty — the authorized subset must survive")
	}
}

// ── ARM-4 ──────────────────────────────────────────────────────────────
// Phase-1 SA-walk non-regression: a ServeWatcher on ctx (clause a) +
// canonical-SA identity, with the dispatch informer UNSYNCED (unregistered on
// the serve-watcher, so the informer-serve branch is skipped and the live
// LIST runs) + an UNPUBLISHED RBAC snapshot. The full un-narrowed set is
// served. The exemption is load-bearing: inverting the predicate makes the
// RC1 guard fall through (served=false) here — see the mutant note.
func TestSARegate_ARM4_Phase1SAWalk_Unnarrowed(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.SetGlobal(nil) // UNPUBLISHED snapshot (Global()==nil)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	// A real ServeWatcher with the dispatch GVR NOT registered → WaitForGVRSync
	// returns fast-false → informer-serve branch skipped → live LIST.
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		dispatchTestScheme(), dispatchTestListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{
		{"team-a", "team-a-x"}, {"team-b", "team-b-x"}, {"bench-ns-1", "bench-ns-1-x"},
	})
	ctx := cache.WithServeWatcher(
		cache.WithInternalRESTConfig(ctxWithUser(saRegateIdentity), rc), rw)

	raw, served, derr := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/restactions"))
	if derr != nil || !served {
		t.Fatalf("Phase-1 SA-walk LIST must serve; got served=%v err=%v", served, derr)
	}
	nss := servedItemNamespaces(t, raw)
	t.Logf("ARM-4 served namespaces=%v sha256=%s", sortedSet(nss), sha256Hex(raw))
	if len(nss) != 3 || !nss["team-a"] || !nss["team-b"] || !nss["bench-ns-1"] {
		t.Fatalf("SA-walk must serve the FULL un-narrowed set {team-a,team-b,bench-ns-1}; got %v", sortedSet(nss))
	}
}

// ── ARM-5 ──────────────────────────────────────────────────────────────
// cache-off parity: with cache.Disabled()==true the narrowing runs through a
// live SelfSubjectAccessReview. A denied per-user GET yields the SAME
// Forbidden verdict as ARM-1.
func TestSARegate_ARM5_CacheOff_DeniedGet_Forbidden(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	if !cache.Disabled() {
		t.Fatal("precondition: cache must be disabled for the cache-off arm")
	}
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	// Fake SAR clientset that denies every access review.
	cs := k8sfake.NewSimpleClientset()
	cs.PrependReactor("create", "selfsubjectaccessreviews",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, &authv1.SelfSubjectAccessReview{
				Status: authv1.SubjectAccessReviewStatus{Allowed: false},
			}, nil
		})
	restore := rbac.SetSARClientsetForTest(
		func(_ context.Context, _ endpoints.Endpoint) (kubernetes.Interface, error) {
			return cs, nil
		})
	t.Cleanup(restore)

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"bench-ns-1", "bench-ns-1-x"}})
	// userCanViaSAR reads xcontext.UserConfig(ctx); provide an endpoint so it
	// reaches the fake clientset instead of short-circuiting to deny.
	ctx := xcontext.BuildContext(ctxWithUser(narrowUser),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test"}))
	ctx = cache.WithInternalRESTConfig(ctx, rc)

	raw, served, err := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, getByNamePath("bench-ns-1", "bench-ns-1-x")))

	t.Logf("ARM-5 served=%v err=%v sha256=%s", served, err, sha256Hex(raw))
	if served {
		t.Fatalf("cache-off LEAK: denied per-user GET served under the SA identity (%d bytes)", len(raw))
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("cache-off denied GET must return Forbidden (parity with ARM-1); got %v", err)
	}
}

// ── ARM-6 ──────────────────────────────────────────────────────────────
// A group-only user (Username=="", Groups=[devs]) is a REAL end-user and must
// be narrowed — NOT exempted on the empty Username. Denied where the group has
// no grant (RED against a naive Username=="" exemption); served where the
// group IS granted (proves the deny is grant-driven, not a blanket drop).
func TestSARegate_ARM6_GroupOnlyUser_Narrowed(t *testing.T) {
	t.Run("denied_no_group_grant", func(t *testing.T) {
		resetInternalClientCacheForTest()
		t.Cleanup(resetInternalClientCacheForTest)
		newRBACNarrowGetWatcher(t) // grants the USER narrowUser, NOT the devs group

		rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"team-a", "team-a-x"}})
		// Username deliberately empty; identity is group-only.
		ctx := cache.WithInternalRESTConfig(ctxWithUser("", "devs"), rc)

		raw, served, err := dispatchViaInternalRESTConfig(ctx,
			buildCall(http.MethodGet, getByNamePath("team-a", "team-a-x")))
		t.Logf("ARM-6/denied served=%v err=%v sha256=%s", served, err, sha256Hex(raw))
		if served {
			t.Fatalf("RC2 LEAK: group-only user (empty Username) was served un-narrowed "+
				"(%d bytes) — an empty Username must NOT be exempted", len(raw))
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("group-only denied GET must return Forbidden; got %v", err)
		}
	})

	t.Run("served_with_group_grant", func(t *testing.T) {
		resetInternalClientCacheForTest()
		t.Cleanup(resetInternalClientCacheForTest)
		newGroupGrantGetWatcher(t, "devs", "team-a")

		rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"team-a", "team-a-x"}})
		ctx := cache.WithInternalRESTConfig(ctxWithUser("", "devs"), rc)

		raw, served, err := dispatchViaInternalRESTConfig(ctx,
			buildCall(http.MethodGet, getByNamePath("team-a", "team-a-x")))
		t.Logf("ARM-6/granted served=%v err=%v sha256=%s", served, err, sha256Hex(raw))
		if err != nil || !served {
			t.Fatalf("group-only user WITH a devs group grant must be served; got served=%v err=%v", served, err)
		}
		if !strings.Contains(string(raw), `"name":"team-a-x"`) {
			t.Fatalf("served object is not team-a-x: %s", string(raw))
		}
	})
}

// newGroupGrantGetWatcher builds a synced cache=on watcher granting `get` on
// the dispatch GVR to a GROUP subject in a single namespace, sets it global,
// and publishes the RBAC snapshot.
func newGroupGrantGetWatcher(t *testing.T, group, ns string) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	seed := []runtime.Object{
		newTestRestActionRuntimeObject(ns, ns+"-x", "marker"),
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "restaction-getter"},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{dispatchTestGVR.Group}, Resources: []string{dispatchTestGVR.Resource}, Verbs: []string{"get"}},
			},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "get-grp-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: "rbac.authorization.k8s.io", Name: group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "restaction-getter"},
		},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		dispatchTestScheme(), dispatchTestListKinds(), seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	added, syncCh := rw.EnsureResourceType(dispatchTestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("dispatch informer did not sync")
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// ── ARM-7 ──────────────────────────────────────────────────────────────
// Refresher identity-free: a canonical-SA identity with NO ServeWatcher.
// clause (d) fires → the LIST is served un-narrowed even though the PUBLISHED
// snapshot grants the SA nothing. Inverting the predicate narrows to empty
// (see the mutant note).
func TestSARegate_ARM7_RefresherIdentityFree_Unnarrowed(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	newRBACNarrowWatcher(t) // PUBLISHED snapshot; the SA identity has NO list grant

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{
		{"team-a", "team-a-x"}, {"team-b", "team-b-x"}, {"bench-ns-1", "bench-ns-1-x"},
	})
	// Canonical SA, NO ServeWatcher on ctx.
	ctx := cache.WithInternalRESTConfig(ctxWithUser(saRegateIdentity), rc)

	raw, served, err := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/restactions"))
	if err != nil || !served {
		t.Fatalf("refresher SA LIST must serve; got served=%v err=%v", served, err)
	}
	nss := servedItemNamespaces(t, raw)
	t.Logf("ARM-7 served namespaces=%v sha256=%s", sortedSet(nss), sha256Hex(raw))
	if len(nss) != 3 || !nss["team-a"] || !nss["team-b"] || !nss["bench-ns-1"] {
		t.Fatalf("refresher (clause d) must serve the FULL un-narrowed set; got %v", sortedSet(nss))
	}
}

// ── ARM-8 ──────────────────────────────────────────────────────────────
// RC1 guard: a non-exempt per-user GET is DENIED because the RBAC snapshot is
// not yet published (cache=on, Global()==nil → EvaluateRBAC cannot vouch).
// The dispatcher must FALL THROUGH (served==false, err==nil) — NOT return a
// Forbidden — so a mid-rebuild refresher / representative identity does not
// overwrite a warm cell with a spurious 403.
func TestSARegate_ARM8_RC1Guard_UnpublishedSnapshot_FallsThrough(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // cache ON, but…
	cache.SetGlobal(nil)              // …no watcher/snapshot published (unpublished)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	// Precondition drives internalDispatchRBACSnapshotUnpublished() true via the
	// Global()==nil disjunct (cache=on, no watcher wired). NOTE: LiveRBACSnapshot()
	// is a PROCESS-GLOBAL atomic that a prior arm may have published and that
	// SetGlobal(nil) does not clear — so we deliberately do NOT assert it nil; the
	// guard fires on Global()==nil regardless, which is what this arm exercises.
	if cache.Disabled() || cache.Global() != nil {
		t.Fatal("precondition: cache=on (not Disabled) with no watcher wired (Global()==nil)")
	}

	rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"bench-ns-1", "bench-ns-1-x"}})
	ctx := cache.WithInternalRESTConfig(ctxWithUser(narrowUser), rc)

	raw, served, err := dispatchViaInternalRESTConfig(ctx,
		buildCall(http.MethodGet, getByNamePath("bench-ns-1", "bench-ns-1-x")))

	t.Logf("ARM-8 served=%v err=%v sha256=%s", served, err, sha256Hex(raw))
	if served {
		t.Fatalf("RC1 guard: an unpublished-snapshot deny must NOT serve; got served=true (%d bytes)", len(raw))
	}
	if err != nil {
		t.Fatalf("RC1 guard: an unpublished-snapshot deny must FALL THROUGH (err==nil), "+
			"not forbid — a spurious Forbidden here would poison a warm cell; got %v", err)
	}
}
