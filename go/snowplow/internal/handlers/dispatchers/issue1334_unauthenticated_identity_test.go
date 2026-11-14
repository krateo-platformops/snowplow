// issue1334_unauthenticated_identity_test.go — #1334 prerequisite: an identity
// presenting system:unauthenticated is refused what is granted only to
// system:authenticated, by snowplow exactly as by the apiserver, on the live
// path AND from cache.
//
// The identity is #1334's public visitor: user krateo:public with the single
// group system:unauthenticated. The apiserver adds no system:authenticated to it
// (k8s.io/apiserver v0.36.5/v0.37.2 impersonation.go:126-144, and the
// AuthenticatedGroupAdder on the authentication path).
//
// WHY THE SERVE IS DRIVEN, NOT ONLY THE KEY. Before #1334 the evaluator matched
// a system:authenticated subject for ANY non-empty username, and the key fold
// added system:authenticated to every group set. So krateo:public was permitted
// the RESTAction, derived the SAME first-match BindingUID and the SAME
// binding-set digest as an authenticated user with no other grants — the SAME
// key — and was served that user's cell. This arm asserts on what leaves the
// handler: the status and the bytes.
//
// RED before the fix: the cold public request is 200 (live path) and the warm one
// is served alice's sentinel (from cache).

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	i1334Public       = "krateo:public"
	i1334Unauth       = "system:unauthenticated"
	i1334Alice        = "alice-1334" // authenticated, no grant of her own
	i1334AuthOnlyRA   = "catalog-index"
	i1334PublicRA     = "public-page"
	i1334AliceMarker  = "AUTHENTICATED-ONLY-1334"
	i1334PublicMarker = "PUBLIC-PAGE-1334"
)

// i1334BuildWatcher publishes the 057 shape #1334 found: a ClusterRoleBinding
// granting `get restactions` to Group system:authenticated ONLY (057's
// authenticated-get-catalog-index), plus the public page's own Role, bound to
// the USER krateo:public with resourceNames [public-page]. extra objects are
// published alongside.
func i1334BuildWatcher(t *testing.T, extra ...runtime.Object) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)

	g := "rbac.authorization.k8s.io"
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:     "RESTActionList",
		h1WidgetGVR: "PanelList",
		{Group: g, Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: g, Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: g, Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: g, Version: "v1", Resource: "roles"}:               "RoleList",
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "catalog-reader-1334"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group},
				Resources: []string{h1RAGVR.Resource}, ResourceNames: []string{i1334AuthOnlyRA}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "authenticated-get-catalog-index", UID: types.UID("uid-crb-1334-authenticated")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: "system:authenticated"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "catalog-reader-1334"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: h1NS, Name: "public-page-1334"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group},
				Resources: []string{h1RAGVR.Resource}, ResourceNames: []string{i1334PublicRA}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: h1NS, Name: "public-page-1334", UID: types.UID("uid-rb-1334-public")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: g, Name: i1334Public}},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "public-page-1334"},
		},
	}
	seed = append(seed, extra...)
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...))
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

func i1334Ctx(user string, groups []string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
}

func i1334RA(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec":       map[string]any{"api": []any{}},
	}}
}

// i1334Serve drives the REAL restActionHandler with the REAL dispatch RBAC gate
// (checkDispatchRBAC → EvaluateRBAC; the seam is NOT stubbed) and the real L1.
// Only the CR fetch and the resolver are seamed; the resolver stamps the
// requester's marker so a body can only cross identities through the cache.
func i1334Serve(t *testing.T, ctx context.Context, name, marker string, resolves *int) *httptest.ResponseRecorder {
	t.Helper()
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: i1334RA(name)}
	})
	defer r1()
	r2 := setRestactionsResolveForTest(func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		*resolves++
		raw, _ := json.Marshal(map[string]any{"marker": marker})
		out := &templatesv1.RESTAction{Status: &runtime.RawExtension{Raw: raw}}
		out.SetName(name)
		out.SetNamespace(h1NS)
		return out, nil
	})
	defer r2()
	rec := httptest.NewRecorder()
	RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
	return rec
}

// i1334Visitor is a second identity presenting system:unauthenticated, with NO
// grant of its own. Before #1334 its binding set and sub-gen equalled alice's,
// so it derived alice's exact key and was served her cell.
const i1334Visitor = "visitor-1334"

func TestIssue1334_UnauthenticatedIdentity_RefusedAuthenticatedOnlyGrant_LiveAndFromCache(t *testing.T) {
	i1334BuildWatcher(t)
	unauth := []string{i1334Unauth}
	aliceCtx := i1334Ctx(i1334Alice, nil)

	verdict := func(user string, groups []string, name string) bool {
		t.Helper()
		ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.ObjectCheck(user, groups, "get",
			rbac.ObjectTarget{Group: h1RAGVR.Group, Resource: h1RAGVR.Resource, Namespace: h1NS, Name: name}))
		if err != nil {
			t.Fatalf("EvaluateRBAC(%s): %v", user, err)
		}
		return ok
	}
	key := func(ctx context.Context) (string, cacheHandle, *cache.ResolvedKeyInputs) {
		return dispatchCacheLookupKey(ctx, "restactions",
			h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, i1334AuthOnlyRA, -1, -1, nil)
	}

	// --- Preconditions and controls (live verdicts). ---------------------------
	if !verdict(i1334Alice, nil, i1334AuthOnlyRA) {
		t.Fatalf("PRECONDITION: an authenticated user must be granted %s through system:authenticated", i1334AuthOnlyRA)
	}
	if !verdict(i1334Public, unauth, i1334PublicRA) {
		t.Fatalf("CONTROL: %s must be granted its own page %s (bound to the user)", i1334Public, i1334PublicRA)
	}
	// The same username WITHOUT system:unauthenticated is an ordinary
	// authenticated user: the group set decides, not the username.
	if !verdict(i1334Public, nil, i1334AuthOnlyRA) {
		t.Errorf("CONTROL: %s with no groups is authenticated and must be granted %s", i1334Public, i1334AuthOnlyRA)
	}
	aliceKey, handle, aliceIn := key(aliceCtx)
	if handle == nil || aliceIn == nil || aliceIn.BindingUID == "" {
		t.Fatalf("setup: expected a live handle and alice's non-empty BindingUID; handle=%v alice=%+v", handle != nil, aliceIn)
	}

	resolves := 0
	unauthIDs := []string{i1334Public, i1334Visitor}

	// --- Live path: refused verdict, distinct key, cold request 403. -----------
	for _, user := range unauthIDs {
		ctx := i1334Ctx(user, unauth)
		if verdict(user, unauth, i1334AuthOnlyRA) {
			t.Errorf("LIVE: %s + %s is granted %s, which only system:authenticated holds — the apiserver refuses it",
				user, i1334Unauth, i1334AuthOnlyRA)
		}
		if verdict(user, unauth, i1334AuthOnlyRA) { // the repeat is answered by the authz memo
			t.Errorf("MEMO: the repeated check for %s is granted", user)
		}
		k, _, in := key(ctx)
		if k == aliceKey {
			t.Errorf("KEY: %s + %s derives the authenticated user's key %q — it would be served her cell", user, i1334Unauth, aliceKey)
		}
		if in == nil || in.BindingUID != "" {
			t.Errorf("KEY: %s derives first-match BindingUID %q (the system:authenticated binding); want \"\" (no permit)",
				user, in.BindingUID)
		}
		if rbac.IdentityClassOf(jwtutil.UserInfo{Username: user, Groups: unauth}) == rbac.IdentityClassOf(jwtutil.UserInfo{Username: i1334Alice}) {
			t.Errorf("KEY: %s + %s shares the authenticated user's RBAC identity class", user, i1334Unauth)
		}
		if rec := i1334Serve(t, ctx, i1334AuthOnlyRA, i1334PublicMarker, &resolves); rec.Code != http.StatusForbidden {
			t.Errorf("LIVE SERVE: %s cold request for %s = %d, want 403. body=%s", user, i1334AuthOnlyRA, rec.Code, rec.Body.String())
		}
		if resolves != 0 {
			t.Errorf("LIVE SERVE: the resolver ran %d time(s) for a refused request", resolves)
			resolves = 0
		}
	}

	// --- An authenticated user warms the cell. ---------------------------------
	if rec := i1334Serve(t, aliceCtx, i1334AuthOnlyRA, i1334AliceMarker, &resolves); rec.Code != http.StatusOK ||
		!bytes.Contains(rec.Body.Bytes(), []byte(i1334AliceMarker)) {
		t.Fatalf("SETUP: alice must be served her own body (200 + marker); got %d %s", rec.Code, rec.Body.String())
	}
	if e, ok := handle.Get(aliceKey); !ok || !bytes.Contains(e.RawJSON, []byte(i1334AliceMarker)) {
		t.Fatalf("SETUP: alice's cell must be populated under her key, else the from-cache half proves nothing")
	}

	// --- From cache: the same requests with the authenticated cell warm. -------
	for _, user := range unauthIDs {
		before := resolves
		rec := i1334Serve(t, i1334Ctx(user, unauth), i1334AuthOnlyRA, i1334PublicMarker, &resolves)
		if bytes.Contains(rec.Body.Bytes(), []byte(i1334AliceMarker)) {
			t.Errorf("FROM CACHE LEAK: %s + %s was served the authenticated user's cell: %s", user, i1334Unauth, rec.Body.String())
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("FROM CACHE: %s request for %s with an authenticated user's cell warm = %d, want 403. body=%s",
				user, i1334AuthOnlyRA, rec.Code, rec.Body.String())
		}
		if resolves != before {
			t.Errorf("FROM CACHE: the resolver ran for %s's refused request", user)
		}
	}

	// --- Control: the public visitor IS served its own page. --------------------
	if rec := i1334Serve(t, i1334Ctx(i1334Public, unauth), i1334PublicRA, i1334PublicMarker, &resolves); rec.Code != http.StatusOK ||
		!bytes.Contains(rec.Body.Bytes(), []byte(i1334PublicMarker)) {
		t.Errorf("CONTROL: %s must be served %s (200 + its own marker); got %d %s", i1334Public, i1334PublicRA, rec.Code, rec.Body.String())
	}
}
