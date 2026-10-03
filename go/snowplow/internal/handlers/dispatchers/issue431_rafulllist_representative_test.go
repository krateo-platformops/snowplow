// issue431_rafulllist_representative_test.go — #431: raFullList cells carry a
// representative identity, so the REFRESHER re-resolves them under their class.
//
// Before the fix the raFullList key producer (apiref.seedFullListRAKey) never
// stamped RepresentativeUsername/Groups. The refresher therefore re-resolved
// every raFullList cell as ("", nil): pre-#424 that overwrote a good cell with
// an EMPTY list, and since #424 the identity-class guard declines it
// (binding_set) — raFullList cells were refresh-by-traffic-only, stale after a
// dep change until the next customer Put or the TTL.
//
// REAL BOUNDARY: the cell is minted by the real apiref.Resolve (customer) or the
// real seedRAFullListForWidget (seed) over a real ResourceWatcher + published
// RBAC snapshot; the RESTAction LISTs configmaps cluster-wide and the informer
// pivot narrows each item per requester (filterListByRBAC). The dep change is a
// real informer UPDATE; the refresh is the real refresher loop
// (cache.StartRefresher → resolveAndPopulateL1 → resolveOnceProd). No customer
// request runs between the mint and the assertion.
//
// RBAC shape (K>1 classes):
//   - Group:portal may GET the RESTAction (psBuildWatcher's shared CRB) and
//     LIST configmaps in ns x (k423PortalReaders) — carol and dave, group-only
//     members, are ONE class (narrow: sees x/y only).
//   - alice additionally holds a User RoleBinding in ns w — her own class
//     (wide: sees x/y and w/v).
//
// ARMS
//   (a) TestIssue431_RefresherConvergesRAFullListWithoutTraffic — RED on main
//       (the refresh is declined binding_set; the cell keeps the pre-update body).
//   (b) TestIssue431_RefreshNeverEmptyOrNarrowed — both classes' cells refresh;
//       each equals its own member's fresh unpaginated resolve, neither is empty,
//       the wide cell keeps w/v, the narrow cell never gains it. RED on main
//       (stale).
//   (c) TestIssue431_RepresentativeDriftStillDeclines — the representative
//       (carol) gains a binding in ns w; the refresh is declined
//       (representative_drift), dave is never served w/v. And
//       TestIssue431_SubGenMovesInsideRefresh_NoWrite — a grant+revoke on the
//       representative inside the re-resolve (binding set unchanged at both
//       checks) must not be written. Mutation: dropping the sub-gen bracket
//       makes the second arm RED.
//   (d) TestIssue431_SeedAndCustomerRAKeyParity — the seed-minted raKey equals
//       the customer's raKey byte-for-byte (pre-hash inputs equal modulo the
//       representative), and both carry an in-class representative. RED on main
//       (no representative on either).

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	i431WideNS  = "w"
	i431WideObj = "v"
)

var i431CMGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

// i431SliceJQ sorts the cluster-wide configmap LIST deterministically and
// returns one top-level array (the Ship-4a sliceable shape).
const i431SliceJQ = `
{
  cms: (
    ((.cms.items // []) | map({ns: .metadata.namespace, name: .metadata.name, v: .data.password})) as $items
    | ($items | sort_by(.ns + "/" + .name)) as $sorted
    | (.slice.offset  // 0)                 as $offset
    | (.slice.perPage // ($sorted | length)) as $perPage
    | [ $sorted | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $sorted[.] ]
  )
}
`

func i431RACR(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec": map[string]any{
			"api": []any{
				map[string]any{"name": "cms", "path": "/api/v1/configmaps", "continueOnError": true},
			},
			"filter": i431SliceJQ,
		},
	}}
}

func i431ApiRef(name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: h1NS},
		Resource:   h1RAGVR.Resource,
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
	}
}

// i431Fixture stands up the watcher (RBAC + the RA CR + both configmaps), the
// per-user clientconfigs and the SA transport. Returns the dynamic client (for
// the informer UPDATE / grants), the SA endpoint + rest config.
func i431Fixture(t *testing.T, raName string) (*dynamicfake.FakeDynamicClient, *endpoints.Endpoint, *rest.Config) {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	extra := append(k423PortalReaders(a),
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: i431WideNS, Name: "target-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: i431WideNS, Name: "alice-wide", UID: types.UID("uid-rb-431-alice-wide")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psAlice}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
		},
		&corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Namespace: i431WideNS, Name: i431WideObj},
			Data:       map[string]string{"password": "wide-only"},
		},
		i431RACR(raName),
	)
	dyn := psBuildWatcher(t, a, extra...)
	rw := cache.Global()
	if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !rw.IsServable(h1RAGVR) {
		t.Fatalf("PRE: restactions must be servable (objects.Get serves the RA from the informer)")
	}
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	return dyn, &endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, &rest.Config{Host: srv.URL}
}

func i431Ctx(user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{psGroup}}))
}

// i431Serve is the customer path: a paginated apiRef resolve under ctx.
func i431Serve(t *testing.T, ctx context.Context, raName string, saRC *rest.Config) {
	t.Helper()
	if _, err := apiref.Resolve(ctx, apiref.ResolveOptions{
		RC: saRC, ApiRef: i431ApiRef(raName), AuthnNS: psAuthnNS, PerPage: 1, Page: 1,
	}); err != nil {
		t.Fatalf("apiref.Resolve: %v", err)
	}
}

// i431RAKey derives the production raKey for ctx's identity.
func i431RAKey(t *testing.T, ctx context.Context, raName string) (string, cache.ResolvedKeyInputs) {
	t.Helper()
	in, ok := apiref.SeedFullListRAKeyInputsForTest(ctx, h1RAGVR, h1NS, raName, nil)
	if !ok {
		t.Fatalf("PRE: no raKey for this identity (RA GET not permitted?)")
	}
	return cache.ComputeKey(in), in
}

// i431Fresh is a member's FRESH unpaginated resolve of the RA, encoded exactly
// as the cell stores it (json.Marshal of the status map).
func i431Fresh(t *testing.T, ctx context.Context, raName string, saRC *rest.Config) []byte {
	t.Helper()
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(i431RACR(raName).Object, &cr); err != nil {
		t.Fatalf("convert RA: %v", err)
	}
	res, err := restactions.Resolve(ctx, restactions.ResolveOptions{In: &cr, SArc: saRC, AuthnNS: psAuthnNS})
	if err != nil || res.Status == nil {
		t.Fatalf("fresh resolve: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(res.Status.Raw, &m); err != nil {
		t.Fatalf("fresh status: %v", err)
	}
	b, _ := json.Marshal(m)
	return b
}

func i431Body(key string) string {
	if e, ok := cache.ResolvedCache().Get(key); ok {
		return string(e.RawJSON)
	}
	return "<absent>"
}

// i431UpdateCM flips x/y's password — a real informer UPDATE on an object both
// classes read.
func i431UpdateCM(t *testing.T, dyn *dynamicfake.FakeDynamicClient, value string) {
	t.Helper()
	u, err := dyn.Resource(i431CMGVR).Namespace(psTargetNS).Get(context.Background(), psTargetObj, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get cm: %v", err)
	}
	_ = unstructured.SetNestedField(u.Object, value, "data", "password")
	if _, err := dyn.Resource(i431CMGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update cm: %v", err)
	}
}

// i431Loop starts the real refresher with the production RefreshFunc body for
// the raFullList class.
func i431Loop(t *testing.T, saEP *endpoints.Endpoint, saRC *rest.Config) *atomic.Int64 {
	t.Helper()
	i187RefresherEnv(t)
	n := &atomic.Int64{}
	cache.RegisterRefreshFunc(cache.CacheEntryClassRAFullList, func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		n.Add(1)
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })
	return n
}

func i431Eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// --- (a) ---------------------------------------------------------------------

func TestIssue431_RefresherConvergesRAFullListWithoutTraffic(t *testing.T) {
	const ra = "rafl431-a"
	dyn, saEP, saRC := i431Fixture(t, ra)
	carol := i431Ctx(psCarol)
	key, _ := i431RAKey(t, carol, ra)
	i431Serve(t, carol, ra, saRC)
	pre := i431Body(key)
	if !strings.Contains(pre, psSentinel) {
		t.Fatalf("SETUP: the customer resolve must leave a resident raFullList cell carrying x/y; body=%s", psTrunc(pre, 300))
	}

	invocations := i431Loop(t, saEP, saRC)
	i431UpdateCM(t, dyn, "after-431-update")

	converged := i431Eventually(10*time.Second, func() bool { return strings.Contains(i431Body(key), "after-431-update") })
	if !converged {
		t.Fatalf("STALE raFullList cell: a dep change on a resident cell was not refreshed by the refresher "+
			"(no customer traffic). refresher invocations=%d, drift declines=%d, body=%s",
			invocations.Load(), driftDeclined424("refresher"), psTrunc(i431Body(key), 300))
	}
	if invocations.Load() == 0 {
		t.Fatalf("the cell changed but the refresher never ran — something other than the refresher wrote it")
	}
	if got, want := i431Body(key), string(i431Fresh(t, i431Ctx(psDave), ra, saRC)); got != want {
		t.Fatalf("refreshed cell != a class member's fresh body\n cell =%s\n fresh=%s", psTrunc(got, 300), psTrunc(want, 300))
	}
}

// --- (b) ---------------------------------------------------------------------

func TestIssue431_RefreshNeverEmptyOrNarrowed(t *testing.T) {
	const ra = "rafl431-b"
	dyn, saEP, saRC := i431Fixture(t, ra)
	alice, carol := i431Ctx(psAlice), i431Ctx(psCarol)
	wKey, _ := i431RAKey(t, alice, ra)
	nKey, _ := i431RAKey(t, carol, ra)
	if wKey == nKey {
		t.Fatalf("PRE: alice (wide) and carol (narrow) must be different classes")
	}
	i431Serve(t, alice, ra, saRC)
	i431Serve(t, carol, ra, saRC)
	if b := i431Body(wKey); !strings.Contains(b, "wide-only") || !strings.Contains(b, psSentinel) {
		t.Fatalf("SETUP: the wide cell must hold x/y and w/v; body=%s", psTrunc(b, 300))
	}
	if b := i431Body(nKey); strings.Contains(b, "wide-only") || !strings.Contains(b, psSentinel) {
		t.Fatalf("SETUP: the narrow cell must hold x/y only; body=%s", psTrunc(b, 300))
	}

	i431Loop(t, saEP, saRC)
	i431UpdateCM(t, dyn, "after-431-b")
	ok := i431Eventually(10*time.Second, func() bool {
		return strings.Contains(i431Body(wKey), "after-431-b") && strings.Contains(i431Body(nKey), "after-431-b")
	})
	if !ok {
		t.Fatalf("STALE: both classes' cells must be refreshed; wide=%s narrow=%s",
			psTrunc(i431Body(wKey), 200), psTrunc(i431Body(nKey), 200))
	}
	wide, narrow := i431Body(wKey), i431Body(nKey)
	if !strings.Contains(wide, "wide-only") {
		t.Fatalf("NARROWED: the wide class's refreshed cell lost w/v (refreshed under a narrower identity); body=%s", psTrunc(wide, 300))
	}
	if strings.Contains(narrow, "wide-only") {
		t.Fatalf("WIDENED: the narrow class's refreshed cell carries w/v (another class's view); body=%s", psTrunc(narrow, 300))
	}
	for name, b := range map[string]string{"wide": wide, "narrow": narrow} {
		var m map[string]any
		if err := json.Unmarshal([]byte(b), &m); err != nil {
			t.Fatalf("%s cell not JSON: %v", name, err)
		}
		if arr, _ := m["cms"].([]any); len(arr) == 0 {
			t.Fatalf("EMPTY: the %s cell was refreshed to an empty list (the pre-#424 poison shape); body=%s", name, b)
		}
	}
	if want := string(i431Fresh(t, alice, ra, saRC)); wide != want {
		t.Fatalf("wide cell != alice's fresh body\n cell =%s\n fresh=%s", wide, want)
	}
	if want := string(i431Fresh(t, i431Ctx(psDave), ra, saRC)); narrow != want {
		t.Fatalf("narrow cell != dave's fresh body\n cell =%s\n fresh=%s", narrow, want)
	}
}

// --- (c) ---------------------------------------------------------------------

func i431GrantWide(t *testing.T, dyn *dynamicfake.FakeDynamicClient, user string) {
	t.Helper()
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: i431WideNS, Name: user + "-wide", UID: types.UID("uid-rb-431-" + user)},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
	}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if _, err := dyn.Resource(k424RBGVR).Namespace(i431WideNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if !i431Eventually(5*time.Second, func() bool { return i431CanListWide(user) }) {
		t.Fatalf("grant to %s did not reach the snapshot", user)
	}
}

func i431RevokeWide(t *testing.T, dyn *dynamicfake.FakeDynamicClient, user string) {
	t.Helper()
	if err := dyn.Resource(k424RBGVR).Namespace(i431WideNS).Delete(context.Background(), user+"-wide", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if !i431Eventually(5*time.Second, func() bool { return !i431CanListWide(user) }) {
		t.Fatalf("revoke of %s did not reach the snapshot", user)
	}
}

func i431CanListWide(user string) bool {
	ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: user, Groups: []string{psGroup}, Verb: "list", Resource: "configmaps", Namespace: i431WideNS,
	})
	return ok
}

func TestIssue431_RepresentativeDriftStillDeclines(t *testing.T) {
	const ra = "rafl431-c"
	dyn, saEP, saRC := i431Fixture(t, ra)
	carol, dave := i431Ctx(psCarol), i431Ctx(psDave)
	key, _ := i431RAKey(t, carol, ra)
	if dk, _ := i431RAKey(t, dave, ra); dk != key {
		t.Fatalf("PRE: carol and dave must share one raFullList cell")
	}
	i431Serve(t, carol, ra, saRC)
	e, ok := cache.ResolvedCache().Get(key)
	if !ok || e.Inputs == nil {
		t.Fatalf("SETUP: no resident cell")
	}
	if e.Inputs.RepresentativeUsername != psCarol {
		t.Fatalf("SETUP: the cell's representative must be its first writer (carol); got %q", e.Inputs.RepresentativeUsername)
	}
	pre := string(e.RawJSON)

	i431GrantWide(t, dyn, psCarol) // carol leaves the class; dave stays in it
	if dk, _ := i431RAKey(t, dave, ra); dk != key {
		t.Fatalf("PRE: dave must still derive the cell's key")
	}
	if ck, _ := i431RAKey(t, carol, ra); ck == key {
		t.Fatalf("PRE: carol's own key must have moved (her binding set changed)")
	}
	if fresh := i431Fresh(t, carol, ra, saRC); !bytes.Contains(fresh, []byte("wide-only")) {
		t.Fatalf("CONTROL: carol's post-grant fresh view must carry w/v, else the arm cannot fail")
	}

	before := identityClassDriftDeclinedForTest("refresher", "binding_set")
	invocations := i431Loop(t, saEP, saRC)
	i431UpdateCM(t, dyn, "after-431-c")
	if !i431Eventually(10*time.Second, func() bool {
		return identityClassDriftDeclinedForTest("refresher", "binding_set") > before
	}) {
		t.Fatalf("the refresh was not declined by the #424 binding-set limb (invocations=%d, body=%s)",
			invocations.Load(), psTrunc(i431Body(key), 300))
	}
	if got := i431Body(key); strings.Contains(got, "wide-only") {
		t.Fatalf("REPRESENTATIVE DRIFT LEAK: the refresher re-Put carol's widened view under the key dave derives; body=%s", psTrunc(got, 300))
	}
	if got := i431Body(key); got != pre && got != "<absent>" {
		t.Fatalf("the declined refresh changed the cell: pre=%s now=%s", psTrunc(pre, 200), psTrunc(got, 200))
	}
}

// TestIssue431_SubGenMovesInsideRefresh_NoWrite — carol is granted ns w and
// revoked again INSIDE the refresher's re-resolve, which reads under the
// transient grant. At both drift checks her binding set equals the minted one,
// so only the sub-gen can see it: since #435 the raFullList key folds RBACSubGen
// and identityClassDrift's rbac_subgen limb declines the write (it replaced the
// #431 refresher bracket, which this arm drove first).
func TestIssue431_SubGenMovesInsideRefresh_NoWrite(t *testing.T) {
	const ra = "rafl431-c2"
	dyn, saEP, saRC := i431Fixture(t, ra)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, i431CMGVR}) // activates sub-gen deltas
	carol := i431Ctx(psCarol)
	key, in := i431RAKey(t, carol, ra)
	i431Serve(t, carol, ra, saRC)
	e, ok := cache.ResolvedCache().Get(key)
	if !ok || e.Inputs == nil {
		t.Fatalf("SETUP: no resident cell")
	}
	stored := *e.Inputs

	var armed atomic.Bool
	restore := setResolveOnceForTest(func(ctx context.Context, i cache.ResolvedKeyInputs) ([]byte, error) {
		if armed.CompareAndSwap(false, true) {
			sg0 := cache.RBACSubGenForSubject(psCarol, rbac.WithAuthenticatedGroup([]string{psGroup}))
			i431GrantWide(t, dyn, psCarol)
			out, err := resolveOnceProd(ctx, i) // reads under the transient grant
			i431RevokeWide(t, dyn, psCarol)
			k424SubGenSettled(t, psCarol, []string{psGroup}, sg0)
			return out, err
		}
		return resolveOnceProd(ctx, i)
	})
	defer restore()

	before := identityClassDriftDeclinedForTest("refresher", "rbac_subgen")
	if err := resolveAndPopulateL1(context.Background(), stored, saEP, saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if identityClassDriftDeclinedForTest("refresher", "rbac_subgen") <= before {
		t.Fatalf("the decline must be counted on refresher/rbac_subgen")
	}
	if rbac.SubjectBindingSetDigest(psCarol, []string{psGroup}) != in.SubjectBindingSet {
		t.Fatalf("PRE: carol's binding set must be back to the minted one (sub-gen-only drift)")
	}
	if !armed.Load() {
		t.Fatalf("the refresh never reached the re-resolve — the arm did not run")
	}
	if got := i431Body(key); strings.Contains(got, "wide-only") {
		t.Fatalf("SUB-GEN DRIFT LEAK: a body read under carol's transient grant was written under the class key; body=%s",
			psTrunc(got, 300))
	}
	// The representative's own key rotated with its sub-gen (#435): carol's next
	// call mints a new cell; the old one is not hers any more.
	if nk, _ := i431RAKey(t, carol, ra); nk == key {
		t.Fatalf("carol's raKey must rotate with her RBAC sub-gen (#435)")
	}
}

// --- (d) ---------------------------------------------------------------------

func TestIssue431_SeedAndCustomerRAKeyParity(t *testing.T) {
	const ra = "rafl431-d"
	_, saEP, saRC := i431Fixture(t, ra)
	// Customer: carol (group-only member of portal).
	carol := i431Ctx(psCarol)
	i431Serve(t, carol, ra, saRC)
	cKey, cIn := i431RAKey(t, carol, ra)
	ce, ok := cache.ResolvedCache().Get(cKey)
	if !ok || ce.Inputs == nil {
		t.Fatalf("SETUP: customer did not mint a cell")
	}
	custStored := *ce.Inputs

	// Seed: the portal cohort's group representative through the REAL seed
	// primitive (a widget whose apiRef names the RA).
	cache.ResetResolvedCacheForTest()
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": "w431", "namespace": h1NS},
		"spec": map[string]any{"apiRef": map[string]any{
			"name": ra, "namespace": h1NS,
		}},
	}}
	cohort := withCohortSeedContext(context.Background(), seedTarget{Username: "", Groups: []string{psGroup}}, *saEP, saRC)
	seedRAFullListForWidget(cohort, widget, psAuthnNS, h1NS, "w431")
	sKey, sIn := i431RAKey(t, cohort, ra)
	se, ok := cache.ResolvedCache().Get(sKey)
	if !ok || se.Inputs == nil {
		t.Fatalf("SETUP: the seed did not mint a raFullList cell")
	}
	seedStored := *se.Inputs

	if sKey != cKey {
		t.Fatalf("KEY PARITY: seed raKey %s != customer raKey %s\n seed=%+v\n cust=%+v", sKey, cKey, sIn, cIn)
	}
	strip := func(in cache.ResolvedKeyInputs) cache.ResolvedKeyInputs {
		in.RepresentativeUsername, in.RepresentativeGroups = "", nil
		return in
	}
	if !reflect.DeepEqual(strip(seedStored), strip(custStored)) {
		t.Fatalf("PRE-HASH PARITY: stored inputs differ beyond the representative\n seed=%+v\n cust=%+v", seedStored, custStored)
	}
	// Both carry an in-class representative (the #431 fix): the refresher can
	// re-resolve the cell under it and the #424 guard accepts it.
	for name, in := range map[string]cache.ResolvedKeyInputs{"customer": custStored, "seed": seedStored} {
		if in.RepresentativeUsername == "" && len(in.RepresentativeGroups) == 0 {
			t.Fatalf("NO REPRESENTATIVE on the %s-minted raFullList cell: the refresher would re-resolve it as (\"\", nil)", name)
		}
		if d := identityClassDrift(context.Background(), &in, in.RepresentativeUsername, in.RepresentativeGroups); d != "" {
			t.Fatalf("the %s cell's representative is not a member of the key's class at mint (drift=%s)", name, d)
		}
	}
}
