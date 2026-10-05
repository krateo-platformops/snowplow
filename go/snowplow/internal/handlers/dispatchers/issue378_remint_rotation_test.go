package dispatchers

// issue378_remint_rotation_test.go — #378 F3: a re-mint across an RBAC rotation,
// KEY and OUTPUT arms (brief issuecomment-5990884686).
//
// Shape: two subjects (carol, dave) in ONE RBAC class — one RoleBinding names
// both — with carol the cell K's representative (first writer) and dave a
// recent hitter. The RESTAction reads three namespaces, so the three identities
// in play have three DIFFERENT views:
//
//	dave   : x            (the shared class grant)
//	carol' : x + z        (after her own binding ADD in z)
//	SA     : x + z + w    (the refresher transport identity: never a body)
//
// A real binding ADD (dyn create → informer → RBAC flush) moves carol out of
// K's class INSIDE K's lead window, before the in-window refresh.
//
// KEY ARM: the refresher declines to refresh under carol (identity_class_drift
// {refresher,*} +1), re-picks dave (#444, outcome hitter), and re-mints K under
// the SAME key (BornAt advanced, representative dave). The #258 reseed then
// INSERTS carol's new-class key K′.
// OUTPUT ARM: K's body is dave's view (x only, the refreshed x value); K′'s body
// is carol's current view (x + z); neither carries the SA-only namespace w.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const (
	r378RotRA   = "r378-rot"
	r378RotZ    = "ROT-Z-ONLY-CAROL"
	r378RotW    = "ROT-W-ONLY-SA"
	r378RotX2   = "ROT-X-V2"
	r378RotZNS  = "z"
	r378RotWNS  = "w378"
	r378RotZObj = "zc"
	r378RotWObj = "wc"
)

func r378RotObjects() []runtime.Object {
	list := func(name, ns string) map[string]any {
		return map[string]any{"name": name, "path": "/api/v1/namespaces/" + ns + "/configmaps", "continueOnError": true}
	}
	return []runtime.Object{
		r378RAObj(r378RotRA, []any{list("xs", psTargetNS), list("zs", r378RotZNS), list("ws", r378RotWNS)}, ""),
		// ONE class for carol and dave: a single binding names both.
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "carol-dave-378", UID: types.UID("uid-rb-378-cd")},
			Subjects: []rbacv1.Subject{
				{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol},
				{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psDave},
			},
			RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: r378RotZNS, Name: "cm-z-378"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		&corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Namespace: r378RotZNS, Name: r378RotZObj},
			Data:       map[string]string{"v": r378RotZ},
		},
		&corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Namespace: r378RotWNS, Name: r378RotWObj},
			Data:       map[string]string{"v": r378RotW},
		},
	}
}

func r378RotQuery() string {
	return "/call?resource=restactions&apiVersion=" + h1RAGVR.Group + "/" + h1RAGVR.Version +
		"&namespace=" + h1NS + "&name=" + r378RotRA
}

func r378RotKey(t *testing.T, ctx context.Context) (string, *cache.ResolvedKeyInputs) {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, r378RotRA, -1, -1, map[string]any{})
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: need a live handle and a non-empty BindingUID")
	}
	return k, in
}

func r378RefreshDriftTotal() int64 {
	var n int64
	for _, c := range IdentityClassDriftDeclinedCells() {
		if c.Site == "refresher" {
			n += c.Count
		}
	}
	return n
}

func TestIssue378_F3_ReMintAcrossRotation_KeyAndOutput(t *testing.T) {
	const ttlS, maxAgeS = 4, 6 // L = min(4, 3) = 3s → lead window [3s, 6s)
	e := r378Setup(t, ttlS, maxAgeS, 0, r378RotObjects()...)
	carol, dave := psUserCtx(e.a, psCarol), psUserCtx(e.a, psDave)
	c := cache.ResolvedCache()

	serve := func(ctx context.Context) string {
		t.Helper()
		rec := httptestRecorderServe(e, ctx, r378RotQuery())
		if rec.code != 200 {
			t.Fatalf("serve: %d %s", rec.code, psTrunc(rec.body, 300))
		}
		return rec.body
	}
	K, _ := r378RotKey(t, carol)
	if kd, _ := r378RotKey(t, dave); kd != K {
		t.Fatalf("PRE: carol and dave must share one cell")
	}
	if b := serve(carol); !strings.Contains(b, psSentinel) || strings.Contains(b, r378RotZ) || strings.Contains(b, r378RotW) {
		t.Fatalf("SETUP: carol's pre-rotation view must be x only; body=%s", psTrunc(b, 400))
	}
	ent0, ok := c.GetNoTouch(K)
	if !ok || ent0.Inputs.RepresentativeUsername != psCarol {
		t.Fatalf("SETUP: K must be cached with carol as its representative")
	}
	if hits0 := r378CellHits("restactions"); serve(dave) == "" || r378CellHits("restactions") != hits0+1 {
		t.Fatalf("SETUP: dave must be an L1 hit on K (he joins the hitter pool)")
	}
	e.startRefresher(t)

	// Into K's lead window by real elapse, THEN rotate carol.
	r378SleepUntil(ent0.BornAt.Add(time.Duration(maxAgeS)*time.Second/2 + 300*time.Millisecond))
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: r378RotZNS, Name: "carol-z-378", UID: types.UID("uid-rb-378-carol-z")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-z-378"},
	}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if _, err := e.dyn.Resource(k424RBGVR).Namespace(r378RotZNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("binding ADD: %v", err)
	}
	var Kp string
	if !i444Wait(5*time.Second, func() bool { Kp, _ = r378RotKey(t, carol); return Kp != K }) {
		t.Fatalf("PRE: carol's key must move after her binding ADD")
	}
	if kd, _ := r378RotKey(t, dave); kd != K {
		t.Fatalf("PRE: dave must still derive K")
	}
	if _, ok := c.GetNoTouch(Kp); ok {
		t.Fatalf("PRE: carol's new-class key K′ must be absent before the #258 reseed")
	}

	drift0, hitter0 := r378RefreshDriftTotal(), representativeRepickForTest(repSourceHitter)
	i431UpdateCM(t, e.dyn, r378RotX2) // the in-window refresh trigger
	if !i444Wait(10*time.Second, func() bool {
		ent, ok := c.GetNoTouch(K)
		return ok && strings.Contains(string(ent.RawJSON), r378RotX2)
	}) {
		t.Fatalf("SETUP: the in-window refresh of K never landed")
	}

	// ---- KEY ARM ----
	ent1, ok := c.GetNoTouch(K)
	if !ok {
		t.Fatalf("#378 F3: K must stay resident under the SAME key across the rotation")
	}
	if d := r378RefreshDriftTotal() - drift0; d < 1 {
		t.Errorf("#378 F3 KEY: the refresher must decline carol's drifted identity (identity_class_drift{refresher} Δ=%d, want ≥1)", d)
	}
	t.Logf("F3: refresher drift cells after the rotation: %v", IdentityClassDriftDeclinedCells())
	if d := representativeRepickForTest(repSourceHitter) - hitter0; d < 1 {
		t.Errorf("#378 F3 KEY: the #444 re-pick of dave (hitter) did not run (Δ=%d)", d)
	}
	if ent1.Inputs.RepresentativeUsername != psDave {
		t.Errorf("#378 F3 KEY: K's representative after the re-mint = %q, want dave", ent1.Inputs.RepresentativeUsername)
	}
	if !ent1.BornAt.After(ent0.BornAt) {
		t.Errorf("#378 F3 KEY RED: the in-window refresh under the re-picked representative did not RE-MINT K "+
			"(BornAt %v → %v): K reaches the cap under dave", ent0.BornAt, ent1.BornAt)
	}
	// ---- OUTPUT ARM (K) ----
	if b := string(ent1.RawJSON); strings.Contains(b, r378RotZ) || strings.Contains(b, r378RotW) {
		t.Errorf("#378 F3 OUTPUT LEAK: K (dave's class) carries carol's or the SA's view; body=%s", psTrunc(b, 400))
	}
	if b := serve(dave); !strings.Contains(b, r378RotX2) || strings.Contains(b, r378RotZ) || strings.Contains(b, r378RotW) {
		t.Errorf("#378 F3 OUTPUT: dave must be served HIS refreshed view from K; body=%s", psTrunc(b, 400))
	}

	// ---- #258 INSERT of K′ for carol ----
	deps := rePrewarmDeps{saEP: *e.saEP, saRC: e.saRC, authnNS: psAuthnNS}
	ref := templatesv1.ObjectReference{Reference: templatesv1.Reference{Name: r378RotRA, Namespace: h1NS},
		Resource: h1RAGVR.Resource, APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version}
	before := time.Now()
	if re := reseedTargets(context.Background(), deps, []reseedRequest{{
		identity: seedTarget{Username: psCarol, Groups: psGroups(e.a)}, ra: ref}}); len(re) != 0 {
		t.Fatalf("#258 reseed re-enqueued %d targets", len(re))
	}
	entP, ok := c.GetNoTouch(Kp)
	if !ok {
		t.Fatalf("#378 F3 KEY: the #258 reseed did not INSERT carol's new-class key K′")
	}
	if entP.BornAt.Before(before) {
		t.Errorf("#378 F3 KEY: K′ must be a fresh birth (BornAt %v < reseed start %v)", entP.BornAt, before)
	}
	// ---- OUTPUT ARM (K′) ----
	if b := string(entP.RawJSON); !strings.Contains(b, r378RotZ) || strings.Contains(b, r378RotW) || !strings.Contains(b, r378RotX2) {
		t.Errorf("#378 F3 OUTPUT: K′ must be carol's CURRENT view (x + z, never the SA's w); body=%s", psTrunc(b, 400))
	}
	if b := serve(carol); !strings.Contains(b, r378RotZ) || strings.Contains(b, r378RotW) {
		t.Errorf("#378 F3 OUTPUT: carol must be served her current view; body=%s", psTrunc(b, 400))
	}
	if ent, ok := c.GetNoTouch(K); !ok || strings.Contains(string(ent.RawJSON), r378RotZ) {
		t.Errorf("#378 F3 OUTPUT LEAK: carol's reseed wrote her view into K")
	}
}

type r378Rec struct {
	code int
	body string
}

func httptestRecorderServe(e *r378Env, ctx context.Context, q string) r378Rec {
	rec := httptest.NewRecorder()
	(&restActionHandler{authnNS: psAuthnNS, saRC: e.saRC}).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, q, nil).WithContext(ctx))
	return r378Rec{code: rec.Code, body: rec.Body.String()}
}
