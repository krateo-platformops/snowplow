package dispatchers

// stepup_identity_class_994_test.go — #994: a step-up session is its own
// identity class, from the JWT to the background refresher.
//
// REAL BOUNDARY: real RS256 JWTs through the REAL middleware.UserConfig (which
// reads the step-up Secret from the informer snapshot), into the REAL
// restActionHandler.ServeHTTP over a real ResourceWatcher + published RBAC
// snapshot (the #423 ps harness); the dep change is a real informer UPDATE and
// the refresh is the real refresher loop → resolveAndPopulateL1 → the real
// restactions resolver.
//
// THE WORLD. acmp guards actions on the group krateo:loa:2. The arm binds that
// group (a ClusterRoleBinding to a write-only role), as a guard that grants
// through RBAC does. The group is part of the token's `groups`, so it is part
// of the binding-set digest and the per-subject sub-generation — every input
// rbac.IdentityClassOf folds — and therefore of every identity-bound key.
//
//   K  the same user's session and step-up requests mint DIFFERENT keys and
//      classes; each cell records its own representative (the step-up cell's
//      carries krateo:loa:2).
//   R  a dep change refreshes the step-up cell UNDER the step-up identity: the
//      refresher's ctx carries krateo:loa:2, its class is the cell's class, the
//      representative is never re-picked, and the refreshed body lands under
//      the step-up key.
//   S  when NO binding names krateo:loa:2, the two sessions hold the same
//      binding set: every RBAC verdict is equal, so the #423 invariant (equal
//      binding sets ⇒ byte-identical output) makes them one class — reads are
//      not admission-controlled, and a write is never answered from a cell
//      (#1011). Pinned so the claim "always a separate class" is never made.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const su994KeyID = "kid-su994"

// su994StepUpGuard binds Group:krateo:loa:2 to a write-only role: acmp's guard,
// expressed through RBAC.
func su994StepUpGuard() []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "su994-stepup-deleter"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"delete"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "su994-stepup-deleter", UID: types.UID("uid-crb-su994-stepup")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: credsecret.StepUpGroup}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "su994-stepup-deleter"},
		},
	}
}

type su994Env struct {
	a      psArm
	dyn    *dynamicfake.FakeDynamicClient
	srvURL string
	keys   jwtutil.KeySource
	priv   *rsa.PrivateKey
}

// su994Setup builds the ps world plus extra, and publishes carol's session AND
// step-up Secrets (both authenticate as carol on the fake apiserver).
func su994Setup(t *testing.T, extra ...runtime.Object) *su994Env {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	objs := append(k423PortalReaders(a), extra...)
	dyn := psBuildWatcher(t, a, objs...)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	byName := map[string]*corev1.Secret{}
	for _, u := range psUsers {
		for _, name := range []string{credsecret.SessionName(u), credsecret.StepUpName(u)} {
			byName[name] = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: name},
				Data:       map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-" + u)},
			}
		}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: byName})
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return &su994Env{a: a, dyn: dyn, srvURL: srv.URL, keys: jwtutil.NewStaticKeySource(&priv.PublicKey), priv: priv}
}

func (e *su994Env) token(t *testing.T, groups []string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: psCarol, Groups: groups, Duration: time.Hour, KeyID: su994KeyID, PrivateKey: e.priv,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// serve drives one GET /call through the real auth middleware into the real
// RESTAction handler, and returns the request ctx the handler saw.
func (e *su994Env) serve(t *testing.T, groups []string) (context.Context, *httptest.ResponseRecorder) {
	t.Helper()
	cr := k423ListCR(e.a)
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	defer restore()
	var seen context.Context
	h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: e.srvURL}}
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Context()
		h.ServeHTTP(w, r)
	})
	chain := use.NewChain(middleware.UserConfig(e.keys, psAuthnNS)).Then(terminal)
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Header.Set("Authorization", "Bearer "+e.token(t, groups))
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, req)
	if seen == nil {
		t.Fatalf("the middleware refused the request: code=%d body=%s", rec.Code, psTrunc(rec.Body.String(), 300))
	}
	return seen, rec
}

func su994Groups(stepUp bool) []string {
	g := []string{psGroup}
	if stepUp {
		g = append(g, credsecret.StepUpGroup)
	}
	return g
}

// K — the step-up session mints its own key and class; both cells are stored,
// each with its own representative.
func TestStepUp994_K_StepUpSessionIsItsOwnIdentityClass(t *testing.T) {
	e := su994Setup(t, su994StepUpGuard()...)

	sessCtx, sessRec := e.serve(t, su994Groups(false))
	upCtx, upRec := e.serve(t, su994Groups(true))
	if !psHasSentinel(sessRec.Body.Bytes()) || !psHasSentinel(upRec.Body.Bytes()) {
		t.Fatalf("SETUP: both sessions must read x/y; session=%s stepup=%s",
			psTrunc(sessRec.Body.String(), 200), psTrunc(upRec.Body.String(), 200))
	}
	upUI, err := xcontext.UserInfo(upCtx)
	if err != nil || !credsecret.IsStepUp(upUI.Groups) {
		t.Fatalf("the step-up token's group must reach the request ctx: groups=%v err=%v", upUI.Groups, err)
	}
	sessUI, _ := xcontext.UserInfo(sessCtx)
	if rbac.IdentityClassOf(upUI) == rbac.IdentityClassOf(sessUI) {
		t.Fatalf("with a binding naming %s, the step-up session must be its own RBAC class", credsecret.StepUpGroup)
	}

	kSess, _ := k423RAKey(t, sessCtx)
	kUp, _ := k423RAKey(t, upCtx)
	if kSess == kUp {
		t.Fatalf("the step-up session shares the session's cell key %s… — the group did not fold into the key", kSess[:12])
	}
	entSess, okSess := cache.ResolvedCache().Get(kSess)
	entUp, okUp := cache.ResolvedCache().Get(kUp)
	if !okSess || !okUp || entSess.Inputs == nil || entUp.Inputs == nil {
		t.Fatalf("both sessions' cells must be stored: session=%v stepup=%v", okSess, okUp)
	}
	if !credsecret.IsStepUp(entUp.Inputs.RepresentativeGroups) || credsecret.IsStepUp(entSess.Inputs.RepresentativeGroups) {
		t.Fatalf("each cell records its own representative: stepup groups=%v session groups=%v",
			entUp.Inputs.RepresentativeGroups, entSess.Inputs.RepresentativeGroups)
	}
}

// su994Refresh records one refresher re-resolve: the cell's key and the
// identity the re-resolve ran under.
type su994Refresh struct {
	key string
	ui  jwtutil.UserInfo
}

// R — the background refresher stays on the step-up class.
func TestStepUp994_R_RefresherStaysOnTheStepUpClass(t *testing.T) {
	e := su994Setup(t, su994StepUpGuard()...)
	sessCtx, _ := e.serve(t, su994Groups(false))
	upCtx, _ := e.serve(t, su994Groups(true))
	kSess, _ := k423RAKey(t, sessCtx)
	kUp, upIn := k423RAKey(t, upCtx)
	if kSess == kUp {
		t.Fatal("PRE: the two sessions must hold distinct cells")
	}
	stored, ok := cache.ResolvedCache().Get(kUp)
	if !ok || stored.Inputs == nil {
		t.Fatal("PRE: the step-up cell must be stored")
	}
	upClass := stored.Inputs.Class()
	if upClass != upIn.Class() {
		t.Fatal("PRE: the stored cell's class is the step-up request's class")
	}

	var mu sync.Mutex
	var refreshes []su994Refresh
	i187RefresherEnv(t)
	cr := k423ListCR(e.a)
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		ui, _ := xcontext.UserInfo(ctx)
		mu.Lock()
		refreshes = append(refreshes, su994Refresh{key: cache.ComputeKey(in), ui: ui})
		mu.Unlock()
		ctx = cache.WithBackgroundResolve(ctx)
		return resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}, in, psAuthnNS)
	})
	t.Cleanup(restore)
	saEP := &endpoints.Endpoint{ServerURL: e.srvURL, Token: "tok-sa"}
	saRC := &rest.Config{Host: e.srvURL}
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})
	rctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })

	repickGroup, repickHitter, evicted := representativeRepickForTest(repSourceGroup),
		representativeRepickForTest(repSourceHitter), representativeRepickForTest("evicted")

	const newValue = "after-994-update"
	i431UpdateCM(t, e.dyn, newValue)
	body := func() string {
		if ent, ok := cache.ResolvedCache().Get(kUp); ok {
			return string(ent.RawJSON)
		}
		return "<absent>"
	}
	if !i444Wait(10*time.Second, func() bool { return strings.Contains(body(), newValue) }) {
		t.Fatalf("the step-up cell was never refreshed under its own key; body=%s", psTrunc(body(), 300))
	}

	mu.Lock()
	got := append([]su994Refresh(nil), refreshes...)
	mu.Unlock()
	var upRuns int
	for _, r := range got {
		if r.key != kUp {
			continue
		}
		upRuns++
		if r.ui.Username != psCarol || !credsecret.IsStepUp(r.ui.Groups) {
			t.Fatalf("the step-up cell was refreshed under %q groups=%v; want carol WITH %s",
				r.ui.Username, r.ui.Groups, credsecret.StepUpGroup)
		}
		if c := rbac.IdentityClassOf(r.ui); c != upClass {
			t.Fatalf("the step-up cell was refreshed under another RBAC class (%s, want %s)", c, upClass)
		}
	}
	if upRuns == 0 {
		t.Fatalf("NON-VACUITY: no refresh of the step-up cell was observed (refreshes=%d)", len(got))
	}
	if representativeRepickForTest(repSourceGroup) != repickGroup ||
		representativeRepickForTest(repSourceHitter) != repickHitter ||
		representativeRepickForTest("evicted") != evicted {
		t.Fatal("the refresher re-picked or evicted a representative: the step-up representative drifted out of its own class")
	}
	ent, _ := cache.ResolvedCache().Get(kUp)
	if ent.Inputs == nil || ent.Inputs.RepresentativeUsername != psCarol || !credsecret.IsStepUp(ent.Inputs.RepresentativeGroups) {
		t.Fatalf("the re-Put must keep the step-up representative; got %+v", ent.Inputs)
	}
	if ent.Inputs.Class() != upClass {
		t.Fatal("the refreshed step-up cell changed class")
	}
}

// S — without any binding naming krateo:loa:2 the two sessions are RBAC-equal,
// so they share the class (the #423 sharing invariant). Equal verdicts are
// checked from the real evaluator, not assumed.
func TestStepUp994_S_UnboundStepUpGroupSharesTheRBACClass(t *testing.T) {
	e := su994Setup(t)
	sessCtx, _ := e.serve(t, su994Groups(false))
	upCtx, _ := e.serve(t, su994Groups(true))
	sessUI, _ := xcontext.UserInfo(sessCtx)
	upUI, _ := xcontext.UserInfo(upCtx)
	if !credsecret.IsStepUp(upUI.Groups) {
		t.Fatal("PRE: the step-up group must reach the request ctx")
	}
	for _, verb := range []string{"get", "list", "delete"} {
		v := func(ui jwtutil.UserInfo) bool {
			ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
				Username: ui.Username, Groups: ui.Groups, Verb: verb,
				Resource: "configmaps", Namespace: psTargetNS, Name: psTargetObj, SkipBindingUID: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			return ok
		}
		if v(sessUI) != v(upUI) {
			t.Fatalf("PRE: with no binding naming %s the verdicts must be equal (%s)", credsecret.StepUpGroup, verb)
		}
	}
	if rbac.IdentityClassOf(upUI) != rbac.IdentityClassOf(sessUI) {
		t.Fatal("RBAC-equal sessions hold the same binding set, hence the same class")
	}
	kSess, _ := k423RAKey(t, sessCtx)
	kUp, _ := k423RAKey(t, upCtx)
	if kSess != kUp {
		t.Fatal("RBAC-equal sessions derive the same key (the #423 sharing invariant)")
	}
}
