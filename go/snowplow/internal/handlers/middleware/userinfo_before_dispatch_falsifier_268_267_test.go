//go:build falsifier_268_267

// userinfo_before_dispatch_falsifier_268_267_test.go — Arm E (MIDDLEWARE level,
// PM condition 3b — the REAL guard). The UserInfo-err residual (design §6) is not
// a live leak ONLY because the auth middleware sets xcontext.WithUserInfo on every
// validated request BEFORE the dispatcher attaches the SA creds — so clause (c) of
// internalDispatchServesUnnarrowed (serve un-narrowed when UserInfo is absent) is
// unreachable for a real end-user. The predicate-level arm
// (api.TestUnnarrowedPredicate_ArmE) mints its own UserInfo and therefore CANNOT
// catch the middleware dropping WithUserInfo; THIS arm drives a request through the
// REAL middleware.UserConfig and asserts UserInfo is present on the dispatcher-
// facing ctx.
//
// Reuses the userconfig_test.go harness (testKeys, helperMakeToken, testUsername,
// testAuthnNS) — same package (middleware_test). A cache-HIT clientconfig Secret is
// seeded so the middleware reaches next.ServeHTTP (skipping the out-of-cluster
// InClusterConfig 500), modelled on TestUserConfig_CacheHit_BypassesApiserver.
//
// GUARD arm: GREEN now, MUST STAY GREEN. RED against a middleware regression that
// drops WithUserInfo (or a mutant that routes a validated user with no UserInfo to
// the dispatcher) — which would make clause (c) reachable and re-open the leak.
package middleware_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestUserInfoSetBeforeDispatch_ArmE_Middleware — a validated request through the
// REAL middleware.UserConfig reaches the dispatcher-facing next handler with
// xcontext.UserInfo PRESENT (no error) and carrying the validated username. This
// is the invariant that makes internalDispatchServesUnnarrowed clause (c)
// unreachable for a real authenticated end-user (design §6 / question 4).
func TestUserInfoSetBeforeDispatch_ArmE_Middleware(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetFallthroughCountersForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})

	// Seed the <user>-clientconfig Secret so the middleware takes the cache-HIT
	// path and reaches next.ServeHTTP (no out-of-cluster InClusterConfig 500).
	seedSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAuthnNS, Name: testUsername + "-clientconfig"},
		Data: map[string][]byte{
			"server-url": []byte("https://alice.example/k8s"),
			"token":      []byte("alice-bearer-token-from-cache"),
		},
	}
	cli := fake.NewSimpleClientset(seedSecret)
	restore := cache.SetSecretsClientForTest(cli)
	t.Cleanup(restore)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cache.StartSecretsInformer(ctx, nil, testAuthnNS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}

	// The dispatcher-facing next handler: capture what UserInfo the dispatcher
	// would read. This is exactly what internalDispatchServesUnnarrowed reads.
	var userInfoErr error
	var observedUser jwtutil.UserInfo
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUser, userInfoErr = xcontext.UserInfo(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	tok := helperMakeToken(t, testUsername, 1*time.Hour)
	chain := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS))
	wrapped := chain.Then(terminal)

	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, r)

	t.Logf("Arm E/middleware code=%d userinfo_err=%v observed_user=%q", rec.Code, userInfoErr, observedUser.Username)

	if rec.Code != http.StatusOK {
		t.Fatalf("Arm E/middleware: a validated request must reach the dispatcher (200); got %d body=%s", rec.Code, rec.Body.String())
	}
	// THE GUARD: the dispatcher-facing ctx MUST carry UserInfo. If UserInfo were
	// absent here, internalDispatchServesUnnarrowed clause (c) would serve a real
	// end-user's SA-credentialed read UN-narrowed — the residual becoming a live leak.
	if userInfoErr != nil {
		t.Fatalf("Arm E/middleware — RED: the validated request reached the dispatcher WITHOUT UserInfo (err=%v) — clause (c) of internalDispatchServesUnnarrowed becomes reachable for a real end-user (leak). The middleware must set WithUserInfo before next.ServeHTTP.", userInfoErr)
	}
	if observedUser.Username != testUsername {
		t.Fatalf("Arm E/middleware: dispatcher-facing UserInfo.Username = %q; want %q", observedUser.Username, testUsername)
	}
	t.Logf("Arm E/middleware GREEN: the validated request reaches the dispatcher with UserInfo set (Username=%q) — clause (c) unreachable for a real end-user.", observedUser.Username)
}
