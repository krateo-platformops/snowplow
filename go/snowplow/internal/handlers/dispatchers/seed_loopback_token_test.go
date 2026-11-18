// seed_loopback_token_test.go — #57 (C-a) and #1530: the prewarm seed's authn
// token SOURCE and the degrade-not-fail posture.
//
// The prewarm context carries the source of the authn JWT (via the wired
// provider = authn.Client.Token), never the token itself: a bearer-carrying stage
// asks it when it needs it (restactions/api stageBearer). C-a: a failed ask MUST
// degrade (WARN + expvar counter, the stage token-less), never fail the warmup.
// #1530: building the context asks nothing, so it never waits for authn.

package dispatchers

import (
	"context"
	"errors"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/loopbacktoken"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func testSAEndpoint() endpoints.Endpoint {
	return endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc"}
}

// TestPhase1RootsRead_NeverWaitsForAnUnavailableTokenSource: Phase 1's eager
// read of the frontend ConfigMap, under the Phase 1 SA context, with authn never
// answering. It reads INIT at once and asks nothing (#1530: before, the context
// it ran under had already spent its whole deadline on the token, and the read
// failed "context deadline exceeded" without reaching the apiserver).
func TestPhase1RootsRead_NeverWaitsForAnUnavailableTokenSource(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	asked := make(chan struct{}, 64)
	SetSeedLoopbackTokenProvider(func(ctx context.Context) (string, error) {
		asked <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	const init = "/call?resource=navmenus&apiVersion=widgets.templates.krateo.io/v1beta1&name=sidebar-nav-menu&namespace=krateo-system"
	cm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": "krateo-system", "name": frontendConfigConfigMapName()},
		"data":     map[string]any{frontendConfigDataKey: `{"api":{"INIT":"` + init + `"}}`},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(k8sruntime.NewScheme(), cm)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	cfg, err := readFrontendConfig(withPhase1SAContext(ctx, testSAEndpoint(), nil), dyn, "krateo-system",
		frontendConfigConfigMapName())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("the eager read failed after %v: %v", elapsed, err)
	}
	if cfg.API.Init != init {
		t.Fatalf("INIT: want %q, got %q", init, cfg.API.Init)
	}
	if len(asked) != 0 || elapsed > time.Second {
		t.Fatalf("the eager read asked authn %d time(s) and took %v: it waited on the token source", len(asked), elapsed)
	}
}

func TestSeedTokenSource_InstalledNotFetched(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	calls := 0
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		calls++
		return "authn-jwt-xyz", nil
	})

	got := installSeedLoopbackTokenSource(context.Background())
	if calls != 0 {
		t.Fatalf("installing the source asked authn %d time(s): the context must carry the source, not a token "+
			"fetched while it is built (#1530)", calls)
	}
	if tok, _ := xcontext.AccessToken(got); tok != "" {
		t.Fatalf("the seed ctx carries an access token %q: it must carry only the source", tok)
	}
	src := loopbacktoken.SourceFrom(got)
	if src == nil {
		t.Fatal("the seed ctx carries no loopback token source")
	}
	tok, err := src(context.Background())
	if err != nil || tok != "authn-jwt-xyz" || calls != 1 {
		t.Fatalf("asking the source: got (%q, %v) after %d call(s), want the provider's JWT from exactly one", tok, err, calls)
	}
}

func TestSeedTokenSource_DegradesOnProviderError(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	before := SeedLoopbackTokenErrTotal()
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		return "", errors.New("authn returned 403: no allowlist CR")
	})
	src := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))
	tok, err := src(context.Background())
	if err == nil || tok != "" {
		t.Fatalf("a provider error must reach the asking stage as an error with no token; got (%q, %v)", tok, err)
	}
	if SeedLoopbackTokenErrTotal() != before+1 {
		t.Fatalf("a failed ask must bump SeedLoopbackTokenErrTotal (C-a observability); before=%d after=%d",
			before, SeedLoopbackTokenErrTotal())
	}
}

func TestSeedTokenSource_DegradesOnEmptyToken(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	before := SeedLoopbackTokenErrTotal()
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		return "", nil // no error but empty — still a degrade (counted)
	})
	tok, err := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(context.Background())
	if err == nil || tok != "" {
		t.Fatalf("an empty token must reach the stage as an error, never an empty bearer; got (%q, %v)", tok, err)
	}
	if SeedLoopbackTokenErrTotal() != before+1 {
		t.Fatalf("an empty token must bump the degrade counter; before=%d after=%d", before, SeedLoopbackTokenErrTotal())
	}
}

func TestSeedTokenSource_NoProviderIsPreR1NoOp(t *testing.T) {
	SetSeedLoopbackTokenProvider(nil) // authn not configured
	before := SeedLoopbackTokenErrTotal()
	ctx := context.Background()
	got := installSeedLoopbackTokenSource(ctx)
	if got != ctx || loopbacktoken.SourceFrom(got) != nil {
		t.Fatal("with no provider wired the seed ctx must be unchanged (pre-#57 token-less): no source")
	}
	if SeedLoopbackTokenErrTotal() != before {
		t.Fatalf("no-provider is the configured-off path, not an error — counter must not tick; before=%d after=%d",
			before, SeedLoopbackTokenErrTotal())
	}
}

// TestSeedTokenSource_EachNeedAsksAgain: authn unavailable for the first three
// asks, then up. Each ask is one attempt (no retry loop: a wait belongs only to
// the stage that needs the token, bounded by its own context), the failed ones
// counted; the fourth stage that needs it gets it (mutation: caching a failure
// leaves the fourth token-less).
func TestSeedTokenSource_EachNeedAsksAgain(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	calls := 0
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		calls++
		if calls <= 3 {
			return "", errors.New("dial tcp: connection refused")
		}
		return "authn-jwt-up", nil
	})
	before := SeedLoopbackTokenErrTotal()
	src := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))
	for i := 1; i <= 3; i++ {
		if tok, err := src(context.Background()); err == nil || tok != "" || calls != i {
			t.Fatalf("ask %d: got (%q, %v) after %d provider call(s), want one failed attempt", i, tok, err, calls)
		}
	}
	if tok, err := src(context.Background()); err != nil || tok != "authn-jwt-up" || calls != 4 {
		t.Fatalf("ask 4 (authn up): got (%q, %v) after %d call(s), want the token", tok, err, calls)
	}
	if SeedLoopbackTokenErrTotal() != before+3 {
		t.Fatalf("3 failed asks must count 3; before=%d after=%d", before, SeedLoopbackTokenErrTotal())
	}
}

// TestWithPhase1SAContext_NeverWaitsForAuthn is #1530's regression arm. authn
// never answers (the provider blocks until its context ends); building the
// Phase 1 SA context must return at once, ask nothing, and hand back a context
// that is not done. Before #1530 it retried the exchange until this context's
// deadline and returned it expired (mutation: asking the provider while the
// context is built turns this RED).
func TestWithPhase1SAContext_NeverWaitsForAuthn(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	asked := make(chan struct{}, 64)
	SetSeedLoopbackTokenProvider(func(ctx context.Context) (string, error) {
		asked <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	got := withPhase1SAContext(ctx, testSAEndpoint(), nil)
	elapsed := time.Since(start)
	if len(asked) != 0 {
		t.Fatalf("building the Phase 1 SA context asked authn %d time(s)", len(asked))
	}
	if got.Err() != nil {
		t.Fatalf("the Phase 1 SA context came back done (%v) after %v", got.Err(), elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("building the Phase 1 SA context took %v: it waited for authn", elapsed)
	}
	if loopbacktoken.SourceFrom(got) == nil {
		t.Fatal("the Phase 1 SA context carries no loopback token source")
	}
}
