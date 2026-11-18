// seed_loopback_token_test.go — #57 (C-a), #1530 and #1531's review: the prewarm
// seed's authn token SOURCE (one shared acquisition) and the degrade-not-fail posture.
//
// The prewarm context carries the source of the authn JWT (via the wired
// provider = authn.Client.Token), never the token itself: a bearer-carrying stage
// asks it when it needs it (restactions/api stageBearer). C-a: a failed ask MUST
// degrade (WARN + expvar counter, the stage token-less), never fail the warmup.
// #1530: building the context asks nothing, so it never waits for authn.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/authn"
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

// shortCtx is a stage's own context: a stage waits for the token within it, never longer.
func shortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestSeedTokenSource_DegradesOnProviderError(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	before := SeedLoopbackTokenErrTotal()
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		return "", errors.New("authn returned 403: refused")
	})
	var tok string
	var err error
	within(t, "a stage asking within 400 ms", 1400*time.Millisecond, func() {
		tok, err = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 400*time.Millisecond))
	})
	if err == nil || tok != "" {
		t.Fatalf("a failing authn must leave the asking stage token-less, with an error; got (%q, %v)", tok, err)
	}
	if SeedLoopbackTokenErrTotal() <= before {
		t.Fatalf("a failed exchange must bump SeedLoopbackTokenErrTotal (C-a observability); before=%d after=%d",
			before, SeedLoopbackTokenErrTotal())
	}
}

func TestSeedTokenSource_DegradesOnEmptyToken(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	before := SeedLoopbackTokenErrTotal()
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		return "", nil // no error but empty — still a failure (counted)
	})
	var tok string
	var err error
	within(t, "a stage asking within 400 ms", 1400*time.Millisecond, func() {
		tok, err = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 400*time.Millisecond))
	})
	if err == nil || tok != "" {
		t.Fatalf("an empty token must reach the stage as an error, never an empty bearer; got (%q, %v)", tok, err)
	}
	if SeedLoopbackTokenErrTotal() <= before {
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

// TestSeedTokenSource_RetriesThenAcquires is shape A's RetriesThenAcquires on the shared acquisition (#1531 review
// B2): authn refuses its first three exchanges, then answers. One stage asking within a 10 s context gets the token
// after the backoff (0, 250, 500, 1000 ms), the three failures counted (mutation: one try per ask, #1531's first
// shape, leaves it token-less).
func TestSeedTokenSource_RetriesThenAcquires(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	var calls atomic.Int64
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		if calls.Add(1) <= 3 {
			return "", errors.New("dial tcp: connection refused")
		}
		return "authn-jwt-up", nil
	})
	before := SeedLoopbackTokenErrTotal()
	src := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))
	start := time.Now()
	tok, err := src(shortCtx(t, 10*time.Second))
	if err != nil || tok != "authn-jwt-up" || calls.Load() != 4 {
		t.Fatalf("got (%q, %v) after %d exchange(s) in %v, want the token on the 4th", tok, err, calls.Load(), time.Since(start))
	}
	if SeedLoopbackTokenErrTotal() != before+3 {
		t.Fatalf("3 failed exchanges must count 3; before=%d after=%d", before, SeedLoopbackTokenErrTotal())
	}
}

// TestSeedTokenSource_CtxBoundedDegrades is shape A's CtxBoundedDegrades: authn never answers well; a stage asking
// within 600 ms returns token-less at its own deadline, never later, and the failures are counted.
func TestSeedTokenSource_CtxBoundedDegrades(t *testing.T) {
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(func(context.Context) (string, error) {
		return "", errors.New("dial tcp: connection refused (forever)")
	})
	before := SeedLoopbackTokenErrTotal()
	start := time.Now()
	var tok string
	var err error
	within(t, "a stage asking within 600 ms", 1600*time.Millisecond, func() {
		tok, err = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 600*time.Millisecond))
	})
	elapsed := time.Since(start)
	if tok != "" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got (%q, %v), want token-less at the stage's own deadline", tok, err)
	}
	if elapsed < 550*time.Millisecond || elapsed > time.Second {
		t.Fatalf("the stage returned after %v, want at its own 600 ms deadline", elapsed)
	}
	if SeedLoopbackTokenErrTotal() <= before {
		t.Fatalf("the failed exchanges were not counted (%d -> %d)", before, SeedLoopbackTokenErrTotal())
	}
}

// TestSeedTokenSource_AMissingTokenFileIsNotRetried: no projected authn-audience token file (the sandbox's shape): no
// wait can fix it, so the first ask ends at once and the failure is memoised for the process (#1531 review N5): five
// bearer needs, one exchange attempt, one WARN, every stage token-less at once (mutations: retrying it makes the stage
// wait its whole context; not memoising it logs one WARN per need).
func TestSeedTokenSource_AMissingTokenFileIsNotRetried(t *testing.T) {
	logs := captureLogs(t)
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New("http://authn.invalid:8082", filepath.Join(t.TempDir(), "absent")).Token)
	before := SeedLoopbackTokenErrTotal()
	for i := 1; i <= 5; i++ {
		start := time.Now()
		var tok string
		var err error
		within(t, fmt.Sprintf("bearer need %d", i), time.Second, func() {
			tok, err = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 5*time.Second))
		})
		if tok != "" || !errors.Is(err, fs.ErrNotExist) || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("need %d: got (%q, %v) after %v, want token-less at once with the missing file's error", i, tok, err,
				time.Since(start))
		}
	}
	if n := strings.Count(logs(), "prewarm.seed_loopback_token_unavailable"); n != 1 {
		t.Fatalf("five bearer needs logged %d WARN(s), want one per boot", n)
	}
	if got := SeedLoopbackTokenErrTotal() - before; got != 1 {
		t.Fatalf("five bearer needs counted %d failed exchange(s), want 1", got)
	}
}

// stand-in authn: answers /serviceaccount/login as `answer` says, counting the exchanges
func standInAuthn(t *testing.T, answer func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *atomic.Int64, string) {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		answer(w, r)
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-sa"), 0o600); err != nil {
		t.Fatal(err)
	}
	return srv, &n, tokenFile
}

func testJWT(exp time.Time) string {
	return "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".sig"
}

// TestSeedTokenSource_AFailingAuthnIsNotHitOncePerAsk (#1531 review, probe B): authn answers 503; 64 stages ask at
// once, each within 1.2 s. They share one acquisition: the exchanges follow its backoff (at 0, ~0.25, ~0.75 s), not
// the asks (64 at #1531's first shape); every stage returns token-less at its own deadline.
func TestSeedTokenSource_AFailingAuthnIsNotHitOncePerAsk(t *testing.T) {
	srv, n, tokenFile := standInAuthn(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	})
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New(srv.URL, tokenFile).Token)
	var wg sync.WaitGroup
	var got atomic.Int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
			defer cancel()
			if tok, _ := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(ctx))(ctx); tok != "" {
				got.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got.Load() != 0 {
		t.Fatalf("%d stage(s) got a token from a failing authn", got.Load())
	}
	if e := n.Load(); e < 1 || e > 4 {
		t.Fatalf("64 asks made %d exchange(s), want the shared acquisition's backoff (1 to 4 in 1.2 s), not one per ask", e)
	}
}

// TestSeedTokenSource_AHungAuthnHoldsNoStagePastItsDeadline (#1531 review, probe C): authn never answers; stage A
// asks within 2 s, stage B 50 ms later within 200 ms. B returns at its own deadline (#1531's first shape: 1.95 s,
// waiting on authn.Client's mutex behind A's exchange).
func TestSeedTokenSource_AHungAuthnHoldsNoStagePastItsDeadline(t *testing.T) {
	release := make(chan struct{})
	srv, _, tokenFile := standInAuthn(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	defer close(release)
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New(srv.URL, tokenFile).Token)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(ctx))(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	var tok string
	var err error
	within(t, "stage B (200 ms)", 1200*time.Millisecond, func() {
		tok, err = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(ctx))(ctx)
	})
	elapsed := time.Since(start)
	if tok != "" || !errors.Is(err, context.DeadlineExceeded) || elapsed > 450*time.Millisecond {
		t.Fatalf("stage B got (%q, %v) after %v, want token-less at its own 200 ms deadline", tok, err, elapsed)
	}
}

// TestSeedTokenSource_TheAcquisitionOutlivesAShortFirstAsker (#1531 review N4, probe F): the exchange takes 500 ms;
// a stage asking within 200 ms starts it, a stage asking within 3 s joins 50 ms later. The first leaves at its
// deadline, the acquisition goes on for the second, which gets the token, in one exchange (mutation Q8: the
// acquisition bound to its first asker's context ends with it, and the second is token-less).
func TestSeedTokenSource_TheAcquisitionOutlivesAShortFirstAsker(t *testing.T) {
	var calls atomic.Int64
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(func(ctx context.Context) (string, error) {
		calls.Add(1)
		select {
		case <-time.After(500 * time.Millisecond):
			return "authn-jwt-slow", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	src := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))
	first := make(chan error, 1)
	go func() {
		_, err := src(shortCtx(t, 200*time.Millisecond))
		first <- err
	}()
	time.Sleep(50 * time.Millisecond)
	var tok string
	var err error
	within(t, "the 3 s stage", 4*time.Second, func() { tok, err = src(shortCtx(t, 3*time.Second)) })
	if tok != "authn-jwt-slow" || err != nil || calls.Load() != 1 {
		t.Fatalf("the 3 s stage got (%q, %v) after %d exchange(s), want the token from the one exchange", tok, err, calls.Load())
	}
	if e := <-first; !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("the 200 ms stage: %v, want its own deadline", e)
	}
}

// TestSeedTokenSource_AuthnLateBoot_TheBearerStagesGetTheTokenWithinTheBoot is the rt8rv shape (#1531 review B2):
// the frontend ConfigMap is there from the start and authn starts serving 1.2 s after snowplow. The walk's
// bearer-carrying stages (asking under the Phase 1 SA context, within the walk's own budget) get the token once authn
// serves, within the same boot; building the context waited for nothing (mutation: one try per ask leaves them
// token-less for the boot).
func TestSeedTokenSource_AuthnLateBoot_TheBearerStagesGetTheTokenWithinTheBoot(t *testing.T) {
	up := time.Now().Add(1200 * time.Millisecond)
	jwt := testJWT(time.Now().Add(time.Hour))
	srv, n, tokenFile := standInAuthn(t, func(w http.ResponseWriter, _ *http.Request) {
		if time.Now().Before(up) {
			http.Error(w, "not serving yet", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"accessToken":%q}`, jwt)
	})
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New(srv.URL, tokenFile).Token)
	walk := shortCtx(t, 25*time.Second) // the sandbox's PHASE1_TIMEOUT; production's is 900 s
	start := time.Now()
	rctx := withPhase1SAContext(walk, testSAEndpoint(), nil)
	if built := time.Since(start); built > 100*time.Millisecond {
		t.Fatalf("building the Phase 1 SA context took %v", built)
	}
	var wg sync.WaitGroup
	toks := make([]string, 8)
	for i := range toks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			toks[i], _ = loopbacktoken.SourceFrom(rctx)(rctx)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for i, tok := range toks {
		if tok != jwt {
			t.Fatalf("bearer stage %d got %q after %v: no token within the boot", i, tok, elapsed)
		}
	}
	if elapsed < 1100*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("the stages got the token after %v, want soon after authn started serving (1.2 s)", elapsed)
	}
	t.Logf("authn late by 1.2 s: 8 bearer stages got the token after %v, %d exchange(s)", elapsed, n.Load())
}

// TestSeedTokenSource_TheTokenIsNeverLogged (#1531 review N1, N2): a healthy acquisition, then a refusing authn whose
// error body echoes BOTH the projected SA token snowplow presented (read from its token file) and an issued JWT,
// with every log line captured at debug. Neither the issued token nor either echoed credential is in the returned
// error, the WARN or any log line; the status class's diagnostic is (mutation: the body back in authn's error).
func TestSeedTokenSource_TheTokenIsNeverLogged(t *testing.T) {
	logs := captureLogs(t)
	jwt := testJWT(time.Now().Add(time.Hour))
	ok, _, okFile := standInAuthn(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"accessToken":%q}`, jwt)
	})
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New(ok.URL, okFile).Token)
	if tok, _ := loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 5*time.Second)); tok != jwt {
		t.Fatalf("no token from a healthy authn: %q", tok)
	}
	var presented string
	bad, _, badFile := standInAuthn(t, func(w http.ResponseWriter, r *http.Request) {
		presented = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		http.Error(w, "no allowlist CR for "+presented+"; last issued "+jwt, http.StatusForbidden)
	})
	saToken, err := os.ReadFile(badFile)
	if err != nil {
		t.Fatal(err)
	}
	SetSeedLoopbackTokenProvider(authn.New(bad.URL, badFile).Token)
	var askErr error
	within(t, "a stage asking within 400 ms", 1400*time.Millisecond, func() {
		_, askErr = loopbacktoken.SourceFrom(installSeedLoopbackTokenSource(context.Background()))(shortCtx(t, 400*time.Millisecond))
	})
	out := logs()
	if presented != string(saToken) {
		t.Fatalf("premise: the stand-in authn saw %q presented, want the token file's %q", presented, saToken)
	}
	if !strings.Contains(out, "prewarm.seed_loopback_token_unavailable") ||
		!strings.Contains(out, "refused the service account (not authenticated, or not on authn's allowlist)") {
		t.Fatalf("the refusal's WARN, with its status-class diagnostic, is not in the log:\n%s", out)
	}
	for _, secret := range []string{jwt, string(saToken)} {
		if strings.Contains(out, secret) {
			t.Fatalf("a credential reached the log (%q):\n%s", secret, out)
		}
	}
	if askErr == nil {
		t.Fatal("the refused stage got no error")
	}
}

// within runs f and fails the test, by name, when it has not returned by limit: a regression that blocks (an asker
// waiting on the acquisition instead of its own context, a context build that asks authn) fails in about a second
// with a message, never after the test binary's timeout.
func within(t *testing.T, what string, limit time.Duration, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("%s did not return within %v: it waited past its own context", what, limit)
	}
}

// captureLogs sends every log line, at debug, to a buffer for the test's life and returns its reader.
func captureLogs(t *testing.T) func() string {
	t.Helper()
	var buf bytes.Buffer
	var mu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(lockedWriter{&mu, &buf}, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestWithPhase1SAContext_NeverWaitsForAuthn is #1530's regression arm. authn
// never answers (the provider blocks until its context ends); building the
// Phase 1 SA context must return at once, ask nothing, and hand back a context
// that is not done. Before #1530 it retried the exchange until this context's
// deadline and returned it expired (mutation: asking the provider while the
// context is built fails this by name within a second).
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
	var got context.Context
	within(t, "building the Phase 1 SA context", time.Second, func() { got = withPhase1SAContext(ctx, testSAEndpoint(), nil) })
	elapsed := time.Since(start)
	if len(asked) != 0 {
		t.Fatalf("building the Phase 1 SA context asked authn %d time(s)", len(asked))
	}
	if got.Err() != nil {
		t.Fatalf("the Phase 1 SA context came back done (%v) after %v", got.Err(), elapsed)
	}
	if loopbacktoken.SourceFrom(got) == nil {
		t.Fatal("the Phase 1 SA context carries no loopback token source")
	}
}
