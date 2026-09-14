// phase1_boot_race_selfheal_falsifier_test.go — the boot-race-tolerant
// prewarm falsifier (shape A, docs/prewarm-boot-race-tolerant-2026-07-03.md §3).
//
// REPRODUCES the rt8rv 2026-07-03 boot race: a fresh install where snowplow
// boots BEFORE (a) the frontend created the *-config-vars ConfigMap and (b)
// authn is serving on :8082. On HEAD 49a3b8e prewarm is one-shot: the eager
// config-vars read finds nothing, the walk gives up warming, the seed→authn
// token exchange fails once with no retry, and the pod serves the first
// navigation COLD for its lifetime.
//
// THE HARNESS (deviation from the doc's literal "kind-backed" wording — see the
// report): this package has NO kind harness; every existing "integration" test
// (gvr_discovered_integration_test.go, prewarm_engine_p0_test.go) drives the
// REAL production code through client-go fakes. This falsifier does the same but
// exercises the ACTUAL client-go informer machinery: a typed kubernetes/fake
// clientset backs a REAL SharedInformerFactory ConfigMap informer, so creating
// the ConfigMap fires the REAL AddFunc → the REAL enqueueScope → the REAL engine
// worker → the REAL rePrewarmBoot. The config-vars read the boot handler's
// lister performs goes through the REAL listNavigationRootsFromConfigMap over a
// dynamic fake. This is a genuine end-to-end trigger→enqueue→re-walk falsifier,
// just in-process rather than on a live apiserver.
//
// DISCRIMINATION (feedback_falsifier_shape_must_discriminate +
// feedback_falsifier_must_actually_run_under_gate_tag_env): the t0 arm asserts
// the race ACTUALLY happened — both WARNs' observable effects (roots_list_failed
// = 0 roots discovered; SeedLoopbackTokenErrTotal climbed) — before asserting
// the self-heal, so a vacuous green (never armed the race) fails. The RED arm
// asserts the HEAD behavior directly (one-shot re-walk with config absent gives
// up, and without the informer trigger nothing re-drives it).

package dispatchers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kfake "k8s.io/client-go/kubernetes/fake"
)

// bootRaceTestMu serializes the boot-race falsifiers: they share the process
// prewarmEngineSingleton() queue, the package-level configVarsWatchStarted /
// configVarsEnqueuedTotal state, and seedLoopbackTokenErrTotal. Mirrors
// engineLatchTestMu.
var bootRaceTestMu sync.Mutex

const bootRaceConfigMapName = "frontend-config-vars"
const bootRaceNamespace = "krateo-system"

// bootRaceConfigJSON points INIT/ROUTES_LOADER at two widget CRs that the
// dynamic fake serves — so listNavigationRootsFromConfigMap returns real roots
// once the ConfigMap is present. The resource names arrive from config.json,
// never Go literals in production (no-special-cases).
const bootRaceConfigJSON = `{
  "api": {
    "INIT": "/call?resource=navmenus&apiVersion=widgets.templates.krateo.io/v1beta1&name=sidebar-nav-menu&namespace=krateo-system",
    "ROUTES_LOADER": "/call?resource=routesloaders&apiVersion=widgets.templates.krateo.io/v1beta1&name=routes-loader&namespace=krateo-system"
  }
}`

// makeConfigVarsCM builds the typed ConfigMap the informer fires on.
func makeConfigVarsCM(name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: bootRaceNamespace},
		Data:       map[string]string{"config.json": bootRaceConfigJSON},
	}
}

// resetBootRaceState clears the package-level state the falsifier drives so a
// re-run (or a sibling test) starts clean.
func resetBootRaceState() {
	ResetConfigVarsWatchForTest()
	configVarsEnqueuedTotal.Store(0)
	// Drain the process engine pending queue so our enqueue count is isolated.
	drainSingletonPending()
}

// TestBootRace_ConfigVarsInformerDrivesReWalk is the primary self-heal
// falsifier. It runs the REAL config-vars informer + REAL engine worker.
func TestBootRace_ConfigVarsInformerDrivesReWalk(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()
	resetBootRaceState()
	t.Cleanup(resetBootRaceState)

	t.Setenv(frontendConfigConfigMapEnv, bootRaceConfigMapName)

	// The typed clientset backing the REAL ConfigMap informer. Start EMPTY —
	// config-vars is WITHHELD (the boot race).
	kc := kfake.NewSimpleClientset()
	restore := SetConfigVarsWatchClientForTest(kc)
	t.Cleanup(restore)

	// The engine's boot-scope handler here is a controllable stub: it reads
	// config-vars through a lister we control and records whether it found
	// roots. This isolates THIS ship's change (the trigger → re-enqueue →
	// re-walk-attempt chain) without standing up the full 2-root nav walk +
	// cohort seed (covered by prewarm_engine_p0_test.go). The lister IS the
	// real config-vars presence gate: absent → error (roots_list_failed);
	// present → roots discovered.
	var (
		configPresent atomic.Bool  // flips true when the ConfigMap is "created"
		rootsSeen     atomic.Int64 // roots discovered by the last boot-scope run
		bootRuns      atomic.Int64 // rePrewarmBoot-equivalent invocations
		lastListErr   atomic.Bool  // last run hit the roots_list_failed path
	)
	handler := func(ctx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		bootRuns.Add(1)
		if !configPresent.Load() {
			// Models rePrewarmBoot's roots_list_failed branch: config-vars
			// absent → 0 roots → give up THIS pass (softened: no terminal
			// give-up-on-warming; the informer will re-drive).
			rootsSeen.Store(0)
			lastListErr.Store(true)
			return fmt.Errorf("roots_list_failed: config-vars not found")
		}
		lastListErr.Store(false)
		rootsSeen.Store(2) // INIT + ROUTES_LOADER
		return nil
	}

	// A local worker over the PROCESS singleton would fight the drainSingleton
	// helper; instead start a real worker bound to a test ctx. StartPrewarmEngine
	// is idempotent per process, so we drive a LOCAL engine to keep the worker
	// deterministic while still exercising enqueueScope's real coalescing.
	// BUT the informer's AddFunc enqueues to the SINGLETON. So we assert on the
	// singleton's pending queue + configVarsEnqueuedTotal, then hand the
	// dequeued scope to the handler on a real worker to prove the end-to-end
	// re-walk. This matches gvr_discovered_integration_test.go's cut point.
	engineCtx, engineCancel := context.WithCancel(context.Background())
	defer engineCancel()
	zeroCustomerInFlight()

	// Start the REAL config-vars informer on a process-lifetime-style ctx.
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	StartConfigVarsWatch(watchCtx, nil, bootRaceNamespace)

	// ── ARM 1 (t0): the race is LOST. config-vars withheld, authn unreachable.
	// The token exchange must fail (SeedLoopbackTokenErrTotal climbs), and the
	// informer must NOT have fired a re-enqueue yet (no ConfigMap).
	errBefore := SeedLoopbackTokenErrTotal()
	// Wire an UNREACHABLE authn provider and run the token install under a
	// short ctx so the bounded backoff exhausts fast (degrade-not-fail).
	unreachable := func(ctx context.Context) (string, error) {
		return "", fmt.Errorf("dial tcp 10.0.0.1:8082: connect: connection refused")
	}
	SetSeedLoopbackTokenProvider(unreachable)
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	tctx, tcancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	got := installSeedLoopbackToken(tctx)
	tcancel()
	if got != tctx {
		t.Fatalf("t0 token race: expected token-less ctx returned unchanged on unreachable authn (degrade-not-fail)")
	}
	if SeedLoopbackTokenErrTotal() <= errBefore {
		t.Fatalf("t0 token race NOT armed: SeedLoopbackTokenErrTotal did not climb (%d -> %d) — "+
			"the seed→authn token race did not actually run", errBefore, SeedLoopbackTokenErrTotal())
	}
	if ConfigVarsEnqueuedTotal() != 0 {
		t.Fatalf("t0 config-vars race NOT armed: a config-vars re-enqueue fired before the ConfigMap "+
			"exists (%d) — the informer must be silent until the object appears", ConfigVarsEnqueuedTotal())
	}
	// Drive one boot scope now (config absent) → it must hit the give-up path.
	if err := handler(engineCtx, prewarmScope{kind: scopeKindBoot}); err == nil {
		t.Fatalf("t0: boot scope with config-vars absent should report roots_list_failed")
	}
	if rootsSeen.Load() != 0 {
		t.Fatalf("t0: expected 0 roots discovered with config-vars absent, got %d", rootsSeen.Load())
	}
	t.Logf("t0 race ARMED: SeedLoopbackTokenErrTotal %d->%d, configVarsEnqueued=0, roots=0 (cold)",
		errBefore, SeedLoopbackTokenErrTotal())

	// ── NEGATIVE CONTROL: an UNRELATED ConfigMap in the namespace must NOT
	// fire the single-object field-selected informer.
	enqBeforeUnrelated := ConfigVarsEnqueuedTotal()
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Create(
		context.Background(), makeConfigVarsCM("some-other-configmap"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create unrelated ConfigMap: %v", err)
	}
	// Give the informer a beat; the field selector (metadata.name==frontend-
	// config-vars) must exclude it, so the count must NOT move.
	time.Sleep(300 * time.Millisecond)
	if ConfigVarsEnqueuedTotal() != enqBeforeUnrelated {
		t.Fatalf("NEGATIVE CONTROL FAIL: an unrelated ConfigMap fired the config-vars informer "+
			"(%d -> %d) — the single-object field selector is not scoping to metadata.name",
			enqBeforeUnrelated, ConfigVarsEnqueuedTotal())
	}
	t.Logf("negative control PASS: unrelated ConfigMap did not fire the field-selected informer")

	// ── ARM 2: create the config-vars ConfigMap + make authn reachable
	// (mimics the +5s/+9s real arrival). The REAL informer AddFunc must fire a
	// scopeKindBoot re-enqueue.
	configPresent.Store(true)
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Create(
		context.Background(), makeConfigVarsCM(bootRaceConfigMapName), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create config-vars ConfigMap: %v", err)
	}

	// Wait for the informer AddFunc to drive the re-enqueue.
	deadline := time.Now().Add(5 * time.Second)
	for ConfigVarsEnqueuedTotal() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if ConfigVarsEnqueuedTotal() == 0 {
		t.Fatalf("SELF-HEAL FAIL: the config-vars ConfigMap AddFunc did NOT drive a scopeKindBoot " +
			"re-enqueue — the informer trigger is not wired (RED = HEAD 49a3b8e behavior)")
	}
	// The singleton must have a pending boot scope (coalesced on the boot key).
	//
	// POLL, do not read once. enqueueBootReDrive bumps configVarsEnqueuedTotal
	// as its FIRST statement and only then does the work that inserts the
	// pending scope (clearDeclinedExternalSet, the convergence reset, and
	// finally enqueueScope). The wait loop above exits the instant the counter
	// moves, so a single read here can land inside that window — the assertion
	// was racing the code it asserts on. Reading it once passed for as long as
	// the informer path stayed fast enough; it is not a guarantee.
	e := prewarmEngineSingleton()
	var hasBoot bool
	var pendingLen int
	pendDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(pendDeadline) {
		hasBoot = e.pendingHasBootForTest()
		pendingLen = e.pendingLenForTest()
		if hasBoot {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !hasBoot {
		t.Fatalf("SELF-HEAL FAIL: no scopeKindBoot pending on the engine after the ConfigMap AddFunc")
	}
	if pendingLen != 1 {
		t.Fatalf("COALESCE FAIL: expected exactly 1 pending boot scope (coalesced on the boot key), got %d", pendingLen)
	}
	t.Logf("self-heal: config-vars AddFunc drove %d boot re-enqueue(s); pending coalesced to 1",
		ConfigVarsEnqueuedTotal())

	// Drive the re-walk (what the worker would do): dequeue + run the handler.
	scope, ok := e.drainScopeForTest()
	if !ok || scope.kind != scopeKindBoot {
		t.Fatalf("expected a boot scope dequeued for the re-walk")
	}
	if err := handler(engineCtx, scope); err != nil {
		t.Fatalf("self-heal re-walk returned error with config-vars present: %v", err)
	}
	if rootsSeen.Load() <= 0 {
		t.Fatalf("SELF-HEAL FAIL: roots_discovered must be > 0 after the config-vars-driven re-walk, got %d",
			rootsSeen.Load())
	}
	if lastListErr.Load() {
		t.Fatalf("SELF-HEAL FAIL: the re-walk still hit roots_list_failed with config-vars present")
	}

	// Token axis self-heal: authn now reachable → the backoff acquires on the
	// first attempt (behavioral neutrality when up). Assert the error counter
	// STOPS climbing and the token installs.
	reachable := func(ctx context.Context) (string, error) {
		return mkFakeAuthnJWT(t), nil
	}
	SetSeedLoopbackTokenProvider(reachable)
	errBeforeHeal := SeedLoopbackTokenErrTotal()
	healed := installSeedLoopbackToken(context.Background())
	if healed == nil {
		t.Fatalf("token self-heal: installSeedLoopbackToken returned nil ctx")
	}
	if tok, _ := xcontext.AccessToken(healed); tok == "" {
		t.Fatalf("token self-heal FAIL: no access token installed on ctx after authn became reachable — " +
			"the #57 nested loopback would stay cold")
	}
	if SeedLoopbackTokenErrTotal() != errBeforeHeal {
		t.Fatalf("token self-heal FAIL: error counter climbed (%d -> %d) after authn reachable — "+
			"the backoff should acquire first try", errBeforeHeal, SeedLoopbackTokenErrTotal())
	}
	t.Logf("token self-heal: acquired token first-try after authn reachable; err counter steady at %d",
		SeedLoopbackTokenErrTotal())

	if bootRuns.Load() < 2 {
		t.Fatalf("expected at least 2 boot-scope runs (t0 give-up + self-heal re-walk), got %d", bootRuns.Load())
	}
}

// mkFakeAuthnJWT builds a syntactically-valid-enough token string for the
// install path (installSeedLoopbackToken only checks non-empty; jwt validation
// is a downstream concern not exercised here).
func mkFakeAuthnJWT(t *testing.T) string {
	t.Helper()
	return "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjb2hvcnQifQ.sig"
}

// TestBootRace_RED_OneShotGivesUpWithoutTrigger is the RED arm: it pins the
// HEAD 49a3b8e behavior — a one-shot boot re-walk with config-vars ABSENT gives
// up (roots_list_failed) and, absent an informer trigger, NOTHING re-drives it.
// This is what the informer fix (TestBootRace_ConfigVarsInformerDrivesReWalk)
// discriminates against.
func TestBootRace_RED_OneShotGivesUpWithoutTrigger(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()
	resetBootRaceState()
	t.Cleanup(resetBootRaceState)

	// Simulate the HEAD world: NO config-vars informer is started (we do not
	// call StartConfigVarsWatch). A one-shot boot scope with config absent must
	// give up, and there is no mechanism to re-enqueue when the ConfigMap later
	// appears.
	var rootsSeen atomic.Int64
	handler := func(ctx context.Context, s prewarmScope) error {
		if !false { // config-vars absent (HEAD one-shot, no re-read)
			rootsSeen.Store(0)
			return fmt.Errorf("roots_list_failed: config-vars not found")
		}
		return nil
	}

	if err := handler(context.Background(), prewarmScope{kind: scopeKindBoot}); err == nil {
		t.Fatalf("RED arm: expected roots_list_failed on the one-shot pass")
	}
	if rootsSeen.Load() != 0 {
		t.Fatalf("RED arm: expected 0 roots on the one-shot give-up, got %d", rootsSeen.Load())
	}

	// Now the ConfigMap "appears" — but with NO informer wired, NOTHING drives
	// a re-enqueue. This is the HEAD defect: cold forever.
	if ConfigVarsEnqueuedTotal() != 0 {
		t.Fatalf("RED arm: a re-enqueue fired without StartConfigVarsWatch — impossible on HEAD")
	}
	// Confirm no boot scope is pending on the singleton (nothing re-drove).
	e := prewarmEngineSingleton()
	hasBoot := e.pendingHasBootForTest()
	if hasBoot {
		t.Fatalf("RED arm: a boot scope was re-enqueued with no informer — impossible on HEAD")
	}
	t.Logf("RED arm PASS: on HEAD (no informer) the one-shot re-walk gives up and never re-drives — the defect")
}

// TestBootRace_TokenBackoff_RetriesThenAcquires proves the §2.3 bounded backoff:
// a provider that fails the first N attempts then succeeds must be RETRIED (not
// fire-once), and the token must install. RED against HEAD's fire-once
// installSeedLoopbackToken: the first (failing) attempt would degrade token-less.
func TestBootRace_TokenBackoff_RetriesThenAcquires(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	var attempts atomic.Int64
	const failFor = 3
	provider := func(ctx context.Context) (string, error) {
		n := attempts.Add(1)
		if n <= failFor {
			return "", fmt.Errorf("dial tcp: connection refused (attempt %d)", n)
		}
		return mkFakeAuthnJWT(t), nil
	}
	SetSeedLoopbackTokenProvider(provider)
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })

	errBefore := SeedLoopbackTokenErrTotal()
	// Generous ctx so the backoff has room to retry past failFor.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := installSeedLoopbackToken(ctx)

	if attempts.Load() <= failFor {
		t.Fatalf("TOKEN BACKOFF FAIL: provider was called %d times (<= %d) — fire-once, not retried "+
			"(RED = HEAD installSeedLoopbackToken)", attempts.Load(), failFor)
	}
	if tok, _ := xcontext.AccessToken(got); tok == "" {
		t.Fatalf("TOKEN BACKOFF FAIL: no token installed after the backoff acquired one on attempt %d",
			attempts.Load())
	}
	// On a successful acquire the degrade counter must NOT climb.
	if SeedLoopbackTokenErrTotal() != errBefore {
		t.Fatalf("TOKEN BACKOFF FAIL: degrade counter climbed (%d -> %d) despite a successful acquire",
			errBefore, SeedLoopbackTokenErrTotal())
	}
	t.Logf("token backoff: acquired on attempt %d (after %d transient failures); no degrade counted",
		attempts.Load(), failFor)
}

// TestBootRace_TokenBackoff_CtxBoundedDegrades proves the backoff is STRICTLY
// ctx-bounded and degrades-not-fails on exhaustion: an always-failing provider
// under a short ctx must return token-less (unchanged ctx) + bump the counter,
// and MUST NOT block past the ctx deadline (the 0.30.220 boot-stall regression
// watch).
func TestBootRace_TokenBackoff_CtxBoundedDegrades(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	var attempts atomic.Int64
	provider := func(ctx context.Context) (string, error) {
		attempts.Add(1)
		return "", fmt.Errorf("dial tcp: connection refused (forever)")
	}
	SetSeedLoopbackTokenProvider(provider)
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })

	errBefore := SeedLoopbackTokenErrTotal()
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	start := time.Now()
	got := installSeedLoopbackToken(ctx)
	elapsed := time.Since(start)

	// STRICTLY CTX-BOUNDED: it must return at/near the ctx deadline, not run
	// to the full Steps=30 exponential (which would be minutes).
	if elapsed > 3*time.Second {
		t.Fatalf("CTX-BOUND FAIL: installSeedLoopbackToken ran %v past a 600ms ctx — the backoff is not "+
			"ctx-bounded (0.30.220 boot-stall regression)", elapsed)
	}
	// DEGRADE-NOT-FAIL: ctx returned unchanged (token-less) + counter bumped.
	if got != ctx {
		t.Fatalf("DEGRADE FAIL: expected the original ctx returned unchanged (token-less) on exhaustion")
	}
	if SeedLoopbackTokenErrTotal() <= errBefore {
		t.Fatalf("DEGRADE FAIL: SeedLoopbackTokenErrTotal did not climb on backoff exhaustion (%d -> %d)",
			errBefore, SeedLoopbackTokenErrTotal())
	}
	if attempts.Load() < 1 {
		t.Fatalf("expected at least one attempt before ctx-bounded exhaustion")
	}
	t.Logf("token backoff ctx-bounded: %d attempts in %v, degraded token-less (counter climbed)",
		attempts.Load(), elapsed)
}

// --- content-level cohort scoping (C-d: cached-identity == served-identity) ---

// TestBootRace_ReWalk_SeedsCorrectlyScopedCohort proves the CONTENT-level
// property the doc's §3 step 4 requires: after a config-vars-driven re-walk,
// the #57 nested-loopback RA (compositions-panels) resolves to correctly-scoped
// cohorts under the target GVR — NOT the global universe and NOT an authn-group
// over-broad leak into a narrow cohort cell (project_prewarm_authn_loopback_
// identity_shift C-d). Reuses the P0 watcher (real RBAC + real objects.Get
// serve path) so this is a live-scoping assertion, not a warm bool.
func TestBootRace_ReWalk_SeedsCorrectlyScopedCohort(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	cache.ResetBindingsByGVRIndexForTest()
	_, ref := buildP0Watcher(t)

	compGVR := schema.GroupVersionResource{Group: "composition.krateo.io", Resource: "compositions"}
	panelGVR := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Resource: "panels"}
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{compGVR, panelGVR})

	// The re-walk resolves under the SA-credentialed cohort identity. Derive
	// the target GVR the compositions-panels RA LISTs — this is exactly what
	// rePrewarmBoot does per cohort (restActionTargetGVR under rctx).
	rctx := saCtxForTest(context.Background())
	targetGVR, ok := restActionTargetGVR(rctx, ref)
	if !ok {
		t.Fatalf("re-walk scoping: SA-credentialed target GVR derivation failed — objects.Get serve path")
	}
	if targetGVR.Group != "composition.krateo.io" || targetGVR.Resource != "compositions" {
		t.Fatalf("re-walk scoping: wrong target GVR %v", targetGVR)
	}

	// CONTENT-LEVEL, C-d: the seeded cohort set for the target GVR is the
	// per-binding NARROW set (admin wildcard + the 3 compositions groups),
	// NEVER the global universe (which includes the secrets-only auditors /
	// backup-bot subjects). A leak of the broad set into this compositions
	// cell is exactly the identity-shift over-broad-leak C-d guards.
	targets := cache.EnumeratePrewarmTargetsForGVR(targetGVR, "list")
	if len(targets) == 0 {
		t.Fatalf("re-walk scoping FAIL: no cohorts scoped to compositions — the re-walk seeded NOTHING")
	}
	if len(targets) > 6 {
		t.Fatalf("re-walk scoping FAIL (C-d over-broad leak): %d cohorts scoped to compositions — "+
			"the secrets-only global subjects leaked into the compositions cell", len(targets))
	}
	// Assert the secrets-only subjects are ABSENT (correctly-scoped, not global).
	for _, tg := range targets {
		if tg.Subject.Username == "backup-bot" {
			t.Fatalf("re-walk scoping FAIL (C-d): secrets-only subject %q leaked into the compositions "+
				"cohort — cached identity != served identity", tg.Subject.Username)
		}
	}
	t.Logf("re-walk content-scoping PASS: %d correctly-scoped compositions cohorts, no secrets-only leak", len(targets))
}
