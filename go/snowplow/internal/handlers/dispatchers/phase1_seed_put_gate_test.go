// phase1_seed_put_gate_test.go — #102 GTTL-1 / ARM-BACKSTOP: the error-aware
// seed Put-gate falsifier (declineSeedPutOnError).
//
// The keepwarm sweep re-resolves ALREADY-WARM cells. A re-resolve that hits a
// swallowed/continueOnError'd STAGE error (resolve returns nil-overall with
// degraded bytes) or touches an EXTERNAL endpoint must NOT blind-re-Put over the
// good warm entry — it must decline (keep the prior entry; TTL is the outer net,
// design §1.6). This mirrors the refresher's gate (resolve_populate.go:251-285).
// The gate is uniform across boot + sweep (arch Option A).
//
// The gate keys on error PRESENCE, never on result emptiness — a legitimately-
// empty result (0 compositions, no stage error, sink==0) MUST still Put and HIT.
// That is the discriminating control (feedback_falsifier_shape_must_discriminate):
// if the empty-result arm DECLINED, the gate would be wrong-shaped (keying on
// emptiness, not error).
//
// declineSeedPutOnError is a pure function of the two sinks (bumped by the
// resolver at api/resolve.go:338 stage + :1048 external when a sink is on ctx).
// Driving it with directly-bumped sinks exercises the SAME decision the seed
// primitives make around handle.Put — the sinks are installed on resCtx BEFORE
// Resolve in both seedOneRestaction and seedOneWidget (asserted structurally by
// TestSeedPrimitives_InstallErrorSinks). Hermetic, -race, no cluster.

package dispatchers

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// GTTL-1 GREEN + control: stage-error → decline + counter++; empty (no error) →
// NO decline (still Puts). The empty arm is the discriminator.
func TestSeedPutGate_DeclinesOnStageError_NotOnEmptyResult(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	ctx := context.Background()

	// (control) NO error observed → do NOT decline (a legitimately-empty result
	// keys sink==0 and MUST be Put). Fresh sinks read Count()==0.
	_, emptyStage := cache.WithStageErrorSink(ctx)
	_, emptyExt := cache.WithExternalTouchedSink(ctx)
	before := pipSeedSkippedStageErrorTotal.Load()
	if declineSeedPutOnError(ctx, "widgets", "krateo/empty-ok", "key/empty-ok", emptyStage, emptyExt, nil) {
		t.Fatal("GTTL-1 control VIOLATED: declined a Put with NO stage error — the gate keys on emptiness, not error presence (a 0-composition user would lose their cell)")
	}
	if got := pipSeedSkippedStageErrorTotal.Load(); got != before {
		t.Fatalf("GTTL-1 control: counter moved on a no-error resolve (before=%d after=%d)", before, got)
	}

	// (GREEN) a swallowed stage error (the resolver bumped the sink) → DECLINE +
	// counter++. Simulate the resolver's dict[ErrorKey] bump.
	_, stageSink := cache.WithStageErrorSink(ctx)
	_, extSink := cache.WithExternalTouchedSink(ctx)
	stageSink.Bump("exportJwt-loopback", "401 no per-user JWT in background seed")
	before = pipSeedSkippedStageErrorTotal.Load()
	if !declineSeedPutOnError(ctx, "widgets", "krateo/dashboard-flex", "key/dashboard-flex", stageSink, extSink, nil) {
		t.Fatal("GTTL-1 ARM-BACKSTOP VIOLATED: did NOT decline the Put despite a stage error — the sweep would blind-re-Put degraded bytes over the good warm entry")
	}
	if got := pipSeedSkippedStageErrorTotal.Load(); got != before+1 {
		t.Fatalf("GTTL-1: seed-decline counter want +1; before=%d after=%d", before, got)
	}
}

// GTTL-1 external-touch arm: an external endpoint touch (no dep edge) → decline
// + counter++.
func TestSeedPutGate_DeclinesOnExternalTouch(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	ctx := context.Background()
	_, stageSink := cache.WithStageErrorSink(ctx)
	_, extSink := cache.WithExternalTouchedSink(ctx)
	extSink.Bump() // resolver touched a genuine external endpoint

	before := pipSeedSkippedStageErrorTotal.Load()
	if !declineSeedPutOnError(ctx, "restactions", "krateo/ext-ra", "key/ext-ra", stageSink, extSink, nil) {
		t.Fatal("GTTL-1: did NOT decline the Put despite an external touch — external bytes have no dep edge to invalidate them")
	}
	if got := pipSeedSkippedStageErrorTotal.Load(); got != before+1 {
		t.Fatalf("GTTL-1 external arm: counter want +1; before=%d after=%d", before, got)
	}
}

// MUTATION (blind-re-Put): prove the gate is what declines. A neutered gate
// that ignored the stage sink (the pre-#102 behavior — the seed primitives
// Put unconditionally) would NOT decline. We can't mutate the prod function
// from the test, so we assert the DISCRIMINATOR directly: the gate's verdict
// flips SOLELY on sink Count — a bumped sink declines, the identical call with
// an unbumped sink does not. If the gate ever regressed to blind-Put (ignoring
// the sink), the bumped-sink arm below would return false and FAIL.
func TestSeedPutGate_VerdictFlipsSolelyOnSinkCount(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	ctx := context.Background()

	_, unbumped := cache.WithStageErrorSink(ctx)
	_, ext := cache.WithExternalTouchedSink(ctx)
	if declineSeedPutOnError(ctx, "widgets", "krateo/w", "key/w", unbumped, ext, nil) {
		t.Fatal("unbumped sink must NOT decline")
	}

	_, bumped := cache.WithStageErrorSink(ctx)
	_, ext2 := cache.WithExternalTouchedSink(ctx)
	bumped.Bump("stage", "err")
	if !declineSeedPutOnError(ctx, "widgets", "krateo/w", "key/w", bumped, ext2, nil) {
		t.Fatal("mutation guard: a bumped stage sink MUST decline — if this returns false the gate has regressed to blind-Put (the GTTL-1 defect)")
	}
}

// TestSeedPrimitives_InstallErrorSinks is the STRUCTURAL wiring assert: both
// shared seed primitives install WithStageErrorSink + WithExternalTouchedSink on
// the resolve ctx BEFORE calling Resolve, and gate the Put via
// declineSeedPutOnError. Source-level (the hermetic constraint: driving the real
// resolver's dict[ErrorKey] bump needs the full stack). Guards against a future
// edit dropping a sink install (which would silently re-open the blind-Put).
func TestSeedPrimitives_InstallErrorSinks(t *testing.T) {
	src, err := os.ReadFile("phase1_pip_seed.go")
	if err != nil {
		t.Fatalf("read phase1_pip_seed.go: %v", err)
	}
	s := string(src)
	for _, want := range []string{
		"cache.WithStageErrorSink(resCtx)",
		"cache.WithExternalTouchedSink(resCtx)",
		// #643 PR-B: the decline runs on resCtx — it reads the resolve's
		// captured deps + entry stamp for the declined-external mark.
		"declineSeedPutOnError(resCtx, \"restactions\"",
		"declineSeedPutOnError(resCtx, \"widgets\"",
	} {
		if strings.Count(s, want) < 1 {
			t.Fatalf("GTTL-1 wiring: expected %q in the shared seed primitives (the Put-gate must be installed on the resolve ctx + gated); not found", want)
		}
	}
	// Both sinks must appear TWICE (once per primitive).
	if got := strings.Count(s, "cache.WithStageErrorSink(resCtx)"); got != 2 {
		t.Fatalf("GTTL-1 wiring: WithStageErrorSink must be installed in BOTH seedOneRestaction and seedOneWidget; found %d", got)
	}
	if got := strings.Count(s, "cache.WithExternalTouchedSink(resCtx)"); got != 2 {
		t.Fatalf("GTTL-1 wiring: WithExternalTouchedSink must be installed in BOTH primitives; found %d", got)
	}
}
