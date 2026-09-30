// cache_key_diag_lazy_rbac_test.go — #251 regression falsifier.
//
// emitDispatchCacheKeyDiag's nil-inputs branch computes `binding_uid` by
// calling rbac.EvaluateRBAC, and that value is consumed ONLY by the
// log.Info("dispatch.cache_key.computed") line and the env-gated stderr
// lane. Go evaluates a call argument BEFORE slog checks the handler
// level, so on a warn/error logger (the chart default, LOG_LEVEL=warn)
// the full RBAC evaluation was paid to build a line that is never
// emitted — a pure-waste cost on the /readyz-gating seed path.
//
// This test drives the nil-inputs branch under a logger whose level is
// above Info and with the stderr lane disabled, then asserts EvaluateRBAC
// was NOT invoked. rbac.EvaluateRBACCallCount() is the side-effect
// observer (an atomic incremented at the top of every EvaluateRBAC call).
//
// RED on pre-fix code: the else branch calls EvaluateRBAC unconditionally,
// so the count is 1. GREEN after the fix gates the evaluation on the
// handler level (and the stderr-lane env), so the count stays 0.
package dispatchers

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestEmitDispatchCacheKeyDiag_NoEagerRBACEvalWhenLineWontEmit(t *testing.T) {
	// Stderr diagnostic lane OFF — otherwise binding_uid has a live
	// consumer and the evaluation is legitimately required.
	t.Setenv("DISPATCH_KEY_DIAG_ENABLED", "false")

	// Logger at warn: the log.Info line below Info level will NOT emit,
	// so nothing consumes the RBAC-derived binding_uid.
	warnLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	rbac.ResetEvaluateRBACCallCount()
	t.Cleanup(rbac.ResetEvaluateRBACCallCount)

	// nil inputs drives the else branch (cache-disabled / no-identity),
	// the only branch that calls EvaluateRBAC.
	emitDispatchCacheKeyDiag(
		warnLogger, "test_site", context.Background(),
		"key-hash", nil,
		"restaction", "templates.krateo.io", "v1", "restactions", "demo-ns", "demo-name",
		20, 0, nil,
	)

	if got := rbac.EvaluateRBACCallCount(); got != 0 {
		t.Fatalf("EvaluateRBAC was evaluated %d time(s) to build a log line that never emits at warn "+
			"(#251 eager-argument footgun); want 0 — the evaluation must be gated on the handler level",
			got)
	}
}

// TestEmitDispatchCacheKeyDiag_StillEvaluatesWhenInfoEmits pins the
// behaviour-preserving half: when the logger WOULD emit at Info, the
// diagnostic still computes binding_uid (one EvaluateRBAC call), so the
// fix does not silently drop the field in kind/bench runs where INFO is on.
func TestEmitDispatchCacheKeyDiag_StillEvaluatesWhenInfoEmits(t *testing.T) {
	t.Setenv("DISPATCH_KEY_DIAG_ENABLED", "false")

	infoLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	rbac.ResetEvaluateRBACCallCount()
	t.Cleanup(rbac.ResetEvaluateRBACCallCount)

	emitDispatchCacheKeyDiag(
		infoLogger, "test_site", context.Background(),
		"key-hash", nil,
		"restaction", "templates.krateo.io", "v1", "restactions", "demo-ns", "demo-name",
		20, 0, nil,
	)

	if got := rbac.EvaluateRBACCallCount(); got != 1 {
		t.Fatalf("EvaluateRBAC ran %d time(s) when the Info line emits; want exactly 1 "+
			"(the binding_uid field must still be computed when the line is readable)", got)
	}
}

// TestEmitDispatchCacheKeyDiag_StillEvaluatesWhenDiagEnvOn pins the SECOND
// carrier (pm C3, arm-per-carrier): the fix gates the eval on
// `Enabled(Info) || DISPATCH_KEY_DIAG_ENABLED`, so the DISPATCH_KEY_DIAG_ENABLED
// stderr lane is an INDEPENDENT live consumer. At warn (the log.Info line is
// dropped) with the diag env ON, the binding_uid eval MUST still run — else a
// fix that gated on the handler level ALONE would silently disable the diag
// lane's field. Without this arm that carrier's re-enable is unproven.
func TestEmitDispatchCacheKeyDiag_StillEvaluatesWhenDiagEnvOn(t *testing.T) {
	// Diag stderr lane ON — a live consumer of binding_uid independent of the
	// log level.
	t.Setenv("DISPATCH_KEY_DIAG_ENABLED", "true")

	// warn logger: the log.Info line will NOT emit, so the diag-env lane is the
	// ONLY consumer keeping the eval alive here.
	warnLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	rbac.ResetEvaluateRBACCallCount()
	t.Cleanup(rbac.ResetEvaluateRBACCallCount)

	emitDispatchCacheKeyDiag(
		warnLogger, "test_site", context.Background(),
		"key-hash", nil,
		"restaction", "templates.krateo.io", "v1", "restactions", "demo-ns", "demo-name",
		20, 0, nil,
	)

	if got := rbac.EvaluateRBACCallCount(); got != 1 {
		t.Fatalf("EvaluateRBAC ran %d time(s) with DISPATCH_KEY_DIAG_ENABLED=true at warn; want exactly 1 "+
			"— the diag-env stderr lane is a live consumer and must keep the binding_uid eval enabled even "+
			"when the Info line is dropped (#251 arm-per-carrier / pm C3)", got)
	}
}
