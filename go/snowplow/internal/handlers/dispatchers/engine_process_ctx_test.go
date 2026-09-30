// engine_process_ctx_test.go — Ship 2 Stage 2.5 / 0.30.248 (Fix v2)
// PM Change #3 unit tests. Verifies the engineProcessCtx wiring +
// one-time slog.Error nil-fallback semantics in isolation.

package dispatchers

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestEngineWorker_NilProcessCtxLogsErrorOnce drives the
// resolveEngineProcessCtx nil-fallback path and verifies:
//   - First call with nil engineProcessCtx logs ONE slog.Error.
//   - Subsequent calls with nil engineProcessCtx log ZERO additional
//     errors (one-time semantics via sync.Once).
//   - Every call returns a non-nil ctx (context.Background fallback).
func TestEngineWorker_NilProcessCtxLogsErrorOnce(t *testing.T) {
	quiesceDefaultSummaryForTest(t) // #221: this test captures the default slog (TextHandler) — quiesce the summary goroutine so its ticker can't race &buf
	ResetEngineProcessCtxForTest()
	defer ResetEngineProcessCtxForTest()

	// Capture slog output via a buffered handler. Replace the default
	// logger and restore after the test.
	var buf bytes.Buffer
	prev := slog.Default()
	defer slog.SetDefault(prev)
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	// First call — error MUST fire and fallback ctx returned.
	ctx1 := resolveEngineProcessCtx()
	if ctx1 == nil {
		t.Fatal("resolveEngineProcessCtx returned nil — fallback ctx missing")
	}
	if ctx1 != context.Background() {
		t.Fatal("resolveEngineProcessCtx fallback should be context.Background()")
	}
	if !strings.Contains(buf.String(), "engine.process_ctx.missing") {
		t.Fatalf("expected one-time slog.Error to fire on first nil "+
			"resolve, got log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("expected ERROR level slog emit, got log:\n%s", buf.String())
	}

	// Capture the first-fire buf state for diff.
	firstFireSnapshot := buf.String()

	// Second + third calls — MUST NOT add additional error log lines
	// (one-time semantics via sync.Once).
	ctx2 := resolveEngineProcessCtx()
	ctx3 := resolveEngineProcessCtx()
	if ctx2 == nil || ctx3 == nil {
		t.Fatal("subsequent resolveEngineProcessCtx calls returned nil — fallback ctx missing")
	}
	if buf.String() != firstFireSnapshot {
		t.Fatalf("expected one-time semantics: subsequent resolves added "+
			"log lines after the first fire. Before:\n%s\nAfter:\n%s",
			firstFireSnapshot, buf.String())
	}
}

// TestEngineWorker_SetEngineProcessContextSuppressesNilFallback
// asserts the happy path: when SetEngineProcessContext has been
// called with a non-nil ctx, resolveEngineProcessCtx returns that
// exact ctx and emits ZERO error logs.
func TestEngineWorker_SetEngineProcessContextSuppressesNilFallback(t *testing.T) {
	quiesceDefaultSummaryForTest(t) // #221: TextHandler default-slog capture — quiesce the summary goroutine
	ResetEngineProcessCtxForTest()
	defer ResetEngineProcessCtxForTest()

	var buf bytes.Buffer
	prev := slog.Default()
	defer slog.SetDefault(prev)
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	wired, wiredCancel := context.WithCancel(context.Background())
	defer wiredCancel()
	SetEngineProcessContext(wired)

	got := resolveEngineProcessCtx()
	if got != wired {
		t.Fatalf("resolveEngineProcessCtx should return the wired ctx; "+
			"got %v want %v", got, wired)
	}
	if strings.Contains(buf.String(), "engine.process_ctx.missing") {
		t.Fatalf("expected NO error emit when ctx is wired; got log:\n%s",
			buf.String())
	}
}

// TestEngineWorker_ResetEngineProcessCtxForTestResetsOnceGate verifies
// the TEST-ONLY reset helper clears both the package var AND the
// sync.Once gate, so a test that drives the nil-fallback path
// repeatedly within one binary doesn't see the first-test's gate.
func TestEngineWorker_ResetEngineProcessCtxForTestResetsOnceGate(t *testing.T) {
	quiesceDefaultSummaryForTest(t) // #221: two TextHandler default-slog captures (buf1/buf2) — quiesce the summary goroutine for the whole func
	// First nil-fire cycle.
	ResetEngineProcessCtxForTest()
	var buf1 bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf1, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	resolveEngineProcessCtx()
	if !strings.Contains(buf1.String(), "engine.process_ctx.missing") {
		slog.SetDefault(prev)
		t.Fatalf("first cycle: expected error fire, got:\n%s", buf1.String())
	}

	// Reset + second nil-fire cycle MUST fire again.
	ResetEngineProcessCtxForTest()
	var buf2 bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf2, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	resolveEngineProcessCtx()
	slog.SetDefault(prev)
	if !strings.Contains(buf2.String(), "engine.process_ctx.missing") {
		t.Fatalf("second cycle (post-reset): expected error fire, got:\n%s",
			buf2.String())
	}
}
