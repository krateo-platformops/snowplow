package dispatchers

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/objects"
	restapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// captureDefaultSlogForTest redirects the default slog logger into the
// returned buffer for the duration of the test (at the given level) and
// QUIESCES the background dispatch-summary goroutine so its ticker cannot
// write into that buffer concurrently with the test's read.
//
// # Why (#221)
//
// startDispatchSummary (internal/resolvers/restactions/api) launches, on the
// first dispatchViaInformer call, a single process-lifetime goroutine that
// emits an `informer_dispatch.summary` line every N seconds (60s default) via
// the DEFAULT slog logger. A test that captures the default logger into a
// bytes.Buffer and later reads it therefore races that goroutine's ticker: a
// tick landing in the test's capture window writes the shared buffer while the
// test reads it (a -race DATA RACE, observed as TestF4bLeverA_R4 under
// -race -count). #221 gave the goroutine a stop+join seam
// (restapi.ResetDispatchSummaryForTest); this helper is the single place that
// wires it into the capture, so every slog-capturing test is quiesced by
// construction and a new one cannot re-introduce the race.
//
// It stops the summary BEFORE installing the buffer (joining any goroutine a
// prior test left running) and again on cleanup (joining any goroutine THIS
// test started before its buffer goes away), then restores the previous
// logger. The stop resets the summary's sync.Once, so a later dispatch
// relaunches it — production behaviour is unaffected (production never calls
// the reset).
func captureDefaultSlogForTest(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	quiesceDefaultSummaryForTest(t)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// quiesceDefaultSummaryForTest stops + joins the process-lifetime summary
// goroutines that log via the DEFAULT slog logger for the duration of the test
// — joining any a prior test left running now, and any this test starts on
// cleanup — so no ticker can write the DEFAULT slog logger while a test has
// captured it (#221 / #329 / #348). Use it DIRECTLY for capture sites that
// install their OWN handler and cannot use captureDefaultSlogForTest's JSON
// buffer (e.g. a TextHandler whose format the JSON helper would change, or a
// multi-buffer flow); captureDefaultSlogForTest wires it for the JSON-capture
// sites.
//
// It enumerates ALL default-slog summary EMITTERS, not just the dispatch one:
//   - startDispatchSummary  (internal/resolvers/restactions/api) — #221 / #329
//   - startObjectsGetSummary (internal/objects)                  — #348
//
// (startResolvedCacheSummary owns its own in-package quiescence and is never
// started from a dispatchers test, so it cannot leak into this capture.) One
// seam quiesces EVERY emitter for EVERY default-slog capture site. Test-only.
func quiesceDefaultSummaryForTest(t *testing.T) {
	t.Helper()
	restapi.ResetDispatchSummaryForTest()
	objects.ResetObjectsGetSummaryForTest()
	t.Cleanup(restapi.ResetDispatchSummaryForTest)
	t.Cleanup(objects.ResetObjectsGetSummaryForTest)
}
