package dispatchers

import (
	"bytes"
	"log/slog"
	"sync"
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
//
// The buffer is a syncBuffer (#392): quiescing the summary emitters cannot stop
// EVERY background goroutine that logs through the default logger (the RBAC
// initial publish of a test's watcher, an informer, a refresher worker), so the
// capture itself must be safe to write from any goroutine while the test reads
// or resets it.
func captureDefaultSlogForTest(t *testing.T, level slog.Level) *syncBuffer {
	t.Helper()
	quiesceDefaultSummaryForTest(t)
	var buf syncBuffer
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

// syncBuffer is a goroutine-safe bytes.Buffer for capturing slog output: the
// default logger is process-global, so a background goroutine (the RBAC
// initial publish, an informer, a summary ticker) may write it while the test
// reads or resets the capture. Every default-slog capture in this package uses
// it (#392). Bytes returns a copy, so a caller never holds the live slice.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}
