package main

// main_telemetry_1020_delta_test.go — #1020 delta review (FAIL at a26bf7ff):
// D1 (a repeated JSON key restarted the attribute counters) and D2 (one user's
// slow bodies could hold every decode slot). Same harness as the other #1015
// arms: the real router, RefreshAuth, Ingest and Forwarder.

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"
)

// D1 — a key repeated in ANY object the decoder walks is refused with 400:
// the reviewer's three shapes (span "attributes", resource "attributes",
// resourceSpan "resource") plus the attribute KeyValue and its AnyValue.
//
// MUTATION (RED, shown in the PR): the repeated-key check in walker.object
// removed (the shapes are answered 200).
func TestS1020_RepeatedKeysAreRefused(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	const svc = `{"key":"service.name","value":{"stringValue":"krateo-frontend"}}`
	const kv = `{"key":"http.route","value":{"stringValue":"/call"}}`
	const span = `"name":"krateo.widget.fetch","traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba90001","startTimeUnixNano":"1","endTimeUnixNano":"2"`
	shapes := map[string]string{
		"span attributes":     `{"resourceSpans":[{"resource":{"attributes":[` + svc + `]},"scopeSpans":[{"spans":[{` + span + `,"attributes":[` + kv + `],"attributes":[` + kv + `]}]}]}]}`,
		"resource attributes": `{"resourceSpans":[{"resource":{"attributes":[` + svc + `],"attributes":[` + svc + `]},"scopeSpans":[{"spans":[{` + span + `}]}]}]}`,
		"resource":            `{"resourceSpans":[{"resource":{"attributes":[` + svc + `]},"resource":{"attributes":[` + svc + `]},"scopeSpans":[{"spans":[{` + span + `}]}]}]}`,
		"KeyValue key":        `{"resourceSpans":[{"resource":{"attributes":[` + svc + `]},"scopeSpans":[{"spans":[{` + span + `,"attributes":[{"key":"http.route","key":"x","value":{"stringValue":"/call"}}]}]}]}]}`,
		"AnyValue stringValue": `{"resourceSpans":[{"resource":{"attributes":[` + svc + `]},"scopeSpans":[{"spans":[{` + span +
			`,"attributes":[{"key":"http.route","value":{"stringValue":"/call","stringValue":"/x"}}]}]}]}]}`,
	}
	before := rum.Read()
	i := 0
	for name, body := range shapes {
		rec := postRUM(t, h, mintRUMToken(t, fmt.Sprintf("dup-%d", i)), []byte(body))
		i++
		if rec.Code != http.StatusBadRequest {
			t.Errorf("repeated %s: status %d, want 400", name, rec.Code)
		}
	}
	if d := rum.Read().Requests["dropped_malformed"] - before.Requests["dropped_malformed"]; d != uint64(len(shapes)) {
		t.Errorf("dropped_malformed delta %d, want %d", d, len(shapes))
	}
	if n := c.hits.Load(); n != 0 {
		t.Errorf("a refused body reached the collector (%d posts)", n)
	}
}

// D2 — one request in flight per user: while user A is still sending a body,
// A's second request is refused 429 busy at once, and another user is still
// served, even though free decode slots remain.
//
// MUTATION (RED, shown in the PR): the per-user in-flight check removed (A's
// second request takes a free slot and is answered 200).
func TestS1020_OneRequestInFlightPerUser(t *testing.T) {
	prev := runtime.GOMAXPROCS(4) // 4 decode slots: A's slow body holds only one
	defer runtime.GOMAXPROCS(prev)
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	tokA, tokB := mintRUMToken(t, "inflight-a"), mintRUMToken(t, "inflight-b")

	pr, pw := io.Pipe()
	slow := httptest.NewRequest(http.MethodPost, rum.Path, pr)
	slow.ContentLength = -1
	slow.Header.Set("Content-Type", "application/json")
	slow.Header.Set("Authorization", "Bearer "+tokA)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(httptest.NewRecorder(), slow)
	}()
	if _, err := pw.Write([]byte(`{"resourceSpans":[`)); err != nil { // returns once the decoder read it
		t.Fatal(err)
	}
	defer func() {
		_ = pw.CloseWithError(io.ErrUnexpectedEOF)
		wg.Wait()
	}()

	body := otlpBody(t, rum.ServiceName, nil, widgetFetch())
	before := rum.Read()
	start := time.Now()
	recA := postRUM(t, h, tokA, body)
	if recA.Code != http.StatusTooManyRequests {
		t.Fatalf("user A's second request while the first is in flight: status %d, want 429", recA.Code)
	}
	if recA.Header().Get("Retry-After") == "" {
		t.Fatal("busy 429 without Retry-After")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("the busy refusal took %v; it must not wait", el)
	}
	if d := rum.Read().Requests["dropped_busy"] - before.Requests["dropped_busy"]; d != 1 {
		t.Fatalf("dropped_busy delta %d, want 1", d)
	}
	if recB := postRUM(t, h, tokB, body); recB.Code != http.StatusOK {
		t.Fatalf("user B while user A is in flight: status %d, want 200", recB.Code)
	}
}
