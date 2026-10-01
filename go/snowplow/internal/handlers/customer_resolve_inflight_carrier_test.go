package handlers_test

// customer_resolve_inflight_carrier_test.go — #386 M3, PM gate condition 2: PROVE
// the resolve-path-only SCOPE of snowplow_customer_resolve_inflight, including the
// NEGATIVE arms (writes / GET /list / direct-proxy Call() must NOT move the count).
// External test package so it can drive BOTH the resolve-path handlers
// (dispatchers.RESTAction/Widgets) AND the non-marking handlers (handlers.Call/List).
//
// Technique: markCustomerInFlight is a `defer ...()()` at the TOP of the resolve
// handlers' ServeHTTP (before validation), so it is active for the whole dispatch
// incl. an early error write. A ResponseWriter that samples the count at the first
// WriteHeader/Write reads it MID-FLIGHT — 1 for a resolve-path handler, 0 for a
// non-marking one. Every request is driven to an early 400 (malformed extras / a
// missing required param), so no arm touches the apiserver.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

// inflightSamplingRW captures the resolve-path in-flight count at the moment the
// handler first writes — i.e. while the handler is still on the stack, before its
// deferred mark-decrement runs.
type inflightSamplingRW struct {
	http.ResponseWriter
	sampled int64
	done    bool
}

func (w *inflightSamplingRW) sample() {
	if !w.done {
		w.sampled = dispatchers.CustomerResolveInFlightCount()
		w.done = true
	}
}
func (w *inflightSamplingRW) WriteHeader(code int)         { w.sample(); w.ResponseWriter.WriteHeader(code) }
func (w *inflightSamplingRW) Write(b []byte) (int, error) { w.sample(); return w.ResponseWriter.Write(b) }

func TestIssue386_M3_InflightCountedOnlyOnResolvePath(t *testing.T) {
	if n := dispatchers.CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: precondition inflight=%d, want 0", n)
	}

	driveMidFlight := func(h http.Handler, method, target string) int64 {
		rw := &inflightSamplingRW{ResponseWriter: httptest.NewRecorder()}
		h.ServeHTTP(rw, httptest.NewRequest(method, target, nil))
		if !rw.done {
			t.Fatalf("#386 M3: handler never wrote a response for %s %s — cannot sample mid-flight", method, target)
		}
		return rw.sampled
	}

	// POSITIVE — resolve-path handlers MARK in-flight. A malformed `extras`
	// 400s AFTER the top-of-ServeHTTP mark defer, so the sample reads 1.
	if got := driveMidFlight(dispatchers.RESTAction(), "GET", "/call?extras=%7Bbad"); got != 1 {
		t.Fatalf("#386 M3: restactions mid-flight inflight=%d, want 1 — the resolve path MUST mark", got)
	}
	if got := driveMidFlight(dispatchers.Widgets(), "GET", "/call?extras=%7Bbad"); got != 1 {
		t.Fatalf("#386 M3: widgets mid-flight inflight=%d, want 1 — the resolve path MUST mark", got)
	}

	// NEGATIVE — non-resolve carriers MUST NOT mark (the anti-mislabel guarantee).
	// Each 400s in its own early validation before (and without) any mark.
	negatives := []struct {
		name           string
		h              http.Handler
		method, target string
	}{
		{"direct-proxy Call() GET (no resolve handler → fallthrough)", handlers.Call(), "GET", "/call"},
		{"write POST /call", handlers.Call(), "POST", "/call"},
		{"write PUT /call", handlers.Call(), "PUT", "/call"},
		{"write DELETE /call", handlers.Call(), "DELETE", "/call"},
		{"read-only fallthrough CallRead()", handlers.CallRead(), "POST", "/call/read"},
		{"GET /list", handlers.List(), "GET", "/list"},
	}
	for _, n := range negatives {
		if got := driveMidFlight(n.h, n.method, n.target); got != 0 {
			t.Fatalf("#386 M3: %s moved resolve-inflight to %d; want 0 — this carrier is OUT of scope "+
				"(I/O-bound proxy, not resolve-CPU work) and must never inflate the count", n.name, got)
		}
	}

	// Drains to 0 after every handler returned (defer-dec on every exit path).
	if n := dispatchers.CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: after all handlers inflight=%d, want 0 (balanced defer inc/dec)", n)
	}
}
