// main_cors_test.go — H4 [SEC]: the CORS contract that lets the browser
// frontend READ the live-refresh signalling headers off a cross-origin /call
// response.
//
// EventSource/fetch in a cross-origin browser context can only read a response
// header if the server lists it in Access-Control-Expose-Headers. The
// live-refresh feature (refreshes.go) rides X-Snowplow-Refresh-Key /
// X-Snowplow-Refresh-Class on the /call response; the frontend also reads Link
// (pagination). If any of those drops out of the CORS ExposedHeaders the
// browser silently cannot see them and live-refresh / pagination break with no
// server-side error — a SEC-class silent regression.
//
// snowplowCORSOptions() (main.go) is the extracted, named constructor for the
// exact cors.Options main's http.Server is built from (prod-testability H4). We
// wrap a trivial handler with the REAL use.CORS(snowplowCORSOptions()) — the
// identical construction main uses — and drive a cross-origin GET, so the
// assertion binds to production wiring, not a hand-copied literal.
//
// RED arm (TestSnowplowCORS_RED_DroppingExposedHeaderIsCaught): dropping either
// X-Snowplow-Refresh-Key or X-Snowplow-Refresh-Class from the options makes the
// exposed-headers assertion FAIL — proving the check is discriminating.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/plumbing/server/use/cors"
)

// exposeList parses an Access-Control-Expose-Headers value ("A, B, C") into a
// case-insensitive membership set.
func exposeList(v string) map[string]bool {
	out := map[string]bool{}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out[strings.ToLower(p)] = true
		}
	}
	return out
}

// serveCORS wraps a 204 handler with use.CORS(opts), drives a cross-origin GET
// carrying an Origin header, and returns the response headers.
func serveCORS(t *testing.T, opts cors.Options) http.Header {
	t.Helper()
	h := use.CORS(opts)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://snowplow.example/call", nil)
	req.Header.Set("Origin", "https://frontend.example") // cross-origin actual request
	h.ServeHTTP(rec, req)
	return rec.Result().Header
}

// requiredExposed is the header set the frontend MUST be able to read.
var requiredExposed = []string{
	"X-Snowplow-Refresh-Key",
	"X-Snowplow-Refresh-Class",
	"Link",
	"X-Snowplow-Serve-Outcome", // #1265 P0
}

// TestSnowplowCORS_ExposesRefreshHeaders — a cross-origin GET with an Origin
// header gets an Access-Control-Expose-Headers that CONTAINS all three
// frontend-required headers, Access-Control-Allow-Credentials: true, and an
// Access-Control-Allow-Origin. This is the production contract exercised through
// the real use.CORS(snowplowCORSOptions()) construction.
func TestSnowplowCORS_ExposesRefreshHeaders(t *testing.T) {
	hdr := serveCORS(t, snowplowCORSOptions())

	exposed := exposeList(hdr.Get("Access-Control-Expose-Headers"))
	for _, want := range requiredExposed {
		if !exposed[strings.ToLower(want)] {
			t.Fatalf("H4: Access-Control-Expose-Headers must CONTAIN %q so the browser can read it; "+
				"got %q", want, hdr.Get("Access-Control-Expose-Headers"))
		}
	}

	if got := hdr.Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("H4: Access-Control-Allow-Credentials must be \"true\"; got %q", got)
	}
	if got := hdr.Get("Access-Control-Allow-Origin"); got == "" {
		t.Fatalf("H4: a cross-origin GET must receive an Access-Control-Allow-Origin; got empty")
	}
}

// TestSnowplowCORS_OptionsShapeInvariants pins the options-level invariants the
// http.Server relies on, independent of the header round-trip: AllowCredentials
// on, the three exposed headers present. This is the cheap, wiring-level half
// (the round-trip test above is the behavioural half).
func TestSnowplowCORS_OptionsShapeInvariants(t *testing.T) {
	opts := snowplowCORSOptions()
	if !opts.AllowCredentials {
		t.Fatalf("H4: snowplowCORSOptions().AllowCredentials must be true")
	}
	have := map[string]bool{}
	for _, h := range opts.ExposedHeaders {
		have[strings.ToLower(h)] = true
	}
	for _, want := range requiredExposed {
		if !have[strings.ToLower(want)] {
			t.Fatalf("H4: ExposedHeaders must include %q; got %v", want, opts.ExposedHeaders)
		}
	}
}

// TestSnowplowCORS_RED_DroppingExposedHeaderIsCaught is the RED proof. We build
// a plausible wrong impl — the same options with a live-refresh header dropped
// from ExposedHeaders — and assert the H4 exposed-headers check FAILS on it.
// This proves TestSnowplowCORS_ExposesRefreshHeaders is discriminating: a
// silent drop of X-Snowplow-Refresh-Key (or -Class) is caught, not tolerated.
func TestSnowplowCORS_RED_DroppingExposedHeaderIsCaught(t *testing.T) {
	for _, drop := range []string{"X-Snowplow-Refresh-Key", "X-Snowplow-Refresh-Class"} {
		t.Run("drop "+drop, func(t *testing.T) {
			wrong := snowplowCORSOptions()
			var kept []string
			for _, h := range wrong.ExposedHeaders {
				if !strings.EqualFold(h, drop) {
					kept = append(kept, h)
				}
			}
			wrong.ExposedHeaders = kept

			hdr := serveCORS(t, wrong)
			exposed := exposeList(hdr.Get("Access-Control-Expose-Headers"))

			// The SAME assertion the green test uses MUST now fail for the
			// dropped header — i.e. the dropped header is absent from the
			// exposed set.
			if exposed[strings.ToLower(drop)] {
				t.Fatalf("RED: dropping %q from ExposedHeaders should make it un-exposed, "+
					"but it is still present (%q) — the H4 check is not discriminating",
					drop, hdr.Get("Access-Control-Expose-Headers"))
			}
			// And the other two remain — the drop is surgical, so the green
			// assertion fails ONLY on the dropped header (not a blanket break).
			for _, other := range requiredExposed {
				if strings.EqualFold(other, drop) {
					continue
				}
				if !exposed[strings.ToLower(other)] {
					t.Fatalf("RED: dropping %q must not disturb %q, but it is gone", drop, other)
				}
			}
		})
	}
}
