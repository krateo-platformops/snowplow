package tracing

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// F3 — sampler family, DISCRIMINATING (1.12.4 design §2.3 / §10, gate
// condition C1).
//
// PRODUCTION FUNCTION UNDER TEST: tracing.Setup — the ONLY place the
// TracerProvider is built. It passes no WithSampler, so the SDK's
// samplerFromEnv reads OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG; the
// chart's sampler values are live exactly because Setup does not override
// them. The span decision is then made by the real otelhttp server handler
// over the provider Setup installed globally.
//
// WHY THE ARMS DISCRIMINATE. traceIDRatioSampler.ShouldSample computes
// x := BigEndian.Uint64(TraceID[8:16]) >> 1 and samples iff
// x < uint64(ratio * (1<<63)). With ratio 0.05 the bound is ≈ 4.61e17.
// A trace id whose bytes 8..15 are all 0xff gives x ≈ 9.22e18 → DROP; all
// 0x00 gives x = 0 → SAMPLE. Under parentbased_traceidratio the incoming
// traceparent's sampled=01 flag would WIN and the all-ff request would be
// RECORDED — so arm 1 reds if the parent-based family ships. Rev 3's
// always_on/always_off arms pass under either family and pin nothing; they
// are kept below only as contract pins.
//
// RED on main (8de5295): main has no chart value for the sampler at all
// (SDK default = ParentBased(AlwaysSample) → arm 1 recorded), and the
// pre-1.12.4 chart draft carried parentbased_traceidratio (arm 1 recorded).
//
// #1013: the chart now ships parentbased_always_on (Diego's ruling
// 2026-10-30: the SLIs need 100% of server spans). The traceidratio arms stay
// as SDK-family pins (what an operator gets by setting the ratio sampler
// back); the S1013 arms at the bottom pin the shipped family's behaviour,
// including the edge-sampling caveat: an UNSAMPLED parent drops the span.

const (
	// bytes 8..15 all 0xff → x ≈ 9.22e18 > 4.61e17 → Drop under traceidratio 0.05
	traceIDHighTail = "0123456789abcdefffffffffffffffff"
	// bytes 8..15 all 0x00 → x = 0 → Sample
	traceIDLowTail = "0123456789abcdef0000000000000000"
	spanIDParent   = "00f067aa0ba902b7"
)

// setupWithEnv drives the REAL tracing.Setup under the given sampler env and
// an in-process OTLP receiver (nothing is asserted on export; the batcher
// is flushed at cleanup). Globals are reset afterwards so arms do not leak
// providers into one another.
func setupWithEnv(t *testing.T, sampler, arg string) {
	t.Helper()
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	t.Cleanup(sink.Close)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_TRACING_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", sink.URL)
	t.Setenv("OTEL_TRACES_SAMPLER", sampler)
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", arg)

	shutdown, err := Setup(context.Background(), "f3-build")
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	if _, isNoop := otel.GetTracerProvider().(noop.TracerProvider); isNoop {
		t.Fatal("harness: Setup did not install an SDK TracerProvider")
	}
}

// driveWithParent sends one request through the REAL otelhttp server
// handler carrying a W3C traceparent with the given trace id and
// sampled=01, and reports whether the server span was recording and
// sampled.
func driveWithParent(t *testing.T, traceIDHex string) (recording, sampled bool, gotTraceID string) {
	t.Helper()
	if _, err := hex.DecodeString(traceIDHex); err != nil {
		t.Fatalf("harness: bad trace id %q", traceIDHex)
	}
	return drive(t, "00-"+traceIDHex+"-"+spanIDParent+"-01")
}

// drive sends one request through the REAL otelhttp server handler with the
// given traceparent header ("" = none: a root request) and reports what the
// server span decided.
func drive(t *testing.T, traceparent string) (recording, sampled bool, gotTraceID string) {
	t.Helper()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		recording = span.IsRecording()
		sampled = span.SpanContext().IsSampled()
		gotTraceID = span.SpanContext().TraceID().String()
		w.WriteHeader(200)
	})
	h := otelhttp.NewHandler(inner, "snowplow")
	req := httptest.NewRequest(http.MethodGet, "/call?resource=x", nil)
	if traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("handler status %d", rr.Code)
	}
	return recording, sampled, gotTraceID
}

// TestF3_TraceIDRatioIgnoresSampledParent is the discriminating arm.
func TestF3_TraceIDRatioIgnoresSampledParent(t *testing.T) {
	setupWithEnv(t, "traceidratio", "0.05")

	// Arm 1: parent says sampled=1, but the trace id hashes OUTSIDE the 5%
	// → must NOT be sampled. parentbased_* would sample it.
	rec, samp, tid := driveWithParent(t, traceIDHighTail)
	if tid != traceIDHighTail {
		t.Fatalf("propagator did not carry the incoming trace id (got %s) — the request did not go through the real propagation path", tid)
	}
	if samp || rec {
		t.Fatalf("all-ff trace id with sampled=01 parent was sampled=%v recording=%v under traceidratio 0.05 — the parent-based family is in effect (C1 violated)", samp, rec)
	}

	// Arm 2: trace id hashes INSIDE the 5% → sampled + recording.
	rec, samp, tid = driveWithParent(t, traceIDLowTail)
	if tid != traceIDLowTail {
		t.Fatalf("propagator did not carry the incoming trace id (got %s)", tid)
	}
	if !samp || !rec {
		t.Fatalf("all-00 trace id was sampled=%v recording=%v under traceidratio 0.05 — expected sampled", samp, rec)
	}
}

// TestF3_ParentBasedWouldHaveSampled documents the failure mode the chart
// must never ship: under parentbased_traceidratio the SAME all-ff request
// IS sampled because the parent's flag wins. This arm is what makes the
// arm above discriminating rather than a tautology.
func TestF3_ParentBasedWouldHaveSampled(t *testing.T) {
	setupWithEnv(t, "parentbased_traceidratio", "0.05")
	rec, samp, _ := driveWithParent(t, traceIDHighTail)
	if !samp || !rec {
		t.Fatalf("parentbased_traceidratio did not honour the parent's sampled flag (sampled=%v recording=%v) — the discriminator no longer discriminates; re-derive the arm", samp, rec)
	}
}

// TestF3_SamplerEnvIsLive — contract pins (pass on main by design): the
// env knob reaches the provider because Setup passes no WithSampler. Reds
// the day someone adds WithSampler to tracing.go and silently kills the
// chart's knob.
func TestF3_SamplerEnvIsLive(t *testing.T) {
	setupWithEnv(t, "always_off", "")
	if rec, samp, _ := driveWithParent(t, traceIDLowTail); rec || samp {
		t.Fatalf("always_off still sampled (recording=%v sampled=%v)", rec, samp)
	}
}

func TestF3_SamplerEnvIsLive_AlwaysOn(t *testing.T) {
	setupWithEnv(t, "always_on", "")
	if rec, samp, _ := driveWithParent(t, traceIDHighTail); !rec || !samp {
		t.Fatalf("always_on did not sample (recording=%v sampled=%v)", rec, samp)
	}
}

// TestS1013_ParentBasedAlwaysOnSamplesEveryRootAndSampledParent — the shipped
// family (#1013). A root request (no traceparent: in-cluster callers, the
// LoadBalancer path) is always sampled, and a request whose parent says
// sampled=1 (the agent gateway today) is sampled even when its trace id hashes
// outside any ratio: the all-ff tail that traceidratio 0.05 DROPS in
// TestF3_TraceIDRatioIgnoresSampledParent, so this arm is red under the old
// chart's family.
func TestS1013_ParentBasedAlwaysOnSamplesEveryRootAndSampledParent(t *testing.T) {
	setupWithEnv(t, "parentbased_always_on", "")
	for i := 0; i < 32; i++ {
		if rec, samp, _ := drive(t, ""); !rec || !samp {
			t.Fatalf("root request %d not sampled (recording=%v sampled=%v): the SLIs would miss it", i, rec, samp)
		}
	}
	if rec, samp, _ := driveWithParent(t, traceIDHighTail); !rec || !samp {
		t.Fatalf("sampled=1 parent with an all-ff trace id not sampled (recording=%v sampled=%v)", rec, samp)
	}
}

// TestS1013_ParentBasedAlwaysOnDropsAnUnsampledParent pins the caveat the PR
// documents for installer#402: once the gateway samples at its edge and
// propagates sampled=0 for a request it dropped, parentbased_always_on drops
// snowplow's Server span for that request too. If this arm ever flips, the
// remedies described in tracing.go (SAMPLING) need re-reading.
func TestS1013_ParentBasedAlwaysOnDropsAnUnsampledParent(t *testing.T) {
	setupWithEnv(t, "parentbased_always_on", "")
	rec, samp, tid := drive(t, "00-"+traceIDLowTail+"-"+spanIDParent+"-00")
	if tid != traceIDLowTail {
		t.Fatalf("propagator did not carry the incoming trace id (got %s)", tid)
	}
	if rec || samp {
		t.Fatalf("sampled=0 parent was recording=%v sampled=%v under parentbased_always_on; the edge-sampling caveat no longer holds", rec, samp)
	}
}
