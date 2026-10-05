package tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// sentinel is a credential shape installer #394's JWT detector would NOT
// match, which is the point: the redaction must be structural (every query
// value), not pattern-matched against known token shapes.
const sentinel = "zq489-api-key-not-a-jwt"

// exportedSpans drives a real outbound GET through the production wrapper and
// returns what the SDK would have exported.
//
// Everything here is the production path: the transport comes from
// WrapTransport (the same call external_fetch.go makes for every RESTAction
// api-step), the provider comes from newTracerProvider (the same constructor
// Setup uses), and otelhttp resolves the provider through the global exactly
// as it does in the pod. Only the EXPORTER is swapped for an in-memory one —
// there is no seam in the span path itself.
func exportedSpans(t *testing.T, target string) []tracetest.SpanStub {
	t.Helper()

	t.Setenv(EnvTracingEnabled, "true")
	if !Enabled() {
		t.Fatalf("SETUP: tracing must resolve to enabled for WrapTransport to instrument")
	}

	exp := tracetest.NewInMemoryExporter()
	tp := newTracerProvider(exp)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	rt := WrapTransport(http.DefaultTransport)
	if rt == nil {
		t.Fatalf("SETUP: WrapTransport returned nil")
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+target, nil)
	if err != nil {
		t.Fatalf("SETUP: %v", err)
	}
	resp, err := (&http.Client{Transport: rt}).Do(req)
	if err != nil {
		t.Fatalf("SETUP: outbound GET failed: %v", err)
	}
	_ = resp.Body.Close()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("SETUP: flush: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) == 0 {
		t.Fatalf("SETUP: no span was exported — the arm would pass vacuously")
	}
	return spans
}

// TestS489_ClientSpanNeverCarriesAQueryCredential — the #489 falsifier.
//
// otelhttp records url.full WITH the query (it strips only userinfo), so a
// RESTAction api-step to `…?api_key=…` wrote that credential straight into
// otel_traces. RED without the redactor: the sentinel appears in url.full.
func TestS489_ClientSpanNeverCarriesAQueryCredential(t *testing.T) {
	spans := exportedSpans(t, "/x?api_key="+sentinel+"&page=2")

	sawURLAttr := false
	for _, s := range spans {
		for _, a := range s.Attributes {
			v := a.Value.Emit()
			if strings.Contains(v, sentinel) {
				t.Errorf("#489 RED: span %q attribute %q carries the credential: %s", s.Name, a.Key, v)
			}
			for _, k := range urlAttrKeys {
				if a.Key == k {
					sawURLAttr = true
					if !strings.Contains(v, "api_key=") {
						t.Errorf("the query KEY must survive so the span stays diagnostic: %s=%s", a.Key, v)
					}
					if !strings.Contains(v, "page=") {
						t.Errorf("every query key must survive, not just the credential-looking one: %s=%s", a.Key, v)
					}
				}
			}
		}
		if strings.Contains(s.Name, sentinel) {
			t.Errorf("#489 RED: the SPAN NAME carries the credential: %s", s.Name)
		}
	}
	if !sawURLAttr {
		t.Fatalf("NON-VACUITY: no span recorded a URL attribute %v — the arm proved nothing", urlAttrKeys)
	}
}

// TestS489_ClientSpanNeverCarriesURLUserinfo — the same span path for the
// other credential carrier. otelhttp strips userinfo itself today; this pins
// it so a dependency bump cannot quietly reintroduce it, and it shares
// redact.URL with #499 so both carriers answer to one implementation.
func TestS489_ClientSpanNeverCarriesURLUserinfo(t *testing.T) {
	for _, s := range exportedSpans(t, "/x?token="+sentinel) {
		for _, a := range s.Attributes {
			if v := a.Value.Emit(); strings.Contains(v, "@") && strings.Contains(v, sentinel) {
				t.Errorf("#489 RED: span %q attribute %q carries userinfo: %s", s.Name, a.Key, v)
			}
		}
	}
}

// TestS489_EveryTracerProviderInThisPackageCarriesTheRedactor is the
// structural guard: the redactor only protects spans that go through a
// provider built by newTracerProvider, so a bare sdktrace.NewTracerProvider
// anywhere in this package would be an unprotected export path. The helper's
// own body is the single permitted call site.
func TestS489_EveryTracerProviderInThisPackageCarriesTheRedactor(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("SETUP: %v", err)
	}
	found := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("SETUP: %v", err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			// Comments describe these calls (including the rule itself), they do
			// not make them: scan code only.
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			if strings.Contains(line, "sdktrace.NewTracerProvider(") {
				found++
				if name != "tracing.go" {
					t.Errorf("%s:%d constructs a TracerProvider outside newTracerProvider — "+
						"its spans would export url.full with the query in clear (#489): %s",
						name, i+1, strings.TrimSpace(line))
				}
			}
			// Every exporter registration must go through the redacting wrapper:
			// a bare WithBatcher(exp)/WithSyncer(exp) is an unprotected export path.
			for _, reg := range []string{"sdktrace.WithBatcher(", "sdktrace.WithSyncer("} {
				if strings.Contains(line, reg) && !strings.Contains(line, "redactingExporter{") {
					t.Errorf("%s:%d registers an exporter without redactingExporter — "+
						"spans would export url.full with the query in clear (#489): %s",
						name, i+1, strings.TrimSpace(line))
				}
			}
		}
	}
	if found == 0 {
		t.Fatalf("NON-VACUITY: no sdktrace.NewTracerProvider call was found at all — " +
			"the guard is scanning the wrong thing")
	}
}
