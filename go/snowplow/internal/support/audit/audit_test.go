package audit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/logtest"
)

// flattenRecords collapses the scope-grouped Recording returned by
// logtest.Recorder.Result() (v0.20.0: map[Scope][]Record) into a flat slice
// of the emitted records across all instrumentation scopes.
func flattenRecords(rec logtest.Recording) []logtest.Record {
	out := make([]logtest.Record, 0, len(rec))
	for _, recs := range rec {
		out = append(out, recs...)
	}
	return out
}

func TestSanitizeID(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"abc-123", "abc-123"},
		{"portal-req.42_a", "portal-req.42_a"},
		{"portal:req/42", ""},
		{"has spaces", ""},
		{"bad\nnewline", ""},
		{"quote\"", ""},
		{strings.Repeat("a", 129), ""},
		{strings.Repeat("a", 128), strings.Repeat("a", 128)},
	}
	for _, c := range cases {
		if got := SanitizeID(c.in); got != c.want {
			t.Errorf("SanitizeID(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestMiddlewareSeedsBaggageFromInbound verifies an inbound baggage
// session.id is preserved on the request context (no bespoke header).
func TestMiddlewareSeedsBaggageFromInbound(t *testing.T) {
	var seen string
	h := Middleware()(http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		seen = SessionID(req.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	// Simulate the propagator having already extracted the inbound baggage.
	req = req.WithContext(WithSessionID(req.Context(), "portal-abc-123"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen != "portal-abc-123" {
		t.Errorf("baggage session.id = %q, want %q", seen, "portal-abc-123")
	}
}

// TestMiddlewareMintsWhenAbsent verifies a session id is minted into baggage
// when nothing inbound provides one.
func TestMiddlewareMintsWhenAbsent(t *testing.T) {
	var seen string
	h := Middleware()(http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		seen = SessionID(req.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen == "" {
		t.Fatal("expected a minted session id in baggage, got empty")
	}
}

// TestMiddlewareRejectsHostileID verifies a non-token inbound id is replaced
// rather than propagated.
func TestMiddlewareRejectsHostileID(t *testing.T) {
	var seen string
	h := Middleware()(http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		seen = SessionID(req.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	// A hostile value that survives baggage encoding but not SanitizeID.
	req = req.WithContext(WithSessionID(req.Context(), "evil-injection"))
	// Overwrite with something SanitizeID rejects via raw baggage member.
	if m, err := baggage.NewMember(BaggageSessionKey, "evil%20injection"); err == nil {
		if bag, err := baggage.FromContext(req.Context()).SetMember(m); err == nil {
			req = req.WithContext(baggage.ContextWithBaggage(req.Context(), bag))
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen == "evil%20injection" || seen == "" {
		t.Errorf("hostile id must be replaced, got %q", seen)
	}
}

// TestEmitOTLPRecord verifies Emit produces one OTLP LogRecord with
// event.name=audit, INFO severity on success, the semconv attributes, and
// the session.id read from baggage.
func TestEmitOTLPRecord(t *testing.T) {
	rec := logtest.NewRecorder()
	e := New(rec.Logger("test"))

	ctx := WithSessionID(context.Background(), "corr-42")

	e.Emit(ctx, Event{
		Action:    "call",
		Verb:      "POST",
		Group:     "composition.krateo.io",
		Version:   "v1alpha1",
		Resource:  "fireworksapps",
		Name:      "demo",
		Namespace: "team-a",
		Outcome:   "success",
		Code:      200,
	})

	got := flattenRecords(rec.Result())
	if len(got) != 1 {
		t.Fatalf("expected exactly one emitted record, got %+v", got)
	}
	r := got[0]

	if r.Severity != log.SeverityInfo {
		t.Errorf("severity = %v, want Info", r.Severity)
	}

	attrs := map[string]log.Value{}
	for _, kv := range r.Attributes {
		attrs[kv.Key] = kv.Value
	}
	if v, ok := attrs["event.name"]; !ok || v.AsString() != EventName {
		t.Errorf("event.name = %v, want %q", attrs["event.name"], EventName)
	}
	if v, ok := attrs["krateo.action"]; !ok || v.AsString() != "call" {
		t.Errorf("krateo.action = %v, want call", attrs["krateo.action"])
	}
	if v, ok := attrs["k8s.resource.resource"]; !ok || v.AsString() != "fireworksapps" {
		t.Errorf("k8s.resource.resource = %v, want fireworksapps", attrs["k8s.resource.resource"])
	}
	if v, ok := attrs["outcome"]; !ok || v.AsString() != "success" {
		t.Errorf("outcome = %v, want success", attrs["outcome"])
	}
	if v, ok := attrs["session.id"]; !ok || v.AsString() != "corr-42" {
		t.Errorf("session.id = %v, want corr-42", attrs["session.id"])
	}
	if v, ok := attrs["http.request.method"]; !ok || v.AsString() != "POST" {
		t.Errorf("http.request.method = %v, want POST", attrs["http.request.method"])
	}
	if v, ok := attrs["http.response.status_code"]; !ok || v.AsInt64() != 200 {
		t.Errorf("http.response.status_code = %v, want 200", attrs["http.response.status_code"])
	}
}

// TestEmitFailureSeverity verifies a non-success outcome maps to Error.
func TestEmitFailureSeverity(t *testing.T) {
	rec := logtest.NewRecorder()
	e := New(rec.Logger("test"))
	e.Emit(context.Background(), Event{Action: "call", Outcome: "failure", Code: 500})

	got := flattenRecords(rec.Result())
	if len(got) != 1 {
		t.Fatalf("expected one record, got %+v", got)
	}
	if s := got[0].Severity; s != log.SeverityError {
		t.Errorf("severity = %v, want Error", s)
	}
}

// TestPackageEmitNoOpWhenUnset verifies the package-level Emit is a no-op
// when no default emitter is installed (default-off path).
func TestPackageEmitNoOpWhenUnset(t *testing.T) {
	defaultEmitter.Store(nil)
	// Must not panic.
	Emit(context.Background(), Event{Action: "call", Outcome: "success"})
}

// TestEmitRecordsSubresource (#282, arm 5) verifies a subresource write records
// k8s.resource.subresource for correlation, and that a whole-object call OMITS
// it (the add() empty-skip → backward-compatible record shape).
func TestEmitRecordsSubresource(t *testing.T) {
	rec := logtest.NewRecorder()
	New(rec.Logger("test")).Emit(context.Background(), Event{
		Action: "call", Verb: "PUT", Group: "example.com", Version: "v1",
		Resource: "widgets", Subresource: "status", Name: "w1", Namespace: "ns1",
		Outcome: "success", Code: 200,
	})
	got := flattenRecords(rec.Result())
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	attrs := map[string]log.Value{}
	for _, kv := range got[0].Attributes {
		attrs[kv.Key] = kv.Value
	}
	if v, ok := attrs["k8s.resource.subresource"]; !ok || v.AsString() != "status" {
		t.Errorf("k8s.resource.subresource = %v (present=%v), want status", attrs["k8s.resource.subresource"], ok)
	}

	// A whole-object call (no subresource) must OMIT the attribute entirely.
	rec2 := logtest.NewRecorder()
	New(rec2.Logger("test")).Emit(context.Background(), Event{
		Action: "call", Verb: "PUT", Resource: "widgets", Name: "w1",
		Namespace: "ns1", Outcome: "success", Code: 200,
	})
	g2 := flattenRecords(rec2.Result())
	if len(g2) != 1 {
		t.Fatalf("want 1 record, got %d", len(g2))
	}
	for _, kv := range g2[0].Attributes {
		if kv.Key == "k8s.resource.subresource" {
			t.Errorf("whole-object call must omit k8s.resource.subresource, got %v", kv.Value)
		}
	}
}
