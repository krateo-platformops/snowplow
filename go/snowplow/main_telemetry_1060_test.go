package main

// main_telemetry_1060_test.go — #1060: the browser RUM ingest forwards
// krateo.interaction.abandoned, the frontend's mark for an interaction the
// user left before it finished (excluded from the latency SLI, still counted
// for throughput and error rate). The contract (frontend owner, 2026-11-01):
// a bool, only ever true, only on krateo.composition.open, krateo.form.submit
// and krateo.page.publish. Driven through the same real router, ingest,
// forwarder and in-process collector as main_telemetry_1015_test.go.
//
// The rum counters are process-global, so every arm reads deltas, and each
// post uses its own user so no per-user limit applies.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// interactionSpans are the only span names the contract allows the key on.
// Literals, not rum's constants, so a change to the package cannot move the
// arm with it.
var interactionSpans = []string{"krateo.composition.open", "krateo.form.submit", "krateo.page.publish"}

const abandonedKey = "krateo.interaction.abandoned"

func boolAttr(k string, v bool) map[string]any {
	return map[string]any{"key": k, "value": map[string]any{"boolValue": v}}
}

func rawAttr(k string, value map[string]any) map[string]any {
	return map[string]any{"key": k, "value": value}
}

// postAndExport posts spans as user, requires 200, and returns the forwarded
// spans and the attributes_dropped delta.
func postAndExport(t *testing.T, user string, spans ...otlpSpan) ([]*tracepb.Span, uint64) {
	t.Helper()
	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	before := rum.Read().Attributes
	if rec := postRUM(t, h, mintRUMToken(t, user), otlpBody(t, rum.ServiceName, nil, spans...)); rec.Code != http.StatusOK {
		t.Fatalf("status %d (%s), want 200", rec.Code, rec.Body)
	}
	dropped := rum.Read().Attributes - before
	got := awaitExport(t, c).GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(got) != len(spans) {
		t.Fatalf("forwarded %d spans, want %d", len(got), len(spans))
	}
	return got, dropped
}

func attr(s *tracepb.Span, key string) *commonpb.KeyValue {
	for _, a := range s.GetAttributes() {
		if a.GetKey() == key {
			return a
		}
	}
	return nil
}

func attrKeys(s *tracepb.Span) string {
	var keys []string
	for _, a := range s.GetAttributes() {
		keys = append(keys, a.GetKey())
	}
	return strings.Join(keys, ",")
}

// On each interaction span, krateo.interaction.abandoned=true is forwarded as
// a BOOL true (not a string, not an int, not a status), and the span's other
// attributes are still filtered exactly as before: the http.* keys kept,
// enduser.id, url.full and a lower-case method dropped and counted.
//
// MUTATIONS (RED, shown in the PR): the key removed from the interaction
// rows; boolTrue forwarding a string "true".
func TestS1060_AbandonedForwardedOnInteractionSpans(t *testing.T) {
	var spans []otlpSpan
	for _, name := range interactionSpans {
		spans = append(spans, otlpSpan{name: name, attrs: []map[string]any{
			strAttr("http.route", "/compositions/{namespace}/{name}"),
			strAttr("http.request.method", "post"), // not a method token: dropped
			intAttr("http.response.status_code", 200),
			boolAttr(abandonedKey, true),
			strAttr("enduser.id", "zq1060-person"),
			strAttr("url.full", "https://portal/compositions/team-a/prod-db"),
		}})
	}
	out, dropped := postAndExport(t, "abandoned-forwarded", spans...)
	if want := uint64(3 * len(interactionSpans)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (method, enduser.id, url.full per span)", dropped, want)
	}
	for i, s := range out {
		if s.GetName() != interactionSpans[i] {
			t.Fatalf("span %d is %s, want %s", i, s.GetName(), interactionSpans[i])
		}
		if got, want := attrKeys(s), "http.route,http.response.status_code,"+abandonedKey; got != want {
			t.Errorf("%s forwards attributes [%s], want [%s]", s.GetName(), got, want)
		}
		a := attr(s, abandonedKey)
		if a == nil {
			t.Errorf("%s: %s not forwarded", s.GetName(), abandonedKey)
			continue
		}
		b, isBool := a.GetValue().GetValue().(*commonpb.AnyValue_BoolValue)
		if !isBool || !b.BoolValue {
			t.Errorf("%s: %s forwarded as %T %v, want bool true", s.GetName(), abandonedKey, a.GetValue().GetValue(), a.GetValue())
		}
		if s.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_UNSET {
			t.Errorf("%s: status %v, want the sent UNSET (abandoned is never a status)", s.GetName(), s.GetStatus().GetCode())
		}
	}
}

// On krateo.widget.fetch (a child span) and krateo.page.load the key is not
// valid: it is dropped and counted, and the span's allowlisted attributes are
// still forwarded.
//
// MUTATION (RED, shown in the PR): the key allowed on every span name.
func TestS1060_AbandonedDroppedOnNonInteractionSpans(t *testing.T) {
	wf := widgetFetch()
	wf.attrs = append(wf.attrs, boolAttr(abandonedKey, true))
	pl := otlpSpan{name: "krateo.page.load", attrs: []map[string]any{
		strAttr("http.route", "/compositions/{namespace}/{name}"),
		boolAttr(abandonedKey, true),
	}}
	out, dropped := postAndExport(t, "abandoned-wrong-span", wf, pl)
	if dropped != 2 {
		t.Errorf("attributes_dropped delta %d, want 2 (the key on each span)", dropped)
	}
	for _, s := range out {
		if attr(s, abandonedKey) != nil {
			t.Errorf("%s forwards %s; it is valid only on the interaction spans", s.GetName(), abandonedKey)
		}
	}
	if got, want := attrKeys(out[0]), "http.route,http.request.method,http.response.status_code"; got != want {
		t.Errorf("krateo.widget.fetch forwards [%s], want [%s]", got, want)
	}
	if got, want := attrKeys(out[1]), "http.route"; got != want {
		t.Errorf("krateo.page.load forwards [%s], want [%s]", got, want)
	}
}

// Only bool true is the key's value. false, a string "true", an int, a double,
// an array holding true, and a value carrying a string beside the bool are
// each dropped and counted, never coerced; the span is still forwarded with
// its other attributes.
//
// MUTATIONS (RED, shown in the PR): boolTrue accepting false; boolTrue
// accepting a stringValue "true".
func TestS1060_AbandonedNonTrueValuesDropped(t *testing.T) {
	values := []struct {
		name  string
		value map[string]any
	}{
		{"bool false", map[string]any{"boolValue": false}},
		{`string "true"`, map[string]any{"stringValue": "true"}},
		{"int 1", map[string]any{"intValue": "1"}},
		{"double 1", map[string]any{"doubleValue": 1.0}},
		{"array [true]", map[string]any{"arrayValue": map[string]any{"values": []map[string]any{{"boolValue": true}}}}},
		{`bool true beside string "x"`, map[string]any{"boolValue": true, "stringValue": "x"}},
	}
	var spans []otlpSpan
	for i, v := range values {
		spans = append(spans, otlpSpan{name: interactionSpans[i%len(interactionSpans)], attrs: []map[string]any{
			strAttr("http.route", "/compositions"),
			rawAttr(abandonedKey, v.value),
		}})
	}
	out, dropped := postAndExport(t, "abandoned-bad-values", spans...)
	if want := uint64(len(values)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per bad value)", dropped, want)
	}
	for i, s := range out {
		if a := attr(s, abandonedKey); a != nil {
			t.Errorf("%s: forwarded %s=%v for a %s", s.GetName(), abandonedKey, a.GetValue(), values[i].name)
		}
		if got := attrKeys(s); got != "http.route" {
			t.Errorf("%s (%s): forwards [%s], want [http.route]", s.GetName(), values[i].name, got)
		}
	}
}

// Absent means not abandoned: an interaction span without the key is
// forwarded exactly as before, with nothing added and nothing counted.
func TestS1060_AbandonedAbsentIsUnchanged(t *testing.T) {
	var spans []otlpSpan
	for _, name := range interactionSpans {
		spans = append(spans, otlpSpan{name: name, attrs: []map[string]any{
			strAttr("http.route", "/compositions"),
			strAttr("http.request.method", "POST"),
			intAttr("http.response.status_code", 201),
		}})
	}
	out, dropped := postAndExport(t, "abandoned-absent", spans...)
	if dropped != 0 {
		t.Errorf("attributes_dropped delta %d, want 0", dropped)
	}
	for _, s := range out {
		if got, want := attrKeys(s), "http.route,http.request.method,http.response.status_code"; got != want {
			t.Errorf("%s forwards [%s], want [%s]", s.GetName(), got, want)
		}
	}
}

// A boolValue whose JSON is not a bool is not OTLP/JSON (the proto3 JSON
// mapping of a bool is true or false), exactly like any other mistyped field:
// the request is refused 400 and counted dropped_malformed.
func TestS1060_BoolValueNotABoolIsMalformed(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	body := otlpBody(t, rum.ServiceName, nil, otlpSpan{name: "krateo.form.submit", attrs: []map[string]any{
		rawAttr(abandonedKey, map[string]any{"boolValue": "true"}),
	}})
	before := rum.Read().Requests["dropped_malformed"]
	if rec := postRUM(t, h, mintRUMToken(t, "abandoned-malformed"), body); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if d := rum.Read().Requests["dropped_malformed"] - before; d != 1 {
		t.Fatalf("dropped_malformed delta %d, want 1", d)
	}
	if n := c.hits.Load(); n != 0 {
		t.Fatalf("collector received %d posts from a malformed request", n)
	}
}
