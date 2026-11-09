package main

// main_telemetry_agentiko_claim_test.go — the browser RUM ingest forwards
// krateo.agentiko.claim.unbacked (Diego's decision, contract agreed with the
// frontend session 2026-11-09): two string attributes,
// krateo.agentiko.claim.family and krateo.agentiko.claim.outcome, each kept
// only when it matches ^[a-z][a-z0-9_]{0,31}$, and NO other attribute (no
// http.*, no session or conversation id, no identity). An attribute that fails
// is dropped and counted, never forwarded; the span is still forwarded, the
// same per-attribute rule as every other span (build.go buildSpan). Driven
// through the same real router, ingest, forwarder and in-process collector as
// main_telemetry_1015_test.go.
//
// The rum counters are process-global, so every arm reads deltas, and each
// post uses its own user so no per-user limit applies. Names, keys and
// patterns are literals, not rum's constants, so a change to the package
// cannot move the arms with it.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	claimSpan       = "krateo.agentiko.claim.unbacked"
	claimFamilyKey  = "krateo.agentiko.claim.family"
	claimOutcomeKey = "krateo.agentiko.claim.outcome"
)

// claimSpanWith is a krateo.agentiko.claim.unbacked span carrying attrs.
func claimSpanWith(attrs ...map[string]any) otlpSpan {
	return otlpSpan{name: claimSpan, attrs: attrs}
}

// stringAttrValue returns a's value when it is a string AnyValue.
func stringAttrValue(t *testing.T, s *tracepb.Span, key string) (string, bool) {
	t.Helper()
	a := attr(s, key)
	if a == nil {
		return "", false
	}
	v, ok := a.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
	if !ok {
		t.Errorf("%s: %s forwarded as %T, want a string", s.GetName(), key, a.GetValue().GetValue())
		return "", false
	}
	return v.StringValue, true
}

// The span with a valid family and outcome is forwarded with EXACTLY those two
// attributes, as the strings sent, and the kept span fields as sent: ids,
// kind, times and the status code (never its message). Its envelope is the
// one every forwarded span gets: resource service.name=krateo-frontend only,
// scope snowplow/rum-ingest. The pattern's bounds are both accepted: one
// character, and 32.
//
// MUTATIONS (RED, shown in the PR): the span's row removed from spanAllowlist
// (the span is dropped_name: this arm is red on main before the change); the
// row's check for either key replaced by one that never passes.
func TestRUMAgentikoClaim_AcceptedWithExactlyFamilyAndOutcome(t *testing.T) {
	max32 := "a" + strings.Repeat("b_9", 10) + "z" // 32 characters
	if len(max32) != 32 {
		t.Fatalf("SETUP: max32 is %d characters", len(max32))
	}
	values := [][2]string{
		{"grounding", "unbacked"},
		{"a", "z"},
		{max32, "no_source_0"},
	}
	var spans []otlpSpan
	for _, v := range values {
		spans = append(spans, claimSpanWith(strAttr(claimFamilyKey, v[0]), strAttr(claimOutcomeKey, v[1])))
	}

	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	// The browser's kind and status, not otlpBody's defaults: INTERNAL (1) and
	// ERROR (2), so a forwarded value can only be the one sent.
	var body map[string]any
	if err := json.Unmarshal(otlpBody(t, rum.ServiceName, []map[string]any{strAttr("enduser.id", "zq-claim-person")}, spans...), &body); err != nil {
		t.Fatal(err)
	}
	sent := body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	for _, s := range sent {
		s.(map[string]any)["kind"] = 1
		s.(map[string]any)["status"] = map[string]any{"code": 2, "message": "free text that must not be forwarded"}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, "claim-accepted"), raw); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "{}" {
		t.Fatalf("status %d body %s, want 200 {}", rec.Code, rec.Body)
	}
	after := rum.Read()
	if d := after.Spans["dropped_name"] - before.Spans["dropped_name"]; d != 0 {
		t.Fatalf("dropped_name delta %d, want 0: %s is not accepted", d, claimSpan)
	}
	if d := after.Attributes - before.Attributes; d != 0 {
		t.Errorf("attributes_dropped delta %d, want 0", d)
	}

	got := awaitExport(t, c)
	rs := got.GetResourceSpans()
	if len(rs) != 1 || len(rs[0].GetScopeSpans()) != 1 {
		t.Fatalf("forwarded %d resource spans, want exactly 1 with 1 scope", len(rs))
	}
	if ra := rs[0].GetResource().GetAttributes(); len(ra) != 1 || ra[0].GetKey() != "service.name" ||
		ra[0].GetValue().GetStringValue() != "krateo-frontend" {
		t.Errorf("forwarded resource attributes %v, want only service.name=krateo-frontend", ra)
	}
	if sc := rs[0].GetScopeSpans()[0].GetScope(); sc.GetName() != "snowplow/rum-ingest" || len(sc.GetAttributes()) != 0 || sc.GetVersion() != "" {
		t.Errorf("forwarded scope %v, want only name=snowplow/rum-ingest", sc)
	}
	out := rs[0].GetScopeSpans()[0].GetSpans()
	if len(out) != len(values) {
		t.Fatalf("forwarded %d spans, want %d", len(out), len(values))
	}
	for i, s := range out {
		if s.GetName() != claimSpan {
			t.Errorf("span %d is %q, want %q", i, s.GetName(), claimSpan)
		}
		if got, want := attrKeys(s), claimFamilyKey+","+claimOutcomeKey; got != want {
			t.Errorf("span %d forwards attributes [%s], want [%s]", i, got, want)
		}
		if f, _ := stringAttrValue(t, s, claimFamilyKey); f != values[i][0] {
			t.Errorf("span %d: family %q, want %q", i, f, values[i][0])
		}
		if o, _ := stringAttrValue(t, s, claimOutcomeKey); o != values[i][1] {
			t.Errorf("span %d: outcome %q, want %q", i, o, values[i][1])
		}
		if got := hex.EncodeToString(s.GetTraceId()); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("span %d: traceId %s", i, got)
		}
		if got, want := hex.EncodeToString(s.GetSpanId()), fmt.Sprintf("00f067aa0ba9%04x", i+1); got != want {
			t.Errorf("span %d: spanId %s, want %s", i, got, want)
		}
		if got := hex.EncodeToString(s.GetParentSpanId()); got != "00f067aa0ba90200" {
			t.Errorf("span %d: parentSpanId %s", i, got)
		}
		if s.GetKind() != tracepb.Span_SPAN_KIND_INTERNAL {
			t.Errorf("span %d: kind %v, want the sent INTERNAL", i, s.GetKind())
		}
		if s.GetStartTimeUnixNano() != 1761800000000000000 || s.GetEndTimeUnixNano() != 1761800000250000000 {
			t.Errorf("span %d: times %d..%d, want the sent ones", i, s.GetStartTimeUnixNano(), s.GetEndTimeUnixNano())
		}
		if s.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR || s.GetStatus().GetMessage() != "" {
			t.Errorf("span %d: status %v, want code ERROR and no message", i, s.GetStatus())
		}
		if len(s.GetEvents()) != 0 || len(s.GetLinks()) != 0 || s.GetTraceState() != "" {
			t.Errorf("span %d carries a non-allowlisted field: %v", i, s)
		}
	}
}

// A value that does not match ^[a-z][a-z0-9_]{0,31}$, or is not exactly a
// string, drops THAT attribute and is counted; the span is forwarded with its
// other, valid attribute. Each bad value is tried on family (with a valid
// outcome) and on outcome (with a valid family).
//
// MUTATIONS (RED, shown in the PR): the check accepting upper case; the
// length bound raised to 33; the first character allowed to be a digit; the
// check accepting the empty string; the one-member check removed.
func TestRUMAgentikoClaim_InvalidValueDropsOnlyThatAttribute(t *testing.T) {
	bad := []struct {
		name  string
		value map[string]any
	}{
		{"upper case", map[string]any{"stringValue": "Grounding"}},
		{"all upper case", map[string]any{"stringValue": "UNBACKED"}},
		{"33 characters", map[string]any{"stringValue": "a" + strings.Repeat("b", 32)}},
		{"leading digit", map[string]any{"stringValue": "1grounding"}},
		{"leading underscore", map[string]any{"stringValue": "_grounding"}},
		{"empty", map[string]any{"stringValue": ""}},
		{"hyphen", map[string]any{"stringValue": "no-source"}},
		{"dot", map[string]any{"stringValue": "no.source"}},
		{"space", map[string]any{"stringValue": "no source"}},
		{"trailing newline", map[string]any{"stringValue": "grounding\n"}},
		{"non-ASCII letter", map[string]any{"stringValue": "fondatezza_è"}},
		{"int", map[string]any{"intValue": "1"}},
		{"bool", map[string]any{"boolValue": true}},
		{"double", map[string]any{"doubleValue": 1.5}},
		{"array of a valid string", map[string]any{"arrayValue": map[string]any{"values": []map[string]any{{"stringValue": "grounding"}}}}},
		{"valid string beside an int", map[string]any{"stringValue": "grounding", "intValue": "1"}},
		{"valid string beside a bytes", map[string]any{"stringValue": "grounding", "bytesValue": "eA=="}},
	}
	var spans []otlpSpan
	for _, b := range bad {
		spans = append(spans,
			claimSpanWith(rawAttr(claimFamilyKey, b.value), strAttr(claimOutcomeKey, "unbacked")),
			claimSpanWith(strAttr(claimFamilyKey, "grounding"), rawAttr(claimOutcomeKey, b.value)),
		)
	}
	out, dropped := postAndExport(t, "claim-bad-values", spans...)
	if want := uint64(2 * len(bad)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per bad value)", dropped, want)
	}
	for i, s := range out {
		b := bad[i/2]
		kept, keptValue := claimOutcomeKey, "unbacked"
		if i%2 == 1 {
			kept, keptValue = claimFamilyKey, "grounding"
		}
		if got := attrKeys(s); got != kept {
			t.Errorf("%s on %s: forwards [%s], want [%s]", b.name, map[bool]string{true: "outcome", false: "family"}[i%2 == 1], got, kept)
		}
		if v, _ := stringAttrValue(t, s, kept); v != keptValue {
			t.Errorf("%s: the valid %s was not forwarded as sent (%q)", b.name, kept, v)
		}
	}
}

// No attribute other than family and outcome is forwarded on the span, even
// one allowlisted on another span (the http.* keys, the abandoned mark) and
// even a key that looks like the contract's (krateo.agentiko.session.id, a
// case or whitespace variant of family). Each is dropped and counted.
//
// MUTATION (RED, shown in the PR): the span's row carrying the http.* keys
// like the page-level rows.
func TestRUMAgentikoClaim_ExtraAttributesDropped(t *testing.T) {
	extra := []map[string]any{
		strAttr("krateo.agentiko.session.id", "s_123"),
		strAttr("krateo.agentiko.conversation.id", "c_123"),
		strAttr("krateo.agentiko.claim.text", "unbacked"),
		strAttr("enduser.id", "zq-claim-person"),
		strAttr("user.name", "zq-claim-person"),
		strAttr("session.id", "s_123"),
		strAttr("http.route", "/agentiko"),
		strAttr("http.request.method", "POST"),
		intAttr("http.response.status_code", 200),
		boolAttr("krateo.interaction.abandoned", true),
		strAttr("Krateo.Agentiko.Claim.Family", "grounding"),
		strAttr("krateo.agentiko.claim.family ", "grounding"),
	}
	s := claimSpanWith(append([]map[string]any{
		strAttr(claimFamilyKey, "grounding"), strAttr(claimOutcomeKey, "unbacked"),
	}, extra...)...)
	out, dropped := postAndExport(t, "claim-extra-attrs", s)
	if want := uint64(len(extra)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (every extra key)", dropped, want)
	}
	if got, want := attrKeys(out[0]), claimFamilyKey+","+claimOutcomeKey; got != want {
		t.Errorf("forwards [%s], want [%s]", got, want)
	}
}

// The span name matches EXACTLY: a case variant, a suffix, a prefix of it or
// surrounding whitespace is an unknown name, dropped whole (dropped_name). A
// valid claim span under a resource whose service.name is not krateo-frontend
// is dropped whole too (dropped_service).
//
// MUTATIONS (RED, shown in the PR): the span-name lookup ignoring case; the
// span-name lookup matching a prefix.
func TestRUMAgentikoClaim_NameVariantsNotAccepted(t *testing.T) {
	variants := []string{
		"Krateo.Agentiko.Claim.Unbacked", "KRATEO.AGENTIKO.CLAIM.UNBACKED", "krateo.agentiko.claim.Unbacked",
		"krateo.agentiko.claim.unbacked.v2", "krateo.agentiko.claim.unbacked_x", "krateo.agentiko.claim.unbackedx",
		"krateo.agentiko.claim", "krateo.agentiko.claim.backed",
		"krateo.agentiko.claim.unbacked ", " krateo.agentiko.claim.unbacked",
	}
	valid := []map[string]any{strAttr(claimFamilyKey, "grounding"), strAttr(claimOutcomeKey, "unbacked")}
	var spans []otlpSpan
	for _, n := range variants {
		spans = append(spans, otlpSpan{name: n, attrs: valid})
	}
	spans = append(spans, claimSpanWith(valid...))

	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	tok := mintRUMToken(t, "claim-name-variants")
	before := rum.Read()
	if rec := postRUM(t, h, tok, otlpBody(t, "evil-service", nil, claimSpanWith(valid...))); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"rejectedSpans":"1"`) {
		t.Fatalf("wrong service: status %d body %s, want 200 with 1 rejected span", rec.Code, rec.Body)
	}
	rec := postRUM(t, h, tok, otlpBody(t, rum.ServiceName, nil, spans...))
	if want := fmt.Sprintf(`"rejectedSpans":"%d"`, len(variants)); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("status %d body %s, want 200 with %s", rec.Code, rec.Body, want)
	}
	after := rum.Read()
	if d := after.Spans["dropped_name"] - before.Spans["dropped_name"]; d != uint64(len(variants)) {
		t.Errorf("dropped_name delta %d, want %d (%q)", d, len(variants), variants)
	}
	if d := after.Spans["dropped_service"] - before.Spans["dropped_service"]; d != 1 {
		t.Errorf("dropped_service delta %d, want 1", d)
	}
	got := awaitExport(t, c).GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(got) != 1 || got[0].GetName() != claimSpan {
		t.Fatalf("forwarded %d spans (%v), want only the exact %s", len(got), got, claimSpan)
	}
}

// The other five spans are unchanged: each still forwards its own allowlisted
// attributes exactly as before, and the claim keys, even with values the
// claim span would accept, are dropped and counted on every one of them.
//
// MUTATION (RED, shown in the PR): the claim keys added to every row.
func TestRUMAgentikoClaim_OtherFiveSpansUnchanged(t *testing.T) {
	type row struct {
		name  string
		route string
		want  string
	}
	http3 := "http.route,http.request.method,http.response.status_code"
	rows := []row{
		{"krateo.widget.fetch", "/call", http3},
		{"krateo.page.load", "/compositions/{namespace}/{name}", http3},
		{"krateo.composition.open", "/compositions/{namespace}/{name}", http3 + ",krateo.interaction.abandoned"},
		{"krateo.form.submit", "/compositions/{namespace}/{name}", http3 + ",krateo.interaction.abandoned"},
		{"krateo.page.publish", "/compositions/{namespace}/{name}", http3 + ",krateo.interaction.abandoned"},
	}
	var spans []otlpSpan
	for _, r := range rows {
		spans = append(spans, otlpSpan{name: r.name, attrs: []map[string]any{
			strAttr("http.route", r.route),
			strAttr("http.request.method", "GET"),
			intAttr("http.response.status_code", 200),
			boolAttr("krateo.interaction.abandoned", true),
			strAttr(claimFamilyKey, "grounding"),
			strAttr(claimOutcomeKey, "unbacked"),
		}})
	}
	out, dropped := postAndExport(t, "claim-other-five", spans...)
	// Per span: the two claim keys; on widget.fetch and page.load the
	// abandoned mark too.
	if want := uint64(2*len(rows) + 2); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d", dropped, want)
	}
	for i, s := range out {
		if s.GetName() != rows[i].name {
			t.Fatalf("span %d is %s, want %s", i, s.GetName(), rows[i].name)
		}
		if got := attrKeys(s); got != rows[i].want {
			t.Errorf("%s forwards [%s], want [%s]", s.GetName(), got, rows[i].want)
		}
	}
}
