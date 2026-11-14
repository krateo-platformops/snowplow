package main

// main_telemetry_agentiko_a2a_test.go — #1398: the browser RUM ingest
// forwards krateo.agentiko.a2a, the portal's fetch span for one of the
// agent's JSON-RPC POSTs under the Agentiko A2A base (contract agreed with
// the frontend, 2026-11-13, both comments on #1398; frontend#725, 1.8.43).
// It carries exactly four attributes, each with its own check:
// http.request.method (exactly POST), http.response.status_code (100–599),
// http.route (the template check, not snowplow's registered routes: the A2A
// route is the gateway's) and krateo.a2a.method (message/stream,
// message/send, tasks/get, tasks/resubscribe). No other key, so no identity,
// session or conversation key. An attribute that fails is dropped and
// counted; the span is still forwarded (build.go buildSpan). Driven through
// the same real router, ingest, forwarder and in-process collector as
// main_telemetry_1015_test.go.
//
// The rum counters are process-global, so every arm reads deltas, and each
// post uses its own user so no per-user limit applies. Names, keys and values
// are literals, not rum's constants, so a change to the package cannot move
// the arms with it.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	a2aSpan      = "krateo.agentiko.a2a"
	a2aMethodKey = "krateo.a2a.method"
	a2aRoute     = "/api/a2a/{namespace}/{agent}"
	// a2aKeys is the forwarded attribute list of a valid span, in the order
	// a2aSpanWith sends them.
	a2aKeys = "http.request.method,http.response.status_code,http.route," + a2aMethodKey
)

// a2aSpanWith is a krateo.agentiko.a2a span with the four contract
// attributes (POST, 200, the template route, method), each replaced by the
// matching entry of override when one has the same key, then extra appended.
func a2aSpanWith(method string, override []map[string]any, extra ...map[string]any) otlpSpan {
	attrs := []map[string]any{
		strAttr("http.request.method", "POST"),
		intAttr("http.response.status_code", 200),
		strAttr("http.route", a2aRoute),
		strAttr(a2aMethodKey, method),
	}
	for _, o := range override {
		for i, a := range attrs {
			if a["key"] == o["key"] {
				attrs[i] = o
			}
		}
	}
	return otlpSpan{name: a2aSpan, attrs: append(attrs, extra...)}
}

// postA2AAndExport posts body as user, requires 200 {} (every span
// forwarded), and returns the forwarded spans, the attributes_dropped and
// widget_routes_unregistered deltas.
func postA2AAndExport(t *testing.T, user string, body []byte, want int) (out []*tracepb.Span, dropped, unregistered uint64) {
	t.Helper()
	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, user), body); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "{}" {
		t.Fatalf("status %d body %s, want 200 {}", rec.Code, rec.Body)
	}
	after := rum.Read()
	if d := after.Spans["dropped_name"] - before.Spans["dropped_name"]; d != 0 {
		t.Fatalf("dropped_name delta %d, want 0", d)
	}
	out = awaitExport(t, c).GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(out) != want {
		t.Fatalf("forwarded %d spans, want %d", len(out), want)
	}
	return out, after.Attributes - before.Attributes, after.UnregisteredWidgetRoutes - before.UnregisteredWidgetRoutes
}

// A valid span is forwarded with EXACTLY the four contract attributes, as
// sent, for each of the four A2A methods (tasks/resubscribe included, the
// second contract comment), and nothing is dropped.
//
// MUTATIONS (RED, shown in the PR): the span's row removed from spanAllowlist
// (dropped_name: this arm is red on main before the change); the row's check
// for any one of the four keys replaced by one that never passes; a2aMethods
// without tasks/resubscribe.
func TestRUMAgentikoA2A_ForwardedWithExactlyFourAttributes(t *testing.T) {
	methods := []string{"message/stream", "message/send", "tasks/get", "tasks/resubscribe"}
	var spans []otlpSpan
	for _, m := range methods {
		spans = append(spans, a2aSpanWith(m, nil))
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-accepted", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if dropped != 0 {
		t.Errorf("attributes_dropped delta %d, want 0", dropped)
	}
	for i, s := range out {
		if s.GetName() != a2aSpan {
			t.Errorf("span %d is %q, want %q", i, s.GetName(), a2aSpan)
		}
		if got := attrKeys(s); got != a2aKeys {
			t.Errorf("%s: forwards [%s], want [%s]", methods[i], got, a2aKeys)
		}
		if v, _ := stringAttrValue(t, s, "http.request.method"); v != "POST" {
			t.Errorf("%s: method %q, want POST", methods[i], v)
		}
		if v := attr(s, "http.response.status_code").GetValue().GetIntValue(); v != 200 {
			t.Errorf("%s: status %d, want 200", methods[i], v)
		}
		if v, _ := stringAttrValue(t, s, "http.route"); v != a2aRoute {
			t.Errorf("%s: route %q, want %q", methods[i], v, a2aRoute)
		}
		if v, _ := stringAttrValue(t, s, a2aMethodKey); v != methods[i] {
			t.Errorf("a2a method %q, want %q", v, methods[i])
		}
	}
}

// http.request.method on this span is exactly POST: GET (the availability
// probe, never exported by contract) and every other value is dropped and
// counted, and the span is forwarded with its other three attributes. The
// same values on krateo.page.load show the difference is this span's check,
// not methodToken: GET, PUT and OPTIONS pass there.
//
// MUTATIONS (RED, shown in the PR): the row's method check replaced by
// methodToken (GET, PUT, OPTIONS pass); postMethod's POST comparison removed
// (every string passes); postMethod's one-member check removed.
func TestRUMAgentikoA2A_NonPostMethodDropped(t *testing.T) {
	bad := []struct {
		name  string
		value map[string]any
	}{
		{"GET", map[string]any{"stringValue": "GET"}},
		{"PUT", map[string]any{"stringValue": "PUT"}},
		{"OPTIONS", map[string]any{"stringValue": "OPTIONS"}},
		{"lower-case post", map[string]any{"stringValue": "post"}},
		{"POST with a space", map[string]any{"stringValue": "POST "}},
		{"empty", map[string]any{"stringValue": ""}},
		{"int", map[string]any{"intValue": "1"}},
		{"POST beside an int", map[string]any{"stringValue": "POST", "intValue": "1"}},
	}
	var spans []otlpSpan
	for _, b := range bad {
		spans = append(spans, a2aSpanWith("message/send", []map[string]any{rawAttr("http.request.method", b.value)}))
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-bad-method", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if want := uint64(len(bad)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per bad method)", dropped, want)
	}
	for i, s := range out {
		if got, want := attrKeys(s), "http.response.status_code,http.route,"+a2aMethodKey; got != want {
			t.Errorf("%s: forwards [%s], want [%s]", bad[i].name, got, want)
		}
	}

	// Control: methodToken accepts GET, PUT and OPTIONS on a page span.
	var controls []otlpSpan
	for _, m := range []string{"GET", "PUT", "OPTIONS"} {
		controls = append(controls, otlpSpan{name: "krateo.page.load", attrs: []map[string]any{strAttr("http.request.method", m)}})
	}
	if _, d := postAndExport(t, "a2a-method-control", controls...); d != 0 {
		t.Errorf("CONTROL: krateo.page.load dropped %d methods, want 0", d)
	}
}

// http.route on this span is checked by routeTemplate, the page-span check,
// not by snowplow's registered routes (the A2A route is the gateway's).
//
// WHAT routeTemplate REALLY DOES with a concrete path: it checks syntax only
// (characters, length, token-shaped segments), so it CANNOT tell the concrete
// path /api/a2a/ns1/agent1 from the template /api/a2a/{namespace}/{agent}.
// The arm pins that: the concrete path is forwarded. The contract's "never
// the concrete path" is the frontend's guarantee, exactly as for the page
// spans (howto/rum-ingest.md, "The limit of the page-span checks"). What the
// check does refuse, it refuses here: a query string, a percent-escape, a
// token-shaped segment, a relative path; each is dropped and counted.
//
// MUTATIONS (RED, shown in the PR): the row's route check replaced by
// registeredRoute (the template and concrete path are dropped); the row's
// route check replaced by a pass-through (the refused values pass).
func TestRUMAgentikoA2A_RouteIsTheTemplateCheck(t *testing.T) {
	const jwtSegment = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9" // gitleaks:allow
	refused := []string{
		"/api/a2a/ns1/agent1?token=x",
		"/api/a2a/ns%2F1/agent1",
		"/api/a2a/ns1/" + jwtSegment,
		"api/a2a/{namespace}/{agent}",
	}
	accepted := []string{a2aRoute, "/api/a2a/ns1/agent1"}
	var spans []otlpSpan
	for _, r := range append(append([]string{}, accepted...), refused...) {
		spans = append(spans, a2aSpanWith("tasks/get", []map[string]any{strAttr("http.route", r)}))
	}
	out, dropped, unregistered := postA2AAndExport(t, "a2a-route", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if want := uint64(len(refused)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per refused route)", dropped, want)
	}
	if unregistered != 0 {
		t.Errorf("widget_routes_unregistered delta %d, want 0: the a2a route is not a widget route", unregistered)
	}
	for i, r := range accepted {
		if v, _ := stringAttrValue(t, out[i], "http.route"); v != r {
			t.Errorf("route %q forwarded as %q, want it as sent", r, v)
		}
	}
	for i, r := range refused {
		if a := attr(out[len(accepted)+i], "http.route"); a != nil {
			t.Errorf("route %q forwarded, want it dropped", r)
		}
	}
}

// http.response.status_code on this span is an integer in 100–599 (the
// contract, second comment on #1398; howto/rum-ingest.md): 0, 99 and 600,
// and a value that is not an integer (the string "200", a double) are each
// dropped and counted, and the span is forwarded with its other three
// attributes (POST, the template route, the a2a method) as sent.
//
// MUTATION (RED, shown in the PR): the row's status check replaced by one
// that accepts any integer (0, 99 and 600 are forwarded).
func TestRUMAgentikoA2A_BadStatusCodeDropped(t *testing.T) {
	bad := []struct {
		name  string
		value map[string]any
	}{
		{"0", map[string]any{"intValue": "0"}},
		{"99", map[string]any{"intValue": "99"}},
		{"600", map[string]any{"intValue": "600"}},
		{"string 200", map[string]any{"stringValue": "200"}},
		{"double 200", map[string]any{"doubleValue": 200.0}},
	}
	var spans []otlpSpan
	for _, b := range bad {
		spans = append(spans, a2aSpanWith("message/send", []map[string]any{rawAttr("http.response.status_code", b.value)}))
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-bad-status", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if want := uint64(len(bad)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per bad status)", dropped, want)
	}
	for i, s := range out {
		if s.GetName() != a2aSpan {
			t.Errorf("%s: span is %q, want %q", bad[i].name, s.GetName(), a2aSpan)
		}
		if got, want := attrKeys(s), "http.request.method,http.route,"+a2aMethodKey; got != want {
			t.Errorf("%s: forwards [%s], want [%s]", bad[i].name, got, want)
		}
		if v, _ := stringAttrValue(t, s, "http.request.method"); v != "POST" {
			t.Errorf("%s: method %q, want POST", bad[i].name, v)
		}
		if v, _ := stringAttrValue(t, s, "http.route"); v != a2aRoute {
			t.Errorf("%s: route %q, want %q", bad[i].name, v, a2aRoute)
		}
		if v, _ := stringAttrValue(t, s, a2aMethodKey); v != "message/send" {
			t.Errorf("%s: a2a method %q, want message/send", bad[i].name, v)
		}
	}
}

// krateo.a2a.method is a closed set: anything else is dropped and counted,
// the span forwarded with its other three attributes.
//
// MUTATIONS (RED, shown in the PR): a2aMethod's set lookup replaced by a
// pass-through (every string passes); a2aMethod's one-member check removed.
func TestRUMAgentikoA2A_UnknownA2AMethodDropped(t *testing.T) {
	bad := []struct {
		name  string
		value map[string]any
	}{
		{"tasks/cancel", map[string]any{"stringValue": "tasks/cancel"}},
		{"message/stream case variant", map[string]any{"stringValue": "Message/Stream"}},
		{"message/stream with a space", map[string]any{"stringValue": "message/stream "}},
		{"message prefix", map[string]any{"stringValue": "message"}},
		{"free text", map[string]any{"stringValue": "hello, can you deploy my app"}},
		{"empty", map[string]any{"stringValue": ""}},
		{"int", map[string]any{"intValue": "1"}},
		{"valid beside an int", map[string]any{"stringValue": "message/send", "intValue": "1"}},
	}
	var spans []otlpSpan
	for _, b := range bad {
		spans = append(spans, a2aSpanWith("", []map[string]any{rawAttr(a2aMethodKey, b.value)}))
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-bad-a2a-method", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if want := uint64(len(bad)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (one per bad a2a method)", dropped, want)
	}
	for i, s := range out {
		if got, want := attrKeys(s), "http.request.method,http.response.status_code,http.route"; got != want {
			t.Errorf("%s: forwards [%s], want [%s]", bad[i].name, got, want)
		}
	}
}

// No key outside the four is forwarded: an identity key (enduser.id), a
// session, conversation or message key, a key allowlisted on another span
// (the abandoned mark, the claim family) and a case variant of the a2a key
// are each dropped and counted. The four are kept.
//
// MUTATION (RED, shown in the PR): enduser.id added to the row.
func TestRUMAgentikoA2A_IdentityAndExtraKeysDropped(t *testing.T) {
	extra := []map[string]any{
		strAttr("enduser.id", "zq-a2a-person"),
		strAttr("user.name", "zq-a2a-person"),
		strAttr("session.id", "s_123"),
		strAttr("krateo.agentiko.conversation.id", "c_123"),
		strAttr("krateo.a2a.message", "deploy my app"),
		strAttr("url.full", "https://portal.example/api/a2a/ns1/agent1"),
		boolAttr("krateo.interaction.abandoned", true),
		strAttr("krateo.agentiko.claim.family", "grounding"),
		strAttr("Krateo.A2A.Method", "message/send"),
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-extra-keys",
		otlpBody(t, rum.ServiceName, nil, a2aSpanWith("message/send", nil, extra...)), 1)
	if want := uint64(len(extra)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (every extra key)", dropped, want)
	}
	if got := attrKeys(out[0]); got != a2aKeys {
		t.Errorf("forwards [%s], want [%s]", got, a2aKeys)
	}
}

// An aborted stream (second contract comment): the portal omits
// http.response.status_code and sets span status ERROR. The span is
// forwarded with status ERROR (never its message), its other three
// attributes, and nothing dropped: an abort is not a contract violation.
//
// This arm adds no check of its own: it pins that a missing allowlisted key
// is neither required nor counted, and that the span status is forwarded.
//
// MUTATIONS (RED, shown in the PR): the span's row removed from
// spanAllowlist; buildSpan forwarding status UNSET instead of the sent code.
func TestRUMAgentikoA2A_AbortWithoutStatusCodeForwarded(t *testing.T) {
	s := a2aSpanWith("message/stream", nil)
	s.attrs = append(s.attrs[:1], s.attrs[2:]...) // drop http.response.status_code
	var body map[string]any
	if err := json.Unmarshal(otlpBody(t, rum.ServiceName, nil, s), &body); err != nil {
		t.Fatal(err)
	}
	sent := body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	sent[0].(map[string]any)["status"] = map[string]any{"code": 2, "message": "stream aborted: free text that must not be forwarded"}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	out, dropped, _ := postA2AAndExport(t, "a2a-abort", raw, 1)
	if dropped != 0 {
		t.Errorf("attributes_dropped delta %d, want 0", dropped)
	}
	if got, want := attrKeys(out[0]), "http.request.method,http.route,"+a2aMethodKey; got != want {
		t.Errorf("forwards [%s], want [%s]", got, want)
	}
	if st := out[0].GetStatus(); st.GetCode() != tracepb.Status_STATUS_CODE_ERROR || st.GetMessage() != "" {
		t.Errorf("status %v, want code ERROR and no message", st)
	}
}

// krateo.widget.fetch is unchanged: an http.route under /api/a2a/ (template
// or concrete) is not a snowplow route, so it is dropped and counted as an
// unregistered widget route; and krateo.a2a.method is not on that span's row,
// so it is dropped too.
//
// MUTATION (RED, shown in the PR): the widget row's route check replaced by
// routeTemplate.
func TestRUMAgentikoA2A_WidgetFetchA2ARouteStaysRefused(t *testing.T) {
	routes := []string{a2aRoute, "/api/a2a/ns1/agent1"}
	var spans []otlpSpan
	for _, r := range routes {
		spans = append(spans, otlpSpan{name: "krateo.widget.fetch", attrs: []map[string]any{
			strAttr("http.route", r),
			strAttr("http.request.method", "POST"),
			intAttr("http.response.status_code", 200),
			strAttr(a2aMethodKey, "message/send"),
		}})
	}
	out, dropped, unregistered := postA2AAndExport(t, "a2a-widget-fetch", otlpBody(t, rum.ServiceName, nil, spans...), len(spans))
	if want := uint64(2 * len(routes)); dropped != want {
		t.Errorf("attributes_dropped delta %d, want %d (the route and the a2a method, per span)", dropped, want)
	}
	if want := uint64(len(routes)); unregistered != want {
		t.Errorf("widget_routes_unregistered delta %d, want %d", unregistered, want)
	}
	for i, s := range out {
		if got, want := attrKeys(s), "http.request.method,http.response.status_code"; got != want {
			t.Errorf("widget.fetch with route %q forwards [%s], want [%s]", routes[i], got, want)
		}
	}
}
