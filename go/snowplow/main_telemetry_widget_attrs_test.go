package main

// main_telemetry_widget_attrs_test.go: the browser RUM ingest forwards
// krateo.widget.resource, krateo.widget.namespace and krateo.widget.name on
// krateo.widget.fetch only, each a DNS-1123 name (resource and namespace a
// label of at most 63 characters, name a subdomain of at most 253). A value
// of another shape is dropped and counted; the span is still forwarded. Names,
// keys and values are literals, not rum's constants. The rum counters are
// process-global, so every arm reads deltas.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"
)

const (
	wResKey  = "krateo.widget.resource"
	wNsKey   = "krateo.widget.namespace"
	wNameKey = "krateo.widget.name"
	wBase    = "http.route,http.request.method,http.response.status_code"
)

func widgetFetchWith(attrs ...map[string]any) otlpSpan {
	s := widgetFetch()
	s.attrs = append(s.attrs, attrs...)
	return s
}

func postWidgetSpans(t *testing.T, user string, spans ...otlpSpan) (keys []string, dropped uint64) {
	t.Helper()
	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, user), otlpBody(t, rum.ServiceName, nil, spans...)); rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	after := rum.Read()
	out := awaitExport(t, c).GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(out) != len(spans) {
		t.Fatalf("forwarded %d spans, want %d", len(out), len(spans))
	}
	for _, s := range out {
		keys = append(keys, attrKeys(s))
	}
	return keys, after.Attributes - before.Attributes
}

// MUTATIONS (RED): any of the three rows removed from the widget-fetch entry
// of spanAllowlist; a check that never passes.
func TestRUMWidgetAttrs_ValidValuesForwarded(t *testing.T) {
	long63 := "a" + strings.Repeat("b", 61) + "c"
	span := widgetFetchWith(strAttr(wResKey, "tables"), strAttr(wNsKey, "demo-system"), strAttr(wNameKey, "my.table-1"))
	span2 := widgetFetchWith(strAttr(wResKey, long63), strAttr(wNsKey, long63), strAttr(wNameKey, strings.Repeat(long63+".", 3)+"x"))
	keys, dropped := postWidgetSpans(t, "widget-attrs-ok", span, span2)
	want := wBase + "," + wResKey + "," + wNsKey + "," + wNameKey
	for i, k := range keys {
		if k != want {
			t.Errorf("span %d forwards [%s], want [%s]", i, k, want)
		}
	}
	if dropped != 0 {
		t.Errorf("attributes_dropped delta %d, want 0", dropped)
	}
}

// MUTATIONS (RED): dnsLabel/dnsSubdomain replaced by a pass-through; the
// length bound removed; the one-member check removed; the '-' edge rule
// removed.
func TestRUMWidgetAttrs_InvalidValuesDroppedAndCounted(t *testing.T) {
	bad := []struct {
		name string
		val  map[string]any
	}{
		{"spaces", map[string]any{"stringValue": "my table"}},
		{"url", map[string]any{"stringValue": "https://portal.example/call?name=x"}},
		{"path", map[string]any{"stringValue": "a/b"}},
		{"email", map[string]any{"stringValue": "alice@example.com"}},
		{"upper case", map[string]any{"stringValue": "Tables"}},
		{"leading dash", map[string]any{"stringValue": "-x"}},
		{"trailing dash", map[string]any{"stringValue": "x-"}},
		{"underscore", map[string]any{"stringValue": "a_b"}},
		{"empty", map[string]any{"stringValue": ""}},
		{"unicode", map[string]any{"stringValue": "tablé"}},
		{"overlong", map[string]any{"stringValue": strings.Repeat("a", 254)}},
		{"int", map[string]any{"intValue": "1"}},
		{"valid beside an int", map[string]any{"stringValue": "tables", "intValue": "1"}},
	}
	for _, key := range []string{wResKey, wNsKey, wNameKey} {
		var spans []otlpSpan
		for _, b := range bad {
			spans = append(spans, widgetFetchWith(rawAttr(key, b.val)))
		}
		keys, dropped := postWidgetSpans(t, "widget-attrs-bad", spans...)
		if want := uint64(len(bad)); dropped != want {
			t.Errorf("%s: attributes_dropped delta %d, want %d", key, dropped, want)
		}
		for i, k := range keys {
			if k != wBase {
				t.Errorf("%s/%s: forwards [%s], want [%s]", key, bad[i].name, k, wBase)
			}
		}
	}
	// Per-key bounds: 64 characters and a dot fail as a label (resource,
	// namespace); a dot passes as a subdomain (name), 64 characters in one
	// label and an empty label do not.
	for _, tc := range []struct {
		key, val string
		ok       bool
	}{
		{wResKey, strings.Repeat("a", 64), false},
		{wNsKey, "a.b", false},
		{wNameKey, "a.b", true},
		{wNameKey, "a..b", false},
		{wNameKey, strings.Repeat("a", 64), false},
		{wNameKey, strings.Repeat("a", 63), true},
		{wNameKey, strings.Repeat("a.", 126) + "a", true},  // 253 characters
		{wNameKey, strings.Repeat("a.", 127) + "a", false}, // 255 characters, every label valid
	} {
		keys, dropped := postWidgetSpans(t, "widget-attrs-bounds", widgetFetchWith(strAttr(tc.key, tc.val)))
		if tc.ok == (dropped != 0) || tc.ok != strings.Contains(keys[0], tc.key) {
			t.Errorf("%s=%q: ok=%v, dropped %d, forwards [%s]", tc.key, tc.val, tc.ok, dropped, keys[0])
		}
	}
}

// The three keys are allowed on krateo.widget.fetch only; every other key
// stays dropped.
//
// MUTATIONS (RED): the rows added to another span's entry; enduser.id added to
// the widget-fetch entry.
func TestRUMWidgetAttrs_OnlyOnWidgetFetchAndNoOtherKey(t *testing.T) {
	w := []map[string]any{strAttr(wResKey, "tables"), strAttr(wNsKey, "demo"), strAttr(wNameKey, "t1")}
	other := []otlpSpan{
		{name: "krateo.page.load", attrs: w},
		{name: "krateo.composition.open", attrs: w},
		{name: "krateo.agentiko.a2a", attrs: w},
		{name: "krateo.agentiko.claim.unbacked", attrs: w},
	}
	keys, dropped := postWidgetSpans(t, "widget-attrs-other-spans", other...)
	if dropped != uint64(3*len(other)) {
		t.Errorf("attributes_dropped delta %d, want %d", dropped, 3*len(other))
	}
	for i, k := range keys {
		if k != "" {
			t.Errorf("%s forwards [%s], want none", other[i].name, k)
		}
	}
	extra := []map[string]any{
		strAttr("enduser.id", "zq-widget-person"),  // passes any DNS check
		strAttr("enduser.id", "/zq-widget-person"), // passes the route check
		strAttr("user.name", "zq-widget-person"),
		strAttr("krateo.widget.kind", "Table"),
		strAttr("krateo.widget.uid", "u1"),
		strAttr("Krateo.Widget.Name", "t1"),
		strAttr("url.full", "https://portal.example/call?name=t1"),
	}
	keys, dropped = postWidgetSpans(t, "widget-attrs-extra", widgetFetchWith(append(w, extra...)...))
	if dropped != uint64(len(extra)) {
		t.Errorf("attributes_dropped delta %d, want %d", dropped, len(extra))
	}
	if want := wBase + "," + wResKey + "," + wNsKey + "," + wNameKey; keys[0] != want {
		t.Errorf("forwards [%s], want [%s]", keys[0], want)
	}
}
