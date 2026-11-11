package audit

// utf8_kinds_1251_test.go — #1251 re-review: the UTF-8 sanitiser is TOTAL over
// the attribute value kinds of go.opentelemetry.io/otel/attribute.
//
//   K  every kind the API defines (every Type its stringer names) has a
//      sample here and comes out of the shared writer's sanitiser
//      (audit.SanitizeValue, which Emit applies to every attribute and the
//      body, #1291) of the same kind with every string valid UTF-8; a kind
//      added by an otel upgrade fails this arm until it gets a sample;
//   W  one record per kind, each holding invalid UTF-8 wherever the kind can
//      hold a string (a nested SLICE, a MAP key and value), sanitised and sent
//      through the REAL otlploghttp exporter and BatchProcessor in ONE batch:
//      every record arrives.

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	krateoaudit "github.com/krateo-platformops/audit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// kindSamples1251 is one value of each kind, holding invalid UTF-8 wherever
// the kind can hold a string.
var kindSamples1251 = map[attribute.Type]attribute.Value{
	attribute.EMPTY:        {},
	attribute.BOOL:         attribute.BoolValue(true),
	attribute.INT64:        attribute.Int64Value(7),
	attribute.FLOAT64:      attribute.Float64Value(1.5),
	attribute.STRING:       attribute.StringValue("s-\xff"),
	attribute.BOOLSLICE:    attribute.BoolSliceValue([]bool{true}),
	attribute.INT64SLICE:   attribute.Int64SliceValue([]int64{1}),
	attribute.FLOAT64SLICE: attribute.Float64SliceValue([]float64{1}),
	attribute.STRINGSLICE:  attribute.StringSliceValue([]string{"ok", "ss-\xff"}),
	attribute.BYTESLICE:    attribute.ByteSliceValue([]byte{0xff, 0xfe}),
	attribute.SLICE: attribute.SliceValue(
		attribute.StringValue("n-\xff"),
		attribute.SliceValue(attribute.StringValue("nn-\xff")),
		attribute.MapValue(attribute.String("k", "nm-\xff"))),
	attribute.MAP: attribute.MapValue(
		attribute.String("key-\xff", "v-\xff"),
		attribute.Map("inner", attribute.String("ik", "iv-\xff")),
		attribute.Slice("list", attribute.StringValue("l-\xff"))),
}

// definedKinds1251 is every attribute.Type the API names: the stringer writes
// "Type(N)" for a number it does not know.
func definedKinds1251() []attribute.Type {
	var out []attribute.Type
	for t := attribute.Type(0); t < 64; t++ {
		if !strings.HasPrefix(t.String(), "Type(") {
			out = append(out, t)
		}
	}
	return out
}

// allValidUTF8 reports whether every string v holds, at any depth, is valid.
// BYTESLICE is bytes on the wire and is not a string.
func allValidUTF8(v attribute.Value) bool {
	switch v.Type() {
	case attribute.STRING:
		return utf8.ValidString(v.AsString())
	case attribute.STRINGSLICE:
		for _, s := range v.AsStringSlice() {
			if !utf8.ValidString(s) {
				return false
			}
		}
	case attribute.SLICE:
		for _, e := range v.AsSlice() {
			if !allValidUTF8(e) {
				return false
			}
		}
	case attribute.MAP:
		for _, kv := range v.AsMap() {
			if !utf8.ValidString(string(kv.Key)) || !allValidUTF8(kv.Value) {
				return false
			}
		}
	}
	return true
}

func TestUTF8_1251_K_EveryKindHasACase(t *testing.T) {
	for _, k := range definedKinds1251() {
		in, ok := kindSamples1251[k]
		if !ok {
			t.Errorf("attribute kind %v has no sample in this arm", k)
			continue
		}
		if in.Type() != k {
			t.Fatalf("precondition: the %v sample is a %v", k, in.Type())
		}
		out := krateoaudit.SanitizeValue(in)
		if out.Type() != k {
			t.Errorf("%v: the sanitiser changed the kind to %v", k, out.Type())
		}
		if !allValidUTF8(out) {
			t.Errorf("%v: a string is still not valid UTF-8 after the sanitiser: %v", k, out.Emit())
		}
		if k == attribute.BYTESLICE && string(out.AsByteSlice()) != "\xff\xfe" {
			t.Errorf("BYTESLICE changed (%q): bytes are not strings on the wire and stay as they are", out.AsByteSlice())
		}
	}
	// A key that is not valid UTF-8.
	if kv := krateoaudit.Sanitize(attribute.String("bad-\xff-key", "v")); !utf8.ValidString(string(kv.Key)) {
		t.Errorf("key %q is still not valid UTF-8", kv.Key)
	}
	// The body takes the same path: a nested body.
	if b := krateoaudit.SanitizeValue(kindSamples1251[attribute.MAP]); !allValidUTF8(b) {
		t.Errorf("a MAP body is still not valid UTF-8")
	}
}

func TestUTF8_1251_W_EveryKindSurvivesTheRealExporterInOneBatch(t *testing.T) {
	col := &collector1251{}
	srv := httptest.NewServer(col)
	t.Cleanup(srv.Close)
	ctx := context.Background()
	exp, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(srv.URL+"/v1/logs"))
	if err != nil {
		t.Fatal(err)
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	logger := lp.Logger("test-1251-kinds")

	kinds := definedKinds1251()
	for _, k := range kinds {
		var rec otellog.Record
		rec.SetBody(krateoaudit.SanitizeValue(kindSamples1251[k]))
		rec.AddAttributes(
			attribute.String("kind", k.String()),
			krateoaudit.Sanitize(attribute.KeyValue{Key: "value-\xff", Value: kindSamples1251[k]}),
		)
		logger.Emit(ctx, rec)
	}
	if err := lp.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown (flush) failed: %v", err)
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.records) != len(kinds) {
		t.Fatalf("the collector received %d of %d records (one per kind) in %d requests", len(col.records), len(kinds), col.requests)
	}
}
