package tracing

// log_handler_each_key_once_916_test.go — #916: the decorator puts a key a
// derivation sets on the record once, the later value winning; keys at
// different levels are different keys; the trace stamp is never doubled; and a
// sibling derived from the same parent is not changed by the rebuild.
//
// Keys are read from the bytes (internal/testsupport/jsonkeys): encoding/json
// folds a repeated key and would hide the defect.

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/testsupport/jsonkeys"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func span916(t *testing.T) (context.Context, string) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("s916").Start(context.Background(), "s916")
	t.Cleanup(func() { span.End() })
	return ctx, trace.SpanContextFromContext(ctx).TraceID().String()
}

func newLogger916() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(NewTraceCorrelationHandler(slog.NewJSONHandler(&buf, nil))), &buf
}

// read916 reads the last line written to buf and fails on any repeated key.
func read916(t *testing.T, buf *bytes.Buffer) (jsonkeys.Line, string) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	line := lines[len(lines)-1]
	got, err := jsonkeys.Read([]byte(line))
	if err != nil {
		t.Fatalf("not one JSON object: %v: %s", err, line)
	}
	if len(got.Repeated) > 0 {
		t.Errorf("#916 RED: keys %v repeat in one object: %s", got.Repeated, line)
	}
	return got, line
}

// TestS916_PlumbingReWrapStampsTraceIdOnce: the plumbing mechanism itself, a
// context's logger handed to xcontext.WithLogger again, with and without a
// span, keeps one traceId, the later context's.
func TestS916_PlumbingReWrapStampsTraceIdOnce(t *testing.T) {
	for _, withSpan := range []bool{false, true} {
		log, buf := newLogger916()
		base, traceID := context.Background(), ""
		if withSpan {
			base, traceID = span916(t)
		}
		ctx := xcontext.BuildContext(base, xcontext.WithTraceId("first"), xcontext.WithLogger(log))
		ctx = xcontext.BuildContext(ctx, xcontext.WithTraceId("second"), xcontext.WithLogger(xcontext.Logger(ctx)))
		xcontext.Logger(ctx).WarnContext(ctx, "m")
		got, line := read916(t, buf)
		if got.Count("traceId") != 1 || !strings.Contains(line, `"traceId":"second"`) {
			t.Errorf("span=%v: want one traceId, the later context's: %s", withSpan, line)
		}
		wantTraceIDs := 0
		if withSpan {
			wantTraceIDs = 1
			if !strings.Contains(line, `"trace_id":"`+traceID+`"`) {
				t.Errorf("trace_id is not the span's %s: %s", traceID, line)
			}
		}
		if got.Count("trace_id") != wantTraceIDs || got.Count("span_id") != wantTraceIDs {
			t.Errorf("span=%v: trace_id %d, span_id %d, want %d each: %s", withSpan, got.Count("trace_id"), got.Count("span_id"), wantTraceIDs, line)
		}
	}
}

// TestS916_LaterValueWinsInPlaceOfTheEarlier: the record's keys, in order,
// show the earlier copy dropped and the later one where it was set.
func TestS916_LaterValueWinsInPlaceOfTheEarlier(t *testing.T) {
	log, buf := newLogger916()
	log.With("a", 1, "k", "old", "b", 2).With("k", "new").With("k", "newest", "c", 3, "c", 4).Warn("m")
	got, line := read916(t, buf)
	if want := []string{"time", "level", "msg", "a", "b", "k", "c"}; !reflect.DeepEqual(got.Top, want) {
		t.Fatalf("keys %v, want %v: %s", got.Top, want, line)
	}
	if !strings.Contains(line, `"k":"newest"`) || !strings.Contains(line, `"c":4`) {
		t.Fatalf("the later value must win: %s", line)
	}
}

// TestS916_KeysAtDifferentLevelsAreDifferentKeys: a key inside a group is not
// the top-level key of the same name, and a repeat inside the group collapses
// within the group only.
func TestS916_KeysAtDifferentLevelsAreDifferentKeys(t *testing.T) {
	ctx, _ := span916(t)
	log, buf := newLogger916()
	log.With("traceId", "top").WithGroup("g").With("traceId", "in-g").With("traceId", "in-g-2").WarnContext(ctx, "m", "r", 1)
	got, line := read916(t, buf)
	if got.Count("traceId") != 1 || !strings.Contains(line, `"traceId":"top"`) {
		t.Fatalf("the top-level traceId must stay: %s", line)
	}
	if !strings.Contains(line, `"g":{"traceId":"in-g-2","r":1}`) {
		t.Fatalf("the group keeps its own traceId, once, the later: %s", line)
	}
	if got.Count("trace_id") != 1 {
		t.Fatalf("trace_id must stay top-level and once: %s", line)
	}
}

// TestS916_TheStampIsNotDoubled: a top-level derivation that sets trace_id or
// span_id does not double the stamp; the live span's ids win. Inside a group
// the same key is a different key and stays.
func TestS916_TheStampIsNotDoubled(t *testing.T) {
	ctx, traceID := span916(t)
	log, buf := newLogger916()
	l := log.With("trace_id", "stale", "span_id", "stale", "x", 1)
	l.WarnContext(ctx, "m")
	got, line := read916(t, buf)
	if got.Count("trace_id") != 1 || got.Count("span_id") != 1 || !strings.Contains(line, `"trace_id":"`+traceID+`"`) || strings.Contains(line, "stale") {
		t.Fatalf("want the span's ids once: %s", line)
	}
	// Without a span the derivation's own value is all there is, and stays.
	l.Warn("m")
	if _, line := read916(t, buf); !strings.Contains(line, `"trace_id":"stale"`) {
		t.Fatalf("a record with no span keeps the derivation's trace_id: %s", line)
	}
	log.WithGroup("g").With("trace_id", "inner").WarnContext(ctx, "m")
	if _, line := read916(t, buf); !strings.Contains(line, `"g":{"trace_id":"inner"}`) {
		t.Fatalf("a grouped trace_id is not the stamp's key: %s", line)
	}
}

// TestS916_ASiblingIsNotChangedByTheRebuild: dropping the earlier copy builds a
// new chain; the parent and a sibling derived from it still write what they
// wrote.
func TestS916_ASiblingIsNotChangedByTheRebuild(t *testing.T) {
	ctx, _ := span916(t)
	log, buf := newLogger916()
	parent := log.With("k", "p", "o", 1)
	sibling := parent.With("s", 1)
	_ = parent.With("k", "child")
	for _, c := range []struct {
		l    *slog.Logger
		want string
	}{
		{parent, `"k":"p","o":1`},
		{sibling, `"k":"p","o":1,"s":1`},
	} {
		for _, rctx := range []context.Context{context.Background(), ctx} {
			c.l.WarnContext(rctx, "m")
			if _, line := read916(t, buf); !strings.Contains(line, c.want) {
				t.Fatalf("want %s: %s", c.want, line)
			}
		}
	}
}
