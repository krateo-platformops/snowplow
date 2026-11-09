package main

import (
	"io"
	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/tracing"
)

// buildLogHandler returns the PRODUCTION slog handler chain: the base
// text/JSON handler wrapped in the OTel trace-correlation decorator.
//
// 1.12.4 (design §4.3, gate condition C6). Before this release the chain
// was built inline in func main() and no test could reach it, so the one
// site that attached trace_id/span_id to a log record
// (per_call_log.go, INFO, suppressed by the production LOG_LEVEL=warn)
// was the whole of snowplow's trace/log correlation. main() and the F6
// falsifier both call THIS function — there is no second construction
// path, and the F10 AST guard reds the day someone reconstructs the
// handler inline and bypasses the decorator.
//
// prettyLog selects the text handler (operator terminals) over the JSON
// handler (the stdout -> filelog -> ClickHouse otel_logs pipeline). w is
// the destination stream; main passes os.Stderr for pretty and os.Stdout
// for JSON, preserving the pre-1.12.4 stream choice exactly.
func buildLogHandler(prettyLog bool, level slog.Level, w io.Writer) slog.Handler {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
		// #1192: no site logs an identity (internal/redact's guard), but the
		// text of an error can carry one that the apiserver or authn wrote
		// there. Both handlers take the scrub, so it sees every slog record
		// written through this function, plumbing's too. klog and a context
		// with no logger write elsewhere (#1223).
		ReplaceAttr: redact.ScrubLogAttr,
	}
	var base slog.Handler
	if prettyLog {
		base = slog.NewTextHandler(w, opts)
	} else {
		base = slog.NewJSONHandler(w, opts)
	}
	// #1260: and work_origin, the driver that started the work, named by the
	// one classifier of the driver markers.
	return tracing.NewCorrelationHandler(base, cache.WorkOrigin)
}
