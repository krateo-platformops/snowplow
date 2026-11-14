package dispatchers

// learned_rep_traceid_once_916_test.go — #916: resolveAndPopulateL1's learned
// representative wraps the context's logger in the redacting handler and
// hands it to xcontext.WithLogger again (resolve_populate.go). The context's
// logger already carries traceId, and WithLogger stamps it once more, through
// the redacting handler, into the log decorator. The line carries it once.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/testsupport/jsonkeys"
	"github.com/krateo-platformops/snowplow/internal/tracing"
)

func TestS916_LearnedRepresentativeReWrapCarriesTraceIdOnce(t *testing.T) {
	var buf bytes.Buffer
	root := slog.New(tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(&buf, nil)))
	// The background context main.go builds: its logger, stamped with its
	// (empty) traceId.
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(root))

	// resolve_populate.go's learned-representative branch, verbatim in shape.
	log := learnedRedactingLogger(xcontext.Logger(ctx), "s916-user", []string{"s916-group"})
	rctx := xcontext.BuildContext(ctx, xcontext.WithLogger(log))
	xcontext.Logger(rctx).WarnContext(rctx, "s916.learned", slog.String("who", "s916-user"))

	line := strings.TrimSpace(buf.String())
	got, err := jsonkeys.Read([]byte(line))
	if err != nil {
		t.Fatalf("not one JSON object: %v: %s", err, line)
	}
	if got.Count("traceId") != 1 || len(got.Repeated) > 0 {
		t.Errorf("#916 RED: the learned representative's line carries traceId %d times (repeats %v): %s", got.Count("traceId"), got.Repeated, line)
	}
	if strings.Contains(line, "s916-user") {
		t.Errorf("CONTROL: the redacting handler is not on the path, so this is not the learned branch: %s", line)
	}
}
