// stage_outcomes_span.go — #1264: the request's Server span carries what its
// X-Snowplow-Stage-Outcomes header says.
//
// A RESTAction resolve whose stages failed is answered HTTP 200, with the
// failure in the body and in the header. Its Server span has status Unset
// (semconv: a 200 is not an error, and it stays Unset here) and usually no
// failing child span, so the collector's tail sampler (every error trace, 5%
// of the rest) kept it 5% of the time. The span now carries:
//
//   - snowplow.stages.failed (int): the number of entries the header lists,
//     every stage that did not succeed (failed, skipped, not run, refused by
//     a dry run) plus the resolve-level Aborted row. 0 when the header is []:
//     the response reported every stage succeeded. Observability's sampler
//     keeps a trace on snowplow.stages.failed > 0;
//   - snowplow.stages.failure_reasons (string slice): the distinct reason
//     codes of those entries, sorted, from the header's closed set; any other
//     value is folded to "other" (api.StageReasonOther), never passed raw.
//     Not set when failed is 0, nor for the truncated header summary, which
//     no longer names its reasons (failed is still its N).
//
// Neither carries a stage name, a RESTAction or target name, an identity, a
// message or a body: the header's names are dropped by the parser
// (api.SummarizeStageOutcomes). Distinct from #1265's snowplow.serve.*.
//
// WHICH RESPONSES. Exactly the ones whose header carries the outcomes: a
// RESTAction /call (GET, POST /call/read, inline), cold or an L1 hit, and an
// /export (its pages merged). A hit answers with the outcomes the resolve that
// stored the cell produced, kept on the entry (stage_outcomes_header.go), so
// the span carries those: nothing is invented for a hit. A response without
// the header (a widget, any other route, a request refused before the
// resolve) gets neither attribute. The value read is the response's FINAL
// header, after the handler returned, so a withdrawn stale pass (#710, which
// deletes the header and re-runs) reports its re-run's outcomes.
//
// WHERE. StageOutcomesSpan wraps the route table (main.go newServerHandler),
// inside the Server span, so it sees the response the span describes and
// nothing else: an export's per-page re-dispatch writes into its own
// recorder and a #547 demand revalidation into a discard writer, neither of
// which passes through it. It sets the attributes when the handler returns,
// like #1265's customerServe.end, while the span is still open.
package dispatchers

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// Span attribute names (#1264). Distinct from #1265's snowplow.serve.*.
const (
	spanAttrStagesFailed         = "snowplow.stages.failed"
	spanAttrStagesFailureReasons = "snowplow.stages.failure_reasons"
)

// StageOutcomesSpan sets the #1264 stage-outcome attributes on the request's
// Server span from the X-Snowplow-Stage-Outcomes header the response carries,
// once next returns. It never touches the span's status.
func StageOutcomesSpan(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		span := trace.SpanFromContext(r.Context())
		if !span.IsRecording() {
			return
		}
		if attrs := stageOutcomeSpanAttrs(w.Header().Get(util.HeaderStageOutcomes)); len(attrs) > 0 {
			span.SetAttributes(attrs...)
		}
	})
}

// stageOutcomeSpanAttrs is the attributes for one header value; none when the
// response carries no (readable) header.
func stageOutcomeSpanAttrs(v string) []attribute.KeyValue {
	s, ok := api.SummarizeStageOutcomes(v)
	if !ok {
		return nil
	}
	attrs := []attribute.KeyValue{attribute.Int(spanAttrStagesFailed, s.Failed)}
	if len(s.Reasons) > 0 {
		attrs = append(attrs, attribute.StringSlice(spanAttrStagesFailureReasons, s.Reasons))
	}
	return attrs
}
