// stage_trace.go — one span per RESTAction resolve, one child span per stage.
//
// The frontend sees multi-second placeholders on RESTActions of many stages
// (dashboard-inbox 16, blueprint-detail 7), and stages run one after another
// (runStage waits on its iterator group), so "where does the time go" needs a
// per-stage clock a trace store can aggregate. Until now snowplow's spans were
// the Server span (`GET /call`) and the client spans (`HTTP GET`): neither names
// a RESTAction or a stage.
//
//	restaction.resolve   one per api.ResolveWithFailure: the whole stage loop
//	restaction.stage     one per stage the loop reached, child of the resolve
//
// A stage's dial and client spans are children of its stage span (the stage
// context is what planStage, the endpoint hop and the errgroup derive from), so
// a trace shows the stage tree.
//
// EVERY resolve path emits them, because they sit in the one function every path
// goes through: a customer /call (a child of its Server span), the refresher and
// the prewarms (roots). snowplow.restaction.source says which, read from the
// resolve's context (cache.RefDenialOrigin) and folded to a closed set.
//
// ATTRIBUTES (all bounded; none identity: a span never carries a username, a
// group, a token, a CN, a Secret name, a URL or its query, or response content):
//
//	snowplow.restaction.source         customer | refresher | prewarm | background
//	snowplow.restaction.namespace      the RESTAction's namespace (author config)
//	snowplow.restaction.name           the RESTAction's name        (author config)
//	snowplow.restaction.stage_count    declared stages              (resolve span)
//	snowplow.restaction.outcome        ok | failed                  (resolve span)
//	snowplow.restaction.stage          the stage's name             (author config)
//	snowplow.restaction.stage.target   in_cluster | external
//	snowplow.restaction.stage.shape    single | iterator
//	snowplow.restaction.stage.items    calls the stage planned (iterator items)
//	snowplow.restaction.stage.outcome  ok | item_errors | failed | not_executed
//	snowplow.restaction.stage.budget_cause
//	                                   not_sent | exhausted_connect |
//	                                   exhausted_answer, only when the page
//	                                   budget cut an exchange of the stage
//
// A span's status stays Unset (a 200 with failures in the body is not an error,
// #1264); the outcome attribute carries it.
//
// COST. With tracing off, tracing.Active() is false and nothing here runs beyond
// that one atomic load. With tracing on and the resolve span not recording (the
// sampler dropped it) the stage spans are not started either, so a sampled-out
// resolve costs the one resolve Start, which is the tracer's own no-op.
package api

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/tracing"
)

// Span names and attribute keys. TraceAttrKeys lists them for the hygiene test:
// a key not listed there is a key no reviewer has seen.
const (
	SpanRestactionResolve = "restaction.resolve"
	SpanRestactionStage   = "restaction.stage"

	attrRASource     = "snowplow.restaction.source"
	attrRANamespace  = "snowplow.restaction.namespace"
	attrRAName       = "snowplow.restaction.name"
	attrRAStageCount = "snowplow.restaction.stage_count"
	attrRAOutcome    = "snowplow.restaction.outcome"
	attrStage        = "snowplow.restaction.stage"
	attrStageTarget  = "snowplow.restaction.stage.target"
	attrStageShape   = "snowplow.restaction.stage.shape"
	attrStageItems   = "snowplow.restaction.stage.items"
	attrStageOutcome = "snowplow.restaction.stage.outcome"
	attrStageBudget  = "snowplow.restaction.stage.budget_cause"

	traceTracerScope = "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"

	sourceCustomer   = "customer"
	sourceRefresher  = "refresher"
	sourcePrewarm    = "prewarm"
	sourceBackground = "background"
	sourceUnknown    = "unknown"

	targetInCluster = "in_cluster"
	targetExternal  = "external"
	shapeSingle     = "single"
	shapeIterator   = "iterator"

	outcomeOK         = "ok"
	outcomeFailed     = "failed"
	outcomeItemErrors = "item_errors"
	outcomeNotExec    = "not_executed"
)

// TraceAttrKeys is the closed set of attribute keys these spans carry.
func TraceAttrKeys() []string {
	return []string{attrRASource, attrRANamespace, attrRAName, attrRAStageCount, attrRAOutcome,
		attrStage, attrStageTarget, attrStageShape, attrStageItems, attrStageOutcome, attrStageBudget}
}

// TraceAttrEnumValues is the closed value set of each enum-valued key. The
// other keys are author config (namespace, name, stage) or an int.
func TraceAttrEnumValues() map[string][]string {
	return map[string][]string{
		attrRASource:     {sourceCustomer, sourceRefresher, sourcePrewarm, sourceBackground, sourceUnknown},
		attrRAOutcome:    {outcomeOK, outcomeFailed},
		attrStageTarget:  {targetInCluster, targetExternal},
		attrStageShape:   {shapeSingle, shapeIterator},
		attrStageOutcome: {outcomeOK, outcomeItemErrors, outcomeFailed, outcomeNotExec},
		attrStageBudget:  {pagebudget.NotSent.String(), pagebudget.ExhaustedConnect.String(), pagebudget.ExhaustedAnswer.String()},
	}
}

type customerResolveKey struct{}

// WithCustomerResolve marks ctx as a customer's own request (the /call dispatchers
// set it). It is what makes snowplow.restaction.source read "customer": a path
// that forgets the marker reads "unknown", so the omission is visible.
func WithCustomerResolve(ctx context.Context) context.Context {
	return context.WithValue(ctx, customerResolveKey{}, true)
}

// traceSource folds the resolve's context into the closed source set. A
// background marker outranks the customer one.
func traceSource(ctx context.Context) string {
	switch cache.RefDenialOrigin(ctx) {
	case cache.RefDenialOriginServe:
		if v, _ := ctx.Value(customerResolveKey{}).(bool); v {
			return sourceCustomer
		}
		return sourceUnknown
	case cache.BackgroundOriginRefresher:
		return sourceRefresher
	case cache.BackgroundOriginCohortSeed, cache.BackgroundOriginPrewarmEngineBoot, cache.RefDenialOriginPrewarmPath:
		return sourcePrewarm
	default:
		return sourceBackground
	}
}

func resolveTracer() trace.Tracer { return otel.Tracer(traceTracerScope) }

// resolveTrace is the resolve span's handle; nil when tracing is off.
type resolveTrace struct{ span trace.Span }

// startResolveTrace starts the resolve span and returns ctx carrying it. With
// tracing off it returns ctx unchanged and a nil handle, without allocating.
func startResolveTrace(ctx context.Context, opts ResolveOptions) (context.Context, *resolveTrace) {
	if !tracing.Active() || !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}
	ctx, span := resolveTracer().Start(ctx, SpanRestactionResolve, trace.WithAttributes(
		attribute.String(attrRASource, traceSource(ctx)),
		attribute.String(attrRANamespace, opts.RESTActionNamespace),
		attribute.String(attrRAName, opts.RESTActionName),
		attribute.Int(attrRAStageCount, len(opts.Items)),
	))
	return ctx, &resolveTrace{span: span}
}

func (t *resolveTrace) end(failure *StageFailure, completed bool) {
	if t == nil {
		return
	}
	out := outcomeOK
	if failure != nil || !completed {
		out = outcomeFailed
	}
	t.span.SetAttributes(attribute.String(attrRAOutcome, out))
	t.span.End()
}

// stageTrace is one stage's span handle. All methods are nil-safe: with tracing
// off, or the resolve span not recording, the handle is nil.
type stageTrace struct {
	span   trace.Span
	budget string
	items  int
	shape  string
	target string
}

// beginStageTrace starts a stage span under the resolve span r.ctx carries, and
// returns the context the stage derives its work from.
func (r *resolveRun) beginStageTrace(id string) (context.Context, *stageTrace) {
	if !tracing.Active() || !trace.SpanFromContext(r.ctx).IsRecording() {
		return r.ctx, nil
	}
	ctx, span := resolveTracer().Start(r.ctx, SpanRestactionStage, trace.WithAttributes(
		attribute.String(attrRANamespace, r.opts.RESTActionNamespace),
		attribute.String(attrRAName, r.opts.RESTActionName),
		attribute.String(attrStage, id),
	))
	return ctx, &stageTrace{span: span}
}

// runStageTraced runs one stage under its span and returns runStage's stop. The
// span ends in a defer, so a panic still ends it (marked failed); a truncating
// stop records the stage's Error reason first, so the span reads it.
func (r *resolveRun) runStageTraced(id string, apiMap map[string]*templates.API) (stop bool) {
	spanCtx, st := r.beginStageTrace(id)
	done := false
	defer func() { st.end(stop || !done, false, r.outcomes.reasonOf(id)) }()
	stop = r.runStage(spanCtx, st, id, apiMap)
	done = true
	if stop {
		r.outcomes.fail(id, StageReasonError)
	}
	return stop
}

// traceNotRun records a stage the loop reached and did not enter (refused before
// it ran, or skipped behind a failed dependency): an instant span marked
// not_executed, so the trace lists every stage the resolve decided on.
func (r *resolveRun) traceNotRun(id string) {
	_, st := r.beginStageTrace(id)
	st.end(false, true, "")
}

// planned records the stage's shape once its endpoint and calls are known.
func (t *stageTrace) planned(iterator, inCluster bool, calls int) {
	if t == nil {
		return
	}
	t.shape, t.target, t.items = shapeSingle, targetExternal, calls
	if iterator {
		t.shape = shapeIterator
	}
	if inCluster {
		t.target = targetInCluster
	}
}

// noteItemErrs reads the page-budget cause off the stage's item errors.
func (t *stageTrace) noteItemErrs(errs []error) {
	if t == nil {
		return
	}
	for _, e := range errs {
		if be := (*pagebudget.ExhaustedError)(nil); e != nil && errors.As(e, &be) {
			t.budget = be.Result.String()
			return
		}
	}
}

// end finishes the span. stop is runStage's truncating return, notRun a stage
// the loop never entered, reason the stage's recorded outcome reason ("" when it
// recorded none).
func (t *stageTrace) end(stop, notRun bool, reason string) {
	if t == nil {
		return
	}
	out := outcomeOK
	switch {
	case notRun || stageNotExecutedByDesign(reason):
		out = outcomeNotExec
	case stop:
		out = outcomeFailed
	case reason != "":
		out = outcomeItemErrors
	}
	attrs := make([]attribute.KeyValue, 0, 5)
	attrs = append(attrs, attribute.String(attrStageOutcome, out))
	if t.shape != "" {
		attrs = append(attrs,
			attribute.String(attrStageShape, t.shape),
			attribute.String(attrStageTarget, t.target),
			attribute.Int(attrStageItems, t.items))
	}
	if t.budget != "" {
		attrs = append(attrs, attribute.String(attrStageBudget, t.budget))
	}
	t.span.SetAttributes(attrs...)
	t.span.End()
}
