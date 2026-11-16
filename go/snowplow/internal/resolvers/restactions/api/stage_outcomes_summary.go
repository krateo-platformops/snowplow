package api

import (
	"encoding/json"
	"slices"
)

// stage_outcomes_summary.go — #1264: what an X-Snowplow-Stage-Outcomes value
// says, without its stage names, for the request's Server span.
//
// A RESTAction resolve whose stages failed is answered 200: the failure is in
// the body (under each stage's errorKey) and in the header, never in the
// span's status. The collector's tail sampler keeps every error trace and 5%
// of the rest, so such a trace was kept 5% of the time. The span therefore
// carries the header's content as low-cardinality attributes (set by
// dispatchers.StageOutcomesSpan): how many listed stages failed, how many
// snowplow deliberately did not execute, and the failures' reason codes.
//
// FAILED IS NOT "DID NOT SUCCEED" (#1288). The header lists every stage that
// did not succeed (#904), and callers rely on that. The summary splits the
// list by the class of each row's reason (stageNotExecutedByDesign, the one
// classifier): a dry run's refused write (StageNotExecuted) and an export's
// or background work's refused write (WriteRefused) are not failures; they
// are what the request asked for. Every other code is a failure, Skipped
// included: it is recorded only downstream of a stage that failed
// (blocksDependentWrite never blocks on a deliberate non-execution), so it is
// that failure's consequence and is counted with it.
//
// THE SOURCE IS THE HEADER VALUE, NOT A SECOND DERIVATION. The value is what
// HeaderValue renders from the resolve's own collector (a cold resolve), what
// an L1 hit answers with (the value the storing resolve rendered, kept on
// cache.ResolvedEntry.StageOutcomes), or what an export merged from its pages.
// Reading it back means the span says exactly what the response said, on
// every one of those paths, from one parser.

// StageReasonOther is the summary's value for a reason code outside the
// closed set (stageReasonKnown). It never appears in the header: it exists so
// an unexpected value is never passed on raw.
const StageReasonOther = "other"

// stageReasonKnown reports whether r is one of the header's reason codes: the
// closed set stageReasonClasses (stage_outcomes.go), which holds every code
// fail() keeps (it folds any other to Error) and the three the orchestrator
// records itself (a skipped stage, a stage that never ran, the resolve-level
// abort row).
func stageReasonKnown(r string) bool {
	_, known := stageReasonClasses[r]
	return known
}

// StageOutcomeSummary is an X-Snowplow-Stage-Outcomes value without its stage
// names.
type StageOutcomeSummary struct {
	// Failed is the number of listed entries that are failures: every stage
	// that did not succeed and that snowplow did not deliberately leave
	// unexecuted, plus the resolve-level Aborted row when the resolve stopped
	// before running any stage. An entry whose reason is outside the closed
	// set counts here. For a truncated header it is the summary's N, which
	// counts every listed entry: the summary no longer says which ones were
	// deliberate. 0 means no stage failed.
	Failed int
	// NotExecuted is the number of listed entries snowplow deliberately did
	// not execute (stageNotExecutedByDesign). 0 for a truncated header, which
	// cannot tell (Truncated).
	NotExecuted int
	// Truncated reports the {"truncated":true,"failed":N} summary: Failed is
	// its N, NotExecuted is unknown and Reasons is nil.
	Truncated bool
	// Reasons is the distinct reason codes of the failed entries, sorted, each
	// one from the closed set or StageReasonOther. nil when Failed is 0, and
	// when the header is the truncated summary, which no longer names them.
	Reasons []string
}

// SummarizeStageOutcomes reads a header value: HeaderValue's array, or the
// {"truncated":true,"failed":N} summary. ok is false for "" (no header) and
// for a value that is neither shape.
func SummarizeStageOutcomes(v string) (s StageOutcomeSummary, ok bool) {
	if v == "" {
		return StageOutcomeSummary{}, false
	}
	var rows []struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(v), &rows); err == nil {
		for _, r := range rows {
			if stageNotExecutedByDesign(r.Reason) {
				s.NotExecuted++
				continue
			}
			s.Failed++
			reason := r.Reason
			if !stageReasonKnown(reason) {
				reason = StageReasonOther
			}
			if !slices.Contains(s.Reasons, reason) { // a handful at most: the set is closed
				s.Reasons = append(s.Reasons, reason)
			}
		}
		slices.Sort(s.Reasons)
		return s, true
	}
	var sum struct {
		Truncated bool `json:"truncated"`
		Failed    *int `json:"failed"`
	}
	if err := json.Unmarshal([]byte(v), &sum); err != nil || !sum.Truncated || sum.Failed == nil || *sum.Failed < 0 {
		return StageOutcomeSummary{}, false
	}
	return StageOutcomeSummary{Failed: *sum.Failed, Truncated: true}, true
}
