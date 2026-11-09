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
// carries the header's content as two low-cardinality attributes (set by
// dispatchers.StageOutcomesSpan): how many entries the header lists, and
// which reason codes they carry.
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

// stageReasonKnown reports whether r is one of the header's reason codes: a
// ranked one (the only reasons fail() records; it folds any other to Error)
// or one of the three the orchestrator records itself (a skipped stage, a
// stage that never ran, the resolve-level abort row).
func stageReasonKnown(r string) bool {
	if _, ranked := reasonRank[r]; ranked {
		return true
	}
	return r == StageReasonSkipped || r == StageReasonNotRun || r == StageReasonAborted
}

// StageOutcomeSummary is an X-Snowplow-Stage-Outcomes value without its stage
// names.
type StageOutcomeSummary struct {
	// Failed is the number of entries the header lists: every stage that did
	// not succeed, plus the resolve-level Aborted row when the resolve stopped
	// before running any stage. For a truncated header it is the summary's N,
	// which counts the same entries. 0 means every stage succeeded.
	Failed int
	// Reasons is the distinct reason codes of those entries, sorted, each one
	// from the closed set or StageReasonOther. nil when Failed is 0, and when
	// the header is the truncated summary, which no longer names them.
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
		s.Failed = len(rows)
		for _, r := range rows {
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
	return StageOutcomeSummary{Failed: *sum.Failed}, true
}
