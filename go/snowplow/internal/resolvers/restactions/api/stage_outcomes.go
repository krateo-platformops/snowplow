package api

import (
	"context"
	"encoding/json"
	"sync"
)

// Per-stage outcome reporting for an INLINE (caller-supplied, #443 part 2)
// resolve. The builder gate needs to know which stages failed even when the
// RESTAction's own filter drops the error keys from the body, and the body
// must stay byte-identical to a stored resolve. So the outcomes travel in a
// response header instead (X-Snowplow-Stage-Outcomes), as a compact JSON
// array of {"name","ok","reason"}.
//
// The header carries REASON CODES ONLY, from the closed set below: never an
// error message, a path or any response data (error text can carry data, and
// headers have size limits). Stage names are the caller's own draft's names.
// It is recorded only for the top-level caller-supplied resolve: a nested
// stored resolve builds its own ResolveOptions (ProvenanceStored) and records
// nothing.

// Stage outcome reason codes (closed set).
const (
	StageReasonNotExecuted  = "StageNotExecuted" // write-verb stage refused by a dry run (#443 k)
	StageReasonForbidden    = "Forbidden"
	StageReasonNotFound     = "NotFound"
	StageReasonUnauthorized = "Unauthorized"
	StageReasonNotRun       = "NotRun" // the resolve was truncated before this stage ran
	StageReasonError        = "Error"  // any other failure
)

// StageOutcomesMaxHeaderBytes bounds the header value. Above it the header is
// {"truncated":true,"failed":N} instead of the per-stage array.
const StageOutcomesMaxHeaderBytes = 4096

// reasonRank orders reasons when one stage records several (an iterator stage
// with many failing items): the most specific one wins, deterministically,
// whatever order concurrent items finish in.
var reasonRank = map[string]int{
	StageReasonNotExecuted:  5,
	StageReasonForbidden:    4,
	StageReasonUnauthorized: 3,
	StageReasonNotFound:     2,
	StageReasonError:        1,
}

type stageOutcome struct {
	ran    bool
	reason string // "" = ok
}

// StageOutcomes collects per-stage outcomes for one inline resolve. Safe for
// the concurrent iterator workers of a stage.
type StageOutcomes struct {
	mu    sync.Mutex
	order []string
	by    map[string]*stageOutcome
}

type stageOutcomesKey struct{}

// WithStageOutcomes installs a fresh collector on ctx. The RESTAction handler
// installs it only for an inline (caller-supplied) resolve.
func WithStageOutcomes(ctx context.Context) (context.Context, *StageOutcomes) {
	s := &StageOutcomes{by: map[string]*stageOutcome{}}
	return context.WithValue(ctx, stageOutcomesKey{}, s), s
}

func stageOutcomesFrom(ctx context.Context) *StageOutcomes {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(stageOutcomesKey{}).(*StageOutcomes)
	return s
}

func (s *StageOutcomes) register(names []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range names {
		if _, ok := s.by[n]; !ok {
			s.order = append(s.order, n)
			s.by[n] = &stageOutcome{}
		}
	}
}

func (s *StageOutcomes) ran(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.by[name]; o != nil {
		o.ran = true
	}
}

func (s *StageOutcomes) fail(name, reason string) {
	if s == nil {
		return
	}
	if _, known := reasonRank[reason]; !known {
		reason = StageReasonError
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.by[name]
	if o == nil {
		return // not a stage of this resolve (cannot happen for a registered run)
	}
	o.ran = true
	if reasonRank[reason] > reasonRank[o.reason] {
		o.reason = reason
	}
}

type stageOutcomeJSON struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// HeaderValue renders the X-Snowplow-Stage-Outcomes value: the per-stage
// array in topological order, or {"truncated":true,"failed":N} when that would
// exceed StageOutcomesMaxHeaderBytes. A stage that never ran (the resolve was
// truncated) reports NotRun.
func (s *StageOutcomes) HeaderValue() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	out := make([]stageOutcomeJSON, 0, len(s.order))
	failed := 0
	for _, n := range s.order {
		o := s.by[n]
		row := stageOutcomeJSON{Name: n, OK: true}
		switch {
		case o.reason != "":
			row.OK, row.Reason = false, o.reason
		case !o.ran:
			row.OK, row.Reason = false, StageReasonNotRun
		}
		if !row.OK {
			failed++
		}
		out = append(out, row)
	}
	s.mu.Unlock()
	b, err := json.Marshal(out)
	if err != nil || len(b) > StageOutcomesMaxHeaderBytes {
		t, _ := json.Marshal(map[string]any{"truncated": true, "failed": failed})
		return string(t)
	}
	return string(b)
}

// stageReasonOf maps an accumulated per-item error value to a reason code.
// Only machine fields are read (reason / code); the message never leaves.
func stageReasonOf(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return StageReasonError
	}
	if r, _ := m["reason"].(string); r != "" {
		switch r {
		case StageReasonNotExecuted, StageReasonForbidden, StageReasonNotFound, StageReasonUnauthorized:
			return r
		}
	}
	switch code := toInt(m["code"]); code {
	case 401:
		return StageReasonUnauthorized
	case 403:
		return StageReasonForbidden
	case 404:
		return StageReasonNotFound
	}
	return StageReasonError
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}
