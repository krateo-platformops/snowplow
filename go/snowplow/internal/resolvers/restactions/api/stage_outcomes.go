package api

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// Per-stage outcomes of one resolve.
//
// Every resolve tracks them: the #904 rule (a write stage is not sent when the
// stage it depends on did not succeed) reads them between stages. A /call of a
// RESTAction also REPORTS them in a response header (X-Snowplow-Stage-Outcomes),
// so a caller can tell a half-done write sequence from a done one without
// parsing the body, which the RESTAction's own filter may have reshaped. #443
// part 2 introduced the header for inline drafts; #904 sets it on every
// RESTAction /call. It is a compact JSON array, in topological order, of every
// stage that did NOT succeed: {"name","ok":false,"reason"} plus, for a skipped
// stage, "dependsOn" (the stage that did not succeed). [] means every stage
// succeeded.
//
// Each stage ends in exactly one outcome (#904):
//   - succeeded: it ran and every item it dialed succeeded. An iterator stage
//     that yields zero items succeeded (it had nothing to do);
//   - failed: at least one item failed, or the stage could not be built (its
//     iterator errored, its endpoint could not be resolved). Reason: a code;
//   - skipped: a write stage that was not dialed because the stage it depends
//     on failed or was skipped (reason Skipped).
//
// The header carries STAGE NAMES AND REASON CODES ONLY, from the closed set
// below: never an error message, a path, an identity or any response data
// (error text can carry data, and headers have size limits).
//
// Only the TOP-LEVEL resolve reports into the collector a caller installs
// (WithStageOutcomes): the first resolve that finds it claims it. A nested
// resolve runs inside one of the top-level's stages, so always after the claim,
// and tracks its own stages privately.

// Stage outcome reason codes (closed set).
const (
	StageReasonNotExecuted  = "StageNotExecuted" // write-verb stage refused by a dry run (#443 k)
	StageReasonForbidden    = "Forbidden"
	StageReasonNotFound     = "NotFound"
	StageReasonUnauthorized = "Unauthorized"
	StageReasonNotRun       = "NotRun" // the resolve was truncated before this stage ran
	StageReasonError        = "Error"  // any other failure
	// StageReasonSkipped (#904) — a write stage not dialed because the stage it
	// depends on failed or was skipped. Recorded only by skip(), never by fail().
	StageReasonSkipped = "Skipped"
	// StageReasonAborted (#904 review F3) — the RESOLVE-level row (name "")
	// of a resolve that stopped before running any stage: its stage graph
	// cannot be ordered (a dependsOn naming no stage, a cycle) or it has no
	// identity or no client config. Every declared stage is then NotRun, so
	// the header can never read "all succeeded" ([]) for it.
	StageReasonAborted = "Aborted"
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
	ran       bool
	reason    string // "" = ok
	skippedBy string // #904: the dependsOn stage that did not succeed; "" = not skipped
}

// StageOutcomes collects per-stage outcomes for one resolve. Safe for the
// concurrent iterator workers of a stage.
type StageOutcomes struct {
	claimed atomic.Bool // a top-level resolve took this collector (claimStageOutcomes)
	mu      sync.Mutex
	aborted bool // the resolve stopped before running any stage (F3)
	order   []string
	by      map[string]*stageOutcome
}

func newStageOutcomes() *StageOutcomes {
	return &StageOutcomes{by: map[string]*stageOutcome{}}
}

type stageOutcomesKey struct{}

// WithStageOutcomes installs a fresh collector on ctx. A caller that reports a
// RESTAction's outcomes (the /call dispatcher, a Put site that stores them with
// the cell) installs it before the resolve; the top-level resolve claims it.
func WithStageOutcomes(ctx context.Context) (context.Context, *StageOutcomes) {
	s := newStageOutcomes()
	return context.WithValue(ctx, stageOutcomesKey{}, s), s
}

// claimStageOutcomes returns the collector this resolve records into: the
// ctx's, when no earlier resolve on this ctx claimed it (this one is then the
// top-level resolve), else a private one. Never nil, so the #904 rule always
// has the outcomes it reads.
func claimStageOutcomes(ctx context.Context) *StageOutcomes {
	if ctx != nil {
		if s, _ := ctx.Value(stageOutcomesKey{}).(*StageOutcomes); s != nil && s.claimed.CompareAndSwap(false, true) {
			return s
		}
	}
	return newStageOutcomes()
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

// abortedResolve (#904 review F3) is ResolveWithFailure's answer when it stops
// before running any stage: an empty dict, and — on the collector this resolve
// claims — a resolve-level Aborted row plus every declared stage as NotRun, so
// a controller reading the header never reads "all succeeded".
func abortedResolve(ctx context.Context, items []*templates.API) (map[string]any, *StageFailure) {
	o := claimStageOutcomes(ctx)
	names := make([]string, 0, len(items))
	for _, it := range items {
		if it != nil {
			names = append(names, it.Name)
		}
	}
	o.register(names)
	o.mu.Lock()
	o.aborted = true
	o.mu.Unlock()
	return map[string]any{}, nil
}

// skip records that a stage was not dialed because dependsOn did not succeed
// (#904). Called by the orchestrator between stages, never by a worker.
func (s *StageOutcomes) skip(name, dependsOn string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.by[name]; o != nil {
		o.reason, o.skippedBy = StageReasonSkipped, dependsOn
	}
}

// blocksDependentWrite reports whether a write stage that depends on name must
// not be sent (#904): name failed or was skipped. A dry-run refusal
// (StageNotExecuted, #443 k) does not block. It is not an outcome of the
// stage's own call (a real run would have sent it), and the dry run refuses the
// dependent write for the same reason, so the preview reports what a real run
// would do. A name that is not a stage of this resolve never blocks.
func (s *StageOutcomes) blocksDependentWrite(name string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.by[name]
	return o != nil && o.reason != "" && o.reason != StageReasonNotExecuted
}

type stageOutcomeJSON struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Reason    string `json:"reason,omitempty"`
	DependsOn string `json:"dependsOn,omitempty"`
}

// HeaderValue renders the X-Snowplow-Stage-Outcomes value: every stage that
// did not succeed, in topological order ([] when all succeeded), or
// {"truncated":true,"failed":N} when that would exceed
// StageOutcomesMaxHeaderBytes (N counts every listed stage). A stage that
// never ran because the resolve was truncated reports NotRun; a skipped one
// reports Skipped and the stage it depended on.
func (s *StageOutcomes) HeaderValue() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	out := make([]stageOutcomeJSON, 0, len(s.order)+1)
	if s.aborted {
		out = append(out, stageOutcomeJSON{Name: "", Reason: StageReasonAborted})
	}
	for _, n := range s.order {
		o := s.by[n]
		switch {
		case o.reason != "":
			out = append(out, stageOutcomeJSON{Name: n, Reason: o.reason, DependsOn: o.skippedBy})
		case !o.ran:
			out = append(out, stageOutcomeJSON{Name: n, Reason: StageReasonNotRun})
		}
	}
	failed := len(out)
	s.mu.Unlock()
	b, err := json.Marshal(out)
	if err != nil || len(b) > StageOutcomesMaxHeaderBytes {
		return TruncatedStageOutcomes(failed)
	}
	return string(b)
}

// TruncatedStageOutcomes is the header value that replaces a per-stage list
// longer than StageOutcomesMaxHeaderBytes: {"truncated":true,"failed":N}.
// Both writers of the header, a resolve and an export's merge of its pages,
// render the summary here, so it has one shape.
func TruncatedStageOutcomes(failed int) string {
	t, _ := json.Marshal(map[string]any{"truncated": true, "failed": failed})
	return string(t)
}

// stageReasonOf maps an accumulated per-item error value to a reason code.
// Only machine fields are read (reason / code); the message never leaves.
//
// #714 — the CODE decides, not a reason: a recorded reason can be an
// upstream's claim (a 500 calling itself "Forbidden"). The one reason read is
// StageNotExecuted, which only snowplow writes (the #443 dry-run refusal, which
// has no code).
func stageReasonOf(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return StageReasonError
	}
	if r, _ := m["reason"].(string); r == StageReasonNotExecuted {
		return r
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
