package api

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
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
//     iterator errored, its endpoint could not be resolved), or (#1112 D2) it
//     is a write and snowplow refused to send at least one of its items
//     (InvalidPath, TemplateError, NotSent: undialled_write.go). Reason: a code;
//   - skipped: a write stage that was not dialed because the stage it depends
//     on failed or was skipped (reason Skipped);
//   - not executed (#1288): a write stage snowplow deliberately did not send,
//     because of what the request is, not because anything went wrong: a dry
//     run (StageNotExecuted, #443 k), or an export or background work
//     (WriteRefused, #903, #970). It is listed (it did not succeed), and it is
//     no failure: stageNotExecutedByDesign is the one classifier.
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
	// #1112 D2 — a write stage, or one item of it, that snowplow refused to
	// send (undialled_write.go). Recorded only by snowplow, never read from an
	// upstream answer.
	StageReasonInvalidPath   = "InvalidPath"   // the #288 guard refused the rendered apiserver path
	StageReasonTemplateError = "TemplateError" // the path, payload or a header did not render (#302, #293)
	StageReasonNotSent       = "NotSent"       // planned, not sent, and no other failure recorded
	// StageReasonTimeout (#1115) — the stage's exchange was ended by the
	// request's page budget (a 504 Timeout): the page's 10 s goal ran out.
	StageReasonTimeout = "Timeout"
	// StageReasonDestinationRefused (#1131) — the destination policy refused the
	// address the stage's endpoint resolved to (a 502): nothing was sent, and
	// asking again gives the same answer. Recorded only by snowplow: the reason
	// is no Kubernetes StatusReason, so trustedReason drops it from an upstream
	// answer (#714).
	StageReasonDestinationRefused = credhttp.DestinationRefusedReason
	// StageReasonUAFOnNonReadVerb (#1281) — the stage carries userAccessFilter
	// on a verb other than GET or HEAD, which the CRD rejects (core.go): it
	// would have been sent as snowplow's ServiceAccount, so nothing was sent
	// (uaf_non_read.go). Recorded only by snowplow: the reason is no Kubernetes
	// StatusReason, so trustedReason drops it from an upstream answer (#714).
	StageReasonUAFOnNonReadVerb = "UserAccessFilterOnNonReadVerb"
	// StageReasonWriteRefused (#1288) — a write stage, or one item of it, that
	// snowplow refused to send because the request is not a customer's own
	// write: an export (#970) or background work (#903). background_write.go
	// records it, never from an upstream answer. Distinct from
	// StageNotExecuted, whose #443 contract is "refused by a dry run" (its
	// message carries the stable "dry-run: " prefix, which this refusal's
	// message does not).
	StageReasonWriteRefused = "WriteRefused"
)

// stageReasonClass is what a reason code says about its stage (#1288).
type stageReasonClass uint8

const (
	// stageReasonFailed — the stage failed: it ran and an item failed, it
	// could not be built or sent, it was skipped because the stage it depends
	// on failed (Skipped follows a failure, never a deliberate non-execution:
	// blocksDependentWrite), it never ran because the resolve stopped early
	// (NotRun), or the resolve ran no stage (Aborted).
	stageReasonFailed stageReasonClass = iota + 1
	// stageReasonNotExecuted — snowplow deliberately did not execute the
	// stage, by what the request is: a dry run, an export, background work.
	stageReasonNotExecuted
)

// stageReasonClasses is the closed set of reason codes, each with its class.
// A code the header can carry is in it, and only those (stageReasonKnown).
var stageReasonClasses = map[string]stageReasonClass{
	StageReasonNotExecuted:        stageReasonNotExecuted,
	StageReasonWriteRefused:       stageReasonNotExecuted,
	StageReasonForbidden:          stageReasonFailed,
	StageReasonNotFound:           stageReasonFailed,
	StageReasonUnauthorized:       stageReasonFailed,
	StageReasonNotRun:             stageReasonFailed,
	StageReasonError:              stageReasonFailed,
	StageReasonSkipped:            stageReasonFailed,
	StageReasonAborted:            stageReasonFailed,
	StageReasonInvalidPath:        stageReasonFailed,
	StageReasonTemplateError:      stageReasonFailed,
	StageReasonNotSent:            stageReasonFailed,
	StageReasonTimeout:            stageReasonFailed,
	StageReasonDestinationRefused: stageReasonFailed,
	// #1281: a misconfigured stage (userAccessFilter on a write) is a real
	// failure, in every mode, never a deliberate non-execution.
	StageReasonUAFOnNonReadVerb: stageReasonFailed,
}

// stageNotExecutedByDesign reports whether reason says snowplow deliberately
// did not execute the stage (#1288): the one classifier, read by the span
// summary (such a stage is not counted as failed) and by the #904 rule (such
// a stage does not block a dependent write, which the same rule refuses for
// the same reason).
func stageNotExecutedByDesign(reason string) bool {
	return stageReasonClasses[reason] == stageReasonNotExecuted
}

// StageOutcomesMaxHeaderBytes bounds the header value. Above it the header is
// {"truncated":true,"failed":N} instead of the per-stage array.
const StageOutcomesMaxHeaderBytes = 4096

// reasonRank orders reasons when one stage records several (an iterator stage
// with many failing items): the most specific one wins, deterministically,
// whatever order concurrent items finish in.
var reasonRank = map[string]int{
	// #1281: recorded once for the whole stage, before anything else can be
	// (the stage is refused before it is planned); ranked first because the
	// stage itself is misconfigured.
	StageReasonUAFOnNonReadVerb: 12,
	// The #443 dry-run refusal outranks every other failure: a dry run reports
	// StageNotExecuted for its write stages (#443 k, a stable contract).
	StageReasonNotExecuted:        11,
	StageReasonForbidden:          10,
	StageReasonUnauthorized:       9,
	StageReasonNotFound:           8,
	StageReasonInvalidPath:        7,
	StageReasonDestinationRefused: 6,
	StageReasonTemplateError:      5,
	StageReasonTimeout:            4, // #1115: a recorded budget cut, above the NotSent fallback
	StageReasonNotSent:            3,
	StageReasonError:              2,
	// #1288: an export's or background work's refusal ranks BELOW every
	// failure (and above 0, the "" of a stage that recorded nothing). Items
	// the plan refuses (InvalidPath, TemplateError: failRefusedItems) are
	// recorded before dispatch and never reach the refusal, so a write stage
	// with one such item and one refused item FAILED: it reads that failure,
	// and blocks its dependents. A stage whose every item was refused reads
	// WriteRefused.
	StageReasonWriteRefused: 1,
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
	// replayed (#1319) is set when the body this collector reports on was
	// served WITHOUT running its resolve (a stored cell, a seed-memo entry):
	// stored is then the StorableStageOutcomes value the producing resolve
	// left, and HeaderValue reports it instead of the (empty) stage list.
	replayed bool
	stored   string
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

// StageOutcomesFromContext returns the collector WithStageOutcomes installed
// on ctx, nil when none is. A Put site that installed it on a ctx it passed
// down (the walker's content populate) reads it back here.
func StageOutcomesFromContext(ctx context.Context) *StageOutcomes {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(stageOutcomesKey{}).(*StageOutcomes)
	return s
}

// StorableStageOutcomes (#904, #1319) is the value a Put keeps on the cell it
// stores: "" when every stage succeeded (HeaderValue's []), so a complete cell
// costs no memory for it, else the header value. nil (no collector) is "".
func StorableStageOutcomes(o *StageOutcomes) string {
	if o == nil {
		return ""
	}
	if v := o.HeaderValue(); v != stageOutcomesAllSucceeded {
		return v
	}
	return ""
}

// stageOutcomesAllSucceeded is HeaderValue's value for a resolve with no
// failed, skipped or not-run stage.
const stageOutcomesAllSucceeded = "[]"

// ReplayStageOutcomes (#1319) reports stored (a StorableStageOutcomes value)
// as the outcomes of the resolve the collector on ctx waits for, when the
// body is served without running that resolve: a stored cell (a RESTAction
// full list), or a seed-memo entry. It claims the collector exactly as that
// resolve would have (claimStageOutcomes), so a resolve that runs later on
// ctx keeps its own outcomes privately. No-op when ctx carries no collector
// or a resolve already claimed it.
func ReplayStageOutcomes(ctx context.Context, stored string) {
	s := StageOutcomesFromContext(ctx)
	if s == nil || !s.claimed.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	s.replayed, s.stored = true, stored
	s.mu.Unlock()
}

// SealStageOutcomes (#1319) ends the collector's claim window: the resolve it
// reports on is over (or there was none: a widget with no apiRef), so a
// resolve that runs later on ctx (a widget's inline child) never reports into
// it. A collector nothing claimed reads every stage succeeded. No-op when
// ctx carries no collector or it is already claimed.
func SealStageOutcomes(ctx context.Context) { ReplayStageOutcomes(ctx, "") }

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

// failed reports whether stage name recorded a failure (any reason). Read by
// the #1112 D2 catch-all after the stage's workers finished.
func (s *StageOutcomes) failed(name string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.by[name]
	return o != nil && o.reason != ""
}

// reasonOf is the reason stage name's outcome reports (the highest-ranked
// one it recorded, reasonRank), "" when it recorded none. #1139: a log line
// that names a stage's cause reads it here, so it names the cause the header
// does.
func (s *StageOutcomes) reasonOf(name string) string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.by[name]; o != nil {
		return o.reason
	}
	return ""
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
// not be sent (#904): name failed or was skipped. A stage snowplow did not
// execute by design (stageNotExecutedByDesign: a dry-run refusal,
// StageNotExecuted, #443 k; an export or background refusal, WriteRefused,
// #1288) does not block. It is not an outcome of the stage's own call (a
// customer's real run would have sent it), and the same rule refuses the
// dependent write for the same reason, so the dependent reports that reason
// (or, when its iterator runs over the refused stage's output, yields zero
// items and succeeds), never Skipped: Skipped only ever follows a failure. A
// name that is not a stage of this resolve never blocks.
func (s *StageOutcomes) blocksDependentWrite(name string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.by[name]
	return o != nil && o.reason != "" && !stageNotExecutedByDesign(o.reason)
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
	if s.replayed {
		v := s.stored
		s.mu.Unlock()
		if v == "" {
			return stageOutcomesAllSucceeded
		}
		return v
	}
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
	if r, _ := m["reason"].(string); r == StageReasonNotExecuted || r == StageReasonDestinationRefused {
		return r
	}
	switch code := toInt(m["code"]); code {
	case 401:
		return StageReasonUnauthorized
	case 403:
		return StageReasonForbidden
	case 404:
		return StageReasonNotFound
	case 504:
		return StageReasonTimeout
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
