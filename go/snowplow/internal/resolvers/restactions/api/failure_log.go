// failure_log.go — a failure that repeats unchanged in one cell's generation
// logs once at its own level, then at DEBUG (#711, generalised by #1476).
//
// THE DEFECT. A resident cell is re-resolved on every refresh (#604), keepwarm
// pass and prewarm pass. A stage that fails there fails the same way each
// time: the same object is still missing, the same endpointRef Secret is still
// absent, the same identity is still denied. Each of those resolves wrote its
// failure line again, at ERROR: one fault, hundreds of lines (#711: ~4,650
// lines/h of re-gate denies on 057; #1476: ~1,057 ERROR lines/24 h from three
// unchanged failures). Log volume then measures refresh work, not failures,
// and buries the failures that are new.
//
// THE RULE. A failure line is logged at ITS OWN level the first time its
// (target, class) is seen in the cell's current generation, and at DEBUG while
// it repeats unchanged. A different class for the same target, a new target,
// an eviction or a new generation logs at its own level again. It is neither a
// rate limit nor a cutoff: the dedupe key is the cell's generation, so the line
// comes back whenever the cell could have a different answer.
//
//   - target: what failed, named by the log site: the stage and the path it
//     read, the stage's endpointRef, a widget's apiRef. Held only as a SHA-256
//     digest, so the notes hold no path, name or identity in clear.
//   - class: a closed, identity-free label decided by the error's TYPE and its
//     status code — never its message, which can name the requester (#681 S1).
//     failureClassOf / failureClassOfCode build it; classPerIdentityDenied is
//     the expected per-identity deny (#711: a re-gate deny, #1476: the
//     caller's own Forbidden).
//   - its own level: decided by the site from the class: WARN for an expected
//     per-identity deny, DEBUG for a declared outcome (an authorized definite
//     NotFound under continueOnError, which does not go through the notes at
//     all), ERROR for everything else.
//
// Counters are not touched here: every stage-error, pre-dial, per-identity
// and external sink and expvar is bumped where the failure is decided, for
// every occurrence, exactly as before.
//
// WHAT A CELL GENERATION IS. The cell is the L1 key on the resolve's ctx (the
// refresher, the seed, the dispatchers and /call all set it). Its generation
// is one residency of that key in the resolved cache: the notes for a key are
// dropped whenever a failure is seen while the key is NOT resident (the cell
// was evicted, or has not been Put yet), so the next resident period logs its
// first failure again. The key itself folds the identity's RBAC class (#424),
// so an RBAC change that could change the verdict is a new key and logs again
// too. The notes hold nothing beyond that key: a digest of the target and a
// constant class label.
//
// BOUNDED, NO KNOB. Only keys that saw a failure are held, and a key holds one
// note per target its resolves failed on. Inserting a new key sweeps the notes
// of every key that is no longer resident, so the set tracks the resident
// failure-holding cells. With no L1 key or no resolved cache there is no cell
// to deduplicate on: the line logs at its own level each time.
//
// DRY RUNS. An inert (#443) resolve never touches the notes, so a draft resolve
// can neither spend nor reset a cell's first line. Its expected per-identity
// deny logs at DEBUG (#711); every other failure logs at its own level.

package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"strconv"
	"sync"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The closed failure classes that are not a status code.
const (
	// classPerIdentityDenied is an expected per-identity deny: the identity
	// may not make the read (IsPerIdentityDenied). Its own level is WARN.
	classPerIdentityDenied = "per_identity_denied"
	// classUncoded is a failure that carries no status code (a transport or
	// decode error).
	classUncoded = "error"
)

// failureTarget is the digest a failure's target is noted under.
type failureTarget [sha256.Size]byte

// failureNoted maps an L1 key to the class last logged at its own level for
// each target in the key's current generation.
var failureNoted sync.Map // string -> *sync.Map (failureTarget -> string)

// failureResidentFn reports whether an L1 key is resident. A seam only so a
// unit arm can drive generations without a store; production never reassigns
// it.
var failureResidentFn = func(l1Key string) (resident, tracked bool) {
	store := cache.ResolvedCache()
	if store == nil {
		return false, false
	}
	return store.Has(l1Key), true
}

// FailureLogLevel returns the level of a failure line logged under ctx's cell:
// own the first time (target, class) is seen in the cell's generation, DEBUG
// while it repeats unchanged. own is the class's level; target names what
// failed (any text: only its digest is kept); class is a closed,
// identity-free label (FailureClassOf, classPerIdentityDenied).
func FailureLogLevel(ctx context.Context, own slog.Level, target, class string) slog.Level {
	// #443 — an inert (dry-run) resolve neither reads nor writes the notes: it
	// cannot spend a cell's first line. Its expected deny is DEBUG (#711).
	if cache.Inert(ctx) {
		if class == classPerIdentityDenied {
			return slog.LevelDebug
		}
		return own
	}
	l1Key := cache.L1KeyFromContext(ctx)
	if l1Key == "" {
		return own
	}
	resident, tracked := failureResidentFn(l1Key)
	if !tracked {
		return own
	}
	if !resident {
		// No cell holds this key now: whatever was noted belonged to an
		// earlier generation. Start this one fresh.
		failureNoted.Delete(l1Key)
	}
	v, loaded := failureNoted.LoadOrStore(l1Key, &sync.Map{})
	if !loaded {
		sweepFailureNoted(l1Key)
	}
	prev, seen := v.(*sync.Map).Swap(failureTarget(sha256.Sum256([]byte(target))), class)
	if seen && prev.(string) == class {
		return slog.LevelDebug
	}
	return own
}

// expectedDenyLevel is the level of an expected per-identity deny's failure
// line for target (#711): WARN the first time in the cell's generation, DEBUG
// after.
func expectedDenyLevel(ctx context.Context, target string) slog.Level {
	return FailureLogLevel(ctx, slog.LevelWarn, target, classPerIdentityDenied)
}

// stageFailureTarget names a stage read as a failure target: the stage and the
// path it read (one stage reads many paths under an iterator).
func stageFailureTarget(id, path string) string {
	return "stage\x00" + id + "\x00" + path
}

// endpointRefTarget names a stage's endpointRef as a failure target.
func endpointRefTarget(id string, ref *templates.Reference) string {
	if ref == nil {
		return "endpointRef\x00" + id
	}
	return "endpointRef\x00" + id + "\x00" + ref.Namespace + "/" + ref.Name
}

// declaredOutcome reports whether a stage declared that a failure of call is
// an expected outcome: it continues on error and records the failure under its
// errorKey (#1476). An authorized definite NotFound (#1080) on such a stage is
// logged at DEBUG; it is counted as before.
func declaredOutcome(call httpcall.RequestOptions) bool {
	return call.ContinueOnError && call.ErrorKey != ""
}

// FailureClassOf is the closed, identity-free class of err: the expected
// per-identity deny, else the status code its chain carries (an apiserver
// Status, the page budget's 504), else classUncoded. Never its message.
func FailureClassOf(err error) string {
	if IsPerIdentityDenied(err) {
		return classPerIdentityDenied
	}
	if st, ok := credhttp.BudgetStatus(err); ok {
		return failureClassOfCode(int(st.Code))
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		return failureClassOfCode(int(status.Status().Code))
	}
	return classUncoded
}

// failureClassOfCode is the class of a failure that carries the status code.
func failureClassOfCode(code int) string {
	if code == 0 {
		return classUncoded
	}
	return strconv.Itoa(code)
}

// perIdentityDeniedError marks an error as an expected per-identity deny that
// is not a *regateDeniedError: the apiserver's Forbidden for the caller's own
// identity (#681 F2 on a stage, an apiRef read on the caller's credential, the
// SA read re-gated for the ctx identity), carried to the log site that reports
// it (#1476). It unwraps to the error it marks, so errors.As / apierrors.Is*
// and the text are unchanged.
type perIdentityDeniedError struct{ err error }

func (e *perIdentityDeniedError) Error() string { return e.err.Error() }
func (e *perIdentityDeniedError) Unwrap() error { return e.err }

// MarkPerIdentityDenied marks err as an expected per-identity deny. A nil err
// stays nil.
func MarkPerIdentityDenied(err error) error {
	if err == nil {
		return nil
	}
	return &perIdentityDeniedError{err: err}
}

// IsPerIdentityDenied reports whether err is an expected per-identity deny:
// snowplow's own re-gate deny (#711) or a deny marked by MarkPerIdentityDenied.
// Decided by type only.
func IsPerIdentityDenied(err error) bool {
	if isRegateDenied(err) {
		return true
	}
	var e *perIdentityDeniedError
	return errors.As(err, &e)
}

// sweepFailureNoted drops the notes of every key, other than keep, that is no
// longer resident.
func sweepFailureNoted(keep string) {
	failureNoted.Range(func(k, _ any) bool {
		key := k.(string)
		if key == keep {
			return true
		}
		if resident, tracked := failureResidentFn(key); tracked && !resident {
			failureNoted.Delete(key)
		}
		return true
	})
}

// resetFailureNotedForTest clears every note. Tests only.
func resetFailureNotedForTest() {
	failureNoted.Range(func(k, _ any) bool {
		failureNoted.Delete(k)
		return true
	})
}

// SetFailureLogResidentForTest makes the failure-log dedupe read residency
// from fn and clears every note, for an arm in another package. Production
// code MUST NOT call it. Returns a restore func that clears the notes again.
func SetFailureLogResidentForTest(fn func(l1Key string) bool) (restore func()) {
	resetFailureNotedForTest()
	prev := failureResidentFn
	failureResidentFn = func(k string) (bool, bool) { return fn(k), true }
	return func() {
		failureResidentFn = prev
		resetFailureNotedForTest()
	}
}
