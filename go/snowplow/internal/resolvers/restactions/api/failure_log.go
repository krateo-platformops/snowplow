// failure_log.go — a failure that is unchanged since the cell's previous
// resolve logs at DEBUG, not at its own level again (#711, generalised by
// #1476).
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
// THE RULE. Each resolve of a cell is a ROUND. A failure line logs at ITS OWN
// level unless the cell's PREVIOUS round failed with the same (target, class);
// then it logs at DEBUG. At the end of the round its own failures replace the
// previous round's. So, literally, "unchanged since the last resolve":
//   - a failure that repeats on every resolve logs once at its level, then DEBUG;
//   - a target that succeeds, or is not failing, in a round loses its note: if
//     it fails again later it logs at its own level (a recurrence is new);
//   - a different class for the same target, or a new target, logs again;
//   - a new generation of the cell logs again: when the first failure of a
//     round finds the cell's key NOT resident (evicted, never stored), the
//     previous round's notes are dropped first.
// It is neither a rate limit nor a cutoff.
//
// A RESOLVE SERVED BY REPLAY (#1569). A memo hit serves a widget the body
// another widget's resolve produced; nothing runs, so no line is logged. Its
// round still notes the failures that body carries (ReplayFailures), so it
// never ends as a success that wipes the cell's notes. A replayed failure is
// noted as not yet LOGGED in this cell: the cell's own first resolve of it
// logs at its level, and only a failure the cell has logged repeats at DEBUG.
// So per cell: the first occurrence of a failure in its residency at its
// level, every unchanged repeat at DEBUG, a changed class at its level again.
//
//   - target: what failed, named by the log site: the stage and the path it
//     read, the stage's endpointRef, a widget's apiRef. Held only as a SHA-256
//     digest, so the notes hold no path, name or identity in clear.
//   - class: a closed, identity-free label decided by the error's TYPE and its
//     status code — never its message, which can name the requester (#681 S1).
//     FailureClassOf / failureClassOfCode build it; classPerIdentityDenied is
//     the expected per-identity deny (#711: a re-gate deny, #1476: the
//     caller's own Forbidden), classUncoded a transport fault with no code.
//   - its own level: decided by the site from the class, BEFORE the line: WARN
//     for an expected per-identity deny, DEBUG for an authorized definite
//     NotFound under continueOnError (which does not go through the notes at
//     all), ERROR for everything else.
//
// Counters are not touched here: every stage-error, pre-dial, per-identity
// and external sink and expvar is bumped where the failure is decided, for
// every occurrence, exactly as before.
//
// WHERE A ROUND IS. BeginFailureRound opens one for the L1 key on the ctx, at
// the top of api.ResolveWithFailure and of widgets.Resolve; a resolve nested
// under the SAME key (a widget's apiRef RESTAction, a `resolve: true` stage)
// joins the open round, so the cell's failures are one set. A nested resolve
// under another key (the RAFullList cell) is that key's own round. A failure
// logged with no round for its ctx's key (no L1 key, the cache off) logs at its
// own level.
//
// OVERLAPPING ROUNDS. Two rounds of one key can run at once (a refresh and a
// /call; a keepwarm and a gvr-discovered pass on one cohort cell). Each reads
// the notes at its first failure and writes them at its end, as one
// compare-and-swap. A round whose read is stale by its end (another round of
// the key ended in between) does not write the targets it logged at DEBUG,
// so the next round logs them at their own level. So a failure is logged at
// DEBUG by a round only if that round read it as failing unchanged, and a
// recurrence that a concurrent recovery hides is hidden for that one
// overlapping round only. Two overlapping rounds that both fail can cost an
// extra line at the failure's own level, never a hidden one.
//
// BOUNDED BY CONSTRUCTION, NO KNOB.
//   - Per cell: the notes ARE the previous round's failures, so they are
//     bounded by the cell's current failures (an iterator over a churning list
//     holds the names failing now, not every name it ever failed on).
//   - Across cells: a round with no failure deletes its key's notes, and a
//     failing round on a non-resident key resets them, so what is held is the
//     resident cells whose last resolve failed, plus the keys noted since the
//     last sweep. The sweep drops non-resident keys; it runs once as many keys
//     have been inserted as were held when the last one ended, so its
//     residency probes (each one takes the resolved cache's lock) amortise to
//     at most two per inserted key. A round probes residency once, at its
//     first failure; a round without a failure never probes.
//
// WHAT IT DOES NOT REDUCE. A cell whose failing resolve is never stored is
// never resident, so every round resets: its line logs at its own level each
// time. The dedupe follows residency on purpose: a cell nobody holds has no
// generation to dedupe within, and holding notes for keys that are never
// stored (a request's extras make the key) would be the unbounded set this
// avoids.
//
// DRY RUNS. An inert (#443) resolve opens no round and never touches the
// notes, so a draft resolve can neither spend nor reset a cell's first line.
// Its expected per-identity deny logs at DEBUG (#711); every other failure
// logs at its own level.

package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/http/response"
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
	// classUncoded is a failure that carries no status code (a transport
	// fault: a reset, a refused connection, a TLS error).
	classUncoded = "error"
)

// failureTarget is the digest a failure's target is noted under.
type failureTarget [sha256.Size]byte

// noteVal is one noted failure: its class, and whether the cell itself logged
// it (#1569): a failure the cell's round only took over from a replayed resolve
// (ReplayFailures) has not had its line in this cell yet.
type noteVal struct {
	class  string
	logged bool
}

// failureNotes is one cell's previous-round failures: target -> noted failure.
type failureNotes map[failureTarget]noteVal

// noteEntry is one write of a key's notes. Every write stores a new entry, so
// its pointer is the key's version: a round that loaded entry e knows the
// notes moved under it when the key holds anything but e at its end.
type noteEntry struct{ notes failureNotes }

var (
	// failureNoted maps an L1 key to the failures of its last round.
	failureNoted sync.Map // string -> *noteEntry
	// failureNotedKeys is the number of keys held; failureInsertsSinceSweep
	// the number inserted since the last sweep, and failureSweepAfter the
	// number held when it ended: the next sweep runs once that many have been
	// inserted (the amortisation clock).
	failureNotedKeys         atomic.Int64
	failureInsertsSinceSweep atomic.Int64
	failureSweepAfter        atomic.Int64
	failureSweeping          atomic.Bool
)

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

// failureRound is one resolve of a cell.
type failureRound struct {
	key  string
	once sync.Once
	// off: no resolved cache to follow residency in; no dedupe.
	off bool
	// loaded is the entry the round read (nil: none), prev its notes.
	loaded *noteEntry
	prev   failureNotes
	mu     sync.Mutex
	cur    failureNotes
	// debugged are the targets this round logged at DEBUG, on prev's word.
	debugged map[failureTarget]struct{}
	// inherited are the replayed targets this round noted as logged, on prev's
	// word (#1569).
	inherited map[failureTarget]struct{}
}

type failureRoundCtxKey struct{}

// BeginFailureRound opens the failure-log round of the cell whose L1 key is on
// ctx, and returns the ctx that carries it and the func that ends it (its
// failures replace the cell's notes). With no L1 key, on an inert ctx, or when
// a round for the same key is already open on ctx, it returns ctx unchanged
// and a no-op: the enclosing round is the cell's.
func BeginFailureRound(ctx context.Context) (context.Context, func()) {
	if cache.Inert(ctx) {
		return ctx, func() {}
	}
	key := cache.L1KeyFromContext(ctx)
	if key == "" {
		return ctx, func() {}
	}
	if r, ok := ctx.Value(failureRoundCtxKey{}).(*failureRound); ok && r.key == key {
		return ctx, func() {}
	}
	r := &failureRound{key: key}
	return context.WithValue(ctx, failureRoundCtxKey{}, r), r.end
}

// load reads the cell's previous-round notes, at the round's first failure.
func (r *failureRound) load() {
	resident, tracked := failureResidentFn(r.key)
	if !tracked {
		r.off = true
		return
	}
	if !resident {
		// No cell holds this key now: whatever was noted belonged to an
		// earlier generation, or to a resolve whose cell was never stored.
		if _, ok := failureNoted.LoadAndDelete(r.key); ok {
			failureNotedKeys.Add(-1)
		}
		return
	}
	if v, ok := failureNoted.Load(r.key); ok {
		r.loaded = v.(*noteEntry)
		r.prev = r.loaded.notes
	}
}

// level notes (target, class) in the round and returns its line's level: DEBUG
// only when the cell's previous round failed on target with the same class AND
// the cell had logged that failure.
func (r *failureRound) level(own slog.Level, target, class string) slog.Level {
	r.once.Do(r.load)
	if r.off {
		return own
	}
	t := failureTarget(sha256.Sum256([]byte(target)))
	prev, seen := r.prev[t]
	debug := seen && prev.class == class && prev.logged
	r.mu.Lock()
	if r.cur == nil {
		r.cur = failureNotes{}
	}
	r.cur[t] = noteVal{class: class, logged: true}
	if debug {
		if r.debugged == nil {
			r.debugged = map[failureTarget]struct{}{}
		}
		r.debugged[t] = struct{}{}
	}
	r.mu.Unlock()
	if debug {
		return slog.LevelDebug
	}
	return own
}

// replayed notes a failure the round's body carries without the round having
// run what failed (#1569: a memo hit). Nothing is logged. It is the cell's
// failure now, so it is noted and survives the round, and it counts as logged
// in this cell only if the cell's previous round had logged it with the same
// class: the cell's own first resolve of it still logs at its level. A failure
// the round logged itself is kept as it is.
func (r *failureRound) replayed(t failureTarget, class string) {
	r.once.Do(r.load)
	if r.off {
		return
	}
	prev, seen := r.prev[t]
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur == nil {
		r.cur = failureNotes{}
	}
	if c, own := r.cur[t]; own && c.logged {
		return
	}
	logged := seen && prev.class == class && prev.logged
	r.cur[t] = noteVal{class: class, logged: logged}
	if logged {
		if r.inherited == nil {
			r.inherited = map[failureTarget]struct{}{}
		}
		r.inherited[t] = struct{}{}
	}
}

// end replaces the cell's notes with this round's failures: none (a round
// without a failure, or one with no store to follow) deletes them.
//
// If the notes moved since this round read them (another round of the key
// ended in between: a success that deleted them, a failure that rewrote
// them), the targets this round logged at DEBUG on the old notes' word are
// not written: the next round logs them at their own level. So a failure is
// logged at DEBUG only by rounds whose own read of the notes shows it failing
// unchanged, and a round that overlapped a change can hide it for that one
// round only. The check and the write are one compare-and-swap on the key's
// entry, so no other round's end can slip between them.
func (r *failureRound) end() {
	r.mu.Lock()
	cur, debugged, inherited := r.cur, r.debugged, r.inherited
	r.mu.Unlock()
	if r.off {
		cur = nil
	}
	for {
		v, present := failureNoted.Load(r.key)
		var now *noteEntry
		if present {
			now = v.(*noteEntry)
		}
		next := cur
		if now != r.loaded && (len(debugged) > 0 || len(inherited) > 0) {
			// #1569 — and a replayed failure noted as logged on the old notes'
			// word is written as not logged: the cell's next resolve of it logs
			// at its level.
			next = make(failureNotes, len(cur))
			for t, c := range cur {
				if _, d := debugged[t]; d {
					continue
				}
				if _, i := inherited[t]; i {
					c.logged = false
				}
				next[t] = c
			}
		}
		if len(next) == 0 {
			if !present {
				return
			}
			if failureNoted.CompareAndDelete(r.key, v) {
				failureNotedKeys.Add(-1)
				return
			}
			continue
		}
		e := &noteEntry{notes: next}
		if !present {
			if _, raced := failureNoted.LoadOrStore(r.key, e); raced {
				continue
			}
			failureNotedKeys.Add(1)
			failureInsertsSinceSweep.Add(1)
			sweepFailureNoted(r.key)
			return
		}
		if failureNoted.CompareAndSwap(r.key, v, e) {
			return
		}
	}
}

// FailureLogLevel returns the level of a failure line logged under ctx's
// cell: own, unless the cell's previous round failed on target with the same
// class, then DEBUG. own is the class's level; target names what failed (any
// text: only its digest is kept); class is a closed, identity-free label
// (FailureClassOf, classPerIdentityDenied). With no open round for ctx's key
// the line keeps its own level.
func FailureLogLevel(ctx context.Context, own slog.Level, target, class string) slog.Level {
	// #443 — an inert (dry-run) resolve neither reads nor writes the notes: it
	// cannot spend a cell's first line. Its expected deny is DEBUG (#711).
	if cache.Inert(ctx) {
		if class == classPerIdentityDenied {
			return slog.LevelDebug
		}
		return own
	}
	r, ok := ctx.Value(failureRoundCtxKey{}).(*failureRound)
	if !ok || r.key != cache.L1KeyFromContext(ctx) {
		return own
	}
	t := failureTarget(sha256.Sum256([]byte(target)))
	captureFailure(ctx, r.key, t, class)
	return r.level(own, target, class)
}

// ── a resolve served by replay (#1569) ───────────────────────────────────────
//
// A resolve can be served without running what it replaces: a SeedResolveMemo
// hit returns the body another widget's resolve produced (#130 F4, on the
// refresher since #598). Its round then logs no failure, and a round without a
// failure deletes the cell's notes, so the cell's next real resolve logged its
// unchanged failures at their level again. On 057 seven widgets share
// dashboard-inbox: each dirty-mark burst, whichever widget is dequeued first
// resolves it and the others hit the memo, and every producer had been a
// hitter the burst before (~0.5 lines/s, #1569).
//
// So the producing resolve's failures travel with what it produced, as its
// stage outcomes and Put-gate effects do (#1319, #912): WithFailureCapture
// collects the (target, class) pairs noted in the producer's cell, the memo
// stores them (Stored), and the hit notes them in the hitter's round
// (ReplayFailures), logging nothing. The hitter's notes are then its body's
// real failures: an unchanged one stays DEBUG at its next real resolve, a
// recovery the producer saw clears it (a recurrence logs again), and a changed
// class logs at its level.

type failureCaptureCtxKey struct{}

// FailureCapture collects the failures noted in one cell (its key) by the
// resolves under the ctx that carries it.
type FailureCapture struct {
	key    string
	parent *FailureCapture
	mu     sync.Mutex
	seen   map[failureTarget]string
}

// WithFailureCapture returns a ctx whose resolves' failures, noted in the round
// of ctx's cell (its L1 key), are also collected by the returned capture. A
// failure noted in another cell's round (a nested resolve under its own key) is
// that cell's, and is not collected. With no L1 key, or on an inert ctx, it
// collects nothing.
func WithFailureCapture(ctx context.Context) (context.Context, *FailureCapture) {
	c := &FailureCapture{key: cache.L1KeyFromContext(ctx)}
	if ctx == nil || c.key == "" || cache.Inert(ctx) {
		return ctx, c
	}
	c.parent, _ = ctx.Value(failureCaptureCtxKey{}).(*FailureCapture)
	return context.WithValue(ctx, failureCaptureCtxKey{}, c), c
}

// captureFailure records (t, class), noted in the round of cell key, in every
// capture on ctx collecting that cell.
func captureFailure(ctx context.Context, key string, t failureTarget, class string) {
	c, _ := ctx.Value(failureCaptureCtxKey{}).(*FailureCapture)
	for ; c != nil; c = c.parent {
		if c.key != key {
			continue
		}
		c.mu.Lock()
		if c.seen == nil {
			c.seen = map[failureTarget]string{}
		}
		c.seen[t] = class
		c.mu.Unlock()
	}
}

// Stored is the captured failures as one string, for a memo entry to carry:
// "<target digest hex>=<class>" pairs, sorted, newline-separated; "" for none.
// It holds digests and closed class labels only, never a target in clear.
// nil-receiver-safe.
func (c *FailureCapture) Stored() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(c.seen))
	for t, class := range c.seen {
		pairs = append(pairs, hex.EncodeToString(t[:])+"="+class)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\n")
}

// ReplayFailures notes the failures a replayed resolve produced (Stored) in the
// round of ctx's cell, logging nothing (see failureRound.replayed), and in any
// capture collecting that cell. With no round for ctx's key it does nothing.
func ReplayFailures(ctx context.Context, stored string) {
	if stored == "" || cache.Inert(ctx) {
		return
	}
	r, ok := ctx.Value(failureRoundCtxKey{}).(*failureRound)
	if !ok || r.key != cache.L1KeyFromContext(ctx) {
		return
	}
	for _, pair := range strings.Split(stored, "\n") {
		h, class, ok := strings.Cut(pair, "=")
		if !ok || class == "" {
			continue
		}
		var t failureTarget
		if b, err := hex.DecodeString(h); err != nil || len(b) != len(t) {
			continue
		} else {
			copy(t[:], b)
		}
		captureFailure(ctx, r.key, t, class)
		r.replayed(t, class)
	}
}

// FailureLevelOf is FailureLogLevel for a failure line about err: its class is
// FailureClassOf(err). Every failure line a cell's resolve logs goes through
// FailureLogLevel, so an unchanged failure of a resident cell is DEBUG at every
// site (#1569).
func FailureLevelOf(ctx context.Context, own slog.Level, target string, err error) slog.Level {
	return FailureLogLevel(ctx, own, target, FailureClassOf(err))
}

// expectedDenyLevel is the level of an expected per-identity deny's failure
// line for target (#711): WARN unless the cell's previous round was denied
// the same way, then DEBUG.
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

// FailureClassOf is the closed, identity-free class of err: the expected
// per-identity deny, else the status code its chain carries (an apiserver
// Status, the page budget's 504), else classUncoded. Never its message.
//
// At the internal-rest-config and discovery sites the code is the one
// client-go read, which for a Status body is the body's code. Those sites dial
// only the cluster's own apiserver with snowplow's credential, the authority
// every RBAC decision already trusts, and the class steers only which repeat
// is DEBUG, never a line's own level: so it is taken as read. The external
// fetch, where the upstream may be anyone, uses the HTTP status line's code.
func FailureClassOf(err error) string {
	if IsPerIdentityDenied(err) {
		return classPerIdentityDenied
	}
	if st, ok := credhttp.BudgetStatus(err); ok {
		return failureClassOfCode(int(st.Code))
	}
	if st, ok := credhttp.DestinationStatus(err); ok {
		return failureClassOfCode(int(st.Code))
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		return failureClassOfCode(int(status.Status().Code))
	}
	return classUncoded
}

// externalFailureClass is the class of an external fetch's failure. The fetch
// shapes every fault into one 500 envelope (an upstream 5xx after the retry
// client gave up, a reset connection, a refused dial, a 2xx body that could
// not be used), so the class reads what the dial itself observed: answered is
// the status line of its last attempt (credhttp.Answer.LastStatus), 0 when no
// attempt got one. With an answer, its code (a body that arrived and could not
// be used is its 2xx); with none, the fault's own class (the page budget's
// 504, the destination refusal, else classUncoded).
func externalFailureClass(res *response.Status, fetchErr error, answered int) string {
	switch {
	case answered != 0:
		return failureClassOfCode(answered)
	case fetchErr != nil:
		return FailureClassOf(fetchErr)
	case res != nil:
		return failureClassOfCode(res.Code)
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

// sweepFailureNoted drops the notes of every key that is no longer resident,
// once as many keys have been inserted since the last sweep as were held when
// it ended. A sweep probes every held key, at most that number plus the
// inserts since: at most two residency probes per inserted key, amortised.
// One sweep at a time. keep, the key whose insertion ran it, is kept: its
// round has just ended and its cell is about to be stored (a first resolve
// ends before the dispatcher's Put).
func sweepFailureNoted(keep string) {
	if failureInsertsSinceSweep.Load() < failureSweepAfter.Load() {
		return
	}
	if !failureSweeping.CompareAndSwap(false, true) {
		return
	}
	defer failureSweeping.Store(false)
	failureInsertsSinceSweep.Store(0)
	failureNoted.Range(func(k, _ any) bool {
		if k.(string) == keep {
			return true
		}
		if resident, tracked := failureResidentFn(k.(string)); tracked && !resident {
			if _, ok := failureNoted.LoadAndDelete(k); ok {
				failureNotedKeys.Add(-1)
			}
		}
		return true
	})
	failureSweepAfter.Store(failureNotedKeys.Load())
}

// resetFailureNotedForTest clears every note. Tests only.
func resetFailureNotedForTest() {
	failureNoted.Range(func(k, _ any) bool {
		failureNoted.Delete(k)
		return true
	})
	failureNotedKeys.Store(0)
	failureInsertsSinceSweep.Store(0)
	failureSweepAfter.Store(0)
}

// failureNotedForTest is the number of keys held and the number of notes the
// key holds. Tests only.
func failureNotedForTest(key string) (keys int64, notes int) {
	if v, ok := failureNoted.Load(key); ok {
		notes = len(v.(*noteEntry).notes)
	}
	return failureNotedKeys.Load(), notes
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
