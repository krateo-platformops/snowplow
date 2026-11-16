package api

// issue1476_failure_log_test.go — #1476: a failure whose class and target are
// unchanged since its last line in the cell's generation logs once at its own
// level, then DEBUG; an authorized definite NotFound a stage declared
// (continueOnError, recorded under its errorKey) logs at DEBUG; the caller's
// own Forbidden is an expected per-identity deny (WARN, then DEBUG). Every
// occurrence is still counted. Driven through the REAL api.Resolve: branch C
// (#677 harness, an internal driver's SA dial re-gated for u677) and branch E
// (#681 harness, the requester's own dial).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// stage1476 is a GET stage; coe is its continueOnError.
func stage1476(id, path string, coe bool) *templates.API {
	return &templates.API{Name: id, Path: path, Verb: ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(coe), ErrorKey: ptr.To(id + "Err")}
}

// Rule 1 — an identical failure repeated in one generation: its own level
// once, DEBUG after; every occurrence is counted on the stage-error sink.
func TestIssue1476_UnchangedFailure_OnceAtItsLevelThenDebug_EveryOneCounted(t *testing.T) {
	const n = 5
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	stage := stage1476("cm", "/api/v1/namespaces/team-a/configmaps/ghost", false)
	for i := 0; i < n; i++ {
		s := resolveStage711(t, rec, "cell-1476-a", stage)
		if s.stage.Count() != 1 {
			t.Fatalf("resolve %d: stage-error sink counted %d, want 1 (every occurrence counts)", i, s.stage.Count())
		}
	}
	by, _ := rec.failures()
	if by[slog.LevelError] != 1 || by[slog.LevelDebug] != n-1 || by[slog.LevelWarn] != 0 {
		t.Fatalf("want 1 ERROR then %d DEBUG, got %v", n-1, by)
	}
}

// A different target in the same cell logs at its own level again; another
// cell is its own generation.
func TestIssue1476_DifferentTargetOrCell_LogsAgain(t *testing.T) {
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	ghost := stage1476("cm", "/api/v1/namespaces/team-a/configmaps/ghost", false)
	ghost2 := stage1476("cm", "/api/v1/namespaces/team-a/configmaps/ghost2", false)
	resolveStage711(t, rec, "cell-1476-a", ghost)
	resolveStage711(t, rec, "cell-1476-a", ghost)
	resolveStage711(t, rec, "cell-1476-a", ghost2)
	resolveStage711(t, rec, "cell-1476-b", ghost)
	by, _ := rec.failures()
	if by[slog.LevelError] != 3 || by[slog.LevelDebug] != 1 {
		t.Fatalf("a new target and a new cell must each log at ERROR: want 3 ERROR / 1 DEBUG, got %v", by)
	}
}

// A changed class on the same target logs again, and the line follows the
// class last logged (it is not a set of everything ever seen).
func TestIssue1476_ChangedClass_LogsAgain(t *testing.T) {
	setup711(t, func(string) bool { return true })
	ctx := cache.WithL1KeyContext(context.Background(), "cell-1476-c")
	steps := []struct {
		class string
		want  slog.Level
	}{
		{"404", slog.LevelError}, {"404", slog.LevelDebug},
		{"500", slog.LevelError}, {"500", slog.LevelDebug},
		{"404", slog.LevelError}, {"404", slog.LevelDebug},
	}
	for i, s := range steps {
		if got := round1476(ctx, "t", s.class); got != s.want {
			t.Fatalf("step %d (class %s): level %v, want %v", i, s.class, got, s.want)
		}
	}
	// No L1 key: no cell to deduplicate on, the line keeps its level.
	for i := 0; i < 2; i++ {
		if got := round1476(context.Background(), "t", "404"); got != slog.LevelError {
			t.Fatalf("no cell: level %v, want ERROR every time", got)
		}
	}
}

// round1476 is one resolve of ctx's cell that fails on target with class
// (class "" = the resolve does not fail), and the failure line's level.
func round1476(ctx context.Context, target, class string) slog.Level {
	ctx, end := BeginFailureRound(ctx)
	defer end()
	if class == "" {
		return slog.LevelInfo
	}
	return FailureLogLevel(ctx, slog.LevelError, target, class)
}

// B2 — "unchanged since the last resolve" is literal: a resolve in which the
// target succeeds drops its note, so a recurrence logs at its own level.
func TestIssue1476_RecoveryThenRecurrence_LogsAgain(t *testing.T) {
	setup711(t, func(string) bool { return true })
	ctx := cache.WithL1KeyContext(context.Background(), "cell-1476-r")
	want := []struct {
		class string
		level slog.Level
	}{
		{"500", slog.LevelError}, {"500", slog.LevelDebug},
		{"", slog.LevelInfo}, {"", slog.LevelInfo},
		{"500", slog.LevelError}, {"500", slog.LevelDebug},
	}
	for i, s := range want {
		if got := round1476(ctx, "t", s.class); got != s.level {
			t.Fatalf("resolve %d (fails=%q): level %v, want %v", i, s.class, got, s.level)
		}
	}
	if _, notes := failureNotedForTest("cell-1476-r"); notes != 1 {
		t.Fatalf("the cell holds %d notes, want 1 (its last resolve's failure)", notes)
	}
	// A successful resolve holds nothing.
	round1476(ctx, "t", "")
	if keys, notes := failureNotedForTest("cell-1476-r"); notes != 0 || keys != 0 {
		t.Fatalf("after a successful resolve: %d keys, %d notes held, want 0 and 0", keys, notes)
	}
}

// B2 — evicted, then stored again by a SUCCESSFUL resolve: the next failure is
// a new generation's first.
func TestIssue1476_EvictedThenRePutBySuccess_FirstFailureLogs(t *testing.T) {
	var resident atomic.Bool
	resident.Store(true)
	setup711(t, func(string) bool { return resident.Load() })
	ctx := cache.WithL1KeyContext(context.Background(), "cell-1476-g")
	if got := round1476(ctx, "t", "500"); got != slog.LevelError {
		t.Fatalf("first failure: %v", got)
	}
	resident.Store(false)
	round1476(ctx, "t", "") // re-resolved successfully while evicted
	resident.Store(true)    // re-Put
	if got := round1476(ctx, "t", "500"); got != slog.LevelError {
		t.Fatalf("the first failure of the new generation logged at %v, want ERROR", got)
	}
}

// C1 — two rounds of one cell overlap while the upstream changes: P fails; S
// starts (the upstream recovered: it succeeds) and, before S ends, F starts on
// the same key and meets the fault again. F read P's notes, so its line is
// DEBUG: that one overlapping round is the most a recurrence can be hidden.
// The notes moved under F (S ended and deleted them), so F must not write the
// target it logged at DEBUG: the next round logs the fault at its own level.
// Both orders of the two ends.
func TestIssue1476_OverlappingRecoveryAndRecurrence_HiddenForOneRoundAtMost(t *testing.T) {
	for _, sEndsFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("s_ends_first=%v", sEndsFirst), func(t *testing.T) {
			setup711(t, func(string) bool { return true })
			base := cache.WithL1KeyContext(context.Background(), "cell-1476-c1")
			lv := func(ctx context.Context) slog.Level { return FailureLogLevel(ctx, slog.LevelError, "t", "500") }
			if got := round1476(base, "t", "500"); got != slog.LevelError {
				t.Fatalf("P: %v, want ERROR", got)
			}
			_, sEnd := BeginFailureRound(base)
			fctx, fEnd := BeginFailureRound(base)
			if got := lv(fctx); got != slog.LevelDebug {
				t.Fatalf("PRECONDITION: F read P's notes, so its line is DEBUG, got %v", got)
			}
			if sEndsFirst {
				sEnd()
				fEnd()
			} else {
				fEnd()
				sEnd()
			}
			want := []slog.Level{slog.LevelError, slog.LevelDebug, slog.LevelDebug}
			var later []slog.Level
			for range want {
				later = append(later, round1476(base, "t", "500"))
			}
			if fmt.Sprint(later) != fmt.Sprint(want) {
				t.Fatalf("after the overlap the recurring fault must log at its level once: want %v, got %v", want, later)
			}
		})
	}
}

// Two overlapping rounds that both fail the same way: the one whose read went
// stale by its end does not write what it logged at DEBUG, which can cost an
// extra line at the failure's own level, never a hidden one.
func TestIssue1476_OverlappingFailingRounds_NeverHide(t *testing.T) {
	setup711(t, func(string) bool { return true })
	base := cache.WithL1KeyContext(context.Background(), "cell-1476-c1b")
	round1476(base, "t", "500") // ERROR
	actx, aEnd := BeginFailureRound(base)
	bctx, bEnd := BeginFailureRound(base)
	FailureLogLevel(actx, slog.LevelError, "t", "500")
	FailureLogLevel(bctx, slog.LevelError, "t", "500")
	aEnd()
	bEnd()
	if got := round1476(base, "t", "500"); got != slog.LevelError {
		t.Fatalf("after two overlapping failing rounds, the next line is %v: the stale round must not keep the DEBUG", got)
	}
}

// Rounds of one key in many goroutines, each with concurrent failures, some
// succeeding: no race (-race), and the key count matches what is held.
func TestIssue1476_ConcurrentRounds_KeepTheCountTrue(t *testing.T) {
	setup711(t, func(string) bool { return true })
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				base := cache.WithL1KeyContext(context.Background(), fmt.Sprintf("cell-1476-cc-%d", i%3))
				ctx, end := BeginFailureRound(base)
				if (g+i)%3 != 0 {
					var iw sync.WaitGroup
					for s := 0; s < 4; s++ {
						iw.Add(1)
						go func(s int) {
							defer iw.Done()
							FailureLogLevel(ctx, slog.LevelError, fmt.Sprintf("t%d", s), "500")
						}(s)
					}
					iw.Wait()
				}
				end()
			}
		}(g)
	}
	wg.Wait()
	held := int64(0)
	failureNoted.Range(func(_, _ any) bool { held++; return true })
	if keys, _ := failureNotedForTest(""); keys != held {
		t.Fatalf("the key count reads %d, %d keys are held", keys, held)
	}
}

// A resolve nested under ANOTHER cell key (the RAFullList cell under a
// widget's) is that key's own round: its failures are noted under its key,
// not joined into the enclosing cell's round.
func TestIssue1476_NestedResolveUnderAnotherKey_IsItsOwnRound(t *testing.T) {
	setup711(t, func(string) bool { return true })
	outer := cache.WithL1KeyContext(context.Background(), "cell-1476-outer")
	nested := func() slog.Level {
		octx, oEnd := BeginFailureRound(outer)
		defer oEnd()
		ictx, iEnd := BeginFailureRound(cache.WithL1KeyContext(octx, "cell-1476-inner"))
		defer iEnd()
		return FailureLogLevel(ictx, slog.LevelError, "t", "500")
	}
	if got := nested(); got != slog.LevelError {
		t.Fatalf("first: %v, want ERROR", got)
	}
	if got := nested(); got != slog.LevelDebug {
		t.Fatalf("the nested cell's unchanged failure logged at %v: its round must be its own", got)
	}
	if _, notes := failureNotedForTest("cell-1476-inner"); notes != 1 {
		t.Fatalf("the nested cell holds %d notes, want 1", notes)
	}
}

// B3 — the notes are the cell's current failures: an iterator whose failing
// item changes on every refresh holds one note, not one per name it ever
// failed on.
func TestIssue1476_ChurningTargets_HoldOnlyTheCurrentFailures(t *testing.T) {
	setup711(t, func(string) bool { return true })
	ctx := cache.WithL1KeyContext(context.Background(), "cell-1476-churn")
	const n = 2000
	for i := 0; i < n; i++ {
		target := stageFailureTarget("pods", fmt.Sprintf("/api/v1/namespaces/a/pods/p-%d", i))
		if got := round1476(ctx, target, "500"); got != slog.LevelError {
			t.Fatalf("refresh %d: a new target logged at %v, want ERROR", i, got)
		}
	}
	if keys, notes := failureNotedForTest("cell-1476-churn"); keys != 1 || notes != 1 {
		t.Fatalf("after %d churned targets: %d keys, %d notes, want 1 and 1", n, keys, notes)
	}
}

// N2 — the notes of keys that are no longer resident are swept, and the
// sweep's residency probes amortise: a failing resolve of a non-resident key
// probes once, it does not sweep every noted key.
func TestIssue1476_NonResidentKeysAreSwept_AndProbesAmortise(t *testing.T) {
	var mu sync.Mutex
	gone := map[string]bool{}
	var probes atomic.Int64
	resetFailureNotedForTest()
	t.Cleanup(resetFailureNotedForTest)
	prev := failureResidentFn
	failureResidentFn = func(k string) (bool, bool) {
		probes.Add(1)
		mu.Lock()
		defer mu.Unlock()
		return !gone[k], true
	}
	t.Cleanup(func() { failureResidentFn = prev })
	const k = 500
	for i := 0; i < k; i++ {
		round1476(cache.WithL1KeyContext(context.Background(), fmt.Sprintf("old-%d", i)), "t", "500")
	}
	mu.Lock()
	for i := 0; i < k; i++ {
		gone[fmt.Sprintf("old-%d", i)] = true
	}
	mu.Unlock()
	// Ten failing resolves of a key that is never stored: one probe each.
	probes.Store(0)
	cold := cache.WithL1KeyContext(context.Background(), "cold")
	mu.Lock()
	gone["cold"] = true
	mu.Unlock()
	for i := 0; i < 10; i++ {
		if got := round1476(cold, "t", "500"); got != slog.LevelError {
			t.Fatalf("a never-stored cell is never resident: each failure keeps its level, got %v", got)
		}
	}
	if p := probes.Load(); p > 20 {
		t.Fatalf("10 failing resolves of a non-resident key with %d noted keys made %d residency probes", k, p)
	}
	// New resident cells fail: before as many have been inserted as were held,
	// the evicted keys are swept.
	probes.Store(0)
	for i := 0; i < 2*k; i++ {
		round1476(cache.WithL1KeyContext(context.Background(), fmt.Sprintf("new-%d", i)), "t", "500")
	}
	for i := 0; i < k; i++ {
		if _, ok := failureNoted.Load(fmt.Sprintf("old-%d", i)); ok {
			t.Fatalf("an evicted key's notes were never swept (old-%d)", i)
		}
	}
	if keys, _ := failureNotedForTest(""); keys > int64(2*k)+1 {
		t.Fatalf("%d keys held, want at most the %d resident failing cells (+1)", keys, 2*k)
	}
	if p := probes.Load(); p > int64(2*2*k)+int64(2*k) {
		t.Fatalf("%d failing resolves made %d residency probes: not amortised", 2*k, p)
	}
}

// A new generation (the key left the store) logs at its own level again.
func TestIssue1476_NewGeneration_LogsAgain(t *testing.T) {
	var mu sync.Mutex
	resident := true
	setup711(t, func(string) bool { mu.Lock(); defer mu.Unlock(); return resident })
	rec := &rec711{}
	stage := stage1476("cm", "/api/v1/namespaces/team-a/configmaps/ghost", false)
	resolveStage711(t, rec, "cell-1476-a", stage)
	resolveStage711(t, rec, "cell-1476-a", stage)
	mu.Lock()
	resident = false
	mu.Unlock()
	resolveStage711(t, rec, "cell-1476-a", stage)
	mu.Lock()
	resident = true
	mu.Unlock()
	resolveStage711(t, rec, "cell-1476-a", stage)
	by, _ := rec.failures()
	if by[slog.LevelError] != 2 || by[slog.LevelDebug] != 2 {
		t.Fatalf("new generation: want 2 ERROR / 2 DEBUG, got %v", by)
	}
}

// Rule 2 — an authorized definite NotFound (#1080 N2) on a stage that declares
// continueOnError and records it under its errorKey is a declared outcome:
// DEBUG every time, the first one included, and counted every time.
func TestIssue1476_DeclaredAuthorizedNotFound_IsDebug_AndCounted(t *testing.T) {
	const n = 3
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	stage := stage1476("cm", "/api/v1/namespaces/team-a/configmaps/ghost", true)
	for i := 0; i < n; i++ {
		s := resolveStage711(t, rec, "cell-1476-a", stage)
		if s.stage.Count() != 1 || s.forb.Stage() != 1 {
			t.Fatalf("resolve %d: stage-error %d, per-identity %d, want 1 and 1 (the definite NotFound is counted)",
				i, s.stage.Count(), s.forb.Stage())
		}
	}
	by, _ := rec.failures()
	if by[slog.LevelDebug] != n || by[slog.LevelError] != 0 || by[slog.LevelWarn] != 0 {
		t.Fatalf("a declared authorized NotFound must log at DEBUG every time, got %v", by)
	}
}

// resolveAsUser1476 is resolveAsUser681 under a cell key and a recording
// logger: the requester's own dial (branch E).
func resolveAsUser1476(t *testing.T, url, l1Key string, rec *rec711, stages ...*templates.API) sinks681 {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secret := kubeutil.MakeDNS1123Compatible(snt681User) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		secret: {ObjectMeta: authnMeta("krateo-system", secret, snt681User),
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte("tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	ctx := cache.WithL1KeyContext(ctxWithUser(snt681User, snt681Group), l1Key)
	ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(slog.New(rec)))
	ctx, s := withSinks681(ctx)
	Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: url}, AuthnNS: "krateo-system", Items: stages,
		RESTActionNamespace: "krateo-system", RESTActionName: "ra1476"})
	return s
}

func setup1476E(t *testing.T) {
	t.Helper()
	resetFailureNotedForTest()
	t.Cleanup(resetFailureNotedForTest)
	prev := failureResidentFn
	failureResidentFn = func(string) (bool, bool) { return true, true }
	t.Cleanup(func() { failureResidentFn = prev })
}

// Rule 3 — the caller's own Forbidden from the apiserver on its own dial (#681
// F2) is an expected per-identity deny: WARN once in the generation, then
// DEBUG; counted every time.
func TestIssue1476_CallersOwnForbidden_WarnOnceThenDebug(t *testing.T) {
	const n = 3
	setup1476E(t)
	srv, _ := userAPI681(t)
	rec := &rec711{}
	for i := 0; i < n; i++ {
		s := resolveAsUser1476(t, srv.URL, "cell-1476-e", rec, stage1476("catalog", "/api/v1/namespaces/team-b/configmaps/catalog", true))
		if s.stage.Count() != 1 || s.forb.Stage() != 1 {
			t.Fatalf("resolve %d: stage-error %d, per-identity %d, want 1 and 1", i, s.stage.Count(), s.forb.Stage())
		}
	}
	by, _ := rec.failures()
	if by[slog.LevelWarn] != 1 || by[slog.LevelDebug] != n-1 || by[slog.LevelError] != 0 {
		t.Fatalf("the caller's own 403: want 1 WARN then %d DEBUG, got %v", n-1, by)
	}
}

// A failure under continueOnError that is not a definite NotFound keeps its
// ERROR (once per generation): the declaration demotes only the declared
// outcome. "ratelimited" is a 403 without the Forbidden reason, so it is not
// the caller's Forbidden either.
func TestIssue1476_NonNotFoundUnderContinueOnError_KeepsError(t *testing.T) {
	setup1476E(t)
	srv, _ := userAPI681(t)
	rec := &rec711{}
	for i := 0; i < 2; i++ {
		resolveAsUser1476(t, srv.URL, "cell-1476-e", rec, stage1476("rl", "/api/v1/namespaces/team-a/configmaps/ratelimited", true))
	}
	by, _ := rec.failures()
	if by[slog.LevelError] != 1 || by[slog.LevelDebug] != 1 || by[slog.LevelWarn] != 0 {
		t.Fatalf("a non-NotFound under continueOnError: want 1 ERROR then 1 DEBUG, got %v", by)
	}
}

// The caller's own Forbidden on a stage WITHOUT continueOnError reaches the
// resolve's StageFailure still marked as a per-identity deny (so a widget's
// apiRef line classifies it), and still reads as a typed 403.
func TestIssue1476_CallersOwnForbidden_StageFailureIsMarked(t *testing.T) {
	setup1476E(t)
	srv, _ := userAPI681(t)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secret := kubeutil.MakeDNS1123Compatible(snt681User) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		secret: {ObjectMeta: authnMeta("krateo-system", secret, snt681User),
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	ctx, _ := withSinks681(ctxWithUser(snt681User, snt681Group))
	for _, c := range []struct {
		path string
		want bool
	}{
		{"/api/v1/namespaces/team-b/configmaps/catalog", true},
		{"/api/v1/namespaces/team-a/configmaps/ratelimited", false},
	} {
		_, failure := ResolveWithFailure(ctx, ResolveOptions{RC: &rest.Config{Host: srv.URL}, AuthnNS: "krateo-system",
			Items:               []*templates.API{stage1476("s", c.path, false)},
			RESTActionNamespace: "krateo-system", RESTActionName: "ra1476"})
		if failure == nil {
			t.Fatalf("%s: precondition: the stage must fail the resolve", c.path)
		}
		err := failure.Err("krateo-system", "ra1476")
		if got := IsPerIdentityDenied(err); got != c.want {
			t.Fatalf("%s: IsPerIdentityDenied=%v, want %v", c.path, got, c.want)
		}
		var se *apierrors.StatusError
		if !errors.As(err, &se) || se.Status().Code != http.StatusForbidden {
			t.Fatalf("%s: the failure must stay a typed 403, got %v", c.path, err)
		}
	}
}

// Modes of switchAPI1476's "sw" object.
const (
	sw1476Upstream500 = iota // a 500 Status whose message changes on every answer
	sw1476Reset              // the connection is reset: a transport fault
	sw1476BadBody            // a 200 whose body does not decode
	sw1476NotFound           // a 404 Status
	sw1476Conflict           // a 409 Status (not retried) whose message changes on every answer
)

// switchAPI1476 is the requester's own apiserver; "sw" answers per *mode.
func switchAPI1476(t *testing.T, mode *atomic.Int32) *httptest.Server {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch mode.Load() {
		case sw1476Upstream500:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500,"message":"boom %d"}`, n.Add(1))
		case sw1476Reset:
			if hj, ok := w.(http.Hijacker); ok {
				c, _, _ := hj.Hijack()
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				_ = c.Close()
			}
		case sw1476BadBody:
			_, _ = w.Write([]byte(`{not json`))
		case sw1476NotFound:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
		case sw1476Conflict:
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","code":409,"message":"conflict %d"}`, n.Add(1))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// failureLevels1476 is the level of each failure line in rec, in order.
func failureLevels1476(rec *rec711) []slog.Level {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []slog.Level
	for _, r := range rec.recs {
		if r.Message == "api call response failure" {
			out = append(out, r.Level)
		}
	}
	return out
}

// N4 — the class is the status line's code, never the message: an upstream
// answer whose message changes on every refresh is the same failure, for a
// code the retry client hands back as is (409) and one it turns into its own
// error (500).
func TestIssue1476_SameCodeDifferentMessage_IsTheSameFailure(t *testing.T) {
	for _, m := range []int32{sw1476Conflict, sw1476Upstream500} {
		t.Run(fmt.Sprintf("mode%d", m), func(t *testing.T) {
			setup1476E(t)
			var mode atomic.Int32
			mode.Store(m)
			srv := switchAPI1476(t, &mode)
			rec := &rec711{}
			for i := 0; i < 3; i++ {
				resolveAsUser1476(t, srv.URL, "cell-1476-m", rec, stage1476("sw", "/api/v1/namespaces/team-a/configmaps/sw", false))
			}
			got := failureLevels1476(rec)
			if len(got) != 3 || got[0] != slog.LevelError || got[1] != slog.LevelDebug || got[2] != slog.LevelDebug {
				t.Fatalf("an upstream answer with a changing message: want ERROR, DEBUG, DEBUG, got %v", got)
			}
		})
	}
}

// N1 — a transport fault and an undecodable body are not the upstream's 500,
// though the fetch shapes both into one: each change of cause logs again.
func TestIssue1476_TransportAndDecodeFaults_AreNotTheUpstreams500(t *testing.T) {
	setup1476E(t)
	var mode atomic.Int32
	srv := switchAPI1476(t, &mode)
	rec := &rec711{}
	st := stage1476("sw", "/api/v1/namespaces/team-a/configmaps/sw", false)
	for _, m := range []int32{sw1476Upstream500, sw1476Reset, sw1476Reset, sw1476BadBody, sw1476Upstream500} {
		mode.Store(m)
		resolveAsUser1476(t, srv.URL, "cell-1476-t", rec, st)
	}
	got := failureLevels1476(rec)
	want := []slog.Level{slog.LevelError, slog.LevelError, slog.LevelDebug, slog.LevelError, slog.LevelError}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("500, reset, reset, bad body, 500: want %v, got %v", want, got)
	}
}

// N4 — continueOnError demotes only the AUTHORIZED DEFINITE NotFound: a 404
// under continueOnError that the watcher does not confirm absent (here no
// watcher holds the GVR) keeps its ERROR first line.
func TestIssue1476_ContinueOnError404NotDefinite_KeepsError(t *testing.T) {
	setup1476E(t)
	var mode atomic.Int32
	mode.Store(sw1476NotFound)
	srv := switchAPI1476(t, &mode)
	rec := &rec711{}
	for i := 0; i < 2; i++ {
		resolveAsUser1476(t, srv.URL, "cell-1476-nf", rec, stage1476("sw", "/api/v1/namespaces/team-a/configmaps/sw", true))
	}
	got := failureLevels1476(rec)
	if len(got) != 2 || got[0] != slog.LevelError || got[1] != slog.LevelDebug {
		t.Fatalf("a 404 under continueOnError that is not a definite NotFound: want ERROR, DEBUG, got %v", got)
	}
}

// N4 — branch C: a 404 under continueOnError on an SA dial whose pre-dial
// authorization did NOT run (the GVR's scope cannot be resolved, so a
// cluster-scoped GET falls back) says nothing about the identity: it is not a
// definite NotFound and keeps its ERROR first line.
func TestIssue1476_BranchC404WithoutPreDialAuthorization_KeepsError(t *testing.T) {
	setup711(t, func(string) bool { return true })
	prevScope := preDialScopeFn
	preDialScopeFn = func(schema.GroupVersionResource) (bool, error) { return false, errors.New("scope unknown") }
	t.Cleanup(func() { preDialScopeFn = prevScope })
	rec := &rec711{}
	stage := stage1476("kt", "/apis/core.krateo.io/v1alpha1/kubernetestargets/ghost", true)
	for i := 0; i < 2; i++ {
		s := resolveStage711(t, rec, "cell-1476-c404", stage)
		if s.stage.Count() != 1 || s.forb.Stage() != 0 {
			t.Fatalf("resolve %d: stage-error %d, per-identity %d, want 1 and 0 (a plain 404, not a definite answer)",
				i, s.stage.Count(), s.forb.Stage())
		}
	}
	got := failureLevels1476(rec)
	if len(got) != 2 || got[0] != slog.LevelError || got[1] != slog.LevelDebug {
		t.Fatalf("an unauthorized SA dial's 404 under continueOnError: want ERROR, DEBUG, got %v", got)
	}
}

// The classes are closed and identity-free: the deny, a status code, or the
// uncoded label, never message text.
func TestIssue1476_FailureClassOf_IsClosed(t *testing.T) {
	gr := schema.GroupResource{Resource: "configmaps"}
	for _, c := range []struct {
		err  error
		want string
	}{
		{newRegateForbidden(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "ns", "x"), classPerIdentityDenied},
		{MarkPerIdentityDenied(apierrors.NewForbidden(gr, "x", errors.New("User \"someone\" cannot get"))), classPerIdentityDenied},
		{apierrors.NewForbidden(gr, "x", errors.New("User \"someone\" cannot get")), "403"},
		{apierrors.NewNotFound(gr, "x"), "404"},
		{errors.New("dial tcp: connection refused for someone"), classUncoded},
	} {
		if got := FailureClassOf(c.err); got != c.want {
			t.Fatalf("FailureClassOf(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
