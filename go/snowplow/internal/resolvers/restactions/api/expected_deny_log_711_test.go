package api

// expected_deny_log_711_test.go — #711: N refreshes of one denial-only cell log
// the expected per-identity deny ONCE (WARN), and the pre-dial counter counts
// all N. Driven through the REAL api.Resolve on the #677 branch C harness: the
// identity u677 may not get configmaps in team-b, so the pre-dial check denies
// before any dial.

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

type rec711 struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *rec711) Enabled(context.Context, slog.Level) bool { return true }
func (r *rec711) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec.Clone())
	r.mu.Unlock()
	return nil
}
func (r *rec711) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *rec711) WithGroup(string) slog.Handler      { return r }

// failures counts "api call response failure" records by level, and returns the
// RESTAction name each carried.
func (r *rec711) failures() (map[slog.Level]int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	by := map[slog.Level]int{}
	var ras []string
	for _, rec := range r.recs {
		if rec.Message != "api call response failure" {
			continue
		}
		by[rec.Level]++
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "restaction_name" {
				ras = append(ras, a.Value.String())
			}
			return true
		})
	}
	return by, ras
}

func deniedCount711() uint64 {
	for _, c := range PreDialSnapshot() {
		if c.Outcome == preDialDenied {
			return c.Count
		}
	}
	return 0
}

// resolve711 runs the catalog RESTAction once under the cell key, as a refresh
// of that cell does.
func resolve711(t *testing.T, rec *rec711, l1Key string) {
	t.Helper()
	resolvePath711(t, rec, l1Key, "/api/v1/namespaces/team-b/configmaps/catalog", true)
}

func resolvePath711(t *testing.T, rec *rec711, l1Key, path string, wantDenialOnly bool) {
	t.Helper()
	s := resolveStage711(t, rec, l1Key, stage681("catalog", path))
	if wantDenialOnly && !cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatal("precondition: the resolve must be denial-only (the #685 cell shape)")
	}
}

// resolveStage711 runs one stage under the cell key on the branch C harness
// ("ghost" is missing) and returns the resolve's sinks.
func resolveStage711(t *testing.T, rec *rec711, l1Key string, stage *templates.API) sinks681 {
	t.Helper()
	f, rc := newFakeAPI677(t)
	f.missing["ghost"] = true
	f.missing["ghost2"] = true
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx = cache.WithL1KeyContext(ctx, l1Key)
	ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(slog.New(rec)))
	ctx, s := withSinks681(ctx)
	Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: []*templates.API{stage},
		RESTActionNamespace: "krateo-system", RESTActionName: "global-search",
	})
	return s
}

func setup711(t *testing.T, resident func(string) bool) {
	t.Helper()
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	resetFailureNotedForTest()
	t.Cleanup(resetFailureNotedForTest)
	prev := failureResidentFn
	failureResidentFn = func(k string) (bool, bool) { return resident(k), true }
	t.Cleanup(func() { failureResidentFn = prev })
}

func TestIssue711_NRefreshesOfOneDenialOnlyCell_OneLine_NCounts(t *testing.T) {
	const n = 7
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	before := deniedCount711()
	for i := 0; i < n; i++ {
		resolve711(t, rec, "cell-711-a")
	}
	if got := deniedCount711() - before; got != n {
		t.Fatalf("predial_total{denied} moved by %d, want %d (one per occurrence)", got, n)
	}
	by, ras := rec.failures()
	if by[slog.LevelError] != 0 {
		t.Fatalf("an expected per-identity deny logged at ERROR %d times", by[slog.LevelError])
	}
	if by[slog.LevelWarn] != 1 || by[slog.LevelDebug] != n-1 {
		t.Fatalf("want exactly 1 WARN and %d DEBUG failure lines, got %v", n-1, by)
	}
	if len(ras) != n {
		t.Fatalf("every failure line must carry restaction_name: %d of %d did", len(ras), n)
	}
	for _, ra := range ras {
		if ra != "global-search" {
			t.Fatalf("the failure line must carry the RESTAction name, got %q", ra)
		}
	}
}

// A new generation of the cell (it left the store and came back) and a second
// cell each log their first deny again.
func TestIssue711_NewGenerationAndOtherCell_LogAgain(t *testing.T) {
	resident := map[string]bool{"cell-711-a": true, "cell-711-b": true}
	var mu sync.Mutex
	setup711(t, func(k string) bool { mu.Lock(); defer mu.Unlock(); return resident[k] })
	rec := &rec711{}

	resolve711(t, rec, "cell-711-a")
	resolve711(t, rec, "cell-711-a")
	resolve711(t, rec, "cell-711-b")
	if by, _ := rec.failures(); by[slog.LevelWarn] != 2 {
		t.Fatalf("two cells: want 2 WARN, got %v", by)
	}

	// Evicted: the resolve that runs while no cell holds the key starts a new
	// generation (WARN); the refreshes of the re-Put cell are DEBUG.
	mu.Lock()
	resident["cell-711-a"] = false
	mu.Unlock()
	resolve711(t, rec, "cell-711-a")
	mu.Lock()
	resident["cell-711-a"] = true
	mu.Unlock()
	resolve711(t, rec, "cell-711-a")
	if by, _ := rec.failures(); by[slog.LevelWarn] != 3 || by[slog.LevelError] != 0 {
		t.Fatalf("new generation: want 3 WARN / 0 ERROR, got %v", by)
	}
}

// A dry-run (#443 inert) resolve of a denial-only cell logs the deny at DEBUG,
// does not spend the cell's WARN and does not reset its generation: the next
// real refresh still WARNs once, and the one after is DEBUG.
func TestIssue711_InertResolve_DoesNotConsumeTheWarn(t *testing.T) {
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	before := deniedCount711()
	inert := func() {
		t.Helper()
		f, rc := newFakeAPI677(t)
		_ = f
		ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
		ctx = cache.WithL1KeyContext(ctx, "cell-711-a")
		ctx = cache.WithInert(ctx)
		ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(slog.New(rec)))
		Resolve(context.WithoutCancel(ctx), ResolveOptions{
			RC: rc, Items: []*templates.API{stage681("catalog", "/api/v1/namespaces/team-b/configmaps/catalog")},
			RESTActionNamespace: "krateo-system", RESTActionName: "global-search",
		})
	}
	inert()
	inert()
	if by, _ := rec.failures(); by[slog.LevelWarn] != 0 || by[slog.LevelError] != 0 || by[slog.LevelDebug] != 2 {
		t.Fatalf("inert resolves: want 2 DEBUG / 0 WARN / 0 ERROR, got %v", by)
	}
	if _, noted := failureNoted.Load("cell-711-a"); noted {
		t.Fatal("an inert resolve wrote the dedupe state")
	}
	resolve711(t, rec, "cell-711-a")
	resolve711(t, rec, "cell-711-a")
	if by, _ := rec.failures(); by[slog.LevelWarn] != 1 || by[slog.LevelDebug] != 3 || by[slog.LevelError] != 0 {
		t.Fatalf("after inert resolves the real refreshes must WARN once: want 1 WARN / 3 DEBUG, got %v", by)
	}
	if got := deniedCount711() - before; got != 4 {
		t.Fatalf("predial_total{denied} moved by %d, want 4 (inert denies are counted too)", got)
	}
}

// A failure that is NOT the per-identity re-gate class is never demoted to the
// deny's WARN, on the same cell and the same branch — the distinction is the
// error's type, not the stage or the RESTAction. u677 may get any configmap in
// team-a, so the pre-dial check allows and the dial answers 404. The stage
// does not declare continueOnError, so the 404 is not a declared outcome
// (#1476): its first line is ERROR, and its unchanged repeats in the cell's
// generation are DEBUG (#1476 — before it, ERROR every time).
func TestIssue711_OtherFailures_StayError(t *testing.T) {
	setup711(t, func(string) bool { return true })
	rec := &rec711{}
	before := deniedCount711()
	stage := &templates.API{Name: "catalog", Path: "/api/v1/namespaces/team-a/configmaps/ghost",
		Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To("catalogErr")}
	for i := 0; i < 3; i++ {
		resolveStage711(t, rec, "cell-711-a", stage)
	}
	if by, _ := rec.failures(); by[slog.LevelError] != 1 || by[slog.LevelWarn] != 0 || by[slog.LevelDebug] != 2 {
		t.Fatalf("a 404 must log at ERROR once, then DEBUG, and never WARN, got %v", by)
	}
	if got := deniedCount711() - before; got != 0 {
		t.Fatalf("a 404 is not a pre-dial deny, predial denied moved by %d", got)
	}
}
