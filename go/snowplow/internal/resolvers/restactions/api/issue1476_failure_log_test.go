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
	"log/slog"
	"net/http"
	"sync"
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
		if got := FailureLogLevel(ctx, slog.LevelError, "t", s.class); got != s.want {
			t.Fatalf("step %d (class %s): level %v, want %v", i, s.class, got, s.want)
		}
	}
	// No L1 key: no cell to deduplicate on, the line keeps its level.
	for i := 0; i < 2; i++ {
		if got := FailureLogLevel(context.Background(), slog.LevelError, "t", "404"); got != slog.LevelError {
			t.Fatalf("no cell: level %v, want ERROR every time", got)
		}
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
