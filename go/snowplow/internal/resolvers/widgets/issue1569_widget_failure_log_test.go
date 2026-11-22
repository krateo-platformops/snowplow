package widgets

// issue1569_widget_failure_log_test.go — #1569: every failure line a widget
// cell's resolve logs goes through the cell's failure-log round, not only the
// stage lines. A widgetDataTemplate that fails on every resolve of a resident
// cell logs at its level once, then DEBUG; a different failure on the same cell
// logs at its level again.

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

type rec1569 struct {
	mu   sync.Mutex
	msgs map[string][]slog.Level
}

func (r *rec1569) Enabled(context.Context, slog.Level) bool { return true }
func (r *rec1569) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.msgs == nil {
		r.msgs = map[string][]slog.Level{}
	}
	r.msgs[rec.Message] = append(r.msgs[rec.Message], rec.Level)
	return nil
}
func (r *rec1569) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *rec1569) WithGroup(string) slog.Handler      { return r }

func (r *rec1569) levels(msg string) []slog.Level {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]slog.Level(nil), r.msgs[msg]...)
}

// widget1569 is an apiRef-less widget whose widgetDataTemplate expression
// fails to evaluate (expr).
func widget1569(expr string) *Widget {
	w := baseWidget()
	w.Object["spec"].(map[string]any)["widgetDataTemplate"] = []any{
		map[string]any{"forPath": "label", "expression": expr},
	}
	return w
}

func TestIssue1569_WidgetTemplateFailure_OnceAtItsLevelThenDebug_ChangedLogsAgain(t *testing.T) {
	stubApiRef(t, nil, nil)
	stubValidate(t, nil)
	t.Cleanup(api.SetFailureLogResidentForTest(func(string) bool { return true }))
	rec := &rec1569{}
	resolve := func(expr string) {
		ctx := xcontext.BuildContext(hermeticCtx(t), xcontext.WithLogger(slog.New(rec)))
		ctx = cache.WithL1KeyContext(ctx, "widget-cell-1569")
		if _, err := Resolve(ctx, ResolveOptions{In: widget1569(expr)}); err == nil {
			t.Fatal("PRECONDITION: the widgetDataTemplate must fail to resolve")
		}
	}
	const msg = "unable to resolve widgetDataTemplate"
	for i := 0; i < 3; i++ {
		resolve("${ error(\"boom-1569\") }")
	}
	if got := rec.levels(msg); len(got) != 3 || got[0] != slog.LevelError || got[1] != slog.LevelDebug || got[2] != slog.LevelDebug {
		t.Fatalf("#1569 RED: %q on three resolves of one resident cell logged at %v, want [ERROR DEBUG DEBUG]", msg, got)
	}
	// The cell recovers (the template evaluates), then fails again: the
	// recurrence is new, at its level.
	ctx := xcontext.BuildContext(hermeticCtx(t), xcontext.WithLogger(slog.New(rec)))
	ctx = cache.WithL1KeyContext(ctx, "widget-cell-1569")
	if _, err := Resolve(ctx, ResolveOptions{In: widget1569("${ \"ok\" }")}); err != nil {
		t.Fatalf("SETUP: the recovered template must resolve: %v", err)
	}
	resolve("${ error(\"boom-1569\") }")
	if got := rec.levels(msg); len(got) != 4 || got[3] != slog.LevelError {
		t.Fatalf("#1569: the failure recurring after a recovery logged at %v, want ERROR last", got)
	}
}
