package widgets

// issue1319_seal_outcomes_test.go — #1319: a widget's stage outcomes are its
// apiRef RESTAction's. Once that resolve is over, a resolve that runs later on
// the same ctx (an inline child's, after Resolve returns to the dispatcher)
// must not report into the widget's collector.
//
//   S1 a widget whose apiRef claims nothing (no apiRef) reads complete, even if
//      a later resolve on the ctx tries to report;
//   S2 the apiRef resolve's outcomes survive a later resolve's report.
//
// The "later resolve" is the validate seam, which runs after the apiRef step:
// it reports through api.ReplayStageOutcomes, the claim any resolve makes.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/client-go/rest"
)

const (
	s1319ApiRef = `[{"name":"sa","ok":false,"reason":"Forbidden"}]`
	s1319Later  = `[{"name":"child","ok":false,"reason":"Error"}]`
)

func s1319LaterResolve(t *testing.T) {
	t.Helper()
	orig := validateObjectStatusFn
	validateObjectStatusFn = func(ctx context.Context, _ *rest.Config, _ map[string]any) error {
		api.ReplayStageOutcomes(ctx, s1319Later)
		return nil
	}
	t.Cleanup(func() { validateObjectStatusFn = orig })
}

func TestIssue1319_S1_NoApiRefReadsCompleteWhateverRunsLater(t *testing.T) {
	stubApiRef(t, nil, nil) // claims nothing: a widget without an apiRef
	s1319LaterResolve(t)
	ctx, outcomes := api.WithStageOutcomes(hermeticCtx(t))
	if _, err := Resolve(ctx, ResolveOptions{In: baseWidget()}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := api.StorableStageOutcomes(outcomes); got != "" {
		t.Fatalf("S1 RED: a widget with no apiRef stage reads %q (a later resolve reported into it), want complete", got)
	}
}

func TestIssue1319_S2_ApiRefOutcomesSurviveALaterResolve(t *testing.T) {
	orig := resolveApiRefFn
	resolveApiRefFn = func(ctx context.Context, _ ResolveOptions) (map[string]any, error) {
		api.ReplayStageOutcomes(ctx, s1319ApiRef)
		return map[string]any{}, nil
	}
	t.Cleanup(func() { resolveApiRefFn = orig })
	s1319LaterResolve(t)
	ctx, outcomes := api.WithStageOutcomes(hermeticCtx(t))
	if _, err := Resolve(ctx, ResolveOptions{In: baseWidget()}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := api.StorableStageOutcomes(outcomes); got != s1319ApiRef {
		t.Fatalf("S2 RED: the widget's outcomes are %q, want its apiRef RESTAction's %q", got, s1319ApiRef)
	}
}
