//go:build integration

// issue905_stale_error_rerun_test.go — #905 review B3, from the review's arm.
//
// The usual write RESTAction: its write stage keeps the default
// continueOnError=false and its filter needs the write's output
// (`.create | select(. != null)`). On a stale-derived first pass the write is
// held (rule a), the filter yields no value, and restactions.Resolve returns
// the held stage's failure (#709) — an ERROR, which used to be answered
// directly (500) before the write point, so the write was never sent at all.
//
//	W3  one customer request sends its POST exactly once and answers 200: the
//	    error of the stale-derived pass is withdrawn and the request re-run
//	    fresh (rule c).
//	    Mutation → RED: no write point on the error path (0 POSTs, 500).
//	W4  the widgets twin: a widget whose stale-derived pass ERRORS (its apiRef
//	    RESTAction's held write failed it) is withdrawn and re-run fresh, and
//	    answers the fresh body with 200, never the stale pass's error.
//	    Mutation → RED: no write point on widgets.go's error path (an error
//	    status).
package dispatchers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// w905StrictSeam reproduces restactions.Resolve's #709 filter rule verbatim:
// when the filter yields no value and a stage without continueOnError failed,
// that failure's StatusError is the answer.
func (h *k547Harness) w905StrictSeam(up string) func(context.Context, restactions.ResolveOptions) (*templates.RESTAction, error) {
	return func(ctx context.Context, opts restactions.ResolveOptions) (*templates.RESTAction, error) {
		rw := cache.Global()
		rctx := cache.WithServeWatcher(ctx, rw)
		rctx = cache.WithInternalEndpoint(rctx, &endpoints.Endpoint{ServerURL: up})
		found := &templates.API{Name: "found", Verb: ptr.To("GET"),
			Path: "/apis/" + k547CompGVR.Group + "/" + k547CompGVR.Version + "/" + k547CompGVR.Resource}
		create := &templates.API{Name: "create", Verb: ptr.To("POST"), Path: "/create",
			Payload: ptr.To(`{"n":1}`), DependsOn: &templates.Dependency{Name: "found"}}
		dict, failure := restactionsapi.ResolveWithFailure(rctx, restactionsapi.ResolveOptions{
			RC: &rest.Config{Host: up}, Watcher: rw, Items: []*templates.API{found, create},
			RESTActionNamespace: k547WNS, RESTActionName: k547RAName,
		})
		if dict["create"] == nil && failure != nil { // filter `.create | select(. != null)`: no value
			return opts.In, failure.StatusError(opts.In.GetNamespace(), opts.In.GetName())
		}
		out := opts.In.DeepCopy()
		out.Status = &runtime.RawExtension{Raw: []byte(`{"conditions":[{"reason":"v1"}]}`)}
		return out, nil
	}
}

func TestIssue905_W3_StrictWriteRA_StalePassError_ReRun_SentOnce200(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	h, _ := k710KnownStaleFound(t)
	up, posts := w905Upstream(t)
	t.Cleanup(setRestactionsResolveForTest(h.w905StrictSeam(up.URL)))
	s0 := restactionsapi.StaleServeStatsSnapshot()

	_, code := h.getRA(0, k547Changed)

	st := restactionsapi.StaleServeStatsSnapshot()
	if d := st.ServeTotal - s0.ServeTotal; d != 1 {
		t.Fatalf("PRE: the first pass must be served stale (stale decisions %d) — else this arm is vacuous", d)
	}
	if n := posts.Load(); n != 1 || code != http.StatusOK {
		t.Fatalf("#905 RED: one customer write request sent its POST %d time(s) and answered %d; want exactly 1 POST and 200 "+
			"(stale decisions %d, withdrawn %d)", n, code, st.ServeTotal-s0.ServeTotal, st.WithdrawnUnarmable-s0.WithdrawnUnarmable)
	}
}

func TestIssue905_W4_Widget_StalePassError_ReRun_Fresh200(t *testing.T) {
	h, _ := k710KnownStaleFound(t)
	inner := h.resolve
	// The widget's apiRef RESTAction holds its write on a pass served stale
	// (rule a) and fails without it (#709): the widget resolve errors on THAT
	// pass only.
	t.Cleanup(setWidgetsResolveForTest(func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		w, err := inner(ctx, opts)
		if err == nil && cache.StaleServeSinkFromContext(ctx).StaleDerived() {
			return nil, apierrors.NewInternalError(errors.New("apiRef failed: its write was held on a stale pass (injected)"))
		}
		return w, err
	}))
	s0 := restactionsapi.StaleServeStatsSnapshot()

	r, code := h.get(0, k547Changed)

	st := restactionsapi.StaleServeStatsSnapshot()
	if d := st.ServeTotal - s0.ServeTotal; d != 1 {
		t.Fatalf("PRE: the first pass must be served stale (stale decisions %d) — else this arm is vacuous", d)
	}
	if code != http.StatusOK || r != "v1" {
		t.Fatalf("#905 RED: the widget answered %d %q; want 200 and the fresh re-run's v1 — the stale pass's error must be "+
			"withdrawn and re-run (withdrawn %d)", code, r, st.WithdrawnUnarmable-s0.WithdrawnUnarmable)
	}
}
