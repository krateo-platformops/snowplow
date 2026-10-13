package dispatchers

// issue643_prb_f7_f8_fmix_test.go — #643 PR-B arms F7, F8 and F-mix, on the
// shared #643 harness.
//
//   F7  keepwarm (I-4): a live internal cell at or past TTL/4 is re-Put, a
//       younger one is age-skipped, an external one is not dialled after boot
//       marked it.
//   F8  request path untouched (I-6): a customer /call through the REAL widgets
//       ServeHTTP dials the external endpoint every time, mark or no mark.
//   F-mix (brief amendment): a stage error on an INTERNAL stage plus an external
//       stage → marked (the resolve touched external); an internal stage error
//       with no external stage → never marked, and retried by the next pass.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestIssue643_F7_Keepwarm_AgesInternalCells_SkipsMarkedExternal(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	h643Budget(t, nil)
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != h643K*h643M*2 {
		t.Fatalf("precondition: boot dialled %d", d)
	}
	store := cache.ResolvedCache()
	old := h643WidgetKeysFor("int-w0")[0]
	young := h643WidgetKeysFor("int-w1")[0]
	oldEntry, _ := store.GetNoTouch(old)
	oldEntry.CreatedAt = time.Now().Add(-keepwarmAgeSkipThreshold() - time.Second)
	oldStamp := oldEntry.CreatedAt
	youngEntry, _ := store.GetNoTouch(young)
	youngStamp := youngEntry.CreatedAt

	if d := env.pass(prewarmScope{kind: scopeKindKeepwarm}); d != 0 {
		t.Fatalf("F7: keepwarm dialled the external endpoint %d times after boot marked every external key, want 0", d)
	}
	if e, ok := store.GetNoTouch(old); !ok || !e.CreatedAt.After(oldStamp) {
		t.Fatal("F7 (I-4): a live internal cell older than TTL/4 must be re-resolved and re-Put by keepwarm")
	}
	if e, ok := store.GetNoTouch(young); !ok || !e.CreatedAt.Equal(youngStamp) {
		t.Fatal("F7 (I-4): a young internal cell must be age-skipped, not re-Put")
	}
}

func TestIssue643_F8_CallPathDialsEveryTime_MarkOrNot(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	keys := h643BootAndMark(t, env, "ext-w0")
	if !cache.EngineDeclinedExternalSet().Marked(keys[0]) {
		t.Fatal("precondition: ext-w0 must be marked")
	}

	w, ok := env.rw.GetObject(h643WidgetGVR, h643NS, "ext-w0")
	if !ok {
		t.Fatal("precondition: ext-w0 not in the informer")
	}
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h643WidgetGVR, Unstructured: w.DeepCopy()}
	})
	t.Cleanup(r1)
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	t.Cleanup(r2)
	// The request path has no internal rest config: restactions.Resolve takes
	// the in-cluster one, which a hermetic test must supply.
	t.Cleanup(restactionsapi.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: env.ep.srv.URL}, nil
	}))

	reqCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: env.ep.srv.URL}),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "h643-user", Groups: []string{"h643-g0"}}))
	for i := 0; i < 3; i++ {
		before := env.ep.total()
		rec := httptest.NewRecorder()
		Widgets().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
		if rec.Code != http.StatusOK {
			t.Fatalf("F8 call %d: status %d body=%s", i, rec.Code, rec.Body.String())
		}
		if d := env.ep.total() - before; d != 2 {
			t.Fatalf("F8 (I-6): /call %d dialled %d times, want 2 (both external stages, fresh, every call) — a marked key must never suppress a request", i, d)
		}
	}
}

func TestIssue643_FMix_InternalErrorPlusExternalMarks_InternalErrorAloneRetries(t *testing.T) {
	// An internal stage that fails: a GET of an h643things object that does not exist.
	failing := map[string]any{"name": "s0", "verb": "GET",
		"path": "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/namespaces/" + h643NS + "/" + h643ThingsGVR.Resource + "/missing"}
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: 1, things: 1, extra: []h643Extra{
		{widget: "mix-w", ra: "mix-ra", external: true, api: []any{failing, h643ExtStage("s1", "/ext/fast/mix/s1")}},
		{widget: "ierr-w", ra: "ierr-ra", external: false, api: []any{failing}},
	}})
	h643Budget(t, nil)
	env.pass(prewarmScope{kind: scopeKindBoot})
	if got := len(h643MarksFor("mix-w")); got != h643K {
		t.Fatalf("F-mix: an internal stage error alongside an external touch must mark the key (%d of %d marked) — the resolve touched external", got, h643K)
	}
	if got := len(h643MarksFor("ierr-w")); got != 0 {
		t.Fatalf("F-mix: an internal stage error with NO external touch marked %d keys — it is transient and must be retried", got)
	}

	skip0 := h643SkipSnap()
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR}); d != 0 {
		t.Fatalf("F-mix: the marked mix-w was re-dialled (%d) by the next background pass", d)
	}
	if got := h643SkipDelta(skip0)[string(scopeKindGVRDiscovered)]; got != uint64(h643K) {
		t.Fatalf("F-mix: the next pass skipped %d units, want exactly %d (mix-w only — ierr-w must be retried, not skipped)", got, h643K)
	}
	if got := len(h643MarksFor("ierr-w")); got != 0 {
		t.Fatalf("F-mix: the retried ierr-w got marked (%d)", got)
	}
}
