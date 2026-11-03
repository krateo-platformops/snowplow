package dispatchers

// #1116 — a /call whose extras name a key the RESTAction resolver writes (a
// stage name, an errorKey, slice) is answered 400 with details.causes naming
// every such key, on GET /call and on POST /call/read's body channel, cache on
// and off, for a RESTAction and for a widget's apiRef; the refresher's
// re-resolve from stored extras refuses the same way. The resolver is the
// REAL restactions.Resolve: the refusal is decided before anything runs, so
// no apiserver or endpoint is needed (and none is dialled: the SA rest.Config
// points at a recording server).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// rx16RA is a two-stage chain: stage1 (errorKey stage1Err), stage2 with no
// errorKey (so it writes "error"), its path built from stage1.
func rx16RA() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": h1RAName, "namespace": h1NS},
		"spec": map[string]any{"api": []any{
			map[string]any{"name": "stage1", "path": "/api/v1/namespaces/team-a/configmaps", "errorKey": "stage1Err"},
			map[string]any{"name": "stage2", "path": `${ "/x/" + (.stage1.items[0].id | tostring) }`,
				"dependsOn":   map[string]any{"name": "stage1"},
				"endpointRef": map[string]any{"name": "held-endpoint", "namespace": "krateo-system"}},
		}},
	}}
}

// rx16Dialer is the SA rest.Config's host: any request is a dial.
func rx16Dialer(t *testing.T) (*rest.Config, *atomic.Int64) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}, &n
}

// rx16RealResolve is restactions.Resolve with the SA rest.Config pointed at
// the recording server, counting how often it is entered.
func rx16RealResolve(rc *rest.Config, entered *atomic.Int64) func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
	return func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		entered.Add(1)
		opts.SArc = rc
		return restactions.Resolve(ctx, opts)
	}
}

type rx16Body struct {
	Kind, APIVersion, Status, Message, Reason string
	Code                                      int
	Details                                   struct {
		Causes []struct{ Reason, Field, Message string } `json:"causes"`
	} `json:"details"`
}

// rx16Assert400 checks the #1116 contract: a 400 whose Status prefix is
// plumbing's response.New(400, err) byte for byte, and one cause per key.
func rx16Assert400(t *testing.T, label string, rec *httptest.ResponseRecorder, wantFields ...string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: code %d, want 400; body=%s", label, rec.Code, rec.Body.String())
	}
	t.Logf("%s: %s %s=%s", label, strings.TrimSpace(rec.Body.String()), util.HeaderStageOutcomes, rec.Header().Get(util.HeaderStageOutcomes))
	var b rx16Body
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("%s: body is not JSON: %v (%s)", label, err, rec.Body.String())
	}
	if b.Kind != "Status" || b.APIVersion != "v1" || b.Status != response.StatusFailure ||
		b.Reason != string(response.StatusReasonBadRequest) || b.Code != 400 {
		t.Errorf("%s: Status prefix differs from plumbing's 400: %+v", label, b)
	}
	plumbing, _ := json.Marshal(response.New(http.StatusBadRequest, fmt.Errorf("%s", b.Message)))
	if prefix := strings.TrimSuffix(string(plumbing), "}"); !strings.HasPrefix(rec.Body.String(), prefix) {
		t.Errorf("%s: the body does not start with plumbing's Status encoding\n got  %s\n want %s…", label, rec.Body.String(), prefix)
	}
	var got []string
	for _, c := range b.Details.Causes {
		if c.Reason != CauseReservedExtrasKey || c.Message == "" {
			t.Errorf("%s: cause %+v: want reason %s and a message", label, c, CauseReservedExtrasKey)
		}
		got = append(got, c.Field)
	}
	if strings.Join(got, ",") != strings.Join(wantFields, ",") {
		t.Errorf("%s: cause fields %v, want %v", label, got, wantFields)
	}
	if h := rec.Header().Get(util.HeaderStageOutcomes); !strings.Contains(h, `"reason":"Aborted"`) {
		t.Errorf("%s: %s = %q, want the resolve-level Aborted row", label, util.HeaderStageOutcomes, h)
	}
}

// rx16DocEqualsWire keeps the frontend integration doc's example (section 3)
// byte-equal to the wire: its body line and its X-Snowplow-Stage-Outcomes line.
func rx16DocEqualsWire(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	b, err := os.ReadFile("../../../docs/howto-frontend-reserved-extras-keys.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := strings.Split(string(b), "```http\n")
	if len(blocks) != 2 {
		t.Fatalf("the doc must carry exactly one ```http example, found %d", len(blocks)-1)
	}
	blk := blocks[1][:strings.Index(blocks[1], "```")]
	var body, outcomes string
	for _, ln := range strings.Split(blk, "\n") {
		switch {
		case strings.HasPrefix(ln, "{"):
			body = ln
		case strings.HasPrefix(ln, util.HeaderStageOutcomes+": "):
			outcomes = strings.TrimPrefix(ln, util.HeaderStageOutcomes+": ")
		}
	}
	if got := strings.TrimSuffix(rec.Body.String(), "\n"); got != body {
		t.Errorf("doc body differs from the wire:\n doc  %s\n wire %s", body, got)
	}
	if got := rec.Header().Get(util.HeaderStageOutcomes); got != outcomes {
		t.Errorf("doc %s differs from the wire:\n doc  %s\n wire %s", util.HeaderStageOutcomes, outcomes, got)
	}
}

func TestReservedExtras1116_CallAnswers400(t *testing.T) {
	for _, mode := range []string{"cache-on", "cache-off"} {
		t.Run(mode, func(t *testing.T) {
			h1BuildWatcher(t)
			if mode == "cache-off" {
				t.Setenv("CACHE_ENABLED", "false")
			}
			rc, dials := rx16Dialer(t)
			var entered atomic.Int64
			defer installRAFakes(t, rx16RA(), func() bool { return true }, rx16RealResolve(rc, &entered))()

			extras := map[string]any{"namespace": "team-b", "stage1": map[string]any{"items": []any{map[string]any{"id": 2002}}}, "error": "x"}
			ej, _ := json.Marshal(extras)

			// GET /call?extras=
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/call?extras="+url.QueryEscape(string(ej)), nil).WithContext(h1ReqCtx(h1User))
			RESTAction().ServeHTTP(rec, req)
			rx16Assert400(t, mode+" GET /call", rec, "extras.error", "extras.stage1")
			rx16DocEqualsWire(t, rec)

			// POST /call/read: the body-decode middleware stashes the decoded
			// extras on the context (util.WithExtras); ParseExtras reads it first.
			rec = httptest.NewRecorder()
			req = httptest.NewRequest("POST", "/call/read", nil).WithContext(util.WithExtras(h1ReqCtx(h1User), extras))
			RESTAction().ServeHTTP(rec, req)
			rx16Assert400(t, mode+" POST /call/read", rec, "extras.error", "extras.stage1")

			// Each refusal is one resolve that dialled nothing and stored nothing.
			if entered.Load() != 2 || dials.Load() != 0 {
				t.Errorf("%s: resolves=%d dials=%d, want 2 resolves and 0 dials", mode, entered.Load(), dials.Load())
			}
			if mode == "cache-on" && cache.ResolvedCache() != nil && cache.ResolvedCache().Len() != 0 {
				t.Errorf("a refused /call stored a cell (%d entries)", cache.ResolvedCache().Len())
			}
		})
	}
}

// The widget's apiRef: widgets.Resolve wraps the apiRef error in a
// cache.ResolveStageError; the refusal must still be found and answered 400.
func TestReservedExtras1116_WidgetAnswers400(t *testing.T) {
	h1BuildWatcher(t)
	refusal := api.CheckReservedExtras([]*templatesv1.API{{Name: "crds"}}, map[string]any{"crds": []any{}})
	if refusal == nil {
		t.Fatal("setup: no refusal")
	}
	defer installWidgetFakes(t, h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": "ra", "namespace": h1NS}}),
		func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			return nil, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef Panel "+h1NS+"/"+h1WName,
				fmt.Errorf("apiref resolve: %w", refusal))
		})()
	rec := httptest.NewRecorder()
	ej := url.QueryEscape(`{"crds":[]}`)
	req := httptest.NewRequest("GET", "/call?extras="+ej, nil).WithContext(h1ReqCtx(h1User))
	Widgets().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"field":"extras.crds"`) {
		t.Fatalf("widget: code %d body %s, want 400 naming extras.crds", rec.Code, rec.Body.String())
	}
}

// The refresher re-resolves a cell from the extras stored with it. A stored
// reserved key (unreachable after #1116: a refused /call stores nothing) is
// refused the same way, with 0 dials, for a RESTAction cell and a RAFullList
// cell.
func TestReservedExtras1116_RefresherRefusesStoredReservedExtras(t *testing.T) {
	h1BuildWatcher(t)
	rc, dials := rx16Dialer(t)
	ctx := cache.WithInternalRESTConfig(h1ReqCtx(h1User), rc)
	got := objects.Result{GVR: h1RAGVR, Unstructured: rx16RA()}
	inputs := cache.ResolvedKeyInputs{Group: h1RAGVR.Group, Version: h1RAGVR.Version, Resource: h1RAGVR.Resource,
		Namespace: h1NS, Name: h1RAName, Extras: map[string]any{"stage1Err": "forged"}}
	for name, fn := range map[string]func(context.Context, objects.Result, cache.ResolvedKeyInputs, string) ([]byte, error){
		"restactions": resolveRestActionForRefresh,
		"raFullList":  resolveRAFullListForRefresh,
	} {
		_, err := fn(ctx, got, inputs, "")
		if _, ok := api.AsReservedExtrasError(err); !ok {
			t.Errorf("%s refresh: want the #1116 refusal, got %v", name, err)
		}
	}
	if dials.Load() != 0 {
		t.Errorf("refresher refusals dialled %d time(s)", dials.Load())
	}
}
