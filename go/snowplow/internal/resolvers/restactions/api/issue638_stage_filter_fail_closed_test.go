//go:build unit
// +build unit

package api

// issue638_stage_filter_fail_closed_test.go — #638 RED-first arms.
//
// THE DEFECT (origin/main 1205faad, handler.go:163-167): a stage `filter` that
// fails to compile or evaluate was only LOGGED. jsonHandlerCore kept tmp = the
// RAW, unfiltered response and merged it into dict[stage]; no stage-error sink
// was bumped, so every L1 Put accepted the raw answer as if it were the
// projection. The same "logged, Put not declined" shape sat on the iterator
// (setup.go: a non-benign `dependsOn.iterator` error ran a partial/empty plan).
//
// WHAT THE ARMS PIN, per identity, over K=2 users x M=2 cohorts (distinct group
// sets), stored AND caller-supplied provenance, three filter-failure shapes
// (parse error, runtime error, unknown-module import):
//   - the RAW body never reaches the output (dict[stage] absent, the sentinel
//     appears nowhere in the resolved dict);
//   - the stage-error sink is bumped — the exact signal every L1 Put gate reads
//     to decline (restactions.go/widgets.go dispatchers, the refresher; the
//     decline branch itself is pinned by TestIssue548_RESTActions_NoDeclineBranchArms);
//   - dict[errorKey] carries the failure, and the caller-supplied stage outcome
//     is Error (X-Snowplow-Stage-Outcomes);
//   - a downstream stage still runs (#313 C-A: no truncation).
// A valid-filter CONTROL proves the arm is not vacuous (sentinel present, sink 0).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

const a638Sentinel = "RAW-UNFILTERED-SENTINEL-638"

// a638Server answers /raw with a body carrying the sentinel (what an
// unfiltered pass-through would leak) and /down with a small object. It counts
// /items/* calls so the iterator arm can prove nothing was dispatched.
type a638Server struct {
	srv        *httptest.Server
	itemsCalls atomic.Int64
}

func newA638Server(t *testing.T) *a638Server {
	t.Helper()
	s := &a638Server{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/raw":
			fmt.Fprintf(w, `{"secret":%q,"items":"not-an-array"}`, a638Sentinel)
		case r.URL.Path == "/down":
			fmt.Fprint(w, `{"ok":true}`)
		case strings.HasPrefix(r.URL.Path, "/items/"):
			s.itemsCalls.Add(1)
			fmt.Fprintf(w, `{"secret":%q}`, a638Sentinel)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","code":404}`)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// a638Identity is one requester: K users per cohort, M cohorts (group sets).
type a638Identity struct {
	user   string
	groups []string
}

func a638Identities() []a638Identity {
	var out []a638Identity
	for m := 0; m < 2; m++ { // M=2 cohorts
		for k := 0; k < 2; k++ { // K=2 users per cohort
			out = append(out, a638Identity{
				user:   fmt.Sprintf("u638-c%d-k%d", m, k),
				groups: []string{fmt.Sprintf("cohort-638-%d", m), "devs"},
			})
		}
	}
	return out
}

type a638Result struct {
	dict     map[string]any
	sink     *cache.StageErrorSink
	outcomes *StageOutcomes
}

func a638Resolve(t *testing.T, s *a638Server, id a638Identity, prov Provenance, stages []*templates.API, extras map[string]any) a638Result {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: id.user, Groups: id.groups}),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: s.srv.URL})
	ctx, sink := cache.WithStageErrorSink(ctx)
	ctx, outcomes := WithStageOutcomes(ctx)
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               stages,
		Extras:              extras,
		RESTActionNamespace: "ns-638",
		RESTActionName:      "ra-638",
		Provenance:          prov,
	})
	return a638Result{dict: dict, sink: sink, outcomes: outcomes}
}

func a638FilterStages(filter string) []*templates.API {
	return []*templates.API{
		{
			Name:   "raw",
			Path:   "/raw",
			Verb:   ptr.To(http.MethodGet),
			Filter: ptr.To(filter),
		},
		{
			Name:      "after",
			Path:      "/down",
			Verb:      ptr.To(http.MethodGet),
			DependsOn: &templates.Dependency{Name: "raw"},
		},
	}
}

func a638DictJSON(t *testing.T, dict map[string]any) string {
	t.Helper()
	b, err := json.Marshal(dict)
	if err != nil {
		t.Fatalf("marshal dict: %v", err)
	}
	return string(b)
}

// a638AssertStageFailedClosed is the shared post-condition for a failed stage.
func a638AssertStageFailedClosed(t *testing.T, res a638Result, stage string, prov Provenance) {
	t.Helper()
	if v, ok := res.dict[stage]; ok {
		t.Errorf("dict[%q] present after a failed jq expression — the RAW response passed through: %v", stage, v)
	}
	if body := a638DictJSON(t, res.dict); strings.Contains(body, a638Sentinel) {
		t.Errorf("the raw-response sentinel reached the resolved output: %s", body)
	}
	if res.sink.Count() == 0 {
		t.Errorf("stage-error sink NOT bumped — every L1 Put gate would accept (cache) this result")
	} else if st, _ := res.sink.Sample(); st != stage {
		t.Errorf("stage-error sink sample names stage %q, want %q", st, stage)
	}
	if _, ok := res.dict["error"]; !ok {
		t.Errorf("dict[\"error\"] absent — the stage failure is not surfaced (keys=%v)", keysOf(res.dict))
	}
	if _, ok := res.dict["after"]; !ok {
		t.Errorf("downstream stage absent — the stage error truncated the resolve (#313 C-A) (keys=%v)", keysOf(res.dict))
	}
	if prov == ProvenanceCallerSupplied {
		want := fmt.Sprintf(`{"name":%q,"ok":false,"reason":"Error"}`, stage)
		if hv := res.outcomes.HeaderValue(); !strings.Contains(hv, want) {
			t.Errorf("stage outcome for %q is not Error: header=%s", stage, hv)
		}
	}
}

// TestIssue638_InvalidStageFilter_NeverServesOrCachesRaw — RED on origin/main:
// dict["raw"] is the unfiltered body, the sentinel is served, sink stays 0.
func TestIssue638_InvalidStageFilter_NeverServesOrCachesRaw(t *testing.T) {
	iterFailFastRetries(t)
	s := newA638Server(t)

	filters := []struct{ name, q string }{
		{"parse_error", `.raw | (`},
		{"runtime_error", `.raw.items | keys`},
		{"unknown_module", `include "no-such-module-638"; .raw`},
	}
	provs := []struct {
		name string
		p    Provenance
	}{
		{"stored", ProvenanceStored},
		{"caller_supplied", ProvenanceCallerSupplied},
	}

	for _, f := range filters {
		for _, pv := range provs {
			t.Run(f.name+"/"+pv.name, func(t *testing.T) {
				// All K x M identities resolve CONCURRENTLY: each carries its
				// own sink, so a per-identity verdict is not borrowed from a peer.
				ids := a638Identities()
				results := make([]a638Result, len(ids))
				var wg sync.WaitGroup
				for i, id := range ids {
					wg.Add(1)
					go func(i int, id a638Identity) {
						defer wg.Done()
						results[i] = a638Resolve(t, s, id, pv.p, a638FilterStages(f.q), nil)
					}(i, id)
				}
				wg.Wait()
				for i, id := range ids {
					t.Run(id.user, func(t *testing.T) {
						a638AssertStageFailedClosed(t, results[i], "raw", pv.p)
					})
				}
			})
		}
	}
}

// TestIssue638_ValidStageFilter_Control proves the arm can see the sentinel
// and a zero sink: a VALID filter that keeps the secret serves it, sink 0.
func TestIssue638_ValidStageFilter_Control(t *testing.T) {
	iterFailFastRetries(t)
	s := newA638Server(t)
	for _, id := range a638Identities() {
		res := a638Resolve(t, s, id, ProvenanceStored, a638FilterStages(`.raw.secret`), nil)
		if res.sink.Count() != 0 {
			t.Fatalf("%s: control sink=%d, want 0", id.user, res.sink.Count())
		}
		if got := res.dict["raw"]; got != a638Sentinel {
			t.Fatalf("%s: control dict[raw]=%v, want the sentinel — the arm cannot see a pass-through", id.user, got)
		}
	}
}

// TestIssue638_HandlerCore_FilterError_ReturnsError pins the unit seam every
// feed site shares: the handler returns the error and never writes out[key].
func TestIssue638_HandlerCore_FilterError_ReturnsError(t *testing.T) {
	for _, q := range []string{`.test | (`, `.test.items | keys`, `include "no-such-module-638"; .test`} {
		out := map[string]any{}
		err := jsonHandlerCore(context.Background(), jsonHandlerOptions{
			key: "test", out: out, filter: ptr.To(q),
		}, map[string]any{"secret": a638Sentinel, "items": "not-an-array"})
		if err == nil {
			t.Errorf("filter %q: handler returned nil — the stage continues with the raw response", q)
		}
		if v, ok := out["test"]; ok {
			t.Errorf("filter %q: out[test] written with %v — raw pass-through", q, v)
		}
	}
}

// TestIssue638_InvalidIterator_FailsStage — the iterator twin. RED on
// origin/main: the error is logged, the stage runs an empty plan, the sink
// stays 0 and no error is surfaced.
func TestIssue638_InvalidIterator_FailsStage(t *testing.T) {
	iterFailFastRetries(t)
	s := newA638Server(t)
	extras := map[string]any{"vals": []any{map[string]any{"ns": "a"}, map[string]any{"ns": "b"}}}

	stages := func(iter string) []*templates.API {
		return []*templates.API{
			{
				Name:      "fan",
				Path:      `${ "/items/" + .ns }`,
				Verb:      ptr.To(http.MethodGet),
				DependsOn: &templates.Dependency{Iterator: ptr.To(iter)},
			},
			{
				Name:      "after",
				Path:      "/down",
				Verb:      ptr.To(http.MethodGet),
				DependsOn: &templates.Dependency{Name: "fan"},
			},
		}
	}

	for _, iter := range []string{`.vals | (`, `.vals | keys | .[0] | keys`} {
		for _, prov := range []Provenance{ProvenanceStored, ProvenanceCallerSupplied} {
			for _, id := range a638Identities() {
				before := s.itemsCalls.Load()
				res := a638Resolve(t, s, id, prov, stages(iter), extras)
				a638AssertStageFailedClosed(t, res, "fan", prov)
				if n := s.itemsCalls.Load() - before; n != 0 {
					t.Errorf("iterator %q: %d item calls dispatched from a failed iterator", iter, n)
				}
			}
		}
	}

	// CONTROL: "upstream produced no data" stays benign — no stage error. A
	// null result (`.missing`), a nil iteration (`.missing[]`, the existing
	// IsBenignNilIteration case) and a zero-yield (`empty`) are all the C-3
	// empty plan, not an authoring fault.
	for _, iter := range []string{`.missing`, `.missing[]`, `empty`, `.vals | map(select(false))`} {
		res := a638Resolve(t, s, a638Identities()[0], ProvenanceStored, stages(iter), extras)
		if res.sink.Count() != 0 {
			t.Errorf("iterator %q: benign empty iteration bumped the sink (%d) — #638 must not turn the C-3 empty case into an error", iter, res.sink.Count())
		}
		if _, ok := res.dict["error"]; ok {
			t.Errorf("iterator %q: benign empty iteration surfaced an error: %v", iter, res.dict["error"])
		}
	}
}
