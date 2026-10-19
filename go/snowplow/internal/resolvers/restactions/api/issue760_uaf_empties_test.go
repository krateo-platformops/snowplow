//go:build unit || integration

package api

// issue760_uaf_empties_test.go — #760 RED-first arms (residual of #651 (b)).
//
// THE DEFECT (main 7372771f): two more userAccessFilter fail-closed branches
// serve an EMPTY answer that nothing marks as an error — no stage-error sink
// bump, no stage outcome — so since #561 (the identity-bound widgets cell and
// RAFullList STORE a UAF-touched resolve) the empty is persisted:
//   - the ServiceAccount endpoint cannot be acquired (resolveStageEndpoint):
//     typically TRANSIENT, so a token-file blip became a cached 0-row widget;
//   - the refilter finds no UserInfo on ctx (both refilter twins). Resolve
//     itself returns early without a UserInfo, so this branch is not reachable
//     through Resolve today: it is pinned at the handler contract, where the
//     resolver's stageFailed (#651) is the one recorder.
// Both keep the fail-closed drop. A LEGITIMATELY empty list and an all-denied
// list are real answers and stay cacheable (sink 0): the controls.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

func newA760Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/list":
			fmt.Fprint(w, `{"kind":"AnyPluralList","items":[`+
				`{"metadata":{"name":"item-a","namespace":"ns-a"}},`+
				`{"metadata":{"name":"item-b","namespace":"ns-b"}}]}`)
		case "/empty":
			fmt.Fprint(w, `{"kind":"AnyPluralList","items":[]}`)
		case "/down":
			fmt.Fprint(w, `{"ok":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","code":404}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func a760Stages(path string) []*templates.API {
	return []*templates.API{
		{
			Name: "items", Path: path, Verb: ptr.To(http.MethodGet),
			UserAccessFilter: &templates.UserAccessFilterSpec{
				Verb: "list", Group: "example.test", Resource: "anyplural", NamespaceFrom: ".metadata.namespace",
			},
		},
		{Name: "after", Path: "/down", Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Name: "items"}},
	}
}

func a760Resolve(srvURL, user string, stages []*templates.API) (map[string]any, *cache.StageErrorSink) {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"cohort-760"}}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srvURL})
	ctx, sink := cache.WithStageErrorSink(ctx)
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               stages,
		RESTActionNamespace: "ns-760",
		RESTActionName:      "ra-760",
		Provenance:          ProvenanceStored,
	})
	return dict, sink
}

func a760SAEndpoint(t *testing.T, ep *endpoints.Endpoint, err error) {
	t.Helper()
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) { return ep, err }
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	t.Setenv("CLIENT_MAX_RETRIES", "0")
}

func a760Items(t *testing.T, dict map[string]any) string {
	t.Helper()
	m, _ := dict["items"].(map[string]any)
	b, err := json.Marshal(m["items"])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// TestIssue760_SAEndpointAcquireFailureIsAStageError — RED on 7372771f: the
// empty is served and the sink stays 0.
func TestIssue760_SAEndpointAcquireFailureIsAStageError(t *testing.T) {
	srv := newA760Server(t)
	a760SAEndpoint(t, nil, errors.New("sa token file unreadable (injected)"))
	for _, user := range []string{"u760-a", "u760-b"} {
		t.Run(user, func(t *testing.T) {
			dict, sink := a760Resolve(srv.URL, user, a760Stages("/list"))
			if got := a760Items(t, dict); got != "[]" {
				t.Errorf("the fail-closed drop is not preserved: items=%s", got)
			}
			if sink.Count() == 0 {
				t.Fatalf("#760 RED: SA-endpoint acquire failure served an empty with no stage error — every L1 Put gate would store it")
			}
			if st, msg := sink.Sample(); st != "items" || strings.Contains(msg, "injected") {
				t.Errorf("sink sample = (%q, %q): want stage items and a fixed class, never the acquire error's text", st, msg)
			}
			if _, ok := dict["after"]; !ok {
				t.Errorf("the downstream stage did not run")
			}
		})
	}
}

// TestIssue760_MissingUserInfoIsAStageFailure — the handler contract: the
// refilter's missing-UserInfo drop reports the stage failed. RED on 7372771f.
func TestIssue760_MissingUserInfoIsAStageFailure(t *testing.T) {
	for _, filter := range []*string{nil, ptr.To(`.stage`)} {
		var failed []string
		out := map[string]any{}
		h := jsonHandlerBytes(context.Background(), jsonHandlerOptions{
			key: "stage", out: out, dict: out, filter: filter, apiCallName: "stage",
			uaf:         &templates.UserAccessFilterSpec{Verb: "list", Group: "example.test", Resource: "anyplural"},
			stageFailed: func(msg string) { failed = append(failed, msg) },
		})
		if err := h([]byte(`{"items":[{"metadata":{"name":"x","namespace":"ns-a"}}]}`)); err != nil {
			t.Fatalf("handler: %v", err)
		}
		if b, _ := json.Marshal(out["stage"]); string(b) != `{"items":[]}` {
			t.Errorf("filter=%v: the fail-closed drop is not preserved: %s", filter != nil, b)
		}
		if len(failed) != 1 {
			t.Fatalf("#760 RED (filter=%v): missing UserInfo reported %d stage failures, want 1", filter != nil, len(failed))
		}
	}
}

// TestIssue760_LegitimateEmptyAndAllDenied_StayCacheable — the controls: a
// list that is empty upstream, and a list the requester is granted none of,
// are real answers. No stage error, so the Put gates may store them.
func TestIssue760_LegitimateEmptyAndAllDenied_StayCacheable(t *testing.T) {
	srv := newA760Server(t)
	newRefilterTestWatcher(t) // RBAC installed, no grants for anyone
	a760SAEndpoint(t, &endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-760"}, nil)
	for _, c := range []struct{ name, path string }{{"empty_upstream", "/empty"}, {"all_denied", "/list"}} {
		t.Run(c.name, func(t *testing.T) {
			dict, sink := a760Resolve(srv.URL, "u760-nogrant", a760Stages(c.path))
			if got := a760Items(t, dict); got != "[]" {
				t.Fatalf("items=%s, want []", got)
			}
			if sink.Count() != 0 {
				st, msg := sink.Sample()
				t.Fatalf("a legitimate empty answer was marked a stage error (%s: %s) — it would never be cached", st, msg)
			}
		})
	}
	// Handler contract twin: a requester present, nothing kept → no failure.
	var failed int
	out := map[string]any{}
	h := jsonHandlerBytes(ctxWithUser("u760-nogrant"), jsonHandlerOptions{
		key: "stage", out: out, dict: out, apiCallName: "stage",
		uaf:         &templates.UserAccessFilterSpec{Verb: "list", Group: "example.test", Resource: "anyplural", NamespaceFrom: ".metadata.namespace"},
		stageFailed: func(string) { failed++ },
	})
	if err := h([]byte(`{"items":[{"metadata":{"name":"x","namespace":"ns-a"}}]}`)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if failed != 0 {
		t.Fatalf("an all-denied list reported %d stage failures, want 0", failed)
	}
}

// TestIssue760_ComposesWithTheNullElementSkip — #760 on top of #767: a
// resourcesFrom that yields only null (an input not resolved yet) is skipped,
// denies quietly and stays cacheable; the two #760 failures still record the
// stage error whatever resourcesFrom yields, because each fires before the
// resource set is evaluated.
func TestIssue760_ComposesWithTheNullElementSkip(t *testing.T) {
	srv := newA760Server(t)
	newRefilterTestWatcher(t) // RBAC installed, no grants for anyone
	nullOnly := func(path string) []*templates.API {
		st := a760Stages(path)
		st[0].UserAccessFilter.Resource = ""
		st[0].UserAccessFilter.ResourcesFrom = "[ null ]"
		return st
	}

	t.Run("null_only_set_denies_quietly", func(t *testing.T) {
		a760SAEndpoint(t, &endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-760"}, nil)
		dict, sink := a760Resolve(srv.URL, "u760-nogrant", nullOnly("/list"))
		if got := a760Items(t, dict); got != "[]" {
			t.Fatalf("items=%s, want [] (an empty resource set denies every item)", got)
		}
		if sink.Count() != 0 {
			st, msg := sink.Sample()
			t.Fatalf("a [null]-only resource set was marked a stage error (%s: %s) — #767 makes it a quiet deny", st, msg)
		}
	})
	t.Run("sa_unavailable_still_a_stage_error", func(t *testing.T) {
		a760SAEndpoint(t, nil, errors.New("sa token file unreadable (injected)"))
		dict, sink := a760Resolve(srv.URL, "u760-a", nullOnly("/list"))
		if got := a760Items(t, dict); got != "[]" {
			t.Errorf("items=%s, want []", got)
		}
		if sink.Count() == 0 {
			t.Fatalf("SA-endpoint acquire failure with a [null] resource set recorded no stage error")
		}
	})
	t.Run("missing_userinfo_still_a_stage_failure", func(t *testing.T) {
		var failed []string
		out := map[string]any{}
		h := jsonHandlerBytes(context.Background(), jsonHandlerOptions{
			key: "stage", out: out, dict: out, apiCallName: "stage",
			uaf:         &templates.UserAccessFilterSpec{Verb: "list", Group: "example.test", ResourcesFrom: "[ null ]"},
			stageFailed: func(msg string) { failed = append(failed, msg) },
		})
		if err := h([]byte(`{"items":[{"metadata":{"name":"x","namespace":"ns-a"}}]}`)); err != nil {
			t.Fatalf("handler: %v", err)
		}
		if len(failed) != 1 || failed[0] != uafUserInfoMissingMsg {
			t.Fatalf("stage failures = %q, want exactly the missing-UserInfo one", failed)
		}
	})
}
