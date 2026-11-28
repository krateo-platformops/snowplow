package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// #1619 — an unresolved stage carries a STABLE reason token, never parsed from
// the prose, and a stage the dependsOn graph cannot order is unresolved by cause
// instead of failing the whole pass.

func ra1619(stages ...*templates.API) *templates.RESTAction {
	return &templates.RESTAction{Spec: templates.RESTActionSpec{API: stages}}
}

func stage1619(name, path string) *templates.API { return &templates.API{Name: name, Path: path} }

func dep1619(name, path, on string) *templates.API {
	s := stage1619(name, path)
	s.DependsOn = &templates.Dependency{Name: on}
	return s
}

// failingDiscovery answers every discovery read with a 500: discovery is down,
// which says nothing about the stage.
func failingDiscovery(t *testing.T) *rest.Config {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pemEncodeCert(srv.Certificate())}}
}

// TestIssue1619_EveryReasonTokenIsReachable drives one stage per token through
// InspectReadSet and requires the token, and requires that the tokens reached
// are exactly the closed set UnresolvedReasons: a cause without a token, or a
// token without a cause, fails here.
func TestIssue1619_EveryReasonTokenIsReachable(t *testing.T) {
	const core = "/api/v1/namespaces/x/configmaps"
	uafBad := func(resource string) *templates.API {
		s := stage1619("s", core)
		s.UserAccessFilter = &templates.UserAccessFilterSpec{Verb: "list", Group: "", Resource: resource}
		return s
	}
	withPayload := stage1619("s", core)
	withPayload.Payload = ptr.To(`${ error }`)
	withHeader := stage1619("s", core)
	withHeader.Headers = []string{`${ error }`}
	uafFrom := stage1619("s", core)
	uafFrom.UserAccessFilter = &templates.UserAccessFilterSpec{Verb: "list", Group: "", ResourcesFrom: `.comp.plural`}
	iterEmpty := dep1619("s", core, "prior")
	iterEmpty.DependsOn.Iterator = ptr.To(`.els`)

	reached := map[string]bool{}
	for _, c := range []struct {
		token  string
		stage  *templates.API
		extra  *templates.API // a stage the first depends on or that sits beside it
		extras map[string]any
		down   bool // discovery is down
	}{
		{ReasonStageDataInResourcesFrom, uafFrom, nil, nil, false},
		{ReasonResourceNotServed, stage1619("s", "/api/v1/namespaces/x/nosuch"), nil, nil, false},
		{ReasonResourceNotServed, uafBad("nosuch"), nil, nil, false},
		{ReasonDiscoveryFailed, stage1619("s", core), nil, nil, true},
		{ReasonNoRequestPath, iterEmpty, stage1619("prior", core), map[string]any{"els": []any{}}, false},
		{ReasonUnrenderedTemplate, stage1619("s", "/api/v1/namespaces/${ .x /configmaps"), nil, nil, false},
		{ReasonPathNotAsServed, stage1619("s", `${ "api/v1/namespaces/x/configmaps" }`), nil, nil, false},
		// More than one embedded ${...} in one path does not render as a path.
		{ReasonPathNotAsServed, stage1619("s", `/api/v1/namespaces/${ .a }/configmaps/${ .b }`), nil, map[string]any{"a": "x", "b": "y"}, false},
		{ReasonPathValueRefused, stage1619("s", `${ "/api/v1/namespaces/" + .a + "/configmaps" }`), nil, map[string]any{"a": "a/b"}, false},
		{ReasonJQPathError, stage1619("s", `${ "/api/v1/" + (1|error) }`), nil, nil, false},
		{ReasonJQPayloadError, withPayload, nil, nil, false},
		{ReasonJQHeaderError, withHeader, nil, nil, false},
		{ReasonTemplatedResource, stage1619("s", `${ "/apis/apps/v1/" + (.kind // "") }`), nil, nil, false},
		{ReasonNoStaticResource, stage1619("s", `${ "/apis/" + (.g // "") + "/v1/namespaces/x/pods" }`), nil, nil, false},
		{ReasonNotEnumerablePath, stage1619("s", "/healthz"), nil, nil, false},
		{ReasonUnknownDependency, dep1619("s", core, "ghost"), nil, nil, false},
		{ReasonCyclicDependency, dep1619("s", core, "other"), dep1619("other", core, "s"), nil, false},
	} {
		t.Run(c.token+"/"+c.stage.Name, func(t *testing.T) {
			if c.down {
				withInspectSARESTConfig(t, failingDiscovery(t))
			} else {
				withInspectSARESTConfig(t, discovery504(t))
			}
			stages := []*templates.API{c.stage}
			if c.extra != nil {
				stages = append(stages, c.extra)
			}
			_, un, err := InspectReadSet(context.Background(), ra1619(stages...), c.extras)
			if err != nil {
				t.Fatalf("InspectReadSet errored: %v", err)
			}
			var got []string
			for _, u := range un {
				if u.Stage == "s" {
					got = append(got, u.Reason)
					if u.Message == "" {
						t.Errorf("unresolved %+v has no message", u)
					}
				}
			}
			if !reflect.DeepEqual(got, []string{c.token}) {
				t.Fatalf("want exactly token %q for stage s, got %v (unresolved %+v)", c.token, got, un)
			}
			reached[c.token] = true
		})
	}
	for _, tok := range UnresolvedReasons {
		if !reached[tok] {
			t.Errorf("token %q is in the set but no case reaches it", tok)
		}
	}
	for tok := range reached {
		if !slices.Contains(UnresolvedReasons, tok) {
			t.Errorf("token %q is reached but not in UnresolvedReasons", tok)
		}
	}
}

// The tokens the table above reaches are the closed set: kept as its own check so
// a token added to the set without a case is named.
func TestIssue1619_ReasonSetIsClosedAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range UnresolvedReasons {
		if r == "" || seen[r] {
			t.Errorf("token %q is empty or repeated", r)
		}
		seen[r] = true
	}
	want := []string{
		"stageDataInResourcesFrom", "resourceNotServed", "discoveryFailed", "noRequestPath", "unrenderedTemplate",
		"pathNotAsServed", "pathValueRefused", "jqPathError", "jqPayloadError", "jqHeaderError", "templatedResource",
		"noStaticResource", "notEnumerablePath", "unknownDependency", "cyclicDependency",
	}
	got := append([]string(nil), UnresolvedReasons...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the token set is a wire contract: want %v, got %v", want, got)
	}
}

// An unknown dependency is not a cycle, is not an error of the pass, and the
// stages that can be enumerated still are.
func TestIssue1619_UnknownDependencyVersusRealCycle(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))
	const core = "/api/v1/namespaces/x/configmaps"
	svc := "/api/v1/namespaces/x/services"

	t.Run("unknown dependency", func(t *testing.T) {
		rows, un, err := InspectReadSet(context.Background(), ra1619(
			stage1619("good", core), dep1619("bad", svc, "ghost")), nil)
		if err != nil {
			t.Fatalf("an unknown dependsOn must not fail the pass: %v", err)
		}
		if len(rows) != 1 || rows[0].Resource != "configmaps" {
			t.Fatalf("the enumerable stage's rows must survive, and the invalid stage must add none: %+v", rows)
		}
		if len(un) != 1 || un[0].Stage != "bad" || un[0].Reason != ReasonUnknownDependency {
			t.Fatalf("want one unknownDependency for bad, got %+v", un)
		}
	})

	t.Run("real cycle", func(t *testing.T) {
		rows, un, err := InspectReadSet(context.Background(), ra1619(
			stage1619("good", core), dep1619("a", svc, "b"), dep1619("b", svc, "a"), dep1619("tail", svc, "a")), nil)
		if err != nil {
			t.Fatalf("a cycle must not fail the pass: %v", err)
		}
		if len(rows) != 1 || rows[0].Resource != "configmaps" {
			t.Fatalf("rows = %+v", rows)
		}
		var got []string
		for _, u := range un {
			if u.Reason != ReasonCyclicDependency {
				t.Errorf("a cycle is not %q: %+v", u.Reason, u)
			}
			got = append(got, u.Stage)
		}
		if !reflect.DeepEqual(got, []string{"a", "b", "tail"}) {
			t.Fatalf("cycle members and what hangs off them, in declared order: got %v", got)
		}
	})

	t.Run("self dependency is a cycle", func(t *testing.T) {
		_, un, err := InspectReadSet(context.Background(), ra1619(dep1619("a", svc, "a")), nil)
		if err != nil || len(un) != 1 || un[0].Reason != ReasonCyclicDependency {
			t.Fatalf("got %+v, err %v", un, err)
		}
	})

	t.Run("an unknown dependency does not hide a real cycle", func(t *testing.T) {
		_, un, err := InspectReadSet(context.Background(), ra1619(
			dep1619("bad", svc, "ghost"), dep1619("a", svc, "b"), dep1619("b", svc, "a")), nil)
		if err != nil {
			t.Fatal(err)
		}
		want := []Unresolved{
			{Stage: "bad", Reason: ReasonUnknownDependency},
			{Stage: "a", Reason: ReasonCyclicDependency},
			{Stage: "b", Reason: ReasonCyclicDependency},
		}
		if len(un) != len(want) {
			t.Fatalf("got %+v", un)
		}
		for i := range want {
			if un[i].Stage != want[i].Stage || un[i].Reason != want[i].Reason {
				t.Fatalf("entry %d: got %+v want %+v", i, un[i], want[i])
			}
		}
	})

	t.Run("a stage after an unknown dependency is still enumerated", func(t *testing.T) {
		rows, un, err := InspectReadSet(context.Background(), ra1619(
			dep1619("bad", core, "ghost"), dep1619("after", svc, "bad")), nil)
		if err != nil || len(un) != 1 || un[0].Stage != "bad" {
			t.Fatalf("un=%+v err=%v", un, err)
		}
		if len(rows) != 1 || rows[0].Resource != "services" {
			t.Fatalf("rows = %+v", rows)
		}
	})
}
