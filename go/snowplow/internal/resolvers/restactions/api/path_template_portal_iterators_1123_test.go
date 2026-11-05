package api

// #1123 — the 8 portal stages that declare segments(), run with their REAL
// iterators. testdata/portal_segments_ras_1123.yaml is the helm render of the 7
// RESTActions that hold them, at portal main. Each stage's path is rewritten to
// its declared form (corpusChanges1123); nothing else in the spec changes.
//
// The question is what a stage does when its value is absent: a template that
// renders no path fails its stage (TestIssue1123_TemplateThatRendersNoPathIsRefused),
// and the portal's Phase-1 prewarm runs with nil extras. The arms show that the
// iterators, not the refusal, decide these stages: an absent value yields no
// element, so there is nothing to call and nothing to refuse.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

type portalSegments1123 struct {
	ra, stage, field string
	// present is the dict the stage sees on a live cluster, want the paths its
	// iterator plans from it. Every present dict also holds entries the iterator
	// must drop (a null path, a Secret).
	present map[string]any
	want    []string
}

func proposals1123(apiVersion string) map[string]any {
	return map[string]any{"items": []any{map[string]any{"spec": map[string]any{"change": map[string]any{
		"format": "yaml", "content": "apiVersion: " + apiVersion + "\nkind: Thing\n"}}}}}
}

var portalSegmentStages1123 = []portalSegments1123{
	{ra: "composition-architecture", stage: "objs", field: "path",
		present: map[string]any{
			"arch": map[string]any{"graph": map[string]any{"nodes": []any{map[string]any{
				"present": true, "apiVersion": "apps/v1", "names": []any{"web", "no-path", "cred"}}}}},
			"comp": []any{map[string]any{"managed": []any{
				map[string]any{"name": "web", "apiVersion": "apps/v1", "path": "/apis/apps/v1/namespaces/demo/deployments/web"},
				map[string]any{"name": "no-path", "apiVersion": "apps/v1", "path": nil},
				map[string]any{"name": "cred", "apiVersion": "apps/v1", "path": "/api/v1/namespaces/demo/secrets/cred"},
			}}},
		},
		want: []string{"/apis/apps/v1/namespaces/demo/deployments/web"}},
	{ra: "composition-resources", stage: "resources", field: "_getpath",
		present: map[string]any{"name": "app", "namespace": "demo", "found": []any{
			map[string]any{"_cn": "app", "_cns": "demo", "_getpath": "/api/v1/namespaces/demo/configmaps/app-values"},
			map[string]any{"_cn": "app", "_cns": "demo", "_getpath": nil},
			map[string]any{"_cn": "app", "_cns": "demo", "_getpath": "/api/v1/namespaces/demo/secrets/app"},
			map[string]any{"_cn": "other", "_cns": "demo", "_getpath": "/api/v1/namespaces/demo/configmaps/other"},
		}},
		want: []string{"/api/v1/namespaces/demo/configmaps/app-values"}},
	{ra: "resource-detail", stage: "disco", field: "base",
		present: map[string]any{"group": "apps", "version": "v1", "plural": "deployments", "name": "web"},
		want:    []string{"/apis/apps/v1"}},
	{ra: "resource-detail", stage: "object", field: "path",
		present: map[string]any{"group": "apps", "version": "v1", "plural": "deployments", "name": "web", "namespace": "demo",
			"disco": map[string]any{"resources": []any{map[string]any{"name": "deployments", "namespaced": true}}}},
		want: []string{"/apis/apps/v1/namespaces/demo/deployments/web"}},
	{ra: "platform-reviews", stage: "kinds", field: "discovery",
		present: map[string]any{"apis": []any{"apps/v1", "v1"}, "proposals": proposals1123("apps/v1")},
		want:    []string{"/apis/apps/v1"}},
	{ra: "review-proposal", stage: "kinds", field: "discovery",
		present: map[string]any{"apis": []any{"apps/v1", "v1"}, "proposal": proposals1123("v1")},
		want:    []string{"/api/v1"}},
	{ra: "review-run", stage: "kinds", field: "discovery",
		present: map[string]any{"apis": []any{"apps/v1", "v1"}, "all": proposals1123("apps/v1")},
		want:    []string{"/apis/apps/v1"}},
	{ra: "ticket-detail", stage: "object", field: "path",
		present: map[string]any{"ticket": map[string]any{"spec": map[string]any{"objectRef": map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "namespace": "demo"}}}},
		want: []string{"/apis/apps/v1/namespaces/demo/deployments/web"}},
}

func loadPortalSegmentRAs1123(t *testing.T) map[string]*templates.RESTAction {
	t.Helper()
	b, err := os.ReadFile("testdata/portal_segments_ras_1123.yaml")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*templates.RESTAction{}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(b), 4096)
	for {
		var ra templates.RESTAction
		if err := dec.Decode(&ra); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		out[ra.Name] = &ra
	}
	return out
}

// declaredStage1123 is the stage as the portal ships it with this release: its
// real iterator, its path declared with segments(). It fails if the render no
// longer holds the shape the arm reads.
func declaredStage1123(t *testing.T, ras map[string]*templates.RESTAction, c portalSegments1123) *templates.API {
	t.Helper()
	ra := ras[c.ra]
	if ra == nil {
		t.Fatalf("testdata has no RESTAction %s", c.ra)
	}
	for _, s := range ra.Spec.API {
		if s.Name != c.stage {
			continue
		}
		if s.Path != "${ ."+c.field+" }" {
			t.Fatalf("%s/%s: path %q; the arm expects the undeclared whole value", c.ra, c.stage, s.Path)
		}
		if s.DependsOn == nil || s.DependsOn.Iterator == nil || *s.DependsOn.Iterator == "" {
			t.Fatalf("%s/%s has no iterator", c.ra, c.stage)
		}
		d := *s
		d.Path = "${ segments(." + c.field + ") }"
		return &d
	}
	t.Fatalf("%s has no stage %s", c.ra, c.stage)
	return nil
}

func planPortal1123(in *templates.API, dict map[string]any) ([]string, []skippedOption, error) {
	opts, skips, err := createRequestOptionsWithSkips(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), in, dict, true)
	var paths []string
	for _, o := range opts {
		paths = append(paths, o.Path)
	}
	return paths, skips, err
}

// A value that is there reaches the path the stage read before, and what the
// iterator drops stays dropped. The same stage with no value plans nothing and
// refuses nothing: on nil extras and with no upstream stage in the dict.
func TestIssue1123_PortalSegmentStagesRunTheirRealIterators(t *testing.T) {
	ras := loadPortalSegmentRAs1123(t)
	for _, c := range portalSegmentStages1123 {
		t.Run(c.ra+"/"+c.stage, func(t *testing.T) {
			in := declaredStage1123(t, ras, c)

			paths, skips, err := planPortal1123(in, c.present)
			if err != nil {
				t.Fatalf("present: iterator: %v", err)
			}
			if len(skips) != 0 {
				t.Fatalf("present: refused %+v", skips)
			}
			if strings.Join(paths, " ") != strings.Join(c.want, " ") {
				t.Fatalf("present: planned %v, want %v", paths, c.want)
			}

			for name, dict := range map[string]map[string]any{"nil extras": nil, "empty dict": {}} {
				paths, skips, err := planPortal1123(in, dict)
				if err != nil {
					t.Fatalf("%s: iterator: %v", name, err)
				}
				if len(paths) != 0 || len(skips) != 0 {
					t.Fatalf("%s: planned %v, refused %+v; want nothing to call", name, paths, skips)
				}
			}
		})
	}
}

// A whole RESTAction resolved the way Phase-1 prewarm resolves it: no extras,
// every other stage real, an apiserver with empty lists. None of the 8
// stages is refused or even listed in the outcomes: a listed stage failed or
// never ran, so an arm that stopped early cannot pass here. Nothing is counted
// as refused, and the endpoint's root is never dialled.
func TestIssue1123_PortalSegmentStagesOnANilExtrasPrewarm(t *testing.T) {
	ras := loadPortalSegmentRAs1123(t)
	api := newX1106Srv(t, false, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"List","apiVersion":"v1","metadata":{},"items":[]}`)
	})
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	t.Cleanup(SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: api.srv.URL, Token: "SA"}, nil
	}))
	for _, c := range portalSegmentStages1123 {
		t.Run(c.ra+"/"+c.stage, func(t *testing.T) {
			in := declaredStage1123(t, ras, c)
			var stages []*templates.API
			for _, s := range ras[c.ra].Spec.API {
				if s.Name == c.stage {
					stages = append(stages, in)
					continue
				}
				stages = append(stages, s)
			}
			empty, refused := MalformedDialSkippedByReason(reasonEmptyInterp), MalformedDialSkippedByReason(reasonPathValueRefused)
			o := rr16Resolve(t, workclass.Prewarm, nil, stages...)

			var rows []struct {
				Name   string `json:"name"`
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal([]byte(o.outcomes), &rows); err != nil {
				t.Fatalf("outcomes %s: %v", o.outcomes, err)
			}
			for _, r := range rows {
				if r.Name == c.stage {
					t.Fatalf("the stage is listed: %+v (outcomes %s)", r, o.outcomes)
				}
				if r.Reason == StageReasonInvalidPath {
					t.Errorf("stage %s failed with InvalidPath: %s", r.Name, o.outcomes)
				}
			}
			if got := MalformedDialSkippedByReason(reasonPathValueRefused) - refused; got != 0 {
				t.Errorf("path_value_refused moved by %d", got)
			}
			if got := MalformedDialSkippedByReason(reasonEmptyInterp) - empty; got != 0 {
				t.Errorf("empty_interp moved by %d", got)
			}
			for _, h := range api.all() {
				if h.Path == "/" || h.Path == "" {
					t.Fatalf("dialled the root: %+v", h)
				}
			}
		})
	}
}
