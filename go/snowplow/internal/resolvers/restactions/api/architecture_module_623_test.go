//go:build unit
// +build unit

package api

// #623: the built-in module krateo/architecture/v1 resolves through the REAL
// stage-filter path (jsonHandlerCore → EvalValue with jqsupport.ModuleLoader(),
// the core behind every dispatch path), and each stage's output there equals
// the inlined stage filter the frontend generator emits. The full fixture
// matrix lives in internal/support/jq/architecture_v1_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
)

type refStage623 struct {
	Name       string `json:"name"`
	APIVersion string `json:"apiVersion"`
	Filter     string `json:"filter"`
}

func eventSiteStages623(t *testing.T) []refStage623 {
	t.Helper()
	raw, err := os.ReadFile("../../../support/jq/testdata/architecture/reference_filters.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Charts []struct {
			Name   string        `json:"name"`
			Stages []refStage623 `json:"stages"`
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Charts {
		if c.Name == "event-site" {
			return c.Stages
		}
	}
	t.Fatal("event-site chart missing from reference filters")
	return nil
}

func stageResponse623(t *testing.T, stage refStage623) any {
	t.Helper()
	var src string
	if stage.Name == "arch" {
		graph, _ := json.Marshal(map[string]any{"v": 1, "chart": "event-site", "nodes": []any{}, "states": []any{"A"}})
		b, _ := json.Marshal(map[string]any{
			"metadata": map[string]any{"name": "es1-architecture", "labels": map[string]any{
				"krateo.io/composition-name": "es1", "krateo.io/composition-id": "uid"}},
			"data": map[string]any{"graph": string(graph)},
		})
		src = string(b)
	} else {
		src = `{"kind":"List","items":[
		  {"metadata":{"name":"es1-x","namespace":"es-ns","creationTimestamp":"2026-10-06T10:00:00Z","uid":"u"},"spec":{"a":1},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
		  {"metadata":{"name":""}}, "garbage",
		  {"metadata":{"name":"es1-y"}}]}`
	}
	var v any
	if err := json.Unmarshal([]byte(src), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestArchitectureModule623_StageFiltersThroughHandler(t *testing.T) {
	stages := eventSiteStages623(t)
	if len(stages) != 5 {
		t.Fatalf("event-site must have 5 stages, got %d", len(stages))
	}
	for _, stage := range stages {
		module := `include "krateo/architecture/v1"; arch_step`
		if stage.Name != "arch" {
			module = fmt.Sprintf(`include "krateo/architecture/v1"; list_step(%q; %q)`, stage.Name, stage.APIVersion)
		}
		run := func(filter string) any {
			out := map[string]any{}
			opts := jsonHandlerOptions{
				key:    stage.Name,
				out:    out,
				extras: map[string]any{"compositionName": "es1", "compositionNamespace": "es-ns"},
				filter: ptr.To(filter),
			}
			if err := jsonHandlerCore(context.Background(), opts, stageResponse623(t, stage)); err != nil {
				t.Fatalf("stage %s: jsonHandlerCore: %v", stage.Name, err)
			}
			return out[stage.Name]
		}
		inlined, got := run(stage.Filter), run(module)
		if !reflect.DeepEqual(inlined, got) {
			t.Fatalf("stage %s: module output differs from the inlined filter\n inlined: %#v\n  module: %#v", stage.Name, inlined, got)
		}
		// Not vacuous: a filter that failed would leave the RAW response (#638),
		// which is a map with "items"/"metadata"; the projected outputs are not.
		switch v := got.(type) {
		case []any:
			if len(v) != 2 {
				t.Fatalf("stage %s: want the 2 named items projected, got %#v", stage.Name, v)
			}
			if v[0].(map[string]any)["apiVersion"] != stage.APIVersion {
				t.Fatalf("stage %s: apiVersion not stamped: %#v", stage.Name, v[0])
			}
		case map[string]any:
			if _, ok := v["graph"]; !ok || stage.Name != "arch" {
				t.Fatalf("stage %s: not the projected arch shape: %#v", stage.Name, v)
			}
		default:
			t.Fatalf("stage %s: unexpected output %#v", stage.Name, got)
		}
	}
}
