package jq

// Tests for the built-in module krateo/architecture/v1 (#623).
//
// The module replaces jq that the Blueprint Composer used to inline into every
// chart's <chart>-status RESTAction. The ORACLE is that inlined jq, exactly as
// the frontend generator emits it: testdata/architecture/reference_filters.json
// holds the spec.filter and stage filters produced by projectionCompile.ts at
// frontend tag 1.8.4 (provenance inside the file), including the live
// krateo-system/event-site-status shape. For every fixture the module-based
// filters must produce output byte-identical to the inlined ones, stage by
// stage and for the final spec.filter.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/jqutil"
)

const archModule = "krateo/architecture/v1"

type archStep struct {
	IDs  []string `json:"ids"`
	Step string   `json:"step"`
}

type archStage struct {
	Name       string `json:"name"`
	APIVersion string `json:"apiVersion"`
	Filter     string `json:"filter"`
}

type archChart struct {
	Name       string      `json:"name"`
	Source     string      `json:"source"`
	Steps      []archStep  `json:"steps"`
	Stages     []archStage `json:"stages"`
	SpecFilter string      `json:"specFilter"`
}

func loadArchCharts(t *testing.T) map[string]archChart {
	t.Helper()
	raw, err := os.ReadFile("testdata/architecture/reference_filters.json")
	if err != nil {
		t.Fatalf("read reference filters: %v", err)
	}
	var doc struct {
		Charts []archChart `json:"charts"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse reference filters: %v", err)
	}
	out := map[string]archChart{}
	for _, c := range doc.Charts {
		out[c.Name] = c
	}
	for _, want := range []string{"event-site", "shared-kind", "single"} {
		if _, ok := out[want]; !ok {
			t.Fatalf("reference filters lack chart %q", want)
		}
	}
	if n := len(out["event-site"].Stages); n != 5 {
		t.Fatalf("event-site must be the live 5-stage shape, got %d stages", n)
	}
	return out
}

func jqLit(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// moduleStageFilter is what a chart's stage filter becomes with the module.
func moduleStageFilter(t *testing.T, s archStage) string {
	if s.Name == "arch" {
		return `include "` + archModule + `"; arch_step`
	}
	return fmt.Sprintf(`include "%s"; list_step(%s; %s)`, archModule, jqLit(t, s.Name), jqLit(t, s.APIVersion))
}

// moduleSpecFilter is what a chart's spec.filter becomes with the module.
func moduleSpecFilter(t *testing.T, steps []archStep) string {
	return fmt.Sprintf(`include "%s"; fold_steps(%s) | architecture_state`, archModule, jqLit(t, steps))
}

// norm turns Go literals into the decoded-JSON shapes a resolve dict holds.
func norm(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func evalJQ(t *testing.T, query string, data any) string {
	t.Helper()
	// The exact call restactions.Resolve makes for spec.filter.
	s, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: query, Data: data, ModuleLoader: ModuleLoader()})
	if err != nil {
		t.Fatalf("jq eval failed: %v\nquery:\n%s", err, query)
	}
	return s
}

// stageInput is one stage's outcome in a fixture: a response, an error value
// (accumulated under <name>Err, as accumulateErrorKey does), or neither (the
// stage left no entry at all).
type stageInput struct {
	resp any
	err  any
}

type archCase struct {
	name   string
	chart  string
	extras map[string]any
	stages map[string]stageInput
}

// runPipeline resolves a fixture the way api.Resolve + restactions.Resolve do:
// each stage's filter sees {extras, <name>: response}, its output lands under
// <name> in the dict, the extras sit at the top level, and spec.filter runs
// over the dict. It returns every stage output and the final output.
func runPipeline(t *testing.T, c archCase, ch archChart, stageFilter func(archStage) string, specFilter string) (map[string]string, string) {
	t.Helper()
	dict := map[string]any{}
	for k, v := range c.extras {
		dict[k] = v
	}
	outs := map[string]string{}
	for _, s := range ch.Stages {
		in, ok := c.stages[s.Name]
		if !ok {
			continue
		}
		if in.err != nil {
			dict[s.Name+"Err"] = []any{norm(t, in.err)}
			continue
		}
		pig := map[string]any{s.Name: norm(t, in.resp)}
		if len(c.extras) > 0 {
			pig["extras"] = norm(t, c.extras)
		}
		got := evalJQ(t, stageFilter(s), pig)
		outs[s.Name] = got
		var v any
		if err := json.Unmarshal([]byte(got), &v); err != nil {
			t.Fatalf("stage %s output is not JSON: %v", s.Name, err)
		}
		dict[s.Name] = v
	}
	return outs, evalJQ(t, specFilter, dict)
}

// ---- fixtures -----------------------------------------------------------

const archNS = "es-ns"

func archConfigMap(composition string, graph map[string]any) map[string]any {
	g, _ := json.Marshal(graph)
	return map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"name": composition + "-architecture", "namespace": archNS,
			"labels": map[string]any{
				"krateo.io/composition-name": composition,
				"krateo.io/composition-id":   "0b7c-uid",
			},
		},
		"data": map[string]any{"graph": string(g)},
	}
}

func node(id, kind, apiVersion string, level int, names []string, readyWhen string, deps ...map[string]any) map[string]any {
	n := map[string]any{
		"id": id, "kind": kind, "apiVersion": apiVersion, "class": "native",
		"level": level, "present": true, "names": names,
		"dependsOn": deps,
	}
	if deps == nil {
		n["dependsOn"] = []map[string]any{}
	}
	if readyWhen != "" {
		n["readyWhen"] = readyWhen
	}
	return n
}

func dep(ref string, ready bool) map[string]any {
	return map[string]any{"ref": ref, "active": true, "ready": ready}
}

func obj(name, created string, status map[string]any) map[string]any {
	o := map[string]any{
		"kind": "X",
		"metadata": map[string]any{
			"name": name, "namespace": archNS, "creationTimestamp": created, "uid": "u-" + name,
			"labels": map[string]any{"app": name},
		},
	}
	if status != nil {
		o["status"] = status
	}
	return o
}

func cond(typ, status string) map[string]any {
	return map[string]any{"type": typ, "status": status, "reason": typ + status, "message": "condition " + typ + " is " + status}
}

func list(items ...map[string]any) map[string]any {
	if items == nil {
		items = []map[string]any{}
	}
	return map[string]any{"kind": "List", "apiVersion": "v1", "metadata": map[string]any{"resourceVersion": "42"}, "items": items}
}

const (
	condReady = `.status.conditions[] | select(.type == "Ready") | .status == "True"`
	condAvail = `.status.conditions[] | select(.type == "Available") | .status == "True"`
)

func eventSiteGraph() map[string]any {
	return map[string]any{
		"v": 1, "chart": "event-site",
		"composition": map[string]any{"name": "es1"},
		"states":      []string{"Provisioning", "Configuring", "Running"},
		"nodes": []map[string]any{
			node("deployment", "Deployment", "apps/v1", 0, []string{"es1-web"}, ".status.readyReplicas"),
			node("service", "Service", "v1", 0, []string{"es1-web"}, ""),
			node("form", "Form", "acmefeedback.example.io/v1alpha1", 1, []string{"es1-form"}, condReady, dep("deployment", true), dep("service", false)),
			node("computeinstance", "ComputeInstance", "compute.cnrm.cloud.google.com/v1beta1", 2, []string{"es1-vm"}, condReady, dep("form", true)),
		},
	}
}

func sharedKindGraph() map[string]any {
	hook := node("hook", "Job", "batch/v1", 0, []string{"sk1-hook"}, "")
	hook["lifecycle"] = "pre-install"
	return map[string]any{
		"v": 1, "chart": "shared-kind",
		"composition": map[string]any{"name": "sk1"},
		"states":      []string{"Data", "App"},
		"nodes": []map[string]any{
			node("db", "Deployment", "apps/v1", 0, []string{"sk1-db"}, condAvail),
			node("web", "Deployment", "apps/v1", 1, []string{"sk1-web-a", "sk1-web-b"}, ".status.readyReplicas", map[string]any{"ref": "db", "active": true, "ready": true, "all": true}),
			node("cfg", "ConfigMap", "v1", 0, []string{"sk1-cfg"}, ""),
			hook,
		},
	}
}

func singleGraph() map[string]any {
	return map[string]any{
		"v": 1, "chart": "single",
		"composition": map[string]any{"name": "s1"},
		"states":      []string{"Ready"},
		"nodes":       []map[string]any{node("only", "ConfigMap", "v1", 0, []string{"s1-only"}, ".status.ok")},
	}
}

func statusErr(code int, reason, kind, name, msg string) map[string]any {
	return map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason, "message": msg,
		"details": map[string]any{"kind": kind, "name": name},
	}
}

func archCases() []archCase {
	esExtras := map[string]any{"compositionName": "es1", "compositionNamespace": archNS}
	t0, t1, t2 := "2026-10-06T10:00:00Z", "2026-10-06T10:01:00Z", "2026-10-06T10:02:00Z"
	esAllReady := func() map[string]stageInput {
		return map[string]stageInput{
			"arch": {resp: archConfigMap("es1", eventSiteGraph())},
			"o0":   {resp: list(obj("es1-web", t0, map[string]any{"readyReplicas": 1}))},
			"o1":   {resp: list(obj("es1-web", t0, nil))},
			"o2":   {resp: list(obj("es1-form", t1, map[string]any{"conditions": []any{cond("Synced", "True"), cond("Ready", "True")}}))},
			"o3":   {resp: list(obj("es1-vm", t2, map[string]any{"conditions": []any{cond("Ready", "True")}}))},
		}
	}
	with := func(base map[string]stageInput, k string, v *stageInput) map[string]stageInput {
		if v == nil {
			delete(base, k)
		} else {
			base[k] = *v
		}
		return base
	}
	si := func(resp, err any) *stageInput { return &stageInput{resp: resp, err: err} }

	skExtras := map[string]any{"compositionName": "sk1", "compositionNamespace": archNS}
	skBase := func() map[string]stageInput {
		return map[string]stageInput{
			"arch": {resp: archConfigMap("sk1", sharedKindGraph())},
			"o0": {resp: list(
				obj("sk1-db", t0, map[string]any{"conditions": []any{cond("Available", "True")}}),
				obj("sk1-web-a", t1, map[string]any{"readyReplicas": 2}),
			)},
			"o1": {resp: list(obj("sk1-cfg", t0, nil), obj("unrelated", t0, nil))},
		}
	}
	sExtras := map[string]any{"name": "s1", "namespace": archNS}

	return []archCase{
		// event-site, the live 5-stage shape.
		{name: "all-ready", chart: "event-site", extras: esExtras, stages: esAllReady()},
		{name: "condition-not-ready", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o3",
			si(list(obj("es1-vm", t2, map[string]any{"conditions": []any{cond("Synced", "True"), cond("Ready", "False")}})), nil))},
		{name: "condition-absent-and-not-synced", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o2",
			si(list(obj("es1-form", t1, map[string]any{"conditions": []any{cond("Synced", "False")}})), nil))},
		{name: "field-form-falsy", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o0",
			si(list(obj("es1-web", t0, map[string]any{"readyReplicas": 0})), nil))},
		{name: "list-forbidden-object", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o2",
			si(nil, statusErr(403, "Forbidden", "forms", "", `forms.acmefeedback.example.io is forbidden: User "u" cannot list resource "forms"`)))},
		{name: "list-forbidden-string", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o3",
			si(nil, `computeinstances.compute.cnrm.cloud.google.com is forbidden: User "u" cannot list resource "computeinstances"`))},
		{name: "list-not-found", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o1",
			si(nil, statusErr(404, "NotFound", "services", "", `the server could not find the requested resource`)))},
		{name: "list-other-error", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o0",
			si(nil, "context deadline exceeded"))},
		{name: "missing-stage", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "o1", nil)},
		{name: "empty-lists", chart: "event-site", extras: esExtras, stages: func() map[string]stageInput {
			m := esAllReady()
			for _, k := range []string{"o0", "o1", "o2", "o3"} {
				m[k] = stageInput{resp: list()}
			}
			return m
		}()},
		{name: "arch-not-found", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "arch",
			si(nil, statusErr(404, "NotFound", "configmaps", "es1-architecture", `configmaps "es1-architecture" not found`)))},
		{name: "arch-forbidden", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "arch",
			si(nil, statusErr(403, "Forbidden", "configmaps", "es1-architecture", `configmaps "es1-architecture" is forbidden`)))},
		{name: "arch-other-error", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "arch", si(nil, "dial tcp: i/o timeout"))},
		{name: "arch-missing-stage", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "arch", nil)},
		{name: "arch-other-composition", chart: "event-site", extras: map[string]any{"compositionName": "other", "compositionNamespace": archNS}, stages: esAllReady()},
		{name: "arch-bad-graph", chart: "event-site", extras: esExtras, stages: with(esAllReady(), "arch", si(map[string]any{
			"metadata": map[string]any{"labels": map[string]any{"krateo.io/composition-name": "es1"}},
			"data":     map[string]any{"graph": "{not json"},
		}, nil))},
		{name: "no-extras", chart: "event-site", extras: nil, stages: esAllReady()},

		// shared-kind: two nodes behind one LIST, a lifecycle node, a missing instance.
		{name: "partial", chart: "shared-kind", extras: skExtras, stages: skBase()},
		{name: "shared-list-forbidden", chart: "shared-kind", extras: skExtras, stages: with(skBase(), "o0",
			si(nil, statusErr(403, "Forbidden", "deployments", "", `deployments.apps is forbidden: User "u" cannot list resource "deployments"`)))},
		{name: "shared-list-not-found-string", chart: "shared-kind", extras: skExtras, stages: with(skBase(), "o1",
			si(nil, `configmaps "sk1-cfg" not found`))},
		{name: "shared-empty-lists", chart: "shared-kind", extras: skExtras, stages: with(with(skBase(), "o0", si(list(), nil)), "o1", si(list(), nil))},
		{name: "shared-list-garbage-items", chart: "shared-kind", extras: skExtras, stages: with(skBase(), "o1",
			si(map[string]any{"items": []any{"str", 3, map[string]any{"metadata": map[string]any{}}, obj("sk1-cfg", t0, nil)}}, nil))},
		{name: "shared-list-no-items-key", chart: "shared-kind", extras: skExtras, stages: with(skBase(), "o1", si(map[string]any{"kind": "List"}, nil))},

		// single: name/namespace extras fallback, field readyWhen on .data.
		{name: "single-ready", chart: "single", extras: sExtras, stages: map[string]stageInput{
			"arch": {resp: archConfigMap("s1", singleGraph())},
			"o0":   {resp: list(map[string]any{"metadata": map[string]any{"name": "s1-only"}, "status": map[string]any{"ok": "yes"}})},
		}},
		{name: "single-empty", chart: "single", extras: sExtras, stages: map[string]stageInput{
			"arch": {resp: archConfigMap("s1", singleGraph())},
			"o0":   {resp: list()},
		}},
	}
}

// ---- tests --------------------------------------------------------------

// TestArchitectureV1_ByteParity is the module's contract: for every fixture,
// each stage output and the final spec.filter output of the module-based
// RESTAction are byte-identical to the inlined original's.
func TestArchitectureV1_ByteParity(t *testing.T) {
	charts := loadArchCharts(t)
	for _, c := range archCases() {
		t.Run(c.chart+"/"+c.name, func(t *testing.T) {
			ch := charts[c.chart]
			origStages, origOut := runPipeline(t, c, ch, func(s archStage) string { return s.Filter }, ch.SpecFilter)
			modStages, modOut := runPipeline(t, c, ch, func(s archStage) string { return moduleStageFilter(t, s) }, moduleSpecFilter(t, ch.Steps))
			for name, want := range origStages {
				if got := modStages[name]; got != want {
					t.Errorf("stage %s differs\n inlined: %s\n  module: %s", name, want, got)
				}
			}
			if len(modStages) != len(origStages) {
				t.Errorf("stage count differs: inlined %d, module %d", len(origStages), len(modStages))
			}
			if modOut != origOut {
				t.Errorf("spec.filter output differs\n inlined: %s\n  module: %s", origOut, modOut)
			}
		})
	}
}

func archState(t *testing.T, c archCase) map[string]any {
	t.Helper()
	ch := loadArchCharts(t)[c.chart]
	_, out := runPipeline(t, c, ch, func(s archStage) string { return moduleStageFilter(t, s) }, moduleSpecFilter(t, ch.Steps))
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("state is not an object: %v (%s)", err, out)
	}
	return m
}

func archNode(t *testing.T, state map[string]any, id string) map[string]any {
	t.Helper()
	nodes, _ := state["nodes"].([]any)
	for _, n := range nodes {
		if m, ok := n.(map[string]any); ok && m["id"] == id {
			return m
		}
	}
	t.Fatalf("node %q not in state %v", id, state)
	return nil
}

func archCaseByName(t *testing.T, name string) archCase {
	t.Helper()
	for _, c := range archCases() {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no case %q", name)
	return archCase{}
}

// TestArchitectureV1_ConditionReadyWhenResolves pins the escaping. readyWhen's
// condition form is recognised by condType's regex. The TS template literal
// doubles every jq escape; copied without halving, the regex never matches, the
// condition is read as a field path, and a Ready resource reports unsatisfied.
// Parsing alone cannot see that. This asserts the RESOLVED answer.
func TestArchitectureV1_ConditionReadyWhenResolves(t *testing.T) {
	st := archState(t, archCaseByName(t, "all-ready"))
	for _, id := range []string{"form", "computeinstance"} {
		n := archNode(t, st, id)
		inst := n["instances"].([]any)[0].(map[string]any)
		if inst["satisfied"] != true || inst["value"] != "True" || n["phase"] != "done" {
			t.Fatalf("condition-form readyWhen on Ready %s did not resolve: satisfied=%v value=%v phase=%v", id, inst["satisfied"], inst["value"], n["phase"])
		}
	}
	if st["allReady"] != true || st["state"] != "Running" {
		t.Fatalf("all-ready chart: allReady=%v state=%v", st["allReady"], st["state"])
	}

	st = archState(t, archCaseByName(t, "condition-not-ready"))
	n := archNode(t, st, "computeinstance")
	inst := n["instances"].([]any)[0].(map[string]any)
	if inst["satisfied"] != false || inst["value"] != "False" || n["phase"] != "waiting" {
		t.Fatalf("condition-form readyWhen on a NotReady object: satisfied=%v value=%v phase=%v", inst["satisfied"], inst["value"], n["phase"])
	}
	if exc, _ := n["exception"].(map[string]any); exc["label"] != "NotReady" {
		t.Fatalf("NotReady exception not raised: %v", n["exception"])
	}
	if st["allReady"] != false || st["level"] != float64(2) {
		t.Fatalf("not-ready chart: allReady=%v level=%v", st["allReady"], st["level"])
	}
}

// TestArchitectureV1_NameInCapture pins nameIn's escaping: a denied LIST is
// written as `"<name>" is forbidden: ...`, and nameIn must pull <name> back out
// so the instance reads unreadable rather than unavailable.
func TestArchitectureV1_NameInCapture(t *testing.T) {
	for _, name := range []string{"list-forbidden-object", "list-forbidden-string"} {
		st := archState(t, archCaseByName(t, name))
		id := map[string]string{"list-forbidden-object": "form", "list-forbidden-string": "computeinstance"}[name]
		inst := archNode(t, st, id)["instances"].([]any)[0].(map[string]any)
		if inst["read"] != "unreadable" {
			t.Fatalf("%s: %s instance read=%v, want unreadable", name, id, inst["read"])
		}
	}
	got := evalJQ(t, `include "`+archModule+`"; architecture_state`, norm(t, map[string]any{
		"arch":   map[string]any{"graph": eventSiteGraph()},
		"comp":   []any{map[string]any{"managed": []any{map[string]any{"apiVersion": "apps/v1", "name": "es1-web"}}}},
		"objErr": []any{`"es1-web" not found`},
	}))
	if !strings.Contains(got, `"read":"missing"`) {
		t.Fatalf(`nameIn did not capture the name from '"es1-web" not found': %s`, got)
	}
}

// TestArchitectureV1_HelpersDoNotLeak: the module exports exactly its four
// interface defs. The S11 helpers are nested inside architecture_state, so an
// includer's own truthy/condType neither collide with them nor change the
// module's answer, and the helpers are not callable from outside.
func TestArchitectureV1_HelpersDoNotLeak(t *testing.T) {
	charts := loadArchCharts(t)
	c := archCaseByName(t, "all-ready")
	ch := charts[c.chart]
	_, want := runPipeline(t, c, ch, func(s archStage) string { return s.Filter }, ch.SpecFilter)

	shadowed := fmt.Sprintf(`include "%s"; def truthy: "shadow"; def condType($rw): null; def errKind: "shadow";
(fold_steps(%s) | architecture_state) as $s | {s: $s, t: (1 | truthy), c: condType("x"), e: errKind}`, archModule, jqLit(t, ch.Steps))
	_, got := runPipeline(t, c, ch, func(s archStage) string { return moduleStageFilter(t, s) }, shadowed)
	if wantWrapped := `{"c":null,"e":"shadow","s":` + want + `,"t":"shadow"}`; got != wantWrapped {
		t.Fatalf("includer's own helpers changed the module's answer or were overridden\n want %s\n  got %s", wantWrapped, got)
	}

	for _, helper := range []string{"truthy", "condType(\"x\")", "readyBy(\"x\")", "readyValue(\"x\")", "errKind", "nameIn", "errName", "errs(.)", "exception"} {
		_, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{
			Query: `include "` + archModule + `"; ` + helper, Data: map[string]any{}, ModuleLoader: ModuleLoader(),
		})
		if err == nil {
			t.Errorf("helper %s is exported by %s; the interface is the four defs only", helper, archModule)
		}
	}
}

// TestArchitectureV1_Interface pins the four exported names and their arity.
func TestArchitectureV1_Interface(t *testing.T) {
	q, err := ModuleLoader().(interface {
		LoadModule(string) (*gojq.Query, error)
	}).LoadModule(archModule)
	if err != nil {
		t.Fatalf("load %s: %v", archModule, err)
	}
	var got []string
	for _, fd := range q.FuncDefs {
		got = append(got, fmt.Sprintf("%s/%d", fd.Name, len(fd.Args)))
	}
	want := []string{"arch_step/0", "list_step/2", "fold_steps/1", "architecture_state/0"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s exports %v, want exactly %v", archModule, got, want)
	}
	if len(q.Imports) != 0 {
		t.Fatalf("%s imports other modules: %v", archModule, q.Imports)
	}
}
