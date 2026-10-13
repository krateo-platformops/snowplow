package jqinterest

// #623: the analyzer over the built-in module krateo/architecture/v1. The
// module replaces jq the generator used to inline, so moving that jq behind an
// include must not change what the analyzer concludes: for every LIST edge of
// the event-site status RESTAction the Interest of the module-based RESTAction
// equals the Interest of the inlined one. The analyzer follows the include
// through the embedded loader (importModule), so the module is analysed, not
// treated as an opaque import.

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

func eventSiteRAs623(t *testing.T) (inlined, module RESTAction, lists []string) {
	t.Helper()
	raw, err := os.ReadFile("../../support/jq/testdata/architecture/reference_filters.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Charts []struct {
			Name   string
			Steps  []map[string]any
			Stages []struct {
				Name       string
				APIVersion string `json:"apiVersion"`
				Filter     string
			}
			SpecFilter string `json:"specFilter"`
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Charts {
		if c.Name != "event-site" {
			continue
		}
		steps, _ := json.Marshal(c.Steps)
		inlined.Filter = &c.SpecFilter
		mf := `include "krateo/architecture/v1"; fold_steps(` + string(steps) + `) | architecture_state`
		module.Filter = &mf
		for _, s := range c.Stages {
			path := `${ "/apis/x/namespaces/" + .compositionNamespace + "/` + s.Name + `" }`
			inF := s.Filter
			modF := `include "krateo/architecture/v1"; arch_step`
			if s.Name != "arch" {
				modF = fmt.Sprintf(`include "krateo/architecture/v1"; list_step(%q; %q)`, s.Name, s.APIVersion)
				lists = append(lists, s.Name)
			}
			inlined.Stages = append(inlined.Stages, Stage{Name: s.Name, Path: path, Filter: &inF})
			module.Stages = append(module.Stages, Stage{Name: s.Name, Path: path, Filter: &modF})
		}
		return inlined, module, lists
	}
	t.Fatal("event-site chart missing")
	return
}

func TestArchitectureModule623_AnalyzerConclusionUnchanged(t *testing.T) {
	inlined, module, lists := eventSiteRAs623(t)
	if len(lists) != 4 {
		t.Fatalf("want 4 LIST edges, got %v", lists)
	}
	loader := jqsupport.ModuleLoader()
	for _, tracked := range lists {
		in := Analyze(Chain{RA: inlined, Tracked: tracked}, loader)
		mod := Analyze(Chain{RA: module, Tracked: tracked}, loader)
		t.Logf("edge %s: module All=%v Why=%q Reads=%v Membership=%v | inlined All=%v Why=%q", tracked, mod.All, mod.Why, mod.Reads, mod.Membership, in.All, in.Why)
		if in.All != mod.All || !reflect.DeepEqual(in.Reads, mod.Reads) || !reflect.DeepEqual(in.Membership, mod.Membership) {
			t.Fatalf("edge %s: the module changed the analyzer's conclusion\n inlined: %+v\n  module: %+v", tracked, in, mod)
		}
	}
}
