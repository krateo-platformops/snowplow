package jqinterest

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/itchyny/gojq"
)

// The portal JQ corpus: testdata/portal_corpus.json, extracted by
// internal/corpusgen from `helm template` of krateo-platformops/portal (the
// provenance is inside the file). One chain per (RESTAction, stage) for the
// RESTAction cell, and per (widget, stage of its apiRef) for the widget cell.

type corpusFile struct {
	Source struct {
		Repo   string   `json:"repo"`
		Commit string   `json:"commit"`
		Render string   `json:"render"`
		Files  []string `json:"files"`
	} `json:"source"`
	RESTActions []struct {
		Name      string  `json:"name"`
		Namespace string  `json:"namespace"`
		Filter    *string `json:"filter"`
		Stages    []struct {
			Name      string   `json:"name"`
			Path      string   `json:"path"`
			Verb      string   `json:"verb"`
			Payload   string   `json:"payload"`
			Headers   []string `json:"headers"`
			DependsOn string   `json:"dependsOn"`
			Iterator  string   `json:"iterator"`
			Filter    *string  `json:"filter"`
			UAF       *struct {
				Verb          string `json:"verb"`
				ResourcesFrom string `json:"resourcesFrom"`
				NamespaceFrom string `json:"namespaceFrom"`
				NameFrom      string `json:"nameFrom"`
			} `json:"uaf"`
		} `json:"stages"`
	} `json:"restactions"`
	Widgets []struct {
		Kind          string `json:"kind"`
		Name          string `json:"name"`
		Namespace     string `json:"namespace"`
		APIRef        string `json:"apiRef"`
		DataTemplates []struct {
			ForPath    string `json:"forPath"`
			Expression string `json:"expression"`
		} `json:"widgetDataTemplate"`
		ResourcesRefs []struct {
			Iterator string            `json:"iterator"`
			Template map[string]string `json:"template"`
		} `json:"resourcesRefsTemplate"`
	} `json:"widgets"`
}

type namedChain struct {
	name  string
	chain Chain
}

func loadCorpus(t testing.TB) (*corpusFile, []namedChain) {
	t.Helper()
	b, err := os.ReadFile("testdata/portal_corpus.json")
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var c corpusFile
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatalf("parse corpus: %v", err)
	}
	ras := map[string]RESTAction{}
	var out []namedChain
	for _, ra := range c.RESTActions {
		r := RESTAction{Filter: ra.Filter}
		for _, st := range ra.Stages {
			s := Stage{Name: st.Name, Path: st.Path, Payload: st.Payload, Headers: st.Headers,
				DependsOn: st.DependsOn, Iterator: st.Iterator, Filter: st.Filter}
			if st.UAF != nil {
				s.UAF = &UAF{NamespaceFrom: st.UAF.NamespaceFrom, NameFrom: st.UAF.NameFrom,
					ResourcesFrom: st.UAF.ResourcesFrom, NameSpecificVerb: nameSpecific(st.UAF.Verb)}
			}
			r.Stages = append(r.Stages, s)
		}
		ras[ra.Namespace+"/"+ra.Name] = r
		for _, st := range r.Stages {
			out = append(out, namedChain{name: "ra:" + ra.Name + "#" + st.Name, chain: Chain{RA: r, Tracked: st.Name}})
		}
	}
	for _, w := range c.Widgets {
		r, ok := ras[w.APIRef]
		if !ok {
			continue
		}
		wd := &Widget{}
		for _, d := range w.DataTemplates {
			wd.DataTemplates = append(wd.DataTemplates, d.Expression)
		}
		for _, rr := range w.ResourcesRefs {
			keys := make([]string, 0, len(rr.Template))
			for k := range rr.Template {
				if k != "verb" {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			x := ResourcesRef{Iterator: rr.Iterator}
			for _, k := range keys {
				x.Fields = append(x.Fields, rr.Template[k])
			}
			wd.ResourcesRefs = append(wd.ResourcesRefs, x)
		}
		for _, st := range r.Stages {
			out = append(out, namedChain{name: "w:" + w.Kind + "/" + w.Name + "->" + w.APIRef + "#" + st.Name,
				chain: Chain{RA: r, Tracked: st.Name, Widget: wd}})
		}
	}
	return &c, out
}

func nameSpecific(verb string) bool {
	switch verb {
	case "get", "update", "patch", "delete":
		return true
	}
	return false
}

// corpusPrograms lists every distinct JQ program in the corpus.
func corpusPrograms(c *corpusFile) []string {
	set := map[string]bool{}
	add := func(s string, template bool) {
		if s == "" {
			return
		}
		if template {
			q, ok := MaybeQuery(s)
			if !ok {
				return
			}
			s = q
		}
		set[s] = true
	}
	for _, ra := range c.RESTActions {
		if ra.Filter != nil {
			add(*ra.Filter, false)
		}
		for _, st := range ra.Stages {
			if st.Filter != nil {
				add(*st.Filter, false)
			}
			add(st.Iterator, false)
			add(st.Path, true)
			add(st.Payload, true)
			for _, h := range st.Headers {
				add(h, true)
			}
			if st.UAF != nil {
				add(st.UAF.NamespaceFrom, false)
				add(st.UAF.NameFrom, false)
				add(st.UAF.ResourcesFrom, false)
			}
		}
	}
	for _, w := range c.Widgets {
		for _, d := range w.DataTemplates {
			add(d.Expression, true)
		}
		for _, rr := range w.ResourcesRefs {
			add(rr.Iterator, true)
			for _, v := range rr.Template {
				add(v, true)
			}
		}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func parses(p string) bool {
	_, err := gojq.Parse(p)
	return err == nil
}
