// Command corpusgen extracts the JQ corpus the #603 differential fuzz runs on
// from RENDERED portal charts (helm template output of
// krateo-platformops/portal), writing testdata/portal_corpus.json.
//
// Usage:
//
//	go run ./internal/cache/jqinterest/internal/corpusgen \
//	  -repo krateo-platformops/portal -commit <sha> -render "<how>" \
//	  -o internal/cache/jqinterest/testdata/portal_corpus.json rendered1.yaml ...
//
// Only RESTActions (spec.api stages, spec.filter) and widgets (apiRef,
// widgetDataTemplate, resourcesRefsTemplate) are kept; nothing else of the
// chart is copied.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

type Corpus struct {
	Source      Source       `json:"source"`
	RESTActions []RESTAction `json:"restactions"`
	Widgets     []Widget     `json:"widgets"`
}

type Source struct {
	Repo   string   `json:"repo"`
	Commit string   `json:"commit"`
	Render string   `json:"render"`
	Files  []string `json:"files"`
}

type UAF struct {
	Verb          string `json:"verb,omitempty"`
	Group         string `json:"group,omitempty"`
	Resource      string `json:"resource,omitempty"`
	ResourcesFrom string `json:"resourcesFrom,omitempty"`
	NamespaceFrom string `json:"namespaceFrom,omitempty"`
	NameFrom      string `json:"nameFrom,omitempty"`
}

type Stage struct {
	Name      string   `json:"name"`
	Path      string   `json:"path,omitempty"`
	Verb      string   `json:"verb,omitempty"`
	Payload   string   `json:"payload,omitempty"`
	Headers   []string `json:"headers,omitempty"`
	DependsOn string   `json:"dependsOn,omitempty"`
	Iterator  string   `json:"iterator,omitempty"`
	Filter    *string  `json:"filter,omitempty"`
	UAF       *UAF     `json:"uaf,omitempty"`
}

type RESTAction struct {
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Stages    []Stage `json:"stages"`
	Filter    *string `json:"filter,omitempty"`
}

type DataTemplate struct {
	ForPath    string `json:"forPath"`
	Expression string `json:"expression"`
}

type ResourcesRef struct {
	Iterator string            `json:"iterator,omitempty"`
	Template map[string]string `json:"template"`
}

type Widget struct {
	Kind          string         `json:"kind"`
	Name          string         `json:"name"`
	Namespace     string         `json:"namespace"`
	APIRef        string         `json:"apiRef,omitempty"` // namespace/name
	DataTemplates []DataTemplate `json:"widgetDataTemplate,omitempty"`
	ResourcesRefs []ResourcesRef `json:"resourcesRefsTemplate,omitempty"`
}

func main() {
	repo := flag.String("repo", "", "source repository")
	commit := flag.String("commit", "", "source commit")
	render := flag.String("render", "", "how the charts were rendered")
	out := flag.String("o", "", "output file")
	flag.Parse()
	c := Corpus{Source: Source{Repo: *repo, Commit: *commit, Render: *render}}
	for _, f := range flag.Args() {
		parts := strings.Split(f, "/")
		c.Source.Files = append(c.Source.Files, parts[len(parts)-1])
		data, err := os.ReadFile(f) // #nosec G304 -- developer tool: reads the rendered chart files it is given
		if err != nil {
			fail(err)
		}
		for _, doc := range bytes.Split(data, []byte("\n---")) {
			var m map[string]any
			if err := yaml.Unmarshal(doc, &m); err != nil || m == nil {
				continue
			}
			add(&c, m)
		}
	}
	sort.Slice(c.RESTActions, func(i, j int) bool { return c.RESTActions[i].Name < c.RESTActions[j].Name })
	sort.Slice(c.Widgets, func(i, j int) bool {
		return c.Widgets[i].Kind+"/"+c.Widgets[i].Name < c.Widgets[j].Kind+"/"+c.Widgets[j].Name
	})
	b, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, append(b, '\n'), 0o600); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "corpusgen: %d restactions, %d widgets\n", len(c.RESTActions), len(c.Widgets))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "corpusgen:", err)
	os.Exit(1)
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func obj(m map[string]any, k string) map[string]any {
	o, _ := m[k].(map[string]any)
	return o
}

func add(c *Corpus, m map[string]any) {
	kind := str(m, "kind")
	md := obj(m, "metadata")
	spec := obj(m, "spec")
	if md == nil || spec == nil {
		return
	}
	apiVersion := str(m, "apiVersion")
	if kind == "RESTAction" && strings.HasPrefix(apiVersion, "templates.krateo.io/") {
		ra := RESTAction{Name: str(md, "name"), Namespace: str(md, "namespace")}
		if f, ok := spec["filter"].(string); ok {
			ra.Filter = &f
		}
		apis, _ := spec["api"].([]any)
		for _, a := range apis {
			am, _ := a.(map[string]any)
			if am == nil {
				continue
			}
			st := Stage{Name: str(am, "name"), Path: str(am, "path"), Verb: str(am, "verb"), Payload: str(am, "payload")}
			if hs, ok := am["headers"].([]any); ok {
				for _, h := range hs {
					if hstr, ok := h.(string); ok {
						st.Headers = append(st.Headers, hstr)
					}
				}
			}
			if d := obj(am, "dependsOn"); d != nil {
				st.DependsOn = str(d, "name")
				st.Iterator = str(d, "iterator")
			}
			if f, ok := am["filter"].(string); ok {
				st.Filter = &f
			}
			if u := obj(am, "userAccessFilter"); u != nil {
				st.UAF = &UAF{
					Verb: str(u, "verb"), Group: str(u, "group"), Resource: str(u, "resource"),
					ResourcesFrom: str(u, "resourcesFrom"), NamespaceFrom: str(u, "namespaceFrom"),
					NameFrom: str(u, "nameFrom"),
				}
			}
			ra.Stages = append(ra.Stages, st)
		}
		c.RESTActions = append(c.RESTActions, ra)
		return
	}
	if !strings.HasPrefix(apiVersion, "widgets.templates.krateo.io/") {
		return
	}
	w := Widget{Kind: kind, Name: str(md, "name"), Namespace: str(md, "namespace")}
	if r := obj(spec, "apiRef"); r != nil {
		w.APIRef = str(r, "namespace") + "/" + str(r, "name")
	}
	if dts, ok := spec["widgetDataTemplate"].([]any); ok {
		for _, d := range dts {
			dm, _ := d.(map[string]any)
			if dm != nil {
				w.DataTemplates = append(w.DataTemplates, DataTemplate{ForPath: str(dm, "forPath"), Expression: str(dm, "expression")})
			}
		}
	}
	if rrs, ok := spec["resourcesRefsTemplate"].([]any); ok {
		for _, r := range rrs {
			rm, _ := r.(map[string]any)
			if rm == nil {
				continue
			}
			rr := ResourcesRef{Iterator: str(rm, "iterator"), Template: map[string]string{}}
			for k, v := range obj(rm, "template") {
				if s, ok := v.(string); ok {
					rr.Template[k] = s
				}
			}
			w.ResourcesRefs = append(w.ResourcesRefs, rr)
		}
	}
	if w.APIRef == "" { // no RESTAction: no list edge
		return
	}
	c.Widgets = append(c.Widgets, w)
}
