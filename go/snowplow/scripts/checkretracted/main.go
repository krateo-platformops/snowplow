// Command checkretracted guards the repo root's retracted-versions.yaml: it is
// well formed, names only this repo's charts, and points at real releases.
//
// installer's CI reads that file and refuses a pin on a listed version. A
// malformed entry would not fail there: it would match nothing, and the pin would
// go through. So the file is checked here, where it is written, as
// krateo-platformops/portal (scripts/test-retracted-versions.py) and frontend
// (crdgen/retracted_test.go) check their own:
//
//   - the top level is `retracted:` and nothing else, and the list is not empty (a
//     parse that reads nothing must not pass);
//   - each entry has exactly version, charts, use and reason; version and use are
//     plain semver and differ; the reason is not empty;
//   - every chart is one this repo publishes: a `name:` in helm/*/Chart.yaml, read
//     from the tree, not a list kept here. A component name (snowplow-crd), or
//     another repo's chart, would match nothing in installer;
//   - (online, the default) the retracted version and `use` are both PUBLISHED for
//     every chart named, read anonymously from the chart registry. Retracting a
//     version nobody can pin is noise, and a `use` that does not exist sends the
//     pinning session to a dead end.
//
// Usage: checkretracted [--offline] <repo root>. --self-test runs the controls
// alone: an unknown chart, a component name, a bad semver, use == version, a
// missing key and an empty list must each be refused, and (online) an unpublished
// version must be reported. A normal run runs the controls first too: a guard that
// refuses nothing must not pass.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"sigs.k8s.io/yaml"
)

// chartRepo is where this repo's charts are published (the release workflow's
// push target), as a registry host and repository prefix.
const (
	registryHost = "ghcr.io"
	chartRepo    = "krateo-platformops/charts"
)

var semver = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

var keys = []string{"charts", "reason", "use", "version"}

// publishedCharts reads every helm/*/Chart.yaml `name:` under root.
func publishedCharts(root string) (map[string]bool, error) {
	files, err := filepath.Glob(filepath.Join(root, "helm", "*", "Chart.yaml"))
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var c struct {
			Name string `json:"name"`
		}
		if err := yaml.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if c.Name != "" {
			out[c.Name] = true
		}
	}
	return out, nil
}

type entry struct {
	Version string
	Charts  []string
	Use     string
}

// check returns the structural failures of a parsed file, and its entries.
func check(doc any, known map[string]bool) ([]string, []entry) {
	top, ok := doc.(map[string]any)
	if !ok || len(top) != 1 || top["retracted"] == nil {
		return []string{"the file must hold exactly one key, `retracted`"}, nil
	}
	list, ok := top["retracted"].([]any)
	if !ok || len(list) == 0 {
		return []string{"no entries: a parse that reads nothing must not pass"}, nil
	}
	var out []string
	var entries []entry
	for _, raw := range list {
		e, ok := raw.(map[string]any)
		if !ok {
			out = append(out, fmt.Sprintf("%v: an entry is a map of %v", raw, keys))
			continue
		}
		label := fmt.Sprintf("%v", e["version"])
		got := make([]string, 0, len(e))
		for k := range e {
			got = append(got, k)
		}
		sort.Strings(got)
		if fmt.Sprint(got) != fmt.Sprint(keys) {
			out = append(out, fmt.Sprintf("%s: an entry has exactly %v, got %v", label, keys, got))
			continue
		}
		version, vok := e["version"].(string)
		use, uok := e["use"].(string)
		if !vok || !uok || !semver.MatchString(version) || !semver.MatchString(use) {
			out = append(out, fmt.Sprintf("%s: version and use must be plain semver strings, got %v and %v", label, e["version"], e["use"]))
		}
		if vok && uok && version == use {
			out = append(out, fmt.Sprintf("%s: use names the retracted version itself", label))
		}
		if r, ok := e["reason"].(string); !ok || len(regexp.MustCompile(`\S`).FindString(r)) == 0 {
			out = append(out, fmt.Sprintf("%s: a retraction needs a reason", label))
		}
		charts, ok := e["charts"].([]any)
		if !ok || len(charts) == 0 {
			out = append(out, fmt.Sprintf("%s: names no chart", label))
			continue
		}
		var names []string
		for _, c := range charts {
			name, _ := c.(string)
			if !known[name] {
				out = append(out, fmt.Sprintf("%s: %v is not a chart this repo publishes (a component name, or another repo's chart, matches nothing in installer)", label, c))
				continue
			}
			names = append(names, name)
		}
		entries = append(entries, entry{Version: version, Charts: names, Use: use})
	}
	return out, entries
}

var client = &http.Client{Timeout: 25 * time.Second}

// published reports whether chart:version resolves in the registry,
// anonymously (as installer and the cluster pull it). A registry that cannot
// be asked is an error, never "published".
func published(chart, version string) (bool, error) {
	repo := chartRepo + "/" + chart
	resp, err := client.Get("https://" + registryHost + "/token?scope=" + url.QueryEscape("repository:"+repo+":pull"))
	if err != nil {
		return false, err
	}
	var tok struct {
		Token string `json:"token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&tok)
	_ = resp.Body.Close()
	if err != nil {
		return false, fmt.Errorf("token for %s: %w", repo, err)
	}
	req, err := http.NewRequest(http.MethodHead, "https://"+registryHost+"/v2/"+repo+"/manifests/"+version, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+tok.Token)
	req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")
	resp, err = client.Do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("%s:%s: registry answered %d", repo, version, resp.StatusCode)
}

// controls proves the checks can refuse.
func controls(known map[string]bool, online bool) []string {
	if !known["snowplow"] || !known["snowplow-crds"] {
		return []string{"control: snowplow / snowplow-crds are not among the discovered charts, so the controls mean nothing"}
	}
	good := func() map[string]any {
		return map[string]any{"version": "1.12.71", "charts": []any{"snowplow"}, "use": "1.12.70", "reason": "x"}
	}
	with := func(k string, v any) map[string]any {
		e := good()
		if v == nil {
			delete(e, k)
		} else {
			e[k] = v
		}
		return e
	}
	cases := map[string]map[string]any{
		"an unknown chart":                         with("charts", []any{"not-a-chart"}),
		"a component name (snowplow-crd)":          with("charts", []any{"snowplow-crd"}),
		"a bad semver":                             with("version", "v1.12.71"),
		"use == version":                           with("use", "1.12.71"),
		"a missing reason key":                     with("reason", nil),
		"an empty reason":                          with("reason", "  "),
		"an extra key":                             with("note", "x"),
		"a non-string use (YAML number 1.12 form)": with("use", 1.12),
	}
	var problems []string
	if f, _ := check(map[string]any{"retracted": []any{good()}}, known); len(f) > 0 {
		problems = append(problems, fmt.Sprintf("control: a valid entry was refused: %v", f))
	}
	if f, _ := check(map[string]any{"retracted": []any{}}, known); len(f) == 0 {
		problems = append(problems, "control: an empty list was accepted")
	}
	if f, _ := check(map[string]any{"other": 1}, known); len(f) == 0 {
		problems = append(problems, "control: a file without `retracted` was accepted")
	}
	names := make([]string, 0, len(cases))
	for n := range cases {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if f, _ := check(map[string]any{"retracted": []any{cases[n]}}, known); len(f) == 0 {
			problems = append(problems, "control: "+n+" was NOT refused")
		}
	}
	if online {
		// The registry check must be able to say no: a version far beyond any
		// release is not published.
		if ok, err := published("snowplow", "0.0.1"); err != nil || ok {
			problems = append(problems, fmt.Sprintf("control: snowplow 0.0.1 must read as not published (ok=%v err=%v)", ok, err))
		}
	}
	return problems
}

func main() {
	args := os.Args[1:]
	offline, selfTest := false, false
	root := ""
	for _, a := range args {
		switch a {
		case "--offline":
			offline = true
		case "--self-test":
			selfTest = true
		default:
			root = a
		}
	}
	if root == "" {
		fmt.Fprintln(os.Stderr, "usage: checkretracted [--offline] [--self-test] <repo root>")
		os.Exit(2)
	}
	known, err := publishedCharts(root)
	if err != nil {
		fmt.Printf("[FAIL] reading helm/*/Chart.yaml: %v\n", err)
		os.Exit(1)
	}
	if problems := controls(known, !offline); len(problems) > 0 {
		for _, p := range problems {
			fmt.Println("[FAIL] " + p)
		}
		os.Exit(1)
	}
	fmt.Println("[PASS] controls: an unknown chart, a component name, a bad semver, use == version, a missing or empty reason, an extra key, a non-string use, an empty list and a file without `retracted` are refused" +
		map[bool]string{true: "", false: "; an unpublished version reads as unpublished"}[offline])
	if selfTest {
		return
	}
	b, err := os.ReadFile(filepath.Join(root, "retracted-versions.yaml"))
	if err != nil {
		fmt.Printf("[FAIL] %v\n", err)
		os.Exit(1)
	}
	var doc any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		fmt.Printf("[FAIL] retracted-versions.yaml does not parse: %v\n", err)
		os.Exit(1)
	}
	failures, entries := check(doc, known)
	if len(failures) == 0 && !offline {
		for _, e := range entries {
			for _, c := range e.Charts {
				for _, v := range []struct{ ver, role string }{{e.Version, "retracted version"}, {e.Use, "use"}} {
					ok, err := published(c, v.ver)
					switch {
					case err != nil:
						failures = append(failures, fmt.Sprintf("%s: %s %s (%s): %v", e.Version, c, v.ver, v.role, err))
					case !ok:
						failures = append(failures, fmt.Sprintf("%s: %s %s (%s) is not published", e.Version, c, v.ver, v.role))
					}
				}
			}
		}
	}
	for _, f := range failures {
		fmt.Println("[FAIL] " + f)
	}
	if len(failures) > 0 {
		os.Exit(1)
	}
	fmt.Printf("[PASS] retracted-versions.yaml: %d entries, charts from %d published charts%s\n", len(entries), len(known),
		map[bool]string{true: "", false: ", every version and use published"}[offline])
}
