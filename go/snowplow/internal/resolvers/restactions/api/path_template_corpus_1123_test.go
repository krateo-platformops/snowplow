package api

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/pathtemplate"
)

// #1123 corpus arm. Every RESTAction stage path in
//   - portal main (testdata/stage_path_corpus_1123.json: helm template of every
//     chart at portal da5f590c, plus every RESTAction in snowplow's yaml
//     testdata), and
//   - the portal suite deployed on krateo-057 (testdata/
//     portal_deployed_1.8.84_corpus.json),
// is rendered through the dial path (createRequestOption) and through the
// pre-#1123 render (evalJQE), with realistic values. A stage renders
// identically, or it is listed in corpusChanges1123 with the reason it is
// refused and the declared form that renders what it rendered before. Every
// stage is judged with the own-cluster guard (apiserver=true), the stricter of
// the two verdicts (#1112 D2).

// corpusInput1123 is realistic data for every field a corpus path reads: the
// shapes these fields hold on a live cluster (an apiVersion is group/version,
// a path is a path).
func corpusInput1123() map[string]any {
	return map[string]any{
		"namespace": "demo-system", "name": "demo-app", "ns": "krateo-system", "nsName": "demo-system",
		"cdNamespace": "krateo-system", "n": "demo-app", "oas": "petstore-oas", "claim": "review-abc",
		"apiVersion": "composition.krateo.io/v1-2-0", "group": "composition.krateo.io", "version": "v1-2-0",
		"resource": "fireworksapps", "plural": "fireworksapps", "gv": "apps/v1",
		"path":     "/apis/composition.krateo.io/v1-2-0/namespaces/demo-system/fireworksapps/demo-app",
		"_getpath": "/api/v1/namespaces/demo-system/configmaps/demo-app-values",
		"base":     "/apis/apps/v1", "discovery": "/apis/apps/v1",
		"range": "7d", "serviceName": "snowplow", "limit": "40",
		"id": 42, "email": "alice@example.com", "age": 30, "uid": "AA-BB-CC",
	}
}

// corpusInputOverride1123 is the input for a stage whose field holds a
// different shape than the shared input's.
var corpusInputOverride1123 = map[string]map[string]any{
	// The iterator builds the sse-proxy path with a query.
	"composition-events/getEvents": {"path": "/events?limit=200&composition_id=0f8fad5b-d9cb-469f-a165-70867728950e"},
}

type corpusChange1123 struct {
	reason   string // pathtemplate refusal reason
	declared string // the declared form, rendering what the stage rendered before
	why      string
	// declaredInput is the declared form's input when the declaration moves a
	// part of the path out of the iterator (nil: the stage's own input).
	declaredInput any
}

func chg(reason, declared, why string) corpusChange1123 {
	return corpusChange1123{reason: reason, declared: declared, why: why}
}

// splitGV is the declared form for an apiVersion: two single segments, so the
// value cannot add a third (the #1116 blueprint-detail split).
const splitGV = `(.apiVersion | split("/")) as [$g, $v] | `

// corpusChanges1123 is every corpus stage whose render changes, keyed
// "<ra>/<stage>". Each one splices a value that holds several segments; none
// renders a different path — each is refused until it declares the value.
var corpusChanges1123 = map[string]corpusChange1123{
	"blueprint-detail/comps": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/" + .resource }`,
		"apiVersion is group/version; this is the #1116 split (a userAccessFilter stage: the ServiceAccount dials it)"),
	"controller-builder-deliverables/installs": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/" + .resource }`, "apiVersion is group/version"),
	"controller-builder-deliverables/configs": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/" + .plural }`, "apiVersion is group/version"),
	"composition-architecture/comp": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/namespaces/" + .namespace + "/" + .resource + "/" + .name }`,
		"apiVersion is group/version"),
	"blueprint-palette/running": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/" + .plural }`, "apiVersion is group/version"),
	"review-proposal/claim": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/namespaces/default/" + .resource + "/" + .claim }`,
		"apiVersion is group/version"),
	// The deployed 1.8.84 suite wrote the claim's namespace as a literal.
	"deployed:review-proposal/claim": chg(pathtemplate.ReasonValueSpansSegments,
		`${ `+splitGV+`"/apis/" + $g + "/" + $v + "/namespaces/krateo-system/" + .resource + "/" + .claim }`,
		"apiVersion is group/version"),
	"composition-events/apiresources": chg(pathtemplate.ReasonValueSpansSegments,
		`${ (.gv | split("/")) as [$g, $v] | "/apis/" + $g + "/" + $v }`, "gv is group/version"),
	"composition-architecture/objs": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.path) }`, "the iterator yields a whole path (a composition's status.managed[].path)"),
	"composition-resources/resources": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(._getpath) }`, "the iterator yields a whole path"),
	"resource-detail/disco": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.base) }`, "the iterator yields a whole discovery path"),
	"resource-detail/object": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.path) }`, "the iterator yields a whole path built from the route's extras"),
	"platform-reviews/kinds": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.discovery) }`, "the iterator yields a whole discovery path"),
	"review-proposal/kinds": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.discovery) }`, "the iterator yields a whole discovery path"),
	"review-run/kinds": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.discovery) }`, "the iterator yields a whole discovery path"),
	"ticket-detail/object": chg(pathtemplate.ReasonValueSpansSegments,
		`${ segments(.path) }`, "the iterator yields a whole path built from the ticket's spec.objectRef (requester-written)"),
	"composition-events/getEvents": {reason: pathtemplate.ReasonValueSpansSegments,
		// The query moves into the template; the iterator yields {uid}.
		declared:      `${ "/events?limit=200" + (if (.uid // "") != "" then "&composition_id=" + .uid else "" end) }`,
		why:           "the iterator yields a whole path WITH a query; a query is declared in the template",
		declaredInput: map[string]any{"uid": "0f8fad5b-d9cb-469f-a165-70867728950e"}},
}

const deployedSource = "portal deployed 1.8.84 (krateo-057)"

type corpusStage1123 struct {
	source, ra, stage, path string
}

func loadCorpus1123(t *testing.T) []corpusStage1123 {
	t.Helper()
	var out []corpusStage1123
	{
		raw, err := os.ReadFile("testdata/stage_path_corpus_1123.json")
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Stages []struct {
				Source, RA, Stage, Path string
			} `json:"stages"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, s := range doc.Stages {
			out = append(out, corpusStage1123{s.Source, s.RA, s.Stage, s.Path})
		}
	}
	{
		raw, err := os.ReadFile("testdata/portal_deployed_1.8.84_corpus.json")
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			RestActions []struct {
				Name   string `json:"name"`
				Stages []struct {
					Name string `json:"name"`
					Path string `json:"path"`
				} `json:"stages"`
			} `json:"restactions"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, ra := range doc.RestActions {
			for _, s := range ra.Stages {
				out = append(out, corpusStage1123{deployedSource, ra.Name, s.Name, s.Path})
			}
		}
	}
	return out
}

// corpusRenderBefore is the pre-#1123 render with the first input it accepts:
// the shared object, or a scalar for an iterator over a list of names.
func corpusRenderBefore(key, path string) (string, any, bool) {
	inputs := []any{corpusInput1123(), "kube-system"}
	if o, ok := corpusInputOverride1123[key]; ok {
		inputs = []any{o}
	}
	for _, in := range inputs {
		if p, err := evalJQE(path, in); err == nil {
			return p, in, true
		}
	}
	return "", nil, false
}

func TestIssue1123_CorpusRendersIdenticallyOrIsListed(t *testing.T) {
	stages := loadCorpus1123(t)
	if len(stages) < 400 {
		t.Fatalf("corpus has %d stages; the testdata did not load", len(stages))
	}
	seenChange := map[string]bool{}
	templated, identical, changed := 0, 0, 0
	for _, st := range stages {
		key := st.ra + "/" + st.stage
		before, in, ok := corpusRenderBefore(key, st.path)
		if !ok {
			t.Errorf("%s %s: the pre-#1123 render fails on every input: fix the corpus input", st.source, key)
			continue
		}
		if strings.Contains(st.path, "${") {
			templated++
		}
		opt, valid, sr := createRequestOption(&templates.API{Name: st.stage, Path: st.path}, in, true)
		if strings.HasPrefix(st.source, deployedSource) {
			if _, ok := corpusChanges1123["deployed:"+key]; ok {
				key = "deployed:" + key
			}
		}
		ch, listed := corpusChanges1123[key]
		if !listed {
			if !valid || opt.Path != before {
				t.Errorf("%s %s: render changed and is not listed\n path   %s\n before %q\n after  %q (valid=%v %s)",
					st.source, key, st.path, before, opt.Path, valid, sr.detail)
				continue
			}
			identical++
			continue
		}
		seenChange[key] = true
		changed++
		if valid {
			t.Errorf("%s %s is listed as changed but still renders %q", st.source, key, opt.Path)
			continue
		}
		if _, err := pathtemplate.Render(t.Context(), st.path, in); !isRefusedFor(err, ch.reason) {
			t.Errorf("%s %s: refused for %v, listed as %s", st.source, key, err, ch.reason)
		}
		// The declared form renders exactly what the stage rendered before.
		din := in
		if ch.declaredInput != nil {
			din = ch.declaredInput
		}
		dopt, dvalid, dsr := createRequestOption(&templates.API{Name: st.stage, Path: ch.declared}, din, true)
		if !dvalid || dopt.Path != before {
			t.Errorf("%s %s: declared form %s\n renders %q (valid=%v %s)\n before  %q", st.source, key, ch.declared, dopt.Path, dvalid, dsr.detail, before)
		}
	}
	var stale []string
	for k := range corpusChanges1123 {
		if !seenChange[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("corpusChanges1123 lists stages the corpus no longer has: %v", stale)
	}
	if identical+changed != len(stages) {
		t.Errorf("%d identical + %d changed != %d stages", identical, changed, len(stages))
	}
	t.Logf("corpus: %d stages (%d templated); %d render identically, %d are refused until declared (%d distinct listed changes)",
		len(stages), templated, identical, changed, len(corpusChanges1123))
}

func isRefusedFor(err error, reason string) bool {
	r, ok := err.(*pathtemplate.RefusedError)
	return ok && r.Reason == reason
}
