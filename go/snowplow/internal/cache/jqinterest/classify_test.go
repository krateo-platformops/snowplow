package jqinterest

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

var reLiteral = regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`)

// pathSkeleton renders a stage path with every jq part replaced by "{}":
// a template's string literals are kept, anything computed becomes "{}".
func pathSkeleton(p string) string {
	q, ok := MaybeQuery(p)
	if !ok {
		return p
	}
	lits := reLiteral.FindAllStringSubmatch(q, -1)
	var b strings.Builder
	for i, l := range lits {
		if i > 0 {
			b.WriteString("{}")
		}
		b.WriteString(l[1])
	}
	if len(lits) == 0 {
		return "{}"
	}
	if !strings.HasSuffix(strings.TrimSpace(q), `"`) {
		b.WriteString("{}")
	}
	return b.String()
}

// isListCall reports whether a stage path is a Kubernetes collection URL
// (the only calls that record a LIST dependency): /api/v1/<plural>,
// /api/v1/namespaces/<ns>/<plural>, /apis/<g>/<v>/<plural>,
// /apis/<g>/<v>/namespaces/<ns>/<plural>. Anything else (a named GET, a
// nested /call, an external endpoint) has no list edge.
func isListCall(path string) bool {
	s := pathSkeleton(path)
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	var seg []string
	for _, x := range strings.Split(s, "/") {
		if x != "" {
			seg = append(seg, x)
		}
	}
	var rest []string
	switch {
	case len(seg) >= 2 && seg[0] == "api":
		rest = seg[2:]
	case len(seg) >= 3 && seg[0] == "apis":
		rest = seg[3:]
	case len(seg) == 1 && seg[0] == "{}":
		// a wholly computed path: the corpus builds /apis/<g>/<v>/<plural>
		// this way; count it as a list call (over-counts, never hides one)
		return true
	default:
		return false
	}
	switch len(rest) {
	case 1:
		return true
	case 3:
		return rest[0] == "namespaces"
	}
	return false
}

// TestCorpus_EveryChainClassified analyses every chain of the portal corpus
// and requires each to get a verdict. It prints the verdict table over the
// LIST edges (the ones the narrowing applies to) and what blocks the rest.
func TestCorpus_EveryChainClassified(t *testing.T) {
	c, chains := loadCorpus(t)
	progs := corpusPrograms(c)
	unparsable := 0
	for _, p := range progs {
		if !parses(p) {
			unparsable++
		}
	}
	type tally struct{ all, everything, narrowed, member, total int }
	var list, other tally
	why := map[string]int{}
	for _, nc := range chains {
		var stage *Stage
		for i := range nc.chain.RA.Stages {
			if nc.chain.RA.Stages[i].Name == nc.chain.Tracked {
				stage = &nc.chain.RA.Stages[i]
			}
		}
		tl := &other
		if isListCall(stage.Path) {
			tl = &list
		}
		tl.total++
		in := Analyze(nc.chain, jqsupport.ModuleLoader())
		switch {
		case in.All:
			tl.all++
			if tl == &list {
				why[firstWords(in.Why)]++
			}
		case in.ReadsEverything():
			tl.everything++
		default:
			tl.narrowed++
		}
		if in.Membership != nil {
			tl.member++
		}
	}
	t.Logf("corpus %s@%s: %d RESTActions, %d widgets, %d distinct JQ programs (%d unparsable), %d chains",
		c.Source.Repo, c.Source.Commit[:12], len(c.RESTActions), len(c.Widgets), len(progs), unparsable, len(chains))
	t.Logf("LIST-edge chains: %d — all(unprovable)=%d reads-whole-item=%d narrowed-by-field=%d (membership-keyed=%d)",
		list.total, list.all, list.everything, list.narrowed, list.member)
	t.Logf("non-LIST stage chains (named GET, nested call, external; no list edge): %d", other.total)
	t.Logf("LIST edges unprovable because: %s", table(why))
	if list.all+list.everything+list.narrowed+other.all+other.everything+other.narrowed != len(chains) {
		t.Fatalf("unclassified chains")
	}
	if unparsable != 0 {
		t.Fatalf("%d corpus programs do not parse", unparsable)
	}
}

func TestIsListCall(t *testing.T) {
	for p, want := range map[string]bool{
		"/api/v1/pods":                                                   true,
		"/api/v1/namespaces/x/pods":                                      true,
		"/api/v1/namespaces/x/pods/p":                                    false,
		"/apis/core.krateo.io/v1alpha1/compositiondefinitions":           true,
		"/apis/g/v1/namespaces/ns/things":                                true,
		"/apis/g/v1/namespaces/ns/things/name":                           false,
		`${ "/apis/composition.krateo.io/" + .version + "/" + .plural }`: true,
		`${ "/apis/g/v1/namespaces/" + .ns + "/things/" + .name }`:       false,
		"/call?resource=restactions&name=x":                              false,
		"https://example.com/x":                                          false,
	} {
		if got := isListCall(p); got != want {
			t.Errorf("isListCall(%q) = %v, want %v (skeleton %q)", p, got, want, pathSkeleton(p))
		}
	}
}

func firstWords(s string) string {
	f := strings.Fields(s)
	if len(f) > 4 {
		f = f[:4]
	}
	return strings.Join(f, " ")
}

func table(m map[string]int) string {
	type kv struct {
		k string
		v int
	}
	var xs []kv
	for k, v := range m {
		xs = append(xs, kv{k, v})
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].v > xs[j].v || xs[i].v == xs[j].v && xs[i].k < xs[j].k })
	var b strings.Builder
	for _, x := range xs {
		fmt.Fprintf(&b, "\n    %5d  %s", x.v, x.k)
	}
	return b.String()
}
