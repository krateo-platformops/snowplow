package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"sigs.k8s.io/yaml"
)

// #1185 — a value that leaves a path segment empty is refused with InvalidPath
// 400, on every verb, on a named endpoint and on this cluster alike. Every
// "nothing was dialled" below is a count at the server, with a present value
// through the same stage as its control: a zero means the refusal, not a harness
// that cannot dial.

// target1185 is one kind of dial: a stage with a named endpointRef, or one with
// none, which dials the requester's own cluster.
type target1185 struct {
	name  string
	stage func(verb, path string) *templates.API
	// first, middle and last put the value at that position of a path of the
	// shape this target can serve; present is a value that makes it a real path.
	first, middle, last string
	present             map[string]string
}

var targets1185 = []target1185{
	{
		name: "named endpoint",
		stage: func(verb, path string) *templates.API {
			return &templates.API{Name: "s", Verb: ptr.To(verb), Path: path, ErrorKey: ptr.To("sErr"),
				EndpointRef: &templates.Reference{Name: x1106Ref, Namespace: x1106NS}}
		},
		first:   `${ "/" + .x + "/tickets/42" }`,
		middle:  `${ "/v2/tickets/" + .x + "/comments" }`,
		last:    `${ "/v2/tickets/" + .x }`,
		present: map[string]string{"first": "v2", "middle": "42", "last": "42"},
	},
	{
		name: "own cluster",
		stage: func(verb, path string) *templates.API {
			return &templates.API{Name: "s", Verb: ptr.To(verb), Path: path, ErrorKey: ptr.To("sErr")}
		},
		first:   `${ "/" + .x + "/v1/namespaces/demo/configmaps/cm" }`,
		middle:  `${ "/api/v1/namespaces/" + .x + "/configmaps/cm" }`,
		last:    `${ "/api/v1/namespaces/demo/configmaps/" + .x }`,
		present: map[string]string{"first": "api", "middle": "demo", "last": "cm"},
	},
}

func (c target1185) positions() map[string]string {
	return map[string]string{"first": c.first, "middle": c.middle, "last": c.last}
}

var verbs1185 = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// server1185 is one fake host behind both dials, counting every request.
func server1185(t *testing.T) *x1106Srv {
	t.Helper()
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{x1106Ref: x1106Secret(api.srv.URL)})
	return api
}

func TestIssue1185_EmptyValueIsRefusedOnEveryVerbAndDial(t *testing.T) {
	values := map[string]map[string]any{
		"empty":  {"x": ""},
		"null":   {"x": nil},
		"absent": {},
	}
	api := server1185(t)
	for _, target := range targets1185 {
		for pos, tmpl := range target.positions() {
			for _, verb := range verbs1185 {
				t.Run(fmt.Sprintf("%s/%s/%s", target.name, pos, verb), func(t *testing.T) {
					// The control: the same stage with the value there is dialled, once.
					dialled := len(api.all())
					o := rr16Resolve(t, workclass.None, map[string]any{"x": target.present[pos]}, target.stage(verb, tmpl))
					if got := len(api.all()) - dialled; got != 1 {
						t.Fatalf("control: the stage with its value was dialled %d time(s), want 1; outcomes %s", got, o.outcomes)
					}

					for name, extras := range values {
						dialled := len(api.all())
						skipped := MalformedDialSkippedByReason(reasonEmptyInterp)
						o := rr16Resolve(t, workclass.None, extras, target.stage(verb, tmpl))
						if hits := api.all()[dialled:]; len(hits) != 0 {
							t.Fatalf("%s: dialled %+v", name, hits)
						}
						x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
						if o.failure == nil || o.failure.Stage != "s" || o.failure.Code != http.StatusBadRequest {
							t.Fatalf("%s: StageFailure = %+v, want the stage's 400", name, o.failure)
						}
						if o.sinkN == 0 {
							t.Fatalf("%s: the stage-error sink was not bumped: a Put would store the body around the missing call", name)
						}
						if _, set := o.dict["s"]; set {
							t.Fatalf("%s: the refused stage wrote its key: %v", name, o.dict["s"])
						}
						// One count per refusal, in the #288 class (inspect reads it).
						if got := MalformedDialSkippedByReason(reasonEmptyInterp) - skipped; got != 1 {
							t.Fatalf("%s: empty_interp moved by %d, want 1", name, got)
						}
					}
				})
			}
		}
	}
}

// A refused read is every other InvalidPath: it fails the stage on a customer
// read and on a prewarm, nothing around it may be stored, and continueOnError
// keeps the resolve going with the failure on record.
func TestIssue1185_ARefusedReadFailsItsStageLikeEveryOtherInvalidPath(t *testing.T) {
	api := server1185(t)
	for _, target := range targets1185 {
		for _, class := range []workclass.Class{workclass.None, workclass.Prewarm} {
			t.Run(fmt.Sprintf("%s/class=%v", target.name, class), func(t *testing.T) {
				dialled := len(api.all())
				o := rr16Resolve(t, class, map[string]any{}, target.stage(http.MethodGet, target.last))
				x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
				rr16ErrRecord(t, o, "sErr", StageReasonInvalidPath)
				if o.sinkN == 0 {
					t.Fatal("the stage-error sink was not bumped")
				}

				s := target.stage(http.MethodGet, target.last)
				s.ContinueOnError = ptr.To(true)
				o = rr16Resolve(t, class, map[string]any{}, s)
				x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
				rr16ErrRecord(t, o, "sErr", StageReasonInvalidPath)
				if o.failure != nil {
					t.Fatalf("continueOnError: StageFailure = %+v, want none", o.failure)
				}
				if o.sinkN == 0 {
					t.Fatal("continueOnError: the stage-error sink was not bumped")
				}
				if hits := api.all()[dialled:]; len(hits) != 0 {
					t.Fatalf("dialled %+v", hits)
				}
			})
		}
	}
}

// An iterator: one empty element is one refused item, and the others are sent.
func TestIssue1185_OneEmptyElementIsOneRefusedItem(t *testing.T) {
	api := server1185(t)
	for _, target := range targets1185 {
		for _, verb := range []string{http.MethodGet, http.MethodDelete} {
			t.Run(target.name+"/"+verb, func(t *testing.T) {
				dialled := len(api.all())
				s := target.stage(verb, target.last)
				s.ContinueOnError = ptr.To(true)
				s.DependsOn = &templates.Dependency{Iterator: ptr.To(`[{x: "a"}, {x: ""}, {x: null}, {}, {x: "b"}]`)}
				o := rr16Resolve(t, workclass.None, map[string]any{}, s)
				hits := api.all()[dialled:]
				if len(hits) != 2 {
					t.Fatalf("dialled %+v, want the two present elements", hits)
				}
				for _, h := range hits {
					if !strings.HasSuffix(h.Path, "/a") && !strings.HasSuffix(h.Path, "/b") {
						t.Fatalf("dialled %+v", h)
					}
				}
				x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
			})
		}
	}
}

// `"/" + segments(.p)` (#1185 item B) on both dials: nothing is sent, and the
// endpoint's root in particular is not. Nor is a declared value made of slashes.
func TestIssue1185_ADeclaredEmptyValueIsRefused(t *testing.T) {
	api := server1185(t)
	for _, target := range targets1185 {
		for _, verb := range verbs1185 {
			for _, c := range []struct {
				tmpl string
				p    any
			}{
				{`${ "/" + segments(.p) }`, ""},
				{`${ "/" + segments(.p) }`, nil},
				{`${ "/v2/tickets/" + segments(.p) }`, ""},
				{`${ "/v2/tickets/" + segments(.p) }`, nil},
				{`${ "/v2/tickets/" + segments(.p) + "/comments" }`, ""},
				{`${ segments(.p) }`, "/"},
				{`${ "/v2/tickets/" + segments(.p) }`, "/"},
				{`${ "/v2/tickets/" + segments(.p) }`, "42/"},
			} {
				t.Run(fmt.Sprintf("%s/%s/%s/%v", target.name, verb, c.tmpl, c.p), func(t *testing.T) {
					dialled := len(api.all())
					o := rr16Resolve(t, workclass.None, map[string]any{"p": c.p}, target.stage(verb, c.tmpl))
					if hits := api.all()[dialled:]; len(hits) != 0 {
						t.Fatalf("dialled %+v", hits)
					}
					x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
				})
			}
		}
	}
}

// Whitespace is not empty (pathtemplate.TestIssue1185_WhitespaceIsNotEmpty): the
// segment holds %20, so the path has as many segments as the template meant and
// names nothing wider. On a named endpoint that request is sent, whatever the
// verb, to the one path the template names. On this cluster an escape is refused
// before any dial, as it was (#1123, #1137): no name or namespace holds one.
func TestIssue1185_WhitespaceIsOneEscapedSegment(t *testing.T) {
	api := server1185(t)
	named, own := targets1185[0], targets1185[1]
	wantSent := map[string]string{"first": "/ /tickets/42", "middle": "/v2/tickets/ /comments", "last": "/v2/tickets/ "}
	for _, verb := range verbs1185 {
		for pos, tmpl := range named.positions() {
			t.Run("named/"+pos+"/"+verb, func(t *testing.T) {
				dialled := len(api.all())
				rr16Resolve(t, workclass.None, map[string]any{"x": " "}, named.stage(verb, tmpl))
				hits := api.all()[dialled:]
				if len(hits) != 1 || hits[0].Path != wantSent[pos] || hits[0].Method != verb {
					t.Fatalf("dialled %+v, want one %s %q", hits, verb, wantSent[pos])
				}
			})
		}
		for pos, tmpl := range own.positions() {
			t.Run("own/"+pos+"/"+verb, func(t *testing.T) {
				dialled := len(api.all())
				o := rr16Resolve(t, workclass.None, map[string]any{"x": " "}, own.stage(verb, tmpl))
				if hits := api.all()[dialled:]; len(hits) != 0 {
					t.Fatalf("dialled %+v", hits)
				}
				x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
			})
		}
	}
}

// The shapes that keep working, sent on the wire, byte for byte, on both dials.
func TestIssue1185_ShapesThatKeepWorking(t *testing.T) {
	api := server1185(t)
	for _, c := range []struct {
		name, tmpl string
		extras     map[string]any
		want       string
	}{
		{"a value empty inside a segment", `${ "/v2/configmaps/" + .plural + "-" + .version + "-jsonschema" }`,
			map[string]any{"plural": "things", "version": ""}, "/v2/configmaps/things--jsonschema"},
		{"every value empty inside a segment", `${ "/v2/configmaps/" + .plural + "-" + .version + "-jsonschema" }`,
			map[string]any{}, "/v2/configmaps/--jsonschema"},
		{"an empty query value", `${ "/v2/tickets?status=" + .x }`, map[string]any{}, "/v2/tickets?status="},
		{"an empty query value after a segment", `${ "/v2/tickets/42?status=" + .x + "&limit=5" }`, map[string]any{"x": ""}, "/v2/tickets/42?status=&limit=5"},
		{"the optional shape, absent", `${ "/v2/tickets" + (if (.pl // "") == "" then "" else "/" + .pl end) }`, map[string]any{"pl": ""}, "/v2/tickets"},
		{"the optional shape, present", `${ "/v2/tickets" + (if (.pl // "") == "" then "" else "/" + .pl end) }`, map[string]any{"pl": "42"}, "/v2/tickets/42"},
		{"an author's trailing slash", `${ "/v2/tickets/" }`, map[string]any{}, "/v2/tickets/"},
		{"an author's trailing slash after a value", `${ "/v2/" + .x + "/" }`, map[string]any{"x": "tickets"}, "/v2/tickets/"},
		{"an author's double slash", `${ "/v2//tickets/" + .x }`, map[string]any{"x": "42"}, "/v2//tickets/42"},
		{"a literal path", `/v2//tickets/`, map[string]any{}, "/v2//tickets/"},
		{"a declared value", `${ "/v2/" + segments(.p) }`, map[string]any{"p": "tickets/42"}, "/v2/tickets/42"},
		{"a declared whole path", `${ segments(.p) }`, map[string]any{"p": "/v2/tickets/42"}, "/v2/tickets/42"},
	} {
		for _, verb := range []string{http.MethodGet, http.MethodDelete} {
			t.Run(c.name+"/"+verb, func(t *testing.T) {
				opts, skips := plan1123On(t, false, c.tmpl, c.extras)
				if len(opts) != 1 || opts[0].Path != c.want {
					t.Fatalf("planned %+v, skips %+v, want %q", opts, skips, c.want)
				}
				dialled := len(api.all())
				o := rr16Resolve(t, workclass.None, c.extras, targets1185[0].stage(verb, c.tmpl))
				if hits := api.all()[dialled:]; len(hits) != 1 || hits[0].Method != verb {
					t.Fatalf("dialled %+v, want one; outcomes %s", hits, o.outcomes)
				}
				if o.failure != nil {
					t.Fatalf("StageFailure = %+v", o.failure)
				}
			})
		}
	}
	// On this cluster a value empty inside a segment or a query value is as
	// planned as it was, and so is the optional shape.
	for _, c := range []struct {
		tmpl   string
		extras map[string]any
		want   string
	}{
		{`${ "/api/v1/namespaces/demo/configmaps/" + .plural + "-" + .version + "-jsonschema" }`, map[string]any{"plural": "things"}, "/api/v1/namespaces/demo/configmaps/things--jsonschema"},
		{`${ "/api/v1/namespaces/demo/pods?labelSelector=" + .x }`, map[string]any{}, "/api/v1/namespaces/demo/pods?labelSelector="},
		{`${ "/api/v1" + (if (.ns // "") == "" then "" else "/namespaces/" + .ns end) + "/pods" }`, map[string]any{}, "/api/v1/pods"},
		{`${ "/api/v1/namespaces/demo/pods/" }`, map[string]any{}, "/api/v1/namespaces/demo/pods/"},
	} {
		if opts, skips := plan1123(t, c.tmpl, c.extras); len(opts) != 1 || opts[0].Path != c.want {
			t.Errorf("%s: planned %+v, skips %+v, want %q", c.tmpl, opts, skips, c.want)
		}
	}
}

// An author's literal is not this rule's to refuse or to excuse: each dial guard
// says what it said before #1185, for the reason it gave then. A value that is
// there does not change that.
func TestIssue1185_AuthorLiteralsKeepTheirVerdict(t *testing.T) {
	for _, c := range []struct {
		name, tmpl, verb string
		apiserver        bool
		class            string // "" = planned
	}{
		{"named //", `${ "/v2//tickets/" + .x }`, http.MethodDelete, false, ""},
		{"named trailing /", `${ "/v2/" + .x + "/" }`, http.MethodDelete, false, ""},
		{"own //", `${ "/api/v1//namespaces/demo/" + .x }`, http.MethodGet, true, reasonPathNotAsServed},
		{"own // write", `${ "/api/v1//namespaces/demo/" + .x }`, http.MethodDelete, true, reasonPathNotAsServed},
		{"own collection read with /", `${ "/api/v1/namespaces/" + .x + "/pods/" }`, http.MethodGet, true, ""},
		{"own collection delete with /", `${ "/api/v1/namespaces/" + .x + "/pods/" }`, http.MethodDelete, true, reasonEmptyInterp},
		{"own /namespaces//", `${ "/api/v1/namespaces//pods/" + .x }`, http.MethodGet, true, reasonEmptyInterp},
		{"named /namespaces//", `${ "/api/v1/namespaces//pods/" + .x }`, http.MethodGet, false, reasonEmptyInterp},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := &templates.API{Name: "s", Verb: ptr.To(c.verb), Path: c.tmpl}
			opts, skips, err := createRequestOptionsWithSkips(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), in, map[string]any{"x": "demo"}, c.apiserver)
			if err != nil {
				t.Fatal(err)
			}
			if c.class == "" {
				if len(opts) != 1 || len(skips) != 0 {
					t.Fatalf("planned %+v, skips %+v, want it planned", opts, skips)
				}
				return
			}
			if len(opts) != 0 || len(skips) != 1 || skips[0].reason.class != c.class {
				t.Fatalf("planned %+v, skips %+v, want one %s", opts, skips, c.class)
			}
		})
	}
}

// A value a path was refused for never reaches the skip line (#1185 item C), from
// any branch: the '%' branch held it escaped, so searching for the raw value found
// nothing. The canary survives escaping, so the arm sees it either way, and a line
// that is not there cannot pass.
func TestIssue1185_NoRefusalPutsARenderedValueOnTheSkipLine(t *testing.T) {
	const canary = "sup3rsecretvalue"
	for _, c := range []struct {
		name, tmpl string
		id         string
		apiserver  bool
		class      string
	}{
		{"the percent branch", `${ "/api/v1/namespaces/demo/configmaps/" + .id }`, canary + "?", true, reasonPathValueRefused},
		{"a path no parser reads", `${ "/x/" + .id }`, canary + "?", true, reasonPathNotAsServed},
		{"a value the guard reads as a verb", `${ "/api/v1/" + .id + "/" + .id2 }`, "watch", true, reasonPathNotAsServed},
		{"an invalid name", `${ "/api/v1/namespaces/demo/configmaps/" + .id }`, canary + "-", true, reasonEmptyInterp},
		{"an invalid namespace", `${ "/api/v1/namespaces/" + .id + "/configmaps" }`, canary + "-", true, reasonEmptyInterp},
		{"an empty value beside another", `${ "/v2/" + .empty + "/" + .id }`, canary, false, reasonEmptyInterp},
		{"an empty value beside a query value", `${ "/api/v1/namespaces/" + .empty + "/configmaps?token=" + .id }`, canary, true, reasonEmptyInterp},
		{"a value with a slash", `${ "/v2/" + .id }`, canary + "/x", false, reasonPathValueRefused},
		{"a dot segment beside a value", `${ "/v2/" + .id + "/.." }`, canary, false, reasonPathValueRefused},
		{"a jq error beside a value", `${ "/v2/" + .id + "/" + (1 | error) }`, canary, false, reasonJQPathError},
	} {
		for _, iterated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/iterator=%v", c.name, iterated), func(t *testing.T) {
				dict := map[string]any{"id": c.id, "id2": canary}
				in := &templates.API{Name: "s", Path: c.tmpl}
				if iterated {
					dict = map[string]any{"els": []any{dict}}
					in.DependsOn = &templates.Dependency{Iterator: ptr.To(".els")}
				}
				var line bytes.Buffer
				log := slog.New(slog.NewTextHandler(&line, &slog.HandlerOptions{Level: slog.LevelDebug}))
				opts, skips, err := createRequestOptionsWithSkips(context.Background(), log, in, dict, c.apiserver)
				if err != nil {
					t.Fatal(err)
				}
				if len(opts) != 0 || len(skips) != 1 || skips[0].reason.class != c.class {
					t.Fatalf("planned %+v, skips %+v, want one %s", opts, skips, c.class)
				}
				got := line.String()
				if !strings.Contains(got, "skipping api call") || !strings.Contains(got, "reason_class="+c.class) {
					t.Fatalf("no skip line to read: %q", got)
				}
				if strings.Contains(got, canary) || strings.Contains(skips[0].path, canary) || strings.Contains(fmt.Sprintf("%+v", skips), canary) {
					t.Fatalf("a rendered value reached the skip: %q %+v", got, skips)
				}
			})
		}
	}
}

// One real portal stage from each class of the #1185 measurement, as portal tag
// 1.8.93 renders it (testdata/portal_stages_1185.yaml), with its real iterator.

type portalStage1185 struct {
	RA        string               `json:"ra"`
	Stage     string               `json:"stage"`
	DependsOn templates.Dependency `json:"dependsOn"`
	Path      string               `json:"path"`
}

func loadPortalStage1185(t *testing.T, ra, stage string) *templates.API {
	t.Helper()
	b, err := os.ReadFile("testdata/portal_stages_1185.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var all []portalStage1185
	if err := yaml.Unmarshal(b, &all); err != nil {
		t.Fatal(err)
	}
	for _, s := range all {
		if s.RA == ra && s.Stage == stage {
			return &templates.API{Name: stage, Path: s.Path, DependsOn: &templates.Dependency{Iterator: s.DependsOn.Iterator}}
		}
	}
	t.Fatalf("testdata has no stage %s/%s", ra, stage)
	return nil
}

func TestIssue1185_RealPortalStages(t *testing.T) {
	type want struct {
		paths   []string
		skipped int
	}
	crd := func(version, plural string) map[string]any {
		return map[string]any{"version": version, "plural": plural}
	}
	for _, c := range []struct {
		name, ra, stage string
		dict            map[string]any
		want            want
		named           bool
	}{
		// The class of 8 stages with an optional part: `if .pl == "" then "" else
		// "/" + .pl end`. Discovery (no resource, no name), then a whole object.
		{"optional parts absent", "composition-architecture", "objs", map[string]any{
			"arch": map[string]any{"graph": map[string]any{"nodes": []any{map[string]any{
				"present": true, "apiVersion": "apps/v1", "names": []any{"web"}}}}},
			"comp": []any{map[string]any{"managed": []any{
				map[string]any{"name": "web", "apiVersion": "apps/v1", "path": "/apis/apps/v1"}}}},
		}, want{paths: []string{"/apis/apps/v1"}}, false},
		{"optional parts present", "composition-architecture", "objs", map[string]any{
			"arch": map[string]any{"graph": map[string]any{"nodes": []any{map[string]any{
				"present": true, "apiVersion": "apps/v1", "names": []any{"web"}}}}},
			"comp": []any{map[string]any{"managed": []any{
				map[string]any{"name": "web", "apiVersion": "apps/v1", "path": "/apis/apps/v1/namespaces/demo/deployments/web"}}}},
		}, want{paths: []string{"/apis/apps/v1/namespaces/demo/deployments/web"}}, false},

		// The iterator gate: an absent value yields no element, so nothing to
		// call and nothing to refuse.
		{"gate open", "composition-architecture", "arch", map[string]any{"name": "app", "namespace": "demo"},
			want{paths: []string{"/api/v1/namespaces/demo/configmaps/app-architecture"}}, false},
		{"gate shut, no name", "composition-architecture", "arch", map[string]any{"namespace": "demo"}, want{}, false},
		{"gate shut, empty name", "composition-architecture", "arch", map[string]any{"namespace": "demo", "name": ""}, want{}, false},
		{"gate shut, nil extras", "composition-architecture", "arch", nil, want{}, false},

		// A value empty inside a segment is not an empty segment.
		{"inside a segment, present", "composition-editdef", "jsonschema", map[string]any{
			"name": "app", "namespace": "demo",
			"found": []any{map[string]any{"kind": "Thing", "metadata": map[string]any{"name": "app", "namespace": "demo"}}},
			"crds":  []any{map[string]any{"kind": "Thing", "cdNamespace": "defs", "plural": "things", "version": "v1"}},
		}, want{paths: []string{"/api/v1/namespaces/defs/configmaps/things-v1-jsonschema-configmap"}}, false},
		{"inside a segment, empty version", "composition-editdef", "jsonschema", map[string]any{
			"name": "app", "namespace": "demo",
			"found": []any{map[string]any{"kind": "Thing", "metadata": map[string]any{"name": "app", "namespace": "demo"}}},
			"crds":  []any{map[string]any{"kind": "Thing", "cdNamespace": "defs", "plural": "things", "version": ""}},
		}, want{paths: []string{"/api/v1/namespaces/defs/configmaps/things--jsonschema-configmap"}}, false},

		// The known cost in the ruling: a CompositionDefinition with
		// `status.resource: ""`. Real values list; an empty plural used to read
		// discovery, and now fails that one element.
		{"found, real values", "composition-detail", "found", map[string]any{
			"name": "app", "namespace": "demo", "crds": []any{crd("v1-2-0", "things")},
		}, want{paths: []string{"/apis/composition.krateo.io/v1-2-0/things"}}, false},
		{"found, an empty plural", "composition-detail", "found", map[string]any{
			"name": "app", "namespace": "demo", "crds": []any{crd("v1-2-0", ""), crd("v1-3-0", "things")},
		}, want{paths: []string{"/apis/composition.krateo.io/v1-3-0/things"}, skipped: 1}, false},
		{"allCompositions, an empty plural", "dashboard-data", "allCompositions", map[string]any{
			"crds": []any{crd("v1-2-0", "")},
		}, want{skipped: 1}, false},

		// The two stages the live measurement added to the ruling's 8: they read
		// the same field. The second dials a named endpoint, a spoke cluster.
		{"running, real values", "blueprint-palette", "running", map[string]any{
			"compdefs": []any{map[string]any{"apiVersion": "composition.krateo.io/v1-2-0", "plural": "things"}},
		}, want{paths: []string{"/apis/composition.krateo.io/v1-2-0/things"}}, false},
		{"running, an empty plural", "blueprint-palette", "running", map[string]any{
			"compdefs": []any{map[string]any{"apiVersion": "composition.krateo.io/v1-2-0", "plural": ""}},
		}, want{skipped: 1}, false},
		{"running, an absent plural", "blueprint-palette", "running", map[string]any{
			"compdefs": []any{map[string]any{"apiVersion": "composition.krateo.io/v1-2-0"}},
		}, want{skipped: 1}, false},
		{"spoke, real values", "cluster-compositions", "spokeCompositions", map[string]any{
			"spokeCrds": map[string]any{"compositionCrds": []any{map[string]any{"group": "composition.krateo.io", "version": "v1-2-0", "plural": "things"}}},
		}, want{paths: []string{"/composition.krateo.io/v1-2-0/things"}}, true},
		{"spoke, an empty plural", "cluster-compositions", "spokeCompositions", map[string]any{
			"spokeCrds": map[string]any{"compositionCrds": []any{map[string]any{"group": "composition.krateo.io", "version": "v1-2-0", "plural": ""}}},
		}, want{skipped: 1}, true},
		{"spoke, an absent version", "cluster-compositions", "spokeCompositions", map[string]any{
			"spokeCrds": map[string]any{"compositionCrds": []any{map[string]any{"group": "composition.krateo.io", "plural": "things"}}},
		}, want{skipped: 1}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := loadPortalStage1185(t, c.ra, c.stage)
			opts, skips, err := createRequestOptionsWithSkips(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), in, c.dict, !c.named)
			if err != nil {
				t.Fatalf("iterator: %v", err)
			}
			var paths []string
			for _, o := range opts {
				paths = append(paths, o.Path)
			}
			if strings.Join(paths, " ") != strings.Join(c.want.paths, " ") {
				t.Fatalf("planned %v, want %v (skips %+v)", paths, c.want.paths, skips)
			}
			if len(skips) != c.want.skipped {
				t.Fatalf("refused %+v, want %d", skips, c.want.skipped)
			}
			for _, sk := range skips {
				if sk.reason.class != reasonEmptyInterp || !sk.reason.collapsed {
					t.Fatalf("refused %+v, want an empty segment", sk)
				}
			}
		})
	}
}

// The cluster-list probe reads a refused path as "not a list this element would
// read", never as a jq error: #1128 mutant R9 had no arm on either side.
func TestIssue1185_TheProbeCountsAJQErrorAndNotARefusal(t *testing.T) {
	const tmpl = `${ "/apis/g.io/v1/namespaces/" + .ns + "/things" }`
	for name, el := range map[string]any{
		"an empty value":    map[string]any{"ns": ""},
		"a null value":      map[string]any{"ns": nil},
		"an absent value":   map[string]any{},
		"a value with '/'":  map[string]any{"ns": "a/b"},
		"a dot-dot segment": map[string]any{"ns": ".."},
	} {
		t.Run(name, func(t *testing.T) {
			before := NondialJQErrorByReason(nondialClusterListGVRProbe)
			if gvr, _, ok := clusterListElementTarget(tmpl, el); ok {
				t.Fatalf("a refused element probed as a list of %v", gvr)
			}
			if d := NondialJQErrorByReason(nondialClusterListGVRProbe) - before; d != 0 {
				t.Fatalf("a refused element moved the jq-error counter by %d", d)
			}
		})
	}

	before := NondialJQErrorByReason(nondialClusterListGVRProbe)
	if _, _, ok := clusterListElementTarget(`${ "/apis/g.io/v1/namespaces/" + (1 | error) + "/things" }`, map[string]any{}); ok {
		t.Fatal("a jq error probed as a list")
	}
	if d := NondialJQErrorByReason(nondialClusterListGVRProbe) - before; d != 1 {
		t.Fatalf("a jq error moved the counter by %d, want 1", d)
	}

	if gvr, _, ok := clusterListElementTarget(tmpl, map[string]any{"ns": "demo"}); !ok || gvr.Resource != "things" {
		t.Fatalf("a clean element: %v ok=%v", gvr, ok)
	}
}

// Inspect (#504) asks for the read-set before any value exists, so every value
// is empty. A refusal is the #288 class, so the stage still enumerates from its
// template: a by-name read is `get` on that resource. Rendered, the same stage
// looked like a list, which grants a verb the first /call does not use.
func TestIssue1185_InspectEnumeratesARefusedStageFromItsTemplate(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))
	rows, unresolved, err := InspectReadSet(context.Background(), &templates.RESTAction{Spec: templates.RESTActionSpec{API: []*templates.API{
		{Name: "byName", Path: `${ "/api/v1/namespaces/demo/configmaps/" + (.name // "") }`},
		{Name: "list", Path: `${ "/api/v1/namespaces/demo/services" }`},
	}}}, nil)
	if err != nil {
		t.Fatalf("InspectReadSet: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved %+v: an empty value made the stage unenumerable", unresolved)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Resource] = r.Verb
	}
	if got["configmaps"] != "get" || got["services"] != "list" || len(rows) != 2 {
		t.Fatalf("rows %+v, want get on configmaps and list on services", rows)
	}
}

// The other side of item C: a path refused for what is missing, not for what a
// value holds, keeps its shape on the line (#1138 owns its query redaction).
func TestIssue1185_ACollapsedPathKeepsItsShapeOnTheSkipLine(t *testing.T) {
	var line bytes.Buffer
	log := slog.New(slog.NewTextHandler(&line, &slog.HandlerOptions{Level: slog.LevelDebug}))
	in := &templates.API{Name: "s", Path: `${ "/api/v1/namespaces//configmaps/" + .id }`}
	for _, apiserver := range []bool{true, false} {
		line.Reset()
		opts, skips, err := createRequestOptionsWithSkips(context.Background(), log, in, map[string]any{"id": "cm"}, apiserver)
		if err != nil || len(opts) != 0 || len(skips) != 1 || !skips[0].reason.collapsed {
			t.Fatalf("apiserver=%v: planned %+v, skips %+v, err %v", apiserver, opts, skips, err)
		}
		if !strings.Contains(line.String(), "path=/api/v1/namespaces//configmaps/cm") {
			t.Fatalf("apiserver=%v: the skip line lost the path: %q", apiserver, line.String())
		}
	}
}

// The extended ruling at the server: the empty came from the author's literal
// `""` branch, not from a value, on every verb and both dials. The control is the
// same stage with its value there, dialled once.
func TestIssue1185_ALiteralEmptyBranchIsRefusedOnEveryVerbAndDial(t *testing.T) {
	const x = `(if (.x // "") == "" then "" else .x end)`
	api := server1185(t)
	for _, target := range targets1185 {
		for pos, tmpl := range target.positions() {
			spelled := strings.NewReplacer(`+ .x +`, `+ `+x+` +`, `+ .x }`, `+ `+x+` }`).Replace(tmpl)
			if spelled == tmpl {
				t.Fatalf("%s: the arm did not respell %s", pos, tmpl)
			}
			for _, verb := range verbs1185 {
				t.Run(fmt.Sprintf("%s/%s/%s", target.name, pos, verb), func(t *testing.T) {
					dialled := len(api.all())
					o := rr16Resolve(t, workclass.None, map[string]any{"x": target.present[pos]}, target.stage(verb, spelled))
					if got := len(api.all()) - dialled; got != 1 {
						t.Fatalf("control: dialled %d time(s), want 1; outcomes %s", got, o.outcomes)
					}
					for name, extras := range map[string]map[string]any{"empty": {"x": ""}, "null": {"x": nil}, "absent": {}} {
						dialled := len(api.all())
						skipped := MalformedDialSkippedByReason(reasonEmptyInterp)
						o := rr16Resolve(t, workclass.None, extras, target.stage(verb, spelled))
						if hits := api.all()[dialled:]; len(hits) != 0 {
							t.Fatalf("%s: dialled %+v", name, hits)
						}
						x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
						if o.failure == nil || o.failure.Code != http.StatusBadRequest {
							t.Fatalf("%s: StageFailure = %+v, want a 400", name, o.failure)
						}
						if o.sinkN == 0 {
							t.Fatalf("%s: the stage-error sink was not bumped", name)
						}
						if got := MalformedDialSkippedByReason(reasonEmptyInterp) - skipped; got != 1 {
							t.Fatalf("%s: empty_interp moved by %d, want 1", name, got)
						}
					}
				})
			}
		}
	}
}

// The other spellings the ruling names by "wherever the empty comes from", at the
// server, on a read and a write.
func TestIssue1185_EverySpellingOfAnEmptyExpressionDialsNothing(t *testing.T) {
	api := server1185(t)
	for _, tmpl := range []string{
		`${ (if (.x // "") == "" then "/v2/tickets/" else "/v2/tickets/" + .x end) }`,
		`${ (if (.x // "") == "" then "" else .x end) as $v | "/v2/tickets/" + $v }`,
		`${ "" as $e | "/v2/tickets/" + $e }`,
		`${ "/v2/tickets/" + (.x | "") }`,
		`${ "/v2/tickets/" + "" }`,
		`${ (if (.x // "") == "" then "/" else "/" + .x end) }`,
	} {
		for _, target := range targets1185 {
			for _, verb := range []string{http.MethodGet, http.MethodDelete} {
				t.Run(fmt.Sprintf("%s/%s/%s", target.name, verb, tmpl), func(t *testing.T) {
					dialled := len(api.all())
					o := rr16Resolve(t, workclass.None, map[string]any{}, target.stage(verb, tmpl))
					if hits := api.all()[dialled:]; len(hits) != 0 {
						t.Fatalf("dialled %+v", hits)
					}
					x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
				})
			}
		}
	}
}

// The portal's shape on the wire: the real template (composition-architecture/objs
// at tag 1.8.93), with its optional parts absent and present, read through both
// dials. Each is sent, once, to exactly the path it names.
func TestIssue1185_ThePortalShapeIsSent(t *testing.T) {
	path := loadPortalStage1185(t, "composition-architecture", "objs").Path
	api := server1185(t)
	for _, c := range []struct {
		g, v, ns, pl, nm, want string
	}{
		{"", "v1", "", "", "", "/api/v1"},
		{"apps", "v1", "", "", "", "/apis/apps/v1"},
		{"apps", "v1", "", "deployments", "", "/apis/apps/v1/deployments"},
		{"apps", "v1", "demo", "deployments", "web", "/apis/apps/v1/namespaces/demo/deployments/web"},
	} {
		for _, target := range targets1185 {
			t.Run(target.name+c.want, func(t *testing.T) {
				dialled := len(api.all())
				extras := map[string]any{"g": c.g, "v": c.v, "ns": c.ns, "pl": c.pl, "nm": c.nm}
				if opts, skips := plan1123On(t, target.name == "own cluster", path, extras); len(opts) != 1 || opts[0].Path != c.want {
					t.Fatalf("planned %+v, skips %+v, want %s", opts, skips, c.want)
				}
				if target.name == "own cluster" && c.pl == "" {
					// Own-cluster discovery is dialled with the REST config, which
					// this harness points nowhere. The plan above is the pin.
					return
				}
				o := rr16Resolve(t, workclass.None, extras, target.stage(http.MethodGet, path))
				hits := api.all()[dialled:]
				if len(hits) != 1 || hits[0].Path != c.want || o.failure != nil {
					t.Fatalf("dialled %+v, want one %s; failure %+v, outcomes %s", hits, c.want, o.failure, o.outcomes)
				}
			})
		}
	}
}

// An expression that writes nothing before a query or a fragment dials the
// endpoint's root, on every verb: `DELETE /?q=1` with the stage's credential. The
// review of #1197 found these sent on a named endpoint, where `.x + "?q=1"` and
// the same text in a branch were refused. The control is the value there.
func TestIssue1185_NothingBeforeAQueryDialsNoRoot(t *testing.T) {
	const opt = `(if (.x // "") == "" then "" else "/" + .x end)`
	api := server1185(t)
	for _, c := range []struct {
		tmpl    string
		control string // the path a present value reaches; "" = the stage has no value
		ownToo  bool   // the control is a path this cluster's guard also plans
	}{
		{`${ ` + opt + ` + "?q=1" }`, "/v", true},
		{`${ ` + opt + ` + "#f" }`, "/v", true},
		// With a value this spelling has no leading '/', which this cluster refuses.
		{`${ (if (.x // "") == "" then "" else .x end) + "?q=1" }`, "/v", false},
		{`${ "" + "?q=1" }`, "", false},
		{`${ "" + "#f" }`, "", false},
		// The same root with its '/' written out, `DELETE /` with the credential.
		{`${ ` + opt + ` + "/" }`, "/v/", true},
		{`${ ` + opt + ` + "/?q=1" }`, "/v/", true},
		{`${ "" + "/" }`, "", false},
		{`${ "" + "/?q=1" }`, "", false},
	} {
		for _, target := range targets1185 {
			for _, verb := range []string{http.MethodGet, http.MethodDelete} {
				t.Run(fmt.Sprintf("%s/%s/%s", target.name, verb, c.tmpl), func(t *testing.T) {
					if c.control != "" && (c.ownToo || target.name != "own cluster") {
						dialled := len(api.all())
						rr16Resolve(t, workclass.None, map[string]any{"x": "v"}, target.stage(verb, c.tmpl))
						if hits := api.all()[dialled:]; len(hits) != 1 || hits[0].Path != c.control {
							t.Fatalf("control: dialled %+v, want one %s", hits, c.control)
						}
					}
					for name, extras := range map[string]map[string]any{"empty": {"x": ""}, "null": {"x": nil}, "absent": {}} {
						dialled := len(api.all())
						o := rr16Resolve(t, workclass.None, extras, target.stage(verb, c.tmpl))
						if hits := api.all()[dialled:]; len(hits) != 0 {
							t.Fatalf("%s: dialled %+v", name, hits)
						}
						x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
						if o.failure == nil || o.failure.Code != http.StatusBadRequest {
							t.Fatalf("%s: StageFailure = %+v, want a 400", name, o.failure)
						}
					}
				})
			}
		}
	}
}
