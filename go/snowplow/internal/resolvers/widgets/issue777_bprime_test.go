//go:build unit || integration

package widgets

// issue777_bprime_test.go — #777 (b′): the extras-free predicate, its corpus
// property, and the structural guards that keep "no extras-reading surface"
// meaning "no path from the request extras to the body".
//
//   - B2–B6 (predicate): any extras-reading field PRESENT — even null, empty or
//     malformed — and any field outside the closed allowlist fail closed.
//   - B9 (corpus property): fixtures copied field-for-field from the chrome
//     widgets the exemption exists for render byte-identical bodies under the
//     REAL Resolve whatever undeclared extras arrive, and are exempt.
//   - B10 (structural guard): every call in resolve.go that receives the
//     request extras (or a value derived from them) is a documented consumer.
//   - the static resourcesRefs path takes no extras (signature pinned).

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	v1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/resourcesrefs"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// Compile-time pin: static resourcesRefs resolve with no extras parameter. A
// signature that grows one (or any new argument) fails to build here, which is
// the review prompt: map the new input to a spec field outside
// ExtrasFreeSpecFields, or prove it extras-free.
var _ func(context.Context, []v1.ResourceRef) ([]v1.ResourceRefResult, error) = resourcesrefs.Resolve

func b777Exempt() map[string]any {
	return map[string]any{
		"widgetData": map[string]any{"label": "chrome"},
		"keyExtras":  []any{"projects"},
	}
}

// TestB777_Predicate covers B2–B6 at the predicate: presence, not parse; an
// allowlist, not a denylist; a missing or non-map spec fails closed.
func TestB777_Predicate(t *testing.T) {
	with := func(k string, v any) map[string]any {
		s := b777Exempt()
		s[k] = v
		return map[string]any{"spec": s}
	}
	cases := []struct {
		name string
		cr   map[string]any
		want bool
	}{
		{"exempt_widgetData_keyExtras", map[string]any{"spec": b777Exempt()}, true},
		{"exempt_static_resourcesRefs", with("resourcesRefs", map[string]any{"items": []any{}}), true},
		{"B2_apiRef", with("apiRef", map[string]any{"name": "ra", "namespace": "ns"}), false},
		{"B2_apiRef_null", with("apiRef", nil), false},
		{"B3_widgetDataTemplate", with("widgetDataTemplate", []any{map[string]any{"forPath": "x", "expression": "${ 1 }"}}), false},
		{"B3_widgetDataTemplate_empty", with("widgetDataTemplate", []any{}), false},
		{"B4_resourcesRefsTemplate", with("resourcesRefsTemplate", []any{}), false},
		{"B4_resourcesRefsTemplateExtras", with("resourcesRefsTemplateExtras", map[string]any{}), false},
		{"B5_malformed_widgetDataTemplate", with("widgetDataTemplate", "not-a-list"), false},
		{"B6_unknown_field", with("futureThing", map[string]any{}), false},
		{"identityContext_not_allowlisted", with("identityContext", []any{"username"}), false},
		{"no_spec", map[string]any{"kind": "Tag"}, false},
		{"spec_not_a_map", map[string]any{"spec": "nope"}, false},
	}
	for _, c := range cases {
		if got := WidgetReadsNoRequestExtras(c.cr); got != c.want {
			t.Errorf("%s: WidgetReadsNoRequestExtras = %v, want %v", c.name, got, c.want)
		}
	}
	// B5's premise: the typed accessor DOES fail soft on that value, so an
	// accessor-based predicate would read it as absent.
	if _, err := GetWidgetDataTemplate(with("widgetDataTemplate", "not-a-list")); err == nil {
		t.Fatal("PRECONDITION B5: GetWidgetDataTemplate accepted a malformed value — the arm no longer separates presence from parse")
	}
}

// b777Chrome are the chrome widgets (b′) exists for, copied field-for-field from
// krateo-platformops/portal-platformops helm/portal/templates
// (flex.header-context.yaml, tag.tenant-chip.yaml, layout.app-shell.yaml), Helm
// values rendered with their defaults. Their top-level spec keys match 057's
// live CRs (read-only, key names only, by team-lead on 2026-10-19):
// header-context and tenant-chip {keyExtras, resourcesRefs, widgetData};
// app-shell {resourcesRefs, widgetData} — the shell Layout renders the page
// outlet, so it receives every page's route params, and (b′) is its only fix
// (frontend#492 deliberately does not scope it).
func b777Chrome() map[string]map[string]any {
	return map[string]map[string]any{
		"app-shell": {
			"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Layout",
			"metadata": map[string]any{"name": "app-shell", "namespace": "krateo-system"},
			"spec": map[string]any{
				"widgetData": map[string]any{
					"hasSider": true,
					"sider":    map[string]any{"resourceRefId": "sider-content", "theme": "light", "width": int64(236)},
					"effects":  []any{"app-theme"},
				},
				"resourcesRefs": map[string]any{"items": []any{
					map[string]any{"id": "sider-content", "apiVersion": "widgets.templates.krateo.io/v1beta1",
						"name": "sider-content", "namespace": "krateo-system", "resource": "flexes", "verb": "GET"},
					map[string]any{"id": "app-theme", "apiVersion": "widgets.templates.krateo.io/v1beta1",
						"name": "app-theme", "namespace": "krateo-system", "resource": "themes", "verb": "GET"},
				}},
			},
		},
		"header-context": {
			"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Flex",
			"metadata": map[string]any{"name": "header-context", "namespace": "krateo-system"},
			"spec": map[string]any{
				"keyExtras": []any{"projects"},
				"widgetData": map[string]any{
					"gap": "small", "align": "center",
					"allowedResources": []any{"tags", "selects"},
					"items": []any{
						map[string]any{"resourceRefId": "tenant-chip"},
						map[string]any{"resourceRefId": "project-select"},
					},
				},
				"resourcesRefs": map[string]any{"items": []any{
					map[string]any{"id": "tenant-chip", "apiVersion": "widgets.templates.krateo.io/v1beta1",
						"name": "tenant-chip", "namespace": "krateo-system", "resource": "tags", "verb": "GET"},
					map[string]any{"id": "project-select", "apiVersion": "widgets.templates.krateo.io/v1beta1",
						"name": "project-select", "namespace": "krateo-system", "resource": "selects", "verb": "GET"},
				}},
			},
		},
		"tenant-chip": {
			"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Tag",
			"metadata": map[string]any{"name": "tenant-chip", "namespace": "krateo-system"},
			"spec": map[string]any{
				"keyExtras": []any{"projects"},
				"widgetData": map[string]any{
					"label": "⬡ krateo-enterprise",
					"style": map[string]any{
						"fontFamily": "var(--font-mono)", "fontSize": "11px", "color": "var(--gray-color)",
						"background": "transparent", "borderColor": "var(--border-color)",
					},
				},
				"resourcesRefs": map[string]any{"items": []any{}},
			},
		},
	}
}

// b777Render resolves cr through the REAL Resolve (no seams on the apiRef or
// resourcesRefs hops: an apiRef-less widget never dials) for one request's
// extras, and returns the resolved object's status as bytes.
func b777Render(t *testing.T, cr map[string]any, extras map[string]any) string {
	t.Helper()
	stubValidate(t, nil) // the CRD-schema hop needs a cluster; it reads status only
	ctx := hermeticCtx(t)
	ctx = xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: "u777", Groups: []string{"devs"}}))
	in := &unstructured.Unstructured{Object: deepCopyJSON(t, cr)}
	obj, err := Resolve(ctx, ResolveOptions{In: in, Extras: extras, RC: &rest.Config{Host: "https://test.invalid"}})
	var errText string
	if err != nil {
		errText = err.Error()
	}
	b, merr := json.Marshal(map[string]any{"status": obj.Object["status"], "err": errText})
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	return string(b)
}

func deepCopyJSON(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("copy: %v", err)
	}
	return out
}

// TestB777_B9_LiveChromeCorpusIsExtrasIndependent — B9 (and B7's resolve-side
// reading): each chrome widget renders byte-identically for extras {} and three
// undeclared maps (route params, a query, and keys named like a stage or a
// reserved dict key), and each is exempt.
func TestB777_B9_LiveChromeCorpusIsExtrasIndependent(t *testing.T) {
	extrasSets := []map[string]any{
		{"namespace": "team-a", "name": "comp-1"},
		{"q": "search-term", "page": "3"},
		{"projects": "p1", "slice": map[string]any{"page": 9}, "tenant-chip": "poison", "items": []any{"x"}},
	}
	for name, cr := range b777Chrome() {
		t.Run(name, func(t *testing.T) {
			if !WidgetReadsNoRequestExtras(cr) {
				t.Fatalf("B9: %s is not exempt — its spec keys %v are not all in ExtrasFreeSpecFields", name, specKeys(cr))
			}
			base := b777Render(t, cr, map[string]any{})
			for i, ex := range extrasSets {
				if got := b777Render(t, cr, ex); got != base {
					t.Fatalf("B9: %s rendered differently under extras set %d:\nbase %s\ngot  %s", name, i, base, got)
				}
			}
		})
	}
}

// TestB777_B7_StaticResourcesRefsAreExtrasIndependent — B7 (resolve side): a
// widget with only static resourcesRefs resolves its children's refs
// identically whatever undeclared extras arrive, and is exempt. The refs use a
// builtin kind (v1 namespaces) so they really resolve here (the chrome fixtures'
// widget kinds need discovery, which the hermetic ctx cannot reach).
func TestB777_B7_StaticResourcesRefsAreExtrasIndependent(t *testing.T) {
	cr := map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Flex",
		"metadata": map[string]any{"name": "w777-refs", "namespace": "demo"},
		"spec": map[string]any{
			"widgetData": map[string]any{"items": []any{map[string]any{"resourceRefId": "ns-a"}}},
			"resourcesRefs": map[string]any{"items": []any{
				map[string]any{"id": "ns-a", "apiVersion": "v1", "resource": "namespaces", "name": "team-a", "verb": "GET"},
			}},
		},
	}
	if !WidgetReadsNoRequestExtras(cr) {
		t.Fatal("B7: a static-resourcesRefs widget must be exempt")
	}
	base := b777Render(t, cr, map[string]any{})
	if !strings.Contains(base, `"resourcesRefs"`) || !strings.Contains(base, `"ns-a"`) {
		t.Fatalf("PRECONDITION B7: the static ref did not resolve, so the arm compares nothing: %s", base)
	}
	// The child's /call path is built from the static ref alone: no extras ride
	// it, so the child's own request (and its own declared-keys cache key) is
	// whatever the client sends it, unaffected by the parent.
	if !strings.Contains(base, `/call?`) || strings.Contains(base, "extras") {
		t.Fatalf("B7: the child /call path must exist and carry no extras: %s", base)
	}
	for _, ex := range []map[string]any{{"name": "other"}, {"namespace": "team-b", "id": "ns-b"}} {
		if got := b777Render(t, cr, ex); got != base {
			t.Fatalf("B7: static resourcesRefs changed with undeclared extras %v:\nbase %s\ngot  %s", ex, base, got)
		}
	}
}

func specKeys(cr map[string]any) []string {
	spec, _ := cr["spec"].(map[string]any)
	out := make([]string, 0, len(spec))
	for k := range spec {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestB777_B8_UndeclaredIdentityNeverReachesAnExemptBody — B8 (resolve side):
// the identity strip still runs for an exempt widget; a client-supplied
// username renders nowhere (and the body is the one with no extras).
func TestB777_B8_UndeclaredIdentityNeverReachesAnExemptBody(t *testing.T) {
	cr := map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Tag",
		"metadata": map[string]any{"name": "w777", "namespace": "demo"},
		"spec":     b777Exempt(),
	}
	base := b777Render(t, cr, map[string]any{})
	got := b777Render(t, cr, map[string]any{"username": "evil-777"})
	if strings.Contains(got, "evil-777") || got != base {
		t.Fatalf("B8: an undeclared identity extra changed an exempt widget's body:\nbase %s\ngot  %s", base, got)
	}
}

// b777Consumers is the documented set of calls in resolve.go that receive the
// request extras, a value derived from them, or the extras-bearing dict, with
// what disables the (b′) exemption for each. A call that shows up here without
// being listed fails TestB777_ExtrasConsumersAreEnumerated.
var b777Consumers = map[string]string{
	// extras-READING consumers — each is gated by a spec field outside
	// ExtrasFreeSpecFields, so a widget reaching it is never exempt:
	"resolveApiRefFn":  "spec.apiRef (an absent apiRef resolves to an empty dict, apiref.Resolve)",
	"mergeRequestWins": "spec.apiRef (folds the extras into the apiRef fetch)",
	"resolveWidgetData": "spec.widgetDataTemplate (static spec.widgetData is returned verbatim " +
		"when no template is present)",
	"resolveResourceRefs": "spec.resourcesRefsTemplate (static spec.resourcesRefs go to " +
		"resourcesrefs.Resolve without the dict)",
	// extras-MOVING or extras-FREE handlers — proven not to put an extra in the body:
	"mergeExtras":                      "puts the extras into the dict only; the dict reaches the body only through the templates above",
	"injectSlice":                      "writes ds[slice] from request pagination; reads no extra",
	"sanitizeUndeclaredIdentityExtras": "strips undeclared identity keys (A-3); emits a subset of its input",
	"resourcesrefstemplate.Resolve":    "spec.resourcesRefsTemplate (inside resolveResourceRefs)",
	"widgetdatatemplate.Resolve":       "spec.widgetDataTemplate (inside resolveWidgetData)",
	"apiref.Resolve":                   "spec.apiRef (inside resolveApiRef)",
	"apirefResolveFn":                  "spec.apiRef (inside resolveApiRef; the #874 seam over apiref.Resolve)",
	// #874 — reads the apiRef RA's own output slice (continue/cursor) to emit
	// status.resourcesRefs.slice; only reached with a resourcesRefs fan-out.
	"cache.SourcePagination": "spec.apiRef (reads the apiRef RA output's slice; an absent apiRef yields an empty dict, declaring nothing)",
}

// TestB777_ExtrasConsumersAreEnumerated — B10, the Go-side structural guard. It
// parses resolve.go, follows the request extras from their sources (opts — which
// carries Extras —, opts.Extras, and every local assigned from a tainted value)
// through Resolve and the helpers it calls, and requires every call that
// receives one to be in b777Consumers.
func TestB777_ExtrasConsumersAreEnumerated(t *testing.T) {
	got := b777TaintedCalls(t, "resolve.go")
	var unknown []string
	for _, c := range got {
		if _, ok := b777Consumers[c]; !ok {
			unknown = append(unknown, c)
		}
	}
	if len(unknown) > 0 {
		t.Fatalf("new request-extras consumer in widget resolve: %v — map it to a spec field that disables the "+
			"#777 b′ exemption (keep it out of ExtrasFreeSpecFields), or prove it extras-free, and list it in b777Consumers",
			unknown)
	}
	// The guard must SEE the consumers it exists for, or it proves nothing.
	for _, must := range []string{"resolveApiRefFn", "mergeExtras", "resolveWidgetData", "resolveResourceRefs", "mergeRequestWins"} {
		if !contains(got, must) {
			t.Fatalf("B10 PRECONDITION: the taint walk no longer finds %q (found %v) — the guard is blind", must, got)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// b777TaintedCalls returns, sorted and de-duplicated, the callee name of every
// call in file whose arguments mention a tainted value. Taint sources, per
// function: a parameter named opts or extras, a parameter named ds of map type
// (the extras-bearing dict), and `opts.Extras`; any local assigned from an
// expression that mentions a tainted value is tainted too (fixpoint).
func b777TaintedCalls(t *testing.T, file string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	seen := map[string]struct{}{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		// The orchestration only: Resolve, and resolveApiRef (where the extras
		// become apiRefEff). Calls INSIDE a gated consumer (resolveWidgetData,
		// resolveResourceRefs) are behind that consumer's spec field already.
		if !ok || fn.Body == nil || !b777Walked[fn.Name.Name] {
			continue
		}
		tainted := map[string]bool{}
		for _, p := range fn.Type.Params.List {
			for _, n := range p.Names {
				switch n.Name {
				case "opts", "extras", "ds", "request":
					tainted[n.Name] = true
				}
			}
		}
		if len(tainted) == 0 {
			continue
		}
		mentions := func(e ast.Node) bool {
			hit := false
			ast.Inspect(e, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && tainted[id.Name] {
					hit = true
				}
				return !hit
			})
			return hit
		}
		for changed := true; changed; {
			changed = false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, lhs := range as.Lhs {
					id, ok := lhs.(*ast.Ident)
					if !ok || id.Name == "_" || tainted[id.Name] {
						continue
					}
					rhs := as.Rhs[0]
					if len(as.Rhs) == len(as.Lhs) {
						rhs = as.Rhs[i]
					}
					if mentions(rhs) {
						tainted[id.Name] = true
						changed = true
					}
				}
				return true
			})
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, a := range call.Args {
				if mentions(a) {
					if name := b777CalleeName(call.Fun); name != "" && !b777Benign[name] {
						seen[name] = struct{}{}
					}
					break
				}
			}
			return true
		})
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var b777Walked = map[string]bool{"Resolve": true, "resolveApiRef": true}

// b777Benign are calls that read a tainted value but cannot carry it into the
// body: logging, the stage-error text of a failed resolve (it carries the
// widget's identity, not extras), length/slicing builtins, and the CR accessors
// called with opts.In.Object (the CR, not the extras — opts is tainted as a
// whole, so its In field shows up too).
var b777Benign = map[string]bool{
	"len": true, "make": true, "append": true,
	"log.Error": true, "log.Warn": true, "log.Debug": true, "log.Info": true,
	"slog.Any": true, "slog.String": true, "slog.Int64": true, "slog.Int": true,
	"loggerAttr": true, "stageObject": true, "time.Since": true,
	"maps.SetNestedField": true, "maps.StructSliceToMapSlice": true,
	"GetApiRef": true, "GetApiRefExtras": true, "GetResourcesRefsExtras": true,
	"DeclaredIdentity": true, "cache.WrapResolveStage": true,
	"validateObjectStatusFn": true, "xcontext.Logger": true,
	// the logger's With(loggerAttr(opts.In.Object)), and errors.As on a
	// stage error (err is tainted only because it comes back from a tainted call)
	"With": true, "errors.As": true,
}

func b777CalleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			return x.Name + "." + f.Sel.Name
		}
		return f.Sel.Name
	}
	return ""
}
