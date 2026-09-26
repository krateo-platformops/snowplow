package dispatchers

import (
	"encoding/json"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/utils/ptr"
)

// The golden tests below assert the EXACT derived AccessDomain (rendered as its
// canonical, sorted, human-readable String) for a hand-built widget/RA spec per
// row of design §3.1's derivation table. They are the identity-free contract:
// each covers one row, and the tricky rows (resourcesFrom→wildcard, templated
// segment→wildcard, resolve:true nesting, endpointRef→nothing,
// non-GET-token-step→escape) get their own case.

// stringResolver resolves an apiRef / resolve:true target from an in-memory map.
type stringResolver struct {
	ras     map[string]*templatesv1.RESTAction
	widgets map[string]map[string]any
}

func (r stringResolver) RESTActionByRef(resource, ns, name string) (*templatesv1.RESTAction, bool) {
	ra, ok := r.ras[visitKey(resource, ns, name)]
	return ra, ok
}
func (r stringResolver) WidgetByRef(resource, ns, name string) (map[string]any, bool) {
	w, ok := r.widgets[visitKey(resource, ns, name)]
	return w, ok
}

func adStep(name, path, verb string) *templatesv1.API {
	s := &templatesv1.API{Name: name, Path: path}
	if verb != "" {
		s.Verb = ptr.To(verb)
	}
	return s
}

func ra(steps ...*templatesv1.API) *templatesv1.RESTAction {
	return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: steps}}
}

// assertDomain compares the domain's canonical class+escape lines against want.
func assertDomain(t *testing.T, got AccessDomain, wantClasses []string, wantEscapes []string) {
	t.Helper()
	gotC := make([]string, 0, len(got.Classes))
	for _, c := range got.Classes {
		gotC = append(gotC, c.String())
	}
	gotE := make([]string, 0, len(got.Escapes))
	for _, e := range got.Escapes {
		gotE = append(gotE, e.String())
	}
	if !equalStrings(gotC, wantClasses) {
		t.Errorf("classes mismatch\n got: %v\nwant: %v", gotC, wantClasses)
	}
	if !equalStrings(gotE, wantEscapes) {
		t.Errorf("escapes mismatch\n got: %v\nwant: %v", gotE, wantEscapes)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Row: Non-UAF GET-by-name, literal namespaced path → exact(get,g,r,ns,name).
func TestDerive_GetByName_Namespaced(t *testing.T) {
	d := DeriveRESTActionAccessDomain(
		ra(adStep("get1", "/apis/apps/v1/namespaces/team-a/deployments/web", "GET")),
		NilChainResolver,
	)
	assertDomain(t, d, []string{`exact(get apps/deployments ns="team-a" name="web")`}, nil)
}

// Row: Non-UAF GET-by-name, cluster-scoped literal path → exact(get,g,r,"",name).
func TestDerive_GetByName_ClusterScoped(t *testing.T) {
	d := DeriveRESTActionAccessDomain(
		ra(adStep("crd", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.io", "GET")),
		NilChainResolver,
	)
	assertDomain(t, d, []string{`exact(get apiextensions.k8s.io/customresourcedefinitions ns="" name="foos.example.io")`}, nil)
}

// Row: Non-UAF LIST namespaced (literal ns) → exact(list,g,r,ns,"").
func TestDerive_List_Namespaced(t *testing.T) {
	d := DeriveRESTActionAccessDomain(
		ra(adStep("list1", "/api/v1/namespaces/team-a/configmaps", "GET")),
		NilChainResolver,
	)
	assertDomain(t, d, []string{`exact(list core/configmaps ns="team-a" name="")`}, nil)
}

// Row: Non-UAF LIST cluster-wide → namespace-set(list,g,r).
func TestDerive_List_ClusterWide(t *testing.T) {
	d := DeriveRESTActionAccessDomain(
		ra(adStep("list2", "/apis/kagent.dev/v1alpha2/agents", "GET")),
		NilChainResolver,
	)
	assertDomain(t, d, []string{`namespace-set(list kagent.dev/agents)`}, nil)
}

// Row: UAF stanza, static resource → namespace-set(uaf.verb,uaf.group,uaf.resource).
// The path is irrelevant (the per-identity check is the refilter).
func TestDerive_UAF_Static(t *testing.T) {
	s := adStep("uaf", "/apis/kagent.dev/v1alpha2/agents", "GET")
	s.UserAccessFilter = &templatesv1.UserAccessFilterSpec{Verb: "list", Group: "kagent.dev", Resource: "agents"}
	d := DeriveRESTActionAccessDomain(ra(s), NilChainResolver)
	assertDomain(t, d, []string{`namespace-set(list kagent.dev/agents)`}, nil)
}

// Tricky row: UAF stanza with resourcesFrom → wildcard(uaf.verb,uaf.group,*).
func TestDerive_UAF_ResourcesFrom(t *testing.T) {
	s := adStep("uaf", "/apis/composition.krateo.io/v1/compositions", "GET")
	s.UserAccessFilter = &templatesv1.UserAccessFilterSpec{
		Verb: "get", Group: "composition.krateo.io", ResourcesFrom: "[ (.crds // [])[] | .plural ]",
	}
	d := DeriveRESTActionAccessDomain(ra(s), NilChainResolver)
	assertDomain(t, d, []string{`wildcard(get composition.krateo.io/*)`}, nil)
}

// Tricky row: templated NAMESPACE segment on a namespaced list → namespace-set
// (namespace unknown identity-free; the group/resource stay literal).
func TestDerive_TemplatedNamespace_List(t *testing.T) {
	p := `${ "/apis/observability.krateo.io/v1alpha1/namespaces/" + (.namespace // "krateo-system") + "/troubleshootingreports" }`
	d := DeriveRESTActionAccessDomain(ra(adStep("obs", p, "GET")), NilChainResolver)
	assertDomain(t, d, []string{`namespace-set(list observability.krateo.io/troubleshootingreports)`}, nil)
}

// Tricky row: templated NAMESPACE + NAME on a namespaced get → namespace-set.
func TestDerive_TemplatedNamespaceAndName_Get(t *testing.T) {
	p := `${ "/apis/observability.krateo.io/v1alpha1/namespaces/" + (.namespace // "krateo-system") + "/alerts/" + .name }`
	d := DeriveRESTActionAccessDomain(ra(adStep("alert", p, "GET")), NilChainResolver)
	assertDomain(t, d, []string{`namespace-set(get observability.krateo.io/alerts)`}, nil)
}

// Tricky row: literal namespace but templated NAME → exact with name dropped
// (namespace known, name unconstrained).
func TestDerive_LiteralNamespace_TemplatedName_Get(t *testing.T) {
	p := `${ "/apis/apps/v1/namespaces/krateo-system/deployments/" + ((.name // "") | gsub("[^a-zA-Z0-9.-]"; "")) }`
	d := DeriveRESTActionAccessDomain(ra(adStep("comp", p, "GET")), NilChainResolver)
	assertDomain(t, d, []string{`exact(get apps/deployments ns="krateo-system" name="")`}, nil)
}

// Tricky row: templated RESOURCE segment (group literal) → wildcard(verb,g,*).
func TestDerive_TemplatedResource_Wildcard(t *testing.T) {
	p := `${ "/apis/composition.krateo.io/" + .version + "/namespaces/" + .namespace + "/" + .plural }`
	d := DeriveRESTActionAccessDomain(ra(adStep("cr", p, "GET")), NilChainResolver)
	assertDomain(t, d, []string{`wildcard(list composition.krateo.io/*)`}, nil)
}

// Tricky row: templated GROUP segment → wildcard(verb,*,*).
func TestDerive_TemplatedGroup_Wildcard(t *testing.T) {
	p := `${ "/apis/" + ((.compdef.status.apiVersion) // "none.krateo.io/v1") + "/" + ((.compdef.status.resource) // "none") }`
	d := DeriveRESTActionAccessDomain(ra(adStep("bp", p, "GET")), NilChainResolver)
	assertDomain(t, d, []string{`wildcard(list */*)`}, nil)
}

// Tricky row: endpointRef / external → nothing. A non-apiserver literal path and
// a fully-dynamic (non-apiserver) templated path both derive an empty domain.
func TestDerive_External_Nothing(t *testing.T) {
	s := adStep("gh", "/orgs/krateo-platformops/repos", "GET")
	s.EndpointRef = &templatesv1.Reference{Name: "github", Namespace: "krateo-system"}
	d := DeriveRESTActionAccessDomain(ra(s), NilChainResolver)
	assertDomain(t, d, nil, nil)
	if !d.IsEmpty() {
		t.Fatalf("expected empty domain, got %s", d)
	}

	// A clickhouse-style templated query path (no /api or /apis) → nothing.
	ch := `${ (.range // "24h") as $r | "/?query=SELECT%20count()%20FROM%20otel" }`
	d2 := DeriveRESTActionAccessDomain(ra(adStep("ch", ch, "GET")), NilChainResolver)
	assertDomain(t, d2, nil, nil)
}

// Tricky row: non-GET step using the requester token → escape marker (no class).
// A SAR POST to the apiserver escapes; an external POST (helm-render) does not.
func TestDerive_NonGetToken_Escape(t *testing.T) {
	sar := adStep("sar", "/apis/authorization.k8s.io/v1/subjectaccessreviews", "POST")
	d := DeriveRESTActionAccessDomain(ra(sar), NilChainResolver)
	assertDomain(t, d, nil,
		[]string{`escape(create authorization.k8s.io/subjectaccessreviews step="sar": non-read apiserver step dispatched with the requester token)`})

	// External POST → nothing (not an escape; touches no apiserver RBAC).
	helm := adStep("render", "/render", "POST")
	helm.EndpointRef = &templatesv1.Reference{Name: "helm-render-endpoint", Namespace: "krateo-system"}
	d2 := DeriveRESTActionAccessDomain(ra(helm), NilChainResolver)
	assertDomain(t, d2, nil, nil)
}

// Tricky row: resolve:true nested target → exact get on target + D(target).
func TestDerive_ResolveTrue_Nesting(t *testing.T) {
	// The nested RA lists deployments cluster-wide.
	nested := ra(adStep("inner", "/apis/apps/v1/deployments", "GET"))
	res := stringResolver{ras: map[string]*templatesv1.RESTAction{
		visitKey("restactions", "krateo-system", "inner-ra"): nested,
	}}
	parent := adStep("outer", "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra", "GET")
	parent.Resolve = ptr.To(true)
	d := DeriveRESTActionAccessDomain(ra(parent), res)
	assertDomain(t, d, []string{
		`exact(get templates.krateo.io/restactions ns="krateo-system" name="inner-ra")`, // the direct GET of the RA CR
		`namespace-set(list apps/deployments)`,                                          // D(inner-ra)
	}, nil)
}

// Tricky row: resolve:true with a TEMPLATED target path → (*,*,*).
func TestDerive_ResolveTrue_Templated(t *testing.T) {
	s := adStep("outer", `${ "/apis/templates.krateo.io/v1/namespaces/" + .ns + "/restactions/" + .name }`, "GET")
	s.Resolve = ptr.To(true)
	d := DeriveRESTActionAccessDomain(ra(s), NilChainResolver)
	// The read-path class for a templated-namespace get-by-name of restactions is
	// namespace-set; resolve:true then adds the (*,*,*) wildcard for the target.
	assertDomain(t, d, []string{
		`namespace-set(get templates.krateo.io/restactions)`,
		`wildcard(* */*)`,
	}, nil)
}

// Row 1 (widget apiRef) + Row 2 (resourcesRefs): a widget derives the apiRef GET
// of its RESTAction, D(that RA), and its static resourcesRefs checks.
func TestDerive_Widget_ApiRefPlusResourcesRefs(t *testing.T) {
	widgetJSON := `{
      "apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Table",
      "metadata":{"name":"grants","namespace":"krateo-system"},
      "spec":{
        "apiRef":{"name":"list-grants","namespace":"krateo-system"},
        "resourcesRefs":{"items":[
          {"apiVersion":"widgets.templates.krateo.io/v1beta1","id":"g","name":"grant-form","namespace":"krateo-system","resource":"forms","verb":"GET"}
        ]}
      }}`
	var w map[string]any
	if err := json.Unmarshal([]byte(widgetJSON), &w); err != nil {
		t.Fatal(err)
	}
	// The apiRef'd RA is a UAF RA over rolebindings.
	uaf := adStep("rb", "/apis/rbac.authorization.k8s.io/v1/rolebindings", "GET")
	uaf.UserAccessFilter = &templatesv1.UserAccessFilterSpec{Verb: "list", Group: "rbac.authorization.k8s.io", Resource: "rolebindings"}
	res := stringResolver{ras: map[string]*templatesv1.RESTAction{
		visitKey("restactions", "krateo-system", "list-grants"): ra(uaf),
	}}
	d := DeriveWidgetAccessDomain(w, res)
	assertDomain(t, d, []string{
		`exact(get templates.krateo.io/restactions ns="krateo-system" name="list-grants")`, // apiRef GET
		`exact(get widgets.templates.krateo.io/forms ns="krateo-system" name="")`,          // resourcesRefs entry (no name)
		`namespace-set(list rbac.authorization.k8s.io/rolebindings)`,                       // D(apiRef'd RA)
	}, nil)
}

// Row 2 (resourcesRefsTemplate): the Template.Verb is literal; apiVersion /
// resource literal but namespace templated → namespace-set on the mapped verb.
func TestDerive_Widget_ResourcesRefsTemplate_TemplatedNamespace(t *testing.T) {
	widgetJSON := `{
      "apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Flex",
      "metadata":{"name":"del","namespace":"krateo-system"},
      "spec":{
        "resourcesRefs":{"items":[]},
        "resourcesRefsTemplate":[
          {"template":{"apiVersion":"rbac.authorization.k8s.io/v1","id":"d","name":"${ .current.bindingName }","namespace":"${ .current.namespace }","resource":"rolebindings","verb":"DELETE"}}
        ]
      }}`
	var w map[string]any
	if err := json.Unmarshal([]byte(widgetJSON), &w); err != nil {
		t.Fatal(err)
	}
	d := DeriveWidgetAccessDomain(w, NilChainResolver)
	assertDomain(t, d, []string{`namespace-set(delete rbac.authorization.k8s.io/rolebindings)`}, nil)
}

// resourcesRefsTemplate with a templated apiVersion → wildcard(verb,*,*).
func TestDerive_Widget_ResourcesRefsTemplate_TemplatedGroup(t *testing.T) {
	widgetJSON := `{
      "apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Flex",
      "metadata":{"name":"del2","namespace":"krateo-system"},
      "spec":{
        "resourcesRefs":{"items":[]},
        "resourcesRefsTemplate":[
          {"template":{"apiVersion":"${ .gvr.apiVersion }","id":"d","name":"${ .gvr.name }","namespace":"${ .gvr.namespace }","resource":"${ .gvr.resource }","verb":"DELETE"}}
        ]
      }}`
	var w map[string]any
	if err := json.Unmarshal([]byte(widgetJSON), &w); err != nil {
		t.Fatal(err)
	}
	d := DeriveWidgetAccessDomain(w, NilChainResolver)
	assertDomain(t, d, []string{`wildcard(delete */*)`}, nil)
}

// A resourcesRefs entry with an EMPTY verb maps to the full verb set (mirrors
// resourcesrefs.mapVerbs fallthrough): five exact classes.
func TestDerive_Widget_ResourcesRef_EmptyVerb_AllVerbs(t *testing.T) {
	widgetJSON := `{
      "apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Card",
      "metadata":{"name":"c","namespace":"krateo-system"},
      "spec":{"resourcesRefs":{"items":[
        {"apiVersion":"v1","id":"cm","name":"x","namespace":"krateo-system","resource":"configmaps"}
      ]}}}`
	var w map[string]any
	if err := json.Unmarshal([]byte(widgetJSON), &w); err != nil {
		t.Fatal(err)
	}
	d := DeriveWidgetAccessDomain(w, NilChainResolver)
	assertDomain(t, d, []string{
		`exact(create core/configmaps ns="krateo-system" name="")`,
		`exact(delete core/configmaps ns="krateo-system" name="")`,
		`exact(get core/configmaps ns="krateo-system" name="")`,
		`exact(patch core/configmaps ns="krateo-system" name="")`,
		`exact(update core/configmaps ns="krateo-system" name="")`,
	}, nil)
}

// Determinism / dedup: two steps yielding the same class collapse to one, and
// Canonical() is stable across builds.
func TestDerive_DedupAndCanonicalStable(t *testing.T) {
	d := DeriveRESTActionAccessDomain(ra(
		adStep("a", "/apis/kagent.dev/v1alpha2/agents", "GET"),
		adStep("b", "/apis/kagent.dev/v1alpha2/agents", "GET"),
	), NilChainResolver)
	if len(d.Classes) != 1 {
		t.Fatalf("expected 1 deduped class, got %d: %s", len(d.Classes), d)
	}
	if d.Canonical() != DeriveRESTActionAccessDomain(ra(adStep("a", "/apis/kagent.dev/v1alpha2/agents", "GET")), NilChainResolver).Canonical() {
		t.Fatalf("canonical not stable")
	}
}
