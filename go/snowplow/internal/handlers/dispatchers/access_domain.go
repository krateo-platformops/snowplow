// access_domain.go — v7 UAF-scope digest, Step 2a: the identity-free
// access-domain deriver D.
//
// WHAT THIS IS. D(widget, RA-chain) is a SET of typed access "classes"
// describing WHICH per-identity RBAC checks a cell's resolve COULD ever make,
// derived PURELY from the widget CR spec + its RESTAction chain — before any
// resolve, carrying NO identity, answering NO check. It is the identity-free
// half of the v7 digest design (§3.1). It exists so a later step can fold D
// into a per-identity digest and decide when two identities may share a cell;
// D ITSELF must never enter a cache key, change any behaviour, be wired into
// dispatch/seed/resolve, or evaluate RBAC. This file adds a pure function, its
// canonical encoding, and nothing else. It is deliberately UNwired.
//
// WHY HERE. The design asks for one function conceptually shared by
// dispatch / seed / subscription / raFullList. It lives beside
// dispatchCacheLookupKey (helpers.go) in package dispatchers so all four could
// call it without a new import edge — but no caller is wired yet (Step 2a).
//
// TRACED against origin/main (HEAD a6b9d348, incl. #256 ed466daf / #264
// a6b9d348). The RBAC checks a resolve makes are exactly:
//   - dispatch reads:   filterGetByRBAC / filterListByRBAC
//     (internal/resolvers/restactions/api/informer_dispatch_rbac.go,
//     internal_dispatch.go branch B/C) — EvaluateRBAC(verb get|list, GVR from
//     the parsed apiserver path, per-item namespace, per-item name).
//   - UAF refilter:     applyUserAccessFilterOnPig / evalSingle
//     (internal/resolvers/restactions/api/refilter.go) — EvaluateRBAC(uaf.verb,
//     uaf.group, uaf.resource|resourcesFrom, per-object ns, per-object name).
//   - widget resourcesRefs: rbac.UserCan(verb, group-resource, namespace)
//     (internal/resolvers/widgets/resourcesrefs/resolve.go) — no name.
//   - widget apiRef:     a GET of the referenced RESTAction CR, then D(that RA).
// The path→GVR shape parser mirrors cache.ParseAPIServerPathToDep verbatim for
// literal paths; templated paths are handled by a sentinel-aware skeleton
// parser here (ParseAPIServerPathToDep rejects any "${" path).
//
// IDENTITY-FREE + PURE. D is a deterministic function of spec CONTENT only, so
// it is memoisable per (UID, resourceVersion) of each CR in the chain. Templated
// segments (jq over request extras) are NOT evaluated — a templated
// group/resource segment becomes a wildcard, a templated namespace/name becomes
// an unknown (namespace-set / dropped name). Live extras are never substituted.

package dispatchers

import (
	"fmt"
	"sort"
	"strings"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// AccessWildcard is the wildcard token used in the group/resource/verb position
// of a class whose value the spec does not fix identity-free (a templated
// segment, or a resourcesFrom-discovered plural set). It is the RBAC rule
// wildcard "*".
const AccessWildcard = "*"

// tmplSentinel marks a path segment that came from a jq interpolation (a
// templated segment) during skeleton reconstruction. It is an ASCII control
// byte that never appears in a real apiserver path segment.
const tmplSentinel = "\x00"

// AccessClassKind is one of the three access-class kinds of design §3.1.
type AccessClassKind uint8

const (
	// ClassExact is a single concrete (verb, group, resource, namespace, name)
	// SubjectAccessReview — "one bit". Namespace/Name may be "" (cluster-scope /
	// no named object).
	ClassExact AccessClassKind = iota
	// ClassNamespaceSet is (verb, group, resource) checked per-object across an
	// unknown or multiple namespace set — the shape of a cluster-wide LIST
	// refilter, a UAF static-resource stanza, or a namespaced read whose
	// namespace is templated. Its answer is ⊤ or a set of namespaces.
	ClassNamespaceSet
	// ClassWildcard is (verb, group|*, resource|*): the group and/or resource is
	// itself unknown identity-free (a templated group/resource segment, or a
	// resourcesFrom-discovered plural set). Verb may also be "*" for a templated
	// resolve:true target.
	ClassWildcard
)

// AccessClass is one typed access class. Group "" is the core API group. For
// ClassExact, Namespace "" means cluster-scope and Name "" means "no single
// named object" (a collection verb, or a name the spec does not fix). Namespace
// and Name are meaningful only for ClassExact and are always "" otherwise.
type AccessClass struct {
	Kind      AccessClassKind
	Verb      string
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// canonical returns a stable, delimiter-safe encoding of the class. Fixed arity
// per kind; the field delimiter is US (0x1f), which never appears in a kube
// verb/group/resource/namespace/name. This is the token a later digest folds —
// this function computes NO hash.
func (c AccessClass) canonical() string {
	switch c.Kind {
	case ClassExact:
		return strings.Join([]string{"exact", c.Verb, c.Group, c.Resource, c.Namespace, c.Name}, "\x1f")
	case ClassNamespaceSet:
		return strings.Join([]string{"nsset", c.Verb, c.Group, c.Resource}, "\x1f")
	case ClassWildcard:
		return strings.Join([]string{"wild", c.Verb, c.Group, c.Resource}, "\x1f")
	default:
		return strings.Join([]string{"unknown", c.Verb, c.Group, c.Resource}, "\x1f")
	}
}

// String is a human-readable rendering; core group shows as "core".
func (c AccessClass) String() string {
	g := c.Group
	if g == "" {
		g = "core"
	}
	switch c.Kind {
	case ClassExact:
		return fmt.Sprintf("exact(%s %s/%s ns=%q name=%q)", c.Verb, g, c.Resource, c.Namespace, c.Name)
	case ClassNamespaceSet:
		return fmt.Sprintf("namespace-set(%s %s/%s)", c.Verb, g, c.Resource)
	case ClassWildcard:
		return fmt.Sprintf("wildcard(%s %s/%s)", c.Verb, g, c.Resource)
	default:
		return fmt.Sprintf("unknown(%s %s/%s)", c.Verb, g, c.Resource)
	}
}

// AccessEscape records a resolve step that is identity-dependent but that D
// cannot describe as an access class — a non-read (POST/PUT/PATCH/DELETE)
// apiserver step dispatched with the requester token (design §3.1 last row).
// It is RECORDED, never turned into a class: its presence tells a later digest
// that the domain is not fully classifiable, so the cell is not purely
// shareable on D alone. External non-read steps (endpointRef to a non-apiserver
// URL) are NOT escapes — they touch no apiserver RBAC — and add nothing.
type AccessEscape struct {
	// Verb is the kube verb the step maps to (create/update/patch/delete).
	Verb string
	// Group/Resource are the parsed apiserver GVR of the step, or "*" when the
	// path is templated and the GVR is unknown identity-free.
	Group    string
	Resource string
	// Step is the api-step name (spec-derived, identity-free) for attribution.
	Step string
	// Reason is a short, stable description of why the step escaped.
	Reason string
}

func (e AccessEscape) canonical() string {
	return strings.Join([]string{"escape", e.Verb, e.Group, e.Resource, e.Step, e.Reason}, "\x1f")
}

func (e AccessEscape) String() string {
	g := e.Group
	if g == "" {
		g = "core"
	}
	return fmt.Sprintf("escape(%s %s/%s step=%q: %s)", e.Verb, g, e.Resource, e.Step, e.Reason)
}

// AccessDomain is the canonical, sorted, deduplicated set of access classes a
// resolve could make, plus any escape markers. It is canonically encodable
// (Canonical) so a later step can digest it; it computes no hash and folds
// nothing.
type AccessDomain struct {
	Classes []AccessClass
	Escapes []AccessEscape
}

// HasClasses reports whether the domain derived any access class. (An
// escape-only domain has no classes.)
func (d AccessDomain) HasClasses() bool { return len(d.Classes) > 0 }

// IsEmpty reports whether the domain derived neither a class nor an escape.
func (d AccessDomain) IsEmpty() bool { return len(d.Classes) == 0 && len(d.Escapes) == 0 }

// Canonical is the stable encoding a later digest consumes: sorted class
// canonicals then sorted escape canonicals, each on its own RS-delimited line.
// Deterministic for a given spec content — the memoisation contract.
func (d AccessDomain) Canonical() string {
	parts := make([]string, 0, len(d.Classes)+len(d.Escapes))
	for _, c := range d.Classes {
		parts = append(parts, c.canonical())
	}
	for _, e := range d.Escapes {
		parts = append(parts, e.canonical())
	}
	return strings.Join(parts, "\x1e")
}

// String is a human-readable multi-line rendering (for logs / the measurement).
func (d AccessDomain) String() string {
	if d.IsEmpty() {
		return "AccessDomain{}"
	}
	var b strings.Builder
	b.WriteString("AccessDomain{\n")
	for _, c := range d.Classes {
		b.WriteString("  " + c.String() + "\n")
	}
	for _, e := range d.Escapes {
		b.WriteString("  " + e.String() + "\n")
	}
	b.WriteString("}")
	return b.String()
}

// AccessChainResolver resolves the CRs referenced by an apiRef or a resolve:true
// target so the deriver can recurse into D(referenced RA/widget). Purity is
// preserved: the output is a deterministic function of the resolved specs. A
// resolver that returns ok=false for a ref makes the deriver record only the
// direct GET class for that ref (no recursion) — the safe, information-losing
// direction.
type AccessChainResolver interface {
	// RESTActionByRef returns the RESTAction CR named by an apiRef / resolve
	// target (resource is typically "restactions"), or ok=false when it cannot
	// be resolved from the chain.
	RESTActionByRef(resource, namespace, name string) (*templatesv1.RESTAction, bool)
	// WidgetByRef returns the unstructured widget CR named by a resolve:true
	// target, or ok=false. Widgets have no typed Go spec; the map is the raw
	// unstructured CR.
	WidgetByRef(resource, namespace, name string) (map[string]any, bool)
}

// nilChainResolver resolves nothing — every ref loses its recursion. Callers
// that only want a CR's own classes (no chain) can pass it.
type nilChainResolver struct{}

func (nilChainResolver) RESTActionByRef(string, string, string) (*templatesv1.RESTAction, bool) {
	return nil, false
}
func (nilChainResolver) WidgetByRef(string, string, string) (map[string]any, bool) { return nil, false }

// NilChainResolver is a resolver that resolves no references.
var NilChainResolver AccessChainResolver = nilChainResolver{}

const accessDeriveMaxDepth = 8

// accessBuilder deduplicates classes and escapes by canonical key.
type accessBuilder struct {
	classes map[string]AccessClass
	escapes map[string]AccessEscape
}

func newAccessBuilder() *accessBuilder {
	return &accessBuilder{classes: map[string]AccessClass{}, escapes: map[string]AccessEscape{}}
}

func (b *accessBuilder) add(c AccessClass) { b.classes[c.canonical()] = c }

func (b *accessBuilder) exact(verb, group, resource, ns, name string) {
	b.add(AccessClass{Kind: ClassExact, Verb: verb, Group: group, Resource: resource, Namespace: ns, Name: name})
}
func (b *accessBuilder) nsSet(verb, group, resource string) {
	b.add(AccessClass{Kind: ClassNamespaceSet, Verb: verb, Group: group, Resource: resource})
}
func (b *accessBuilder) wildcard(verb, group, resource string) {
	b.add(AccessClass{Kind: ClassWildcard, Verb: verb, Group: group, Resource: resource})
}
func (b *accessBuilder) escape(e AccessEscape) { b.escapes[e.canonical()] = e }

func (b *accessBuilder) build() AccessDomain {
	classes := make([]AccessClass, 0, len(b.classes))
	for _, c := range b.classes {
		classes = append(classes, c)
	}
	sort.Slice(classes, func(i, j int) bool { return classes[i].canonical() < classes[j].canonical() })
	escapes := make([]AccessEscape, 0, len(b.escapes))
	for _, e := range b.escapes {
		escapes = append(escapes, e)
	}
	sort.Slice(escapes, func(i, j int) bool { return escapes[i].canonical() < escapes[j].canonical() })
	return AccessDomain{Classes: classes, Escapes: escapes}
}

// deriveState carries the recursion guard across a chain.
type deriveState struct {
	resolver AccessChainResolver
	b        *accessBuilder
	visited  map[string]bool
	depth    int
}

func visitKey(resource, ns, name string) string { return resource + "/" + ns + "/" + name }

// DeriveRESTActionAccessDomain derives D for a RESTAction and its chain (its
// resolve:true targets). Pure and identity-free. resolver may be
// NilChainResolver to derive only this RA's own classes.
func DeriveRESTActionAccessDomain(ra *templatesv1.RESTAction, resolver AccessChainResolver) AccessDomain {
	if resolver == nil {
		resolver = NilChainResolver
	}
	st := &deriveState{resolver: resolver, b: newAccessBuilder(), visited: map[string]bool{}}
	st.restAction(ra)
	return st.b.build()
}

// DeriveWidgetAccessDomain derives D for a widget CR (unstructured map) and its
// chain: the apiRef GET + D(referenced RA), and its resourcesRefs /
// resourcesRefsTemplate checks. Pure and identity-free.
func DeriveWidgetAccessDomain(widget map[string]any, resolver AccessChainResolver) AccessDomain {
	if resolver == nil {
		resolver = NilChainResolver
	}
	st := &deriveState{resolver: resolver, b: newAccessBuilder(), visited: map[string]bool{}}
	st.widget(widget)
	return st.b.build()
}

// widget derives the widget-side classes: apiRef (row 1) and resourcesRefs /
// resourcesRefsTemplate entries (row 2).
func (st *deriveState) widget(widget map[string]any) {
	if widget == nil || st.depth > accessDeriveMaxDepth {
		return
	}

	// Row 1 — widget apiRef: a GET of the referenced RESTAction CR, then D(RA).
	if ref, err := widgets.GetApiRef(widget); err == nil && ref.Name != "" {
		gv, _ := schema.ParseGroupVersion(ref.APIVersion)
		st.b.exact("get", gv.Group, ref.Resource, ref.Namespace, ref.Name)
		key := visitKey(ref.Resource, ref.Namespace, ref.Name)
		if !st.visited[key] {
			if ra, ok := st.resolver.RESTActionByRef(ref.Resource, ref.Namespace, ref.Name); ok && ra != nil {
				st.visited[key] = true
				st.depth++
				st.restAction(ra)
				st.depth--
			}
		}
	}

	// Row 2 — static resourcesRefs entries: rbac.UserCan(verb, GR, namespace).
	if refs, err := widgets.GetResourcesRefs(widget); err == nil {
		for i := range refs {
			st.resourceRef(refs[i].APIVersion, refs[i].Resource, refs[i].Namespace, refs[i].Verb)
		}
	}
	// Row 2 — templated resourcesRefsTemplate entries. The Template.Verb is used
	// verbatim (createResourceRef does not jq-evaluate it); apiVersion / resource
	// / namespace are jq-evaluated, so a "${...}" value is templated → wildcard /
	// unknown here (we do NOT evaluate the jq).
	if tmpls, err := widgets.GetResourcesRefsTemplate(widget); err == nil {
		for i := range tmpls {
			t := tmpls[i].Template
			st.resourceRef(t.APIVersion, t.Resource, t.Namespace, t.Verb)
		}
	}
}

// resourceRef adds the class for one (templated-or-literal) widget resource ref.
// The runtime check is rbac.UserCan(verb, group-resource, namespace) with NO
// name (resourcesrefs/resolve.go), one per mapped verb.
func (st *deriveState) resourceRef(apiVersion, resource, namespace, restVerb string) {
	if resource == "" && apiVersion == "" {
		return
	}
	grpTmpl := isTemplated(apiVersion)
	resTmpl := isTemplated(resource)
	nsTmpl := isTemplated(namespace)
	group := ""
	if !grpTmpl {
		gv, _ := schema.ParseGroupVersion(apiVersion)
		group = gv.Group
	}
	for _, verb := range widgetRefKubeVerbs(restVerb) {
		switch {
		case grpTmpl || resTmpl:
			g := group
			if grpTmpl {
				g = AccessWildcard
			}
			r := resource
			if resTmpl {
				r = AccessWildcard
			}
			st.b.wildcard(verb, g, r)
		case nsTmpl:
			st.b.nsSet(verb, group, resource)
		default:
			st.b.exact(verb, group, resource, namespace, "")
		}
	}
}

// restAction derives D for a RESTAction: one contribution per api-step.
func (st *deriveState) restAction(ra *templatesv1.RESTAction) {
	if ra == nil || st.depth > accessDeriveMaxDepth {
		return
	}
	for _, step := range ra.Spec.API {
		st.restActionStep(step)
	}
}

func (st *deriveState) restActionStep(step *templatesv1.API) {
	if step == nil {
		return
	}
	httpVerb := "GET"
	if step.Verb != nil && *step.Verb != "" {
		httpVerb = strings.ToUpper(*step.Verb)
	}
	isRead := httpVerb == "GET" || httpVerb == "HEAD"

	// Row: UAF stanza. The per-identity check for a UAF step is the refilter
	// (EvaluateRBAC on uaf.verb/group/resource per object) — the SA-credentialed
	// cluster read is identity-free, so the stanza is the ONLY class this step
	// contributes. resourcesFrom → wildcard (plurals discovered at runtime);
	// static resource → namespace-set (checked per-object namespace).
	if step.UserAccessFilter != nil {
		uaf := step.UserAccessFilter
		v := strings.ToLower(uaf.Verb)
		if uaf.ResourcesFrom != "" {
			st.b.wildcard(v, uaf.Group, AccessWildcard)
		} else {
			st.b.nsSet(v, uaf.Group, uaf.Resource)
		}
		return
	}

	pi := classifyStepPath(step.Path)

	if !pi.apiserver {
		// endpointRef / external / opaque-templated / bare group-discovery: no
		// in-process RBAC check → nothing (ExternalTouchedSink declines these;
		// group discovery is SA-served identity-free).
		return
	}

	if !isRead {
		// Non-read apiserver step dispatched with the requester token → escape.
		st.b.escape(AccessEscape{
			Verb:     httpVerbToKube(httpVerb),
			Group:    pi.groupOrStar(),
			Resource: pi.resourceOrStar(),
			Step:     step.Name,
			Reason:   "non-read apiserver step dispatched with the requester token",
		})
		return
	}

	// Read step → dispatch RBAC classes (filterGet / filterList shape).
	st.addReadClasses(pi)

	// Row: resolve:true nested target — the fetched RA/widget is run through the
	// resolver in-process, so its own D is reachable. The direct GET class was
	// added by addReadClasses above; add D(target) (or (*,*,*) if templated).
	if step.Resolve != nil && *step.Resolve {
		st.resolveNested(pi)
	}
}

// addReadClasses maps a parsed apiserver read path to its access class(es).
func (st *deriveState) addReadClasses(pi pathInfo) {
	// Group/resource unknown identity-free → wildcard (rule pattern).
	if pi.groupTmpl || pi.resourceTmpl || pi.resource == "" {
		verb := "list"
		if pi.hasName {
			verb = "get"
		}
		g := pi.group
		if pi.groupTmpl {
			g = AccessWildcard
		}
		r := pi.resource
		if pi.resourceTmpl || r == "" {
			r = AccessWildcard
		}
		st.b.wildcard(verb, g, r)
		return
	}

	if pi.hasName {
		// GET-by-name.
		if !pi.nsTmpl && !pi.nameTmpl {
			st.b.exact("get", pi.group, pi.resource, pi.namespace, pi.name)
			return
		}
		if !pi.nsTmpl && pi.namespace != "" {
			// namespace known, name templated → drop the name (unconstrained).
			st.b.exact("get", pi.group, pi.resource, pi.namespace, "")
			return
		}
		if pi.namespaced {
			// namespace templated → per-object namespace set.
			st.b.nsSet("get", pi.group, pi.resource)
			return
		}
		// cluster-scoped, name templated → get, name unconstrained.
		st.b.exact("get", pi.group, pi.resource, "", "")
		return
	}

	// LIST (no name).
	if !pi.nsTmpl && pi.namespace != "" {
		// namespaced list, namespace known.
		st.b.exact("list", pi.group, pi.resource, pi.namespace, "")
		return
	}
	// cluster-wide list, or templated namespace → per-object namespace set.
	st.b.nsSet("list", pi.group, pi.resource)
}

// resolveNested recurses into D(target) for a resolve:true api-step. The target
// is a direct apiserver path to a snowplow RESTAction or Widget CR.
func (st *deriveState) resolveNested(pi pathInfo) {
	if pi.groupTmpl || pi.resourceTmpl || pi.nameTmpl || pi.name == "" {
		// Templated target: cannot name the CR identity-free → (*,*,*).
		st.b.wildcard(AccessWildcard, AccessWildcard, AccessWildcard)
		return
	}
	key := visitKey(pi.resource, pi.namespace, pi.name)
	if st.visited[key] || st.depth > accessDeriveMaxDepth {
		return
	}
	switch pi.resource {
	case "restactions":
		if ra, ok := st.resolver.RESTActionByRef(pi.resource, pi.namespace, pi.name); ok && ra != nil {
			st.visited[key] = true
			st.depth++
			st.restAction(ra)
			st.depth--
		}
	default:
		// widgets.* (forms/tables/…): recurse into the widget's own D.
		if w, ok := st.resolver.WidgetByRef(pi.resource, pi.namespace, pi.name); ok && w != nil {
			st.visited[key] = true
			st.depth++
			st.widget(w)
			st.depth--
		}
	}
}

// pathInfo is the shape of an apiserver read path, identity-free. A templated
// segment sets the corresponding *Tmpl flag; the concrete value is then unknown.
type pathInfo struct {
	apiserver    bool
	group        string
	groupTmpl    bool
	resource     string
	resourceTmpl bool
	namespaced   bool
	namespace    string
	nsTmpl       bool
	name         string
	nameTmpl     bool
	hasName      bool
}

func (pi pathInfo) groupOrStar() string {
	if pi.groupTmpl {
		return AccessWildcard
	}
	return pi.group
}
func (pi pathInfo) resourceOrStar() string {
	if pi.resourceTmpl || pi.resource == "" {
		return AccessWildcard
	}
	return pi.resource
}

// classifyStepPath classifies an api-step path into pathInfo. Literal paths use
// cache.ParseAPIServerPathToDep verbatim (the TRACED production parser).
// Templated ("${…}") paths are reconstructed into a sentinel-marked skeleton and
// parsed here — ParseAPIServerPathToDep rejects any "${" path.
func classifyStepPath(path string) pathInfo {
	if path == "" {
		return pathInfo{}
	}
	if !strings.Contains(path, "${") {
		gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path)
		if !ok {
			// external URL, bare group-discovery, or malformed → no class.
			return pathInfo{}
		}
		return pathInfo{
			apiserver: true,
			group:     gvr.Group,
			resource:  gvr.Resource,
			namespace: ns,
			name:      name,
			hasName:   name != "",
		}
	}
	return parseTemplatedSkeleton(reconstructPathSkeleton(path))
}

// parseTemplatedSkeleton parses a reconstructed skeleton (literal segments with
// tmplSentinel marking interpolated ones). It anchors on the first "/apis/" or
// "/api/" substring — matching the calibration policy that reproduces the
// design's ~58/91 expectation — and mirrors ParseAPIServerPathToDep's segment
// layout, marking sentinel segments templated.
//
// SHADOW-PARITY ACCEPTANCE ITEM (leak-direction under-approximation): if a whole
// "/apis/" or "/api/" prefix is itself produced inside ${…} (no literal anchor
// survives skeleton reconstruction), the step is classified external → no class
// — an UNDER-approximation. Harmless while D is dark, but this MUST be caught by
// shadow parity before any digest folds D into a cache key.
func parseTemplatedSkeleton(sk string) pathInfo {
	// Strip a trailing query string if the skeleton carried one literally.
	if i := strings.IndexByte(sk, '?'); i >= 0 {
		sk = sk[:i]
	}
	idxApis := strings.Index(sk, "/apis/")
	idxApi := strings.Index(sk, "/api/")
	var start int
	var apis bool
	switch {
	case idxApis >= 0 && (idxApi < 0 || idxApis <= idxApi):
		start, apis = idxApis, true
	case idxApi >= 0:
		start, apis = idxApi, false
	default:
		return pathInfo{} // no apiserver prefix in the skeleton → external.
	}

	var rest string
	if apis {
		rest = sk[start+len("/apis/"):]
	} else {
		rest = sk[start+len("/api/"):]
	}
	rest = strings.TrimRight(rest, "/")
	parts := strings.Split(rest, "/")

	pi := pathInfo{apiserver: true}
	setName := func(seg string) {
		pi.hasName = true
		if segTemplated(seg) {
			pi.nameTmpl = true
		} else {
			pi.name = seg
		}
	}
	setNS := func(seg string) {
		if segTemplated(seg) {
			pi.nsTmpl = true
		} else {
			pi.namespace = seg
		}
	}

	if apis {
		// /apis/<group>/<version>/<resource>[/<name>]  or
		// /apis/<group>/<version>/namespaces/<ns>/<resource>[/<name>]
		if len(parts) < 3 {
			pi.groupTmpl = true // too little structure → treat group as unknown.
			return pi
		}
		if segTemplated(parts[0]) {
			pi.groupTmpl = true
		} else {
			pi.group = parts[0]
		}
		if len(parts) >= 5 && parts[2] == "namespaces" {
			pi.namespaced = true
			setNS(parts[3])
			if segTemplated(parts[4]) {
				pi.resourceTmpl = true
			} else {
				pi.resource = parts[4]
			}
			if len(parts) >= 6 {
				setName(parts[5])
			}
			return pi
		}
		if segTemplated(parts[2]) {
			pi.resourceTmpl = true
		} else {
			pi.resource = parts[2]
		}
		if len(parts) >= 4 {
			setName(parts[3])
		}
		return pi
	}

	// core group: /api/<version>/<resource>[/<name>]  or
	// /api/<version>/namespaces/<ns>/<resource>[/<name>]
	if len(parts) < 2 {
		pi.resourceTmpl = true
		return pi
	}
	if len(parts) >= 4 && parts[1] == "namespaces" {
		pi.namespaced = true
		setNS(parts[2])
		if segTemplated(parts[3]) {
			pi.resourceTmpl = true
		} else {
			pi.resource = parts[3]
		}
		if len(parts) >= 5 {
			setName(parts[4])
		}
		return pi
	}
	if segTemplated(parts[1]) {
		pi.resourceTmpl = true
	} else {
		pi.resource = parts[1]
	}
	if len(parts) >= 3 {
		setName(parts[2])
	}
	return pi
}

func segTemplated(seg string) bool { return strings.Contains(seg, tmplSentinel) }

// reconstructPathSkeleton turns a possibly-jq-templated path into a literal
// skeleton where each interpolated segment is replaced by tmplSentinel. It
// handles the two shapes the corpus uses: "+"-concatenation of string literals
// and jq "\(…)" string interpolation. It does NOT evaluate any jq.
func reconstructPathSkeleton(path string) string {
	if !strings.Contains(path, "${") {
		return path
	}
	var out strings.Builder
	n := len(path)
	i := 0
	for i < n {
		j := strings.Index(path[i:], "${")
		if j < 0 {
			out.WriteString(path[i:])
			break
		}
		out.WriteString(path[i : i+j]) // literal text outside the ${…} block.
		blockStart := i + j + 2
		k := blockStart
		depth := 1
		inStr := false
		var q byte
		for k < n && depth > 0 {
			c := path[k]
			if inStr {
				if c == '\\' {
					k += 2
					continue
				}
				if c == q {
					inStr = false
				}
				k++
				continue
			}
			switch c {
			case '"', '\'':
				inStr = true
				q = c
			case '{':
				depth++
			case '}':
				depth--
			}
			k++
		}
		inner := path[blockStart : k-1]
		out.WriteString(blockToSkeleton(inner))
		i = k
	}
	return out.String()
}

// blockToSkeleton extracts the literal skeleton of one jq expression: its
// double-quoted string literals in order, with a tmplSentinel wherever a
// non-trivial jq expression sits between/around them (a "+ .foo +" gap, a
// leading/trailing interpolation, or a "\(…)" inside a literal).
func blockToSkeleton(inner string) string {
	var out strings.Builder
	n := len(inner)
	i := 0
	gapStart := 0
	anyLit := false
	for i < n {
		if inner[i] != '"' {
			i++
			continue
		}
		if hasJQContent(inner[gapStart:i]) {
			out.WriteString(tmplSentinel)
		}
		// Read the string literal, honouring \( … ) interpolation and escapes.
		j := i + 1
		for j < n {
			d := inner[j]
			if d == '\\' && j+1 < n {
				if inner[j+1] == '(' {
					depth := 1
					j += 2
					for j < n && depth > 0 {
						switch inner[j] {
						case '(':
							depth++
						case ')':
							depth--
						}
						j++
					}
					out.WriteString(tmplSentinel)
					continue
				}
				out.WriteByte(inner[j+1]) // simplistic unescape (\/ \" \\ …).
				j += 2
				continue
			}
			if d == '"' {
				j++
				break
			}
			out.WriteByte(d)
			j++
		}
		anyLit = true
		i = j
		gapStart = i
	}
	if hasJQContent(inner[gapStart:]) {
		out.WriteString(tmplSentinel)
	}
	if !anyLit {
		return tmplSentinel
	}
	return out.String()
}

// hasJQContent reports whether s carries non-trivial jq (anything other than
// whitespace and the "+" concatenation operator). A "+"-only gap is pure
// concatenation and contributes no segment.
func hasJQContent(s string) bool {
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r', '+':
			continue
		default:
			return true
		}
	}
	return false
}

// httpVerbToKube maps an HTTP method to its Kubernetes RBAC verb.
func httpVerbToKube(httpVerb string) string {
	switch strings.ToUpper(httpVerb) {
	case "POST":
		return "create"
	case "PUT":
		return "update"
	case "PATCH":
		return "patch"
	case "DELETE":
		return "delete"
	case "HEAD", "GET":
		return "get"
	default:
		return strings.ToLower(httpVerb)
	}
}

// widgetRefKubeVerbs mirrors resourcesrefs.mapVerbs (unexported): a known REST
// method maps to its one kube verb; an empty/unknown verb maps to the full
// {create,update,patch,delete,get} set (all kubeToREST keys). Returned sorted
// for determinism (the runtime set is identical; only its order is arbitrary).
func widgetRefKubeVerbs(restVerb string) []string {
	switch strings.ToUpper(restVerb) {
	case "POST":
		return []string{"create"}
	case "PUT":
		return []string{"update"}
	case "PATCH":
		return []string{"patch"}
	case "DELETE":
		return []string{"delete"}
	case "GET":
		return []string{"get"}
	default:
		return []string{"create", "delete", "get", "patch", "update"}
	}
}

func isTemplated(s string) bool { return strings.Contains(s, "${") }
