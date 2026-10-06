// inspect.go — the dispatch-free RESTAction read-set enumeration that backs
// the snowplow `GET /rbac` endpoint (design docs/restaction-rbac-endpoint-design.md).
//
// PURPOSE: core-provider needs the full set of (group, resource, verb) tuples
// that resolving a given RESTAction WILL read, so it can pre-generate the
// RBAC (Roles/RoleBindings) a user needs BEFORE the first `/call`. This file
// enumerates that read-set from the RA's `api[]` stages WITHOUT dispatching —
// no apiserver reads of the referenced data, no per-user creds. Every
// primitive it calls is existing snowplow code (the §3 reuse map); only the
// enumeration walk is new.
//
// ZERO dispatch-path modification: InspectReadSet is a read-only sibling of
// Resolve; it does NOT touch Resolve, the dispatcher, or the refilter. It
// lives in package `api` only because the primitives it reuses
// (topologicalSort, createRequestOptions, resolveUAFResources,
// discoveryClientFor) are package-private here — exporting one orchestrator is
// cleaner than exporting four internals.
//
// CLASSIFICATION mirrors resolveStageEndpoint's PRECEDENCE (resolve.go:370-392):
// uafActive is checked BEFORE the EndpointRef branch, so a stage with BOTH a
// userAccessFilter AND an endpointRef dispatches via the SA + refilters — it is
// a UAF stage, NOT an external one. The classification order therefore is:
//   - UserAccessFilter != nil      → EMIT one row per resolved plural, with
//                                    the UAF's own verb verbatim (refilter.go:260).
//                                    Takes precedence over EndpointRef.
//   - EndpointRef != nil (no UAF)  → OMIT (external; not an in-cluster read).
//   - otherwise (in-cluster GET)   → EMIT one `get` row for the stage's GVR.
//
// At inspect time the dict holds ONLY `extras`, seeded in the SAME SHAPE the
// dispatcher uses before the first stage — extras at TOP LEVEL
// (resolve.go, `dict = maps.DeepCopyJSON(opts.Extras)`), not nested under an
// "extras" key. #512: this comment used to claim "the same map the dispatcher
// would seed it with" while the code nested them, so `${ "/api/v1/namespaces/"
// + .nsName }` — the shape of the real fixture at
// testdata/widgets/button.extras.apiref.yaml:26 — rendered `.nsName` as null at
// enumeration time and as the caller's value at dispatch. No templated path had
// ever rendered a real value through this pass.
//
// A stage whose path templates off an UPSTREAM stage's output (not present in
// the seed dict) cannot be materialized → it is reported as UNRESOLVABLE (a
// fail-loud non-200 at the handler), never silently dropped.

package api

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/plumbing/maps"
)

// Resource is one (group, version, resource, namespace, verb) tuple the
// RESTAction's resolve would read. `name` is always "" — RBAC pre-generation
// grants at the resource level, not the object level. The full tuple is the
// dedupe key (design §4).
type Resource struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Verb      string `json:"verb"`
	// NonReadVerb flags a row whose Verb is not a read verb (get/list/watch —
	// uafVerbIsRead, refilter.go). Only a userAccessFilter stage can produce
	// one: its verb is threaded verbatim into rbac.EvaluateRBAC, so a UAF
	// authored (or written straight to etcd) with a write verb would otherwise
	// suggest a Role granting that write verb (#179). The verb is still emitted
	// verbatim — bounding it would corrupt the read-set for the legit
	// `verb: create` portal pickers (refilter.go warn-only rationale) — but the
	// flag lets the caller (core-provider) refuse to mint a write-granting Role.
	// omitempty: read rows omit it, so every existing response stays
	// byte-identical (design §4).
	NonReadVerb bool `json:"nonReadVerb,omitempty"`
}

// Unresolved names a stage the inspect pass could not enumerate from the
// empty (extras-only) dict, with a human reason. Any non-empty Unresolved
// slice makes the handler fail loud (non-200) — under-enumerating the read-set
// would silently under-grant RBAC at the first /call.
type Unresolved struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

// saRESTConfigForInspectFn is the package-private seam over the SA
// *rest.Config source the inspect pass uses for discovery-validation. Mirrors
// saRESTConfigForDiscoveryFn (discovery_dispatch.go:106): production wires
// dynamic.ServiceAccountRESTConfig (the CA-bearing in-cluster singleton — NOT
// a per-caller token client, which would re-introduce the #43 x509); the
// falsifier swaps in a builder pointed at an httptest TLS server with a
// synthetic CA.
var saRESTConfigForInspectFn = dynamic.ServiceAccountRESTConfig

// InspectReadSet walks `in.Spec.API` in topologicalSort order and returns the
// in-cluster (group, version, resource, verb) read-set that resolving `in`
// would touch, plus any stages that could not be enumerated from the
// extras-only dict. It dispatches NOTHING: paths are jq-evaluated against the
// empty dict (createRequestOptions), classified (ParseAPIServerPathToDep for
// resource paths, ParseAPIServerDiscoveryPath for bare discovery paths),
// discovery-validated under the SA *rest.Config, and emitted. UAF stages emit
// their own verb verbatim, fanned out one row per resolved plural.
//
// The returned []Resource is deduped over the full tuple and lexicographically
// sorted, so an unchanged RA yields a byte-identical response.
func InspectReadSet(ctx context.Context, in *templates.RESTAction, extras map[string]any) ([]Resource, []Unresolved, error) {
	log := xcontext.Logger(ctx)

	if in == nil {
		return nil, nil, fmt.Errorf("inspect: nil RESTAction")
	}

	stages := in.Spec.API
	// The inspect dict holds ONLY extras — the seed the dispatcher would
	// start the first stage with. Upstream stage outputs are deliberately
	// absent: a stage that needs them is UNRESOLVABLE (reported, not guessed).
	//
	// #512 — SHAPE PARITY WITH THE DISPATCHER IS THE POINT. The dispatcher does
	// `dict = maps.DeepCopyJSON(opts.Extras)` (resolve.go), putting extras at the
	// TOP LEVEL. This used to do `dict["extras"] = extras`, one level down, so a
	// template reading `.nsName` saw null here and the caller's value there — the
	// enumeration was computed against a path the dispatcher would never request.
	// Same copy helper as the dispatcher, so an extras value cannot be mutated
	// through the caller's map either.
	//
	// NOT seeded, deliberately: the dispatcher's `dict["slice"]` paging block.
	// /rbac takes no paging parameters, and a read-set is per-RESOURCE, not
	// per-page — a stage templating off `.slice.*` is therefore UNRESOLVABLE here
	// rather than silently enumerated against a guessed page.
	dict := map[string]any{}
	if extras != nil {
		dict = maps.DeepCopyJSON(extras)
	}

	// Index stages by name so we can walk them in dependency order.
	byName := make(map[string]*templates.API, len(stages))
	for _, s := range stages {
		byName[s.Name] = s
	}

	order, err := topologicalSort(stages)
	if err != nil {
		// A cyclic api[] never resolves at /call either — fail loud.
		return nil, nil, fmt.Errorf("inspect: %w", err)
	}

	var (
		out        []Resource
		unresolved []Unresolved
	)

	// discoveryClientFor is keyed on the SA *rest.Config pointer; acquire the
	// rc once. A build failure is fatal (we cannot validate any GVR).
	rc, rcErr := saRESTConfigForInspectFn()
	if rcErr != nil {
		return nil, nil, fmt.Errorf("inspect: ServiceAccount rest.Config: %w", rcErr)
	}

	for _, name := range order {
		stage := byName[name]
		if stage == nil {
			continue
		}

		// (1) UAF stages — classified FIRST, mirroring the dispatcher's
		// precedence (resolveStageEndpoint resolve.go:370-392 checks uafActive
		// BEFORE the EndpointRef branch). A UAF stage dispatches via the SA
		// (cluster-wide read) and refilters each returned object through
		// EvaluateRBAC(Verb: uaf.Verb, Group: uaf.Group, Resource: r) per
		// resolved plural r (refilter.go:254-285) — REGARDLESS of whether the
		// stage ALSO carries an endpointRef. So a UAF stage MUST emit its
		// read-set even when endpointRef is set; classifying EndpointRef first
		// would silently OMIT the UAF read-set (DEFECT 1: silent under-grant).
		// Emit exactly the refilter triple — one row per plural, uaf.Verb verbatim.
		if uaf := stage.UserAccessFilter; uaf != nil {
			resources, ok := resolveUAFResources(ctx, log, uaf, dict)
			if !ok {
				// resourcesFrom referenced upstream stage data not present in
				// the empty dict (or otherwise failed closed) — UNRESOLVABLE.
				unresolved = append(unresolved, Unresolved{
					Stage:  name,
					Reason: "userAccessFilter.resourcesFrom not materializable from empty dict (references upstream stage data)",
				})
				continue
			}
			rows, sErr := inspectUAFStage(rc, uaf, resources)
			if sErr != nil {
				unresolved = append(unresolved, Unresolved{Stage: name, Reason: sErr.Error()})
				continue
			}
			out = append(out, rows...)
			continue
		}

		// (2) Non-UAF EndpointRef stages are EXTERNAL — they dispatch through a
		// named Endpoint, not the in-cluster apiserver. They contribute no
		// in-cluster read-set row. Omitted (absent), NOT an error (design §7).
		// Checked AFTER UAF so a UAF+endpointRef stage is never omitted.
		if stage.EndpointRef != nil {
			continue
		}

		// (3) In-cluster GET stage. Evaluate the path against the empty dict
		// WITHOUT dispatching, then classify the concrete URI.
		rows, sErr := inspectInClusterStage(rc, stage, dict, log)
		if sErr != nil {
			unresolved = append(unresolved, Unresolved{Stage: name, Reason: sErr.Error()})
			continue
		}
		out = append(out, rows...)
	}

	out = dedupeSortResources(out)
	return out, unresolved, nil
}

// inspectInClusterStage materializes a non-UAF stage's concrete path(s) from
// the (extras-only) dict and classifies EACH one. A `dependsOn.iterator` stage
// expands to ONE RequestOptions per iterator element (createRequestOptions /
// jqutil.ForEach, setup.go:49) — at inspect time the iterator can be driven by
// `extras` — so we must classify EVERY opt, not just opts[0] (DEFECT 2: taking
// only the first opt under-enumerated an extras-driven per-namespace iterator).
// Per opt:
//   - resource path → one `get` row (core.go:30 bounds the HTTP-stage verb to
//     GET/HEAD; HEAD requires the same `get` RBAC verb, so `get` is exact).
//   - bare group-discovery path (/apis/<g>/<v>, /api/<v>) → ZERO rows
//     (resolvable, anonymous-readable catalogue, no per-resource RBAC).
//   - anything else (residual ${...}, upstream-dependent, non-kube) →
//     UNRESOLVABLE: the WHOLE stage fails loud (a partial read-set would
//     under-grant).
//
// The rows are deduped by the caller's dedupeSortResources, so the common
// constant-GVR iterator (every opt the same GVR, differing only by name)
// collapses to one row.
func inspectInClusterStage(rc *rest.Config, stage *templates.API, dict map[string]any, log *slog.Logger) ([]Resource, error) {
	opts := createRequestOptions(context.Background(), log, stage, dict)
	if len(opts) == 0 {
		return nil, fmt.Errorf("stage produced no request path")
	}

	var rows []Resource
	for _, opt := range opts {
		path := opt.Path

		// Resource path → GVR (the dominant case).
		if gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path); ok {
			if err := validateGVR(rc, gvr); err != nil {
				return nil, fmt.Errorf("discovery validation failed for %s: %w", gvr.String(), err)
			}
			// The RBAC verb is determined by the path SHAPE, not a constant:
			// a COLLECTION read (`.../resource`, no object name) is authorized
			// as `list`; a BY-NAME read (`.../resource/<name>`) as `get`. The
			// HTTP-stage method is CEL-bound to GET/HEAD (core.go:30), but the
			// apiserver authorizes a collection GET under the `list` verb — so
			// the prior constant `get` UNDER-GRANTED every collection stage and
			// 403'd the real LIST at /call (#44 verb bug, design §4). Resource
			// granularity is unchanged (Name stays "" — a resource-level grant,
			// per design §3.2); only the verb reflects the read shape.
			verb := "list"
			if name != "" {
				verb = "get"
			}
			rows = append(rows, Resource{
				Group:     gvr.Group,
				Version:   gvr.Version,
				Resource:  gvr.Resource,
				Namespace: ns,
				Verb:      verb,
				// #179 (arch nit): the in-cluster verb is get/list by
				// construction, so this is ALWAYS false today (byte-identical
				// wire, omitempty) — but computing it at EVERY emit site makes
				// the invariant NonReadVerb == !uafVerbIsRead(Verb) hold
				// structurally, so a future change that emits a non-read verb
				// here cannot ship unflagged.
				NonReadVerb: !uafVerbIsRead(verb),
			})
			continue
		}

		// Bare group-discovery path: resolvable, anonymous-readable catalogue;
		// contributes no read-set row but is NOT unresolvable.
		if _, ok := cache.ParseAPIServerDiscoveryPath(path); ok {
			continue
		}

		// Anything else: residual ${...}, upstream-dependent, or non-kube →
		// the whole stage is UNRESOLVABLE.
		return nil, fmt.Errorf("path %q is not an enumerable in-cluster apiserver path (residual template, upstream-dependent, or non-kube)", path)
	}

	return rows, nil
}

// inspectUAFStage emits the read-set rows for a UAF stage. The refilter calls
// EvaluateRBAC(Verb: uaf.Verb, Group: uaf.Group, Resource: r) verbatim for
// each resolved plural r (refilter.go:254-285) — and the SubjectAccessReview
// it builds is VERSION-LESS (ResourceAttributes takes group/resource/verb, no
// version). So a UAF row emits group+resource+verb and an EMPTY version
// (json omitempty): there is no single "correct" version to attach — a
// multi-version group has several, and picking one (e.g. preferred) is
// non-deterministic across discovery refreshes, which would break the
// byte-identical-response invariant (design §4). The plural is still
// discovery-VALIDATED for EXISTENCE (does the group serve it under ANY served
// version?); a bogus plural the apiserver serves nowhere → error → the stage
// is UNRESOLVABLE/fail-loud, never silently emitted.
func inspectUAFStage(rc *rest.Config, uaf *templates.UserAccessFilterSpec, resources []string) ([]Resource, error) {
	for _, plural := range resources {
		if plural == "" {
			continue // resolveUAFResources already skips these; defensive.
		}
		exists, err := pluralExistsInGroup(rc, uaf.Group, plural)
		if err != nil {
			return nil, fmt.Errorf("userAccessFilter resource %q (group %q): %w", plural, uaf.Group, err)
		}
		if !exists {
			return nil, fmt.Errorf("userAccessFilter resource %q served by no version of group %q", plural, uaf.Group)
		}
	}
	out := make([]Resource, 0, len(resources))
	for _, plural := range resources {
		if plural == "" {
			continue
		}
		out = append(out, Resource{
			Group:    uaf.Group,
			Version:  "", // version-less: the SAR check is version-less; see doc above.
			Resource: plural,
			Verb:     uaf.Verb,
			// #179: flag (do not bound) a non-read verb, using the same
			// single-source predicate the refilter warn uses (refilter.go).
			NonReadVerb: !uafVerbIsRead(uaf.Verb),
		})
	}
	return out, nil
}

// validateGVR confirms the apiserver serves gvr.Resource under gvr.Group /
// gvr.Version via discovery. A discovery miss (group/version not served, or the
// plural absent from the GroupVersion's resource list) is an error — the stage
// would 404 at /call, so emitting it as a granted resource would be wrong.
func validateGVR(rc *rest.Config, gvr schema.GroupVersionResource) error {
	cli, err := discoveryClientFor(rc)
	if err != nil {
		return err
	}
	gv := schema.GroupVersion{Group: gvr.Group, Version: gvr.Version}.String()
	list, err := cli.ServerResourcesForGroupVersion(gv)
	if err != nil {
		return fmt.Errorf("discovery for %s: %w", gv, err)
	}
	if !pluralServed(list, gvr.Resource) {
		return fmt.Errorf("resource %q not served under %s", gvr.Resource, gv)
	}
	return nil
}

// pluralExistsInGroup reports whether `group` serves `plural` under ANY of its
// served versions — a deterministic EXISTENCE check (no version is chosen or
// returned, so it cannot vary across discovery refreshes). Used to fail-loud on
// a bogus UAF plural without attaching a non-deterministic version to the row
// (Q1 ruling). A group the apiserver does not serve at all → (false, nil),
// which the caller turns into UNRESOLVABLE.
func pluralExistsInGroup(rc *rest.Config, group, plural string) (bool, error) {
	cli, err := discoveryClientFor(rc)
	if err != nil {
		return false, err
	}

	groups, err := cli.ServerGroups()
	if err != nil {
		return false, fmt.Errorf("discovery ServerGroups: %w", err)
	}

	var versions []string
	for _, g := range groups.Groups {
		if g.Name != group {
			continue
		}
		for _, v := range g.Versions {
			versions = append(versions, v.Version)
		}
		break
	}
	if len(versions) == 0 {
		return false, nil // group not served — caller fails loud.
	}

	for _, v := range versions {
		gv := schema.GroupVersion{Group: group, Version: v}.String()
		list, lerr := cli.ServerResourcesForGroupVersion(gv)
		if lerr != nil {
			continue
		}
		if pluralServed(list, plural) {
			return true, nil
		}
	}
	return false, nil
}

// pluralServed reports whether list contains a resource whose plural Name
// matches plural (subresources like "pods/status" carry a "/" and never match
// a bare plural).
func pluralServed(list *metav1.APIResourceList, plural string) bool {
	if list == nil {
		return false
	}
	for _, r := range list.APIResources {
		if r.Name == plural {
			return true
		}
	}
	return false
}

// dedupeSortResources collapses duplicate rows over the full
// (group, version, resource, namespace, verb) tuple and sorts the result
// lexicographically by that tuple — so an unchanged RA yields a byte-identical
// response (design §4). name is always "" and is not part of the key.
func dedupeSortResources(in []Resource) []Resource {
	seen := make(map[Resource]struct{}, len(in))
	out := make([]Resource, 0, len(in))
	for _, r := range in {
		key := Resource{
			Group: r.Group, Version: r.Version, Resource: r.Resource,
			Namespace: r.Namespace, Verb: r.Verb,
			// #179: NonReadVerb is a pure function of Verb, so it never splits a
			// tuple into two rows; carry it here so dedupe (which rebuilds the
			// row from this literal) does not drop the flag.
			NonReadVerb: r.NonReadVerb,
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Version != b.Version {
			return a.Version < b.Version
		}
		if a.Resource != b.Resource {
			return a.Resource < b.Resource
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Verb < b.Verb
	})
	return out
}
