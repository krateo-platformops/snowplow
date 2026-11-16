// informer_dispatch_rbac.go — Tag 0.30.100: post-LIST per-item RBAC
// filter for the resolver pivot's served LIST branch. Tag 0.30.101
// extends the same pattern to the served GET-by-name branch
// (filterGetByRBAC, below).
//
// THE BUG (0.30.99 Phase-6 bench "Finding 1"):
//   dispatchViaInformer's LIST branch returned the raw informer
//   namespace partition (rw.ListObjectsServable) with NO per-user
//   filter. dispatchViaInformer never reads call.Endpoint, so it
//   bypasses the per-user `<username>-clientconfig` bearer token
//   entirely. For an apiserver-routed call WITHOUT a userAccessFilter
//   stanza (e.g. compositions-list) that per-user token was the ONLY
//   RBAC gate — the pivot bypassed it and a narrow-RBAC user
//   (`cyberjoker`) saw ALL 49,999 compositions.
//
//   Tag 0.30.101 — GET-verb sibling: the SAME bypass exists on the
//   pivot's GET-by-name branch. dispatchViaInformer's GET branch
//   returned the raw informer object (rw.GetObject → json.Marshal)
//   with no RBAC check, so a narrow-RBAC user GETting a known object
//   name in a namespace they have no `get` grant for received the
//   object. filterGetByRBAC closes that GET-path leak.
//
// THE FIX:
//   filterListByRBAC runs every pivot-served LIST through a per-item
//   EvaluateRBAC check — the same in-process, tokenless typed-RBAC
//   indexer the userAccessFilter refilter uses (internal/rbac/
//   evaluate.go). It generalises the UAF refilter (refilter.go) to fire
//   on EVERY pivot-served LIST, not just stages that declare a
//   userAccessFilter stanza. filterGetByRBAC (Tag 0.30.101) is the
//   single-object GET analogue: one EvaluateRBAC call with Verb "get"
//   against the object's own namespace.
//
// FAIL-CLOSED CONTRACT (binding — security):
//   - No identity on the context (xcontext.UserInfo error) → the whole
//     LIST is not served (servable=false → apiserver fallthrough). The
//     apiserver path narrows via the per-user token; falling through is
//     correct and never leaks.
//   - EvaluateRBAC returns an error for an item → that item is DROPPED.
//   - A nil/empty username after a successful UserInfo read → treated
//     as "no subject matches" by EvaluateRBAC → every item dropped.
//   Under no path does an unfiltered partition reach the caller.
//
// PERFORMANCE:
//   EvaluateRBAC is an indexed read against the typed-RBAC informer
//   cache (ListTypedObjects + GetTypedObject — O(bindings) per call,
//   not a per-call apiserver round-trip). The existing UAF refilter
//   already does per-item EvaluateRBAC at production scale, so per-item
//   eval on a large LIST is proven-acceptable. At ~50K items the cost
//   is 50K × (typed-indexer walk) — bounded, in-process, no I/O. The
//   0.30.10 permission-check cache amortises repeat (user, ns) tuples.
//
// BINDINGS:
//   - feedback_no_special_cases.md: the filter is uniform over every
//     GVR — no per-resource carve-out. Verb is hardcoded "list"
//     because this is the served-LIST branch (the call IS a list).
//   - feedback_restaction_no_widget_logic.md: lives in the resolver
//     layer, not widget canonicalization.
//   - feedback_l1_invalidation_delete_only.md: filtering happens before
//     the bytes reach call.ResponseHandler / the resolved-cache write,
//     so any cached entry is already user-scoped.

package api

import (
	"context"
	"errors"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// errGateFallthrough is the per-user gate's "not decided here" answer: it
// cannot vouch for the read (no identity on the context, an evaluator error, a
// denied GET-by-name). The caller does not
// serve the read and falls through to the apiserver, whose answer for the
// requester's own credential is authoritative. It never reaches a result.
var errGateFallthrough = errors.New("rbac gate: not decided in process, fall through to the apiserver")

// newListForbidden is the deny of a LIST the requester may not make (#961): the
// typed re-gate Forbidden (internal_dispatch_predial.go), so resolve.go records
// it exactly as it records the apiserver's own 403 on the requester's dial
// (#681). Its text IS the canonical identity-free message
// classifyOwnDialForbidden derives for the same LIST path.
func newListForbidden(gvr schema.GroupVersionResource, namespace string) error {
	return newRegateForbiddenVerb(gvr, namespace, "", "list")
}

// filterListByRBAC is the per-user gate of a LIST served from snowplow's own
// copy of the data (the informer, the api-stage content cell, branch C's SA
// read). namespace is the REQUEST's namespace ("" = a cluster-wide list) and
// rawQuery the request's own query string (#1049): with namespace and gvr it
// is the request the apiserver would authorize.
//
// It answers in two steps, both from the requester's RBAC rules through the
// in-process evaluator, never from the items:
//
//  1. Scope (#961, listScopeAllowed): may the requester make this request,
//     exactly as the apiserver authorizes it? The check is
//     rbac.CollectionCheck's — verb list, and the Name a
//     `fieldSelector=metadata.name=<x>` carries (#1049), so a
//     `list resourceNames: [x]` grant allows a list selecting x and nothing
//     else. If not, the apiserver refuses the request with 403, and so does
//     this gate: it returns newListForbidden, whatever the items. An allowed
//     request whose collection is empty is served empty.
//  2. Items (defence in depth): of an allowed request, the items the
//     requester may read through this same request moved to each item's own
//     coordinates — its namespace (cluster-scoped items, namespace "", against
//     the cluster scope) and, when the request names an object, its name. An
//     allowed scope already covers every item a correct copy holds — a cluster
//     grant covers every namespace, a namespace grant covers that namespace's
//     partition, and a name-selected list holds only that name — so this step
//     only drops an item that does not belong to the request's scope. It
//     evaluates EVERY item, the request's own namespace included (#963 F1),
//     so it is a real backstop: a wrong allow in step 1 cannot serve an item
//     step 2 would refuse.
//     "What this requester may see across namespaces" is userAccessFilter's
//     job, never a cluster-wide list's.
//
// Return contract:
//   - (subset, nil) — serve the subset (possibly empty).
//   - (nil, a *regateDeniedError) — the requester may not make this LIST:
//     record it as a per-identity Forbidden, the same answer the apiserver gives.
//   - (nil, errGateFallthrough) — not decided here (no identity, an evaluator
//     error): do not serve, fall through to the
//     apiserver. An absent identity never yields an unfiltered partition.
//
// gvr supplies the API group + resource for the EvaluateRBAC tuple. The verb
// is the request's (rbac.CollectionRead): list, since a watch query is never
// served here (it falls through to the apiserver, list_query.go).
//
// Per-item EvaluateRBAC errors fail closed (item dropped) — see file header.
func filterListByRBAC(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	namespace, rawQuery string,
	items []*unstructured.Unstructured,
) ([]*unstructured.Unstructured, error) {
	log := xcontext.Logger(ctx)

	user, err := xcontext.UserInfo(ctx)
	if err != nil {
		// FAIL-CLOSED: no identity → cannot vouch for ANY item. Do not
		// serve; fall through to the apiserver. Returning an empty
		// served list here would also be safe, but fallthrough keeps
		// the apiserver's per-user-token narrowing as the authoritative
		// answer and avoids masking a genuine "no objects" result.
		log.WarnContext(ctx, "informer_dispatch.rbac_filter.no_identity",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.Any("err", err),
			slog.String("action", "fallthrough_to_apiserver"),
		)
		return nil, errGateFallthrough
	}

	// Step 1 — scope (#961), on the request's own check (#1049).
	scope := rbac.CollectionCheck(user.Username, user.Groups, gvr, namespace, rawQuery)
	// Ship L1 (0.30.252): the gate discards matchedBindingUID — skip the
	// CRB/RB stable-sort.
	scope.SkipBindingUID = true
	allowed, decided := listScopeAllowed(ctx, scope)
	if !decided {
		return nil, errGateFallthrough
	}
	if !allowed {
		log.Debug("informer_dispatch.rbac_filter.list_denied",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("namespace", namespace),
		)
		return nil, newListForbidden(gvr, namespace)
	}

	// 0.30.111 Part 3 — coordinate-keyed EvaluateRBAC memo. Each item is
	// evaluated on the scope check moved to the item's coordinates
	// (itemCheck): its namespace, and its name only when the request carries
	// a Name. A request without a name selector carries Name "" (Kubernetes'
	// ResourceNameMatches compares rule.ResourceNames with the request's Name,
	// and a plain collection URL names no object), so its decision is
	// NAMESPACE-scoped and every item of a namespace shares one verdict; a
	// name-selected request holds only items of that name (#1049), so it is
	// one verdict per namespace too. The memo turns an N-item LIST spanning K
	// distinct namespaces into K EvaluateRBAC calls instead of N.
	//
	// AC-6 (PM gate #113): the memo key IS the evaluated tuple's variable
	// part (namespace, name), so it is correct BY CONSTRUCTION — two items
	// share an entry only if they share the evaluated check.
	//
	// rbacMemo is a LOCAL variable scoped to this single filterListByRBAC
	// invocation — never a struct field, never package-scoped. It is
	// discarded at return: no persistence, no invalidation, no staleness.
	// A subsequent LIST builds a fresh memo against the live RBAC store.
	//
	// #963 F1 — the memo starts EMPTY: it is never seeded from the scope
	// verdict. Step 2 is the backstop of step 1, so an item of the request's
	// own namespace is evaluated by step 2 like every other item. Seeded with
	// the scope's allow, a wrong allow in step 1 (an evaluator error read as
	// allowed) served every item of the request's namespace unchecked; the
	// cost of not seeding is one EvaluateRBAC per request namespace present
	// in the items.
	rbacMemo := make(map[itemCoord]bool, len(items))

	kept := make([]*unstructured.Unstructured, 0, len(items))
	dropped := 0
	for _, it := range items {
		if it == nil {
			// Defensive: a nil entry cannot be vouched for. Drop it
			// (fail-closed) — listFromIndexer never emits nil, but the
			// per-item loop must not trust that invariant blindly.
			dropped++
			continue
		}
		check := itemCheck(scope, it)
		coord := itemCoord{namespace: check.Namespace, name: check.Name}
		ns := check.Namespace
		allowed, memoized := rbacMemo[coord]
		if !memoized {
			// Ship 0.30.242 H.c-layered Phase 2 step 2a — per-item caller
			// ignores matchedBindingUID (SkipBindingUID, set on scope).
			ok, _, err := rbac.EvaluateRBAC(ctx, check)
			if err != nil {
				// FAIL-CLOSED: an evaluator hiccup never permits a leak.
				// The error is NOT memoized — a transient evaluator
				// failure must not poison every later item in this
				// namespace; the next same-namespace item retries.
				log.WarnContext(ctx, "informer_dispatch.rbac_filter.evaluate_error",
					slog.String("subsystem", "cache"),
					slog.String("gvr", gvr.String()),
					slog.String("namespace", ns),
					slog.Any("err", err),
					slog.String("action", "drop_item"),
				)
				dropped++
				continue
			}
			allowed = ok
			rbacMemo[coord] = ok
		}
		if allowed {
			kept = append(kept, it)
		} else {
			dropped++
		}
	}

	if dropped > 0 {
		dispatchInformerRBACDropped.Add(uint64(dropped))
	}
	log.Debug("informer_dispatch.rbac_filter",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.Int("served", len(items)),
		slog.Int("kept", len(kept)),
		slog.Int("dropped", dropped),
	)
	return kept, nil
}

// listScopeAllowed reports whether the requester may make the LIST whose check
// is scope (#961) — rbac.CollectionCheck of the request's gvr, namespace and
// query — exactly as the apiserver authorizes the same request: one
// EvaluateRBAC at the request's own scope:
//
//   - a namespaced LIST (namespace != ""): a ClusterRoleBinding, or a
//     RoleBinding in that namespace;
//   - a cluster-wide LIST (namespace == ""): a ClusterRoleBinding, full stop.
//     A RoleBinding grants nothing at the cluster scope, whatever the resource;
//     a requester holding only namespace grants gets the apiserver's 403, and a
//     view across the namespaces it may read is what userAccessFilter is for.
//
// A rule with resourceNames grants the request only when the request carries
// one of those names, i.e. its `fieldSelector` requires `metadata.name=` one
// of them (#1049).
//
// decided is false when the evaluator errors: the gate then falls through to
// the apiserver rather than guess.
func listScopeAllowed(ctx context.Context, scope rbac.EvaluateOptions) (allowed, decided bool) {
	ok, _, err := rbac.EvaluateRBAC(ctx, scope)
	if err != nil {
		xcontext.Logger(ctx).WarnContext(ctx, "informer_dispatch.rbac_filter.scope_undecided",
			slog.String("subsystem", "cache"),
			slog.String("gvr", scope.Group+"/"+scope.Resource),
			slog.String("namespace", scope.Namespace),
			slog.Any("err", err),
			slog.String("action", "fallthrough_to_apiserver"),
		)
		return false, false
	}
	return ok, true
}

// itemCoord is the part of a per-item check that varies across a LIST's
// items: the memo key of filterListByRBAC's step 2.
type itemCoord struct{ namespace, name string }

// itemCheck is the request's scope check moved to one item's coordinates
// (filterListByRBAC step 2): the item's own namespace and — only when the
// request carries a Name (a `fieldSelector=metadata.name=`, #1049) — the
// item's own name. A request without a Name names no object, so its item
// checks carry none either: a resourceNames rule must not grant through the
// backstop a list the apiserver would refuse.
func itemCheck(scope rbac.EvaluateOptions, it *unstructured.Unstructured) rbac.EvaluateOptions {
	check := scope
	check.Namespace = it.GetNamespace()
	if scope.Name != "" {
		check.Name = it.GetName()
	}
	return check
}

// filterGetByRBAC reports whether the context's user is authorized to
// `get` a single informer-served object — the GET-by-name analogue of
// filterListByRBAC. Tag 0.30.101.
//
// Return contract:
//   - true  — identity present AND the user has a `get` grant for the
//     object's namespace. Caller serves the object.
//   - false — caller MUST NOT serve; fall through to the apiserver
//     (whose per-user token gate narrows correctly — a denied GET
//     becomes a 403). false covers every fail-closed path:
//   - NO identity on the context (xcontext.UserInfo error);
//   - EvaluateRBAC returned an error (evaluator hiccup never
//     permits a serve);
//   - EvaluateRBAC returned deny.
//     Under no path does an unauthorized informer object reach the
//     caller.
//
// gvr supplies the API group + resource for the EvaluateRBAC tuple; the
// verb is fixed "get" — this is the served GET-by-name branch.
// namespace is the object's OWN metadata.namespace (a cluster-scoped
// object, namespace=="", evaluates against cluster-wide RBAC, which is
// the correct apiserver-equivalent semantics) — mirrors filterListByRBAC.
//
// Per feedback_no_special_cases.md: uniform over every GVR — no
// per-resource carve-out.
func filterGetByRBAC(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	obj *unstructured.Unstructured,
) bool {
	if obj == nil {
		// Defensive: a nil object cannot be vouched for. The GET branch
		// only reaches here on an indexer HIT (obj non-nil), but the
		// helper must not trust that invariant blindly — fail closed.
		return false
	}
	return authorizeGetByRBAC(ctx, gvr, obj.GetNamespace(), obj.GetName())
}

// authorizeGetByRBAC is the ONE `get` evaluation for a single object
// coordinate (#677). filterGetByRBAC calls it with the fetched object's own
// namespace/name; branch C's pre-dial step (internal_dispatch.go) calls it
// with the REF's namespace/name before any SA dial, so both checks share the
// identity source (xcontext.UserInfo), the verb, the GVR, the snapshot read
// (rbac.EvaluateRBAC) and every fail-closed path. Same return contract as
// filterGetByRBAC.
func authorizeGetByRBAC(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	namespace, name string,
) bool {
	log := xcontext.Logger(ctx)

	user, err := xcontext.UserInfo(ctx)
	if err != nil {
		// FAIL-CLOSED: no identity → cannot vouch for the object. Do not
		// serve; fall through to the apiserver, whose per-user-token
		// narrowing is the authoritative answer.
		log.WarnContext(ctx, "informer_dispatch.rbac_get_filter.no_identity",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("namespace", namespace),
			slog.String("name", name),
			slog.Any("err", err),
			slog.String("action", "fallthrough_to_apiserver"),
		)
		return false
	}

	// Verb is "get" — a name-specific verb. The object's OWN name is
	// threaded so a resourceNames-scoped rule (resourceNames: ["foo"])
	// grants `get foo` but not `get bar` (0.30.109, G1).
	//
	// Ship 0.30.242 H.c-layered Phase 2 step 2a — per-item caller ignores
	// matchedBindingUID.
	// #1046 — built by rbac.ObjectCheck, the one per-object construction.
	opts := rbac.ObjectCheck(user.Username, user.Groups, "get", rbac.TargetOf(gvr, namespace, name))
	// Ship L1 (0.30.252): per-item caller discards matchedBindingUID — skip
	// the CRB/RB stable-sort.
	opts.SkipBindingUID = true
	allowed, _, err := rbac.EvaluateRBAC(ctx, opts)
	if err != nil {
		// FAIL-CLOSED: an evaluator hiccup never permits a serve.
		log.WarnContext(ctx, "informer_dispatch.rbac_get_filter.evaluate_error",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("namespace", namespace),
			slog.String("name", name),
			slog.Any("err", err),
			slog.String("action", "fallthrough_to_apiserver"),
		)
		return false
	}

	if !allowed {
		dispatchInformerRBACDropped.Add(1)
	}
	log.Debug("informer_dispatch.rbac_get_filter",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("namespace", namespace),
		slog.String("name", name),
		slog.Bool("allowed", allowed),
	)
	return allowed
}
