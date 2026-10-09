// granted_atoms.go — #180 Phase A: enumerate the (group, resource) coordinates a
// requester profile actually grants for a verb, each with its own namespace
// answer.
//
// WHY THIS EXISTS. v7's projection answers one question per access class. For a
// ClassWildcard — where the group and/or resource is itself unknown
// identity-free (a templated segment, or a resourcesFrom-discovered plural set)
// — there was no answer: projectClass returned the constant AnswerWildcardGated.
// A constant is identity-INDEPENDENT, so every requester projects the same value
// and the digest cannot tell two identities apart. That is why the wildcard class
// is gated out of sharing today, and it is why a UAF-backed widget is never
// cached at all (#561): 32% of RESTActions declare a userAccessFilter, including
// composition-detail, and their Put is declined because no trustworthy per-
// requester digest exists.
//
// The v7 golden demonstrates the consequence rather than asserting it: with the
// constant in place, alice and bob derive the SAME ClassWildcard digest while
// being served [tenant-a] and [tenant-b] respectively.
//
// WHY THE WALK LIVES IN THIS PACKAGE. stringSliceMatches and rulesPermit are
// unexported here, and shadow_parity.go's header makes "no re-implementation of
// rule matching" a hard constraint — a second matcher that drifts from the
// evaluator is the defect class that produced the 6-revert L1/RBAC chain. So the
// enumeration is written where it can call the evaluator's own primitives, and
// the dispatchers package consumes the result.
//
// TWO ENCODING DECISIONS, both of which the design deliberately refused to guess
// and which are now settled:
//
//  1. MATCH DIRECTION when the CLASS side carries "*". Settled by SAFETY, not
//     preference, and the reasoning is in projectClass's own doc: the literal
//     reading (rule as `allowed`, class field as `want`) makes a class
//     group="*" match only rules whose APIGroups literally contain "*", so two
//     identities with different CONCRETE grants both project to the empty set,
//     collide, and false-share once the cell is shared. Under-counting is the
//     unsafe direction. So a class "*" COLLECTS every token the rules name.
//
//  2. THE ATOM TOKEN when the class field is CONCRETE. Ruled by the owner
//     2026-10-09: NORMALISE to the value the class asked about, not the rule's
//     literal token. Two identities whose rules are written differently but whose
//     effective access is the same then share one cell. Both forms are
//     leak-free — this is the cache-efficiency choice, and its risk is share
//     rate rather than correctness. A rule-side "*" is still PRESERVED as a
//     literal "*" atom (see below), so grant-all never collapses into an
//     explicit enumeration of the same concretes.
package rbac

import (
	"sort"

	rbacv1 "k8s.io/api/rbac/v1"
)

// GrantedAtom is one (group, resource) coordinate the profile grants for a verb,
// together with WHERE it grants it.
//
// The namespace answer is computed exactly as the ClassNamespaceSet projection
// computes its own — cluster-permit as ⊤, otherwise the sorted set of namespaces
// drawn from the profile's own NamespacedRules keys. It is carried PER ATOM
// rather than once per domain because the refilter runs per-object-namespace:
// two identities can hold the same resource set over the same namespace union
// and still differ in WHICH resource maps to WHICH namespace. A domain-level
// namespace answer cannot tell those apart; a per-atom one can.
type GrantedAtom struct {
	// Group and Resource are the coordinate. Either may be the literal "*" when a
	// rule grants all groups or all resources — preserved rather than expanded,
	// because "grants everything" is a different grant from "grants exactly these
	// concretes", including for namespaces that do not exist yet.
	Group    string
	Resource string

	// ClusterPermit is ⊤: the coordinate is granted in every namespace via a
	// cluster-wide rule. When true, Namespaces is empty.
	ClusterPermit bool

	// Namespaces is the sorted set of namespaces granting the coordinate, when
	// ClusterPermit is false. Never empty on a returned atom — an atom that
	// grants nowhere is dropped (see GrantedAtoms).
	Namespaces []string
}

// GrantedAtoms returns the coordinates p grants for verb, filtered by the class's
// group and resource fields, each carrying its own namespace answer. The result
// is sorted and deduplicated, so it is a canonical value suitable for a digest.
//
// group and resource are the CLASS fields: "*" means "unknown identity-free —
// any concrete value could be discovered at runtime", and a concrete value means
// the class pinned it.
//
// A nil profile grants nothing and returns nil, which is distinct from a profile
// whose rules grant nothing: both yield an empty set, and that is correct — an
// identity with no grants and an identity that does not exist are
// indistinguishable to a reader of the cell, and neither may share with an
// identity that does have grants.
//
// WHY AN ATOM THAT GRANTS NOWHERE IS DROPPED. A rule can name a coordinate and
// still grant nothing for this verb — most importantly a ResourceNames-scoped
// rule under a COLLECTION verb, which the Kubernetes authorizer (and therefore
// rulesPermit, via resourceNameMatches) refuses. Rather than re-implement that
// rule, every candidate atom is put back through Permits and dropped when the
// answer is "nowhere". That keeps the enumeration exactly as permissive as the
// evaluator, with no second opinion to drift.
func GrantedAtoms(p *RequesterProfile, verb, group, resource string) []GrantedAtom {
	if p == nil {
		return nil
	}

	// 1. Collect candidate coordinates from every rule the profile holds, cluster
	//    and namespaced alike. The namespace dimension is answered per atom in
	//    step 2, so WHERE a rule was found does not matter here — only what
	//    coordinate it names.
	type coord struct{ g, r string }
	seen := make(map[coord]struct{})

	collect := func(rules []rbacv1.PolicyRule) {
		for _, rule := range rules {
			if !stringSliceMatches(rule.Verbs, verb) {
				continue
			}
			for _, g := range tokensFor(rule.APIGroups, group) {
				for _, r := range tokensFor(rule.Resources, resource) {
					seen[coord{g, r}] = struct{}{}
				}
			}
		}
	}
	collect(p.ClusterRules)
	for _, rules := range p.NamespacedRules {
		collect(rules)
	}
	if len(seen) == 0 {
		return nil
	}

	// 2. Answer the namespace dimension per atom, using the evaluator only, and
	//    drop an atom the evaluator grants nowhere.
	out := make([]GrantedAtom, 0, len(seen))
	for c := range seen {
		// EMPTY Namespace ⇒ Permits consults only ClusterRules, which is the
		// cluster-permit (⊤) term — identical to the ClassNamespaceSet arm.
		if p.Permits(EvaluateOptions{Verb: verb, Group: c.g, Resource: c.r}) {
			out = append(out, GrantedAtom{Group: c.g, Resource: c.r, ClusterPermit: true})
			continue
		}
		var nss []string
		for ns := range p.NamespacedRules {
			if ns == "" {
				continue // RoleBindings always carry a non-empty namespace.
			}
			if p.Permits(EvaluateOptions{Verb: verb, Group: c.g, Resource: c.r, Namespace: ns}) {
				nss = append(nss, ns)
			}
		}
		if len(nss) == 0 {
			continue // granted nowhere — not an atom.
		}
		sort.Strings(nss)
		out = append(out, GrantedAtom{Group: c.g, Resource: c.r, Namespaces: nss})
	}

	// 3. Total order, so the value is canonical regardless of map iteration.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Resource < out[j].Resource
	})
	return out
}

// tokensFor maps one rule dimension (its APIGroups or Resources) against the
// class's field for that dimension, returning the atom tokens it contributes.
//
//	class field "*"       -> every token the rule names, VERBATIM. A rule-side
//	                         "*" stays "*": "grants everything" must not collapse
//	                         into an explicit list of today's concretes, or an
//	                         identity granted all resources would share a cell
//	                         with one granted exactly the resources that happen
//	                         to exist right now — and they diverge the moment a
//	                         new resource appears.
//	class field concrete  -> the CLASS value when the rule matches it, nothing
//	                         otherwise. This is the owner's normalisation ruling:
//	                         a rule saying "*" and a rule naming the value
//	                         exactly both yield the same token, so identities
//	                         with the same effective access share.
//
// Matching is stringSliceMatches in its ONE correct direction — rule dimension as
// `allowed`, class value as `want` — which is the direction the evaluator itself
// uses in rulesPermit. The "*" case does not call it at all: it is a collect, not
// a match.
func tokensFor(ruleDim []string, classField string) []string {
	if classField == "*" {
		return ruleDim
	}
	if stringSliceMatches(ruleDim, classField) {
		return []string{classField}
	}
	return nil
}
