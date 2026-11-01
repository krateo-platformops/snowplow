// object_check.go — #1046: the ONE construction of an authorization check about
// a single object.
//
// THE DEFECT. Several sites decide whether the requester may act on ONE named
// object — the /call dispatch gate, a nested `resolve: true` stage, the
// informer-served objects.Get, a widget's resourcesRefs `allowed` flag — and
// each assembled its EvaluateOptions (or its SelfSubjectAccessReview) by hand.
// Some forgot the object's Name. Kubernetes evaluates a rule that carries
// `resourceNames` against the name of the object the request targets, so a
// check without the name can never match such a rule: a user whose only grant
// is `get restactions resourceNames: [x]` was 403'd by the cache=on dispatch
// gate while the apiserver (cache=off) let the same request through. The
// reverse happened under cache=off for userAccessFilter: the refilter carried
// the name, but the SelfSubjectAccessReview it fell back to dropped it.
//
// THE RULE. A decision about one object is built from that object's
// coordinates by ObjectCheck, and nowhere else. ObjectCheck carries the name
// exactly as the apiserver's own request would: a request that names a single
// object (get / update / patch / delete) carries it; a collection request
// (list / watch / create / deletecollection) has no object name, so none is
// carried. That is the Kubernetes request shape, not a carve-out, and it is the
// same predicate the evaluator scopes `resourceNames` by (IsNameSpecificVerb).
// Carrying the name on a collection verb would be inert in-process but NOT in
// a SelfSubjectAccessReview — the apiserver would match a `create
// resourceNames: [x]` rule that the real POST (which has no name) never
// matches — so the drop is what keeps the cache=on and cache=off answers equal.
//
// A collection check (a LIST scope gate, a per-namespace list filter) is not a
// decision about one object and keeps its own Name-less literal.

package rbac

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ObjectTarget is the single object an authorization decision is about: the
// API group and plural resource of its type, and its own namespace and name.
// Namespace "" is a cluster-scoped object; Name "" means the request names no
// single object.
type ObjectTarget struct {
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// ObjectNamer is the part of an object a target is read from. Every
// Kubernetes object (metav1.Object, *unstructured.Unstructured) satisfies it.
type ObjectNamer interface {
	GetNamespace() string
	GetName() string
}

// TargetOf is the target at the given coordinates, for a site that holds the
// coordinates rather than the object (a /call path, a stage path, a resource
// ref).
func TargetOf(gvr schema.GroupVersionResource, namespace, name string) ObjectTarget {
	return ObjectTarget{Group: gvr.Group, Resource: gvr.Resource, Namespace: namespace, Name: name}
}

// TargetOfObject is the target of a fetched object: its namespace and name are
// read from the object itself, so a site holding the object cannot pass one
// coordinate and forget the other.
func TargetOfObject(gvr schema.GroupVersionResource, obj ObjectNamer) ObjectTarget {
	return TargetOf(gvr, obj.GetNamespace(), obj.GetName())
}

// ObjectCheck builds the EvaluateOptions for `verb` on target by the requester
// (username, groups). The name is carried iff the verb names a single object
// (see the file header). Optional fields (SkipBindingUID,
// WinningSubjectClassOut) are the caller's to set on the returned value.
func ObjectCheck(username string, groups []string, verb string, target ObjectTarget) EvaluateOptions {
	opts := EvaluateOptions{
		Username:  username,
		Groups:    groups,
		Verb:      verb,
		Group:     target.Group,
		Resource:  target.Resource,
		Namespace: target.Namespace,
	}
	if IsNameSpecificVerb(verb) {
		opts.Name = target.Name
	}
	return opts
}
