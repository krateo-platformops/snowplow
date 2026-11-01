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
// coordinates by ObjectCheck, and nowhere else.
//
// WHAT UPSTREAM DOES. Kubernetes' ResourceNameMatches does NOT look at the
// verb: a rule with resourceNames matches iff the request's Name is listed. The
// Name comes from the request URL (RequestInfo): it is the object segment of
// an item URL, and "" for a collection URL. (RequestInfo also takes it from a
// `fieldSelector=metadata.name=` on a list/watch, from a named subresource, and
// a non-HTTP verb — impersonate, bind, escalate, use, a custom verb — carries
// whatever name its check names. snowplow builds none of those shapes here.)
//
// WHAT SNOWPLOW MAPS. The requests snowplow decides about are plain item and
// collection URLs: get / update / patch / delete address an item URL, which
// names the object; list / watch / create / deletecollection address the
// collection URL, which names none. IsNameSpecificVerb is that mapping, and
// for those URL shapes it gives exactly the Name the apiserver would see. So
// ObjectCheck carries the name for the item verbs and drops it for the
// collection verbs. The drop matters in a SelfSubjectAccessReview: a review
// carrying x would match a `create resourceNames: [x]` rule that the real POST
// (whose URL names no object) never matches — dropping it keeps the cache=on
// and cache=off answers equal to the apiserver's.
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
