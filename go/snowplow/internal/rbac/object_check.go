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
// Name comes from the request (RequestInfo, k8s.io/apiserver
// pkg/endpoints/request): it is the object segment of an item URL, "" for a
// collection URL — except a list or watch whose `fieldSelector` requires
// `metadata.name=<x>`, which carries x (#1049, CollectionCheck below). (A
// named subresource also carries its parent's name, and a non-HTTP verb —
// impersonate, bind, escalate, use, a custom verb — carries whatever name its
// check names. snowplow builds none of those shapes here.)
//
// WHAT SNOWPLOW MAPS. A decision about ONE object is about a plain item or
// collection URL: get / update / patch / delete address an item URL, which
// names the object; list / watch / create / deletecollection address the
// collection URL, which names none. IsNameSpecificVerb is that mapping, and
// for those URL shapes it gives exactly the Name the apiserver would see. So
// ObjectCheck carries the name for the item verbs and drops it for the
// collection verbs. The drop matters because the evaluator, like upstream,
// matches the Name whatever the verb: a check carrying x would match a
// `create resourceNames: [x]` rule that the real POST (whose URL names no
// object) never matches — dropping it keeps the cache=on and cache=off
// answers equal to the apiserver's.
//
// A decision about a whole collection READ — the scope gate of a served LIST
// and its per-item backstop — is built by CollectionCheck from the request's
// own query, which is where a list or watch gets its Name.

package rbac

import (
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// CollectionRead is the verb and Name the apiserver authorizes for an HTTP
// GET of a collection URL whose query string is rawQuery (#1049). It is the
// list/watch branch of RequestInfoFactory.NewRequestInfo (k8s.io/apiserver
// pkg/endpoints/request/requestinfo.go), step for step:
//
//   - the query is decoded into ListOptions with the apiserver's own
//     parameter codec. If ANY parameter fails to decode, the options are
//     discarded — the Name with them — and only `watch` is read back;
//   - the verb is "watch" when the options ask to watch, else "list";
//   - the Name is the value the field selector REQUIRES for metadata.name
//     (fields.Selector.RequiresExactMatch): the first `metadata.name=` or
//     `metadata.name==` term once fields.ParseSelector has sorted the terms
//     as strings — so `metadata.name=y,metadata.name=x` carries x, whatever
//     the order written. A `!=` term requires
//     nothing, an empty value yields Name "", and a value that cannot be a
//     path segment (".", "..", or one containing "/" or "%") is dropped.
//
// Any other selector — a label selector, metadata.namespace, any other field
// — leaves the Name "". The parity test runs this against the vendored
// NewRequestInfo on the same URLs.
func CollectionRead(rawQuery string) (verb, name string) {
	// req.URL.Query(): a malformed pair is skipped, the rest kept.
	query, _ := url.ParseQuery(rawQuery)
	opts := metainternalversion.ListOptions{}
	if err := metainternalversionscheme.ParameterCodec.DecodeParameters(query, metav1.SchemeGroupVersion, &opts); err != nil {
		opts = metainternalversion.ListOptions{}
		if values := query["watch"]; len(values) > 0 {
			switch strings.ToLower(values[0]) {
			case "false", "0":
			default:
				opts.Watch = true
			}
		}
	}
	verb = "list"
	if opts.Watch {
		verb = "watch"
	}
	if opts.FieldSelector != nil {
		if n, ok := opts.FieldSelector.RequiresExactMatch("metadata.name"); ok {
			if len(content.IsPathSegmentName(n)) == 0 {
				name = n
			}
		}
	}
	return verb, name
}

// CollectionCheck builds the EvaluateOptions for a read of the collection gvr
// in namespace ("" = cluster-wide) whose query string is rawQuery, by the
// requester (username, groups). The verb and Name are CollectionRead's, so a
// `list resourceNames: [x]` grant allows `?fieldSelector=metadata.name=x`
// exactly as the apiserver does, and nothing else. Optional fields
// (SkipBindingUID) are the caller's to set on the returned value.
func CollectionCheck(username string, groups []string, gvr schema.GroupVersionResource, namespace, rawQuery string) EvaluateOptions {
	verb, name := CollectionRead(rawQuery)
	return EvaluateOptions{
		Username:  username,
		Groups:    groups,
		Verb:      verb,
		Group:     gvr.Group,
		Resource:  gvr.Resource,
		Namespace: namespace,
		Name:      name,
	}
}
