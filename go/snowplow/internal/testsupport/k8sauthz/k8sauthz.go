// Package k8sauthz is an INDEPENDENT model of the Kubernetes RBAC authorizer,
// for tests only (#1046). It answers what a SubjectAccessReview — or the
// apiserver authorizing a real request — answers for the same RBAC objects, so
// a test can compare snowplow's in-process verdicts (cache on) and its
// SelfSubjectAccessReview fallback (cache off) against one ground truth.
//
// WHY A SECOND MODEL. Comparing the evaluator to itself cannot find a
// divergence from Kubernetes. This package shares no code with internal/rbac:
// it is written from the upstream authorizer
// (plugin/pkg/auth/authorizer/rbac, pkg/apis/rbac/v1 evaluation helpers) and
// takes the request ATTRIBUTES the apiserver would see. In particular
// ResourceNameMatches does not look at the verb: a request that names no
// object (a LIST or a POST to the collection) simply carries no name, and a
// rule with resourceNames then cannot match it. RequestName models that from
// the HTTP shape of the request, not from snowplow's verb table.
//
// Scope: User / Group / ServiceAccount subjects, ClusterRoleBindings,
// RoleBindings to a Role or a ClusterRole, the "*" wildcards, the implicit
// system:authenticated group. Aggregated ClusterRoles and non-resource URLs
// are out of scope (no #1046 arm uses them).
package k8sauthz

import (
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Attributes are the authorization attributes of one resource request, as the
// apiserver's RequestInfo resolver produces them.
type Attributes struct {
	User      string
	Groups    []string
	Verb      string
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// Authorizer holds a fixed set of RBAC objects.
type Authorizer struct {
	crbs  []*rbacv1.ClusterRoleBinding
	rbs   []*rbacv1.RoleBinding
	crs   map[string]*rbacv1.ClusterRole
	roles map[string]*rbacv1.Role // key: namespace/name
}

// New builds an Authorizer from typed RBAC objects; any other object is
// ignored, so a test can pass its whole seed.
func New(objs ...runtime.Object) *Authorizer {
	a := &Authorizer{crs: map[string]*rbacv1.ClusterRole{}, roles: map[string]*rbacv1.Role{}}
	for _, o := range objs {
		switch v := o.(type) {
		case *rbacv1.ClusterRoleBinding:
			a.crbs = append(a.crbs, v)
		case *rbacv1.RoleBinding:
			a.rbs = append(a.rbs, v)
		case *rbacv1.ClusterRole:
			a.crs[v.Name] = v
		case *rbacv1.Role:
			a.roles[v.Namespace+"/"+v.Name] = v
		}
	}
	return a
}

// RequestName is the object name the apiserver's request carries for an
// operation of verb on the object called name, for a PLAIN item or collection
// URL: an item URL (get, update, patch, delete) names it; a collection URL
// (list, watch, create, deletecollection) does not.
//
// NOT INDEPENDENT of snowplow: it is the same verb → URL-shape table as
// rbac.IsNameSpecificVerb, so a matrix built on it cannot detect a request
// shape that table misses — a create on a named subresource, a non-HTTP verb.
// Allowed's rule matching is independent; this helper is the shared
// assumption. A list/watch with `fieldSelector=metadata.name=` (RequestInfo
// sets Name) is NOT modelled here: #1049's matrix takes the verb and Name of
// such a read from the vendored RequestInfo resolver itself
// (internal/rbac/evaltest/issue1049_collection_check_parity_test.go).
func RequestName(verb, name string) string {
	switch verb {
	case "get", "update", "patch", "delete":
		return name
	default:
		return ""
	}
}

// Allowed reports whether Kubernetes RBAC allows attr.
func (a *Authorizer) Allowed(attr Attributes) bool {
	groups := append(append([]string(nil), attr.Groups...), "system:authenticated")
	for _, b := range a.crbs {
		if !subjectsMatch(b.Subjects, attr.User, groups, "") {
			continue
		}
		if b.RoleRef.Kind != "ClusterRole" {
			continue
		}
		if cr := a.crs[b.RoleRef.Name]; cr != nil && rulesAllow(cr.Rules, attr) {
			return true
		}
	}
	if attr.Namespace == "" {
		return false
	}
	for _, b := range a.rbs {
		if b.Namespace != attr.Namespace || !subjectsMatch(b.Subjects, attr.User, groups, b.Namespace) {
			continue
		}
		var rules []rbacv1.PolicyRule
		switch b.RoleRef.Kind {
		case "ClusterRole":
			if cr := a.crs[b.RoleRef.Name]; cr != nil {
				rules = cr.Rules
			}
		case "Role":
			if r := a.roles[b.Namespace+"/"+b.RoleRef.Name]; r != nil {
				rules = r.Rules
			}
		}
		if rulesAllow(rules, attr) {
			return true
		}
	}
	return false
}

func subjectsMatch(subjects []rbacv1.Subject, user string, groups []string, bindingNS string) bool {
	for _, s := range subjects {
		switch s.Kind {
		case rbacv1.UserKind:
			if s.Name == user {
				return true
			}
		case rbacv1.GroupKind:
			for _, g := range groups {
				if g == s.Name {
					return true
				}
			}
		case rbacv1.ServiceAccountKind:
			ns := s.Namespace
			if ns == "" {
				ns = bindingNS
			}
			if user == "system:serviceaccount:"+ns+":"+s.Name {
				return true
			}
		}
	}
	return false
}

func rulesAllow(rules []rbacv1.PolicyRule, attr Attributes) bool {
	for _, r := range rules {
		if has(r.Verbs, attr.Verb) && has(r.APIGroups, attr.Group) &&
			has(r.Resources, attr.Resource) && nameMatches(r.ResourceNames, attr.Name) {
			return true
		}
	}
	return false
}

// has is the upstream VerbMatches / APIGroupMatches / ResourceMatches shape:
// "*" or an exact (case-sensitive) match.
func has(list []string, want string) bool {
	for _, v := range list {
		if v == rbacv1.VerbAll || v == want {
			return true
		}
	}
	return false
}

// nameMatches is upstream ResourceNameMatches: an empty list matches every
// request; otherwise the request's name must be listed. It ignores the verb.
func nameMatches(names []string, name string) bool {
	if len(names) == 0 {
		return true
	}
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}
