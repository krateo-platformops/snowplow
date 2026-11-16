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
// group (system:authenticated, or system:unauthenticated for system:anonymous)
// as the apiserver's own impersonation filter decides it (requestGroups), and
// (#1362) non-resource requests: Attributes
// with a Path are authorized as upstream RuleAllows authorizes a request whose
// RequestInfo has IsResourceRequest=false, which means VerbMatches &&
// NonResourceURLMatches, with ClusterRoleBindings only (the namespace is "").
// Aggregated ClusterRoles are out of scope (no arm uses them).
package k8sauthz

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	authnv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kuser "k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters/impersonation"
	"k8s.io/apiserver/pkg/endpoints/request"
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
	// Path, when non-empty, makes this a NON-RESOURCE request (#1362): the
	// request path, as RequestInfo.Path carries it. Group, Resource, Namespace
	// and Name are then ignored, as upstream ignores them, and Verb is the
	// lowercase HTTP method.
	Path string
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

// requestGroups is the group set the apiserver's authorizer sees for a request
// snowplow sends as (user, groups) — Impersonate-User: user plus one
// Impersonate-Group per group. It is not modelled: the request runs through the
// apiserver's own k8s.io/apiserver/pkg/endpoints/filters/impersonation filter
// (v0.36.5 impersonation.go:126-157), and the groups are those of the user the
// filter hands downstream. That filter adds system:authenticated unless the
// user is system:anonymous or the groups already hold system:authenticated or
// system:unauthenticated (126-144), and adds system:unauthenticated to an
// impersonated system:anonymous that does not already hold it (145-157)
// (#1334). The authentication path's AuthenticatedGroupAdder applies only the
// first rule — it adds nothing for system:anonymous, whose real anonymous
// authenticator already yields [system:unauthenticated] — so it is not the
// reference for an impersonated system:anonymous with no groups. Before #1334
// this model appended system:authenticated unconditionally — the same mistake
// as the evaluator it judges, so it could not have caught it.
//
// A request the filter does not turn into (user, ...) — an empty user, which
// the apiserver never sees — has no answer here: requestGroups panics rather
// than guess one.
func requestGroups(user string, groups []string) []string {
	var got kuser.Info
	h := impersonation.WithImpersonation(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = request.UserFrom(r.Context())
	}), allowImpersonation{}, nil)
	req, err := http.NewRequestWithContext(request.WithUser(context.Background(), &kuser.DefaultInfo{
		Name:   "system:serviceaccount:krateo-system:snowplow",
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:krateo-system", kuser.AllAuthenticated},
	}), http.MethodGet, "/api", nil)
	if err != nil {
		panic(fmt.Sprintf("k8sauthz: build impersonation request: %v", err))
	}
	req.Header.Set(authnv1.ImpersonateUserHeader, user)
	for _, g := range groups {
		req.Header.Add(authnv1.ImpersonateGroupHeader, g)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got == nil || got.GetName() != user {
		panic(fmt.Sprintf("k8sauthz: the apiserver's impersonation filter does not accept user %q groups %q (%d %s)",
			user, groups, rec.Code, rec.Body.String()))
	}
	return got.GetGroups()
}

// allowImpersonation authorizes every impersonation: requestGroups needs the
// filter's group rule, not its impersonate permission check.
type allowImpersonation struct{}

func (allowImpersonation) Authorize(context.Context, authorizer.Attributes) (authorizer.Decision, string, error) {
	return authorizer.DecisionAllow, "", nil
}

// Allowed reports whether Kubernetes RBAC allows attr.
func (a *Authorizer) Allowed(attr Attributes) bool {
	groups := requestGroups(attr.User, attr.Groups)
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
	// A non-resource request has no namespace, so upstream visits no
	// RoleBinding for it.
	if attr.Namespace == "" || attr.Path != "" {
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
		if attr.Path != "" {
			if has(r.Verbs, attr.Verb) && nonResourceURLMatches(r.NonResourceURLs, attr.Path) {
				return true
			}
			continue
		}
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

// nonResourceURLMatches is upstream NonResourceURLMatches
// (pkg/apis/rbac/v1/evaluation_helpers.go): "*", an exact entry, or an entry
// ending in "*" whose TrimRight("*") is a prefix of the path.
func nonResourceURLMatches(ruleURLs []string, path string) bool {
	for _, u := range ruleURLs {
		if u == rbacv1.NonResourceAll || u == path {
			return true
		}
		if strings.HasSuffix(u, "*") && strings.HasPrefix(path, strings.TrimRight(u, "*")) {
			return true
		}
	}
	return false
}
