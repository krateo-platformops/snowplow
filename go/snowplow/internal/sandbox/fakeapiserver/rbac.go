//go:build js && wasm

package fakeapiserver

import (
	"fmt"
	"slices"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RBAC authorization over the snapshot's Roles, ClusterRoles and bindings, as kube-apiserver's
// RBAC authorizer decides it. k8s.io/kubernetes is not a library, so these are transcribed from
// v1.36.2:
//
//	plugin/pkg/auth/authorizer/rbac/rbac.go:56-66       the visitor and the allow reason
//	plugin/pkg/auth/authorizer/rbac/rbac.go:129-150     RulesFor: a rule with no resources is no resource rule
//	pkg/registry/rbac/validation/rule.go:139-177        describing a binding and its subject
//	pkg/registry/rbac/validation/rule.go:280-305        appliesToUser (a ServiceAccount subject with
//	                                                    no namespace takes the RoleBinding's)
//	pkg/apis/rbac/v1/evaluation_helpers.go              the rule matching
//
// It is the only authorizer: no allow-all, no node authorizer, no webhook.

const saPrefix = "system:serviceaccount:"

// identity is an authenticated (or impersonated) user.
type identity struct {
	User   string
	Groups []string
	Label  string // the request log's identity: labelSA, labelVisitor, labelOther or labelNone
}

// The request log's identity values (sandbox SNAPSHOT.md §4). A request's line carries one of the
// first four, from authentication and impersonation. labelController is no request's: it is the
// line of a scripted status write (applyStatus), the simulated controller's own, and nothing that
// reads a request sets it.
const (
	labelSA         = "sa"
	labelVisitor    = "visitor"
	labelOther      = "other"
	labelNone       = "none"
	labelController = "controller"
)

// serviceAccountIdentity is the user and groups the apiserver gives a ServiceAccount token.
func serviceAccountIdentity(namespace, name string) identity {
	return identity{
		User:   saPrefix + namespace + ":" + name,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + namespace, "system:authenticated"},
	}
}

// attributes are what a request asks: a resource request, or a non-resource Path.
type attributes struct {
	Verb, Group, Resource, Subresource, Namespace, Name string
	Path                                                string
	NonResource                                         bool
}

type authorizer struct {
	clusterRoles        map[string]*rbacv1.ClusterRole
	roles               map[string]*rbacv1.Role // ns/name
	clusterRoleBindings []*rbacv1.ClusterRoleBinding
	roleBindings        []*rbacv1.RoleBinding
}

func newAuthorizer(objects []map[string]any) (*authorizer, error) {
	a := &authorizer{clusterRoles: map[string]*rbacv1.ClusterRole{}, roles: map[string]*rbacv1.Role{}}
	for _, o := range objects {
		if o["apiVersion"] != "rbac.authorization.k8s.io/v1" {
			continue
		}
		var err error
		switch o["kind"] {
		case "ClusterRole":
			r := &rbacv1.ClusterRole{}
			if err = runtime.DefaultUnstructuredConverter.FromUnstructured(o, r); err == nil {
				a.clusterRoles[r.Name] = r
			}
		case "Role":
			r := &rbacv1.Role{}
			if err = runtime.DefaultUnstructuredConverter.FromUnstructured(o, r); err == nil {
				a.roles[r.Namespace+"/"+r.Name] = r
			}
		case "ClusterRoleBinding":
			b := &rbacv1.ClusterRoleBinding{}
			if err = runtime.DefaultUnstructuredConverter.FromUnstructured(o, b); err == nil {
				a.clusterRoleBindings = append(a.clusterRoleBindings, b)
			}
		case "RoleBinding":
			b := &rbacv1.RoleBinding{}
			if err = runtime.DefaultUnstructuredConverter.FromUnstructured(o, b); err == nil {
				a.roleBindings = append(a.roleBindings, b)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("rbac object %v: %w", o["kind"], err)
		}
	}
	return a, nil
}

// appliesTo is rule.go appliesToUser: the first subject that names id, if any. bindingNS is ""
// for a ClusterRoleBinding.
func appliesTo(subjects []rbacv1.Subject, id identity, bindingNS string) (rbacv1.Subject, bool) {
	for _, s := range subjects {
		switch s.Kind {
		case rbacv1.UserKind:
			if s.Name == id.User {
				return s, true
			}
		case rbacv1.GroupKind:
			if slices.Contains(id.Groups, s.Name) {
				return s, true
			}
		case rbacv1.ServiceAccountKind:
			ns := s.Namespace
			if ns == "" {
				ns = bindingNS
			}
			if ns != "" && id.User == saPrefix+ns+":"+s.Name {
				return s, true
			}
		}
	}
	return rbacv1.Subject{}, false
}

func describeSubject(s rbacv1.Subject, bindingNS string) string {
	if s.Kind == rbacv1.ServiceAccountKind {
		ns := s.Namespace
		if ns == "" {
			ns = bindingNS
		}
		return fmt.Sprintf("%s %q", s.Kind, s.Name+"/"+ns)
	}
	return fmt.Sprintf("%s %q", s.Kind, s.Name)
}

func (a *authorizer) roleRules(ref rbacv1.RoleRef, ns string) []rbacv1.PolicyRule {
	switch ref.Kind {
	case "ClusterRole":
		if r := a.clusterRoles[ref.Name]; r != nil {
			return r.Rules
		}
	case "Role":
		if r := a.roles[ns+"/"+ref.Name]; r != nil {
			return r.Rules
		}
	}
	return nil
}

// visit calls f with each rule that applies to id in namespace ns ("" = cluster scope only) and
// the description of the binding it came through, until f returns false.
func (a *authorizer) visit(id identity, ns string, f func(source string, r rbacv1.PolicyRule) bool) {
	for _, b := range a.clusterRoleBindings {
		s, ok := appliesTo(b.Subjects, id, "")
		if !ok {
			continue
		}
		src := fmt.Sprintf("ClusterRoleBinding %q of %s %q to %s", b.Name, b.RoleRef.Kind, b.RoleRef.Name, describeSubject(s, ""))
		for _, r := range a.roleRules(b.RoleRef, "") {
			if !f(src, r) {
				return
			}
		}
	}
	if ns == "" {
		return
	}
	for _, b := range a.roleBindings {
		if b.Namespace != ns {
			continue
		}
		s, ok := appliesTo(b.Subjects, id, b.Namespace)
		if !ok {
			continue
		}
		src := fmt.Sprintf("RoleBinding %q of %s %q to %s", b.Name+"/"+b.Namespace, b.RoleRef.Kind, b.RoleRef.Name, describeSubject(s, b.Namespace))
		for _, r := range a.roleRules(b.RoleRef, ns) {
			if !f(src, r) {
				return
			}
		}
	}
}

// rulesFor is the rules that apply to id in namespace ns.
func (a *authorizer) rulesFor(id identity, ns string) []rbacv1.PolicyRule {
	var rules []rbacv1.PolicyRule
	a.visit(id, ns, func(_ string, r rbacv1.PolicyRule) bool { rules = append(rules, r); return true })
	return rules
}

func has(list []string, v string) bool { return slices.Contains(list, "*") || slices.Contains(list, v) }

// ruleAllows is evaluation_helpers.go RuleAllows.
func ruleAllows(r rbacv1.PolicyRule, at attributes) bool {
	if !has(r.Verbs, at.Verb) {
		return false
	}
	if at.NonResource {
		return slices.ContainsFunc(r.NonResourceURLs, func(u string) bool {
			return u == "*" || u == at.Path || (strings.HasSuffix(u, "*") && strings.HasPrefix(at.Path, strings.TrimSuffix(u, "*")))
		})
	}
	if !has(r.APIGroups, at.Group) {
		return false
	}
	combined := at.Resource
	if at.Subresource != "" {
		combined += "/" + at.Subresource
	}
	if !slices.ContainsFunc(r.Resources, func(res string) bool {
		return res == "*" || res == combined || (at.Subresource != "" && res == "*/"+at.Subresource)
	}) {
		return false
	}
	return len(r.ResourceNames) == 0 || (at.Name != "" && slices.Contains(r.ResourceNames, at.Name))
}

// authorize is rbac.go Authorize: allowed, with "RBAC: allowed by <binding>" as its reason.
func (a *authorizer) authorize(id identity, at attributes) (bool, string) {
	ns := at.Namespace
	if at.NonResource {
		ns = ""
	}
	allowed, why := false, ""
	a.visit(id, ns, func(src string, r rbacv1.PolicyRule) bool {
		if ruleAllows(r, at) {
			allowed, why = true, "RBAC: allowed by "+src
			return false
		}
		return true
	})
	return allowed, why
}

func (a *authorizer) allowed(id identity, at attributes) bool {
	ok, _ := a.authorize(id, at)
	return ok
}

// reason is the apiserver's denial text (k8s.io/apiserver pkg/endpoints/handlers/responsewriters
// errors.go forbiddenMessage); RBAC's own deny reason is empty when no rule failed to resolve.
func reason(id identity, at attributes) string {
	if at.NonResource {
		return fmt.Sprintf("User %q cannot %s path %q", id.User, at.Verb, at.Path)
	}
	res := at.Resource
	if at.Subresource != "" {
		res += "/" + at.Subresource
	}
	if at.Namespace != "" {
		return fmt.Sprintf("User %q cannot %s resource %q in API group %q in the namespace %q", id.User, at.Verb, res, at.Group, at.Namespace)
	}
	return fmt.Sprintf("User %q cannot %s resource %q in API group %q at the cluster scope", id.User, at.Verb, res, at.Group)
}
