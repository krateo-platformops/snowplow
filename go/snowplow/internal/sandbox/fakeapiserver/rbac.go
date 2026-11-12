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
// RBAC authorizer decides it: plugin/pkg/auth/authorizer/rbac/rbac.go for the binding walk, and
// pkg/apis/rbac/v1/evaluation_helpers.go for the rule matching (k8s.io/kubernetes, which no
// library exports, so the two are transcribed here). It is the only authorizer: no allow-all, no
// node authorizer, no webhook.

const saPrefix = "system:serviceaccount:"

// identity is an authenticated (or impersonated) user.
type identity struct {
	User   string
	Groups []string
	Label  string // the request log's identity: sa | visitor | other | none
}

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

func subjectMatches(s rbacv1.Subject, id identity) bool {
	switch s.Kind {
	case rbacv1.UserKind:
		return s.Name == id.User
	case rbacv1.GroupKind:
		return slices.Contains(id.Groups, s.Name)
	case rbacv1.ServiceAccountKind:
		return id.User == saPrefix+s.Namespace+":"+s.Name
	}
	return false
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

// rulesFor is the rules that apply to id in namespace ns ("" = cluster scope only).
func (a *authorizer) rulesFor(id identity, ns string) []rbacv1.PolicyRule {
	var rules []rbacv1.PolicyRule
	for _, b := range a.clusterRoleBindings {
		if slices.ContainsFunc(b.Subjects, func(s rbacv1.Subject) bool { return subjectMatches(s, id) }) {
			rules = append(rules, a.roleRules(b.RoleRef, "")...)
		}
	}
	if ns != "" {
		for _, b := range a.roleBindings {
			if b.Namespace != ns {
				continue
			}
			if slices.ContainsFunc(b.Subjects, func(s rbacv1.Subject) bool { return subjectMatches(s, id) }) {
				rules = append(rules, a.roleRules(b.RoleRef, ns)...)
			}
		}
	}
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

func (a *authorizer) allowed(id identity, at attributes) bool {
	ns := at.Namespace
	if at.NonResource {
		ns = ""
	}
	return slices.ContainsFunc(a.rulesFor(id, ns), func(r rbacv1.PolicyRule) bool { return ruleAllows(r, at) })
}

// reason is the RBAC authorizer's denial text (rbac.go: "User %q cannot %s resource %q ...").
func reason(id identity, at attributes) string {
	if at.NonResource {
		return fmt.Sprintf("User %q cannot %s path %q", id.User, at.Verb, at.Path)
	}
	res := at.Resource
	if at.Subresource != "" {
		res += "/" + at.Subresource
	}
	scope := "at the cluster scope"
	if at.Namespace != "" {
		scope = fmt.Sprintf("in the namespace %q", at.Namespace)
	}
	return fmt.Sprintf("User %q cannot %s resource %q in API group %q %s", id.User, at.Verb, res, at.Group, scope)
}
