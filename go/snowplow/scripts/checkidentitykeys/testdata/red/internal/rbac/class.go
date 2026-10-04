// Fixture: the single class derivation, intact.
package rbac

import "example/internal/cache"

type UserInfo struct {
	Username string
	Groups   []string
}

func SubjectBindingSetDigest(username string, groups []string) string { return username }

func WithAuthenticatedGroup(groups []string) []string { return groups }

func IdentityClassOf(ui UserInfo) cache.IdentityClass {
	return cache.IdentityClass{
		SubjectBindingSet: SubjectBindingSetDigest(ui.Username, ui.Groups),
		RBACSubGen:        cache.RBACSubGenForSubject(ui.Username, WithAuthenticatedGroup(ui.Groups)),
	}
}
