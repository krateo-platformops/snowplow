// Package cache — Ship 0.30.242 H.c-layered Phase 2 step 2a.
//
// This file is the SINGLE SOURCE OF TRUTH for binding-identity derivation.
//
// PACKAGE BOUNDARY:
//   internal/rbac/evaluate.go imports internal/cache (TRACED at evaluate.go:23).
//   internal/cache MUST NOT import internal/rbac (cycle).
//   Therefore the BindingUID constructors live HERE (the cache package),
//   and the rbac package calls back via the cache.BindingUIDFromCRB /
//   cache.BindingUIDFromRB exports.
//
// LINT GATE (Phase 3 F4 falsifier):
//   This file is the lint's SOLE WHITELISTED location for deriving a
//   BindingUID from snapshot indexes. Every "snap.CRBsBy*" or
//   "snap.RBsBy*" identity-projection iteration that produces a
//   BindingUID OUTSIDE this file fails the lint gate. The existing
//   snapshot-writer (rbac_snapshot.go), the reverse index builder
//   (bindings_by_gvr.go), and the rbac package's selectCRBCandidates /
//   selectRBCandidates are file-allowlisted (their iterations are NOT
//   BindingUID derivation — writer / index / candidate-fan-out).
//
// HISTORY:
//   Pre-ship the cohort identity lived as a uint64 hash
//   (`BindingSetHash` — rbac_cohort_gen.go:231) over the full matched
//   binding-set. Under H.c-layered, the L1 cell key carries a per-layer
//   BindingUID string — the metadata.uid of the FIRST-MATCH binding that
//   authorised the layer's access. The empty-UID fallback content tuple
//   matches v3's `crbIdentity` / `rbIdentity` (preserved at byte-equivalent
//   shape so synthetic test fixtures keep their pre-ship identity).
package cache

import (
	rbacv1 "k8s.io/api/rbac/v1"
)

// The kube-apiserver's implicit identities (k8s.io/apiserver
// pkg/authentication/user: AllAuthenticated, AllUnauthenticated, Anonymous).
// match_subject.go is the canonical home for RBAC subject-matching primitives
// (design §4.3). Which implicit group a request carries is decided ONLY by
// ImplicitGroup below: nothing in snowplow may assume that every requester is
// authenticated (#1334).
const (
	systemAuthenticatedGroup   = "system:authenticated"
	systemUnauthenticatedGroup = "system:unauthenticated"
	systemAnonymousUser        = "system:anonymous"
)

// ImplicitGroup returns the one group the kube-apiserver adds to a request
// whose user is username and whose groups are groups, or "" when it adds none.
// It is the apiserver's own rule, which both of its paths apply identically
// (k8s.io/apiserver v0.36.5 — the module's pin — and v0.37.2, same lines):
//
//   - authentication: pkg/authentication/group/authenticated_group_adder.go
//     AuthenticateRequest adds system:authenticated unless the user is
//     system:anonymous or the groups already hold system:authenticated or
//     system:unauthenticated. A real anonymous request is authenticated by
//     pkg/authentication/request/anonymous as system:anonymous with exactly
//     [system:unauthenticated], and the adder leaves it alone.
//   - impersonation: pkg/endpoints/filters/impersonation/impersonation.go
//     126-144 applies the same rule to an impersonated non-anonymous user, and
//     145-157 adds system:unauthenticated to an impersonated system:anonymous
//     unless it is already present.
//
// The rule does not special-case an empty username (the apiserver never sees
// one). Snowplow's evaluator separately treats Username=="" as "no requester"
// and matches no implicit group for it (rbac.anySubjectMatches); the prewarm
// group representative ({"", [g]}) gets system:authenticated explicitly via
// WithAuthenticatedGroup, exactly as a real member of g would.
func ImplicitGroup(username string, groups []string) string {
	if username == systemAnonymousUser {
		for _, g := range groups {
			if g == systemUnauthenticatedGroup {
				return ""
			}
		}
		return systemUnauthenticatedGroup
	}
	for _, g := range groups {
		if g == systemAuthenticatedGroup || g == systemUnauthenticatedGroup {
			return ""
		}
	}
	return systemAuthenticatedGroup
}

// Authenticated reports whether the kube-apiserver treats a request by
// (username, groups) as authenticated: whether its effective group set
// (WithAuthenticatedGroup, the apiserver's rule) holds system:authenticated.
//
// An identity it does not treat as authenticated — one presenting
// system:unauthenticated, or system:anonymous — holds no credential snowplow
// could present for it, so snowplow may act for it only by impersonation, never
// under its own ServiceAccount (#1334). The background drivers read this to
// refuse their ServiceAccount fallback for such an identity.
func Authenticated(username string, groups []string) bool {
	for _, g := range WithAuthenticatedGroup(username, groups) {
		if g == systemAuthenticatedGroup {
			return true
		}
	}
	return false
}

// The two groups the kube-apiserver places every ServiceAccount in
// (k8s.io/apiserver pkg/authentication/serviceaccount: AllServiceAccountsGroup
// and MakeNamespaceGroupName).
const (
	allServiceAccountsGroup       = "system:serviceaccounts"
	serviceAccountsNamespaceGroup = "system:serviceaccounts:"
)

// ServiceAccountGroups returns the two groups every ServiceAccount of namespace
// is a member of: all ServiceAccounts, and the ServiceAccounts of namespace.
func ServiceAccountGroups(namespace string) (all, ofNamespace string) {
	return allServiceAccountsGroup, serviceAccountsNamespaceGroup + namespace
}

// MatchedSubject is one binding subject a requester matches bindings through:
// the rbac/v1 subject Kind, its Name and, for a ServiceAccount, its Namespace.
// Implicit is true when the requester holds the subject without presenting it
// (the apiserver's implicit group, the two ServiceAccount groups).
type MatchedSubject struct {
	Kind      string
	Name      string
	Namespace string
	Implicit  bool
}

func (m MatchedSubject) key() subjectKey {
	return subjectKey{Kind: m.Kind, Name: m.Name, Namespace: m.Namespace}
}

// SubjectMatcher holds one requester (username, groups) and answers the ONE
// question every RBAC consumer asks of it: through which binding subjects does
// this requester match a binding (#1443).
//
// THE RULE. A requester matches a binding iff one of the binding's subjects is
// in the requester's matched-subject set. That set is enumerated by Visit and
// by nothing else:
//
//   - the evaluator matches with it (rbac.anySubjectMatches → Match) and routes
//     its candidate indexes with it (selectCRBCandidates, routeRBSubjects);
//   - the identity class folds the sub-generation counter of exactly it
//     (RBACSubGenForSubject, foldSubjects), as do the reseed predicates built on
//     foldSubjects (RotatedSubjectSet.Rotated, IdentityAuthorisedForGVR);
//   - a binding event bumps the binding's subjects in the same (Kind, Name,
//     Namespace) form (subjectsFromRBAC).
//
// So "a change to this binding can move this requester's verdict" implies "the
// change bumps a counter this requester folds": the class rotates whenever the
// verdict can. Before #1443 the evaluator and the fold each kept their own
// list, and the fold's lacked the ServiceAccount groups and the User form of a
// ServiceAccount username; a rules edit behind such a binding moved the verdict
// and rotated nothing.
//
// The zero value matches nothing.
type SubjectMatcher struct {
	username string
	groups   []string
	implicit string
	saNS     string
	saName   string
	isSA     bool
	// The two ServiceAccount groups of saNS, built once (the namespace one is
	// the only string this type allocates).
	saAll, saOfNS string
}

// NewSubjectMatcher returns the matcher for a request by (username, groups).
// It keeps groups by reference and allocates nothing, except one string (the
// namespace group's name) for a ServiceAccount username.
func NewSubjectMatcher(username string, groups []string) SubjectMatcher {
	m := SubjectMatcher{username: username, groups: groups}
	if username == "" {
		// "No requester": the evaluator gives it no implicit group. The prewarm
		// group representative ({"", [g]}) carries system:authenticated as a
		// PRESENTED group instead (WithAuthenticatedGroup).
		return m
	}
	m.implicit = ImplicitGroup(username, groups)
	if m.saNS, m.saName, m.isSA = parseServiceAccountUsername(username); m.isSA {
		m.saAll, m.saOfNS = ServiceAccountGroups(m.saNS)
	}
	return m
}

// Visit calls visit for every subject the requester matches bindings through,
// until visit returns false. It is the single enumeration of the set:
//
//  1. User <username>, for a non-empty username — a ServiceAccount's username
//     (system:serviceaccount:<ns>:<name>) included: a User-kind subject naming
//     it matches like any other user name;
//  2. Group <g> for every presented group;
//  3. for a ServiceAccount username, Group system:serviceaccounts and Group
//     system:serviceaccounts:<ns>, presented or not;
//  4. the apiserver's implicit group (ImplicitGroup), if it adds one;
//  5. for a ServiceAccount username, ServiceAccount <ns>/<name>.
//
// A subject may be visited twice (a presented group that is also an implicit
// one); the presented visit comes first.
func (m SubjectMatcher) Visit(visit func(MatchedSubject) bool) {
	if m.username != "" && !visit(MatchedSubject{Kind: subjectKindUser, Name: m.username}) {
		return
	}
	for _, g := range m.groups {
		if !visit(MatchedSubject{Kind: subjectKindGroup, Name: g}) {
			return
		}
	}
	if m.isSA {
		if !visit(MatchedSubject{Kind: subjectKindGroup, Name: m.saAll, Implicit: true}) ||
			!visit(MatchedSubject{Kind: subjectKindGroup, Name: m.saOfNS, Implicit: true}) {
			return
		}
	}
	if m.implicit != "" && !visit(MatchedSubject{Kind: subjectKindGroup, Name: m.implicit, Implicit: true}) {
		return
	}
	if m.isSA {
		visit(MatchedSubject{Kind: subjectKindServiceAccount, Name: m.saName, Namespace: m.saNS})
	}
}

// Match reports whether binding subject s is in the requester's matched-subject
// set, and whether the requester holds it implicitly (see MatchedSubject). A
// subject of any other kind than User, Group or ServiceAccount matches no one.
// A ServiceAccount subject is compared by its namespace as written.
func (m SubjectMatcher) Match(s rbacv1.Subject) (matched, implicit bool) {
	want, ok := bindingSubjectKey(s)
	if !ok {
		return false, false
	}
	m.Visit(func(ms MatchedSubject) bool {
		if ms.key() == want {
			matched, implicit = true, ms.Implicit
			return false
		}
		return true
	})
	return matched, implicit
}

// bindingSubjectKey is the (Kind, Name, Namespace) form of one binding subject:
// the form a binding event bumps (subjectsFromRBAC) and the form Match compares.
// ok is false for a kind RBAC does not define.
func bindingSubjectKey(s rbacv1.Subject) (subjectKey, bool) {
	switch s.Kind {
	case rbacv1.UserKind, rbacv1.GroupKind:
		return subjectKey{Kind: s.Kind, Name: s.Name}, true
	case rbacv1.ServiceAccountKind:
		return subjectKey{Kind: s.Kind, Name: s.Name, Namespace: s.Namespace}, true
	}
	return subjectKey{}, false
}

// SubjectIdentity is the minimal subject shape consumed by the RBAC
// evaluator. Equivalent in surface to k8s authn/v1.UserInfo's required
// fields (Username + Groups). The cache-package mirror exists because
// internal/cache cannot import the rbac package (package-boundary cycle —
// see header).
//
// Used by:
//   - the prewarm enumerator (prewarm_enumeration.go — Phase 2b) as the
//     representative tuple for each prewarm target.
//   - pickRepresentativeFromSubjects — derives a SubjectIdentity from a
//     binding's Subjects slice.
//
// Username == "" denotes anonymous / no permit / no snapshot. Groups may
// be nil (User-kind subjects), single-element (Group-kind), or unset
// (ServiceAccount-kind — Username carries the canonical
// system:serviceaccount:<ns>:<name> form).
type SubjectIdentity struct {
	Username string
	Groups   []string
}

// BindingUIDFromCRB returns the stable per-binding identity for a CRB.
// The "C:" prefix namespaces CRB UIDs away from RB UIDs (the latter
// also prefix with "R:<ns>/" — see BindingUIDFromRB).
//
// Empty-UID fallback: synthetic / test-fixture bindings (and theoretically
// any apiserver pre-stamp gap) may have an empty UID. The fallback hashes
// the binding's content tuple (Name + RoleRef apiGroup/Kind/Name). This is
// the SAME mechanism the deleted crbIdentity (was at rbac_cohort_gen.go
// pre-Ship-S.2 line ~420) used — preserved across migration so synthetic
// fixtures retain their pre-ship identity.
//
// SAFETY: returns "" iff p == nil. Production binding pointers come from
// the snapshot's typed indexes — never nil; the nil-guard is defensive.
func BindingUIDFromCRB(p *rbacv1.ClusterRoleBinding) string {
	if p == nil {
		return ""
	}
	if uid := string(p.UID); uid != "" {
		return "C:" + uid
	}
	// US-001 separator (\x1f) keeps Name and RoleRef substrings from
	// colliding in the fallback tuple.
	return "C:fallback/" + p.Name +
		"\x1f" + p.RoleRef.APIGroup + "/" + p.RoleRef.Kind + "/" + p.RoleRef.Name
}

// BindingUIDFromRB returns the stable per-binding identity for an RB.
// "R:<ns>/" prefix keeps RB UIDs distinct from CRB UIDs AND distinct
// across namespaces (two RBs in different namespaces with the same UID
// produce different BindingUIDs — defensive; apiserver never reuses UIDs
// across namespaces but the prefix carries scope information into the
// key shape directly).
//
// Empty-UID fallback: same content-tuple shape as BindingUIDFromCRB,
// preserved from the deleted rbIdentity.
//
// SAFETY: returns "" iff p == nil.
func BindingUIDFromRB(p *rbacv1.RoleBinding) string {
	if p == nil {
		return ""
	}
	if uid := string(p.UID); uid != "" {
		return "R:" + p.Namespace + "/" + uid
	}
	return "R:" + p.Namespace + "/fallback/" + p.Name +
		"\x1f" + p.RoleRef.APIGroup + "/" + p.RoleRef.Kind + "/" + p.RoleRef.Name
}

// pickRepresentativeFromSubjects derives a representative SubjectIdentity
// from a binding's Subjects slice. Used by the prewarm enumerator (Phase
// 2b — prewarm_enumeration.go) to pick a single concrete identity to
// drive per-binding prewarm dispatches.
//
// CONTRACT (per design §7.2 + 4-ambiguity resolution):
//   - User-kind subject:           SubjectIdentity{Username: <user>, Groups: nil}
//   - Group-kind subject:          SubjectIdentity{Username: "",     Groups: []string{<group>}}
//   - ServiceAccount-kind subject: SubjectIdentity{Username: "system:serviceaccount:<ns>:<name>", Groups: nil}
//   - empty subjects:              SubjectIdentity{Username: "", Groups: nil} (the zero value)
//
// First-wins among the subject kinds in subjects[]; the cache cell is
// keyed by the BINDING's UID, not by subject identity, so the
// representative is only used at evaluate-time for the prewarm dispatch
// (any cohort member granted by the same binding produces the same cell
// contents — that's the whole sharing rationale, design §1.2).
//
// SAFETY: returns the zero value for nil/empty subjects.
func pickRepresentativeFromSubjects(subjects []rbacv1.Subject) SubjectIdentity {
	for _, s := range subjects {
		switch s.Kind {
		case rbacv1.UserKind:
			if s.Name != "" {
				return SubjectIdentity{Username: s.Name}
			}
		case rbacv1.GroupKind:
			if s.Name != "" {
				return SubjectIdentity{Groups: []string{s.Name}}
			}
		case rbacv1.ServiceAccountKind:
			if s.Name != "" && s.Namespace != "" {
				return SubjectIdentity{
					Username: "system:serviceaccount:" + s.Namespace + ":" + s.Name,
				}
			}
		}
	}
	return SubjectIdentity{}
}
