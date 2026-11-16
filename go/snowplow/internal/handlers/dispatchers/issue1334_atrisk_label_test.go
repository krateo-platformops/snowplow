// issue1334_atrisk_label_test.go — #1334 (PR #1342 review, mutation survivor
// 1): the #261 rotation at-risk label a cell is minted with
// (ResolvedKeyInputs.AtRiskClass) follows the apiserver's effective group set
// for an identity that is NOT authenticated.
//
// The label comes from rbac's firstMatchSubjectClass, which classifies the
// WINNING subject of the first-permitting binding. Its implicit-group branch
// must be the apiserver's implicit group for THIS requester (implicitGroup),
// not "system:authenticated for any non-empty username":
//
//   - krateo:public presenting system:unauthenticated, granted by a binding
//     whose subjects are [Group system:authenticated, Group
//     system:unauthenticated]: the first subject is NOT this requester's
//     group (the apiserver adds no system:authenticated to it); the winning
//     subject is the presented system:unauthenticated — REGULAR, AtRiskNone.
//     Main's rule labels it implicit-group (AtRiskImplicitGroup).
//   - system:anonymous with no groups, granted only through Group
//     system:unauthenticated: that group is the apiserver's implicit one for
//     this requester (impersonation.go:145-157, k8s.io/apiserver v0.36.5),
//     not a presented one — AtRiskImplicitGroup. Main's rule labels it
//     regular (AtRiskNone).
//
// The label is driven through the REAL key mint (dispatchCacheLookupKey →
// EvaluateRBAC with WinningSubjectClassOut) over a REAL published snapshot.

package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const (
	i1334LabelMixed  = "label-mixed-1334"  // granted to [system:authenticated, system:unauthenticated]
	i1334LabelUnauth = "label-unauth-1334" // granted to [system:unauthenticated]
	i1334Anonymous   = "system:anonymous"
)

func i1334LabelSeed() []runtime.Object {
	g := "rbac.authorization.k8s.io"
	cr := func(name, raName string) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group},
				Resources: []string{h1RAGVR.Resource}, ResourceNames: []string{raName}}},
		}
	}
	return []runtime.Object{
		cr("label-mixed-reader-1334", i1334LabelMixed),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "label-mixed-1334", UID: types.UID("uid-crb-1334-label-mixed")},
			Subjects: []rbacv1.Subject{
				{Kind: "Group", APIGroup: g, Name: "system:authenticated"},
				{Kind: "Group", APIGroup: g, Name: i1334Unauth},
			},
			RoleRef: rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "label-mixed-reader-1334"},
		},
		cr("label-unauth-reader-1334", i1334LabelUnauth),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "label-unauth-1334", UID: types.UID("uid-crb-1334-label-unauth")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: i1334Unauth}},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "label-unauth-reader-1334"},
		},
	}
}

func TestIssue1334_AtRiskLabel_FollowsTheApiserverEffectiveGroups(t *testing.T) {
	i1334BuildWatcher(t, i1334LabelSeed()...)
	unauth := []string{i1334Unauth}
	const (
		mixedUID  = "C:uid-crb-1334-label-mixed"
		unauthUID = "C:uid-crb-1334-label-unauth"
		authUID   = "C:uid-crb-1334-authenticated"
	)
	cases := []struct {
		name      string
		user      string
		groups    []string
		ra        string
		wantUID   string
		wantClass cache.AtRiskClass
	}{
		// The survivor-1 rows.
		{"public + system:unauthenticated, mixed binding: presented group wins", i1334Public, unauth, i1334LabelMixed, mixedUID, cache.AtRiskNone},
		{"visitor + system:unauthenticated, mixed binding: presented group wins", i1334Visitor, unauth, i1334LabelMixed, mixedUID, cache.AtRiskNone},
		{"system:anonymous, no groups, unauthenticated-only binding: implicit", i1334Anonymous, nil, i1334LabelUnauth, unauthUID, cache.AtRiskImplicitGroup},
		// The rest of the matrix.
		{"system:anonymous, no groups, mixed binding: implicit system:unauthenticated", i1334Anonymous, nil, i1334LabelMixed, mixedUID, cache.AtRiskImplicitGroup},
		{"system:anonymous presenting system:unauthenticated: regular", i1334Anonymous, unauth, i1334LabelUnauth, unauthUID, cache.AtRiskNone},
		{"public + system:unauthenticated, unauthenticated-only binding: regular", i1334Public, unauth, i1334LabelUnauth, unauthUID, cache.AtRiskNone},
		{"system:anonymous, authenticated-only grant: no permit", i1334Anonymous, nil, i1334AuthOnlyRA, "", cache.AtRiskNone},
		// Controls: an authenticated user holds system:authenticated implicitly.
		{"authenticated user, authenticated-only grant: implicit", i1334Alice, nil, i1334AuthOnlyRA, authUID, cache.AtRiskImplicitGroup},
		{"authenticated user, mixed binding: implicit system:authenticated", i1334Alice, nil, i1334LabelMixed, mixedUID, cache.AtRiskImplicitGroup},
		{"authenticated user, unauthenticated-only grant: no permit", i1334Alice, nil, i1334LabelUnauth, "", cache.AtRiskNone},
	}
	// Twice: the first mint walks the snapshot cold, the second reads the
	// class back off the snapshot authz memo.
	for pass := 0; pass < 2; pass++ {
		for _, c := range cases {
			_, handle, in := dispatchCacheLookupKey(i1334Ctx(c.user, c.groups), "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, c.ra, -1, -1, nil)
			if handle == nil || in == nil {
				t.Fatalf("pass %d %s: no live handle/inputs (cache not engaged)", pass, c.name)
			}
			if in.BindingUID != c.wantUID {
				t.Errorf("pass %d %s: first-match BindingUID = %q, want %q", pass, c.name, in.BindingUID, c.wantUID)
			}
			if in.AtRiskClass != c.wantClass {
				t.Errorf("pass %d %s: at-risk label = %v, want %v", pass, c.name, in.AtRiskClass, c.wantClass)
			}
		}
	}
}
