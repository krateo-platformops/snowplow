// issue1334_apiserver_group_parity_test.go — #1334: snowplow computes the SAME
// effective group set as the kube-apiserver, for the key/digest fold and for the
// evaluator's live path.
//
// THE REFERENCE IS THE APISERVER ITSELF, not a copy of its rule: k8s.io/apiserver
// is already a dependency (v0.36.5), so each input runs through
//
//   - impersonation: the real filter
//     pkg/endpoints/filters/impersonation.WithImpersonation (the group rule is
//     impersonation.go:126-157, the same lines in v0.36.5 and v0.37.2), with an
//     allow-all authorizer, reading the user the filter hands downstream. This
//     is how the apiserver sees a request snowplow impersonates.
//   - authentication: the real pkg/authentication/group.AuthenticatedGroupAdder
//     over the groups the authenticator produced — for a real anonymous request,
//     the real pkg/authentication/request/anonymous authenticator.
//
// Snowplow is read through its PRODUCTION derivations only:
//
//   - key/digest group set: effectiveGroups over cache.WithAuthenticatedGroup
//     (what SubjectBindingIDs evaluates — the SA synthetic groups come from
//     effectiveGroups, the apiserver's implicit group from WithAuthenticatedGroup);
//   - the binding-set digest's ids (rbac.SubjectBindingIDs) over a published
//     snapshot holding one binding per candidate group;
//   - the live path: rbac.EvaluateRBAC for the RAW (username, groups) against
//     the same snapshot — index route, subject matcher and rule walk together,
//     through both a ClusterRoleBinding and a RoleBinding per group.
//
// Every one of them must grant through exactly the apiserver's group set.

package evaltest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters/impersonation"
	"k8s.io/apiserver/pkg/endpoints/request"
)

type allowAll1334 struct{}

func (allowAll1334) Authorize(context.Context, authorizer.Attributes) (authorizer.Decision, string, error) {
	return authorizer.DecisionAllow, "", nil
}

// apiserverImpersonatedGroups runs (username, groups) through the apiserver's
// impersonation filter as snowplow's ServiceAccount sends it (Impersonate-User
// plus one Impersonate-Group per group) and returns the groups of the user the
// filter passes downstream.
func apiserverImpersonatedGroups(t *testing.T, username string, groups []string) []string {
	t.Helper()
	var got user.Info
	h := impersonation.WithImpersonation(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = request.UserFrom(r.Context())
	}), allowAll1334{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
	req = req.WithContext(request.WithUser(req.Context(), &user.DefaultInfo{
		Name:   "system:serviceaccount:krateo-system:snowplow",
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:krateo-system", user.AllAuthenticated},
	}))
	req.Header.Set(authnv1.ImpersonateUserHeader, username)
	for _, g := range groups {
		req.Header.Add(authnv1.ImpersonateGroupHeader, g)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got == nil {
		t.Fatalf("impersonation filter refused (%q, %q): %d %s", username, groups, rec.Code, rec.Body.String())
	}
	return got.GetGroups()
}

// apiserverAuthenticatedGroups runs a request its authenticator resolved to
// (username, groups) through the apiserver's AuthenticatedGroupAdder.
func apiserverAuthenticatedGroups(t *testing.T, username string, groups []string) []string {
	t.Helper()
	inner := authenticator.RequestFunc(func(*http.Request) (*authenticator.Response, bool, error) {
		return &authenticator.Response{User: &user.DefaultInfo{Name: username, Groups: append([]string(nil), groups...)}}, true, nil
	})
	return apiserverAuthenticate(t, group.NewAuthenticatedGroupAdder(inner))
}

func apiserverAuthenticate(t *testing.T, a authenticator.Request) []string {
	t.Helper()
	resp, ok, err := a.AuthenticateRequest(httptest.NewRequest(http.MethodGet, "/api", nil))
	if err != nil || !ok {
		t.Fatalf("authenticate: ok=%v err=%v", ok, err)
	}
	return resp.User.GetGroups()
}

func set1334(gs []string) []string {
	m := map[string]struct{}{}
	for _, g := range gs {
		if g != "" {
			m[g] = struct{}{}
		}
	}
	out := make([]string, 0, len(m))
	for g := range m {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

func equal1334(a, b []string) bool {
	return strings.Join(set1334(a), "\x00") == strings.Join(set1334(b), "\x00")
}

const (
	p1334APIGroup = "p1334.test"
	p1334NS       = "ns-1334"
	p1334OtherNS  = "elsewhere-1334"
)

// p1334Universe is every group name a row's apiserver set can contain (the
// test asserts want ⊆ universe, so a grant through a group outside it cannot go
// unseen).
var p1334Universe = []string{
	user.AllAuthenticated, user.AllUnauthenticated,
	"system:serviceaccounts", "system:serviceaccounts:ns1", "devs",
}

// p1334World: for the i-th universe group, a ClusterRoleBinding granting `get
// cres-i` cluster-wide and a RoleBinding granting `get nsres-i` in p1334NS,
// both to Group universe[i] only.
func p1334World() []runtime.Object {
	var objs []runtime.Object
	for i, g := range p1334Universe {
		n := strings.ReplaceAll(strings.ReplaceAll(g, ":", "-"), "system-", "s-")
		cr := clusterRole("cr-1334-"+n, rule([]string{p1334APIGroup}, []string{p1334ClusterRes(i)}, []string{"get"}))
		crb := clusterRoleBinding("crb-1334-"+n, cr.Name, groupSubject(g))
		crb.UID = types.UID("uid-crb-1334-" + n)
		r := role(p1334NS, "r-1334-"+n, rule([]string{p1334APIGroup}, []string{p1334NSRes(i)}, []string{"get"}))
		rb := roleBinding(p1334NS, "rb-1334-"+n, "Role", r.Name, groupSubject(g))
		rb.UID = types.UID("uid-rb-1334-" + n)
		objs = append(objs, cr, crb, r, rb)
	}
	return objs
}

func p1334ClusterRes(i int) string { return "cres" + string(rune('a'+i)) }
func p1334NSRes(i int) string      { return "nsres" + string(rune('a'+i)) }

// p1334BindingIDs is the pair of binding ids SubjectBindingIDs reports for a
// grant through universe[i].
func p1334BindingIDs(i int) (string, string) {
	n := strings.ReplaceAll(strings.ReplaceAll(p1334Universe[i], ":", "-"), "system-", "s-")
	return "C:uid-crb-1334-" + n, "R:" + p1334NS + "/uid-rb-1334-" + n
}

type row1334 struct {
	name     string
	username string
	groups   []string
	// authnGroups: the groups the apiserver's AUTHENTICATOR yields for this
	// identity on a direct request; nil means groups.
	authnGroups []string
	realAnon    bool // the authenticator is the real anonymous one
}

func rows1334() []row1334 {
	return []row1334{
		{name: "plain user", username: "alice", groups: []string{"devs"}},
		{name: "plain user, no groups", username: "alice"},
		{name: "user + system:unauthenticated (the #1334 public identity)", username: "krateo:public", groups: []string{user.AllUnauthenticated}},
		{name: "user + group + system:unauthenticated", username: "krateo:public", groups: []string{"devs", user.AllUnauthenticated}},
		{name: "system:anonymous", username: user.Anonymous, realAnon: true},
		{name: "system:anonymous + system:unauthenticated", username: user.Anonymous, groups: []string{user.AllUnauthenticated}, realAnon: true},
		{name: "user already in system:authenticated", username: "bob", groups: []string{"devs", user.AllAuthenticated}},
		{name: "serviceaccount", username: "system:serviceaccount:ns1:sa1",
			authnGroups: []string{"system:serviceaccounts", "system:serviceaccounts:ns1"}},
	}
}

func TestIssue1334_EffectiveGroups_ParityWithAPIServer(t *testing.T) {
	newTestWatcher(t, p1334World()...)
	snap := cache.LiveRBACSnapshot()
	if snap == nil {
		t.Fatalf("setup: no published RBAC snapshot")
	}
	inUniverse := map[string]bool{}
	for _, g := range p1334Universe {
		inUniverse[g] = true
	}

	for _, r := range rows1334() {
		t.Run(r.name, func(t *testing.T) {
			want := set1334(apiserverImpersonatedGroups(t, r.username, r.groups))

			// The apiserver's two paths must agree, else the reference itself
			// is ambiguous for this input.
			var authn []string
			switch {
			case r.realAnon:
				authn = apiserverAuthenticate(t, group.NewAuthenticatedGroupAdder(anonymous.NewAuthenticator(nil)))
			case r.authnGroups != nil:
				authn = apiserverAuthenticatedGroups(t, r.username, r.authnGroups)
			default:
				authn = apiserverAuthenticatedGroups(t, r.username, r.groups)
			}
			if !equal1334(authn, want) {
				t.Fatalf("reference split: impersonation %q vs authentication %q", want, set1334(authn))
			}
			for _, g := range want {
				if !inUniverse[g] {
					t.Fatalf("setup: apiserver group %q is outside the probed universe", g)
				}
			}

			// (1) The key/digest group set.
			saNS, _, isSA := rbac.ParseServiceAccountUsernameForTest(r.username)
			keyGroups := rbac.EffectiveGroupsForTest(cache.WithAuthenticatedGroup(r.username, r.groups), isSA, saNS)
			if !equal1334(keyGroups, want) {
				t.Errorf("key/digest group set (WithAuthenticatedGroup + effectiveGroups) = %q, apiserver = %q",
					set1334(keyGroups), want)
			}

			// (2) The binding-set digest's ids, and (3) the live evaluator, per
			// universe group.
			ids := map[string]bool{}
			for _, id := range rbac.SubjectBindingIDs(snap, r.username, r.groups) {
				ids[id] = true
			}
			var digestVia, liveVia []string
			for i, g := range p1334Universe {
				crbID, rbID := p1334BindingIDs(i)
				if ids[crbID] != ids[rbID] {
					t.Errorf("group %q: digest holds the CRB=%v but the RB=%v", g, ids[crbID], ids[rbID])
				}
				if ids[crbID] {
					digestVia = append(digestVia, g)
				}
				viaCRB, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: r.username, Groups: r.groups, Verb: "get",
					Group: p1334APIGroup, Resource: p1334ClusterRes(i), Namespace: p1334OtherNS,
				})
				if err != nil {
					t.Fatalf("EvaluateRBAC: %v", err)
				}
				viaRB, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: r.username, Groups: r.groups, Verb: "get",
					Group: p1334APIGroup, Resource: p1334NSRes(i), Namespace: p1334NS,
				})
				if err != nil {
					t.Fatalf("EvaluateRBAC: %v", err)
				}
				if viaCRB != viaRB {
					t.Errorf("group %q: live verdict via ClusterRoleBinding=%v but via RoleBinding=%v", g, viaCRB, viaRB)
				}
				if viaCRB {
					liveVia = append(liveVia, g)
				}
			}
			if !equal1334(digestVia, want) {
				t.Errorf("binding-set digest counts the bindings of groups %q, apiserver = %q", set1334(digestVia), want)
			}
			if !equal1334(liveVia, want) {
				t.Errorf("live EvaluateRBAC grants through groups %q, apiserver = %q", set1334(liveVia), want)
			}
		})
	}
}
