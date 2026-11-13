//go:build integration

package chartcrd

// issue1334_public_identity_kind_test.go — #1334, the real-apiserver half of the
// public identity's chart guard. It runs inside
// TestIssue657_Kind_CrdgenCRDAdmittedAndRulesEnforced, on that test's kind
// cluster (057's apiserver minor), so it adds no cluster of its own.
//
//  1. The crdgen CRD admits spec.publicIdentity.enabled=true and refuses a
//     non-boolean (the value core-provider writes).
//  2. The ClusterRole the chart renders with publicIdentity.enabled=true is
//     admitted by a server-side dry-run, and then created and bound to a
//     ServiceAccount.
//  3. That ServiceAccount, impersonating EXACTLY user krateo:public + group
//     system:unauthenticated (what snowplow sends):
//     - is authenticated by the apiserver as that user with exactly
//       [system:unauthenticated] (SelfSubjectReview): no system:authenticated
//       is added (impersonation.go);
//     - is refused a ConfigMap granted ONLY to system:authenticated, and served
//       one granted to the USER krateo:public;
//     - may NOT impersonate the group system:authenticated (the grant is by
//       resourceNames).
//     Control: the same user impersonated with NO group is authenticated, and is
//     served the system:authenticated-only ConfigMap.

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

const (
	i1334PublicUser  = "krateo:public"
	i1334PublicGroup = "system:unauthenticated"
	i1334NS          = "krateo-public"
)

// issue1334RenderClusterRole renders templates/clusterrole.yaml with helm, the
// way the release workflow does (placeholder versions substituted).
func issue1334RenderClusterRole(t *testing.T, args ...string) *rbacv1.ClusterRole {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("SNOWPLOW_REQUIRE_HELM") == "1" {
			t.Fatal("helm not on PATH and SNOWPLOW_REQUIRE_HELM=1")
		}
		t.Skip("helm not on PATH — NOT RUN: the #1334 ClusterRole dry-run needs the rendered chart")
	}
	dst := filepath.Join(t.TempDir(), "snowplow")
	if out, err := exec.Command("cp", "-R", filepath.Dir(chartSchemaPath), dst).CombinedOutput(); err != nil {
		t.Fatalf("copy chart: %v: %s", err, out)
	}
	chartYAML := filepath.Join(dst, "Chart.yaml")
	raw, err := os.ReadFile(chartYAML)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("CHART_VERSION"), []byte("0.0.0"))
	raw = bytes.ReplaceAll(raw, []byte("APP_VERSION"), []byte("0.0.0"))
	if err := os.WriteFile(chartYAML, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := append([]string{"template", "t", dst, "--namespace", "krateo-system", "--show-only", "templates/clusterrole.yaml"}, args...)
	out, err := exec.Command(helm, cmd...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, out)
	}
	var cr rbacv1.ClusterRole
	if err := yaml.Unmarshal(out, &cr); err != nil {
		t.Fatalf("rendered ClusterRole does not parse: %v\n%s", err, out)
	}
	return &cr
}

// issue1334KindArms runs the #1334 arms on the caller's cluster. crCreate
// dry-runs a CR of the crdgen CRD with the given spec.
func issue1334KindArms(t *testing.T, ctx context.Context, cfg *rest.Config, crCreate func(spec map[string]any) error) {
	t.Run("crd/publicIdentity", func(t *testing.T) {
		if err := crCreate(map[string]any{"publicIdentity": map[string]any{"enabled": true}}); err != nil {
			t.Fatalf("the apiserver refused spec.publicIdentity.enabled=true: %v", err)
		}
		if err := crCreate(map[string]any{"publicIdentity": map[string]any{"enabled": "yes"}}); err == nil {
			t.Fatal("the apiserver admitted spec.publicIdentity.enabled=\"yes\"; only a boolean opts in")
		}
	})

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dryRun := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	g := rbacv1.GroupName

	cr := issue1334RenderClusterRole(t, "--set", "publicIdentity.enabled=true")
	cr.Name = "issue1334-snowplow"
	t.Run("clusterrole/server-dry-run", func(t *testing.T) {
		if _, err := cs.RbacV1().ClusterRoles().Create(ctx, cr, dryRun); err != nil {
			t.Fatalf("server-side dry-run refused the ClusterRole rendered with publicIdentity.enabled=true: %v", err)
		}
		off := issue1334RenderClusterRole(t)
		off.Name = "issue1334-snowplow-off"
		if _, err := cs.RbacV1().ClusterRoles().Create(ctx, off, dryRun); err != nil {
			t.Fatalf("server-side dry-run refused the default ClusterRole: %v", err)
		}
	})

	// The rendered ClusterRole, bound to a ServiceAccount standing in for
	// snowplow's.
	mustCreate := func(what string, err error) {
		t.Helper()
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create %s: %v", what, err)
		}
	}
	_, err = cs.RbacV1().ClusterRoles().Create(ctx, cr, metav1.CreateOptions{})
	mustCreate("ClusterRole", err)
	_, err = cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: i1334NS}}, metav1.CreateOptions{})
	mustCreate("namespace", err)
	_, err = cs.CoreV1().ServiceAccounts("default").Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "issue1334-snowplow"}}, metav1.CreateOptions{})
	mustCreate("ServiceAccount", err)
	_, err = cs.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "issue1334-snowplow"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "issue1334-snowplow", Namespace: "default"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: cr.Name},
	}, metav1.CreateOptions{})
	mustCreate("ClusterRoleBinding", err)

	// Two ConfigMaps in the public namespace: one granted only to
	// system:authenticated, one granted only to the USER krateo:public.
	for _, name := range []string{"authenticated-only", "public-page"} {
		_, err = cs.CoreV1().ConfigMaps(i1334NS).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		mustCreate("ConfigMap "+name, err)
	}
	role := func(name, cm string) {
		_, err := cs.RbacV1().Roles(i1334NS).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{cm}, Verbs: []string{"get"}}},
		}, metav1.CreateOptions{})
		mustCreate("Role "+name, err)
	}
	role("issue1334-authenticated-only", "authenticated-only")
	role("issue1334-public-page", "public-page")
	bind := func(name string, subject rbacv1.Subject) {
		_, err := cs.RbacV1().RoleBindings(i1334NS).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Subjects:   []rbacv1.Subject{subject},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: name},
		}, metav1.CreateOptions{})
		mustCreate("RoleBinding "+name, err)
	}
	bind("issue1334-authenticated-only", rbacv1.Subject{Kind: "Group", APIGroup: g, Name: "system:authenticated"})
	bind("issue1334-public-page", rbacv1.Subject{Kind: "User", APIGroup: g, Name: i1334PublicUser})

	exp := int64(600)
	tok, err := cs.CoreV1().ServiceAccounts("default").CreateToken(ctx, "issue1334-snowplow",
		&authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{ExpirationSeconds: &exp}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("token for the ServiceAccount: %v", err)
	}
	saCfg := rest.AnonymousClientConfig(cfg)
	saCfg.BearerToken = tok.Status.Token
	as := func(user string, groups []string) *kubernetes.Clientset {
		c := rest.CopyConfig(saCfg)
		c.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: groups}
		out, err := kubernetes.NewForConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	public := as(i1334PublicUser, []string{i1334PublicGroup})

	t.Run("impersonated/effective-groups", func(t *testing.T) {
		got, err := public.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authnv1.SelfSubjectReview{}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("SelfSubjectReview as the public identity: %v", err)
		}
		ui := got.Status.UserInfo
		if ui.Username != i1334PublicUser || !slices.Equal(ui.Groups, []string{i1334PublicGroup}) {
			t.Fatalf("the apiserver authenticates the public identity as %q %v; want %q [%s] exactly (no system:authenticated)",
				ui.Username, ui.Groups, i1334PublicUser, i1334PublicGroup)
		}
		ctl, err := as(i1334PublicUser, nil).AuthenticationV1().SelfSubjectReviews().Create(ctx, &authnv1.SelfSubjectReview{}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("SelfSubjectReview control: %v", err)
		}
		if !slices.Contains(ctl.Status.UserInfo.Groups, "system:authenticated") {
			t.Fatalf("CONTROL: the same user with no impersonated group must be authenticated; got %v", ctl.Status.UserInfo.Groups)
		}
	})

	t.Run("impersonated/rbac", func(t *testing.T) {
		if _, err := public.CoreV1().ConfigMaps(i1334NS).Get(ctx, "authenticated-only", metav1.GetOptions{}); !apierrors.IsForbidden(err) {
			t.Fatalf("the public identity read a ConfigMap granted only to system:authenticated (err=%v); want 403", err)
		}
		if _, err := public.CoreV1().ConfigMaps(i1334NS).Get(ctx, "public-page", metav1.GetOptions{}); err != nil {
			t.Fatalf("the public identity was refused the ConfigMap bound to its USER: %v", err)
		}
		if _, err := public.CoreV1().ConfigMaps(i1334NS).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
			t.Fatalf("the public identity listed ConfigMaps (err=%v); no rule grants list, want 403", err)
		}
		if _, err := as(i1334PublicUser, nil).CoreV1().ConfigMaps(i1334NS).Get(ctx, "authenticated-only", metav1.GetOptions{}); err != nil {
			t.Fatalf("CONTROL: the same user with no group (authenticated) must read the system:authenticated ConfigMap: %v", err)
		}
	})

	t.Run("impersonated/grant-is-by-name", func(t *testing.T) {
		_, err := as(i1334PublicUser, []string{"system:authenticated"}).CoreV1().ConfigMaps(i1334NS).Get(ctx, "public-page", metav1.GetOptions{})
		if !apierrors.IsForbidden(err) {
			t.Fatalf("the ServiceAccount impersonated group system:authenticated (err=%v); the grant must name system:unauthenticated only", err)
		}
	})

	// The same Role read straight from the apiserver with no credential at all
	// is refused: the page's grant names the USER, never a group.
	t.Run("anonymous/refused", func(t *testing.T) {
		anon, err := dynamic.NewForConfig(rest.AnonymousClientConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		_, err = anon.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).
			Namespace(i1334NS).Get(ctx, "public-page", metav1.GetOptions{})
		if err == nil {
			t.Fatal("an anonymous apiserver request read the public page's ConfigMap; its Role must bind the user, not a group")
		}
	})
}
