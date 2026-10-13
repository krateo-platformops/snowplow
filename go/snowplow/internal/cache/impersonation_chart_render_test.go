package cache

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// impersonation_chart_render_test.go — #608 (a): the chart RENDERS the group
// allowlist 057 needs, and the empty-list trap stays closed in the RENDERED
// ClusterRole, not just in the template text (impersonation_chart_test.go reads
// text only and says so).
//
// It runs the real `helm template` against a copy of helm/snowplow, with the
// release workflow's placeholder versions substituted exactly as lint.yaml does.
// helm is required: set SNOWPLOW_REQUIRE_HELM=1 to make its absence a failure
// instead of a skip.

func renderSnowplowClusterRole(t *testing.T, args ...string) (*rbacv1.ClusterRole, string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("SNOWPLOW_REQUIRE_HELM") == "1" {
			t.Fatal("helm not on PATH and SNOWPLOW_REQUIRE_HELM=1")
		}
		t.Skip("helm not on PATH — NOT RUN: the chart render arm needs helm (CI lint renders the default values only)")
	}
	src := chartPath(t)
	dst := filepath.Join(t.TempDir(), "snowplow")
	if out, err := exec.Command("cp", "-R", src, dst).CombinedOutput(); err != nil {
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
	cmdArgs := append([]string{"template", "t", dst, "--namespace", "krateo-system", "--show-only", "templates/clusterrole.yaml"}, args...)
	out, err := exec.Command(helm, cmdArgs...).CombinedOutput()
	if err != nil {
		return nil, string(out), err
	}
	var cr rbacv1.ClusterRole
	if err := yaml.Unmarshal(out, &cr); err != nil {
		t.Fatalf("rendered ClusterRole does not parse: %v\n%s", err, out)
	}
	return &cr, string(out), nil
}

func impersonateRules(cr *rbacv1.ClusterRole) map[string]rbacv1.PolicyRule {
	out := map[string]rbacv1.PolicyRule{}
	for _, r := range cr.Rules {
		for _, v := range r.Verbs {
			if v == "impersonate" {
				for _, res := range r.Resources {
					out[res] = r
				}
			}
		}
	}
	return out
}

func TestIssue608_ChartRendersGroupAllowlist(t *testing.T) {
	cr, out, err := renderSnowplowClusterRole(t,
		"--set-json", `rbac.impersonateGroups=["devs","viewers"]`,
		"--set-json", `rbac.impersonateServiceAccounts=["bot"]`)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	rules := impersonateRules(cr)
	if g := rules["groups"]; strings.Join(g.ResourceNames, ",") != "devs,viewers" {
		t.Fatalf("groups rule resourceNames = %v, want [devs viewers]\n%s", g.ResourceNames, out)
	}
	if s := rules["serviceaccounts"]; strings.Join(s.ResourceNames, ",") != "bot" {
		t.Fatalf("serviceaccounts rule resourceNames = %v, want [bot]\n%s", s.ResourceNames, out)
	}
	if u, ok := rules["users"]; !ok || len(u.ResourceNames) != 0 {
		t.Fatalf("users rule must stay unconditional and name-free; got %+v", u)
	}
}

// THE TRAP, rendered: an empty allowlist must emit NO rule, because an empty
// resourceNames grants every name.
func TestIssue608_ChartEmptyAllowlistRendersNoRule(t *testing.T) {
	for _, args := range [][]string{
		nil, // shipped defaults
		{"--set-json", `rbac.impersonateGroups=[]`, "--set-json", `rbac.impersonateServiceAccounts=[]`},
	} {
		cr, out, err := renderSnowplowClusterRole(t, args...)
		if err != nil {
			t.Fatalf("helm template %v: %v\n%s", args, err, out)
		}
		rules := impersonateRules(cr)
		for _, res := range []string{"groups", "serviceaccounts"} {
			if r, ok := rules[res]; ok {
				t.Fatalf("SECURITY: an empty allowlist rendered an impersonate %s rule with resourceNames=%v — "+
					"in RBAC that grants EVERY %s\n%s", res, r.ResourceNames, res, out)
			}
		}
	}
}

// The schema refuses the values that would turn the allowlist into an
// escalation: system:masters (bypasses RBAC entirely), "*", and an empty name.
func TestIssue608_ChartSchemaRejectsPrivilegedOrEmptyNames(t *testing.T) {
	for _, v := range []string{`["system:masters"]`, `["*"]`, `[""]`, `["devs","devs"]`} {
		_, out, err := renderSnowplowClusterRole(t, "--set-json", "rbac.impersonateGroups="+v)
		if err == nil {
			t.Fatalf("rbac.impersonateGroups=%s rendered; the schema must reject it\n%s", v, out)
		}
		if !strings.Contains(out, "values don't meet the specifications of the schema") {
			t.Fatalf("rbac.impersonateGroups=%s failed for a reason other than the schema: %s", v, out)
		}
	}
}
