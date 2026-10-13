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
		assertSchemaRejectedValue(t, "rbac.impersonateGroups="+v, out)
	}
}

// assertSchemaRejectedValue: helm failed because the VALUE broke the schema —
// not because the schema itself is invalid. An invalid schema (e.g. a regex
// helm's RE2 validator cannot compile) fails every render with the same
// "values don't meet the specifications" banner, which would turn every reject
// arm green for the wrong reason (#631: a `(?!system:)` lookahead did exactly that).
func assertSchemaRejectedValue(t *testing.T, what, out string) {
	t.Helper()
	if !strings.Contains(out, "values don't meet the specifications of the schema") {
		t.Fatalf("%s failed for a reason other than the schema: %s", what, out)
	}
	if strings.Contains(out, "not valid against metaschema") {
		t.Fatalf("%s failed because values.schema.json itself is invalid, not because the value was rejected: %s", what, out)
	}
}

// #631: every built-in `system:` group is a broad population (all authenticated
// callers, all ServiceAccounts, all nodes, the RBAC bypass group). Impersonating
// any of them hands the background refresher rights far beyond any one cohort, so
// the schema refuses the whole prefix. One arm per value, so a schema that only
// enumerates a few names fails on the first one it forgot.
func TestIssue631_ChartSchemaRejectsSystemPrefixedGroups(t *testing.T) {
	for _, g := range []string{
		"system:authenticated",
		"system:unauthenticated",
		"system:serviceaccounts",
		"system:serviceaccounts:krateo-system",
		"system:nodes",
		"system:masters",
		"system:", // the bare prefix
	} {
		t.Run(g, func(t *testing.T) {
			// Listed after an allowed group: the reject must be per item,
			// not "the list's first entry".
			v := `["devs","` + g + `"]`
			_, out, err := renderSnowplowClusterRole(t, "--set-json", "rbac.impersonateGroups="+v)
			if err == nil {
				t.Fatalf("rbac.impersonateGroups=%s rendered; the schema must reject the system: prefix\n%s", v, out)
			}
			assertSchemaRejectedValue(t, "rbac.impersonateGroups="+v, out)
		})
	}
}

// #631 positive control: the prefix reject must not catch ordinary groups,
// including ones that merely CONTAIN "system:" past the start, or differ in case.
func TestIssue631_ChartSchemaAcceptsOrdinaryGroups(t *testing.T) {
	for _, v := range []string{`["devs"]`, `["devs","team:system:ops","System:Ops"]`} {
		cr, out, err := renderSnowplowClusterRole(t, "--set-json", "rbac.impersonateGroups="+v)
		if err != nil {
			t.Fatalf("rbac.impersonateGroups=%s was rejected; only the system: prefix is refused: %v\n%s", v, err, out)
		}
		if _, ok := impersonateRules(cr)["groups"]; !ok {
			t.Fatalf("rbac.impersonateGroups=%s rendered no groups rule\n%s", v, out)
		}
	}
}
