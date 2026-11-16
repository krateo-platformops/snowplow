// issue1334_doc_rule_test.go — #1334 (PR #1342 review nit): the architecture
// doc's statement of the implicit-group rule (docs/architecture/rbac-uaf.md,
// §2.3 route 3) is the rule ImplicitGroup implements, and its k8s.io/apiserver
// citations point at the lines that hold the apiserver's rule in the module's
// pinned version.
//
// Three checks, each able to say no:
//   - the pre-#1334 wording (system:authenticated gated on a non-empty
//     username) is gone;
//   - each rule the doc states is ImplicitGroup's answer for that input;
//   - the cited version is go.mod's pin, and each cited file:line range in
//     that module holds the code the doc says it does.

package cache

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestIssue1334_RBACUAFDoc_StatesTheImplementedImplicitGroupRule(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "architecture", "rbac-uaf.md"))
	if err != nil {
		t.Fatalf("read rbac-uaf.md: %v", err)
	}
	doc := string(b)

	// (1) The old rule is gone.
	for _, stale := range []string{
		"gated on `Username != \"\"`",
		"`CRBsByGroup[\"system:authenticated\"]`",
		"`system:authenticated` for any authenticated identity",
	} {
		if strings.Contains(doc, stale) {
			t.Errorf("rbac-uaf.md still states the pre-#1334 rule: %q", stale)
		}
	}

	// (2) Each stated rule is ImplicitGroup's answer.
	rules := []struct {
		docText string // the doc's statement of the rule
		user    string
		groups  []string
		want    string
	}{
		{"`system:anonymous` gets `system:unauthenticated` (unless its groups already hold it)",
			systemAnonymousUser, nil, systemUnauthenticatedGroup},
		{"`system:anonymous` gets `system:unauthenticated` (unless its groups already hold it)",
			systemAnonymousUser, []string{systemUnauthenticatedGroup}, ""},
		{"any other user gets `system:authenticated`, unless its groups already hold",
			"alice", []string{"devs"}, systemAuthenticatedGroup},
		{"`system:authenticated` or `system:unauthenticated` — then nothing is added",
			"alice", []string{"devs", systemUnauthenticatedGroup}, ""},
		{"`system:authenticated` or `system:unauthenticated` — then nothing is added",
			"alice", []string{systemAuthenticatedGroup}, ""},
		{"`system:anonymous` is never given `system:authenticated`",
			systemAnonymousUser, []string{"devs"}, systemUnauthenticatedGroup},
	}
	flat := strings.Join(strings.Fields(doc), " ")
	for _, r := range rules {
		if !strings.Contains(flat, r.docText) {
			t.Errorf("rbac-uaf.md does not state the rule %q", r.docText)
		}
		if got := ImplicitGroup(r.user, r.groups); got != r.want {
			t.Errorf("doc rule %q: ImplicitGroup(%q, %q) = %q, want %q", r.docText, r.user, r.groups, got, r.want)
		}
	}

	// (3) The citations: version = go.mod's pin; each range holds the code.
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	pin := ""
	for _, l := range strings.Split(string(gomod), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "k8s.io/apiserver" {
			pin = f[1]
		}
	}
	if pin == "" {
		t.Fatalf("go.mod pins no k8s.io/apiserver")
	}
	if !strings.Contains(flat, "k8s.io/apiserver "+pin) {
		t.Errorf("rbac-uaf.md does not cite k8s.io/apiserver %s (go.mod's pin)", pin)
	}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "k8s.io/apiserver").Output()
	if err != nil {
		t.Fatalf("go list -m k8s.io/apiserver: %v", err)
	}
	modDir := strings.TrimSpace(string(out))
	if !strings.HasSuffix(modDir, "@"+pin) {
		t.Fatalf("k8s.io/apiserver resolves to %s, not the pin %s", modDir, pin)
	}

	cites := []struct {
		docCite string // as the doc writes it
		file    string
		from    int
		to      int
		// first/last: the range's first and last line (trimmed); holds: code
		// inside the range.
		first, last string
		holds       []string
	}{
		{"`pkg/endpoints/filters/impersonation/impersonation.go:126-144`",
			"pkg/endpoints/filters/impersonation/impersonation.go", 126, 144,
			"if username != user.Anonymous {", "}",
			[]string{"if group == user.AllAuthenticated || group == user.AllUnauthenticated {",
				"groups = append(groups, user.AllAuthenticated)"}},
		{"`:145-157`",
			"pkg/endpoints/filters/impersonation/impersonation.go", 145, 157,
			"} else {", "}",
			[]string{"if group == user.AllUnauthenticated {",
				"groups = append(groups, user.AllUnauthenticated)"}},
		{"`pkg/authentication/group/authenticated_group_adder.go:45-56`",
			"pkg/authentication/group/authenticated_group_adder.go", 45, 56,
			"if r.User.GetName() == user.Anonymous {", "newGroups = append(newGroups, user.AllAuthenticated)",
			[]string{"if group == user.AllAuthenticated || group == user.AllUnauthenticated {"}},
	}
	for _, c := range cites {
		if !strings.Contains(flat, c.docCite) {
			t.Errorf("rbac-uaf.md does not cite %s", c.docCite)
			continue
		}
		src, err := os.ReadFile(filepath.Join(modDir, c.file))
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		lines := strings.Split(string(src), "\n")
		if c.to > len(lines) {
			t.Errorf("%s has %d lines; the doc cites %d-%d", c.file, len(lines), c.from, c.to)
			continue
		}
		rng := lines[c.from-1 : c.to]
		if got := strings.TrimSpace(rng[0]); got != c.first {
			t.Errorf("%s:%d = %q, want %q", c.file, c.from, got, c.first)
		}
		if got := strings.TrimSpace(rng[len(rng)-1]); got != c.last {
			t.Errorf("%s:%d = %q, want %q", c.file, c.to, got, c.last)
		}
		body := strings.Join(rng, "\n")
		for _, h := range c.holds {
			if !strings.Contains(body, h) {
				t.Errorf("%s:%s does not hold %q", c.file, strconv.Itoa(c.from)+"-"+strconv.Itoa(c.to), h)
			}
		}
	}
}
