// main_test.go — the #1184 guard's own discrimination proof. Each arm runs the
// checker compiled fresh (`go run .`) over a fixture, exactly as
// scripts/check_request_log_context.sh runs it over the module.
package main

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestGate_FlagsEveryShape(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "testdata/red").CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: the checker exited 0 on testdata/red.\n%s", out)
	}
	s := string(out)
	for _, want := range []string{
		"sites.go:18  log.Warn drops the context in scope (ctx)",
		"sites.go:22  slog.Error drops the context in scope (ctx)",
		"sites.go:26  slog.Default().Warn drops the context in scope (ctx)",
		"sites.go:30  slog.Warn drops the context in scope (req.Context())",
		"sites.go:34  r.log.Error drops the context in scope (r.ctx)",
		"sites.go:38  slog.Warn drops the context in scope (b.ctx)",
		"sites.go:43  slog.Warn drops the context in scope (ctx)",
		"sites.go:48  slog.Warn drops the context in scope (the unnamed context parameter)",
		"sites.go:52  slog.Error drops the context in scope (the unnamed context parameter)",
		"sites.go:56  slog.WarnContext drops the context in scope (ctx)",
		"sites.go:60  log.Log drops the context in scope (ctx)",
		"sites.go:64  log.LogAttrs drops the context in scope (ctx)",
		"sites.go:68  log.Warn drops the context in scope (ctx)", // the method value
		"sites.go:73  slog.Warn in noContextNoWaiver has no context in scope and no waiver",
		"sites.go:77  slog.ErrorContext in detachedNoContextNoWaiver has no context in scope and no waiver",
		"sites.go:80  waiver on no WARN/ERROR site",
		"sites.go:83  slog.Warn drops the context in scope (ctx)",
		"sites.go:82  waiver on a site with a context in scope",
		`sites.go:87  waiver names an unknown class "because"`,
		"sites.go:92  waiver has no reason",
		"sites.go:97  waiver on no WARN/ERROR site",
		// L3 and L4
		"sites.go:110  logsWith is handed a literal background context, with ctx in scope",
		"sites.go:114  forwards is handed a literal background context, with req.Context() in scope",
		"sites.go:126  wrapper delegates context.Background(), which drops the context in scope (ctx)",
		"sites.go:130  wrapperOfAWrapper delegates context.Background(), which drops the context in scope (req.Context())",
		"sites.go:134  wrapper delegates context.Background(), which drops the context in scope (r.ctx)",
		"sites.go:137  wrapper delegates context.Background() and is taken as a function value",
		"sites.go:142  wrapper delegates context.Background() and is taken as a function value",
		"sites.go:146  waiver class detached excuses only a cut context, not a site with none",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", want, s)
		}
	}
	if !strings.Contains(s, "ERROR: 29 WARN/ERROR log site(s)") {
		t.Errorf("expected exactly 29 findings.\nOutput:\n%s", s)
	}
}

func TestGate_AcceptsEveryContextShape(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "testdata/green").CombinedOutput()
	if err != nil {
		t.Fatalf("the checker flagged the clean fixture: %v\n%s", err, out)
	}
	if want := "OK: 8 WARN/ERROR log sites pass a context; 3 have none to pass"; !strings.Contains(string(out), want) {
		t.Fatalf("expected %q (the look-alikes must not count).\nOutput:\n%s", want, out)
	}
}

// TestGate_ReportNamesEverySiteOnce pins the enumeration the PR's classification
// comes from: one line per site, with its state.
func TestGate_ReportNamesEverySiteOnce(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "-report", "testdata/green").CombinedOutput()
	if err != nil {
		t.Fatalf("report: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 11 {
		t.Fatalf("want 11 report lines, got %d:\n%s", len(lines), out)
	}
	waived := 0
	for _, l := range lines {
		if strings.Contains(l, "\twaived:") {
			waived++
		}
	}
	if waived != 3 {
		t.Fatalf("want 3 waived sites, got %d:\n%s", waived, fmt.Sprint(lines))
	}
}

// TestGate_ModuleIsClean is the guard over the real tree under `go test ./...`:
// a violation is RED here as well as in lint-gates.
func TestGate_ModuleIsClean(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "../..").CombinedOutput()
	if err != nil {
		t.Fatalf("the module has WARN/ERROR sites that drop their context: %v\n%s", err, out)
	}
	m := regexp.MustCompile(`OK: (\d+) WARN/ERROR log sites pass a context; (\d+) have none`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("no verdict line:\n%s", out)
	}
	// A floor over the enumeration: a checker that stops seeing sites reads as
	// clean on an empty list.
	if n, _ := strconv.Atoi(string(m[1])); n < 240 {
		t.Fatalf("the checker saw only %d passing sites; the module has about 250:\n%s", n, out)
	}
}
