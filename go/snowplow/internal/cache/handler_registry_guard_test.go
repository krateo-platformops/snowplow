// handler_registry_guard_test.go — #708: no test in this package may leave the
// handler-extension registry different from what init() registered.
//
// THE DEFECT THIS EXISTS FOR (fixed locally in #706). The registry arms cleaned
// up with ResetHandlerExtensionsForTest, which left the registry EMPTY for every
// later test in the package: every watcher built afterwards had no typed-RBAC
// snapshot handler, so RBAC-event-driven arms passed VACUOUSLY. Nothing went red.
//
// THE GUARD. TestMain (featuregate_watchlist_test.go) snapshots the registry's
// extension names after package init — before any test runs — and compares them
// with the registry after m.Run. Any difference (an init extension MISSING, an
// EXTRA one a test registered and never removed, or a different order) fails
// the package run, naming the drift and the helper to use.
//
// WHY A TEARDOWN CHECK AND NOT A NARROWED RESET HELPER. Unexporting
// ResetHandlerExtensionsForTest (or routing it through
// isolateHandlerExtensionsForTest) closes only the reset path. The registry has
// a second way to drift: RegisterHandlerExtension is exported and append-only,
// so an arm that registers an extension and never resets leaves an EXTRA entry
// that every later watcher attaches. The teardown check reads the final state,
// whatever produced it, so it covers both — and any future mutator too.
//
// WHAT IT CANNOT SAY. It fails the package run, not the offending test: Go has
// no per-test hook. The message names the drift; isolateHandlerExtensionsForTest
// is the save-and-restore helper to use. It covers this package only (go test
// runs one process per package); the registry is not mutated anywhere else.
//
// THE MUTATION ARM. TestIssue708_GuardFailsTheRunOnALeak re-executes this test
// binary on TestIssue708_LeakyArm — an arm that resets the registry and does NOT
// restore it, active only under issue708LeakEnv — and asserts the run exits
// non-zero with the guard's message. A second child run of the same arm WITH
// the restore must exit zero, so the arm cannot pass on a failure unrelated to
// the guard.

package cache

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// issue708LeakEnv activates TestIssue708_LeakyArm in a child test process.
// Values: "leak" (reset, no restore), "restore" (reset via the save-and-restore
// helper). Unset in a normal run, where the arm skips.
const issue708LeakEnv = "SNOWPLOW_TEST_708_LEAK"

// issue708GuardMarker prefixes the guard's failure message.
const issue708GuardMarker = "#708 handler-extension registry drift"

// handlerExtensionNames is the registry's extension names in registration
// order.
func handlerExtensionNames() []string {
	snap := HandlerExtensionsSnapshot()
	out := make([]string, len(snap))
	for i, ext := range snap {
		out[i] = ext.Name
	}
	return out
}

// handlerRegistryDrift describes how after differs from the init-time set, or
// returns "" when they are identical (same names, same order).
func handlerRegistryDrift(initial, after []string) string {
	if strings.Join(initial, "\x00") == strings.Join(after, "\x00") {
		return ""
	}
	in := map[string]bool{}
	for _, n := range initial {
		in[n] = true
	}
	out := map[string]bool{}
	for _, n := range after {
		out[n] = true
	}
	var missing, extra []string
	for _, n := range initial {
		if !out[n] {
			missing = append(missing, n)
		}
	}
	for _, n := range after {
		if !in[n] {
			extra = append(extra, n)
		}
	}
	return fmt.Sprintf("%s: init registered %q, after the tests the registry holds %q (missing %q, extra %q). "+
		"A test reset or registered an extension without restoring the init set; use isolateHandlerExtensionsForTest.",
		issue708GuardMarker, initial, after, missing, extra)
}

// runWithHandlerRegistryGuard runs the package's tests and fails the run if
// they left the registry different from the init-time set. Called by TestMain.
func runWithHandlerRegistryGuard(m *testing.M) int {
	initial := handlerExtensionNames()
	code := m.Run()
	if drift := handlerRegistryDrift(initial, handlerExtensionNames()); drift != "" {
		fmt.Fprintln(os.Stderr, drift)
		if code == 0 {
			code = 1
		}
	}
	return code
}

// TestIssue708_LeakyArm is the deliberately broken arm the mutation arm runs in
// a child process. It is a no-op (skipped) in every normal run.
func TestIssue708_LeakyArm(t *testing.T) {
	switch os.Getenv(issue708LeakEnv) {
	case "leak":
		ResetHandlerExtensionsForTest() // and never restore — the #706 defect
	case "restore":
		isolateHandlerExtensionsForTest(t)
	default:
		t.Skip("child-process arm for TestIssue708_GuardFailsTheRunOnALeak")
	}
}

func TestIssue708_GuardFailsTheRunOnALeak(t *testing.T) {
	if os.Getenv(issue708LeakEnv) != "" {
		t.Skip("already a child process")
	}
	if len(handlerExtensionNames()) == 0 {
		t.Fatal("precondition: init() must register at least one extension (rbac.snapshot_writer)")
	}
	run := func(mode string) (string, error) {
		cmd := exec.Command(os.Args[0], "-test.run", "^TestIssue708_LeakyArm$", "-test.count=1")
		cmd.Env = append(os.Environ(), issue708LeakEnv+"="+mode)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	out, err := run("leak")
	if err == nil {
		t.Fatalf("#708 RED: a test that reset the registry without restoring it left the package run GREEN:\n%s", out)
	}
	if !strings.Contains(out, issue708GuardMarker) || !strings.Contains(out, `missing ["rbac.snapshot_writer"]`) {
		t.Fatalf("the leaky run failed, but not by the guard:\n%s", out)
	}

	out, err = run("restore")
	if err != nil {
		t.Fatalf("control: the same arm WITH save-and-restore must pass the guard: %v\n%s", err, out)
	}
}

// An extra extension registered and never removed is drift too.
func TestIssue708_DriftDetectsMissingExtraAndOrder(t *testing.T) {
	cases := []struct {
		after []string
		want  string
	}{
		{[]string{"a", "b"}, ""},
		{nil, `missing ["a" "b"]`},
		{[]string{"a", "b", "test.extra"}, `extra ["test.extra"]`},
		{[]string{"b", "a"}, issue708GuardMarker},
	}
	for _, c := range cases {
		got := handlerRegistryDrift([]string{"a", "b"}, c.after)
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Fatalf("drift(%q) = %q, want it to contain %q", c.after, got, c.want)
		}
	}
}
