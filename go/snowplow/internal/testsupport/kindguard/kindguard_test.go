//go:build unit

package kindguard

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

// #742 — the reaper's selection rule. Only MARKED krateo-sptest-<pid> names
// are ever candidates; of those, a cluster whose pid runs a live Go test
// binary (or is this process) is kept, and one whose pid is dead or reused by
// a non-test process is reaped. An UNMARKED krateo-<pid> is never reaped, even
// with a dead pid: it may be another session's (team-lead, 2026-10-16).
func TestIssue742_SelectStaleReapsOnlyOrphanedKrateoPidClusters(t *testing.T) {
	const self = 4242
	procs := map[int]string{
		100:  "/var/folders/xx/go-build123/b001/rbac.test -test.v -test.timeout=20m",                    // a live test process
		101:  "/private/var/folders/T/go-build9/b301/restactions.test -test.paniconexit0 -test.count=1", // live, long name (comm would be truncated)
		200:  "/Applications/Safari.app/Contents/MacOS/Safari",                                          // pid REUSED by an unrelated process
		201:  "go test ./...",                                                                           // `go` itself, not a test binary
		self: "/usr/local/bin/dlv exec ./kindguard.test",                                                // self under a wrapper: kept only by the self rule
	}
	proc := func(pid int) (string, bool) {
		c, ok := procs[pid]
		return c, ok
	}
	names := []string{
		"krateo-sptest-100",    // live owner: keep
		"krateo-sptest-101",    // live owner, long binary name: keep
		"krateo-sptest-200",    // reused pid: reap
		"krateo-sptest-201",    // `go`, not a *.test: reap
		"krateo-sptest-300",    // dead pid: reap
		"krateo-sptest-4242",   // this process: keep
		" krateo-sptest-301 ",  // whitespace around a dead owner: reap (kind output lines)
		"krateo-300",           // UNMARKED, dead pid: KEPT (may be another session's)
		"krateo-200",           // UNMARKED, reused pid: KEPT
		"aruba-110",            // foreign: never
		"krateo-sptest-",       // foreign: never
		"krateo-sptest-12x",    // foreign: never
		"my-krateo-sptest-300", // foreign: never
		"krateo-sptest-300-x",  // foreign: never
	}
	got := selectStale(names, self, proc)
	sort.Strings(got)
	want := []string{"krateo-sptest-200", "krateo-sptest-201", "krateo-sptest-300", "krateo-sptest-301"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectStale = %v, want %v", got, want)
	}
}

// The arm the ownership rule exists for: an unmarked krateo-<pid> whose pid is
// dead is KEPT — a dead pid does not prove the cluster is ours.
func TestIssue742_AnUnmarkedKrateoPidClusterWithADeadPidIsKept(t *testing.T) {
	dead := func(int) (string, bool) { return "", false }
	if got := selectStale([]string{"krateo-48152", "krateo-4365"}, 1, dead); len(got) != 0 {
		t.Fatalf("selectStale reaped unmarked clusters %v — only krateo-sptest-<pid> may ever be reaped", got)
	}
	if got := selectStale([]string{ClusterName()[:len(markedPrefix)] + "48152"}, 1, dead); len(got) != 1 {
		t.Fatalf("selectStale kept a MARKED cluster with a dead pid: %v", got)
	}
	if n := ClusterName(); !ownedName.MatchString(n) {
		t.Fatalf("ClusterName %q is not reapable by its own rule", n)
	}
}

func TestIssue742_IsTestBinary(t *testing.T) {
	for cmd, want := range map[string]bool{
		"/tmp/go-build1/b001/cache.test -test.v": true,
		"cache.test":                             true,
		"/usr/local/bin/kind get clusters":       false,
		"":                                       false,
		"/opt/test/bin/server --test":            false,
		"/tmp/attest":                            false,
		"/usr/local/bin/foo.tester serve":        false, // ".test" inside the name, not its suffix
	} {
		if got := isTestBinary(cmd); got != want {
			t.Errorf("isTestBinary(%q) = %v, want %v", cmd, got, want)
		}
	}
}

// The production ps probe agrees with the rule for this very process: it is
// alive and a Go test binary, so its own krateo-<pid> is never reaped.
func TestIssue742_PsProbeSeesThisTestProcessAsALiveOwner(t *testing.T) {
	cmd, alive := psProc(os.Getpid())
	if !alive || !isTestBinary(cmd) {
		t.Fatalf("psProc(self) = (%q, %v): this test binary must read as a live *.test owner", cmd, alive)
	}
	if got := selectStale([]string{ClusterName()}, -1, psProc); len(got) != 0 {
		t.Fatalf("selectStale reaped this live process's own cluster: %v", got)
	}
	if _, alive := psProc(0x7ffffffe); alive {
		t.Fatalf("psProc reported a nonexistent pid alive")
	}
}
