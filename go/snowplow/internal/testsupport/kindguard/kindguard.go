// Package kindguard keeps the kind clusters the integration tests create from
// leaking, for tests only (#742).
//
// THE LEAK. Every kind-backed test names its cluster krateo-<pid> (#522: a
// per-process name, so a process only ever deletes its own cluster) and
// deletes it in TestMain's Finish or a t.Cleanup. A test process killed from
// outside — a session-limit SIGTERM, a Ctrl-C, a `go test -timeout` panic —
// skips that cleanup, and the cluster keeps running in Docker. On 2026-10-15
// thirteen such orphans (24 min to 28 h old) starved later runs until kind
// itself failed at `kubeadm init`.
//
// THE MARK. A dead pid in a krateo-<pid> name does not prove the cluster is
// ours: other sessions create krateo-<pid> clusters too, and one of them was
// kept on purpose. So snowplow's kind tests name their clusters with a
// DEDICATED prefix, krateo-sptest-<pid> (ClusterName), and the reaper only
// ever considers that shape. A legacy unmarked krateo-<pid> cluster, or any
// other name (aruba-110, a developer's cluster), is never reaped.
//
// THE GUARD. Prepare does two things before a test creates its cluster:
//
//   - REAP: it deletes every stale MARKED cluster — its pid is dead, or alive
//     but not a Go test binary (the pid was reused). A marked cluster whose
//     owner is a live *.test process is another package's, running now, and is
//     left alone. The rule is selectStale.
//   - SIGNAL: it deletes THIS process's cluster on SIGINT / SIGTERM, then
//     re-raises the signal so the process still dies of it — unless the test
//     has already exited on its own (deleting the cluster under a TestMain
//     still in Setup fails that Setup, which exits 1 first). Either way the
//     cluster is gone: `kind delete` is a child process that outlives us.
//
// A timeout panic cannot be intercepted; the next run's reap collects it.
package kindguard

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// markedPrefix marks a kind cluster as created by snowplow's tests.
const markedPrefix = "krateo-sptest-"

// ClusterName is this process's kind cluster name: per-process (#522) and
// MARKED as snowplow's (#742), krateo-sptest-<pid>.
func ClusterName() string { return fmt.Sprintf("%s%d", markedPrefix, os.Getpid()) }

// ownedName is the only shape the reaper may ever touch: a marked name.
var ownedName = regexp.MustCompile(`^` + markedPrefix + `([0-9]+)$`)

// procInfo reports whether pid is alive and, if so, its command line.
type procInfo func(pid int) (cmdline string, alive bool)

// selectStale returns the clusters to reap: MARKED names of the exact form
// krateo-sptest-<pid> whose pid is not self and is either dead or not running
// a Go test binary (a basename ending in ".test"). Every other name — including
// a legacy unmarked krateo-<pid> — is not ours and is never returned.
func selectStale(names []string, self int, proc procInfo) []string {
	var stale []string
	for _, n := range names {
		m := ownedName.FindStringSubmatch(strings.TrimSpace(n))
		if m == nil {
			continue // foreign: never touched
		}
		pid, err := strconv.Atoi(m[1])
		if err != nil || pid <= 0 || pid == self {
			continue
		}
		cmd, alive := proc(pid)
		if alive && isTestBinary(cmd) {
			continue // a live test process owns it
		}
		stale = append(stale, m[0])
	}
	return stale
}

// isTestBinary reports whether a command line runs a Go test binary: its
// first word's basename ends in ".test" (pkg.test, as `go test` names it).
func isTestBinary(cmdline string) bool {
	f := strings.Fields(cmdline)
	return len(f) > 0 && strings.HasSuffix(filepath.Base(f[0]), ".test")
}

// psProc reads a process's full command line with ps (macOS and Linux). The
// full args, not comm: Linux truncates comm to 15 characters, which would cut
// "restactions.test" to "restactions.tes".
func psProc(pid int) (string, bool) {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "args=").Output()
	if err != nil {
		return "", false // ps exits non-zero when the pid does not exist
	}
	cmd := strings.TrimSpace(string(out))
	return cmd, cmd != ""
}

func kindClusters(ctx context.Context) ([]string, error) {
	out, err := exec.CommandContext(ctx, "kind", "get", "clusters").Output()
	if err != nil {
		return nil, err
	}
	var names []string
	for _, l := range strings.Split(string(out), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "No kind clusters") {
			names = append(names, l)
		}
	}
	return names, nil
}

func deleteCluster(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	return exec.CommandContext(ctx, "kind", "delete", "cluster", "--name", name).Run()
}

// Reap deletes every stale marked cluster (selectStale) and returns the names
// it deleted. Without a kind binary it is a no-op.
func Reap() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names, err := kindClusters(ctx)
	if err != nil {
		return nil
	}
	var reaped []string
	for _, n := range selectStale(names, os.Getpid(), psProc) {
		if err := deleteCluster(n); err != nil {
			fmt.Fprintf(os.Stderr, "kindguard: reap stale kind cluster %s: %v\n", n, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "kindguard: reaped stale kind cluster %s (its test process is gone)\n", n)
		reaped = append(reaped, n)
	}
	return reaped
}

var (
	sigMu   sync.Mutex
	sigStop chan struct{}
)

// Prepare reaps stale clusters, then deletes name on SIGINT / SIGTERM and
// re-raises the signal. Call it before creating the cluster; Release (or
// process exit) ends the signal watch. One watch per process.
func Prepare(name string) {
	Reap()
	sigMu.Lock()
	defer sigMu.Unlock()
	if sigStop != nil {
		return
	}
	stop := make(chan struct{})
	sigStop = stop
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-ch:
			fmt.Fprintf(os.Stderr, "kindguard: %s — deleting kind cluster %s before exit\n", sig.String(), name)
			if err := deleteCluster(name); err != nil {
				fmt.Fprintf(os.Stderr, "kindguard: delete kind cluster %s: %v\n", name, err)
			}
			// Re-raise with the default action so the process dies OF the
			// signal (exit 128+n), as it would have without the guard. The
			// delivery is asynchronous: give it a moment before the fallback.
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			if s, ok := sig.(syscall.Signal); ok {
				_ = syscall.Kill(os.Getpid(), s)
				time.Sleep(2 * time.Second)
			}
			os.Exit(1)
		case <-stop:
			signal.Stop(ch)
		}
	}()
}

// Release ends the signal watch (the cluster was deleted normally).
func Release() {
	sigMu.Lock()
	defer sigMu.Unlock()
	if sigStop != nil {
		close(sigStop)
		sigStop = nil
	}
}
