// issue1127_obs_watch_carriers_test.go — 1.12.7 review C-2: one arm per
// watch-error carrier, pinning the PLACEMENT rather than the arithmetic.
//
// WHAT THE OLD ARM GOT WRONG. It called recordWatchError() five times in a loop
// and asserted the counter moved by five. That asserts an atomic add works. The
// counter's entire value is that it counts EVERY reflector retry rather than
// only the first transition, and that property lives in WHERE the increment
// sits relative to each handler's one-shot guard. Moving the increment below
// the guard destroys the counter completely — and the old arm stayed green.
//
// WHAT THESE ASSERT INSTEAD. Each drives the REAL installed handler twice and
// asserts the counter moved by 2 while the WARN appeared once. Below-the-guard
// placement fails them immediately: the second invocation returns early (or
// skips the increment), the counter reads 1, and the arm names the defect.
//
// THREE CARRIERS, THREE ARMS. The three handlers have DIFFERENT one-shot
// mechanisms — per-GVR map membership, and two process-sticky atomic.Bool CAS
// guards — so one arm cannot stand for the others. The two CAS handlers are the
// dangerous ones: their guard never clears, so a below-the-CAS increment would
// record exactly one error for the life of the pod however long the watch
// stayed broken.
package cache

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	clientcache "k8s.io/client-go/tools/cache"
)

// wcSyncBuf is a mutex-guarded slog sink (a process-lived ticker logs into
// whatever default is installed, so a bare buffer races under -race).
type wcSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *wcSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *wcSyncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// wcCountWarns counts emitted records whose msg is exactly want.
func wcCountWarns(captured, want string) int {
	n := 0
	for _, line := range strings.Split(captured, "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec["msg"] == want {
			n++
		}
	}
	return n
}

// wcDriveTwice invokes the real handler twice under a captured logger and
// returns how far watch_errors_total moved and what was logged.
func wcDriveTwice(t *testing.T, handler func(*clientcache.Reflector, error)) (moved uint64, captured string) {
	t.Helper()
	var buf wcSyncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	before := InformerWatchStatsSnapshot().WatchErrorsTotal
	handler(nil, errWatchProbe{})
	handler(nil, errWatchProbe{})
	return InformerWatchStatsSnapshot().WatchErrorsTotal - before, buf.String()
}

type errWatchProbe struct{}

func (errWatchProbe) Error() string { return "synthetic reflector failure" }

// TestIssue1127Obs_WatchErrors_PerGVRCarrier — the per-GVR handler, whose
// one-shot is membership in rw.watchBroken.
func TestIssue1127Obs_WatchErrors_PerGVRCarrier(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetInformerWatchStatsForTest()
	t.Cleanup(ResetInformerWatchStatsForTest)

	rw := newSyntheticRemoveWatcher(t, obsGVR())
	t.Cleanup(rw.Stop)

	// The REAL handler the installer wires onto the informer.
	moved, captured := wcDriveTwice(t, rw.watchErrorHandlerFor(obsGVR()))

	if moved != 2 {
		t.Fatalf("RED (C-2): two reflector errors on the per-GVR handler moved watch_errors_total "+
			"by %d, want 2. The increment is below the one-shot, so the counter reports transitions "+
			"instead of retries — a watch failing once and a watch failing every second read the "+
			"same, which is the blindness this counter exists to remove", moved)
	}
	if n := wcCountWarns(captured, "cache.watch.broken"); n != 1 {
		t.Fatalf("C-2: the one-shot WARN fired %d times, want exactly 1 — counting every error must "+
			"not cost the log its suppression:\n%s", n, captured)
	}
}

// TestIssue1127Obs_WatchErrors_SecretsCarrier — a STICKY process-wide CAS that
// never clears, so a below-the-CAS increment would record one error for the
// life of the pod.
func TestIssue1127Obs_WatchErrors_SecretsCarrier(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetInformerWatchStatsForTest()
	t.Cleanup(ResetInformerWatchStatsForTest)
	secretsWatchBroken.Store(false)
	t.Cleanup(func() { secretsWatchBroken.Store(false) })

	moved, captured := wcDriveTwice(t, secretsWatchErrorHandler())

	if moved != 2 {
		t.Fatalf("RED (C-2): two reflector errors on the SECRETS handler moved watch_errors_total "+
			"by %d, want 2. Its one-shot is a sticky CAS that never clears, so an increment below "+
			"it records exactly ONE error for the life of the process however long the watch stays "+
			"broken", moved)
	}
	if n := wcCountWarns(captured, "cache.secrets.watch.broken"); n != 1 {
		t.Fatalf("C-2: the secrets one-shot WARN fired %d times, want exactly 1:\n%s", n, captured)
	}
}

// TestIssue1127Obs_WatchErrors_ControllerHealthCarrier — the third carrier,
// same sticky-CAS shape, separate guard variable.
func TestIssue1127Obs_WatchErrors_ControllerHealthCarrier(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetInformerWatchStatsForTest()
	t.Cleanup(ResetInformerWatchStatsForTest)
	controllerHealthWatchBroken.Store(false)
	t.Cleanup(func() { controllerHealthWatchBroken.Store(false) })

	moved, captured := wcDriveTwice(t, controllerHealthWatchErrorHandler())

	if moved != 2 {
		t.Fatalf("RED (C-2): two reflector errors on the CONTROLLER-HEALTH handler moved "+
			"watch_errors_total by %d, want 2. Sticky CAS, same as secrets: below it the counter "+
			"is pinned at 1 forever", moved)
	}
	if n := wcCountWarns(captured, "cache.controller_health.watch.broken"); n != 1 {
		t.Fatalf("C-2: the controller-health one-shot WARN fired %d times, want exactly 1:\n%s",
			n, captured)
	}
}
