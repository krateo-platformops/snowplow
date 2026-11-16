package cache

// ready_settle_819_test.go — #819 / #828 review: the readiness settle's edges.
//
// Since #1458 the settle only gives the bindings index its navigated set: the
// index is never built from a snapshot, so there is no build to run or defer
// at the flip (the old P2, a publish between the deferral's request and its
// re-check, has nothing left to test).
//
//   P1  a NON-NIL but INCOMPLETE snapshot at the flip (the initial publish has
//       not returned: an event-driven publish during the initial LIST can hold
//       a RoleBinding before its Role) → the flip must NOT make the index
//       ready; when the binding's event and the complete publish have come and
//       the index is marked ready, the binding is under its role.
//   P3  a panic inside the settle is recovered and counted; Ready still flips.
//   O1  RefreshStream.Rotated offers only keys THIS connection armed.

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pendingInitialPublishWatcher is a watcher whose initial RBAC publish has not
// returned yet, installed as Global().
func pendingInitialPublishWatcher(t *testing.T) *ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	rw := &ResourceWatcher{rbacInitialPublishDone: make(chan struct{})}
	prev := Global()
	SetGlobal(rw)
	prevSnap := rbacSnap.Load()
	reset := func() {
		ResetPhase1DoneForTest()
		ResetBindingsByGVRIndexForTest()
		ResetRBACSubGenForTest()
		readySettleHookForTest.Store(nil)
	}
	reset()
	t.Cleanup(func() {
		SetGlobal(prev)
		rbacSnap.Store(prevSnap)
		reset()
	})
	return rw
}

func rb819() *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-819", Name: "reader-bind", UID: "rb-819"},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-819"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "reader"},
	}
}

// partial819 holds the binding but not its role; complete819 holds both.
func partial819() *RBACSnapshot {
	return &RBACSnapshot{RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{"tenant-819": {rb819()}}}
}

func complete819() *RBACSnapshot {
	s := partial819()
	s.RolesByNSName = map[string]*rbacv1.Role{"tenant-819/reader": {
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-819", Name: "reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}},
	}}
	return s
}

func byRoleLen() int {
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.membersByRole)
}

func TestIssue819_P1_IncompleteSnapshotAtFlip_IndexIsNotReady(t *testing.T) {
	rw := pendingInitialPublishWatcher(t)
	rbacSnap.Store(partial819())

	MarkPhase1Done()

	if !IsPhase1Done() {
		t.Fatal("P1: readiness did not flip")
	}
	if BindingsByGVRIndexBuilt() {
		t.Fatal("P1 RED: the readiness flip made the index ready over an INCOMPLETE snapshot, before the RBAC " +
			"handlers were delivered their initial lists (#686, #1458)")
	}
	// The binding's event, the complete publish, then ready: what the informer
	// and the initial-publish goroutine do.
	onBindingAdd(rb819())
	rbacSnap.Store(complete819())
	close(rw.rbacInitialPublishDone)
	markBindingsIndexReady()
	if !BindingsByGVRIndexBuilt() || byRoleLen() != 1 {
		t.Fatalf("P1: ready with the binding delivered before it: ready=%v byRole=%d, want true / 1",
			BindingsByGVRIndexBuilt(), byRoleLen())
	}
}

func TestIssue819_P3_PanicInSettleIsRecovered_ReadyStillFlips(t *testing.T) {
	pendingInitialPublishWatcher(t)
	rbacSnap.Store(partial819())
	const panicValue = "P3-settle-panic-value"
	hook := func() { panic(panicValue) }
	readySettleHookForTest.Store(&hook)
	before := ReadySettleStatsSnapshot().Panics
	logs := &lockedBuffer819{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	defer slog.SetDefault(prevLogger)

	MarkPhase1Done() // must not panic out
	slog.SetDefault(prevLogger)
	out := logs.String()
	if !strings.Contains(out, "ready_settle_panic") || !strings.Contains(out, "panic_type=string") {
		t.Fatalf("P3: the recovered panic was not logged with its type: %q", out)
	}
	if strings.Contains(out, panicValue) {
		t.Fatal("P3: the recovered panic's VALUE reached the log — log only its type (metadata only)")
	}

	if !IsPhase1Done() {
		t.Fatal("P3 RED: a panic in the settle skipped the flip — /readyz would stay 503 for good")
	}
	if got := ReadySettleStatsSnapshot().Panics - before; got != 1 {
		t.Fatalf("P3: panics moved by %d, want 1", got)
	}
}

func TestIssue819_O1_RotatedOffersOnlyThisConnectionsArmedKeys(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "")
	ResetRefreshBroadcasterForTest()
	t.Cleanup(ResetRefreshBroadcasterForTest)

	mine := SubscribeRefreshStream(map[string]struct{}{"k-mine": {}})
	defer mine.Unsub()
	other := SubscribeRefreshStream(map[string]struct{}{"k-other": {}})
	defer other.Unsub()

	mine.Rotated([]string{"k-mine", "k-other", "k-nobody"})

	got := map[string]bool{}
	deadline := time.After(300 * time.Millisecond)
loop:
	for {
		select {
		case k := <-mine.Keys:
			got[k] = true
		case <-deadline:
			break loop
		}
	}
	if !got["k-mine"] {
		t.Fatal("O1: the connection's own rotated key was not offered")
	}
	if got["k-other"] || got["k-nobody"] {
		t.Fatalf("O1 RED: a key this connection did not arm reached it: %v — a rotation frame must carry only "+
			"the subscriber's own key", got)
	}
	select {
	case k := <-other.Keys:
		t.Fatalf("O1: another connection received %q from this connection's rotation", k)
	default:
	}
}

// lockedBuffer819 is a goroutine-safe log sink (other goroutines may log while
// the default logger points at it).
type lockedBuffer819 struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer819) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer819) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
