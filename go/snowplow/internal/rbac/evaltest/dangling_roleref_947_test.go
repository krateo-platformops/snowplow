// dangling_roleref_947_test.go — #947 falsifiers for the evaluate side.
//
// A binding whose roleRef names an absent role used to log
// `rbac.evaluate.snapshot.miss` on EVERY evaluation that reached it (392M
// rows on one cluster). In a Synced snapshot the evaluator must deny without
// logging; the snapshot writer reports the binding once. In a snapshot built
// before every RBAC informer finished its initial LIST, the miss is a
// consistency fault and stays loud on every evaluation.
//
// Assertions are counts of log records, miss-counter deltas and verdicts;
// none reads a clock.
package evaltest

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

// sink947 captures every record at Info and above.
type sink947 struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (s *sink947) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }
func (s *sink947) WithAttrs([]slog.Attr) slog.Handler           { return s }
func (s *sink947) WithGroup(string) slog.Handler                { return s }
func (s *sink947) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	s.recs = append(s.recs, r.Clone())
	s.mu.Unlock()
	return nil
}

// count returns how many records satisfy keep.
func (s *sink947) count(keep func(slog.Record) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if keep(r) {
			n++
		}
	}
	return n
}

func attr947(r slog.Record, key string) string {
	v := ""
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}

// evaluatePathLine reports a line from the evaluate path: every message it
// emits starts with "rbac.".
func evaluatePathLine(r slog.Record) bool { return strings.HasPrefix(r.Message, "rbac.") }

// capture947 routes the default logger AND the request logger to one sink, so
// a line from either reaches the count. Returns the sink and a context that
// carries the request logger.
func capture947(t *testing.T) (*sink947, context.Context) {
	t.Helper()
	sink := &sink947{}
	prev := slog.Default()
	slog.SetDefault(slog.New(sink))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sink, xcontext.BuildContext(context.Background(), xcontext.WithLogger(slog.New(sink)))
}

// TestDanglingRoleRef947_EvaluateLogsNothing: with a binding to an absent
// role in a Synced snapshot, N evaluations that reach the binding deny, emit
// 0 lines from the evaluate path and 0 snapshot misses; the snapshot writer
// emitted exactly 1 "dangling" line and the gauge is 1. One case per shape.
func TestDanglingRoleRef947_EvaluateLogsNothing(t *testing.T) {
	const evaluations = 200
	cases := []struct {
		name        string
		binding     runtime.Object
		bindingKind string
		bindingName string
		namespace   string
		wantCR      int
		wantR       int
	}{
		{
			name:        "ClusterRoleBinding_to_absent_ClusterRole",
			binding:     clusterRoleBinding("crb-947", "absent-cr-947", groupSubject("grp-947")),
			bindingKind: "ClusterRoleBinding", bindingName: "crb-947", wantCR: 1,
		},
		{
			name:        "RoleBinding_to_absent_Role",
			binding:     roleBinding("ns-947", "rb-947", "Role", "absent-r-947", groupSubject("grp-947")),
			bindingKind: "RoleBinding", bindingName: "rb-947", namespace: "ns-947", wantR: 1,
		},
		{
			name:        "RoleBinding_to_absent_ClusterRole",
			binding:     roleBinding("ns-947", "rbcr-947", "ClusterRole", "absent-cr-947", groupSubject("grp-947")),
			bindingKind: "RoleBinding", bindingName: "rbcr-947", namespace: "ns-947", wantCR: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetDanglingRoleRefsForTest()
			t.Cleanup(cache.ResetDanglingRoleRefsForTest)
			sink, ctx := capture947(t)
			newTestWatcher(t, tc.binding)
			if s := cache.RBACSnapshotForTest(); s == nil || !s.Synced {
				t.Fatalf("harness snapshot is not Synced: %+v", s)
			}

			opts := rbac.EvaluateOptions{
				Username: "u-947", Groups: []string{"grp-947"},
				Verb: "get", Resource: "pods", Namespace: tc.namespace,
			}
			linesBefore := sink.count(evaluatePathLine)
			missBefore := cache.RBACSnapshotMissCount()
			for i := 0; i < evaluations; i++ {
				allowed, _, err := rbac.EvaluateRBAC(ctx, opts)
				if err != nil {
					t.Fatalf("EvaluateRBAC: %v", err)
				}
				if allowed {
					t.Fatalf("a binding to an absent role permitted the request")
				}
			}
			if got := sink.count(evaluatePathLine) - linesBefore; got != 0 {
				t.Errorf("%d evaluations emitted %d evaluate-path lines; want 0", evaluations, got)
			}
			if got := cache.RBACSnapshotMissCount() - missBefore; got != 0 {
				t.Errorf("%d evaluations recorded %d snapshot misses; want 0", evaluations, got)
			}
			dangling := sink.count(func(r slog.Record) bool {
				return r.Message == "cache.rbac.snapshot.dangling_roleref" && attr947(r, "binding") == cache.RBACBindingLabel(tc.bindingKind, tc.namespace, tc.bindingName)
			})
			if dangling != 1 {
				t.Errorf("dangling lines = %d; want exactly 1", dangling)
			}
			if cr, r := cache.RBACDanglingRoleRefs(); cr != tc.wantCR || r != tc.wantR {
				t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:%d Role:%d}", cr, r, tc.wantCR, tc.wantR)
			}
		})
	}
}

// TestDanglingRoleRef947_UnsyncedSnapshotStaysLoud: a snapshot built while the
// ClusterRole informer had not finished its initial LIST lacks a ClusterRole
// that exists. Each evaluation that misses it is a consistency fault and logs
// one `rbac.evaluate.snapshot.miss` line and one counted miss. Once the LIST
// completes the role resolves and the same evaluation permits silently.
func TestDanglingRoleRef947_UnsyncedSnapshotStaysLoud(t *testing.T) {
	const evaluations = 50
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.ResetDanglingRoleRefsForTest()
	t.Cleanup(cache.ResetDanglingRoleRefsForTest)
	sink, ctx := capture947(t)

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, rbacListKinds(),
		clusterRoleBinding(tok947+"-crb-unsynced", tok947+"-present-cr", groupSubject(tok947+"-group")),
		clusterRole(tok947+"-present-cr", rule([]string{""}, []string{"pods"}, []string{"get"})),
	)
	var released atomic.Bool
	dyn.PrependReactor("list", "clusterroles", func(ktesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		return true, nil, errors.New("clusterroles LIST held by the test (#947)")
	})
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	t.Cleanup(func() { released.Store(true) })
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	// The binding's ADD publishes an event-driven snapshot while the
	// ClusterRole LIST is still failing: it holds the binding, not the role.
	// Matched by name: the published snapshot is process-global, and a
	// previous test's snapshot may still be live.
	var unsynced *cache.RBACSnapshot
	waitFor947(t, "a snapshot holding this test's binding", func() bool {
		s := cache.RBACSnapshotForTest()
		if s == nil || len(s.ClusterRoleBindings) != 1 || s.ClusterRoleBindings[0].Name != tok947+"-crb-unsynced" {
			return false
		}
		unsynced = s
		return true
	})
	if unsynced.Synced {
		t.Fatalf("snapshot built while the ClusterRole LIST was failing claims Synced")
	}

	opts := rbac.EvaluateOptions{Username: tok947, Groups: []string{tok947 + "-group"}, Verb: "get", Resource: "pods"}
	isMiss := func(r slog.Record) bool {
		return r.Message == "rbac.evaluate.snapshot.miss" && attr947(r, "role") == cache.RBACRoleLabel("ClusterRole", "", tok947+"-present-cr")
	}
	linesBefore := sink.count(isMiss)
	missBefore := cache.RBACSnapshotMissCount()
	for i := 0; i < evaluations; i++ {
		allowed, _, err := rbac.EvaluateRBAC(ctx, opts)
		if err != nil {
			t.Fatalf("EvaluateRBAC: %v", err)
		}
		if allowed {
			t.Fatalf("permitted against a snapshot that lacks the role")
		}
	}
	if got := sink.count(isMiss) - linesBefore; got != evaluations {
		t.Errorf("%d evaluations against an unsynced snapshot emitted %d snapshot.miss lines; want %d", evaluations, got, evaluations)
	}
	if got := cache.RBACSnapshotMissCount() - missBefore; got != evaluations {
		t.Errorf("%d evaluations against an unsynced snapshot recorded %d misses; want %d", evaluations, got, evaluations)
	}
	if got := sink.count(func(r slog.Record) bool { return r.Message == "cache.rbac.snapshot.dangling_roleref" }); got != 0 {
		t.Errorf("an unsynced snapshot reported %d dangling lines; want 0", got)
	}

	released.Store(true)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	if s := cache.RBACSnapshotForTest(); s == nil || !s.Synced {
		t.Fatalf("snapshot after every LIST completed is not Synced: %+v", s)
	}
	linesBefore = sink.count(evaluatePathLine)
	allowed, _, err := rbac.EvaluateRBAC(ctx, opts)
	if err != nil || !allowed {
		t.Fatalf("after the LIST completed: allowed=%v err=%v; want a permit", allowed, err)
	}
	if got := sink.count(evaluatePathLine) - linesBefore; got != 0 {
		t.Errorf("a permit emitted %d evaluate-path lines; want 0", got)
	}
	if got := sink.count(func(r slog.Record) bool { return r.Message == "cache.rbac.snapshot.dangling_roleref" }); got != 0 {
		t.Errorf("a binding whose role exists was reported dangling %d times; want 0", got)
	}
	// The binding, role, group and user names all carry the token; the loud
	// miss lines name the role by label only.
	assertNoClear947(t, sink, tok947)
}

// tok947 is a username-shaped token planted in the unsynced arm's binding,
// role, group and user names: no log line may carry it in clear (#947 F1).
const tok947 = "u947alice"

// assertNoClear947 fails if any captured record, of any message, carries tok
// in its message or in any attribute value.
func assertNoClear947(t *testing.T, sink *sink947, tok string) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.recs {
		if strings.Contains(r.Message, tok) {
			t.Errorf("record %q carries %q in its message", r.Message, tok)
		}
		r.Attrs(func(a slog.Attr) bool {
			if strings.Contains(a.Value.String(), tok) {
				t.Errorf("record %q carries %q in attribute %q", r.Message, tok, a.Key)
			}
			return true
		})
	}
}

// waitFor947 polls cond until it holds; the deadline only bounds a hang.
func waitFor947(t *testing.T, label string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
