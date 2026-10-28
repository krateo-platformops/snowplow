// rbac_dangling_roleref_947_test.go — #947 falsifiers for the snapshot side.
//
// A binding whose roleRef names an absent role is reported where its state
// changes: one line when it becomes dangling, one when it stops, and a gauge
// with the current count. Every assertion is a count of log records or a
// gauge value; none reads a clock.
//
// The evaluate side (N evaluations log nothing; an unsynced snapshot stays
// loud on every miss) is in internal/rbac/evaltest/dangling_roleref_947_test.go.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

const (
	msgDangling947 = "cache.rbac.snapshot.dangling_roleref"
	msgResolved947 = "cache.rbac.snapshot.dangling_roleref_resolved"
	subject947     = "subject-947-must-not-appear"
)

// logRecord947 is one captured slog record: its message and its attributes.
type logRecord947 struct {
	msg   string
	attrs map[string]string
}

// logSink947 captures every record at Info and above.
type logSink947 struct {
	mu   sync.Mutex
	recs []logRecord947
}

func (s *logSink947) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }
func (s *logSink947) WithAttrs([]slog.Attr) slog.Handler           { return s }
func (s *logSink947) WithGroup(string) slog.Handler                { return s }
func (s *logSink947) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord947{msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	s.mu.Lock()
	s.recs = append(s.recs, rec)
	s.mu.Unlock()
	return nil
}

// count returns how many captured records have message msg and name the
// binding bindingName.
func (s *logSink947) count(msg, bindingName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if r.msg == msg && r.attrs["binding_name"] == bindingName {
			n++
		}
	}
	return n
}

// find returns the captured records with message msg naming bindingName.
func (s *logSink947) find(msg, bindingName string) []logRecord947 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []logRecord947
	for _, r := range s.recs {
		if r.msg == msg && r.attrs["binding_name"] == bindingName {
			out = append(out, r)
		}
	}
	return out
}

// captureLogs947 routes the default logger to a fresh sink for the test.
func captureLogs947(t *testing.T) *logSink947 {
	t.Helper()
	sink := &logSink947{}
	prev := slog.Default()
	slog.SetDefault(slog.New(sink))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sink
}

func gauge947(t *testing.T) (clusterRole, role int) {
	t.Helper()
	return RBACDanglingRoleRefs()
}

// danglingShape947 is one kind of dangling reference: the binding, the role it
// is missing, and a decoy role with the same name somewhere the evaluator does
// not look.
type danglingShape947 struct {
	name        string
	binding     runtime.Object
	bindingName string
	bindingGVR  string // "crb" | "rb"
	missing     runtime.Object
	missingGVR  string         // "cr" | "r"
	decoy       runtime.Object // same name, wrong scope: must NOT resolve it
	wantCR      int
	wantR       int
}

func shapes947() []danglingShape947 {
	crbToCR := mkCRB("crb-947", userSub(subject947))       // roleRef ClusterRole "crb-947-role"
	rbToR := mkRB("ns-947", "rb-947", userSub(subject947)) // roleRef Role "rb-947-role" in ns-947
	rbToCR := mkRB("ns-947", "rbcr-947", userSub(subject947))
	rbToCR.RoleRef.Kind = "ClusterRole" // roleRef ClusterRole "rbcr-947-role"
	return []danglingShape947{
		{
			name: "ClusterRoleBinding_to_absent_ClusterRole", binding: crbToCR, bindingName: "crb-947", bindingGVR: "crb",
			missing: mkCR("crb-947-role"), missingGVR: "cr",
			decoy: mkR("ns-947", "crb-947-role"), wantCR: 1,
		},
		{
			name: "RoleBinding_to_absent_Role", binding: rbToR, bindingName: "rb-947", bindingGVR: "rb",
			missing: mkR("ns-947", "rb-947-role"), missingGVR: "r",
			decoy: mkR("other-ns-947", "rb-947-role"), wantR: 1,
		},
		{
			name: "RoleBinding_to_absent_ClusterRole", binding: rbToCR, bindingName: "rbcr-947", bindingGVR: "rb",
			missing: mkCR("rbcr-947-role"), missingGVR: "cr",
			decoy: mkR("ns-947", "rbcr-947-role"), wantCR: 1,
		},
	}
}

// indexerOf947 returns the informer indexer for a short GVR name.
func indexerOf947(t *testing.T, rw *ResourceWatcher, short string) clientcache.Indexer {
	t.Helper()
	gvr := map[string]schema.GroupVersionResource{
		"crb": clusterRoleBindingsTypedGVR, "rb": roleBindingsTypedGVR,
		"cr": clusterRolesTypedGVR, "r": rolesTypedGVR,
	}[short]
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	rw.mu.RUnlock()
	if !ok {
		t.Fatalf("no informer for %s", short)
	}
	return gi.Informer().GetIndexer()
}

// setup947 builds a watcher seeded with the shape's binding and its decoy
// role, but not the role the binding names. It returns once the watcher is
// quiet, so only the test's own synchronous rebuilds publish from then on.
func setup947(t *testing.T, sh danglingShape947) (*ResourceWatcher, *logSink947) {
	t.Helper()
	ResetDanglingRoleRefsForTest()
	t.Cleanup(ResetDanglingRoleRefsForTest)
	sink := captureLogs947(t)
	rw := newSnapshotTestWatcher(t, sh.binding, sh.decoy)
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	return rw, sink
}

// TestDanglingRoleRef947_OneLinePerStateChange: for each shape, many publishes
// of an unchanged dangling binding log exactly one "dangling" line and hold
// the gauge at 1; creating the missing role logs exactly one "resolved" line
// and drops the gauge to 0; further publishes log nothing more.
func TestDanglingRoleRef947_OneLinePerStateChange(t *testing.T) {
	const publishes = 25
	for _, sh := range shapes947() {
		t.Run(sh.name+"/role_created", func(t *testing.T) {
			rw, sink := setup947(t, sh)
			for i := 0; i < publishes; i++ {
				RebuildRBACSnapshotForTest(rw)
			}
			if s := RBACSnapshotForTest(); s == nil || !s.Synced {
				t.Fatalf("snapshot after a synced watcher's rebuild is not Synced: %+v", s)
			}
			if got := sink.count(msgDangling947, sh.bindingName); got != 1 {
				t.Errorf("dangling lines after %d publishes = %d; want exactly 1", publishes, got)
			}
			if cr, r := gauge947(t); cr != sh.wantCR || r != sh.wantR {
				t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:%d Role:%d}", cr, r, sh.wantCR, sh.wantR)
			}
			if got := sink.count(msgResolved947, sh.bindingName); got != 0 {
				t.Errorf("resolved lines before the role exists = %d; want 0", got)
			}

			if err := indexerOf947(t, rw, sh.missingGVR).Add(sh.missing); err != nil {
				t.Fatalf("add role: %v", err)
			}
			for i := 0; i < publishes; i++ {
				RebuildRBACSnapshotForTest(rw)
			}
			if got := sink.count(msgResolved947, sh.bindingName); got != 1 {
				t.Errorf("resolved lines after the role was created = %d; want exactly 1", got)
			}
			if recs := sink.find(msgResolved947, sh.bindingName); len(recs) == 1 && recs[0].attrs["reason"] != "role_present" {
				t.Errorf("resolved reason = %q; want role_present", recs[0].attrs["reason"])
			}
			if got := sink.count(msgDangling947, sh.bindingName); got != 1 {
				t.Errorf("dangling lines after resolution = %d; want still exactly 1", got)
			}
			if cr, r := gauge947(t); cr != 0 || r != 0 {
				t.Errorf("gauge after the role was created = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
			}
			assertNoSubject947(t, sink)
		})

		t.Run(sh.name+"/binding_deleted", func(t *testing.T) {
			rw, sink := setup947(t, sh)
			RebuildRBACSnapshotForTest(rw)
			if got := sink.count(msgDangling947, sh.bindingName); got != 1 {
				t.Fatalf("dangling lines = %d; want exactly 1", got)
			}
			if err := indexerOf947(t, rw, sh.bindingGVR).Delete(sh.binding); err != nil {
				t.Fatalf("delete binding: %v", err)
			}
			for i := 0; i < publishes; i++ {
				RebuildRBACSnapshotForTest(rw)
			}
			if got := sink.count(msgResolved947, sh.bindingName); got != 1 {
				t.Errorf("resolved lines after the binding was deleted = %d; want exactly 1", got)
			}
			if recs := sink.find(msgResolved947, sh.bindingName); len(recs) == 1 && recs[0].attrs["reason"] != "binding_changed_or_deleted" {
				t.Errorf("resolved reason = %q; want binding_changed_or_deleted", recs[0].attrs["reason"])
			}
			if cr, r := gauge947(t); cr != 0 || r != 0 {
				t.Errorf("gauge after the binding was deleted = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
			}
			assertNoSubject947(t, sink)
		})
	}
}

// assertNoSubject947 fails if any captured dangling/resolved line carries the
// binding's subject anywhere in its attributes.
func assertNoSubject947(t *testing.T, sink *logSink947) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.recs {
		if r.msg != msgDangling947 && r.msg != msgResolved947 {
			continue
		}
		for k, v := range r.attrs {
			if v == subject947 {
				t.Errorf("%s carries the binding's subject in %q", r.msg, k)
			}
		}
	}
}

// TestDanglingRoleRef947_GaugeExpvar: the gauge is published in /debug/vars
// as snowplow_rbac_dangling_roleref, keyed by roleRef kind only.
func TestDanglingRoleRef947_GaugeExpvar(t *testing.T) {
	sh := shapes947()[0]
	rw, _ := setup947(t, sh)
	RebuildRBACSnapshotForTest(rw)
	RegisterRBACSnapshotExpvar()
	v := expvar.Get("snowplow_rbac_dangling_roleref")
	if v == nil {
		t.Fatalf("snowplow_rbac_dangling_roleref is not published")
	}
	var got map[string]int
	if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
		t.Fatalf("decode %q: %v", v.String(), err)
	}
	want := map[string]int{"ClusterRole": 1, "Role": 0}
	if len(got) != len(want) || got["ClusterRole"] != 1 || got["Role"] != 0 {
		t.Errorf("snowplow_rbac_dangling_roleref = %v; want %v", got, want)
	}
}

// TestDanglingRoleRef947_UnsyncedSnapshotNotTracked: a snapshot built while an
// RBAC informer has not finished its initial LIST is not Synced, and a role
// it is missing is not reported as dangling (it may exist and not be listed
// yet). Once the LIST completes, only the genuinely absent role is reported.
//
// The ClusterRole LIST fails until the test releases it, as when the
// apiserver is not yet answering for that resource at boot.
func TestDanglingRoleRef947_UnsyncedSnapshotNotTracked(t *testing.T) {
	ResetDanglingRoleRefsForTest()
	t.Cleanup(ResetDanglingRoleRefsForTest)
	sink := captureLogs947(t)
	t.Setenv("CACHE_ENABLED", "true")

	present := mkCRB("present-947", userSub(subject947)) // its role exists, but is not listed yet
	absent := mkCRB("absent-947", userSub(subject947))   // its role does not exist
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), rbacListKinds(),
		present, absent, mkCR("present-947-role"))
	var released atomic.Bool
	var failedLists atomic.Int64
	dyn.PrependReactor("list", "clusterroles", func(k8stesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		failedLists.Add(1)
		return true, nil, errors.New("clusterroles LIST held by the test (#947)")
	})
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	t.Cleanup(func() { released.Store(true) })

	// Wait for the ClusterRoleBinding informer to list both bindings and for
	// the ClusterRole LIST to have failed at least once.
	waitFor947(t, "CRB informer synced and CR LIST failed", func() bool {
		rw.mu.RLock()
		gi := rw.informers[clusterRoleBindingsTypedGVR]
		rw.mu.RUnlock()
		return gi.Informer().HasSynced() && failedLists.Load() > 0
	})
	RebuildRBACSnapshotForTest(rw)
	s := RBACSnapshotForTest()
	if s == nil || len(s.ClusterRoleBindings) != 2 {
		t.Fatalf("unsynced snapshot does not hold both bindings: %+v", s)
	}
	if s.Synced {
		t.Fatalf("snapshot built while the ClusterRole LIST had not completed claims Synced")
	}
	if got := sink.count(msgDangling947, "present-947") + sink.count(msgDangling947, "absent-947"); got != 0 {
		t.Errorf("dangling lines from an unsynced snapshot = %d; want 0", got)
	}
	if cr, r := gauge947(t); cr != 0 || r != 0 {
		t.Errorf("gauge after an unsynced snapshot = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
	}

	released.Store(true)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	RebuildRBACSnapshotForTest(rw)
	if s := RBACSnapshotForTest(); s == nil || !s.Synced {
		t.Fatalf("snapshot after every LIST completed is not Synced: %+v", s)
	}
	if got := sink.count(msgDangling947, "present-947"); got != 0 {
		t.Errorf("dangling lines for the binding whose role exists = %d; want 0", got)
	}
	if got := sink.count(msgDangling947, "absent-947"); got != 1 {
		t.Errorf("dangling lines for the binding whose role is absent = %d; want exactly 1", got)
	}
	if cr, r := gauge947(t); cr != 1 || r != 0 {
		t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:1 Role:0}", cr, r)
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
