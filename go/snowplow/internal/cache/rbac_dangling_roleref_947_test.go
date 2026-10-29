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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
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

// logStore947 holds every record captured by a logSink947 and by every
// handler derived from it with WithAttrs / WithGroup.
type logStore947 struct {
	mu   sync.Mutex
	recs []logRecord947
}

// logSink947 captures every record at Info and above. It KEEPS what a logger
// binds with With (WithAttrs) and WithGroup, so an attribute attached as
// slog.With("k", v).Warn(...) is captured and scanned like a record
// attribute (#947 F5). Attributes are flattened to "group.key" -> value.
type logSink947 struct {
	*logStore947
	bound  []slog.Attr // already wrapped in the groups open when they were bound
	groups []string
}

// wrapInGroups947 nests as inside groups, outermost first.
func wrapInGroups947(groups []string, as []slog.Attr) []slog.Attr {
	for i := len(groups) - 1; i >= 0; i-- {
		as = []slog.Attr{{Key: groups[i], Value: slog.GroupValue(as...)}}
	}
	return as
}

// flatten947 writes a's leaves into out under "prefix.key" (groups inlined).
func flatten947(prefix string, a slog.Attr, out map[string]string) {
	v := a.Value.Resolve()
	key := a.Key
	if prefix != "" && key != "" {
		key = prefix + "." + key
	} else if prefix != "" {
		key = prefix
	}
	if v.Kind() == slog.KindGroup {
		for _, ga := range v.Group() {
			flatten947(key, ga, out)
		}
		return
	}
	out[key] = v.String()
}

func (s *logSink947) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }
func (s *logSink947) WithAttrs(as []slog.Attr) slog.Handler {
	c := *s
	c.bound = append(append([]slog.Attr(nil), s.bound...), wrapInGroups947(s.groups, as)...)
	return &c
}
func (s *logSink947) WithGroup(name string) slog.Handler {
	if name == "" {
		return s
	}
	c := *s
	c.groups = append(append([]string(nil), s.groups...), name)
	return &c
}
func (s *logSink947) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord947{msg: r.Message, attrs: map[string]string{}}
	for _, a := range s.bound {
		flatten947("", a, rec.attrs)
	}
	var own []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		own = append(own, a)
		return true
	})
	for _, a := range wrapInGroups947(s.groups, own) {
		flatten947("", a, rec.attrs)
	}
	s.mu.Lock()
	s.recs = append(s.recs, rec)
	s.mu.Unlock()
	return nil
}

// count returns how many captured records have message msg and carry the
// binding label binding (RBACBindingLabel).
func (s *logSink947) count(msg, binding string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if r.msg == msg && r.attrs["binding"] == binding {
			n++
		}
	}
	return n
}

// countRole returns how many captured records have message msg, carry the
// binding label binding and the role label role (RBACRoleLabel).
func (s *logSink947) countRole(msg, binding, role string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if r.msg == msg && r.attrs["binding"] == binding && r.attrs["roleref"] == role {
			n++
		}
	}
	return n
}

// find returns the captured records with message msg carrying the binding
// label binding.
func (s *logSink947) find(msg, binding string) []logRecord947 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []logRecord947
	for _, r := range s.recs {
		if r.msg == msg && r.attrs["binding"] == binding {
			out = append(out, r)
		}
	}
	return out
}

// captureLogs947 routes the default logger to a fresh sink for the test.
func captureLogs947(t *testing.T) *logSink947 {
	t.Helper()
	sink := &logSink947{logStore947: &logStore947{}}
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
	bindingKind string // "ClusterRoleBinding" | "RoleBinding"
	bindingNS   string
	bindingName string
	bindingGVR  string // "crb" | "rb"
	missing     runtime.Object
	missingGVR  string         // "cr" | "r"
	decoy       runtime.Object // same name, wrong scope: must NOT resolve it
	wantCR      int
	wantR       int
}

// label is the binding's log label.
func (sh danglingShape947) label() string {
	return RBACBindingLabel(sh.bindingKind, sh.bindingNS, sh.bindingName)
}

// crbLabel947 is the log label of a ClusterRoleBinding.
func crbLabel947(name string) string { return RBACBindingLabel("ClusterRoleBinding", "", name) }

func shapes947() []danglingShape947 {
	crbToCR := mkCRB("crb-947", userSub(subject947))       // roleRef ClusterRole "crb-947-role"
	rbToR := mkRB("ns-947", "rb-947", userSub(subject947)) // roleRef Role "rb-947-role" in ns-947
	rbToCR := mkRB("ns-947", "rbcr-947", userSub(subject947))
	rbToCR.RoleRef.Kind = "ClusterRole" // roleRef ClusterRole "rbcr-947-role"
	return []danglingShape947{
		{
			name: "ClusterRoleBinding_to_absent_ClusterRole", binding: crbToCR, bindingKind: "ClusterRoleBinding", bindingName: "crb-947", bindingGVR: "crb",
			missing: mkCR("crb-947-role"), missingGVR: "cr",
			decoy: mkR("ns-947", "crb-947-role"), wantCR: 1,
		},
		{
			name: "RoleBinding_to_absent_Role", binding: rbToR, bindingKind: "RoleBinding", bindingNS: "ns-947", bindingName: "rb-947", bindingGVR: "rb",
			missing: mkR("ns-947", "rb-947-role"), missingGVR: "r",
			decoy: mkR("other-ns-947", "rb-947-role"), wantR: 1,
		},
		{
			name: "RoleBinding_to_absent_ClusterRole", binding: rbToCR, bindingKind: "RoleBinding", bindingNS: "ns-947", bindingName: "rbcr-947", bindingGVR: "rb",
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
			if got := sink.count(msgDangling947, sh.label()); got != 1 {
				t.Errorf("dangling lines after %d publishes = %d; want exactly 1", publishes, got)
			}
			if cr, r := gauge947(t); cr != sh.wantCR || r != sh.wantR {
				t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:%d Role:%d}", cr, r, sh.wantCR, sh.wantR)
			}
			if got := sink.count(msgResolved947, sh.label()); got != 0 {
				t.Errorf("resolved lines before the role exists = %d; want 0", got)
			}

			if err := indexerOf947(t, rw, sh.missingGVR).Add(sh.missing); err != nil {
				t.Fatalf("add role: %v", err)
			}
			for i := 0; i < publishes; i++ {
				RebuildRBACSnapshotForTest(rw)
			}
			if got := sink.count(msgResolved947, sh.label()); got != 1 {
				t.Errorf("resolved lines after the role was created = %d; want exactly 1", got)
			}
			if recs := sink.find(msgResolved947, sh.label()); len(recs) == 1 && recs[0].attrs["reason"] != "role_present" {
				t.Errorf("resolved reason = %q; want role_present", recs[0].attrs["reason"])
			}
			if got := sink.count(msgDangling947, sh.label()); got != 1 {
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
			if got := sink.count(msgDangling947, sh.label()); got != 1 {
				t.Fatalf("dangling lines = %d; want exactly 1", got)
			}
			if err := indexerOf947(t, rw, sh.bindingGVR).Delete(sh.binding); err != nil {
				t.Fatalf("delete binding: %v", err)
			}
			for i := 0; i < publishes; i++ {
				RebuildRBACSnapshotForTest(rw)
			}
			if got := sink.count(msgResolved947, sh.label()); got != 1 {
				t.Errorf("resolved lines after the binding was deleted = %d; want exactly 1", got)
			}
			if recs := sink.find(msgResolved947, sh.label()); len(recs) == 1 && recs[0].attrs["reason"] != "binding_changed_or_deleted" {
				t.Errorf("resolved reason = %q; want binding_changed_or_deleted", recs[0].attrs["reason"])
			}
			if cr, r := gauge947(t); cr != 0 || r != 0 {
				t.Errorf("gauge after the binding was deleted = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
			}
			assertNoSubject947(t, sink)
		})
	}
}

// TestDanglingRoleRef947_FlapIsOneLinePerTransition: dangling, then resolved
// (role created), then dangling again (role deleted) is three transitions and
// exactly three lines, with the gauge reading 1, 0, 1. Many publishes sit
// between the transitions and add nothing. A per-process "warned once" flag
// fails here: it stays silent on the second transition into dangling.
func TestDanglingRoleRef947_FlapIsOneLinePerTransition(t *testing.T) {
	const publishes = 10
	sh := shapes947()[0]
	rw, sink := setup947(t, sh)
	phase := func(label string, wantDangling, wantResolved, wantCR int) {
		t.Helper()
		for i := 0; i < publishes; i++ {
			RebuildRBACSnapshotForTest(rw)
		}
		d, r := sink.count(msgDangling947, sh.label()), sink.count(msgResolved947, sh.label())
		if d != wantDangling || r != wantResolved {
			t.Errorf("%s: dangling lines = %d, resolved lines = %d; want %d and %d", label, d, r, wantDangling, wantResolved)
		}
		if cr, role := gauge947(t); cr != wantCR || role != 0 {
			t.Errorf("%s: gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:%d Role:0}", label, cr, role, wantCR)
		}
	}

	phase("1 dangling", 1, 0, 1)
	roles := indexerOf947(t, rw, sh.missingGVR)
	if err := roles.Add(sh.missing); err != nil {
		t.Fatalf("add role: %v", err)
	}
	phase("2 resolved by the role", 1, 1, 0)
	if err := roles.Delete(sh.missing); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	phase("3 dangling again", 2, 1, 1)
}

// TestDanglingRoleRef947_UnrelatedChangesAddNoLines: once a binding has been
// reported, publishes caused by changes to OTHER bindings and roles (adds, an
// update, a delete) add no line for it, and the gauge stays 1. The reported
// state is the writer's dangling set, carried from publish to publish; if it
// were reset on each publish, every one of these publishes would report the
// binding again.
func TestDanglingRoleRef947_UnrelatedChangesAddNoLines(t *testing.T) {
	sh := shapes947()[0]
	rw, sink := setup947(t, sh)
	RebuildRBACSnapshotForTest(rw)
	if got := sink.count(msgDangling947, sh.label()); got != 1 {
		t.Fatalf("dangling lines = %d; want exactly 1", got)
	}

	crbs, crs := indexerOf947(t, rw, "crb"), indexerOf947(t, rw, "cr")
	for i := 0; i < 10; i++ {
		other := mkCRB("unrelated-947-"+strconv.Itoa(i), userSub("unrelated-subject-947"))
		if err := crbs.Add(other); err != nil {
			t.Fatalf("add unrelated binding: %v", err)
		}
		if err := crs.Add(mkCR(other.RoleRef.Name)); err != nil {
			t.Fatalf("add unrelated role: %v", err)
		}
		RebuildRBACSnapshotForTest(rw)
	}
	changed := mkCR("unrelated-947-0-role")
	changed.Rules = append(changed.Rules, rbacv1.PolicyRule{Verbs: []string{"list"}, APIGroups: []string{""}, Resources: []string{"secrets"}})
	if err := crs.Update(changed); err != nil {
		t.Fatalf("update unrelated role: %v", err)
	}
	RebuildRBACSnapshotForTest(rw)
	if err := crbs.Delete(mkCRB("unrelated-947-1", userSub("unrelated-subject-947"))); err != nil {
		t.Fatalf("delete unrelated binding: %v", err)
	}
	for i := 0; i < 10; i++ {
		RebuildRBACSnapshotForTest(rw)
	}

	if got := sink.count(msgDangling947, sh.label()); got != 1 {
		t.Errorf("dangling lines after 22 publishes of unrelated changes = %d; want still exactly 1", got)
	}
	if got := sink.count(msgResolved947, sh.label()); got != 0 {
		t.Errorf("resolved lines = %d; want 0 (the binding never stopped dangling)", got)
	}
	if cr, r := gauge947(t); cr != 1 || r != 0 {
		t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:1 Role:0}", cr, r)
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

// assertNoClear947 fails if any captured record, of any message, carries one
// of tokens in its message or in any attribute value.
func assertNoClear947(t *testing.T, sink *logSink947, tokens ...string) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.recs {
		for _, tok := range tokens {
			if strings.Contains(r.msg, tok) {
				t.Errorf("record %q carries %q in its message", r.msg, tok)
			}
			for k, v := range r.attrs {
				if strings.Contains(k, tok) || strings.Contains(v, tok) {
					t.Errorf("record %q carries %q in attribute %q", r.msg, tok, k)
				}
			}
		}
	}
}

// assertNoSubject947 fails if any captured record carries the binding's
// subject.
func assertNoSubject947(t *testing.T, sink *logSink947) {
	t.Helper()
	assertNoClear947(t, sink, subject947)
}

// countMsg returns how many captured records have message msg.
func (s *logSink947) countMsg(msg string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if r.msg == msg {
			n++
		}
	}
	return n
}

// TestDanglingRoleRef947_NoNameInClear: binding, role and namespace names
// often embed a tenant username. With every name (and the subject) planted
// with a username-shaped token, the full dangling -> resolved cycle of all
// three shapes logs the expected lines, and NO captured record of any message
// carries the token. The lines carry labels instead, and the dangling and
// resolved line of one binding carry the same ones.
func TestDanglingRoleRef947_NoNameInClear(t *testing.T) {
	const tok = "u947alice"
	ResetDanglingRoleRefsForTest()
	t.Cleanup(ResetDanglingRoleRefsForTest)
	sink := captureLogs947(t)

	crb := mkCRB(tok+"-crb", userSub(tok)) // -> ClusterRole tok-crb-role
	rbR := mkRB(tok+"-ns", tok+"-rb", userSub(tok))
	rbCR := mkRB(tok+"-ns", tok+"-rbcr", userSub(tok))
	rbCR.RoleRef.Kind = "ClusterRole"
	rw := newSnapshotTestWatcher(t, crb, rbR, rbCR)
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	RebuildRBACSnapshotForTest(rw)
	if got := sink.countMsg(msgDangling947); got != 3 {
		t.Fatalf("dangling lines = %d; want 3", got)
	}

	if err := indexerOf947(t, rw, "cr").Add(mkCR(tok + "-crb-role")); err != nil {
		t.Fatalf("add role: %v", err)
	}
	if err := indexerOf947(t, rw, "r").Add(mkR(tok+"-ns", tok+"-rb-role")); err != nil {
		t.Fatalf("add role: %v", err)
	}
	if err := indexerOf947(t, rw, "rb").Delete(rbCR); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	RebuildRBACSnapshotForTest(rw)
	if got := sink.countMsg(msgResolved947); got != 3 {
		t.Fatalf("resolved lines = %d; want 3", got)
	}

	for _, b := range []struct{ kind, ns, name, roleKind, roleNS, role string }{
		{"ClusterRoleBinding", "", tok + "-crb", "ClusterRole", "", tok + "-crb-role"},
		{"RoleBinding", tok + "-ns", tok + "-rb", "Role", tok + "-ns", tok + "-rb-role"},
		{"RoleBinding", tok + "-ns", tok + "-rbcr", "ClusterRole", "", tok + "-rbcr-role"},
	} {
		bl, rl := RBACBindingLabel(b.kind, b.ns, b.name), RBACRoleLabel(b.roleKind, b.roleNS, b.role)
		if d, r := sink.countRole(msgDangling947, bl, rl), sink.countRole(msgResolved947, bl, rl); d != 1 || r != 1 {
			t.Errorf("%s %s: dangling/resolved lines carrying its labels = %d/%d; want 1/1", b.kind, bl, d, r)
		}
		for _, rec := range sink.find(msgDangling947, bl) {
			if rec.attrs["binding_namespace"] != RBACNamespaceLabel(b.ns) {
				t.Errorf("%s: binding_namespace = %q; want the namespace label", bl, rec.attrs["binding_namespace"])
			}
		}
	}
	assertNoClear947(t, sink, tok)
}

// TestDanglingRoleRef947_MovedBindingIsTwoTransitions: a binding that now
// names a different absent role (deleted and recreated under the same name,
// since roleRef is immutable) leaves the old dangling reference and enters a
// new one: one resolved line for role A, one dangling line for role B, and the
// gauge stays 1.
func TestDanglingRoleRef947_MovedBindingIsTwoTransitions(t *testing.T) {
	ResetDanglingRoleRefsForTest()
	t.Cleanup(ResetDanglingRoleRefsForTest)
	sink := captureLogs947(t)
	toA := mkCRB("move-947", userSub(subject947))
	toA.RoleRef.Name = "role-a-947"
	rw := newSnapshotTestWatcher(t, toA)
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	RebuildRBACSnapshotForTest(rw)

	toB := mkCRB("move-947", userSub(subject947))
	toB.RoleRef.Name = "role-b-947"
	if err := indexerOf947(t, rw, "crb").Update(toB); err != nil {
		t.Fatalf("update binding: %v", err)
	}
	for i := 0; i < 5; i++ {
		RebuildRBACSnapshotForTest(rw)
	}

	b := crbLabel947("move-947")
	a, bRole := RBACRoleLabel("ClusterRole", "", "role-a-947"), RBACRoleLabel("ClusterRole", "", "role-b-947")
	if got := sink.countRole(msgDangling947, b, a); got != 1 {
		t.Errorf("dangling lines for role A = %d; want 1", got)
	}
	if got := sink.countRole(msgResolved947, b, a); got != 1 {
		t.Errorf("resolved lines for role A after the binding moved = %d; want 1", got)
	}
	if got := sink.countRole(msgDangling947, b, bRole); got != 1 {
		t.Errorf("dangling lines for role B after the binding moved = %d; want 1", got)
	}
	if got := sink.countRole(msgResolved947, b, bRole); got != 0 {
		t.Errorf("resolved lines for role B = %d; want 0", got)
	}
	for _, rec := range sink.find(msgResolved947, b) {
		if rec.attrs["reason"] != "binding_changed_or_deleted" {
			t.Errorf("resolved reason = %q; want binding_changed_or_deleted", rec.attrs["reason"])
		}
	}
	if cr, r := gauge947(t); cr != 1 || r != 0 {
		t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:1 Role:0}", cr, r)
	}
}

// heldListWatcher947 builds a watcher over a fake whose LIST of resource held
// fails until the returned release is called, as when the apiserver does not
// yet answer for that resource at boot. It returns once every OTHER RBAC
// informer has synced and the held LIST has failed at least once.
//
// Seed: two bindings whose roles exist (a ClusterRoleBinding and a
// RoleBinding to a Role) and one ClusterRoleBinding to an absent ClusterRole.
func heldListWatcher947(t *testing.T, held string) (*ResourceWatcher, func()) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), rbacListKinds(),
		mkCRB("crb-present-947", userSub(subject947)), mkCR("crb-present-947-role"),
		mkRB("ns-947", "rb-present-947", userSub(subject947)), mkR("ns-947", "rb-present-947-role"),
		mkCRB("absent-947", userSub(subject947)),
	)
	var released atomic.Bool
	var failedLists atomic.Int64
	dyn.PrependReactor("list", held, func(k8stesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		failedLists.Add(1)
		return true, nil, errors.New(held + " LIST held by the test (#947)")
	})
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	release := func() { released.Store(true) }
	t.Cleanup(release)
	waitFor947(t, "every other RBAC informer synced and the "+held+" LIST failed", func() bool {
		if failedLists.Load() == 0 {
			return false
		}
		rw.mu.RLock()
		defer rw.mu.RUnlock()
		for gvr, gi := range rw.informers {
			if isTypedRBACGVR(gvr) && gvr.Resource != held && !gi.Informer().HasSynced() {
				return false
			}
		}
		return true
	})
	return rw, release
}

// TestDanglingRoleRef947_UnsyncedSnapshotNotTracked, for each of the four RBAC
// informers in turn: while that one informer's LIST has not completed, the
// snapshot is not Synced and reports nothing as dangling (a role may exist and
// not be listed yet). Once the LIST completes, only the genuinely absent role
// is reported, once. A synced check that skipped any one informer fails its
// subtest.
func TestDanglingRoleRef947_UnsyncedSnapshotNotTracked(t *testing.T) {
	for _, held := range []string{"clusterrolebindings", "rolebindings", "clusterroles", "roles"} {
		t.Run(held, func(t *testing.T) {
			ResetDanglingRoleRefsForTest()
			t.Cleanup(ResetDanglingRoleRefsForTest)
			sink := captureLogs947(t)
			rw, release := heldListWatcher947(t, held)

			RebuildRBACSnapshotForTest(rw)
			s := RBACSnapshotForTest()
			if s == nil {
				t.Fatalf("no snapshot published")
			}
			if s.Synced {
				t.Errorf("snapshot built while the %s LIST had not completed claims Synced", held)
			}
			if got := sink.countMsg(msgDangling947); got != 0 {
				t.Errorf("dangling lines from an unsynced snapshot = %d; want 0", got)
			}
			if cr, r := gauge947(t); cr != 0 || r != 0 {
				t.Errorf("gauge after an unsynced snapshot = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
			}

			release()
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
			if got := sink.countMsg(msgDangling947); got != 1 {
				t.Errorf("dangling lines once every LIST completed = %d; want exactly 1", got)
			}
			if got := sink.count(msgDangling947, crbLabel947("absent-947")); got != 1 {
				t.Errorf("dangling lines for the binding whose role is absent = %d; want exactly 1", got)
			}
			if cr, r := gauge947(t); cr != 1 || r != 0 {
				t.Errorf("gauge = {ClusterRole:%d Role:%d}; want {ClusterRole:1 Role:0}", cr, r)
			}
		})
	}
}

// flipInformer947 wraps the ClusterRole informer so that its indexer's List()
// flips HasSynced to true as it returns: the initial LIST lands right after the
// snapshot has read the (still empty) indexer. The synced state must be read
// BEFORE the indexers; read after them, it would claim a complete snapshot
// over an incomplete read.
type flipInformer947 struct {
	informers.GenericInformer
	sii *flipSII947
}

func (f flipInformer947) Informer() clientcache.SharedIndexInformer { return f.sii }

type flipSII947 struct {
	clientcache.SharedIndexInformer
	synced atomic.Bool
}

func (s *flipSII947) HasSynced() bool { return s.synced.Load() }
func (s *flipSII947) GetIndexer() clientcache.Indexer {
	return flipIndexer947{Indexer: s.SharedIndexInformer.GetIndexer(), s: s}
}

type flipIndexer947 struct {
	clientcache.Indexer
	s *flipSII947
}

func (i flipIndexer947) List() []interface{} {
	out := i.Indexer.List()
	i.s.synced.Store(true)
	return out
}

// TestDanglingRoleRef947_SyncedReadBeforeIndexers: the ClusterRole informer
// becomes synced while the snapshot is reading the indexers. The snapshot
// read an empty ClusterRole set, so it must not claim Synced, and the binding
// whose role exists must not be reported as dangling.
func TestDanglingRoleRef947_SyncedReadBeforeIndexers(t *testing.T) {
	ResetDanglingRoleRefsForTest()
	t.Cleanup(ResetDanglingRoleRefsForTest)
	sink := captureLogs947(t)
	rw, _ := heldListWatcher947(t, "clusterroles")

	rw.mu.Lock()
	real := rw.informers[clusterRolesTypedGVR]
	rw.informers[clusterRolesTypedGVR] = flipInformer947{GenericInformer: real, sii: &flipSII947{SharedIndexInformer: real.Informer()}}
	rw.mu.Unlock()
	t.Cleanup(func() {
		rw.mu.Lock()
		rw.informers[clusterRolesTypedGVR] = real
		rw.mu.Unlock()
	})

	RebuildRBACSnapshotForTest(rw)
	if s := RBACSnapshotForTest(); s == nil || s.Synced {
		t.Errorf("snapshot whose ClusterRole read preceded the sync claims Synced: %+v", s)
	}
	if got := sink.count(msgDangling947, crbLabel947("crb-present-947")); got != 0 {
		t.Errorf("the binding whose role exists was reported dangling %d times; want 0", got)
	}
	if cr, r := gauge947(t); cr != 0 || r != 0 {
		t.Errorf("gauge = {ClusterRole:%d Role:%d}; want 0/0", cr, r)
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
