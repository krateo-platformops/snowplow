package cache

// served_view_memo_test.go — #1430, the memo on its own (the gate and the
// /refreshes callers are internal/handlers/dispatchers
// issue1430_served_view_memo_test.go):
//
//	M1  same (answer, identity, snapshot), concurrently: the gate runs once and
//	    every caller gets its digest; a caller for ANOTHER identity completes
//	    while that gate is still running (no lock is held across the gate).
//	M2  a publish between two calls: the stored digest is discarded and the
//	    gate runs again; one slot per identity, never two.
//	M3  the snapshot moved WHILE the gate ran: the caller gets its live digest
//	    and nothing is stored.
//	M4  no snapshot, a snapshot with no sequence, or one that is not Synced:
//	    live every time, nothing stored.
//	M5  a refusal is never stored.
//	M6  the /call fill: stored under the snapshot read before its gate, never
//	    when that snapshot moved; an identity is exact (group order aside).
//	M7  the memo holds digests only: its types have no byte slice and no field
//	    this test does not know.

import (
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sv1430Snapshot(t *testing.T, snap *RBACSnapshot) {
	t.Helper()
	prev := rbacSnap.Load()
	t.Cleanup(func() { rbacSnap.Store(prev) })
	rbacSnap.Store(snap)
}

func sv1430Entry(body string) *ResolvedEntry { return &ResolvedEntry{RawJSON: []byte(body)} }

func sv1430Stats() RefreshBroadcasterStats { return RefreshBroadcasterStatsSnapshot() }

func TestIssue1430_M1_OneGatePerSlot_OtherIdentitiesAreNotBlocked(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":1}`)
	alice, bob := ServedViewIdentityOf("alice", []string{"g"}), ServedViewIdentityOf("bob", []string{"g"})

	var runs atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	slow := func() (string, bool) {
		if runs.Add(1) == 1 {
			close(entered)
		}
		<-release
		return "v1.alice", true
	}
	const callers = 16
	got := make([]string, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = e.ServedViewDigest(alice, slow)
		}(i)
	}
	<-entered

	// Another identity on the same cell, while alice's gate is still running.
	bobDone := make(chan string, 1)
	go func() { bobDone <- e.ServedViewDigest(bob, func() (string, bool) { return "v1.bob", true }) }()
	select {
	case d := <-bobDone:
		if d != "v1.bob" {
			t.Fatalf("M1 RED: bob was answered %q, want his own digest", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("M1 RED: another identity is blocked while one identity's gate runs — a lock is held across the gate")
	}

	time.Sleep(50 * time.Millisecond) // let every alice caller reach the slot
	close(release)
	wg.Wait()
	if n := runs.Load(); n != 1 {
		t.Fatalf("M1 RED: the gate ran %d times for %d callers of one (answer, identity, snapshot), want 1", n, callers)
	}
	for i, d := range got {
		if d != "v1.alice" {
			t.Fatalf("M1 RED: caller %d got %q", i, d)
		}
	}
	if d := e.ServedViewDigest(alice, func() (string, bool) { t.Error("M1 RED: a stored digest was gated again"); return "", false }); d != "v1.alice" {
		t.Fatalf("M1 RED: reuse returned %q", d)
	}
}

func TestIssue1430_M2_APublishDiscardsTheStoredDigest(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":2}`)
	id := ServedViewIdentityOf("alice", nil)
	view := "v1.before"
	gate := func() (string, bool) { return view, true }
	if d := e.ServedViewDigest(id, gate); d != "v1.before" {
		t.Fatalf("PRE: %q", d)
	}
	before := sv1430Stats()
	rbacSnap.Store(&RBACSnapshot{Synced: true, PublishSeq: 8})
	view = "v1.after"
	if d := e.ServedViewDigest(id, gate); d != "v1.after" {
		t.Fatalf("M2 RED: after a publish the memo answered %q, want the live digest v1.after — a stored digest "+
			"outlived the RBAC snapshot it was gated under", d)
	}
	after := sv1430Stats()
	if after.GatedViewRBACMoved-before.GatedViewRBACMoved != 1 || after.GatedViewGateRuns-before.GatedViewGateRuns != 1 {
		t.Fatalf("M2 RED: snapshot_moved Δ=%d gate_runs Δ=%d, want 1 and 1",
			after.GatedViewRBACMoved-before.GatedViewRBACMoved, after.GatedViewGateRuns-before.GatedViewGateRuns)
	}
	if n := e.ServedViewSlotsForTest(); n != 1 {
		t.Fatalf("M2 RED: %d slots for one identity after a publish, want 1 (replaced in place)", n)
	}
}

func TestIssue1430_M3_ASnapshotThatMovedDuringTheGateStoresNothing(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":3}`)
	id := ServedViewIdentityOf("alice", nil)
	d := e.ServedViewDigest(id, func() (string, bool) {
		rbacSnap.Store(&RBACSnapshot{Synced: true, PublishSeq: 8}) // a publish lands mid-gate
		return "v1.mixed", true
	})
	if d != "v1.mixed" {
		t.Fatalf("M3: the caller got %q, want its own live digest", d)
	}
	if n := e.ServedViewSlotsForTest(); n != 0 {
		t.Fatalf("M3 RED: a digest gated across a publish was stored (%d slots): its verdicts may come from two snapshots", n)
	}
	ran := false
	if d := e.ServedViewDigest(id, func() (string, bool) { ran = true; return "v1.clean", true }); d != "v1.clean" || !ran {
		t.Fatalf("M3 RED: next call got %q (gate ran: %v), want a fresh gate", d, ran)
	}

	// The fill obeys the same rule.
	f := sv1430Entry(`{"m":"3-fill"}`)
	before := ServedViewRBACNow()
	rbacSnap.Store(&RBACSnapshot{Synced: true, PublishSeq: 9})
	f.NoteServedView(id, before, "v1.stale")
	if n := f.ServedViewSlotsForTest(); n != 0 {
		t.Fatalf("M3 RED: a /call fill gated under a snapshot that is no longer published was stored")
	}
}

func TestIssue1430_M4_NoCompleteSnapshotStoresNothing(t *testing.T) {
	for name, snap := range map[string]*RBACSnapshot{
		"none":        nil,
		"not_synced":  {Synced: false, PublishSeq: 7},
		"no_sequence": {Synced: true},
	} {
		t.Run(name, func(t *testing.T) {
			sv1430Snapshot(t, snap)
			if ServedViewRBACNow().Storable() {
				t.Fatal("M4 RED: this RBAC state is storable")
			}
			e := sv1430Entry(`{"m":4}`)
			id := ServedViewIdentityOf("alice", nil)
			runs := 0
			gate := func() (string, bool) { runs++; return "v1.deny", true }
			for i := 0; i < 3; i++ {
				if d := e.ServedViewDigest(id, gate); d != "v1.deny" {
					t.Fatalf("M4: %q", d)
				}
			}
			e.NoteServedView(id, ServedViewRBACNow(), "v1.fill")
			if runs != 3 || e.ServedViewSlotsForTest() != 0 {
				t.Fatalf("M4 RED: %d gate runs for 3 calls and %d slots; a digest gated against an incomplete RBAC "+
					"snapshot must never be stored", runs, e.ServedViewSlotsForTest())
			}
		})
	}
}

func TestIssue1430_M5_ARefusalIsNeverStored(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":5}`)
	id := ServedViewIdentityOf("alice", nil)
	runs := 0
	for i := 0; i < 2; i++ {
		if d := e.ServedViewDigest(id, func() (string, bool) { runs++; return "ignored", false }); d != "" {
			t.Fatalf("M5 RED: a refusal answered %q, want \"\"", d)
		}
	}
	if runs != 2 || e.ServedViewSlotsForTest() != 0 {
		t.Fatalf("M5 RED: %d gate runs, %d slots after two refusals", runs, e.ServedViewSlotsForTest())
	}
}

func TestIssue1430_M6_TheCallFill_AndTheIdentityIsExact(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":6}`)
	alice := ServedViewIdentityOf("alice", []string{"b", "a"})
	before := sv1430Stats()
	e.NoteServedView(alice, ServedViewRBACNow(), "v1.alice")
	never := func() (string, bool) {
		t.Error("M6 RED: the gate ran for a digest the /call hit stored")
		return "", false
	}
	if d := e.ServedViewDigest(ServedViewIdentityOf("alice", []string{"a", "b"}), never); d != "v1.alice" {
		t.Fatalf("M6 RED: %q", d)
	}
	after := sv1430Stats()
	if after.GatedViewCallFills-before.GatedViewCallFills != 1 || after.GatedViewReused-before.GatedViewReused != 1 ||
		after.GatedViewGateRuns != before.GatedViewGateRuns {
		t.Fatalf("M6 RED: call_fills Δ=%d reused Δ=%d gate_runs Δ=%d, want 1, 1, 0", after.GatedViewCallFills-before.GatedViewCallFills,
			after.GatedViewReused-before.GatedViewReused, after.GatedViewGateRuns-before.GatedViewGateRuns)
	}
	for name, other := range map[string]ServedViewIdentity{
		"another user":    ServedViewIdentityOf("bob", []string{"a", "b"}),
		"one group less":  ServedViewIdentityOf("alice", []string{"a"}),
		"one group more":  ServedViewIdentityOf("alice", []string{"a", "b", "c"}),
		"no groups":       ServedViewIdentityOf("alice", nil),
		"user is a group": ServedViewIdentityOf("a", []string{"alice", "b"}),
	} {
		ran := false
		d := e.ServedViewDigest(other, func() (string, bool) { ran = true; return "v1.other", true })
		if !ran || d != "v1.other" {
			t.Fatalf("M6 RED: %s was answered alice's digest (%q, gate ran: %v)", name, d, ran)
		}
	}
}

var sv1430DigestShape = regexp.MustCompile(`^v1\.[0-9a-f]{32}$`)

func TestIssue1430_M7_TheMemoHoldsDigestsOnly(t *testing.T) {
	// Every field of every memo type, by name and type. A new field fails here
	// until its author shows it cannot hold a served body.
	want := map[string]map[string]string{
		"servedViewMemo":     {"answer": "string", "mu": "sync.Mutex", "slots": "map[cache.ServedViewIdentity]*cache.servedViewSlot"},
		"servedViewSlot":     {"seq": "uint64", "once": "sync.Once", "digest": "string", "stored": "bool"},
		"ServedViewIdentity": {"username": "string", "groups": "string"},
		"ServedViewRBAC":     {"seq": "uint64", "complete": "bool"},
	}
	for _, typ := range []reflect.Type{reflect.TypeOf((*servedViewMemo)(nil)).Elem(), reflect.TypeOf((*servedViewSlot)(nil)).Elem(),
		reflect.TypeOf(ServedViewIdentity{}), reflect.TypeOf(ServedViewRBAC{})} {
		fields := want[typ.Name()]
		if typ.NumField() != len(fields) {
			t.Fatalf("M7 RED: %s has %d fields, this test knows %d", typ.Name(), typ.NumField(), len(fields))
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if fields[f.Name] != f.Type.String() {
				t.Fatalf("M7 RED: %s.%s is %s, want %q — the memo may hold a digest, a sequence and an identity, never bytes",
					typ.Name(), f.Name, f.Type, fields[f.Name])
			}
		}
	}
	// What it stores is the #1324 digest of the answer, nothing longer.
	if d := AnswerDigest([]byte(`{"a":1}`), "outcomes"); !sv1430DigestShape.MatchString(d) {
		t.Fatalf("M7: AnswerDigest shape %q", d)
	}
	// ResolvedEntryMeta (the /debug surface) carries nothing of it.
	meta := reflect.TypeOf(ResolvedEntryMeta{})
	for i := 0; i < meta.NumField(); i++ {
		if n := meta.Field(i).Name; n == "ServedViews" || n == "ServedView" {
			t.Fatalf("M7 RED: ResolvedEntryMeta exports %s", n)
		}
	}
}
