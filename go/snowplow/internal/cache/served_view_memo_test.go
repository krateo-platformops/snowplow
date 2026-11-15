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
//	    this test does not know; a slot's size does not depend on the groups.
//	M8  a caller that finds a slot whose gate already ran and stored nothing is
//	    answered live, never that slot's empty digest.
//	M9  the evaluator's mode: with the cache disabled or no watcher wired,
//	    nothing is stored and nothing stored is reused.
//	M10 the identity: injective (a 0x00 in a group name, an empty group, a
//	    name split another way), order-free, duplicates kept.

import (
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// sv1430Snapshot publishes snap in the one mode in which the RBAC evaluator
// reads it: the cache enabled and a watcher wired.
func sv1430Snapshot(t testing.TB, snap *RBACSnapshot) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	prevW := Global()
	SetGlobal(&ResourceWatcher{})
	prev := rbacSnap.Load()
	t.Cleanup(func() {
		rbacSnap.Store(prev)
		SetGlobal(prevW)
	})
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
		"servedViewMemo": {"answer": "string", "mu": "sync.Mutex", "slots": "map[cache.ServedViewIdentity]*cache.servedViewSlot"},
		"servedViewSlot": {"seq": "uint64", "once": "sync.Once", "digest": "string", "stored": "bool"},
		"ServedViewRBAC": {"seq": "uint64", "complete": "bool"},
	}
	for _, typ := range []reflect.Type{reflect.TypeOf((*servedViewMemo)(nil)).Elem(), reflect.TypeOf((*servedViewSlot)(nil)).Elem(),
		reflect.TypeOf(ServedViewRBAC{})} {
		fields := want[typ.Name()]
		if typ.NumField() != len(fields) {
			t.Fatalf("M7 RED: %s has %d fields, this test knows %d", typ.Name(), typ.NumField(), len(fields))
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if fields[f.Name] != f.Type.String() {
				t.Fatalf("M7 RED: %s.%s is %s, want %q — the memo may hold a digest, a sequence and an identity digest, never bytes",
					typ.Name(), f.Name, f.Type, fields[f.Name])
			}
		}
	}
	// The identity is a fixed 32-byte digest: no name, and no size that grows
	// with the requester's groups.
	if typ := reflect.TypeOf(ServedViewIdentity{}); typ.Kind() != reflect.Array || typ.Len() != 32 || typ.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("M7 RED: ServedViewIdentity is %s, want [32]uint8", typ)
	}
	if n := unsafe.Sizeof(ServedViewIdentity{}); n != 32 {
		t.Fatalf("M7 RED: an identity is %d bytes, want 32", n)
	}
	if n := unsafe.Sizeof(servedViewSlot{}); n > 48 {
		t.Fatalf("M7 RED: a slot is %d bytes, want at most 48", n)
	}
	// Measured: what one (identity, shell) slot costs on the heap, for an
	// identity with one group and for one with 200.
	perSlot := func(groups int) float64 {
		gs := make([]string, groups)
		for i := range gs {
			gs[i] = fmt.Sprintf("oidc:team-%04d-of-a-large-directory", i)
		}
		const n = 20000
		e := sv1430Entry(`{"m":7}`)
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		for i := 0; i < n; i++ {
			e.NoteServedView(ServedViewIdentityOf(fmt.Sprintf("user-%d", i), gs), ServedViewRBACNow(), AnswerDigest([]byte{byte(i), byte(i >> 8)}, ""))
		}
		runtime.GC()
		runtime.ReadMemStats(&m1)
		if got := e.ServedViewSlotsForTest(); got != n {
			t.Fatalf("PRE: %d slots stored, want %d", got, n)
		}
		runtime.KeepAlive(e)
		return float64(m1.HeapAlloc-m0.HeapAlloc) / n
	}
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	one, many := perSlot(1), perSlot(200)
	t.Logf("M7: heap per (identity, shell) slot: %.0f bytes with 1 group, %.0f with 200", one, many)
	if one > 256 || many > 256 {
		t.Fatalf("M7 RED: a slot costs %.0f bytes with 1 group and %.0f with 200, want at most 256 for both — its size "+
			"must not follow the requester's group count", one, many)
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

func TestIssue1430_M8_ACallerOfASpentSlotIsAnsweredLive(t *testing.T) {
	sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
	e := sv1430Entry(`{"m":8}`)
	id := ServedViewIdentityOf("alice", nil)
	// The slot's gate runs and stores nothing: it panics, which leaves the
	// slot in the memo with its once spent (what a waiter on a gate that
	// crossed a publish, or refused, also finds).
	func() {
		defer func() { _ = recover() }()
		e.ServedViewDigest(id, func() (string, bool) { panic("gate") })
	}()
	if n := e.ServedViewSlotsForTest(); n != 1 {
		t.Fatalf("PRE: %d slots after a gate that panicked, want the spent slot still there", n)
	}
	ran := false
	if d := e.ServedViewDigest(id, func() (string, bool) { ran = true; return "v1.live", true }); d != "v1.live" || !ran {
		t.Fatalf("M8 RED: a caller that found a slot whose gate stored nothing was answered %q (its own gate ran: %v), "+
			"want its live digest — an empty digest tells the tab it is behind", d, ran)
	}
	if n := e.ServedViewSlotsForTest(); n != 0 {
		t.Fatalf("M8 RED: the spent slot is still in the memo (%d slots): every later caller would find it again", n)
	}
}

func TestIssue1430_M9_OutsideTheSnapshotReadingModeNothingIsStoredOrReused(t *testing.T) {
	modes := map[string]func(t *testing.T) (leave func()){
		"watcher_not_wired": func(t *testing.T) func() {
			w := Global()
			SetGlobal(nil)
			return func() { SetGlobal(w) }
		},
		"cache_disabled": func(t *testing.T) func() {
			t.Setenv("CACHE_ENABLED", "false")
			return func() { t.Setenv("CACHE_ENABLED", "true") }
		},
	}
	for name, enter := range modes {
		t.Run(name, func(t *testing.T) {
			sv1430Snapshot(t, &RBACSnapshot{Synced: true, PublishSeq: 7})
			id := ServedViewIdentityOf("alice", nil)

			// Gated in the other mode, under the same complete snapshot.
			e := sv1430Entry(`{"m":9}`)
			leave := enter(t)
			if ServedViewRBACNow().Storable() {
				t.Fatalf("M9 RED: storable in mode %s — the evaluator does not read the snapshot there", name)
			}
			runs := 0
			for i := 0; i < 2; i++ {
				if d := e.ServedViewDigest(id, func() (string, bool) { runs++; return "v1.other-mode", true }); d != "v1.other-mode" {
					t.Fatalf("M9: %q", d)
				}
			}
			e.NoteServedView(id, ServedViewRBACNow(), "v1.other-mode")
			if runs != 2 || e.ServedViewSlotsForTest() != 0 {
				t.Fatalf("M9 RED: %d gate runs for 2 calls and %d slots in mode %s, want 2 and 0", runs, e.ServedViewSlotsForTest(), name)
			}
			leave()
			if d := e.ServedViewDigest(id, func() (string, bool) { return "v1.real", true }); d != "v1.real" {
				t.Fatalf("M9 RED: back in the snapshot-reading mode the memo answered %q, want the live v1.real — a digest "+
					"gated in mode %s was stored under the snapshot's sequence", d, name)
			}

			// Stored in the snapshot-reading mode, then the mode changes: not reused.
			enter(t)
			if d := e.ServedViewDigest(id, func() (string, bool) { return "v1.other-mode", true }); d != "v1.other-mode" {
				t.Fatalf("M9 RED: in mode %s the memo answered the stored %q, want the live digest", name, d)
			}

			// The mode changes WHILE the gate runs: nothing stored.
			leave()
			f := sv1430Entry(`{"m":"9-mid"}`)
			var back func()
			f.ServedViewDigest(id, func() (string, bool) { back = enter(t); return "v1.mixed", true })
			back()
			if n := f.ServedViewSlotsForTest(); n != 0 {
				t.Fatalf("M9 RED: a digest gated across a change to mode %s was stored", name)
			}
		})
	}
}

func TestIssue1430_M10_TheIdentityIsInjectiveAndOrderFree(t *testing.T) {
	type who struct {
		user   string
		groups []string
	}
	distinct := map[string][2]who{
		"a 0x00 inside a group name":  {{"u", []string{"a", "b"}}, {"u", []string{"a\x00b"}}},
		"no group vs one empty group": {{"u", nil}, {"u", []string{""}}},
		"one empty vs two empty":      {{"u", []string{""}}, {"u", []string{"", ""}}},
		"a name split another way":    {{"u", []string{"ab", "c"}}, {"u", []string{"a", "bc"}}},
		"user/group boundary":         {{"ua", []string{"b"}}, {"u", []string{"ab"}}},
		"a group that is the user":    {{"u", []string{"g"}}, {"g", []string{"u"}}},
		"a duplicate group":           {{"u", []string{"a"}}, {"u", []string{"a", "a"}}},
		// Same group count, same bytes once joined by a 0x00: only the
		// per-group length prefix tells them apart.
		"a 0x00 moved between two groups": {{"u", []string{"a\x00b", "c"}}, {"u", []string{"a", "b\x00c"}}},
		"a length-prefix look-alike":      {{"u", []string{"a", "b"}}, {"u", []string{"a\x01\x00\x00\x00\x00\x00\x00\x00b"}}},
	}
	for name, pair := range distinct {
		if ServedViewIdentityOf(pair[0].user, pair[0].groups) == ServedViewIdentityOf(pair[1].user, pair[1].groups) {
			t.Fatalf("M10 RED: %s: %q and %q are one identity — they would share a slot", name, pair[0], pair[1])
		}
	}
	if ServedViewIdentityOf("u", []string{"c", "a", "b", "a"}) != ServedViewIdentityOf("u", []string{"a", "a", "b", "c"}) {
		t.Fatal("M10 RED: the same groups in another order are two identities")
	}
	if ServedViewIdentityOf("u", nil) != ServedViewIdentityOf("u", []string{}) {
		t.Fatal("M10 RED: nil and empty groups are two identities")
	}
	in := []string{"c", "a", "b"}
	ServedViewIdentityOf("u", in)
	if strings.Join(in, "") != "cab" {
		t.Fatal("M10 RED: the caller's group slice was reordered")
	}
}

// BenchmarkIssue1430_ReusePerKey is what one armed key costs once the identity
// is built (the reuse path), and what building the identity costs once per
// connection, for a requester with 1, 20 and 200 groups. The per-key cost must
// not follow the group count.
func BenchmarkIssue1430_ReusePerKey(b *testing.B) {
	for _, n := range []int{1, 20, 200} {
		groups := make([]string, n)
		for i := range groups {
			groups[i] = fmt.Sprintf("oidc:team-%04d-of-a-large-directory", n-i)
		}
		sv1430Snapshot(b, &RBACSnapshot{Synced: true, PublishSeq: 7})
		e := sv1430Entry(`{"bench":1}`)
		id := ServedViewIdentityOf("alice", groups)
		e.NoteServedView(id, ServedViewRBACNow(), "v1.0123456789abcdef0123456789abcdef")
		gate := func() (string, bool) { b.Fatal("gated"); return "", false }
		b.Run(fmt.Sprintf("per_key/groups=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e.ServedViewDigest(id, gate)
			}
		})
		b.Run(fmt.Sprintf("identity_once_per_connection/groups=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ServedViewIdentityOf("alice", groups)
			}
		})
	}
}
