// issue1458_atomic_update_test.go — #1458: no reader of the bindings index, and
// no role hook, ever sees a binding absent while it is being UPDATED; and the
// index is marked ready only with all four RBAC handler registrations.
//
// A binding UPDATE replaces the binding's old side by its new side. It used to
// do so in two separate holds of the index lock (remove, then record). In
// between the binding was in the index under neither side, and a rules edit of
// its role handled in that gap — the role hooks run on another informer's
// goroutine — found no binding and bumped no subject: the holder's key did not
// move. ANY update opened the gap, the unchanged re-deliveries a relist sends
// for every binding included.
//
//	TestIssue1458_BindingUpdateIsOneCriticalSection
//	    deterministic: a role's rules edit is started exactly between the two
//	    halves of a binding update (the test hook there). With one critical
//	    section it cannot run until the update is done, and then finds the
//	    binding and records the bump. RED with two lock sections: it runs in
//	    the gap and bumps nobody.
//	TestIssue1458_NoRulesEditIsLostWhileBindingsAreUpdated
//	    stress: unchanged binding updates on one goroutine, rules edits of the
//	    binding's role on another; every edit must record the subject's bump.
//	TestIssue1458_ReadyNeedsAllFourHandlerRegistrations
//	    a watcher with one RBAC handler registration missing never marks the
//	    index ready and never completes readiness; it says which kind.
package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// a1458Index stands a ready, empty index up with no watcher: the arms drive the
// hooks directly, as the informers' handler goroutines do.
func a1458Index(t *testing.T) {
	t.Helper()
	if err := WaitRBACRebuildQuiesceForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	prevSnap := rbacSnap.Load()
	prevGlobal := Global()
	SetGlobal(nil)
	reset := func() {
		ResetBindingsByGVRIndexForTest()
		ResetPendingSubGenBumpsForTest()
		ResetRBACSubGenForTest()
		bindingUpdateMidHookForTest.Store(nil)
	}
	reset()
	rbacSnap.Store(&RBACSnapshot{})
	markBindingsIndexReady()
	t.Cleanup(func() {
		reset()
		rbacSnap.Store(prevSnap)
		SetGlobal(prevGlobal)
	})
}

// a1458Pending reports whether a sub-generation bump is recorded for the group,
// and clears the pending set.
func a1458TakePending(group string) bool {
	pendingSubGenBumps.mu.Lock()
	defer pendingSubGenBumps.mu.Unlock()
	_, ok := pendingSubGenBumps.set[subjectKey{Kind: rbacv1.GroupKind, Name: group}]
	pendingSubGenBumps.set = map[subjectKey]subGenBumpSource{}
	return ok
}

type a1458Pair struct {
	name                 string
	binding              any
	roleBefore, roleEdit any
}

func a1458Pairs() []a1458Pair {
	return []a1458Pair{
		{"rolebinding-to-role", c1458RB("a", "rb-atomic", "r-atomic", "g-atomic"),
			c1458Role("a", "r-atomic", c1458Rules("configmaps")), c1458Role("a", "r-atomic", c1458Rules("pods"))},
		{"clusterrolebinding-to-clusterrole", c1458CRB("crb-atomic", "cr-atomic", "g-atomic"),
			c1458CR("cr-atomic", c1458Rules("configmaps")), c1458CR("cr-atomic", c1458Rules("pods"))},
	}
}

func TestIssue1458_BindingUpdateIsOneCriticalSection(t *testing.T) {
	for _, p := range a1458Pairs() {
		p := p
		t.Run(p.name, func(t *testing.T) {
			a1458Index(t)
			onBindingAdd(p.binding)
			if !a1458TakePending("g-atomic") {
				t.Fatalf("SETUP: the binding's ADD recorded no bump for its subject")
			}
			// Control: with no update in flight the rules edit bumps the subject.
			onRoleUpdated(p.roleBefore, p.roleEdit)
			if !a1458TakePending("g-atomic") {
				t.Fatalf("SETUP: a rules edit of the binding's role recorded no bump for its subject")
			}

			// The rules edit is started exactly between the two halves of the
			// binding's update, and given a moment to run there.
			edited := make(chan struct{})
			ranInTheGap := false
			hook := func() {
				go func() {
					onRoleUpdated(p.roleEdit, p.roleBefore)
					close(edited)
				}()
				select {
				case <-edited:
					ranInTheGap = true
				case <-time.After(150 * time.Millisecond):
				}
			}
			bindingUpdateMidHookForTest.Store(&hook)
			onBindingUpdate(p.binding, p.binding) // an unchanged re-delivery, as a relist sends for every binding
			bindingUpdateMidHookForTest.Store(nil)
			select {
			case <-edited:
			case <-time.After(10 * time.Second):
				t.Fatalf("the rules edit never completed")
			}
			bumped := a1458TakePending("g-atomic")
			t.Logf("the rules edit ran between the two halves of the binding update=%v; it recorded the subject's bump=%v", ranInTheGap, bumped)
			if ranInTheGap {
				t.Errorf("A ROLE HOOK RAN INSIDE A BINDING UPDATE: the removal of the old side and the record of the new one are not one critical section")
			}
			if !bumped {
				t.Errorf("RULES EDIT BUMPED NOBODY: handled while the binding was being updated, the edit of its role found no " +
					"binding and recorded no bump for its subject — the holder's key does not move")
			}
		})
	}
}

func TestIssue1458_NoRulesEditIsLostWhileBindingsAreUpdated(t *testing.T) {
	for _, p := range a1458Pairs() {
		p := p
		t.Run(p.name, func(t *testing.T) {
			a1458Index(t)
			onBindingAdd(p.binding)
			a1458TakePending("g-atomic")

			var updates atomic.Int64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					onBindingUpdate(p.binding, p.binding)
					updates.Add(1)
				}
			}()
			const edits = 200000
			lost := 0
			for i := 0; i < edits; i++ {
				if i%2 == 0 {
					onRoleUpdated(p.roleBefore, p.roleEdit)
				} else {
					onRoleUpdated(p.roleEdit, p.roleBefore)
				}
				if !a1458TakePending("g-atomic") {
					lost++
				}
			}
			close(stop)
			wg.Wait()
			t.Logf("rules edits=%d, concurrent unchanged binding updates=%d, edits that bumped nobody=%d", edits, updates.Load(), lost)
			if updates.Load() < edits/10 {
				t.Fatalf("VACUOUS: only %d binding updates ran beside %d edits", updates.Load(), edits)
			}
			if lost != 0 {
				t.Errorf("%d of %d rules edits BUMPED NOBODY while the binding was being updated", lost, edits)
			}
		})
	}
}

func TestIssue1458_ReadyNeedsAllFourHandlerRegistrations(t *testing.T) {
	for _, gone := range c1458Kinds {
		gone := gone
		t.Run(fmt.Sprintf("no-registration-on-%s", gone.Resource), func(t *testing.T) {
			// The roles LIST is refused until released, which holds the
			// initial-publish goroutine before it looks for the registrations.
			var open atomic.Bool
			rw, _ := r1458Start(t, func(dyn *dynamicfake.FakeDynamicClient) {
				dyn.PrependReactor("list", "roles", func(clienttesting.Action) (bool, runtime.Object, error) {
					if open.Load() {
						return false, nil, nil
					}
					return true, nil, fmt.Errorf("the roles LIST is not answered yet (test)")
				})
			}, r1458World(4)...)
			t.Cleanup(func() { rbacHandlersMissing.Store(nil) })
			rw.mu.RLock()
			in := rw.instances[gone]
			rw.mu.RUnlock()
			if in == nil || in.extensionRegistration(rbacSnapshotHandlerExtension) == nil {
				t.Fatalf("SETUP: the %s informer has no RBAC handler registration to remove", gone.Resource)
			}
			in.mu.Lock()
			delete(in.extRegs, rbacSnapshotHandlerExtension)
			in.mu.Unlock()
			open.Store(true)

			if !c1458Wait(20*time.Second, func() bool { return len(RBACHandlersMissing()) > 0 }) {
				t.Errorf("READY FAILED OPEN: the initial publish never reported the missing %s registration (index ready=%v, readiness complete=%v)",
					gone.Resource, BindingsByGVRIndexBuilt(), RBACReadiness().Complete)
			}
			if got := RBACHandlersMissing(); len(got) != 1 || got[0] != gone.Resource {
				t.Errorf("missing registrations reported: %v, want [%s]", got, gone.Resource)
			}
			MarkPhase1Done()
			if err := rw.WaitInitialRBACPublishForTest(500 * time.Millisecond); err == nil {
				t.Errorf("the initial publish RETURNED with a handler registration missing: readiness is released")
			}
			if BindingsByGVRIndexBuilt() {
				t.Errorf("THE INDEX IS READY with no RBAC handler registered on %s: its events never reach the index", gone.Resource)
			}
			if st := RBACReadiness(); st.Complete {
				t.Errorf("READINESS IS COMPLETE with no RBAC handler registered on %s", gone.Resource)
			}
		})
	}
}

// TestIssue1458_RecordIsAnUpsert — a binding recorded again under an id the
// index already holds (its ADD delivered again, or an update whose old side the
// hook could not normalise) is filed ONCE, under the role it now names.
func TestIssue1458_RecordIsAnUpsert(t *testing.T) {
	a1458Index(t)
	first := c1458RB("a", "rb-upsert", "r-one", "g-one")
	second := c1458RB("a", "rb-upsert", "r-two", "g-two")
	onBindingAdd(first)
	onBindingAdd(second)
	got := c1458IndexMembership()
	want := map[string]string{string(rbBindingID(second)): "R/a/r-two <- Group:/g-two"}
	if !h1458Same(got, want) {
		t.Errorf("after a second record under the same id the index holds:\n%s", h1458Diff(got, want))
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	stale := len(idx.membersByRole["R/a/r-one"])
	idx.mu.RUnlock()
	if stale != 0 {
		t.Errorf("the binding is still filed under the role it no longer names (%d): that role's edits would bump its subjects for nothing, "+
			"and a re-point back would file it twice", stale)
	}
}

// TestIssue1458_UpdateThatChangesTheBindingIdentityRemovesTheOldOne — a binding
// deleted and created again behind a deaf watch reaches the handler as ONE
// update whose two sides have different UIDs: the old identity must leave the
// index with it.
func TestIssue1458_UpdateThatChangesTheBindingIdentityRemovesTheOldOne(t *testing.T) {
	a1458Index(t)
	before := c1458RB("a", "rb-reborn", "r-one", "g-one")
	after := c1458RB("a", "rb-reborn", "r-one", "g-two")
	after.UID = "uid-rb-rb-reborn-second-life"
	onBindingAdd(before)
	onBindingUpdate(before, after)
	got := c1458IndexMembership()
	want := map[string]string{string(rbBindingID(after)): "R/a/r-one <- Group:/g-two"}
	if !h1458Same(got, want) {
		t.Errorf("after an update that changed the binding's identity the index holds:\n%s", h1458Diff(got, want))
	}
}
