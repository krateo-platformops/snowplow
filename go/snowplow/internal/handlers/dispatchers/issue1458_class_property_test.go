// issue1458_class_property_test.go — #1458, the served-key half of the
// generated-history property (the index half is
// cache/issue1458_index_is_a_fold_test.go): over random RBAC histories with a
// prewarm pass's index step run BETWEEN each event and its publish, ONE
// IDENTITY-CLASS VALUE NEVER NAMES TWO VERDICT VECTORS.
//
// An identity's class (rbac.IdentityClassOf) is what every identity-bound cache
// key folds. If the same class value is read at two moments — by the same
// identity or by two — while the evaluator's verdicts differ, a cell stored at
// the first is served at the second under rights it was not resolved for. That
// is what a dropped binding produced (#1449, #1458): the role's later rules
// edit moved the verdicts and not the class.
//
// Worlds and mutations are the #449 property's (identity_class_property_test.go:
// a Role rules edit, a binding moved to another subject, a binding granted, a
// binding deleted), through the fake apiserver into the REAL ResourceWatcher.
// At every step the snapshot writer is held (the test barrier), the mutation's
// event is delivered, the pass step (prewarmPassIndexStep, the function the
// boot pass, the keep-warm sweep and the GVR discovery pass call) runs several
// times around it, and the writer is released. Classes and verdicts are read
// only once the publish has settled.
//
// Mutation, RED here: the pass step refilling membership from the published
// snapshot.
package dispatchers

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestIssue1458_NoClassNamesTwoVerdictVectors_PassBetweenEveryEventAndItsPublish(t *testing.T) {
	const worlds, steps = 16, 12
	reads, reReads, moved, aliases, passesInWindow := 0, 0, 0, 0, 0
	for seed := int64(1); seed <= worlds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("world-%02d", seed), func(t *testing.T) {
			objs := icsWorld(1458000 + seed)
			dyn := icsPublish(t, objs)
			rw := cache.Global()
			r := rand.New(rand.NewSource(1458100 + seed))

			type seen struct {
				who      string
				step     int
				what     string
				verdicts string
			}
			first := map[string]seen{}
			last := map[string]string{}
			record := func(step int, what string) {
				for _, id := range icsIdentities() {
					class := rbac.IdentityClassOf(jwtutil.UserInfo{Username: id.user, Groups: id.groups}).String()
					v := icsVerdicts(t, id)
					reads++
					if prev, ok := last[id.String()]; ok && prev != v {
						moved++
					}
					last[id.String()] = v
					prior, ok := first[class]
					if !ok {
						first[class] = seen{who: id.String(), step: step, what: what, verdicts: v}
						continue
					}
					if prior.step != step {
						reReads++
					}
					if prior.verdicts != v {
						aliases++
						t.Errorf("ONE CLASS VALUE, TWO VERDICT VECTORS: class %s read by %s at step %d (after %q) with verdicts [%s], "+
							"and by %s at step %d (after %q) with verdicts [%s]",
							class, prior.who, prior.step, prior.what, prior.verdicts, id, step, what, v)
					}
				}
			}
			record(0, "start")

			for step := 1; step <= steps; step++ {
				// Hold the snapshot writer.
				release := make(chan struct{})
				entered := make(chan struct{}, 64)
				cache.SetRebuildBarrierForTest(func() {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-release
				})
				freed := false
				free := func() {
					if !freed {
						freed = true
						close(release)
						cache.SetRebuildBarrierForTest(nil)
					}
				}
				t.Cleanup(free)
				seq := cache.RBACSnapshotForTest().PublishSeq

				what := icsMutate(t, dyn, objs, r, step)
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatalf("step %d (%s): the event never reached the snapshot writer", step, what)
				}
				// The handler schedules the snapshot rebuild and applies the event
				// to the index right after, on the same goroutine: pass steps
				// spread over the next moments run after it, with the snapshot
				// still unpublished.
				for i := 0; i < 5; i++ {
					time.Sleep(4 * time.Millisecond)
					prewarmPassIndexStep(rw)
					passesInWindow++
				}
				if cache.RBACSnapshotForTest().PublishSeq != seq {
					t.Fatalf("step %d (%s): a snapshot was published while the writer was held", step, what)
				}
				free()
				icsAwaitRepublish(t, seq, what)
				if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
					t.Fatalf("step %d (%s): %v", step, what, err)
				}
				record(step, what)
			}
		})
	}
	t.Logf("worlds=%d class reads=%d, of a class value already read at an earlier step=%d; identities whose verdicts moved=%d; "+
		"pass steps between an event and its publish=%d; aliases=%d", worlds, reads, reReads, moved, passesInWindow, aliases)
	if !t.Failed() && (reReads == 0 || moved == 0 || passesInWindow == 0) {
		t.Fatalf("VACUOUS: the property was never exercised")
	}
}
