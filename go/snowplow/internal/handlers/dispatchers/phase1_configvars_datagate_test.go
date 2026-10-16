// phase1_configvars_datagate_test.go — #106 config-vars data-change redrive gate
// falsifiers (docs/configvars-update-datagate-design-2026-07-07.md §Falsifier spec).
//
// Substrate: the SAME real-informer-over-fake-clientset harness as
// phase1_boot_race_selfheal_falsifier_test.go (SetConfigVarsWatchClientForTest +
// kfake + a REAL SharedInformerFactory ConfigMap informer). A real
// ConfigMaps().Update() fires the REAL UpdateFunc → the REAL configVarsDataChanged
// gate → (enqueueBootReDrive | configVarsSkippedTotal++). We read
// ConfigVarsEnqueuedTotal / ConfigVarsSkippedTotal. Serializes on bootRaceTestMu
// (shared singleton queue + package-level configVars* counters). No ./internal/rbac.
//
// ARM MAP (design §Falsifier spec):
//   ARM-1+2 (paired, TestConfigVarsGate_AnnotationOnlySkips_DataChangeRedrives):
//     annotation-only Update → Enqueued FLAT + Skipped +1; then a
//     Data["config.json"] Update → Enqueued +1 (reason configmap_data_changed).
//     Discriminator = the data-diff.
//   ARM-3 (TestConfigVarsGate_FlapRedrivesBothEvents): A→B→A two Data updates →
//     Enqueued +2 (per-event-delta semantics, stateless last-seen).
//   ARM-4: TestBootRace_ConfigVarsInformerDrivesReWalk (ADD path) stays GREEN
//     unchanged — not re-implemented here.
//   ARM-5 (TestConfigVarsGate_DeleteRecreateRedrivesViaAdd): Delete then Create
//     same Data → +1 via the ungated ADD path.
//
// RED mutation (source-revert, byte-clean restore): drop the gate (UpdateFunc
// calls enqueueBootReDrive unconditionally) → ARM-1 annotation arm goes RED
// (Enqueued increments on the metadata-only write). Captured to /tmp/c106/.

package dispatchers

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// configMapWatchOpened returns a channel closed once the informer's ConfigMap
// WATCH is registered with kc's object tracker (#702). The fake tracker has no
// resourceVersion replay: an object written after the informer's LIST but
// before its WATCH registers is never delivered, so a Delete+Create there is
// either missed outright or folded into the next relist as an UPDATE of
// identical Data (gated, skipped). A real apiserver replays from the LIST's
// resourceVersion; the harness must instead wait for this signal before it
// writes. The reactor performs the registration itself, so the close happens
// after the watcher is live, never before.
func configMapWatchOpened(kc *kfake.Clientset) <-chan struct{} {
	opened := make(chan struct{})
	var once sync.Once
	kc.PrependWatchReactor("configmaps", func(action ktesting.Action) (bool, watch.Interface, error) {
		var opts metav1.ListOptions
		if wa, ok := action.(ktesting.WatchActionImpl); ok {
			opts = wa.ListOptions
		}
		w, err := kc.Tracker().Watch(action.GetResource(), action.GetNamespace(), opts)
		if err != nil {
			return false, nil, err
		}
		once.Do(func() { close(opened) })
		return true, w, nil
	})
	return opened
}

// awaitConfigMapWatch blocks until configMapWatchOpened fires, failing the test
// if the informer never opens its WATCH.
func awaitConfigMapWatch(t *testing.T, opened <-chan struct{}) {
	t.Helper()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatalf("setup: the config-vars informer never opened its ConfigMap WATCH")
	}
}

// waitForCount polls until getter() reaches want (or the deadline), returning
// the final value. Lets the REAL informer deliver the event asynchronously.
func waitForCount(getter func() uint64, want uint64, d time.Duration) uint64 {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if getter() >= want {
			return getter()
		}
		time.Sleep(10 * time.Millisecond)
	}
	return getter()
}

func writeC106Artifact(t *testing.T, name, body string) {
	t.Helper()
	_ = os.MkdirAll("/tmp/c106", 0o755)
	_ = os.WriteFile("/tmp/c106/"+name, []byte(body), 0o644)
}

// cvGateCM builds a config-vars ConfigMap with the given config.json value and
// an optional traceparent-shaped annotation (models the CDC re-apply churn).
func cvGateCM(configJSON, traceparent string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bootRaceConfigMapName,
			Namespace: bootRaceNamespace,
		},
		Data: map[string]string{"config.json": configJSON},
	}
	if traceparent != "" {
		cm.ObjectMeta.Annotations = map[string]string{"krateo.io/traceparent": traceparent}
	}
	return cm
}

// startCVGateInformer starts the REAL config-vars informer over a fake clientset
// pre-seeded with an initial config-vars CM (so the ADD/initial-list has already
// fired and settled before the arm drives UPDATEs). Returns the clientset.
func startCVGateInformer(t *testing.T, initial *corev1.ConfigMap) *kfake.Clientset {
	t.Helper()
	resetBootRaceState()
	t.Cleanup(resetBootRaceState)
	t.Setenv(frontendConfigConfigMapEnv, bootRaceConfigMapName)

	kc := kfake.NewSimpleClientset(initial)
	watchOpened := configMapWatchOpened(kc)
	restore := SetConfigVarsWatchClientForTest(kc)
	t.Cleanup(restore)

	watchCtx, watchCancel := context.WithCancel(context.Background())
	t.Cleanup(watchCancel)
	StartConfigVarsWatch(watchCtx, nil, bootRaceNamespace)

	// Wait for the initial ADD (ungated) to fire so Enqueued==1 before UPDATEs.
	if got := waitForCount(ConfigVarsEnqueuedTotal, 1, 5*time.Second); got != 1 {
		t.Fatalf("setup: initial config-vars ADD did not fire exactly once (Enqueued=%d)", got)
	}
	// The initial ADD comes from the LIST; the arms' writes must land after
	// the WATCH is live or the fake tracker drops them (#702).
	awaitConfigMapWatch(t, watchOpened)
	return kc
}

// TestConfigVarsGate_AnnotationOnlySkips_DataChangeRedrives — ARM-1 (crux) +
// ARM-2, paired. The gate suppresses a metadata-only UPDATE and passes a genuine
// Data change. This ONE test is the discriminator: a gate-less build passes the
// data arm but FAILS the annotation arm (Enqueued climbs); a gate-everything
// build FAILS the data arm.
func TestConfigVarsGate_AnnotationOnlySkips_DataChangeRedrives(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	const cfgA = `{"api":{"INIT":"/call?resource=navmenus&name=a"}}`
	const cfgB = `{"api":{"INIT":"/call?resource=navmenus&name=b"}}`
	kc := startCVGateInformer(t, cvGateCM(cfgA, "00-aaaaaaaa-1111-01"))

	enqAfterAdd := ConfigVarsEnqueuedTotal() // == 1
	skipBefore := ConfigVarsSkippedTotal()

	// ── ARM-1 (crux): annotation-only UPDATE (fresh traceparent, SAME Data).
	// The CDC re-apply shape. Must be SKIPPED: Enqueued FLAT, Skipped +1.
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Update(
		context.Background(), cvGateCM(cfgA, "00-bbbbbbbb-2222-01"), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("annotation-only update: %v", err)
	}
	if got := waitForCount(ConfigVarsSkippedTotal, skipBefore+1, 5*time.Second); got != skipBefore+1 {
		t.Fatalf("ARM-1: annotation-only UPDATE must be SKIPPED (Skipped %d -> want %d); the data-change gate did not fire",
			skipBefore, skipBefore+1)
	}
	// Give any (erroneous) enqueue a beat to show up, then assert Enqueued FLAT.
	time.Sleep(300 * time.Millisecond)
	if got := ConfigVarsEnqueuedTotal(); got != enqAfterAdd {
		t.Fatalf("ARM-1 (crux) RED-shape: annotation-only UPDATE drove a boot re-enqueue (Enqueued %d -> %d) — "+
			"the metadata-only CDC churn is NOT gated (this is the pre-#106 defect / the RED mutation)", enqAfterAdd, got)
	}

	// ── ARM-2: a genuine Data["config.json"] change MUST redrive.
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Update(
		context.Background(), cvGateCM(cfgB, "00-cccccccc-3333-01"), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("data-change update: %v", err)
	}
	if got := waitForCount(ConfigVarsEnqueuedTotal, enqAfterAdd+1, 5*time.Second); got != enqAfterAdd+1 {
		t.Fatalf("ARM-2: a genuine config.json change must REDRIVE (Enqueued %d -> want %d); the gate over-suppressed a real change",
			enqAfterAdd, enqAfterAdd+1)
	}
	// The skip counter must NOT have moved on the data change.
	if got := ConfigVarsSkippedTotal(); got != skipBefore+1 {
		t.Fatalf("ARM-2: the data change must NOT be counted as a skip (Skipped=%d, want %d)", got, skipBefore+1)
	}

	writeC106Artifact(t, "arm1_2_annotation_skip_data_redrive.txt",
		"ARM-1 annotation-only UPDATE: Enqueued FLAT + Skipped+1 (gated).\n"+
			"ARM-2 config.json change: Enqueued+1 (redrive), Skipped flat.\n"+
			"Discriminator = configVarsDataChanged; RED mutation (unconditional enqueue) flips ARM-1.")
}

// TestConfigVarsGate_FlapRedrivesBothEvents — ARM-3. A→B→A as two Data updates
// each differ from the previously-delivered state → Enqueued +2. Pins the
// stateless per-event-delta semantics (no stored baseline hash that a
// revert-to-A could match and wrongly suppress).
func TestConfigVarsGate_FlapRedrivesBothEvents(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	const cfgA = `{"api":{"INIT":"/call?resource=navmenus&name=a"}}`
	const cfgB = `{"api":{"INIT":"/call?resource=navmenus&name=b"}}`
	kc := startCVGateInformer(t, cvGateCM(cfgA, "00-aaaaaaaa-1111-01"))

	enqStart := ConfigVarsEnqueuedTotal() // == 1 (initial ADD)

	// A→B (differs → redrive).
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Update(
		context.Background(), cvGateCM(cfgB, ""), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("flap A->B: %v", err)
	}
	if got := waitForCount(ConfigVarsEnqueuedTotal, enqStart+1, 5*time.Second); got != enqStart+1 {
		t.Fatalf("ARM-3: A->B must redrive (Enqueued %d -> want %d)", enqStart, enqStart+1)
	}

	// B→A (differs from the previously-delivered B → redrive AGAIN). A stored
	// boot-baseline hash of A would wrongly suppress this; the stateless
	// last-seen (old=B) does not.
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Update(
		context.Background(), cvGateCM(cfgA, ""), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("flap B->A: %v", err)
	}
	if got := waitForCount(ConfigVarsEnqueuedTotal, enqStart+2, 5*time.Second); got != enqStart+2 {
		t.Fatalf("ARM-3: B->A (revert-to-baseline) must ALSO redrive (Enqueued %d -> want %d) — "+
			"a stored-baseline-hash design would wrongly suppress it; the stateless last-seen must fire", enqStart, enqStart+2)
	}

	writeC106Artifact(t, "arm3_flap.txt",
		"A->B->A two Data updates → Enqueued +2 (per-event-delta, stateless last-seen; no baseline-hash suppression).")
}

// TestConfigVarsGate_DeleteRecreateRedrivesViaAdd — ARM-5. Delete then Create
// the SAME Data → +1 via the ungated ADD path (recreate == ADD). The stateless
// design has no stored last-hash to survive the delete and wrongly suppress the
// recreate.
func TestConfigVarsGate_DeleteRecreateRedrivesViaAdd(t *testing.T) {
	bootRaceTestMu.Lock()
	defer bootRaceTestMu.Unlock()

	const cfg = `{"api":{"INIT":"/call?resource=navmenus&name=a"}}`
	kc := startCVGateInformer(t, cvGateCM(cfg, ""))

	enqStart := ConfigVarsEnqueuedTotal() // == 1 (initial ADD)

	// Delete (no DeleteFunc → no enqueue, no skip).
	if err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Delete(
		context.Background(), bootRaceConfigMapName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := ConfigVarsEnqueuedTotal(); got != enqStart {
		t.Fatalf("ARM-5: DELETE must not enqueue (Enqueued %d -> %d); there is no DeleteFunc", enqStart, got)
	}

	// Recreate SAME Data → ADD fires → +1 (ungated ADD, no data-diff on recreate).
	if _, err := kc.CoreV1().ConfigMaps(bootRaceNamespace).Create(
		context.Background(), cvGateCM(cfg, ""), metav1.CreateOptions{}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if got := waitForCount(ConfigVarsEnqueuedTotal, enqStart+1, 5*time.Second); got != enqStart+1 {
		t.Fatalf("ARM-5: delete->recreate (same Data) must REDRIVE via the ungated ADD (Enqueued %d -> want %d) — "+
			"a stored last-hash would wrongly suppress the recreate", enqStart, enqStart+1)
	}

	writeC106Artifact(t, "arm5_delete_recreate.txt",
		"Delete (no enqueue) then Create same Data → +1 via ungated ADD (recreate==ADD; no stored-hash suppression).")
}
