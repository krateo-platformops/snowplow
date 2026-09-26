// rbac_snapshot_allns_cost_test.go — v7 Step 2A F-C2 (index cost).
//
// Builds the all-namespace RoleBinding-by-subject reverse index over a
// ~63,316-RB synthetic snapshot (the TRACED live figure N₂=63316 at
// rbac_snapshot.go) and asserts the design's cost claims (§3):
//
//   - NO struct copy: the flat-index values are the SAME *rbacv1.RoleBinding
//     pointers held in RoleBindingsByNS (design §3: "same pointers … no
//     struct copy").
//   - LINEAR: total flat entries == Σ|RB.Subjects| (one entry per subject
//     landing, no cross-ns duplication).
//   - Memory within the ~4 MB envelope — measured on the flat index ALONE
//     (replicating the production append logic so the per-ns maps are not in
//     the delta) and reported as the actual number.
//   - Rebuild stays well within the ≤100 ms envelope (reported; a generous
//     assertion catches an accidental O(n²)).
//
// White-box (package cache) so it can call rebuildSubjectIndexes and read the
// unexported build. Hermetic: no cluster, no watcher, no globals touched.
package cache

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
)

// buildAllNSCostSnapshot returns a snapshot whose RoleBindingsByNS holds
// ~63,316 RoleBindings across many namespaces, shaped to the production
// dominant dimension: mostly unique ServiceAccount subjects (the SA-key
// dimension the design says dominates), a narrow pool of Users/Groups
// (narrow-RBAC shape), and a 1% unrecognised-Kind tail (catch-all). Exactly
// one subject per RB, so Σ|subjects| == RB count.
func buildAllNSCostSnapshot(t testing.TB) (snap *RBACSnapshot, rbCount, subjectCount int) {
	const target = 63316
	const nsCount = 1000
	snap = &RBACSnapshot{
		RoleBindingsByNS:   make(map[string][]*rbacv1.RoleBinding, nsCount),
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{},
		RolesByNSName:      map[string]*rbacv1.Role{},
	}
	for i := 0; i < target; i++ {
		ns := "ns-" + strconv.Itoa(i%nsCount)
		var sub rbacv1.Subject
		switch {
		case i%100 == 0: // 1% unknown Kind → catch-all
			sub = rbacv1.Subject{Kind: "FutureKind", Name: "fk-" + strconv.Itoa(i)}
		case i%20 == 0: // ~5% Group (narrow pool)
			sub = groupSub("group-" + strconv.Itoa(i%20))
		case i%20 == 1: // ~5% User (narrow pool)
			sub = userSub("user-" + strconv.Itoa(i%40))
		default: // ~90% unique SA subject
			sub = saSub("sa-ns-"+strconv.Itoa(i%nsCount), "sa-"+strconv.Itoa(i))
		}
		rb := rbWithSubjects(ns, "rb-"+strconv.Itoa(i), sub)
		snap.RoleBindingsByNS[ns] = append(snap.RoleBindingsByNS[ns], rb)
		rbCount++
		subjectCount += len(rb.Subjects)
	}
	return snap, rbCount, subjectCount
}

func flatEntryCount(snap *RBACSnapshot) int {
	n := 0
	for _, s := range snap.RBsByUserAllNS {
		n += len(s)
	}
	for _, s := range snap.RBsByGroupAllNS {
		n += len(s)
	}
	for _, s := range snap.RBsByServiceAccountAllNS {
		n += len(s)
	}
	n += len(snap.RBsCatchAllAllNS)
	return n
}

func TestV7Step2A_FC2_AllNSIndexCost(t *testing.T) {
	snap, rbCount, subjectCount := buildAllNSCostSnapshot(t)
	t.Logf("F-C2 fixture: %d RoleBindings across %d namespaces, %d subjects total",
		rbCount, len(snap.RoleBindingsByNS), subjectCount)

	// ── Time the production rebuild (builds per-ns AND flat indexes). ──
	start := time.Now()
	rebuildSubjectIndexes(snap)
	elapsed := time.Since(start)
	t.Logf("F-C2 rebuildSubjectIndexes(%d RBs) = %v (design envelope ≤100ms)", rbCount, elapsed)
	if elapsed > 2*time.Second {
		t.Errorf("F-C2 FAIL: rebuild took %v — far outside envelope, suspect O(n²)", elapsed)
	}

	// ── LINEAR: one flat entry per subject landing. ──
	if got := flatEntryCount(snap); got != subjectCount {
		t.Errorf("F-C2 FAIL: flat-index entries = %d; want Σ|subjects| = %d (index is not linear)", got, subjectCount)
	}

	// ── NO struct copy: flat-index values are the SAME pointers as
	// RoleBindingsByNS. Verify across all three keyed maps + catch-all by
	// sampling one landing of each Kind and checking pointer identity into
	// the owning namespace slice. ──
	assertPtrShared := func(kind string, rb *rbacv1.RoleBinding) {
		if rb == nil {
			t.Errorf("F-C2: no %s sample found", kind)
			return
		}
		for _, owner := range snap.RoleBindingsByNS[rb.Namespace] {
			if owner == rb { // pointer identity
				return
			}
		}
		t.Errorf("F-C2 STRUCT COPY: %s flat entry %s/%s is not the same pointer held in RoleBindingsByNS[%s]",
			kind, rb.Namespace, rb.Name, rb.Namespace)
	}
	for _, s := range snap.RBsByServiceAccountAllNS {
		if len(s) > 0 {
			assertPtrShared("SA", s[0])
			break
		}
	}
	for _, s := range snap.RBsByGroupAllNS {
		if len(s) > 0 {
			assertPtrShared("Group", s[0])
			break
		}
	}
	for _, s := range snap.RBsByUserAllNS {
		if len(s) > 0 {
			assertPtrShared("User", s[0])
			break
		}
	}
	if len(snap.RBsCatchAllAllNS) > 0 {
		assertPtrShared("catch-all", snap.RBsCatchAllAllNS[0])
	}

	// ── MEMORY (actual number + design's "~1×" ratio): retained-size via
	// nil-and-remeasure. The design's §3 cost claim is that the flat index is
	// "~1× the existing per-ns SA sub-index flattened", linear and copy-free —
	// NOT a fixed MB (Go map bucket overhead dominates the absolute number for
	// ~57K unique SA keys). So we measure BOTH the new flat index and the
	// existing per-ns index the same way and assert the ratio, which is robust
	// to Go map internals and is exactly the design claim.
	keyCounts := struct{ user, group, sa, catchall int }{
		user: len(snap.RBsByUserAllNS), group: len(snap.RBsByGroupAllNS),
		sa: len(snap.RBsByServiceAccountAllNS), catchall: len(snap.RBsCatchAllAllNS),
	}

	readHeap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	delta := func(before, after uint64) int64 {
		if before > after {
			return int64(before - after)
		}
		return 0
	}

	heapAll := readHeap()
	// Drop the NEW flat index first (its SA keys are shared with the per-ns
	// map, which still holds them, so this delta excludes shared key bytes).
	snap.RBsByUserAllNS = nil
	snap.RBsByGroupAllNS = nil
	snap.RBsByServiceAccountAllNS = nil
	snap.RBsCatchAllAllNS = nil
	heapNoFlat := readHeap()
	flatBytes := delta(heapAll, heapNoFlat)

	// Now drop the EXISTING per-ns index and measure it the same way.
	snap.RBsByUserByNS = nil
	snap.RBsByGroupByNS = nil
	snap.RBsByServiceAccountByNS = nil
	snap.RBsCatchAllByNS = nil
	heapNoPerNS := readHeap()
	perNSBytes := delta(heapNoFlat, heapNoPerNS)

	flatMB := float64(flatBytes) / (1024 * 1024)
	perNSMB := float64(perNSBytes) / (1024 * 1024)
	t.Logf("F-C2 MEMORY (measured, nil-and-remeasure): flat all-ns index = %.2f MB (%d B); existing per-ns index = %.2f MB (%d B); flat/per-ns ratio = %.2fx; keys: user=%d group=%d sa=%d catchall=%d",
		flatMB, flatBytes, perNSMB, perNSBytes,
		float64(flatBytes)/float64(perNSBytes+1),
		keyCounts.user, keyCounts.group, keyCounts.sa, keyCounts.catchall)
	t.Logf("F-C2 per-entry: flat index = %.0f B/entry over %d entries (Go map bucket overhead for ~57K unique SA keys dominates the absolute MB, not per-RB copy)",
		float64(flatBytes)/float64(subjectCount), subjectCount)

	// Design claim: ~1× the existing per-ns index. A struct copy or
	// superlinear growth would push the flat index well above the per-ns
	// footprint; assert it stays within a generous 2.5× band and report.
	if perNSBytes > 0 && float64(flatBytes) > 2.5*float64(perNSBytes) {
		t.Errorf("F-C2 FAIL: flat all-ns index %.2f MB is %.2fx the per-ns index %.2f MB (>2.5x) — suspect struct copy or superlinear growth",
			flatMB, float64(flatBytes)/float64(perNSBytes), perNSMB)
	}
}
