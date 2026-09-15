// issue1126_c3_red_probe_wiring_test.go — the ONE C3 symbol the committed
// RED probe (issue1126_c3_red_probe_test.go) touches, kept apart so the
// probe itself compiles on origin/main against a no-op stub of this
// function. Here it starts the real reconcile ticker at period 1 s, exactly
// as main.go does at boot (StartDepsReconcile on the process cacheCtx).

package cache

import (
	"context"
	"testing"
)

func c3StartAudit(t *testing.T) {
	t.Helper()
	t.Setenv(envDepsReconcilePeriodSeconds, "1")
	t.Setenv(envDepsReconcileSample, "512")
	resetDepsReconcileForTest()
	t.Cleanup(resetDepsReconcileForTest)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartDepsReconcile(ctx)
	if !DepsReconcileStatsSnapshot().Started {
		t.Fatalf("c3StartAudit: the reconcile ticker did not start")
	}
}
