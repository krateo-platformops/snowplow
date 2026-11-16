//go:build integration
// +build integration

// issue1458_rbac_streaming_kind_integration_test.go — #1458 design premise,
// against a REAL apiserver (kind) with STREAMING lists (WatchListClient on, as
// production runs): every RBAC object reaches the four RBAC handlers as an add,
// an update or a delete, at start and across a relist, so a fold of those calls
// equals the live objects.
//
// The arm itself is c1458StreamedListArm
// (issue1458_rbac_streamed_list_test.go, which says what it asserts); the
// unit suite runs the same arm over a watch-list stub. Here the server is a
// real kube-apiserver, so the streamed list, the bookmark and the relist are
// the apiserver's own. The arm FAILS if the client fell back to a plain list.
//
// CLUSTER. Its own kind cluster, as issue624_rbac_kind_integration_test.go.
package cache

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/kindguard"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/e2e-framework/support/kind"
)

func TestIssue1458_Kind_StreamedListAndRelistReachTheRBACHandlers(t *testing.T) {
	ctx := context.Background()
	clusterName := kindguard.ClusterName()
	kindguard.Prepare(clusterName)
	provider := kind.NewProvider().WithName(clusterName)
	cctx, ccancel := context.WithTimeout(ctx, 6*time.Minute)
	defer ccancel()
	kubeconfig, err := provider.Create(cctx)
	if err != nil {
		t.Fatalf("create kind cluster %s: %v", clusterName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer dcancel()
		if err := provider.Destroy(dctx); err != nil {
			t.Logf("destroy kind cluster %s: %v", clusterName, err)
		}
		kindguard.Release()
	})
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	c1458StreamedListArm(t, cfg)
}
