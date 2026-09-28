// sa_attach_falsifier_test.go — Ship 0.30.166 dispatcher-attach falsifier,
// SUPERSEDED by Part 1 (#268/#269).
//
// The 0.30.166 attach (WithInternalEndpoint/WithInternalRESTConfig on every
// per-user dispatch ctx) was the SA-serve leak vector and has been REMOVED in
// Part 1. The two original tests here asserted the attach's out-of-cluster
// no-op (snowplowSACtx→(nil,nil) and the inline attach not engaging); with the
// attach gone they are obsolete:
//   - TestSnowplowSACtx_OutOfCluster_ReturnsNilNil → replaced by
//     TestSnowplowSARC_OutOfCluster_ReturnsNil below (the surviving rc-only helper).
//   - TestSnowplowSACtx_NilReturn_NoAttach → DELETED (there is no attach to not
//     engage; the "no internal transport on the resolve ctx" invariant is now
//     asserted end-to-end by the M12 tests in servehttp_orchestration_test.go and
//     by the #268/#269 falsifier arms flipping GREEN).
package dispatchers

import (
	"os"
	"testing"
)

// TestSnowplowSARC_OutOfCluster_ReturnsNil pins the AC-307.7-equivalent invariant
// for the surviving rc-only helper: out of cluster (no projected SA volume, no
// KUBERNETES_SERVICE_HOST) snowplowSARC() returns nil, so the handler's saRC is nil
// → ResolveOptions.SArc is nil → the unchanged empty-resolve behaviour. (The SA
// ENDPOINT is no longer acquired here at all — Part 1.)
func TestSnowplowSARC_OutOfCluster_ReturnsNil(t *testing.T) {
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Skip("test host has a projected SA volume; this out-of-cluster invariant does not apply")
	}
	if saRC := snowplowSARC(); saRC != nil {
		t.Fatalf("AC-307.7 (rc-only): snowplowSARC() must return nil out-of-cluster "+
			"(rest.InClusterConfig errors without KUBERNETES_SERVICE_HOST/PORT); got %+v", saRC)
	}
}
