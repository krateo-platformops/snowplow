package dispatchers

// issue1214_put_decline_partial_snapshot_test.go — #1214: on a partial RBAC
// snapshot, the L1 Put decline on a per-user fall-through is what stops a
// cross-identity serve.
//
// A partial snapshot (an unsynced one, as a rebuild racing the initial LIST
// publishes) holds fewer bindings, so two identities with different rights can
// derive one key. "RBAC only grants, so a partial snapshot grants a subset" covers
// verdicts read from the snapshot. It does not cover a stage dialled as the
// requester: there the apiserver judges by real rights, so the first requester's
// body can hold data the second may not read, under the key they share.
//
// What keeps it out of the shared cell is the dispatch site marking that dial as
// external (resolve.go), and the Put gate declining on it. Nothing else does, so
// this arm exists to fail if either goes.
//
// The fake apiserver here answers by identity and NEVER reads the snapshot. The
// #423 harness's own fake asks the evaluator, so on a partial snapshot it would
// refuse alice too: her body would hold no sentinel and the arm would pass on a
// 403, proving nothing.
//
// Mutation → RED, on the sentinel in bob's body: the dispatch site stops marking
// the touch, or the gate stops counting it.
//
// Soft-boot scope: the partial snapshot is published directly. How traffic reaches
// an unsynced snapshot (#1206) is not exercised here. Refs #423.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
)

// k1214RealRights is an apiserver that judges by the identity on the bearer token
// alone: alice may read the target, bob gets 403, whatever snowplow's snapshot
// holds. Each user's dials and answers are counted.
func k1214RealRights(t *testing.T, a psArm) (*httptest.Server, *k1214Wire) {
	t.Helper()
	w := &k1214Wire{}
	want := "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		if r.URL.Path != want {
			rw.WriteHeader(http.StatusNotFound)
			_, _ = rw.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
			return
		}
		switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
		case "tok-" + psAlice:
			w.aliceOK.Add(1)
			body, _ := json.Marshal(map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
				"data":     map[string]any{"password": psWireValue(a)},
			})
			_, _ = rw.Write(body)
		case "tok-" + psBob:
			w.bob403.Add(1)
			rw.WriteHeader(http.StatusForbidden)
			_, _ = rw.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
		default:
			w.other.Add(1)
			rw.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, w
}

type k1214Wire struct{ aliceOK, bob403, other atomic.Int64 }

// k1214PublishPartial publishes full without its RoleBindings and unsynced: the
// shape of a rebuild that read the indexers before the initial LIST completed.
func k1214PublishPartial(t *testing.T) {
	t.Helper()
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("PRECONDITION: %v", err)
	}
	full := cache.RBACSnapshotForTest()
	p := &cache.RBACSnapshot{
		Synced:              false,
		PublishSeq:          full.PublishSeq + 1<<32, // the authz memo is keyed by this
		ClusterRoleBindings: full.ClusterRoleBindings,
		RoleBindingsByNS:    map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName:  full.ClusterRolesByName,
		RolesByNSName:       full.RolesByNSName,
	}
	cache.RebuildSubjectIndexesForTest(p)
	cache.PublishRBACSnapshotForTest(p)
}

func TestIssue1214_APutDeclinedOnAPerUserFallThroughKeepsAPartialSnapshotsSharedKeySafe(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	for _, a := range []psArm{
		{name: "informer_servable", target: psConfigmapsGVR, watchShape: true},
		{name: "list_shape", target: psConfigmapsGVR, watchShape: false},
		{name: "informer_not_registered", target: psConfigmapsGVR, watchShape: true, noPrereg: true},
		{name: "user_bound", target: psConfigmapsGVR, watchShape: true, userBound: true},
	} {
		t.Run(a.name, func(t *testing.T) {
			psBuildWatcher(t, a)
			srv, wire := k1214RealRights(t, a)
			psSeedClientconfigs(t, srv.URL)
			aliceCtx, bobCtx := psUserCtx(a, psAlice), psUserCtx(a, psBob)

			// On the complete snapshot they have different binding sets, so the
			// sharing below comes from the partial snapshot and nothing else.
			if aFull, _ := k423RAKey(t, aliceCtx); aFull == mustKey(t, bobCtx) {
				t.Fatalf("PRECONDITION: alice and bob share a key on the complete snapshot")
			}
			k1214PublishPartial(t)
			key := mustKey(t, aliceCtx)
			if key != mustKey(t, bobCtx) {
				t.Fatalf("PRECONDITION: alice and bob must derive one key on the partial snapshot")
			}
			store := cache.ResolvedCache()
			if _, ok := store.GetNoTouch(key); ok {
				t.Fatalf("PRECONDITION: the shared key must be cold before alice's request")
			}

			alice := psServe(t, a, srv.URL, aliceCtx)
			if alice.Code != http.StatusOK || !psHasSentinel(alice.Body.Bytes()) || wire.aliceOK.Load() == 0 {
				t.Fatalf("PRECONDITION: alice's own dial must bring the sentinel, or nothing here can leak (code=%d dials=%d body=%s)",
					alice.Code, wire.aliceOK.Load(), psTrunc(alice.Body.String(), 300))
			}
			// Not Fatal: with the decline gone the arm must go on and show the leak
			// itself, not stop at the cell that would have carried it.
			_, resident := store.GetNoTouch(key)
			if resident {
				t.Errorf("RED (#1214): alice's body was Put under the key bob derives: the per-user fall-through must decline it")
			}

			bob := psServe(t, a, srv.URL, bobCtx)
			if psHasSentinel(bob.Body.Bytes()) {
				t.Fatalf("CROSS-IDENTITY SERVE (#1214): bob, who the apiserver refuses, was served alice's sentinel from the shared cell (resident=%v, bob's dials=%d, code=%d)",
					resident, wire.bob403.Load(), bob.Code)
			}
			// A clean body proves nothing unless bob really resolved and was
			// refused: the zero must come from the apiserver, not from a harness
			// that served nobody.
			if wire.bob403.Load() != 1 {
				t.Fatalf("PRECONDITION: bob must have dialled once as himself and been refused (403s=%d code=%d body=%s)",
					wire.bob403.Load(), bob.Code, psTrunc(bob.Body.String(), 300))
			}
			if wire.other.Load() != 0 {
				t.Fatalf("PRECONDITION: %d dial(s) under a credential that is neither user's: the stage must dial as the requester", wire.other.Load())
			}
		})
	}
}

func mustKey(t *testing.T, ctx context.Context) string {
	t.Helper()
	k, _ := k423RAKey(t, ctx)
	return k
}
