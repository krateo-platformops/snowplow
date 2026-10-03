// sensitive398_arms_test.go — #398 arms: a core v1/secrets read is never
// informed and its body is held nowhere.
//
// All arms drive the real restActionHandler.ServeHTTP + the real resolver +
// the real RBAC evaluator over a real ResourceWatcher (the #423 harness in
// l1_perstep_rbac_cross_user_falsifier_test.go), with a fake apiserver that
// authorises each per-user token through the real evaluator. The target
// informer is NOT pre-registered (noPrereg): the arms observe what the request
// path itself does.
//
//   TestS398_SecretStep_NeverInformed       — GET and LIST steps on secrets add
//                                              no v1/secrets informer.
//   TestS398_DeniedUserGets403NotCachedBody — bob (no `get secrets`) after alice
//                                              read the Secret: bob's own 403.
//   TestS398_NoL1CellHoldsSecretData        — after allowed and denied reads
//                                              plus a cluster-list-shaped LIST,
//                                              no L1 entry (any class) carries
//                                              the Secret's data.
//   TestS398_DebugStoreHasNoSecretOracle    — /debug/store's view of
//                                              v1/secrets: not registered, no
//                                              body hash.
//   TestS398_MetadataOnlyReturnsPartialObjectMetadata — an `as=` Accept header
//                                              reaches the apiserver, for a
//                                              secret AND for an informed
//                                              resource (the gate is general).

package dispatchers

import (
	"bytes"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func s398Arm() psArm {
	return psArm{name: "secrets/group", target: psSecretsGVR, watchShape: true, noPrereg: true}
}

func s398ListCR(a psArm) *unstructured.Unstructured { return k423ListCR(a) }

func TestS398_SecretStep_NeverInformed(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	aliceCtx := psUserCtx(a, psAlice)

	for _, cr := range []*unstructured.Unstructured{psRACR(a), s398ListCR(a)} {
		rec := psServeCR(t, cr, srv.URL, aliceCtx)
		if !psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("SETUP: alice (allowed) must be served the Secret live; body=%s", psTrunc(rec.Body.String(), 300))
		}
	}
	if cache.Global().IsRegistered(psSecretsGVR) {
		t.Fatalf("#398: a RESTAction step on /api/v1/namespaces/x/secrets added v1/secrets to rw.informers — a " +
			"cluster-wide informer now holds every Secret's data")
	}
	// Every other registration path funnels through EnsureResourceType: it must
	// refuse too (closed skip, never added).
	if added, ch := cache.Global().EnsureResourceType(psSecretsGVR); added || ch == nil || cache.Global().IsRegistered(psSecretsGVR) {
		t.Fatalf("#398: EnsureResourceType(v1/secrets) must be a closed skip (added=%v ch=%v registered=%v)",
			added, ch != nil, cache.Global().IsRegistered(psSecretsGVR))
	}
}

func TestS398_DeniedUserGets403NotCachedBody(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)

	if rec := psServe(t, a, srv.URL, psUserCtx(a, psAlice)); !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: alice must read the Secret")
	}
	before := counters[psBob].Load()
	rec := psServe(t, a, srv.URL, psUserCtx(a, psBob))
	if psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("#398: bob (no `get secrets`) was served Secret data; body=%s", psTrunc(rec.Body.String(), 300))
	}
	if counters[psBob].Load() == before {
		t.Fatalf("#398: bob's read must reach the apiserver under his own credentials (it was served from a cache)")
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"code":403`)) {
		t.Fatalf("#398: bob must see the apiserver's own 403; body=%s", psTrunc(rec.Body.String(), 400))
	}
}

func TestS398_NoL1CellHoldsSecretData(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	for _, u := range []string{psAlice, psBob, psAlice} { // repeat alice: a second read must not find a cell either
		psServe(t, a, srv.URL, psUserCtx(a, u))
		psServeCR(t, s398ListCR(a), srv.URL, psUserCtx(a, u))
	}
	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
			class := ""
			if e.Inputs != nil {
				class = e.Inputs.CacheEntryClass
			}
			t.Fatalf("#398: an L1 cell (class %q) holds the Secret's data", class)
		}
	}
	if sensitiveSkipped398() == 0 {
		t.Fatalf("#398: the sensitive-resource Put decline never fired — the arm did not exercise it")
	}
}

func TestS398_DebugStoreHasNoSecretOracle(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	psServe(t, a, srv.URL, psUserCtx(a, psAlice))
	st := cache.Global().StoreObjectState(psSecretsGVR, psTargetNS, psTargetObj)
	if st.Registered || st.Found || st.BodySHA256 != "" {
		t.Fatalf("#398: /debug/store still answers for v1/secrets (registered=%v found=%v sha=%q) — an "+
			"existence / body-hash oracle over every Secret", st.Registered, st.Found, st.BodySHA256)
	}
}

func TestS398_MetadataOnlyReturnsPartialObjectMetadata(t *testing.T) {
	k423Env(t)
	for _, a := range []psArm{
		s398Arm(), // never informed
		{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}, // informed + servable
	} {
		a := a
		t.Run(a.name, func(t *testing.T) {
			psBuildWatcher(t, a)
			srv := psFakeAPIServer(t, a, k423Counters())
			psSeedClientconfigs(t, srv.URL)
			cr := psRACR(a)
			steps, _, _ := unstructured.NestedSlice(cr.Object, "spec", "api")
			step := steps[0].(map[string]any)
			step["headers"] = []any{"Accept: application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"}
			_ = unstructured.SetNestedSlice(cr.Object, steps, "spec", "api")

			rec := psServeCR(t, cr, srv.URL, psUserCtx(a, psAlice))
			body := rec.Body.Bytes()
			if !bytes.Contains(body, []byte("PartialObjectMetadata")) || psHasSentinel(body) {
				t.Fatalf("#398: a metadata-only request must return PartialObjectMetadata and no data (the informer "+
					"answered with the full object instead); body=%s", psTrunc(rec.Body.String(), 400))
			}
		})
	}
}
