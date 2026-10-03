// issue426_failclosed_producer_test.go — #426 review condition (a)(4): the
// producer side of the fail-closed guard the raFullList subscription shares.
// seedFullListRAKey refuses an identity whose EvaluateRBAC first-match is ""
// (no binding grants the RA). Without that refusal the key would collapse to
// the shared empty-identity raFullList cell, and raFullListServe would populate
// it for every no-binding identity (the A4 class, #95 C-1).
package apiref

import (
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestIssue426_RAFullListServe_NoBindingIdentityStoresNoCell(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	k424Watcher(t, k423RAFixture()...)

	const ns, name = "krateo-system", "k426-no-binding-ra"
	ctx := f6CtxWithUser(t, "mallory-426", []string{"nobody-426"})
	if allowed, uid, _ := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{Username: "mallory-426",
		Groups: []string{"nobody-426"}, Verb: "get", Group: gvr().Group, Resource: gvr().Resource,
		Namespace: ns, Name: name}); allowed || uid != "" {
		t.Fatalf("PRE: mallory must hold no grant on the RA (allowed=%v uid=%q)", allowed, uid)
	}

	var calls atomic.Int64
	_, served, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ), 5, 1, nil,
		stubResolveRA(t, panelDict(12), &calls))
	if err != nil {
		t.Fatalf("raFullListServe: %v", err)
	}
	if served {
		t.Fatalf("a no-binding identity must fall back (served=false), not be served from a raFullList cell")
	}
	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && e.Inputs != nil &&
			e.Inputs.CacheEntryClass == cache.CacheEntryClassRAFullList {
			t.Fatalf("A4: a no-binding identity populated a raFullList cell (BindingUID=%q key=%s)",
				e.Inputs.BindingUID, k)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("a no-binding identity must not drive the unpaginated resolve (calls=%d)", n)
	}
}
