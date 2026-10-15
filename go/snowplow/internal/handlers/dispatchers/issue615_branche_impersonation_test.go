package dispatchers

// issue615_branche_impersonation_test.go — #615 (SECURITY): under #574
// impersonation the refresher stamps the impersonated-dial marker on its whole
// re-resolve ctx, but only the *rest.Config transport impersonates. A non-UAF
// stage that falls through to branch E dials the SA ENDPOINT, and
// rbac.MustRegateSADial stands down on the marker — so the dial went out with
// snowplow's plain ServiceAccount bearer and no Impersonate-* header: an
// un-gated read with full SA breadth, under a cohort identity.
//
// Driven through the REAL refresher entry (resolveAndPopulateL1) on the #608
// harness, for a cohort whose impersonation IS permitted (its group is
// allowlisted, so the per-principal capability check passes and the marker is
// set). The step is a pod subresource — not branch-C servable, so it reaches
// branch E.
//
// THE INVARIANT: every apiserver request the refresher makes for an
// impersonated cohort carries that cohort's Impersonate-User / Impersonate-Group
// — the apiserver evaluates exactly the cohort, as it evaluates the user's own
// credential on the customer path. A plain-SA request is the defect.

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue615_RefresherBranchEDial_IsImpersonated(t *testing.T) {
	i608Env(t)
	cohort := i608Cohort{"u615", "g615"}
	const podPath = "/api/v1/namespaces/" + i608NS + "/pods/p615/status"
	srv := newI608APIServer(t, map[string]map[string]any{
		i608RAPath("ra615-sub"): i608RA("ra615-sub", []any{map[string]any{
			"name": "podstatus", "path": podPath, "verb": "GET", "filter": ".podstatus",
		}}),
		podPath: {"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"namespace": i608NS, "name": "p615"},
			"status":   map[string]any{"phase": "Running"}},
	})
	srv.allowGroups(cohort.group) // impersonation of this cohort IS permitted
	i608Watcher(t, []i608Cohort{cohort})
	store := cache.ResolvedCache()
	_, in := i608Cell(t, store, cohort, "ra615-sub")
	saRC := &rest.Config{Host: srv.srv.URL, BearerToken: i608Token}
	saEP := &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: i608Token}
	i608InstallCapabilityCheck(t, saRC)

	var marked, seen atomic.Int32
	t.Cleanup(setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		seen.Add(1)
		if cache.ImpersonatedDialFromContext(ctx) {
			marked.Add(1)
		}
		return resolveOnceProd(ctx, in)
	}))

	_ = resolveAndPopulateL1(context.Background(), in, saEP, saRC)

	if seen.Load() == 0 || marked.Load() != seen.Load() {
		t.Fatalf("precondition: the permitted cohort's refresh must carry the impersonated-dial marker (%d of %d)",
			marked.Load(), seen.Load())
	}
	reqs := srv.requests("/pods/p615/status")
	if len(reqs) == 0 {
		t.Fatal("precondition: the branch-E pod-subresource step must be dialed (impersonated) — it never reached the apiserver")
	}
	for _, r := range reqs {
		if r.impUser != cohort.user || len(r.impGroups) != 1 || r.impGroups[0] != cohort.group {
			t.Fatalf("#615 RED: the refresher's branch-E dial went out as snowplow's ServiceAccount, NOT as the cohort "+
				"(%s %s auth=%q Impersonate-User=%q Impersonate-Group=%v). Under the impersonated-dial marker the SA "+
				"re-gate stands down, so this was an un-gated SA-breadth read.", r.method, r.path, r.auth, r.impUser, r.impGroups)
		}
	}
	// Every other request in the refresh is impersonated too (the RA fetch rides
	// the impersonating *rest.Config), so the refresh reads as the cohort end to end.
	for _, r := range srv.requests("") {
		if r.path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" || isI608DiscoveryPath(r.path) {
			continue // the capability probe and discovery are snowplow's own, by design
		}
		if r.impUser == "" {
			t.Fatalf("#615: a refresher request for an impersonated cohort went out un-impersonated: %s %s", r.method, r.path)
		}
	}
}

func isI608DiscoveryPath(p string) bool {
	_, ok := i608DiscoveryDoc(p)
	return ok
}
