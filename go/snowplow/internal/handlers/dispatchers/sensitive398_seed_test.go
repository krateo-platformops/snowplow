package dispatchers

// TestS398_SeedNeverPersistsSecret — the prewarm seed resolves through snowplow's
// own ServiceAccount transport (withCohortSeedContext: internal endpoint +
// internal rest.Config). That path is NOT the "external" dispatch branch, so
// the external-touched decline does not cover it: before #398 a seed resolve of
// a RESTAction whose step reads a Secret would persist the Secret body in the
// cohort's resolved-output cell. The #398 sensitive sink must decline the
// seed's terminal write.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/client-go/rest"
)

func TestS398_SeedNeverPersistsSecret(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)

	origGet := seedObjectsGetFn
	t.Cleanup(func() { seedObjectsGetFn = origGet })
	cr := psRACR(a)
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	}
	// The cohort is alice (allowed `get secrets x/y`), seeded through the SA
	// transport the production seed uses.
	cohort := withCohortSeedContext(context.Background(), seedTarget{Username: psAlice, Groups: psGroups(a)},
		endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, &rest.Config{Host: srv.URL, BearerToken: "tok-sa"})
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: psRAName, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Resource: h1RAGVR.Resource,
	}
	_ = seedOneRestaction(cohort, "cohort-s398", ref, psAuthnNS, seedModeBoot)

	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
			t.Fatalf("#398: the prewarm seed persisted a Secret body in an L1 cell")
		}
	}
	if cache.Global().IsRegistered(psSecretsGVR) {
		t.Fatalf("#398: the seed resolve registered a v1/secrets informer")
	}
}
