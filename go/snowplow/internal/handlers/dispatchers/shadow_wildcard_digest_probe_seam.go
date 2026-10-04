package dispatchers

import (
	"fmt"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
)

// DriveWildcardDigestProbeForTest drives the #368 probe through its production
// recorder (wildcardProbe.observe) so the OTLP arms in internal/metrics can read
// its three counters off the wire with known values (#455). It resets the probe,
// then:
//
//   - collisions times: two identities with DIFFERENT access under one digest of
//     a shareable, ungated wildcard cell (collision +1, observed +1 each);
//   - observedClean times: two identities with the SAME access under one digest
//     (observed +1, no collision);
//   - evictions times: one identity on one digest is offered evictions more
//     distinct coordinates than maxWildcardCoordsPerEntry allows (each one over
//     the cap is a dropped observation, evicted +1).
//
// So collision = collisions, observed = collisions + observedClean, evicted =
// evictions. The requester-profile seam is overridden for the drive and restored
// by the returned func, which also resets the probe. TEST ONLY; never called in
// production.
func DriveWildcardDigestProbeForTest(collisions, observedClean, evictions int) (restore func()) {
	resetWildcardDigestProbeForTest()
	prev := shadowProfileFor
	permit := &rbac.RequesterProfile{ClusterRules: []rbacv1.PolicyRule{
		{Verbs: []string{"list"}, APIGroups: []string{""}, Resources: []string{"pods"}},
	}}
	shadowProfileFor = func(_ *cache.RBACSnapshot, id rbac.EvaluateOptions) *rbac.RequesterProfile {
		if strings.HasPrefix(id.Username, "permit") {
			return permit
		}
		return &rbac.RequesterProfile{}
	}
	snap := &cache.RBACSnapshot{}
	b := newAccessBuilder()
	b.add(AccessClass{Kind: ClassWildcard, Verb: "list", Group: "*", Resource: "*"})
	dom := b.build()
	sc := func(user, digest string) *shadowContext {
		return &shadowContext{domain: dom, identity: rbac.EvaluateOptions{Username: user}, digest: digest, shareable: true}
	}
	opts := rbac.EvaluateOptions{Verb: "list", Resource: "pods", Namespace: "default"}
	for i := 0; i < collisions; i++ {
		d := fmt.Sprintf("seam-collide-%d", i)
		wildcardProbe.observe(snap, sc(fmt.Sprintf("permit-c%d", i), d), opts)
		wildcardProbe.observe(snap, sc(fmt.Sprintf("deny-c%d", i), d), opts)
	}
	for i := 0; i < observedClean; i++ {
		d := fmt.Sprintf("seam-clean-%d", i)
		wildcardProbe.observe(snap, sc(fmt.Sprintf("permit-o%d-a", i), d), opts)
		wildcardProbe.observe(snap, sc(fmt.Sprintf("permit-o%d-b", i), d), opts)
	}
	if evictions > 0 {
		for i := 0; i < maxWildcardCoordsPerEntry+evictions; i++ {
			wildcardProbe.observe(snap, sc("permit-e", "seam-evict"),
				rbac.EvaluateOptions{Verb: "list", Resource: "pods", Namespace: fmt.Sprintf("seam-ns-%d", i)})
		}
	}
	return func() {
		shadowProfileFor = prev
		resetWildcardDigestProbeForTest()
	}
}
