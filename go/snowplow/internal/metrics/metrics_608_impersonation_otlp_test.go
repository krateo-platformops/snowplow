package metrics

// metrics_608_impersonation_otlp_test.go — #608 (b): the impersonation capability
// counters reach OTLP. They are driven by REAL capability decisions
// (cache.PermittedImpersonatingRESTConfig against a reviewer that permits users
// and refuses groups, which is 057's grant), then observed through the REAL
// registration function and callback as snowplow_impersonation{stat=...} and
// snowplow_impersonation_probe{stat=...}. Unregistering either instrument, or
// dropping its observe loop, turns this arm red.

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue608_ImpersonationCountersReachOTLP(t *testing.T) {
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))

	failGroups := false
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		switch resource {
		case cache.ImpersonateResourceUsers:
			return true, nil
		case cache.ImpersonateResourceGroups:
			if failGroups && name != "" {
				return false, errors.New("apiserver unavailable")
			}
			return name == "allowed", nil
		}
		return false, nil
	})
	cache.SetImpersonationProbeResult(true, false, false)

	base := &rest.Config{Host: "https://h"}
	ctx := context.Background()
	_ = cache.PermittedImpersonatingRESTConfig(ctx, base, "alice", []string{"allowed"}) // permitted
	_ = cache.PermittedImpersonatingRESTConfig(ctx, base, "bob", nil)                   // permitted
	_ = cache.PermittedImpersonatingRESTConfig(ctx, base, "carol", []string{"refused"}) // fallback
	failGroups = true
	_ = cache.PermittedImpersonatingRESTConfig(ctx, base, "dave", []string{"other"}) // fallback + check error

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	got := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_impersonation") {
		got[p.attrs["stat"]] = p.value
	}
	for stat, want := range map[string]int64{
		"impersonation_permitted":              2,
		"impersonation_fallback_sa":            2,
		"impersonation_capability_check_error": 1,
	} {
		if got[stat] != want {
			t.Fatalf("#608: OTLP snowplow_impersonation{stat=%s} = %d, want %d (all: %v). A missing series means "+
				"the instrument is unregistered or the callback does not observe it.", stat, got[stat], want, got)
		}
	}

	probe := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_impersonation_probe") {
		probe[p.attrs["stat"]] = p.value
	}
	if probe["users"] != 1 || probe["groups_all_names"] != 0 || probe["serviceaccounts_all_names"] != 0 || len(probe) != 3 {
		t.Fatalf("#608: OTLP snowplow_impersonation_probe = %v, want users=1 groups_all_names=0 serviceaccounts_all_names=0", probe)
	}
}
