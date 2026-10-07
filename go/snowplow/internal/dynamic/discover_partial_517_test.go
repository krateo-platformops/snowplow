package dynamic

// discover_partial_517_test.go — the #517 falsifier set.
//
// THE BOUNDARY UNDER TEST is Discover's handling of what
// discovery.ServerPreferredResources actually returns: results AND an error
// together. The arms drive that real return shape through the real Discover; no
// arm installs the end state it is meant to detect.
//
// The set covers BOTH directions, because the fix makes one path more
// permissive:
//   - Partial  → the healthy groups MUST survive and the degradation MUST be
//     reported (the fix).
//   - Transport → MUST still be fatal (the thing that must STILL work; it is
//     what keeps GET /list honest when nothing was discovered).
//   - All groups failed → MUST still be fatal (a 200 with an empty list would
//     be the same under-report, inverted).
//   - Healthy    → MUST stay a clean, non-degraded success.

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// disco517 is a fake discovery whose ServerPreferredResources returns exactly
// the pair client-go returns: the lists it managed to fetch, plus whatever
// error the fetch produced. Nothing else is reimplemented.
type disco517 struct {
	discovery.DiscoveryInterface // embedded nil — unreached methods panic
	lists                        []*metav1.APIResourceList
	err                          error
}

func (d disco517) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.lists, d.err
}

const cat517 = "krateo517"

// healthy517 is the two HEALTHY group/versions. Group and Version are left
// empty on the APIResources, as a conformant apiserver sends them (#508): the
// coordinates come from the containing list.
func healthy517() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{
		{
			GroupVersion: "good.example.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "goodthings", Kind: "GoodThing", Namespaced: true, Categories: []string{cat517}},
			},
		},
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "configmaps", SingularName: "configmap", Kind: "ConfigMap", Namespaced: true, Categories: []string{cat517}},
			},
		},
	}
}

// staleAPIServiceErr is what client-go hands back when ONE aggregated
// APIService is stale: the group/version named, with its cause.
func staleAPIServiceErr() error {
	return &discovery.ErrGroupDiscoveryFailed{
		Groups: map[schema.GroupVersion]error{
			{Group: "stale.example.io", Version: "v1"}: errors.New(
				"the server is currently unable to handle the request"),
		},
	}
}

type gvr517 struct{ g, v, r string }

func seen517(got []schema.GroupVersionResource) map[gvr517]bool {
	out := map[gvr517]bool{}
	for _, x := range got {
		out[gvr517{x.Group, x.Version, x.Resource}] = true
	}
	return out
}

// TestIssue517_PartialDiscovery_HonoursHealthyGroups is the headline falsifier.
//
// RED before the fix: Discover's `if err != nil { return }` discarded `lists`,
// so this returned 0 resources and a fatal error, and handlers/list.go turned
// it into a 500 for EVERY category — including the two healthy groups here.
func TestIssue517_PartialDiscovery_HonoursHealthyGroups(t *testing.T) {
	before := DiscoveryStatsSnapshot()

	cli := newPerCallClient(t, disco517{lists: healthy517(), err: staleAPIServiceErr()})

	got, err := cli.Discover(context.Background(), cat517)

	// 1. THE HEALTHY LISTS SURVIVE.
	if len(got) != 2 {
		t.Fatalf("#517 RED: partial discovery returned %d resources, want 2 — the healthy groups' "+
			"resources were discarded, which is what makes one stale APIService break every category; got %+v (err %v)",
			len(got), got, err)
	}
	s := seen517(got)
	for _, want := range []gvr517{
		{"good.example.io", "v1", "goodthings"},
		{"", "v1", "configmaps"},
	} {
		if !s[want] {
			t.Errorf("#517 RED: missing %+v — a healthy group must survive a sibling group's failure; got %+v", want, got)
		}
	}

	// 2. THE DEGRADATION IS REPORTED TO THE CALLER, not left as a silently
	//    shorter list.
	if err == nil {
		t.Fatalf("#517: partial discovery returned a nil error — a degraded read that looks complete " +
			"is exactly the under-report shape this fix exists to avoid")
	}
	pd, ok := AsPartialDiscovery(err)
	if !ok {
		t.Fatalf("#517: err %T (%v) is not a *PartialDiscoveryError — the caller cannot tell a degradation "+
			"from a fatal failure, so it will 500", err, err)
	}
	if want := []string{"stale.example.io/v1"}; len(pd.FailedGroupVersions()) != 1 || pd.FailedGroupVersions()[0] != want[0] {
		t.Errorf("#517: FailedGroupVersions()=%v, want %v — the failed group NAMES the stale APIService "+
			"and is the whole point of surfacing the error", pd.FailedGroupVersions(), want)
	}
	if !strings.Contains(pd.Error(), "stale.example.io/v1") ||
		!strings.Contains(pd.Error(), "unable to handle the request") {
		t.Errorf("#517: Error()=%q must carry both the failed group/version and its cause", pd.Error())
	}

	// 3. IT IS ALSO VISIBLE WITHOUT THE CALLER: the counter moved, with a
	//    denominator, and the failed group is on the metrics surface.
	after := DiscoveryStatsSnapshot()
	if d := after.Partial - before.Partial; d != 1 {
		t.Errorf("#517: partial_total moved by %d, want 1 — a degradation nobody counts is undetectable", d)
	}
	if d := after.Total - before.Total; d != 1 {
		t.Errorf("#517: total moved by %d, want 1 — without the denominator a zero partial_total "+
			"cannot be told apart from no /list traffic at all", d)
	}
	if d := after.Fatal - before.Fatal; d != 0 {
		t.Errorf("#517: fatal_total moved by %d on a PARTIAL discovery, want 0", d)
	}
	if len(after.FailedGroups) != 1 || after.FailedGroups[0] != "stale.example.io/v1" {
		t.Errorf("#517: failed_groups=%v, want [stale.example.io/v1]", after.FailedGroups)
	}
}

// TestIssue517_TransportFailure_StaysFatal is the other direction: the fix made
// one path more permissive, so the path that must STILL fail is asserted too.
// A genuine transport failure discovered NOTHING, so answering 200 with an
// empty list would be a lie.
func TestIssue517_TransportFailure_StaysFatal(t *testing.T) {
	before := DiscoveryStatsSnapshot()

	// Not an *ErrGroupDiscoveryFailed. client-go returns no result at all for
	// this class (withRetries: `return nil, nil, err`), so the fake does too.
	transport := errors.New("dial tcp 10.0.0.1:443: connect: connection refused")
	cli := newPerCallClient(t, disco517{lists: nil, err: transport})

	got, err := cli.Discover(context.Background(), cat517)

	if err == nil {
		t.Fatalf("#517: a transport failure must stay FATAL — got nil error and %+v", got)
	}
	if !errors.Is(err, transport) {
		t.Errorf("#517: a transport failure must reach the caller unchanged; got %v", err)
	}
	if _, ok := AsPartialDiscovery(err); ok {
		t.Errorf("#517: a transport failure was misclassified as a PARTIAL discovery (%v) — "+
			"that would make /list answer 200 with nothing discovered", err)
	}
	if len(got) != 0 {
		t.Errorf("#517: a transport failure must return no resources; got %+v", got)
	}

	after := DiscoveryStatsSnapshot()
	if d := after.Fatal - before.Fatal; d != 1 {
		t.Errorf("#517: fatal_total moved by %d, want 1", d)
	}
	if d := after.Partial - before.Partial; d != 0 {
		t.Errorf("#517: partial_total moved by %d on a transport failure, want 0", d)
	}
	if d := after.Total - before.Total; d != 1 {
		t.Errorf("#517: total moved by %d, want 1 — every call counts in the denominator", d)
	}
}

// TestIssue517_EveryGroupFailed_StaysFatal pins the edge the partial-result API
// allows and the issue does not mention: a group failure with NO healthy list
// behind it. Honouring that would serve an empty 200 and call a total discovery
// outage healthy — the same under-report, inverted.
func TestIssue517_EveryGroupFailed_StaysFatal(t *testing.T) {
	before := DiscoveryStatsSnapshot()

	allFailed := &discovery.ErrGroupDiscoveryFailed{
		Groups: map[schema.GroupVersion]error{
			{Group: "a.example.io", Version: "v1"}: errors.New("stale"),
			{Group: "b.example.io", Version: "v1"}: errors.New("stale"),
		},
	}
	cli := newPerCallClient(t, disco517{lists: []*metav1.APIResourceList{}, err: allFailed})

	got, err := cli.Discover(context.Background(), cat517)

	if err == nil {
		t.Fatalf("#517: a group failure with NOTHING healthy must stay fatal — got nil error and %+v", got)
	}
	if _, ok := AsPartialDiscovery(err); ok {
		t.Errorf("#517: %q was honoured as PARTIAL although no healthy list came back — "+
			"\"partial\" needs a part, and an empty 200 hides a total discovery outage", err)
	}
	if len(got) != 0 {
		t.Errorf("#517: want no resources; got %+v", got)
	}

	after := DiscoveryStatsSnapshot()
	if d := after.Fatal - before.Fatal; d != 1 {
		t.Errorf("#517: fatal_total moved by %d, want 1", d)
	}
	if d := after.Partial - before.Partial; d != 0 {
		t.Errorf("#517: partial_total moved by %d, want 0", d)
	}
	// The groups are still named, because that is what an operator needs.
	if len(after.FailedGroups) != 2 {
		t.Errorf("#517: failed_groups=%v, want both failed group/versions named even on the fatal path",
			after.FailedGroups)
	}
}

// TestIssue517_HealthyDiscovery_IsNotDegraded is the must-still-work arm: the
// ordinary path stays a clean success, reports no degradation, and does not
// inflate either failure counter.
func TestIssue517_HealthyDiscovery_IsNotDegraded(t *testing.T) {
	before := DiscoveryStatsSnapshot()

	cli := newPerCallClient(t, disco517{lists: healthy517(), err: nil})

	got, err := cli.Discover(context.Background(), cat517)
	if err != nil {
		t.Fatalf("#517: a healthy discovery must return a nil error; got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("#517: a healthy discovery must return both resources; got %+v", got)
	}

	after := DiscoveryStatsSnapshot()
	if d := after.Partial - before.Partial; d != 0 {
		t.Errorf("#517: partial_total moved by %d on a HEALTHY discovery, want 0 — a detector that "+
			"fires when nothing is wrong is not a detector", d)
	}
	if d := after.Fatal - before.Fatal; d != 0 {
		t.Errorf("#517: fatal_total moved by %d on a healthy discovery, want 0", d)
	}
	if d := after.Total - before.Total; d != 1 {
		t.Errorf("#517: total moved by %d, want 1", d)
	}
}

// TestIssue517_DegradationIsOnDebugVars pins the operator-facing surface: the
// family is published, it is NOT gated on the cache flag (GET /list performs
// discovery either way), and a scrape names the stale group.
func TestIssue517_DegradationIsOnDebugVars(t *testing.T) {
	v := expvar.Get("snowplow_discovery")
	if v == nil {
		t.Fatalf("#517: /debug/vars has no snowplow_discovery key — the degradation would only be " +
			"readable by tailing logs")
	}

	cli := newPerCallClient(t, disco517{lists: healthy517(), err: staleAPIServiceErr()})
	if _, err := cli.Discover(context.Background(), cat517); err == nil {
		t.Fatalf("#517: precondition — the partial arm must produce a degradation")
	}

	var got DiscoveryStats
	if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
		t.Fatalf("#517: snowplow_discovery is not readable JSON (%v): %s", err, v.String())
	}
	if got.Total == 0 {
		t.Errorf("#517: scraped total=0 after a Discover call — the denominator is not wired")
	}
	if got.Partial == 0 {
		t.Errorf("#517: scraped partial_total=0 after a partial discovery")
	}
	if len(got.FailedGroups) == 0 || got.FailedGroups[len(got.FailedGroups)-1] != "stale.example.io/v1" {
		t.Errorf("#517: scraped failed_groups=%v, want the stale group named", got.FailedGroups)
	}
}
