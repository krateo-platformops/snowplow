// memo_would_join_test.go — the resolve memo's measurement-only would-join
// counter through the REAL apiref.Resolve: a miss counts when a produce of
// the same memo key is already in flight and not otherwise, Resolve takes its
// own produce out of flight when it returns, and the body Resolve returns is
// exactly the one it returns with no memo (the counter changes nothing).

package apiref

import (
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestMemoWouldJoin_ThroughResolve(t *testing.T) {
	cacheOn(t)
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	const raName = "ra-would-join"
	raObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": raName, "namespace": edge3BackingNS},
		"spec":       map[string]any{"filter": richFilter883},
	}}
	edge3NewWatcher(t, append(f6BuildFixture(), raObj)...)
	if added, syncCh := cache.Global().EnsureResourceType(gvr()); added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gvr(), 3*time.Second) {
		t.Fatalf("restactions GVR never became servable")
	}

	const user = "admin"
	groups := []string{"system:masters"}
	base := f6CtxWithUser(t, user, groups)
	opts := ResolveOptions{ApiRef: templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: raName, Namespace: edge3BackingNS},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}}
	resolveAs := func(l1 string, m *cache.SeedResolveMemo) map[string]any {
		t.Helper()
		ctx := cache.WithL1KeyContext(base, l1)
		if m != nil {
			ctx = cache.WithSeedResolveMemo(ctx, m)
		}
		out, err := Resolve(ctx, opts)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", l1, err)
		}
		return out
	}
	// The memo key this Resolve folds — built the way Resolve builds it.
	memo := cache.NewSeedResolveMemo()
	key := memo.Key(edge3BackingNS, raName, user, groups, rbacClassForMemo(user, groups),
		cache.HashExtras(opts.Extras), opts.PerPage, opts.Page)

	// 1. Another produce of the same key is in flight: the miss would join.
	other := memo.BeginProduce(key)
	joined := resolveAs("w-join-1", memo)
	if got := memo.WouldJoin(); got != 1 {
		t.Fatalf("a miss while the same key was in flight: WouldJoin=%d, want 1", got)
	}
	other.End()

	// 2. Resolve took its own produce out of flight when it returned: a new
	// produce of the key finds nothing in flight.
	memo.BeginProduce(key).End()
	if got := memo.WouldJoin(); got != 1 {
		t.Fatalf("Resolve left its produce in flight: WouldJoin=%d, want 1", got)
	}

	// 3. A hit is not a produce: nothing counts.
	hit := resolveAs("w-join-2", memo)
	if h, _ := memo.Stats(); h != 1 {
		t.Fatalf("setup: the sibling must hit (hits=%d)", h)
	}
	if got := memo.WouldJoin(); got != 1 {
		t.Fatalf("a memo hit counted a would-join: %d", got)
	}

	// 4. A miss of ANOTHER key (another identity) while this key is in flight
	// does not count.
	other = memo.BeginProduce(key)
	otherUser := f6CtxWithUser(t, "cyberjoker", []string{"devs"})
	if _, err := Resolve(cache.WithSeedResolveMemo(cache.WithL1KeyContext(otherUser, "w-join-3"), memo), opts); err != nil {
		t.Fatalf("Resolve(other identity): %v", err)
	}
	other.End()
	if got := memo.WouldJoin(); got != 1 {
		t.Fatalf("a miss of another memo key counted a would-join: %d", got)
	}

	// The counter changes nothing served: the would-join miss, the hit and a
	// resolve with no memo at all return the same body.
	fresh := resolveAs("w-join-4", nil)
	assertSemanticallyIdentical883(t, "would-join miss vs no memo", fresh, joined)
	assertSemanticallyIdentical883(t, "hit vs no memo", fresh, hit)
}
