// memo_would_join_test.go — the resolve memo's measurement-only would-join
// counter through the REAL apiref.Resolve: a Resolve that misses while a
// produce of its memo key is in flight is that produce's joiner; a Resolve
// that misses with none in flight leads, and its joiners count only if the body
// it stores passes the memo's freshness rule when it returns — never when a
// dependency of that body moved after it started. A hit is not a produce, and
// the body Resolve returns is exactly the one it returns with no memo.

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
	memo := cache.NewSeedResolveMemo()
	// Each step resolves its own memo key (another extras value), so each
	// starts with a miss.
	optsFor := func(step string) ResolveOptions {
		return ResolveOptions{ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: raName, Namespace: edge3BackingNS},
			Resource:   "restactions",
			APIVersion: "templates.krateo.io/v1",
		}, Extras: map[string]any{"step": step}}
	}
	keyFor := func(o ResolveOptions) string {
		return memo.Key(edge3BackingNS, raName, user, groups, rbacClassForMemo(user, groups),
			cache.HashExtras(o.Extras), o.PerPage, o.Page)
	}
	resolveAs := func(l1 string, o ResolveOptions, m *cache.SeedResolveMemo) map[string]any {
		t.Helper()
		ctx := cache.WithL1KeyContext(base, l1)
		if m != nil {
			ctx = cache.WithSeedResolveMemo(ctx, m)
		}
		out, err := Resolve(ctx, o)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", l1, err)
		}
		return out
	}
	// whileLeading runs fn once, on Resolve's goroutine, right after Resolve's
	// produce registered: the produce is in flight.
	whileLeading := func(fn func(key string)) {
		done := false
		restore := cache.SetBeginProduceHookForTest(func(key string) {
			if done {
				return // the joiner fn registers itself re-enters here
			}
			done = true
			fn(key)
		})
		t.Cleanup(restore)
	}

	// 1. Resolve JOINS a produce of its key already in flight: it counts once
	// that leader hands over a servable body.
	o1 := optsFor("joiner")
	leader := memo.BeginProduce(keyFor(o1))
	joined := resolveAs("w-join-1", o1, memo)
	if got := memo.WouldJoin(); got != 0 {
		t.Fatalf("counted before the leader handed over: WouldJoin=%d", got)
	}
	leader.Produced(nil, cache.DepGenEpochNow())
	leader.End()
	if got := memo.WouldJoin(); got != 1 {
		t.Fatalf("Resolve as the joiner of a servable leader: WouldJoin=%d, want 1", got)
	}

	// 2. Resolve LEADS, a joiner attaches while it is in flight, nothing
	// moves: Resolve stores a servable body and the joiner counts.
	o2 := optsFor("leader-fresh")
	var joiner *cache.MemoProduce
	whileLeading(func(key string) { joiner = memo.BeginProduce(key) })
	resolveAs("w-join-2", o2, memo)
	joiner.End()
	if got := memo.WouldJoin(); got != 2 {
		t.Fatalf("a joiner of Resolve's servable body: WouldJoin=%d, want 2", got)
	}
	if _, _, _, ok := memo.Load(keyFor(o2)); !ok {
		t.Fatal("setup: Resolve's fresh body is not in the memo")
	}

	// 3. The reviewer's scenario: Resolve LEADS, its RESTAction moves after it
	// started, and a joiner attaches. The memo refuses that body, so a join
	// would have refused it too: the joiner does not count.
	o3 := optsFor("leader-moved")
	whileLeading(func(key string) {
		cache.Deps().OnUpdate(gvr(), edge3BackingNS, raName)
		joiner = memo.BeginProduce(key)
	})
	resolveAs("w-join-3", o3, memo)
	joiner.End()
	if got := memo.WouldJoin(); got != 2 {
		t.Fatalf("a joiner of a body whose dependency moved after the leader started: WouldJoin=%d, want 2", got)
	}
	if _, _, _, ok := memo.Load(keyFor(o3)); ok {
		t.Fatal("setup: the memo served a body whose dependency moved after its stamp")
	}

	// 4. A hit is not a produce: no leader, no joiner.
	before := memo.WouldJoin()
	hit := resolveAs("w-join-4", o2, memo)
	if h, _ := memo.Stats(); h == 0 {
		t.Fatal("setup: the sibling must hit")
	}
	if memo.WouldJoin() != before {
		t.Fatal("a memo hit counted a would-join")
	}

	// The counter changes nothing served: the joiner's body and the hit's body
	// are the ones a resolve with no memo returns.
	assertSemanticallyIdentical883(t, "joiner vs no memo", resolveAs("w-join-5", o1, nil), joined)
	assertSemanticallyIdentical883(t, "hit vs no memo", resolveAs("w-join-6", o2, nil), hit)
}
