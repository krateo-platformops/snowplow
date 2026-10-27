// issue883_memo_encoded_test.go — #883 F4: the resolve memo stores the
// RESTAction's ENCODED result and a hit decodes it once. The cache must not
// constrain JQ, so a decoded hit must be semantically identical to what the miss
// path produces — same Go types, same values, same answer to any jq program.
//
// Both production store routes are driven through the REAL apiref.Resolve:
//   - the page-keyed resolve (restactions.Resolve → Status bytes): the memo
//     stores the very bytes the miss path decoded;
//   - the Ship 4a serve (a Go-slice of the cached full list): storeMemo encodes
//     the served map.
//
// For each: a memo MISS (the producer), a memo HIT (a sibling widget) and a
// resolve with NO memo at all must agree under reflect.DeepEqual (which compares
// dynamic types, so float64-vs-json.Number or a nil-vs-empty map is caught) and
// under a battery of jq programs evaluated over each.

package apiref

import (
	"reflect"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/cache/jqinterest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// richFilter883 is a filter-only RESTAction whose output exercises every JSON
// value class and the encodings that could drift between a decoded tree and its
// re-encoding: a float that is not a short decimal, an integer beyond 2^53, an
// exponent, negative zero, HTML-escaped and line-separator characters, non-ASCII,
// nested arrays, null, and empty object / empty array (an empty map must stay a
// map, not become nil).
const richFilter883 = `{
  "f": 0.1, "third": (1/3), "big": 9007199254740993, "exp": 1e300, "negz": -0,
  "s": "<a&b>   é ✓", "arr": [1, [2, {"k": [3.5]}], null, true, false],
  "n": null, "empty": {}, "earr": [], "nested": {"a": {"b": {"c": "d"}}},
  "items": [range(0; 7) | {"metadata": {"name": ("i-" + tostring)}, "v": (. * 1.5)}]
}`

// jqPrograms883 are evaluated over the hit, the miss and the no-memo body.
var jqPrograms883 = []string{
	`.`,
	`tojson`,
	`[paths]`,
	`.big`, `.f * 3`, `.third`, `.exp`, `.negz`,
	`.s | explode`,
	`.empty | type`, `.earr | length`, `.n == null`,
	`[.items[] | .v] | add`,
	`.items | map(select(.v > 3)) | length`,
	`keys`,
	`.arr | map(type)`,
}

func assertSemanticallyIdentical883(t *testing.T, label string, want, got map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("#883 RED (%s): the memo hit differs from the miss path:\n  miss: %#v\n  hit:  %#v", label, want, got)
	}
	for _, q := range jqPrograms883 {
		a, errA := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: q, Data: want})
		b, errB := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: q, Data: got})
		if (errA != nil) != (errB != nil) || a != b {
			t.Fatalf("#883 RED (%s): jq %q answers differently on the memo hit:\n  miss: %q (%v)\n  hit:  %q (%v)", label, q, a, errA, b, errB)
		}
	}
}

// TestIssue883_MemoHitIsTheMissPath_PageKeyed — the page-keyed route: an
// unpaginated widget resolves the RESTAction for real, and the memo keeps the
// Status bytes that resolve produced.
func TestIssue883_MemoHitIsTheMissPath_PageKeyed(t *testing.T) {
	cacheOn(t)
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	const raName = "ra-883-rich"
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

	base := f6CtxWithUser(t, "admin", []string{"system:masters"})
	opts := ResolveOptions{ApiRef: templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: raName, Namespace: edge3BackingNS},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}} // unpaginated: never the 4a serve, always the page-keyed resolve
	memo := cache.NewSeedResolveMemo()
	resolveAs := func(key string, m *cache.SeedResolveMemo) map[string]any {
		t.Helper()
		ctx := cache.WithL1KeyContext(base, key)
		if m != nil {
			ctx = cache.WithSeedResolveMemo(ctx, m)
		}
		out, err := Resolve(ctx, opts)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", key, err)
		}
		return out
	}

	miss := resolveAs("w-883-1", memo)
	if memo.Entries() != 1 || memo.BodyBytes() == 0 {
		t.Fatalf("the miss must store one encoded body: entries=%d bytes=%d", memo.Entries(), memo.BodyBytes())
	}
	hit := resolveAs("w-883-2", memo)
	if h, _ := memo.Stats(); h != 1 {
		t.Fatalf("the sibling resolve must be a memo HIT (hits=%d)", h)
	}
	fresh := resolveAs("w-883-3", nil)

	if _, isMap := miss["empty"].(map[string]any); !isMap {
		t.Fatalf("harness: the RESTAction did not produce the rich body (empty=%#v)", miss["empty"])
	}
	assertSemanticallyIdentical883(t, "page-keyed hit vs miss", miss, hit)
	assertSemanticallyIdentical883(t, "page-keyed hit vs no memo", fresh, hit)

	// A hit hands out its own tree: mutating it must not reach the next hit.
	hit["s"] = "MUTATED"
	if again := resolveAs("w-883-4", memo); again["s"] == "MUTATED" {
		t.Fatal("#883 RED: a memo hit aliases the tree another hit returned")
	}
}

// TestIssue883_MemoHitIsTheMissPath_FullListServe — the Ship 4a route: the
// served page is a Go-slice of the cached full list, encoded by storeMemo.
func TestIssue883_MemoHitIsTheMissPath_FullListServe(t *testing.T) {
	cacheOn(t)
	e := setup765b(t, "pending", jqinterest.Interest{})
	// Re-pin the full list with every value class richFilter883 covers (the
	// shared fixture's list carries strings only, which would let a number-type
	// drift through): one array to slice, plus a sibling object.
	items := make([]any, 9)
	for i := range items {
		items[i] = map[string]any{
			"metadata": map[string]any{"name": "panel-" + itoa3(i)},
			"v":        float64(i) * 1.5, "third": 1.0 / 3.0, "big": 9007199254740993.0,
			"tags": []any{"<a&b>", " ", "é", nil, true}, "empty": map[string]any{}, "earr": []any{},
		}
	}
	e.store.PutRAFullList(cache.PutGateVerdict(cache.PutGateSinks{}, cache.CacheEntryClassRAFullList), e.raKey, e.raIn, map[string]any{
		"compositionspanels": items,
		"meta":               map[string]any{"f": 0.1, "exp": 1e300, "n": nil, "nested": map[string]any{"a": []any{1.0, 2.5}}},
	})
	memo := cache.NewSeedResolveMemo()
	viaMemo := e.seed(t, memo, "w-883-a", "w-883-b")
	if h, m := memo.Stats(); h != 1 || m != 1 {
		t.Fatalf("want one memo miss (producer) then one hit, got hits=%d misses=%d", h, m)
	}
	noMemo := e.seed(t, nil, "w-883-c")

	miss, hit, fresh := viaMemo["w-883-a"], viaMemo["w-883-b"], noMemo["w-883-c"]
	if _, ok := miss["meta"].(map[string]any); !ok {
		t.Fatalf("harness: the 4a serve did not slice the re-pinned full list: %#v", miss)
	}
	if page, _ := miss["compositionspanels"].([]any); len(page) != 5 {
		t.Fatalf("harness: want a 5-item page of the 9-item list, got %d", len(page))
	}
	assertSemanticallyIdentical883(t, "4a hit vs miss", miss, hit)
	assertSemanticallyIdentical883(t, "4a hit vs no memo", fresh, hit)
}
