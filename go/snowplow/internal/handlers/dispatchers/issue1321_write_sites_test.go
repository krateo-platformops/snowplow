package dispatchers

// issue1321_write_sites_test.go — #1321 review B1 and M2: every write of a cell
// a /refreshes stream can be armed on announces its own change, so withholding
// an identical Put's frame never strands a tab.
//
//	B1   the review's sequence, on the real seed terminal Put and the real
//	     refresh terminal: a dep moved (A → B); the keep-warm sweep reaches the
//	     key first and stores B over the resident, armed cell; the refresher
//	     then resolves B again. A tab holding A must be told exactly once. On
//	     a26cb2b2 the keep-warm write published nothing and the refresher's
//	     identical Put was withheld: 0 frames, the tab kept A until the next
//	     real change. A keep-warm re-Put of the same bytes announces nothing.
//	B1w  the same for the walker's widgetContent shell re-Put (post-readyz).
//	M2   "identical" is the body: a Put of the same body with other stage
//	     outcomes is identical, publishes nothing, and leaves the #1324 digest
//	     unchanged (the only armed client never renders the stage outcomes —
//	     cache/put_outcome.go). A changed body is still a change.
//	C1w  no stream can ever be armed on an apistage key, the one class whose
//	     writes report no outcome.
//
// Publishing is synchronous into the stream's buffered sink (coalesce window
// 0), so "no frame" is read at once after the call returned.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue1321_B1_AKeepwarmSeedThatChangesAnArmedCellAnnouncesItAndTheIdenticalRefreshStaysSilent(t *testing.T) {
	e := newR1321(t, "b1-1321", `{"v":"A"}`) // a tab holds A, armed on the key

	// 1–2. A dep moved; the keep-warm sweep re-resolves the key first and
	// stores B through the exact store call seedTerminalPut makes post-readyz.
	g := seedTerminalGuardFor(seedModeKeepwarm, e.c, e.key)
	if !g.guarded {
		t.Fatal("PRE: a keep-warm seed's terminal Put is not gen-guarded")
	}
	// The seed resolves as its representative (the identity-class guard re-checks
	// it at the Put), on the resolve ctx that carries the #375 sink.
	sctx := cache.WithL1KeyContext(xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "b1-1321-rep"})), e.key)
	if !seedTerminalPut(sctx, e.c, fixtureStoreVerdict, e.key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"B"}`), Inputs: &e.in}, g) {
		t.Fatal("PRE: the keep-warm write was refused")
	}
	if got, _ := e.c.GetNoTouch(e.key); got == nil || string(got.Raw()) != `{"v":"B"}` {
		t.Fatal("PRE: the keep-warm write did not store B over the resident cell")
	}
	seedFrames := e.frames()

	// 3. The refresher dequeues the key (no skip-if-fresh) and resolves B again.
	e.refresh(t, `{"v":"B"}`)
	if got := r1321Puts("identical"); got != 1 {
		t.Fatalf("PRE: put_total{restactions,direct,identical}=%d, want 1 — the refresh Put must be the identical one", got)
	}
	refreshFrames := e.frames()

	if seedFrames+refreshFrames != 1 {
		t.Fatalf("B1 RED: the tab holding A was sent %d frame(s) for the change A → B (keep-warm write %d, refresh %d), "+
			"want exactly 1 — a write that changes an armed cell's bytes must announce it, or the refresher's identical "+
			"Put withholds the only frame that would have told the tab (#1321 review B1)",
			seedFrames+refreshFrames, seedFrames, refreshFrames)
	}
	if seedFrames != 1 || refreshFrames != 0 {
		t.Fatalf("B1 RED: keep-warm write %d frame(s), refresh %d; want 1 and 0 — the change is announced by the write "+
			"that made it, and the identical refresh announces nothing", seedFrames, refreshFrames)
	}

	// A keep-warm re-Put of the bytes the cell already holds announces nothing.
	g = seedTerminalGuardFor(seedModeKeepwarm, e.c, e.key)
	if !seedTerminalPut(sctx, e.c, fixtureStoreVerdict, e.key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"B"}`), Inputs: &e.in}, g) {
		t.Fatal("PRE: the second keep-warm write was refused")
	}
	if n := e.frames(); n != 0 {
		t.Fatalf("B1 RED: a keep-warm re-Put of identical bytes published %d frame(s), want 0", n)
	}
}

// b1wShell is a resolved widget envelope whose one resourcesRefs item carries
// path, so two values of path are two different shells.
func b1wShell(t *testing.T, path string) *unstructured.Unstructured {
	t.Helper()
	res := newUnstructuredWidget("b1w-ns", "b1w")
	if err := setNested(res.Object, []any{
		map[string]any{"id": "i", "path": path, "verb": "GET", "allowed": true},
	}, "status", "resourcesRefs", "items"); err != nil {
		t.Fatal(err)
	}
	return res
}

func TestIssue1321_B1w_TheWalkerShellRePutThatChangesAnArmedShellAnnouncesIt(t *testing.T) {
	cache.CompleteRBACSnapshotForTest(t)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	cache.MarkPhase1Done() // post-readyz: the keep-warm / gvr-discovered branch
	t.Cleanup(cache.ResetPhase1DoneForTest)

	gvr := someWidgetContentGVR()
	in := newUnstructuredWidget("b1w-ns", "b1w")
	key, _ := widgetContentL1Key(gvr, "b1w-ns", "b1w", 5, 1)
	c := cache.ResolvedCache()
	walk := func(path string) {
		t.Helper()
		populateWidgetContentL1(withWidgetContentPutSinks(context.Background()), gvr, in, 5, 1, b1wShell(t, path), c.CaptureGen(key))
	}
	walk("/call?resource=a&apiVersion=v1")
	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatal("PRE: the walker stored no shell")
	}
	st := cache.SubscribeRefreshStream(map[string]struct{}{key: {}}) // a tab holds shell A
	t.Cleanup(st.Unsub)
	frames := func() int {
		n := 0
		for {
			select {
			case <-st.Keys:
				n++
			default:
				return n
			}
		}
	}

	walk("/call?resource=b&apiVersion=v1") // keep-warm re-Put: shell B over the resident, armed A
	if got, _ := c.GetNoTouch(key); got == nil || !strings.Contains(string(got.Raw()), "resource=b") {
		t.Fatal("PRE: the re-Put did not store shell B")
	}
	if n := frames(); n != 1 {
		t.Fatalf("B1w RED: the walker's re-Put of a changed shell over an armed cell published %d frame(s), want exactly 1 "+
			"— otherwise the refresher's later identical Put withholds the only frame (#1321 review B1)", n)
	}
	walk("/call?resource=b&apiVersion=v1") // the same shell again
	if n := frames(); n != 0 {
		t.Fatalf("B1w RED: an identical shell re-Put published %d frame(s), want 0", n)
	}
}

func TestIssue1321_M2_StageOutcomesAreNotPartOfIdentical(t *testing.T) {
	e := newR1321(t, "m2-1321", `{"v":"same"}`)
	ctx := context.Background()
	put := func(body, outcomes string) cache.PutOutcome {
		t.Helper()
		return e.c.PutResolvedIfGenOutcome(ctx, fixtureStoreVerdict, e.key,
			&cache.ResolvedEntry{RawJSON: []byte(body), Inputs: &e.in, StageOutcomes: outcomes}, e.c.CaptureGen(e.key))
	}
	before, _ := e.c.GetNoTouch(e.key)
	digest0 := before.ContentDigest()

	failed := `[{"name":"list","reason":"Forbidden"}]`
	got := put(`{"v":"same"}`, failed)
	publishIfSubscribed(e.key, got)
	if got != cache.PutIdentical {
		t.Fatalf("M2 RED: the same body with other stage outcomes reported %v, want identical — 'identical' is the body "+
			"an armed client renders (cache/put_outcome.go)", got)
	}
	if n := e.frames(); n != 0 {
		t.Fatalf("M2 RED: a stage-outcome-only change published %d frame(s), want 0", n)
	}
	after, _ := e.c.GetNoTouch(e.key)
	if after.StageOutcomes != failed {
		t.Fatal("PRE: the Put did not store the new stage outcomes (the cell must still carry them for the hit header)")
	}
	if after.ContentDigest() != digest0 {
		t.Fatal("M2 RED: the #1324 digest moved on a stage-outcome-only change — the frame rule and the catch-up rule " +
			"must agree on what 'the client holds' means")
	}
	// Scope: the same stream does see a body change, with stage outcomes unchanged.
	if got := put(`{"v":"other"}`, failed); got != cache.PutChanged {
		t.Fatalf("M2 RED: a changed body reported %v, want changed", got)
	} else {
		publishIfSubscribed(e.key, got)
	}
	if n := e.frames(); n != 1 {
		t.Fatalf("M2 RED: a changed body published %d frame(s), want 1", n)
	}
}

func TestIssue1321_C1w_ApistageCellsAreNeverArmable(t *testing.T) {
	// The one write that reports no outcome is PutContentIfGen, which writes
	// only apistage cells. No connection can arm that class …
	if !cache.IsIdentityFreeClass(cache.CacheEntryClassApistage) || identityFreeArmable[cache.CacheEntryClassApistage] {
		t.Fatalf("C1w RED: apistage is armable (identity-free=%v, identityFreeArmable=%v) — then PutContentIfGen writes a "+
			"cell a stream can watch, and it must report its outcome to the publish gate",
			cache.IsIdentityFreeClass(cache.CacheEntryClassApistage), identityFreeArmable[cache.CacheEntryClassApistage])
	}
	if _, _, ok := deriveSubscription(context.Background(), SubscriptionCoordinates{
		Class: cache.CacheEntryClassApistage, Version: "v1", Resource: "configmaps", Namespace: "x", Name: "y",
	}); ok {
		t.Fatal("C1w RED: an apistage coordinate derived a subscription key")
	}
	// … and no response stamps it: the class of every refresh-key stamp in
	// this package is one whose writes report their outcome.
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	stamped := map[string]bool{}
	for _, p := range files {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || !strings.HasPrefix(id.Name, "setRefreshKeyHeader") || len(call.Args) < 3 {
				return true
			}
			lit, ok := call.Args[2].(*ast.BasicLit)
			if !ok {
				// Inside the helpers the class is their parameter; every
				// caller passes a literal.
				if pid, isID := call.Args[2].(*ast.Ident); isID && pid.Name == "class" {
					return true
				}
				t.Fatalf("C1w: a refresh-key stamp at %s passes a non-literal class", fset.Position(call.Pos()))
			}
			s, _ := strconv.Unquote(lit.Value)
			stamped[s] = true
			return true
		})
	}
	var got []string
	for s := range stamped {
		got = append(got, s)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "restactions,widgetContent,widgets" {
		t.Fatalf("C1w RED: the stamped refresh classes are %v, want [restactions widgetContent widgets] — a new armable "+
			"class's writes must report their outcome to the publish gate (cache C1w census)", got)
	}
}
