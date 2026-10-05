package dispatchers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// reseed_strategy_test.go — #258 reseed put-strategy arms at the dispatcher level
// (the seedOneWidget terminal Put) plus the gen-race / single-setter guards. The
// store-level mechanism (only ReplaceIfGenRefresh resets, every other write
// inherits) is proven by cache.TestIssue378_F2c_OnlyTheRefresherTerminalReMints;
// these arms prove the WIRING: that seedOneWidget routes its terminal Put through
// the ONE #394 mechanism (seedTerminalGuardFor/seedTerminalPut), that no seed mode
// resets BornAt (#378), and the refusal policy.

// reseedWidgetEntry / reseedSeedCtx — the shared widget-seed fixture (apiRef'd
// widget under a co-bound cohort), matching a1_uaf_seed_ctx_propagation_test.go.
func reseedWidgetEntry() navWidgetEntry {
	return navWidgetEntry{
		W:          h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}),
		GVR:        h1WidgetGVR,
		PerPage:    -1,
		Page:       -1,
		KeyPerPage: -1,
		KeyPage:    -1,
	}
}

func reseedSeedCtx() context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: a1Alice, Groups: []string{a1Group}}, endpoints.Endpoint{}, nil)
}

// stubWidgetResolve installs a trivial (non-nested) widget resolver so the seed
// reaches its terminal Put deterministically; the strategy arms are about that
// terminal Put, not the resolve body.
func stubWidgetResolve(t *testing.T) {
	t.Helper()
	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(_ context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		return h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}), nil
	}
}

func reseedWidgetKey(t *testing.T, ctx context.Context, e navWidgetEntry) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	t.Helper()
	key, handle, inputs := dispatchCacheLookupKey(ctx, "widgets",
		e.GVR.Group, e.GVR.Version, e.GVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, e.W.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("precondition: a live widgets key with non-empty BindingUID; key=%q handle=%v inputs=%+v",
			key, handle != nil, inputs)
	}
	return key, handle, inputs
}

// TestSeedOneWidget_GVRDiscovered_GuardedReplaceInheritsBornAt — TL condition 2. A
// non-re-mint seed mode (gvr-discovered: never skips, #394 PutIfGen) re-Puts an existing cell and
// INHERITS BornAt — it must NEVER reset the max-age clock. This is the restriction
// proof's dispatcher half: no seed mode resets (#378: only the refresher terminal).
func TestSeedOneWidget_GVRDiscovered_GuardedReplaceInheritsBornAt(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)
	e := reseedWidgetEntry()
	ctx := reseedSeedCtx()
	key, handle, inputs := reseedWidgetKey(t, ctx, e)

	past := time.Now().Add(-90 * time.Minute)
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"seed":1}`), Inputs: inputs, BornAt: past, CreatedAt: time.Now()})

	if err := seedOneWidget(ctx, e, h1NS, seedModeGVRDiscovered); err != nil {
		t.Fatalf("seedOneWidget(seedModeGVRDiscovered): %v", err)
	}
	got, ok := handle.Get(key)
	if !ok {
		t.Fatal("gvr-discovered re-Put must leave the cell present")
	}
	if !got.BornAt.Equal(past) {
		t.Fatalf("a plain seed re-Put must INHERIT BornAt (never reset the max-age clock); got %v want %v",
			got.BornAt, past)
	}
}

// TestSeedOneWidget_RBACShift_InsertsAbsentKeyFresh — #258. seedModeRBACShift
// mints the rotated subject's NEW-sub-gen key, which is ABSENT, as a gen-guarded
// INSERT with a fresh BornAt. The cell must come into being warm.
func TestSeedOneWidget_RBACShift_InsertsAbsentKeyFresh(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)
	e := reseedWidgetEntry()
	ctx := reseedSeedCtx()
	key, handle, _ := reseedWidgetKey(t, ctx, e)

	if _, ok := handle.Get(key); ok {
		t.Fatal("precondition: the new-sub-gen key must be ABSENT before the reseed")
	}
	before := time.Now()
	if err := seedOneWidget(ctx, e, h1NS, seedModeRBACShift); err != nil {
		t.Fatalf("seedOneWidget(seedModeRBACShift): %v", err)
	}
	got, ok := handle.Get(key)
	if !ok {
		t.Fatal("#258 reseed must INSERT the absent new-sub-gen cell")
	}
	if got.BornAt.Before(before) {
		t.Fatalf("#258 INSERT must have a fresh BornAt (>= reseed start); got %v start %v", got.BornAt, before)
	}
}

// TestSeedTerminalPut_ReseedModes_RefusedOnRemoval — the reseed modes' terminal
// write is the #394 mechanism: a removal (DELETE) between seedTerminalGuardFor's
// capture and seedTerminalPut REFUSES the write for the reseed mode (rbacShift →
// PutIfGen) and writes nothing (anti-resurrection).
// CONTROL: boot stays a plain Put and writes.
func TestSeedTerminalPut_ReseedModes_RefusedOnRemoval(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("precondition: a live resolved cache")
	}
	for _, mode := range []seedScopeMode{seedModeRBACShift} {
		t.Run(mode.String(), func(t *testing.T) {
			k := "reseed-genrace-probe-" + mode.String()
			store.Put(k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":1}`)})
			g := seedTerminalGuardFor(mode, store, k)
			if !g.guarded {
				t.Fatalf("%s: guard = %+v, want guarded", mode, g)
			}
			store.DeleteForTest(k) // a removal lands after capture → the generation moves
			if seedTerminalPut(context.Background(), store, k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":2}`)}, g) {
				t.Fatalf("%s: a gen-moved terminal Put must be REFUSED", mode)
			}
			if got, ok := store.Get(k); ok && string(got.RawJSON) == `{"v":2}` {
				t.Fatalf("%s: a refused Put must not have written the body (anti-resurrection)", mode)
			}
		})
	}
	const pk = "reseed-plain-probe"
	if !seedTerminalPut(context.Background(), store, pk, &cache.ResolvedEntry{RawJSON: []byte(`{"v":1}`)},
		seedTerminalGuardFor(seedModeBoot, store, pk)) {
		t.Fatal("boot (plain) must never refuse")
	}
	if _, ok := store.Get(pk); !ok {
		t.Fatal("boot (plain) must write the cell")
	}
}

// TestReseedRefusalPolicy_PerMode — the #394 one-shot re-seed applies to the
// rbacShift mode (a removal mid-resolve is re-filled ONCE, a second refusal is
// swallowed, never re-enqueued). The refusal never surfaces as an error (no
// failure classification, no re-arm of the whole rotated set). RED if the
// rbacShift retry is removed (calls=1).
func TestReseedRefusalPolicy_PerMode(t *testing.T) {
	orig := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = orig })
	req := reseedRequest{identity: seedTarget{Username: a1Alice}, isWidget: true, widget: reseedWidgetEntry()}

	cases := []struct {
		mode      seedScopeMode
		wantCalls int
	}{
		{seedModeRBACShift, 2}, // one-shot inline re-seed (#394)
	}
	for _, tc := range cases {
		t.Run(tc.mode.String(), func(t *testing.T) {
			calls := 0
			seedOneWidgetFn = func(_ context.Context, _ navWidgetEntry, _ string, m seedScopeMode) error {
				calls++
				if m != tc.mode {
					t.Errorf("re-seed must run in the SAME mode %s, got %s", tc.mode, m)
				}
				return fmt.Errorf("widget x/y: %w", errSeedTerminalPutRefused)
			}
			if err := reseedWithRefusalPolicy(context.Background(), rePrewarmDeps{}, req, tc.mode); err != nil {
				t.Fatalf("%s: a refused terminal Put must be swallowed (nil), got %v", tc.mode, err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("%s: seed primitive called %d times, want %d", tc.mode, calls, tc.wantCalls)
			}
		})
	}

	// rbacShift: a refusal followed by an accepted re-seed is a success (2 calls).
	calls := 0
	seedOneWidgetFn = func(context.Context, navWidgetEntry, string, seedScopeMode) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("widget x/y: %w", errSeedTerminalPutRefused)
		}
		return nil
	}
	if err := reseedWithRefusalPolicy(context.Background(), rePrewarmDeps{}, req, seedModeRBACShift); err != nil || calls != 2 {
		t.Fatalf("refused-then-accepted: err=%v calls=%d, want nil/2", err, calls)
	}
	// reseedTargets never returns a refused target for re-enqueue.
	calls = 0
	seedOneWidgetFn = func(context.Context, navWidgetEntry, string, seedScopeMode) error {
		calls++
		return fmt.Errorf("widget x/y: %w", errSeedTerminalPutRefused)
	}
	if re := reseedTargets(context.Background(), rePrewarmDeps{}, []reseedRequest{req, req}); len(re) != 0 || calls != 4 {
		t.Fatalf("reseedTargets: re-enqueue=%d calls=%d, want 0/4 (one-shot per target, no re-arm)", len(re), calls)
	}
}

// TestReMint_SingleSetterAudit — #378 (re-pointed from the retired seed carrier):
// the BornAt-resetting write ReplaceIfGenRefresh has EXACTLY ONE production
// (non-test) caller in the module — the refresher terminal in resolveAndPopulateL1
// (resolve_populate.go). Inside the store, putCoreLocked is called with a literal
// `false` freshMint everywhere except that one method, and the retired
// ReplaceIfGenReMint is gone. A second caller, or a second freshMint=true path,
// would let a keepwarm / seed / customer write extend the C5 cap.
func TestReMint_SingleSetterAudit(t *testing.T) {
	root := filepath.Join("..", "..", "..") // go/snowplow module root
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("VACUOUS GUARD: module root %s has no go.mod: %v", root, err)
	}
	callRe := regexp.MustCompile(`\.ReplaceIfGenRefresh\(`)
	retiredRe := regexp.MustCompile(`ReplaceIfGenReMint|seedModeReMint|reseedFromInputs`)
	coreRe := regexp.MustCompile(`c\.putCoreLocked\([^\n]*, ([A-Za-z]+)\)`)
	callers := map[string]int{}
	var retired []string
	freshArgs := map[string]int{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if n := len(callRe.FindAll(b, -1)); n > 0 {
			callers[filepath.ToSlash(path)] += n
		}
		if retiredRe.Match(b) {
			for _, line := range strings.Split(string(b), "\n") {
				// Comments may name the retired carrier (history); code may not.
				if retiredRe.MatchString(line) && !strings.HasPrefix(strings.TrimSpace(line), "//") {
					retired = append(retired, filepath.ToSlash(path)+": "+strings.TrimSpace(line))
				}
			}
		}
		for _, m := range coreRe.FindAllSubmatch(b, -1) {
			freshArgs[string(m[1])]++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	total := 0
	for _, n := range callers {
		total += n
	}
	want := filepath.ToSlash(filepath.Join(root, "internal", "handlers", "dispatchers", "resolve_populate.go"))
	if total != 1 || callers[want] != 1 {
		t.Fatalf("ReplaceIfGenRefresh must have EXACTLY ONE production caller (the refresher terminal in %s); "+
			"found %d call(s) across %v. A second caller lets a non-refresher write reset BornAt.", want, total, callers)
	}
	if len(retired) != 0 {
		t.Fatalf("the retired seed-path re-mint is referenced by production code: %v", retired)
	}
	if freshArgs["freshMint"] != 1 || len(freshArgs) != 2 || freshArgs["false"] == 0 {
		t.Fatalf("putCoreLocked's freshMint must be a literal false at every call site except ReplaceIfGenRefresh's "+
			"computed freshMint; found %v", freshArgs)
	}
}
