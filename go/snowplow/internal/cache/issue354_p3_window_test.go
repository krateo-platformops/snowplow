// issue354_p3_window_test.go — #354 P3 cache-package falsifiers (brief: #354
// issuecomment-5990885016). The refresher-loop arms (F6, F6b, F6c, F6e, F6u)
// live in internal/handlers/dispatchers (they need resolveAndPopulateL1);
// D-OTLP lives in internal/metrics.
//
//	F6d  map leak — issue354_p3_f6d_test.go.
//	F6f  B3 key-shape gauges — per identity-bound class, 3 warm page-keyed + 2
//	     warm extras-keyed + 4 warm plain cells + 2 COLD page-keyed cells:
//	     warm_keyed_page_<class> = 3, warm_keyed_extras_<class> = 2 (cold cells
//	     excluded). Read through snowplow_resolved_cache, so on main it compiles
//	     and fails on the absent stats.
//	SS   stale-serve coverage guard (team-lead condition 3) — the note runs
//	     inside the customer Get, the one funnel every hit_total-counted hit
//	     goes through; hit_total is bumped nowhere else. A new customer hit
//	     path either goes through Get (covered by construction) or adds its
//	     own hitTotal bump (this arm fails).

package cache

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func p3CacheEnv(t *testing.T) *ResolvedCacheStore {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	resetResolvedCacheForTest()
	ResetDepsForTest()
	t.Cleanup(func() {
		resetRefresherForTest()
		resetResolvedCacheForTest()
		ResetDepsForTest()
	})
	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)
	return c
}

func p3RefresherStat(name string) int64 {
	switch x := refresherStatsValues()[name].(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

// --- F6f ---------------------------------------------------------------------

func TestIssue354_P3_F6f_B3KeyShapeGaugesCountWarmCellsOnly(t *testing.T) {
	c := p3CacheEnv(t)
	classes := map[string]string{
		"restactions":             "restactions",
		"widgets":                 "widgets",
		CacheEntryClassRAFullList: "ra_full_list",
	}
	n := 0
	put := func(class string, page, perPage int, extras map[string]any, warm bool) {
		n++
		in := ResolvedKeyInputs{CacheEntryClass: class, Group: "g", Version: "v1", Resource: "r",
			Namespace: "ns", Name: fmt.Sprintf("c%d", n), BindingUID: "uid", Page: page, PerPage: perPage, Extras: extras}
		key := ComputeKey(in)
		if !warm {
			// COLD: the fill was read two TTLs ago (a cold fill stamps lastRead at
			// its CreatedAt) and the refresh re-Put below inherits that lastRead.
			c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now().Add(-2 * c.ttl)})
		}
		c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
		if warm {
			if _, ok := c.Get(key); !ok { // a customer read stamps lastRead: WARM
				t.Fatalf("PRE: Put cell not resident")
			}
		}
	}
	for class := range classes {
		for i := 0; i < 3; i++ {
			put(class, 2, 10, nil, true)
		}
		for i := 0; i < 2; i++ {
			put(class, 0, 0, map[string]any{"k": fmt.Sprintf("v%d", i)}, true)
		}
		for i := 0; i < 4; i++ {
			put(class, 0, 0, nil, true)
		}
		for i := 0; i < 2; i++ {
			put(class, 2, 10, nil, false) // cold: never read, not seeded
		}
	}
	c.ReapPastMaxEntryAgeForTest()
	st := ResolvedCacheStatsByStat()
	for _, suffix := range classes {
		if got := st["warm_keyed_page_"+suffix]; got != 3 {
			t.Errorf("F6f: warm_keyed_page_%s = %d, want 3 (cold page-keyed cells excluded)", suffix, got)
		}
		if got := st["warm_keyed_extras_"+suffix]; got != 2 {
			t.Errorf("F6f: warm_keyed_extras_%s = %d, want 2", suffix, got)
		}
	}
	if w := st["warm_lastread"]; w != 27 {
		t.Errorf("PRE: the denominator warm_seeded+warm_lastread must be the 27 warm cells, got lastread=%d", w)
	}
}

// --- SS: stale-serve coverage guard ------------------------------------------

func TestIssue354_P3_StaleServeNoteCoversEveryCountedHit(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var hitBumpFns []string
	getNotes := false
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "hitTotal" && sel.Sel.Name == "Add" {
					hitBumpFns = append(hitBumpFns, f+":"+fd.Name.Name)
				}
				return true
			})
			if fd.Name.Name == "Get" && fd.Recv != nil {
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "noteServeWhileDirty" {
						getNotes = true
					}
					return true
				})
			}
		}
	}
	if len(hitBumpFns) != 1 || !strings.HasSuffix(hitBumpFns[0], ":getCore") {
		t.Fatalf("SS: hit_total must be bumped ONLY in getCore (the customer Get funnel), found %v — a new customer "+
			"hit path must go through Get, which notes a stale serve", hitBumpFns)
	}
	if !getNotes {
		t.Fatalf("SS: ResolvedCacheStore.Get must call noteServeWhileDirty — every counted hit is a candidate stale serve")
	}
}
