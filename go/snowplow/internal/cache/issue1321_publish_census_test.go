package cache

// issue1321_publish_census_test.go — #1321 C1: the census of every /refreshes
// frame publish site in the production tree.
//
// The defect #1321 fixed was a publish gated on a new GENERATION (the Put was
// accepted), not on the bytes changing. The structural fix is that a
// Put-driven publish takes the Put's PutOutcome (PublishRefreshed, PublishPut,
// publishIfSubscribed) and publishPut fans out only a PutChanged. This census
// keeps it that way:
//
//   - every call of a RAW publisher (PublishRefresh, publishRefresh,
//     fanoutRefresh — no outcome argument) must be a site listed below with
//     the content rule it applies; a new raw call fails here until it is
//     reviewed and listed (or rewritten to take a PutOutcome);
//   - every call of a GATED publisher must pass a real outcome, never a
//     PutChanged / PutIdentical / PutRefused constant: a forged PutChanged is a
//     generation gate in disguise.
//
// PublishEviction (a cell LEFT the store), the #819 rotation frame and the
// owed-push arm emit are not Put-driven and are not refresh publishers here.
//
// C1b runs the same scanner over source probes (a new raw site, a forged
// outcome, an aliased import), so the census is shown to catch what it claims.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// rawPublishers1321 publish a frame with no Put outcome: whoever calls one has
// decided on its own that the bytes changed.
var rawPublishers1321 = map[string]bool{"PublishRefresh": true, "publishRefresh": true, "fanoutRefresh": true}

// gatedPublishers1321 take a PutOutcome, at this argument index.
var gatedPublishers1321 = map[string]int{"PublishRefreshed": 2, "PublishPut": 1, "publishPut": 1, "publishIfSubscribed": 1, "PublishPutIfSubscribed": 1}

// allowedRawSites1321 is every reviewed raw publish site: "<package dir>:
// <enclosing func>-><callee>", with the rule that makes it a change.
var allowedRawSites1321 = map[string]string{
	"cache:PublishRefresh->publishRefresh":   "the exported wrapper itself (its callers are what this census lists)",
	"cache:publishPut->publishRefresh":       "THE gate (put_outcome.go): only a PutChanged reaches here",
	"cache:publishRefresh->fanoutRefresh":    "the broadcaster's own fan-out of a publish that passed the coalescer",
	"cache:windowEnd->fanoutRefresh":         "#484 trailing edge: re-announces a CHANGE the window suppressed",
	"api:finishStaleReplace->PublishRefresh": "D2 replace: withheld when the cell holds exactly the served body (replacedIdentically)",
}

var putOutcomeConsts1321 = map[string]bool{"PutChanged": true, "PutIdentical": true, "PutRefused": true}

func calleeName1321(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

func constName1321(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.ParenExpr:
		return constName1321(x.X)
	}
	return ""
}

// census1321 scans one file of package dir pkg: raw publish sites, gated
// sites, and gated sites that forge their outcome.
func census1321(fset *token.FileSet, f *ast.File, pkg string) (raw, gated, forged []string) {
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := calleeName1321(call)
			site := pkg + ":" + fd.Name.Name + "->" + name
			pos := fset.Position(call.Pos()).String()
			if rawPublishers1321[name] {
				raw = append(raw, site+" @ "+pos)
			}
			if i, ok := gatedPublishers1321[name]; ok && len(call.Args) > i {
				gated = append(gated, site+" @ "+pos)
				if putOutcomeConsts1321[constName1321(call.Args[i])] {
					forged = append(forged, site+" @ "+pos)
				}
			}
			return true
		})
	}
	return raw, gated, forged
}

func siteOf1321(s string) string { return s[:strings.Index(s, " @ ")] }

func TestIssue1321_C1_PublishSiteCensus(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..")) // the module root (go/snowplow)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var raw, gated, forged []string
	files := 0
	err = filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			switch de.Name() {
			case "testdata", "vendor", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		files++
		r, g, fg := census1321(fset, f, filepath.Base(filepath.Dir(path)))
		raw, gated, forged = append(raw, r...), append(gated, g...), append(forged, fg...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var unlisted []string
	seen := map[string]bool{}
	for _, s := range raw {
		site := siteOf1321(s)
		seen[site] = true
		if _, ok := allowedRawSites1321[site]; !ok {
			unlisted = append(unlisted, s)
		}
	}
	if len(unlisted) > 0 {
		t.Fatalf("#1321 C1 RED: %d raw /refreshes publish site(s) not in the reviewed census — a frame published without the "+
			"Put's outcome announces a new GENERATION, not a byte change. Take the PutOutcome (cache.PublishRefreshed / "+
			"cache.PublishPut) or apply the content rule and list the site in allowedRawSites1321:\n  %s",
			len(unlisted), strings.Join(unlisted, "\n  "))
	}
	if len(forged) > 0 {
		t.Fatalf("#1321 C1 RED: %d gated publish site(s) pass a constant outcome — a forged PutChanged is a generation gate:\n  %s",
			len(forged), strings.Join(forged, "\n  "))
	}
	// The census must be looking at the tree: every listed site exists, and the
	// three Put-driven sites are found.
	var stale []string
	for site := range allowedRawSites1321 {
		if !seen[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Fatalf("#1321 C1: listed raw site(s) no longer in the tree (remove them, or the scan is broken): %v", stale)
	}
	want := []string{"dispatchers:resolveAndPopulateL1->PublishRefreshed", "dispatchers:publishIfSubscribed->PublishPutIfSubscribed", "cache:PublishPutIfSubscribed->PublishPut"}
	for _, w := range want {
		found := false
		for _, g := range gated {
			found = found || siteOf1321(g) == w
		}
		if !found {
			t.Fatalf("#1321 C1: the scan of %d files did not find the gated site %s — it is not looking at the production tree (gated: %v)", files, w, gated)
		}
	}
	callers := 0
	for _, g := range gated {
		if strings.HasPrefix(siteOf1321(g), "dispatchers:") && strings.HasSuffix(siteOf1321(g), "->publishIfSubscribed") {
			callers++
		}
	}
	if callers < 2 {
		t.Fatalf("#1321 C1: found %d cold-dispatch callers of publishIfSubscribed, want the restactions and widgets fills (gated: %v)", callers, gated)
	}
	t.Logf("%d files; raw sites %d (all reviewed), gated sites %d (no forged outcome)", files, len(raw), len(gated))
}

// TestIssue1321_C1b_TheCensusCatchesANewGenerationGatedSite — the scanner over
// probes: each RED probe is a shape a future change could take.
func TestIssue1321_C1b_TheCensusCatchesANewGenerationGatedSite(t *testing.T) {
	cases := []struct {
		name, pkg, src string
		unlisted       int
		forged         int
	}{
		{"new_raw_site_after_a_put", "dispatchers", `package dispatchers
import "github.com/krateo-platformops/snowplow/internal/cache"
func newFill(c *cache.ResolvedCacheStore, k string) {
	if c.PutResolvedIfGen(nil, v, k, e, 0) { cache.PublishRefresh(k) }
}`, 1, 0},
		{"aliased_import_raw_site", "handlers", `package handlers
import sc "github.com/krateo-platformops/snowplow/internal/cache"
func f(k string) { sc.PublishRefresh(k) }`, 1, 0},
		{"in_package_raw_site", "cache", `package cache
func newPutter(k string) { publishRefresh(k, refreshOwed{}) }`, 1, 0},
		{"forged_changed", "dispatchers", `package dispatchers
import "github.com/krateo-platformops/snowplow/internal/cache"
func g(k string) { if stored { cache.PublishPut(k, cache.PutChanged) } }`, 0, 1},
		{"forged_in_closure", "dispatchers", `package dispatchers
func h(k string) { go func() { publishIfSubscribed(k, cache.PutChanged) }() }`, 0, 1},
		{"real_outcome", "dispatchers", `package dispatchers
import "github.com/krateo-platformops/snowplow/internal/cache"
func ok(c *cache.ResolvedCacheStore, k string) {
	put := c.PutResolvedIfGenOutcome(nil, v, k, e, 0)
	publishIfSubscribed(k, put)
}`, 0, 0},
		{"reviewed_site", "api", `package api
func finishStaleReplace(k string) { cache.PublishRefresh(k) }`, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, c.name+".go", c.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			raw, _, forged := census1321(fset, f, c.pkg)
			unlisted := 0
			for _, s := range raw {
				if _, ok := allowedRawSites1321[siteOf1321(s)]; !ok {
					unlisted++
				}
			}
			if unlisted != c.unlisted || len(forged) != c.forged {
				t.Fatalf("#1321 C1b RED: unlisted=%d forged=%d (raw %v, forged %v), want %d/%d", unlisted, len(forged),
					raw, forged, c.unlisted, c.forged)
			}
		})
	}
}
