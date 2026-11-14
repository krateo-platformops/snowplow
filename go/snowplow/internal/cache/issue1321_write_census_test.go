package cache

// issue1321_write_census_test.go — #1321 C1w: the census of every store WRITE
// site in the production tree (the review's B1).
//
// C1 pins the publish sites: a frame is published only through the Put-outcome
// gate. That is not enough. Withholding the frame of an identical Put is right
// only if the bytes it replaced were announced when they were written, so every
// write that can change the bytes of a cell a /refreshes stream can be armed on
// must hand its PutOutcome to the gate. Before B1 two writes did not (the seed
// terminal Put and the walker's widgetContent shell Put): a keep-warm write
// stored new bytes over a resident, armed cell with no frame, and the
// refresher's later identical Put was withheld, so the tab was never told.
//
// The census, over every non-test .go file of the module:
//
//   - every exported store write method (each Put* / Replace* method of
//     *ResolvedCacheStore, enumerated by reflection, so a new one fails until
//     it is classified here) is either an OUTCOME writer or a no-frame writer;
//   - every production call of an OUTCOME writer passes its result to a gated
//     publisher in the same function: as the outcome argument itself, or
//     through the variable it is assigned to. An outcome computed and dropped
//     is a write that can change bytes without a frame;
//   - every production call of a no-frame writer is a listed site, with the
//     reason no stream can ever be armed on what it writes; the fixture-only
//     writers have no production site at all;
//   - inside this package, the unexported core writers are called only from
//     the store's own write methods (a new in-package writer fails).
//
// C1w-b runs the scanner over source probes, so it is shown to catch the
// shapes it claims to.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// storeWriters1321 classifies every exported store write method: true = it
// reports a PutOutcome, which every production caller must hand to a gated
// publisher; false = it reports only whether it wrote.
var storeWriters1321 = map[string]bool{
	"PutResolvedIfGenOutcome":     true,
	"PutResolvedThenRemark":       true,
	"PutResolvedEndingKnownStale": true,
	"ReplaceResolvedIfGenRefresh": true,
	"PutRAFullListIfGenOutcome":   true,
	// No-frame writers. PutContentIfGen has listed production sites; the rest
	// are fixture-only (no production call may exist).
	"PutContentIfGen":      false,
	"PutResolved":          false,
	"PutResolvedIfGen":     false,
	"ReplaceResolvedIfGen": false,
	"PutRAFullList":        false,
	"PutRAFullListIfGen":   false,
}

// allowedNoFrameWriteSites1321 is every production call of a no-frame writer:
// "<package dir>:<enclosing func>-><method>", with the reason no stream can be
// armed on the cell it writes.
var allowedNoFrameWriteSites1321 = map[string]string{
	"api:apistageContentServeAttempt->PutContentIfGen": "apistage content cell (PutContentIfGen refuses any other class); no stream can arm an apistage key (#449 identityFreeArmable, TestIssue1321_C1w_ApistageCellsAreNeverArmable)",
	"api:populateClusterListCellSync->PutContentIfGen": "apistage content cell (the cluster_list fill); same reason",
}

// coreWriters1321 are the unexported writes behind the exported methods.
var coreWriters1321 = map[string]bool{
	"put": true, "putPlain": true, "putEndingKnownStale": true, "putThenRemark": true,
	"putIfGen": true, "putIfGenOutcome": true, "putIfGenWrite": true, "putRAFullListIfGen": true,
	"replaceIfGen": true, "replaceIfGenRefresh": true, "putCoreLocked": true,
}

// allowedCoreWriteSites1321 is every in-package caller of a core writer: the
// store's own write methods, each the implementation of a classified one.
var allowedCoreWriteSites1321 = map[string]bool{
	"cache:PutResolved->put":                                 true,
	"cache:PutResolvedIfGen->putIfGen":                       true,
	"cache:PutResolvedIfGenOutcome->putIfGenOutcome":         true,
	"cache:PutResolvedThenRemark->putThenRemark":             true,
	"cache:PutResolvedEndingKnownStale->putEndingKnownStale": true,
	"cache:ReplaceResolvedIfGen->replaceIfGen":               true,
	"cache:ReplaceResolvedIfGenRefresh->replaceIfGenRefresh": true,
	"cache:PutContentIfGen->putIfGen":                        true,
	"cache:PutRAFullList->put":                               true,
	"cache:PutRAFullListIfGen->putRAFullListIfGen":           true,
	"cache:PutRAFullListIfGenOutcome->putRAFullListIfGen":    true,
	"cache:putRAFullListIfGen->putIfGenWrite":                true,
	"cache:put->putPlain":                                    true,
	"cache:putEndingKnownStale->putPlain":                    true,
	"cache:putThenRemark->putPlain":                          true,
	"cache:putIfGen->putIfGenWrite":                          true,
	"cache:putIfGenOutcome->putIfGenWrite":                   true,
	"cache:putPlain->putCoreLocked":                          true,
	"cache:putIfGenWrite->putCoreLocked":                     true,
	"cache:replaceIfGen->putCoreLocked":                      true,
	"cache:replaceIfGenRefresh->putCoreLocked":               true,
}

type writeCensus1321 struct {
	unflowed  []string // an outcome writer whose outcome reaches no gated publisher
	noFrame   []string // a no-frame writer's site ("site @ pos")
	core      []string // an in-package core writer's site
	flowed    []string // an outcome writer handing its outcome on
	writerAny int
}

// censusWrites1321 scans one file of package dir pkg.
func censusWrites1321(fset *token.FileSet, f *ast.File, pkg string) writeCensus1321 {
	var out writeCensus1321
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		// What the function hands to a gated publisher: idents and direct calls.
		published := map[string]bool{}
		direct := map[*ast.CallExpr]bool{}
		assigned := map[*ast.CallExpr]string{}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if i, ok := gatedPublishers1321[calleeName1321(x)]; ok && len(x.Args) > i {
					switch a := x.Args[i].(type) {
					case *ast.Ident:
						published[a.Name] = true
					case *ast.CallExpr:
						direct[a] = true
					}
				}
			case *ast.AssignStmt:
				if len(x.Lhs) == len(x.Rhs) {
					for i, r := range x.Rhs {
						if call, ok := r.(*ast.CallExpr); ok {
							if id, ok := x.Lhs[i].(*ast.Ident); ok {
								assigned[call] = id.Name
							}
						}
					}
				}
			case *ast.ValueSpec:
				if len(x.Names) == len(x.Values) {
					for i, v := range x.Values {
						if call, ok := v.(*ast.CallExpr); ok {
							assigned[call] = x.Names[i].Name
						}
					}
				}
			}
			return true
		})
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, isSel := call.Fun.(*ast.SelectorExpr)
			if !isSel {
				return true
			}
			name := sel.Sel.Name
			site := pkg + ":" + fd.Name.Name + "->" + name
			at := site + " @ " + fset.Position(call.Pos()).String()
			if outcome, ok := storeWriters1321[name]; ok {
				out.writerAny++
				switch {
				case !outcome:
					out.noFrame = append(out.noFrame, at)
				case direct[call] || published[assigned[call]] && assigned[call] != "":
					out.flowed = append(out.flowed, at)
				default:
					out.unflowed = append(out.unflowed, at)
				}
			}
			if pkg == "cache" && coreWriters1321[name] {
				out.core = append(out.core, at)
			}
			return true
		})
	}
	return out
}

func TestIssue1321_C1w_EveryWriteSiteHandsItsOutcomeToThePublishGate(t *testing.T) {
	// Every exported write method is classified (a new one fails here).
	var unclassified []string
	rt := reflect.TypeOf(&ResolvedCacheStore{})
	for i := 0; i < rt.NumMethod(); i++ {
		m := rt.Method(i).Name
		if strings.HasPrefix(m, "Put") || strings.HasPrefix(m, "Replace") {
			if _, ok := storeWriters1321[m]; !ok {
				unclassified = append(unclassified, m)
			}
		}
	}
	if len(unclassified) > 0 {
		t.Fatalf("#1321 C1w RED: store write method(s) not classified in storeWriters1321: %v — a write that can change a "+
			"cell's bytes must report its PutOutcome and every caller must hand it to the publish gate, or the method "+
			"is listed as a no-frame writer with the reason no stream can be armed on what it writes", unclassified)
	}
	for m := range storeWriters1321 {
		if _, ok := rt.MethodByName(m); !ok {
			t.Fatalf("#1321 C1w: classified writer %s no longer exists (remove it, or the census is stale)", m)
		}
	}

	root, err := filepath.Abs(filepath.Join("..", "..")) // go/snowplow
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var all writeCensus1321
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
		c := censusWrites1321(fset, f, filepath.Base(filepath.Dir(path)))
		all.unflowed = append(all.unflowed, c.unflowed...)
		all.noFrame = append(all.noFrame, c.noFrame...)
		all.core = append(all.core, c.core...)
		all.flowed = append(all.flowed, c.flowed...)
		all.writerAny += c.writerAny
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.unflowed) > 0 {
		t.Fatalf("#1321 C1w RED: %d write site(s) compute a PutOutcome and hand it to no publisher — a write that "+
			"changes an armed cell's bytes without a frame makes the next identical Put withhold the announcement "+
			"forever. Pass it to publishIfSubscribed / cache.PublishPutIfSubscribed / PublishRefreshed:\n  %s",
			len(all.unflowed), strings.Join(all.unflowed, "\n  "))
	}
	var unlisted []string
	seenNoFrame := map[string]bool{}
	for _, s := range all.noFrame {
		site := siteOf1321(s)
		seenNoFrame[site] = true
		if _, ok := allowedNoFrameWriteSites1321[site]; !ok {
			unlisted = append(unlisted, s)
		}
	}
	if len(unlisted) > 0 {
		t.Fatalf("#1321 C1w RED: %d production call(s) of a write that reports no PutOutcome — use the Outcome "+
			"variant and hand it to the publish gate, or list the site with the reason no stream can be armed on what "+
			"it writes:\n  %s", len(unlisted), strings.Join(unlisted, "\n  "))
	}
	var stale []string
	for site := range allowedNoFrameWriteSites1321 {
		if !seenNoFrame[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Fatalf("#1321 C1w: listed no-frame site(s) no longer in the tree (remove them, or the scan is broken): %v", stale)
	}
	var strayCore []string
	seenCore := map[string]bool{}
	for _, s := range all.core {
		site := siteOf1321(s)
		seenCore[site] = true
		if !allowedCoreWriteSites1321[site] {
			strayCore = append(strayCore, s)
		}
	}
	if len(strayCore) > 0 {
		t.Fatalf("#1321 C1w RED: core store write(s) called outside the store's own write methods — a new in-package "+
			"writer must be an exported, classified method:\n  %s", strings.Join(strayCore, "\n  "))
	}
	for site := range allowedCoreWriteSites1321 {
		if !seenCore[site] {
			t.Fatalf("#1321 C1w: listed core site %s not found (remove it, or the scan is broken)", site)
		}
	}
	// The census must be looking at the tree: the write sites B1 is about, and
	// the ones that were already gated, are each found handing their outcome on.
	for _, w := range []string{
		"dispatchers:resolveAndPopulateL1->ReplaceResolvedIfGenRefresh",
		"dispatchers:seedTerminalPut->PutResolvedIfGenOutcome",
		"dispatchers:seedTerminalPut->PutResolvedThenRemark",
		"dispatchers:populateWidgetContentL1->PutResolvedIfGenOutcome",
		"dispatchers:populateWidgetContentL1->PutResolvedThenRemark",
		"dispatchers:ServeHTTP->PutResolvedEndingKnownStale",
		"apiref:raFullListServe->PutRAFullListIfGenOutcome",
	} {
		found := false
		for _, s := range all.flowed {
			found = found || siteOf1321(s) == w
		}
		if !found {
			t.Fatalf("#1321 C1w: the scan of %d files did not find %s handing its outcome on (flowed: %v)", files, w, all.flowed)
		}
	}
	t.Logf("%d files; %d write calls: %d hand their outcome to the gate, %d listed no-frame, %d in-package core",
		files, all.writerAny, len(all.flowed), len(all.noFrame), len(all.core))
}

// TestIssue1321_C1wb_TheWriteCensusCatchesAnUnannouncedWrite — the scanner over
// probes: each RED probe is a shape a future write could take.
func TestIssue1321_C1wb_TheWriteCensusCatchesAnUnannouncedWrite(t *testing.T) {
	cases := []struct {
		name, pkg, src        string
		unflowed, noFrame, cr int
	}{
		{"outcome_discarded", "dispatchers", `package dispatchers
func f(c *cache.ResolvedCacheStore) { if c.PutResolvedThenRemark(ctx, v, k, e).Stored() { record() } }`, 1, 0, 0},
		{"outcome_assigned_not_published", "dispatchers", `package dispatchers
func f(c cacheHandle) bool { put := c.PutResolvedIfGenOutcome(ctx, v, k, e, g); return put.Stored() }`, 1, 0, 0},
		{"outcome_published_other_var", "dispatchers", `package dispatchers
func f(c cacheHandle, old cache.PutOutcome) { put := c.PutResolvedIfGenOutcome(ctx, v, k, e, g); _ = put; publishIfSubscribed(k, old) }`, 1, 0, 0},
		{"outcome_flows", "dispatchers", `package dispatchers
func f(c cacheHandle) { put := c.PutResolvedIfGenOutcome(ctx, v, k, e, g); publishIfSubscribed(k, put) }`, 0, 0, 0},
		{"outcome_flows_direct", "apiref", `package apiref
func f(c *cache.ResolvedCacheStore) { cache.PublishPutIfSubscribed(k, c.PutRAFullListIfGenOutcome(ctx, v, k, in, full, g)) }`, 0, 0, 0},
		{"outcome_flows_via_branches", "dispatchers", `package dispatchers
func f(c cacheHandle) bool {
	var put cache.PutOutcome
	if guarded { put = c.PutResolvedIfGenOutcome(ctx, v, k, e, g) } else { put = c.PutResolvedThenRemark(ctx, v, k, e) }
	publishIfSubscribed(k, put)
	return put.Stored()
}`, 0, 0, 0},
		{"bool_writer_new_site", "dispatchers", `package dispatchers
func newWarm(c *cache.ResolvedCacheStore) { if c.PutResolvedIfGen(ctx, v, k, e, g) { record() } }`, 0, 1, 0},
		{"core_writer_new_in_package_site", "cache", `package cache
func sneaky(c *ResolvedCacheStore) { c.putPlain(ctx, false, false, k, e) }`, 0, 0, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, c.name+".go", c.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			got := censusWrites1321(fset, f, c.pkg)
			noFrame := 0
			for _, s := range got.noFrame {
				if _, ok := allowedNoFrameWriteSites1321[siteOf1321(s)]; !ok {
					noFrame++
				}
			}
			cr := 0
			for _, s := range got.core {
				if !allowedCoreWriteSites1321[siteOf1321(s)] {
					cr++
				}
			}
			if len(got.unflowed) != c.unflowed || noFrame != c.noFrame || cr != c.cr {
				t.Fatalf("#1321 C1wb RED: unflowed=%d unlisted-no-frame=%d stray-core=%d (%+v), want %d/%d/%d",
					len(got.unflowed), noFrame, cr, got, c.unflowed, c.noFrame, c.cr)
			}
		})
	}
}
