package api

// work_origin_stale_replace_1260_test.go — #1260: the D2 roots and the
// cluster-list populate name their work origin, and every detached root in the
// module (workclass.Background, workclass.Detach) is classified.
//
// The D2 debt refill and the dependent replace start from workclass.Background:
// detached work no resolve call site census (#720) sees, since the refill is a
// content fill and the replace calls cache.RunRefreshNow. They start from
// staleReplaceRoot, which stamps stale-replace; the AST half pins that both
// roots do, and the census half makes any NEW workclass.Background or
// workclass.Detach root in the module (the convention for "a detached site
// that would start from context.Background()", #871) be classified with the
// origin it names. The cluster-list cold-miss populate (workclass.Detach) is
// proven on its real goroutine below.
//
// UNTAGGED on purpose, like the #563/#720 censuses.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templatesapi "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1260_StaleReplaceRoot_NamesStaleReplace(t *testing.T) {
	if got, ok := cache.WorkOrigin(staleReplaceRoot()); !ok || got != cache.WorkOriginStaleReplace {
		t.Fatalf("#1260: the D2 root has work origin (%q, %v), want %q", got, ok, cache.WorkOriginStaleReplace)
	}
	// The replace runs the refresher's resolve, which stamps "refresher" on top:
	// the line must still name the replace.
	inner := cache.WithBackgroundResolveOrigin(staleReplaceRoot(), cache.BackgroundOriginRefresher)
	if got, _ := cache.WorkOrigin(inner); got != cache.WorkOriginStaleReplace {
		t.Fatalf("#1260: the refresher resolve under a D2 replace has work origin %q, want %q", got, cache.WorkOriginStaleReplace)
	}
}

// workclassDetachedRoots1260 — every production call of workclass.Background
// or workclass.Detach in the module, keyed "<module-relative file>:<func>",
// with the work origin the root names (for a WithDetachedWork root, the name
// it gets when a request started it). A new detached root fails until it names
// one, is listed, and has an arm in this file.
var workclassDetachedRoots1260 = map[string]string{
	"internal/resolvers/restactions/api/apistage_stale_debt.go:staleReplaceRoot":      cache.WorkOriginStaleReplace,
	"internal/resolvers/restactions/api/cluster_list.go:populateClusterListCellAsync": cache.WorkOriginClusterListPopulate,
}

// staleReplaceRootCallers1260 — the D2 roots that must start from
// staleReplaceRoot.
var staleReplaceRootCallers1260 = []string{"staleDebtRefillOnce", "replaceStaleDependentOnce"}

func TestIssue1260_WorkclassBackgroundRootCensus(t *testing.T) {
	const moduleRoot = "../../../.."
	if _, err := os.Stat(filepath.Join(moduleRoot, "main.go")); err != nil {
		t.Fatalf("census cannot find the module root from cwd: %v — it would scan nothing", err)
	}
	fset := token.NewFileSet()
	found := map[string]bool{}
	callsRoot := map[string]bool{}
	scanned := 0
	err := filepath.WalkDir(moduleRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "testdata", "vendor", "node_modules", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(moduleRoot, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/workclass/") {
			return nil // the definition
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		scanned++
		alias := ""
		for _, im := range f.Imports {
			if strings.Trim(im.Path.Value, `"`) == "github.com/krateo-platformops/snowplow/internal/workclass" {
				alias = "workclass"
				if im.Name != nil {
					alias = im.Name.Name
				}
			}
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if pkg, ok := fun.X.(*ast.Ident); ok && alias != "" && pkg.Name == alias && (fun.Sel.Name == "Background" || fun.Sel.Name == "Detach") {
						found[rel+":"+fd.Name.Name] = true
					}
				case *ast.Ident:
					if fun.Name == "staleReplaceRoot" && strings.HasPrefix(rel, "internal/resolvers/restactions/api/") {
						callsRoot[fd.Name.Name] = true
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if scanned < 200 {
		t.Fatalf("census scanned only %d files — the walk is broken and cannot fail", scanned)
	}
	sites := make([]string, 0, len(found))
	for s := range found {
		sites = append(sites, s)
	}
	sort.Strings(sites)
	closed := map[string]bool{}
	for _, o := range cache.WorkOrigins {
		closed[o] = true
	}
	for _, s := range sites {
		o, ok := workclassDetachedRoots1260[s]
		if !ok {
			t.Errorf("#1260 census: detached root %s starts from workclass.Background or workclass.Detach and is not "+
				"classified — stamp the work origin its driver is (cache/work_origin.go), list it in "+
				"workclassDetachedRoots1260 and prove it with an arm", s)
			continue
		}
		if !closed[o] {
			t.Errorf("#1260 census: %s names %q, outside cache.WorkOrigins", s, o)
		}
	}
	for s := range workclassDetachedRoots1260 {
		if !found[s] {
			t.Errorf("#1260 census: stale entry %s — no such workclass.Background/Detach root any more", s)
		}
	}
	for _, fn := range staleReplaceRootCallers1260 {
		if !callsRoot[fn] {
			t.Errorf("#1260: D2 root %s no longer starts from staleReplaceRoot — its lines would not say stale-replace", fn)
		}
	}
}

// The cluster-list cold-miss populate detaches to a fresh context
// (workclass.Detach), so its lines would carry no work origin at all. Driven
// through the real populateClusterListCellAsync, its goroutine's context names
// the populate when a request started it, and keeps the caller's driver when a
// background driver did — the workclass.Detach rule (#871 E1) for the log.
func TestIssue1260_ClusterListPopulate_NamesItsOrigin(t *testing.T) {
	bg := context.Background()
	cases := []struct {
		name   string
		caller context.Context
		want   string
	}{
		{"started by a request", cache.WithRequestWork(bg), cache.WorkOriginClusterListPopulate},
		{"started by unattributed work", bg, cache.WorkOriginClusterListPopulate},
		{"started by the refresher", cache.WithBackgroundResolveOrigin(bg, cache.BackgroundOriginRefresher), cache.BackgroundOriginRefresher},
		{"started by a prewarm walk", cache.WithPrewarmPath(bg), cache.RefDenialOriginPrewarmPath},
		{"started under a D2 replace", staleReplaceRoot(), cache.WorkOriginStaleReplace},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetClusterListAsyncStateForTest()
			t.Cleanup(ResetClusterListAsyncStateForTest)
			type seen struct {
				origin string
				ok     bool
			}
			got := make(chan seen, 1)
			prev := populateClusterListCellSyncFn
			populateClusterListCellSyncFn = func(ctx context.Context, _ *slog.Logger, _ *templatesapi.API, _ schema.GroupVersionResource, _ string, _ httpcall.RequestOptions, _ *cache.ResolvedCacheStore) bool {
				o, ok := cache.WorkOrigin(ctx)
				got <- seen{o, ok}
				return true
			}
			t.Cleanup(func() { populateClusterListCellSyncFn = prev })

			populateClusterListCellAsync(tc.caller, slog.New(slog.NewTextHandler(io.Discard, nil)),
				&templatesapi.API{Name: "wo1260"}, endpoints.Endpoint{},
				schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "r"},
				"wo1260-cell-"+string(rune('a'+i)), httpcall.RequestOptions{}, cache.ResolvedCache())
			var s seen
			select {
			case s = <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("PRECONDITION: the async populate never ran")
			}
			if !s.ok || s.origin != tc.want {
				t.Fatalf("#1260: the cluster-list populate's ctx has work origin (%q, %v), want %q — its lines "+
					"would not say which driver logged them", s.origin, s.ok, tc.want)
			}
			if !fill710WaitFor(5*time.Second, func() bool { return len(clusterListAsyncSemaphore) == 0 }) {
				t.Fatal("the async populate never released its slot")
			}
		})
	}
}
