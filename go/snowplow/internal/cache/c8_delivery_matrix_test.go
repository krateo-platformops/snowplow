// c8_delivery_matrix_test.go — 1.12.6 C8: the delivery-failure matrix in
// docs/architecture/observability.md cannot rot.
//
// The matrix lists every loss mode from the informer to the SPA with the
// counter that detects it and the arm that pins it, and lists OPEN rows for
// the ones with neither. Its value is that every name in it is real: this
// guard parses the table between the c8-matrix markers and fails when
//
//	- a detector names an expvar key that is not published, or a
//	  `<family>.<stat>` whose stat the family does not derive;
//	- an arm names a Test function that exists in no _test.go under the
//	  module;
//	- a row is neither PINNED nor OPEN, a PINNED row has no arm, or an OPEN
//	  row does not say what is open.
//
// Mutation probe (TestC8_DeliveryFailureMatrix_MutationProbe): a row naming
// a counter and an arm that do not exist must be rejected by the same
// checker, so a passing guard is reading the tree, not the table.

package cache

import (
	"expvar"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	c8Backtick = regexp.MustCompile("`([^`]+)`")
	c8TestFunc = regexp.MustCompile(`(?m)^func (Test\w+)\(`)
)

// c8TestFunctions collects every Test function name under the module root
// (the go/snowplow directory, two levels above this package).
func c8TestFunctions(t *testing.T) map[string]bool {
	t.Helper()
	root := filepath.Join("..", "..")
	names := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "vendor" || d.Name() == "node_modules" || strings.HasPrefix(d.Name(), ".") && d.Name() != "." && d.Name() != ".." {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(p) // #nosec G304 -- walking the repo's own test files
		if rerr != nil {
			return rerr
		}
		for _, m := range c8TestFunc.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if len(names) < 100 {
		t.Fatalf("only %d Test functions found under %s — wrong root?", len(names), root)
	}
	return names
}

// c8FamilyStats maps each map-family expvar key to its derived stat set.
func c8FamilyStats() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	add := func(fam string, m map[string]int64) {
		s := map[string]bool{}
		for k := range m {
			s[k] = true
		}
		out[fam] = s
	}
	add("snowplow_deps", DepsStatsByStat())
	add("snowplow_resolved_cache", ResolvedCacheStatsByStat())
	add("snowplow_crd_discovery", CRDDiscoveryStatsByStat())
	rb := map[string]bool{}
	for k := range RefreshBroadcasterStatsByStat() {
		rb[k] = true
	}
	out["snowplow_refresh_broadcaster"] = rb
	return out
}

// c8DetectorExists resolves one backticked detector name.
func c8DetectorExists(name string, fams map[string]map[string]bool) bool {
	if fam, stat, ok := strings.Cut(name, "."); ok {
		stats, known := fams[fam]
		return known && stats[stat]
	}
	return expvar.Get(name) != nil
}

type c8Row struct {
	id, hop, mode, detector, arm, status string
}

func c8ParseMatrix(t *testing.T, doc string) []c8Row {
	t.Helper()
	i := strings.Index(doc, "<!-- c8-matrix:begin -->")
	j := strings.Index(doc, "<!-- c8-matrix:end -->")
	if i < 0 || j < 0 || j < i {
		t.Fatalf("observability.md has no c8-matrix markers")
	}
	var rows []c8Row
	for _, line := range strings.Split(doc[i:j], "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "| # ") || strings.HasPrefix(line, "|---") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), " | ")
		if len(cells) != 6 {
			t.Fatalf("matrix row has %d cells, want 6: %q", len(cells), line)
		}
		for k := range cells {
			cells[k] = strings.TrimSpace(cells[k])
		}
		rows = append(rows, c8Row{cells[0], cells[1], cells[2], cells[3], cells[4], cells[5]})
	}
	if len(rows) < 15 {
		t.Fatalf("matrix has %d rows; the 1.12.6 table has at least 15", len(rows))
	}
	return rows
}

// c8CheckRows returns every defect found in the rows (empty = clean).
func c8CheckRows(rows []c8Row, tests map[string]bool, fams map[string]map[string]bool) []string {
	var defects []string
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.id] {
			defects = append(defects, fmt.Sprintf("%s: duplicate row id", r.id))
		}
		seen[r.id] = true
		for _, m := range c8Backtick.FindAllStringSubmatch(r.detector, -1) {
			name := m[1]
			if strings.HasPrefix(name, "traffic.") || name == "rateLimit" || strings.HasPrefix(name, "REFRESH_") ||
				strings.HasPrefix(name, "DEPS_") || strings.HasPrefix(name, "RESOLVED_") || strings.HasPrefix(name, "RELIST_") {
				continue // config knobs / gateway fields named in prose, not detectors
			}
			if !c8DetectorExists(name, fams) {
				defects = append(defects, fmt.Sprintf("%s: detector `%s` is not a published expvar key or a derived <family>.<stat>", r.id, name))
			}
		}
		arms := 0
		for _, m := range c8Backtick.FindAllStringSubmatch(r.arm, -1) {
			name := m[1]
			if !strings.HasPrefix(name, "Test") {
				continue
			}
			arms++
			if !tests[name] {
				defects = append(defects, fmt.Sprintf("%s: arm `%s` does not exist in any _test.go", r.id, name))
			}
		}
		switch {
		case strings.HasPrefix(r.status, "PINNED"):
			if arms == 0 {
				defects = append(defects, fmt.Sprintf("%s: PINNED with no arm", r.id))
			}
			if !strings.Contains(r.detector, "`") {
				defects = append(defects, fmt.Sprintf("%s: PINNED with no detector — say which counter moves, or mark it OPEN", r.id))
			}
		case strings.HasPrefix(r.status, "OPEN"):
			if len(strings.TrimSpace(strings.TrimPrefix(r.status, "OPEN"))) < 10 {
				defects = append(defects, fmt.Sprintf("%s: OPEN without saying what is open", r.id))
			}
		default:
			defects = append(defects, fmt.Sprintf("%s: status %q is neither PINNED nor OPEN", r.id, r.status))
		}
	}
	return defects
}

func TestC8_DeliveryFailureMatrix_EveryRowNamesLiveCountersAndArms(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	withLiveResolvedCache(t)
	registerAllExpvarForTest()
	RegisterResolvedCacheExpvarForTest()
	rows := c8ParseMatrix(t, observabilityDoc(t))
	defects := c8CheckRows(rows, c8TestFunctions(t), c8FamilyStats())
	for _, d := range defects {
		t.Error(d)
	}
	open, pinned := 0, 0
	for _, r := range rows {
		if strings.HasPrefix(r.status, "OPEN") {
			open++
		} else {
			pinned++
		}
	}
	t.Logf("matrix: %d rows, %d PINNED, %d OPEN", len(rows), pinned, open)
	if open == 0 {
		t.Errorf("the matrix lists no OPEN row — the gateway hop and the SPA recovery are not pinned by anything in this tree; a table with no OPEN row is a marketing document")
	}
}

func TestC8_DeliveryFailureMatrix_MutationProbe(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	withLiveResolvedCache(t)
	registerAllExpvarForTest()
	tests := c8TestFunctions(t)
	fams := c8FamilyStats()
	bad := []c8Row{
		{"X1", "store", "probe", "`snowplow_deps.no_such_stat_total`", "`TestC8_NoSuchArm`", "PINNED — probe"},
		{"X2", "store", "probe", "`snowplow_no_such_expvar`", "`TestC8_DeliveryFailureMatrix_MutationProbe`", "PINNED — probe"},
		{"X3", "store", "probe", "`snowplow_deps.evict_delete_total`", "", "PINNED — no arm"},
		{"X4", "store", "probe", "`snowplow_deps.evict_delete_total`", "", "OPEN"},
		{"X5", "store", "probe", "", "", "maybe"},
	}
	defects := c8CheckRows(bad, tests, fams)
	want := []string{"X1: detector", "X1: arm", "X2: detector", "X3: PINNED with no arm", "X4: OPEN without", "X5: status"}
	for _, w := range want {
		found := false
		for _, d := range defects {
			if strings.HasPrefix(d, w) {
				found = true
			}
		}
		if !found {
			t.Errorf("the checker did not reject %q (defects: %v) — the guard is not reading the tree", w, defects)
		}
	}
	good := []c8Row{{"G1", "store", "ok", "`snowplow_deps.evict_delete_total`, `snowplow_refresher_dropped_total`", "`TestC8_DeliveryFailureMatrix_MutationProbe`", "PINNED — ok"}}
	if d := c8CheckRows(good, tests, fams); len(d) != 0 {
		t.Errorf("a valid row was rejected: %v", d)
	}
}
