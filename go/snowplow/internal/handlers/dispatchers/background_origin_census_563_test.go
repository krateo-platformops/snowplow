// background_origin_census_563_test.go — #563 drift-guard for the denial
// attribution, the static half of a two-part detector.
//
// snowplow_ref_denied_by_origin has a `background-unattributed` cell that is
// supposed to stay 0. On its own that is not a detector: it reads zero both when
// every background producer declares itself AND when the resolver is never
// reached, and it only ever fires after a denial has already been miscounted in
// production (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector).
// This census reads the actual producer set every run instead.
//
// The invariant: every PRODUCTION context marked as a background resolve is
// marked through cache.WithBackgroundResolveOrigin, i.e. it NAMES its driver.
// The bare cache.WithBackgroundResolve stays the marker's definition (and the
// form tests use when the driver is irrelevant), but no production caller may
// use it — that is what keeps the three origins exhaustive and keeps a new
// producer from landing as an anonymous "background".
//
// UNTAGGED on purpose, same as the #268/#269 SA producer census it is modelled
// on: it must run in ordinary CI, not only under a falsifier gate. It lives in
// dispatchers because all three producers do.

package dispatchers

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// backgroundOriginProducers is the COMPLETE, verified set of production call
// sites that mark a ctx as a background resolve, keyed by path relative to
// internal/, with the origin constant each one must name.
//
//	resolve_populate.go       — resolveOnceProd: the refresher's per-user
//	                            representative re-resolve. THE load-bearing stamp
//	                            for rbac.MustRegateSADial (#268/#269 Part 2).
//	phase1_pip_seed.go        — withCohortSeedContext: the per-identity cohort
//	                            seed (stamps WithPrewarmPath too, :1606 — the
//	                            origin is what keeps it out of the prewarm cell).
//	prewarm_engine_boot.go    — rePrewarmBootScoped: the prewarm engine's
//	                            boot-scope walk / re-drive.
//
// A new producer must be added HERE and given a cell in
// cache/background_origin.go, or this census fails.
var backgroundOriginProducers = map[string]string{
	"handlers/dispatchers/resolve_populate.go":    "cache.BackgroundOriginRefresher",
	"handlers/dispatchers/phase1_pip_seed.go":     "cache.BackgroundOriginCohortSeed",
	"handlers/dispatchers/prewarm_engine_boot.go": "cache.BackgroundOriginPrewarmEngineBoot",
}

// stripLineComments removes // comments so prose references ("see
// cache.WithBackgroundResolve") are not counted as call sites.
func stripLineComments(src string) []string {
	lines := strings.Split(src, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		out = append(out, line)
	}
	return out
}

func TestIssue563_BackgroundOriginCensus_EveryProducerNamesItsDriver(t *testing.T) {
	// Tests run with cwd == the package source dir, so "../.." is internal/.
	root := filepath.Join("..", "..")
	// Non-vacuity: the walk must actually be looking at production code. Without
	// this a wrong root yields an empty scan, which would read as "no
	// unattributed producers" — the exact false green this guard exists to stop.
	if _, err := os.Stat(filepath.Join(root, "rbac", "sa_regate.go")); err != nil {
		t.Fatalf("census cannot locate internal/ from cwd (looked for %s): %v — the drift-guard is not scanning production code",
			filepath.Join(root, "rbac", "sa_regate.go"), err)
	}

	originCalls := map[string][]string{} // rel path → origin argument text per call
	var bareCalls []string               // "<rel>:<line>" for every bare marker call
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		// internal/cache owns the marker: its definition, and the origin form
		// that delegates to it.
		if strings.HasPrefix(rel, "cache/") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		scanned++
		for i, line := range stripLineComments(string(b)) {
			if j := strings.Index(line, "WithBackgroundResolveOrigin("); j >= 0 {
				arg := line[j+len("WithBackgroundResolveOrigin("):]
				if k := strings.Index(arg, ")"); k >= 0 {
					arg = arg[:k]
				}
				// "ctx, cache.BackgroundOriginX" → the origin argument.
				parts := strings.Split(arg, ",")
				originCalls[rel] = append(originCalls[rel], strings.TrimSpace(parts[len(parts)-1]))
				continue
			}
			if strings.Contains(line, "WithBackgroundResolve(") {
				bareCalls = append(bareCalls, rel+":"+strconv.Itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if scanned < 100 {
		t.Fatalf("the census scanned only %d production files — the walk is wrong and the arm cannot fail", scanned)
	}

	// (1) No production context may be marked without naming its driver.
	if len(bareCalls) != 0 {
		t.Errorf("#563: %d production call site(s) use the BARE cache.WithBackgroundResolve: %v\n"+
			"  Every production producer must use cache.WithBackgroundResolveOrigin and name its driver, "+
			"or its resourceRef denials land in the background-unattributed cell with nothing to attribute them to. "+
			"Add the origin here and a cell in cache/background_origin.go.", len(bareCalls), bareCalls)
	}

	// (2) The origin-stamping set is exactly the censused one...
	for rel, args := range originCalls {
		want, ok := backgroundOriginProducers[rel]
		if !ok {
			t.Errorf("#563: NEW background-resolve producer %s (origins %v) is not in the census: "+
				"classify it in the sa_regate invariant table (#268/#269) as well, since the marker also "+
				"scopes rbac.MustRegateSADial, then add it here.", rel, args)
			continue
		}
		if len(args) != 1 {
			t.Errorf("#563: %s stamps %d origins %v, want exactly 1 — one root, one driver", rel, len(args), args)
			continue
		}
		if args[0] != want {
			t.Errorf("#563: %s stamps origin %s, want %s — the census pins which driver owns this root",
				rel, args[0], want)
		}
	}
	// ...and nothing censused has disappeared.
	for rel, want := range backgroundOriginProducers {
		if _, ok := originCalls[rel]; !ok {
			t.Errorf("#563: censused producer %s no longer stamps %s. If the stamp moved, move the census row; "+
				"if it was REMOVED, note that dropping it also drops the rbac.MustRegateSADial re-gate for that "+
				"driver (#268/#269 Part 2).", rel, want)
		}
	}
}
