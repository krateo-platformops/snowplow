// sa_producer_census_268_267_test.go — #268/#269 Part 2 drift-guard, MANDATORY
// tripwire (architect's form; TL ship-choice: keep the 3-conjunct MustRegateSADial
// + enforce it with this census, so the guard is ENFORCED not TRUSTED).
//
// rbac.MustRegateSADial = BackgroundResolve && saCredentialOnContext && !ServesUnnarrowed.
// Its production-equivalence to the design's 2-conjunct rests on ONE invariant:
//
//	every PRODUCTION context that carries a snowplow SA credential
//	(cache.WithInternalEndpoint / cache.WithInternalRESTConfig) is EITHER
//	ServesUnnarrowed (a genuine SA / identity-free operation) OR a
//	BackgroundResolve (the refresher / prewarm re-resolve).
//
// If a NEW SA-transport producer appears that is neither, the 3-conjunct guard
// silently stops re-gating it (a leak). This census pins the COMPLETE set of
// production call sites that attach an SA credential to a ctx. Any added / removed /
// moved producer trips CI, forcing the author to (1) classify the new context in the
// sa_regate invariant table (sa_regate_invariant_268_267_test.go, this package) and
// (2) update this allowlist.
//
// It is UNTAGGED on purpose — it must run in ordinary CI, not only under the
// falsifier gate, so the protection is permanent. (It lives here, not in package
// rbac, because rbac's TestMain os.Exit(0)s without RBAC_TEST_ALLOW_DESTRUCTIVE, so a
// census there would never run in CI; dispatchers has no TestMain and holds 4 of the
// 5 producers plus the invariant table.)
//
// It is a source scan (not a runtime probe) BECAUSE a runtime counter that reads
// zero when no new producer exists is indistinguishable from one that never fires —
// feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector. A static census
// reads the actual producer set every run.

package dispatchers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// saProducerAllowlist is the COMPLETE, verified set of production call sites that
// attach a snowplow SA credential to a context, keyed by path relative to internal/,
// with the expected number of such calls in that file. Each entry MUST have a
// matching row (ServesUnnarrowed or BackgroundResolve) in the sa_regate invariant
// table.
//
//	cluster_list.go           — populateCtx (async cluster-scope populate): ServesUnnarrowed
//	                            via no-UserInfo (identity-free Class-3 by-name collapse).
//	phase1_walk.go            — withPhase1SAContext:          ServesUnnarrowed (ServeWatcher / canonical SA).
//	phase1_pip_seed.go        — withCohortSeedContext:        ServesUnnarrowed (ServeWatcher); a REAL cohort
//	                            identity, so ServeWatcher is the SOLE unnarrowed mechanism here.
//	phase1_content_prewarm.go — withContentPrewarmSAContext:  ServesUnnarrowed (ServeWatcher / canonical SA).
//	resolve_populate.go       — the refresher rctx: a per-user REPRESENTATIVE identity (!ServesUnnarrowed),
//	                            made safe by WithBackgroundResolve (resolveOnceProd:496). THE load-bearing case.
//
// The func DEFINITIONS live in internal/cache/phase1.go and are NOT call sites (they
// are subtracted below), so cache/phase1.go is deliberately absent.
var saProducerAllowlist = map[string]int{
	"resolvers/restactions/api/cluster_list.go":      2,
	"handlers/dispatchers/phase1_walk.go":            2,
	"handlers/dispatchers/phase1_pip_seed.go":        2,
	"handlers/dispatchers/phase1_content_prewarm.go": 2,
	"handlers/dispatchers/resolve_populate.go":       2,
}

// countSAProducerCalls returns the number of WithInternalEndpoint( +
// WithInternalRESTConfig( CALL sites in src. It strips line comments (so prose
// references such as "see cache.WithInternalEndpoint" are ignored) and subtracts the
// `func WithInternal...(` definitions (so package cache's own declarations are not
// counted). It matches the bare token, so an in-package cache call written without
// the `cache.` qualifier is still counted.
func countSAProducerCalls(src string) int {
	total := 0
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		ep := strings.Count(line, "WithInternalEndpoint(") - strings.Count(line, "func WithInternalEndpoint(")
		rc := strings.Count(line, "WithInternalRESTConfig(") - strings.Count(line, "func WithInternalRESTConfig(")
		total += ep + rc
	}
	return total
}

func TestSAProducerCensus_MatchesAllowlist(t *testing.T) {
	// Tests run with cwd == the package source dir (internal/handlers/dispatchers),
	// so "../.." is internal/. Walk every production .go file under it.
	root := filepath.Join("..", "..")
	if _, err := os.Stat(filepath.Join(root, "rbac", "sa_regate.go")); err != nil {
		t.Fatalf("census cannot locate internal/ from cwd (looked for %s): %v — the drift-guard is not scanning production code",
			filepath.Join(root, "rbac", "sa_regate.go"), err)
	}

	found := map[string]int{}
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
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		n := countSAProducerCalls(string(b))
		if n == 0 {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		found[filepath.ToSlash(rel)] = n
		return nil
	})
	if err != nil {
		t.Fatalf("walking internal/ for SA-transport producers: %v", err)
	}

	// NEW or COUNT-CHANGED producer: a call site not in the allowlist, or a
	// different number of calls than the allowlist records.
	for rel, n := range found {
		want, ok := saProducerAllowlist[rel]
		if !ok {
			t.Errorf("NEW SA-transport producer: %s attaches an SA credential (%d cache.WithInternal* call(s)) "+
				"but is not in saProducerAllowlist.\n"+
				"  Every context that carries an SA credential MUST satisfy MustRegateSADial's invariant "+
				"(ServesUnnarrowed OR BackgroundResolve). Classify this producer in the sa_regate invariant "+
				"table (this package), then add %q: %d here. See internal/rbac/sa_regate.go.", rel, n, rel, n)
			continue
		}
		if n != want {
			t.Errorf("SA-transport producer %s call count changed: found %d, allowlist has %d.\n"+
				"  A changed producer may have altered its ServesUnnarrowed / BackgroundResolve classification. "+
				"Re-verify it against MustRegateSADial's invariant, update the sa_regate invariant table, then "+
				"set %q: %d here.", rel, n, want, rel, n)
		}
	}

	// REMOVED producer: an allowlisted file that no longer attaches an SA
	// credential (moved / deleted). Not a leak, but the allowlist has drifted from
	// reality and must be corrected so a future NEW producer is still caught.
	for rel, want := range saProducerAllowlist {
		if _, ok := found[rel]; !ok {
			t.Errorf("SA-transport producer %s (allowlist expected %d) no longer attaches an SA credential.\n"+
				"  Remove it from saProducerAllowlist and its row from the sa_regate invariant table.",
				rel, want)
		}
	}
}
