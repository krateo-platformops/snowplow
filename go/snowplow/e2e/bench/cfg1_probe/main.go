// cfg1_probe — minimal standalone binary used by the HG-321 falsifier
// (e2e/bench/cfg1_falsifier.sh) to verify CFG-1 cache-off compliance.
//
// What it does:
//   - Imports internal/cache (triggers the package init() that
//     registers — or skips registering — the five snowplow_* expvar
//     gauges depending on CACHE_ENABLED at process start).
//   - Mounts expvar.Handler() at /debug/vars on a chosen port.
//   - Logs the listen address and blocks until SIGTERM.
//
// This binary is NOT shipped to production; it lives under e2e/bench/
// alongside the falsifier shell script. Its sole purpose is to give
// the falsifier a 4-env-value process-spawn matrix.
//
// Ship CFG-1 / 0.30.163 — see project memory
// `project_cache_off_is_transparent_fallback`.
package main

import (
	"expvar"
	"flag"
	"log"
	"net/http"
	"os"

	// Side-effect imports: trigger the init() of EVERY package that owns a
	// CFG-1-gated expvar publisher, so the falsifier's derived key set is
	// observable from one process. #192 (1.12.6 C0): the structural test
	// (cfg1_structural_test.go) asserts via `go list -deps` that every
	// gated package is in this binary's import graph — adding a gated
	// publisher in a package not listed here fails that test, which is the
	// point: the hand-maintained list below is checked, not trusted.
	_ "github.com/krateo-platformops/snowplow/internal/cache"
	_ "github.com/krateo-platformops/snowplow/internal/dynamic"
	_ "github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	_ "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
)

func main() {
	addr := flag.String("addr", ":18099", "listen address for /debug/vars")
	flag.Parse()

	log.Printf("cfg1_probe: CACHE_ENABLED=%q  addr=%s", os.Getenv("CACHE_ENABLED"), *addr)

	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: *addr, Handler: mux}
	log.Printf("cfg1_probe: listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("cfg1_probe: ListenAndServe: %v", err)
	}
}
