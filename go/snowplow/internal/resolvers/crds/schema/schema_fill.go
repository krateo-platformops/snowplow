// schema_fill.go — #1318: a CRD-schema memo miss is refilled by ONE shared,
// detached fill, never under the page budget of the request that missed.
//
// THE DEFECT. The compiled-schema memo (schema_cache.go) is shared server
// state: identity-free cluster metadata, the same for every user. Its refill —
// the CRD GET and the compile — ran on the ctx of whichever request happened to
// miss, so under that request's page budget (#1115). On 057 (1.12.67, during a
// load test) a CRD lifecycle event reset the memo while every caller's 10 s
// budget was already spent on a slow external stage: each miss's CRD GET was
// refused before the wire ("budget ran out before the request was sent"), the
// widget answered 500, and since no request could refill the memo, every later
// request failed the same way. Self-sustaining under load.
//
// THE FIX: the #710 detached shared-fill pattern. A FLIGHT is one refill of one
// memo slot. The request that misses first LEADS it: it runs the read and the
// compile under a DETACHED ctx (pagebudget.Detach: every value, none of its
// cancellation and no customer budget, as every unit that outlives a request
// detaches), bounded by cache.DemandRevalidationDeadline — the server write
// deadline, the longest any customer resolve may run, the same bound a #710
// content-fill leader runs under. Every other request that misses the same slot
// meanwhile JOINS the flight; a joiner's wait is bounded only by its own ctx
// (pagebudget: the budget bounds the network, not a single-flight wait). So a
// request whose budget is spent still gets its schema, and the memo refills
// whatever the budget of the request that happened to miss.
//
// THE FLIGHT KEY folds every input of the fill's result:
//   - the GVR (the memo's own key);
//   - the memo generation the fill snapshots before its read (the architect-A1
//     fence): a request that misses after a CRD lifecycle reset must read the
//     post-reset CRD, so it never joins a flight that may have read the old one;
//   - the transport the CRD is read with (schemaRESTConfig). In production that
//     is always the one process ServiceAccount config, so there is one flight
//     per GVR. Without one (cache off, out of cluster) it is the caller's
//     config, possibly an identity's: a joiner never takes another identity's
//     answer (a 403, say) for its own.
//
// A flight ends with the memo stored (if the fence allows) BEFORE it leaves the
// flight table, so a request that misses after it ended finds the memo, or
// leads a new flight; nothing is served older than its own read would be.
package schema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/slowcall"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// crdSchemaFlightKey is one memo slot's refill: see the file header.
type crdSchemaFlightKey struct {
	gvr runtimeschema.GroupVersionResource
	gen uint64
	rc  *rest.Config
}

// crdSchemaFlight is one in-flight refill. crv and err are written by the
// leader before done is closed and read by joiners only after.
type crdSchemaFlight struct {
	done chan struct{}
	crv  *apiextensions.CustomResourceValidation
	err  error
}

var (
	crdSchemaFlightsMu sync.Mutex
	// crdSchemaFlights is each memo slot's current refill. Bounded by the
	// concurrent refills: a flight leaves it when it ends.
	crdSchemaFlights = map[crdSchemaFlightKey]*crdSchemaFlight{}

	// crdSchemaFills counts refills led; crdSchemaFillJoins counts misses that
	// joined one instead (test observability: the coalescing falsifier).
	crdSchemaFills     atomic.Uint64
	crdSchemaFillJoins atomic.Uint64
)

// errCRDSchemaFillAbandoned is a flight's error when its leader exited without
// a result (a panic in the read or the compile): its joiners fail with it and
// the next miss leads a new flight.
var errCRDSchemaFillAbandoned = errors.New("crd schema fill ended without a result")

// crdNameFor is the CRD object name that defines gvr: <plural>.<group>.
func crdNameFor(gvr runtimeschema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s", gvr.Resource, gvr.Group)
}

// fillCRDSchema returns gvr's compiled schema after a memo miss: it joins the
// slot's current refill, or leads one (see the file header). rc is the
// transport the CRD is read with (schemaRESTConfig's choice).
func fillCRDSchema(ctx context.Context, rc *rest.Config, gvr runtimeschema.GroupVersionResource) (*apiextensions.CustomResourceValidation, error) {
	gen := currentSchemaGen()
	key := crdSchemaFlightKey{gvr: gvr, gen: gen, rc: rc}
	// #1054 — the key a slow-call record names (its redact digest only).
	recKey := fmt.Sprintf("%s\x00%d", gvr.String(), gen)

	crdSchemaFlightsMu.Lock()
	// A flight that ended between the caller's memo lookup and here already
	// stored the schema: do not read the CRD again.
	if v, ok := crdSchemaMemo.Load(gvr); ok {
		crdSchemaFlightsMu.Unlock()
		return v.(*apiextensions.CustomResourceValidation), nil // storeCRDSchema stores only non-nil
	}
	if f := crdSchemaFlights[key]; f != nil {
		crdSchemaFlightsMu.Unlock()
		crdSchemaFillJoins.Add(1)
		return f.wait(ctx, recKey, gvr)
	}
	f := &crdSchemaFlight{done: make(chan struct{})}
	crdSchemaFlights[key] = f
	crdSchemaFlightsMu.Unlock()
	crdSchemaFills.Add(1)

	lead := slowcall.StartLead(ctx, "crd_schema", recKey, gvr.String())
	f.lead(ctx, key)
	lead.End()
	return f.crv, f.err
}

// lead runs the refill under the detached fill ctx, stores the memo under the
// generation fence, and ends the flight. The flight always ends: a leader that
// exits without a result fails its joiners with errCRDSchemaFillAbandoned.
func (f *crdSchemaFlight) lead(ctx context.Context, key crdSchemaFlightKey) {
	finished := false
	defer func() {
		if !finished {
			f.crv, f.err = nil, errCRDSchemaFillAbandoned
		}
		crdSchemaFlightsMu.Lock()
		if crdSchemaFlights[key] == f {
			delete(crdSchemaFlights, key)
		}
		crdSchemaFlightsMu.Unlock()
		close(f.done)
	}()
	fillCtx, cancel := context.WithTimeout(pagebudget.Detach(ctx), cache.DemandRevalidationDeadline())
	defer cancel()
	f.crv, f.err = readCRDSchema(fillCtx, key.rc, key.gvr)
	if f.err == nil {
		storeCRDSchema(key.gvr, f.crv, key.gen)
	}
	finished = true
}

// wait blocks until the flight ends or the joiner's own ctx does.
func (f *crdSchemaFlight) wait(ctx context.Context, recKey string, gvr runtimeschema.GroupVersionResource) (*apiextensions.CustomResourceValidation, error) {
	w := slowcall.StartWait(ctx, slowcall.PhaseFlightWait, "crd_schema", recKey, gvr.String())
	select {
	case <-f.done:
		if f.err != nil {
			w.EndRelease(slowcall.ReleaseLeaderFailed)
			return nil, f.err
		}
		w.End()
		return f.crv, nil
	case <-ctx.Done():
		w.EndRelease(slowcall.CauseOf(ctx.Err()))
		return nil, fmt.Errorf("crd schema %s: %w", crdNameFor(gvr), ctx.Err())
	}
}

// readCRDSchema reads gvr's CRD with rc and compiles its widgetData schema. It
// runs only inside a flight, under the flight's detached ctx.
func readCRDSchema(ctx context.Context, rc *rest.Config, gvr runtimeschema.GroupVersionResource) (*apiextensions.CustomResourceValidation, error) {
	// Ship 0.30.231 (2026-06-01) — inlined CRD GET. The deleted
	// internal/resolvers/crds.Get helper wrapped the same two-line
	// dynamic.NewClient + Get call below; inlining removes the
	// indirection. Earlier (Ship 2) attempted to also skip the
	// mapper build via dynamic.WithSkipMapper, but the contract was
	// unsafe — resourceInterfaceFor (client.go:146) calls
	// uc.mapper.RESTMapping unconditionally; the opts.GVR/opts.GVK
	// branch at line 138 only chooses the source GVK, it does not
	// skip the mapper. WithSkipMapper has been removed.
	//
	// Task #322 (#318-R2) Commit 1 — the per-call dynamic.NewClient
	// here built a FRESH memCacheClient + DeferredDiscoveryRESTMapper
	// every child GET, so the first GVR-only KindFor re-downloaded the
	// full API surface per call (TRACED 2.13% of the 0.30.258 drain
	// profile). SharedSADiscoveryClient returns a process-singleton
	// whose mapper is built ONCE and reused warm; the discovery
	// download is amortised to one boot download. rc here is ALWAYS the
	// SA rest.Config (opts.RC = widgets.go:228 r.saRC on the customer
	// /call path; phase1_walk_pagination_jobs.go:433/:579 saRC on the
	// drain) — the singleton's identity invariant. This is the cached-
	// mapper correction, NOT the WithSkipMapper revival: the mapper is
	// always non-nil and populated (cached_client.go header).
	//
	// FALLBACK — if the singleton errors (nil rc / startup race),
	// fall back to the per-call dynamic.NewClient so the path is never
	// worse than today (project_cache_off_is_transparent_fallback).
	cli, err := dynamic.SharedSADiscoveryClient(rc)
	if err != nil {
		// Fall back LOUDLY: a systemically broken singleton would
		// otherwise silently evaporate the caching win back to
		// per-call discovery downloads.
		slog.WarnContext(ctx, "schema: SA discovery singleton unavailable; "+
			"falling back to per-call client",
			slog.String("error", err.Error()))
		// #1318 — the per-call client serves this flight alone, so every
		// exchange it sends ends with the flight: its mapper's discovery, which
		// client-go sends on its own context.TODO(), included (review F1).
		cli, err = dynamic.NewClient(credhttp.BindContext(ctx, rc))
		if err != nil {
			return nil, err
		}
	}
	crdName := crdNameFor(gvr)
	crdObj, err := cli.Get(ctx, crdName, dynamic.Options{
		GVR:       crdGVR,
		Namespace: "",
	})
	if err != nil {
		return nil, fmt.Errorf("crd schema %s: %w", crdName, err)
	}
	var crd map[string]any
	if crdObj != nil {
		crd = crdObj.UnstructuredContent()
	} else {
		crd = map[string]any{}
	}

	// Compile the CRV (the 0.91% extract + v1->internal build).
	// compileCRDSchemaFn is the package-private seam the RED/GREEN falsifier
	// counts (per-call recompile N -> 1 with the memo); it is initialised once
	// to extractOpenAPISchemaFromCRD and only reassigned by test code (mirrors
	// discoveryClientForConfigFn, dynamic/cached_client.go:176).
	crv, err := compileCRDSchemaFn(crd, gvr.Version)
	if err != nil {
		return nil, fmt.Errorf("compile crd schema %s: %w", crdName, err)
	}
	return crv, nil
}

// resetCRDSchemaFlightsForTest forgets every current flight and the flight
// counters. Test-only.
func resetCRDSchemaFlightsForTest() {
	crdSchemaFlightsMu.Lock()
	crdSchemaFlights = map[crdSchemaFlightKey]*crdSchemaFlight{}
	crdSchemaFlightsMu.Unlock()
	crdSchemaFills.Store(0)
	crdSchemaFillJoins.Store(0)
}
