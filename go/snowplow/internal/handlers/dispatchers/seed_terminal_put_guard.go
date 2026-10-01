// seed_terminal_put_guard.go — #394: generation-guard the TERMINAL resolved-L1
// Put of the two seed primitives (seedOneRestaction's tail and seedOneWidget)
// for the POST-readyz seed modes.
//
// THE DEFECT. Both primitives ended in a plain handle.Put for every seed mode.
// Two modes run after /readyz, while customers are served and the dep tracker /
// TTL / LRU / max-age bounds are removing cells: seedModeKeepwarm (the sweep)
// and seedModeGVRDiscovered (the new-GVR re-walk). A seed that resolves while
// its cell is REMOVED (a DELETE via deleteForDep, or a TTL / LRU / max-age
// eviction through removeElementLocked) then plain-Puts the body it resolved
// BEFORE the removal, resurrecting the removed cell with an older body.
//
// THE FIX (#323 pattern, CaptureGen → PutIfGen). The resolved-L1 generation
// moves ONLY on removal (bumpGenTombstoneLocked, called from both removal
// funnels); Put, PutIfGen and ReplaceIfGen all keep it. So the seed captures
// the cell's generation at seed ENTRY — after the per-mode skip decision (whose
// own Get may lazily evict an expired cell; capturing after it means the seed
// does not refuse itself) and before the memory admission and the resolve —
// and the terminal Put becomes PutIfGen(key, entry, capturedGen). A removal in
// that window tombstones the generation and the Put is REFUSED.
//
// PutIfGen, NOT ReplaceIfGen: keepwarm re-fills cells whose TTL lapsed between
// sweeps and gvr-discovered FIRST-fills cells, so an insert-on-absent must be
// accepted (ReplaceIfGen refuses every first fill — the cold-nav regression of
// reference_putifgen_not_replaceifgen_on_coldfill_carriers).
//
// SCOPE — REMOVAL ONLY. Because the generation does not move on a write, a
// refresher ReplaceIfGen that lands mid-seed is NOT detectable here: the seed's
// PutIfGen is accepted over the refresher's newer body. That refresh-overwrite
// case closes by COMPOSITION with #375 (PR #395): #375's remarkIfDepsMoved runs
// inside the IfGen Put methods, so once this terminal Put is a PutIfGen, a dep
// that moved after the seed started re-marks the key and the refresher
// re-resolves it at mark latency. seedModeBoot stays a plain Put and is
// therefore NOT under #375's IfGen check either (pre-readyz, no customer race).
package dispatchers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// errSeedTerminalPutRefused is returned (wrapped) by a seed primitive whose
// generation-guarded terminal Put was REFUSED because the cell was removed
// during the seed's resolve. It is NOT an operational failure: the removal is
// authoritative and nothing was written (no resolves-counter tick, no
// seeded-set Mark, no dep Record). The engine seed closures consume it with ONE
// inline re-seed in the same post-readyz mode (reseedAfterTerminalPutRefusal);
// it must never reach classifyEngineSeedErr / failedSet, whose
// finalizeBootReEnqueue tail redrives a BOOT scope — a plain-Put scope that
// would reintroduce the overwrite this guard exists to stop.
var errSeedTerminalPutRefused = errors.New("seed terminal Put refused: the cell was removed during the seed resolve (#394)")

// seedTerminalGuard is the per-seed-unit write discipline for the terminal Put.
// The zero value is UNGUARDED (plain Put) — the boot exemption.
type seedTerminalGuard struct {
	// guarded selects PutIfGen(gen) over a plain Put.
	guarded bool
	// gen is the cell's generation captured at seed entry (CaptureGen).
	gen uint64
}

// seedTerminalGuardFor captures the terminal-Put guard for one seed unit. Call
// it at seed ENTRY: after seedSkipDecision, before enterSeedUnit and the
// resolve.
//
//   - seedModeBoot: boot = plain, pre-readyz exemption (#323); not under #375's
//     IfGen check. A boot re-fill of an LRU-evicted cell must not be
//     over-refused, and no served /call can race a pre-readyz seed.
//   - every other mode (seedModeKeepwarm, seedModeGVRDiscovered, and any mode
//     added later): guarded. Fail closed — a new mode is post-readyz unless
//     someone argues otherwise here.
func seedTerminalGuardFor(mode seedScopeMode, handle cacheHandle, key string) seedTerminalGuard {
	if mode == seedModeBoot {
		// boot = plain, pre-readyz exemption (#323); not under #375's IfGen check.
		return seedTerminalGuard{}
	}
	return seedTerminalGuard{guarded: true, gen: handle.CaptureGen(key)}
}

// seedTerminalPut is the ONLY terminal L1 write of the two seed primitives. It
// returns false iff a guarded Put was refused (the cell was removed after the
// guard was captured). The #394 enumeration guard
// (TestS394_SeedTerminalPutSitesAreGenGuarded) pins that the seed primitives
// write through here and nowhere else.
func seedTerminalPut(handle cacheHandle, key string, entry *cache.ResolvedEntry, g seedTerminalGuard) bool {
	if !g.guarded {
		// boot = plain, pre-readyz exemption (#323); not under #375's IfGen check.
		handle.Put(key, entry)
		return true
	}
	return handle.PutIfGen(key, entry, g.gen)
}

// seedTerminalGuardCtxKey carries the captured seedTerminalGuard from
// seedOneRestaction into the reassignable seedRestactionResolveAndPutFn tail on
// resCtx, so the seam's signature (and its test reassignments) stay unchanged.
type seedTerminalGuardCtxKey struct{}

// withSeedTerminalGuard installs g on ctx for the restaction resolve+Put tail.
func withSeedTerminalGuard(ctx context.Context, g seedTerminalGuard) context.Context {
	return context.WithValue(ctx, seedTerminalGuardCtxKey{}, g)
}

// seedTerminalGuardFromContext reads the guard back. Nil-safe: a nil ctx or a
// ctx without the key yields the zero (unguarded) guard. The only production
// caller of the tail (seedOneRestaction) ALWAYS installs the guard, so the
// unguarded default is reachable only by a test that drives the seam directly.
func seedTerminalGuardFromContext(ctx context.Context) seedTerminalGuard {
	if ctx == nil {
		return seedTerminalGuard{}
	}
	g, _ := ctx.Value(seedTerminalGuardCtxKey{}).(seedTerminalGuard)
	return g
}

// logSeedTerminalPutRefused is the Debug line for a refused terminal Put. It
// carries the class + target only (metadata, never the body or the key).
func logSeedTerminalPutRefused(class, target string) {
	slog.Default().Debug("phase1.seed.terminal_put_refused_gen_moved",
		slog.String("subsystem", "cache"),
		slog.String("class", class),
		slog.String("target", target),
		slog.String("effect", "#394 the cell was removed (DELETE / TTL / LRU / max-age) during this post-readyz "+
			"seed resolve; the generation-guarded Put is refused so the pre-removal body is not resurrected"),
	)
}

// reseedAfterTerminalPutRefusal is the ONE-SHOT inline re-seed of a unit whose
// terminal Put was refused. reseed re-runs the SAME primitive in the SAME
// post-readyz mode, so it captures a fresh generation and PutIfGens again. A
// second refusal is logged at Info and swallowed (returns nil): the cell is
// left alone — something keeps removing it, and the customer path / refresher
// own it from here. Any other error from the re-seed is returned unchanged for
// the caller's normal classification. Never routed through failedSet (see
// errSeedTerminalPutRefused).
func reseedAfterTerminalPutRefusal(kind, label, cohort string, reseed func() error) error {
	err := reseed()
	if errors.Is(err, errSeedTerminalPutRefused) {
		slog.Default().Info("prewarm.engine.seed.terminal_put_refused_twice",
			slog.String("subsystem", "cache"),
			slog.String("kind", kind),
			slog.String(kind, label),
			slog.String("target", cohort),
			slog.String("effect", "#394 the one-shot re-seed was refused again (the cell was removed during "+
				"the re-seed resolve too); leaving the cell alone — not re-enqueued, not a failure"),
		)
		return nil
	}
	return err
}
