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
// re-resolves it at mark latency.
//
// #408 — BOOT. seedModeBoot is plain only BEFORE /readyz, and even then it gets
// the #375 remark (PutThenRemark): a boot resolve records its deps before its
// Put, so a dep event in between dirty-marks a key that is not resident yet,
// and without the remark the plain Put would store the pre-event body with no
// mark pending. AFTER /readyz (the boot scope keeps seeding the RA content tail
// after the first-nav latch) a boot-mode Put is a PutIfGen like every other
// post-readyz mode.
//
// #258 RESEED MODE — the same guard, one mechanism. The reseed mode is a
// post-readyz seed like keepwarm / gvr-discovered and writes its ONE final
// target cell through this file too; there is no second terminal-Put path:
//
//	mode                  terminal write              why
//	seedModeBoot          PutThenRemark pre-readyz,   #323 / #408
//	                      PutIfGen after /readyz
//	seedModeKeepwarm      PutIfGen(resCtx)            #394 (re-fills a lapsed cell)
//	seedModeGVRDiscovered PutIfGen(resCtx)            #394 (first fills)
//	seedModeRBACShift     PutIfGen(resCtx)            #258: the rotated subject's
//	                                                  new-sub-gen key is ABSENT, so
//	                                                  this is an INSERT with a fresh
//	                                                  BornAt (putPreamble) — no
//	                                                  fresh-mint needed
//
// Every gen-guarded write passes resCtx, so it is under #375's put-then-remark,
// and every write runs after the #424 identity-class guard. NO seed write resets
// BornAt (#378): every mode above inherits it on a resident cell; the only
// re-mint is the refresher terminal's cache.ReplaceIfGenRefresh
// (TestReMint_SingleSetterAudit).
//
// #507 — AND NO SEED WRITE BUT BOOT'S CONFERS WARMTH. The same way BornAt is the
// refresher terminal's to move, SeededAtBoot is the BOOT seed's to stamp: it is
// provenance that also satisfies warmLocked, so a post-readyz mode stamping it
// manufactures warmth on a cell no customer has read. Each mode above therefore
// carries the RESIDENT cell's flag through (seedProvenanceForMode, decided at
// seed entry and carried on the guard); only seedModeBoot writes true.
//
// REFUSAL HANDLING. keepwarm / gvr-discovered / rbacShift take the #394 one-shot
// inline re-seed (reseedAfterTerminalPutRefusal): the retry recaptures the
// generation and PutIfGen can INSERT, so a cell removed mid-resolve is re-filled
// once.
package dispatchers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
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
// The zero value is UNGUARDED (a plain Put plus the #375 remark, PutThenRemark),
// reachable only by a test that drives the restaction tail seam directly.
type seedTerminalGuard struct {
	// guarded selects PutIfGen(gen) over a plain Put.
	guarded bool
	// boot (#408) marks a seedModeBoot unit captured BEFORE /readyz. It decides at
	// the Put: PutIfGen(gen) if /readyz has flipped by then (the boot scope keeps
	// seeding the RA content tail after the first-nav latch, so its Puts can land
	// post-readyz), otherwise a plain Put plus the #375 remark (PutThenRemark).
	boot bool
	// gen is the cell's generation captured at seed entry (CaptureGen).
	gen uint64
	// seededAtBoot (#507) is the ResolvedEntry.SeededAtBoot the terminal write
	// carries: boot-seed PROVENANCE, decided ONCE at seed entry by
	// seedProvenanceForMode. It lives on the guard rather than as a constant at
	// the two seed-cell literals because that flag is ALSO half of THE warm
	// predicate (resolved.go warmLocked = SeededAtBoot || lastRead-within-TTL),
	// so hardcoding it true let an internal seed write manufacture warmth on a
	// cell no customer had ever read. The zero value is false — a test driving
	// the restaction tail seam with the zero guard writes an UNSEEDED cell, which
	// is the conservative direction (a cell can only be colder, never warmer).
	seededAtBoot bool
}

// seedProvenanceForMode is the SeededAtBoot the terminal write carries (#507).
//
// THE DEFECT IT CLOSES. Both seed-cell literals hardcoded `SeededAtBoot: true`,
// mode-INDEPENDENTLY. That field is provenance ("the boot seed warmed this
// cell"), but it is also half of warmLocked, and a keepwarm terminal write is
// accepted on an ALREADY-RESIDENT cell (the sweep re-resolves every
// keepwarm-scoped cell whose body is older than keepwarmAgeSkipThreshold =
// TTL/4). So every sweep pass re-stamped warmth onto cells no customer had ever
// read, and #496's re-mint gate — which refuses to re-mint a COLD in-window cell
// precisely so the reaper's cold-evict can still reclaim it — never fired for
// the keepwarm-scoped set: those cells were re-minted indefinitely and the
// #259/#191 24h bound was gone for them. This is the #376 lesson surviving in a
// flag: internal READS were stopped from faking warmth via lastRead (that is
// what GetNoTouch is for), but an internal WRITE could still confer it.
//
// THE RULE. Only a BOOT-mode seed IS the boot seed, so only it stamps the
// provenance. Every other mode (keepwarm, gvr-discovered, #258 rbac-shift)
// CARRIES THROUGH what the resident cell already holds:
//
//   - a genuinely boot-seeded cell that the sweep re-Puts keeps its attribution,
//     so hits_seed_attributable (l1_lookup_metrics.go) and the warm_seeded gauge
//     (resolved.go reapPastMaxEntryAge) stay honest;
//   - a cell that is NOT boot-seeded — because it never was, or because a
//     refresher re-Put already cleared the flag (putCoreLocked's replace branch
//     inherits BornAt and recentHitters, never this flag) — cannot be made warm
//     by an internal write;
//   - an ABSENT cell carries nothing, so a first fill is false. A keepwarm
//     re-fill of a cell whose TTL lapsed between sweeps, and every
//     gvr-discovered / rbac-shift INSERT, is therefore unseeded, and
//     putCoreLocked's fresh-insert branch stamps lastRead on it (open #494: that
//     branch calls every non-seeded insert a customer cold-fill). That is ONE
//     TTL of lastRead warmth rather than the indefinite seed warmth this flag
//     conferred, so it is strictly narrower than what it replaces — but it is
//     #494's carrier and #507 does not close it.
//
// READ ORDER — GetNoTouch, and BEFORE CaptureGen. GetNoTouch is the #376
// internal read (no MoveToFront, no lastRead stamp, no hit_total, metric-neutral
// on the miss side too), so reading the provenance cannot itself fake the warmth
// this function exists to stop manufacturing. It KEEPS the lazy TTL/maxAge
// evict, which is exactly why it must run BEFORE handle.CaptureGen: an evicting
// read bumps the generation (bumpGenTombstoneLocked), and capturing first would
// make the seed's own PutIfGen refuse itself. Same ordering constraint
// seedTerminalGuardFor already observes with respect to seedSkipDecision.
func seedProvenanceForMode(mode seedScopeMode, handle cacheHandle, key string) bool {
	if mode == seedModeBoot {
		// The boot seed IS the provenance, pre- and post-readyz alike (the boot scope
		// keeps seeding the RA content tail after the first-nav latch). A boot-seeded
		// cell rides seed warmth until real traffic stamps a Get-HIT or a refresher
		// re-Put clears the flag — the design putCoreLocked's insert branch documents.
		return true
	}
	if handle == nil {
		// A nil cacheHandle INTERFACE never reaches a seed site in production
		// (cache-off yields no seed scope at all), and the concrete store's methods
		// are nil-RECEIVER safe but not nil-interface safe (restactions.go:300). Fail
		// to the cold direction rather than panic on a hand-built caller.
		return false
	}
	entry, live := handle.GetNoTouch(key)
	return live && entry != nil && entry.SeededAtBoot
}

// seedTerminalGuardFor captures the terminal-Put guard for one seed unit. Call
// it at seed ENTRY: after seedSkipDecision, before enterSeedUnit and the
// resolve.
//
//   - seedModeBoot BEFORE /readyz: plain, the pre-readyz exemption (#323). A boot
//     re-fill of an LRU-evicted cell must not be over-refused, and no served
//     /call can race a pre-readyz seed. The generation is still captured here,
//     and the Put re-checks cache.IsPhase1Done (#408): see seedTerminalPut.
//   - seedModeBoot AFTER /readyz (#408): guarded. The boot scope keeps seeding
//     the RA content tail after the first-nav latch flips /readyz.
//   - every other mode (seedModeKeepwarm, seedModeGVRDiscovered,
//     seedModeRBACShift, and any mode added later): guarded → PutIfGen. Fail
//     closed — a new mode is post-readyz unless someone argues otherwise here.
func seedTerminalGuardFor(mode seedScopeMode, handle cacheHandle, key string) seedTerminalGuard {
	// #507 — the carried provenance is read FIRST, with GetNoTouch: that read may
	// lazily evict a past-TTL/maxAge cell, which MOVES the generation, so taking it
	// before CaptureGen is what keeps the seed from refusing its own Put. See
	// seedProvenanceForMode.
	seeded := seedProvenanceForMode(mode, handle, key)
	if mode == seedModeBoot && !cache.IsPhase1Done() {
		// boot, captured pre-readyz (#323 exemption, #408): capture the generation
		// NOW, before the resolve, so that a Put landing after /readyz flips can
		// still be gen-guarded against a removal during this resolve. The
		// pre-readyz Put stays plain and gets the #375 remark.
		return seedTerminalGuard{boot: true, gen: handle.CaptureGen(key), seededAtBoot: seeded}
	}
	// Post-readyz: every mode is guarded, including seedModeBoot (#408). The boot
	// scope seeds the RA content tail in the background after the first-nav
	// latch flips /readyz (phase1_walk.go engineSeed select → MarkPhase1Done;
	// prewarm_engine_boot.go RA tail), so a boot-mode Put can race a served /call
	// and a removal exactly like keepwarm.
	return seedTerminalGuard{guarded: true, gen: handle.CaptureGen(key), seededAtBoot: seeded}
}

// seedTerminalPut is the ONLY terminal L1 write of the two seed primitives. It
// returns false iff a guarded Put was refused (the cell was removed after the
// guard was captured). The #394 enumeration guard
// (TestS394_SeedTerminalPutSitesAreGenGuarded) pins that the seed primitives
// write through here and nowhere else.
//
// ctx MUST be the seed's resCtx (built by cache.WithL1KeyContext for key), never
// the outer ctx: it carries the #375 dep-gen sink + startSeq, so an accepted
// guarded Put whose deps moved after the seed started re-marks the key once
// (the refresh-overwrite case above converges fresh). A sink-less ctx would make
// every accepted keepwarm / gvr-discovered seed Put a nil-sink drift
// (unguarded_put_total++ plus a fail-fresh remark = refresher amplification).
func seedTerminalPut(ctx context.Context, handle cacheHandle, key string, entry *cache.ResolvedEntry, g seedTerminalGuard) bool {
	// #424 — the cohort's RBAC class must still be the one key was minted for
	// (a grant/revoke on the representative mid-resolve makes the body another
	// class's). Refused like a generation move: no cell, no dep Record. Applies
	// to boot seeds too — #323's boot exemption is about LRU eviction, not about
	// writing a body into the wrong identity class. Runs BEFORE every write below,
	// the pre-readyz PutThenRemark included (#408).
	// #398 — a seed resolve that read a sensitive resource writes nothing.
	if cache.DeclineSensitivePut(ctx) {
		return false
	}
	if entry != nil {
		if drift := identityClassDriftCtx(ctx, entry.Inputs); drift != "" {
			noteIdentityClassDrift("seed", drift)
			return false
		}
	}
	if g.guarded || (g.boot && cache.IsPhase1Done()) {
		// Post-readyz (any mode, or a boot unit whose Put crossed the /readyz
		// flip): gen-guarded. The IfGen method runs the #375 remark on accept.
		return handle.PutIfGen(ctx, key, entry, g.gen)
	}
	// Pre-readyz boot: plain, so an LRU-evicted cell is never refused (#323), but
	// with the #375 remark (#408). A dep that moved during this resolve dirty-
	// marked a key that was not resident yet, so the refresher skipped it; the
	// remark re-marks it now that it is.
	handle.PutThenRemark(ctx, key, entry)
	return true
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
func reseedAfterTerminalPutRefusal(kind, label string, cohort redact.Label, reseed func() error) error {
	err := reseed()
	if errors.Is(err, errSeedTerminalPutRefused) {
		slog.Default().Info("prewarm.engine.seed.terminal_put_refused_twice",
			slog.String("subsystem", "cache"),
			slog.String("kind", kind),
			slog.String(kind, label),
			slog.String("target", cohort.String()),
			slog.String("effect", "#394 the one-shot re-seed was refused again (the cell was removed during "+
				"the re-seed resolve too); leaving the cell alone — not re-enqueued, not a failure"),
		)
		return nil
	}
	return err
}
