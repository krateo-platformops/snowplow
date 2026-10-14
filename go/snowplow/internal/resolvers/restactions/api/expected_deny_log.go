// expected_deny_log.go — #711: log an expected per-identity deny once per cell
// generation, not on every refresh.
//
// THE DEFECT. A branch C re-gate deny (#677's pre-dial check, or the post-fetch
// re-gate — both return regateDeniedError) is the CORRECT answer for an
// identity that may not read a stage's object. Since #685 the result whose only
// failures are such denials is cached, and the refresher keeps that cell warm
// (#604). Every refresh re-runs the RESTAction, and the stage's deny was logged
// at ERROR "api call response failure" every time: ~4,650 lines/h on 057 for 38
// resident cells, burying the real failures (architect-623 on #711).
//
// WHAT CHANGES. The deny is classified by its TYPE (isRegateDenied) — never by
// a resource, a stage or a RESTAction name. For that class:
//   - the first deny of a (cell, stage path) in the cell's generation logs at
//     WARN; every later one in the same generation logs at DEBUG;
//   - snowplow_internal_dispatch_predial_total{outcome=denied} still counts
//     every occurrence (it is bumped at the deny point, not here);
//   - every other failure keeps logging at ERROR, every time.
//
// WHAT A CELL GENERATION IS. The cell is the L1 key on the resolve's ctx (the
// refresher, the seed and /call all set it). Its generation is one residency of
// that key in the resolved cache: the notes for a key are dropped whenever a
// deny is seen while the key is NOT resident (the cell was evicted, or has not
// been Put yet), so the next resident period logs its first deny again. The key
// itself folds the identity's RBAC class (#424), so an RBAC change that could
// change the verdict is a new key and logs again too.
//
// BOUNDED, NO KNOB. Only keys that saw a deny are held. Inserting a new key
// sweeps the notes of every key that is no longer resident, so the set tracks
// the resident denial-holding cells (38 on 057). With no L1 key or no resolved
// cache there is no cell to deduplicate on: the line logs at WARN each time.
//
// DRY RUNS. An inert (#443) resolve logs the deny at DEBUG and never touches the
// notes, so a draft resolve can neither spend nor reset a cell's WARN.

package api

import (
	"context"
	"log/slog"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// expectedDenyNoted maps an L1 key to the set of stage paths whose expected
// deny was already logged in the key's current generation.
var expectedDenyNoted sync.Map // string -> *sync.Map (string -> struct{})

// expectedDenyResidentFn reports whether an L1 key is resident. A seam only so
// a unit arm can drive generations without a store; production never
// reassigns it.
var expectedDenyResidentFn = func(l1Key string) (resident, tracked bool) {
	store := cache.ResolvedCache()
	if store == nil {
		return false, false
	}
	return store.Has(l1Key), true
}

// expectedDenyLevel returns the level for the failure line of an expected
// per-identity deny of path, resolved under ctx's cell: WARN for the first in
// the cell's generation, DEBUG after.
func expectedDenyLevel(ctx context.Context, path string) slog.Level {
	// #443 — an inert (dry-run) resolve neither reads nor writes the notes: it
	// cannot spend a cell's one WARN, and dry runs can be frequent, so its line
	// is DEBUG.
	if cache.Inert(ctx) {
		return slog.LevelDebug
	}
	l1Key := cache.L1KeyFromContext(ctx)
	if l1Key == "" {
		return slog.LevelWarn
	}
	resident, tracked := expectedDenyResidentFn(l1Key)
	if !tracked {
		return slog.LevelWarn
	}
	if !resident {
		// No cell holds this key now: whatever was noted belonged to an
		// earlier generation. Start this one fresh.
		expectedDenyNoted.Delete(l1Key)
	}
	v, loaded := expectedDenyNoted.LoadOrStore(l1Key, &sync.Map{})
	if !loaded {
		sweepExpectedDenyNoted(l1Key)
	}
	if _, seen := v.(*sync.Map).LoadOrStore(path, struct{}{}); seen {
		return slog.LevelDebug
	}
	return slog.LevelWarn
}

// sweepExpectedDenyNoted drops the notes of every key, other than keep, that is
// no longer resident.
func sweepExpectedDenyNoted(keep string) {
	expectedDenyNoted.Range(func(k, _ any) bool {
		key := k.(string)
		if key == keep {
			return true
		}
		if resident, tracked := expectedDenyResidentFn(key); tracked && !resident {
			expectedDenyNoted.Delete(key)
		}
		return true
	})
}

// resetExpectedDenyNotedForTest clears every note. Tests only.
func resetExpectedDenyNotedForTest() {
	expectedDenyNoted.Range(func(k, _ any) bool {
		expectedDenyNoted.Delete(k)
		return true
	})
}
