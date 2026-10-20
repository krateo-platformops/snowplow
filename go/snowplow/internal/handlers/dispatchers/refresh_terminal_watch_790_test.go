// refresh_terminal_watch_790_test.go — #790: the refresh-cycle harnesses
// (i187RunRefreshCycle, f608Run) wait for THEIR cell's key to reach the
// refresher's drop point, not for a fixed deadline and not for any drop.
//
// The refresher reports the drop point per key on two WARN records, both
// carrying the L1 key as key_hash: refresher.refresh_dropped (resident,
// known-stale) and refresher.self_object_gone_evicted (a confirmed self-404).
// The watch tees the default slog handler and records those keys, so a drop
// of any OTHER key (a decoy) can never end a wait.
package dispatchers

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// refreshTerminalWatch records the L1 keys the refresher reported at its drop
// point while it was installed.
type refreshTerminalWatch struct {
	next slog.Handler
	mu   *sync.Mutex
	keys map[string]bool
}

// slogBuiltinHandler is the process's built-in default handler. It writes
// through the log package, which slog.SetDefault redirects back into slog, so
// a tee that forwarded to it would loop; the watch forwards to a text handler
// on stderr instead.
var slogBuiltinHandler = slog.Default().Handler()

// watchRefreshTerminal installs the watch as the default slog handler and
// returns it with the function that restores the previous default.
func watchRefreshTerminal() (*refreshTerminalWatch, func()) {
	prev := slog.Default()
	next := prev.Handler()
	if next == slogBuiltinHandler {
		next = slog.NewTextHandler(os.Stderr, nil)
	}
	w := &refreshTerminalWatch{next: next, mu: &sync.Mutex{}, keys: map[string]bool{}}
	slog.SetDefault(slog.New(w))
	return w, func() { slog.SetDefault(prev) }
}

func (w *refreshTerminalWatch) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelWarn || w.next.Enabled(ctx, l)
}

func (w *refreshTerminalWatch) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "refresher.refresh_dropped" || r.Message == "refresher.self_object_gone_evicted" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "key_hash" {
				w.mu.Lock()
				w.keys[a.Value.String()] = true
				w.mu.Unlock()
				return false
			}
			return true
		})
	}
	if !w.next.Enabled(ctx, r.Level) {
		return nil
	}
	return w.next.Handle(ctx, r)
}

func (w *refreshTerminalWatch) WithAttrs(as []slog.Attr) slog.Handler {
	return &refreshTerminalWatch{next: w.next.WithAttrs(as), mu: w.mu, keys: w.keys}
}

func (w *refreshTerminalWatch) WithGroup(g string) slog.Handler {
	return &refreshTerminalWatch{next: w.next.WithGroup(g), mu: w.mu, keys: w.keys}
}

// dropped reports whether key itself reached the drop point.
func (w *refreshTerminalWatch) dropped(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.keys[key]
}

// TestIssue790_RefreshTerminalWatchIgnoresAnotherKeysDrop — the decoy arm. The
// real refresh loop drops a decoy cell (its object GET is a 403: non-retryable,
// dropped on the first attempt). The watch must report that drop for the decoy
// and must NOT report it for a cell that was never refreshed. A watch that
// answered for any drop would let a decoy end a cycle's wait while the cell
// the cycle is about was still in flight.
func TestIssue790_RefreshTerminalWatchIgnoresAnotherKeysDrop(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusForbidden)
	store, inputs, decoyKey, saEP, saRC := i187Fixture(t, srv, true)
	other := inputs
	other.Name = "never-refreshed-790"
	otherKey := cache.ComputeKey(other)

	// The outer watch sees what the cycle's own watch forwards to it.
	w, restore := watchRefreshTerminal()
	defer restore()
	i187RunRefreshCycle(t, inputs, decoyKey, saEP, saRC, store)

	if !w.dropped(decoyKey) {
		t.Fatalf("SETUP: the decoy's 403 never reached the drop point")
	}
	if w.dropped(otherKey) {
		t.Fatalf("#790 RED: the decoy's drop was reported for a cell that was never refreshed — a " +
			"refresh-cycle wait would end on another cell's drop")
	}
}
