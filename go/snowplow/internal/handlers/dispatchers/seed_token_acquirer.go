package dispatchers

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

// seedTokenAcquirer is the ONE acquisition of the authn-issued loopback token that every bearer-carrying prewarm
// stage asking at the same time shares (#1531 review B2). A stage asks (ask) and waits for it under its OWN context:
// a short-deadline stage never waits past its deadline, and a hung exchange holds nobody but the acquisition.
//
// The acquisition keeps trying until authn answers (the rt8rv boot race, shape A: snowplow up before authn serves),
// with the backoff shape A had (first try at once, then 250 ms doubling to a 30 s cap, ±10 % jitter, 30 tries at
// most per acquisition), and it lasts only while some stage waits for it: when the last waiter's context ends, it
// is cancelled. The backoff's state outlives the acquisition (the failure memo): a stage asking after a failure
// does not try again before the backoff says, so a failing authn is not hit once per ask. A success clears it.
//
// A failure no wait can fix ends the acquisition at once: the projected authn-audience token file missing or
// unreadable (the kubelet mounts it before the container starts; the sandbox has none). Each failed exchange is
// counted (SeedLoopbackTokenErrTotal: per failed exchange, not per asking stage) and logged with its error redacted.
type seedTokenAcquirer struct {
	fn func(context.Context) (string, error)

	mu      sync.Mutex
	cur     *seedTokenAcquisition // the acquisition in flight, nil when none
	delay   time.Duration         // the backoff's wait after the last failure (0: no failure since the last success)
	nextTry time.Time             // the failure memo: no exchange before this
}

type seedTokenAcquisition struct {
	done    chan struct{} // closed with tok/err set
	tok     string
	err     error
	waiters int
	cancel  context.CancelFunc
}

// shape A's backoff (it was installSeedLoopbackToken's wait.Backoff, bounded by a context build's deadline)
const (
	seedTokenFirstDelay = 250 * time.Millisecond
	seedTokenMaxDelay   = 30 * time.Second
	seedTokenJitter     = 0.1
	seedTokenTries      = 30
)

func newSeedTokenAcquirer(fn func(context.Context) (string, error)) *seedTokenAcquirer {
	return &seedTokenAcquirer{fn: fn}
}

// ask returns the token, or the acquisition's error, or ctx's error when ctx ends first (the asker then leaves the
// acquisition, which is cancelled when nobody waits for it any more).
func (a *seedTokenAcquirer) ask(ctx context.Context) (string, error) {
	a.mu.Lock()
	acq := a.cur
	if acq == nil {
		actx, cancel := context.WithCancel(context.Background())
		acq = &seedTokenAcquisition{done: make(chan struct{}), cancel: cancel}
		a.cur = acq
		go a.run(actx, acq)
	}
	acq.waiters++
	a.mu.Unlock()
	select {
	case <-acq.done:
		return acq.tok, acq.err
	case <-ctx.Done():
		a.mu.Lock()
		acq.waiters--
		if acq.waiters == 0 && a.cur == acq {
			a.cur = nil // a later ask starts its own (after the memo), never joins one being cancelled
			acq.cancel()
		}
		a.mu.Unlock()
		return "", ctx.Err()
	}
}

func (a *seedTokenAcquirer) run(actx context.Context, acq *seedTokenAcquisition) {
	finish := func(tok string, err error) {
		a.mu.Lock()
		if a.cur == acq {
			a.cur = nil
		}
		acq.tok, acq.err = tok, err
		a.mu.Unlock()
		acq.cancel()
		close(acq.done)
	}
	var last error
	for try := 0; try < seedTokenTries; try++ {
		a.mu.Lock()
		wait := time.Until(a.nextTry)
		a.mu.Unlock()
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-actx.Done():
				t.Stop()
				finish("", actx.Err())
				return
			}
		}
		tok, err := a.fn(actx)
		if err == nil && tok == "" {
			err = errors.New("authn answered an empty token")
		}
		if err == nil {
			a.mu.Lock()
			a.delay, a.nextTry = 0, time.Time{}
			a.mu.Unlock()
			finish(tok, nil)
			return
		}
		if actx.Err() != nil { // every waiter left: the exchange was cancelled, not answered
			finish("", actx.Err())
			return
		}
		last = err
		terminal := errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission)
		seedLoopbackTokenErrTotal.Add(1)
		slog.Warn("prewarm.seed_loopback_token_unavailable",
			slog.String("subsystem", "cache"),
			slog.String("err", redact.Err(err)),
			slog.Bool("retried", !terminal),
			slog.String("effect", "authn's token exchange failed; the bearer-carrying prewarm stages waiting for it "+
				"keep waiting, each within its own context, while it is tried again (shape A backoff), and a stage "+
				"whose context ends first runs token-less (its nested loopback RA is not warmed this pass). Nothing "+
				"else waits for it (#1530). A missing or unreadable token file is not retried"),
		)
		if terminal {
			finish("", err)
			return
		}
		a.mu.Lock()
		if a.delay == 0 {
			a.delay = seedTokenFirstDelay
		} else if a.delay *= 2; a.delay > seedTokenMaxDelay {
			a.delay = seedTokenMaxDelay
		}
		a.nextTry = time.Now().Add(time.Duration(float64(a.delay) * (1 + seedTokenJitter*(2*rand.Float64()-1))))
		a.mu.Unlock()
	}
	finish("", last)
}
