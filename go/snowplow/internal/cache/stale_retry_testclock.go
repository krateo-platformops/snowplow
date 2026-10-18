package cache

// stale_retry_testclock.go — the #740 engine's manual clock, for arms in this
// package and the dispatchers'. A wake fires only when the test says so, so a
// 60 s backoff is asserted without sleeping. Production MUST NOT use it.

import (
	"sync"
	"time"
)

// StaleRetryManualClock is a StaleRetryClock driven by the test.
type StaleRetryManualClock struct {
	mu   sync.Mutex
	now  time.Time
	pend []*staleRetryManualTimer
}

type staleRetryManualTimer struct {
	c       *StaleRetryManualClock
	d       time.Duration
	f       func()
	stopped bool
}

func (t *staleRetryManualTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

// NewStaleRetryManualClockForTest returns a manual clock at a fixed instant.
func NewStaleRetryManualClockForTest() *StaleRetryManualClock {
	return &StaleRetryManualClock{now: time.Unix(1_700_000_000, 0)}
}

// Now is the clock's current instant.
func (c *StaleRetryManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock by d without firing anything.
func (c *StaleRetryManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// AfterFunc records a pending wake.
func (c *StaleRetryManualClock) AfterFunc(d time.Duration, f func()) StaleRetryTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &staleRetryManualTimer{c: c, d: d, f: f}
	c.pend = append(c.pend, t)
	return t
}

// Pending returns the delays of the pending, unstopped wakes, in order.
func (c *StaleRetryManualClock) Pending() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, t := range c.pend {
		if !t.stopped {
			out = append(out, t.d)
		}
	}
	return out
}

// FireNext fires the oldest pending wake (advancing the clock by its delay) and
// returns its delay; ok=false when nothing is pending. The wake runs on the
// caller's goroutine, attempt included.
func (c *StaleRetryManualClock) FireNext() (time.Duration, bool) {
	c.mu.Lock()
	var w *staleRetryManualTimer
	for _, t := range c.pend {
		if !t.stopped {
			w = t
			break
		}
	}
	if w == nil {
		c.mu.Unlock()
		return 0, false
	}
	w.stopped = true
	c.now = c.now.Add(w.d)
	c.mu.Unlock()
	w.f()
	return w.d, true
}
