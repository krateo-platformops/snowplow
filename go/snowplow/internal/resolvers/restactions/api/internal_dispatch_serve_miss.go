package api

// internal_dispatch_serve_miss.go — #796: why an internal-dispatch LIST was not
// served from its informer, and how long the bounded sync-wait cost before it
// fell through to the live paged LIST. The INFO serve_miss line already names
// the conjuncts but is not shipped at LOG_LEVEL=warn, so production frequency
// was unreadable; these counters make it readable (metadata only: a closed
// reason enum, never a GVR).

import (
	"expvar"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// --- snowplow_internal_dispatch_informer_serve_miss_total{reason} ------------

const (
	serveMissUnregistered    = "unregistered"     // no informer for the GVR
	serveMissListRefused     = "list_refused"     // unsynced; the reflector's LIST was refused (404/403)
	serveMissSyncTimeout     = "sync_timeout"     // unsynced; no terminal refusal; the wait ran out
	serveMissWatchBroken     = "watch_broken"     // synced, but conjunct 3 (watch healthy) failed
	serveMissTypeUnconfirmed = "type_unconfirmed" // synced, watch healthy, conjunct 4 failed
	serveMissNotServable     = "not_servable"     // the conjuncts held, the serve read still declined
)

// ServeMissReasons is the closed reason set, in emit order.
var ServeMissReasons = []string{
	serveMissUnregistered, serveMissListRefused, serveMissSyncTimeout,
	serveMissWatchBroken, serveMissTypeUnconfirmed, serveMissNotServable,
}

var (
	serveMissCounts    [6]atomic.Uint64
	serveSyncWaitMsSum atomic.Uint64
)

// serveMissReason classifies a miss from the servability snapshot taken after
// the wait. Pure, so the mapping is pinned by its own table test.
func serveMissReason(s cache.ServabilitySnapshot) string {
	switch {
	case !s.Registered:
		return serveMissUnregistered
	case !s.HasSynced && s.ListRefused:
		return serveMissListRefused
	case !s.HasSynced:
		return serveMissSyncTimeout
	case !s.WatchHealthy:
		return serveMissWatchBroken
	case !s.TypeConfirmed:
		return serveMissTypeUnconfirmed
	}
	return serveMissNotServable
}

func recordServeMiss(reason string) {
	for i, r := range ServeMissReasons {
		if r == reason {
			serveMissCounts[i].Add(1)
			return
		}
	}
}

// recordServeSyncWait adds one bounded sync-wait's duration (served or not).
func recordServeSyncWait(d time.Duration) {
	if ms := d.Milliseconds(); ms > 0 {
		serveSyncWaitMsSum.Add(uint64(ms))
	}
}

// ServeMissCount is one {reason} cell.
type ServeMissCount struct {
	Reason string
	Count  uint64
}

// ServeMissSnapshot returns every reason cell, zeros included.
func ServeMissSnapshot() []ServeMissCount {
	out := make([]ServeMissCount, len(ServeMissReasons))
	for i, r := range ServeMissReasons {
		out[i] = ServeMissCount{Reason: r, Count: serveMissCounts[i].Load()}
	}
	return out
}

// ServeSyncWaitMsTotal is the total milliseconds internal-dispatch LISTs spent
// in the bounded informer sync-wait.
func ServeSyncWaitMsTotal() uint64 { return serveSyncWaitMsSum.Load() }

var serveMissExpvarOnce sync.Once

// CFG-1: the same Disabled() gate as every other cache-side publisher in this
// package.
func init() {
	if cache.Disabled() {
		return
	}
	serveMissExpvarOnce.Do(func() {
		expvar.Publish("snowplow_internal_dispatch_informer_serve_miss_total", expvar.Func(func() any {
			m := map[string]uint64{}
			for _, c := range ServeMissSnapshot() {
				m[c.Reason] = c.Count
			}
			return m
		}))
		expvar.Publish("snowplow_internal_dispatch_informer_sync_wait_ms_total", expvar.Func(func() any {
			return ServeSyncWaitMsTotal()
		}))
	})
}
