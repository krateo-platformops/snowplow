//go:build js && wasm

package fakeapiserver

import "time"

// One resource's event history, sized as kube-apiserver sizes its watch cache, so a watch from a
// resourceVersion that has left the window gets the same 410 Expired. k8s.io/apiserver v0.36.5
// (the minor 057 runs) pkg/storage/cacher:
//
//	watch_cache.go:59-64   defaultLowerBoundCapacity = 100, defaultUpperBoundCapacity = 100 * 1024
//	watch_cache.go:179     a cache starts at the lower bound
//	watch_cache.go:360-389 updateCache/resizeCacheLocked: when full, double if the oldest kept event
//	                       is younger than eventFreshDuration, halve (not below the lower bound) if
//	                       the event a quarter from the end is older; then evict the oldest
//	cacher.go:68-77        eventFreshDuration = defaultBookmarkFrequency (1 min) + 15 s
//	watch_cache.go:885-917 the oldest deliverable RV is listResourceVersion+1 until an event has been
//	                       evicted, then the first kept event's; older is "too old resource version"
const (
	lowerBoundCapacity = 100
	upperBoundCapacity = 100 * 1024
	bookmarkFrequency  = time.Minute
	eventFreshDuration = bookmarkFrequency + 15*time.Second
)

type event struct {
	Type   string
	Object map[string]any
	Prev   map[string]any
	RV     uint64
	Time   time.Time
}

type watchCache struct {
	capacity                int
	events                  []event
	listResourceVersion     uint64
	removedEventSinceRelist bool
}

func newWatchCache(listRV uint64) *watchCache {
	return &watchCache{capacity: lowerBoundCapacity, listResourceVersion: listRV}
}

func (c *watchCache) add(e event) {
	c.resize(e.Time)
	if len(c.events) >= c.capacity {
		c.events = c.events[1:]
		c.removedEventSinceRelist = true
	}
	c.events = append(c.events, e)
}

func (c *watchCache) resize(now time.Time) {
	if len(c.events) < c.capacity {
		return
	}
	if now.Sub(c.events[0].Time) < eventFreshDuration {
		c.capacity = min(c.capacity*2, upperBoundCapacity)
		return
	}
	if q := c.events[len(c.events)-c.capacity/4]; now.Sub(q.Time) > eventFreshDuration {
		capacity := max(c.capacity/2, lowerBoundCapacity)
		if capacity < c.capacity {
			// doCacheResizeLocked moves startIndex without setting removedEventSinceRelist; the cache
			// is then full, so the add that follows evicts and sets it (updateCache).
			if drop := len(c.events) - capacity; drop > 0 {
				c.events = c.events[drop:]
			}
			c.capacity = capacity
		}
	}
}

func (c *watchCache) oldest() uint64 {
	if !c.removedEventSinceRelist || len(c.events) == 0 {
		return c.listResourceVersion + 1
	}
	return c.events[0].RV
}

// since is the events after rv, or ok=false when rv is too old (the caller answers 410).
func (c *watchCache) since(rv uint64) (evs []event, ok bool) {
	if rv+1 < c.oldest() {
		return nil, false
	}
	for _, e := range c.events {
		if e.RV > rv {
			evs = append(evs, e)
		}
	}
	return evs, true
}
