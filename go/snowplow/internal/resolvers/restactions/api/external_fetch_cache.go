// external_fetch_cache.go — the external api-step fetch: identical concurrent
// reads share one in-flight call, and the CONVERSION is memoised by content.
//
// WHY. External results never reach L1 (the external-touched sink declines the
// Put: they have no dependency edge to invalidate them), so every visit
// re-fetched the same document AND re-ran YAMLToJSON on it. Bench run 12 spent
// 15–19% of steady-state CPU in httpFetchAllowingNonJSON → toJSONBytes →
// yaml.YAMLToJSON, and one external-backed widget ran 870 times in the storm,
// once per user. On 057 (#1320) every page load opened its own identical call,
// up to 48 at once, each cut at its page budget while the upstream ran it on.
//
// WHAT IS SHARED: an identical READ while it is in flight, and nothing else
// (#1320, owner ruling 2026-11-10; external_fetch_flight.go). Snowplow has no
// event source for an external endpoint. A read (GET, HEAD, OPTIONS, TRACE)
// arriving while an identical one is in flight joins it; identical is the
// whole outbound request — endpoint with every credential field, method,
// resolved URL, every header (an exported JWT included), payload and dial
// provenance — so identities never share. THE FRESHNESS CONTRACT: a joiner may
// get an answer from a call that started up to one call-duration before it
// arrived. It never gets bytes received before it arrived: a call stops
// admitting joiners at its first response byte. Nothing is reused after
// completion — no TTL, no Cache-Control/Expires window, no conditional request
// (a 304 on If-Modified-Since is second-granular and answers "not modified"
// for a file rolled back to an older mtime, #845 review) — so a read arriving
// after a call completed makes a full, unconditional call of its own, and a
// change upstream is in the next call. A write is never shared. The shared
// call runs detached from every page, under the server write deadline; each
// caller's page budget and context bound only its own wait, and each caller
// records its own stage and audit outcome.
//
// WHAT IS MEMOISED: the conversion of the bytes just received. key =
// sha256(conversion mode ‖ raw bytes) → toJSONBytes' output for them. The
// conversion is a pure function of (mode, bytes), so a hit is byte-identical to
// converting afresh; different bytes convert fresh. Because the key IS the
// content, a hit across users hands each user exactly what their own response
// converts to — sharing carries no identity. A valid-JSON body under a non-YAML
// content type keeps toJSONBytes' pass-through (nothing converted, nothing
// memoised). Errors are never stored. Concurrent conversions of identical bytes
// single-flight.
//
// BOUND. Byte-accounted LRU. The budget is cache.ExternalFetchCacheBudget: a
// share of the process's memory headroom (GOMEMLIMIT − live heap). It is
// re-read on EVERY access — hit, miss or store — and the memo evicts down to
// it, so it shrinks as the heap fills even when every read is a hit. Evictions
// are counted.
//
// With the cache off the memo and the shared call both step aside (cache-off
// byte-identical: every read is its caller's own fetch).

package api

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/json"
	"expvar"
	"sync"
	"sync/atomic"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"golang.org/x/sync/singleflight"
)

// externalContentKey addresses a conversion: sha256(mode ‖ raw bytes).
type externalContentKey [sha256.Size]byte

// externalConversion is one memoised conversion. Immutable once stored.
type externalConversion struct {
	key  externalContentKey
	out  []byte
	size int64
	elem *list.Element
}

// externalFetchCache is the conversion memo. One per process (externalFetches).
type externalFetchCache struct {
	mu          sync.Mutex
	conversions map[externalContentKey]*externalConversion
	lru         *list.List // front = most recently used
	bytes       int64
	converting  singleflight.Group

	// flights (#1320) is each outbound read's joinable in-flight call, keyed by
	// the whole request (externalReadKeyOf): held only from the call's start to
	// its first response byte, never after it completes.
	flightsMu sync.Mutex
	flights   map[externalFlightKey]*externalFlight

	// fetch, toJSON and budget are seams for the tests.
	fetch  func(context.Context, httpcall.RequestOptions, func(string, []byte) ([]byte, error)) (*response.Status, []byte, string, error)
	toJSON func(string, []byte) ([]byte, error)
	budget func(held int64) int64

	convertHits, convertMisses, evictions atomic.Uint64
	// #1320 — reads that started a shared call, reads that joined one, and
	// fetches that are never shared (a write, or an endpoint the key cannot
	// fold exactly). Calls sent = led + not_shared.
	flightLed, flightJoined, flightNotShared atomic.Uint64
}

func newExternalFetchCache() *externalFetchCache {
	return &externalFetchCache{
		conversions: map[externalContentKey]*externalConversion{},
		lru:         list.New(),
		flights:     map[externalFlightKey]*externalFlight{},
		fetch:       httpFetchConverting,
		toJSON:      toJSONBytes,
		budget:      cache.ExternalFetchCacheBudget,
	}
}

var externalFetches = newExternalFetchCache()

// sharedExternalFetch is the external dispatch's fetch (resolve.go). Same
// return contract as httpFetchAllowingNonJSON; a read may share an identical
// in-flight call (external_fetch_flight.go), never a completed one.
func sharedExternalFetch(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	return externalFetches.do(ctx, opts)
}

func (c *externalFetchCache) do(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	if cache.Disabled() {
		return c.fetch(ctx, opts, toJSONBytes)
	}
	return c.fetchShared(ctx, opts)
}

// convert is toJSONBytes memoised by content: the body's bytes are looked up by
// sha256(mode ‖ bytes), converted on a miss (single-flight per key), and stored
// when they converted cleanly. A valid-JSON body under a non-YAML content type
// is toJSONBytes' own pass-through — returned as-is, not memoised.
func (c *externalFetchCache) convert(ct string, dat []byte) ([]byte, error) {
	yamlMode := isYAMLContentType(ct)
	if !yamlMode && json.Valid(dat) {
		return dat, nil
	}
	key := externalContentKeyOf(yamlMode, dat)
	if out := c.lookup(key); out != nil {
		c.convertHits.Add(1)
		return out, nil
	}
	v, err, _ := c.converting.Do(string(key[:]), func() (any, error) {
		if out := c.lookup(key); out != nil {
			c.convertHits.Add(1)
			return out, nil
		}
		c.convertMisses.Add(1)
		out, err := c.toJSON(ct, dat)
		if err != nil {
			return nil, err // never stored
		}
		c.store(key, out)
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (c *externalFetchCache) lookup(key externalContentKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trimLocked(0)
	conv := c.conversions[key]
	if conv == nil {
		return nil
	}
	c.lru.MoveToFront(conv.elem)
	return conv.out
}

func (c *externalFetchCache) store(key externalContentKey, out []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conv := c.conversions[key]; conv != nil {
		c.lru.MoveToFront(conv.elem)
		return
	}
	e := &externalConversion{key: key, out: out, size: int64(len(out)) + int64(len(key))}
	if !c.trimLocked(e.size) {
		return
	}
	e.elem = c.lru.PushFront(e)
	c.conversions[key] = e
	c.bytes += e.size
}

// trimLocked re-reads the budget and evicts from the LRU back until what is
// held, plus room more bytes, fits it. false when room alone exceeds the
// budget (it is then not stored; what is held is still trimmed).
func (c *externalFetchCache) trimLocked(room int64) bool {
	budget := c.budget(c.bytes)
	fits := room <= budget
	if !fits {
		room = 0
	}
	for c.bytes+room > budget {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeLocked(back.Value.(*externalConversion))
		c.evictions.Add(1)
	}
	return fits
}

func (c *externalFetchCache) removeLocked(e *externalConversion) {
	if c.conversions[e.key] != e {
		return
	}
	delete(c.conversions, e.key)
	c.lru.Remove(e.elem)
	c.bytes -= e.size
}

func (c *externalFetchCache) held() (entries int, bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.conversions), c.bytes
}

// externalContentKeyOf is sha256(mode ‖ bytes). The mode is in the key because
// toJSONBytes' contract is per (content type class, bytes); for today's
// conversion the two modes agree on every body the memo stores, and the key
// keeps that from being an assumption the memo depends on.
func externalContentKeyOf(yamlMode bool, dat []byte) externalContentKey {
	h := sha256.New()
	if yamlMode {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write(dat)
	var k externalContentKey
	h.Sum(k[:0])
	return k
}

// ExternalFetchCacheCount is one {outcome} cell of the memo's counter.
type ExternalFetchCacheCount struct {
	Outcome string
	Count   uint64
}

// ExternalFetchCacheSnapshot returns snowplow_external_fetch_conversion_total by
// outcome (closed set, zeros included):
//   - convert_hit: a fetched body's exact bytes were already converted; reused;
//   - convert_miss: the body was converted (YAMLToJSON).
func ExternalFetchCacheSnapshot() []ExternalFetchCacheCount {
	c := externalFetches
	return []ExternalFetchCacheCount{
		{Outcome: "convert_hit", Count: c.convertHits.Load()},
		{Outcome: "convert_miss", Count: c.convertMisses.Load()},
	}
}

// ExternalFetchFlightSnapshot returns snowplow_external_fetch_flight_total by
// outcome (closed set, zeros included; #1320):
//   - led: a read that started a call identical reads may join;
//   - joined: a read that shared an identical call already in flight;
//   - not_shared: a fetch that is never shared (a write, or an endpoint the key
//     cannot fold exactly).
//
// Calls sent = led + not_shared; joined is the calls not sent.
func ExternalFetchFlightSnapshot() []ExternalFetchCacheCount {
	c := externalFetches
	return []ExternalFetchCacheCount{
		{Outcome: "led", Count: c.flightLed.Load()},
		{Outcome: "joined", Count: c.flightJoined.Load()},
		{Outcome: "not_shared", Count: c.flightNotShared.Load()},
	}
}

// ExternalFetchCacheHeld returns the memo's current entries and bytes.
func ExternalFetchCacheHeld() (entries int, bytes int64) { return externalFetches.held() }

// ExternalFetchCacheEvictions returns how many entries the byte bound evicted.
func ExternalFetchCacheEvictions() uint64 { return externalFetches.evictions.Load() }

// RecordExternalFetchCacheForTest bumps every outcome cell (conversion and
// flight) and the eviction counter once — the OTLP falsifier's way to move a real series. Production
// MUST NOT use it.
func RecordExternalFetchCacheForTest() {
	c := externalFetches
	for _, ctr := range []*atomic.Uint64{&c.convertHits, &c.convertMisses, &c.evictions, &c.flightLed, &c.flightJoined, &c.flightNotShared} {
		ctr.Add(1)
	}
}

var externalFetchCacheExpvarOnce sync.Once

// CFG-1: published only with the cache on, like every cache-side publisher.
func init() {
	if cache.Disabled() {
		return
	}
	externalFetchCacheExpvarOnce.Do(func() {
		expvar.Publish("snowplow_external_fetch_conversion_total", expvar.Func(func() any {
			m := map[string]uint64{}
			for _, c := range ExternalFetchCacheSnapshot() {
				m[c.Outcome] = c.Count
			}
			return m
		}))
		expvar.Publish("snowplow_external_fetch_conversion_held", expvar.Func(func() any {
			n, b := ExternalFetchCacheHeld()
			return map[string]int64{"conversions": int64(n), "bytes": b}
		}))
		expvar.Publish("snowplow_external_fetch_conversion_evictions_total", expvar.Func(func() any {
			return ExternalFetchCacheEvictions()
		}))
		expvar.Publish("snowplow_external_fetch_flight_total", expvar.Func(func() any {
			m := map[string]uint64{}
			for _, c := range ExternalFetchFlightSnapshot() {
				m[c.Outcome] = c.Count
			}
			return m
		}))
	})
}
