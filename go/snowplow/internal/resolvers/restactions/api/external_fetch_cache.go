// external_fetch_cache.go — the external api-step fetch, with its CONVERSION
// memoised by content.
//
// WHY. External results never reach L1 (the external-touched sink declines the
// Put: they have no dependency edge to invalidate them), so every visit
// re-fetched the same document AND re-ran YAMLToJSON on it. Bench run 12 spent
// 15–19% of steady-state CPU in httpFetchAllowingNonJSON → toJSONBytes →
// yaml.YAMLToJSON, and one external-backed widget ran 870 times in the storm,
// once per user.
//
// WHAT IS NOT CACHED: the response. Snowplow has no event source for an
// external endpoint, so any reuse decided by time (a TTL, Cache-Control,
// Expires) would serve data that may have changed, for as long as the window
// says. EVERY read performs the fetch. A change upstream is in the next read.
//
// WHAT IS CACHED: the conversion, by content. key = sha256(conversion mode ‖ the
// raw response bytes) → the converted JSON bytes. Identical bytes reuse the
// result of converting those same bytes; different bytes convert fresh. The
// output is byte-identical to a fresh conversion by construction (the
// conversion is a pure function of mode and bytes). Because the key IS the
// content, a hit across users hands a user exactly the bytes their own
// response would have converted to — sharing it carries no identity. Errors are
// not stored. Concurrent conversions of identical bytes single-flight.
//
// CONDITIONAL REQUESTS (optional, still a live check on every read). When an
// upstream answered with an ETag or Last-Modified, the NEXT read by the SAME
// request identity sends them back (If-None-Match / If-Modified-Since). A 304
// serves the converted bytes of exactly the response those validators came
// with — recorded as its content key, so it is the bytes that identity last
// saw for that URL. If those bytes were evicted, the read is re-made
// unconditionally. Validators are keyed per request identity: sha256 over
// method, path, payload, every stage header in order, and EVERY Endpoint field
// (ServerURL and every credential, folded by reflection so a field added later
// is folded too). A request with another credential, another forwarded token
// (exportJwt) or another header never sees them. Requests whose identity lives
// in the ctx rather than in the request — snowplow's SA endpoint, a held
// per-user credential, an impersonated dial — never send validators; nor does a
// stage that sets its own conditional or range headers, or a write.
//
// BOUND. One byte-accounted LRU over conversions and validators. The budget is
// cache.ExternalFetchCacheBudget: a share of the process's memory headroom
// (GOMEMLIMIT − live heap), read at each store, so it shrinks as the heap
// fills. Evictions are counted.
//
// With the cache off the layer steps aside entirely (cache-off byte-identical).

package api

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"golang.org/x/sync/singleflight"
)

// externalContentKey addresses a conversion: sha256(mode ‖ raw bytes).
type externalContentKey [sha256.Size]byte

// externalRequestKey is a request identity: who asks, for what (externalRequestKeyOf).
type externalRequestKey [sha256.Size]byte

// externalConversion is one memoised conversion. Immutable once stored.
type externalConversion struct {
	key  externalContentKey
	out  []byte
	size int64
	elem *list.Element
}

// externalValidatorEntry is what one request identity last saw for one URL:
// its validators, and the content key of the bytes they came with.
type externalValidatorEntry struct {
	key     externalRequestKey
	v       externalValidators
	content externalContentKey
	ct      string
	size    int64
	elem    *list.Element
}

// externalFetchCache is the layer. One per process (externalFetches).
type externalFetchCache struct {
	mu          sync.Mutex
	conversions map[externalContentKey]*externalConversion
	validators  map[externalRequestKey]*externalValidatorEntry
	lru         *list.List // front = most recently used; values are either entry type
	bytes       int64
	converting  singleflight.Group

	// fetch, toJSON and budget are seams for the tests.
	fetch  func(context.Context, httpcall.RequestOptions, *externalValidators, func(string, []byte) ([]byte, error)) (*response.Status, []byte, string, *externalFetchMeta, error)
	toJSON func(string, []byte) ([]byte, error)
	budget func(held int64) int64

	convertHits, convertMisses, notModified, evictions atomic.Uint64
}

func newExternalFetchCache() *externalFetchCache {
	return &externalFetchCache{
		conversions: map[externalContentKey]*externalConversion{},
		validators:  map[externalRequestKey]*externalValidatorEntry{},
		lru:         list.New(),
		fetch:       httpFetchExternal,
		toJSON:      toJSONBytes,
		budget:      cache.ExternalFetchCacheBudget,
	}
}

var externalFetches = newExternalFetchCache()

// sharedExternalFetch is the external dispatch's fetch (resolve.go). Same
// return contract as httpFetchAllowingNonJSON; it always reaches the upstream.
func sharedExternalFetch(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	return externalFetches.do(ctx, opts)
}

func (c *externalFetchCache) do(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	if cache.Disabled() {
		res, body, ct, _, err := c.fetch(ctx, opts, nil, toJSONBytes)
		return res, body, ct, err
	}

	// converted: this read's body went through convert without error.
	// passthrough: it was valid JSON returned as-is (no content key computed).
	var converted, passthrough bool
	var seen externalContentKey
	convert := func(ct string, dat []byte) ([]byte, error) {
		out, key, pass, err := c.convert(ct, dat)
		converted, passthrough, seen = err == nil, pass, key
		return out, err
	}

	conditional := externalConditionalEligible(ctx, opts)
	var rkey externalRequestKey
	var prior *externalValidatorEntry
	var cond *externalValidators
	if conditional {
		rkey = externalRequestKeyOf(opts)
		c.mu.Lock()
		if prior = c.validators[rkey]; prior != nil {
			c.lru.MoveToFront(prior.elem)
			v := prior.v
			cond = &v
		}
		c.mu.Unlock()
	}

	res, body, ct, meta, err := c.fetch(ctx, opts, cond, convert)
	if err == nil && meta != nil && meta.notModified {
		c.mu.Lock()
		conv := c.conversions[prior.content]
		if conv != nil {
			c.lru.MoveToFront(conv.elem)
		}
		c.mu.Unlock()
		if conv != nil {
			c.notModified.Add(1)
			return res, conv.out, prior.ct, nil
		}
		// The bytes those validators came with were evicted: read again,
		// unconditionally.
		c.dropValidator(rkey)
		converted, passthrough = false, false
		res, body, ct, meta, err = c.fetch(ctx, opts, nil, convert)
	}

	if conditional && err == nil && meta != nil {
		v := externalValidators{etag: meta.header.Get("ETag"), lastModified: meta.header.Get("Last-Modified")}
		if meta.code == http.StatusOK && converted && (v.etag != "" || v.lastModified != "") {
			if passthrough {
				seen = externalContentKeyOf(false, body)
			}
			c.storeValidator(rkey, v, seen, ct, body)
		} else {
			c.dropValidator(rkey)
		}
	}
	return res, body, ct, err
}

// convert is toJSONBytes memoised by content. A valid-JSON body under a
// non-YAML content type is toJSONBytes' own fast path — returned as-is, nothing
// converted, nothing memoised (passthrough=true, no key computed). Everything
// else is looked up by sha256(mode ‖ bytes), converted on a miss (single-flight
// per key), and stored when it converted cleanly. Errors are never stored.
func (c *externalFetchCache) convert(ct string, dat []byte) (out []byte, key externalContentKey, passthrough bool, err error) {
	yamlMode := isYAMLContentType(ct)
	if !yamlMode && json.Valid(dat) {
		return dat, key, true, nil
	}
	key = externalContentKeyOf(yamlMode, dat)
	if out := c.lookupConversion(key); out != nil {
		c.convertHits.Add(1)
		return out, key, false, nil
	}
	v, err, _ := c.converting.Do(string(key[:]), func() (any, error) {
		if out := c.lookupConversion(key); out != nil {
			c.convertHits.Add(1)
			return out, nil
		}
		c.convertMisses.Add(1)
		out, err := c.toJSON(ct, dat)
		if err != nil {
			return nil, err
		}
		c.storeConversion(key, out)
		return out, nil
	})
	if err != nil {
		return nil, key, false, err
	}
	return v.([]byte), key, false, nil
}

func (c *externalFetchCache) lookupConversion(key externalContentKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	conv := c.conversions[key]
	if conv == nil {
		return nil
	}
	c.lru.MoveToFront(conv.elem)
	return conv.out
}

func (c *externalFetchCache) storeConversion(key externalContentKey, out []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeConversionLocked(key, out)
}

func (c *externalFetchCache) storeConversionLocked(key externalContentKey, out []byte) *externalConversion {
	if conv := c.conversions[key]; conv != nil {
		c.lru.MoveToFront(conv.elem)
		return conv
	}
	e := &externalConversion{key: key, out: out, size: int64(len(out)) + int64(len(key))}
	if !c.makeRoomLocked(e.size) {
		return nil
	}
	e.elem = c.lru.PushFront(e)
	c.conversions[key] = e
	c.bytes += e.size
	return e
}

// storeValidator records what identity key last saw: its validators and the
// content key of the converted bytes they came with, holding those bytes (a
// JSON passthrough body is stored here, only because a 304 must map to it).
func (c *externalFetchCache) storeValidator(key externalRequestKey, v externalValidators, content externalContentKey, ct string, out []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.validators[key]; old != nil {
		c.removeLocked(old)
	}
	if c.storeConversionLocked(content, out) == nil {
		return // nothing for a 304 to map to
	}
	e := &externalValidatorEntry{key: key, v: v, content: content, ct: ct}
	e.size = int64(len(key)+len(content)+len(v.etag)+len(v.lastModified)+len(ct)) + 16
	if !c.makeRoomLocked(e.size) {
		return
	}
	// makeRoomLocked may have evicted the conversion itself.
	if c.conversions[content] == nil {
		return
	}
	e.elem = c.lru.PushFront(e)
	c.validators[key] = e
	c.bytes += e.size
}

func (c *externalFetchCache) dropValidator(key externalRequestKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.validators[key]; old != nil {
		c.removeLocked(old)
	}
}

// makeRoomLocked evicts from the LRU back until size more bytes fit the
// budget. false when size alone exceeds it (nothing is evicted then).
func (c *externalFetchCache) makeRoomLocked(size int64) bool {
	budget := c.budget(c.bytes)
	if size > budget {
		return false
	}
	for c.bytes+size > budget {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeElemLocked(back)
		c.evictions.Add(1)
	}
	return true
}

func (c *externalFetchCache) removeElemLocked(el *list.Element) {
	switch e := el.Value.(type) {
	case *externalConversion:
		c.removeLocked(e)
	case *externalValidatorEntry:
		c.removeLocked(e)
	}
}

func (c *externalFetchCache) removeLocked(e any) {
	switch e := e.(type) {
	case *externalConversion:
		if c.conversions[e.key] == e {
			delete(c.conversions, e.key)
			c.lru.Remove(e.elem)
			c.bytes -= e.size
		}
	case *externalValidatorEntry:
		if c.validators[e.key] == e {
			delete(c.validators, e.key)
			c.lru.Remove(e.elem)
			c.bytes -= e.size
		}
	}
}

func (c *externalFetchCache) held() (conversions, validators int, bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.conversions), len(c.validators), c.bytes
}

// externalConditionalEligible: may this read send the validators its identity
// last saw? Only when every input that decides who it acts as is in opts.
func externalConditionalEligible(ctx context.Context, opts httpcall.RequestOptions) bool {
	if opts.Endpoint == nil || ptr.Deref(opts.Verb, http.MethodGet) != http.MethodGet {
		return false
	}
	if cache.ServiceAccountDialFromContext(ctx) || credhttp.HeldCredential(ctx) ||
		ctx.Value(endpointImpersonationKey{}) != nil {
		return false
	}
	for _, h := range opts.Headers {
		name, _, _ := strings.Cut(h, ":")
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "if-none-match", "if-modified-since", "if-match", "if-unmodified-since", "if-range", "range":
			return false
		}
	}
	return true
}

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

// externalRequestKeyOf folds every input of the request into its identity. The
// Endpoint is folded field by field by reflection, so every field — each
// credential included — is in the key, now and when the struct grows.
func externalRequestKeyOf(opts httpcall.RequestOptions) externalRequestKey {
	h := sha256.New()
	var n [8]byte
	put := func(s string) {
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	put(ptr.Deref(opts.Verb, http.MethodGet))
	put(opts.Path)
	put(ptr.Deref(opts.Payload, ""))
	put(strconv.Itoa(len(opts.Headers)))
	for _, hd := range opts.Headers {
		put(hd)
	}
	ev := reflect.ValueOf(opts.Endpoint).Elem()
	et := ev.Type()
	for i := 0; i < et.NumField(); i++ {
		put(et.Field(i).Name)
		f := ev.Field(i)
		switch f.Kind() {
		case reflect.String:
			put(f.String())
		case reflect.Bool:
			put(strconv.FormatBool(f.Bool()))
		default:
			// A field kind this fold does not know: fold its formatted value,
			// never skip it.
			put(f.Type().String())
			put(fmt.Sprintf("%#v", f))
		}
	}
	var k externalRequestKey
	h.Sum(k[:0])
	return k
}

// ExternalFetchCacheCount is one {outcome} cell of the layer's counter.
type ExternalFetchCacheCount struct {
	Outcome string
	Count   uint64
}

// ExternalFetchCacheSnapshot returns snowplow_external_fetch_conversion_total by
// outcome (closed set, zeros included):
//   - convert_hit: a fetched body's bytes were already converted; reused;
//   - convert_miss: the body was converted (YAMLToJSON);
//   - not_modified: a conditional read was answered 304; the bytes that
//     identity last saw were served without a body or a conversion.
func ExternalFetchCacheSnapshot() []ExternalFetchCacheCount {
	c := externalFetches
	return []ExternalFetchCacheCount{
		{Outcome: "convert_hit", Count: c.convertHits.Load()},
		{Outcome: "convert_miss", Count: c.convertMisses.Load()},
		{Outcome: "not_modified", Count: c.notModified.Load()},
	}
}

// ExternalFetchCacheHeld returns the layer's current entries (conversions +
// validators) and bytes.
func ExternalFetchCacheHeld() (entries int, bytes int64) {
	conv, val, b := externalFetches.held()
	return conv + val, b
}

// ExternalFetchCacheEvictions returns how many entries the byte bound evicted.
func ExternalFetchCacheEvictions() uint64 { return externalFetches.evictions.Load() }

// RecordExternalFetchCacheForTest bumps every outcome cell and the eviction
// counter once — the OTLP falsifier's way to move a real series. Production
// MUST NOT use it.
func RecordExternalFetchCacheForTest() {
	c := externalFetches
	for _, ctr := range []*atomic.Uint64{&c.convertHits, &c.convertMisses, &c.notModified, &c.evictions} {
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
			conv, val, b := externalFetches.held()
			return map[string]int64{"conversions": int64(conv), "validators": int64(val), "bytes": b}
		}))
		expvar.Publish("snowplow_external_fetch_conversion_evictions_total", expvar.Func(func() any {
			return ExternalFetchCacheEvictions()
		}))
	})
}
