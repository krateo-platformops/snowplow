package credhttp

// endpoint_clients.go — #1088: one HTTP client per endpoint identity, reused
// across fetches.
//
// WHY. Run 20's 30.6 s page waited on a stage fetch that, like every stage
// fetch, built a new transport: a fresh TCP connect and TLS handshake per
// fetch, so any connect whose SYN went unanswered cost the transport's full
// connect timeout. A client kept per identity pays the connect once per idle
// window and multiplexes every later fetch on its HTTP/2 connection.
//
// IDENTITY. The key is a sha256 over EVERY field of endpoints.Endpoint, read by
// reflection so a field plumbing adds is folded without anyone remembering to,
// plus the caller's provenance inputs (what else changes the built chain). Two
// endpoints that differ in any credential, CA, URL or flag never share an entry.
// The key is never logged.
//
// FAIL CLOSED (#1101 review R-d). The fold is exact only for exported string and
// bool fields. Should plumbing ever add a field of any other kind (a pointer
// would print as an address that a later value can reuse; an unexported field
// cannot be read), endpointKeyExact is false and Get keeps nothing: every fetch
// builds its own client, as before #1088, rather than trusting a key that may
// let two identities meet.
//
// WHAT THE CALLER GETS: a shallow copy of the cached client. Its per-fetch
// wrappers (tracing, impersonation, a host pin) go on the copy's Transport, so
// they never reach the shared value or another fetch.
//
// BOUND, WITH NO NEW NUMBER. An entry unused for one idle-connection timeout is
// dropped and its idle connections closed (utilnet.CloseIdleConnectionsFor,
// which walks the wrapper chain to the transport: http.Client.
// CloseIdleConnections reaches a transport only when the OUTERMOST layer
// implements it, and none of these layers does — #1101 review B2). By then the
// transport's own idle timer has closed most of them, so the entry held
// nothing worth keeping and a rebuild costs what reuse would have. The timeout is net/http's DefaultTransport.IdleConnTimeout,
// which is the value client-go's utilnet.SetTransportDefaults installs on every
// transport it builds, read from it rather than restated. So the cache holds at
// most the identities that dialled within the last idle timeout.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	utilnet "k8s.io/apimachinery/pkg/util/net"
)

// EndpointKey identifies one endpoint identity. Opaque; never logged.
type EndpointKey [sha256.Size]byte

// endpointKeyExact reports whether EndpointKeyOf folds endpoints.Endpoint
// exactly: true when every field is an exported string or bool (see the file
// comment). A package var so the fail-closed arm can drive the other branch.
var endpointKeyExact = foldsExactly(reflect.TypeOf(endpoints.Endpoint{}))

// foldsExactly reports whether every field of struct type t is an exported
// string or bool, the only kinds EndpointKeyOf folds by value.
func foldsExactly(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			return false
		}
		switch f.Type.Kind() {
		case reflect.String, reflect.Bool:
		default:
			return false
		}
	}
	return true
}

// EndpointKeyOf folds every field of ep, then each provenance input (whatever
// besides the endpoint changes the chain the caller builds: a held-credential
// mark, the token file an SA dial reads), each in its own position, into a
// key. A nil ep folds as the zero Endpoint. The key identifies an endpoint
// only while endpointKeyExact holds; Get refuses to keep anything otherwise.
func EndpointKeyOf(ep *endpoints.Endpoint, provenance ...string) EndpointKey {
	var zero endpoints.Endpoint
	if ep == nil {
		ep = &zero
	}
	h := sha256.New()
	v := reflect.ValueOf(ep).Elem()
	t := v.Type()
	var n [8]byte
	put := func(b []byte) {
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}
	for i := 0; i < t.NumField(); i++ {
		put([]byte(t.Field(i).Name))
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			put([]byte(f.String()))
		case reflect.Bool:
			if f.Bool() {
				put([]byte{1})
			} else {
				put([]byte{0})
			}
		default:
			// Not folded by value. endpointKeyExact is false for such a type,
			// so no key derived here is ever used to keep a client.
			put([]byte(fmt.Sprintf("unfolded kind %s", f.Kind())))
		}
	}
	for _, m := range provenance {
		put([]byte(m))
	}
	var k EndpointKey
	copy(k[:], h.Sum(nil))
	return k
}

// endpointIdle is the idle-connection timeout of the transports these clients
// are built on (see the file comment).
func endpointIdle() time.Duration {
	if t, ok := http.DefaultTransport.(*http.Transport); ok && t.IdleConnTimeout > 0 {
		return t.IdleConnTimeout
	}
	// http.DefaultTransport replaced by something else: no idle timeout to
	// follow, so entries are kept for as long as they are used at all.
	return 0
}

// EndpointClients holds one client per EndpointKey (see the file comment).
type EndpointClients struct {
	idle time.Duration
	now  func() time.Time

	mu        sync.Mutex
	entries   map[EndpointKey]*endpointClientEntry
	lastSweep time.Time

	builds    atomic.Uint64
	evictions atomic.Uint64
}

type endpointClientEntry struct {
	ready    chan struct{}
	cli      *http.Client
	err      error
	lastUsed time.Time
}

// NewEndpointClients returns an empty cache whose entries live for one
// transport idle-connection timeout past their last use.
func NewEndpointClients() *EndpointClients {
	return newEndpointClients(endpointIdle(), time.Now)
}

func newEndpointClients(idle time.Duration, now func() time.Time) *EndpointClients {
	return &EndpointClients{idle: idle, now: now, entries: map[EndpointKey]*endpointClientEntry{}, lastSweep: now()}
}

// NewEndpointClientsForTest is NewEndpointClients with the idle timeout and
// clock given. Tests only.
func NewEndpointClientsForTest(idle time.Duration, now func() time.Time) *EndpointClients {
	return newEndpointClients(idle, now)
}

// Get returns a shallow copy of key's client, building it with build on the
// first use. build reports whether its client may be kept: one that may not (a
// fallback that must stay per fetch) is handed to this caller and to any caller
// that waited on the same build, and not stored. A failed build is not stored
// either; the next Get retries it. A client bound to one request (an AWS
// SigV4 signature) must never be built through Get: waiters would share it.
func (c *EndpointClients) Get(key EndpointKey, build func() (cli *http.Client, keep bool, err error)) (*http.Client, error) {
	if !endpointKeyExact {
		// Fail closed: a key that may not separate identities keeps nothing.
		cli, _, err := build()
		c.builds.Add(1)
		return cli, err
	}
	now := c.now()
	c.mu.Lock()
	stale := c.sweepLocked(now)
	e, ok := c.entries[key]
	if ok {
		e.lastUsed = now
		c.mu.Unlock()
		closeIdle(stale)
		<-e.ready
		if e.err != nil {
			return nil, e.err
		}
		cp := *e.cli
		return &cp, nil
	}
	e = &endpointClientEntry{ready: make(chan struct{}), lastUsed: now}
	c.entries[key] = e
	c.mu.Unlock()
	closeIdle(stale)

	cli, keep, err := build()
	c.builds.Add(1)
	e.cli, e.err = cli, err
	if err != nil || !keep || cli == nil {
		c.mu.Lock()
		if c.entries[key] == e {
			delete(c.entries, key)
		}
		c.mu.Unlock()
	}
	close(e.ready)
	if err != nil {
		return cli, err
	}
	cp := *cli
	return &cp, nil
}

// sweepLocked drops every built entry unused for c.idle, at most once per
// c.idle, and returns their clients for closeIdle.
func (c *EndpointClients) sweepLocked(now time.Time) []*http.Client {
	if c.idle <= 0 || now.Sub(c.lastSweep) < c.idle {
		return nil
	}
	c.lastSweep = now
	var stale []*http.Client
	for k, e := range c.entries {
		select {
		case <-e.ready:
		default:
			continue // still building
		}
		if now.Sub(e.lastUsed) >= c.idle {
			delete(c.entries, k)
			c.evictions.Add(1)
			if e.cli != nil {
				stale = append(stale, e.cli)
			}
		}
	}
	return stale
}

func closeIdle(cs []*http.Client) {
	for _, cli := range cs {
		utilnet.CloseIdleConnectionsFor(cli.Transport)
	}
}

// Len is the number of entries held (built or building).
func (c *EndpointClients) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Builds is how many clients were built (one per miss): next to the fetch
// count it is the reuse reading — builds far below fetches means reuse.
func (c *EndpointClients) Builds() uint64 { return c.builds.Load() }

// Evictions is how many entries the idle sweep dropped.
func (c *EndpointClients) Evictions() uint64 { return c.evictions.Load() }
