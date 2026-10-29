package credhttp

// resolve_lists.go — #988: snowplow_resolve_collection_lists{gvr,reason}, the
// count of collection LISTs a RESOLVE sends to the apiserver, read on the wire.
//
// WHY ON THE WIRE. snowplow_apiserver_fallthrough_total counts the DECISION
// sites that let a resolve fall through; it correlates with the requests, it
// does not count them. snowplow_reflector_path counts every collection GET on
// the watcher's config, reflectors and fall-throughs together, and sees none of
// the per-user configs a resolve dials. #870 promises that no resolve LISTs a
// GVR live while its store is swapped; only a count of the requests themselves
// can show that.
//
// WHERE. Every request a resolve sends to the apiserver leaves through a config
// or a client this package hardened: Harden (the in-cluster config, every
// per-user NewClientConfig, the #574 impersonating copy, which rest.CopyConfig
// gives the same WrapTransport) and Client (the endpoint clients the resolver
// dials a held credential with, and credhttp.Do). Both install
// CountResolveCollectionLists beside RefuseRedirectHops, inside it, so a refused
// redirect hop never reaches the counter. The resolver's endpoint client for a
// named endpointRef (a third party's own credential, so no redirect policy)
// gets the counter alone, through CountResolveListsOn: every resolve LIST is
// counted whatever credential it is sent with. A followed redirect hop of such
// a client is a request on the wire and is counted as one.
//
// WHAT. A request is counted when it is a collection GET that opens a
// collection — a LIST's first page or a watch-list start, by the path-shape
// classifier internal/apicall shares with the reflector-path instruments — AND
// its context carries the resolve marker. A continue page belongs to a LIST
// already counted; a plain re-watch opens nothing.
//
// THE MARKER. MarkResolve stamps it. internal/cache WithFallthroughScope calls
// it, so every resolve origin that scopes its context is marked: /call (the
// FallthroughScopeMiddleware), the boot prewarm walk and seed scopes, and the
// refresher's scope. A reflector's, a store verification's or a store swap's
// LIST carries no marker and is never counted here.
//
// THE REASON. internal/cache RecordApiserverFallthrough returns its context
// with the fall-through reason stamped (WithResolveReason), and the request
// that falls through is sent with it. A marked request that carries no reason
// counts as reason="unattributed": a decision site that does not hand its
// context on shows up as a gap instead of hiding.
//
// LABELS. gvr (the request path's GVR, schema.GroupVersionResource.String(),
// the same spelling the fall-through cells use) and reason (the closed
// FallthroughReason enum plus "unattributed"). Never an identity, a namespace,
// an object name or a URL: nothing here logs.

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/apicall"
	utilnet "k8s.io/apimachinery/pkg/util/net"
)

// ReasonUnattributed is the reason label of a counted request whose context
// carries the resolve marker and no fall-through reason.
const ReasonUnattributed = "unattributed"

type resolveCtxKey struct{}

// resolveMark is the marker's value: the fall-through reason, "" until a
// decision site stamps one.
type resolveMark struct{ reason string }

// MarkResolve marks ctx as a resolve's: its collection LISTs are counted. A
// mark already on ctx is replaced, so a new scope starts without a reason.
func MarkResolve(ctx context.Context) context.Context {
	return context.WithValue(ctx, resolveCtxKey{}, resolveMark{})
}

// ResolveMarked reports whether ctx carries the resolve marker.
func ResolveMarked(ctx context.Context) bool {
	_, ok := ctx.Value(resolveCtxKey{}).(resolveMark)
	return ok
}

// WithResolveReason returns ctx with reason as its fall-through reason. It
// returns ctx unchanged when ctx is not a resolve's (a reason is an attribute
// of a resolve's request, never a marker of its own) or reason is "".
func WithResolveReason(ctx context.Context, reason string) context.Context {
	if reason == "" || !ResolveMarked(ctx) {
		return ctx
	}
	return context.WithValue(ctx, resolveCtxKey{}, resolveMark{reason: reason})
}

// ResolveReason returns the fall-through reason ctx carries, "" if none.
func ResolveReason(ctx context.Context) string {
	m, _ := ctx.Value(resolveCtxKey{}).(resolveMark)
	return m.reason
}

// CountResolveCollectionLists wraps rt so a resolve's collection LIST is
// counted before it is sent. It adds no second counter to a chain that has one
// (Harden on a copy of a hardened config, Client over a client built from a
// hardened config), so a request is counted once whatever path built its
// client.
func CountResolveCollectionLists(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	if chainCountsResolveLists(rt) {
		return rt
	}
	return &resolveListCounter{next: rt}
}

// CountResolveListsOn installs CountResolveCollectionLists on c's transport,
// in place, and returns c — and nothing else: no redirect policy, no
// credential handling. It is for an endpoint client that carries a third
// party's own credential (a named endpointRef), which keeps its redirect
// behaviour, yet whose collection LISTs a resolve sends like any other's.
// A chain that already counts (a Client-hardened one) is left unchanged.
func CountResolveListsOn(c *http.Client) *http.Client {
	c.Transport = CountResolveCollectionLists(c.Transport)
	return c
}

// chainCountsResolveLists walks rt's wrapper chain (client-go's wrappers and
// this package's all expose their delegate) for a counter.
func chainCountsResolveLists(rt http.RoundTripper) bool {
	for rt != nil {
		if _, ok := rt.(*resolveListCounter); ok {
			return true
		}
		w, ok := rt.(utilnet.RoundTripperWrapper)
		if !ok {
			return false
		}
		rt = w.WrappedRoundTripper()
	}
	return false
}

type resolveListCounter struct{ next http.RoundTripper }

func (t *resolveListCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	countResolveCollectionList(req)
	return t.next.RoundTrip(req)
}

// WrappedRoundTripper keeps the chain walkable (client-go cancellation,
// CloseIdleConnectionsFor, and chainCountsResolveLists itself).
func (t *resolveListCounter) WrappedRoundTripper() http.RoundTripper { return t.next }

// countResolveCollectionList counts req when it is a marked collection GET that
// opens a collection. Cheapest test first: the verb, then the marker (one
// ctx.Value), then the non-allocating path scan; the query is parsed only for a
// marked collection GET.
func countResolveCollectionList(req *http.Request) {
	if req.Method != http.MethodGet || req.URL == nil {
		return
	}
	m, marked := req.Context().Value(resolveCtxKey{}).(resolveMark)
	if !marked {
		return
	}
	gvr, collection, ok := apicall.ParseResourcePath(req.URL.Path)
	if !ok || !collection {
		return
	}
	switch apicall.ClassifyCollectionQuery(req.URL.Query()) {
	case apicall.WireListCacheEligible, apicall.WireListEtcdDelegated, apicall.WireWatchList:
	default:
		return // a continue page or a plain re-watch opens no collection
	}
	reason := m.reason
	if reason == "" {
		reason = ReasonUnattributed
	}
	k := ResolveListKey{GVR: gvr.String(), Reason: reason}
	c, ok := resolveLists.Load(k)
	if !ok {
		c, _ = resolveLists.LoadOrStore(k, new(atomic.Uint64))
	}
	c.(*atomic.Uint64).Add(1)
}

// ResolveListKey is one snowplow_resolve_collection_lists cell.
type ResolveListKey struct {
	GVR    string
	Reason string
}

// resolveLists holds one counter per (gvr, reason). The key set grows with the
// GVRs resolves LIST and is bounded by them; the OTLP mirror caps it.
var resolveLists sync.Map // ResolveListKey -> *atomic.Uint64

// ResolveCollectionLists returns every cell's count.
func ResolveCollectionLists() map[ResolveListKey]uint64 {
	out := map[ResolveListKey]uint64{}
	resolveLists.Range(func(k, v any) bool {
		out[k.(ResolveListKey)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// ResolveCollectionListCount returns one cell's count, 0 if it never counted.
func ResolveCollectionListCount(gvr, reason string) uint64 {
	if c, ok := resolveLists.Load(ResolveListKey{GVR: gvr, Reason: reason}); ok {
		return c.(*atomic.Uint64).Load()
	}
	return 0
}

// ResetResolveCollectionListsForTest deletes every cell. TEST-ONLY.
func ResetResolveCollectionListsForTest() {
	resolveLists.Range(func(k, _ any) bool {
		resolveLists.Delete(k)
		return true
	})
}
