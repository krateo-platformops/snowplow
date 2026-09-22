// reflector_path.go — snowplow#237 deliverable A3: per-GVR attribution of the
// path our informers' LISTs and WATCHes actually take on the wire.
//
// # WHY THIS EXISTS
//
// Which path a reflector takes — watch-list (SendInitialEvents streamed from
// the apiserver watch cache) or the classic paged LIST — is settled statically
// today by reading client-go's feature gates and our ListerWatcher
// construction. A static conclusion is correct but it cannot be re-validated
// after a client-go bump, an apiserver upgrade, or an edit to the
// externally-managed env ConfigMap. And #237 showed what that costs: the only
// in-process signal for the LIST path is a slog.Info, the chart ships at
// LOG_LEVEL=warn, and the live pod's 20h log is 217 WARN + 30 ERROR with ZERO
// INFO. Absence in that log is a statement about the instrument, not about the
// code path. No expvar counter matched `watchlist`, `initial_events`,
// `bookmark` or `reflector` either (captured before this file existed).
//
// A mechanism with no instrument is the same defect class as the rest of #237,
// which is why the instrument ships with the diagnostic.
//
// HOW: ONE rest.Config.WrapTransport, INSTALLED ONCE. main.go calls
// rest.InClusterConfig() at exactly one place in the cache path and builds the
// first client two lines later, so wrapping there covers every informer family
// in one edit: the dynamic client (streaming + standalone informers), the
// metadata client, the streaming REST client (rest.CopyConfig copies
// WrapTransport verbatim), and the secrets / controller-health clientsets.
// Prior art is snowplow's own internal/tracing.WrapTransport — an outbound
// RoundTripper wrapper behind an enabled-check that returns rt unchanged when
// off.
//
// WHAT IS COUNTED, AND WHAT IS DELIBERATELY NOT (the correction that matters).
//
// Classification is on the request PATH SHAPE, not on query tokens:
//
//   - counted: a GET whose path resolves to a COLLECTION endpoint
//     (/apis/<g>/<v>/<r>, /api/<v>/<r>, either with an interposed
//     /namespaces/<ns>/) — i.e. a LIST or a WATCH;
//   - NOT counted, at all: named GETs, subresources, discovery, non-resource
//     paths, and every non-GET verb. They are not bucketed into a catch-all.
//
// Two reasons, both load-bearing:
//
//  1. The wrapper sits on the SHARED rest.Config, which also backs discovery
//     and every hot-path fallthrough GET-by-name. A catch-all bucket would
//     span "the reflector listed from the watch cache" and "a customer request
//     fell through" — two regimes in one number, the defect class this
//     investigation has already produced three times.
//
//  2. A query-token pre-scan (`limit=`/`watch=`) goes BLIND exactly when the
//     C1 fix lands: after it, a relist at a non-zero resourceVersion carries
//     neither token, so list_etcd_delegated_total dropping to 0 would be
//     indistinguishable from "we stopped counting that request class". That
//     counter is C1's only production proof, so it has to keep counting the
//     requests C1 changes.
//
// HONEST SCOPE OF THE NAME. These COUNTERS observe every collection GET this
// process issues through the shared config. In cache-on that is overwhelmingly
// reflector traffic, but a cache-off or fallthrough LIST looks the same on the
// wire and is counted too. Nothing here counts a GET by name. And the scope is
// this config: a future client built from a DIFFERENT rest.Config is invisible
// to every counter in this file, and no counter reports that — so a sudden
// drop in transport_requests_total should be read against whether a new client
// was introduced, not only against cluster activity.
//
// THE PER-GVR MAP IS NARROWER THAN THE COUNTERS, DELIBERATELY. It records only
// a GVR with a registered informer, and only from a request shaped like a
// reflector establishment (see recordReflectorPath and
// reflectorPathEstablishment). The asymmetry is the point: an inflated counter
// is a measurement an operator can caveat, whereas the map drives a WARN, and
// a WARN naming a cause that did not happen actively misleads whoever it
// wakes. We are shipping this work to delete instruments that say untrue
// things; this one must not add another.
package cache

import (
	"expvar"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/transport"
)

// The five wire shapes, plus the two denominators' worth of context. Values are
// the strings published in the per-GVR map, so they are part of the surface.
const (
	// reflectorPathWatchList — watch=true & sendInitialEvents=true: the
	// initial state is streamed from the apiserver watch cache.
	reflectorPathWatchList = "watchlist"
	// reflectorPathWatchPlain — watch=true with no sendInitialEvents: the
	// ordinary re-watch after a clean timeout. Happens under BOTH regimes, so
	// it is not a discriminator; it is the proof the instrument is wired.
	reflectorPathWatchPlain = "watch_plain"
	// reflectorPathListPage — a continue page. A continue token is an etcd
	// read by definition and its page size is what bounds it, so this is
	// correct traffic, not a bypass.
	reflectorPathListPage = "list_page"
	// reflectorPathListEtcdDelegated — limit + a resourceVersion that is
	// neither empty nor "0", no continue. By client-go's own documented rule
	// this LIST is delegated to etcd and SKIPS the watch cache. It is non-zero
	// on today's binary by construction (listOptionsTweak sets Limit
	// unconditionally) and must read 0 once C1 ships.
	reflectorPathListEtcdDelegated = "list_etcd_delegated"
	// reflectorPathListCacheEligible — every other LIST: RV ""/"0", or no
	// limit. The cacher can serve these.
	reflectorPathListCacheEligible = "list_cache_eligible"
)

// reflectorPathList is the second per-GVR path value: the classic
// list()+watch() pair, whatever the LIST's watch-cache eligibility.
const reflectorPathList = "list"

// reflectorPathEstablishment collapses a bucket to the per-GVR PATH, or "" for
// a request that establishes none.
//
// Two exclusions, each closing a way the WARN could name a cause that did not
// happen:
//
//   - A continue page is a continuation of a list already attributed, and a
//     plain re-watch occurs under BOTH regimes. Letting either rewrite the
//     recorded path would emit a transition WARN on ordinary watch recycling,
//     which is the one thing that would make the WARN unreadable.
//
//   - A LIST that carries NO resourceVersion is not a reflector LIST.
//     `r.list()` always sets one — `relistResourceVersion()` returns "0" on the
//     initial sync and the last synced RV afterwards — whereas our own
//     passthrough and fallthrough LISTs build `metav1.ListOptions{Limit: …,
//     Continue: …}` with the field unset (watcher.go listPassthrough). Without
//     this, a fallthrough LIST for a registered-but-unservable GVR would
//     rewrite an established "watchlist" to "list" and the next real
//     re-establishment would flip it back — a WARN and a transition tick per
//     flip, both naming a reflector event that never occurred. The residual is
//     stated rather than hidden: a reflector relisting after an HTTP 410 sends
//     RV="" and that one establishment goes unrecorded; the next one records it.
//
// THE resourceVersion RULE MUST NOT BE APPLIED TO THE WATCH-LIST BUCKET, and
// this is the trap someone will otherwise "simplify" into existence. watchList
// builds its request from rewatchResourceVersion(), which returns the EMPTY
// lastSyncResourceVersion at boot, so the very first watch-list request carries
// no resourceVersion param at all. Gating it the same way would make A3 blind
// to watch-list establishment at boot — the single reading this file exists to
// provide. The switch returns for reflectorPathWatchList BEFORE the check for
// exactly that reason; keep it that way.
func reflectorPathEstablishment(bucket string, hasResourceVersion bool) string {
	switch bucket {
	case reflectorPathWatchList:
		return reflectorPathWatchList
	case reflectorPathListCacheEligible, reflectorPathListEtcdDelegated:
		if !hasResourceVersion {
			return ""
		}
		return reflectorPathList
	default:
		return ""
	}
}

// reflectorPaths holds the per-GVR attribution state: which GVRs currently
// have a registered informer, and the reflector path each is on. Plain maps
// under one mutex, not sync.Maps: `registered` is written only at informer
// registration and teardown, `current` only at LIST/WATCH establishment (a
// handful per GVR per hour, never per request), and the read side is a
// JWT-gated debug scrape.
//
// WHY `registered` IS MIRRORED HERE RATHER THAN READ FROM rw.informers. The
// classifier runs INSIDE every outbound request, on whichever goroutine issued
// it. If it took rw.mu, then any code path that ever performed an apiserver
// request while holding rw.mu would deadlock the entire client — and
// rw.mu.RLock on a goroutine already holding rw.mu.Lock deadlocks outright,
// because sync.RWMutex is not reentrant. No such path exists today (the
// discovery pre-check at EnsureResourceType releases rw.mu before calling
// discovery), but that is an invariant of OTHER code, and an instrument must
// not be the thing that turns a future refactor into a hung pod. Mirroring
// costs two map writes on the registration path and removes the hazard by
// construction.
//
// LOCK ORDER, stated because this introduces a new nesting: reflectorPaths.mu
// is taken INSIDE rw.mu at the two lifecycle sites (rememberReflectorPathGVR
// from addResourceTypeLocked, forgetReflectorPath from
// deletePerGVRStateLocked), and ALONE everywhere else (recordReflectorPath on
// the requesting goroutine, ReflectorPathsSnapshot on the scrape). Nothing
// takes rw.mu while holding reflectorPaths.mu, so there is no cycle.
//
// BOTH MAPS ARE PRUNED ON TEARDOWN. `deletePerGVRStateLocked` is the single
// de-registration site precisely so a per-GVR map cannot be forgotten, and
// leaving this one out would be #219's leak shape verbatim: pruneUnservedGVRs
// retires a composition version on every CRD upgrade, so the snapshot would
// accumulate a permanent row per retired version and an operator would read a
// reflector path for a GVR that no longer exists — mid-incident, from the
// instrument built to stop exactly that class of misreading.
var reflectorPaths = struct {
	mu         sync.Mutex
	registered map[schema.GroupVersionResource]struct{}
	current    map[schema.GroupVersionResource]string
}{
	registered: map[schema.GroupVersionResource]struct{}{},
	current:    map[schema.GroupVersionResource]string{},
}

// rememberReflectorPathGVR records that gvr now has a registered informer.
// Called from addResourceTypeLocked, which holds rw.mu — see the lock-order
// note above.
func rememberReflectorPathGVR(gvr schema.GroupVersionResource) {
	reflectorPaths.mu.Lock()
	reflectorPaths.registered[gvr] = struct{}{}
	reflectorPaths.mu.Unlock()
}

// forgetReflectorPath drops every trace of gvr. Called from
// deletePerGVRStateLocked — the single de-registration site — which holds
// rw.mu.
//
// The path is dropped along with the registration on purpose: a teardown and
// rebuild (a CRD schema relist) SHOULD log the fresh informer's path as a new
// first observation, because "which path did this GVR come back on?" is
// exactly what an operator wants confirmed after a relist, and today that
// event is silent.
func forgetReflectorPath(gvr schema.GroupVersionResource) {
	reflectorPaths.mu.Lock()
	delete(reflectorPaths.registered, gvr)
	delete(reflectorPaths.current, gvr)
	reflectorPaths.mu.Unlock()
}

// ReflectorPathWrapper returns the transport wrapper. Install it on the
// in-cluster rest.Config BEFORE the first client is built, composing rather
// than assigning:
//
//	rc.WrapTransport = transport.Wrappers(rc.WrapTransport, cache.ReflectorPathWrapper())
//
// It counts and delegates — one atomic add and a non-allocating path scan; no
// request modified, no response read, no body touched.
func ReflectorPathWrapper() transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		if rt == nil {
			return rt
		}
		return &reflectorPathRoundTripper{next: rt}
	}
}

type reflectorPathRoundTripper struct{ next http.RoundTripper }

func (rt *reflectorPathRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	classifyReflectorRequest(req)
	return rt.next.RoundTrip(req)
}

// WrappedRoundTripper keeps client-go able to unwrap the chain (it walks it to
// cancel in-flight requests). A wrapper that hides its delegate breaks
// cancellation, which would be a real behaviour change from an instrument.
func (rt *reflectorPathRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.next }

// classifyReflectorRequest buckets one outbound request.
//
// COST ON THE HOT PATH, which matters because this wrapper sits on the SHARED
// rest.Config and every fallthrough GET-by-name pays it: one atomic add, then
// a NON-ALLOCATING scan of the URL path. parseResourcePath walks the path with
// IndexByte and returns substrings that share the request's own backing array,
// so the overwhelmingly common miss — a named GET — allocates nothing and
// never parses the query. Non-GET verbs exit one branch earlier still.
func classifyReflectorRequest(req *http.Request) {
	if req == nil || req.URL == nil {
		return
	}
	reflectorTransportRequestsTotal.Add(1)
	if req.Method != http.MethodGet {
		return
	}
	gvr, collection, ok := parseResourcePath(req.URL.Path)
	if !ok || !collection {
		return
	}
	reflectorCollectionRequestsTotal.Add(1)

	q := req.URL.Query()
	bucket := classifyCollectionQuery(q)
	switch bucket {
	case reflectorPathWatchList:
		reflectorWatchListEstablishedTotal.Add(1)
	case reflectorPathWatchPlain:
		reflectorWatchPlainTotal.Add(1)
	case reflectorPathListPage:
		reflectorListPageTotal.Add(1)
	case reflectorPathListEtcdDelegated:
		reflectorListEtcdDelegatedTotal.Add(1)
	case reflectorPathListCacheEligible:
		reflectorListCacheEligibleTotal.Add(1)
	}
	recordReflectorPath(gvr, bucket, q.Get("resourceVersion") != "")
}

// classifyCollectionQuery maps a collection GET's query to its bucket. Called
// only for requests already known to be collection GETs.
// The "1" spellings are defensive only: client-go's parameter codec always
// emits "true", and the narrower strconv.ParseBool spellings (t/T/True/TRUE)
// are deliberately not accepted, since a request carrying one did not come
// from a client-go informer and should not be attributed to a reflector.
func classifyCollectionQuery(q url.Values) string {
	if w := q.Get("watch"); w == "true" || w == "1" {
		if s := q.Get("sendInitialEvents"); s == "true" || s == "1" {
			return reflectorPathWatchList
		}
		return reflectorPathWatchPlain
	}
	if q.Get("continue") != "" {
		return reflectorPathListPage
	}
	rv := q.Get("resourceVersion")
	if q.Get("limit") != "" && rv != "" && rv != "0" {
		return reflectorPathListEtcdDelegated
	}
	return reflectorPathListCacheEligible
}

// parseResourcePath splits a Kubernetes API path into its GVR and reports
// whether it addresses a COLLECTION (no name segment) rather than a single
// object or a subresource.
//
//	/api/v1/configmaps                         → (v1 configmaps, collection)
//	/api/v1/namespaces/krateo-system/configmaps→ (v1 configmaps, collection)
//	/api/v1/namespaces/krateo-system/configmaps/cm-1 → (…, NOT a collection)
//	/api/v1/namespaces                         → (v1 namespaces, collection)
//	/api/v1/namespaces/krateo-system           → (v1 namespaces, NOT a collection)
//	/apis/widgets…/v1beta1/pageheaders         → (…, collection)
//	/version, /openapi/v2, /apis               → not a resource path at all
//
// ONE KNOWN CORNER, pre-existing and bounded: a namespace SUBRESOURCE path —
// /api/v1/namespaces/<ns>/status, /finalize — parses as a collection LIST of a
// resource literally named "status". It behaved identically under the previous
// strings.Split implementation, so this is not a regression, and the effect
// stops at a counter: GVR{v1, status} is never a registered informer, so the
// registered gate keeps it out of the per-GVR map and out of the WARN. What
// remains is one increment in list_cache_eligible, already covered by the
// honest-scope note in the file header. Recorded so the next reader does not
// rediscover it and take it for something new.
//
// It ALLOCATES NOTHING: cutSegment returns substrings that share p's backing
// array, so the common case — a named GET falling through to the apiserver —
// costs a handful of IndexByte scans and no garbage. That matters because the
// wrapper is on the shared rest.Config and this runs on the serve path's own
// goroutine.
func parseResourcePath(p string) (schema.GroupVersionResource, bool, bool) {
	var none schema.GroupVersionResource
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")

	root, rest, ok := cutSegment(p)
	if !ok {
		return none, false, false
	}
	var group, version string
	switch root {
	case "api":
		if version, rest, ok = cutSegment(rest); !ok {
			return none, false, false
		}
	case "apis":
		if group, rest, ok = cutSegment(rest); !ok {
			return none, false, false
		}
		if version, rest, ok = cutSegment(rest); !ok {
			return none, false, false
		}
	default:
		return none, false, false
	}

	resource, rest, ok := cutSegment(rest)
	if !ok {
		return none, false, false
	}
	// A namespace-scoped path interposes /namespaces/<ns>/ — unless it is
	// addressing the namespaces resource itself (a collection when nothing
	// follows, a named GET when one segment does).
	if resource == "namespaces" {
		if _, afterNS, nsOK := cutSegment(rest); nsOK {
			if scoped, tail, scopedOK := cutSegment(afterNS); scopedOK {
				resource, rest = scoped, tail
			} else {
				return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, false, true
			}
		}
	}
	if resource == "" {
		return none, false, false
	}
	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
	return gvr, rest == "", true
}

// cutSegment splits off the first '/'-delimited segment of p. Both returned
// strings share p's backing array — no allocation.
func cutSegment(p string) (seg, rest string, ok bool) {
	if p == "" {
		return "", "", false
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:], true
	}
	return p, "", true
}

// recordReflectorPath updates the per-GVR path and emits the transition WARN.
//
// WARN, NOT INFO, AND ONLY ON A TRANSITION. The chart ships LOG_LEVEL=warn
// (measured on the live pod: 217 WARN + 30 ERROR, zero INFO over 20h), so an
// INFO line would be invisible exactly when it is needed — the same trap that
// made #237's reflector-path question unanswerable from a log. Steady state is
// therefore one line per GVR at boot and silence afterwards; a flip from
// watchlist to list mid-life is an event worth waking someone for. NEVER log
// per request.
// ONLY A GVR WITH A REGISTERED INFORMER IS RECORDED, and that gate is the
// difference between an instrument and a misleading alarm. The wrapper sees
// every collection GET on the shared rest.Config, including LISTs that no
// reflector issued:
//
//   - in modePassthrough (CACHE_ENABLED=false, a supported diagnostic mode)
//     there are NO reflectors at all, yet listPassthrough issues a paged LIST
//     per call. Ungated, the first one would emit a per-GVR WARN whose own
//     hint says the apiserver stopped honouring SendInitialEvents — an alarm
//     asserting a cause that did not merely fail to happen, but cannot happen
//     in that mode;
//   - a fallthrough LIST for a registered-but-unservable GVR is excluded by
//     the resourceVersion rule in reflectorPathEstablishment instead, since
//     that GVR *is* registered.
//
// The counters are deliberately NOT gated: an extra LIST in
// list_cache_eligible is an inflated total an operator can caveat, whereas a
// WARN naming the wrong cause actively misleads whoever it wakes. That
// asymmetry is the whole reason the two are treated differently, and it is why
// the honest-scope paragraph in the file header applies to the counters only.
func recordReflectorPath(gvr schema.GroupVersionResource, bucket string, hasResourceVersion bool) {
	path := reflectorPathEstablishment(bucket, hasResourceVersion)
	if path == "" {
		return
	}
	reflectorPaths.mu.Lock()
	if _, registered := reflectorPaths.registered[gvr]; !registered {
		reflectorPaths.mu.Unlock()
		return
	}
	previous, seen := reflectorPaths.current[gvr]
	if seen && previous == path {
		reflectorPaths.mu.Unlock()
		return
	}
	reflectorPaths.current[gvr] = path
	reflectorPaths.mu.Unlock()

	reflectorPathTransitionsTotal.Add(1)
	slog.Warn("cache.reflector.path",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("path", path),
		slog.String("previous", previous),
		slog.Bool("first_observation", !seen),
		slog.String("wire_shape", bucket),
		slog.String("hint", "the path this GVR's reflector takes on the wire; a flip from "+
			"watchlist to list means the apiserver stopped honouring SendInitialEvents"),
	)
}

// ReflectorPathsSnapshot returns a copy of the per-GVR path map, for the
// /debug/vars sibling of the counter family.
func ReflectorPathsSnapshot() map[string]string {
	reflectorPaths.mu.Lock()
	defer reflectorPaths.mu.Unlock()
	out := make(map[string]string, len(reflectorPaths.current))
	for gvr, path := range reflectorPaths.current {
		out[gvr.String()] = path
	}
	return out
}

var reflectorPathExpvarOnce sync.Once

// registerReflectorPathExpvar publishes the tagged family plus the per-GVR
// map. The map rides alongside rather than inside the family: a stat tag
// yields one scalar and the C7 tag system has no label facility, so a
// per-GVR breakdown has to be its own map — same shape as
// snowplow_informer_confirm_retracted_by_reason.
//
// The per-GVR map is what answers "has THIS GVR relisted, and how?", which
// #237 recorded as unanswerable from outside the process: watch_errors_total
// is a single global total that cannot separate one GVR failing in a loop from
// every GVR recycling normally.
func registerReflectorPathExpvar() {
	reflectorPathExpvarOnce.Do(func() {
		expvar.Publish("snowplow_reflector_path", expvar.Func(func() any {
			return ReflectorPathStatsByStat()
		}))
		expvar.Publish("snowplow_reflector_path_by_gvr", expvar.Func(func() any {
			return ReflectorPathsSnapshot()
		}))
	})
}

// RegisterReflectorPathExpvarForTest exposes the publish for tests.
func RegisterReflectorPathExpvarForTest() { registerReflectorPathExpvar() }

func init() {
	if Disabled() {
		return
	}
	registerReflectorPathExpvar()
}
