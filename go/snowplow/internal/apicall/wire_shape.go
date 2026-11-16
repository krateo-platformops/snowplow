package apicall

// wire_shape.go — the path-shape classifier of an OUTBOUND request: which
// resource collection a request on the wire addresses, and which of the five
// collection-GET shapes it is. Two instruments read it and must agree:
//
//   - the reflector-path attribution (internal/cache reflector_path.go, #237),
//     which buckets every collection GET on the watcher's shared rest.Config;
//   - the resolve collection-LIST counter (internal/credhttp
//     resolve_lists.go, #988), which counts the collection GETs a resolve
//     sends through any config or client credhttp hardens.
//
// It was written for the first and moved here unchanged so the second reuses
// it instead of copying it.
//
// Classification is on the request PATH SHAPE, not on query tokens: a GET
// whose path resolves to a COLLECTION endpoint (/apis/<g>/<v>/<r>,
// /api/<v>/<r>, either with an interposed /namespaces/<ns>/) is a LIST or a
// WATCH; named GETs, subresources, discovery and non-resource paths are not
// collection GETs at all. A query-token pre-scan (`limit=`/`watch=`) would go
// blind to a relist at a non-zero resourceVersion, which carries neither
// token.
//
// ParsePath (apicall.go) answers a different question: the (namespace, name)
// a RESOLVED stage path addresses, query string included. ParseResourcePath
// reads a request URL's path alone and allocates nothing, because it runs
// inside every outbound request.

import (
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The five collection-GET wire shapes. The values are published by the
// reflector-path instruments, so they are part of that surface.
const (
	// WireWatchList — watch=true & sendInitialEvents=true: the initial state
	// is streamed from the apiserver watch cache.
	WireWatchList = "watchlist"
	// WireWatchPlain — watch=true with no sendInitialEvents: the ordinary
	// re-watch after a clean timeout.
	WireWatchPlain = "watch_plain"
	// WireListPage — a continue page: the continuation of a LIST whose first
	// page was already sent.
	WireListPage = "list_page"
	// WireListEtcdDelegated — limit + a resourceVersion that is neither empty
	// nor "0", no continue. By client-go's documented rule this LIST is
	// delegated to etcd and skips the watch cache.
	WireListEtcdDelegated = "list_etcd_delegated"
	// WireListCacheEligible — every other LIST: RV ""/"0", or no limit. The
	// cacher can serve these.
	WireListCacheEligible = "list_cache_eligible"
)

// ClassifyCollectionQuery maps a collection GET's query to its wire shape.
// Call it only for a request already known to be a collection GET.
//
// The "1" spellings are defensive only: client-go's parameter codec always
// emits "true", and the narrower strconv.ParseBool spellings (t/T/True/TRUE)
// are deliberately not accepted, since a request carrying one did not come
// from a client-go informer.
func ClassifyCollectionQuery(q url.Values) string {
	if w := q.Get("watch"); w == "true" || w == "1" {
		if s := q.Get("sendInitialEvents"); s == "true" || s == "1" {
			return WireWatchList
		}
		return WireWatchPlain
	}
	if q.Get("continue") != "" {
		return WireListPage
	}
	rv := q.Get("resourceVersion")
	if q.Get("limit") != "" && rv != "" && rv != "0" {
		return WireListEtcdDelegated
	}
	return WireListCacheEligible
}

// ParseResourcePath splits a Kubernetes API request path into its GVR and
// reports whether it addresses a COLLECTION (no name segment) rather than a
// single object or a subresource.
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
// /api/v1/namespaces/<ns>/status, /finalize — parses as a collection of a
// resource literally named "status" (or "finalize"). The effect stops at a
// counter cell for that pseudo-GVR, which no informer registers and no real
// resource carries, so it can never be read as a real GVR's count.
//
// It ALLOCATES NOTHING: cutSegment returns substrings that share p's backing
// array, so the common case — a named GET falling through to the apiserver —
// costs a handful of IndexByte scans and no garbage. That matters because the
// classifier runs inside every outbound request, on the caller's goroutine.
func ParseResourcePath(p string) (schema.GroupVersionResource, bool, bool) {
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
