package rum

import (
	"net/http"
	"net/url"
	"strings"
)

// SpanWidgetFetch is the span whose http.route names a SNOWPLOW route: the
// portal widget's fetch to snowplow (/call, /call/read, …). It is the only
// span name whose route is checked against snowplow's own routes.
const SpanWidgetFetch = "krateo.widget.fetch"

// RouteSet reports whether a route (a path such as "/call") is one of the
// routes snowplow itself serves.
type RouteSet func(route string) bool

// WidgetSet looks a widget up in snowplow's own informer cache, in process and
// without an apiserver call. known reports whether resource is a plural snowplow
// serves widgets for; exists, asked with a namespace and name, whether that widget
// is in the cache. An implementation that cannot tell (cache off, informer not
// synced) answers false, false. A nil WidgetSet knows nothing.
type WidgetSet func(resource, namespace, name string) (known, exists bool)

// registeredMethods are the methods a route is looked up under: every method
// snowplow registers a pattern for.
var registeredMethods = [...]string{
	http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
}

// RegisteredRoutes is the RouteSet of the routes registered on mux, derived
// from the mux itself and never from a hand-written list (Diego, 2026-10-30,
// #1020 F3: "exact where possible"). ServeMux cannot enumerate its patterns,
// so a route is checked by asking the mux which pattern WOULD serve it, under
// each method: the route is snowplow's exactly when the path half of that
// pattern equals it. That is the lookup ServeMux.ServeHTTP performs, so the
// set is always the live routing table, including routes registered after the
// ingest was built. A subtree pattern ("/swagger/") matches only itself, not
// "/swagger/<name>"; a path the mux would redirect (unclean, or missing its
// trailing slash) does not match, because the pattern it resolves to differs.
func RegisteredRoutes(mux *http.ServeMux) RouteSet {
	return func(route string) bool {
		if !strings.HasPrefix(route, "/") {
			return false
		}
		for _, m := range registeredMethods {
			r := &http.Request{Method: m, URL: &url.URL{Path: route}, Header: http.Header{}}
			if _, pattern := mux.Handler(r); patternPath(pattern) == route {
				return true
			}
		}
		return false
	}
}

// patternPath is the path half of a ServeMux pattern ("GET /call" → "/call").
func patternPath(pattern string) string {
	if i := strings.IndexByte(pattern, '/'); i >= 0 {
		return pattern[i:]
	}
	return ""
}
