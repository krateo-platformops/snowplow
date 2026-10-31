package main

// main_telemetry_1020_routes_test.go — #1020 F3, Diego's ruling 2026-10-30:
// "exact where possible, trust the rest". Same harness as the other #1015
// arms (real router, RefreshAuth, Ingest, Forwarder); the ingest's RouteSet is
// rum.RegisteredRoutes over the harness mux, which carries snowplow's /call
// routes (GET /call, plus mountCallWriteRoutes' write verbs and /call/dry-run).

import (
	"fmt"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"
)

// Exact where possible: a krateo.widget.fetch http.route must be EXACTLY one
// of snowplow's own registered routes, derived from the mux; anything else,
// even a well-formed template, is dropped and counted.
//
// MUTATIONS (RED, shown in the PR): the widget-fetch branch falling back to
// the page-level checks (a portal template is forwarded); RegisteredRoutes
// matching by prefix instead of exactly (an unregistered path is forwarded).
func TestS1020_WidgetFetchRouteMustBeSnowplowRoute(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	for i, route := range []string{"/call", "/call/dry-run"} {
		if !routeKeptOn(t, h, c, fmt.Sprintf("wf-ok-%d", i), rum.SpanWidgetFetch, route) {
			t.Errorf("widget-fetch route %q is a registered snowplow route and was dropped", route)
		}
	}

	notSnowplow := []string{
		"/compositions/:namespace/:name", // a well-formed template, but not a snowplow route
		"/call/zq1020-person-name",       // a concrete path under a snowplow route
		"/call/",                         // trailing slash: not the registered pattern
		"/calls",                         // a near miss
		"/",                              // root
	}
	before := rum.Read()
	for i, route := range notSnowplow {
		if routeKeptOn(t, h, c, fmt.Sprintf("wf-no-%d", i), rum.SpanWidgetFetch, route) {
			t.Errorf("widget-fetch route %q is not a registered snowplow route and was forwarded", route)
		}
	}
	if d := rum.Read().UnregisteredWidgetRoutes - before.UnregisteredWidgetRoutes; d != uint64(len(notSnowplow)) {
		t.Errorf("widget_routes_unregistered delta %d, want %d (one per dropped route)", d, len(notSnowplow))
	}
}

// Trust the rest: on the page-level spans the route is the portal router's
// pattern, which snowplow cannot know, so it is forwarded as sent when it
// passes today's checks, and dropped when it does not.
//
// MUTATION (RED, shown in the PR): the page-level branch held to snowplow's
// registered routes (the portal's templates are dropped).
func TestS1020_PageRouteAcceptedAsSent(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	pageSpans := []string{"krateo.page.load", "krateo.composition.open", "krateo.form.submit", "krateo.page.publish"}
	for i, route := range []string{"/compositions/:namespace/:name", "/compositions/{namespace}/{name}", "/dashboard"} {
		span := pageSpans[i%len(pageSpans)]
		if !routeKeptOn(t, h, c, fmt.Sprintf("pg-ok-%d", i), span, route) {
			t.Errorf("%s route %q is a portal template that passes the checks and was dropped", span, route)
		}
	}
	before := rum.Read()
	for i, route := range []string{"/compositions/x?name=a", "/s/abcdefghijklmnopqrstuvwx"} {
		if routeKeptOn(t, h, c, fmt.Sprintf("pg-no-%d", i), "krateo.page.publish", route) {
			t.Errorf("page route %q fails today's checks (query / token-shaped) and was forwarded", route)
		}
	}
	if d := rum.Read().UnregisteredWidgetRoutes - before.UnregisteredWidgetRoutes; d != 0 {
		t.Errorf("widget_routes_unregistered moved by %d on page-level spans; it counts widget fetches only", d)
	}
}
