package main

// main_audit_routes_test.go — #1379 review F2: which routes are audited is
// declared once, at registration (routeTable), and a route that can change
// something cannot be mounted without saying whether it is audited.
//
//   P  Handle and handleDebug panic for a pattern whose method can change
//      something (POST, PUT, PATCH, DELETE, or no method), and accept GET,
//      HEAD and OPTIONS; HandleAudited panics with no descriptor and
//      HandleUnaudited with no reason.
//   M  the mount functions main uses declare every route they mount: the
//      write /call and /call/dry-run routes, both /export routes and POST
//      /debug/shadow-parity (#909) audited, the telemetry ingest unaudited,
//      with its reason.

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", name)
		}
	}()
	f()
}

func TestAudit_RouteTable_P_ARouteThatCanChangeSomethingMustBeDeclared(t *testing.T) {
	h := http.NotFoundHandler()
	for _, p := range []string{"POST /x", "PUT /x", "PATCH /x", "DELETE /x", "/x"} {
		mustPanic(t, "Handle("+p+")", func() { newRouteTable(http.NewServeMux()).Handle(p, h) })
	}
	for _, p := range []string{"POST /debug/x", "/debug/x"} {
		mustPanic(t, "handleDebug("+p+")", func() { newRouteTable(http.NewServeMux()).handleDebug(p, h) })
	}
	for _, p := range []string{"GET /x", "HEAD /x", "OPTIONS /x"} {
		newRouteTable(http.NewServeMux()).Handle(p, h) // accepted
	}
	mustPanic(t, "HandleAudited with no descriptor", func() {
		newRouteTable(http.NewServeMux()).HandleAudited("POST /x", audit.Route{}, h)
	})
	mustPanic(t, "HandleUnaudited with no reason", func() {
		newRouteTable(http.NewServeMux()).HandleUnaudited("POST /x", "  ", h)
	})
	mustPanic(t, "declareUnaudited with no reason", func() {
		newRouteTable(http.NewServeMux()).declareUnaudited("POST /debug/x", "")
	})
	declared := newRouteTable(http.NewServeMux())
	declared.declareUnaudited("POST /debug/x", "a reason")
	declared.handleDebug("POST /debug/x", h) // accepted once declared
}

func TestAudit_RouteTable_M_EveryMountedRouteIsDeclared(t *testing.T) {
	rt := newRouteTable(http.NewServeMux())
	pass := func(next http.Handler) http.Handler { return next }
	mountCallWriteRoutes(rt, use.NewChain(), pass)
	mountExportRoutes(rt, use.NewChain(), pass)
	mountTelemetryRoute(rt, use.NewChain(), debugAuthTestKeys, nil)
	registerDebugRoutes(rt, use.NewChain(), debugAuthTestKeys)

	audited := []string{
		"POST /call", "PUT /call", "PATCH /call", "DELETE /call",
		"POST /call/dry-run", "PUT /call/dry-run", "PATCH /call/dry-run",
		"GET /export", "POST /export",
		"POST /debug/shadow-parity", // #909
	}
	for _, p := range audited {
		if _, ok := rt.audited[p]; !ok {
			t.Errorf("%s is not declared audited", p)
		}
	}
	for p := range rt.audited {
		if !slices.Contains(audited, p) {
			t.Errorf("%s is declared audited, not in the expected set", p)
		}
	}
	for _, p := range []string{"POST /telemetry/v1/traces"} {
		if reason, ok := rt.unaudited[p]; !ok || strings.TrimSpace(reason) == "" {
			t.Errorf("%s is not declared unaudited with a reason", p)
		}
	}
	// The descriptors are the handlers' own: a /call write's descriptor
	// names the action the handler records.
	if ev := handlers.CallRoute().Asked(mustRequest(t, http.MethodPut, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x")); ev.Action != "call" {
		t.Errorf("the /call descriptor's action is %q, want call", ev.Action)
	}
}

func mustRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, target, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
