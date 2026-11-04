package api

// servergroups_parity_1115_test.go — the #1122 reviewer's parity arm, adopted
// (#1115 N1/F1): pluralExistsInGroup, now on serverGroups (client-go's
// ServerGroups transcribed, sent on the request's ctx), against the previous
// implementation on client-go's own ServerGroups, over recorded discovery
// documents: legacy v1 and aggregated v2, stale versions, 403/404/500 answers.
// Zero outcome differences: the move onto ctx changes no /rbac answer.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// pluralExistsInGroupOld is 5d4acca9's pluralExistsInGroup, verbatim but for
// the name (client-go ServerGroups + ServerResourcesForGroupVersion).
func pluralExistsInGroupOld(rc *rest.Config, group, plural string) (bool, error) {
	cli, err := discoveryClientFor(rc)
	if err != nil {
		return false, err
	}
	groups, err := cli.ServerGroups()
	if err != nil {
		return false, fmt.Errorf("discovery ServerGroups: %w", err)
	}
	var versions []string
	for _, g := range groups.Groups {
		if g.Name != group {
			continue
		}
		for _, v := range g.Versions {
			versions = append(versions, v.Version)
		}
		break
	}
	if len(versions) == 0 {
		return false, nil
	}
	for _, v := range versions {
		gv := schema.GroupVersion{Group: group, Version: v}.String()
		list, lerr := cli.ServerResourcesForGroupVersion(gv)
		if lerr != nil {
			continue
		}
		if pluralServed(list, plural) {
			return true, nil
		}
	}
	return false, nil
}

const aggCT = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"

type parityScenario struct {
	name string
	// status overrides per path (0 = 200 with the document)
	apiStatus, apisStatus int
	aggregated            bool   // answer v2 aggregated when the client accepts it
	staleVersion          string // version marked Stale in aggregated discovery
	gvStatus              map[string]int
}

func parityServer(t *testing.T, sc parityScenario, hits *atomic.Int32) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		acceptsAgg := strings.Contains(r.Header.Get("Accept"), "apidiscovery.k8s.io")
		fail := func(code int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":%d,"reason":"%s"}`, code, map[int]string{403: "Forbidden", 404: "NotFound", 500: "InternalError", 503: "ServiceUnavailable"}[code])
		}
		switch r.URL.Path {
		case "/api":
			if sc.apiStatus != 0 {
				fail(sc.apiStatus)
				return
			}
			if sc.aggregated && acceptsAgg {
				w.Header().Set("Content-Type", aggCT)
				_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","items":[{"metadata":{"name":""},"versions":[{"version":"v1","resources":[{"resource":"configmaps","responseKind":{"group":"","version":"v1","kind":"ConfigMap"},"scope":"Namespaced","verbs":["get","list"]}]}]}]}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`)
		case "/apis":
			if sc.apisStatus != 0 {
				fail(sc.apisStatus)
				return
			}
			if sc.aggregated && acceptsAgg {
				w.Header().Set("Content-Type", aggCT)
				v := func(ver string) string {
					fresh := ""
					if ver == sc.staleVersion {
						fresh = `,"freshness":"Stale"`
					}
					return `{"version":"` + ver + `"` + fresh + `,"resources":[{"resource":"widgets","responseKind":{"group":"g.example.io","version":"` + ver + `","kind":"Widget"},"scope":"Namespaced","verbs":["get","list"]}]}`
				}
				_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","items":[{"metadata":{"name":"g.example.io"},"versions":[`+v("v2")+`,`+v("v1")+`]}]}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"g.example.io","versions":[{"groupVersion":"g.example.io/v2","version":"v2"},{"groupVersion":"g.example.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"g.example.io/v2","version":"v2"}}]}`)
		case "/api/v1":
			if c := sc.gvStatus["v1"]; c != 0 {
				fail(c)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`)
		case "/apis/g.example.io/v1", "/apis/g.example.io/v2":
			ver := strings.TrimPrefix(r.URL.Path, "/apis/g.example.io/")
			if c := sc.gvStatus[ver]; c != 0 {
				fail(c)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			res := "widgets"
			if ver == "v1" {
				res = "gadgets" // v1 serves gadgets, v2 serves widgets
			}
			_, _ = io.WriteString(w, `{"kind":"APIResourceList","groupVersion":"g.example.io/`+ver+`","resources":[{"name":"`+res+`","namespaced":true,"kind":"X","verbs":["get","list"]}]}`)
		default:
			fail(404)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestServerGroups1115_ParityWithClientGo(t *testing.T) {
	scenarios := []parityScenario{
		{name: "legacy"},
		{name: "aggregated"},
		{name: "aggregated, v2 stale (its GV endpoint answers)", aggregated: true, staleVersion: "v2"},
		{name: "aggregated, v2 stale and its GV endpoint 503", aggregated: true, staleVersion: "v2", gvStatus: map[string]int{"v2": 503}},
		{name: "/api 404 (aggregated-only server)", apiStatus: 404},
		{name: "/api 403", apiStatus: 403},
		{name: "/api 500", apiStatus: 500},
		{name: "/apis 403", apisStatus: 403},
		{name: "/apis 404", apisStatus: 404},
		{name: "core v1 GV 404", gvStatus: map[string]int{"v1": 404}},
	}
	scenarios[1].aggregated = true
	cases := []struct{ group, plural string }{
		{"g.example.io", "widgets"}, {"g.example.io", "gadgets"}, {"g.example.io", "nothing"}, {"", "configmaps"}, {"absent.io", "x"},
	}
	diffs := 0
	for _, sc := range scenarios {
		for _, c := range cases {
			var hOld, hNew atomic.Int32
			srvOld := parityServer(t, sc, &hOld)
			resetDiscoveryClientCacheForTest()
			oOK, oErr := pluralExistsInGroupOld(&rest.Config{Host: srvOld.URL}, c.group, c.plural)
			srvNew := parityServer(t, sc, &hNew)
			resetDiscoveryClientCacheForTest()
			nOK, nErr := pluralExistsInGroup(context.Background(), &rest.Config{Host: srvNew.URL}, c.group, c.plural)
			same := oOK == nOK && (oErr == nil) == (nErr == nil)
			mark := "same"
			if !same {
				mark = "DIFF"
				diffs++
			}
			t.Logf("%-4s | %-48s | %q/%-10s | old=(%v, err=%v) req=%d | new=(%v, err=%v) req=%d",
				mark, sc.name, c.group, c.plural, oOK, oErr != nil, hOld.Load(), nOK, nErr != nil, hNew.Load())
		}
	}
	resetDiscoveryClientCacheForTest()
	t.Logf("%d outcome difference(s)", diffs)
	if diffs > 0 {
		t.Fail()
	}
}
