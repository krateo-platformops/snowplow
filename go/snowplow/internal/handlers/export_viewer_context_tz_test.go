package handlers_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/client-go/rest"
)

// The viewer's tz rides the Table export's extras: the GET /export query
// (?extras=) and the POST /export body ({"extras":{...}}). Both re-dispatch
// through the /call lane, whose single extras reader (util.ParseExtras) applies
// the viewer-context rule. Through the real chain:
//
//	a widget that does not declare tz exports the SAME bytes with or without tz
//	(and with a tz that is not an IANA name);
//	a widget that declares tz exports a body that uses it (the control that
//	makes the equality above meaningful: the template does read .tz).
func TestExport_ViewerContextTZ_UndeclaredSameBytes_DeclaredUsesIt(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	t.Cleanup(widgets.SetValidateObjectStatusForTest(func(context.Context, *rest.Config, map[string]any) error { return nil }))
	tableTZEcho = true
	t.Cleanup(func() { tableTZEcho, tableTZDeclared = false, false })

	api := &fakeTableAPI{callers: map[string]int{}}
	srv := httptest.NewUnstartedServer(api)
	srv.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	eps := callerEndpoints(t, srv, "admin", "viewer")
	api.clientconfigs = eps
	t.Setenv("AUTHN_NAMESPACE", authnNS970)
	t.Cleanup(idynamic.SetServiceAccountRESTConfigForTest(&rest.Config{
		Host:            srv.URL,
		BearerToken:     "token-sa",
		TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})},
	}))

	get := exportChain()
	post := handlers.ExportPOST()
	window := "&page=1&perPage=5"

	exportGET := func(extras map[string]any) string {
		u := tableExportURL("csv") + window
		if extras != nil {
			b, _ := json.Marshal(extras)
			u += "&extras=" + url.QueryEscape(string(b))
		}
		rec := serveAsEndpoint(t, get, eps["admin"], "admin", u)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET export: %d %s", rec.Code, trimBody(rec.Body.String()))
		}
		return rec.Body.String()
	}
	exportPOST := func(extras map[string]any) string {
		body := "{}"
		if extras != nil {
			b, _ := json.Marshal(map[string]any{"extras": extras})
			body = string(b)
		}
		req := httptest.NewRequest(http.MethodPost, tableExportURL("csv")+window, strings.NewReader(body))
		ep := eps["admin"]
		req = req.WithContext(xcontext.BuildContext(req.Context(),
			xcontext.WithAccessToken(ep.Token),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"devs"}}),
			xcontext.WithUserConfig(endpoints.Endpoint(ep)),
		))
		rec := httptest.NewRecorder()
		post.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST export: %d %s", rec.Code, trimBody(rec.Body.String()))
		}
		return rec.Body.String()
	}

	rome := map[string]any{"tz": "Europe/Rome"}
	bad := map[string]any{"tz": "UTC'); DROP TABLE x; --"}
	for name, export := range map[string]func(map[string]any) string{"GET": exportGET, "POST": exportPOST} {
		t.Run(name+"/undeclared", func(t *testing.T) {
			tableTZDeclared = false
			plain := export(nil)
			if strings.Contains(plain, "Europe/Rome") {
				t.Fatal("CONTROL: the baseline export already contains the tz")
			}
			if withTZ := export(rome); withTZ != plain {
				t.Fatalf("an undeclared tz changed the export:\n--- without\n%s\n--- with\n%s", trimBody(plain), trimBody(withTZ))
			}
			if withBad := export(bad); withBad != plain {
				t.Fatalf("an invalid tz changed the export:\n%s", trimBody(withBad))
			}
		})
		t.Run(name+"/declared", func(t *testing.T) {
			tableTZDeclared = true
			t.Cleanup(func() { tableTZDeclared = false })
			if got := export(rome); !strings.Contains(got, "Europe/Rome") {
				t.Fatalf("CONTROL: a widget declaring tz must see it in its export (else the undeclared equality proves nothing):\n%s", trimBody(got))
			}
			if got := export(bad); strings.Contains(got, "DROP") {
				t.Fatalf("an invalid tz reached a declaring widget's export:\n%s", trimBody(got))
			}
		})
	}
}
