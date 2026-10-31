package handlers_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/testsupport/pdfcheck"
	authorizationv1 "k8s.io/api/authorization/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// #970 acceptance — the frontend's export button on a Table WIDGET, through
// the REAL /call chain (Export(Dispatcher(dispatchers.All())(Call())), as
// main.go composes GET /export) and the REAL widget resolver: the Table CR,
// its apiRef RESTAction and the RESTAction's apiserver calls are all served
// by a fake apiserver that answers per bearer token (standing in for RBAC).
// Only the CRD-status validation hop is stood in
// (widgets.SetValidateObjectStatusForTest): the Table CRD is not in this repo.
//
// The button sends:
//   - path:   the frontend's jq constant (tableExportJQ);
//   - fields: columns[].valueKey in display order;
//   - header: columns[].title, one per field.
//
// The Table's parent declares a 50-row slice (resourcesRefs slice perPage 50)
// and its data template windows the rows by .slice, as the portal's tables do.

// tableExportJQ is the frontend's jq constant, verbatim.
const tableExportJQ = `[ .status.widgetData.dataSource[]? | map({ (.valueKey): (.stringValue // .decimalValue // (.numberValue|tostring) // (.booleanValue|tostring) // ((.arrayValue // []) | join(", ")) // "") }) | add ]`

const (
	teamARows      = 620 // more than one 500-row export page
	teamSecretRows = 15  // visible to the admin only
)

// The display order. The data template emits the cells in a DIFFERENT order
// (name, owner, ns, size), so the export's column order provably comes from
// fields, not from the cells.
var (
	tableValueKeys = []string{"name", "ns", "size", "owner", "size"}                         // size twice: two views of one value
	tableTitles    = []string{"Name", "Namespace, scope", "Size (MiB)", "Name", "Size, raw"} // two "Name" columns, told apart by position
)

func tableWidgetCR() string {
	var cols []string
	for i := range tableValueKeys {
		cols = append(cols, fmt.Sprintf(`{"valueKey":%q,"title":%q}`, tableValueKeys[i], tableTitles[i]))
	}
	expr := `${ (if .slice then (.rows[.slice.offset : (.slice.offset + .slice.perPage)]) else .rows end) | map([` +
		`{valueKey:"name",kind:"jsonSchemaType",type:"string",stringValue:.name},` +
		`{valueKey:"owner",kind:"jsonSchemaType",type:"string",stringValue:.owner},` +
		`{valueKey:"ns",kind:"jsonSchemaType",type:"string",stringValue:.ns},` +
		`{valueKey:"size",kind:"jsonSchemaType",type:"integer",numberValue:.size}]) }`
	b, _ := json.Marshal(expr)
	return `{"apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Table",` +
		`"metadata":{"name":"rows-table","namespace":"demo"},` +
		`"spec":{"apiRef":{"name":"rows-ra","namespace":"demo"},` +
		`"widgetData":{"columns":[` + strings.Join(cols, ",") + `],"dataSource":[]},` +
		`"widgetDataTemplate":[{"forPath":"dataSource","expression":` + string(b) + `}]}}`
}

// tableParentCR is the Table's parent: it declares the 50-row slice.
const tableParentCR = `{"apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Panel",
	"metadata":{"name":"rows-panel","namespace":"demo"},
	"spec":{"widgetData":{"title":"Rows"},"resourcesRefs":{"items":[{"id":"table","apiVersion":"widgets.templates.krateo.io/v1beta1",
	"resource":"tables","name":"rows-table","namespace":"demo","verb":"GET","slice":{"page":1,"perPage":50}}]}}}`

const tableRESTActionCR = `{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction",
	"metadata":{"name":"rows-ra","namespace":"demo"},
	"spec":{"api":[
	 {"name":"teamA","path":"/api/v1/namespaces/team-a/configmaps",
	  "filter":"[(.teamA.items // [])[] | {name: .metadata.name, ns: .metadata.namespace, size: (.data.size|tonumber), owner: .data.owner}]"},
	 {"name":"teamSecret","path":"/api/v1/namespaces/team-secret/configmaps","continueOnError":true,"errorKey":"teamSecretError",
	  "filter":"[(.teamSecret.items // [])[] | {name: .metadata.name, ns: .metadata.namespace, size: (.data.size|tonumber), owner: .data.owner}]"}],
	 "filter":"{rows: ((.teamA // []) + (.teamSecret | if type == \"array\" then . else [] end))}"}}`

func configMapList(ns, prefix string, n int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"metadata":{"name":"%s-%04d","namespace":%q},"data":{"size":"%d","owner":%q}}`, prefix, i, ns, i, ns)
	}
	return `{"apiVersion":"v1","kind":"ConfigMapList","items":[` + strings.Join(items, ",") + `]}`
}

// fakeTableAPI serves the fixture per token: the admin may list both
// namespaces; the viewer is refused team-secret, as RBAC would.
type fakeTableAPI struct {
	mu      sync.Mutex
	unknown []string
	callers map[string]int
	// clientconfigs are the per-user clientconfig Secrets in the authn
	// namespace, readable by the service account only.
	clientconfigs map[string]endpoints.Endpoint
	saPaths       []string // every request made with the service account
}

const authnNS970 = "krateo-system"

// callerOf is the identity a request authenticates as: its client
// certificate's CN (the clientconfig credential client-go presents), else
// its bearer token; a request presenting both must agree.
func callerOf(r *http.Request) string {
	tok := strings.TrimPrefix(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "token-")
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		cn := r.TLS.PeerCertificates[0].Subject.CommonName
		if tok != "" && tok != cn {
			return "mismatch:" + cn + "/" + tok
		}
		return cn
	}
	return tok // "sa" for the service account's token-sa
}

// callerEndpoints issues each identity a clientconfig: a client certificate
// (CN = the identity) and a bearer token, for a TLS server.
func callerEndpoints(t *testing.T, srv *httptest.Server, users ...string) map[string]endpoints.Endpoint {
	t.Helper()
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca-970"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	b64 := base64.StdEncoding.EncodeToString
	srvCA := b64(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	eps := map[string]endpoints.Endpoint{}
	for i, u := range users {
		key, _ := rsa.GenerateKey(rand.Reader, 2048)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: u, Organization: []string{"devs"}},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
		}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		eps[u] = endpoints.Endpoint{
			ServerURL:                srv.URL,
			CertificateAuthorityData: srvCA,
			ClientCertificateData:    b64(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
			ClientKeyData:            b64(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
			Token:                    "token-" + u,
		}
	}
	return eps
}

func serveAsEndpoint(t *testing.T, h http.Handler, ep endpoints.Endpoint, user, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken(ep.Token),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"devs"}}),
		xcontext.WithUserConfig(ep),
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// discovery is the fixture's API discovery documents. snowplow discovers
// kinds with its own (service-account) client, which carries no caller
// credential in a unit test, so discovery is served without one; it holds
// no row data.
var discovery = map[string]string{
	"/api": `{"kind":"APIVersions","versions":["v1"]}`,
	"/apis": `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
		`{"name":"widgets.templates.krateo.io","versions":[{"groupVersion":"widgets.templates.krateo.io/v1beta1","version":"v1beta1"}],"preferredVersion":{"groupVersion":"widgets.templates.krateo.io/v1beta1","version":"v1beta1"}},` +
		`{"name":"templates.krateo.io","versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}]}`,
	"/api/v1": `{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`,
	"/apis/widgets.templates.krateo.io/v1beta1": `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"widgets.templates.krateo.io/v1beta1","resources":[` +
		`{"name":"tables","singularName":"table","namespaced":true,"kind":"Table","verbs":["get","list"]},` +
		`{"name":"panels","singularName":"panel","namespaced":true,"kind":"Panel","verbs":["get","list"]}]}`,
	"/apis/templates.krateo.io/v1": `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
		`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RESTAction","verbs":["get","list"]}]}`,
}

// allowedIn is the fixture's RBAC: the admin may read every namespace; the
// viewer every namespace but team-secret. The access reviews and the list
// endpoints both answer from it.
func allowedIn(who, namespace string) bool {
	return who == "admin" || who == "viewer" && namespace != "team-secret"
}

func (f *fakeTableAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if doc, ok := discovery[r.URL.Path]; ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(doc))
		return
	}
	who := callerOf(r)
	f.mu.Lock()
	f.callers[who]++
	if who == "sa" {
		f.saPaths = append(f.saPaths, r.Method+" "+r.URL.Path)
	}
	f.mu.Unlock()
	if who != "admin" && who != "viewer" && who != "sa" {
		http.Error(w, `{"kind":"Status","status":"Failure","code":401}`, http.StatusUnauthorized)
		return
	}
	// snowplow reads the caller's own clientconfig Secret with its service
	// account, and dials the RESTAction's calls with that credential.
	if user, ok := strings.CutPrefix(r.URL.Path, "/api/v1/namespaces/"+authnNS970+"/secrets/"); ok {
		ep, found := f.clientconfigs[strings.TrimSuffix(user, "-clientconfig")]
		if who != "sa" || !found {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
			return
		}
		b64 := func(v string) string { return base64.StdEncoding.EncodeToString([]byte(v)) }
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"apiVersion":"v1","kind":"Secret","metadata":{"name":%q,"namespace":%q},"data":{"server-url":%q,"certificate-authority-data":%q,"client-certificate-data":%q,"client-key-data":%q,"token":%q}}`,
			user, authnNS970, b64(ep.ServerURL), b64(ep.CertificateAuthorityData), b64(ep.ClientCertificateData), b64(ep.ClientKeyData), b64(ep.Token))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/apis/widgets.templates.krateo.io/v1beta1/namespaces/demo/tables/rows-table":
		_, _ = w.Write([]byte(tableWidgetCR()))
	case "/apis/widgets.templates.krateo.io/v1beta1/namespaces/demo/panels/rows-panel":
		_, _ = w.Write([]byte(tableParentCR))
	case "/apis/templates.krateo.io/v1/namespaces/demo/restactions/rows-ra":
		_, _ = w.Write([]byte(tableRESTActionCR))
	case "/api/v1/namespaces/team-a/configmaps":
		_, _ = w.Write([]byte(configMapList("team-a", "cm-a", teamARows)))
	case "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews":
		// client-go posts the review as protobuf; it accepts a JSON reply.
		body, _ := io.ReadAll(r.Body)
		obj, _, err := clientgoscheme.Codecs.UniversalDeserializer().Decode(body, nil, nil)
		ssar, ok := obj.(*authorizationv1.SelfSubjectAccessReview)
		if err != nil || !ok || ssar.Spec.ResourceAttributes == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ssar.APIVersion, ssar.Kind = "authorization.k8s.io/v1", "SelfSubjectAccessReview"
		ssar.Status.Allowed = allowedIn(who, ssar.Spec.ResourceAttributes.Namespace)
		_ = json.NewEncoder(w).Encode(ssar)
	case "/api/v1/namespaces/team-secret/configmaps":
		if !allowedIn(who, "team-secret") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,` +
				`"message":"configmaps is forbidden: cannot list resource \"configmaps\" in the namespace \"team-secret\""}`))
			return
		}
		_, _ = w.Write([]byte(configMapList("team-secret", "cm-s", teamSecretRows)))
	default:
		f.mu.Lock()
		f.unknown = append(f.unknown, r.Method+" "+r.URL.String())
		f.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","code":404}`))
	}
}

// expectedNames is every row the identity's RESTAction returns, in order.
func expectedNames(admin bool) []string {
	var names []string
	for i := range teamARows {
		names = append(names, fmt.Sprintf("cm-a-%04d", i))
	}
	if admin {
		for i := range teamSecretRows {
			names = append(names, fmt.Sprintf("cm-s-%04d", i))
		}
	}
	return names
}

func tableExportURL(format string) string {
	q := url.Values{}
	q.Set("apiVersion", "widgets.templates.krateo.io/v1beta1")
	q.Set("resource", "tables")
	q.Set("name", "rows-table")
	q.Set("namespace", "demo")
	q.Set("format", format)
	q.Set("path", tableExportJQ)
	q.Set("fields", strings.Join(tableValueKeys, ","))
	q["header"] = tableTitles
	return "/export?" + q.Encode()
}

func TestExport970_TableWidgetThroughRealCallChain(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	prev := xenv.TestMode()
	xenv.SetTestMode(true) // keep the injected ServerURL
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	t.Cleanup(widgets.SetValidateObjectStatusForTest(func(context.Context, *rest.Config, map[string]any) error { return nil }))

	api := &fakeTableAPI{callers: map[string]int{}}
	srv := httptest.NewUnstartedServer(api)
	srv.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	eps := callerEndpoints(t, srv, "admin", "viewer")
	api.clientconfigs = eps
	// The dispatchers capture the service-account config and the authn
	// namespace when they are built, so both are set first.
	t.Setenv("AUTHN_NAMESPACE", authnNS970)
	t.Cleanup(idynamic.SetServiceAccountRESTConfigForTest(&rest.Config{
		Host:            srv.URL,
		BearerToken:     "token-sa",
		TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})},
	}))
	export := exportChain()
	call := handlers.Dispatcher(dispatchers.All())(handlers.Call())

	// The parent's declared slice is real: the Table served at the parent's
	// window holds 50 rows.
	var parent struct {
		Spec struct {
			ResourcesRefs struct {
				Items []struct {
					Slice struct{ Page, PerPage int } `json:"slice"`
				} `json:"items"`
			} `json:"resourcesRefs"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(tableParentCR), &parent); err != nil {
		t.Fatal(err)
	}
	slice := parent.Spec.ResourcesRefs.Items[0].Slice
	win := serveAsEndpoint(t, call, eps["admin"], "admin", fmt.Sprintf(
		"/call?apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&name=rows-table&namespace=demo&page=%d&perPage=%d", slice.Page, slice.PerPage))
	var resolved struct {
		Status struct {
			WidgetData struct {
				DataSource []any `json:"dataSource"`
			} `json:"widgetData"`
		} `json:"status"`
	}
	if err := json.Unmarshal(win.Body.Bytes(), &resolved); err != nil || win.Code != http.StatusOK {
		t.Fatalf("table /call at the parent's slice: %d %v %s", win.Code, err, trimBody(win.Body.String()))
	}
	if got := len(resolved.Status.WidgetData.DataSource); got != slice.PerPage {
		t.Fatalf("table /call at the parent's slice holds %d rows, want the declared %d; body %s; unknown %v", got, slice.PerPage, trimBody(win.Body.String()), api.unknown)
	}

	for _, id := range []struct {
		user  string
		admin bool
	}{{"admin", true}, {"viewer", false}} {
		want := expectedNames(id.admin)
		t.Run(id.user, func(t *testing.T) {
			// csv: the title labels head the columns, in valueKey order, and
			// every row the identity's /call returns is there exactly once,
			// in order — across the 500-row export pages.
			// 620+ rows: the 500-row internal pages must run more than once.
			exported := map[string]string{}
			rec := serveAsEndpoint(t, export, eps[id.user], id.user, tableExportURL("csv"))
			exported["csv"] = rec.Body.String()
			if rec.Code != http.StatusOK {
				t.Fatalf("csv: %d %s", rec.Code, trimBody(rec.Body.String()))
			}
			records, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(records[0], "|") != strings.Join(tableTitles, "|") {
				t.Errorf("csv header = %q, want the titles %q", records[0], tableTitles)
			}
			var got []string
			for i, r := range records[1:] {
				got = append(got, r[0])
				ns := "team-a"
				if strings.HasPrefix(r[0], "cm-s-") {
					ns = "team-secret"
				}
				var n int
				fmt.Sscanf(r[0][5:], "%d", &n)
				if want := []string{r[0], ns, fmt.Sprint(n), ns, fmt.Sprint(n)}; strings.Join(r, "|") != strings.Join(want, "|") {
					t.Fatalf("csv row %d = %q, want %q (valueKey order)", i, r, want)
				}
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("csv rows: %d exported, want %d in order with no duplicates or gaps", len(got), len(want))
			}

			// json: the columns (field + title, valueKey order, the two
			// "Name" titles both kept) and every row keyed by valueKey.
			rec = serveAsEndpoint(t, export, eps[id.user], id.user, tableExportURL("json"))
			exported["json"] = rec.Body.String()
			var doc struct {
				Columns []struct{ Field, Label string } `json:"columns"`
				Rows    []json.RawMessage               `json:"rows"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil || rec.Code != http.StatusOK {
				t.Fatalf("json: %d %v", rec.Code, err)
			}
			for i := range tableValueKeys {
				if i >= len(doc.Columns) || doc.Columns[i].Field != tableValueKeys[i] || doc.Columns[i].Label != tableTitles[i] {
					t.Fatalf("json columns = %+v, want valueKeys %q titled %q, in order", doc.Columns, tableValueKeys, tableTitles)
				}
			}
			if len(doc.Rows) != len(want) {
				t.Errorf("json rows = %d, want %d", len(doc.Rows), len(want))
			}
			first := regexp.MustCompile(`\s+`).ReplaceAllString(string(doc.Rows[0]), "")
			if wantFirst := `{"name":"cm-a-0000","ns":"team-a","size":"0","owner":"team-a"}`; first != wantFirst {
				t.Errorf("json first row = %s, want %s (keyed by valueKey, in order)", first, wantFirst)
			}

			// pdf: the titles head every page; every row exactly once.
			rec = serveAsEndpoint(t, export, eps[id.user], id.user, tableExportURL("pdf"))
			if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/pdf" {
				t.Fatalf("pdf: %d %s", rec.Code, trimBody(rec.Body.String()))
			}
			d := pdfcheck.Parse(t, rec.Body.Bytes())
			exported["pdf"] = d.String()
			header := strings.Join(tableTitles, pdfcheck.Sep)
			seen := map[string]int{}
			for i, p := range d.Pages {
				hasHeader := false
				for _, l := range p.Lines {
					hasHeader = hasHeader || l == header
				}
				if !hasHeader {
					t.Fatalf("pdf page %d does not repeat the title labels %q", i+1, header)
				}
				for _, m := range regexp.MustCompile(`cm-[as]-\d{4}`).FindAllString(p.Text(), -1) {
					seen[m]++
				}
			}
			if !strings.Contains(d.Pages[0].Text(), fmt.Sprintf("Rows: %d", len(want))) || len(seen) != len(want) {
				t.Errorf("pdf rows = %d distinct, want %d", len(seen), len(want))
			}
			for _, n := range want {
				if seen[n] != 1 {
					t.Errorf("pdf draws %s %d times, want once", n, seen[n])
					break
				}
			}
			if line := strings.Join([]string{"cm-a-0007", "team-a", "7", "team-a", "7"}, pdfcheck.Sep); !strings.Contains(d.String(), line) {
				t.Errorf("pdf lacks the row %q in valueKey order", line)
			}

			// RBAC: a row the identity's /call does not return is in none of
			// its exports (the admin run is the control that the rows exist).
			for _, f := range []string{"csv", "json", "pdf"} {
				body := exported[f]
				if has := strings.Contains(body, "cm-s-") || strings.Contains(body, "team-secret"); has != id.admin {
					t.Errorf("%s export as %s: team-secret rows present = %v, want %v", f, id.user, has, id.admin)
				}
			}
		})
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.unknown) > 0 {
		t.Logf("apiserver requests outside the fixture (404): %v", api.unknown)
	}
	if api.callers["admin"] == 0 || api.callers["viewer"] == 0 {
		t.Errorf("apiserver requests per caller = %v: both identities must reach the apiserver with their own credential", api.callers)
	}
	for who := range api.callers {
		if who != "admin" && who != "viewer" && who != "sa" {
			t.Errorf("an apiserver request authenticated as %q, not as a caller", who)
		}
	}
	// The service account only ever read the callers' clientconfig Secrets:
	// every row came from a call made with the caller's own credential.
	for _, sp := range api.saPaths {
		if !strings.HasPrefix(sp, "GET /api/v1/namespaces/"+authnNS970+"/secrets/") {
			t.Errorf("the service account made %s; it may only read clientconfig Secrets", sp)
		}
	}
}

func trimBody(s string) string {
	if len(s) > 400 {
		return s[:400] + "…"
	}
	return s
}
