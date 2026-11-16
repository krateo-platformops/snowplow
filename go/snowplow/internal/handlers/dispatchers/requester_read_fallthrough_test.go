package dispatchers

// requester_read_fallthrough_test.go — an object read that leaves snowplow under the
// requester's credential marks the resolve as dialled by the requester, so no Put
// can store what the apiserver allowed one requester under a key another shares.
//
// THE ROUTE. On a partial RBAC snapshot the RoleBinding that lets alice read the
// widget's RESTAction is missing, so objects.Get denies the read in-process and
// falls through to the apiserver under the requester's own credential. Alice's
// read succeeds there and bob's is refused, but the two derive one widget key.
// Before the mark, nothing declined alice's Put: bob was served her body from L1.
//
// Two shapes, both RED before the mark:
//   - T1, the opt-in: a third-party stage and the external-TTL annotation. #1224's
//     rule needs every touch third-party; the requester's read of the RESTAction is not.
//   - T2, no opt-in and no stage at all: a RESTAction with only a filter.
//
// The fake apiserver is TLS and judges the RESTAction read by the client
// certificate, the credential a `<user>-clientconfig` carries: alice is allowed,
// bob is refused, anyone else is unauthorised. Nothing in it reads the snapshot.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"context"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// rrfContent is in the RESTAction itself (T2's filter), so only a reader of the
// RESTAction can have it in their body.
const rrfContent = "content-only-the-restaction-holds"

type rrfPKI struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
}

func newRRFPKI(t *testing.T) *rrfPKI {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rrf-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &rrfPKI{caCert: cert, caKey: key, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (p *rrfPKI) issue(t *testing.T, cn string, server bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
}

// rrfWire counts the reads of the RESTAction, by who the certificate names.
type rrfWire struct{ raAlice, raBob, raOther atomic.Int64 }

type rrfEnv struct {
	*k1221Env
	wire *rrfWire
}

// rrfRoleBindingForAlice lets alice, and only alice, read the RESTAction, through
// the kind of binding a partial snapshot drops.
func rrfRoleBindingForAlice() []runtime.Object {
	return []runtime.Object{
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: h1NS, Name: "rrf-ra-role"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: h1NS, Name: "rrf-ra-alice", UID: types.UID("uid-rrf-ra-alice")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psAlice}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "rrf-ra-role"},
		},
	}
}

// rrfApiserver answers discovery, the CRD, and the RESTAction read by certificate.
func rrfApiserver(t *testing.T, p *rrfPKI, ra *unstructured.Unstructured, w *rrfWire) *httptest.Server {
	t.Helper()
	raJSON, _ := json.Marshal(ra.Object)
	gv := h1RAGVR.Group + "/" + h1RAGVR.Version
	raPath := "/apis/" + gv + "/namespaces/" + ra.GetNamespace() + "/" + h1RAGVR.Resource + "/" + ra.GetName()
	gvd := func(g, v string) metav1.GroupVersionForDiscovery {
		return metav1.GroupVersionForDiscovery{GroupVersion: g + "/" + v, Version: v}
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		writeJSON := func(v any) {
			rw.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(rw).Encode(v)
		}
		switch r.URL.Path {
		case "/apis":
			writeJSON(&metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
				Groups: []metav1.APIGroup{
					{Name: h1RAGVR.Group, Versions: []metav1.GroupVersionForDiscovery{gvd(h1RAGVR.Group, h1RAGVR.Version)}, PreferredVersion: gvd(h1RAGVR.Group, h1RAGVR.Version)},
					{Name: h643WidgetGVR.Group, Versions: []metav1.GroupVersionForDiscovery{gvd(h643WidgetGVR.Group, h643WidgetGVR.Version)}, PreferredVersion: gvd(h643WidgetGVR.Group, h643WidgetGVR.Version)},
					{Name: "apiextensions.k8s.io", Versions: []metav1.GroupVersionForDiscovery{gvd("apiextensions.k8s.io", "v1")}, PreferredVersion: gvd("apiextensions.k8s.io", "v1")},
				}})
			return
		case "/apis/" + gv:
			writeJSON(&metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv,
				APIResources: []metav1.APIResource{{Name: h1RAGVR.Resource, SingularName: "restaction", Namespaced: true, Kind: "RESTAction", Verbs: metav1.Verbs{"get", "list", "watch"}}}})
			return
		case raPath:
			cn := ""
			if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
				cn = r.TLS.PeerCertificates[0].Subject.CommonName
			}
			rw.Header().Set("Content-Type", "application/json")
			switch cn {
			case psAlice:
				w.raAlice.Add(1)
				_, _ = rw.Write(raJSON)
			case psBob:
				w.raBob.Add(1)
				rw.WriteHeader(http.StatusForbidden)
				_, _ = rw.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
			default:
				w.raOther.Add(1)
				rw.WriteHeader(http.StatusUnauthorized)
			}
			return
		}
		if h643ServeDiscovery(rw, r.URL.Path) {
			return
		}
		rw.WriteHeader(http.StatusNotFound)
	}))
	certPEM, keyPEM := p.issue(t, "rrf-apiserver", true)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(p.caCert)
	// A ServiceAccount's discovery and CRD reads present no certificate; a
	// requester's always does, and only those reach the RESTAction.
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func rrfSetup(t *testing.T, ra, cr *unstructured.Unstructured) *rrfEnv {
	t.Helper()
	k423Env(t)
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	crdschema.InvalidateCRDSchemaMemo()
	t.Cleanup(crdschema.InvalidateCRDSchemaMemo)

	a := psArm{name: "ra_by_rolebinding", target: psConfigmapsGVR, watchShape: true, raByRoleBinding: true}
	extra := append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(cr))
	psBuildWatcher(t, a, append(extra, rrfRoleBindingForAlice()...)...)
	if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			t.Fatal("PRECONDITION: the RESTAction informer did not sync")
		}
	}
	p := newRRFPKI(t)
	w := &rrfWire{}
	apiserver := rrfApiserver(t, p, ra, w)
	named := k1221Endpoint(t, &k1221Wire{})
	psSeedClientconfigs(t, apiserver.URL)
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		k1221EP: {
			ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: k1221EP},
			Data:       map[string][]byte{"server-url": []byte(named.URL), "token": []byte(k1221Token)},
		},
	}})
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	ctxFor := func(user string) context.Context {
		certPEM, keyPEM := p.issue(t, user, false)
		return xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: psGroups(a)}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: apiserver.URL,
				CertificateAuthorityData: b64(p.caPEM), ClientCertificateData: b64(certPEM), ClientKeyData: b64(keyPEM)}),
			xcontext.WithAccessToken("jwt-"+user))
	}
	e := &k1221Env{t: t, cr: cr, saURL: apiserver.URL, endpointURL: named.URL,
		aliceCtx: ctxFor(psAlice), bobCtx: ctxFor(psBob), store: cache.ResolvedCache()}
	return &rrfEnv{k1221Env: e, wire: w}
}

// serve is k1221Env.serve, with a ServiceAccount config that trusts the TLS fake.
func (e *rrfEnv) serve(ctx context.Context, caPEM []byte) *httptest.ResponseRecorder {
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: e.cr.DeepCopy()}
	})
	defer restore()
	h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: e.saURL, TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
	return rec
}

// rrfCA reads back the CA the fake's ServiceAccount config must trust.
func rrfCA(t *testing.T, ctx context.Context) []byte {
	t.Helper()
	ep, err := xcontext.UserConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := base64.StdEncoding.DecodeString(ep.CertificateAuthorityData)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

type rrfShape struct {
	name    string
	content string
	ra      func() *unstructured.Unstructured
	widget  func() *unstructured.Unstructured
}

var rrfShapes = []rrfShape{
	{
		name: "T1_opt_in_third_party_stage", content: k1221Value,
		ra:     func() *unstructured.Unstructured { return k1221RA(k1221FeedStage()) },
		widget: func() *unstructured.Unstructured { return k1221Widget("${ .feed.value }", true) },
	},
	{
		name: "T2_no_opt_in_no_stage", content: rrfContent,
		ra: func() *unstructured.Unstructured {
			ra := k1221RA()
			ra.Object["spec"].(map[string]any)["filter"] = `{value: "` + rrfContent + `"}`
			return ra
		},
		widget: func() *unstructured.Unstructured { return k1221Widget("${ .value }", false) },
	},
}

func TestRequesterRead_APartialSnapshotNeverServesBobTheRESTActionAliceRead(t *testing.T) {
	for _, s := range rrfShapes {
		t.Run(s.name, func(t *testing.T) {
			e := rrfSetup(t, s.ra(), s.widget())
			ca := rrfCA(t, e.aliceCtx)
			key := e.sharePartial()

			alice := e.serve(e.aliceCtx, ca)
			if alice.Code != http.StatusOK || !strings.Contains(alice.Body.String(), s.content) || e.wire.raAlice.Load() == 0 {
				t.Fatalf("PRECONDITION: alice's own read of the RESTAction must bring its content (code=%d her reads=%d body=%s)",
					alice.Code, e.wire.raAlice.Load(), psTrunc(alice.Body.String(), 400))
			}
			// Not Fatal: with the mark gone the arm must go on and show the leak.
			if _, ok := e.store.GetNoTouch(key); ok {
				t.Errorf("RED: the shared widget key is resident after a read under alice's own credential")
			}

			bob := e.serve(e.bobCtx, ca)
			if strings.Contains(bob.Body.String(), s.content) {
				t.Fatalf("CROSS-IDENTITY SERVE: bob, whom the apiserver refuses the RESTAction, was served its content from a shared cell (code=%d his reads=%d)",
					bob.Code, e.wire.raBob.Load())
			}
			if e.wire.raBob.Load() == 0 || bob.Code != http.StatusForbidden {
				t.Fatalf("PRECONDITION: bob must have read the RESTAction himself and been refused: 403 (his reads=%d code=%d body=%s)",
					e.wire.raBob.Load(), bob.Code, psTrunc(bob.Body.String(), 300))
			}
			if e.wire.raOther.Load() != 0 {
				t.Fatalf("PRECONDITION: %d read(s) of the RESTAction under a credential that is neither user's", e.wire.raOther.Load())
			}
		})
	}
}

// The controls, on the complete snapshot: each gets their own answer. Alice reads
// the RESTAction from the informer, so nothing marks her resolve and it is stored
// (the mark costs nothing when the read stays inside snowplow); bob is refused by
// the apiserver himself.
func TestRequesterRead_OnACompleteSnapshotEachGetsTheirOwnAnswer(t *testing.T) {
	for _, s := range rrfShapes {
		t.Run(s.name, func(t *testing.T) {
			e := rrfSetup(t, s.ra(), s.widget())
			ca := rrfCA(t, e.aliceCtx)
			if e.key(e.aliceCtx) == e.key(e.bobCtx) {
				t.Fatalf("PRECONDITION: alice and bob must not share a key on the complete snapshot")
			}
			alice := e.serve(e.aliceCtx, ca)
			if alice.Code != http.StatusOK || !strings.Contains(alice.Body.String(), s.content) {
				t.Fatalf("alice must get the RESTAction's content (code=%d body=%s)", alice.Code, psTrunc(alice.Body.String(), 400))
			}
			if got := e.wire.raAlice.Load(); got != 0 {
				t.Fatalf("PRECONDITION: on the complete snapshot alice's read must stay in the informer: %d read(s) left snowplow", got)
			}
			if s.name == "T2_no_opt_in_no_stage" {
				if _, ok := e.store.GetNoTouch(e.key(e.aliceCtx)); !ok {
					t.Fatalf("an informer-served read must still be stored: the mark must cost nothing when no read leaves snowplow")
				}
			}
			bob := e.serve(e.bobCtx, ca)
			if strings.Contains(bob.Body.String(), s.content) || bob.Code != http.StatusForbidden || e.wire.raBob.Load() == 0 {
				t.Fatalf("bob must be refused by the apiserver himself: 403 and no content (code=%d his reads=%d body=%s)",
					bob.Code, e.wire.raBob.Load(), psTrunc(bob.Body.String(), 300))
			}
		})
	}
}

// The dispatched CR's own fetch runs before the sinks exist, so it marks them
// through objects.Result. Driven through the fetch seam: the result is what
// objects.getFromAPIServer returns for a read under the requester's credential.
// The RESTAction handler's twin is TestRequesterRead_TheDispatchedRESTActionsOwnFetchMarksThePut.
func TestRequesterRead_TheDispatchedCRsOwnFetchMarksThePut(t *testing.T) {
	for _, dialled := range []bool{true, false} {
		name := "fetched_in_snowplow"
		if dialled {
			name = "fetched_under_the_requester"
		}
		t.Run(name, func(t *testing.T) {
			ra := k1221RA()
			ra.Object["spec"].(map[string]any)["filter"] = `{value: "` + rrfContent + `"}`
			e := rrfSetup(t, ra, k1221Widget("${ .value }", false))
			ca := rrfCA(t, e.aliceCtx)
			restore := setFetchObjectForTest(func(*http.Request) objects.Result {
				return objects.Result{GVR: h1WidgetGVR, Unstructured: e.cr.DeepCopy(), RequesterDialled: dialled}
			})
			defer restore()
			h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: e.saURL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(e.aliceCtx))
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), rrfContent) {
				t.Fatalf("PRECONDITION: alice must get the content (code=%d body=%s)", rec.Code, psTrunc(rec.Body.String(), 300))
			}
			_, resident := e.store.GetNoTouch(e.key(e.aliceCtx))
			if resident == dialled {
				t.Fatalf("RED: a dispatched CR fetched under the requester's credential=%v left the cell resident=%v", dialled, resident)
			}
		})
	}
}

// The RESTAction handler fetches its CR before its sinks exist too (restactions.go),
// and marks them from objects.Result the same way.
func TestRequesterRead_TheDispatchedRESTActionsOwnFetchMarksThePut(t *testing.T) {
	for _, dialled := range []bool{true, false} {
		name := "fetched_in_snowplow"
		if dialled {
			name = "fetched_under_the_requester"
		}
		t.Run(name, func(t *testing.T) {
			k423Env(t)
			a := psArm{name: "restaction_fetch", target: psConfigmapsGVR, watchShape: true}
			ra := k1221RA()
			ra.Object["spec"].(map[string]any)["filter"] = `{value: "` + rrfContent + `"}`
			psBuildWatcher(t, a, runtime.Object(ra))
			srv := k1221Apiserver(t, a, &k1221Wire{})
			psSeedClientconfigs(t, srv.URL)
			ctx := psUserCtx(a, psAlice)
			restore := setFetchObjectForTest(func(*http.Request) objects.Result {
				return objects.Result{GVR: h1RAGVR, Unstructured: ra.DeepCopy(), RequesterDialled: dialled}
			})
			defer restore()
			h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: srv.URL}}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), rrfContent) {
				t.Fatalf("PRECONDITION: alice must get the RESTAction's content (code=%d body=%s)", rec.Code, psTrunc(rec.Body.String(), 300))
			}
			key, _ := k423RAKey(t, ctx)
			if _, resident := cache.ResolvedCache().GetNoTouch(key); resident == dialled {
				t.Fatalf("RED: a dispatched RESTAction fetched under the requester's credential=%v left its cell resident=%v", dialled, resident)
			}
		})
	}
}
