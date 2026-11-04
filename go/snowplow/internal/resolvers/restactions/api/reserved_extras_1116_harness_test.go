package api

// reserved_extras_1116_harness_test.go — the #1116 harness: a two-stage ticket
// RESTAction (stage 1 reads a Ticket CR as the caller; stage 2 dials an
// external endpoint with a snowplow-held endpointRef credential, its path
// built from stage 1's output), resolved on the four dispatch arms: the
// api-stage content cell, the inline informer, cache on with no informer, and
// cache off. Lifted from the #1116 investigation tests.
//
// The caller's apiserver (kubeRX) answers per bearer token, written down from
// Kubernetes semantics (a RoleBinding per tenant namespace), never computed by
// snowplow's evaluator. A separate ServiceAccount server records any SA dial,
// and the external endpoint records every dial.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var rxGVR = schema.GroupVersionResource{Group: "zendesk.krateo.io", Version: "v1alpha1", Resource: "tickets"}

const (
	rxUA   = "u-iso-a"    // reads tickets in team-a only
	rxUB   = "u-iso-b"    // reads tickets in team-b only
	rxUN   = "u-iso-none" // no grant
	rxGrp  = "g-iso-tenants"
	rxLbl  = "servicedesk.krateo.io/ticket"
	rxAuth = "krateo-system"
	rxZD   = "zendesk-ep"
	rxSAT  = "SA-RC-TOKEN"
)

var rxTokens = map[string]string{"tok-a": rxUA, "tok-b": rxUB, "tok-n": rxUN}
var rxUserTok = map[string]string{rxUA: "tok-a", rxUB: "tok-b", rxUN: "tok-n"}
var rxGrant = map[string]string{rxUA: "team-a", rxUB: "team-b"}

// tickets: CR name, namespace, ticket label, Zendesk id
var rxTickets = []struct {
	ns, name, label string
	id              int
}{{"team-a", "tk-a1", "t-a1", 1001}, {"team-b", "tk-b1", "t-b1", 2002}}

func rxTicket(ns, name, label string, id int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "zendesk.krateo.io/v1alpha1", "kind": "Ticket",
		"metadata": map[string]any{"namespace": ns, "name": name, "labels": map[string]any{rxLbl: label}},
		"status":   map[string]any{"id": int64(id)},
	}}
}

func rxRBAC() []runtime.Object {
	tm := func(k string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: k}
	}
	out := []runtime.Object{&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "ticket-reader"},
		Rules: []rbacv1.PolicyRule{{APIGroups: []string{rxGVR.Group}, Resources: []string{rxGVR.Resource}, Verbs: []string{"get", "list"}}}}}
	for u, ns := range rxGrant {
		out = append(out, &rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"),
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rb-" + u},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: u}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "ticket-reader"}})
	}
	return out
}

func rxWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	kinds[rxGVR] = "TicketList"
	seed := rxRBAC()
	for _, tk := range rxTickets {
		seed = append(seed, rxTicket(tk.ns, tk.name, tk.label, tk.id))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	_, syncCh := rw.EnsureResourceType(rxGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("tickets informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

type rxHit struct{ who, method, uri string }

type rxRec struct {
	mu   sync.Mutex
	hits []rxHit
}

func (r *rxRec) add(h rxHit) { r.mu.Lock(); r.hits = append(r.hits, h); r.mu.Unlock() }
func (r *rxRec) snap() []rxHit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rxHit(nil), r.hits...)
}

// kubeRX: the caller's apiserver. It also serves the clientconfig / endpoint
// Secrets to the SA rest.Config (the cache-off endpoint read).
func kubeRX(t *testing.T, rec *rxRec, secrets func() map[string]map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		who := rxTokens[tok]
		if tok == rxSAT {
			who = "SA-RC"
		}
		if who == "" {
			who = "anon(" + tok + ")"
		}
		rec.add(rxHit{who, r.Method, r.URL.RequestURI()})
		w.Header().Set("Content-Type", "application/json")
		p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		// /api/v1/namespaces/<ns>/secrets/<name>
		if len(p) == 6 && p[0] == "api" && p[4] == "secrets" {
			if who != "SA-RC" {
				w.WriteHeader(403)
				return
			}
			d, ok := secrets()[p[5]]
			if !ok {
				w.WriteHeader(404)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
				return
			}
			data := map[string]string{}
			for k, v := range d {
				data[k] = base64.StdEncoding.EncodeToString([]byte(v))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": p[5], "namespace": p[3]}, "data": data})
			return
		}
		// /apis/zendesk.krateo.io/v1alpha1/namespaces/<ns>/tickets[/<name>]
		if len(p) >= 6 && p[0] == "apis" && p[1] == rxGVR.Group && p[5] == "tickets" {
			ns := p[4]
			verb := "list"
			if len(p) == 7 {
				verb = "get"
			}
			allowed := rxGrant[who] == ns
			if !allowed {
				w.WriteHeader(403)
				_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"tickets.zendesk.krateo.io is forbidden: User \"%s\" cannot %s resource \"tickets\" in API group \"zendesk.krateo.io\" in the namespace \"%s\""}`, who, verb, ns)
				return
			}
			sel := labels.Everything()
			if s := r.URL.Query().Get("labelSelector"); s != "" {
				var err error
				if sel, err = labels.Parse(s); err != nil {
					w.WriteHeader(400)
					return
				}
			}
			items := []any{}
			for _, tk := range rxTickets {
				if tk.ns != ns {
					continue
				}
				o := rxTicket(tk.ns, tk.name, tk.label, tk.id)
				if verb == "get" && tk.name == p[6] {
					_ = json.NewEncoder(w).Encode(o.Object)
					return
				}
				if verb == "list" && sel.Matches(labels.Set(o.GetLabels())) {
					items = append(items, o.Object)
				}
			}
			if verb == "get" {
				w.WriteHeader(404)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "zendesk.krateo.io/v1alpha1", "kind": "TicketList",
				"metadata": map[string]any{"resourceVersion": "9"}, "items": items})
			return
		}
		w.WriteHeader(404)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type rxArm string

const (
	rxContent rxArm = "cache-on/content"
	rxInline  rxArm = "cache-on/inline"
	rxDirect  rxArm = "cache-on/no-informer"
	rxOff     rxArm = "cache-off"
)

var rxArms = []rxArm{rxContent, rxInline, rxDirect, rxOff}

type rxOut struct {
	dict    map[string]any
	dictS   string
	failure *StageFailure
	header  string
	kube    []rxHit
	sa      []rxHit
	zd      []rxHit
}

type rxEnv struct {
	t      *testing.T
	rw     *cache.ResourceWatcher
	kube   *httptest.Server
	kRec   *rxRec
	saRec  *rxRec
	zdRec  *rxRec
	zd     *httptest.Server
	saSrv  *httptest.Server
	secret map[string]map[string]string
}

func newRxEnv(t *testing.T) *rxEnv {
	t.Helper()
	e := &rxEnv{t: t, kRec: &rxRec{}, saRec: &rxRec{}, zdRec: &rxRec{}}
	e.rw = rxWatcher(t)
	e.kube = kubeRX(t, e.kRec, func() map[string]map[string]string { return e.secret })
	e.saSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.saRec.add(rxHit{"SA-EP", r.Method, r.URL.RequestURI()})
		w.WriteHeader(500)
	}))
	t.Cleanup(e.saSrv.Close)
	e.zd = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.zdRec.add(rxHit{r.Header.Get("Authorization"), r.Method, r.URL.RequestURI()})
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ticket":{"subject":"zendesk body for `+r.URL.Path+`"}}`)
	}))
	t.Cleanup(e.zd.Close)
	e.secret = map[string]map[string]string{
		rxZD: {"server-url": e.zd.URL, "token": "ZD-TOKEN"},
	}
	for u, tok := range rxUserTok {
		e.secret[kubeutil.MakeDNS1123Compatible(u)+"-clientconfig"] = map[string]string{"server-url": e.kube.URL, "token": tok}
	}
	t.Cleanup(SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: e.saSrv.URL, Token: "SA-EP-TOKEN"}, nil
	}))
	t.Cleanup(SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: e.saSrv.URL, BearerToken: "SA-IC-TOKEN"}, nil
	}))
	return e
}

// resolve runs one resolve as user on arm. resetL1 false keeps the
// identity-free content cells from the previous resolve (the shared cache).
func (e *rxEnv) resolve(arm rxArm, user string, resetL1 bool, extras map[string]any, stages ...*templates.API) rxOut {
	t := e.t
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	if arm == rxInline {
		t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	}
	if resetL1 {
		cache.ResetResolvedCacheForTest()
	}
	cache.SetGlobal(e.rw)
	if arm == rxDirect || arm == rxOff {
		cache.SetGlobal(nil)
	}
	if arm == rxOff {
		t.Setenv("CACHE_ENABLED", "false")
	}
	defer cache.SetGlobal(e.rw)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)

	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	if arm != rxOff {
		by := map[string]*corev1.Secret{}
		for n, d := range e.secret {
			b := map[string][]byte{}
			for k, v := range d {
				b[k] = []byte(v)
			}
			by[n] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: rxAuth, Name: n}, Data: b}
		}
		cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: by})
		cache.ForceSecretsCacheReadyForTest(rxAuth)
	}
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()

	k0, s0, z0 := len(e.kRec.snap()), len(e.saRec.snap()), len(e.zdRec.snap())
	ctx, _ := withSinks681(ctxWithUser(user, rxGrp))
	ctx, oc := WithStageOutcomes(ctx)
	// deep-copy stages so a run cannot mutate the next
	cp := make([]*templates.API, len(stages))
	for i, s := range stages {
		c := *s
		cp[i] = &c
	}
	dict, f := ResolveWithFailure(ctx, ResolveOptions{
		RC:      &rest.Config{Host: e.kube.URL, BearerToken: rxSAT},
		AuthnNS: rxAuth, Items: cp, Extras: extras,
		RESTActionNamespace: "team-a", RESTActionName: "zendesk-ticket",
	})
	b, _ := json.Marshal(dict)
	return rxOut{dict: dict, dictS: string(b), failure: f, header: oc.HeaderValue(),
		kube: e.kRec.snap()[k0:], sa: e.saRec.snap()[s0:], zd: e.zdRec.snap()[z0:]}
}

const rxTicketsPath = "/apis/zendesk.krateo.io/v1alpha1/namespaces/"

func rxStage1Get(coe bool) *templates.API {
	return &templates.API{Name: "stage1", Verb: ptr.To(http.MethodGet),
		Path:            `${ "` + rxTicketsPath + `" + .namespace + "/tickets/" + .ticket }`,
		ContinueOnError: ptr.To(coe), ErrorKey: ptr.To("stage1Err")}
}

func rxStage1List(coe bool) *templates.API {
	return &templates.API{Name: "stage1", Verb: ptr.To(http.MethodGet),
		Path:            `${ "` + rxTicketsPath + `" + .namespace + "/tickets?labelSelector=" + ("` + rxLbl + `=" + .ticket | @uri) }`,
		ContinueOnError: ptr.To(coe), ErrorKey: ptr.To("stage1Err")}
}

// stage 2 as acmp describes it: the id from stage 1, checked numeric.
func rxStage2(idExpr string) *templates.API {
	return &templates.API{Name: "stage2", Verb: ptr.To(http.MethodGet),
		EndpointRef:     &templates.Reference{Name: rxZD, Namespace: rxAuth},
		DependsOn:       &templates.Dependency{Name: "stage1"},
		Path:            `${ ` + idExpr + ` | if type == "number" then "/api/v2/tickets/" + tostring + ".json" else error("ticket id is not numeric") end }`,
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("stage2Err")}
}

const (
	rxIDList = `.stage1.items[0].status.id`
	rxIDGet  = `.stage1.status.id`
)

func rxWho(h []rxHit) string {
	parts := []string{}
	for _, x := range h {
		parts = append(parts, x.who+" "+x.method+" "+x.uri)
	}
	return "[" + strings.Join(parts, " | ") + "]"
}

func rxShort(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
