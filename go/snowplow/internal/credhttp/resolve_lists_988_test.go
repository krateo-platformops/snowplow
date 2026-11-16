package credhttp

// resolve_lists_988_test.go — #988: snowplow_resolve_collection_lists counts a
// resolve's collection LISTs on the wire, once each, through every config and
// client this package hardens, and nothing else.
//
//	RT    the RoundTripper alone, over a table of requests: what counts, under
//	      which (gvr, reason), and what never counts (an unmarked reflector
//	      LIST or watch-list, a continue page, a plain re-watch, a named GET, a
//	      write, discovery).
//	ONCE  every outbound path a resolve can take — each config source × each
//	      client constructor, a client built OUTSIDE this package from a
//	      hardened config, the #574 impersonating copy, Client over a hardened
//	      client, Do — counts a marked LIST exactly once, and the same LIST
//	      unmarked not at all.
//	HOP   a redirect hop a foreign client follows is refused before it is
//	      counted: one request on the wire, one count.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

const cmGVR = "/v1, Resource=configmaps"

// recordingRT answers every request with an empty 200 and counts them, so the
// RT arm can assert the counter never changes what is sent.
type recordingRT struct{ sent atomic.Int64 }

func (r *recordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r.sent.Add(1)
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: http.Header{}, Request: req}, nil
}

func resetResolveLists(t *testing.T) {
	t.Helper()
	ResetResolveCollectionListsForTest()
	t.Cleanup(ResetResolveCollectionListsForTest)
}

func totalResolveLists() uint64 {
	var n uint64
	for _, c := range ResolveCollectionLists() {
		n += c
	}
	return n
}

func TestIssue988_RT_CountsMarkedCollectionListsOnly(t *testing.T) {
	marked := MarkResolve(context.Background())
	reasoned := WithResolveReason(marked, "informer-fallthrough-not-servable")
	unmarked := context.Background()
	widgets := "widgets.templates.krateo.io/v1beta1, Resource=widgets"

	cases := []struct {
		name       string
		ctx        context.Context
		method     string
		url        string
		wantGVR    string // "" = must not count
		wantReason string
	}{
		{"marked list, no reason", marked, http.MethodGet, "/api/v1/namespaces/a/configmaps?limit=500", cmGVR, ReasonUnattributed},
		{"marked cluster list, reason", reasoned, http.MethodGet, "/apis/widgets.templates.krateo.io/v1beta1/widgets", widgets, "informer-fallthrough-not-servable"},
		{"marked etcd-delegated list", reasoned, http.MethodGet, "/apis/widgets.templates.krateo.io/v1beta1/widgets?limit=5&resourceVersion=42", widgets, "informer-fallthrough-not-servable"},
		{"marked watch-list start", marked, http.MethodGet, "/api/v1/configmaps?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan", cmGVR, ReasonUnattributed},

		{"marked continue page", marked, http.MethodGet, "/api/v1/configmaps?limit=500&continue=abc", "", ""},
		{"marked plain re-watch", marked, http.MethodGet, "/api/v1/configmaps?watch=true&resourceVersion=42", "", ""},
		{"marked named get", marked, http.MethodGet, "/api/v1/namespaces/a/configmaps/cm-1", "", ""},
		{"marked subresource get", marked, http.MethodGet, "/apis/widgets.templates.krateo.io/v1beta1/namespaces/a/widgets/w/status", "", ""},
		{"marked create", marked, http.MethodPost, "/api/v1/namespaces/a/configmaps", "", ""},
		{"marked discovery", marked, http.MethodGet, "/apis/widgets.templates.krateo.io/v1beta1", "", ""},
		{"marked version", marked, http.MethodGet, "/version", "", ""},

		{"reflector initial list", unmarked, http.MethodGet, "/api/v1/configmaps?limit=500&resourceVersion=0", "", ""},
		{"reflector relist", unmarked, http.MethodGet, "/api/v1/configmaps?limit=500&resourceVersion=42", "", ""},
		{"reflector watch-list", unmarked, http.MethodGet, "/api/v1/configmaps?watch=true&sendInitialEvents=true", "", ""},
		{"reason on an unmarked ctx is no marker", WithResolveReason(unmarked, "informer-fallthrough-not-synced"), http.MethodGet, "/api/v1/configmaps", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetResolveLists(t)
			next := &recordingRT{}
			rt := CountResolveCollectionLists(next)
			req, err := http.NewRequestWithContext(c.ctx, c.method, "https://apiserver.invalid"+c.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if next.sent.Load() != 1 {
				t.Fatalf("the counter must delegate every request unchanged; sent=%d", next.sent.Load())
			}
			got := ResolveCollectionLists()
			if c.wantGVR == "" {
				if len(got) != 0 {
					t.Fatalf("must not count; got %v", got)
				}
				return
			}
			if len(got) != 1 || got[ResolveListKey{GVR: c.wantGVR, Reason: c.wantReason}] != 1 {
				t.Fatalf("want exactly {%s|%s}=1; got %v", c.wantGVR, c.wantReason, got)
			}
		})
	}
}

func TestIssue988_RT_MarkerAndReasonPlumbing(t *testing.T) {
	ctx := context.Background()
	if ResolveMarked(ctx) || ResolveReason(ctx) != "" {
		t.Fatal("a bare ctx carries no marker and no reason")
	}
	if ResolveMarked(WithResolveReason(ctx, "x")) {
		t.Fatal("a reason alone must never mark a ctx: an unmarked request would start counting")
	}
	m := WithResolveReason(MarkResolve(ctx), "secret-get")
	if !ResolveMarked(m) || ResolveReason(m) != "secret-get" {
		t.Fatalf("marked+reason: marked=%v reason=%q", ResolveMarked(m), ResolveReason(m))
	}
	if got := ResolveReason(WithResolveReason(m, "")); got != "secret-get" {
		t.Fatalf("an empty reason must leave the decided one; got %q", got)
	}
	if got := ResolveReason(MarkResolve(m)); got != "" {
		t.Fatalf("a new mark (a new scope) starts with no reason; got %q", got)
	}
}

// listServer answers every request with an empty ConfigMapList.
func listServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMapList","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestIssue988_ONCE_EveryOutboundPathCountsAMarkedListOnce(t *testing.T) {
	srv := listServer(t)
	orig := inClusterConfig
	inClusterConfig = func() (*rest.Config, error) { return &rest.Config{Host: srv.URL, BearerToken: "sa"}, nil }
	t.Cleanup(func() { inClusterConfig = orig })

	ic, err := InClusterConfig()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := NewClientConfig(context.Background(), endpoints.Endpoint{ServerURL: srv.URL, Token: "user"})
	if err != nil {
		t.Fatal(err)
	}
	imp := rest.CopyConfig(ic) // the #574 impersonating copy
	imp.Impersonate = rest.ImpersonationConfig{UserName: "u"}
	twice := Harden(rest.CopyConfig(ic)) // a copy of a hardened config hardened again (FromSecret's shape)
	sources := map[string]*rest.Config{
		"InClusterConfig": ic, "NewClientConfig": cc, "impersonating copy": imp, "hardened twice": twice,
	}

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	clients := map[string]func(ctx context.Context, rc *rest.Config) error{
		"Dynamic": func(ctx context.Context, rc *rest.Config) error {
			c, err := Dynamic(rc)
			if err != nil {
				return err
			}
			_, err = c.Resource(gvr).Namespace("n").List(ctx, metav1.ListOptions{})
			return err
		},
		"Metadata": func(ctx context.Context, rc *rest.Config) error {
			c, err := Metadata(rc)
			if err != nil {
				return err
			}
			_, _ = c.Resource(gvr).Namespace("n").List(ctx, metav1.ListOptions{}) // the body is not metadata; the request is what counts
			return nil
		},
		"Kubernetes": func(ctx context.Context, rc *rest.Config) error {
			c, err := Kubernetes(rc)
			if err != nil {
				return err
			}
			_, err = c.CoreV1().ConfigMaps("n").List(ctx, metav1.ListOptions{})
			return err
		},
		"RESTClient": func(ctx context.Context, rc *rest.Config) error {
			cfg := rest.CopyConfig(rc)
			cfg.APIPath = "/api"
			cfg.GroupVersion = &corev1.SchemeGroupVersion
			cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
			c, err := RESTClient(cfg)
			if err != nil {
				return err
			}
			return c.Get().Namespace("n").Resource("configmaps").Do(ctx).Error()
		},
		"HTTPClientFor": func(ctx context.Context, rc *rest.Config) error {
			hc, err := HTTPClientFor(rc)
			if err != nil {
				return err
			}
			return getList(ctx, hc, rc.Host)
		},
		"client-go built outside credhttp": func(ctx context.Context, rc *rest.Config) error {
			c, err := dynamic.NewForConfig(rc)
			if err != nil {
				return err
			}
			_, err = c.Resource(gvr).Namespace("n").List(ctx, metav1.ListOptions{})
			return err
		},
		"Client over a client built from the config": func(ctx context.Context, rc *rest.Config) error {
			hc, err := rest.HTTPClientFor(rc)
			if err != nil {
				return err
			}
			return getList(ctx, Client(hc), rc.Host)
		},
	}
	for sname, rc := range sources {
		for cname, list := range clients {
			t.Run(sname+"/"+cname, func(t *testing.T) {
				resetResolveLists(t)
				if err := list(context.Background(), rc); err != nil {
					t.Fatalf("unmarked list: %v", err)
				}
				if n := totalResolveLists(); n != 0 {
					t.Fatalf("an unmarked LIST (a reflector's) was counted: %v", ResolveCollectionLists())
				}
				ctx := WithResolveReason(MarkResolve(context.Background()), "informer-fallthrough-not-servable")
				if err := list(ctx, rc); err != nil {
					t.Fatalf("marked list: %v", err)
				}
				got := ResolveCollectionLists()
				if len(got) != 1 || got[ResolveListKey{GVR: cmGVR, Reason: "informer-fallthrough-not-servable"}] != 1 {
					t.Fatalf("a marked LIST must count exactly once under its gvr and reason; got %v", got)
				}
			})
		}
	}

	t.Run("endpoint client (the resolver's held-credential dial)", func(t *testing.T) {
		resetResolveLists(t)
		hc := Client(&http.Client{})
		if err := getList(MarkResolve(context.Background()), hc, srv.URL); err != nil {
			t.Fatal(err)
		}
		if got := ResolveCollectionListCount(cmGVR, ReasonUnattributed); got != 1 || totalResolveLists() != 1 {
			t.Fatalf("want {%s|unattributed}=1; got %v", cmGVR, ResolveCollectionLists())
		}
	})
	t.Run("Do (the /call passthrough)", func(t *testing.T) {
		resetResolveLists(t)
		st := Do(WithResolveReason(MarkResolve(context.Background()), "client-build"), request.RequestOptions{
			RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/n/configmaps", Verb: ptr.To(http.MethodGet)},
			Endpoint:    &endpoints.Endpoint{ServerURL: srv.URL},
		})
		if st.Code >= 300 {
			t.Fatalf("Do: %+v", st)
		}
		if got := ResolveCollectionListCount(cmGVR, "client-build"); got != 1 || totalResolveLists() != 1 {
			t.Fatalf("want {%s|client-build}=1; got %v", cmGVR, ResolveCollectionLists())
		}
	})
}

func getList(ctx context.Context, hc *http.Client, host string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, host+"/api/v1/namespaces/n/configmaps", nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func TestIssue988_HOP_ARefusedRedirectHopIsNotCounted(t *testing.T) {
	resetResolveLists(t)
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		// A redirect to another collection LIST: a counted hop would be a
		// second count for a request that never reached the network.
		http.Redirect(w, r, (&url.URL{Path: "/api/v1/namespaces/m/configmaps"}).String(), http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	rc := Harden(&rest.Config{Host: srv.URL, BearerToken: "t"})
	hc, err := rest.HTTPClientFor(rc) // a client NOT built here: it follows redirects
	if err != nil {
		t.Fatal(err)
	}
	if err := getList(MarkResolve(context.Background()), hc, srv.URL); err == nil {
		t.Fatal("the hop must be refused")
	}
	if hits.Load() != 1 {
		t.Fatalf("PRECONDITION: one request on the wire; got %d", hits.Load())
	}
	if got := ResolveCollectionLists(); len(got) != 1 || got[ResolveListKey{GVR: cmGVR, Reason: ReasonUnattributed}] != 1 {
		t.Fatalf("one request on the wire, one count; got %v", got)
	}
}
