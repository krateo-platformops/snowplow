package credhttp

// credhttp_test.go: each layer of the #723/#724 policy, alone and as built by
// every constructor this package exports. Every arm serves a redirect to a
// foreign host that records whatever reaches it; the policy holds only if
// nothing ever does.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type rig struct {
	api      *httptest.Server
	otherHit atomic.Int64
}

func newRig(t *testing.T, code int) *rig {
	t.Helper()
	r := &rig{}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.otherHit.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"x"}}`))
	}))
	t.Cleanup(other.Close)
	r.api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, other.URL+"/stolen", code)
	}))
	t.Cleanup(r.api.Close)
	return r
}

var codes = []int{http.StatusFound, http.StatusTemporaryRedirect}

func TestIssue723_724_CheckRedirectLayerAlone(t *testing.T) {
	for _, code := range codes {
		r := newRig(t, code)
		c := &http.Client{CheckRedirect: CheckRedirect}
		resp, err := c.Get(r.api.URL + "/x")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != code || r.otherHit.Load() != 0 {
			t.Errorf("code=%d: got status %d, other-host hits %d", code, resp.StatusCode, r.otherHit.Load())
		}
	}
}

func TestIssue723_724_HopLayerAlone(t *testing.T) {
	for _, code := range codes {
		r := newRig(t, code)
		c := &http.Client{Transport: RefuseRedirectHops(nil)} // default CheckRedirect: follows
		resp, err := c.Get(r.api.URL + "/x")
		if resp != nil {
			_ = resp.Body.Close()
		}
		var rre *RedirectRefusedError
		if !errors.As(err, &rre) || rre.StatusCode != code {
			t.Errorf("code=%d: want RedirectRefusedError{%d}, got %v", code, code, err)
		}
		if r.otherHit.Load() != 0 {
			t.Errorf("code=%d: the hop reached the other host", code)
		}
	}
}

func TestIssue723_724_HardenIsInheritedByCopyConfig(t *testing.T) {
	r := newRig(t, http.StatusFound)
	rc := Harden(&rest.Config{Host: r.api.URL, BearerToken: "t"})
	cp := rest.CopyConfig(rc)
	cp.Impersonate = rest.ImpersonationConfig{UserName: "u"}
	hc, err := rest.HTTPClientFor(cp) // a client credhttp did NOT build
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get(r.api.URL + "/x")
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil || r.otherHit.Load() != 0 {
		t.Fatalf("a copy of a hardened config followed the redirect (err=%v hits=%d)", err, r.otherHit.Load())
	}
}

// Every exported constructor, from a NON-hardened config: the client layer
// alone must hold.
func TestIssue723_724_EveryConstructorRefusesRedirects(t *testing.T) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	ctx := context.Background()
	arms := map[string]func(rc *rest.Config) error{
		"Dynamic": func(rc *rest.Config) error {
			c, err := Dynamic(rc)
			if err != nil {
				return err
			}
			_, err = c.Resource(gvr).Namespace("n").Get(ctx, "x", metav1.GetOptions{})
			return err
		},
		"Metadata": func(rc *rest.Config) error {
			c, err := Metadata(rc)
			if err != nil {
				return err
			}
			_, err = c.Resource(gvr).Namespace("n").Get(ctx, "x", metav1.GetOptions{})
			return err
		},
		"Kubernetes": func(rc *rest.Config) error {
			c, err := Kubernetes(rc)
			if err != nil {
				return err
			}
			_, err = c.CoreV1().ConfigMaps("n").Get(ctx, "x", metav1.GetOptions{})
			return err
		},
		"APIExtensions": func(rc *rest.Config) error {
			c, err := APIExtensions(rc)
			if err != nil {
				return err
			}
			_, err = c.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "x", metav1.GetOptions{})
			return err
		},
		"Discovery": func(rc *rest.Config) error {
			c, err := Discovery(rc)
			if err != nil {
				return err
			}
			_, err = c.ServerVersion()
			return err
		},
		"RESTClient": func(rc *rest.Config) error {
			cfg := rest.CopyConfig(rc)
			cfg.APIPath = "/api"
			cfg.GroupVersion = &corev1.SchemeGroupVersion
			cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
			c, err := RESTClient(cfg)
			if err != nil {
				return err
			}
			return c.Get().Namespace("n").Resource("configmaps").Name("x").Do(ctx).Error()
		},
		"HTTPClientFor": func(rc *rest.Config) error {
			c, err := HTTPClientFor(rc)
			if err != nil {
				return err
			}
			resp, err := c.Get(rc.Host + "/x")
			if err != nil {
				return err
			}
			_ = resp.Body.Close()
			if resp.StatusCode/100 == 3 {
				return errors.New("3xx surfaced")
			}
			return nil
		},
	}
	for name, arm := range arms {
		for _, code := range codes {
			r := newRig(t, code)
			err := arm(&rest.Config{Host: r.api.URL, BearerToken: "sa-token", Impersonate: rest.ImpersonationConfig{UserName: "u", Groups: []string{"g"}}})
			if r.otherHit.Load() != 0 {
				t.Errorf("%s code=%d: RED — %d request(s) reached the other host", name, code, r.otherHit.Load())
			}
			if err == nil {
				t.Errorf("%s code=%d: RED — the redirect target's answer was accepted", name, code)
			}
		}
	}
}
