package credhttp

// ratelimit_920_test.go — #920: no client snowplow builds carries client-go's
// client-side rate limiter (5 requests/s, burst 10, whenever a config leaves
// QPS at 0 and RateLimiter nil). Server-side Priority & Fairness is the
// authority.
//
// The instrument reads the limiter client-go actually installed on each
// client (RESTClient.GetRateLimiter, through every group of a clientset), not
// the config's fields, and its control arm proves it sees a limiter that is
// there: a client-go client built from the same config sources BEFORE they
// pass through this package reports the 5/s bucket.

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/kubeconfig"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// limiters returns, by name, the rate limiter of every REST client inside c.
// A nil value is a client with no client-side limiter.
func limiters(t *testing.T, c any) map[string]flowcontrol.RateLimiter {
	t.Helper()
	out := map[string]flowcontrol.RateLimiter{}
	// Concrete types, not method sets: a clientset embeds *discovery.DiscoveryClient,
	// so it too has RESTClient() and would be read as its discovery client alone.
	switch x := c.(type) {
	case *rest.RESTClient:
		out["rest"] = x.GetRateLimiter()
		return out
	case *discovery.DiscoveryClient:
		out["discovery"] = x.RESTClient().GetRateLimiter()
		return out
	}
	v := reflect.ValueOf(c)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		t.Fatalf("limiters: unsupported client %T", c)
	}
	// *dynamic.DynamicClient and *metadata.Client keep their REST client in an
	// unexported field named client; read it as is.
	if f := v.Elem().FieldByName("client"); f.IsValid() {
		inner := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface()
		ri, ok := inner.(rest.Interface)
		if !ok || reflect.ValueOf(ri).IsNil() {
			t.Fatalf("limiters: %T.client is %T, not a REST client", c, inner)
		}
		out["client"] = ri.GetRateLimiter()
		return out
	}
	// A clientset: every group accessor (and Discovery) exposes RESTClient().
	for i := 0; i < v.NumMethod(); i++ {
		m := v.Type().Method(i)
		if m.Type.NumIn() != 1 || m.Type.NumOut() != 1 {
			continue
		}
		g := v.Method(i).Call(nil)[0]
		if g.Kind() == reflect.Interface && g.IsNil() {
			continue
		}
		if rc, ok := g.Interface().(interface{ RESTClient() rest.Interface }); ok {
			out[m.Name] = rc.RESTClient().GetRateLimiter()
		}
	}
	if len(out) == 0 {
		t.Fatalf("limiters: found no REST client inside %T", c)
	}
	return out
}

// clientBuilders is every client constructor this package exports.
var clientBuilders = map[string]func(*rest.Config) (any, error){
	"Dynamic":       func(rc *rest.Config) (any, error) { return Dynamic(rc) },
	"Metadata":      func(rc *rest.Config) (any, error) { return Metadata(rc) },
	"Kubernetes":    func(rc *rest.Config) (any, error) { return Kubernetes(rc) },
	"APIExtensions": func(rc *rest.Config) (any, error) { return APIExtensions(rc) },
	"Discovery":     func(rc *rest.Config) (any, error) { return Discovery(rc) },
	"RESTClient": func(rc *rest.Config) (any, error) {
		cfg := rest.CopyConfig(rc)
		cfg.APIPath = "/api"
		cfg.GroupVersion = &corev1.SchemeGroupVersion
		cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
		return RESTClient(cfg)
	},
}

const testHost = "https://127.0.0.1:6443"

// rawInCluster is the shape rest.InClusterConfig returns: no QPS, no Burst,
// no RateLimiter (so client-go would install its 5/s default).
func rawInCluster() (*rest.Config, error) {
	return &rest.Config{Host: testHost, BearerToken: "sa-token"}, nil
}

var testEndpoint = endpoints.Endpoint{ServerURL: testHost, Token: "user-token"}

// configSources is every constructor this package exports that returns a
// *rest.Config, each from the same raw source the production one wraps.
func configSources(t *testing.T) map[string]*rest.Config {
	t.Helper()
	orig := inClusterConfig
	inClusterConfig = rawInCluster
	t.Cleanup(func() { inClusterConfig = orig })

	ic, err := InClusterConfig()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := NewClientConfig(context.Background(), testEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	// Harden is the exported form both use; it must also remove a limiter a
	// config already carries.
	drained := flowcontrol.NewTokenBucketRateLimiter(5, 10)
	hd := Harden(&rest.Config{Host: testHost, QPS: 5, Burst: 10, RateLimiter: drained})
	return map[string]*rest.Config{"InClusterConfig": ic, "NewClientConfig": cc, "Harden": hd}
}

// Control: the instrument sees the default limiter when it is there. Built
// from the raw sources (before this package touches them) with client-go's
// own constructor, a client carries the 5/s bucket.
func TestIssue920_Control_InstrumentSeesClientGoDefaultLimiter(t *testing.T) {
	raw, _ := rawInCluster()
	cc, err := kubeconfig.NewClientConfig(context.Background(), testEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	for name, rc := range map[string]*rest.Config{"rest.InClusterConfig": raw, "kubeconfig.NewClientConfig": cc} {
		cs, err := kubernetes.NewForConfig(rc)
		if err != nil {
			t.Fatal(err)
		}
		got := limiters(t, cs)
		// The clientset has dozens of API groups; reading only a few would mean
		// the instrument stopped at an embedded client.
		if len(got) < 20 {
			t.Errorf("control %s: read only %d REST clients from the clientset (%v); the instrument does not walk every group", name, len(got), got)
		}
		for group, rl := range got {
			if rl == nil {
				t.Errorf("control %s/%s: no limiter seen on a client-go default client; the instrument is blind", name, group)
			} else if rl.QPS() != rest.DefaultQPS {
				t.Errorf("control %s/%s: limiter QPS %v, want client-go's default %v", name, group, rl.QPS(), rest.DefaultQPS)
			}
		}
	}
}

// Every client built by every client constructor, from every config
// constructor, has no client-side limiter. The "unhardened" source is a config
// that never passed through this package and already carries a limiter: the
// client constructors remove it on their own copy, so each one is checked
// alone and not only through the config it was handed.
func TestIssue920_EveryClientFromEveryConstructorHasNoClientRateLimit(t *testing.T) {
	sources := configSources(t)
	sources["unhardened"] = &rest.Config{Host: testHost, QPS: 5, Burst: 10, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(5, 10)}
	for src, rc := range sources {
		for name, build := range clientBuilders {
			c, err := build(rc)
			if err != nil {
				t.Fatalf("%s from %s: %v", name, src, err)
			}
			for inner, rl := range limiters(t, c) {
				if rl != nil {
					t.Errorf("%s from %s (%s): RED — client-side limiter installed (QPS %v); #920 requires none", name, src, inner, rl.QPS())
				}
			}
		}
	}
}

// Every config this package returns carries the no-limiter setting itself,
// so a client built from it by code OUTSIDE this package (plumbing's
// endpoints.FromSecret builds its own REST client from the config it is
// given), or from a rest.CopyConfig of it (the #574 impersonating copy), has
// no limiter either. Shown with client-go's own constructor.
func TestIssue920_ConfigsCarryNoLimiterForClientsBuiltElsewhere(t *testing.T) {
	for src, rc := range configSources(t) {
		if rc.QPS >= 0 || rc.RateLimiter != nil {
			t.Errorf("%s: RED — QPS %v, RateLimiter %v; want QPS < 0 and no RateLimiter", src, rc.QPS, rc.RateLimiter)
		}
		cp := rest.CopyConfig(rc)
		cp.Impersonate = rest.ImpersonationConfig{UserName: "u"}
		for _, cfg := range []*rest.Config{rc, cp} {
			cs, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for group, rl := range limiters(t, cs) {
				if rl != nil {
					t.Errorf("%s/%s (client-go constructor): RED — limiter installed (QPS %v)", src, group, rl.QPS())
				}
			}
		}
	}
}

// The client constructors work on their own copy: the caller's config is
// left as it was.
func TestIssue920_ClientConstructorsDoNotMutateTheCallersConfig(t *testing.T) {
	for name, build := range clientBuilders {
		rl := flowcontrol.NewTokenBucketRateLimiter(5, 10)
		rc := &rest.Config{Host: testHost, QPS: 5, Burst: 10, RateLimiter: rl}
		if _, err := build(rc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if rc.QPS != 5 || rc.Burst != 10 || rc.RateLimiter != rl {
			t.Errorf("%s mutated the caller's config: QPS %v Burst %d RateLimiter %v", name, rc.QPS, rc.Burst, rc.RateLimiter)
		}
	}
}
