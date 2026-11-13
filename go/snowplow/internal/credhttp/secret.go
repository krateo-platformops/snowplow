package credhttp

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// GetSecret reads one Secret over a hardened copy of rc, on the same client
// FromSecret hands plumbing (#723: no redirect followed; #920: no client-side
// rate limit; #1115: the request's page budget; #1054, #847), and returns it
// whole. It is for a reader that must see the Secret itself and not only the
// Endpoint plumbing's endpoints.FromSecret extracts from it: the requester's
// own credential Secret, whose ownership is proven from its annotations or
// its certificate before it is used (endpoints.OwnsSecret, #1382). The
// caller's rc is not changed. Errors are the apiserver's, unwrapped, so
// callers keep telling a NotFound from any other failure.
func GetSecret(ctx context.Context, rc *rest.Config, name, namespace string) (*corev1.Secret, error) {
	if rc == nil {
		return nil, errors.New("credhttp.GetSecret: no rest config")
	}
	cfg := Harden(rest.CopyConfig(rc))
	cfg.APIPath = "/api"
	cfg.GroupVersion = &corev1.SchemeGroupVersion
	cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	cli, err := rest.RESTClientFor(cfg)
	if err != nil {
		return nil, err
	}
	sec := &corev1.Secret{}
	if err := cli.Get().Namespace(namespace).Resource("secrets").Name(name).Do(ctx).Into(sec); err != nil {
		return nil, err
	}
	return sec, nil
}
