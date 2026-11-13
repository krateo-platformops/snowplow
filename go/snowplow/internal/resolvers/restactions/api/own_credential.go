// own_credential.go — #1382: the requester's own credential Secret, read
// exact-then-legacy and used only once it is proven the requester's.
//
// authn#140 names a user's session Secret after the EXACT username whenever
// that is a valid Secret name, and after the username made DNS-1123
// compatible (the legacy name) otherwise and before it. Through the migration
// a reader tries every name plumbing gives (credsecret.For: the exact name,
// then the legacy one where it differs; the step-up names likewise). A name
// is not proof of ownership: john.doe and johndoe share the legacy name
// johndoe-clientconfig, and a legacy name can be another username's exact
// name. So every Secret found, under any name and from either source, is used
// only when endpoints.OwnsSecret(sec, requester) holds; anything else is a
// miss, never another user's credential.
//
// Two sources, in this order:
//
//  1. The AUTHN_NAMESPACE Secrets informer (FromInformerOwnSecret): every
//     name, in order. The first Secret the requester owns is the answer. A
//     name absent from the snapshot, or present but not the requester's, is
//     passed over.
//  2. The apiserver (FromAPIServerOwnSecret), only when the informer answered
//     nothing: every name, in order, one GET each until one returns a Secret
//     the requester owns. That is the extra lookup a miss costs: a second GET
//     when the username's exact and legacy names differ and the first is
//     absent (BenchmarkIssue1382_*).
//
// The informer is consulted for EVERY name before the apiserver for any: a
// user who has only a legacy Secret (every user whose exact and legacy names
// differ, until authn writes exact names) is served from memory, as today,
// rather than paying an apiserver GET for the absent exact name on every
// request.
package api

import (
	"context"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// FromInformerOwnSecret is the informer half of the own-credential read: the
// first of names whose Secret is in the snapshot and owned by username
// (endpoints.OwnsSecret), as an Endpoint.
//
// Returns, like FromInformerSecret:
//   - (endpoint, true,  nil) — an owned Secret, content extracted;
//   - (zero,     false, nil) — nothing servable: cache off, informer not
//     servable, another namespace, or no name holds a Secret username owns.
//     The caller asks the apiserver (FromAPIServerOwnSecret);
//   - (zero,     false, err) — the first owned Secret is malformed (no
//     server-url): upstream's verbatim error.
func FromInformerOwnSecret(ctx context.Context, namespace, username string, names []string) (endpoints.Endpoint, bool, error) {
	_ = ctx
	snap := ownSecretSnapshot(namespace)
	if snap == nil {
		return endpoints.Endpoint{}, false, nil
	}
	for _, name := range names {
		sec, ok := snap.ByName[name]
		if !ok || sec == nil || !endpoints.OwnsSecret(sec, username) {
			continue
		}
		ep, err := extractEndpointFromSecret(sec)
		if err != nil {
			return endpoints.Endpoint{}, false, err
		}
		return ep, true, nil
	}
	return endpoints.Endpoint{}, false, nil
}

// ownSecretSnapshot is the Secrets snapshot when it can serve namespace, nil
// otherwise: the same gates as FromInformerSecret.
func ownSecretSnapshot(namespace string) *cache.SecretsSnapshot {
	if cache.Disabled() || !cache.SecretsCacheServable() || namespace != cache.SecretsCacheNamespace() {
		return nil
	}
	return cache.SecretsSnapshotLoad()
}

// FromAPIServerOwnSecret is the apiserver half: names in order, one GET each
// over rc (credhttp.GetSecret), until one returns a Secret username owns. A
// NotFound, or a Secret username does not own, moves to the next name; any
// other error is returned at once, unwrapped (a 403 or a 500 is not a miss,
// and a spent page budget is the caller's to answer). When no name holds an
// owned Secret the error is a NotFound for the first, preferred name: the
// apiserver's own when that name was absent, else the same NotFound built
// here, so a Secret that exists but is someone else's is reported exactly as
// a missing one and never named.
func FromAPIServerOwnSecret(ctx context.Context, rc *rest.Config, namespace, username string, names []string) (endpoints.Endpoint, error) {
	var firstMissing error
	for i, name := range names {
		sec, err := credhttp.GetSecret(ctx, rc, name, namespace)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return endpoints.Endpoint{}, err
			}
			if i == 0 {
				firstMissing = err
			}
			continue
		}
		if !endpoints.OwnsSecret(sec, username) {
			continue
		}
		return extractEndpointFromSecret(sec)
	}
	if firstMissing != nil {
		return endpoints.Endpoint{}, firstMissing
	}
	first := ""
	if len(names) > 0 {
		first = names[0]
	}
	return endpoints.Endpoint{}, apierrors.NewNotFound(corev1.Resource("secrets"), first)
}
