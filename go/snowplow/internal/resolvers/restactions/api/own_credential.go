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
// #1560 — the names and the proof come from credsecret.Select, the one
// selector: for a token with `act` the single name is its delegated
// credential's, and the proof is endpoints.CheckDelegatedCredential
// (credsecret.Credential.Check).
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
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// FromInformerOwnSecret is the informer half of the own-credential read: the
// first of cred.Names whose Secret is in the snapshot and Held for the token
// (cred.Check: endpoints.OwnsSecret for a session or step-up credential,
// endpoints.CheckDelegatedCredential for a delegated one, #1560), as an
// Endpoint.
//
// Returns, like FromInformerSecret:
//   - (endpoint, true,  nil) — a Held Secret, content extracted;
//   - (zero,     false, nil) — nothing servable: cache off, informer not
//     servable, another namespace, or no name holds a Secret Held for the
//     token (absent, someone else's, or a delegated credential that is
//     expired or a mismatch in the snapshot, which may lag a newer exchange).
//     The caller asks the apiserver (FromAPIServerOwnSecret), which decides;
//   - (zero,     false, err) — the first Held Secret is malformed (no
//     server-url): upstream's verbatim error.
func FromInformerOwnSecret(ctx context.Context, namespace string, cred credsecret.Credential) (endpoints.Endpoint, bool, error) {
	_ = ctx
	snap := ownSecretSnapshot(namespace)
	if snap == nil {
		return endpoints.Endpoint{}, false, nil
	}
	now := time.Now()
	for _, name := range cred.Names {
		sec, ok := snap.ByName[name]
		if !ok || sec == nil || cred.Check(sec, now) != credsecret.Held {
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

// FromAPIServerOwnSecret is the apiserver half: cred.Names in order, one GET
// each over rc (credhttp.GetSecret), until one returns a Secret Held for the
// token (cred.Check). A NotFound, or a Secret the token does not own, moves to
// the next name; any other error is returned at once, unwrapped (a 403 or a
// 500 is not a miss, and a spent page budget is the caller's to answer). When
// no name holds a Held Secret:
//   - #1560 — if a name held the token's delegated credential but its
//     certificate is Expired or a Mismatch, the error is a
//     *credsecret.RefusedError with that outcome (the client exchanges
//     again);
//   - otherwise it is a NotFound for the first, preferred name: the
//     apiserver's own when that name was absent, else the same NotFound built
//     here, so a Secret that exists but is someone else's is reported exactly
//     as a missing one and never named.
func FromAPIServerOwnSecret(ctx context.Context, rc *rest.Config, namespace string, cred credsecret.Credential) (endpoints.Endpoint, error) {
	var firstMissing error
	var refused *credsecret.RefusedError
	now := time.Now()
	for i, name := range cred.Names {
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
		switch o := cred.Check(sec, now); o {
		case credsecret.Held:
			return extractEndpointFromSecret(sec)
		case credsecret.Expired, credsecret.Mismatch:
			if refused == nil {
				refused = &credsecret.RefusedError{Outcome: o}
			}
		}
	}
	if refused != nil {
		return endpoints.Endpoint{}, refused
	}
	if firstMissing != nil {
		return endpoints.Endpoint{}, firstMissing
	}
	first := ""
	if len(cred.Names) > 0 {
		first = cred.Names[0]
	}
	return endpoints.Endpoint{}, apierrors.NewNotFound(corev1.Resource("secrets"), first)
}
