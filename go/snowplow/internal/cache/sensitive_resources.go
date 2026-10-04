// sensitive_resources.go — #398: the single declarative classification of
// resources whose object bodies snowplow must never HOLD in a process-wide
// store.
//
// THE DEFECT (#398): any RESTAction step, widget apiRef / resourcesRef or
// objects.Get on a core `v1/secrets` path lazily registered a Secrets informer
// on the cluster-wide (metav1.NamespaceAll) dynamic factory — holding every
// Secret in the cluster, `.data` included, for the process lifetime — and the
// identity-free apistage content cells then stored the un-gated envelopes the
// informer served. secrets_snapshot.go already named that outcome catastrophic
// and avoided it only for the AUTHN clientconfig Secrets.
//
// THE CLASSIFICATION. Exactly one entry: the core-group `secrets` resource (any
// version). This is a property of the DATA, defined by Kubernetes itself, not a
// resolver special case (feedback_no_special_cases):
//   - Secrets are the Kubernetes credential store by contract (SA tokens,
//     dockerconfigs, TLS keys, Helm release payloads);
//   - they are the default target of encryption at rest
//     (EncryptionConfiguration), and the RBAC docs warn that `list`/`watch` on
//     secrets discloses their contents — which a cluster-wide informer is.
//
// Deliberately NOT classified:
//   - serviceaccounts/token — TokenRequest is a POST; the informer pivot's verb
//     gate (Gate 1) already sends every write verb to the apiserver, and
//     subresources never inform.
//   - configmaps — not credential material by contract, and on the warm path
//     (the seed ClusterRole grants them); classifying them would remove caching
//     from a hot, non-sensitive resource.
//   - certificatesigningrequests — carry a public CSR and the issued
//     certificate, no private key.
// Adding an entry is a one-line, reviewed change here; nothing else in the
// codebase names a resource for this purpose.
//
// ENFORCEMENT POINTS (all read IsSensitiveResource; none re-derive it):
//   1. informer registration — EnsureResourceType returns a closed skip, and
//      addResourceTypeLocked / addResourceTypeMetadataOnlyLocked refuse, so no
//      path (resolver, objects.Get, deps replay, prewarm, eager, CRD repair) can
//      register one. The AUTHN clientconfig informer (secrets_informer.go) is a
//      separate typed factory scoped to AUTHN_NAMESPACE and is not routed here.
//   2. the resolver informer pivot — a dedicated gate before Gate 6 falls the
//      call through to the apiserver with ReasonInformerSensitive. Because the
//      identity-free content layers (apistage content, cluster-list collapse,
//      the refresher's RefreshContentEntry) store ONLY envelopes the informer
//      served, no Secret envelope ever reaches them.
//   3. resolved-output cells — a resolve that dispatched a sensitive read bumps
//      the SensitiveTouchedSink and every resolved-output Put site declines, so
//      no Secret body is resident in any L1 cell (see sensitive_touched_sink.go).

package cache

import (
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// sensitiveResources is the declarative set (group, resource). Version-free:
// every served version of a sensitive resource is sensitive.
var sensitiveResources = map[schema.GroupResource]struct{}{
	{Group: "", Resource: "secrets"}: {},
}

// IsSensitiveResource reports whether gvr names a resource whose bodies must
// never be held in a process-wide store (informer, identity-free cell, or
// resolved-output cell).
func IsSensitiveResource(gvr schema.GroupVersionResource) bool {
	_, ok := sensitiveResources[gvr.GroupResource()]
	return ok
}

// sensitiveRegistrationRefused counts informer registrations refused for a
// sensitive resource (any path). Read by tests; non-zero in production just
// means some RESTAction / widget reads a Secret — which is now served live.
var sensitiveRegistrationRefused atomic.Uint64

func noteSensitiveRegistrationRefused() { sensitiveRegistrationRefused.Add(1) }

// SensitiveRegistrationRefusedForTest reads the counter.
func SensitiveRegistrationRefusedForTest() uint64 { return sensitiveRegistrationRefused.Load() }
