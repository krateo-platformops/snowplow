// secrets_transform.go — #510. The cache.TransformFunc installed on the
// AUTHN_NAMESPACE Secrets informer factory (secrets_informer.go:200-223).
//
// DEFECT. Without a transform the informer store holds every Secret in
// AUTHN_NAMESPACE IN FULL — `data` included — for the whole process lifetime.
// That is standing exposure, not a wrong answer: it is the same concern #398
// addressed from the other end (the fix belongs where the Secret data is HELD),
// and it widens the blast radius of every downstream logging/telemetry mistake
// (#487/#490, #500).
//
// WHAT THE CONSUMERS ACTUALLY READ. The informer's indexer is reachable through
// exactly one handle (secretsInformerHandle.indexer, secrets_informer.go:98-101,
// published only by StartSecretsInformer); the factory is a local in that
// function and its Lister() is never taken. So the store has exactly three
// consumers:
//
//  1. rebuildSecretsSnapshot (secrets_snapshot.go:178-218) — indexer.List(),
//     reads sec.Name only, and publishes the same pointers in SecretsSnapshot.
//  2. syncLearnedFromSecrets (learned_identities.go:335-407, from the rebuild)
//     — reads sec.Name, sec.ResourceVersion, and through
//     parseClientconfigCertificate (learned_identities.go:275-299) the ONE data
//     key "client-certificate-data" (the x509 material: CN → username,
//     O → groups, NotBefore).
//  3. FromInformerSecret → extractEndpointFromSecret
//     (internal/resolvers/restactions/api/endpoints_cache.go:167-242) — reads
//     the 14 plumbing endpoint labels, and nothing else.
//
// That union is secretsCacheRetainedDataKeys below ("client-certificate-data"
// is in it for both (2) and (3)). Nothing reads Labels, Annotations,
// ManagedFields or StringData.
//
// CONSTRAINTS (all three from #510, all honoured here):
//
//   - INSTALLED BEFORE START. client-go refuses a transform on a started
//     informer (sharedIndexInformer.SetTransform, client-go@v0.35.3
//     tools/cache/shared_informer.go:513-523). informers.WithTransform stores
//     it on the factory (informers/factory.go:103-109) and the factory applies
//     it in InformerFor (informers/factory.go:216) — i.e. at the
//     `factory.Core().V1().Secrets().Informer()` call, secrets_informer.go:225-226,
//     which is strictly before `factory.Start(...)` at secrets_informer.go:270.
//   - TOTAL AND CHEAP. It runs on every object that enters the store from the
//     wire: ADD, UPDATE, DELETE and Replace (the initial LIST and every
//     relist). Only a Resync of an object ALREADY in the store skips it, and
//     that object was transformed on its way in — RealFIFO.addToItems_locked
//     (the_real_fifo.go:138-174) is called with skipTransform=false at :183,
//     :194, :207 and :428 and with true only at :481 (Resync) and :390 (a
//     tombstone built from the known store); DeltaFIFO is the same shape
//     (delta_fifo.go:428-478, Replace at :573 passes Replaced, Resync at :678
//     passes Sync). InOrderInformers defaults ON at 1.33, so RealFIFO is the
//     live path. The function never errors — a non-nil error there makes the
//     FIFO DROP the object — and in the common case (a clientconfig Secret
//     whose keys are all retained) it allocates one struct copy and keeps the
//     decoded map as-is.
//   - DEFENSIVE. A non-Secret object and a DeletedFinalStateUnknown tombstone
//     are both handled. client-go v0.35.3 does not currently hand a tombstone
//     to the transformer (the_real_fifo.go:157-158, delta_fifo.go:445-446),
//     but that is not our contract to lean on, and WithTransform is
//     factory-wide: any other informer ever built from this factory would route
//     through here too.
package cache

import (
	corev1 "k8s.io/api/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// lastAppliedConfigAnnotation holds a verbatim copy of the object as applied —
// `data` and all — whenever the Secret was created with `kubectl apply`. No
// consumer of this store reads any annotation, so it is dropped with the
// unread data keys rather than left resident.
const lastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// secretsCacheRetainedDataKeys is the EXACT union of the Secret data keys the
// three consumers above read. Everything else is dropped before the object
// enters the informer store.
//
// The 14 endpoint labels are the plumbing-parity set declared next to the
// extractor (endpoints_cache.go:49-64). They are re-declared here as literals
// because internal/resolvers/restactions/api imports internal/cache, so the
// dependency cannot run the other way. The duplication is GUARDED, not
// trusted: TestS510_TransformKeepsEveryEndpointField
// (endpoints_cache_transform_510_test.go) extracts an Endpoint from a Secret
// that carries all 14 labels, before and after the transform, and requires
// reflect.DeepEqual — so adding a 15th label to the extractor without adding
// it here fails that arm.
var secretsCacheRetainedDataKeys = map[string]struct{}{
	// x509 / clientconfig material — endpoints AND the #262 S1 identity parse.
	"client-certificate-data":    {},
	"client-key-data":            {},
	"certificate-authority-data": {},
	// endpoint addressing + auth.
	"server-url": {},
	"proxy-url":  {},
	"token":      {},
	"username":   {},
	"password":   {},
	"debug":      {},
	"insecure":   {},
	// endpoint AWS signing inputs.
	"aws-access-key": {},
	"aws-secret-key": {},
	"aws-region":     {},
	"aws-service":    {},
}

// transformSecretForCache is the cache.TransformFunc for the AUTHN_NAMESPACE
// Secrets factory. It returns a reduced *corev1.Secret for a Secret, strips the
// Secret inside a tombstone, and passes anything else through untouched.
//
// It NEVER returns a non-nil error: the FIFO drops an object whose transform
// errors, which would turn a reduction into a correctness bug (a missing
// clientconfig Secret = a cold /call, or a lost identity class).
//
// Idempotent, as client-go requires for objects re-fed through Replace():
// re-running it over its own output changes nothing.
func transformSecretForCache(obj interface{}) (interface{}, error) {
	switch o := obj.(type) {
	case *corev1.Secret:
		if o == nil {
			// A typed nil: hand back exactly what we were given rather than
			// synthesizing anything.
			return obj, nil
		}
		return strippedSecretForCache(o), nil
	case clientcache.DeletedFinalStateUnknown:
		// `o` is a struct copy, so writing o.Obj cannot mutate the caller's
		// tombstone. The Key is preserved — a DELETE must still resolve.
		if inner, ok := o.Obj.(*corev1.Secret); ok && inner != nil {
			o.Obj = strippedSecretForCache(inner)
		}
		return o, nil
	default:
		return obj, nil
	}
}

// strippedSecretForCache returns a shallow copy of sec carrying only what the
// store's consumers read: the metadata, and the data keys in
// secretsCacheRetainedDataKeys.
//
// Shallow by design — ObjectMeta's own maps/slices are aliased, never mutated,
// and the input object is left untouched (client-go v1.27+ permits in-place
// mutation here, but the fake clientset's tracker hands out objects a test may
// still be holding, and aliasing bugs in a transform are the kind that surface
// far from here).
func strippedSecretForCache(sec *corev1.Secret) *corev1.Secret {
	cp := *sec
	cp.Data = retainedSecretData(sec.Data)
	// StringData is write-only (the apiserver never returns it); ManagedFields
	// is pure bloat here. Neither is read by any consumer of this store.
	cp.StringData = nil
	cp.ManagedFields = nil
	if _, has := sec.Annotations[lastAppliedConfigAnnotation]; has {
		out := make(map[string]string, len(sec.Annotations)-1)
		for k, v := range sec.Annotations {
			if k == lastAppliedConfigAnnotation {
				continue
			}
			out[k] = v
		}
		cp.Annotations = out
	}
	return &cp
}

// retainedSecretData drops every data key no consumer reads. When nothing is
// dropped it returns the input map unchanged — the common case for a
// `<user>-clientconfig` Secret, and the reason this is cheap enough to sit on
// every informer event.
func retainedSecretData(in map[string][]byte) map[string][]byte {
	if len(in) == 0 {
		return in
	}
	keep := 0
	for k := range in {
		if _, ok := secretsCacheRetainedDataKeys[k]; ok {
			keep++
		}
	}
	if keep == len(in) {
		return in
	}
	out := make(map[string][]byte, keep)
	for k, v := range in {
		if _, ok := secretsCacheRetainedDataKeys[k]; ok {
			out[k] = v
		}
	}
	return out
}

// SecretsCacheTransformForTest exposes the informer transform so an arm in
// another package can drive the REAL function. TEST-ONLY — production code MUST
// NOT call it (the transform is installed by StartSecretsInformer through
// informers.WithTransform and is never invoked by hand). Same posture as
// SyncLearnedFromSecretsForTest (learned_identities.go:571).
func SecretsCacheTransformForTest(obj interface{}) (interface{}, error) {
	return transformSecretForCache(obj)
}
