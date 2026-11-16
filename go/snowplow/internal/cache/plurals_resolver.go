// plurals_resolver.go — Ship 1 / 0.30.225 (v6 design,
// docs/walker-driven-informer-design-2026-06-01.md §3.2 Layer 5).
//
// PURPOSE — process-wide permanent store for kind ↔ plural resolution.
// REPLACES the per-handler TTL-based plumbing/cache.TTLCache that
// internal/handlers/plurals.go used to construct on each Plurals()
// call. The architectural invariant (§2.1 of the design doc) is that
// the (group, version, kind) → plural mapping is K8s-immutable for
// the lifetime of a CRD object — CRD `metadata.name` is required to
// equal `<plural>.<group>` by the apiserver, so a CRD cannot be
// renamed at runtime. Built-in scheme entries are compile-time
// constants. Therefore there is no soundness reason to expire
// entries; the 48h TTL in plumbing/cache was vestigial.
//
// SHIP 1 SCOPE — additive. Public surface (4 functions) is added,
// internal/handlers/plurals.go swaps the per-handler TTL store for
// the shared permanent store via cache.PluralsStore(). No resolver-
// path call sites are swapped — those are Ship 2 territory.
//
// STORE SHAPE — sync.Map keyed by schema.GroupVersionKind, values
// are plurals.Info. Permanent — entries never expire. Population
// is lazy: first PluralFor(gvk) miss issues one
// ServerResourcesForGroupVersion hop, builds the full Info
// (Plural + Singular + Shorts) from the apiserver response, and
// stores it via LoadOrStore. Memory footprint is bounded by the
// GVR set (~50 at production scale, ~bytes per entry), not by
// traffic. < 100 KiB worst-case ceiling per design §4.4.
//
// BUILT-IN FAST PATHS — GVRFor / KindForGVR consult a pre-built
// init() map of (scheme.Scheme.AllKnownTypes() + apiextensions/v1)
// → GVR / Kind. This satisfies the bench gate (built-in arm ≤100
// ns/op, zero allocs/op) and avoids any apiserver hop on the
// resolver hot path for built-in kinds (Ship 2 will wire these in
// as the replacement for internal/dynamic.{KindFor,ResourceFor}).
// PluralFor does NOT consult the built-in arm: the /api-info/names
// handler requires the full Info shape (Singular + Shorts), which
// meta.UnsafeGuessKindToResource cannot produce (it derives only
// Plural). Going through discovery preserves byte-identical /api-
// info/names responses against the pre-Ship-1 baseline.
//
// COUNTERS — new ReasonPluralsDiscoveryHop fires once per gvk per
// process lifetime (monotonic up to the bounded GVR ceiling).
// Telemetry only; never short-circuits. See
// fallthrough_meter.go for the recording mechanism.
//
// CACHE-TOGGLE COMPLIANCE — project_cache_off_is_transparent_fallback.
// Under CACHE_ENABLED=false the handler caller still benefits from
// the permanent store (cache=off mode is a fallback to direct
// apiserver, but in-process kind/plural resolution is correctness-
// neutral). The discovery hop is recorded via
// RecordApiserverFallthrough which short-circuits when
// cache.Disabled() == true (per fallthrough_meter.go contract).

package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/krateo-platformops/plumbing/kubeutil/plurals"
	"github.com/krateo-platformops/snowplow/internal/credhttp"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// pluralsStore is the process-wide permanent (gvk → plurals.Info)
// table. Populated lazily by PluralFor's discovery arm via
// LoadOrStore. Never evicts. Bounded by GVR set, not by traffic.
//
// Race-safety: sync.Map is safe for concurrent Load / Store /
// LoadOrStore — no external mutex required. LoadOrStore guarantees
// that two goroutines racing to populate the same gvk converge on
// a single value; the loser drops its fresh Info on the floor.
var pluralsStore sync.Map // schema.GroupVersionKind → plurals.Info

// pluralsKindReverseStore is the reverse lookup (gvr → kind). Built
// in lockstep with pluralsStore — every PluralFor discovery hop
// also populates the reverse index, so KindForGVR for a previously-
// resolved gvr is a single sync.Map.Load.
var pluralsKindReverseStore sync.Map // schema.GroupVersionResource → string (Kind)

// builtinGVKToGVR maps every (GVK) in the standard Kubernetes API
// surface + apiextensions/v1 to its GVR. Initialised once at
// package load via init(). Used by GVRFor's fast path — zero-
// allocation O(1) lookup for built-in kinds.
//
// kscheme.Scheme wires core/v1, apps/v1, batch/v1, rbac/v1,
// networking/v1, etc. We layer apiextensions/v1 on top because the
// client-go scheme does not include it by default; the CRD GVR
// must be resolvable via the fast path to preserve the Ship 0.5
// invariant (the CRD informer is never spawned for the CRD GVR
// itself, but resolver paths that form `customresourcedefinitions`
// GVR via this fast path are correct because they go through
// EnsureResourceType which is idempotent and a no-op if the
// informer is already registered).
var builtinGVKToGVR map[schema.GroupVersionKind]schema.GroupVersionResource

// builtinGVRToKind is the reverse direction for the built-in
// scheme — used by KindForGVR's fast path. Same source-of-truth
// as builtinGVKToGVR, populated together at init().
var builtinGVRToKind map[schema.GroupVersionResource]string

func init() {
	builtinGVKToGVR = make(map[schema.GroupVersionKind]schema.GroupVersionResource)
	builtinGVRToKind = make(map[schema.GroupVersionResource]string)

	// Standard Kubernetes API surface.
	for gvk := range kscheme.Scheme.AllKnownTypes() {
		gvr, _ := meta.UnsafeGuessKindToResource(gvk)
		builtinGVKToGVR[gvk] = gvr
		builtinGVRToKind[gvr] = gvk.Kind
	}

	// apiextensions/v1 — explicitly layered. kscheme.Scheme does not
	// include it; we extract its known types via a private scheme so
	// we do not pollute the global client-go scheme.
	extScheme := runtime.NewScheme()
	_ = apiextensionsv1.AddToScheme(extScheme)
	for gvk := range extScheme.AllKnownTypes() {
		gvr, _ := meta.UnsafeGuessKindToResource(gvk)
		builtinGVKToGVR[gvk] = gvr
		builtinGVRToKind[gvr] = gvk.Kind
	}
}

// PluralFor resolves a GVK to its plurals.Info. ALWAYS returns full-
// shape Info (Plural + Singular + Shorts) when the gvk exists at
// the apiserver — populated via one ServerResourcesForGroupVersion
// hop on the first miss, served from the permanent store
// thereafter.
//
// WHY no built-in fast path here — the /api-info/names handler
// (internal/handlers/plurals.go) is the Ship 1 consumer. It
// requires byte-identical response shape vs the pre-Ship-1
// baseline. meta.UnsafeGuessKindToResource can only fill `Plural`;
// `Singular` and `Shorts` come exclusively from the apiserver's
// APIResource record. Routing built-in GVKs through discovery
// preserves the response body byte-for-byte. The marginal cost
// (one discovery hop per built-in GVK per process lifetime) is
// bounded by the GVR set.
//
// ERROR SHAPES — propagated verbatim from the discovery client.
// apiserver 404 (group / version absent, or kind absent within
// the version) surfaces as an apierrors.NewNotFound-style error
// the caller can detect via apierrors.IsNotFound. Other errors
// (auth, network, malformed response) surface as wrapped errors.
//
// Idempotent under concurrent calls — sync.Map.LoadOrStore is
// the race-safe init pattern. Concurrent callers racing on the
// same gvk converge on a single stored Info; the loser drops its
// fresh discovery result on the floor (no overwrite, no extra
// apiserver hop after the winner stores).
//
// Soft-fail on nil rc — returns a plain error rather than panic
// so the handler's existing apierrors.IsNotFound check still
// works (the handler funnels into response.InternalError).
func PluralFor(ctx context.Context, gvk schema.GroupVersionKind, rc *rest.Config) (plurals.Info, error) {
	if v, ok := pluralsStore.Load(gvk); ok {
		return v.(plurals.Info), nil
	}
	info, err := discoverPluralInfo(ctx, gvk, rc)
	if err != nil {
		return plurals.Info{}, err
	}
	actual, _ := pluralsStore.LoadOrStore(gvk, info)
	got := actual.(plurals.Info)
	// Mirror into the reverse index. Re-do under LoadOrStore in case
	// two PluralFor calls for distinct GVKs that resolve to the same
	// GVR race; the reverse index is correctness-neutral on collision
	// (both kinds map to the same Resource string by apiserver
	// convention).
	gvr := schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: got.Plural,
	}
	pluralsKindReverseStore.LoadOrStore(gvr, gvk.Kind)
	return got, nil
}

// GVRFor resolves a GVK to its GVR. Built-in scheme arm first
// (≤100 ns/op zero-alloc per bench gate), falls back to PluralFor
// (discovery + store) for CRD-backed kinds.
//
// Used (in Ship 2) by internal/resolvers/crds/schema/schema.go to
// form the CRD GVR for a composition object. Built-in arm covers
// CustomResourceDefinition itself; discovery arm covers the
// composition GVR being inspected.
func GVRFor(ctx context.Context, gvk schema.GroupVersionKind, rc *rest.Config) (schema.GroupVersionResource, error) {
	if gvr, ok := builtinGVKToGVR[gvk]; ok {
		return gvr, nil
	}
	info, err := PluralFor(ctx, gvk, rc)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	return schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: info.Plural,
	}, nil
}

// KindForGVR resolves a GVR to its Kind. Built-in scheme arm first
// (zero-alloc O(1) lookup), falls back to the permanent reverse
// store, then to discovery on miss.
//
// Used (in Ship 2) by internal/resolvers/widgets/resourcesrefs/
// resolve.go to derive the Kind for a widget's resourceRef.gvr.
// Built-in arm covers the standard Kubernetes API surface (Pod,
// Deployment, ConfigMap, etc.). Discovery arm covers CRD-backed
// kinds.
//
// Discovery arm: lists ServerResourcesForGroupVersion for the
// gvr's GroupVersion and finds the APIResource whose Name matches
// gvr.Resource. The Info for the matched kind is also stored into
// pluralsStore (forward direction) — one discovery hop populates
// both indices.
func KindForGVR(ctx context.Context, gvr schema.GroupVersionResource, rc *rest.Config) (string, error) {
	if kind, ok := builtinGVRToKind[gvr]; ok {
		return kind, nil
	}
	if v, ok := pluralsKindReverseStore.Load(gvr); ok {
		return v.(string), nil
	}
	kind, info, err := discoverKindForGVR(ctx, gvr, rc)
	if err != nil {
		return "", err
	}
	// Populate both directions atomically. The forward direction
	// holds the full Info we just resolved; the reverse direction
	// holds the Kind we are about to return.
	gvk := gvr.GroupVersion().WithKind(kind)
	pluralsStore.LoadOrStore(gvk, info)
	actual, _ := pluralsKindReverseStore.LoadOrStore(gvr, kind)
	return actual.(string), nil
}

// IsResolverPluralsHit reports whether KindForGVR(gvr) would be
// served from the in-process cache (builtin fast path OR permanent
// reverse store) without an apiserver discovery hop. Used by the
// resolver call sites in Ship 2 to attribute hit vs miss to the
// per-resolver fall-through cells without changing KindForGVR's
// signature.
//
// Pure read — no LoadOrStore, no mutation. Safe to call from any
// goroutine concurrent with KindForGVR.
func IsResolverPluralsHit(gvr schema.GroupVersionResource) bool {
	if _, ok := builtinGVRToKind[gvr]; ok {
		return true
	}
	if _, ok := pluralsKindReverseStore.Load(gvr); ok {
		return true
	}
	return false
}

// IsResolverGVRHit reports whether GVRFor(gvk) would be served from
// the in-process cache (builtin fast path OR permanent plurals
// store) without an apiserver discovery hop. Sister to
// IsResolverPluralsHit for the GVK→GVR direction.
func IsResolverGVRHit(gvk schema.GroupVersionKind) bool {
	if _, ok := builtinGVKToGVR[gvk]; ok {
		return true
	}
	if _, ok := pluralsStore.Load(gvk); ok {
		return true
	}
	return false
}

// PluralsStore exposes the permanent store for tests / observability
// only. PRODUCTION CODE MUST NOT mutate the returned map directly —
// the only supported writers are PluralFor / KindForGVR (which use
// sync.Map.LoadOrStore for race safety).
//
// Returned value is the live *sync.Map; callers that need a
// snapshot must Range over it.
func PluralsStore() *sync.Map {
	return &pluralsStore
}

// discoverPluralInfo issues one ServerResourcesForGroupVersion hop
// for gvk.GroupVersion() and returns the full Info for the kind.
//
// Records the ReasonPluralsDiscoveryHop counter (telemetry only; no
// behaviour change). The counter is bounded by the GVR set: it
// rises monotonically to a ceiling equal to the number of unique
// CRD-backed GVKs in the walker corpus, then stays.
//
// Uses the same discoveryClientBuilder hook as
// DiscoverGroupResources (discovery_lookup.go:150) so tests can
// swap in a fake without standing up a real REST endpoint.
func discoverPluralInfo(ctx context.Context, gvk schema.GroupVersionKind, rc *rest.Config) (plurals.Info, error) {
	RecordApiserverFallthrough(ctx, ReasonPluralsDiscoveryHop, gvk.String())

	if rc == nil {
		return plurals.Info{}, fmt.Errorf("plurals discovery: nil *rest.Config for %s", gvk)
	}
	// #1115 — this client is built for this miss alone: bind the request's page
	// budget to it, since ServerResourcesForGroupVersion sends on client-go's
	// own context.TODO() (background work has none; nothing changes for it).
	dc, err := discoveryClientBuilder(credhttp.BindRequestBudget(ctx, rc))
	if err != nil {
		return plurals.Info{}, fmt.Errorf("plurals discovery: %w", err)
	}
	list, err := dc.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return plurals.Info{}, err
	}
	if list == nil {
		return plurals.Info{}, nil
	}
	for _, el := range list.APIResources {
		if el.Kind != gvk.Kind {
			continue
		}
		// Skip subresources — they share the parent's Kind but their
		// Name contains "/" (e.g. "pods/status"). The /api-info/names
		// handler expects the resource-level entry; matching
		// subresources first would corrupt the returned Plural.
		if containsSlash(el.Name) {
			continue
		}
		return plurals.Info{
			Plural:   el.Name,
			Singular: el.SingularName,
			Shorts:   el.ShortNames,
		}, nil
	}
	return plurals.Info{}, nil
}

// discoverKindForGVR is the reverse of discoverPluralInfo — lists
// ServerResourcesForGroupVersion for gvr.GroupVersion() and finds
// the APIResource whose Name matches gvr.Resource. Returns the
// resolved Kind plus the full Info (so the forward index can be
// populated in the same hop).
func discoverKindForGVR(ctx context.Context, gvr schema.GroupVersionResource, rc *rest.Config) (string, plurals.Info, error) {
	RecordApiserverFallthrough(ctx, ReasonPluralsDiscoveryHop, gvr.String())

	if rc == nil {
		return "", plurals.Info{}, fmt.Errorf("plurals discovery: nil *rest.Config for %s", gvr)
	}
	// #1115 — this client is built for this miss alone: bind the request's page
	// budget to it, since ServerResourcesForGroupVersion sends on client-go's
	// own context.TODO() (background work has none; nothing changes for it).
	dc, err := discoveryClientBuilder(credhttp.BindRequestBudget(ctx, rc))
	if err != nil {
		return "", plurals.Info{}, fmt.Errorf("plurals discovery: %w", err)
	}
	list, err := dc.ServerResourcesForGroupVersion(gvr.GroupVersion().String())
	if err != nil {
		return "", plurals.Info{}, err
	}
	if list == nil {
		return "", plurals.Info{}, fmt.Errorf("plurals discovery: empty APIResourceList for %s", gvr.GroupVersion())
	}
	for _, el := range list.APIResources {
		if el.Name != gvr.Resource {
			continue
		}
		if containsSlash(el.Name) {
			continue
		}
		return el.Kind, plurals.Info{
			Plural:   el.Name,
			Singular: el.SingularName,
			Shorts:   el.ShortNames,
		}, nil
	}
	return "", plurals.Info{}, fmt.Errorf("plurals discovery: resource %q not found in %s", gvr.Resource, gvr.GroupVersion())
}

// containsSlash is a small inline helper — strings.ContainsRune
// would be equivalent but a single-byte search is cheaper and the
// path is hot enough on first discovery to matter.
func containsSlash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return true
		}
	}
	return false
}

// ResetPluralsStoreForTest zeroes the permanent store and the
// reverse index. TEST-ONLY — production code MUST NOT call it.
// Mirrors the established ResetFallthroughCountersForTest /
// ResetNavigationDiscoveredGroupsForTest pattern.
func ResetPluralsStoreForTest() {
	pluralsStore.Range(func(k, _ any) bool {
		pluralsStore.Delete(k)
		return true
	})
	pluralsKindReverseStore.Range(func(k, _ any) bool {
		pluralsKindReverseStore.Delete(k)
		return true
	})
}
