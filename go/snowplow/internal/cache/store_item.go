// store_item.go — #624: the ONE converter every serve path renders an
// informer store slot through.
//
// THE DEFECT IT CLOSES. The LIST serve paths (listFromIndexer → decoded
// branch; rawFromIndexer → the #578 raw branch) and the GET-by-name path
// each rendered a slot through their own type switch that knew only
// *bytesObject and *unstructured.Unstructured. The four RBAC GVRs store
// TYPED *rbacv1 objects (strip.go typedResourceOverrides — EvaluateRBAC
// reads them without a per-call FromUnstructured), so every one of their
// slots fell into `default: return nil, false` and was DROPPED. The GVR is
// servable, so the caller received ([], served=true): an empty list as the
// truth. A portal grant was created and never shown.
//
// THE STRUCTURAL FIX. One converter, three serve sites, so they cannot
// disagree about what a slot is:
//
//   - *bytesObject              → Decode (decoded) / stored raw (raw).
//   - *unstructured.Unstructured → as is.
//   - *metav1.PartialObjectMetadata → NOT renderable. It carries no
//     spec/status; serving it as a full object would be wrong data. (The
//     metadata-only routing is inert today — cache_mode.go #197 — so this
//     is defence, not a live population.)
//   - any other runtime.Object (the typed kinds) →
//     runtime.DefaultUnstructuredConverter.ToUnstructured, with apiVersion/
//     kind recovered from the client-go scheme if the slot's TypeMeta is
//     empty. A typed kind the scheme does not know AND that carries no
//     TypeMeta is not renderable.
//   - anything else → not renderable.
//
// FAIL CLOSED, NEVER SHRINK. "Not renderable" is never a silent per-item
// drop any more. A LIST containing one unrenderable slot is not served at
// all (served=false → the caller falls through to the apiserver, which
// answers correctly), and a GET of one misses the same way. Each such event
// is counted in snowplow_informer_unrenderable_store_items_total{site,gvr}
// (expvar + OTLP) and logged at WARN with the Go type. A non-zero value
// means a store holds a shape the serve paths cannot render — a regression
// to fix, not load to tune. Zero is only meaningful alongside a served
// LIST count for the same GVR (an unused GVR also reads zero).
//
// WHY CONVERT RATHER THAN FALL THROUGH FOR TYPED GVRs. Falling through
// would be correct but would send every RBAC LIST to the apiserver for no
// reason: the informer already holds the complete object. ToUnstructured of
// a typed object that FromUnstructured produced is the same JSON value the
// apiserver serves (the arms in issue624_*_test.go assert it, item for item,
// against the apiserver's list on both branches).
//
// SCOPE: the RBAC-snapshot rebuild (rbac_snapshot.go
// fallbackUnstructuredFromIndexer) and the CRD side-effect handlers keep
// decodeBytesObject, deliberately. The snapshot reads typed slots
// DIRECTLY and only falls back for a slot that is NOT the expected typed
// pointer; routing that fallback through a typed→Unstructured conversion
// would change nothing on the happy path and could only widen what the
// security-critical evaluator accepts. GetTypedObject/ListTypedObjects
// (asRuntimeObject) return typed slots unchanged — they are the RBAC
// evaluator's read API, not a serve path.
package cache

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kscheme "k8s.io/client-go/kubernetes/scheme"
)

// storeItemToUnstructured renders one informer store slot as a FRESH
// *unstructured.Unstructured private to the caller (for a stored
// Unstructured, the stored pointer — the pre-#624 contract, unchanged).
// Returns (nil, false) only for a slot that cannot be rendered faithfully;
// callers MUST fail the whole read closed on false, never skip the slot.
func storeItemToUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	switch v := obj.(type) {
	case *bytesObject:
		if v == nil {
			return nil, false
		}
		uns, err := v.Decode()
		if err != nil || uns == nil {
			return nil, false
		}
		return uns, true
	case *unstructured.Unstructured:
		if v == nil {
			return nil, false
		}
		return v, true
	case *metav1.PartialObjectMetadata:
		// Metadata-only: no spec/status. Rendering it would serve a
		// truncated object as the whole one.
		return nil, false
	case runtime.Object:
		return typedToUnstructured(v)
	default:
		return nil, false
	}
}

// typedToUnstructured converts a typed store object. The informer store
// object is shared and immutable; ToUnstructured only reads it and builds a
// new map, so the result is private to the caller (the SB-4 contract).
func typedToUnstructured(obj runtime.Object) (*unstructured.Unstructured, bool) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil || m == nil {
		return nil, false
	}
	uns := &unstructured.Unstructured{Object: m}
	if uns.GetAPIVersion() == "" || uns.GetKind() == "" {
		// The informer delivers objects with TypeMeta set (the dynamic
		// client stamps apiVersion/kind on every list item and watch event,
		// and FromUnstructured carries them into the typed TypeMeta). An
		// empty one is recovered from the Go type; an item the apiserver
		// would serve with apiVersion/kind must never be served without.
		gvks, _, err := kscheme.Scheme.ObjectKinds(obj)
		if err != nil || len(gvks) == 0 || gvks[0].Kind == "" {
			return nil, false
		}
		uns.SetAPIVersion(gvks[0].GroupVersion().String())
		uns.SetKind(gvks[0].Kind)
	}
	return uns, true
}

// storeItemJSON renders one store slot as item JSON for the raw (#578)
// branch. The *bytesObject arm returns the stored array (no decode, no
// marshal, no allocation). Every other renderable slot is marshalled from
// the SAME storeItemToUnstructured output the decoded branch marshals, so
// the two branches are byte-identical item for item.
func storeItemJSON(obj interface{}) ([]byte, bool) {
	if bo, ok := obj.(*bytesObject); ok {
		if bo == nil || len(bo.raw) == 0 {
			return nil, false
		}
		return bo.raw, true
	}
	uns, ok := storeItemToUnstructured(obj)
	if !ok {
		return nil, false
	}
	b, err := json.Marshal(uns.Object)
	if err != nil {
		return nil, false
	}
	return b, true
}

// unrenderableStoreItemError names the first slot that failed to render.
type unrenderableStoreItemError struct {
	goType string
}

func (e *unrenderableStoreItemError) Error() string {
	return "informer store slot of type " + e.goType + " cannot be rendered"
}

func newUnrenderableStoreItemError(obj interface{}) error {
	return &unrenderableStoreItemError{goType: fmt.Sprintf("%T", obj)}
}

// Serve sites, the closed `site` label set.
const (
	unrenderableSiteListDecoded = "list_decoded"
	unrenderableSiteListRaw     = "list_raw"
	unrenderableSiteGet         = "get"
)

var (
	unrenderableStoreItemsTotal atomic.Uint64
	// unrenderableStoreItemCells: "site|gvr" -> *atomic.Uint64. Cardinality
	// is bounded by 3 sites × registered GVRs, and in a healthy process it
	// is empty.
	unrenderableStoreItemCells sync.Map
)

// recordUnrenderableStoreItem counts and logs one fail-closed serve.
func recordUnrenderableStoreItem(site string, gvr schema.GroupVersionResource, err error) {
	unrenderableStoreItemsTotal.Add(1)
	key := site + "|" + gvr.String()
	c, _ := unrenderableStoreItemCells.LoadOrStore(key, new(atomic.Uint64))
	c.(*atomic.Uint64).Add(1)
	slog.Warn("cache.store_item.unrenderable",
		slog.String("subsystem", "cache"),
		slog.String("site", site),
		slog.String("gvr", gvr.String()),
		slog.String("error", err.Error()),
		slog.String("action", "fallthrough_to_apiserver"),
	)
}

// UnrenderableStoreItemsTotal is the grand total of fail-closed serves
// caused by an unrenderable store slot (#624). Read by the OTLP callback.
func UnrenderableStoreItemsTotal() uint64 { return unrenderableStoreItemsTotal.Load() }

// UnrenderableStoreItemCell is one (site, gvr) cell of the breakdown.
type UnrenderableStoreItemCell struct {
	Site  string
	GVR   string
	Count uint64
}

// UnrenderableStoreItemsSnapshot returns the per-(site, gvr) breakdown.
func UnrenderableStoreItemsSnapshot() []UnrenderableStoreItemCell {
	var out []UnrenderableStoreItemCell
	unrenderableStoreItemCells.Range(func(k, v any) bool {
		site, gvr, _ := strings.Cut(k.(string), "|")
		out = append(out, UnrenderableStoreItemCell{Site: site, GVR: gvr, Count: v.(*atomic.Uint64).Load()})
		return true
	})
	return out
}

// RecordUnrenderableStoreItemForTest drives the counter through the
// production record path from another package's test (internal/metrics'
// OTLP arm). Never called in production.
func RecordUnrenderableStoreItemForTest(site string, gvr schema.GroupVersionResource) {
	recordUnrenderableStoreItem(site, gvr, &unrenderableStoreItemError{goType: "test"})
}

var unrenderableStoreItemsExpvarOnce sync.Once

func init() {
	// CFG-1: no cache subsystem under CACHE_ENABLED=false → no key.
	if Disabled() {
		return
	}
	registerUnrenderableStoreItemsExpvar()
}

func registerUnrenderableStoreItemsExpvar() {
	unrenderableStoreItemsExpvarOnce.Do(func() {
		expvar.Publish("snowplow_informer_unrenderable_store_items_total", expvar.Func(func() any {
			return unrenderableStoreItemsTotal.Load()
		}))
		expvar.Publish("snowplow_informer_unrenderable_store_items_cells", expvar.Func(func() any {
			m := map[string]uint64{}
			for _, c := range UnrenderableStoreItemsSnapshot() {
				m[c.Site+"|"+c.GVR] = c.Count
			}
			return m
		}))
	})
}
