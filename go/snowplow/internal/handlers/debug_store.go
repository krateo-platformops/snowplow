package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// debugStoreBody is the JSON body returned by /debug/store: the per-object
// store state, plus the cache-subsystem flag its siblings also report.
type debugStoreBody struct {
	// CacheEnabled mirrors the cache subsystem state. When false there is no
	// informer store to read (registered=false); the endpoint still returns
	// 200 so it is usable as a diagnostic regardless of the flag.
	CacheEnabled bool `json:"cacheEnabled"`
	// Embedded, so the wire shape is one flat object.
	cache.StoreObjectState
}

// DebugStore is the read-only PER-OBJECT informer-store diagnostic (#237).
//
// GET /debug/store?gvr=<group>/<version>/<resource>&namespace=<ns>&name=<name>
//
// It answers the one question #237 could not: is the INFORMER STORE stale, or
// is the L1 cell stale? Every other surface is downstream of the store and
// renders those two states identically (the executable proof is
// debug_store_falsifier_test.go's blindness arm). This route reports the
// store's own resourceVersion/uid for one coordinate, which the operator
// compares against `kubectl get -o jsonpath='{.metadata.resourceVersion}'`
// under their OWN RBAC — two sessions collapse to one request plus one kubectl.
//
// NO APISERVER CONTACT, BY CONSTRUCTION. There is no compare mode and no
// client on this path. Reading the apiserver as the snowplow ServiceAccount
// would let any valid-JWT holder learn the existence and resourceVersion of
// objects their own RBAC forbids — a cross-identity read of exactly the class
// the standing debug-surface rule prevents. /debug/apistage is metadata-only
// AND per-identity-scoped by construction; a served comparison would have been
// metadata-only and NOT scoped, which is a different thing.
//
// METADATA AND HASHES ONLY, never a body: the guard is the type
// (cache.StoreObjectState has no []byte, no map, no slice), the same
// structural guard cache.ResolvedEntryMeta gives /debug/apistage.
//
// bodySHA256 IS COMPARABLE ONLY BETWEEN TWO SNOWPLOW READS — never against
// `kubectl get -o json | sha256sum`. The informer's SetTransform has already
// stripped managedFields and last-applied-configuration before the object
// reached the store, so a kubectl comparison reports a divergence that is not
// one. Compare resourceVersion against the apiserver; compare the hash between
// two snowplow reads.
//
// STATUS CODES. 400 for a missing/malformed gvr or name. 200 for everything
// else, INCLUDING a coordinate the store does not hold ({"found": false}) and
// a GVR snowplow has no informer for ({"registered": false}) — never 404,
// because 404 is what an unregistered route looks like and the two must stay
// distinguishable.
//
// READ-ONLY: one rw.mu.RLock, one ListKeys, one GetByKey. No registration, no
// confirm, no Put, no eviction — unlike /debug/reconcile, calling this changes
// nothing.
//
// @Summary Per-object informer-store state
// @Description Read-only metadata-only view of what the informer store holds for one (gvr, namespace, name): resourceVersion, uid, generation, a sha256 of the stored bytes, and the GVR's servability conjuncts and event clocks. Never returns an object body, and never reads the apiserver. bodySHA256 is comparable only between two snowplow reads (the informer transform has already stripped managedFields), not against kubectl.
// @ID debug-store
// @Produce  json
// @Param gvr query string true "group/version/resource (2 segments for the core group, e.g. v1/configmaps)"
// @Param name query string true "object name"
// @Param namespace query string false "object namespace; omit for a cluster-scoped object"
// @Success 200 {object} debugStoreBody
// @Failure 400 {string} string "missing or malformed gvr/name"
// @Router /debug/store [get]
func DebugStore() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		gvr, ok := parseGVRParam(q.Get("gvr"))
		if !ok {
			http.Error(wri, "gvr must be group/version/resource (or version/resource for the core group)", http.StatusBadRequest)
			return
		}
		name := q.Get("name")
		if name == "" {
			http.Error(wri, "name is required", http.StatusBadRequest)
			return
		}

		body := debugStoreBody{
			CacheEnabled:     !cache.Disabled(),
			StoreObjectState: cache.Global().StoreObjectState(gvr, q.Get("namespace"), name), // nil-receiver safe
		}
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(body)
	}
}

// parseGVRParam parses the gvr query parameter.
//
// Accepted: "group/version/resource", and "version/resource" or
// "/version/resource" for the core group (so both `v1/configmaps` and the
// leading-slash form an operator may copy from a path work). Anything else is
// a 400 rather than a lookup of a GVR we guessed at — a diagnostic that
// silently answers about a different resource than the one asked for is worse
// than an error.
func parseGVRParam(raw string) (schema.GroupVersionResource, bool) {
	segs := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	var gvr schema.GroupVersionResource
	switch len(segs) {
	case 2:
		gvr = schema.GroupVersionResource{Version: segs[0], Resource: segs[1]}
	case 3:
		gvr = schema.GroupVersionResource{Group: segs[0], Version: segs[1], Resource: segs[2]}
	default:
		return gvr, false
	}
	if gvr.Version == "" || gvr.Resource == "" {
		return schema.GroupVersionResource{}, false
	}
	return gvr, true
}
