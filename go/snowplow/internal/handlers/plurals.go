package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	snowplowcache "github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Plurals returns the /api-info/names handler. Ship 1 / 0.30.225
// swaps the per-handler plumbing/cache.TTLCache for the process-
// wide permanent store (snowplowcache.PluralsStore() — never
// evicts, populated lazily via apiserver discovery). The handler
// itself carries no cache field; resolution is delegated to
// snowplowcache.PluralFor which encapsulates the store + discovery
// hop + race-safe LoadOrStore.
//
// Pre-Ship-1 behaviour: each Plurals() call constructed a fresh
// TTLCache shared across requests served by THAT handler instance.
// In practice the /api-info/names route registers a single handler
// at startup so the cache was process-shared anyway, but the TTL
// (48h hardcoded in plumbing/cache via plurals.Get) was a
// vestigial bound — CRD plurals are K8s-immutable for the
// lifetime of the CRD object (CRD `metadata.name` MUST equal
// `<plural>.<group>` per apiserver invariant). Ship 1 makes the
// permanence explicit.
func Plurals() http.Handler {
	return &pluralsHandler{}
}

var _ http.Handler = (*pluralsHandler)(nil)

// pluralsHandler is now stateless — store / discovery client are
// owned by snowplowcache.PluralFor. The handler is a thin HTTP
// adapter: parse query → call PluralFor → encode response.
type pluralsHandler struct{}

// @Summary Names Endpoint
// @Description Returns information about Kubernetes API names
// @ID names
// @Param  apiVersion       query   string  true  "API Group and Version"
// @Param  kind             query   string  true  "API Kind"
// @Produce  json
// @Success 200 {object} names
// @Failure 400 {object} response.Status
// @Failure 401 {object} response.Status
// @Failure 404 {object} response.Status
// @Failure 500 {object} response.Status
// @Router /api-info/names [get]
func (r *pluralsHandler) ServeHTTP(wri http.ResponseWriter, req *http.Request) {
	gvk, err := r.validateRequest(req)
	if err != nil {
		response.BadRequest(wri, err)
		return
	}

	log := xcontext.Logger(req.Context())

	start := time.Now()

	rc, err := credhttp.InClusterConfig()
	if err != nil {
		log.Error("unable to load in-cluster rest config",
			slog.String("gvk", gvk.String()), slog.Any("err", err))
		response.InternalError(wri, err)
		return
	}

	tmp, err := snowplowcache.PluralFor(req.Context(), gvk, rc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			response.NotFound(wri, err)
		} else {
			response.InternalError(wri, err)
		}
		return
	}

	// Pre-Ship-1 plumbing/cache plurals.Get returned an apierrors
	// NotFound when len(tmp.Plural) == 0 (plurals.go:61-77). We
	// preserve that contract: PluralFor only returns the zero
	// Info when the apiserver had no entry for the gvk — surface
	// that as 404 to keep the /api-info/names response shape
	// byte-identical against the pre-Ship-1 baseline.
	if len(tmp.Plural) == 0 {
		nfErr := &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Status: metav1.StatusFailure,
				Code:   http.StatusNotFound,
				Reason: metav1.StatusReasonNotFound,
				Details: &metav1.StatusDetails{
					Group: gvk.Group,
					Kind:  gvk.Kind,
				},
				Message: fmt.Sprintf("no names found for %q", gvk.GroupVersion().String()),
			},
		}
		response.NotFound(wri, nfErr)
		return
	}

	log.Info("plurals successfully resolved",
		slog.String("gvk", gvk.String()),
		slog.String("duration", util.ETA(start)),
	)

	wri.Header().Set("Content-Type", "application/json")
	wri.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(wri)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&tmp); err != nil {
		log.Error("unable to serve api call response", slog.Any("err", err))
	}
}

func (r *pluralsHandler) validateRequest(req *http.Request) (gvk schema.GroupVersionKind, err error) {
	apiVersion := req.URL.Query().Get("apiVersion")
	if len(apiVersion) == 0 {
		err = fmt.Errorf("missing 'apiVersion' query parameter")
		return
	}

	kind := req.URL.Query().Get("kind")
	if len(apiVersion) == 0 {
		err = fmt.Errorf("missing 'kind' query parameter")
		return
	}

	var gv schema.GroupVersion
	gv, err = schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return
	}
	gvk = gv.WithKind(kind)

	return
}

type names struct {
	Plural   string   `json:"plural"`
	Singular string   `json:"singular"`
	Shorts   []string `json:"shorts"`
}
