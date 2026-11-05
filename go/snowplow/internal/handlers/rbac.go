// rbac.go — GET /rbac: the dispatch-free RESTAction read-set enumeration
// endpoint (design docs/restaction-rbac-endpoint-design.md). core-provider
// calls it to learn the (group, version, resource, verb) tuples resolving a
// RESTAction WILL read, so it can pre-generate the user's RBAC BEFORE the first
// /call.
//
// This handler is THIN: it parses the query (the referenced RA's name/namespace
// + optional extras), loads the RA CR from the informer/apiserver via
// objects.Get, runs the dispatch-free inspect pass (api.InspectReadSet — the
// orchestrator over the existing snowplow primitives), and marshals the result.
// It NEVER dispatches and modifies NONE of the /call dispatch path.
//
// AUTH (design §5): mounted on the SAME middleware.UserConfig as /call — the
// JWT authenticates the caller; the loaded <user>-clientconfig is harmless and
// UNUSED (the enumeration runs under the SA, not the caller's perms). The
// caller needs NONE of the enumerated RBAC perms — that is the whole point
// (the read-set is generated before any binding exists). /rbac is DELIBERATELY
// NOT cache.RegisterScopedRoute'd: it issues zero per-user apiserver reads
// (mirrors /refreshes), so it sits outside the read-path-scoped invariant.

package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/runtime"
)

// rbacResponse is the GET /rbac 200 body (design §4): the RESTAction identity
// plus readSet — the canonical, deduped, sorted (group, version, resource,
// namespace, verb) list. Unresolvable stages are NOT carried in this body; they
// fail loud via the 422 error string (formatUnresolved), so a partial read-set
// is never returned as a success.
type rbacResponse struct {
	RESTAction restActionRef  `json:"restaction"`
	ReadSet    []api.Resource `json:"readSet"`
}

type restActionRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// rbacGetObjectFn and rbacInspectReadSetFn are package-private test seams over
// the handler's two external dependencies — the RA loader (objects.Get) and the
// dispatch-free inspect pass (api.InspectReadSet). Production wires the real
// functions (assigned below); the hermetic falsifier (rbac_handler_body_test.go,
// H5) swaps them so the handler's body/status logic — 422 naming an unresolvable
// stage, empty-readSet-as-[] not null, deduped-sorted 200 — is exercised without
// an apiserver or informer. Mirrors the saRESTConfigForInspectFn seam pattern in
// internal/resolvers/restactions/api/inspect.go. Behaviour is identical to a
// direct call: the defaults ARE the real functions.
var (
	rbacGetObjectFn      = objects.Get
	rbacInspectReadSetFn = api.InspectReadSet
)

// RBAC returns the GET /rbac handler. It is constructed once at mount and is
// stateless — every request reads the RA fresh from objects.Get (informer cache
// or apiserver), so a CR edit is reflected on the next call.
func RBAC() http.Handler {
	return http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		log := xcontext.Logger(req.Context())

		// The referenced RESTAction's identity. The RA GVR is fixed
		// (templates.krateo.io/v1, restactions); only name/namespace vary.
		name := req.URL.Query().Get("apiRefName")
		if name == "" {
			response.BadRequest(wri, fmt.Errorf("missing 'apiRefName' query parameter"))
			return
		}
		namespace := req.URL.Query().Get("apiRefNamespace")
		if namespace == "" {
			response.BadRequest(wri, fmt.Errorf("missing 'apiRefNamespace' query parameter"))
			return
		}

		extras, err := util.ParseExtras(req)
		if err != nil {
			response.BadRequest(wri, err)
			return
		}

		// Load the referenced RESTAction CR. objects.Get serves from the
		// informer cache when possible and falls back to a direct apiserver GET
		// — the SAME loader /call's fetchObject uses.
		got := rbacGetObjectFn(req.Context(), templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: name, Namespace: namespace},
			APIVersion: templatesv1.SchemeGroupVersion.String(),
			Resource:   "restactions",
		})
		if got.Err != nil {
			response.Encode(wri, got.Err)
			return
		}

		var cr templatesv1.RESTAction
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(got.Unstructured.Object, &cr); err != nil {
			log.Error("rbac: unable to convert unstructured to typed RESTAction",
				slog.String("name", name),
				slog.String("namespace", namespace),
				slog.Any("err", err))
			response.InternalError(wri, err)
			return
		}

		readSet, unresolved, err := rbacInspectReadSetFn(req.Context(), &cr, extras)
		// #1116 — extras that name a key the resolver writes: the same 400 a
		// /call of this RESTAction answers, naming the keys. InspectReadSet
		// refused before it enumerated anything (and logged it once).
		if dispatchers.WriteReservedExtrasRefusal(wri, err) {
			return
		}
		if err != nil {
			// A structural failure (cyclic api[], SA rest.Config unavailable):
			// the read-set cannot be trusted — fail loud.
			log.Error("rbac: inspect pass failed",
				slog.String("name", name),
				slog.String("namespace", namespace),
				slog.Any("err", err))
			response.InternalError(wri, err)
			return
		}
		if len(unresolved) > 0 {
			// At least one stage could not be enumerated from the empty dict.
			// Returning a partial read-set would silently under-grant RBAC at
			// the first /call — fail loud, naming every unresolvable stage.
			log.Warn("rbac: RESTAction has unresolvable stage(s); refusing partial read-set",
				slog.String("name", name),
				slog.String("namespace", namespace),
				slog.Any("unresolved", unresolved))
			response.Encode(wri, response.New(http.StatusUnprocessableEntity,
				fmt.Errorf("RESTAction %s/%s has stage(s) that cannot be enumerated without dispatching: %s",
					namespace, name, formatUnresolved(unresolved))))
			return
		}

		if readSet == nil {
			// Marshal an empty read-set as [] rather than null (a RESTAction
			// whose every stage is external/discovery legitimately reads no
			// in-cluster resource).
			readSet = []api.Resource{}
		}
		body := rbacResponse{
			RESTAction: restActionRef{Name: name, Namespace: namespace},
			ReadSet:    readSet,
		}

		log.Info("rbac read-set enumerated",
			slog.String("name", name),
			slog.String("namespace", namespace),
			slog.Int("read_set_rows", len(readSet)))

		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(wri)
		enc.SetIndent("", "  ")
		_ = enc.Encode(body)
	})
}

// formatUnresolved renders the unresolvable stages into a single error string
// ("stage=<name> reason=<reason>; ...").
func formatUnresolved(u []api.Unresolved) string {
	s := ""
	for i, e := range u {
		if i > 0 {
			s += "; "
		}
		s += fmt.Sprintf("stage=%s reason=%s", e.Stage, e.Reason)
	}
	return s
}
