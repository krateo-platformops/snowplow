// reserved_extras_status.go — #1116: the 400 a /call answers when its extras
// name a key the RESTAction resolver writes itself.
//
// THE CONTRACT (docs/howto-frontend-reserved-extras-keys.md):
//   - the body is a Status whose first six fields — kind, apiVersion, status,
//     message, reason:"BadRequest", code:400 — are byte-identical to
//     plumbing's response.New(400, err), in the same order, so every reader
//     of `message` keeps working;
//   - snowplow appends `details.causes`, one per reserved key the request
//     sent, sorted by key: {reason:"ReservedExtrasKey", field:"extras.<key>",
//     message}. `field` is the machine-readable part: the extras key to stop
//     sending. reason is the one constant below.
//
// Every route that resolves a RESTAction renders it: GET /call and POST
// /call/read of a RESTAction or of a widget (its apiRef), /export, which
// relays the /call envelope unchanged, and GET /rbac, whose read-set
// enumeration seeds the same dict from the same extras (api.InspectReadSet).
// The refusal happens before anything is read or dialled, so a RESTAction
// /call carries the X-Snowplow-Stage-Outcomes of an aborted resolve (a
// resolve-level Aborted row, every stage NotRun).
//
// plumbing's Status is not snowplow's to change, so the type is snowplow-owned
// (the #1029 credential_status.go precedent).
package dispatchers

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// CauseReservedExtrasKey is the details.causes[].reason of a #1116 refusal.
const CauseReservedExtrasKey = "ReservedExtrasKey"

type reservedExtrasCause struct {
	Reason  string `json:"reason"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

type reservedExtrasDetails struct {
	Causes []reservedExtrasCause `json:"causes"`
}

// reservedExtrasStatus is plumbing's response.Status (same fields, same JSON
// names, same order) plus Details, last.
type reservedExtrasStatus struct {
	Kind       string                `json:"kind,omitempty"`
	APIVersion string                `json:"apiVersion,omitempty"`
	Status     string                `json:"status,omitempty"`
	Message    string                `json:"message,omitempty"`
	Reason     response.StatusReason `json:"reason,omitempty"`
	Code       int                   `json:"code,omitempty"`
	Details    reservedExtrasDetails `json:"details"`
}

// isReservedExtrasRefusal reports whether err is (or wraps) a #1116 refusal:
// the resolver refused the request's extras before reading anything.
func isReservedExtrasRefusal(err error) bool {
	_, ok := api.AsReservedExtrasError(err)
	return ok
}

// WriteReservedExtrasRefusal answers the 400 when err is (or wraps) a #1116
// refusal and reports whether it did. Any other error is left to the caller.
func WriteReservedExtrasRefusal(w http.ResponseWriter, err error) bool {
	re, ok := api.AsReservedExtrasError(err)
	if !ok {
		return false
	}
	base := response.New(http.StatusBadRequest, re)
	st := reservedExtrasStatus{
		Kind:       base.Kind,
		APIVersion: base.APIVersion,
		Status:     base.Status,
		Message:    base.Message,
		Reason:     base.Reason,
		Code:       base.Code,
		Details:    reservedExtrasDetails{Causes: make([]reservedExtrasCause, 0, len(re.Keys))},
	}
	for _, k := range re.Keys {
		st.Details.Causes = append(st.Details.Causes, reservedExtrasCause{
			Reason:  CauseReservedExtrasKey,
			Field:   "extras." + k.Key,
			Message: k.Message(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(st.Code)
	_ = json.NewEncoder(w).Encode(st)
	return true
}
