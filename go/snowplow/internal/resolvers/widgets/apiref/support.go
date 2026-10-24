package apiref

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func convertToRESTAction(api map[string]any) (templatesv1.RESTAction, error) {
	dat, err := json.Marshal(api)
	if err != nil {
		return templatesv1.RESTAction{}, err
	}

	var ra templatesv1.RESTAction
	err = json.Unmarshal(dat, &ra)

	return ra, err
}

// statusErrorFromResponse rebuilds an `*apierrors.StatusError` from the
// status code + message that objects.Get preserves on its `Result.Err`
// (`*response.Status`). The reconstructed error satisfies the
// dispatcher's downstream `errors.As(err, *apierrors.StatusError)`
// check at internal/handlers/dispatchers/widgets.go:228-234, so an
// apiserver 403 / 404 (etc.) propagates to the SPA wire with its real
// status code instead of the prior generic HTTP 500.
//
// objects.Get's apiserver branch (internal/objects/get.go:209-214) sets
// res.Err.Code to 403 for apierrors.IsForbidden, 404 for IsNotFound,
// and 500 for any other apiserver error. The reason / message are
// captured verbatim from the apiserver. We faithfully reconstruct
// those fields here; the resulting StatusError's `.Status()` returns
// a metav1.Status whose `.Code` is the apiserver's original code.
//
// A nil input returns nil (no error to wrap).
func statusErrorFromResponse(st *response.Status, ref templatesv1.ObjectReference) *apierrors.StatusError {
	if st == nil {
		return nil
	}
	code := int32(st.Code)
	// Defence-in-depth: response.Status.Code is set by every
	// objects.Get error branch, but a 0 code would round-trip as a
	// missing field on the wire. Default to 500 so the dispatcher
	// still emits a clean status code rather than a 0 / undefined
	// response.
	if code == 0 {
		code = http.StatusInternalServerError
	}
	reason := metav1.StatusReason(st.Reason)
	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  st.Status,
			Message: st.Message,
			Reason:  reason,
			Code:    code,
			Details: &metav1.StatusDetails{
				Name: ref.Name,
				Kind: ref.Resource,
			},
		},
	}
}

func rawExtensionToMap(raw *runtime.RawExtension) (map[string]any, error) {
	m, _, err := rawExtensionToMapEncoded(raw)
	return m, err
}

// rawExtensionToMapEncoded is rawExtensionToMap plus the exact bytes it decoded
// (#883 F4: the RESTAction's encoded result, which the resolve memo stores so a
// hit is this same decode of these same bytes). The bytes are nil when nothing
// was decoded (no status); the map is then the empty map, as before.
func rawExtensionToMapEncoded(raw *runtime.RawExtension) (map[string]any, []byte, error) {
	if raw == nil {
		return map[string]any{}, nil, nil
	}

	var data []byte
	if raw.Raw != nil {
		data = raw.Raw
	} else if raw.Object != nil {
		var err error
		data, err = json.Marshal(raw.Object)
		if err != nil {
			return map[string]any{}, nil, err
		}
	} else {
		return map[string]any{}, nil, nil
	}

	var result map[string]any
	err := json.Unmarshal(data, &result)

	return result, data, err
}
