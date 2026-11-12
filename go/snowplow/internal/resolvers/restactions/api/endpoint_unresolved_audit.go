// endpoint_unresolved_audit.go — the record of a write stage whose endpoint
// could not be resolved (audit#22 (a), AUDIT-4 installer#410): the write was
// asked for and not sent, so it is in the trail.
//
//   - snowplow's own refusals are denied, with their reason:
//     internal_identity_context (a caller-supplied draft resolved on a context
//     that carries snowplow's own identity, or none of the caller's),
//     endpoint_ref_malformed (the named Secret exists and is not a usable
//     endpoint);
//   - a read of the Secret that failed is a failure: the code the apiserver
//     answered that read with (a NotFound, a Forbidden), or none when nothing
//     answered (a transport error, the page budget). The write was attempted
//     and failed, and a failed write attempt is never invisible.
//
// The endpoint was not resolved, so the record names no host: a named
// endpointRef stage is marked target. The reference itself is not named
// either: krateo.audit.endpoint_ref.* only rides a record that names the
// endpoint it was resolved to (schema v1, only_when endpoint_target).
package api

import (
	"errors"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// errInternalIdentityContext marks resolveOneAsCaller's refusals: a
// caller-supplied draft's endpointRef is read only as the caller, never on a
// context carrying snowplow's own identity or none of the caller's.
var errInternalIdentityContext = errors.New("caller-supplied endpointRef refused")

// msgEndpointSecretNoServerURL is the error both endpoint Secret readers return
// for a Secret with no server-url: plumbing's endpoints.FromSecret (v1.54.0
// endpoints/endpoints.go:33) and its byte-identical mirror
// extractEndpointFromSecret (pinned by endpoints_cache_test.go). It is the one
// way an endpoint Secret is malformed.
const msgEndpointSecretNoServerURL = "missed required attribute for endpoint: server-url"

// auditEndpointUnresolved records a write stage whose endpoint (ref; nil: the
// caller's own clientconfig) did not resolve with err.
func (r *resolveRun) auditEndpointUnresolved(apiCall *templates.API, ref *templates.Reference, err error) {
	sr := stageRefusal{apiCall: apiCall, outcome: audit.OutcomeFailure}
	var status apierrors.APIStatus
	switch {
	case errors.Is(err, errInternalIdentityContext):
		sr.outcome, sr.reason = audit.OutcomeDenied, audit.DenialInternalIdentityContext
	case ref != nil && err.Error() == msgEndpointSecretNoServerURL:
		sr.outcome, sr.reason = audit.OutcomeDenied, audit.DenialEndpointRefMalformed
	case errors.As(err, &status):
		sr.code = int(status.Status().Code)
	}
	r.auditStageRefusal(r.ctx, sr)
}
