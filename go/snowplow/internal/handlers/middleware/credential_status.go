// credential_status.go — #1029: the 401 a missing per-user credential answers
// carries a machine-readable cause.
//
// Before #1029 a missing step-up Secret (#994) and a missing session Secret
// both answered plumbing's response.Unauthorized, which has no `details`: the
// only difference was the free-text `message`, which is not a contract, so the
// portal could not tell "step up again" from "log in again".
//
// THE CONTRACT (authn + frontend, #1029 comments):
//   - the body is a Status whose first six fields — kind, apiVersion, status,
//     message, reason:"Unauthorized", code:401 — are byte-identical to
//     plumbing's response.New(401, err), in the same order, so every existing
//     reader of `message` keeps working; snowplow appends
//     `details.causes[0].reason`, the authn-style cause:
//     StepUpRequired           — the validated token carries krateo:loa:2 and
//     its step-up credential Secret is missing;
//     SessionCredentialMissing — any other token, its session credential
//     Secret is missing;
//   - the step-up case ALSO sends the RFC 9470 challenge
//     `WWW-Authenticate: Bearer error="insufficient_user_authentication"`,
//     with NO acr_values: krateo:loa:2 is authn's group, not an IdP ACR value,
//     and the IdP's values are per-OIDCConfig, which snowplow does not know.
//     The header is an optional signal; the portal never depends on it.
//
// The cause is chosen from the validated token's groups — the same input
// credsecret.For used to pick the Secret — never from the request. A re-read
// that fails for any other reason (403, 5xx) stays plumbing's 500 with no
// cause: snowplow does not know the credential is missing.
//
// plumbing's Status is not snowplow's to change, so the type is snowplow-owned.
package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// The causes snowplow emits on a missing per-user credential (#1029). authn's
// own causes (InsufficientAssurance, StaleAuthentication, StepUpNotEnabled,
// StepUpMisconfigured) are authn's and are never emitted here.
const (
	CauseStepUpRequired           = "StepUpRequired"
	CauseSessionCredentialMissing = "SessionCredentialMissing"
)

// stepUpChallenge is the RFC 9470 step-up challenge, bare: no acr_values, no
// max_age (both optional in RFC 9470).
const stepUpChallenge = `Bearer error="insufficient_user_authentication"`

type credentialStatusCause struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

type credentialStatusDetails struct {
	Causes []credentialStatusCause `json:"causes"`
}

// credentialStatus is plumbing's response.Status (same fields, same JSON
// names, same order) plus Details, which is last so the plumbing prefix of the
// encoding is unchanged.
type credentialStatus struct {
	Kind       string                  `json:"kind,omitempty"`
	APIVersion string                  `json:"apiVersion,omitempty"`
	Status     string                  `json:"status,omitempty"`
	Message    string                  `json:"message,omitempty"`
	Reason     response.StatusReason   `json:"reason,omitempty"`
	Code       int                     `json:"code,omitempty"`
	Details    credentialStatusDetails `json:"details"`
}

// credentialMissing answers 401 for a per-user credential Secret that is not
// there (the apiserver re-read said NotFound). groups are the VALIDATED
// token's groups, the input credsecret.For chose the Secret from.
func credentialMissing(w http.ResponseWriter, groups []string, err error) error {
	base := response.New(http.StatusUnauthorized, err)
	cause := credentialStatusCause{
		Reason:  CauseSessionCredentialMissing,
		Message: "the session credential is missing; log in again",
	}
	if credsecret.IsStepUp(groups) {
		cause = credentialStatusCause{
			Reason:  CauseStepUpRequired,
			Message: "the step-up credential is missing or expired; step up again",
		}
		w.Header().Set("WWW-Authenticate", stepUpChallenge)
	}
	st := credentialStatus{
		Kind:       base.Kind,
		APIVersion: base.APIVersion,
		Status:     base.Status,
		Message:    base.Message,
		Reason:     base.Reason,
		Code:       base.Code,
		Details:    credentialStatusDetails{Causes: []credentialStatusCause{cause}},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(st.Code)
	return json.NewEncoder(w).Encode(st)
}
