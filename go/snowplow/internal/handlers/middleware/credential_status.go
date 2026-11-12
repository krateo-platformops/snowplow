// credential_status.go — #1029 + #1076: every 401 snowplow itself writes
// BEFORE dispatch carries a machine-readable cause, from one closed set.
//
// Before #1029 a missing step-up Secret (#994) and a missing session Secret
// both answered plumbing's response.Unauthorized, which has no `details`: the
// only difference was the free-text `message`, which is not a contract, so the
// portal could not tell "step up again" from "log in again". Before #1076 the
// token rejections (no token, not Bearer, bad signature, expired) still had no
// `details`, exactly like a 401 an upstream apiserver or a RESTAction stage
// answered AFTER snowplow had dispatched the request (possibly after a write).
// A client could not tell "snowplow refused; nothing was sent" from "this came
// back from upstream; a write may already have happened".
//
// THE CONTRACT (#1029 authn + frontend comments, #1076):
//   - the body is a Status whose first six fields — kind, apiVersion, status,
//     message, reason:"Unauthorized", code:401 — are byte-identical to
//     plumbing's response.New(401, err), in the same order, so every existing
//     reader of `message` keeps working; snowplow appends
//     `details.causes[0].reason`, one of the closed set below;
//   - every cause in the set means the same thing about dispatch: snowplow
//     refused the request in its auth middleware, before any handler ran, so
//     NOTHING was dispatched — no apiserver call, no RESTAction stage, no
//     write;
//   - a 401 snowplow RELAYS (a raw /call passthrough's apiserver answer, a
//     RESTAction stage's or a widget dependency's upstream status, an export's
//     inner /call failure) is written through plumbing's
//     response.Status, which has no Details field, so it never carries a
//     cause, whatever the upstream body said;
//   - the challenge header: StepUpRequired sends the RFC 9470 challenge
//     `Bearer error="insufficient_user_authentication"` (no acr_values:
//     krateo:loa:2 is authn's group, not an IdP ACR value); TokenMissing sends
//     a bare `Bearer` (RFC 6750 §3.1: a request with no authentication
//     information gets no error code); every other cause sends the RFC 6750
//     `Bearer error="invalid_token"`. The header is an optional signal; the
//     body is the contract.
//
// Each cause is chosen from what the middleware itself established — whether a
// token was presented, jwtutil's typed validation error, the validated
// token's groups (the same input credsecret.For chose the Secret from) — never
// from the request body or an upstream answer. A failure that is OUR fault
// stays a non-401 without a cause: an unresolvable signing key is plumbing's
// 503, a credential re-read that failed for any reason but NotFound (403, 5xx)
// is plumbing's 500.
//
// plumbing's Status is not snowplow's to change, so the type is snowplow-owned.
package middleware

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The closed set of causes snowplow emits on a pre-dispatch 401 (#1029,
// #1076). authn's own causes (InsufficientAssurance, StaleAuthentication,
// StepUpNotEnabled, StepUpMisconfigured) are authn's and are never emitted
// here.
const (
	// CauseTokenMissing — no bearer token was presented: no Authorization
	// header, one whose scheme is not Bearer, or `Bearer ` with an empty token
	// (and, on RefreshAuth's routes, no session cookie either).
	CauseTokenMissing = "TokenMissing"
	// CauseTokenInvalid — the bearer token failed validation: malformed, bad
	// signature, wrong algorithm (jwtutil.ErrTokenInvalid).
	CauseTokenInvalid = "TokenInvalid"
	// CauseTokenExpired — the bearer token is correctly signed but expired
	// (jwtutil.ErrTokenExpired).
	CauseTokenExpired = "TokenExpired"
	// CauseKeyNotPublished — the bearer token names a signing key (kid) that
	// authn's key set, fetched successfully after the token was issued, does
	// not publish: the key was retired, for example to revoke the tokens it
	// signed (jwtutil.ErrKeyNotPublished, plumbing v1.36.0). No retry makes
	// the token verify. Named after its jwtutil error, as TokenInvalid and
	// TokenExpired are.
	CauseKeyNotPublished = "KeyNotPublished"
	// CauseSessionCredentialMissing — a valid token without krateo:loa:2
	// whose session credential Secret is missing.
	CauseSessionCredentialMissing = "SessionCredentialMissing"
	// CauseStepUpRequired — a valid token carrying krateo:loa:2 whose step-up
	// credential Secret is missing.
	CauseStepUpRequired = "StepUpRequired"
)

// stepUpChallenge is the RFC 9470 step-up challenge, bare: no acr_values, no
// max_age (both optional in RFC 9470).
const stepUpChallenge = `Bearer error="insufficient_user_authentication"`

// invalidTokenChallenge is the RFC 6750 §3.1 challenge for a token that was
// presented and cannot be used: obtain a new one.
const invalidTokenChallenge = `Bearer error="invalid_token"`

// bareBearerChallenge is the RFC 6750 §3.1 challenge for a request that
// carried no bearer token at all: the scheme, with no error code ("the
// resource server SHOULD NOT include an error code or other error
// information").
const bareBearerChallenge = `Bearer`

// preDispatchCause is one entry of the closed set: its fixed human sentence
// (no identity in it) and the challenge it sends.
type preDispatchCause struct {
	message   string
	challenge string
}

// preDispatchCauses IS the closed set. preDispatchUnauthorized refuses any
// cause that is not a key here.
var preDispatchCauses = map[string]preDispatchCause{
	CauseTokenMissing:             {"no bearer token was presented; log in", bareBearerChallenge},
	CauseTokenInvalid:             {"the bearer token is not valid; log in again", invalidTokenChallenge},
	CauseTokenExpired:             {"the bearer token has expired; log in again", invalidTokenChallenge},
	CauseKeyNotPublished:          {"the bearer token's signing key is no longer published; log in again", invalidTokenChallenge},
	CauseSessionCredentialMissing: {"the session credential is missing; log in again", invalidTokenChallenge},
	CauseStepUpRequired:           {"the step-up credential is missing or expired; step up again", stepUpChallenge},
}

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

// preDispatchUnauthorized writes the pre-dispatch 401 for cause: plumbing's
// six Status fields for err, then details.causes[0], plus the cause's
// challenge. A cause outside the closed set is a programming error: it is
// answered as plumbing's 500, never as a 401 with an invented cause.
func preDispatchUnauthorized(w http.ResponseWriter, cause string, err error) error {
	c, ok := preDispatchCauses[cause]
	if !ok {
		return response.InternalError(w, errors.New("a pre-dispatch 401 was given a cause outside the closed set"))
	}
	base := response.New(http.StatusUnauthorized, err)
	st := credentialStatus{
		Kind:       base.Kind,
		APIVersion: base.APIVersion,
		Status:     base.Status,
		Message:    base.Message,
		Reason:     base.Reason,
		Code:       base.Code,
		Details:    credentialStatusDetails{Causes: []credentialStatusCause{{Reason: cause, Message: c.message}}},
	}
	w.Header().Set("WWW-Authenticate", c.challenge)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(st.Code)
	return json.NewEncoder(w).Encode(st)
}

// tokenMissing answers 401 when no bearer token was presented.
func tokenMissing(w http.ResponseWriter, err error) error {
	return preDispatchUnauthorized(w, CauseTokenMissing, err)
}

// tokenRejected answers 401 for a token jwtutil.ValidateWithKeySource refused
// for the token's own fault. err is that typed error: ErrKeyNotPublished is
// KeyNotPublished, ErrTokenExpired is TokenExpired, anything else is
// TokenInvalid. The caller has already answered ErrKeyUnavailable (503, our
// fault), which ErrKeyNotPublished never matches.
//
// ErrKeyNotPublished is checked FIRST: jwtutil reports a retired key as
// ErrTokenInvalid AND ErrKeyNotPublished, so the generic branch would swallow
// it.
func tokenRejected(w http.ResponseWriter, err error) error {
	if errors.Is(err, jwtutil.ErrKeyNotPublished) {
		return preDispatchUnauthorized(w, CauseKeyNotPublished, err)
	}
	if errors.Is(err, jwtutil.ErrTokenExpired) {
		return preDispatchUnauthorized(w, CauseTokenExpired, err)
	}
	return preDispatchUnauthorized(w, CauseTokenInvalid, err)
}

// credentialMissingReason is the audit denial reason of credentialMissing's
// refusal, chosen from the same validated groups that chose the Secret:
// step_up_required for a krateo:loa:2 token, session_credential_missing
// otherwise (audit v0.12.0).
func credentialMissingReason(groups []string) string {
	if credsecret.IsStepUp(groups) {
		return audit.DenialStepUpRequired
	}
	return audit.DenialSessionCredentialMissing
}

// lookupFailureCode is the status of a credential lookup that failed for any
// reason but NotFound: the code the apiserver answered the Secret read with,
// when it answered, else snowplow's own 500.
func lookupFailureCode(err error) int {
	var st apierrors.APIStatus
	if errors.As(err, &st) && st.Status().Code != 0 {
		return int(st.Status().Code)
	}
	return http.StatusInternalServerError
}

// credentialMissing answers 401 for a per-user credential Secret that is not
// there (the apiserver re-read said NotFound). groups are the VALIDATED
// token's groups, the input credsecret.For chose the Secret from.
func credentialMissing(w http.ResponseWriter, groups []string, err error) error {
	if credsecret.IsStepUp(groups) {
		return preDispatchUnauthorized(w, CauseStepUpRequired, err)
	}
	return preDispatchUnauthorized(w, CauseSessionCredentialMissing, err)
}
