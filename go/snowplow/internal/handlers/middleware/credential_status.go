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
// token's groups and `act` (the same input credsecret.Select chose the Secret
// from), and, for a delegated token, what endpoints.CheckDelegatedCredential
// said of its Secret (#1560) — never from the request body or an upstream
// answer. A failure that is OUR fault
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
	// CauseDelegatedCredentialMissing — a valid token carrying `act` whose
	// delegated credential Secret (authn#167) is missing, or is not authn's
	// credential for this person through this actor
	// (endpoints.DelegatedCredentialNotOwned). The client exchanges again
	// (#1560).
	CauseDelegatedCredentialMissing = "DelegatedCredentialMissing"
	// CauseDelegatedCredentialMismatch — a valid token carrying `act` whose
	// delegated credential Secret is this person's and actor's, but its
	// certificate is not valid now (endpoints.DelegatedCredentialExpired) or
	// its Organizations are not the token's groups
	// (endpoints.DelegatedCredentialMismatch). The client exchanges again
	// (#1560).
	CauseDelegatedCredentialMismatch = "DelegatedCredentialMismatch"
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
	CauseDelegatedCredentialMissing: {"the delegated credential is missing; exchange the token again",
		invalidTokenChallenge},
	CauseDelegatedCredentialMismatch: {"the delegated credential has expired or does not match the token; exchange the token again",
		invalidTokenChallenge},
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

// credentialMissingReason is the audit denial reason of a credential refusal,
// chosen from the same selection that chose the Secret: step_up_required for
// a krateo:loa:2 token, session_credential_missing otherwise (audit v0.12.0).
// #1560 — a delegated token's refusal (missing, expired or a mismatch) is
// session_credential_missing too: "the verified caller's own credential could
// not be used", the closest reason the audit schema declares. It has no
// delegated reason yet (#1562); the 401's cause carries the distinction.
func credentialMissingReason(cred credsecret.Credential) string {
	if cred.Kind == credsecret.KindStepUp {
		return audit.DenialStepUpRequired
	}
	return audit.DenialSessionCredentialMissing
}

// credentialMissing answers 401 for a per-user credential Secret that is not
// there (the apiserver re-read said NotFound, or found only a Secret that is
// not the token's). cred is credsecret.Select's choice for the VALIDATED
// token: DelegatedCredentialMissing for a token with `act` (#1560),
// StepUpRequired for a krateo:loa:2 token, SessionCredentialMissing
// otherwise.
func credentialMissing(w http.ResponseWriter, cred credsecret.Credential, err error) error {
	switch cred.Kind {
	case credsecret.KindDelegated:
		return preDispatchUnauthorized(w, CauseDelegatedCredentialMissing, err)
	case credsecret.KindStepUp:
		return preDispatchUnauthorized(w, CauseStepUpRequired, err)
	}
	return preDispatchUnauthorized(w, CauseSessionCredentialMissing, err)
}

// credentialRefused answers 401 DelegatedCredentialMismatch for a delegated
// credential that is the token's person's and actor's but cannot serve it:
// expired, or its groups are not the token's (a *credsecret.RefusedError from
// the apiserver re-read, #1560). err names no Secret and no identity.
func credentialRefused(w http.ResponseWriter, err error) error {
	return preDispatchUnauthorized(w, CauseDelegatedCredentialMismatch, err)
}
