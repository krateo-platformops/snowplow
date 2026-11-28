// audience_1596.go — #1596: snowplow is a recipient of authn's tokens, called
// by a name (<service>.<namespace>, the chart's Service). A delegated token
// authn issued for another recipient is refused here.
//
// jwtutil.WithAudience accepts a token with no `aud` (every sign-in, step-up
// and service-account token has none), and a token whose `aud` lists the
// name, among others or alone. It refuses a token whose `aud` lists only
// others, and an empty list, with jwtutil.ErrAudienceMismatch, which also
// matches ErrTokenInvalid and comes back with the verified claims.
// WithRequiredAudience is NOT used: it would refuse every sign-in token.
//
// The refusal is the 401 cause TokenAudienceMismatch (credential_status.go).
// It is COUNTED and RECORDED. The signature has verified when the audience is
// looked at, so the sender is known: the audited route's one record is
// denied with reason audience_mismatch (audit v0.17.0, Diego's ruling
// 2026-11-28), naming the actor and its signed class from the claims plumbing
// returns with the error. A token that did not verify is counted and never a
// row (and never reaches this). The count is by the token's signed actor
// class (ActorPrincipal()), a closed set, never a name.
package middleware

import (
	"errors"
	"expvar"
	"net/http"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// AudienceOptions are the validator options a recipient called name passes to
// every jwtutil.ValidateWithKeySource. An empty name checks no audience (a
// deployment that has not configured one: the chart always does).
func AudienceOptions(name string) []jwtutil.ValidateOption {
	if name == "" {
		return nil
	}
	return []jwtutil.ValidateOption{jwtutil.WithAudience(name)}
}

// audienceMismatches counts refusals by the token's actor class: human, agent,
// service, system, or "unknown" for a token that does not say.
var audienceMismatches = expvar.NewMap("snowplow_token_audience_mismatch")

// AudienceMismatchCount reads one snowplow_token_audience_mismatch counter
// (tests, /debug/vars).
func AudienceMismatchCount(class string) int64 {
	if v, ok := audienceMismatches.Get(class).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}

// rejectToken answers the 401 for a token jwtutil.ValidateWithKeySource
// refused for the token's own fault, counting an audience mismatch first.
// ui is what the validator returned with the error (the verified claims on a
// mismatch).
func rejectToken(w http.ResponseWriter, req *http.Request, ui jwtutil.UserInfo, err error) error {
	// ErrAudienceMismatch is tested FIRST: plumbing wraps it with
	// ErrTokenInvalid, so any test of ErrTokenInvalid before it sends the
	// refusal down the counted, no-row branch.
	if errors.Is(err, jwtutil.ErrAudienceMismatch) {
		// The token verified: ui is its signed claims, so it is safe to name.
		audit.SetCaller(req.Context(), ui)
		audit.DenyRoute(req.Context(), audit.DenialAudienceMismatch)
		class := "unknown"
		if c, ok := krateoaudit.ParseClaim(ui.ActorPrincipal()); ok {
			class = string(c)
		}
		audienceMismatches.Add(class, 1)
	}
	return tokenRejected(w, err)
}
