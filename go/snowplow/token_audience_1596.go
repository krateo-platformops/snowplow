package main

import (
	"expvar"
	"log/slog"

	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// tokenAudienceChecked is a startup gauge: 1 when snowplow checks the
// audience of the tokens it validates, 0 when it does not (#1596).
var tokenAudienceChecked = expvar.NewInt("snowplow_token_audience_checked")

// tokenAudienceOptions is the validator options for the configured audience
// (TOKEN_AUDIENCE, rendered by the chart). It is a security check that can
// turn off silently, so an empty name is a WARN at startup, not an INFO: every
// token is then accepted whatever its `aud`. The name is never logged.
func tokenAudienceOptions(log *slog.Logger, name string) []jwtutil.ValidateOption {
	opts := middleware.AudienceOptions(name)
	if len(opts) == 0 {
		tokenAudienceChecked.Set(0)
		// log-ctx-waiver: boot: main reads the audience once at start, before a context exists
		log.Warn("token audience check is OFF: no audience is configured (TOKEN_AUDIENCE / --token-audience is empty), so a token issued for another service is accepted; the chart sets it to <service>.<namespace>")
		return nil
	}
	tokenAudienceChecked.Set(1)
	log.Info("token audience check is on")
	return opts
}
