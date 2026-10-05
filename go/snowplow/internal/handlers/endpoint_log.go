package handlers

import (
	"log/slog"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// endpointLogAttr is the only form of a user endpoint that may reach a log
// line (#453 review). Pre-fix call.go and list.go logged the whole
// endpoints.Endpoint via slog.Any. At debug the JSON handler then rendered
// Username, Token, Password and AwsSecretKey in clear, and the text handler
// also rendered ClientKeyData and ClientCertificateData. Only the redacted
// username, the server URL and the auth kind are logged here, never a
// credential.
func endpointLogAttr(ep *endpoints.Endpoint) slog.Attr {
	if ep == nil {
		return slog.String("endpoint", "<nil>")
	}
	return slog.Group("endpoint",
		slog.String("user", redact.User(ep.Username)),
		slog.String("server_url", logServerURL(ep.ServerURL)),
		slog.String("auth", endpointAuthKind(ep)),
	)
}

// logServerURL is the server URL with any userinfo (user:password@) removed
// (#487, reviewer-424). An unparseable URL is not logged at all.
//
// #499: this used to be url.Parse + `u.User = nil` inline, which is a NO-OP for
// a schemeless URL — url.Parse reads "u:" of "u:p@host/x" as the scheme, so
// there is no User to clear and the password round-tripped verbatim into
// otel_logs. The stripping now lives in redact.URL, shared with the outbound
// span redactor (#489) so the one invariant has one implementation.
func logServerURL(raw string) string {
	return redact.URL(raw)
}

// endpointAuthKind names the credential kind an endpoint carries, never the
// credential.
func endpointAuthKind(ep *endpoints.Endpoint) string {
	switch {
	case ep.HasCertAuth():
		return "cert"
	case ep.HasTokenAuth():
		return "token"
	case ep.HasBasicAuth():
		return "basic"
	case ep.HasAwsAuth():
		return "aws"
	default:
		return "none"
	}
}
