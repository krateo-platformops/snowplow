package handlers

import (
	"log/slog"
	"net/url"

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
func logServerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable>"
	}
	u.User = nil
	return u.String()
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
