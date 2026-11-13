package main

import (
	"log/slog"

	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// envPublicIdentityEnabled is the #1334 opt-in. The chart renders it from
// publicIdentity.enabled, the same value that ships the impersonate grant, and
// refuses it in values.env so the two cannot disagree.
const envPublicIdentityEnabled = "PUBLIC_IDENTITY_ENABLED"

// newPublicIdentity returns the public identity backed by snowplow's own
// ServiceAccount transport when on, and nil (token-less requests are 401, as
// before #1334) when off.
func newPublicIdentity(on bool) *middleware.PublicIdentity {
	if !on {
		return nil
	}
	// log-ctx-waiver: boot: main wires the routes once at start
	slog.Info("public identity enabled: a request with no Authorization header is served as the public identity, by impersonation",
		slog.String("identity_class", middleware.PublicIdentityClass),
		slog.String("needs", "impersonate on users [krateo:public] and groups [system:unauthenticated]"))
	return middleware.NewPublicIdentity(idynamic.ServiceAccountEndpoint, idynamic.ServiceAccountRESTConfig)
}
