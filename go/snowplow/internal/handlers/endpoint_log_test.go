package handlers

// endpoint_log_test.go — #453 review (reviewer-424): the "user config
// succesfully loaded" debug line must never render a credential or the
// username. Both slog handlers are driven, because the text handler's %+v also
// renders the json:"-" fields the JSON handler omits.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

func TestS453_EndpointLogAttrRendersNoCredential(t *testing.T) {
	ep := endpoints.Endpoint{
		ServerURL:             "https://api.example:6443",
		ClientCertificateData: "CERT-zq453",
		ClientKeyData:         "KEY-zq453",
		Token:                 "TOKEN-zq453",
		Username:              "erin.zq453@tenant.example",
		Password:              "PASS-zq453",
		AwsAccessKey:          "AKIA-zq453",
		AwsSecretKey:          "AWSSECRET-zq453",
	}
	secrets := []string{"CERT-zq453", "KEY-zq453", "TOKEN-zq453", "erin.zq453", "PASS-zq453", "AKIA-zq453", "AWSSECRET-zq453"}
	for name, h := range map[string]func(*bytes.Buffer) slog.Handler{
		"json": func(b *bytes.Buffer) slog.Handler {
			return slog.NewJSONHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})
		},
		"text": func(b *bytes.Buffer) slog.Handler {
			return slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})
		},
	} {
		var buf bytes.Buffer
		slog.New(h(&buf)).Debug("user config succesfully loaded", endpointLogAttr(&ep))
		out := buf.String()
		for _, s := range secrets {
			if strings.Contains(out, s) {
				t.Errorf("%s handler: %q reached the line: %s", name, s, out)
			}
		}
		for _, want := range []string{"https://api.example:6443", "cert", redact.User(ep.Username)} {
			if !strings.Contains(out, want) {
				t.Errorf("NON-VACUITY %s handler: the projection must carry %q: %s", name, want, out)
			}
		}
	}
	if got := endpointAuthKind(&endpoints.Endpoint{Token: "t"}); got != "token" {
		t.Errorf("auth kind for a token endpoint = %q", got)
	}
	if got := endpointAuthKind(&endpoints.Endpoint{}); got != "none" {
		t.Errorf("auth kind for an empty endpoint = %q", got)
	}
}
