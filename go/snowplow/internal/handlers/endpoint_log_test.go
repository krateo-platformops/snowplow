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

// TestS487_EndpointLogAttrStripsURLUserinfo — reviewer-424 (#487): a server
// URL carrying userinfo (user:password@host) must not reach the line.
func TestS487_EndpointLogAttrStripsURLUserinfo(t *testing.T) {
	ep := endpoints.Endpoint{ServerURL: "https://ops-zq487:hunter2-zq487@api.example:6443/base"}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("m", endpointLogAttr(&ep))
	out := buf.String()
	for _, s := range []string{"hunter2-zq487", "ops-zq487"} {
		if strings.Contains(out, s) {
			t.Errorf("userinfo %q reached the line: %s", s, out)
		}
	}
	if !strings.Contains(out, "https://api.example:6443/base") {
		t.Errorf("NON-VACUITY: the host and path must survive: %s", out)
	}
	if got := logServerURL("://bad url"); got != "<unparseable>" {
		t.Errorf("an unparseable URL must not be logged, got %q", got)
	}
}

// TestS499_EndpointLogAttrStripsUserinfoFromASchemelessURL — #499.
//
// TestS487_EndpointLogAttrStripsURLUserinfo above covers only the scheme'd
// form, which is exactly why the schemeless hole shipped: url.Parse reads the
// "ops-zq499:" of "ops-zq499:hunter2@host/x" as the SCHEME, so there is no
// User to clear, and the old `u.User = nil` was a no-op that round-tripped the
// password verbatim into otel_logs. This drives the REAL log attr, not
// redact.URL directly, so it fails if endpointLogAttr ever stops routing
// through the shared sanitiser.
func TestS499_EndpointLogAttrStripsUserinfoFromASchemelessURL(t *testing.T) {
	const (
		user = "ops-zq499"
		pass = "hunter2-zq499"
	)
	for _, raw := range []string{
		user + ":" + pass + "@api.example/base",                   // schemeless — the #499 leak
		user + ":" + pass + "@api.example:6443/base",              // schemeless with a port
		"https://" + user + ":" + pass + "@api.example:6443/base", // scheme'd — must stay fixed
	} {
		ep := endpoints.Endpoint{ServerURL: raw}
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).Info("m", endpointLogAttr(&ep))
		if out := buf.String(); strings.Contains(out, pass) {
			t.Errorf("ServerURL %q: the password reached the line: %s", raw, out)
		}
	}
}

// TestS499_EndpointLogAttrRedactsQueryValues — a credential in the query of a
// server URL ("?token=…") is a value, not metadata: the KEY survives so the
// line still says the URL carried a token, the value never does.
func TestS499_EndpointLogAttrRedactsQueryValues(t *testing.T) {
	const secret = "tok-zq499-secret"
	ep := endpoints.Endpoint{ServerURL: "https://api.example:6443/base?token=" + secret}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("m", endpointLogAttr(&ep))
	out := buf.String()
	if strings.Contains(out, secret) {
		t.Errorf("the query credential reached the line: %s", out)
	}
	if !strings.Contains(out, "token=") {
		t.Errorf("NON-VACUITY: the query KEY must survive so the line stays diagnostic: %s", out)
	}
}
